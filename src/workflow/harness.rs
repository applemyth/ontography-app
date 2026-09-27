//! Builtin workers consume scoped inputs and publish through core admission.

use super::document::{DocumentNode, NodeKind, WorkflowPayload};
use crate::persistence::write_json;
use nix::{
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use ontography::{
    ContentId, ContextPolicy, Emission, ExecutionContext, ExecutionFailure, ExecutionSignal,
    InvocationHandle, InvocationTrigger, OutputAuthority, PackageEnvelope, PackageStore, Payload,
    ProposalDecision, WorkspacePolicy, workspace::WorkspaceStore,
};
use serde_json::json;
use std::{
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::watch,
};

const OUTPUT_LIMIT: usize = 1024 * 1024;
type WorkerResult<T> = std::result::Result<T, ExecutionFailure>;

fn failure(error: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::new("workflow_worker", error.to_string())
}

/// Settings are sampled before each task; an in-flight task keeps its settings.
/// Human and inbox work remains pending for document-level interaction tools.
pub async fn run(
    mut context: ExecutionContext,
    mut settings: watch::Receiver<DocumentNode>,
    project: PathBuf,
    directory: PathBuf,
    mut initial: Option<Payload>,
) -> WorkerResult<()> {
    tokio::fs::create_dir_all(&directory)
        .await
        .map_err(failure)?;
    recover_process(&directory).await?;
    loop {
        if context.stop().is_requested() {
            return Ok(());
        }
        let node = settings.borrow_and_update().clone();
        if initial.is_some() {
            let marker = directory.join("initial-complete.json");
            if marker.exists() && crate::persistence::read_json::<bool>(&marker).map_err(failure)? {
                initial = None;
            }
        }
        if node.kind == NodeKind::Inbox
            && let Some(input) = initial.take()
        {
            accept_initial_sink(&context, &node, &directory, input).await?;
        }
        if matches!(node.kind, NodeKind::Human | NodeKind::Inbox) {
            tokio::select! {
                changed = settings.changed() => { if changed.is_err() { return Ok(()); } }
                signal = context.next_signal() => {
                    if !matches!(signal, ExecutionSignal::FrontierChanged(_)) { return Ok(()); }
                }
            }
            continue;
        }
        let root_input = initial.take();
        let is_initial = root_input.is_some();
        let task = if let Some(input) = root_input {
            let authority = context
                .kernel()
                .await
                .map_err(failure)?
                .root_ceiling(context.node_id())
                .cloned()
                .ok_or_else(|| failure("The initial worker is no longer the entry node"))?;
            Some((
                InvocationTrigger::Root {
                    authority,
                    input: input.clone(),
                },
                vec![input],
            ))
        } else {
            let pending = context.next_trigger().await.map_err(failure)?;
            if pending.packages().is_empty() {
                None
            } else {
                let mut payloads = Vec::new();
                for (_, package) in pending.packages() {
                    payloads.push(
                        context
                            .content(package.content_digest())
                            .await
                            .map_err(failure)?
                            .ok_or_else(|| failure("Pending input content is unavailable"))?,
                    );
                }
                Some((
                    InvocationTrigger::Packages(
                        pending.packages().iter().map(|(id, _)| *id).collect(),
                    ),
                    payloads,
                ))
            }
        };
        let Some((trigger, payloads)) = task else {
            tokio::select! {
                changed = settings.changed() => { if changed.is_err() { return Ok(()); } }
                signal = context.next_signal() => {
                    if !matches!(signal, ExecutionSignal::FrontierChanged(_)) { return Ok(()); }
                }
            }
            continue;
        };
        // The frontier read can await; take the latest committed settings at
        // the task boundary rather than carrying the earlier idle snapshot.
        let node = settings.borrow_and_update().clone();
        if matches!(node.kind, NodeKind::Human | NodeKind::Inbox) {
            if let InvocationTrigger::Root { input, .. } = trigger {
                initial = Some(input);
            }
            continue;
        }
        let invocation = begin(&context, trigger, &payloads).await?;
        let outcome = perform(
            &context,
            &invocation,
            &node,
            &project,
            &directory,
            &payloads,
        )
        .await;
        match outcome {
            Ok(()) => {
                if is_initial {
                    // Core's accepted invocation is authoritative if this
                    // disposable completion marker cannot be written.
                    let _ = write_json(&directory.join("initial-complete.json"), &true);
                }
            }
            Err(error) => {
                let stopped = context.stop().is_requested();
                if stopped {
                    let _ = invocation.interrupt(error.message()).await;
                } else {
                    let _ = invocation.fail(error.message()).await;
                }
                let _ = write_json(
                    &directory.join("output.json"),
                    &json!({
                        "invocation_id":invocation.id().to_string(), "node":node.id,
                        "publication_status":"failed", "error":error.message(),
                    }),
                );
                return if stopped { Ok(()) } else { Err(error) };
            }
        }
    }
}

async fn accept_initial_sink(
    context: &ExecutionContext,
    node: &DocumentNode,
    directory: &Path,
    input: Payload,
) -> WorkerResult<()> {
    let result = WorkflowPayload::decode(&input).map_err(failure)?;
    let contents = if let WorkflowPayload::Workspace(envelope) = &result {
        PackageStore::new(context.content_store().await.map_err(failure)?)
            .dependencies(envelope.ontography_package)
            .await
            .map_err(failure)?
    } else {
        vec![]
    };
    let authority = context
        .kernel()
        .await
        .map_err(failure)?
        .root_ceiling(context.node_id())
        .cloned()
        .ok_or_else(|| failure("The initial inbox is no longer the entry node"))?;
    let invocation = context
        .begin_invocation_with_content(
            InvocationTrigger::Root {
                authority,
                input: input.clone(),
            },
            ContextPolicy::default(),
            contents.clone(),
        )
        .await
        .map_err(failure)?;
    invocation.prepare_context().await.map_err(failure)?;
    let mut report = json!({"node":node.id,"invocation_id":invocation.id().to_string(),"result":result,"publication_status":"prepared"});
    write_json(&directory.join("output.json"), &report).map_err(failure)?;
    match invocation
        .submit(input, vec![], contents)
        .await
        .map_err(failure)?
    {
        ProposalDecision::Committed(_) => {
            report["publication_status"] = json!("committed");
            let _ = write_json(&directory.join("output.json"), &report);
            let _ = write_json(&directory.join("initial-complete.json"), &true);
            Ok(())
        }
        ProposalDecision::Rejected(error) => Err(failure(error)),
    }
}

async fn begin(
    context: &ExecutionContext,
    trigger: InvocationTrigger,
    payloads: &[Payload],
) -> WorkerResult<InvocationHandle> {
    let mut workspaces = Vec::new();
    for payload in payloads {
        if let WorkflowPayload::Workspace(envelope) =
            WorkflowPayload::decode(payload).map_err(failure)?
        {
            workspaces.push(envelope.ontography_package);
        }
    }
    if workspaces.len() > 1 {
        return Err(failure(
            "A task may receive only one workspace; combine workspaces before this node",
        ));
    }
    let mut policy = ContextPolicy::default();
    let mut contents = Vec::new();
    if let Some(workspace) = workspaces.first() {
        let kernel = context.kernel().await.map_err(failure)?;
        let output = kernel.graph().edges().iter().find(|edge| edge.source() == context.node_id())
            .ok_or_else(|| failure("A workspace worker needs an outgoing connection; connect it to an inbox to retain its result"))?;
        policy.workspace = Some(WorkspacePolicy {
            input_edge: None,
            writable: true,
            output_edge: Some(output.id().into()),
        });
        if matches!(trigger, InvocationTrigger::Root { .. }) {
            contents = PackageStore::new(context.content_store().await.map_err(failure)?)
                .resolve(*workspace)
                .await
                .map_err(failure)?
                .dependencies();
        }
    }
    context
        .begin_invocation_with_content(trigger, policy, contents)
        .await
        .map_err(failure)
}

async fn perform(
    context: &ExecutionContext,
    invocation: &InvocationHandle,
    node: &DocumentNode,
    project: &Path,
    directory: &Path,
    payloads: &[Payload],
) -> WorkerResult<()> {
    // Preparation records the exact source exposure, including resolved package views.
    invocation.prepare_context().await.map_err(failure)?;
    let workspace_store = WorkspaceStore::new(
        context.content_store().await.map_err(failure)?,
        directory.join("workspaces"),
    );
    let workspace = if invocation.policy().workspace.is_some() {
        let (_, base) = invocation.workspace_package().await.map_err(failure)?;
        tokio::fs::create_dir_all(workspace_store.checkouts_dir())
            .await
            .map_err(failure)?;
        let checkout = workspace_store
            .checkout(
                &base,
                workspace_store
                    .checkouts_dir()
                    .join(invocation.id().to_string()),
            )
            .await
            .map_err(failure)?;
        let exposure = invocation
            .record_workspace_exposure(base.root())
            .await
            .map_err(failure)?;
        Some((checkout, base.root(), exposure.sequence))
    } else {
        None
    };
    let mut parts = Vec::new();
    if node.kind == NodeKind::Agent {
        parts.push(
            node.config["prompt"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
        );
    }
    for payload in payloads {
        if let WorkflowPayload::Message { message } =
            WorkflowPayload::decode(payload).map_err(failure)?
        {
            parts.push(message);
        }
    }
    let input = parts.join("\n\n");
    let receipt = invocation
        .record_initial_input(input.clone().into_bytes().into())
        .await
        .map_err(failure)?;
    let cwd = workspace
        .as_ref()
        .map_or(project, |(checkout, _, _)| checkout.path());
    let output = process(
        context,
        ProcessInput {
            invocation,
            receipt: receipt.sequence,
            workspace_receipt: workspace.as_ref().map(|(_, _, sequence)| *sequence),
            node,
            cwd,
            directory,
            input: input.into_bytes(),
        },
    )
    .await?;
    let mut contents: Vec<ContentId> = Vec::new();
    let result = if let Some((checkout, base, _)) = &workspace {
        let capture = workspace_store
            .capture_staged(checkout.path(), *base)
            .await
            .map_err(failure)?;
        let envelope = WorkflowPayload::Workspace(PackageEnvelope::new(capture.package().root()));
        contents = capture.package().dependencies();
        invocation
            .record_tool_response("workspace_capture", envelope.encode().map_err(failure)?)
            .await
            .map_err(failure)?;
        capture.retain().await.map_err(failure)?;
        envelope
    } else {
        WorkflowPayload::Message {
            message: output.stdout.clone(),
        }
    };
    let encoded = result.encode().map_err(failure)?;
    invocation
        .record_tool_response("worker_output", encoded.clone())
        .await
        .map_err(failure)?;
    let mut report = json!({"invocation_id":invocation.id().to_string(), "node":node.id,
        "result":result, "stdout":output.stdout, "stderr":output.stderr, "publication_status":"prepared"});
    write_json(&directory.join("output.json"), &report).map_err(failure)?;
    // Fetch routes after execution; the admitted result always follows the current graph.
    let kernel = context.kernel().await.map_err(failure)?;
    let emissions = kernel
        .graph()
        .edges()
        .iter()
        .filter(|edge| edge.source() == context.node_id())
        .map(|edge| Emission::new(edge.id(), OutputAuthority::Carry, encoded.clone()))
        .collect();
    match invocation
        .submit(encoded, emissions, contents)
        .await
        .map_err(failure)?
    {
        ProposalDecision::Committed(id) => {
            report["publication_status"] = json!("committed");
            report["activation_id"] = json!(id.to_string());
        }
        ProposalDecision::Rejected(error) => {
            return Err(failure(format!("Core rejected worker output: {error}")));
        }
    }
    if let Some((checkout, _, _)) = workspace
        && let Err(error) = checkout.remove().await
    {
        report["cleanup_error"] = json!(error.to_string());
    }
    // A cache failure after admission must never turn an accepted task into a
    // retry. Status readers can reconcile this cache with the invocation ID.
    let _ = write_json(&directory.join("output.json"), &report);
    Ok(())
}

struct ProcessOutput {
    stdout: String,
    stderr: String,
}

struct ProcessInput<'a> {
    invocation: &'a InvocationHandle,
    receipt: u64,
    workspace_receipt: Option<u64>,
    node: &'a DocumentNode,
    cwd: &'a Path,
    directory: &'a Path,
    input: Vec<u8>,
}

// The supervisor does not launch user code until its durable lease is saved.
// Its stdin is a lifetime pipe: parent death closes it and terminates the entire
// group. A nonce-bound result file preserves the command's exit code while the
// supervisor kills itself and any remaining descendants, even on normal exit.
const SUPERVISOR: &str = r#"
owner=$$
token=$1
input=$2
result=$3
shift 3
cleanup() {
    trap '' HUP INT TERM
    kill -TERM -"$owner" 2>/dev/null
    sleep 0.05
    kill -KILL -"$owner" 2>/dev/null
    exit 125
}
trap cleanup HUP INT TERM
IFS= read -r permit || cleanup
[ "$permit" = "$token" ] || cleanup
exec 3<&0
( IFS= read -r ignored <&3; kill -TERM -"$owner" 2>/dev/null ) &
"$@" <"$input" 3<&- &
task=$!
wait "$task"
code=$?
printf '%s %s\n' "$token" "$code" >"$result"
cleanup
"#;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessLease {
    pid: i32,
    token: String,
    identity: String,
}

struct ProcessGroup(Option<Pid>);
impl ProcessGroup {
    fn disarm(&mut self) {
        self.0 = None;
    }

    fn terminate(&mut self) {
        self.terminate_with(|pid| {
            let _ = killpg(pid, Signal::SIGKILL);
        });
    }

    fn terminate_with(&mut self, signal: impl FnOnce(Pid)) {
        if let Some(pid) = self.0.take() {
            signal(pid);
        }
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        self.terminate();
    }
}

struct ProcessFiles {
    directory: PathBuf,
    token: String,
    leased: bool,
}

impl ProcessFiles {
    fn path(&self, suffix: &str) -> PathBuf {
        self.directory.join(format!("{}.{}", self.token, suffix))
    }
}

impl Drop for ProcessFiles {
    fn drop(&mut self) {
        for suffix in ["input", "status"] {
            let _ = std::fs::remove_file(self.path(suffix));
        }
        if self.leased
            && crate::persistence::read_json::<ProcessLease>(&lease_path(&self.directory))
                .is_ok_and(|lease| lease.token == self.token)
        {
            let _ = std::fs::remove_file(lease_path(&self.directory));
        }
    }
}

struct SupervisedProcess {
    child: Option<tokio::process::Child>,
    group: ProcessGroup,
    lifetime: Option<tokio::process::ChildStdin>,
    token: String,
    files: Option<ProcessFiles>,
}

impl SupervisedProcess {
    fn child(&mut self) -> &mut tokio::process::Child {
        self.child.as_mut().expect("owned supervisor")
    }

    async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        let result = self.child().wait().await;
        if result.is_ok() {
            // Reaping releases the PID. This must precede any other await or
            // output error, because that number can now belong to a new group.
            self.group.disarm();
        }
        result
    }

    fn terminate(&mut self) {
        if matches!(self.child().try_wait(), Ok(Some(_))) {
            self.group.disarm();
        } else {
            // The unreaped child still reserves its PID while we signal.
            self.group.terminate();
        }
    }
}

impl Drop for SupervisedProcess {
    fn drop(&mut self) {
        if self.child.is_none() {
            return;
        }
        self.terminate();
        self.lifetime.take();
        let mut child = self.child.take().expect("owned supervisor");
        let files = self.files.take();
        // A cancelled startup still owns an unreaped child. Keep its files
        // until reaping finishes, so a late supervisor write cannot recreate
        // the status file after cleanup.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = child.wait().await;
                drop(files);
            });
        }
    }
}

fn lease_path(directory: &Path) -> PathBuf {
    directory.join("worker-process.json")
}

#[derive(Debug, PartialEq, Eq)]
enum ProcessIdentity {
    Gone,
    ArgumentsUnavailable,
    Verified(String),
}

fn classify_identity(pid: i32, token: &str, output: &[u8]) -> WorkerResult<ProcessIdentity> {
    use sha2::{Digest, Sha256};
    let text = String::from_utf8_lossy(output);
    let mut fields = text.split_whitespace().skip(5); // lstart has five fields.
    if fields.next().and_then(|value| value.parse::<i32>().ok()) != Some(pid) {
        return Err(failure(
            "Saved worker process ownership is uncertain; no process was signalled",
        ));
    }
    let command = fields.collect::<Vec<_>>().join(" ");
    if command == "(sh)" {
        // macOS can report the process name before argv becomes readable.
        return Ok(ProcessIdentity::ArgumentsUnavailable);
    }
    if !command.contains(&format!("workflow-worker-{token}")) {
        return Err(failure(
            "Saved worker process ownership is uncertain; no process was signalled",
        ));
    }
    Ok(ProcessIdentity::Verified(format!(
        "{:x}",
        Sha256::digest(output)
    )))
}

async fn inspect_process_identity(pid: i32, token: &str) -> WorkerResult<ProcessIdentity> {
    use nix::{errno::Errno, sys::signal::kill, unistd::getpgid};
    match kill(Pid::from_raw(pid), None) {
        Err(Errno::ESRCH) => return Ok(ProcessIdentity::Gone),
        Err(error) => {
            return Err(failure(format!(
                "Cannot inspect saved worker process: {error}"
            )));
        }
        Ok(()) => {}
    }
    let observed = Command::new("/bin/ps")
        .args([
            "-ww",
            "-p",
            &pid.to_string(),
            "-o",
            "lstart=",
            "-o",
            "pgid=",
            "-o",
            "command=",
        ])
        .kill_on_drop(true)
        .output()
        .await
        .map_err(failure)?;
    if !observed.status.success() {
        if kill(Pid::from_raw(pid), None) == Err(Errno::ESRCH) {
            return Ok(ProcessIdentity::Gone);
        }
        return Err(failure("Cannot verify the saved worker process identity"));
    }
    match getpgid(Some(Pid::from_raw(pid))) {
        Ok(group) if group == Pid::from_raw(pid) => {}
        Err(Errno::ESRCH) => return Ok(ProcessIdentity::Gone),
        _ => {
            return Err(failure(
                "Saved worker process ownership is uncertain; no process was signalled",
            ));
        }
    }
    classify_identity(pid, token, &observed.stdout)
}

async fn process_identity(pid: i32, token: &str) -> WorkerResult<Option<String>> {
    match inspect_process_identity(pid, token).await? {
        ProcessIdentity::Gone => Ok(None),
        ProcessIdentity::Verified(identity) => Ok(Some(identity)),
        ProcessIdentity::ArgumentsUnavailable => Err(failure(
            "Saved worker process ownership is uncertain; no process was signalled",
        )),
    }
}

async fn startup_identity<F, Fut>(mut inspect: F, timeout: Duration) -> WorkerResult<String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = WorkerResult<ProcessIdentity>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let identity = tokio::time::timeout_at(deadline, inspect())
            .await
            .map_err(|_| failure("Worker supervisor arguments were unavailable at startup"))??;
        match identity {
            ProcessIdentity::Verified(identity) => return Ok(identity),
            ProcessIdentity::Gone => {
                return Err(failure(
                    "Worker supervisor exited before its lease was saved",
                ));
            }
            ProcessIdentity::ArgumentsUnavailable => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(failure(
                        "Worker supervisor arguments were unavailable at startup",
                    ));
                }
                tokio::time::sleep_until(
                    deadline.min(tokio::time::Instant::now() + Duration::from_millis(10)),
                )
                .await;
            }
        }
    }
}

async fn wait_group_gone(pid: i32) -> WorkerResult<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        match killpg(Pid::from_raw(pid), None) {
            Err(nix::errno::Errno::ESRCH) => return Ok(()),
            Err(error) => {
                return Err(failure(format!(
                    "Cannot verify worker group cleanup: {error}"
                )));
            }
            Ok(()) if tokio::time::Instant::now() >= deadline => {
                return Err(failure(
                    "Previous worker process group has not exited; refusing to launch a replacement",
                ));
            }
            Ok(()) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
}

/// Called before giving any replacement worker inputs. Never signal a saved PID
/// until its process group, random marker and complete start/command fingerprint
/// have been verified. The supervisor's lifetime pipe normally cleans it first.
pub async fn recover_process(directory: &Path) -> WorkerResult<()> {
    let path = lease_path(directory);
    if !path.exists() {
        return Ok(());
    }
    let lease: ProcessLease = crate::persistence::read_json(&path).map_err(failure)?;
    uuid::Uuid::parse_str(&lease.token).map_err(failure)?;
    if lease.pid <= 1 {
        return Err(failure("Invalid saved worker process identity"));
    }
    if let Some(identity) = process_identity(lease.pid, &lease.token).await? {
        if identity != lease.identity {
            return Err(failure(
                "Saved worker process identity changed; no process was signalled",
            ));
        }
        match killpg(Pid::from_raw(lease.pid), Signal::SIGKILL) {
            Ok(()) | Err(nix::errno::Errno::ESRCH) => {}
            Err(error) => return Err(failure(error)),
        }
    }
    wait_group_gone(lease.pid).await?;
    for suffix in ["input", "status"] {
        match tokio::fs::remove_file(directory.join(format!("{}.{}", lease.token, suffix))).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(failure(error)),
        }
    }
    tokio::fs::remove_file(path).await.map_err(failure)?;
    Ok(())
}

async fn spawn_supervised(
    argv: &[String],
    cwd: &Path,
    directory: &Path,
    input: &[u8],
) -> WorkerResult<SupervisedProcess> {
    spawn_supervised_using(
        argv,
        cwd,
        directory,
        input,
        |pid, token| async move { inspect_process_identity(pid, &token).await },
        Duration::from_secs(1),
    )
    .await
}

async fn spawn_supervised_using<F, Fut>(
    argv: &[String],
    cwd: &Path,
    directory: &Path,
    input: &[u8],
    mut inspect: F,
    timeout: Duration,
) -> WorkerResult<SupervisedProcess>
where
    F: FnMut(i32, String) -> Fut,
    Fut: std::future::Future<Output = WorkerResult<ProcessIdentity>>,
{
    if argv.is_empty() {
        return Err(failure("Worker argv is empty"));
    }
    let token = uuid::Uuid::new_v4().to_string();
    let files = ProcessFiles {
        directory: directory.to_owned(),
        token: token.clone(),
        leased: false,
    };
    // Keep creation synchronous with guard ownership: cancelling tokio's
    // blocking file write could otherwise recreate a file after guard cleanup.
    std::fs::write(files.path("input"), input).map_err(failure)?;
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg(SUPERVISOR)
        .arg(format!("workflow-worker-{token}"))
        .arg(&token)
        .arg(files.path("input"))
        .arg(files.path("status"))
        .args(argv)
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command.as_std_mut().process_group(0);
    let mut child = command.spawn().map_err(failure)?;
    let pid = child
        .id()
        .ok_or_else(|| failure("Worker supervisor has no identity"))? as i32;
    let lifetime = child.stdin.take().expect("piped supervisor lifetime");
    let mut process = SupervisedProcess {
        child: Some(child),
        group: ProcessGroup(Some(Pid::from_raw(pid))),
        lifetime: Some(lifetime),
        token: token.clone(),
        files: Some(files),
    };
    let leased = async {
        let identity = startup_identity(|| inspect(pid, token.clone()), timeout).await?;
        write_json(
            &lease_path(directory),
            &ProcessLease {
                pid,
                token,
                identity,
            },
        )
        .map_err(failure)?;
        process.files.as_mut().expect("owned process files").leased = true;
        Ok::<_, ExecutionFailure>(())
    }
    .await;
    if let Err(error) = leased {
        process.terminate();
        let _ = process.wait().await;
        process.files.take();
        return Err(error);
    }
    Ok(process)
}

async fn read_bounded(reader: impl AsyncRead + Unpin) -> WorkerResult<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .take(OUTPUT_LIMIT as u64 + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(failure)?;
    if bytes.len() > OUTPUT_LIMIT {
        return Err(failure("Worker output exceeded 1 MiB"));
    }
    Ok(bytes)
}

async fn process(
    context: &ExecutionContext,
    request: ProcessInput<'_>,
) -> WorkerResult<ProcessOutput> {
    let ProcessInput {
        invocation,
        receipt,
        workspace_receipt,
        node,
        cwd,
        directory,
        input,
    } = request;
    let last_message = directory.join(format!("{}.last-message", invocation.id()));
    let native_agent = node.kind == NodeKind::Agent && node.config.get("argv").is_none();
    let argv: Vec<String> = if native_agent {
        native_agent_argv(node, &last_message, workspace_receipt.is_some())
    } else {
        node.config
            .get("argv")
            .and_then(|value| value.as_array())
            .ok_or_else(|| failure("Worker argv is missing"))?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| failure("Worker argv must contain strings"))
            })
            .collect::<WorkerResult<_>>()?
    };
    let timeout = Duration::from_secs(
        node.config
            .get("timeout_secs")
            .and_then(|value| value.as_u64())
            .unwrap_or(300),
    );
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| failure("Worker timeout is too large"))?;
    let mut supervised = spawn_supervised(&argv, cwd, directory, &input).await?;
    let stdout = supervised.child().stdout.take().expect("piped stdout");
    let stderr = supervised.child().stderr.take().expect("piped stderr");
    let mut stop = context.stop();
    let completed = {
        let running = async {
            supervised
                .lifetime
                .as_mut()
                .expect("owned lifetime pipe")
                .write_all(format!("{}\n", supervised.token).as_bytes())
                .await
                .map_err(failure)?;
            invocation.mark_sent(receipt).await.map_err(failure)?;
            if let Some(receipt) = workspace_receipt {
                invocation.mark_sent(receipt).await.map_err(failure)?;
            }
            let wait = async { supervised.wait().await.map_err(failure) };
            let (stdout, stderr, _) =
                tokio::try_join!(read_bounded(stdout), read_bounded(stderr), wait)?;
            Ok::<_, ExecutionFailure>((stdout, stderr))
        };
        tokio::select! {
            result = running => result,
            () = stop.requested() => Err(ExecutionFailure::new("interrupted", "Worker stopped")),
            () = tokio::time::sleep_until(deadline) => Err(failure("Worker exceeded its timeout")),
        }
    };
    supervised.terminate();
    supervised.wait().await.map_err(failure)?;
    // Read the exit record before dropping this process's temporary files.
    let exit_record =
        tokio::fs::read_to_string(directory.join(format!("{}.status", supervised.token))).await;
    supervised.files.take();
    let (stdout, stderr) = completed?;
    let stderr = String::from_utf8_lossy(&stderr).into_owned();
    let status =
        exit_record.map_err(|_| failure("Worker supervisor exited without a completed command"))?;
    let code = status
        .strip_prefix(&format!("{} ", supervised.token))
        .and_then(|value| value.trim().parse::<u8>().ok())
        .ok_or_else(|| failure("Invalid worker completion record"))?;
    if code != 0 {
        return Err(failure(format!(
            "Worker exited with status {code}: {stderr}"
        )));
    }
    let stdout = if native_agent {
        let file = tokio::fs::File::open(&last_message)
            .await
            .map_err(failure)?;
        let text = String::from_utf8_lossy(&read_bounded(file).await?).into_owned();
        tokio::fs::remove_file(last_message)
            .await
            .map_err(failure)?;
        text
    } else {
        String::from_utf8_lossy(&stdout).into_owned()
    };
    Ok(ProcessOutput { stdout, stderr })
}

fn native_agent_argv(
    node: &DocumentNode,
    last_message: &Path,
    private_workspace: bool,
) -> Vec<String> {
    let mut args = vec![
        "codex".into(),
        "exec".into(),
        "-".into(),
        "--output-last-message".into(),
        last_message.to_string_lossy().into_owned(),
        "--color".into(),
        "never".into(),
        "--skip-git-repo-check".into(),
    ];
    if private_workspace {
        args.extend(["--sandbox".into(), "workspace-write".into()]);
    }
    if let Some(model) = node.config.get("model").and_then(|value| value.as_str()) {
        args.extend(["--model".into(), model.into()]);
    }
    args
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::document::{Document, IdentityMap, expand};
    use ontography::{ExecutionHost, ProposalRuntime};

    async fn permit(process: &mut SupervisedProcess) {
        process
            .lifetime
            .as_mut()
            .unwrap()
            .write_all(format!("{}\n", process.token).as_bytes())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn startup_retries_only_unavailable_arguments_with_a_deadline() {
        let pid = 123;
        let token = "fixture-token";
        let bare = format!("Sat Sep 26 12:00:00 2026 {pid} (sh)\n");
        let full =
            format!("Sat Sep 26 12:00:00 2026 {pid} /bin/sh -c script workflow-worker-{token}\n");
        let mut attempts = 0;
        let identity = startup_identity(
            || {
                attempts += 1;
                std::future::ready(classify_identity(
                    pid,
                    token,
                    if attempts < 3 {
                        bare.as_bytes()
                    } else {
                        full.as_bytes()
                    },
                ))
            },
            Duration::from_millis(100),
        )
        .await
        .unwrap();
        assert_eq!(attempts, 3);
        assert_eq!(
            classify_identity(pid, token, full.as_bytes()).unwrap(),
            ProcessIdentity::Verified(identity),
        );
        // A readable but different command or a different group is not the
        // macOS metadata race, even if an expected value might appear later.
        for output in [
            format!("Sat Sep 26 12:00:00 2026 {pid} /bin/sh unrelated\n"),
            "Sat Sep 26 12:00:00 2026 456 (sh)\n".into(),
        ] {
            let mut attempts = 0;
            let result = startup_identity(
                || {
                    attempts += 1;
                    std::future::ready(classify_identity(pid, token, output.as_bytes()))
                },
                Duration::from_millis(100),
            )
            .await;
            assert!(
                result
                    .unwrap_err()
                    .message()
                    .contains("ownership is uncertain")
            );
            assert_eq!(attempts, 1);
        }
        let result = startup_identity(
            || std::future::ready(classify_identity(pid, token, bare.as_bytes())),
            Duration::from_millis(20),
        )
        .await;
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("unavailable at startup")
        );
    }

    #[tokio::test]
    async fn rejected_timed_out_and_cancelled_startups_reap_and_remove_temporary_files() {
        use nix::{errno::Errno, sys::signal::kill};
        for timeout in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut pid = 0;
            let result = spawn_supervised_using(
                &["/bin/sh".into(), "-c".into(), "touch ran".into()],
                directory.path(),
                directory.path(),
                b"input",
                |observed, _| {
                    pid = observed;
                    std::future::ready(if timeout {
                        Ok(ProcessIdentity::ArgumentsUnavailable)
                    } else {
                        Err(failure("ownership mismatch"))
                    })
                },
                Duration::from_millis(20),
            )
            .await;
            assert!(result.is_err());
            assert_eq!(kill(Pid::from_raw(pid), None), Err(Errno::ESRCH));
            assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        }
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().to_owned();
        let (pid_sender, pid_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut sender = Some(pid_sender);
            spawn_supervised_using(
                &["/bin/sh".into(), "-c".into(), "touch ran".into()],
                &path,
                &path,
                b"input",
                |pid, _| {
                    sender.take().unwrap().send(pid).unwrap();
                    std::future::pending::<WorkerResult<ProcessIdentity>>()
                },
                Duration::from_secs(30),
            )
            .await
        });
        let pid = pid_receiver.await.unwrap();
        task.abort();
        assert!(task.await.err().unwrap().is_cancelled());
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if kill(Pid::from_raw(pid), None) == Err(Errno::ESRCH)
                    && std::fs::read_dir(directory.path()).unwrap().count() == 0
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        // Even an OS spawn error occurs after the input file was created.
        assert!(
            spawn_supervised(
                &["/bin/true".into()],
                &directory.path().join("missing-working-directory"),
                directory.path(),
                b"input",
            )
            .await
            .is_err()
        );
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn output_error_after_reaping_cannot_signal_a_reused_group() {
        let directory = tempfile::tempdir().unwrap();
        let mut process = spawn_supervised(
            &["/bin/true".into()],
            directory.path(),
            directory.path(),
            b"",
        )
        .await
        .unwrap();
        permit(&mut process).await;
        let (reaped, completion) = tokio::sync::oneshot::channel();
        let result = tokio::try_join!(
            async {
                process.wait().await.map_err(failure)?;
                reaped.send(()).unwrap();
                Ok(())
            },
            async {
                completion.await.unwrap();
                Err::<(), _>(failure("pipe failed after child exit"))
            },
        );
        assert!(result.unwrap_err().message().contains("pipe failed"));
        process
            .group
            .terminate_with(|_| panic!("A reaped child's group ID must never be signalled"));
        assert!(process.group.0.is_none());
    }

    #[tokio::test]
    async fn native_agent_arguments_support_private_non_git_workspaces() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let fake = directory.path().join("fake-codex");
        std::fs::write(
            &fake,
            r#"#!/bin/sh
[ "$1" = exec ] || exit 91
shift
while [ "$#" -gt 0 ]; do
    case "$1" in
        -) ;;
        --skip-git-repo-check) skip=1 ;;
        --output-last-message) output=$2; shift ;;
        --sandbox) sandbox=$2; shift ;;
        --color|--model) shift ;;
        *) exit 92 ;;
    esac
    shift
done
[ "$skip" = 1 ] && [ "$sandbox" = workspace-write ] || exit 93
cat > "$output"
printf changed > artifact.txt
"#,
        )
        .unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o700)).unwrap();
        let node: DocumentNode = serde_json::from_value(
            json!({"id":"agent","kind":"agent","config":{"prompt":"work","model":"fixture-model"}}),
        )
        .unwrap();
        let last = directory.path().join("last-message");
        let mut argv = native_agent_argv(&node, &last, true);
        argv[0] = fake.to_string_lossy().into_owned();
        let mut process = spawn_supervised(
            &argv,
            directory.path(),
            directory.path(),
            b"prompt\n\ninput",
        )
        .await
        .unwrap();
        permit(&mut process).await;
        tokio::time::timeout(Duration::from_secs(3), process.wait())
            .await
            .unwrap()
            .unwrap();
        let status =
            std::fs::read_to_string(directory.path().join(format!("{}.status", process.token)))
                .unwrap();
        assert_eq!(status, format!("{} 0\n", process.token));
        assert_eq!(std::fs::read_to_string(last).unwrap(), "prompt\n\ninput");
        assert_eq!(
            std::fs::read_to_string(directory.path().join("artifact.txt")).unwrap(),
            "changed"
        );
        recover_process(directory.path()).await.unwrap();
    }

    #[tokio::test]
    async fn lifetime_pipe_terminates_orphans_before_and_after_start_permission() {
        for permitted in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut process = spawn_supervised(
                &[
                    "/bin/sh".into(),
                    "-c".into(),
                    "echo started > started; sleep 30".into(),
                ],
                directory.path(),
                directory.path(),
                b"",
            )
            .await
            .unwrap();
            if permitted {
                permit(&mut process).await;
                tokio::time::timeout(Duration::from_secs(3), async {
                    while !directory.path().join("started").exists() {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap();
            }
            // Model abrupt parent loss: no Rust Drop signal is available;
            // the OS closes only the parent's lifetime-pipe write end.
            let mut child = process.child.take().unwrap();
            let lifetime = process.lifetime.take().unwrap();
            process.group.disarm();
            std::mem::forget(process.files.take());
            drop(lifetime);
            tokio::time::timeout(Duration::from_secs(3), child.wait())
                .await
                .unwrap()
                .unwrap();
            recover_process(directory.path()).await.unwrap();
            assert_eq!(directory.path().join("started").exists(), permitted);
            assert!(!lease_path(directory.path()).exists());
        }
    }

    #[tokio::test]
    async fn orphan_recovery_verifies_identity_before_signalling() {
        let directory = tempfile::tempdir().unwrap();
        let mut process = spawn_supervised(
            &["/bin/sleep".into(), "30".into()],
            directory.path(),
            directory.path(),
            b"",
        )
        .await
        .unwrap();
        permit(&mut process).await;
        let mut lease: ProcessLease =
            crate::persistence::read_json(&lease_path(directory.path())).unwrap();
        let original = lease.identity.clone();
        lease.identity = "different process".into();
        write_json(&lease_path(directory.path()), &lease).unwrap();
        assert!(
            recover_process(directory.path())
                .await
                .unwrap_err()
                .message()
                .contains("identity changed")
        );
        assert!(
            process.child().try_wait().unwrap().is_none(),
            "mismatch must not signal the process"
        );
        lease.identity = original;
        write_json(&lease_path(directory.path()), &lease).unwrap();
        let (recovered, exited) = tokio::join!(recover_process(directory.path()), process.wait());
        recovered.unwrap();
        exited.unwrap();
        assert!(!lease_path(directory.path()).exists());
    }

    #[tokio::test]
    async fn initial_inbox_accepts_input_once_without_a_process() {
        let document = Document::parse(
            r#"{"name":"sink","entry":"inbox","nodes":[{"id":"inbox","kind":"inbox"}]}"#,
        )
        .unwrap();
        let ids = IdentityMap::fresh(&document);
        let compiled = expand(&document, "sink", &ids).unwrap().compile().unwrap();
        let runtime = ProposalRuntime::with_grammar(compiled.kernel, compiled.grammar);
        let session = runtime.open().unwrap();
        let host = ExecutionHost::new(session.clone());
        let temporary = tempfile::tempdir().unwrap();
        let directory = temporary.path().join("worker");
        let marker = directory.join("initial-complete.json");
        let (sender, settings) = watch::channel(document.nodes[0].clone());
        let input = WorkflowPayload::Message {
            message: "stored".into(),
        }
        .encode()
        .unwrap();
        let project = temporary.path().to_path_buf();
        let worker = host
            .launch(ids.nodes["inbox"].clone(), move |context| {
                run(
                    context,
                    settings.clone(),
                    project.clone(),
                    directory.clone(),
                    Some(input.clone()),
                )
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !marker.exists() {
                assert!(!worker.status().is_terminal(), "{:?}", worker.status());
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(session.snapshot().await.state().activations().len(), 1);
        let output: serde_json::Value =
            crate::persistence::read_json(&temporary.path().join("worker/output.json")).unwrap();
        assert_eq!(output["result"], json!({"message":"stored"}));
        worker.request_stop();
        worker.wait().await;
        drop(sender);
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn command_publishes_message_and_reloads_settings_for_next_task() {
        let document = Document::parse(r#"{"name":"worker","entry":"command","nodes":[{"id":"command","kind":"command","config":{"argv":["/bin/cat"]}},{"id":"output","kind":"inbox"}],"edges":[{"from":"command","to":"output"},{"from":"output","to":"command"}]}"#).unwrap();
        let ids = IdentityMap::fresh(&document);
        let compiled = expand(&document, "worker", &ids)
            .unwrap()
            .compile()
            .unwrap();
        let runtime = ProposalRuntime::with_grammar(compiled.kernel, compiled.grammar);
        let session = runtime.open().unwrap();
        let host = ExecutionHost::new(session.clone());
        let temporary = tempfile::tempdir().unwrap();
        let project = temporary.path().to_path_buf();
        let directory = project.join("worker");
        let node = document
            .nodes
            .iter()
            .find(|node| node.id == "command")
            .unwrap()
            .clone();
        let (settings, receiver) = watch::channel(node.clone());
        let first = WorkflowPayload::Message {
            message: "first".into(),
        }
        .encode()
        .unwrap();
        let directory_for_worker = directory.clone();
        let worker = host
            .launch(
                ids.nodes["command"].clone(),
                move |context: ExecutionContext| {
                    let receiver = receiver.clone();
                    let project = project.clone();
                    let directory = directory_for_worker.clone();
                    let first = first.clone();
                    async move { run(context, receiver, project, directory, Some(first)).await }
                },
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if directory.join("initial-complete.json").exists() {
                    break;
                }
                assert!(!worker.status().is_terminal(), "{:?}", worker.status());
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let pending = session
            .next_trigger_at(ids.nodes["output"].clone())
            .await
            .unwrap();
        assert_eq!(pending.packages().len(), 1);
        let bytes = session
            .content(pending.packages()[0].1.content_digest())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            WorkflowPayload::decode(&bytes).unwrap(),
            WorkflowPayload::Message {
                message: "first".into()
            }
        );
        // A setting change does not replace the graph node or replay initial input.
        let mut changed = node;
        changed.config = json!({"argv":["/usr/bin/printf","second"]});
        settings.send(changed).unwrap();
        let input = WorkflowPayload::Message {
            message: "next".into(),
        }
        .encode()
        .unwrap();
        let trigger = session
            .begin_invocation(
                ids.nodes["output"].clone(),
                InvocationTrigger::Packages(vec![pending.packages()[0].0]),
                ContextPolicy::default(),
            )
            .await
            .unwrap();
        let edge = &ids.edges[&crate::workflow::document::edge_key("output", "command")];
        assert!(matches!(
            trigger
                .submit(
                    input.clone(),
                    vec![Emission::new(edge.as_str(), OutputAuthority::Carry, input)],
                    vec![]
                )
                .await
                .unwrap(),
            ProposalDecision::Committed(_)
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let pending = session
                    .next_trigger_at(ids.nodes["output"].clone())
                    .await
                    .unwrap();
                if let Some((_, package)) = pending.packages().first() {
                    let bytes = session
                        .content(package.content_digest())
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(
                        WorkflowPayload::decode(&bytes).unwrap(),
                        WorkflowPayload::Message {
                            message: "second".into()
                        }
                    );
                    break;
                }
                assert!(!worker.status().is_terminal(), "{:?}", worker.status());
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        worker.request_stop();
        worker.wait().await;
        assert_eq!(
            session
                .next_trigger_at(ids.nodes["output"].clone())
                .await
                .unwrap()
                .packages()
                .len(),
            1
        );
        runtime.shutdown().await;
    }

    async fn command_fixture(
        config: serde_json::Value,
    ) -> (
        tempfile::TempDir,
        ProposalRuntime,
        ontography::SessionHandle,
        ExecutionHost,
        ontography::ExecutionHandle,
        IdentityMap,
    ) {
        let document: Document = serde_json::from_value(json!({"name":"failure","entry":"source",
            "nodes":[{"id":"source","kind":"inbox"},{"id":"command","kind":"command","config":config}],
            "edges":[{"from":"source","to":"command"}]})).unwrap();
        let ids = IdentityMap::fresh(&document);
        let compiled = expand(&document, "failure", &ids)
            .unwrap()
            .compile()
            .unwrap();
        let runtime = ProposalRuntime::with_grammar(compiled.kernel, compiled.grammar);
        let session = runtime.open().unwrap();
        let host = ExecutionHost::new(session.clone());
        let temporary = tempfile::tempdir().unwrap();
        let project = temporary.path().to_path_buf();
        let node = document
            .nodes
            .iter()
            .find(|node| node.id == "command")
            .unwrap()
            .clone();
        let (_, settings) = watch::channel(node);
        let input = WorkflowPayload::Message {
            message: "input".into(),
        }
        .encode()
        .unwrap();
        let kernel = session.kernel().await.unwrap();
        let invocation = session
            .begin_invocation(
                ids.nodes["source"].clone(),
                InvocationTrigger::Root {
                    authority: kernel.root_ceiling(&ids.nodes["source"]).unwrap().clone(),
                    input: input.clone(),
                },
                ContextPolicy::default(),
            )
            .await
            .unwrap();
        let edge = &ids.edges[&crate::workflow::document::edge_key("source", "command")];
        assert!(matches!(
            invocation
                .submit(
                    input.clone(),
                    vec![Emission::new(edge.as_str(), OutputAuthority::Carry, input)],
                    vec![]
                )
                .await
                .unwrap(),
            ProposalDecision::Committed(_)
        ));
        let worker = host
            .launch(ids.nodes["command"].clone(), move |context| {
                run(
                    context,
                    settings.clone(),
                    project.clone(),
                    project.join("worker"),
                    None,
                )
            })
            .await
            .unwrap();
        (temporary, runtime, session, host, worker, ids)
    }

    #[tokio::test]
    async fn failed_timed_out_and_oversized_commands_preserve_pending_input() {
        for config in [
            json!({"argv":["/bin/sh","-c","exit 7"]}),
            json!({"argv":["/bin/sleep","30"],"timeout_secs":1}),
            json!({"argv":["/usr/bin/yes"]}),
        ] {
            let (_temporary, runtime, session, _host, worker, ids) = command_fixture(config).await;
            let status = tokio::time::timeout(Duration::from_secs(5), worker.wait())
                .await
                .unwrap();
            assert!(
                matches!(status, ontography::ExecutionStatus::Failed(_)),
                "{status:?}"
            );
            assert_eq!(
                session
                    .next_trigger_at(ids.nodes["command"].clone())
                    .await
                    .unwrap()
                    .packages()
                    .len(),
                1
            );
            let invocations = session
                .invocations_page(Some(&ids.nodes["command"]), None, 10)
                .await
                .unwrap();
            assert_eq!(invocations[0].status, ontography::InvocationStatus::Failed);
            runtime.shutdown().await;
        }
    }

    #[tokio::test]
    async fn stop_terminates_command_group_and_preserves_input() {
        let (temporary, runtime, session, _host, worker, ids) = command_fixture(
            json!({"argv":["/bin/sh","-c","sleep 30 & echo $! > child.pid; wait"]}),
        )
        .await;
        let pid_file = temporary.path().join("child.pid");
        let child: i32 = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(pid) = std::fs::read_to_string(&pid_file)
                    .ok()
                    .and_then(|text| text.trim().parse().ok())
                {
                    break pid;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        worker.request_stop();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), worker.wait())
                .await
                .unwrap(),
            ontography::ExecutionStatus::Exited
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            while nix::sys::signal::kill(Pid::from_raw(child), None).is_ok() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            session
                .next_trigger_at(ids.nodes["command"].clone())
                .await
                .unwrap()
                .packages()
                .len(),
            1
        );
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn workspace_command_uses_private_checkout_and_publishes_captured_package() {
        let document = Document::parse(r#"{"name":"workspace","entry":"command","nodes":[{"id":"command","kind":"command","config":{"argv":["/bin/sh","-c","printf changed > file.txt"]}},{"id":"output","kind":"inbox"}],"edges":[{"from":"command","to":"output"}]}"#).unwrap();
        let ids = IdentityMap::fresh(&document);
        let compiled = expand(&document, "workspace", &ids)
            .unwrap()
            .compile()
            .unwrap();
        let runtime = ProposalRuntime::with_grammar(compiled.kernel, compiled.grammar);
        let session = runtime.open().unwrap();
        let host = ExecutionHost::new(session.clone());
        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("source");
        std::fs::create_dir(&source).unwrap();
        std::fs::write(source.join("file.txt"), "original").unwrap();
        let content = session.content_store().await.unwrap();
        let store = WorkspaceStore::new(content.clone(), temporary.path().join("import"));
        let base = store.import_directory(&source).await.unwrap();
        let initial = WorkflowPayload::Workspace(PackageEnvelope::new(base.root()))
            .encode()
            .unwrap();
        let node = document
            .nodes
            .iter()
            .find(|node| node.id == "command")
            .unwrap()
            .clone();
        let (sender, settings) = watch::channel(node);
        let directory = temporary.path().join("worker");
        let complete = directory.join("initial-complete.json");
        let worker = host
            .launch(ids.nodes["command"].clone(), move |context| {
                run(
                    context,
                    settings.clone(),
                    source.clone(),
                    directory.clone(),
                    Some(initial.clone()),
                )
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !complete.exists() {
                assert!(!worker.status().is_terminal(), "{:?}", worker.status());
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let pending = session
            .next_trigger_at(ids.nodes["output"].clone())
            .await
            .unwrap();
        let payload = session
            .content(pending.packages()[0].1.content_digest())
            .await
            .unwrap()
            .unwrap();
        let WorkflowPayload::Workspace(envelope) = WorkflowPayload::decode(&payload).unwrap()
        else {
            panic!("expected workspace");
        };
        let captured = PackageStore::new(content.clone())
            .resolve(envelope.ontography_package)
            .await
            .unwrap();
        let file = captured
            .entries()
            .iter()
            .find(|entry| entry.path == "file.txt")
            .unwrap();
        let ontography::ResolvedEntryKind::File { content: id, .. } = file.kind else {
            panic!("expected file");
        };
        assert_eq!(
            content.read_range(id, 0..id.size()).await.unwrap().as_ref(),
            b"changed"
        );
        assert_eq!(
            std::fs::read_to_string(temporary.path().join("source/file.txt")).unwrap(),
            "original"
        );
        worker.request_stop();
        worker.wait().await;
        drop(sender);
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn workspace_without_output_route_fails_before_running_a_command() {
        let document = Document::parse(r#"{"name":"terminal-workspace","entry":"command","nodes":[{"id":"command","kind":"command","config":{"argv":["/usr/bin/touch","should-not-run"]}}]}"#).unwrap();
        let ids = IdentityMap::fresh(&document);
        let compiled = expand(&document, "terminal-workspace", &ids)
            .unwrap()
            .compile()
            .unwrap();
        let runtime = ProposalRuntime::with_grammar(compiled.kernel, compiled.grammar);
        let session = runtime.open().unwrap();
        let host = ExecutionHost::new(session.clone());
        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("source");
        std::fs::create_dir(&source).unwrap();
        let store = WorkspaceStore::new(
            session.content_store().await.unwrap(),
            temporary.path().join("import"),
        );
        let base = store.import_directory(&source).await.unwrap();
        let initial = WorkflowPayload::Workspace(PackageEnvelope::new(base.root()))
            .encode()
            .unwrap();
        let (sender, settings) = watch::channel(document.nodes[0].clone());
        let directory = temporary.path().join("worker");
        let worker = host
            .launch(ids.nodes["command"].clone(), move |context| {
                run(
                    context,
                    settings.clone(),
                    source.clone(),
                    directory.clone(),
                    Some(initial.clone()),
                )
            })
            .await
            .unwrap();
        let status = tokio::time::timeout(Duration::from_secs(3), worker.wait())
            .await
            .unwrap();
        let ontography::ExecutionStatus::Failed(error) = status else {
            panic!("{status:?}");
        };
        assert!(error.message().contains("outgoing connection"));
        assert!(!temporary.path().join("source/should-not-run").exists());
        assert!(
            session
                .invocations_page(Some(&ids.nodes["command"]), None, 10)
                .await
                .unwrap()
                .is_empty()
        );
        drop(sender);
        runtime.shutdown().await;
    }
}
