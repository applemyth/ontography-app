//! Command workers consume scoped inputs and publish through core admission.

use super::{
    BoundNode, Implementation, WorkflowPayload,
    components::CommandConfig,
    document::DocumentNode,
    tasks::{self, RetryLedger, Task},
};
use crate::environment::Environment;
use crate::persistence::write_json;
use crate::process::{Stdin, recover_process, spawn_supervised};
use crate::workspace::{AttemptCheckout, WorkspaceStore};
use ontography::{
    ContentId, ContextError, ContextPolicy, Emission, ExecutionContext, ExecutionFailure,
    ExecutionSignal, InvocationHandle, InvocationTrigger, OutputAuthority, PackageDocument,
    PackageEnvelope, PackageError, PackageStore, Payload, ProposalDecision, ResolvedEntryKind,
    SessionHandle,
};
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::watch,
    time::Instant,
};

const OUTPUT_LIMIT: usize = 1024 * 1024;
type WorkerResult<T> = std::result::Result<T, ExecutionFailure>;

fn failure(error: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::new("workflow_worker", error.to_string())
}

/// Where a task worker runs: the project its commands work in by default, its
/// node's own directory, and the environment its commands start with.
#[derive(Clone)]
pub struct Place {
    pub project: PathBuf,
    pub directory: PathBuf,
    pub environment: Environment,
}

/// Settings are sampled before each task; an in-flight task keeps its settings.
/// Human and inbox work remains pending for document-level interaction tools.
/// A failed task is counted in the node's retry ledger and waits out its
/// backoff, or is parked, while the worker continues with other tasks.
pub async fn run(
    mut context: ExecutionContext,
    mut settings: watch::Receiver<BoundNode>,
    place: Place,
    mut initial: Option<Payload>,
    session: SessionHandle,
    ledger: Arc<RetryLedger>,
) -> WorkerResult<()> {
    let directory = &place.directory;
    tokio::fs::create_dir_all(directory)
        .await
        .map_err(failure)?;
    recover_process(directory).await?;
    let workspaces = WorkspaceStore::new(
        context.content_store().await.map_err(failure)?,
        directory.join("workspaces"),
    );
    // Checkouts left by a crash belong to attempts that will never finish.
    AttemptCheckout::remove_abandoned(&workspaces).await;
    let mut retries = ledger.subscribe();
    loop {
        if context.stop().is_requested() {
            return Ok(());
        }
        let node = settings.borrow_and_update().clone();
        if node.binding.implementation.is_session() {
            return Err(failure("Sessions run in the persistent node runtime"));
        }
        let definition = node.digest();
        // Failures count against the definition they happen under; renewing
        // here also applies a change that reconcile could not record.
        ledger.renew(&definition).map_err(failure)?;
        // Only retry decisions made after this point wake an idle worker.
        retries.mark_unchanged();
        if initial.is_some() && super::runtime::initial_complete(directory).map_err(failure)? {
            initial = None;
        }
        if matches!(node.binding.implementation, Implementation::Inbox(_))
            && let Some(input) = initial.take()
        {
            accept_initial_sink(&context, &node.node, directory, input).await?;
        }
        // Human and inbox work waits for the manager.
        if node.binding.implementation.command().is_none() {
            if !idle(&mut context, &mut settings, &mut retries, None).await {
                return Ok(());
            }
            continue;
        }
        let source = tasks::TaskSource {
            session: &session,
            node_id: context.node_id(),
            initial: initial.is_some(),
            ledger: &ledger,
            busy: &[],
        };
        // Read before choosing, so a backoff that ends meanwhile still wakes us.
        let wake = ledger.next_wake();
        let Some(task) = source.next().await.map_err(failure)? else {
            if !idle(&mut context, &mut settings, &mut retries, wake).await {
                return Ok(());
            }
            continue;
        };
        // Choosing a task can await: begin it only under the settings it was
        // chosen with, so the attempt runs under the definition renewed above.
        if settings.has_changed().unwrap_or(true) {
            continue;
        }
        // The manager may have discarded the initial input since it was offered.
        if task.is_initial() && super::runtime::initial_complete(directory).map_err(failure)? {
            initial = None;
            continue;
        }
        let policy = node.node.retry_policy();
        let started = match trigger(&context, &task, initial.as_ref()).await? {
            Ok((trigger, payloads)) => begin(&context, trigger, &payloads)
                .await?
                .map(|invocation| (invocation, payloads)),
            Err(error) => Err(error),
        };
        let (invocation, payloads) = match started {
            Ok(started) => started,
            Err(error) => {
                // The same task on the same graph would fail the same way,
                // unless an input was taken meanwhile and the task is gone.
                if source
                    .current(&task.ids())
                    .await
                    .map_err(failure)?
                    .is_some()
                {
                    ledger
                        .record_failure(&task, &error, false, &policy, &definition)
                        .map_err(failure)?;
                }
                continue;
            }
        };
        let outcome = perform(&context, &invocation, &node, &place, &workspaces, &payloads).await;
        match outcome {
            Ok(()) => {
                if task.is_initial() {
                    // Core's accepted invocation is authoritative if this
                    // disposable completion marker cannot be written.
                    let _ = super::runtime::complete_initial(directory);
                    initial = None;
                }
                // Accepted inputs are consumed, so a record left behind by a
                // failed write can never select this task again.
                let _ = ledger.clear(&task.key);
            }
            Err(error) if context.stop().is_requested() => {
                // A stop interrupts the attempt; it is not the task's failure.
                let _ = invocation.interrupt(error.message()).await;
                report_failure(directory, &invocation, &node.node, &error);
                return Ok(());
            }
            Err(error) => {
                // Count the attempt before its failure is visible, so that a
                // crash in between cannot grant an uncounted retry.
                let recorded =
                    ledger.record_failure(&task, error.message(), true, &policy, &definition);
                let _ = invocation.fail(error.message()).await;
                report_failure(directory, &invocation, &node.node, &error);
                recorded.map_err(failure)?;
            }
        }
    }
}

/// Waits until work may have become runnable: new settings, a frontier
/// change, a retry decision, or the end of a backoff. False means exit.
async fn idle(
    context: &mut ExecutionContext,
    settings: &mut watch::Receiver<BoundNode>,
    retries: &mut watch::Receiver<u64>,
    wake: Option<Instant>,
) -> bool {
    tokio::select! {
        changed = settings.changed() => changed.is_ok(),
        signal = context.next_signal() => matches!(signal, ExecutionSignal::FrontierChanged(_)),
        changed = retries.changed() => changed.is_ok(),
        () = tasks::wake_at(wake) => true,
    }
}

/// The node's latest result, as the manager's output view reads it.
fn report_failure(
    directory: &Path,
    invocation: &InvocationHandle,
    node: &DocumentNode,
    error: &ExecutionFailure,
) {
    let _ = write_json(
        &directory.join("output.json"),
        &json!({
            "invocation_id":invocation.id().to_string(), "node":node.id,
            "publication_status":"failed", "error":error.message(),
        }),
    );
}

/// The invocation trigger for a task and the payloads its worker receives.
/// The inner error is the task's own, as in `begin`.
async fn trigger(
    context: &ExecutionContext,
    task: &Task,
    initial: Option<&Payload>,
) -> WorkerResult<Result<(InvocationTrigger, Vec<Payload>), String>> {
    if task.is_initial()
        && let Some(input) = initial
    {
        let authority = context
            .kernel()
            .await
            .map_err(failure)?
            .root_ceiling(context.node_id())
            .cloned()
            .ok_or_else(|| failure("The initial worker is no longer the entry node"))?;
        return Ok(Ok((
            InvocationTrigger::Root {
                authority,
                input: input.clone(),
            },
            vec![input.clone()],
        )));
    }
    let mut payloads = Vec::with_capacity(task.inputs.len());
    for (_, record) in &task.inputs {
        match context
            .content(record.content_digest())
            .await
            .map_err(failure)?
        {
            Some(payload) => payloads.push(payload),
            None => return Ok(Err("Pending input content is unavailable".into())),
        }
    }
    Ok(Ok((InvocationTrigger::Packages(task.ids()), payloads)))
}

async fn accept_initial_sink(
    context: &ExecutionContext,
    node: &DocumentNode,
    directory: &Path,
    input: Payload,
) -> WorkerResult<()> {
    let result = WorkflowPayload::read(&input).map_err(failure)?;
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
            let _ = super::runtime::complete_initial(directory);
            Ok(())
        }
        ProposalDecision::Rejected(reject) => Err(failure(crate::views::rejection(&reject))),
    }
}

/// Begins a task's invocation. The inner error is the task's own: it cannot
/// run as delivered. The outer error stops the worker.
async fn begin(
    context: &ExecutionContext,
    trigger: InvocationTrigger,
    payloads: &[Payload],
) -> WorkerResult<Result<InvocationHandle, String>> {
    let mut workspaces = Vec::new();
    for payload in payloads {
        match WorkflowPayload::read(payload) {
            Ok(WorkflowPayload::Workspace(envelope)) => {
                workspaces.push(envelope.ontography_package);
            }
            Ok(_) => {}
            Err(error) => return Ok(Err(error.to_string())),
        }
    }
    if workspaces.len() > 1 {
        return Ok(Err(
            "A task may receive only one workspace; combine workspaces before this node".into(),
        ));
    }
    let mut contents = Vec::new();
    if let Some(workspace) = workspaces.first() {
        let kernel = context.kernel().await.map_err(failure)?;
        if !kernel
            .graph()
            .edges()
            .iter()
            .any(|edge| edge.source() == context.node_id())
        {
            return Ok(Err("A workspace worker needs an outgoing connection; connect it to an inbox to retain its result".into()));
        }
        let packages = PackageStore::new(context.content_store().await.map_err(failure)?);
        match packages.get(*workspace).await {
            Ok(PackageDocument::Collection { .. } | PackageDocument::Changes { .. }) => {}
            Ok(_) => return Ok(Err("A workspace must be a directory".into())),
            // Storage failing is the worker's problem, not the task's.
            Err(PackageError::Content(error)) => return Err(failure(error)),
            Err(error) => return Ok(Err(error.to_string())),
        }
        if matches!(trigger, InvocationTrigger::Root { .. }) {
            contents = packages
                .resolve(*workspace)
                .await
                .map_err(failure)?
                .dependencies();
        }
    }
    match context
        .begin_invocation_with_content(trigger, ContextPolicy::default(), contents)
        .await
    {
        Ok(invocation) => Ok(Ok(invocation)),
        // Core refused this task as delivered: too large, or no longer here.
        Err(
            error @ (ContextError::Denied(_) | ContextError::Budget(_) | ContextError::NotFound),
        ) => Ok(Err(error.to_string())),
        Err(error) => Err(failure(error)),
    }
}

async fn perform(
    context: &ExecutionContext,
    invocation: &InvocationHandle,
    node: &BoundNode,
    place: &Place,
    workspaces: &WorkspaceStore,
    payloads: &[Payload],
) -> WorkerResult<()> {
    let directory = place.directory.as_path();
    let command = node
        .binding
        .implementation
        .command()
        .ok_or_else(|| failure("Only command nodes run tasks"))?;
    // Preparation records the exact source exposure, including resolved package views.
    invocation.prepare_context().await.map_err(failure)?;
    let mut parts = Vec::new();
    let mut workspace = None;
    for payload in payloads {
        match WorkflowPayload::read(payload).map_err(failure)? {
            // `begin` admitted at most one.
            WorkflowPayload::Workspace(envelope) => workspace = Some(envelope.ontography_package),
            _ => parts.push(&payload[..]),
        }
    }
    let workspace = match workspace {
        Some(root) => Some(open_workspace(invocation, workspaces, root).await?),
        None => None,
    };
    let input = parts.join(&b"\n\n"[..]);
    let receipt = invocation
        .record_initial_input(input.clone().into())
        .await
        .map_err(failure)?;
    let cwd = workspace
        .as_ref()
        .map_or(place.project.as_path(), |(checkout, _)| checkout.path());
    let output = process(
        context,
        ProcessInput {
            invocation,
            receipt: receipt.sequence,
            workspace_receipt: workspace.as_ref().map(|(_, exposure)| *exposure),
            command,
            cwd,
            directory,
            environment: &place.environment,
            input,
        },
    )
    .await?;
    let (encoded, contents) = if let Some((checkout, _)) = &workspace {
        let capture = checkout.capture(workspaces).await.map_err(failure)?;
        let payload = PackageEnvelope::new(capture.package().root())
            .to_payload()
            .map_err(failure)?;
        let contents = capture.package().dependencies();
        invocation
            .record_tool_response("workspace_capture", payload.clone())
            .await
            .map_err(failure)?;
        capture.retain().await.map_err(failure)?;
        (payload, contents)
    } else {
        let payload: Payload = output.stdout.clone().into();
        // Stdout is worker-controlled; retention alone grants no publication rights.
        let contents = invocation
            .validate_worker_output(&payload)
            .await
            .map_err(failure)?;
        (payload, contents)
    };
    let result = WorkflowPayload::read(&encoded).map_err(failure)?;
    invocation
        .record_tool_response("worker_output", encoded.clone())
        .await
        .map_err(failure)?;
    let mut report = json!({"invocation_id":invocation.id().to_string(), "node":node.node.id,
        "result":result, "stdout":String::from_utf8_lossy(&output.stdout), "stderr":output.stderr, "publication_status":"prepared"});
    write_json(&directory.join("output.json"), &report).map_err(failure)?;
    // Fetch routes after execution; the admitted result always follows the current graph.
    let kernel = context.kernel().await.map_err(failure)?;
    let authority = match &command.authority {
        Some(tags) => OutputAuthority::Transition(crate::views::authority(tags).map_err(failure)?),
        None => OutputAuthority::Carry,
    };
    let emissions = kernel
        .graph()
        .edges()
        .iter()
        .filter(|edge| edge.source() == context.node_id())
        .map(|edge| Emission::new(edge.id(), authority.clone(), encoded.clone()))
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
            return Err(failure(format!(
                "Core rejected worker output: {}",
                crate::views::rejection(&error)
            )));
        }
    }
    if let Some((checkout, _)) = workspace
        && let Err(error) = checkout.remove().await
    {
        report["cleanup_error"] = json!(error.to_string());
    }
    // A cache failure after admission must never turn an accepted task into a
    // retry. Status readers can reconcile this cache with the invocation ID.
    let _ = write_json(&directory.join("output.json"), &report);
    Ok(())
}

/// Checks out a task's workspace as its worker's private, writable directory,
/// and records what the worker is given, returning the checkout and that
/// receipt. The workspace must be the root directory of an input core granted
/// the attempt.
async fn open_workspace(
    invocation: &InvocationHandle,
    store: &WorkspaceStore,
    root: ContentId,
) -> WorkerResult<(AttemptCheckout, u64)> {
    let Some(input) = invocation.views().iter().find(|view| {
        view.root == root
            && invocation
                .member(&view.owner)
                .is_some_and(|member| matches!(member.kind, ResolvedEntryKind::Directory))
    }) else {
        return Err(failure(
            "The workspace is not a directory the task received",
        ));
    };
    let view = store.open(root).await.map_err(failure)?;
    let checkout = AttemptCheckout::open(store, &view, &invocation.id().to_string(), true)
        .await
        .map_err(failure)?;
    // The command is given the checkout as its working directory, not these
    // bytes; the receipt is marked sent when the command starts in it.
    let mut exposure = checkout.exposure();
    exposure["handle"] = json!(input.owner);
    let receipt = invocation
        .record_tool_response(
            "workspace_exposure",
            serde_json::to_vec(&exposure).map_err(failure)?.into(),
        )
        .await
        .map_err(failure)?;
    Ok((checkout, receipt.sequence))
}

struct ProcessOutput {
    stdout: Vec<u8>,
    stderr: String,
}

struct ProcessInput<'a> {
    invocation: &'a InvocationHandle,
    receipt: u64,
    workspace_receipt: Option<u64>,
    command: &'a CommandConfig,
    cwd: &'a Path,
    directory: &'a Path,
    environment: &'a Environment,
    input: Vec<u8>,
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
        command,
        cwd,
        directory,
        environment,
        input,
    } = request;
    let argv = &command.argv;
    let timeout = Duration::from_secs(command.timeout_secs.unwrap_or(300));
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| failure("Worker timeout is too large"))?;
    let mut supervised = spawn_supervised(
        argv,
        cwd,
        directory,
        environment.vars(),
        Stdin::Bytes(&input),
    )
    .await?;
    let stdout = supervised.stdout.take().expect("piped stdout");
    let stderr = supervised.stderr.take().expect("piped stderr");
    let mut stop = context.stop();
    let completed = {
        let running = async {
            supervised.permit().await?;
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
    let exit = supervised.finish().await;
    let (stdout, stderr) = completed?;
    let stderr = String::from_utf8_lossy(&stderr).into_owned();
    let code = exit?;
    if code != 0 {
        return Err(failure(format!(
            "Worker exited with status {code}: {stderr}"
        )));
    }
    Ok(ProcessOutput { stdout, stderr })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::document::{Document, IdentityMap, expand_builtin as expand};
    use ontography::{
        ExecutionHandle, ExecutionHost, InvocationStatus, ProposalRuntime, ReceiptState,
    };

    /// Launches a worker as reconciliation does, with its retry ledger in its
    /// node directory.
    async fn launch(
        host: &ExecutionHost,
        node: &str,
        settings: watch::Receiver<BoundNode>,
        project: PathBuf,
        directory: PathBuf,
        initial: Option<Payload>,
    ) -> (ExecutionHandle, Arc<RetryLedger>) {
        let ledger = RetryLedger::open(directory.join("retry.json")).unwrap();
        let session = host.session().clone();
        let worker_ledger = ledger.clone();
        let worker = host
            .launch(node, move |context| {
                run(
                    context,
                    settings.clone(),
                    Place {
                        project: project.clone(),
                        directory: directory.clone(),
                        environment: Environment::current(),
                    },
                    initial.clone(),
                    session.clone(),
                    worker_ledger.clone(),
                )
            })
            .await
            .unwrap();
        (worker, ledger)
    }

    #[tokio::test]
    async fn initial_inbox_accepts_input_once_without_a_process() {
        let document = Document::parse(
            r#"{"name":"sink","entry":"inbox","nodes":[{"id":"inbox","component":"inbox"}]}"#,
        )
        .unwrap();
        let ids = IdentityMap::fresh(&document);
        let compiled = expand(&document, "sink", &ids).unwrap().compile().unwrap();
        let runtime = ProposalRuntime::with_policy(compiled, crate::workflow::edit::policy());
        let session = runtime.open().unwrap();
        let host = ExecutionHost::new(session.clone());
        let temporary = tempfile::tempdir().unwrap();
        let directory = temporary.path().join("worker");
        let marker = directory.join("initial-complete.json");
        let (sender, settings) = watch::channel(BoundNode::of(document.nodes[0].clone()));
        let input = WorkflowPayload::Message {
            message: "stored".into(),
        }
        .encode()
        .unwrap();
        let (worker, _) = launch(
            &host,
            &ids.nodes["inbox"],
            settings,
            temporary.path().to_path_buf(),
            directory,
            Some(input),
        )
        .await;
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
        let document = Document::parse(r#"{"name":"worker","entry":"command","nodes":[{"id":"command","component":"command","config":{"argv":["/bin/cat"]}},{"id":"output","component":"inbox"}],"edges":[{"from":"command","to":"output"},{"from":"output","to":"command"}]}"#).unwrap();
        let ids = IdentityMap::fresh(&document);
        let compiled = expand(&document, "worker", &ids)
            .unwrap()
            .compile()
            .unwrap();
        let runtime = ProposalRuntime::with_policy(compiled, crate::workflow::edit::policy());
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
        let (settings, receiver) = watch::channel(BoundNode::of(node.clone()));
        let first = WorkflowPayload::Message {
            message: "first".into(),
        }
        .encode()
        .unwrap();
        let (worker, _) = launch(
            &host,
            &ids.nodes["command"],
            receiver,
            project,
            directory.clone(),
            Some(first),
        )
        .await;
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
            WorkflowPayload::read(&bytes).unwrap(),
            WorkflowPayload::Message {
                message: "first".into()
            }
        );
        // A setting change does not replace the graph node or replay initial input.
        let mut changed = node;
        changed.config = json!({"argv":["/usr/bin/printf","second"]});
        settings.send(BoundNode::of(changed)).unwrap();
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
                        WorkflowPayload::read(&bytes).unwrap(),
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

    fn strings(packages: &[ontography::PackageId]) -> Vec<String> {
        packages.iter().map(ToString::to_string).collect()
    }

    /// Input packages pending at a node, in identity order.
    async fn pending_inputs(session: &ontography::SessionHandle, node: &str) -> Vec<String> {
        let page = session.pending_page_at(node, None, 10).await.unwrap();
        page.packages()
            .iter()
            .map(|(id, _)| id.to_string())
            .collect()
    }

    /// A command worker launched once its inputs are pending.
    struct CommandFixture {
        runtime: ProposalRuntime,
        session: ontography::SessionHandle,
        worker: ExecutionHandle,
        ledger: Arc<RetryLedger>,
        /// The command node's core identity.
        node: String,
        /// Inputs pending at launch, in identity order.
        inputs: Vec<String>,
        _host: ExecutionHost,
        // An idle worker exits once its settings sender is gone.
        _settings: watch::Sender<BoundNode>,
        temporary: tempfile::TempDir,
    }

    impl CommandFixture {
        /// `command` holds the node's settings, such as config and retry.
        async fn launch(command: serde_json::Value, messages: &[&str]) -> Self {
            let mut command = command;
            command["id"] = json!("command");
            command["kind"] = json!("command");
            let document: Document =
                serde_json::from_value(json!({"name":"failure","entry":"source",
                "nodes":[{"id":"source","component":"inbox"},command],
                "edges":[{"from":"source","to":"command"}]}))
                .unwrap();
            let ids = IdentityMap::fresh(&document);
            let compiled = expand(&document, "failure", &ids)
                .unwrap()
                .compile()
                .unwrap();
            let runtime = ProposalRuntime::with_policy(compiled, crate::workflow::edit::policy());
            let session = runtime.open().unwrap();
            let host = ExecutionHost::new(session.clone());
            let temporary = tempfile::tempdir().unwrap();
            let kernel = session.kernel().await.unwrap();
            let edge = &ids.edges[&crate::workflow::document::edge_key("source", "command")];
            for message in messages {
                let input = WorkflowPayload::Message {
                    message: (*message).into(),
                }
                .encode()
                .unwrap();
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
            }
            let node = ids.nodes["command"].clone();
            let inputs = pending_inputs(&session, &node).await;
            let settings = document
                .nodes
                .iter()
                .find(|node| node.id == "command")
                .unwrap()
                .clone();
            let (sender, receiver) = watch::channel(BoundNode::of(settings));
            let project = temporary.path().to_path_buf();
            let (worker, ledger) = launch(
                &host,
                &node,
                receiver,
                project.clone(),
                project.join("worker"),
                None,
            )
            .await;
            Self {
                runtime,
                session,
                worker,
                ledger,
                node,
                inputs,
                _host: host,
                _settings: sender,
                temporary,
            }
        }

        /// Waits until the node's invocations satisfy `done`.
        async fn wait_invocations(&self, done: impl Fn(&[ontography::InvocationRecord]) -> bool) {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let invocations = self
                        .session
                        .invocations_page(Some(&self.node), None, 10)
                        .await
                        .unwrap();
                    if done(&invocations) {
                        break;
                    }
                    assert!(
                        !self.worker.status().is_terminal(),
                        "{:?}",
                        self.worker.status()
                    );
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn failed_timed_out_and_oversized_commands_park_their_pending_input() {
        for config in [
            json!({"argv":["/bin/sh","-c","exit 7"]}),
            json!({"argv":["/bin/sleep","30"],"timeout_secs":1}),
            json!({"argv":["/usr/bin/yes"]}),
        ] {
            let fixture = CommandFixture::launch(
                json!({"config":config,"retry":{"max_attempts":1}}),
                &["input"],
            )
            .await;
            // The attempt is counted before its invocation fails.
            fixture
                .wait_invocations(|invocations| {
                    invocations
                        .first()
                        .is_some_and(|invocation| invocation.status == InvocationStatus::Failed)
                })
                .await;
            let failed = fixture.ledger.failed();
            assert_eq!(failed.len(), 1);
            assert!(failed[0].failures.parked);
            assert_eq!(failed[0].failures.attempts, 1);
            assert_eq!(strings(&failed[0].failures.inputs), fixture.inputs);
            assert_eq!(
                pending_inputs(&fixture.session, &fixture.node).await,
                fixture.inputs
            );
            // The worker outlives its failed task.
            assert_eq!(
                fixture.worker.status(),
                ontography::ExecutionStatus::Running
            );
            fixture.runtime.shutdown().await;
        }
    }

    #[tokio::test]
    async fn a_parked_task_does_not_block_later_inputs() {
        // The first input the command sees always fails. With nothing failed
        // yet, the worker starts with the first input in identity order.
        let fixture = CommandFixture::launch(
            json!({"config":{"argv":["/bin/sh","-c",
                "read x; [ -e first ] || printf %s \"$x\" > first; [ \"$x\" != \"$(cat first)\" ] || exit 3; printf %s \"$x\""]},
                "retry":{"max_attempts":2,"initial_delay_secs":0}}),
            &["one", "two"],
        )
        .await;
        fixture
            .wait_invocations(|invocations| {
                invocations
                    .iter()
                    .any(|invocation| invocation.status == InvocationStatus::Accepted)
            })
            .await;
        let failed = fixture.ledger.failed();
        assert_eq!(failed.len(), 1);
        assert!(failed[0].failures.parked);
        assert_eq!(failed[0].failures.attempts, 2);
        assert_eq!(strings(&failed[0].failures.inputs), fixture.inputs[..1]);
        assert_eq!(
            pending_inputs(&fixture.session, &fixture.node).await,
            fixture.inputs[..1]
        );
        assert_eq!(
            fixture.worker.status(),
            ontography::ExecutionStatus::Running
        );
        fixture.runtime.shutdown().await;
    }

    #[tokio::test]
    async fn stop_terminates_command_group_and_preserves_input() {
        let fixture = CommandFixture::launch(
            json!({"config":{"argv":["/bin/sh","-c","sleep 30 & echo $! > child.pid; wait"]}}),
            &["input"],
        )
        .await;
        let pid_file = fixture.temporary.path().join("child.pid");
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
        fixture.worker.request_stop();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), fixture.worker.wait())
                .await
                .unwrap(),
            ontography::ExecutionStatus::Exited
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            while nix::sys::signal::kill(nix::unistd::Pid::from_raw(child), None).is_ok() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            pending_inputs(&fixture.session, &fixture.node).await,
            fixture.inputs
        );
        // Stopping interrupts the attempt without counting it as a failure.
        assert!(fixture.ledger.failed().is_empty());
        fixture.runtime.shutdown().await;
    }

    #[tokio::test]
    async fn workspace_command_uses_private_checkout_and_publishes_captured_package() {
        let document = Document::parse(r#"{"name":"workspace","entry":"command","nodes":[{"id":"command","component":"command","config":{"argv":["/bin/sh","-c","printf changed > file.txt"]}},{"id":"output","component":"inbox"}],"edges":[{"from":"command","to":"output"}]}"#).unwrap();
        let ids = IdentityMap::fresh(&document);
        let compiled = expand(&document, "workspace", &ids)
            .unwrap()
            .compile()
            .unwrap();
        let runtime = ProposalRuntime::with_policy(compiled, crate::workflow::edit::policy());
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
        let (sender, settings) = watch::channel(BoundNode::of(node));
        let directory = temporary.path().join("worker");
        let complete = directory.join("initial-complete.json");
        let (worker, _) = launch(
            &host,
            &ids.nodes["command"],
            settings,
            source,
            directory,
            Some(initial),
        )
        .await;
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
        let WorkflowPayload::Workspace(envelope) = WorkflowPayload::read(&payload).unwrap() else {
            panic!("expected workspace");
        };
        let captured = PackageStore::new(content.clone())
            .resolve(envelope.ontography_package)
            .await
            .unwrap();
        let file = captured.entry("file.txt").unwrap();
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
        // The app owns the checkout; the attempt's evidence records what the
        // command was given.
        let invocations = session
            .invocations_page(Some(&ids.nodes["command"]), None, 10)
            .await
            .unwrap();
        let [invocation] = &invocations[..] else {
            panic!("expected one attempt, got {invocations:?}");
        };
        let events = session
            .invocation_events(invocation.id, 0, 100)
            .await
            .unwrap();
        let exposure = events
            .iter()
            .find(|event| {
                event.operation == "tool_response" && event.source["tool"] == "workspace_exposure"
            })
            .expect("the checkout's exposure is recorded");
        let exposed: serde_json::Value = serde_json::from_slice(
            &session
                .invocation_content(invocation.id, exposure.sequence, 0..exposure.bytes)
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(exposed["root"], json!(base.root()));
        assert_eq!(exposed["writable"], true);
        let checkout = PathBuf::from(exposed["path"].as_str().unwrap());
        assert!(checkout.ends_with(invocation.id.to_string()));
        assert!(!checkout.exists());
        // It was marked sent once the command started.
        assert!(events.iter().any(|event| {
            event.receipt_sequence == exposure.sequence && event.state == ReceiptState::Sent
        }));
        worker.request_stop();
        worker.wait().await;
        drop(sender);
        runtime.shutdown().await;
    }

    /// A file sent as a workspace can never be checked out as a directory, so
    /// its task parks at once, before any command runs, under the default
    /// retry policy. The worker keeps running.
    #[tokio::test]
    async fn a_file_sent_as_a_workspace_parks_at_once_and_the_worker_keeps_running() {
        let document = Document::parse(r#"{"name":"file-workspace","entry":"command","nodes":[{"id":"command","component":"command","config":{"argv":["/usr/bin/touch","should-not-run"]}},{"id":"output","component":"inbox"}],"edges":[{"from":"command","to":"output"}]}"#).unwrap();
        let ids = IdentityMap::fresh(&document);
        let compiled = expand(&document, "file-workspace", &ids)
            .unwrap()
            .compile()
            .unwrap();
        let runtime = ProposalRuntime::with_policy(compiled, crate::workflow::edit::policy());
        let session = runtime.open().unwrap();
        let host = ExecutionHost::new(session.clone());
        let temporary = tempfile::tempdir().unwrap();
        let content = session.content_store().await.unwrap();
        let text = content.import_bytes(b"text".to_vec()).await.unwrap();
        let file = PackageStore::new(content)
            .put(&ontography::PackageDocument::File {
                content: text,
                executable: false,
            })
            .await
            .unwrap();
        let initial = WorkflowPayload::Workspace(PackageEnvelope::new(file))
            .encode()
            .unwrap();
        let node = document
            .nodes
            .iter()
            .find(|node| node.id == "command")
            .unwrap()
            .clone();
        let (sender, settings) = watch::channel(BoundNode::of(node));
        let (worker, ledger) = launch(
            &host,
            &ids.nodes["command"],
            settings,
            temporary.path().to_path_buf(),
            temporary.path().join("worker"),
            Some(initial),
        )
        .await;
        let failed = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(failed) = ledger.failed().pop() {
                    break failed;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(failed.failures.parked && failed.failures.attempts == 1);
        assert!(
            failed.failures.error.contains("must be a directory"),
            "{}",
            failed.failures.error
        );
        assert_eq!(worker.status(), ontography::ExecutionStatus::Running);
        assert!(!temporary.path().join("should-not-run").exists());
        drop(sender);
        runtime.shutdown().await;
    }

    /// A symlink sent as a workspace can never be checked out as a directory
    /// either, so its task parks at once as well.
    #[tokio::test]
    async fn a_symlink_sent_as_a_workspace_parks_at_once() {
        let document = Document::parse(r#"{"name":"symlink-workspace","entry":"command","nodes":[{"id":"command","component":"command","config":{"argv":["/usr/bin/touch","should-not-run"]}},{"id":"output","component":"inbox"}],"edges":[{"from":"command","to":"output"}]}"#).unwrap();
        let ids = IdentityMap::fresh(&document);
        let compiled = expand(&document, "symlink-workspace", &ids)
            .unwrap()
            .compile()
            .unwrap();
        let runtime = ProposalRuntime::with_policy(compiled, crate::workflow::edit::policy());
        let session = runtime.open().unwrap();
        let host = ExecutionHost::new(session.clone());
        let temporary = tempfile::tempdir().unwrap();
        let content = session.content_store().await.unwrap();
        let link = PackageStore::new(content)
            .put(&ontography::PackageDocument::Symlink {
                target: "elsewhere".into(),
            })
            .await
            .unwrap();
        let initial = WorkflowPayload::Workspace(PackageEnvelope::new(link))
            .encode()
            .unwrap();
        let node = document
            .nodes
            .iter()
            .find(|node| node.id == "command")
            .unwrap()
            .clone();
        let (sender, settings) = watch::channel(BoundNode::of(node));
        let (worker, ledger) = launch(
            &host,
            &ids.nodes["command"],
            settings,
            temporary.path().to_path_buf(),
            temporary.path().join("worker"),
            Some(initial),
        )
        .await;
        let failed = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(failed) = ledger.failed().pop() {
                    break failed;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            failed.failures.parked && failed.failures.attempts == 1,
            "a symlink workspace should park at once like a file, got {:?}",
            failed.failures
        );
        assert!(!temporary.path().join("should-not-run").exists());
        worker.request_stop();
        worker.wait().await;
        drop(sender);
        runtime.shutdown().await;
    }

    /// Checkouts a crash left behind are removed when a worker starts, even
    /// read-only ones: no attempt will ever finish them.
    #[tokio::test]
    async fn abandoned_checkouts_are_removed_when_a_worker_starts() {
        use std::os::unix::fs::PermissionsExt;
        let document = Document::parse(r#"{"name":"abandoned","entry":"command","nodes":[{"id":"command","component":"command","config":{"argv":["/usr/bin/true"]}}]}"#).unwrap();
        let ids = IdentityMap::fresh(&document);
        let compiled = expand(&document, "abandoned", &ids)
            .unwrap()
            .compile()
            .unwrap();
        let runtime = ProposalRuntime::with_policy(compiled, crate::workflow::edit::policy());
        let session = runtime.open().unwrap();
        let host = ExecutionHost::new(session.clone());
        let temporary = tempfile::tempdir().unwrap();
        let directory = temporary.path().join("worker");
        let abandoned = directory.join("workspaces/checkouts/crashed");
        std::fs::create_dir_all(abandoned.join("nested")).unwrap();
        std::fs::write(abandoned.join("nested/file.txt"), "left behind").unwrap();
        for path in [abandoned.join("nested"), abandoned.clone()] {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o500)).unwrap();
        }
        let (sender, settings) = watch::channel(BoundNode::of(document.nodes[0].clone()));
        let (worker, _) = launch(
            &host,
            &ids.nodes["command"],
            settings,
            temporary.path().to_path_buf(),
            directory,
            None,
        )
        .await;
        tokio::time::timeout(Duration::from_secs(3), async {
            while abandoned.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the abandoned checkout is removed");
        assert_eq!(worker.status(), ontography::ExecutionStatus::Running);
        worker.request_stop();
        worker.wait().await;
        drop(sender);
        runtime.shutdown().await;
    }

    /// Startup cleanup is best-effort: a leftover checkout the app cannot
    /// delete, here holding a file with the user-immutable flag, stays behind,
    /// and the worker runs anyway.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn a_worker_starts_even_if_an_abandoned_checkout_cannot_be_removed() {
        let document = Document::parse(r#"{"name":"undeletable","entry":"command","nodes":[{"id":"command","component":"command","config":{"argv":["/usr/bin/true"]}}]}"#).unwrap();
        let ids = IdentityMap::fresh(&document);
        let compiled = expand(&document, "undeletable", &ids)
            .unwrap()
            .compile()
            .unwrap();
        let runtime = ProposalRuntime::with_policy(compiled, crate::workflow::edit::policy());
        let session = runtime.open().unwrap();
        let host = ExecutionHost::new(session.clone());
        let temporary = tempfile::tempdir().unwrap();
        let directory = temporary.path().join("worker");
        let abandoned = directory.join("workspaces/checkouts/crashed");
        std::fs::create_dir_all(&abandoned).unwrap();
        let locked = abandoned.join("locked.txt");
        std::fs::write(&locked, "cannot be unlinked").unwrap();
        let chflags = |flag: &str| {
            std::process::Command::new("/usr/bin/chflags")
                .arg(flag)
                .arg(&locked)
                .status()
                .unwrap()
                .success()
        };
        assert!(chflags("uchg"));
        let (sender, settings) = watch::channel(BoundNode::of(document.nodes[0].clone()));
        let (worker, _) = launch(
            &host,
            &ids.nodes["command"],
            settings,
            temporary.path().to_path_buf(),
            directory,
            None,
        )
        .await;
        let deadline = Instant::now() + Duration::from_millis(500);
        while Instant::now() < deadline && !worker.status().is_terminal() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let status = worker.status();
        // Unlock first, so the temporary directory can always be removed.
        assert!(chflags("nouchg"));
        assert_eq!(
            status,
            ontography::ExecutionStatus::Running,
            "an undeletable leftover checkout stopped the worker"
        );
        worker.request_stop();
        worker.wait().await;
        drop(sender);
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn workspace_without_output_route_parks_its_task_before_running_a_command() {
        let document = Document::parse(r#"{"name":"terminal-workspace","entry":"command","nodes":[{"id":"command","component":"command","config":{"argv":["/usr/bin/touch","should-not-run"]}}]}"#).unwrap();
        let ids = IdentityMap::fresh(&document);
        let compiled = expand(&document, "terminal-workspace", &ids)
            .unwrap()
            .compile()
            .unwrap();
        let runtime = ProposalRuntime::with_policy(compiled, crate::workflow::edit::policy());
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
        let (sender, settings) = watch::channel(BoundNode::of(document.nodes[0].clone()));
        let (worker, ledger) = launch(
            &host,
            &ids.nodes["command"],
            settings,
            source,
            temporary.path().join("worker"),
            Some(initial),
        )
        .await;
        let failed = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(failed) = ledger.failed().pop() {
                    break failed;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        // Retrying the same input on the same graph cannot succeed.
        assert!(failed.failures.parked);
        assert_eq!(failed.failures.attempts, 1);
        assert!(failed.failures.error.contains("outgoing connection"));
        assert_eq!(failed.key, Task::initial(&ids.nodes["command"]).key);
        assert_eq!(worker.status(), ontography::ExecutionStatus::Running);
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

    /// Submits a root or package-triggered activation at `node` that emits
    /// `payload` on `edge`, as a finished upstream task would.
    async fn emit_on(
        session: &ontography::SessionHandle,
        node: &str,
        trigger: Option<ontography::PackageId>,
        edge: &str,
        payload: Payload,
    ) {
        let kernel = session.kernel().await.unwrap();
        let mut proposal = match trigger {
            Some(package) => ontography::ActivationProposal::join([package], payload.clone()),
            None => ontography::ActivationProposal::root(
                node,
                kernel.root_ceiling(node).unwrap().clone(),
                payload.clone(),
            ),
        };
        proposal.emit(Emission::new(edge, OutputAuthority::Carry, payload));
        assert!(matches!(
            session.submit(proposal).await.unwrap(),
            ProposalDecision::Committed(_)
        ));
    }

    fn text(message: &str) -> Payload {
        WorkflowPayload::Message {
            message: message.into(),
        }
        .encode()
        .unwrap()
    }

    /// A task fails "when the task cannot start as delivered", and its worker
    /// keeps running. Core refuses to begin an input larger than the attempt's
    /// context budget; that is the task's failure, not the worker's.
    #[tokio::test]
    async fn an_input_core_refuses_to_begin_is_recorded_and_the_worker_keeps_running() {
        let fixture =
            CommandFixture::launch(json!({"config":{"argv":["/bin/cat"]}}), &["small"]).await;
        let kernel = fixture.session.kernel().await.unwrap();
        let edge = kernel
            .graph()
            .edges()
            .iter()
            .find(|edge| edge.target() == fixture.node)
            .unwrap()
            .clone();
        emit_on(
            &fixture.session,
            edge.source(),
            None,
            edge.id(),
            text(&"x".repeat(9 * 1024 * 1024)),
        )
        .await;
        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if fixture.worker.status().is_terminal() {
                    break format!("worker stopped: {:?}", fixture.worker.status());
                }
                if !fixture.ledger.failed().is_empty() {
                    break "recorded".to_owned();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            outcome, "recorded",
            "an input that cannot start must be recorded against its task"
        );
        assert_eq!(
            fixture.worker.status(),
            ontography::ExecutionStatus::Running
        );
        fixture.runtime.shutdown().await;
    }

    /// A failed task waits while other tasks at the node proceed, at `all`
    /// nodes too. Core's own next join is the least package on every edge, so
    /// the parked input stays first on its edge in about half of the trials;
    /// the node must still run a complete join without it.
    #[tokio::test]
    async fn a_parked_join_does_not_block_later_joins_at_an_all_node() {
        // Joined messages arrive in package order; fail on "bad" anywhere.
        const JOIN: &str = r#"x=$(cat); case "$x" in *bad*) exit 3;; esac; printf ok"#;
        for _trial in 0..24 {
            let document: Document = serde_json::from_value(json!({
                "name": "join", "entry": "feed",
                "nodes": [
                    {"id": "feed", "component": "inbox"},
                    {"id": "left", "component": "inbox"},
                    {"id": "right", "component": "inbox"},
                    {"id": "join", "component": "command", "join": "all",
                        "config": {"argv": ["/bin/sh", "-c", JOIN]}, "retry": {"max_attempts": 1}},
                    {"id": "done", "component": "inbox"}],
                "edges": [
                    {"from": "feed", "to": "left"}, {"from": "feed", "to": "right"},
                    {"from": "left", "to": "join"}, {"from": "right", "to": "join"},
                    {"from": "join", "to": "done"}],
            }))
            .unwrap();
            let ids = IdentityMap::fresh(&document);
            let compiled = expand(&document, "join", &ids).unwrap().compile().unwrap();
            let runtime = ProposalRuntime::with_policy(compiled, crate::workflow::edit::policy());
            let session = runtime.open().unwrap();
            let host = ExecutionHost::new(session.clone());
            let temporary = tempfile::tempdir().unwrap();
            let edge = |from: &str, to: &str| {
                ids.edges[&crate::workflow::document::edge_key(from, to)].clone()
            };
            let node = ids.nodes["join"].clone();
            // A seed at `feed` reaches `left` and `right`; each forwards its message to `join`.
            let deliver = async |left: &str, right: &str| {
                for (side, message) in [("left", left), ("right", right)] {
                    emit_on(
                        &session,
                        &ids.nodes["feed"],
                        None,
                        &edge("feed", side),
                        text("seed"),
                    )
                    .await;
                    let page = session
                        .pending_page_at(ids.nodes[side].clone(), None, 10)
                        .await
                        .unwrap();
                    let (package, _) = page.packages()[0];
                    emit_on(
                        &session,
                        &ids.nodes[side],
                        Some(package),
                        &edge(side, "join"),
                        text(message),
                    )
                    .await;
                }
            };
            deliver("bad", "r1").await;
            let settings = document
                .nodes
                .iter()
                .find(|candidate| candidate.id == "join")
                .unwrap()
                .clone();
            let (sender, receiver) = watch::channel(BoundNode::of(settings));
            let project = temporary.path().to_path_buf();
            let (worker, ledger) = launch(
                &host,
                &node,
                receiver,
                project.clone(),
                project.join("worker"),
                None,
            )
            .await;
            tokio::time::timeout(Duration::from_secs(5), async {
                while !ledger.failed().iter().any(|failed| failed.failures.parked) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("the bad join parks");
            deliver("good", "r2").await;
            // Which left input does core pair first now?
            let head = session.next_trigger_at(node.clone()).await.unwrap();
            let mut messages = Vec::new();
            for (_, record) in head.packages() {
                let bytes = session
                    .content(record.content_digest())
                    .await
                    .unwrap()
                    .unwrap();
                if let WorkflowPayload::Message { message } = WorkflowPayload::read(&bytes).unwrap()
                {
                    messages.push(message);
                }
            }
            if messages.iter().any(|message| message == "good") {
                // The good input is least on its edge: not the case under test.
                worker.request_stop();
                worker.wait().await;
                drop(sender);
                runtime.shutdown().await;
                continue;
            }
            // "good" and a right input form a complete join that excludes the bad input.
            let accepted = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let invocations = session
                        .invocations_page(Some(&node), None, 20)
                        .await
                        .unwrap();
                    if invocations
                        .iter()
                        .any(|invocation| invocation.status == InvocationStatus::Accepted)
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await;
            let parked = ledger.failed();
            worker.request_stop();
            worker.wait().await;
            drop(sender);
            runtime.shutdown().await;
            assert!(
                accepted.is_ok(),
                "a complete join without the parked input never ran; core's head join {messages:?}; failures {parked:#?}"
            );
            return;
        }
        panic!("no trial placed the parked input first on its edge");
    }

    /// An attempt still running when its node's definition changes fails
    /// afterwards. Its failure counts against the old definition, so the new
    /// one attempts the task afresh instead of finding it parked.
    #[tokio::test]
    async fn an_attempt_begun_under_an_old_definition_does_not_park_the_new_one() {
        let fixture = CommandFixture::launch(
            json!({"config":{"argv":["/bin/sh","-c","sleep 1; exit 3"]},"retry":{"max_attempts":1}}),
            &["input"],
        )
        .await;
        let old = fixture._settings.borrow().clone();
        // Reconcile renews every ledger before launching its worker.
        fixture.ledger.renew(&old.digest()).unwrap();
        fixture
            .wait_invocations(|invocations| {
                invocations
                    .first()
                    .is_some_and(|invocation| invocation.status == InvocationStatus::Open)
            })
            .await;
        // The manager fixes the command while its attempt runs, as reconcile
        // applies it: new settings, then the renewed ledger.
        let mut fixed = old.node.clone();
        fixed.config = json!({"argv":["/bin/sh","-c","printf fixed"]});
        let fixed = BoundNode::of(fixed);
        fixture._settings.send_replace(fixed.clone());
        fixture.ledger.renew(&fixed.digest()).unwrap();
        // The old attempt fails; the fixed command should then run the task.
        let accepted = tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                let invocations = fixture
                    .session
                    .invocations_page(Some(&fixture.node), None, 10)
                    .await
                    .unwrap();
                if invocations
                    .iter()
                    .any(|invocation| invocation.status == InvocationStatus::Accepted)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        let failed = fixture.ledger.failed();
        fixture.runtime.shutdown().await;
        assert!(
            accepted.is_ok(),
            "the fixed definition never attempted the task; failures {failed:#?}"
        );
    }
}
