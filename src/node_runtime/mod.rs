//! One continuing agent execution at a graph node. Terminals are views of this
//! execution; its selected scoped tools are exposed through the node MCP adapter.

#[allow(dead_code, reason = "NodeRuntime::run does not launch it yet")]
pub(crate) mod claude;
pub mod codex;
mod process;
mod rpc;

use crate::{
    AppError, Result,
    node_mcp::NodeMcp,
    node_tool::{NodeScope, NodeToolContext},
    terminal::{LaunchSpec, Terminal, TerminalStatus},
    workflow::{harness, tasks::RetryLedger},
};
use ontography::{ExecutionContext, ExecutionFailure, Payload, SessionHandle, SessionStatus};
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, Weak},
    time::Duration,
};
use tokio::sync::watch;

#[derive(Clone, Debug, Serialize)]
pub struct NodeStatus {
    pub state: &'static str,
    pub directory: PathBuf,
    pub cwd: PathBuf,
    pub conversation_id: Option<String>,
    pub agent_state: Option<String>,
    pub terminal: Option<TerminalStatus>,
    pub exit_code: Option<u32>,
    pub error: Option<AppError>,
}

struct State {
    status: NodeStatus,
    terminal: Weak<Terminal>,
    tools: Weak<NodeToolContext>,
    started: bool,
}

/// The workflow holds this handle; the execution future owns the live resources.
/// Retaining a handle, a terminal view, or a tool context cannot keep a stopped
/// execution's process alive.
pub struct NodeRuntime {
    project: PathBuf,
    directory: PathBuf,
    session: SessionHandle,
    scope: watch::Receiver<NodeScope>,
    ledger: Arc<RetryLedger>,
    initial: Option<Payload>,
    state: Mutex<State>,
}

impl NodeRuntime {
    pub fn new(
        project: PathBuf,
        directory: PathBuf,
        session: SessionHandle,
        scope: watch::Receiver<NodeScope>,
        ledger: Arc<RetryLedger>,
        initial: Option<Payload>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                status: NodeStatus {
                    state: "starting",
                    cwd: directory.join("workspace"),
                    directory: directory.clone(),
                    conversation_id: None,
                    agent_state: None,
                    terminal: None,
                    exit_code: None,
                    error: None,
                },
                terminal: Weak::new(),
                tools: Weak::new(),
                started: false,
            }),
            project,
            directory,
            session,
            scope,
            ledger,
            initial,
        })
    }

    pub fn status(&self) -> NodeStatus {
        let state = lock(&self.state);
        let mut status = state.status.clone();
        if let Some(terminal) = state.terminal.upgrade() {
            status.terminal = Some(terminal.status());
        }
        status
    }

    pub fn terminal(&self) -> Option<Arc<Terminal>> {
        lock(&self.state).terminal.upgrade()
    }

    pub fn tools(&self) -> Option<Arc<NodeToolContext>> {
        lock(&self.state).tools.upgrade()
    }

    pub async fn run(
        self: Arc<Self>,
        context: ExecutionContext,
    ) -> std::result::Result<(), ExecutionFailure> {
        {
            let mut state = lock(&self.state);
            if state.started {
                return Err(ExecutionFailure::new(
                    "node_runtime",
                    "This node execution was already started",
                ));
            }
            state.started = true;
        }
        let mut resources = Resources::new(&self);
        let result = self.run_inner(context, &mut resources).await;
        let cleanup = resources.close().await;
        let result = result.and(cleanup);
        if let Err(error) = &result {
            let mut state = lock(&self.state);
            state.status.state = "failed";
            state.status.error = Some(error.clone());
        }
        result.map_err(|error| ExecutionFailure::new(error.code, error.message))
    }

    async fn run_inner(
        &self,
        context: ExecutionContext,
        resources: &mut Resources<'_>,
    ) -> Result<()> {
        let mut stop = context.stop();
        if stop.is_requested() {
            lock(&self.state).status.state = "stopped";
            return Ok(());
        }
        private_directory(&self.directory)?;
        // Reclaim any previous task/terminal supervisor before lending out this
        // identity or cleaning its abandoned attempt workspaces.
        harness::recover_process(&self.directory)
            .await
            .map_err(worker_error)?;
        let cwd = self.directory.join("workspace");
        private_directory(&cwd)?;
        let tools = Arc::new(
            NodeToolContext::new(
                context,
                self.session.clone(),
                self.scope.clone(),
                self.ledger.clone(),
                self.directory.clone(),
                self.initial.clone(),
            )
            .await?,
        );
        lock(&self.state).tools = Arc::downgrade(&tools);
        // Keep local sockets short enough for Unix sockaddr paths. Each MCP
        // execution gets a fresh endpoint and capability, including on resume.
        let identity = format!(
            "{:x}",
            Sha256::digest(self.directory.as_os_str().as_encoded_bytes())
        );
        let endpoint = PathBuf::from("/tmp").join(format!(
            "ontography-node-{}-{}",
            nix::unistd::geteuid(),
            &identity[..20]
        ));
        private_directory(&endpoint)?;
        resources.mcp = Some(NodeMcp::bind(&endpoint, tools.clone())?);
        resources.tools = Some(tools.clone());
        let node = self.scope.borrow().node.clone();
        let mcp = resources.mcp.as_ref().expect("bound node MCP");
        let mut env = mcp.environment();
        env.insert("ONTOGRAPHY_NODE_NAME".into(), node.id.clone());
        env.insert(
            "ONTOGRAPHY_NODE_DIRECTORY".into(),
            self.directory.to_string_lossy().into_owned(),
        );
        env.insert(
            "ONTOGRAPHY_PROJECT".into(),
            self.project.to_string_lossy().into_owned(),
        );
        let spec = if let Some(argv) = node.config.get("argv") {
            let argv = argv
                .as_array()
                .ok_or_else(|| AppError::invalid("agent argv must be an array"))?;
            let argv: Vec<_> = argv
                .iter()
                .map(|arg| {
                    arg.as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| AppError::invalid("agent argv must contain strings"))
                })
                .collect::<Result<_>>()?;
            let (program, args) = argv
                .split_first()
                .filter(|(program, _)| !program.trim().is_empty())
                .ok_or_else(|| AppError::invalid("agent argv must name a program"))?;
            LaunchSpec {
                program: program.into(),
                args: args.to_vec(),
                env,
                cwd,
                rows: 24,
                cols: 80,
                server_id: uuid::Uuid::new_v4().to_string(),
                session_id: node.id.clone(),
            }
        } else {
            let plan = codex::Plan::new(&node, &self.directory, &cwd, &endpoint)?;
            let spec = plan.launch(Path::new("codex"), &mcp.codex_overrides()?, env);
            resources.plan = Some(plan);
            spec
        };
        let lifetime = process::Lifetime::new(&self.directory)?;
        let spec = lifetime.supervise(spec);
        resources.lifetime = Some(lifetime);
        let terminal = Terminal::launch(spec, endpoint.join("terminal.sock")).await?;
        {
            let mut state = lock(&self.state);
            state.terminal = Arc::downgrade(&terminal);
            state.status.terminal = Some(terminal.status());
        }
        resources.terminal = Some(terminal.clone());
        let lifetime = resources.lifetime.as_mut().expect("prepared lifetime");
        lifetime.permit(&terminal).await?;
        let conversation = if let Some(plan) = &resources.plan {
            let ready = async {
                let rpc = loop {
                    if !terminal.status().running {
                        return Err(AppError::new(
                            "codex_startup",
                            "Codex app-server exited; inspect codex-server.log",
                        ));
                    }
                    if plan.socket.try_exists()? {
                        break rpc::Rpc::connect(&plan.socket).await?;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                };
                let id = plan.open(&rpc).await?;
                plan.attach(&id)?;
                Ok::<_, AppError>((rpc, id))
            };
            let (rpc, id) = tokio::select! {
                () = stop.requested() => { return Ok(()); }
                result = tokio::time::timeout(Duration::from_secs(45), ready) => {
                    result.map_err(|_| AppError::new("codex_startup", "Codex app-server initialization timed out; inspect codex-server.log"))??
                }
            };
            lock(&self.state).status.conversation_id = Some(id.clone());
            resources.rpc = Some(rpc);
            Some(id)
        } else {
            None
        };
        lock(&self.state).status.state = "running";
        let delivery = async {
            match (&mut resources.rpc, conversation) {
                (Some(rpc), Some(id)) => self.deliver(rpc, &id, &tools).await,
                _ => std::future::pending::<Result<()>>().await,
            }
        };
        tokio::pin!(delivery);
        let mut tick = tokio::time::interval(Duration::from_millis(25));
        loop {
            tokio::select! {
                result = &mut delivery => { return result; }
                () = stop.requested() => {
                    lock(&self.state).status.state = "stopped";
                    return Ok(());
                }
                _ = tick.tick() => {
                    if self.session.status() != SessionStatus::Open {
                        lock(&self.state).status.state = "stopped";
                        return Ok(());
                    }
                    let status = terminal.status();
                    if let Some(fault) = &status.fault {
                        return Err(AppError::new("node_terminal", fault));
                    }
                    if !status.running {
                        let code = lifetime.exit_code()?;
                        let mut state = lock(&self.state);
                        state.status.terminal = Some(status);
                        state.status.exit_code = Some(code);
                        state.status.state = "exited";
                        return if code == 0 { Ok(()) } else {
                            Err(AppError::new("node_process_exit", format!("Agent session exited with status {code}")))
                        };
                    }
                }
            }
        }
    }

    async fn deliver(
        &self,
        rpc: &mut rpc::Rpc,
        thread: &str,
        tools: &NodeToolContext,
    ) -> Result<()> {
        let mut tick = tokio::time::interval(Duration::from_millis(250));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut messages = HashMap::<String, String>::new();
        let mut turns = HashMap::<String, Vec<String>>::new();
        loop {
            tokio::select! {
                _ = rpc.health.changed() => {
                    return Err(rpc.health.borrow().clone().unwrap_or_else(|| AppError::new("codex_connection", "Codex controller stopped")));
                }
                event = rpc.events.recv() => {
                    let Some(event) = event else { return Err(AppError::new("codex_connection", "Codex event stream closed")); };
                    if event["params"]["threadId"] != thread { continue; }
                    if event["method"] == "item/started"
                        && let Some(client) = event["params"]["item"]["clientId"].as_str()
                        && let Some(attempt) = messages.remove(client)
                        && let Some(turn) = event["params"]["turnId"].as_str() {
                        turns.entry(turn.into()).or_default().push(attempt);
                    }
                    if event["method"] == "turn/completed"
                        && let Some(turn) = event["params"]["turn"]["id"].as_str()
                        && let Some(attempts) = turns.remove(turn)
                        && event["params"]["turn"]["status"] != "completed" {
                        for attempt in attempts {
                            tools.message_failed(&attempt, &format!("Codex turn ended: {}",event["params"]["turn"]["status"])).await?;
                        }
                    }
                }
                _ = tick.tick() => {
                    messages.retain(|_, attempt| tools.message_is_open(attempt));
                    let state = rpc.call("thread/read", json!({"threadId":thread})).await?;
                    let status = state["thread"]["status"]["type"].as_str().unwrap_or("unknown");
                    lock(&self.state).status.agent_state = Some(status.into());
                    if matches!(status, "notLoaded" | "systemError") {
                        return Err(AppError::new("codex_session", format!("Codex conversation became {status}")));
                    }
                    if let Some(reply) = tools.next_message().await? {
                        let value = reply.value()?;
                        let attempt = value["attempt_id"].as_str().ok_or_else(|| AppError::new("codex_delivery", "Message has no attempt ID"))?;
                        let client_id = format!("ontography:{attempt}");
                        let text = std::str::from_utf8(reply.bytes()).map_err(|error| AppError::new("codex_delivery", error.to_string()))?;
                        rpc.call("thread/queue/add", json!({"threadId":thread,
                            "clientUserMessageId":client_id,
                            "input":[{"type":"text","text":text}]})).await?;
                        // Server acceptance, rather than socket write, is the
                        // receipt boundary. The queue preserves input verbatim.
                        reply.sent().await?;
                        messages.insert(client_id, attempt.into());
                    }
                    if status == "idle" { start_queued(rpc, thread).await?; }
                }
            }
        }
    }
}

async fn start_queued(rpc: &rpc::Rpc, thread: &str) -> Result<()> {
    let queue = rpc
        .call("thread/queue/list", json!({"threadId":thread,"limit":1}))
        .await?;
    let Some(first) = queue["data"].as_array().and_then(|items| items.first()) else {
        return Ok(());
    };
    match rpc
        .call(
            "thread/queue/start",
            json!({"threadId":thread,"queuedSubmissionId":first["id"]}),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(error) if error.code == "codex_rpc" => {
            // The native TUI can start the same queued turn concurrently. Only
            // forgive a refusal after observing that race; never replay input.
            let state = rpc.call("thread/read", json!({"threadId":thread})).await?;
            if state["thread"]["status"]["type"] == "active" {
                return Ok(());
            }
            let queue = rpc
                .call("thread/queue/list", json!({"threadId":thread,"limit":1}))
                .await?;
            if queue["data"][0]["id"] != first["id"] {
                return Ok(());
            }
            Err(error)
        }
        Err(error) => Err(error),
    }
}

/// This guard also runs when core forcibly aborts the execution future. It stops
/// writers synchronously even if outside code retains terminal/tool handles.
struct Resources<'a> {
    owner: &'a NodeRuntime,
    terminal: Option<Arc<Terminal>>,
    tools: Option<Arc<NodeToolContext>>,
    lifetime: Option<process::Lifetime>,
    mcp: Option<NodeMcp>,
    rpc: Option<rpc::Rpc>,
    plan: Option<codex::Plan>,
}

impl<'a> Resources<'a> {
    fn new(owner: &'a NodeRuntime) -> Self {
        Self {
            owner,
            terminal: None,
            tools: None,
            lifetime: None,
            mcp: None,
            rpc: None,
            plan: None,
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.rpc.take();
        if let Some(mut mcp) = self.mcp.take() {
            mcp.shutdown().await;
        }
        if let Some(lifetime) = &mut self.lifetime {
            lifetime.disconnect();
        }
        let stopped = if let Some(terminal) = &self.terminal {
            terminal.request_stop();
            let result = terminal.shutdown().await;
            lock(&self.owner.state).status.terminal = Some(terminal.status());
            result
        } else {
            Ok(())
        };
        if let Some(tools) = &self.tools {
            tools.close().await;
        }
        stopped?;
        if self.lifetime.is_some() {
            harness::recover_process(&self.owner.directory)
                .await
                .map_err(worker_error)?;
        }
        Ok(())
    }
}

impl Drop for Resources<'_> {
    fn drop(&mut self) {
        self.rpc.take();
        self.mcp.take();
        if let Some(lifetime) = &mut self.lifetime {
            lifetime.disconnect();
        }
        if let Some(terminal) = &self.terminal {
            terminal.request_stop();
        }
        let mut state = lock(&self.owner.state);
        if let Some(terminal) = &self.terminal {
            state.status.terminal = Some(terminal.status());
        }
        if matches!(state.status.state, "starting" | "running") {
            state.status.state = "stopped";
        }
        state.terminal = Weak::new();
        state.tools = Weak::new();
    }
}

fn worker_error(error: ExecutionFailure) -> AppError {
    AppError::new(error.class(), error.message())
}

/// Internal helper called from a terminal supervisor after loss of its owner.
/// The helper is itself a member of the session it is allowed to clean.
#[doc(hidden)]
pub fn cleanup_process_session(owner: i32) -> Result<()> {
    use nix::unistd::{Pid, getpgid, getsid};
    let owner = Pid::from_raw(owner);
    if owner.as_raw() <= 1
        || getsid(None).ok() != Some(owner)
        || getpgid(Some(owner)).ok() != Some(owner)
    {
        return Err(AppError::new(
            "node_cleanup",
            "Cleanup must run inside its owning terminal session",
        ));
    }
    crate::terminal::signal_session_groups(owner)
        .map_err(|error| AppError::new("node_cleanup", error.to_string()))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn private_directory(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)?;
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.uid() != nix::unistd::geteuid().as_raw() {
        return Err(AppError::new(
            "invalid_node_directory",
            format!("{} must be an owned directory", path.display()),
        ));
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}
