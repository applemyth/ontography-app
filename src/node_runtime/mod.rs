//! One continuing agent execution at a graph node. Terminals are views of this
//! execution; its selected scoped tools are exposed through the node MCP adapter.

pub mod codex;
mod process;

use crate::{
    AppError, Result,
    node_mcp::NodeMcp,
    node_tool::{NodeScope, NodeToolContext},
    terminal::{LaunchSpec, Terminal, TerminalStatus},
    workflow::{harness, tasks::RetryLedger},
};
use ontography::{ExecutionContext, ExecutionFailure, Payload, SessionHandle, SessionStatus};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
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
        resources.tools = Some(tools);
        let node = self.scope.borrow().node.clone();
        let mut prepared = tokio::select! {
            () = stop.requested() => {
                lock(&self.state).status.state = "stopped";
                return Ok(());
            }
            prepared = codex::prepare(&node, &self.directory, &cwd) => prepared?,
        };
        let mcp = resources.mcp.as_ref().expect("bound node MCP");
        if prepared.conversation_id.is_some() {
            prepared
                .args
                .splice(0..0, ["-c".into(), mcp.codex_config()?]);
        }
        prepared.env.extend(mcp.environment());
        lock(&self.state).status.conversation_id = prepared.conversation_id;
        let mut env = prepared.env;
        env.insert("ONTOGRAPHY_NODE_NAME".into(), node.id.clone());
        env.insert(
            "ONTOGRAPHY_NODE_DIRECTORY".into(),
            self.directory.to_string_lossy().into_owned(),
        );
        env.insert(
            "ONTOGRAPHY_PROJECT".into(),
            self.project.to_string_lossy().into_owned(),
        );
        let spec = LaunchSpec {
            program: prepared.program,
            args: prepared.args,
            env,
            cwd,
            rows: 24,
            cols: 80,
            server_id: uuid::Uuid::new_v4().to_string(),
            session_id: node.id,
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
        lock(&self.state).status.state = "running";
        let mut tick = tokio::time::interval(Duration::from_millis(25));
        loop {
            tokio::select! {
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
}

/// This guard also runs when core forcibly aborts the execution future. It stops
/// writers synchronously even if outside code retains terminal/tool handles.
struct Resources<'a> {
    owner: &'a NodeRuntime,
    terminal: Option<Arc<Terminal>>,
    tools: Option<Arc<NodeToolContext>>,
    lifetime: Option<process::Lifetime>,
    mcp: Option<NodeMcp>,
}

impl<'a> Resources<'a> {
    fn new(owner: &'a NodeRuntime) -> Self {
        Self {
            owner,
            terminal: None,
            tools: None,
            lifetime: None,
            mcp: None,
        }
    }

    async fn close(&mut self) -> Result<()> {
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
