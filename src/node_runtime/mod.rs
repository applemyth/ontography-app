//! One continuing session at a graph node: the implementation its binding
//! names, hosted in a terminal or headless. Terminals are views of this
//! execution; its selected scoped tools are exposed through the node MCP adapter.

pub(crate) mod claude;
pub mod codex;
mod host;
mod process;
mod rpc;

use crate::{
    AppError, Result,
    environment::Environment,
    node_hooks::NodeHooks,
    node_mcp::NodeMcp,
    node_tool::{NodeScope, NodeToolContext},
    process::{Stdin, recover_process},
    terminal::{LaunchSpec, Terminal, TerminalStatus},
    workflow::{
        Implementation,
        components::{AgentConfig, ProgramConfig},
        tasks::RetryLedger,
    },
};
use host::Host;
use ontography::{
    ExecutionContext, ExecutionFailure, ExecutionStop, Payload, SessionHandle, SessionStatus,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    future::Future,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, Weak},
    time::Duration,
};
use tokio::sync::watch;

/// How long a session's own server may take to accept its first request.
const STARTUP: Duration = Duration::from_secs(45);

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

/// Delivers graph work to a started session until the session fails.
type Driver<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

/// The workflow holds this handle; the execution future owns the live resources.
/// Retaining a handle, a terminal view, or a tool context cannot keep a stopped
/// execution's process alive.
pub struct NodeRuntime {
    project: PathBuf,
    directory: PathBuf,
    /// What the session's programs start with, before the node's own variables.
    environment: Environment,
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
        environment: Environment,
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
            environment,
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
        result.map_err(ExecutionFailure::from)
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
        recover_process(&self.directory).await?;
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
        let endpoint = resources.endpoint.insert(self.endpoint()?).clone();
        let mcp = resources
            .mcp
            .insert(NodeMcp::bind(&endpoint, tools.clone(), self.log())?);
        resources.tools = Some(tools.clone());
        let (node, binding) = {
            let scope = self.scope.borrow();
            (scope.node.clone(), scope.binding.clone())
        };
        // The run's environment, and this node's own variables.
        let mut env = self.environment.vars().clone();
        env.extend(mcp.environment());
        env.insert("ONTOGRAPHY_NODE_NAME".into(), node.id.clone());
        env.insert(
            "ONTOGRAPHY_NODE_DIRECTORY".into(),
            self.directory.to_string_lossy().into_owned(),
        );
        env.insert(
            "ONTOGRAPHY_PROJECT".into(),
            self.project.to_string_lossy().into_owned(),
        );
        let terminal = endpoint.join("terminal.sock");
        let driver: Driver<'_> = match binding.implementation {
            Implementation::Program(program) => {
                let spec = program_launch(&program, env, cwd, &node.id)?;
                self.started(
                    resources,
                    Host::terminal(spec, &self.directory, terminal).await?,
                );
                // A program reaches the graph only through its node tools.
                Box::pin(std::future::pending())
            }
            Implementation::Codex(config) => {
                let started = self
                    .start_codex(&node.id, &config, &cwd, env, &mut stop, resources)
                    .await?;
                let Some((plan, mut rpc, thread)) = started else {
                    lock(&self.state).status.state = "stopped";
                    return Ok(());
                };
                let tools = tools.clone();
                Box::pin(async move {
                    // The plan owns the server's socket paths while it runs.
                    let _plan = plan;
                    codex::deliver(&mut rpc, &thread, &tools, |state| {
                        lock(&self.state).status.agent_state = Some(state.into());
                    })
                    .await
                })
            }
            Implementation::Claude(config) => {
                self.start_claude(&node.id, &config, &cwd, env, &tools, resources)
                    .await?
            }
            implementation => {
                return Err(AppError::new(
                    "node_runtime",
                    format!(
                        "A {:?} node runs in the task harness, not a session",
                        implementation.kind()
                    ),
                ));
            }
        };
        lock(&self.state).status.state = "running";
        self.supervise(driver, &mut stop, resources).await
    }

    /// Runs `driver` while watching for a stop, the run closing, and the
    /// session's process ending.
    async fn supervise(
        &self,
        mut driver: Driver<'_>,
        stop: &mut ExecutionStop,
        resources: &mut Resources<'_>,
    ) -> Result<()> {
        let mut tick = tokio::time::interval(Duration::from_millis(25));
        loop {
            tokio::select! {
                result = &mut driver => { return result; }
                () = stop.requested() => {
                    lock(&self.state).status.state = "stopped";
                    return Ok(());
                }
                _ = tick.tick() => {
                    if self.session.status() != SessionStatus::Open {
                        lock(&self.state).status.state = "stopped";
                        return Ok(());
                    }
                    let host = resources.host.as_mut().expect("started session host");
                    if let Some(code) = host.ended().await? {
                        let mut state = lock(&self.state);
                        state.status.terminal = host.terminal_handle().map(|terminal| terminal.status());
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

    /// Starts the Codex server and opens its conversation. `None` means the
    /// execution was asked to stop meanwhile.
    async fn start_codex(
        &self,
        node_id: &str,
        config: &AgentConfig,
        cwd: &Path,
        env: BTreeMap<String, String>,
        stop: &mut ExecutionStop,
        resources: &mut Resources<'_>,
    ) -> Result<Option<(codex::Plan, rpc::Rpc, String)>> {
        let endpoint = self.endpoint()?;
        let plan = codex::Plan::new(
            node_id,
            config,
            &self.directory,
            cwd,
            &endpoint,
            &self.environment,
        )?;
        let mut overrides = resources
            .mcp
            .as_ref()
            .expect("bound node MCP")
            .codex_overrides()?;
        overrides.extend(codex::mcp_overrides(&config.mcp)?);
        let program = Path::new("codex");
        let host = if config.pty {
            let spec = plan.launch(program, &overrides, env);
            Host::terminal(spec, &self.directory, endpoint.join("terminal.sock")).await?
        } else {
            let (host, _) = Host::headless(
                &plan.headless(program, &overrides),
                plan.cwd(),
                &self.directory,
                &env,
                Stdin::Bytes(&[]),
                &self.directory.join("agent.log"),
                false,
            )
            .await?;
            host
        };
        self.started(resources, host);
        let host = resources.host.as_mut().expect("started session host");
        let ready = async {
            let rpc = loop {
                if host.ended().await?.is_some() {
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
            if config.pty {
                plan.attach(&id)?;
            }
            Ok::<_, AppError>((rpc, id))
        };
        let (rpc, id) = tokio::select! {
            () = stop.requested() => { return Ok(None); }
            result = tokio::time::timeout(STARTUP, ready) => {
                result.map_err(|_| AppError::new("codex_startup", "Codex app-server initialization timed out; inspect codex-server.log"))??
            }
        };
        lock(&self.state).status.conversation_id = Some(id.clone());
        Ok(Some((plan, rpc, id)))
    }

    /// Starts Claude on its saved conversation and returns the driver that
    /// delivers graph work to it: typed into its terminal, as its hooks
    /// report it ready, or written to its stream-json input when headless.
    async fn start_claude(
        &self,
        node_id: &str,
        config: &AgentConfig,
        cwd: &Path,
        env: BTreeMap<String, String>,
        tools: &Arc<NodeToolContext>,
        resources: &mut Resources<'_>,
    ) -> Result<Driver<'_>> {
        let plan = claude::Plan::new(node_id, config, &self.directory, cwd, tools.checkouts_dir())?;
        lock(&self.state).status.conversation_id = Some(plan.session_id().into());
        let node_tools = resources.mcp.as_ref().expect("bound node MCP").server()?;
        let program = Path::new("claude");
        let started = plan.started();
        let tools = tools.clone();
        let report = move |status: claude::Status| {
            lock(&self.state).status.agent_state = Some(status.to_string());
        };
        if config.pty {
            let endpoint = self.endpoint()?;
            let (hooks, events) = NodeHooks::bind(&endpoint, self.log())?;
            let spec = plan.session(program, node_tools, &hooks, env)?;
            let host =
                Host::terminal(spec, &self.directory, endpoint.join("terminal.sock")).await?;
            let terminal = host.terminal_handle().expect("terminal host").clone();
            self.started(resources, host);
            // Claude's hook commands report to this listener while it runs.
            resources.hooks = Some(hooks);
            return Ok(Box::pin(async move {
                claude::session::deliver(&*terminal, events, &tools, started, report).await
            }));
        }
        let (host, pipes) = Host::headless(
            &plan.headless(program, node_tools),
            cwd,
            &self.directory,
            &env,
            Stdin::Stream,
            &self.directory.join("agent.log"),
            true,
        )
        .await?;
        self.started(resources, host);
        let (Some(input), Some(output)) = (pipes.input, pipes.output) else {
            return Err(AppError::new(
                "claude_session",
                "Headless Claude has no input or output stream",
            ));
        };
        Ok(Box::pin(async move {
            let output = tokio::io::BufReader::new(output);
            claude::stream::deliver(input, output, &tools, started, report).await
        }))
    }

    /// Records a started session's host, and its terminal if it has one.
    fn started(&self, resources: &mut Resources<'_>, host: Host) {
        let mut state = lock(&self.state);
        if let Some(terminal) = host.terminal_handle() {
            state.terminal = Arc::downgrade(terminal);
            state.status.terminal = Some(terminal.status());
        }
        resources.host = Some(host);
    }

    /// A private directory for this node's sockets. Local socket paths must stay
    /// short enough for Unix sockaddr; each execution gets fresh endpoints and
    /// capabilities in it, including on resume.
    fn endpoint(&self) -> Result<PathBuf> {
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
        Ok(endpoint)
    }

    /// The data directory whose server log records errors of this node's
    /// sockets: the store the run's programs reach.
    fn log(&self) -> Option<&Path> {
        self.environment.get("ONTOGRAPHY_DATA_DIR").map(Path::new)
    }
}

/// An interactive program in the node's terminal, in place of an agent.
fn program_launch(
    program: &ProgramConfig,
    env: BTreeMap<String, String>,
    cwd: PathBuf,
    node_id: &str,
) -> Result<LaunchSpec> {
    let (executable, args) = program
        .argv
        .split_first()
        .ok_or_else(|| AppError::invalid("argv must name a program"))?;
    Ok(LaunchSpec {
        program: executable.into(),
        args: args.to_vec(),
        env,
        cwd,
        rows: 24,
        cols: 80,
        server_id: uuid::Uuid::new_v4().to_string(),
        session_id: node_id.into(),
    })
}

/// This guard also runs when core forcibly aborts the execution future. It stops
/// writers synchronously even if outside code retains terminal/tool handles.
struct Resources<'a> {
    owner: &'a NodeRuntime,
    host: Option<Host>,
    tools: Option<Arc<NodeToolContext>>,
    mcp: Option<NodeMcp>,
    hooks: Option<NodeHooks>,
    /// The directory of this execution's sockets, once created.
    endpoint: Option<PathBuf>,
}

impl<'a> Resources<'a> {
    fn new(owner: &'a NodeRuntime) -> Self {
        Self {
            owner,
            host: None,
            tools: None,
            mcp: None,
            hooks: None,
            endpoint: None,
        }
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(mut mcp) = self.mcp.take() {
            mcp.shutdown().await;
        }
        let stopped = match self.host.as_mut() {
            Some(host) => {
                let result = host.close().await;
                if let Some(terminal) = host.terminal_handle() {
                    lock(&self.owner.state).status.terminal = Some(terminal.status());
                }
                result
            }
            None => Ok(()),
        };
        // Hooks keep reporting until their program has stopped.
        self.hooks.take();
        if let Some(tools) = &self.tools {
            tools.close().await;
        }
        stopped?;
        if self.host.is_some() {
            recover_process(&self.owner.directory).await?;
        }
        Ok(())
    }
}

impl Drop for Resources<'_> {
    fn drop(&mut self) {
        self.mcp.take();
        if let Some(host) = &mut self.host {
            host.request_stop();
        }
        self.hooks.take();
        // Every socket is gone: removed with its owner above, or with the
        // Codex plan when its driver ended. Only an empty directory is
        // removed, so an overlapping execution's sockets keep it.
        if let Some(endpoint) = self.endpoint.take() {
            let _ = std::fs::remove_dir(endpoint);
        }
        let mut state = lock(&self.owner.state);
        if let Some(terminal) = self.host.as_ref().and_then(Host::terminal_handle) {
            state.status.terminal = Some(terminal.status());
        }
        if matches!(state.status.state, "starting" | "running") {
            state.status.state = "stopped";
        }
        state.terminal = Weak::new();
        state.tools = Weak::new();
    }
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
