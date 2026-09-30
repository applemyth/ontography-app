//! Server ownership of core objects. Connections never own graph resources.
use crate::declarations::GraphDeclaration;
use crate::environment::Environment;
use crate::persistence::{Paths, read_json, write_json};
use crate::workflow::runtime::InitialWorkflow;
use crate::workspace::{Checkout, WorkspaceStore};
use crate::{AppError, Result, views};
use ontography::{
    ContentId, ExecutionHandle, ExecutionHost, Kernel, ProposalRuntime, SessionHandle,
    SessionStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunManifest {
    pub version: u32,
    pub run_id: String,
    pub core_version: String,
    #[serde(default)]
    pub core_build: String,
    pub declaration: GraphDeclaration,
    pub declaration_revision: String,
    pub project: PathBuf,
    pub core_path: PathBuf,
    pub status: String,
    pub created_at: u64,
    #[serde(default)]
    pub checkpoints: BTreeMap<String, Checkpoint>,
    /// Original workflow and input, pinned before creating core storage.
    pub workflow: InitialWorkflow,
}

impl RunManifest {
    /// The status of this run while it is not open. A run a stopped server
    /// left active or suspending can be recovered.
    pub fn resting_status(&self) -> &str {
        if matches!(self.status.as_str(), "active" | "suspending") {
            "recoverable"
        } else {
            &self.status
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    pub base: ContentId,
    pub root: ContentId,
    pub dependencies: Vec<ContentId>,
}

pub struct WorkspaceHandle {
    pub checkout: Checkout,
    pub base: ContentId,
}

pub struct LiveRun {
    /// Keeps the run's proposal runtime alive for its session.
    pub runtime: ProposalRuntime,
    pub session: SessionHandle,
    pub host: ExecutionHost,
    pub executions: BTreeMap<String, ExecutionHandle>,
    pub checkouts: BTreeMap<String, WorkspaceHandle>,
    pub suspension_error: Option<AppError>,
    pub workers: BTreeMap<String, crate::workflow::runtime::Worker>,
}

impl LiveRun {
    pub fn new(runtime: ProposalRuntime, session: SessionHandle) -> Self {
        Self {
            host: ExecutionHost::new(session.clone()),
            runtime,
            session,
            executions: BTreeMap::new(),
            checkouts: BTreeMap::new(),
            suspension_error: None,
            workers: BTreeMap::new(),
        }
    }

    pub async fn stop_executions(&mut self) {
        for handle in self.executions.values() {
            handle.request_stop();
        }
        self.host.stop_accepting();
        self.host.request_stop();
        if tokio::time::timeout(Duration::from_secs(3), self.host.wait_idle())
            .await
            .is_err()
        {
            self.host.abort_all();
            self.host.wait_idle().await;
        }
        self.executions.clear();
        self.workers.clear();
    }
}

pub struct ManagedRun {
    pub manifest: RunManifest,
    pub directory: PathBuf,
    pub live: Option<LiveRun>,
    /// Preserve user checkouts while a faulted core owner is reopened.
    pub recovery_checkouts: BTreeMap<String, WorkspaceHandle>,
    /// What its programs start with: its session's environment, or the
    /// unowned one for a run no session owns. Kept in memory only.
    pub environment: Environment,
}

impl ManagedRun {
    pub fn live(&self) -> Result<&LiveRun> {
        self.live.as_ref().ok_or_else(|| {
            AppError::new(
                "run_suspended",
                "resume the run before accessing live resources",
            )
        })
    }

    pub fn live_mut(&mut self) -> Result<&mut LiveRun> {
        self.live.as_mut().ok_or_else(|| {
            AppError::new(
                "run_suspended",
                "resume the run before accessing live resources",
            )
        })
    }

    pub fn workspace(&self) -> Result<PathBuf> {
        Ok(self.directory.join("workspace-cache"))
    }

    pub fn save(&self) -> Result<()> {
        write_json(&self.directory.join("manifest.json"), &self.manifest)
    }

    pub fn core_path(&self) -> Result<PathBuf> {
        if self.manifest.core_path.as_os_str().is_empty()
            || self
                .manifest
                .core_path
                .components()
                .any(|c| !matches!(c, std::path::Component::Normal(_)))
        {
            return Err(AppError::new(
                "invalid_run",
                "core_path must stay within its managed run directory",
            ));
        }
        Ok(self.directory.join(&self.manifest.core_path))
    }

    pub fn summary(&self) -> Value {
        json!({"run_id":self.manifest.run_id,"definition_id":self.manifest.declaration.id,
            "definition_revision":self.manifest.declaration_revision,"project":self.manifest.project,
            "status":if self.live.as_ref().is_some_and(|l|l.session.status()==SessionStatus::Closed){"closed"}else if self.live.as_ref().is_some_and(|l|l.suspension_error.is_some()){"suspension_failed"}else if self.live.is_some(){"active"}else{self.manifest.resting_status()},
            "admission":self.live.as_ref().map(|l|views::status(l.session.status())),
            "created_at":self.manifest.created_at.to_string(),"checkpoints":self.manifest.checkpoints})
    }

    pub async fn inspect(&self, limit: usize) -> Result<Value> {
        let mut result = self.summary();
        if let Some(live) = &self.live {
            let overview = live
                .session
                .frontier_overview(limit)
                .await
                .map_err(AppError::core)?;
            let counts: BTreeMap<_, _> = overview
                .counts()
                .iter()
                .map(|(node, c)| {
                    (
                        node.as_ref(),
                        json!({"received":c.received(),"outbound":c.outbound()}),
                    )
                })
                .collect();
            result["revision"] = json!(overview.revision().to_string());
            result["graph"] = views::graph(overview.kernel());
            result["frontier"] = json!({"counts":counts,
                "received":overview.received().iter().map(|(id,p)|views::package(*id,p)).collect::<Vec<_>>(),
                "outbound":overview.outbound().iter().map(|(id,p)|views::package(*id,p)).collect::<Vec<_>>()});
            result["executions"] = json!(live.executions.iter().map(|(id,h)|json!({"execution_id":id,"node_id":h.node_id(),"status":format!("{:?}",h.status()).to_lowercase()})).collect::<Vec<_>>());
            result["suspension_error"] = json!(live.suspension_error);
            result["resources"] = json!({"checkouts":live.checkouts.iter().map(|(id,h)|json!({"checkout_id":id,"path":h.checkout.path(),"base":h.base})).collect::<Vec<_>>()});
        }
        Ok(result)
    }

    pub async fn resume(&mut self) -> Result<()> {
        if self
            .live
            .as_ref()
            .is_some_and(|live| live.session.status() == SessionStatus::Faulted)
        {
            let live = self.live.as_mut().expect("faulted owner exists");
            live.stop_executions().await;
            self.recovery_checkouts.append(&mut live.checkouts);
            self.live = None;
        }
        self.open()?;
        if self.live()?.session.status() == SessionStatus::Closed {
            // A closed run stays readable; nothing runs in it again.
            return Ok(());
        }
        crate::workflow::runtime::resume(self).await
    }

    fn validate_metadata(&self) -> Result<()> {
        if self.manifest.version != 1
            || self.manifest.core_version != ontography::VERSION
            || self.manifest.core_build != crate::CORE_BUILD
        {
            return Err(AppError::new(
                "incompatible_run",
                "run format/core build does not match this installation",
            ));
        }
        if self
            .manifest
            .declaration
            .fingerprint()
            .map_err(AppError::core)?
            != self.manifest.declaration_revision
        {
            return Err(AppError::new(
                "incompatible_run",
                "saved definition fingerprint mismatch",
            ));
        }
        Ok(())
    }

    /// Open storage for inspection or closing, without launching any executable.
    fn open(&mut self) -> Result<()> {
        if self.live.is_some() {
            return Ok(());
        }
        self.validate_metadata()?;
        let kernel = self
            .manifest
            .declaration
            .compile()
            .map_err(AppError::core)?;
        let runtime = runtime(kernel);
        let session = runtime
            .open_persistent(self.core_path()?)
            .map_err(AppError::core)?;
        // A closed run stays closed; only its reads come back.
        let status = if session.status() == SessionStatus::Closed {
            "closed"
        } else {
            "active"
        };
        self.live = Some(LiveRun::new(runtime, session));
        self.live
            .as_mut()
            .expect("opened")
            .checkouts
            .append(&mut self.recovery_checkouts);
        if self.manifest.status == status {
            return Ok(());
        }
        self.manifest.status = status.into();
        self.save()
    }

    /// Called while the run's exclusive tool mutex is held; no connection can add resources.
    pub async fn suspend(&mut self, close: bool) -> Result<()> {
        let result = self.suspend_inner(close).await;
        if let Err(error) = &result
            && let Some(live) = &mut self.live
        {
            // Writers were already stopped; let the user inspect, repair and retry.
            // Future explicit launches use a fresh host; no work restarts implicitly.
            live.host = ExecutionHost::new(live.session.clone());
            live.suspension_error = Some(error.clone());
        }
        result
    }

    async fn suspend_inner(&mut self, close: bool) -> Result<()> {
        if self.live.is_none() {
            if !close {
                return Ok(());
            }
            self.open()?;
        }
        let live = self.live.as_mut().expect("resume established ownership");
        live.stop_executions().await;
        let content = live.session.content_store().await.map_err(AppError::core)?;
        let workspace =
            WorkspaceStore::new(content.clone(), self.directory.join("workspace-cache"));
        let mut checkpoints = self.manifest.checkpoints.clone();
        for (id, handle) in &live.checkouts {
            let captured = workspace
                .capture(handle.checkout.path(), handle.base)
                .await
                .map_err(AppError::core)?;
            let dependencies = captured.dependencies();
            let staged = content.stage_imports();
            staged
                .protect(&dependencies)
                .await
                .map_err(AppError::core)?;
            staged.retain().await.map_err(AppError::core)?;
            checkpoints.insert(
                id.clone(),
                Checkpoint {
                    base: handle.base,
                    root: captured.root(),
                    dependencies,
                },
            );
        }
        self.manifest.checkpoints = checkpoints;
        // A closed run is never recoverable, even while it is suspended.
        self.manifest.status = if live.session.status() == SessionStatus::Closed {
            "closed"
        } else {
            "suspending"
        }
        .into();
        write_json(&self.directory.join("manifest.json"), &self.manifest)?;
        if close {
            live.session.close().await;
        }
        self.manifest.status = if live.session.status() == SessionStatus::Closed {
            "closed"
        } else {
            "suspended"
        }
        .into();
        write_json(&self.directory.join("manifest.json"), &self.manifest)?;
        let checkouts = std::mem::take(&mut live.checkouts);
        for (_, handle) in checkouts {
            handle.checkout.remove().await.map_err(AppError::core)?;
        }
        drop(workspace);
        drop(content);
        self.live = None;
        Ok(())
    }
}

pub struct Service {
    pub paths: Paths,
    pub server_id: String,
    pub runs: Mutex<BTreeMap<String, Arc<Mutex<ManagedRun>>>>,
    /// Run directories that could not be loaded at startup, by name. A start
    /// that creates its run in one clears its error.
    pub recovery_errors: std::sync::Mutex<BTreeMap<String, AppError>>,
    pub sessions: crate::sessions::Sessions,
    /// What work no session owns starts with: the environment of the latest
    /// command that changed such work, or the server's own before any.
    unowned: std::sync::Mutex<Environment>,
}

/// `environment` as the programs of this server's store start with it: they
/// reach the same store with the `ontography` command.
fn programs(paths: &Paths, environment: Environment) -> Environment {
    environment.with("ONTOGRAPHY_DATA_DIR", paths.root.to_string_lossy())
}

/// The runtime for a compiled run: it accepts only its document editor's edits.
fn runtime(kernel: Arc<Kernel>) -> ProposalRuntime {
    ProposalRuntime::with_policy(kernel, crate::workflow::edit::policy())
}

/// A crash may leave only the reserved directory, perhaps holding the
/// temporaries of an unfinished manifest write, which are removed.
/// Nonempty unknown stores are never overwritten.
fn create_reserved_directory(directory: &std::path::Path) -> Result<()> {
    match std::fs::create_dir(directory) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && directory.is_dir() => {
            let mut temporaries = Vec::new();
            for entry in std::fs::read_dir(directory)? {
                let entry = entry?;
                // `write_json` writes `.<uuid>.tmp` beside its target.
                let temporary = entry.file_type()?.is_file()
                    && entry
                        .file_name()
                        .to_str()
                        .is_some_and(|name| name.starts_with('.') && name.ends_with(".tmp"));
                if !temporary {
                    return Err(error.into());
                }
                temporaries.push(entry.path());
            }
            for temporary in temporaries {
                std::fs::remove_file(temporary)?;
            }
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

/// Whether `name` is the temporary `write_json` writes beside its target.
fn is_metadata_temporary(name: &std::ffi::OsStr) -> bool {
    name.to_str()
        .and_then(|name| name.strip_prefix('.')?.strip_suffix(".tmp"))
        .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok())
}

/// A metadata write cut short by a crash leaves its temporary beside the file
/// it was replacing. Only the server that holds the data directory writes
/// them, so any found as it starts are leftovers. Only the app's own
/// metadata directories are swept: a node's working directory holds whatever
/// its program made.
fn remove_leftover_temporaries(root: &std::path::Path) {
    let entries = |directory: &std::path::Path| {
        std::fs::read_dir(directory)
            .into_iter()
            .flatten()
            .flatten()
            .collect::<Vec<_>>()
    };
    let mut directories = vec![
        root.join("definitions"),
        root.join("definitions/workflows"),
        root.join("sessions"),
    ];
    for session in entries(&root.join("sessions")) {
        directories.push(session.path());
    }
    for run in entries(&root.join("runs")) {
        directories.push(run.path().join("edit-plans"));
        directories.extend(entries(&run.path().join("nodes")).iter().map(|n| n.path()));
        directories.push(run.path());
    }
    for directory in directories {
        for entry in entries(&directory) {
            if is_metadata_temporary(&entry.file_name())
                && entry.file_type().is_ok_and(|kind| kind.is_file())
            {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
}

impl Service {
    pub fn new(paths: Paths) -> Result<Self> {
        remove_leftover_temporaries(&paths.root);
        let environment = programs(&paths, Environment::current());
        let mut runs = BTreeMap::new();
        let mut recovery_errors = BTreeMap::new();
        for entry in std::fs::read_dir(paths.root.join("runs"))? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let loaded = (|| -> Result<RunManifest> {
                let manifest: RunManifest = read_json(&entry.path().join("manifest.json"))?;
                paths.run(&manifest.run_id)?;
                if entry.file_name() != std::ffi::OsStr::new(&manifest.run_id) {
                    return Err(AppError::new(
                        "invalid_run",
                        "run manifest identity disagrees with directory",
                    ));
                }
                Ok(manifest)
            })();
            match loaded {
                Ok(manifest) => {
                    runs.insert(
                        manifest.run_id.clone(),
                        Arc::new(Mutex::new(ManagedRun {
                            manifest,
                            directory: entry.path(),
                            live: None,
                            recovery_checkouts: BTreeMap::new(),
                            environment: environment.clone(),
                        })),
                    );
                }
                Err(error) => {
                    recovery_errors.insert(entry.file_name().to_string_lossy().into_owned(), error);
                }
            }
        }
        let sessions = crate::sessions::Sessions::open(&paths)?;
        Ok(Self {
            paths,
            server_id: uuid::Uuid::new_v4().to_string(),
            runs: Mutex::new(runs),
            recovery_errors: std::sync::Mutex::new(recovery_errors),
            sessions,
            unowned: std::sync::Mutex::new(environment),
        })
    }

    /// The environment work no session owns starts with.
    pub fn environment(&self) -> Environment {
        self.unowned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// A command that changes work no session owns gives it its environment.
    pub fn set_environment(&self, environment: Environment) {
        *self
            .unowned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            programs(&self.paths, environment);
    }

    /// Give a session `environment` for its programs; see
    /// [`Sessions::offer_environment`](crate::sessions::Sessions::offer_environment).
    pub async fn offer_environment(&self, id: &str, environment: Environment, activating: bool) {
        self.sessions
            .offer_environment(id, programs(&self.paths, environment), activating)
            .await;
    }

    /// What a run's programs start with: its session's environment, or the
    /// unowned one.
    pub async fn run_environment(&self, run_id: &str) -> Environment {
        match self.sessions.owner(run_id).await {
            Some(session) => self.session_environment(&session).await,
            None => self.environment(),
        }
    }

    /// Whether any run is open. Never waits: a run busy with an operation
    /// counts as open.
    pub fn has_live_runs(&self) -> bool {
        self.runs.try_lock().map_or(true, |runs| {
            runs.values()
                .any(|run| run.try_lock().map_or(true, |run| run.live.is_some()))
        })
    }

    /// The environment a session's programs start with: the one a command
    /// gave it, or the unowned one if none has.
    pub async fn session_environment(&self, id: &str) -> Environment {
        self.sessions
            .environment(id)
            .await
            .unwrap_or_else(|| self.environment())
    }

    pub async fn run(&self, id: &str) -> Result<Arc<Mutex<ManagedRun>>> {
        self.runs
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| AppError::new("not_found", format!("run {id} does not exist")))
    }

    /// Create the run reserved as `id`, or reconcile it on retry without
    /// creating another. Its programs start with `environment`.
    pub async fn start_reserved(
        &self,
        id: &str,
        declaration: GraphDeclaration,
        project: PathBuf,
        workflow: InitialWorkflow,
        environment: Environment,
    ) -> Result<Value> {
        let kernel = declaration.compile().map_err(AppError::core)?;
        let declaration_revision = declaration.fingerprint().map_err(AppError::core)?;
        let project = std::fs::canonicalize(project)?;
        if !project.is_dir() {
            return Err(AppError::invalid("project must be a directory"));
        }
        let directory = self.paths.run(id)?;
        let mut runs = self.runs.lock().await;
        if let Some(run) = runs.get(id).cloned() {
            // Other runs stay reachable while this one is reconciled.
            drop(runs);
            let mut run = run.lock().await;
            if run.manifest.declaration_revision != declaration_revision
                || run.manifest.project != project
                || serde_json::to_value(&run.manifest.workflow)? != serde_json::to_value(&workflow)?
            {
                return Err(AppError::new(
                    "initialization_conflict",
                    "reserved run identity has different initialization data",
                ));
            }
            run.environment = environment;
            if run.live.is_none()
                && !run.directory.join("core").exists()
                && run.manifest.status == "creating"
            {
                let runtime = runtime(kernel);
                let session = runtime
                    .create_persistent(run.directory.join("core"))
                    .map_err(AppError::core)?;
                run.live = Some(LiveRun::new(runtime, session));
                run.manifest.status = "active".into();
                run.save()?;
                crate::workflow::runtime::resume(&mut run).await?;
            } else if run.live.is_none() {
                run.resume().await?;
            } else {
                let workflow = crate::workflow::runtime::load(&run)?;
                if workflow.pending.is_none() {
                    crate::workflow::runtime::reconcile(&mut run, &workflow, false).await?;
                }
            }
            return run.inspect(100).await;
        }
        // Reserve the id before releasing the map: a concurrent start with it
        // then waits for this run and reconciles it as a retry.
        create_reserved_directory(&directory)?;
        let manifest = RunManifest {
            version: 1,
            run_id: id.into(),
            core_version: ontography::VERSION.into(),
            core_build: crate::CORE_BUILD.into(),
            declaration,
            declaration_revision,
            project,
            core_path: "core".into(),
            status: "creating".into(),
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            checkpoints: BTreeMap::new(),
            workflow,
        };
        write_json(&directory.join("manifest.json"), &manifest)?;
        let run = Arc::new(Mutex::new(ManagedRun {
            manifest,
            directory,
            live: None,
            recovery_checkouts: BTreeMap::new(),
            environment,
        }));
        runs.insert(id.into(), run.clone());
        // Its directory holds a run now, whatever startup found there.
        self.recovery_errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(id);
        // Nothing else can reach the run before the map is released.
        let mut run = run.lock().await;
        drop(runs);
        let runtime = runtime(kernel);
        let session = runtime
            .create_persistent(run.directory.join("core"))
            .map_err(|e| {
                AppError::core(e).details(json!({"run_id":id,"status":"creation_incomplete"}))
            })?;
        run.live = Some(LiveRun::new(runtime, session));
        run.manifest.status = "active".into();
        run.save()?;
        crate::workflow::runtime::resume(&mut run).await?;
        run.inspect(100).await
    }

    /// Suspend every run. A run that fails does not keep the others live;
    /// the error names each run that failed.
    pub async fn shutdown(&self) -> Result<()> {
        let runs = self.runs.lock().await.values().cloned().collect::<Vec<_>>();
        let count = runs.len();
        let mut failed = BTreeMap::new();
        for run in runs {
            let mut run = run.lock().await;
            if let Err(error) = run.suspend(false).await {
                failed.insert(run.manifest.run_id.clone(), error);
            }
        }
        if failed.is_empty() {
            return Ok(());
        }
        let named = failed
            .iter()
            .map(|(id, error)| format!("{id} ({error})"))
            .collect::<Vec<_>>()
            .join("; ");
        Err(AppError::new(
            "shutdown_incomplete",
            format!(
                "{} of {count} runs could not be suspended: {named}",
                failed.len()
            ),
        )
        .details(json!({"runs":failed})))
    }
}
