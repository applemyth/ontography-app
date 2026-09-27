//! Server ownership of core objects. Connections never own graph resources.
use crate::declarations::GraphDeclaration;
use crate::definition::RunDefinition;
use crate::persistence::{Paths, read_json, write_json};
use crate::workspace::{Checkout, WorkspaceStore};
use crate::{AppError, Result, views};
use ontography::{
    ContentId, ExecutionHandle, ExecutionHost, ProposalRuntime, SessionHandle, SessionRewrite,
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
    pub declaration: RunDefinition,
    pub declaration_revision: String,
    pub project: PathBuf,
    pub core_path: PathBuf,
    pub status: String,
    pub created_at: u64,
    #[serde(default)]
    pub checkpoints: BTreeMap<String, Checkpoint>,
    /// Original workflow and input, pinned before creating core storage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow: Option<crate::workflow::runtime::InitialWorkflow>,
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

pub struct RewriteHandle {
    pub plan: SessionRewrite,
}

pub struct LiveRun {
    pub runtime: Option<ProposalRuntime>,
    pub application: Option<ontography::RunningApplication>,
    pub session: SessionHandle,
    pub host: ExecutionHost,
    pub executions: BTreeMap<String, ExecutionHandle>,
    pub rewrites: BTreeMap<String, RewriteHandle>,
    pub checkouts: BTreeMap<String, WorkspaceHandle>,
    pub bindings: Vec<Value>,
    pub suspension_error: Option<AppError>,
    pub workers: BTreeMap<String, crate::workflow::runtime::Worker>,
}

impl LiveRun {
    pub fn new(runtime: ProposalRuntime, session: SessionHandle) -> Self {
        Self {
            host: ExecutionHost::new(session.clone()),
            runtime: Some(runtime),
            application: None,
            session,
            executions: BTreeMap::new(),
            rewrites: BTreeMap::new(),
            checkouts: BTreeMap::new(),
            bindings: Vec::new(),
            suspension_error: None,
            workers: BTreeMap::new(),
        }
    }

    fn from_application(application: ontography::RunningApplication) -> Self {
        let session = application.session().clone();
        let executions = application
            .executions()
            .iter()
            .map(|h| (uuid::Uuid::new_v4().to_string(), h.clone()))
            .collect::<BTreeMap<_, _>>();
        let bindings=executions.iter().map(|(id,h)|json!({"execution_id":id,"node_id":h.node_id(),"lifetime":"application","status":"launched"})).collect();
        Self {
            host: ExecutionHost::new(session.clone()),
            session,
            runtime: None,
            application: Some(application),
            executions,
            bindings,
            rewrites: BTreeMap::new(),
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
        if let Some(application) = self.application.take() {
            application.suspend().await;
        }
        self.executions.clear();
        self.workers.clear();
        for binding in &mut self.bindings {
            binding["status"] = json!("stopped");
        }
    }
}

pub struct ManagedRun {
    pub manifest: RunManifest,
    pub directory: PathBuf,
    pub live: Option<LiveRun>,
    pub registry: Arc<crate::registry::ImplementationRegistry>,
    /// Preserve user checkouts while a faulted core owner is reopened.
    pub recovery_checkouts: BTreeMap<String, WorkspaceHandle>,
}

impl ManagedRun {
    pub fn is_workflow(&self) -> bool {
        self.manifest.workflow.is_some()
    }

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
            && matches!(self.manifest.declaration, RunDefinition::Application(_))
        {
            // Core chooses the application run UUID. Reconcile a crash before the app manifest recorded it.
            let paths = std::fs::read_dir(self.directory.join("application/runs"))?
                .map(|e| e.map(|e| e.path()))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if paths.len() != 1 {
                return Err(AppError::new(
                    "incomplete_creation",
                    "expected exactly one core application directory; inspect recovery files",
                ));
            }
            return Ok(paths[0].clone());
        }
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
        json!({"run_id":self.manifest.run_id,"definition_id":self.manifest.declaration.id(),
            "definition_revision":self.manifest.declaration_revision,"project":self.manifest.project,
            "status":if self.live.as_ref().is_some_and(|l|l.session.status()==SessionStatus::Closed){"closed"}else if self.live.as_ref().is_some_and(|l|l.suspension_error.is_some()){"suspension_failed"}else if self.live.is_some(){"active"}else if self.manifest.status=="active" || self.manifest.status=="suspending"{"recoverable"}else{&self.manifest.status},
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
            result["bindings"] = json!(live.bindings);
            result["suspension_error"] = json!(live.suspension_error);
            result["resources"] = json!({"rewrites":live.rewrites.keys().collect::<Vec<_>>(),"checkouts":live.checkouts.iter().map(|(id,h)|json!({"checkout_id":id,"path":h.checkout.path(),"base":h.base})).collect::<Vec<_>>()});
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
        if self.live.is_some() {
            if self.is_workflow() {
                return crate::workflow::runtime::resume(self).await;
            }
            return Ok(());
        }
        self.validate_metadata()?;
        if let RunDefinition::Application(declaration) = &self.manifest.declaration {
            let compiled = declaration.compile(&self.registry, &self.manifest.project)?;
            match compiled.application.resume(self.core_path()?).await {
                Ok(application) => {
                    self.live = Some(LiveRun::from_application(application));
                    self.manifest.status = "active".into();
                    self.save()
                }
                Err(ontography::ApplicationStartError::NotResumable(_)) => self.open(),
                Err(error) => Err(AppError::new(
                    "application_resume_failed",
                    error.to_string(),
                )),
            }
        } else {
            self.open()?;
            self.launch_bindings().await
        }
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
        if self.manifest.declaration.fingerprint()? != self.manifest.declaration_revision {
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
        let compiled = self
            .manifest
            .declaration
            .compile(&self.registry, &self.manifest.project)?;
        if !self.is_workflow() {
            self.registry.validate_bindings(
                self.manifest.declaration.execution_bindings(),
                &compiled.kernel,
            )?;
        }
        let runtime = ProposalRuntime::with_grammar(compiled.kernel, compiled.grammar);
        let session = runtime
            .open_persistent(self.core_path()?)
            .map_err(AppError::core)?;
        self.live = Some(LiveRun::new(runtime, session));
        self.live
            .as_mut()
            .expect("opened")
            .checkouts
            .append(&mut self.recovery_checkouts);
        self.manifest.status = "active".into();
        self.save()
    }

    async fn launch_bindings(&mut self) -> Result<()> {
        if self.is_workflow() {
            return crate::workflow::runtime::resume(self).await;
        }
        let launched = self
            .registry
            .launch_bindings(
                self.manifest.declaration.execution_bindings(),
                &self.live()?.host,
            )
            .await?;
        let live = self.live_mut()?;
        live.executions.extend(launched.executions);
        live.bindings = launched.reports;
        Ok(())
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
        self.manifest.status = "suspending".into();
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
        live.rewrites.clear();
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
    pub registry: Arc<crate::registry::ImplementationRegistry>,
    pub recovery_errors: BTreeMap<String, AppError>,
    pub sessions: crate::sessions::Sessions,
}

/// A crash may leave only the reserved directory. Nonempty unknown stores are never overwritten.
fn create_reserved_directory(directory: &std::path::Path) -> Result<()> {
    match std::fs::create_dir(directory) {
        Ok(()) => Ok(()),
        Err(error)
            if error.kind() == std::io::ErrorKind::AlreadyExists
                && directory.is_dir()
                && std::fs::read_dir(directory)?.next().is_none() =>
        {
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

impl Service {
    pub fn new(paths: Paths) -> Result<Self> {
        Self::with_registry(
            paths,
            Arc::new(crate::registry::ImplementationRegistry::default()),
        )
    }

    pub fn with_registry(
        paths: Paths,
        registry: Arc<crate::registry::ImplementationRegistry>,
    ) -> Result<Self> {
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
                            registry: registry.clone(),
                            recovery_checkouts: BTreeMap::new(),
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
            registry,
            recovery_errors,
            sessions,
        })
    }

    pub async fn run(&self, id: &str) -> Result<Arc<Mutex<ManagedRun>>> {
        self.runs
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| AppError::new("not_found", format!("run {id} does not exist")))
    }

    pub async fn start(&self, declaration: GraphDeclaration, project: PathBuf) -> Result<Value> {
        self.start_reserved(&uuid::Uuid::new_v4().to_string(), declaration, project)
            .await
    }

    /// Reconcile a durably reserved identity without creating another run on retry.
    pub async fn start_reserved(
        &self,
        id: &str,
        declaration: GraphDeclaration,
        project: PathBuf,
    ) -> Result<Value> {
        self.start_reserved_inner(id, declaration, project, None)
            .await
    }

    pub async fn start_workflow_reserved(
        &self,
        id: &str,
        declaration: GraphDeclaration,
        project: PathBuf,
        workflow: crate::workflow::runtime::InitialWorkflow,
    ) -> Result<Value> {
        self.start_reserved_inner(id, declaration, project, Some(workflow))
            .await
    }

    async fn start_reserved_inner(
        &self,
        id: &str,
        declaration: GraphDeclaration,
        project: PathBuf,
        workflow: Option<crate::workflow::runtime::InitialWorkflow>,
    ) -> Result<Value> {
        let compiled = declaration.compile().map_err(AppError::core)?;
        if workflow.is_none() {
            self.registry
                .validate_bindings(&declaration.execution_bindings, &compiled.kernel)?;
        }
        let declaration_revision = declaration.fingerprint().map_err(AppError::core)?;
        let project = std::fs::canonicalize(project)?;
        if !project.is_dir() {
            return Err(AppError::invalid("project must be a directory"));
        }
        if let Some(run) = self.runs.lock().await.get(id).cloned() {
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
            if run.live.is_none()
                && !run.directory.join("core").exists()
                && run.manifest.status == "creating"
            {
                let runtime = ProposalRuntime::with_grammar(compiled.kernel, compiled.grammar);
                let session = runtime
                    .create_persistent(run.directory.join("core"))
                    .map_err(AppError::core)?;
                run.live = Some(LiveRun::new(runtime, session));
                run.manifest.status = "active".into();
                run.save()?;
                run.launch_bindings().await?;
            } else if run.live.is_none() {
                run.resume().await?;
            } else if run.is_workflow() {
                let workflow = crate::workflow::runtime::load(&run)?;
                if workflow.pending.is_none() {
                    crate::workflow::runtime::reconcile(&mut run, &workflow, false).await?;
                }
            }
            return run.inspect(100).await;
        }
        let directory = self.paths.run(id)?;
        create_reserved_directory(&directory)?;
        let manifest = RunManifest {
            version: 1,
            run_id: id.into(),
            core_version: ontography::VERSION.into(),
            core_build: crate::CORE_BUILD.into(),
            declaration: declaration.into(),
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
            registry: self.registry.clone(),
            recovery_checkouts: BTreeMap::new(),
        }));
        self.runs.lock().await.insert(id.into(), run.clone());
        let mut run = run.lock().await;
        let runtime = ProposalRuntime::with_grammar(compiled.kernel, compiled.grammar);
        let session = runtime
            .create_persistent(run.directory.join("core"))
            .map_err(|e| {
                AppError::core(e).details(json!({"run_id":id,"status":"creation_incomplete"}))
            })?;
        run.live = Some(LiveRun::new(runtime, session));
        run.manifest.status = "active".into();
        run.save()?;
        run.launch_bindings().await?;
        let result = run.inspect(100).await?;
        Ok(result)
    }

    pub async fn shutdown(&self) -> Result<()> {
        let runs = self.runs.lock().await.values().cloned().collect::<Vec<_>>();
        for run in runs {
            run.lock().await.suspend(false).await?;
        }
        Ok(())
    }

    pub async fn start_application(
        &self,
        declaration: crate::application::ApplicationDeclaration,
        project: PathBuf,
        input: ontography::Payload,
    ) -> Result<Value> {
        self.start_application_reserved(
            &uuid::Uuid::new_v4().to_string(),
            declaration,
            project,
            input,
        )
        .await
    }

    pub async fn start_application_reserved(
        &self,
        id: &str,
        declaration: crate::application::ApplicationDeclaration,
        project: PathBuf,
        input: ontography::Payload,
    ) -> Result<Value> {
        let project = std::fs::canonicalize(project)?;
        if !project.is_dir() {
            return Err(AppError::invalid("project must be a directory"));
        }
        let compiled = declaration.compile(&self.registry, &project)?;
        let declaration_revision = declaration.fingerprint()?;
        if let Some(run) = self.runs.lock().await.get(id).cloned() {
            let mut run = run.lock().await;
            if run.manifest.declaration_revision != declaration_revision
                || run.manifest.project != project
            {
                return Err(AppError::new(
                    "initialization_conflict",
                    "reserved application identity has different initialization data",
                ));
            }
            let application_runs = run.directory.join("application/runs");
            let has_core_run =
                application_runs.exists() && std::fs::read_dir(&application_runs)?.next().is_some();
            if run.live.is_none() && run.manifest.status == "creating" && !has_core_run {
                let application = compiled
                    .application
                    .start_in(run.directory.join("application"), input)
                    .await
                    .map_err(|error| {
                        AppError::new("application_start_failed", error.to_string())
                    })?;
                run.manifest.core_path = application
                    .run_path()
                    .expect("persistent application has a path")
                    .strip_prefix(&run.directory)
                    .map_err(AppError::core)?
                    .to_path_buf();
                run.live = Some(LiveRun::from_application(application));
                run.manifest.status = "active".into();
                run.save()?;
            } else if run.live.is_none() {
                // Existing applications resume without replaying their initial input.
                run.resume().await?;
            }
            return run.inspect(100).await;
        }
        let directory = self.paths.run(id)?;
        create_reserved_directory(&directory)?;
        let manifest = RunManifest {
            version: 1,
            run_id: id.into(),
            core_version: ontography::VERSION.into(),
            core_build: crate::CORE_BUILD.into(),
            declaration: RunDefinition::Application(declaration),
            declaration_revision,
            project,
            core_path: PathBuf::new(),
            status: "creating".into(),
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            checkpoints: BTreeMap::new(),
            workflow: None,
        };
        write_json(&directory.join("manifest.json"), &manifest)?;
        let run = Arc::new(Mutex::new(ManagedRun {
            manifest,
            directory,
            live: None,
            registry: self.registry.clone(),
            recovery_checkouts: BTreeMap::new(),
        }));
        self.runs.lock().await.insert(id.into(), run.clone());
        let mut run = run.lock().await;
        let application = compiled
            .application
            .start_in(run.directory.join("application"), input)
            .await
            .map_err(|e| {
                AppError::new("application_start_failed", e.to_string())
                    .details(json!({"run_id":id,"status":"creation_incomplete"}))
            })?;
        let core_path = application
            .run_path()
            .expect("start_in creates a persistent run")
            .strip_prefix(&run.directory)
            .map_err(AppError::core)?
            .to_path_buf();
        run.live = Some(LiveRun::from_application(application));
        run.manifest.core_path = core_path;
        run.manifest.status = "active".into();
        run.save()?;
        run.inspect(100).await
    }
}
