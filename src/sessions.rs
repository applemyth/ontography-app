//! Durable application sessions. Pi conversations and core runs keep their native formats.
use crate::{
    AppError, Result,
    catalog::Operation,
    environment::Environment,
    persistence::{Paths, read_json, write_json},
    state::Service,
    views,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, RwLock};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Active,
    Suspending,
    Suspended,
    Closing,
    Closed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Conversation {
    pub conversation_id: String,
    pub path: Option<PathBuf>,
    pub materialized: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PiState {
    pub active_conversation_id: String,
    pub conversations: BTreeMap<String, Conversation>,
    pub preferences: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphInitialization {
    pub run_id: String,
    pub args: Value,
    pub definition: crate::declarations::GraphDeclaration,
    pub workflow: crate::workflow::runtime::InitialWorkflow,
}

/// Keep old initialization records readable without interpreting or replaying them.
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum SavedGraphInitialization {
    Current(Box<GraphInitialization>),
    Legacy(LegacyGraphInitialization),
}

impl<'de> Deserialize<'de> for SavedGraphInitialization {
    /// Only legacy records name an `operation`. Choosing the format by it
    /// reports that format's own parse error, not serde's untagged summary.
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        let saved = if value.get("operation").is_some() {
            serde_json::from_value(value).map(Self::Legacy)
        } else {
            serde_json::from_value(value).map(|intent| Self::Current(Box::new(intent)))
        };
        saved.map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyGraphInitialization {
    run_id: String,
    operation: LegacyOperation,
    args: Value,
    definition: LegacyDefinition,
    input: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    workflow: Option<BTreeMap<String, Value>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum LegacyOperation {
    #[serde(rename = "flow.start")]
    Flow,
    #[serde(rename = "run.start")]
    Run,
    #[serde(rename = "project.start")]
    Project,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "declaration",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum LegacyDefinition {
    Logical(BTreeMap<String, Value>),
    Application(BTreeMap<String, Value>),
}

impl From<GraphInitialization> for SavedGraphInitialization {
    fn from(intent: GraphInitialization) -> Self {
        Self::Current(Box::new(intent))
    }
}

impl SavedGraphInitialization {
    pub fn run_id(&self) -> &str {
        match self {
            Self::Current(intent) => &intent.run_id,
            Self::Legacy(intent) => &intent.run_id,
        }
    }

    fn current(&self) -> Result<&GraphInitialization> {
        match self {
            Self::Current(intent) => Ok(intent),
            Self::Legacy(legacy) => Err(legacy.unavailable()),
        }
    }
}

impl LegacyGraphInitialization {
    fn unavailable(&self) -> AppError {
        AppError::new(
            "graph_unavailable",
            "this saved graph initialization uses an unsupported older format; its Pi conversations remain available",
        )
        .details(json!({"run_id":self.run_id}))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionRecord {
    pub version: u32,
    pub session_id: String,
    pub name: String,
    pub project: PathBuf,
    pub status: SessionStatus,
    pub run_id: Option<String>,
    pub graph_initialization: Option<SavedGraphInitialization>,
    pub pi: PiState,
    pub created_at: u64,
    pub updated_at: u64,
}

impl SessionRecord {
    /// The run this session reserved or bound, if any.
    fn claimed_run(&self) -> Option<&str> {
        self.run_id.as_deref().or_else(|| {
            self.graph_initialization
                .as_ref()
                .map(SavedGraphInitialization::run_id)
        })
    }

    /// A legacy record keeps its reservation, but is never interpreted, so
    /// it starts, resumes, and stops no run.
    fn legacy(&self) -> Option<&LegacyGraphInitialization> {
        match &self.graph_initialization {
            Some(SavedGraphInitialization::Legacy(legacy)) => Some(legacy),
            _ => None,
        }
    }
}

pub struct Sessions {
    root: PathBuf,
    records: Mutex<BTreeMap<String, Arc<Mutex<SessionRecord>>>>,
    selected: Mutex<Option<String>>,
    claims: Mutex<BTreeMap<String, String>>,
    admission: Mutex<BTreeMap<String, Arc<RwLock<()>>>>,
    /// Each active session's environment, from the client that activated it.
    /// Kept in memory only: environments often hold credentials.
    environments: Mutex<BTreeMap<String, Environment>>,
    /// Why each session's graph last failed to start or resume, until it
    /// works again. Kept in memory only: a restart retries.
    graph_errors: Mutex<BTreeMap<String, AppError>>,
    pub recovery_errors: BTreeMap<String, AppError>,
}

#[derive(Serialize, Deserialize)]
struct Selection {
    session_id: Option<String>,
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn uuid(value: &str, kind: &str) -> Result<()> {
    uuid::Uuid::parse_str(value)
        .map(|_| ())
        .map_err(|_| AppError::invalid(format!("{kind} must be a UUID")))
}

impl Sessions {
    pub fn open(paths: &Paths) -> Result<Self> {
        let root = paths.root.join("sessions");
        std::fs::create_dir_all(&root)?;
        let mut records = BTreeMap::new();
        let mut selectable = BTreeSet::new();
        let mut claims = BTreeMap::new();
        let mut recovery_errors = BTreeMap::new();
        let mut loaded = Vec::new();
        for entry in std::fs::read_dir(&root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let record = (|| {
                let manifest = entry.path().join("session.json");
                let mut record: SessionRecord = read_json(&manifest).map_err(|mut error| {
                    error.message = format!(
                        "Cannot load session record {}: {}",
                        manifest.display(),
                        error.message
                    );
                    error
                })?;
                uuid(&record.session_id, "session_id")?;
                if record.version != 1
                    || entry.file_name() != std::ffi::OsStr::new(&record.session_id)
                {
                    return Err(AppError::new(
                        "invalid_session",
                        "session identity or format does not match its directory",
                    ));
                }
                if !record
                    .pi
                    .conversations
                    .contains_key(&record.pi.active_conversation_id)
                    || !record.pi.preferences.is_object()
                {
                    return Err(AppError::new(
                        "invalid_session",
                        "active conversation or preferences are invalid",
                    ));
                }
                for (id, conversation) in &mut record.pi.conversations {
                    uuid(id, "conversation_id")?;
                    if id != &conversation.conversation_id {
                        return Err(AppError::new(
                            "invalid_session",
                            "conversation identity does not match its record",
                        ));
                    }
                    if let Some(path) = &mut conversation.path {
                        // Migration keeps the former store name as an alias. Resolve
                        // both saved and incoming paths to the same owned directory.
                        *path = canonical_conversation_path(
                            &entry.path().join("pi/conversations"),
                            path,
                        )?;
                    }
                }
                if let Some(id) = &record.run_id {
                    uuid(id, "run_id")?;
                }
                if let Some(intent) = &record.graph_initialization {
                    uuid(intent.run_id(), "run_id")?;
                    if record
                        .run_id
                        .as_ref()
                        .is_some_and(|id| id != intent.run_id())
                    {
                        return Err(AppError::new(
                            "invalid_session",
                            "initialization intent and graph binding disagree",
                        ));
                    }
                }
                Ok(record)
            })();
            loaded.push((entry.file_name().to_string_lossy().into_owned(), record));
        }
        // Claim in a fixed order, current records first: a legacy record stays
        // readable, but cannot take a run a current session already owns.
        loaded.sort_by(|(a, left), (b, right)| {
            let legacy = |record: &Result<SessionRecord>| {
                record
                    .as_ref()
                    .is_ok_and(|record| record.legacy().is_some())
            };
            (legacy(left), a).cmp(&(legacy(right), b))
        });
        for (name, record) in loaded {
            let record = record.and_then(|record| {
                if let Some(run_id) = record.claimed_run() {
                    if !claims.contains_key(run_id) {
                        claims.insert(run_id.to_owned(), record.session_id.clone());
                    } else if record.legacy().is_none() {
                        return Err(AppError::new(
                            "run_already_owned",
                            "multiple sessions claim the same graph run",
                        ));
                    }
                }
                Ok(record)
            });
            match record {
                Ok(record) => {
                    if !matches!(
                        record.status,
                        SessionStatus::Closing | SessionStatus::Closed
                    ) {
                        selectable.insert(record.session_id.clone());
                    }
                    records.insert(record.session_id.clone(), Arc::new(Mutex::new(record)));
                }
                Err(error) => {
                    recovery_errors.insert(name, error);
                }
            }
        }
        let selection_path = root.join("selection.json");
        let selected = if selection_path.exists() {
            match read_json::<Selection>(&selection_path) {
                Ok(selection) => selection.session_id.filter(|id| selectable.contains(id)),
                Err(error) => {
                    recovery_errors.insert("selection".into(), error);
                    None
                }
            }
        } else {
            None
        };
        let admission = records
            .keys()
            .map(|id| (id.clone(), Arc::new(RwLock::new(()))))
            .collect();
        Ok(Self {
            root,
            records: Mutex::new(records),
            selected: Mutex::new(selected),
            claims: Mutex::new(claims),
            admission: Mutex::new(admission),
            environments: Mutex::new(BTreeMap::new()),
            graph_errors: Mutex::new(BTreeMap::new()),
            recovery_errors,
        })
    }

    /// Remember why session `id`'s graph could not start or resume, or
    /// forget it once the graph works.
    pub async fn record_graph_error(&self, id: &str, error: Option<AppError>) {
        let mut errors = self.graph_errors.lock().await;
        if let Some(error) = error {
            errors.insert(id.to_owned(), error);
        } else {
            errors.remove(id);
        }
    }

    async fn graph_error(&self, id: &str) -> Option<AppError> {
        self.graph_errors.lock().await.get(id).cloned()
    }

    /// Give a session `environment` for its programs, unless it has one. A
    /// command that activates the session gives it; so does one that changes
    /// an active session that has none, such as a scripted start. Reading a
    /// session never does. Checked under the session's record lock, which
    /// suspend and close hold while they forget it.
    pub async fn offer_environment(&self, id: &str, environment: Environment, activating: bool) {
        let Ok(handle) = self.get(id).await else {
            return;
        };
        let record = handle.lock().await;
        let open = match record.status {
            SessionStatus::Active => true,
            SessionStatus::Suspending | SessionStatus::Suspended => activating,
            SessionStatus::Closing | SessionStatus::Closed => false,
        };
        if open {
            self.environments
                .lock()
                .await
                .entry(id.to_owned())
                .or_insert(environment);
        }
    }

    /// The environment a command gave this session.
    pub async fn environment(&self, id: &str) -> Option<Environment> {
        self.environments.lock().await.get(id).cloned()
    }

    /// A stopped session forgets its environment; the command that activates
    /// it next brings its own.
    async fn forget_environment(&self, id: &str) {
        self.environments.lock().await.remove(id);
    }

    /// The session that owns a run, if one does.
    pub async fn owner(&self, run_id: &str) -> Option<String> {
        self.claims.lock().await.get(run_id).cloned()
    }

    /// Every saved session, as `session.list` reports them, read while no
    /// server runs.
    pub async fn saved(paths: &Paths) -> Result<Value> {
        let sessions = Self::open(paths)?;
        Ok(json!({
            "sessions": sessions.list().await,
            "selected_session_id": sessions.selected().await,
            "recovery_errors": sessions.recovery_errors,
        }))
    }

    /// A saved session as `session.inspect` shows it, read while no server
    /// runs.
    pub async fn inspect_saved(paths: &Paths, id: &str) -> Result<Value> {
        let sessions = Self::open(paths)?;
        let record = sessions.get(id).await?.lock().await.clone();
        view(&record)
    }

    pub fn directory(&self, id: &str) -> Result<PathBuf> {
        uuid(id, "session_id")?;
        Ok(self.root.join(id))
    }
    pub fn conversations_dir(&self, id: &str) -> Result<PathBuf> {
        Ok(self.directory(id)?.join("pi/conversations"))
    }
    pub async fn get(&self, id: &str) -> Result<Arc<Mutex<SessionRecord>>> {
        self.records.lock().await.get(id).cloned().ok_or_else(|| {
            AppError::new("session_not_found", format!("session {id} does not exist"))
        })
    }
    pub fn save(&self, record: &SessionRecord) -> Result<()> {
        write_json(
            &self.directory(&record.session_id)?.join("session.json"),
            record,
        )
    }
    pub async fn selected(&self) -> Option<String> {
        self.selected.lock().await.clone()
    }
    pub async fn select(&self, id: &str) -> Result<()> {
        let handle = self.get(id).await?;
        // Retain the record lock through selection publication: close uses the
        // same record -> selection lock order and must clear whichever wins first.
        let record = handle.lock().await;
        if matches!(
            record.status,
            SessionStatus::Closing | SessionStatus::Closed
        ) {
            return Err(AppError::new(
                "session_closed",
                "a closing or closed session cannot be selected for attachment",
            ));
        }
        let mut selected = self.selected.lock().await;
        write_json(
            &self.root.join("selection.json"),
            &Selection {
                session_id: Some(id.into()),
            },
        )?;
        *selected = Some(id.into());
        Ok(())
    }
    async fn clear_selection(&self, id: &str) -> Result<()> {
        let mut selected = self.selected.lock().await;
        if selected.as_deref() == Some(id) {
            write_json(
                &self.root.join("selection.json"),
                &Selection { session_id: None },
            )?;
            *selected = None;
        }
        Ok(())
    }
    async fn gate(&self, id: &str) -> Result<Arc<RwLock<()>>> {
        self.admission.lock().await.get(id).cloned().ok_or_else(|| {
            AppError::new("session_not_found", format!("session {id} does not exist"))
        })
    }
    pub async fn create(&self, project: PathBuf, name: Option<String>) -> Result<SessionRecord> {
        let project = std::fs::canonicalize(project)?;
        if !project.is_dir() {
            return Err(AppError::invalid("project must be a directory"));
        }
        let session_id = uuid::Uuid::new_v4().to_string();
        let conversation_id = uuid::Uuid::new_v4().to_string();
        let name = name.unwrap_or_else(|| format!("session-{}", &session_id[..8]));
        if name.trim().is_empty() || name.len() > 200 || name.chars().any(char::is_control) {
            return Err(AppError::invalid(
                "session name must contain 1–200 printable bytes",
            ));
        }
        let timestamp = now();
        let record = SessionRecord {
            version: 1,
            session_id: session_id.clone(),
            name,
            project,
            status: SessionStatus::Active,
            run_id: None,
            graph_initialization: None,
            pi: PiState {
                active_conversation_id: conversation_id.clone(),
                conversations: BTreeMap::from([(
                    conversation_id.clone(),
                    Conversation {
                        conversation_id,
                        path: None,
                        materialized: false,
                    },
                )]),
                preferences: json!({}),
            },
            created_at: timestamp,
            updated_at: timestamp,
        };
        std::fs::create_dir_all(self.conversations_dir(&session_id)?)?;
        self.save(&record)?;
        self.admission
            .lock()
            .await
            .insert(session_id.clone(), Arc::new(RwLock::new(())));
        self.records
            .lock()
            .await
            .insert(session_id.clone(), Arc::new(Mutex::new(record.clone())));
        self.select(&session_id).await?;
        Ok(record)
    }
    pub async fn list(&self) -> Vec<SessionRecord> {
        let handles = self
            .records
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut records = Vec::with_capacity(handles.len());
        for handle in handles {
            records.push(handle.lock().await.clone());
        }
        records
    }
    async fn claim(&self, run_id: &str, session_id: &str) -> Result<()> {
        let mut claims = self.claims.lock().await;
        if claims.get(run_id).is_some_and(|owner| owner != session_id) {
            return Err(AppError::new(
                "run_already_owned",
                "graph run already belongs to another Ontography session",
            ));
        }
        claims.insert(run_id.into(), session_id.into());
        Ok(())
    }
}

pub fn operations() -> Vec<Operation> {
    let text = json!({"type":"string"});
    let session = json!({"session_id":text});
    vec![
        Operation::new(
            "session.create",
            "Create an Ontography session with independent Pi manager state, awaiting graph initialization.",
            json!({"project":text,"name":text}),
            &["project"],
            true,
        ),
        Operation::new(
            "session.list",
            "List durable Ontography sessions and the most recently selected session.",
            json!({}),
            &[],
            false,
        ),
        Operation::new(
            "session.inspect",
            "Read a durable app-session record and current graph state.",
            session.clone(),
            &["session_id"],
            false,
        ),
        Operation::new(
            "session.context",
            "Read the owning session, Pi state location, and current graph for fresh conversation context.",
            session.clone(),
            &["session_id"],
            false,
        ),
        Operation::new(
            "session.select",
            "Record the selected Ontography session; CLI attachment still requires an explicit target.",
            session.clone(),
            &["session_id"],
            true,
        ),
        Operation::new(
            "session.adopt",
            "Explicitly bind an existing unowned graph run to an uninitialized Ontography session.",
            json!({"session_id":text,"run_id":text}),
            &["session_id", "run_id"],
            true,
        ),
        Operation::new(
            "session.resume",
            "Resume the session's bound graph and allow its manager terminal to launch.",
            session.clone(),
            &["session_id"],
            true,
        ),
        Operation::new(
            "session.suspend",
            "Suspend an Ontography session while preserving Pi history and its graph.",
            session.clone(),
            &["session_id"],
            true,
        ),
        Operation::new(
            "session.close",
            "Close an Ontography session and its graph while preserving durable history.",
            session.clone(),
            &["session_id"],
            true,
        ),
        Operation::new(
            "session.conversation",
            "Register/activate an owned Pi conversation, check a resume target, or explicitly import a conversation history.",
            json!({"session_id":text,"action":{"type":"string","enum":["activate","check","import"]},"conversation_id":text,"path":text}),
            &["session_id", "action"],
            true,
        ),
        Operation::new(
            "session.preferences",
            "Read or merge session-scoped Pi manager defaults; these remain separate from native conversation history.",
            json!({"session_id":text,"preferences":{"type":"object"}}),
            &["session_id"],
            true,
        ),
    ]
}

fn require_active(record: &SessionRecord) -> Result<()> {
    if record.status != SessionStatus::Active {
        return Err(AppError::new(
            "session_inactive",
            "resume the Ontography session before performing graph mutations",
        ));
    }
    Ok(())
}

/// The run a session's graph operations act on, or why there is none:
/// `failed` is why its pending start last failed, if it did.
fn graph_run<'a>(record: &'a SessionRecord, failed: Option<&AppError>) -> Result<&'a str> {
    if let Some(legacy) = record.legacy() {
        return Err(legacy.unavailable());
    }
    match (&record.run_id, &record.graph_initialization) {
        (Some(id), _) => Ok(id),
        (None, Some(_)) => Err(failed.cloned().unwrap_or_else(|| {
            AppError::new(
                "graph_unavailable",
                "this session's graph has not finished starting; resume the session to finish it",
            )
        })),
        (None, None) => Err(AppError::new(
            "graph_uninitialized",
            "this Ontography session has no graph run yet",
        )),
    }
}

/// Resolve implicit targets while the caller holds the app session's lifecycle lock.
fn scoped_args(
    record: &SessionRecord,
    failed: Option<&AppError>,
    operation: &str,
    args: &Value,
) -> Result<Value> {
    let mut args = args.clone();
    let object = args
        .as_object_mut()
        .ok_or_else(|| AppError::invalid("args must be an object"))?;
    if let Some(id) = object.get("session_id")
        && id.as_str() != Some(&record.session_id)
    {
        return Err(AppError::new(
            "session_scope_conflict",
            "request names a different Ontography session",
        ));
    }
    let operation = crate::catalog::operations()
        .iter()
        .find(|item| item.name == operation)
        .ok_or_else(|| AppError::new("unknown_operation", operation))?;
    let properties = &operation.parameters["properties"];
    if properties.get("session_id").is_some() {
        object.insert("session_id".into(), json!(record.session_id));
    }
    if properties.get("run_id").is_some() {
        let run_id = graph_run(record, failed)?;
        if object
            .get("run_id")
            .is_some_and(|id| id.as_str() != Some(run_id))
        {
            return Err(AppError::new(
                "session_scope_conflict",
                "request names a graph outside the owning Ontography session",
            ));
        }
        object.insert("run_id".into(), json!(run_id));
    }
    if properties.get("project").is_some() {
        if let Some(path) = object.get("project") {
            let project = std::fs::canonicalize(
                path.as_str()
                    .ok_or_else(|| AppError::invalid("project must be a path string"))?,
            )?;
            if project != record.project {
                return Err(AppError::new(
                    "session_scope_conflict",
                    "project differs from the owning Ontography session",
                ));
            }
        }
        object.insert("project".into(), json!(record.project));
    }
    Ok(args)
}

pub async fn dispatch_scoped(
    service: &Service,
    session_id: &str,
    operation: &str,
    args: &Value,
) -> Result<Value> {
    if matches!(
        operation,
        "session.create" | "session.select" | "session.adopt"
    ) {
        return Err(AppError::new(
            "session_scope_conflict",
            "use explicit app-session administration for this operation",
        ));
    }
    let gate = service.sessions.gate(session_id).await?;
    let lifecycle = matches!(
        operation,
        "session.resume" | "session.suspend" | "session.close"
    );
    let mutating = crate::catalog::operations()
        .iter()
        .any(|item| item.name == operation && item.mutating);
    let _lifecycle = if lifecycle {
        Some(gate.write().await)
    } else {
        None
    };
    let _operation = if mutating && !operation.starts_with("session.") {
        Some(gate.read().await)
    } else {
        None
    };
    let handle = service.sessions.get(session_id).await?;
    let mut record = handle.lock().await;
    let failed = service.sessions.graph_error(&record.session_id).await;
    let args = scoped_args(&record, failed.as_ref(), operation, args)?;
    if operation == "session.list" {
        return Ok(json!({"sessions":[&*record],"selected_session_id":record.session_id}));
    }
    if operation.starts_with("session.") {
        return dispatch_record(service, &mut record, operation, &args).await;
    }
    if mutating {
        require_active(&record)?;
    }
    if operation == "flow.start" {
        return initialize_graph(service, &mut record, &args).await;
    }
    if operation == "run.list" {
        let mut listing = json!({"runs":[],"next_after":null,"recovery_errors":{}});
        if let Some(id) = record
            .run_id
            .as_deref()
            .filter(|_| record.legacy().is_none())
        {
            match service.run(id).await {
                Ok(run) => listing["runs"] = json!([run.lock().await.summary()]),
                Err(error) => {
                    listing["recovery_errors"][id] = json!(recovery_error(service, id, error));
                }
            }
        }
        return Ok(listing);
    }
    // A run that failed to load reports why, whatever the operation.
    if let Some(error) = args
        .get("run_id")
        .and_then(Value::as_str)
        .and_then(|id| service.recovery_errors.get(id))
    {
        return Err(error.clone());
    }
    let session_id = record.session_id.clone();
    drop(record);
    let result = crate::tools::dispatch(service, operation, &args).await;
    if mutating && result.is_ok() {
        // The graph works again, so its last failure no longer describes it.
        service.sessions.record_graph_error(&session_id, None).await;
    }
    result
}

pub async fn dispatch(service: &Service, operation: &str, args: &Value) -> Result<Value> {
    match operation {
        "session.create" => {
            let project = PathBuf::from(views::field(args, "project")?);
            if !project.is_absolute() {
                return Err(AppError::invalid("project must be an absolute directory"));
            }
            Ok(serde_json::to_value(
                service
                    .sessions
                    .create(
                        project,
                        args.get("name").and_then(Value::as_str).map(str::to_owned),
                    )
                    .await?,
            )?)
        }
        "session.list" => Ok(
            json!({"sessions":service.sessions.list().await,"selected_session_id":service.sessions.selected().await,"recovery_errors":service.sessions.recovery_errors}),
        ),
        "session.select" => {
            let id = views::field(args, "session_id")?;
            service.sessions.select(id).await?;
            Ok(serde_json::to_value(
                &*service.sessions.get(id).await?.lock().await,
            )?)
        }
        _ => {
            let id = views::field(args, "session_id")?;
            let gate = service.sessions.gate(id).await?;
            let _lifecycle = if matches!(
                operation,
                "session.resume" | "session.suspend" | "session.close" | "session.adopt"
            ) {
                Some(gate.write().await)
            } else {
                None
            };
            let handle = service.sessions.get(id).await?;
            dispatch_record(service, &mut *handle.lock().await, operation, args).await
        }
    }
}

async fn context(service: &Service, record: &mut SessionRecord) -> Result<Value> {
    let mut changed = false;
    for conversation in record.pi.conversations.values_mut() {
        if !conversation.materialized
            && conversation
                .path
                .as_ref()
                .is_some_and(|path| path.is_file())
        {
            let path = conversation.path.as_ref().expect("path was checked");
            validate_history(path, &conversation.conversation_id)?;
            conversation.materialized = true;
            changed = true;
        }
    }
    if changed {
        record.updated_at = now();
        service.sessions.save(record)?;
    }
    let graph = graph_view(service, record).await;
    let mut session = view(record)?;
    if graph["status"] == "unavailable" {
        session["graph"] = graph.clone();
    }
    Ok(
        json!({"session":session,"conversations_dir":service.sessions.conversations_dir(&record.session_id)?,"graph":graph}),
    )
}

fn unavailable_graph(run_id: &str, error: AppError) -> Value {
    json!({"run_id":run_id,"status":"unavailable","error":error})
}

fn recovery_error(service: &Service, run_id: &str, error: AppError) -> AppError {
    service
        .recovery_errors
        .get(run_id)
        .cloned()
        .unwrap_or(error)
}

/// A session's graph as it stands, or why it cannot run. A failure to start
/// or resume stays visible until the graph works again.
async fn graph_view(service: &Service, record: &SessionRecord) -> Value {
    let Some(id) = record.claimed_run() else {
        return Value::Null;
    };
    let failed = service.sessions.graph_error(&record.session_id).await;
    let Some(bound) = &record.run_id else {
        return match (record.legacy(), failed) {
            (Some(legacy), _) => unavailable_graph(id, legacy.unavailable()),
            (None, Some(error)) => unavailable_graph(id, error),
            (None, None) => Value::Null,
        };
    };
    let handle = match service.run(bound).await {
        Ok(handle) => handle,
        Err(error) => return unavailable_graph(bound, recovery_error(service, bound, error)),
    };
    if let Some(legacy) = record.legacy() {
        return unavailable_graph(bound, legacy.unavailable());
    }
    let run = handle.lock().await;
    match failed {
        // A run that never opened has nothing to show but why.
        Some(error) if run.live.is_none() => unavailable_graph(bound, error),
        failed => match crate::workflow::tools::status(&run).await {
            Ok(mut graph) => {
                if let Some(error) = failed {
                    graph["resume_error"] = json!(error);
                }
                graph
            }
            Err(error) => unavailable_graph(bound, error),
        },
    }
}

/// A session as `session.inspect` shows it: its record, with a workflow's
/// initialization summarized.
fn view(record: &SessionRecord) -> Result<Value> {
    let mut session = serde_json::to_value(record)?;
    if let Some(intent) = &record.graph_initialization {
        session["graph_initialization"] =
            json!({"run_id":intent.run_id(),"operation":"flow.start"});
        if let SavedGraphInitialization::Legacy(legacy) = intent {
            session["graph_initialization"]["operation"] = serde_json::to_value(&legacy.operation)?;
            session["graph_initialization"]["status"] = json!("unavailable");
            session["graph_initialization"]["error"] = json!(legacy.unavailable());
        }
    }
    Ok(session)
}

async fn dispatch_record(
    service: &Service,
    record: &mut SessionRecord,
    operation: &str,
    args: &Value,
) -> Result<Value> {
    match operation {
        "session.inspect" => Ok(context(service, record).await?["session"].clone()),
        "session.context" => context(service, record).await,
        "session.preferences" => {
            if let Some(preferences) = args.get("preferences") {
                if matches!(record.status, SessionStatus::Closed) {
                    return Err(AppError::new(
                        "session_closed",
                        "closed session preferences cannot change",
                    ));
                }
                let preferences = preferences
                    .as_object()
                    .ok_or_else(|| AppError::invalid("preferences must be an object"))?;
                let mut next = record.clone();
                next.pi
                    .preferences
                    .as_object_mut()
                    .expect("preferences validated on load")
                    .extend(preferences.clone());
                next.updated_at = now();
                service.sessions.save(&next)?;
                *record = next;
            }
            Ok(json!({"preferences":record.pi.preferences}))
        }
        "session.conversation" => conversation(service, record, args),
        "session.adopt" => {
            require_active(record)?;
            let run_id = views::field(args, "run_id")?;
            if record.run_id.as_deref() == Some(run_id) {
                return Ok(context(service, record).await?["session"].clone());
            }
            if record.run_id.is_some() || record.graph_initialization.is_some() {
                return Err(AppError::new(
                    "graph_already_initialized",
                    "session already owns or is initializing a graph",
                ));
            }
            let run = service.run(run_id).await?;
            if run.lock().await.manifest.project != record.project {
                return Err(AppError::new(
                    "session_scope_conflict",
                    "adopted graph must use the session's project directory",
                ));
            }
            service.sessions.claim(run_id, &record.session_id).await?;
            let mut next = record.clone();
            next.run_id = Some(run_id.into());
            next.updated_at = now();
            if let Err(error) = service.sessions.save(&next) {
                service.sessions.claims.lock().await.remove(run_id);
                return Err(error);
            }
            *record = next;
            Ok(context(service, record).await?["session"].clone())
        }
        "session.resume" => {
            if matches!(
                record.status,
                SessionStatus::Closing | SessionStatus::Closed
            ) {
                return Err(AppError::new(
                    "session_closed",
                    "a closed session cannot resume",
                ));
            }
            // Activate before touching the graph, so a failed save never
            // leaves work running under a suspended session. A graph that
            // cannot start or resume keeps the session active with the reason
            // kept, so its manager can repair it.
            let mut next = record.clone();
            next.status = SessionStatus::Active;
            next.updated_at = now();
            service.sessions.save(&next)?;
            *record = next;
            let failed = resume_graph(service, record).await.err();
            service
                .sessions
                .record_graph_error(&record.session_id, failed)
                .await;
            Ok(context(service, record).await?["session"].clone())
        }
        "session.suspend" | "session.close" => {
            let close = operation == "session.close";
            if record.status == SessionStatus::Closed {
                return Ok(context(service, record).await?["session"].clone());
            }
            if !close && record.status == SessionStatus::Closing {
                return Err(AppError::new(
                    "session_closing",
                    "retry close to finish closing this session",
                ));
            }
            record.status = if close {
                SessionStatus::Closing
            } else {
                SessionStatus::Suspending
            };
            record.updated_at = now();
            service.sessions.save(record)?;
            if record.legacy().is_none()
                && let Some(id) = record.claimed_run()
                && let Ok(run) = service.run(id).await
            {
                let mut run = run.lock().await;
                // A run this build cannot open has nothing live to stop and
                // could never close; the session closes around it, as it is.
                if let Err(error) = run.suspend(close).await
                    && (!close || run.live.is_some())
                {
                    return Err(error);
                }
            }
            service
                .sessions
                .record_graph_error(&record.session_id, None)
                .await;
            record.status = if close {
                SessionStatus::Closed
            } else {
                SessionStatus::Suspended
            };
            record.updated_at = now();
            service.sessions.save(record)?;
            service
                .sessions
                .forget_environment(&record.session_id)
                .await;
            if close {
                service.sessions.clear_selection(&record.session_id).await?;
            }
            Ok(context(service, record).await?["session"].clone())
        }
        _ => Err(AppError::new("unknown_operation", operation)),
    }
}

/// Start a session's pending graph, then resume its bound one.
async fn resume_graph(service: &Service, record: &mut SessionRecord) -> Result<()> {
    if let Some(legacy) = record.legacy() {
        return Err(legacy.unavailable());
    }
    if record.run_id.is_none()
        && let Some(saved) = &record.graph_initialization
    {
        let args = saved.current()?.args.clone();
        initialize_graph(service, record, &args).await?;
    }
    if let Some(id) = &record.run_id {
        let environment = service.session_environment(&record.session_id).await;
        let run = service
            .run(id)
            .await
            .map_err(|error| recovery_error(service, id, error))?;
        let mut run = run.lock().await;
        run.environment = environment;
        run.resume().await?;
    }
    Ok(())
}

async fn initialize_graph(
    service: &Service,
    record: &mut SessionRecord,
    args: &Value,
) -> Result<Value> {
    let intent = if let Some(saved) = &record.graph_initialization {
        let intent = saved.current()?;
        if intent.args != *args {
            return Err(AppError::new(
                "graph_already_initialized",
                "session graph initialization already has a different declaration; inspect it before proceeding",
            ));
        }
        intent.clone()
    } else {
        if record.run_id.is_some() {
            return Err(AppError::new(
                "graph_already_initialized",
                "session already owns a graph run",
            ));
        }
        // Pin the resolved definition before reserving. Rejected drafts consume no identity,
        // and retries never resolve a changed external declaration/provider as new work.
        let run_id = crate::workflow::tools::optional_str(args, "start_id")?
            .map(str::to_owned)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        uuid::Uuid::parse_str(&run_id).map_err(|_| AppError::invalid("start_id must be a UUID"))?;
        let (definition, workflow) = crate::workflow::tools::prepare_start(service, args, &run_id)?;
        let intent = GraphInitialization {
            run_id,
            args: args.clone(),
            definition,
            workflow,
        };
        let mut next = record.clone();
        next.graph_initialization = Some(intent.clone().into());
        next.updated_at = now();
        service
            .sessions
            .claim(&intent.run_id, &record.session_id)
            .await?;
        if let Err(error) = service.sessions.save(&next) {
            service.sessions.claims.lock().await.remove(&intent.run_id);
            return Err(error);
        }
        *record = next;
        intent
    };
    let environment = service.session_environment(&record.session_id).await;
    service
        .start_reserved(
            &intent.run_id,
            intent.definition,
            record.project.clone(),
            intent.workflow,
            environment,
        )
        .await?;
    let mut next = record.clone();
    next.run_id = Some(intent.run_id.clone());
    next.updated_at = now();
    service.sessions.save(&next)?;
    *record = next;
    crate::workflow::tools::status(&*service.run(&intent.run_id).await?.lock().await).await
}

fn owned_path(sessions: &Sessions, record: &SessionRecord, path: &Path) -> Result<PathBuf> {
    canonical_conversation_path(&sessions.conversations_dir(&record.session_id)?, path)
}

fn canonical_conversation_path(directory: &Path, path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(AppError::invalid("conversation path must be absolute"));
    }
    let directory = std::fs::canonicalize(directory)?;
    let normalized = if path.exists() {
        std::fs::canonicalize(path)?
    } else {
        let parent = path
            .parent()
            .ok_or_else(|| AppError::invalid("conversation path needs a parent"))?;
        std::fs::canonicalize(parent)?.join(
            path.file_name()
                .ok_or_else(|| AppError::invalid("conversation path needs a filename"))?,
        )
    };
    if normalized.parent() != Some(directory.as_path()) {
        return Err(AppError::new(
            "conversation_not_owned",
            "conversation history must be in this Ontography session's conversation directory",
        ));
    }
    Ok(normalized)
}

fn history_id(path: &Path) -> Result<String> {
    use std::io::{BufRead, BufReader, Read};
    let mut line = String::new();
    BufReader::new(std::fs::File::open(path)?)
        .take(1024 * 1024)
        .read_line(&mut line)?;
    let header: Value = serde_json::from_str(&line)?;
    if header["type"] != "session" {
        return Err(AppError::new(
            "invalid_conversation",
            "Pi history must begin with its session header",
        ));
    }
    let id = header["id"].as_str().ok_or_else(|| {
        AppError::new("invalid_conversation", "Pi history has no session identity")
    })?;
    uuid(id, "conversation_id")?;
    Ok(id.into())
}

fn validate_history(path: &Path, id: &str) -> Result<()> {
    if history_id(path)? != id {
        return Err(AppError::new(
            "invalid_conversation",
            "Pi history identity differs from the recorded conversation",
        ));
    }
    Ok(())
}

fn conversation(service: &Service, record: &mut SessionRecord, args: &Value) -> Result<Value> {
    let action = views::field(args, "action")?;
    if action != "check" && record.status == SessionStatus::Closed {
        return Err(AppError::new(
            "session_closed",
            "closed session conversations cannot change",
        ));
    }
    let path = args.get("path").and_then(Value::as_str).map(PathBuf::from);
    let mut imported = None;
    let path = if action == "import" {
        let source = path.ok_or_else(|| AppError::invalid("path is required for import"))?;
        if !source.is_absolute() {
            return Err(AppError::invalid("import path must be absolute"));
        }
        let id = history_id(&source)?;
        if record.pi.conversations.contains_key(&id) {
            return Err(AppError::new(
                "conversation_exists",
                "conversation is already registered; activate it instead",
            ));
        }
        let target = service
            .sessions
            .conversations_dir(&record.session_id)?
            .join(format!("imported-{id}.jsonl"));
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut target_file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&target)?;
        std::io::copy(&mut std::fs::File::open(source)?, &mut target_file)?;
        target_file.flush()?;
        target_file.sync_all()?;
        imported = Some(id);
        Some(target)
    } else {
        path.map(|path| owned_path(&service.sessions, record, &path))
            .transpose()?
    };
    let id = args
        .get("conversation_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or(imported)
        .or_else(|| {
            path.as_ref().and_then(|path| {
                record
                    .pi
                    .conversations
                    .values()
                    .find(|entry| entry.path.as_ref() == Some(path))
                    .map(|entry| entry.conversation_id.clone())
            })
        })
        .ok_or_else(|| AppError::invalid("conversation_id is required for a new conversation"))?;
    uuid(&id, "conversation_id")?;
    if action == "check" {
        let conversation = record.pi.conversations.get(&id).ok_or_else(|| {
            AppError::new(
                "conversation_not_owned",
                "conversation is not registered in this Ontography session",
            )
        })?;
        if path
            .as_ref()
            .is_some_and(|path| conversation.path.as_ref() != Some(path))
        {
            return Err(AppError::new(
                "conversation_not_owned",
                "conversation path differs from its registered history",
            ));
        }
        if conversation.materialized
            && conversation
                .path
                .as_ref()
                .is_none_or(|path| !path.is_file())
        {
            return Err(AppError::new(
                "conversation_missing",
                "recorded conversation history is missing",
            ));
        }
        return Ok(json!({"allowed":true,"conversation":conversation}));
    }
    if !matches!(action, "activate" | "import") {
        return Err(AppError::invalid("invalid conversation action"));
    }
    if let Some(existing) = record.pi.conversations.get(&id) {
        if existing.materialized && existing.path.as_ref().is_none_or(|path| !path.is_file()) {
            return Err(AppError::new(
                "conversation_missing",
                "recorded conversation history is missing; it cannot become a new empty conversation",
            ));
        }
        // Pi writes a history only once something is said, and names a new
        // file each time it starts a conversation it has not saved. An unwritten
        // path only reserved a name, so the conversation may move.
        let written =
            existing.materialized || existing.path.as_ref().is_some_and(|path| path.is_file());
        if written && path.is_some() && path != existing.path {
            return Err(AppError::new(
                "conversation_not_owned",
                "conversation is already registered at another path",
            ));
        }
    } else if path.is_none() {
        return Err(AppError::invalid(
            "new conversations must report an owned history path",
        ));
    }
    let path = path.or_else(|| {
        record
            .pi
            .conversations
            .get(&id)
            .and_then(|entry| entry.path.clone())
    });
    let materialized = path.as_ref().is_some_and(|path| path.is_file());
    if let Some(path) = path.as_ref().filter(|_| materialized) {
        validate_history(path, &id)?;
    }
    let conversation = Conversation {
        conversation_id: id.clone(),
        path,
        materialized,
    };
    let mut next = record.clone();
    next.pi.active_conversation_id = id.clone();
    next.pi.conversations.insert(id, conversation.clone());
    next.updated_at = now();
    service.sessions.save(&next)?;
    *record = next;
    Ok(
        json!({"allowed":true,"conversation":conversation,"active_conversation_id":record.pi.active_conversation_id}),
    )
}
