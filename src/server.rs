//! Local transport and operation ownership. A socket never owns a core operation.
//!
//! The server keeps sessions running after their clients detach. Once nothing
//! runs and no client is connected, it exits: saved sessions live on disk,
//! and the next command starts the current build again.
use crate::{
    AppError, Result, catalog, declarations,
    environment::Environment,
    persistence::Paths,
    protocol::{self, Request, Response},
    state::Service,
    views,
};
use futures_util::FutureExt;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    panic::AssertUnwindSafe,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::BufReader,
    net::{UnixListener, UnixStream},
    sync::{Mutex, Notify, RwLock, watch},
    task::JoinSet,
};

/// How long the server stays with nothing to do before it exits.
pub const IDLE_LIMIT: Duration = Duration::from_secs(30);

/// How long a new server waits for an exiting one to release the data
/// directory, such as when a command arrives just as an idle server stops.
const HANDOVER: Duration = Duration::from_secs(5);

/// An accepted operation's outcome, numbered in the order outcomes arrive.
type Completion = Option<(u64, Arc<Result<Value>>)>;
struct Receipt {
    operation: String,
    app_session_id: Option<String>,
    args: Value,
    result: watch::Receiver<Completion>,
}

pub struct Server {
    pub service: Arc<Service>,
    managers: crate::session_runtime::Managers,
    requests: Mutex<BTreeMap<(String, String), Receipt>>,
    /// How many outcomes have arrived; numbers the next.
    completions: AtomicU64,
    admission: RwLock<()>,
    stopping: AtomicBool,
    manager_monitor_started: AtomicBool,
    stopped: Notify,
    /// Client connections now open.
    connections: AtomicUsize,
}

impl Server {
    pub fn new(paths: Paths) -> Result<Arc<Self>> {
        // Sessions' shells bind their sockets beside the server's.
        paths.create_endpoint()?;
        Ok(Arc::new(Self {
            service: Arc::new(Service::new(paths)?),
            managers: crate::session_runtime::Managers::default(),
            requests: Mutex::new(BTreeMap::new()),
            completions: AtomicU64::new(0),
            admission: RwLock::new(()),
            stopping: AtomicBool::new(false),
            manager_monitor_started: AtomicBool::new(false),
            stopped: Notify::new(),
            connections: AtomicUsize::new(0),
        }))
    }

    pub async fn request(self: &Arc<Self>, request: Request) -> Result<Value> {
        request.validate()?;
        self.start_manager_monitor();
        if request.operation != "system.hello"
            && request.expected_server_id.as_deref() != Some(&self.service.server_id)
        {
            return Err(AppError::new(
                "server_restarted",
                "handshake again and refresh state before sending operations",
            ));
        }
        if request.operation == "operation.get" {
            let key = (
                views::field(&request.args, "client_id")?.to_owned(),
                views::field(&request.args, "request_id")?.to_owned(),
            );
            let requests = self.requests.lock().await;
            let receipt = requests.get(&key).ok_or_else(|| {
                AppError::new(
                    "unknown_outcome",
                    "receipt is absent or expired; reconcile against current run state",
                )
            })?;
            if request.app_session_id.is_some() && receipt.app_session_id != request.app_session_id
            {
                return Err(AppError::new(
                    "session_scope",
                    "receipt belongs to another session",
                ));
            }
            return Ok(match receipt.result.borrow().as_ref() {
                None => json!({"state":"running","operation":receipt.operation}),
                Some((_, result)) => match result.as_ref() {
                    Ok(value) => {
                        json!({"state":"completed","operation":receipt.operation,"result":value})
                    }
                    Err(error) => {
                        json!({"state":"failed","operation":receipt.operation,"error":error})
                    }
                },
            });
        }
        if request.operation == "server.stop" {
            if request.app_session_id.is_some() {
                return Err(AppError::new(
                    "global_operation",
                    "stop the server through the explicit server CLI",
                ));
            }
            return self.stop().await;
        }
        let operation = catalog::operations()
            .iter()
            .find(|o| o.name == request.operation)
            .ok_or_else(|| AppError::new("unknown_operation", &request.operation))?;
        let mut parameters = operation.parameters.clone();
        if request.app_session_id.is_some()
            && let Some(required) = parameters.get_mut("required").and_then(Value::as_array_mut)
        {
            required
                .retain(|name| !matches!(name.as_str(), Some("run_id" | "project" | "session_id")));
        }
        validate_arguments(&parameters, &request.args, "args")?;
        if !operation.mutating {
            let result = AssertUnwindSafe(self.managers.dispatch(
                &self.service,
                request.app_session_id.as_deref(),
                &request.operation,
                &request.args,
                request.environment.clone().map(Environment::from_vars),
            ))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| {
                Err(AppError::new(
                    "runtime_fault",
                    "inspection panicked; inspect server and run state before retrying",
                ))
            });
            let mut result = bounded(result, false);
            if request.operation == "system.hello"
                && let Ok(hello) = &mut result
            {
                // A client of another build may replace an idle server: one
                // with nothing running and no connection but this one.
                hello["idle"] =
                    json!(!self.busy() && self.connections.load(Ordering::Acquire) <= 1);
            }
            return result;
        }
        let key = (request.client_id, request.request_id);
        let mut requests = self.requests.lock().await;
        let mut result = if let Some(receipt) = requests.get(&key) {
            if receipt.operation != request.operation
                || receipt.args != request.args
                || receipt.app_session_id != request.app_session_id
            {
                return Err(AppError::new(
                    "request_id_conflict",
                    "request identity was already used with different arguments",
                ));
            }
            receipt.result.clone()
        } else {
            if self.stopping.load(Ordering::Acquire) {
                return Err(AppError::new(
                    "server_stopping",
                    "server is settling accepted work",
                ));
            }
            if requests.len() >= 128 {
                // Forget the outcome its client has had longest to read: one
                // retrying a reply it just lost still finds its receipt.
                let completed = requests
                    .iter()
                    .filter_map(|(key, r)| Some((r.result.borrow().as_ref()?.0, key)))
                    .min_by_key(|(order, _)| *order)
                    .map(|(_, key)| key.clone());
                if let Some(key) = completed {
                    requests.remove(&key);
                } else {
                    return Err(AppError::new(
                        "busy",
                        "too many accepted operations; wait for one to finish",
                    ));
                }
            }
            let (sender, receiver) = watch::channel(None);
            requests.insert(
                key,
                Receipt {
                    operation: request.operation.clone(),
                    app_session_id: request.app_session_id.clone(),
                    args: request.args.clone(),
                    result: receiver.clone(),
                },
            );
            let server = self.clone();
            tokio::spawn(async move {
                let result = AssertUnwindSafe(async {
                    let _guard = server.admission.read().await;
                    if server.stopping.load(Ordering::Acquire) {
                        return Err(AppError::new(
                            "server_stopping",
                            "operation did not begin before shutdown",
                        ));
                    }
                    server
                        .managers
                        .dispatch(
                            &server.service,
                            request.app_session_id.as_deref(),
                            &request.operation,
                            &request.args,
                            request.environment.clone().map(Environment::from_vars),
                        )
                        .await
                })
                .catch_unwind()
                .await
                .unwrap_or_else(|_| {
                    Err(AppError::new(
                        "runtime_fault",
                        "operation panicked; inspect run state before further mutations",
                    ))
                });
                let order = server.completions.fetch_add(1, Ordering::AcqRel);
                sender.send_replace(Some((order, Arc::new(bounded(result, true)))));
            });
            receiver
        };
        drop(requests);
        loop {
            if let Some((_, value)) = result.borrow().as_ref() {
                return value.as_ref().clone();
            }
            result.changed().await.map_err(|_| {
                AppError::new("unknown_outcome", "operation owner ended without a receipt")
            })?;
        }
    }

    /// Whether anything runs: a session's shell, an open run, or an accepted
    /// operation still in progress. Never waits on work in progress, which
    /// counts as running.
    fn busy(&self) -> bool {
        self.managers.any()
            || self.service.has_live_runs()
            || self.requests.try_lock().map_or(true, |requests| {
                requests
                    .values()
                    .any(|receipt| receipt.result.borrow().is_none())
            })
    }

    /// Reap naturally exited shells independently of attached clients. A stale
    /// terminal is fenced by Managers before it can suspend a newer one.
    fn start_manager_monitor(self: &Arc<Self>) {
        if self.manager_monitor_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut last_error = None;
            loop {
                interval.tick().await;
                let Some(server) = weak.upgrade() else {
                    break;
                };
                let _guard = server.admission.read().await;
                if server.stopping.load(Ordering::Acquire) {
                    continue;
                }
                match server.managers.reconcile(&server.service).await {
                    Ok(()) => last_error = None,
                    Err(error) => {
                        let message = error.to_string();
                        if last_error.as_ref() != Some(&message) {
                            crate::logging::record(&server.service.paths.root, &message);
                            last_error = Some(message);
                        }
                    }
                }
            }
        });
    }

    pub async fn stop(&self) -> Result<Value> {
        if self.stopping.swap(true, Ordering::AcqRel) {
            return Err(AppError::new(
                "server_stopping",
                "shutdown is already in progress",
            ));
        }
        let _guard = self.admission.write().await;
        if let Err(error) = self.managers.shutdown().await {
            self.stopping.store(false, Ordering::Release);
            return Err(error);
        }
        if let Err(error) = self.service.shutdown().await {
            self.stopping.store(false, Ordering::Release);
            return Err(error);
        }
        // Stopped shells have removed their sockets. The directory goes only
        // if empty: while `serve` runs, its own socket keeps it.
        let _ = std::fs::remove_dir(self.service.paths.endpoint());
        self.stopped.notify_one();
        Ok(json!({"stopped":true,"runs_preserved":true}))
    }
}

/// A result too large to send becomes an error. A `mutating` operation that
/// succeeded has applied its change, so its error says so.
fn bounded(result: Result<Value>, mutating: bool) -> Result<Value> {
    let result = result.map(|mut value| {
        crate::tools::content::normalize_content_ids_output(&mut value);
        value
    });
    match result {
        Ok(value) if serde_json::to_vec(&value)?.len() > protocol::MAX_FRAME_BYTES / 2 => {
            Err(if mutating {
                AppError::new(
                    "result_too_large",
                    "operation finished and its change was applied, but its result exceeds the response budget; do not retry it: use bounded inspection or export",
                )
                .details(json!({"committed":true}))
            } else {
                AppError::new(
                    "result_too_large",
                    "operation finished, but its result exceeds the response budget; use bounded inspection or export",
                )
            })
        }
        other => other,
    }
}

/// The same schema supplied to Pi also validates incoming arguments in Rust.
fn validate_arguments(schema: &Value, value: &Value, path: &str) -> Result<()> {
    if schema == &Value::Bool(false) {
        return Err(AppError::invalid(format!("{path} is not permitted")));
    }
    if let Some(clauses) = schema.get("allOf").and_then(Value::as_array) {
        for clause in clauses {
            validate_arguments(clause, value, path)?;
        }
    }
    if let Some(expected) = schema.get("const")
        && expected != value
    {
        return Err(AppError::invalid(format!("{path} must equal {expected}")));
    }
    if let Some(excluded) = schema.get("not")
        && validate_arguments(excluded, value, path).is_ok()
    {
        return Err(AppError::invalid(format!(
            "{path} matches a forbidden form"
        )));
    }
    if let Some(choices) = schema.get("oneOf").and_then(Value::as_array)
        && choices
            .iter()
            .filter(|s| validate_arguments(s, value, path).is_ok())
            .count()
            != 1
    {
        return Err(AppError::invalid(format!(
            "{path} must match exactly one permitted form"
        )));
    }
    if let Some(choices) = schema.get("anyOf").and_then(Value::as_array)
        && !choices
            .iter()
            .any(|s| validate_arguments(s, value, path).is_ok())
    {
        return Err(AppError::invalid(format!(
            "{path} does not match any permitted form"
        )));
    }
    if let Some(choices) = schema.get("enum").and_then(Value::as_array)
        && !choices.contains(value)
    {
        return Err(AppError::invalid(format!(
            "{path} has an unsupported value"
        )));
    }
    if let Some(kind) = schema.get("type") {
        let matches = |kind: &str| match kind {
            "object" => value.is_object(),
            "array" => value.is_array(),
            "string" => value.is_string(),
            "boolean" => value.is_boolean(),
            "number" => value.is_number(),
            "integer" => value.is_i64() || value.is_u64(),
            "null" => value.is_null(),
            _ => true,
        };
        let valid = kind.as_str().map(matches).unwrap_or_else(|| {
            kind.as_array()
                .is_some_and(|kinds| kinds.iter().filter_map(Value::as_str).any(matches))
        });
        if !valid {
            return Err(AppError::invalid(format!("{path} must be {kind}")));
        }
    }
    if let Some(object) = value.as_object() {
        if let Some(required) = schema.get("required").and_then(Value::as_array) {
            for field in required.iter().filter_map(Value::as_str) {
                if !object.contains_key(field) {
                    return Err(AppError::invalid(format!("{path}.{field} is required")));
                }
            }
        }
        for (key, value) in object {
            if let Some(property) = schema.get("properties").and_then(|p| p.get(key)) {
                validate_arguments(property, value, &format!("{path}.{key}"))?;
            } else if let Some(additional) = schema.get("additionalProperties") {
                validate_arguments(additional, value, &format!("{path}.{key}"))?;
            }
        }
    }
    if let (Some(pattern), Some(text)) = (
        schema.get("pattern").and_then(Value::as_str),
        value.as_str(),
    ) {
        let pattern = regex::Regex::new(pattern)
            .map_err(|e| AppError::new("invalid_tool_schema", e.to_string()))?;
        if !pattern.is_match(text) {
            return Err(AppError::invalid(format!(
                "{path} does not match its required pattern"
            )));
        }
    }
    if let (Some(items), Some(values)) = (schema.get("items"), value.as_array()) {
        for (index, value) in values.iter().enumerate() {
            validate_arguments(items, value, &format!("{path}[{index}]"))?;
        }
    }
    for (size, min, max) in [
        (value.as_array().map(Vec::len), "minItems", "maxItems"),
        (
            value.as_str().map(|s| s.chars().count()),
            "minLength",
            "maxLength",
        ),
    ] {
        if let Some(size) = size
            && (schema
                .get(min)
                .and_then(Value::as_u64)
                .is_some_and(|n| (size as u64) < n)
                || schema
                    .get(max)
                    .and_then(Value::as_u64)
                    .is_some_and(|n| (size as u64) > n))
        {
            return Err(AppError::invalid(format!(
                "{path} length is outside its bounds"
            )));
        }
    }
    if let Some(number) = value.as_f64() {
        for (field, lower) in [("minimum", true), ("maximum", false)] {
            if let Some(bound) = schema.get(field).and_then(Value::as_f64)
                && ((lower && number < bound) || (!lower && number > bound))
            {
                return Err(AppError::invalid(format!("{path} exceeds its {field}")));
            }
        }
    }
    Ok(())
}

/// Counts a connection as open while it lives.
struct Open<'a>(&'a AtomicUsize);

impl Drop for Open<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The data directory's lock and the server's socket. On exit the socket
/// goes, then its directory if nothing else is left in it, and only then
/// the lock: a server waiting for it makes the directory again.
struct Ownership {
    _lock: File,
    socket: PathBuf,
}
impl Drop for Ownership {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.socket);
        if let Some(directory) = self.socket.parent() {
            let _ = std::fs::remove_dir(directory);
        }
    }
}

/// Remove what a server that ended abruptly left in `directory`: its socket
/// and its shells'. Only the server holding the lock may, as none of them
/// can be live.
fn remove_stale_sockets(directory: &Path) -> Result<()> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == "server.sock"
            || (name.ends_with(".sock") && (name.starts_with("pty-") || name.starts_with("pi-")))
        {
            let _ = std::fs::remove_file(entry.path());
        }
    }
    Ok(())
}

/// Raise this process's soft limit on open files toward its hard limit. A
/// server admits 256 connections besides its terminals, stores and sockets,
/// while launchd starts programs with a soft limit of 256. macOS refuses a
/// limit above its per-process maximum, so ask for at most its OPEN_MAX,
/// 10240; a limit that cannot be raised stays. Only the dedicated server
/// process calls this, never an embedding application.
pub fn raise_file_limit() {
    use nix::sys::resource::{Resource, getrlimit, setrlimit};
    const OPEN_MAX: nix::sys::resource::rlim_t = 10240;
    if let Ok((soft, hard)) = getrlimit(Resource::RLIMIT_NOFILE)
        && soft < hard.min(OPEN_MAX)
    {
        let _ = setrlimit(Resource::RLIMIT_NOFILE, hard.min(OPEN_MAX), hard);
    }
}

/// Serve until stopped. With an `idle_limit`, as for a background server,
/// also stop once nothing has run and no client has connected for that long.
/// A server under an external supervisor runs without one.
pub async fn serve(paths: Paths, idle_limit: Option<Duration>) -> Result<()> {
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(paths.root.join("server.lock"))?;
    let handover = Instant::now() + HANDOVER;
    loop {
        match lock.try_lock() {
            Ok(()) => break,
            Err(std::fs::TryLockError::WouldBlock) if Instant::now() < handover => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => {
                return Err(AppError::new(
                    "server_owned",
                    format!("another server owns this data directory: {error}"),
                ));
            }
        }
    }
    crate::migration::check_root_available(&paths.root)?;
    // A server that exited removed the directory before releasing the lock,
    // so it is made only now.
    paths.create_endpoint()?;
    let _ownership = Ownership {
        _lock: lock,
        socket: paths.socket.clone(),
    };
    remove_stale_sockets(paths.endpoint())?;
    let listener = UnixListener::bind(&paths.socket)?;
    std::fs::set_permissions(&paths.socket, std::fs::Permissions::from_mode(0o600))?;
    let server = Server::new(paths)?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut clients = JoinSet::new();
    let mut idle_check =
        tokio::time::interval(idle_limit.map_or(Duration::from_secs(1), |limit| {
            limit.min(Duration::from_secs(1))
        }));
    let mut idle_since: Option<Instant> = None;
    // Why accepting last failed, while it still fails.
    let mut accept_error: Option<String> = None;
    loop {
        tokio::select! {
            accepted = listener.accept(), if clients.len() < 256 => match accepted {
                Ok((socket, _)) => {
                    if let Some(error) = accept_error.take() {
                        // Recording the failure may have needed a descriptor too.
                        crate::logging::record(&server.service.paths.root, &format!("accepting connections again after: {error}"));
                    }
                    // A command's connections come and go between idle checks;
                    // any of them means the server is in use.
                    idle_since = None;
                    let server = server.clone();
                    clients.spawn(async move { let _ = connection(server,socket).await; });
                }
                // Running out of descriptors must not end the server: they
                // return as connections close. Pause rather than spin.
                Err(error) => {
                    let error = error.to_string();
                    if accept_error.as_ref() != Some(&error) {
                        crate::logging::record(&server.service.paths.root, &format!("cannot accept connections: {error}"));
                        accept_error = Some(error);
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
            _ = server.stopped.notified() => break,
            _ = terminate.recv() => { if let Err(error) = server.stop().await {crate::logging::record(&server.service.paths.root,&format!("shutdown failed; resources remain available: {error}"));} },
            _ = interrupt.recv() => { if let Err(error) = server.stop().await {crate::logging::record(&server.service.paths.root,&format!("shutdown failed; resources remain available: {error}"));} },
            _ = clients.join_next(), if !clients.is_empty() => {},
            _ = idle_check.tick(), if idle_limit.is_some() => {
                if !clients.is_empty() || server.busy() {
                    idle_since = None;
                } else if idle_limit.is_some_and(|limit| idle_since.get_or_insert_with(Instant::now).elapsed() >= limit) {
                    match server.stop().await {
                        Ok(_) => break,
                        Err(error) => {
                            crate::logging::record(&server.service.paths.root, &format!("idle shutdown failed: {error}"));
                            idle_since = None;
                        }
                    }
                }
            }
        }
    }
    drop(listener);
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        while clients.join_next().await.is_some() {}
    })
    .await;
    clients.abort_all();
    Ok(())
}

async fn connection(server: Arc<Server>, socket: UnixStream) -> Result<()> {
    server.connections.fetch_add(1, Ordering::AcqRel);
    let _open = Open(&server.connections);
    let (reader, writer) = socket.into_split();
    let writer = Arc::new(Mutex::new(writer));
    let mut reader = BufReader::new(reader);
    let mut requests = JoinSet::new();
    let slots = Arc::new(tokio::sync::Semaphore::new(32));
    let result = loop {
        while requests.try_join_next().is_some() {}
        let permit = slots
            .clone()
            .acquire_owned()
            .await
            .expect("connection semaphore stays open");
        // Never cancel read_frame midway: it owns the partial frame buffer.
        let frame = match protocol::read_frame(&mut reader).await {
            Ok(Some(frame)) => frame,
            Ok(None) => break Ok(()),
            Err(error) => break Err(error),
        };
        let parsed = std::str::from_utf8(&frame)
            .map_err(|e| AppError::invalid(e.to_string()))
            .and_then(|s| {
                declarations::parse_json::<Request>(s).map_err(|e| AppError::invalid(e.to_string()))
            });
        let server = server.clone();
        let writer = writer.clone();
        requests.spawn(async move {
            let _permit = permit;
            let response = match parsed {
                Ok(request) => {
                    let id = request.request_id.clone();
                    Response::new(
                        &server.service.server_id,
                        &id,
                        server.request(request).await,
                    )
                }
                Err(error) => Response::new(&server.service.server_id, "", Err(error)),
            };
            let _ = protocol::write_frame(&mut *writer.lock().await, &response).await;
        });
    };
    // A complete received request reaches admission even if its client closes immediately.
    while requests.join_next().await.is_some() {}
    result
}
