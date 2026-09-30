//! MCP over stdio, backed by one live node's shared tool context.
//!
//! Codex starts a small stdio proxy. The execution owns the private socket and
//! tool context; the proxy has no management API or choice of node identity.
//! Delivery acknowledgements follow stdout flush, so receipts cover the exact
//! text returned to the MCP client, not just an internal socket write.

use crate::{
    AppError, Result,
    node_tool::{NodeToolContext, Reply},
    protocol,
    workflow::components::McpServer,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::BufReader,
    net::{UnixListener, UnixStream},
    task::{AbortHandle, JoinHandle, JoinSet},
    time::Instant,
};

const SOCKET_ENV: &str = "ONTOGRAPHY_NODE_MCP_SOCKET";
const TOKEN_ENV: &str = "ONTOGRAPHY_NODE_MCP_TOKEN";
const VERSIONS: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CALLS: usize = 16;
/// How long a node's socket waits after an accept error to accept again.
const ACCEPT_PAUSE: Duration = Duration::from_millis(100);

pub struct NodeMcp {
    socket: PathBuf,
    token: String,
    task: JoinHandle<()>,
    stop: tokio::sync::watch::Sender<bool>,
}

impl NodeMcp {
    /// Listen in a private directory. Accept errors are recorded in the
    /// server log of the data directory `log`, when given.
    pub fn bind(
        directory: &Path,
        context: Arc<NodeToolContext>,
        log: Option<&Path>,
    ) -> Result<Self> {
        let metadata = std::fs::symlink_metadata(directory)?;
        if !metadata.is_dir()
            || metadata.uid() != nix::unistd::geteuid().as_raw()
            || metadata.mode() & 0o077 != 0
        {
            return Err(AppError::new(
                "node_mcp",
                "Node MCP requires a private owned directory",
            ));
        }
        let socket = directory.join(format!("mcp-{}.sock", uuid::Uuid::new_v4().simple()));
        let listener = UnixListener::bind(&socket)?;
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
        let token = uuid::Uuid::new_v4().to_string();
        let expected = token.clone();
        let (stop, mut stopping) = tokio::sync::watch::channel(false);
        let mut errors = AcceptLog::new(&socket, log);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    _ = stopping.changed() => break,
                    (stream, _) = next_connection(|| listener.accept(), &mut errors), if connections.len() < 8 => {
                        let context = context.clone();
                        let token = expected.clone();
                        connections.spawn(async move {
                            // A failed client connection never ends the node execution.
                            let _ = connection(stream, &token, context).await;
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Ok(Self {
            socket,
            token,
            task,
            stop,
        })
    }

    pub async fn shutdown(&mut self) {
        self.stop.send_replace(true);
        let _ = (&mut self.task).await;
    }

    /// The node tools as a stdio MCP server, for agents that take a server
    /// list: this application's `node-mcp` proxy, bound to this socket.
    pub fn server(&self) -> Result<McpServer> {
        let program = crate::launcher::application_executable()?;
        let command = program
            .to_str()
            .ok_or_else(|| AppError::invalid("MCP executable path must be UTF-8"))?;
        Ok(McpServer {
            command: command.into(),
            args: vec!["node-mcp".into()],
            env: self.environment(),
        })
    }

    pub fn environment(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                SOCKET_ENV.into(),
                self.socket.to_string_lossy().into_owned(),
            ),
            (TOKEN_ENV.into(), self.token.clone()),
        ])
    }

    /// Process-local transport overrides. Set individual fields so the user's
    /// per-server/tool approval policy is retained; never rewrite their config.
    pub fn codex_overrides(&self) -> Result<Vec<String>> {
        let program = crate::launcher::application_executable()?;
        let program = program
            .to_str()
            .ok_or_else(|| AppError::invalid("MCP executable path must be UTF-8"))?;
        let socket = self
            .socket
            .to_str()
            .ok_or_else(|| AppError::invalid("MCP socket path must be UTF-8"))?;
        Ok(vec![
            format!(
                "mcp_servers.ontography_node.command={}",
                serde_json::to_string(program)?
            ),
            "mcp_servers.ontography_node.args=[\"node-mcp\"]".into(),
            format!(
                "mcp_servers.ontography_node.env.{SOCKET_ENV}={}",
                serde_json::to_string(socket)?
            ),
            format!(
                "mcp_servers.ontography_node.env.{TOKEN_ENV}={}",
                serde_json::to_string(&self.token)?
            ),
            "mcp_servers.ontography_node.enabled=true".into(),
            "mcp_servers.ontography_node.required=true".into(),
            "mcp_servers.ontography_node.startup_timeout_sec=10".into(),
        ])
    }
}

impl Drop for NodeMcp {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_file(&self.socket);
    }
}

/// What a node's socket has logged of its accept errors.
pub(crate) struct AcceptLog {
    socket: PathBuf,
    /// The data directory whose server log records them.
    root: Option<PathBuf>,
    last: Option<String>,
}

impl AcceptLog {
    pub(crate) fn new(socket: &Path, root: Option<&Path>) -> Self {
        Self {
            socket: socket.to_owned(),
            root: root.map(Path::to_owned),
            last: None,
        }
    }
}

/// The next connection `accept` yields. An accept error, such as running out
/// of file descriptors, never closes a node's socket: it is logged unless it
/// repeats the last one, and accepting resumes after a pause, so it cannot
/// spin.
pub(crate) async fn next_connection<T, F: Future<Output = std::io::Result<T>>>(
    mut accept: impl FnMut() -> F,
    log: &mut AcceptLog,
) -> T {
    loop {
        match accept().await {
            Ok(connection) => return connection,
            Err(error) => {
                let error = error.to_string();
                if log.last.as_ref() != Some(&error) {
                    if let Some(root) = &log.root {
                        let socket = log.socket.display();
                        let message = format!("{socket} could not accept a connection: {error}");
                        crate::logging::record(root, &message);
                    }
                    log.last = Some(error);
                }
                tokio::time::sleep(ACCEPT_PAUSE).await;
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Hello {
    version: u32,
    token: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Outbound {
    message: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delivery: Option<u64>,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum Inbound {
    Message { message: Value },
    Delivered { delivered: u64 },
}

struct Pending {
    reply: Reply,
    written: Instant,
}

/// A framing read must survive other events in a select loop. In particular,
/// a notification or completed call must not discard a partially read frame.
struct Frames {
    receiver: tokio::sync::mpsc::Receiver<Result<Vec<u8>>>,
    task: JoinHandle<()>,
}

impl Frames {
    fn new(mut reader: BufReader<tokio::net::unix::OwnedReadHalf>) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(8);
        let task = tokio::spawn(async move {
            loop {
                match protocol::read_frame(&mut reader).await {
                    Ok(Some(bytes)) => {
                        if sender.send(Ok(bytes)).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        break;
                    }
                }
            }
        });
        Self { receiver, task }
    }
}

impl Drop for Frames {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Calls {
    tasks: JoinSet<(Value, Result<Reply>)>,
    active: BTreeMap<String, (AbortHandle, bool)>,
}

impl Drop for Calls {
    fn drop(&mut self) {
        // A transport disconnect is not rollback. Accepted mutations settle
        // under the execution's existing lifecycle, without replay. Their
        // unwritten replies drop as prepared receipts. Reads/waits can stop.
        for (task, mutating) in self.active.values() {
            if !mutating {
                task.abort();
            }
        }
        self.tasks.detach_all();
    }
}

fn success(id: Value, result: Value) -> Value {
    json!({"jsonrpc":"2.0", "id":id, "result":result})
}

fn rpc_error(id: Value, code: i32, message: &str) -> Value {
    json!({"jsonrpc":"2.0", "id":id, "error":{"code":code,"message":message}})
}

fn catalog(context: &NodeToolContext) -> Value {
    json!({"tools": context.catalog().iter().map(|tool| json!({
        "name":tool.name, "description":tool.description, "inputSchema":tool.parameters(),
        "annotations":{"readOnlyHint":!tool.mutating,"destructiveHint":tool.mutating,"openWorldHint":false}
    })).collect::<Vec<_>>()})
}

async fn write(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    message: Value,
    delivery: Option<u64>,
) -> Result<()> {
    tokio::time::timeout(
        WRITE_TIMEOUT,
        protocol::write_frame(writer, &Outbound { message, delivery }),
    )
    .await
    .map_err(|_| AppError::new("node_mcp", "MCP client stopped reading"))?
}

async fn connection(stream: UnixStream, token: &str, context: Arc<NodeToolContext>) -> Result<()> {
    if stream.peer_cred()?.uid() != nix::unistd::geteuid().as_raw() {
        return Err(AppError::new("node_mcp", "MCP peer has a different owner"));
    }
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let authenticated = tokio::time::timeout(WRITE_TIMEOUT, async {
        let bytes = protocol::read_frame(&mut reader)
            .await?
            .ok_or_else(|| AppError::invalid("Missing MCP connection identity"))?;
        let hello: Hello = serde_json::from_slice(&bytes)?;
        if hello.version != 1 || hello.token != token {
            return Err(AppError::new(
                "node_mcp",
                "Stale or invalid MCP connection identity",
            ));
        }
        protocol::write_frame(&mut writer, &json!({"ready":true})).await
    })
    .await
    .map_err(|_| AppError::new("node_mcp", "MCP authentication timed out"))?;
    authenticated?;
    let mut frames = Frames::new(reader);

    let mut initialized = false;
    let mut ready = false;
    let mut scope = context.scope_changes();
    let mut last_catalog = catalog(&context);
    let mut calls = Calls {
        tasks: JoinSet::new(),
        active: BTreeMap::new(),
    };
    let mut pending = BTreeMap::<u64, Pending>::new();
    let mut sequence = 0u64;
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            incoming = frames.receiver.recv() => {
                let Some(bytes) = incoming else { return Ok(()); };
                let incoming: Inbound = serde_json::from_slice(&bytes?)?;
                let message = match incoming {
                    Inbound::Delivered { delivered } => {
                        // The reply was already written, so a mark that fails,
                        // as when its attempt ended first, leaves its receipt
                        // prepared, like a lost acknowledgement. Neither that
                        // nor an unknown acknowledgement affects other calls.
                        if let Some(reply) = pending.remove(&delivered) {
                            let _ = reply.reply.sent().await;
                        }
                        continue;
                    }
                    Inbound::Message { message } => message,
                };
                let id = message.get("id").cloned();
                let method = message.get("method").and_then(Value::as_str);
                if message.get("jsonrpc") != Some(&json!("2.0")) || method.is_none()
                    || id.as_ref().is_some_and(|id| !id.is_string() && id.as_i64().is_none() && id.as_u64().is_none())
                    || message.get("params").is_some_and(|params| !params.is_object())
                {
                    write(&mut writer, rpc_error(Value::Null, -32600, "Invalid JSON-RPC request"), None).await?;
                    continue;
                }
                let method = method.expect("checked method");
                let params = message.get("params").cloned().unwrap_or_else(|| json!({}));
                let Some(id) = id else {
                    match method {
                        "notifications/initialized" if initialized => {
                            ready = true;
                            last_catalog = catalog(&context);
                        }
                        "notifications/cancelled" if ready => {
                            if let Some(id) = params.get("requestId") {
                                let key = id.to_string();
                                if let Some((task, false)) = calls.active.get(&key) {
                                    task.abort();
                                    calls.active.remove(&key);
                                }
                            }
                        }
                        _ => {}
                    }
                    continue;
                };
                if calls.active.contains_key(&id.to_string()) {
                    // Do not send a second response using an active request ID.
                    return Err(AppError::invalid("Duplicate active MCP request id"));
                }
                let response = match method {
                    "initialize" if !initialized => {
                        if params.get("protocolVersion").and_then(Value::as_str).is_none()
                            || !params.get("capabilities").is_some_and(Value::is_object)
                            || !params.get("clientInfo").is_some_and(Value::is_object) {
                            rpc_error(id, -32602, "Invalid initialization parameters")
                        } else {
                            initialized = true;
                            let requested = params["protocolVersion"].as_str().unwrap();
                            let version = if VERSIONS.contains(&requested) { requested } else { VERSIONS[0] };
                            success(id, json!({"protocolVersion":version,"capabilities":{"tools":{"listChanged":true}},
                                "serverInfo":{"name":"ontography-node","version":env!("CARGO_PKG_VERSION")},
                                "instructions":"These tools act only for this graph node. An incoming ontography_message already includes an open attempt_id: use that attempt and its input handles. For additional work, use inspect_node and next_trigger, then begin_invocation before reading packages or publishing a result."}))
                        }
                    }
                    "ping" => success(id, json!({})),
                    _ if !ready => rpc_error(id, -32000, "Initialize this MCP connection first"),
                    "tools/list" => {
                        if params.get("cursor").is_some() { rpc_error(id, -32602, "This tool catalog has no continuation cursor") }
                        else { success(id, catalog(&context)) }
                    }
                    "tools/call" => {
                        let name = params.get("name").and_then(Value::as_str);
                        let args = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
                        let tool = context.catalog().into_iter().find(|tool| Some(tool.name) == name);
                        if let Some(tool) = tool.filter(|_| args.is_object()) {
                            if calls.active.len() + pending.len() >= MAX_CALLS {
                                rpc_error(id, -32000, "Too many outstanding tool calls")
                            } else {
                                let name = tool.name;
                                let key = id.to_string();
                                let context = context.clone();
                                let task = calls.tasks.spawn(async move { (id, context.call(name, args).await) });
                                calls.active.insert(key, (task, tool.mutating));
                                continue;
                            }
                        } else { rpc_error(id, -32602, "Unknown, unavailable, or invalid node tool call") }
                    }
                    _ => rpc_error(id, -32601, "Method not found"),
                };
                write(&mut writer, response, None).await?;
            }
            completed = calls.tasks.join_next(), if !calls.tasks.is_empty() => {
                match completed.expect("pending call") {
                    Ok((id, result)) => {
                        if calls.active.remove(&id.to_string()).is_none() { continue; }
                        let (result, delivery) = match result {
                            Ok(reply) => {
                                let text = std::str::from_utf8(reply.bytes()).map_err(|_| AppError::invalid("Node tool reply is not UTF-8"))?.to_owned();
                                sequence += 1;
                                let delivery = sequence;
                                pending.insert(delivery, Pending { reply, written: Instant::now() });
                                (json!({"content":[{"type":"text","text":text}],"isError":false}), Some(delivery))
                            }
                            Err(error) => (json!({"content":[{"type":"text","text":serde_json::to_string(&error)?}],"isError":true}), None),
                        };
                        write(&mut writer, success(id, result), delivery).await?;
                    }
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => return Err(AppError::new("node_mcp", error.to_string())),
                }
            }
            changed = scope.changed() => {
                if changed.is_err() { return Ok(()); }
                let current = catalog(&context);
                if ready && current != last_catalog {
                    write(&mut writer, json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed"}), None).await?;
                }
                last_catalog = current;
            }
            _ = tick.tick() => {
                if pending.values().any(|reply| reply.written.elapsed() >= DELIVERY_TIMEOUT) {
                    return Err(AppError::new("node_mcp", "MCP delivery acknowledgement timed out"));
                }
            }
        }
    }
}

/// Invoked by Codex as a stdio MCP server. No storage or management connection
/// is opened here, and a lost connection is never automatically replayed.
pub async fn run_stdio() -> Result<()> {
    use std::io::{BufRead, Read};
    let socket = std::env::var_os(SOCKET_ENV)
        .ok_or_else(|| AppError::new("node_mcp", "Node MCP socket is missing"))?;
    let token = std::env::var(TOKEN_ENV)
        .map_err(|_| AppError::new("node_mcp", "Node MCP token is missing"))?;
    let stream = UnixStream::connect(PathBuf::from(socket)).await?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    protocol::write_frame(&mut writer, &Hello { version: 1, token }).await?;
    let ready = tokio::time::timeout(WRITE_TIMEOUT, protocol::read_frame(&mut reader))
        .await
        .map_err(|_| AppError::new("node_mcp", "Node MCP connection timed out"))??;
    if ready
        .as_deref()
        .map(serde_json::from_slice::<Value>)
        .transpose()?
        != Some(json!({"ready":true}))
    {
        return Err(AppError::new(
            "node_mcp",
            "Node execution is no longer available",
        ));
    }
    let mut frames = Frames::new(reader);
    // A dedicated OS thread keeps a blocked stdin read out of Tokio's blocking
    // pool: server loss must let this process exit even while stdin stays open.
    // A line too long for a frame arrives as `None`, the rest of it skipped.
    let (input, mut incoming) = tokio::sync::mpsc::channel(8);
    std::thread::spawn(move || {
        let mut stdin = std::io::stdin().lock();
        loop {
            let mut bytes = Vec::new();
            let result = (&mut stdin)
                .take(protocol::MAX_FRAME_BYTES as u64 + 1)
                .read_until(b'\n', &mut bytes);
            let line = match result {
                Ok(0) => break,
                Ok(_) if bytes.len() > protocol::MAX_FRAME_BYTES => {
                    if bytes.ends_with(b"\n") {
                        Ok(None)
                    } else {
                        stdin
                            .skip_until(b'\n')
                            .map(|_| None)
                            .map_err(AppError::from)
                    }
                }
                Ok(_) if bytes.ends_with(b"\n") => Ok(Some(bytes)),
                _ => Err(AppError::new("node_mcp", "Invalid MCP input frame")),
            };
            let failed = line.is_err();
            if input.blocking_send(line).is_err() || failed {
                break;
            }
        }
    });
    let too_large = format!(
        "An MCP request can be at most {} bytes",
        protocol::MAX_FRAME_BYTES
    );
    loop {
        tokio::select! {
            input = incoming.recv() => {
                let Some(line) = input else { return Ok(()); };
                let Some(bytes) = line? else {
                    print(&rpc_error(Value::Null, -32600, &too_large))?;
                    continue;
                };
                let message: Value = match serde_json::from_slice(&bytes) {
                    Ok(value) => value,
                    Err(_) => {
                        print(&rpc_error(Value::Null, -32700, "Parse error"))?;
                        continue;
                    }
                };
                let id = message.get("id").cloned();
                match protocol::write_frame(&mut writer, &Inbound::Message { message }).await {
                    // Wrapped for the node, the request outgrew one frame;
                    // nothing was sent.
                    Err(error) if error.code == "result_too_large" => {
                        if let Some(id) = id {
                            print(&rpc_error(id, -32600, &too_large))?;
                        }
                    }
                    sent => sent?,
                }
            }
            output = frames.receiver.recv() => {
                let bytes = output.ok_or_else(|| AppError::new("node_mcp", "Node execution disconnected"))??;
                let response: Outbound = serde_json::from_slice(&bytes)?;
                print(&response.message)?;
                if let Some(delivered) = response.delivery {
                    protocol::write_frame(&mut writer, &Inbound::Delivered { delivered }).await?;
                }
            }
        }
    }
}

/// Writes one message to the MCP client, and flushes it.
fn print(message: &Value) -> Result<()> {
    use std::io::Write;
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, message)?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nix::errno::Errno;
    use std::collections::VecDeque;

    #[tokio::test(start_paused = true)]
    async fn accept_errors_pause_and_are_logged_unless_repeated() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("logs")).unwrap();
        let mut log = AcceptLog::new(Path::new("mcp.sock"), Some(root.path()));
        let failed = |errno| Err(std::io::Error::from(errno));
        let mut results = VecDeque::from([
            failed(Errno::EMFILE),
            failed(Errno::EMFILE),
            failed(Errno::ECONNABORTED),
            Ok(1),
            failed(Errno::ECONNABORTED),
            Ok(2),
        ]);
        let started = Instant::now();
        let mut accept = || std::future::ready(results.pop_front().unwrap());
        assert_eq!(next_connection(&mut accept, &mut log).await, 1);
        assert_eq!(started.elapsed(), 3 * ACCEPT_PAUSE);
        assert_eq!(next_connection(&mut accept, &mut log).await, 2);
        let logged = std::fs::read_to_string(root.path().join("logs/server.log")).unwrap();
        let logged: Vec<_> = logged.lines().collect();
        assert_eq!(logged.len(), 2, "{logged:?}");
        assert!(logged[0].contains(&format!(
            "mcp.sock could not accept a connection: {}",
            std::io::Error::from(Errno::EMFILE)
        )));
        assert!(logged[1].contains(&std::io::Error::from(Errno::ECONNABORTED).to_string()));
    }
}
