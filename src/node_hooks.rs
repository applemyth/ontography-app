//! Claude Code hook events for one live node, over a private socket.
//!
//! Claude runs `ontography node-hook <event>` for each hook a node configures.
//! The command forwards the hook's JSON input to the execution that owns the
//! socket and exits without output: hook output can add to Claude's prompt,
//! and exit status 2 would block it. A hook that cannot reach its execution
//! fails with status 1, which Claude reports without blocking anything.

use crate::{AppError, Result, protocol};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::mpsc,
    task::JoinHandle,
};

const SOCKET_ENV: &str = "ONTOGRAPHY_NODE_HOOK_SOCKET";
const TOKEN_ENV: &str = "ONTOGRAPHY_NODE_HOOK_TOKEN";
const TIMEOUT: Duration = Duration::from_secs(5);
/// Claude's own limit for one hook; ours finishes well within it.
const HOOK_TIMEOUT_SECS: u64 = 10;
/// Larger inputs arrive without their JSON; only their event name matters.
const MAX_INPUT_BYTES: u64 = 1024 * 1024;

/// The hook events a node listens to.
pub const EVENTS: [&str; 4] = ["SessionStart", "UserPromptSubmit", "Stop", "Notification"];

/// One hook invocation, in the order Claude ran them.
#[derive(Clone, Debug)]
pub struct HookEvent {
    pub name: String,
    /// Claude's hook input, or `Null` when it exceeded the forwarding limit.
    pub input: Value,
}

impl HookEvent {
    /// The submitted prompt text, for `UserPromptSubmit`.
    pub fn prompt(&self) -> Option<&str> {
        self.input["prompt"].as_str()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Frame {
    version: u32,
    token: String,
    event: String,
    input: Value,
}

pub struct NodeHooks {
    socket: PathBuf,
    token: String,
    task: JoinHandle<()>,
}

impl NodeHooks {
    /// Listen in a private directory. Events arrive in order on the receiver.
    pub fn bind(directory: &Path) -> Result<(Self, mpsc::Receiver<HookEvent>)> {
        let metadata = std::fs::symlink_metadata(directory)?;
        if !metadata.is_dir()
            || metadata.uid() != nix::unistd::geteuid().as_raw()
            || metadata.mode() & 0o077 != 0
        {
            return Err(AppError::new(
                "node_hooks",
                "Node hooks require a private owned directory",
            ));
        }
        let socket = directory.join(format!("hooks-{}.sock", uuid::Uuid::new_v4().simple()));
        let listener = UnixListener::bind(&socket)?;
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
        let token = uuid::Uuid::new_v4().to_string();
        let (events, receiver) = mpsc::channel(64);
        let expected = token.clone();
        // Connections are handled one at a time, so events keep Claude's order.
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                // A failed hook connection never ends the node execution.
                let _ = tokio::time::timeout(TIMEOUT, accept(stream, &expected, &events)).await;
                if events.is_closed() {
                    break;
                }
            }
        });
        Ok((
            Self {
                socket,
                token,
                task,
            },
            receiver,
        ))
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

    /// Claude settings that report every listened-to event through `program`.
    pub fn claude_settings(program: &Path) -> Value {
        let program = shell_quote(&program.to_string_lossy());
        let hooks: serde_json::Map<String, Value> = EVENTS
            .iter()
            .map(|event| {
                let command = json!({
                    "type": "command",
                    "command": format!("{program} node-hook {event}"),
                    "timeout": HOOK_TIMEOUT_SECS,
                });
                ((*event).to_owned(), json!([{"hooks": [command]}]))
            })
            .collect();
        json!({ "hooks": hooks })
    }
}

impl Drop for NodeHooks {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_file(&self.socket);
    }
}

/// Validate one hook connection. The event is acknowledged only after this
/// execution has accepted it, so a hook finishes after its event is recorded.
async fn accept(stream: UnixStream, token: &str, events: &mpsc::Sender<HookEvent>) -> Result<()> {
    if stream.peer_cred()?.uid() != nix::unistd::geteuid().as_raw() {
        return Err(AppError::new("node_hooks", "Hook peer has a different owner"));
    }
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let Some(bytes) = protocol::read_frame(&mut reader).await? else {
        return Ok(());
    };
    let frame: Frame = serde_json::from_slice(&bytes)?;
    if frame.version != 1 || frame.token != token {
        return Err(AppError::new("node_hooks", "Stale or invalid hook identity"));
    }
    if !EVENTS.contains(&frame.event.as_str()) {
        return Err(AppError::invalid(format!(
            "Unsupported hook event {:?}",
            frame.event
        )));
    }
    events
        .send(HookEvent {
            name: frame.event,
            input: frame.input,
        })
        .await
        .map_err(|_| AppError::new("node_hooks", "Node execution stopped"))?;
    protocol::write_frame(&mut writer, &json!({"ok": true})).await
}

fn timed_out() -> AppError {
    AppError::new("node_hooks", "Hook connection timed out")
}

/// Invoked by Claude as a command hook. It prints nothing and opens no store.
pub async fn run(event: &str) -> Result<()> {
    let socket = std::env::var_os(SOCKET_ENV)
        .ok_or_else(|| AppError::new("node_hooks", "Node hook socket is missing"))?;
    let token = std::env::var(TOKEN_ENV)
        .map_err(|_| AppError::new("node_hooks", "Node hook token is missing"))?;
    let mut bytes = Vec::new();
    tokio::io::stdin()
        .take(MAX_INPUT_BYTES + 1)
        .read_to_end(&mut bytes)
        .await?;
    let input = if bytes.len() as u64 > MAX_INPUT_BYTES {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    forward(Path::new(&socket), token, event, input).await
}

async fn forward(socket: &Path, token: String, event: &str, input: Value) -> Result<()> {
    let exchange = async {
        let stream = UnixStream::connect(socket).await?;
        let (reader, mut writer) = stream.into_split();
        protocol::write_frame(
            &mut writer,
            &Frame {
                version: 1,
                token,
                event: event.into(),
                input,
            },
        )
        .await?;
        protocol::read_frame(&mut BufReader::new(reader))
            .await?
            .ok_or_else(|| AppError::new("node_hooks", "Node execution is no longer available"))
    };
    tokio::time::timeout(TIMEOUT, exchange)
        .await
        .map_err(|_| timed_out())??;
    Ok(())
}

/// Quote one word for the POSIX shell that runs Claude's command hooks.
pub(crate) fn shell_quote(word: &str) -> String {
    format!("'{}'", word.replace('\'', r"'\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn directory() -> tempfile::TempDir {
        let directory = tempfile::tempdir_in("/tmp").unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
            .unwrap();
        directory
    }

    #[tokio::test]
    async fn events_arrive_in_order_and_only_with_the_current_identity() {
        let directory = directory();
        let (hooks, mut events) = NodeHooks::bind(directory.path()).unwrap();
        let environment = hooks.environment();
        let socket = PathBuf::from(&environment[SOCKET_ENV]);
        let token = environment[TOKEN_ENV].clone();
        forward(&socket, token.clone(), "SessionStart", json!({"source":"startup"}))
            .await
            .unwrap();
        forward(&socket, token.clone(), "UserPromptSubmit", json!({"prompt":"work"}))
            .await
            .unwrap();
        let first = events.recv().await.unwrap();
        assert_eq!(first.name, "SessionStart");
        assert_eq!(first.input["source"], "startup");
        let second = events.recv().await.unwrap();
        assert_eq!(second.prompt(), Some("work"));

        assert!(forward(&socket, "stale".into(), "Stop", json!({})).await.is_err());
        assert!(forward(&socket, token.clone(), "PreToolUse", json!({})).await.is_err());
        forward(&socket, token, "Stop", Value::Null).await.unwrap();
        let third = events.recv().await.unwrap();
        assert_eq!((third.name.as_str(), third.input.is_null()), ("Stop", true));
    }

    #[test]
    fn settings_run_one_quoted_command_per_event() {
        let settings = NodeHooks::claude_settings(Path::new("/opt/it's here/ontography"));
        for event in EVENTS {
            let hooks = &settings["hooks"][event];
            assert_eq!(hooks.as_array().unwrap().len(), 1);
            assert_eq!(hooks[0]["hooks"][0]["type"], "command");
            assert_eq!(
                hooks[0]["hooks"][0]["command"],
                format!(r"'/opt/it'\''s here/ontography' node-hook {event}")
            );
        }
    }
}
