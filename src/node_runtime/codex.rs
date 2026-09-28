//! Native Codex conversation creation and interactive terminal launch settings.
//!
//! A short-lived app-server creates (or validates) the saved conversation without
//! starting a model turn. The terminal then resumes that exact conversation. This
//! preserves the user's existing Codex authentication/configuration and avoids
//! guessing a conversation from terminal output or `resume --last`.

use crate::{
    AppError, Result,
    persistence::{read_json, write_json},
    workflow::document::DocumentNode,
};
use nix::{sys::signal::Signal, unistd::Pid};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
};

const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(30);
const FRAME_LIMIT: u64 = 4 * 1024 * 1024;

#[derive(Debug)]
pub struct PreparedSession {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub conversation_id: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedSession {
    version: u32,
    node_id: String,
    cwd: PathBuf,
    codex_home: PathBuf,
    conversation_id: String,
}

/// Prepare a native conversation, or pass through an explicit custom runner.
/// The caller owns node-directory serialization and terminal process lifetime.
pub async fn prepare(node: &DocumentNode, node_dir: &Path, cwd: &Path) -> Result<PreparedSession> {
    if let Some(argv) = node.config.get("argv") {
        let argv = argv
            .as_array()
            .filter(|argv| !argv.is_empty())
            .ok_or_else(|| AppError::invalid("agent argv must be a nonempty array of strings"))?;
        let argv: Vec<_> = argv
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| AppError::invalid("agent argv must contain strings"))
            })
            .collect::<Result<_>>()?;
        if argv[0].trim().is_empty() {
            return Err(AppError::invalid("agent program must not be empty"));
        }
        return Ok(PreparedSession {
            program: argv[0].clone().into(),
            args: argv[1..].to_vec(),
            env: BTreeMap::new(),
            conversation_id: None,
        });
    }
    let native_home = std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|path| PathBuf::from(path).join(".codex")))
        .ok_or_else(|| error("Codex requires HOME or CODEX_HOME"))?;
    prepare_native(node, node_dir, cwd, Path::new("codex"), &native_home).await
}

async fn prepare_native(
    node: &DocumentNode,
    node_dir: &Path,
    cwd: &Path,
    program: &Path,
    native_home: &Path,
) -> Result<PreparedSession> {
    let cwd = std::fs::canonicalize(cwd)?;
    // Child processes change cwd to the node workspace. A relative native home
    // would then point somewhere different from the parent's saved identity.
    if !native_home.is_absolute() {
        return Err(error(
            "CODEX_HOME must be absolute for managed Codex sessions",
        ));
    }
    let native_home = if native_home.exists() {
        std::fs::canonicalize(native_home)?
    } else {
        native_home.to_path_buf()
    };
    let metadata_path = node_dir.join("codex-session.json");
    let saved = if metadata_path.try_exists()? {
        Some(read_json::<SavedSession>(&metadata_path)?)
    } else {
        None
    };
    if let Some(saved) = &saved {
        if saved.version != 1
            || saved.node_id != node.id
            || saved.cwd != cwd
            || saved.codex_home != native_home
        {
            return Err(error(
                "Saved Codex conversation belongs to a different node, workspace, or Codex home",
            ));
        }
        validate_id(&saved.conversation_id)?;
    }
    let mut rpc = Bootstrap::spawn(program, node_dir, &cwd)?;
    let result = tokio::time::timeout(BOOTSTRAP_TIMEOUT, async {
        rpc.call(
            "initialize",
            json!({
                "clientInfo":{"name":"ontography_node","version":env!("CARGO_PKG_VERSION")},
                "capabilities":{"experimentalApi":true}
            }),
        )
        .await?;
        rpc.send(&json!({"method":"initialized"})).await?;
        let mut params = json!({
            "cwd":cwd,
            "developerInstructions":node.config.get("prompt").and_then(Value::as_str).unwrap_or("")
        });
        if let Some(model) = node.config.get("model").and_then(Value::as_str) {
            params["model"] = json!(model);
        }
        let conversation_id = if let Some(saved) = &saved {
            saved.conversation_id.clone()
        } else {
            let mut start = params.clone();
            start["ephemeral"] = json!(false);
            start["historyMode"] = json!("legacy");
            let created = rpc.call("thread/start", start).await?;
            let id = response_id(&created)?;
            // Codex lazily persists an empty thread. Naming it writes the
            // initial rollout without injecting a prompt or starting a turn.
            rpc.call(
                "thread/name/set",
                json!({"threadId":id,"name":format!("Ontography: {}", node.id)}),
            )
            .await?;
            // Release the live thread before resuming it. This verifies that
            // the empty conversation can be loaded from persistent storage.
            rpc.call("thread/unsubscribe", json!({"threadId":id}))
                .await?;
            id
        };
        params["threadId"] = json!(conversation_id);
        let resumed = rpc.call("thread/resume", params).await?;
        if response_id(&resumed)? != conversation_id {
            return Err(error("Codex resumed a different conversation"));
        }
        rpc.call("thread/unsubscribe", json!({"threadId":conversation_id}))
            .await?;
        // A first Codex launch can create its home. Record the canonical path
        // now so an absolute path through a symlink remains stable on restart.
        let native_home = if native_home.try_exists()? {
            std::fs::canonicalize(&native_home)?
        } else {
            native_home
        };
        write_json(
            &metadata_path,
            &SavedSession {
                version: 1,
                node_id: node.id.clone(),
                cwd: cwd.clone(),
                codex_home: native_home,
                conversation_id: conversation_id.clone(),
            },
        )?;
        Ok(conversation_id)
    })
    .await
    .map_err(|_| error("Codex conversation initialization timed out"))?;
    // Closing stdio permits app-server to flush and leave before its interactive
    // successor opens the conversation. Errors/cancellation kill its process group.
    let shutdown = rpc.close().await;
    let conversation_id = result?;
    shutdown?;
    let prompt = node
        .config
        .get("prompt")
        .and_then(Value::as_str)
        .unwrap_or("");
    let mut args = vec![
        "--no-daemon".into(),
        "-c".into(),
        format!("developer_instructions={}", serde_json::to_string(prompt)?),
    ];
    if let Some(model) = node.config.get("model").and_then(Value::as_str) {
        args.extend(["--model".into(), model.into()]);
    }
    args.extend([
        "resume".into(),
        conversation_id.clone(),
        "--cd".into(),
        cwd.to_str()
            .ok_or_else(|| error("Codex workspace path must be UTF-8"))?
            .into(),
    ]);
    Ok(PreparedSession {
        program: program.to_path_buf(),
        args,
        env: BTreeMap::from([(
            "ONTOGRAPHY_CODEX_CONVERSATION_ID".into(),
            conversation_id.clone(),
        )]),
        conversation_id: Some(conversation_id),
    })
}

fn response_id(response: &Value) -> Result<String> {
    let id = response["thread"]["id"]
        .as_str()
        .ok_or_else(|| error("Codex did not return a conversation id"))?;
    validate_id(id)?;
    Ok(id.into())
}

fn validate_id(id: &str) -> Result<()> {
    uuid::Uuid::parse_str(id)
        .map(|_| ())
        .map_err(|_| error("Codex conversation id must be a UUID"))
}

fn error(message: impl Into<String>) -> AppError {
    AppError::new("codex_session", message)
}

/// Deliberately limited protocol client: initialize and persist a conversation;
/// never start a turn, answer agent approvals, or run graph operations.
struct Bootstrap {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
    reaped: bool,
}

impl Bootstrap {
    fn spawn(program: &Path, node_dir: &Path, cwd: &Path) -> Result<Self> {
        std::fs::create_dir_all(node_dir)?;
        let stderr = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(node_dir.join("codex-bootstrap.log"))?;
        let mut child = Command::new(program)
            .args(["app-server", "--listen", "stdio://"])
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(stderr)
            .process_group(0)
            .kill_on_drop(true)
            .spawn()
            .map_err(|cause| error(format!("Cannot start Codex: {cause}")))?;
        Ok(Self {
            stdin: child.stdin.take(),
            stdout: BufReader::new(child.stdout.take().expect("piped Codex stdout")),
            child,
            next_id: 0,
            reaped: false,
        })
    }

    async fn send(&mut self, message: &Value) -> Result<()> {
        let mut bytes = serde_json::to_vec(message)?;
        if bytes.len() as u64 > FRAME_LIMIT {
            return Err(error("Codex request exceeds the frame limit"));
        }
        bytes.push(b'\n');
        self.stdin
            .as_mut()
            .ok_or_else(|| error("Codex input closed"))?
            .write_all(&bytes)
            .await?;
        Ok(())
    }

    async fn call(&mut self, method: &str, params: Value) -> Result<Value> {
        self.next_id += 1;
        let id = self.next_id;
        self.send(&json!({"id":id,"method":method,"params":params}))
            .await?;
        loop {
            let mut frame = Vec::new();
            (&mut self.stdout)
                .take(FRAME_LIMIT + 1)
                .read_until(b'\n', &mut frame)
                .await?;
            if frame.is_empty() {
                return Err(error(format!(
                    "Codex closed during {method}; inspect codex-bootstrap.log"
                )));
            }
            if frame.len() as u64 > FRAME_LIMIT {
                return Err(error("Codex response exceeds the frame limit"));
            }
            let response: Value = serde_json::from_slice(&frame)
                .map_err(|cause| error(format!("Invalid Codex response: {cause}")))?;
            if response.get("id") == Some(&json!(id)) {
                if let Some(failure) = response.get("error") {
                    return Err(error(format!("Codex {method} failed: {failure}")));
                }
                return response
                    .get("result")
                    .cloned()
                    .ok_or_else(|| error(format!("Codex {method} response has no result")));
            }
            if response.get("id").is_some() && response.get("method").is_some() {
                return Err(error(
                    "Codex requested interaction while preparing a conversation",
                ));
            }
            // Notifications can be interleaved with the requested response.
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.stdin.take();
        let closed = tokio::time::timeout(Duration::from_secs(3), async {
            // Continue draining notifications so a full stdout pipe cannot
            // prevent app-server from completing its shutdown.
            let mut sink = tokio::io::sink();
            tokio::io::copy(&mut self.stdout, &mut sink).await?;
            self.child.wait().await
        })
        .await;
        match closed {
            Ok(result) => {
                let status = result?;
                self.reaped = true;
                if !status.success() {
                    return Err(error(format!("Codex bootstrap exited with {status}")));
                }
            }
            Err(_) => {
                self.kill_group();
                self.child.wait().await?;
                self.reaped = true;
                return Err(error("Codex bootstrap did not shut down"));
            }
        }
        Ok(())
    }

    fn kill_group(&self) {
        if !self.reaped
            && let Some(pid) = self.child.id()
        {
            let _ = nix::sys::signal::killpg(Pid::from_raw(pid as i32), Signal::SIGKILL);
        }
    }
}

impl Drop for Bootstrap {
    fn drop(&mut self) {
        self.kill_group();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    const THREAD: &str = "6b121953-b116-4a0f-b2dd-a63c293e2c85";

    fn node(config: Value) -> DocumentNode {
        serde_json::from_value(json!({"id":"writer","kind":"agent","config":config})).unwrap()
    }

    fn fake_codex(directory: &Path, reject_resume: bool) -> PathBuf {
        let path = directory.join("codex");
        // Fixed protocol ids make a POSIX shell sufficient; requests are logged
        // for assertions without starting Codex or issuing any model requests.
        let resume = if reject_resume {
            "{\"error\":{\"code\":-1,\"message\":\"missing saved thread\"}}".into()
        } else {
            format!("{{\"result\":{{\"thread\":{{\"id\":\"{THREAD}\"}}}}}}")
        };
        let script = format!(
            "#!/bin/sh\nrequest=0\nwhile IFS= read -r line; do\n  printf '%s\\n' \"$line\" >> requests.jsonl\n  case \"$line\" in\n    *'\"method\":\"initialized\"'*) continue;;\n  esac\n  request=$((request + 1))\n  case \"$line\" in\n    *'\"method\":\"thread/start\"'*) response='{{\"result\":{{\"thread\":{{\"id\":\"{THREAD}\"}}}}}}';;\n    *'\"method\":\"thread/resume\"'*) response='{resume}';;\n    *) response='{{\"result\":{{}}}}';;\n  esac\n  printf '{{\"method\":\"test/notification\"}}\\n'\n  printf '{{\"id\":%s,%s\\n' \"$request\" \"${{response#?}}\"\ndone\n"
        );
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        path
    }

    fn requests(cwd: &Path) -> Vec<Value> {
        std::fs::read_to_string(cwd.join("requests.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn native_launcher_persists_and_resumes_the_exact_thread_without_inference() {
        let temp = tempfile::tempdir().unwrap();
        let program = fake_codex(temp.path(), false);
        let cwd = temp.path().join("workspace");
        std::fs::create_dir(&cwd).unwrap();
        let node = node(json!({"prompt":"Review \"changes\"\ncarefully", "model":"test-model"}));
        let first = prepare_native(&node, temp.path(), &cwd, &program, temp.path())
            .await
            .unwrap();
        assert_eq!(first.conversation_id.as_deref(), Some(THREAD));
        assert!(first.args.windows(2).any(|args| args == ["resume", THREAD]));
        assert!(first.args.iter().any(|arg| arg == "--no-daemon"));
        assert!(first.args.windows(2).any(|args| args
            == [
                "-c",
                &format!("developer_instructions={}", node.config["prompt"]),
            ]));
        assert!(
            !first
                .args
                .iter()
                .any(|arg| matches!(arg.as_str(), "--last" | "exec"))
        );
        let calls = requests(&cwd);
        let start = calls
            .iter()
            .find(|call| call["method"] == "thread/start")
            .unwrap();
        assert_eq!(start["params"]["historyMode"], "legacy");
        assert_eq!(start["params"]["ephemeral"], false);
        assert_eq!(
            start["params"]["developerInstructions"],
            node.config["prompt"]
        );
        assert!(calls.iter().any(|call| call["method"] == "thread/name/set"));
        assert!(!calls.iter().any(|call| call["method"] == "turn/start"));
        std::fs::remove_file(cwd.join("requests.jsonl")).unwrap();
        let second = prepare_native(&node, temp.path(), &cwd, &program, temp.path())
            .await
            .unwrap();
        assert_eq!(second.conversation_id, first.conversation_id);
        let calls = requests(&cwd);
        assert!(!calls.iter().any(|call| call["method"] == "thread/start"));
        let resume = calls
            .iter()
            .find(|call| call["method"] == "thread/resume")
            .unwrap();
        assert_eq!(resume["params"]["threadId"], THREAD);
    }

    #[tokio::test]
    async fn missing_saved_native_thread_is_an_error_and_never_replaced() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path().join("workspace");
        std::fs::create_dir(&cwd).unwrap();
        let program = fake_codex(temp.path(), false);
        let node = node(json!({"prompt":"review"}));
        prepare_native(&node, temp.path(), &cwd, &program, temp.path())
            .await
            .unwrap();
        std::fs::remove_file(cwd.join("requests.jsonl")).unwrap();
        fake_codex(temp.path(), true);
        let failure = prepare_native(&node, temp.path(), &cwd, &program, temp.path())
            .await
            .unwrap_err();
        assert!(failure.message.contains("missing saved thread"));
        assert!(
            !requests(&cwd)
                .iter()
                .any(|call| call["method"] == "thread/start")
        );
        let saved: SavedSession = read_json(&temp.path().join("codex-session.json")).unwrap();
        assert_eq!(saved.conversation_id, THREAD);
    }

    #[tokio::test]
    async fn custom_runner_is_passed_verbatim_without_native_bootstrap() {
        let temp = tempfile::tempdir().unwrap();
        let node = node(
            json!({"prompt":"ignored by custom runner", "argv":["/bin/sh","-c","printf 'a b'"]}),
        );
        let launch = prepare(&node, temp.path(), temp.path()).await.unwrap();
        assert_eq!(launch.program, Path::new("/bin/sh"));
        assert_eq!(launch.args, ["-c", "printf 'a b'"]);
        assert!(launch.conversation_id.is_none());
        assert!(!temp.path().join("codex-session.json").exists());
        assert!(!temp.path().join("codex-bootstrap.log").exists());
    }

    #[tokio::test]
    async fn changed_node_identity_cannot_reuse_saved_conversation() {
        let temp = tempfile::tempdir().unwrap();
        let program = fake_codex(temp.path(), false);
        let mut node = node(json!({"prompt":"review"}));
        prepare_native(&node, temp.path(), temp.path(), &program, temp.path())
            .await
            .unwrap();
        std::fs::remove_file(temp.path().join("requests.jsonl")).unwrap();
        node.id = "another-node".into();
        let failure = prepare_native(&node, temp.path(), temp.path(), &program, temp.path())
            .await
            .unwrap_err();
        assert!(failure.message.contains("different node"));
        assert!(!temp.path().join("requests.jsonl").exists());
    }

    #[tokio::test]
    async fn relative_codex_home_is_rejected_before_changing_child_working_directory() {
        let temp = tempfile::tempdir().unwrap();
        let node = node(json!({"prompt":"review"}));
        let failure = prepare_native(
            &node,
            temp.path(),
            temp.path(),
            Path::new("not-invoked"),
            Path::new("relative-codex-home"),
        )
        .await
        .unwrap_err();
        assert!(failure.message.contains("CODEX_HOME must be absolute"));
        assert!(!temp.path().join("codex-bootstrap.log").exists());
    }

    #[tokio::test]
    async fn cancellation_kills_bootstrap_process_group() {
        let temp = tempfile::tempdir().unwrap();
        let program = temp.path().join("codex");
        std::fs::write(&program, "#!/bin/sh\nexec sleep 60\n").unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
        let rpc = Bootstrap::spawn(&program, temp.path(), temp.path()).unwrap();
        let pid = rpc.child.id().unwrap();
        drop(rpc);
        tokio::time::timeout(Duration::from_secs(2), async {
            while nix::sys::signal::kill(Pid::from_raw(pid as i32), None).is_ok() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    /// Explicit opt-in: creates one empty local conversation in the user's
    /// ordinary Codex storage. It never sends input or starts a model turn.
    #[tokio::test]
    #[ignore = "requires installed/authenticated Codex and writes an empty local conversation"]
    async fn native_codex_idle_smoke() {
        use crate::{
            node_runtime::process::Lifetime,
            terminal::{LaunchSpec, Terminal},
        };

        let temp = tempfile::Builder::new()
            .prefix("ontography-codex-smoke-")
            .tempdir_in("/tmp")
            .unwrap();
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let cwd = temp.path().join("workspace");
        std::fs::create_dir(&cwd).unwrap();
        let node = node(json!({"prompt":"Wait for work. This is an idle launcher verification."}));
        let mut prepared = prepare(&node, temp.path(), &cwd)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "{error}; bootstrap log: {}",
                    temp.path().join("codex-bootstrap.log").display()
                )
            });
        let conversation_id = prepared.conversation_id.clone();
        let mut lifetime = Lifetime::new(temp.path()).unwrap();
        // Trust only this empty fixture via a process-local override. Production
        // retains normal native folder trust, and no user config is modified.
        prepared.args.splice(
            0..0,
            [
                "-c".into(),
                format!(
                    "projects.{}.trust_level=\"trusted\"",
                    serde_json::to_string(&std::fs::canonicalize(&cwd).unwrap()).unwrap()
                ),
            ],
        );
        let spec = LaunchSpec {
            program: prepared.program,
            args: prepared.args,
            env: prepared.env,
            cwd: cwd.clone(),
            rows: 30,
            cols: 110,
            server_id: uuid::Uuid::new_v4().to_string(),
            session_id: node.id.clone(),
        };
        let terminal =
            Terminal::launch(lifetime.supervise(spec), temp.path().join("terminal.sock"))
                .await
                .unwrap();
        lifetime.permit(&terminal).await.unwrap();
        let observed = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let status = terminal.status();
                let snapshot = terminal.snapshot();
                if !status.running || status.fault.is_some() {
                    return Err(format!("{status:?}; screen: {}", snapshot.screen));
                }
                if snapshot.screen.contains("Codex")
                    && !snapshot.screen.contains("Trust this folder?")
                    && (snapshot.screen.contains("context")
                        || snapshot.screen.contains("shortcuts"))
                {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    return Ok(terminal.snapshot().screen);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        let running = terminal.status().running;
        lifetime.disconnect();
        terminal.shutdown().await.unwrap();
        let _screen = observed
            .unwrap_or_else(|_| {
                panic!(
                    "native composer did not render: {}",
                    terminal.snapshot().screen
                )
            })
            .expect("native terminal failed");
        assert!(running, "native Codex exited before idle observation");
        let resumed = prepare(&node, temp.path(), &cwd).await.unwrap();
        assert_eq!(resumed.conversation_id, conversation_id);
    }
}
