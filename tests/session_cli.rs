#![cfg(unix)]

use ontography_app::{client::Client, persistence::Paths};
use portable_pty::{Child, CommandBuilder, MasterPty, PtySize, native_pty_system};
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    os::unix::fs::PermissionsExt,
    process::{Output, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

const BIN: &str = env!("CARGO_BIN_EXE_ontography");

#[path = "session_cli/node_panes.rs"]
mod node_panes;

struct Fixture {
    directory: tempfile::TempDir,
    paths: Paths,
    binary: std::path::PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let paths = Paths::initialize(directory.path().join("store")).unwrap();
        let binary = directory.path().join("ontography-test");
        std::fs::copy(BIN, &binary).unwrap();
        let pi = directory.path().join("pi");
        std::fs::write(&pi, r#"#!/bin/sh
if [ "$1" = --version ]; then echo 0.85.1; exit; fi
base=$(dirname "$0")
launch="$base/launches/$ONTOGRAPHY_SESSION_ID/$$"
mkdir -p "$launch"
env > "$launch/env"
printf '%s\000' "$@" > "$launch/args"
printf '%s\n' "$ONTOGRAPHY_SESSION_ID" > "$launch/session"
stty size > "$launch/size"
trap 'printf exited > "$launch/exited"' EXIT
while [ "$#" -gt 0 ]; do
  case "$1" in
    --session-dir) conversations=$2; shift 2 ;;
    --session-id) conversation=$2; shift 2 ;;
    --session) history=$2; shift 2 ;;
    *) shift ;;
  esac
done
if [ -n "$history" ]; then
  conversation=$(sed -n 's/.*"id":"\([^"]*\)".*/\1/p' "$history")
else
  history="$conversations/$conversation.jsonl"
fi
activate() {
  printf '{"type":"session","id":"%s"}\n' "$conversation" > "$history"
  "$(cat "$base/test-bin")" --data-dir "$base/store" --session "$ONTOGRAPHY_SESSION_ID" call session.conversation --args "$(printf '{"action":"activate","conversation_id":"%s","path":"%s"}' "$conversation" "$history")" > "$launch/registration" || exit 1
  printf '%s\n' "$conversation" > "$launch/conversation"
}
activate
printf 'manager ready\r\n'
while IFS= read -r line; do
  case "$line" in
    /quit) exit 0 ;;
    /new)
      conversation=$(cat "$base/new-conversation-id")
      history="$conversations/$conversation.jsonl"
      activate
      printf 'new conversation ready\r\n'
      ;;
    /size) stty size > "$launch/size" ;;
    *) printf '%s\r\n' "$line" ;;
  esac
done
"#).unwrap();
        std::fs::write(
            directory.path().join("test-bin"),
            binary.as_os_str().as_encoded_bytes(),
        )
        .unwrap();
        std::fs::write(
            directory.path().join("new-conversation-id"),
            uuid::Uuid::new_v4().to_string(),
        )
        .unwrap();
        std::fs::set_permissions(pi, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self {
            directory,
            paths,
            binary,
        }
    }

    async fn output(&self, args: &[&str]) -> Output {
        self.output_with(args, &[]).await
    }

    /// Runs the CLI with `env` added to this test's environment.
    async fn output_with(&self, args: &[&str], env: &[(&str, &str)]) -> Output {
        tokio::time::timeout(
            Duration::from_secs(20),
            tokio::process::Command::new(&self.binary)
                .arg("--data-dir")
                .arg(&self.paths.root)
                .current_dir(self.directory.path())
                .args(args)
                .envs(env.iter().copied())
                .stdin(Stdio::null())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("CLI timed out")
        .unwrap()
    }

    async fn cli(&self, args: &[&str]) -> Value {
        self.cli_with(args, &[]).await
    }

    async fn cli_with(&self, args: &[&str], env: &[(&str, &str)]) -> Value {
        let output = self.output_with(args, env).await;
        assert!(
            output.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }

    async fn error(&self, args: &[&str], code: &str) -> String {
        let output = self.output(args).await;
        assert!(!output.status.success(), "{args:?} unexpectedly succeeded");
        let error = String::from_utf8(output.stderr).unwrap();
        assert!(error.starts_with(code), "{error}");
        error
    }

    async fn terminal(&self, id: &str) -> Value {
        self.cli(&["--session", id, "call", "terminal.status"])
            .await
    }

    async fn selected(&self) -> Value {
        let list = self.cli(&["session", "list"]).await;
        list["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["session_id"] == list["selected_session_id"])
            .unwrap()
            .clone()
    }

    async fn attached(&self, id: &str, terminal: &TerminalClient) -> Value {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let status = self.terminal(id).await;
                if status["attached"] == true && terminal.rendered() {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("manager must attach and render")
    }

    async fn mode(&self, id: &str, expected: &str) -> Value {
        let mut last = Value::Null;
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let status = self.terminal(id).await;
                if status["running"] == true && status["manager_mode"] == expected {
                    return status;
                }
                last = status;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("session {id} must enter {expected} mode; last status: {last}"))
    }

    async fn suspended(&self, id: &str) -> Value {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let record = self.cli(&["show", id]).await;
                if record["status"] == "suspended" {
                    return record;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("shell exit must suspend its session without a client")
    }

    fn launch_dir(&self, id: &str, status: &Value) -> std::path::PathBuf {
        self.directory.path().join("launches").join(id).join(
            status["manager_pid"]
                .as_u64()
                .expect("Pi child has a PID")
                .to_string(),
        )
    }

    async fn launched_args(&self, id: &str, status: &Value) -> Vec<String> {
        let directory = self.launch_dir(id, status);
        wait_file(&directory.join("conversation")).await;
        let path = directory.join("args");
        std::fs::read(path)
            .unwrap()
            .split(|b| *b == 0)
            .filter(|value| !value.is_empty())
            .map(|value| String::from_utf8(value.to_vec()).unwrap())
            .collect()
    }
}

async fn wait_file(path: &std::path::Path) {
    tokio::time::timeout(Duration::from_secs(15), async {
        while !std::fs::metadata(path)
            .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{} must be written", path.display()));
}

struct BackgroundJob(Option<i32>);

impl Drop for BackgroundJob {
    fn drop(&mut self) {
        if let Some(pid) = self.0 {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(pid),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
    }
}

fn option<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].as_str())
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::process::Command::new(&self.binary)
            .arg("--data-dir")
            .arg(&self.paths.root)
            .args(["server", "stop"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = std::fs::remove_dir(self.paths.socket.parent().unwrap());
    }
}

struct TerminalClient {
    child: Box<dyn Child + Send + Sync>,
    _master: Box<dyn MasterPty + Send>,
    input: Box<dyn Write + Send>,
    output: Arc<Mutex<Vec<u8>>>,
    screen: Arc<Mutex<vt100::Parser>>,
}

impl TerminalClient {
    fn start(fixture: &Fixture, args: &[&str]) -> Self {
        Self::start_with(fixture, args, &[])
    }

    /// Starts the CLI in a terminal with `env` added to this test's environment.
    fn start_with(fixture: &Fixture, args: &[&str], env: &[(&str, &str)]) -> Self {
        let pair = native_pty_system()
            .openpty(PtySize {
                rows: 24,
                cols: 100,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        let mut command = CommandBuilder::new(&fixture.binary);
        command.arg("--data-dir");
        command.arg(&fixture.paths.root);
        command.arg("--pi");
        command.arg(fixture.directory.path().join("pi"));
        command.args(args);
        command.cwd(fixture.directory.path());
        command.env("TERM", "xterm-256color");
        for (name, value) in env {
            command.env(name, value);
        }
        let child = pair.slave.spawn_command(command).unwrap();
        drop(pair.slave);
        let mut reader = pair.master.try_clone_reader().unwrap();
        let output = Arc::new(Mutex::new(Vec::<u8>::new()));
        let captured = output.clone();
        let screen = Arc::new(Mutex::new(vt100::Parser::new(24, 100, 0)));
        let rendered = screen.clone();
        std::thread::spawn(move || {
            let mut buffer = [0; 8192];
            while let Ok(count) = reader.read(&mut buffer) {
                if count == 0 {
                    break;
                }
                rendered.lock().unwrap().process(&buffer[..count]);
                let mut output = captured.lock().unwrap();
                output.extend_from_slice(&buffer[..count]);
                let excess = output.len().saturating_sub(65536);
                output.drain(..excess);
            }
        });
        let input = pair.master.take_writer().unwrap();
        Self {
            child,
            _master: pair.master,
            input,
            output,
            screen,
        }
    }

    fn ready(&self) -> bool {
        let output = self.output.lock().unwrap();
        let text = String::from_utf8_lossy(&output);
        text.contains("manager ready") && text.contains("\x1b[?1049h")
    }

    fn rendered(&self) -> bool {
        self.output
            .lock()
            .unwrap()
            .windows(8)
            .any(|part| part == b"\x1b[?1049h")
    }

    fn still_running(&mut self) {
        assert!(
            self.child.try_wait().unwrap().is_none(),
            "terminal client exited: {}",
            String::from_utf8_lossy(&self.output.lock().unwrap())
        );
    }

    fn send(&mut self, bytes: &[u8]) {
        self.input.write_all(bytes).unwrap();
        self.input.flush().unwrap();
    }

    async fn exited(&mut self) {
        let status = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("terminal client must exit");
        assert!(
            status.success(),
            "terminal output: {}",
            String::from_utf8_lossy(&self.output.lock().unwrap())
        );
    }

    async fn ready_before_selection(&mut self) {
        tokio::time::timeout(Duration::from_secs(15), async {
            while !self.ready() {
                assert!(
                    self.child.try_wait().unwrap().is_none(),
                    "client exited before ready: {}",
                    String::from_utf8_lossy(&self.output.lock().unwrap())
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "new session terminal must render: {}",
                String::from_utf8_lossy(&self.output.lock().unwrap())
            )
        });
    }
}

impl Drop for TerminalClient {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reading_sessions_never_starts_a_server() {
    let fixture = Fixture::new();
    let listed = fixture.output(&["ls"]).await;
    assert!(listed.status.success());
    assert!(String::from_utf8_lossy(&listed.stdout).contains("No sessions"));
    assert!(
        !fixture.paths.socket.exists(),
        "listing must not start a server"
    );
    // A session left active when its server stopped reads as not running.
    fixture.cli(&["new", "kept", "--no-attach"]).await;
    fixture.cli(&["server", "stop"]).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.paths.socket.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the stopped server must remove its socket");
    assert_eq!(fixture.cli(&["show", "kept"]).await["status"], "suspended");
    let listed = fixture.output(&["ls"]).await;
    let table = String::from_utf8(listed.stdout).unwrap();
    assert!(
        table.contains("kept") && table.contains("inactive") && table.contains("stopped"),
        "{table}"
    );
    assert!(
        !fixture.paths.socket.exists(),
        "reading must not start a server"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pi_runs_with_the_attaching_terminals_environment() {
    let fixture = Fixture::new();
    // The server starts from inside an agent session, with its own variables.
    let created = fixture
        .cli_with(
            &["new", "--no-attach"],
            &[
                ("SESSION_MARKER", "starter"),
                ("CLAUDECODE", "1"),
                ("CLAUDE_CODE_MESSAGING_TOKEN", "secret"),
            ],
        )
        .await;
    let id = created["session_id"].as_str().unwrap();
    // Another terminal, itself inside an agent session, attaches.
    let mut terminal = TerminalClient::start_with(
        &fixture,
        &["attach", id],
        &[("SESSION_MARKER", "attacher"), ("CLAUDECODE", "1")],
    );
    terminal.ready_before_selection().await;
    let status = fixture.mode(id, "pi").await;
    let path = fixture.launch_dir(id, &status).join("env");
    wait_file(&path).await;
    let environment = std::fs::read_to_string(path).unwrap();
    let has = |line: &str| environment.lines().any(|found| found == line);
    assert!(has("SESSION_MARKER=attacher"), "{environment}");
    assert!(has("TERM=xterm-256color"), "{environment}");
    for absent in ["CLAUDECODE=", "CLAUDE_CODE_MESSAGING_TOKEN="] {
        assert!(
            !environment.lines().any(|line| line.starts_with(absent)),
            "{absent} must not reach Pi: {environment}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_targets_listing_and_noninteractive_creation_are_unambiguous() {
    let fixture = Fixture::new();
    let first = fixture.cli(&["new", "feature-a", "--no-attach"]).await;
    let id = first["session_id"].as_str().unwrap();
    let listed = fixture.cli(&["ls", "--json"]).await;
    assert_eq!(listed["sessions"][0]["terminal"], "stopped");
    assert!(listed["sessions"][0]["graph"].is_null());
    let human = fixture.output(&["ls"]).await;
    assert!(human.status.success());
    let text = String::from_utf8(human.stdout).unwrap();
    for expected in [id, "feature-a", "active", "stopped", "uninitialized"] {
        assert!(text.contains(expected), "{text}");
    }
    assert_eq!(
        fixture.cli(&["session", "list"]).await["sessions"],
        json!([first])
    );
    assert_eq!(fixture.cli(&["show", "feature-a"]).await["session_id"], id);
    assert_eq!(
        fixture
            .cli(&["--session", "feature-a", "call", "session.inspect"])
            .await["session_id"],
        id
    );
    for args in [&[][..], &["new", "orphan"][..]] {
        fixture.error(args, "terminal_required").await;
    }
    fixture
        .error(
            &["--session", id, "new", "ignored", "--no-attach"],
            "invalid_arguments",
        )
        .await;
    assert_eq!(
        fixture.cli(&["session", "list"]).await["sessions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    fixture
        .error(&["close", "missing"], "session_not_found")
        .await;
    let duplicate = fixture
        .cli(&["session", "new", "feature-a", "--no-attach"])
        .await;
    let second = duplicate["session_id"].as_str().unwrap();
    for command in ["attach", "close"] {
        let error = fixture
            .error(&[command, "feature-a"], "ambiguous_session")
            .await;
        assert!(error.contains(id) && error.contains(second));
    }
    // Even a display name that equals another session's UUID cannot shadow that ID.
    fixture.cli(&["new", id, "--no-attach"]).await;
    assert_eq!(fixture.cli(&["close", id]).await["session_id"], id);
    assert_eq!(fixture.cli(&["show", second]).await["status"], "active");
    assert_eq!(
        fixture.cli(&["session", "show", id]).await["status"],
        "closed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bare_launch_and_persistent_shell_preserve_session_and_latest_pi_conversation() {
    let fixture = Fixture::new();
    let mut first_client = TerminalClient::start(&fixture, &[]);
    first_client.ready_before_selection().await;
    let first = fixture.selected().await;
    let id = first["session_id"].as_str().unwrap();
    let name = first["name"].as_str().unwrap();
    fixture.attached(id, &first_client).await;
    let manager = fixture.mode(id, "pi").await;
    let initial_args = fixture.launched_args(id, &manager).await;
    assert_eq!(
        option(&initial_args, "--session-id"),
        first["pi"]["active_conversation_id"].as_str()
    );
    assert_eq!(option(&initial_args, "--session"), None);
    let client = Client::connect(&fixture.paths.socket).await.unwrap();
    let scoped = client.for_session(id);
    let run = scoped.call("run.start", json!({"declaration":serde_json::from_str::<Value>(include_str!("../examples/flow.json")).unwrap()})).await.unwrap();
    first_client.send(b"\x02d");
    first_client.exited().await;
    assert_eq!(fixture.terminal(id).await["running"], true);
    assert_eq!(fixture.terminal(id).await["attached"], false);

    let mut second_client = TerminalClient::start(&fixture, &[]);
    second_client.ready_before_selection().await;
    let second = fixture.selected().await;
    let second_id = second["session_id"].as_str().unwrap();
    assert_ne!(id, second_id);
    assert!(second["run_id"].is_null());
    fixture.attached(second_id, &second_client).await;
    let second_manager = fixture.mode(second_id, "pi").await;
    assert_ne!(manager["pid"], second_manager["pid"]);
    second_client.send(b"\x02d");
    second_client.exited().await;

    let mut reattached = TerminalClient::start(&fixture, &["attach", name]);
    let attached = fixture.attached(id, &reattached).await;
    assert_eq!(attached["pid"], manager["pid"]);
    assert_eq!(attached["terminal_id"], manager["terminal_id"]);
    let list = fixture.cli(&["ls", "--json"]).await;
    let row = list["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["session_id"] == id)
        .unwrap();
    assert_eq!(row["terminal"], "attached");
    assert_eq!(row["graph"]["run_id"], run["run_id"]);
    assert_eq!(row["graph"]["status"], "active");

    reattached.send(b"/quit\r");
    let shell = fixture.mode(id, "shell").await;
    reattached.still_running();
    assert_eq!(shell["pid"], manager["pid"]);
    assert_eq!(shell["terminal_id"], manager["terminal_id"]);
    assert!(shell["manager_pid"].is_null());
    let after_quit = fixture.cli(&["show", id]).await;
    assert_eq!(after_quit["status"], "active");
    assert_eq!(after_quit["run_id"], run["run_id"]);
    assert_eq!(
        after_quit["pi"]["active_conversation_id"],
        first["pi"]["active_conversation_id"]
    );
    assert_eq!(
        scoped.call("run.inspect", json!({})).await.unwrap()["status"],
        "active"
    );

    // The session owns an actual persistent shell, including its local state.
    reattached.send(b"scope_probe=preserved; printf shell-ready > shell-ready\r");
    wait_file(&fixture.directory.path().join("shell-ready")).await;
    reattached.send(b"pi\r");
    let resumed = fixture.mode(id, "pi").await;
    assert_eq!(resumed["pid"], manager["pid"]);
    assert_eq!(resumed["terminal_id"], manager["terminal_id"]);
    assert_ne!(resumed["manager_pid"], manager["manager_pid"]);
    let saved = &after_quit["pi"]["conversations"]
        [first["pi"]["active_conversation_id"].as_str().unwrap()]["path"];
    let resumed_args = fixture.launched_args(id, &resumed).await;
    assert_eq!(option(&resumed_args, "--session"), saved.as_str());
    assert_eq!(option(&resumed_args, "--session-id"), None);
    // Model native Pi's /new hook: only its conversation identity changes.
    reattached.send(b"/new\r");
    let next_id =
        std::fs::read_to_string(fixture.directory.path().join("new-conversation-id")).unwrap();
    let next = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let record = fixture.cli(&["show", id]).await;
            if record["pi"]["active_conversation_id"] == next_id {
                break record;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("/new must update the active Pi conversation");
    assert_eq!(next["run_id"], run["run_id"]);
    assert_ne!(
        next["pi"]["active_conversation_id"],
        first["pi"]["active_conversation_id"]
    );
    reattached.send(b"/quit\r");
    fixture.mode(id, "shell").await;
    reattached.send(b"\x02d");
    reattached.exited().await;

    let mut restored = TerminalClient::start(&fixture, &["--session", id]);
    let replacement = fixture.attached(id, &restored).await;
    assert_eq!(replacement["terminal_id"], manager["terminal_id"]);
    assert_eq!(replacement["manager_mode"], "shell");
    restored.send(b"printf '%s' \"$scope_probe\" > retained-shell-state\r");
    let retained = fixture.directory.path().join("retained-shell-state");
    wait_file(&retained).await;
    assert_eq!(std::fs::read_to_string(retained).unwrap(), "preserved");
    restored.send(b"pi\r");
    let latest = fixture.mode(id, "pi").await;
    let latest_args = fixture.launched_args(id, &latest).await;
    assert_eq!(
        option(&latest_args, "--session"),
        next["pi"]["conversations"][&next_id]["path"].as_str()
    );
    assert_eq!(latest["terminal_id"], manager["terminal_id"]);
    assert_eq!(
        std::fs::read_dir(fixture.directory.path().join("launches").join(id))
            .unwrap()
            .count(),
        3
    );
    restored.send(b"/quit\r");
    fixture.mode(id, "shell").await;
    // A failed relaunch reports the error and leaves a usable owning shell.
    let pi_path = fixture.directory.path().join("pi");
    let unavailable = fixture.directory.path().join("pi-unavailable");
    std::fs::rename(&pi_path, &unavailable).unwrap();
    restored.send(b"pi\r");
    let failed = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let status = fixture.terminal(id).await;
            if status["manager_mode"] == "shell"
                && status["manager_error"]
                    .as_str()
                    .is_some_and(|error| error.contains("cannot run"))
            {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("failed Pi launch must return to shell with an error");
    assert_eq!(failed["terminal_id"], manager["terminal_id"]);
    assert_eq!(failed["running"], true);
    std::fs::rename(unavailable, pi_path).unwrap();
    restored.send(b"printf recovered > launch-failure-recovered\r");
    wait_file(&fixture.directory.path().join("launch-failure-recovered")).await;
    assert_eq!(fixture.cli(&["close", name]).await["status"], "closed");
    restored.exited().await;
    assert_eq!(fixture.terminal(id).await["running"], false);
    assert_eq!(
        scoped.call("run.inspect", json!({})).await.unwrap()["status"],
        "closed"
    );
    assert_eq!(
        fixture.terminal(second_id).await["pid"],
        second_manager["pid"]
    );
    assert_eq!(fixture.terminal(second_id).await["running"], true);
    assert_eq!(
        fixture.cli(&["session", "list"]).await["sessions"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shell_exit_suspends_while_detached_reaps_jobs_and_resumes_after_server_restart() {
    let fixture = Fixture::new();
    let mut terminal = TerminalClient::start(&fixture, &[]);
    terminal.ready_before_selection().await;
    let record = fixture.selected().await;
    let id = record["session_id"].as_str().unwrap();
    let manager = fixture.mode(id, "pi").await;
    let client = Client::connect(&fixture.paths.socket).await.unwrap();
    let scoped = client.for_session(id);
    let run=scoped.call("run.start",json!({"declaration":serde_json::from_str::<Value>(include_str!("../examples/flow.json")).unwrap()})).await.unwrap();
    let other = fixture.cli(&["new", "other", "--no-attach"]).await;
    let other_id = other["session_id"].as_str().unwrap();
    let mut other_terminal = TerminalClient::start(&fixture, &["attach", other_id]);
    other_terminal.ready_before_selection().await;
    let other_manager = fixture.mode(other_id, "pi").await;
    other_terminal.send(b"\x02d");
    other_terminal.exited().await;

    terminal
        ._master
        .resize(PtySize {
            rows: 30,
            cols: 120,
            pixel_width: 0,
            pixel_height: 0,
        })
        .unwrap();
    let size = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let status = fixture.terminal(id).await;
            if status["rows"] == 30 && status["cols"] == 120 {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("native Pi PTY follows attachment dimensions");
    terminal.send(b"/size\r");
    let size_file = fixture.launch_dir(id, &size).join("size");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if std::fs::read_to_string(&size_file)
                .unwrap_or_default()
                .trim()
                == "30 120"
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("Pi child sees resized native dimensions");

    terminal.send(b"/quit\r");
    fixture.mode(id, "shell").await;
    terminal.send(b"printf ready > foreground-started; sleep 30\r");
    wait_file(&fixture.directory.path().join("foreground-started")).await;
    terminal.send(b"\x03");
    terminal.send(b"printf ready > after-interrupt\r");
    wait_file(&fixture.directory.path().join("after-interrupt")).await;
    terminal.still_running();
    // A background job has a separate process group under interactive job
    // control; server cleanup must still reap it when the owning shell exits.
    terminal.send(b"sleep 600 & printf '%s' $! > background-pid\r");
    let child_file = fixture.directory.path().join("background-pid");
    tokio::time::timeout(Duration::from_secs(10), async {
        while !std::fs::metadata(&child_file).is_ok_and(|metadata| metadata.len() > 0) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "background shell command must run: {}",
            String::from_utf8_lossy(&terminal.output.lock().unwrap())
        )
    });
    let background_pid: i32 = std::fs::read_to_string(child_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let mut background_job = BackgroundJob(Some(background_pid));
    terminal.send(b"sleep 1; exit\r\x02d");
    terminal.exited().await;
    let suspended = fixture.suspended(id).await;
    assert_eq!(suspended["run_id"], run["run_id"]);
    assert_eq!(fixture.terminal(id).await["running"], false);
    assert_eq!(
        scoped.call("run.list", json!({})).await.unwrap()["runs"][0]["status"],
        "suspended"
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        while nix::sys::signal::kill(nix::unistd::Pid::from_raw(background_pid), None).is_ok() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("exiting shell must leave no background job running");
    background_job.0 = None;
    let other_after = fixture.terminal(other_id).await;
    assert_eq!(other_after["running"], true);
    assert_eq!(other_after["pid"], other_manager["pid"]);
    assert_eq!(other_after["manager_pid"], other_manager["manager_pid"]);
    assert_eq!(fixture.cli(&["show", other_id]).await["status"], "active");

    fixture.cli(&["server", "stop"]).await;
    fixture.cli(&["server", "start"]).await;
    let mut resumed = TerminalClient::start(&fixture, &["attach", id]);
    resumed.ready_before_selection().await;
    let new_manager = fixture.mode(id, "pi").await;
    assert_ne!(new_manager["terminal_id"], manager["terminal_id"]);
    let restored = fixture.cli(&["show", id]).await;
    assert_eq!(restored["session_id"], record["session_id"]);
    assert_eq!(restored["run_id"], run["run_id"]);
    assert_eq!(
        restored["pi"]["active_conversation_id"],
        record["pi"]["active_conversation_id"]
    );
    assert_eq!(restored["status"], "active");
    let args = fixture.launched_args(id, &new_manager).await;
    assert_eq!(option(&args,"--session"),restored["pi"]["conversations"][record["pi"]["active_conversation_id"].as_str().unwrap()]["path"].as_str());
    resumed.send(b"/quit\r");
    fixture.mode(id, "shell").await;
    resumed.send(b"exit\r");
    resumed.exited().await;
    fixture.suspended(id).await;
    assert_eq!(fixture.cli(&["close", id]).await["status"], "closed");
    fixture.error(&["resume", id], "session_closed").await;
}
