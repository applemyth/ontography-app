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

struct Fixture {
    directory: tempfile::TempDir,
    paths: Paths,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let paths = Paths::initialize(directory.path().join("store")).unwrap();
        let pi = directory.path().join("pi");
        std::fs::write(&pi, "#!/bin/sh\nif [ \"$1\" = --version ]; then echo 0.85.1; exit; fi\nprintf 'manager ready\\r\\n'\nwhile IFS= read -r line; do\n  [ \"$line\" = /quit ] && exit 0\n  printf '%s\\r\\n' \"$line\"\ndone\n").unwrap();
        std::fs::set_permissions(pi, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self { directory, paths }
    }

    async fn output(&self, args: &[&str]) -> Output {
        tokio::time::timeout(
            Duration::from_secs(20),
            tokio::process::Command::new(BIN)
                .arg("--data-dir")
                .arg(&self.paths.root)
                .current_dir(self.directory.path())
                .args(args)
                .stdin(Stdio::null())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("CLI timed out")
        .unwrap()
    }

    async fn cli(&self, args: &[&str]) -> Value {
        let output = self.output(args).await;
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
                if status["attached"] == true && terminal.ready() {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("manager must attach and render")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::process::Command::new(BIN)
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
}

impl TerminalClient {
    fn start(fixture: &Fixture, args: &[&str]) -> Self {
        let pair = native_pty_system()
            .openpty(PtySize {
                rows: 24,
                cols: 100,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        let mut command = CommandBuilder::new(BIN);
        command.arg("--data-dir");
        command.arg(&fixture.paths.root);
        command.arg("--pi");
        command.arg(fixture.directory.path().join("pi"));
        command.args(args);
        command.cwd(fixture.directory.path());
        command.env("TERM", "xterm-256color");
        let child = pair.slave.spawn_command(command).unwrap();
        drop(pair.slave);
        let mut reader = pair.master.try_clone_reader().unwrap();
        let output = Arc::new(Mutex::new(Vec::<u8>::new()));
        let captured = output.clone();
        std::thread::spawn(move || {
            let mut buffer = [0; 8192];
            while let Ok(count) = reader.read(&mut buffer) {
                if count == 0 {
                    break;
                }
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
        }
    }

    fn ready(&self) -> bool {
        let output = self.output.lock().unwrap();
        let text = String::from_utf8_lossy(&output);
        text.contains("manager ready") && text.contains("\x1b[?1049h")
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
        .expect("new session terminal must render");
    }
}

impl Drop for TerminalClient {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
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
async fn bare_launch_creates_detach_preserves_and_quit_only_exits_manager() {
    let fixture = Fixture::new();
    let mut first_client = TerminalClient::start(&fixture, &[]);
    first_client.ready_before_selection().await;
    let first = fixture.selected().await;
    let id = first["session_id"].as_str().unwrap();
    let name = first["name"].as_str().unwrap();
    let manager = fixture.attached(id, &first_client).await;
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
    let second_manager = fixture.attached(second_id, &second_client).await;
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
    reattached.exited().await;
    assert_eq!(fixture.terminal(id).await["running"], false);
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

    let mut restored = TerminalClient::start(&fixture, &["--session", id]);
    let replacement = fixture.attached(id, &restored).await;
    assert_ne!(replacement["terminal_id"], manager["terminal_id"]);
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
