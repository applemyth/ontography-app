//! The server keeps the receipts clients may still need, says when a change
//! applied though its result could not be sent, survives running out of
//! descriptors, keeps answering while it stops, and leaves no endpoint
//! directory behind.
#![cfg(unix)]

use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use ontography_app::{
    client::Client,
    persistence::Paths,
    protocol::{self, Request, Response},
    server::Server,
};
use serde_json::{Value, json};
use std::{
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{ExitStatus, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::BufReader,
    net::UnixStream,
    process::{Child, Command},
};

const BIN: &str = env!("CARGO_BIN_EXE_ontography");

fn document() -> Value {
    serde_json::from_str(include_str!("../examples/flow.json")).unwrap()
}

async fn call(
    server: &Arc<Server>,
    request_id: &str,
    operation: &str,
    args: Value,
) -> ontography_app::Result<Value> {
    server
        .request(Request {
            environment: None,
            version: protocol::VERSION,
            client_id: "server-robustness-test".into(),
            request_id: request_id.into(),
            operation: operation.into(),
            app_session_id: None,
            expected_server_id: Some(server.service.server_id.clone()),
            args,
        })
        .await
}

async fn receipt(server: &Arc<Server>, request_id: &str) -> ontography_app::Result<Value> {
    let id = uuid::Uuid::new_v4().to_string();
    let args = json!({"client_id":"server-robustness-test","request_id":request_id});
    call(server, &id, "operation.get", args).await
}

/// Run a server in the foreground, after the shell commands `limits`.
fn spawn(root: &Path, limits: &str) -> Child {
    Command::new("/bin/sh")
        .arg("-c")
        .arg(format!("{limits}exec \"$0\" \"$@\""))
        .arg(BIN)
        .arg("--data-dir")
        .arg(root)
        .args(["server", "run"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap()
}

async fn connect(paths: &Paths) -> Client {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(client) = Client::connect(&paths.socket).await {
                return client;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the server must serve")
}

async fn exit(server: &mut Child) -> ExitStatus {
    tokio::time::timeout(Duration::from_secs(15), server.wait())
        .await
        .expect("the server must exit")
        .unwrap()
}

fn signal(server: &Child, signal: Signal) {
    kill(Pid::from_raw(server.id().unwrap() as i32), signal).unwrap();
}

fn log(paths: &Paths) -> String {
    std::fs::read_to_string(paths.root.join("logs/server.log")).unwrap_or_default()
}

struct Fixture {
    directory: tempfile::TempDir,
    paths: Paths,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let paths = Paths::initialize(directory.path().join("data")).unwrap();
        Self { directory, paths }
    }

    /// Run a server in the foreground and connect to it.
    async fn run(&self, limits: &str) -> (Child, Client) {
        let server = spawn(&self.paths.root, limits);
        let client = connect(&self.paths).await;
        (server, client)
    }

    async fn stop(&self, server: &mut Child) {
        Client::stop_server(&self.paths.socket).await.unwrap();
        assert!(exit(server).await.success());
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Also clean up after assertion failures. Every fixture owns a unique data root.
        let _ = std::process::Command::new(BIN)
            .arg("--data-dir")
            .arg(&self.paths.root)
            .args(["server", "stop"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = std::fs::remove_dir(self.paths.endpoint());
    }
}

#[tokio::test]
async fn a_full_receipt_table_evicts_the_oldest_completed_receipt() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let server = Server::new(paths.clone()).unwrap();
    // An error is an outcome a receipt keeps, and this one needs no disk.
    let suspend = json!({"run_id":uuid::Uuid::new_v4()});
    // Fill the table, each receipt completing before the next is accepted.
    // The newest has the smallest key and the oldest the largest.
    for index in (0..128).rev() {
        let error = call(
            &server,
            &format!("{index:03}"),
            "run.suspend",
            suspend.clone(),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "not_found");
    }
    call(&server, "new", "run.suspend", suspend)
        .await
        .unwrap_err();
    for kept in ["000", "001", "126", "new"] {
        assert_eq!(
            receipt(&server, kept).await.unwrap()["state"],
            "failed",
            "receipt {kept} must be kept"
        );
    }
    assert_eq!(
        receipt(&server, "127").await.unwrap_err().code,
        "unknown_outcome"
    );
    server.stop().await.unwrap();
    assert!(!paths.endpoint().exists());
}

#[tokio::test]
async fn a_change_whose_result_is_too_large_says_it_was_applied() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let server = Server::new(paths.clone()).unwrap();
    // A run's status carries its document, here larger than a response may be.
    let mut document = document();
    document["nodes"].as_array_mut().unwrap().push(json!({
        "id":"C","component":"human",
        "config":{"prompt":"x".repeat(protocol::MAX_FRAME_BYTES / 2)}
    }));
    let error = call(
        &server,
        "start",
        "flow.start",
        json!({"document":document,"project":directory.path()}),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "result_too_large");
    assert_eq!(error.details, Some(json!({"committed":true})));
    assert!(error.message.contains("applied"), "{}", error.message);
    let runs = call(&server, "list", "run.list", json!({})).await.unwrap();
    let runs = runs["runs"].as_array().unwrap();
    assert_eq!(runs.len(), 1, "the run started");
    // Reading changes nothing, so says nothing of a change.
    let error = call(
        &server,
        "status",
        "flow.status",
        json!({"run_id":runs[0]["run_id"]}),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "result_too_large");
    assert_eq!(error.details, None);
    server.stop().await.unwrap();
    assert!(!paths.endpoint().exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_out_of_descriptors_does_not_end_the_server() {
    let fixture = Fixture::new();
    // The hard limit is too low for the server to raise.
    let (mut server, _) = fixture.run("ulimit -Sn 48 && ulimit -Hn 48 && ").await;
    // Far more connections than the server has descriptors for.
    let mut flood = Vec::new();
    for _ in 0..64 {
        flood.push(UnixStream::connect(&fixture.paths.socket).await.unwrap());
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(flood);
    let client = connect(&fixture.paths).await;
    assert!(
        server.try_wait().unwrap().is_none(),
        "the server must survive"
    );
    client.call("system.status", json!({})).await.unwrap();
    let log = log(&fixture.paths);
    assert!(log.contains("Too many open files"), "{log}");
    fixture.stop(&mut server).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_server_raises_a_low_soft_descriptor_limit() {
    let fixture = Fixture::new();
    // As launchd starts programs: a low soft limit under a high hard one.
    let (mut server, client) = fixture.run("ulimit -Sn 64 && ").await;
    let hello = Request {
        environment: None,
        app_session_id: None,
        version: protocol::VERSION,
        client_id: client.client_id().into(),
        request_id: uuid::Uuid::new_v4().to_string(),
        expected_server_id: None,
        operation: "system.hello".into(),
        args: json!({}),
    };
    // More connections at once than 64 descriptors allow, each answered.
    let mut connections = Vec::new();
    for _ in 0..100 {
        let mut socket = UnixStream::connect(&fixture.paths.socket).await.unwrap();
        protocol::write_frame(&mut socket, &hello).await.unwrap();
        connections.push(socket);
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        for socket in &mut connections {
            let frame = protocol::read_frame(&mut BufReader::new(socket))
                .await
                .unwrap()
                .expect("the server must answer every connection");
            let response: Response = serde_json::from_slice(&frame).unwrap();
            response.into_result().unwrap();
        }
    })
    .await
    .expect("every open connection must be answered");
    drop(connections);
    fixture.stop(&mut server).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requests_during_a_slow_stop_learn_that_the_server_is_stopping() {
    let fixture = Fixture::new();
    let (mut server, client) = fixture.run("").await;
    let directory = fixture.directory.path();
    // A Pi whose version check is slow holds an accepted operation open;
    // the stop must settle it first.
    let checking = directory.join("checking");
    let pi = directory.join("slow-pi");
    std::fs::write(
        &pi,
        format!(
            "#!/bin/sh\n: > '{}'\n/bin/sleep 4\necho 0.0.0\n",
            checking.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&pi, std::fs::Permissions::from_mode(0o700)).unwrap();
    let session = client
        .call("session.create", json!({"project":directory}))
        .await
        .unwrap();
    let scoped = client.for_session(session["session_id"].as_str().unwrap());
    let ensuring =
        tokio::spawn(async move { scoped.call("terminal.ensure", json!({"pi":pi})).await });
    tokio::time::timeout(Duration::from_secs(10), async {
        while !checking.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    signal(&server, Signal::SIGTERM);
    // A client arriving now is answered at once, not when the stop ends.
    let late = Client::connect(&fixture.paths.socket)
        .await
        .expect("a stopping server still completes handshakes");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match late
                .call("flow.define", json!({"document":document()}))
                .await
            {
                Err(error) if error.code == "server_stopping" => break,
                // The signal has not arrived yet.
                Ok(_) => tokio::time::sleep(Duration::from_millis(20)).await,
                Err(error) => panic!("{error}"),
            }
        }
    })
    .await
    .expect("new work must be refused while the stop settles accepted work");
    // Once the slow operation ends, the stop does, and the server exits.
    assert!(exit(&mut server).await.success());
    assert_eq!(ensuring.await.unwrap().unwrap_err().code, "pi_version");
    assert!(!fixture.paths.endpoint().exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signalled_server_exits_even_when_a_run_cannot_suspend() {
    let fixture = Fixture::new();
    let (mut server, client) = fixture.run("").await;
    let started = client
        .call(
            "flow.start",
            json!({"document":document(),"project":fixture.directory.path()}),
        )
        .await
        .unwrap();
    // Suspending saves the run's manifest in its directory: forbid that.
    let run = fixture
        .paths
        .run(started["run_id"].as_str().unwrap())
        .unwrap();
    std::fs::set_permissions(&run, std::fs::Permissions::from_mode(0o500)).unwrap();
    // A client that asks for a stop learns why it failed; the server serves on.
    let error = Client::stop_server(&fixture.paths.socket)
        .await
        .unwrap_err();
    assert_eq!(error.code, "shutdown_incomplete", "{error}");
    client
        .call("flow.define", json!({"document":document()}))
        .await
        .unwrap();
    // A signalled one exits all the same.
    signal(&server, Signal::SIGTERM);
    let exited = tokio::time::timeout(Duration::from_secs(10), server.wait()).await;
    std::fs::set_permissions(&run, std::fs::Permissions::from_mode(0o700)).unwrap();
    exited
        .expect("a signalled server must exit even if it cannot stop in order")
        .unwrap();
    let log = log(&fixture.paths);
    assert!(log.contains("recovered at the next start"), "{log}");
    // The run is recovered at the next start, as after a crash.
    let (mut server, client) = fixture.run("").await;
    let runs = client.call("run.list", json!({})).await.unwrap();
    assert_eq!(runs["runs"][0]["run_id"], started["run_id"]);
    assert_eq!(runs["runs"][0]["status"], "recoverable");
    fixture.stop(&mut server).await;
}

#[test]
fn opening_a_store_creates_no_endpoint_directory() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    assert!(!paths.endpoint().exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_makes_its_endpoint_directory_and_removes_it_when_empty() {
    let fixture = Fixture::new();
    let endpoint = fixture.paths.endpoint();
    let (mut server, _) = fixture.run("").await;
    fixture.stop(&mut server).await;
    assert!(!endpoint.exists());

    // One that ended abruptly left its socket and its shells' sockets.
    std::fs::create_dir(endpoint).unwrap();
    std::fs::set_permissions(endpoint, std::fs::Permissions::from_mode(0o700)).unwrap();
    for name in ["server.sock", "pty-session.sock", "pi-generation.sock"] {
        drop(std::os::unix::net::UnixListener::bind(endpoint.join(name)).unwrap());
    }
    std::fs::write(endpoint.join("other"), "").unwrap();
    let (mut server, _) = fixture.run("").await;
    assert!(!endpoint.join("pty-session.sock").exists());
    assert!(!endpoint.join("pi-generation.sock").exists());
    fixture.stop(&mut server).await;
    // A directory is only ever removed empty.
    assert!(!fixture.paths.socket.exists());
    assert!(endpoint.join("other").exists());

    // An old, empty directory is used as it is.
    std::fs::remove_file(endpoint.join("other")).unwrap();
    let (mut server, _) = fixture.run("").await;
    fixture.stop(&mut server).await;
    assert!(!endpoint.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_waiting_to_take_over_makes_the_directory_its_predecessor_removed() {
    let fixture = Fixture::new();
    let (mut first, client) = fixture.run("").await;
    // The next waits for the data directory while the first still has it.
    let mut next = spawn(&fixture.paths.root, "");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(next.try_wait().unwrap().is_none());
    fixture.stop(&mut first).await;
    let replacement = connect(&fixture.paths).await;
    assert_ne!(replacement.server_id(), client.server_id());
    fixture.stop(&mut next).await;
    assert!(!fixture.paths.endpoint().exists());
}
