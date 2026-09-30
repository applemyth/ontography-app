//! The server keeps the receipts clients may still need, says when a change
//! applied though its result could not be sent, and leaves no endpoint
//! directory behind.
#![cfg(unix)]

use ontography_app::{
    client::Client,
    persistence::Paths,
    protocol::{self, Request},
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
use tokio::process::{Child, Command};

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

struct Fixture {
    _directory: tempfile::TempDir,
    paths: Paths,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let paths = Paths::initialize(directory.path().join("data")).unwrap();
        Self {
            _directory: directory,
            paths,
        }
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
