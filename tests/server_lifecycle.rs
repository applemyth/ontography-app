#![cfg(unix)]

use ontography_app::{
    client::Client,
    persistence::Paths,
    protocol::{self, Request, Response},
};
use serde_json::{Value, json};
use std::{path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    process::Command,
};

const BIN: &str = env!("CARGO_BIN_EXE_ontography");

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

    async fn start(&self) -> Client {
        start(&self.paths.root).await;
        Client::connect(&self.paths.socket).await.unwrap()
    }

    async fn stop(&self) {
        command(&self.paths.root, &["server", "stop"]).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.paths.socket.exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("server must release its endpoint after orderly shutdown");
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
        let _ = std::fs::remove_dir(self.paths.socket.parent().unwrap());
    }
}

async fn command(root: &Path, args: &[&str]) -> Value {
    let output = tokio::time::timeout(
        Duration::from_secs(15),
        Command::new(BIN)
            .arg("--data-dir")
            .arg(root)
            .args(args)
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("CLI command timed out")
    .expect("spawn CLI");
    assert!(
        output.status.success(),
        "CLI {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("structured CLI result")
}

async fn start(root: &Path) -> Value {
    command(root, &["server", "start"]).await
}

fn declaration() -> Value {
    serde_json::from_str(include_str!("../examples/flow.json")).unwrap()
}

async fn create_run(client: &Client, project: &Path) -> String {
    client
        .call(
            "run.start",
            json!({"declaration":declaration(),"project":project}),
        )
        .await
        .unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn raw_request(paths: &Paths, request: &Request) -> Response {
    let mut socket = UnixStream::connect(&paths.socket).await.unwrap();
    protocol::write_frame(&mut socket, request).await.unwrap();
    let bytes = protocol::read_frame(&mut BufReader::new(socket))
        .await
        .unwrap()
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_startups_detach_and_reconnect_to_one_live_server() {
    let fixture = Fixture::new();
    let (first, second) = tokio::join!(start(&fixture.paths.root), start(&fixture.paths.root));
    assert_eq!(first["server_id"], second["server_id"]);
    let client = Client::connect(&fixture.paths.socket).await.unwrap();
    let original_server = client.server_id().to_owned();
    let status = client.call("system.status", json!({})).await.unwrap();
    let process_id = status["process_id"]
        .as_u64()
        .expect("status exposes server process identity");
    assert_ne!(process_id, u64::from(std::process::id()));
    let pid = nix::unistd::Pid::from_raw(i32::try_from(process_id).unwrap());
    assert_eq!(
        nix::unistd::getsid(Some(pid)).unwrap(),
        pid,
        "detached server leads its own OS session"
    );
    let run_id = create_run(&client, fixture.directory.path()).await;
    drop(client);

    // Both launchers have exited and there are no persistent management connections.
    let reattached = Client::connect(&fixture.paths.socket).await.unwrap();
    assert_eq!(reattached.server_id(), original_server);
    let inspected = reattached
        .call("run.inspect", json!({"run_id":run_id}))
        .await
        .unwrap();
    assert_eq!(inspected["status"], "active");
    assert_eq!(inspected["admission"], "open");
    assert_eq!(inspected["executions"], json!([]));

    fixture.stop().await;
    let restarted = fixture.start().await;
    assert_ne!(restarted.server_id(), original_server);
    let runs = restarted.call("run.list", json!({})).await.unwrap();
    assert_eq!(runs["runs"].as_array().unwrap().len(), 1);
    assert_eq!(runs["runs"][0]["run_id"], run_id);
    assert_eq!(runs["runs"][0]["status"], "suspended");
    let resumed = restarted
        .call("run.resume", json!({"run_id":run_id}))
        .await
        .unwrap();
    assert_eq!(resumed["admission"], "open");
    assert_eq!(resumed["revision"], inspected["revision"]);
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_response_is_recoverable_and_request_identity_prevents_duplicate_mutations() {
    let fixture = Fixture::new();
    let client = fixture.start().await;
    let request = Request {
        app_session_id: None,
        version: protocol::VERSION,
        client_id: uuid::Uuid::new_v4().to_string(),
        request_id: uuid::Uuid::new_v4().to_string(),
        expected_server_id: Some(client.server_id().to_owned()),
        operation: "run.start".into(),
        args: json!({"declaration":declaration(),"project":fixture.directory.path()}),
    };
    let mut socket = UnixStream::connect(&fixture.paths.socket).await.unwrap();
    protocol::write_frame(&mut socket, &request).await.unwrap();
    socket.shutdown().await.unwrap();
    drop(socket);

    let outcome = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match client
                .call(
                    "operation.get",
                    json!({"client_id":request.client_id,"request_id":request.request_id}),
                )
                .await
            {
                Ok(outcome) if outcome["state"] == "completed" => break outcome,
                Ok(outcome) => assert_eq!(
                    outcome["state"], "running",
                    "unexpected operation failure: {outcome}"
                ),
                Err(error) => assert_eq!(error.code, "unknown_outcome"),
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("server must finish and retain the accepted mutation after disconnect");
    let run_id = outcome["result"]["run_id"].as_str().unwrap();
    let duplicate = raw_request(&fixture.paths, &request)
        .await
        .into_result()
        .unwrap();
    assert_eq!(duplicate["run_id"], run_id);
    assert_eq!(
        client.call("run.list", json!({})).await.unwrap()["runs"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let mut conflict = request.clone();
    conflict.args["declaration"]["id"] = json!("changed-definition");
    assert_eq!(
        raw_request(&fixture.paths, &conflict)
            .await
            .into_result()
            .unwrap_err()
            .code,
        "request_id_conflict"
    );
    let mut foreign = request;
    foreign.request_id = uuid::Uuid::new_v4().to_string();
    foreign.expected_server_id = Some("an-old-server-instance".into());
    assert_eq!(
        raw_request(&fixture.paths, &foreign)
            .await
            .into_result()
            .unwrap_err()
            .code,
        "server_restarted"
    );
    assert_eq!(
        client.call("run.list", json!({})).await.unwrap()["runs"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    fixture.stop().await;
}

#[tokio::test]
async fn truncated_response_preserves_an_unknown_outcome_receipt_in_the_rust_client() {
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    let socket_path = directory.path().join("s");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let hello: Request = serde_json::from_slice(
            &protocol::read_frame(&mut BufReader::new(&mut stream))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        protocol::write_frame(&mut stream, &Response::new("fake-server", &hello.request_id, Ok(json!({
            "protocol_version":protocol::VERSION,"server_id":"fake-server","app_version":env!("CARGO_PKG_VERSION"),"core_version":ontography::VERSION,"app_build":ontography_app::APP_BUILD,"core_build":ontography_app::CORE_BUILD,"operations":[]
        })))).await.unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        let request: Request = serde_json::from_slice(
            &protocol::read_frame(&mut BufReader::new(&mut stream))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(request.expected_server_id.as_deref(), Some("fake-server"));
        // A response started, then the connection failed before its frame was complete.
        stream.write_all(b"{\"version\":1").await.unwrap();
        stream.shutdown().await.unwrap();
        request
    });
    let client = Client::connect(&socket_path).await.unwrap();
    let error = client.call("run.start", json!({})).await.unwrap_err();
    let sent = server.await.unwrap();
    assert_eq!(error.code, "unknown_outcome");
    let details = error.details.as_ref().unwrap();
    assert_eq!(details["client_id"], sent.client_id);
    assert_eq!(details["request_id"], sent.request_id);
    assert_eq!(details["server_id"], "fake-server");
    assert!(
        error.message.contains(&sent.request_id),
        "CLI error text must preserve the actionable request identity"
    );
}

#[tokio::test]
async fn incompatible_server_handshake_is_reported_before_any_operation() {
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    let socket_path = directory.path().join("s");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request: Request = serde_json::from_slice(
            &protocol::read_frame(&mut BufReader::new(&mut stream))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(request.operation, "system.hello");
        protocol::write_frame(&mut stream, &Response::new("old-server", &request.request_id, Ok(json!({
            "protocol_version":protocol::VERSION,"server_id":"old-server","app_version":env!("CARGO_PKG_VERSION"),"core_version":ontography::VERSION,"app_build":"incompatible-build","core_build":ontography_app::CORE_BUILD,"operations":[]
        })))).await.unwrap();
    });
    assert_eq!(
        Client::connect(&socket_path).await.unwrap_err().code,
        "incompatible_server"
    );
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completing_one_request_does_not_drop_the_partial_frame_of_the_next() {
    let fixture = Fixture::new();
    let client = fixture.start().await;
    let socket = UnixStream::connect(&fixture.paths.socket).await.unwrap();
    let (read, mut write) = socket.into_split();
    let mut read = BufReader::new(read);
    let first = Request {
        app_session_id: None,
        version: protocol::VERSION,
        client_id: uuid::Uuid::new_v4().to_string(),
        request_id: uuid::Uuid::new_v4().to_string(),
        expected_server_id: Some(client.server_id().to_owned()),
        operation: "run.start".into(),
        args: json!({"declaration":declaration(),"project":fixture.directory.path()}),
    };
    let next = Request {
        request_id: uuid::Uuid::new_v4().to_string(),
        operation: "system.status".into(),
        args: json!({}),
        ..first.clone()
    };
    let mut combined = serde_json::to_vec(&first).unwrap();
    combined.push(b'\n');
    let next_frame = serde_json::to_vec(&next).unwrap();
    let split = next_frame.len() / 2;
    combined.extend_from_slice(&next_frame[..split]);
    write.write_all(&combined).await.unwrap();
    let completed = tokio::time::timeout(Duration::from_secs(10), protocol::read_frame(&mut read))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let response: Response = serde_json::from_slice(&completed).unwrap();
    assert_eq!(response.request_id, first.request_id);
    response.into_result().unwrap();
    write.write_all(&next_frame[split..]).await.unwrap();
    write.write_all(b"\n").await.unwrap();
    let completed = tokio::time::timeout(Duration::from_secs(5), protocol::read_frame(&mut read))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let response: Response = serde_json::from_slice(&completed).unwrap();
    assert_eq!(response.request_id, next.request_id);
    assert_eq!(
        response.into_result().unwrap()["server_id"],
        client.server_id()
    );
    drop(write);
    drop(read);
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_and_receipt_bypass_a_long_wait_on_the_same_connection() {
    let fixture = Fixture::new();
    let client = fixture.start().await;
    let start_id = uuid::Uuid::new_v4().to_string();
    let started = client
        .request(
            "run.start",
            json!({"declaration":declaration(),"project":fixture.directory.path()}),
            start_id.clone(),
        )
        .await
        .unwrap()
        .into_result()
        .unwrap();
    let socket = UnixStream::connect(&fixture.paths.socket).await.unwrap();
    let (read, mut write) = socket.into_split();
    let mut read = BufReader::new(read);
    let wait = Request {
        app_session_id: None,
        version: protocol::VERSION,
        client_id: client.client_id().into(),
        request_id: uuid::Uuid::new_v4().to_string(),
        expected_server_id: Some(client.server_id().into()),
        operation: "inspect.wait_frontier".into(),
        args: json!({"run_id":started["run_id"],"after_revision":"0","timeout_ms":30000}),
    };
    let status = Request {
        request_id: uuid::Uuid::new_v4().to_string(),
        operation: "system.status".into(),
        args: json!({}),
        ..wait.clone()
    };
    let receipt = Request {
        request_id: uuid::Uuid::new_v4().to_string(),
        operation: "operation.get".into(),
        args: json!({"client_id":client.client_id(),"request_id":start_id}),
        ..wait.clone()
    };
    for request in [&wait, &status, &receipt] {
        let mut frame = serde_json::to_vec(request).unwrap();
        frame.push(b'\n');
        write.write_all(&frame).await.unwrap();
    }
    let replies = tokio::time::timeout(Duration::from_secs(3), async {
        let mut replies = std::collections::BTreeMap::new();
        for _ in 0..2 {
            let frame = protocol::read_frame(&mut read).await.unwrap().unwrap();
            let response: Response = serde_json::from_slice(&frame).unwrap();
            assert_ne!(
                response.request_id, wait.request_id,
                "the frontier wait must remain pending while status/receipt return"
            );
            replies.insert(response.request_id.clone(), response.into_result().unwrap());
        }
        replies
    })
    .await
    .expect("a long request cannot block status or receipt inspection on its connection");
    assert_eq!(replies[&status.request_id]["server_id"], client.server_id());
    assert_eq!(replies[&receipt.request_id]["state"], "completed");
    assert_eq!(
        replies[&receipt.request_id]["result"]["run_id"],
        started["run_id"]
    );
    client
        .call("run.suspend", json!({"run_id":started["run_id"]}))
        .await
        .unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(3), protocol::read_frame(&mut read))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let response: Response = serde_json::from_slice(&frame).unwrap();
    assert_eq!(response.request_id, wait.request_id);
    response.into_result().unwrap();
    drop(read);
    drop(write);
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_server_recovers_commits_only_on_resume_and_expires_transient_capabilities() {
    let fixture = Fixture::new();
    let client = fixture.start().await;
    let run_id = create_run(&client, fixture.directory.path()).await;
    let committed = Request {
        app_session_id: None,
        version: protocol::VERSION,
        client_id: client.client_id().into(),
        request_id: uuid::Uuid::new_v4().to_string(),
        expected_server_id: Some(client.server_id().into()),
        operation: "workflow.submit".into(),
        args: json!({"run_id":run_id,"trigger":{"kind":"root","node_id":"A","authority":["work"]},"result":"committed before crash","emissions":[{"edge_id":"A_to_B","payload":"retained payload"}]}),
    };
    let accepted = raw_request(&fixture.paths, &committed)
        .await
        .into_result()
        .unwrap();
    let before = client
        .call("run.inspect", json!({"run_id":run_id}))
        .await
        .unwrap();
    let package_id = before["frontier"]["received"][0]["package_id"].clone();
    assert_eq!(
        before["frontier"]["received"][0]["producer"],
        accepted["activation_id"]
    );
    let invocation = client.call("invocation.begin", json!({"run_id":run_id,"node_id":"B","trigger":{"kind":"packages","package_ids":[package_id]}})).await.unwrap();
    let invocation_id = invocation["invocation_id"].clone();
    client.call("context.record",json!({"run_id":run_id,"invocation_id":invocation_id,"payload":"prepared before crash"})).await.unwrap();
    let open_records = client
        .call("invocation.list", json!({"run_id":run_id}))
        .await
        .unwrap();
    assert_eq!(
        open_records["invocations"][0]["invocation_id"],
        invocation_id
    );
    assert_eq!(open_records["invocations"][0]["status"], "open");
    let plan = client.call("rewrite.prepare", json!({"run_id":run_id,"request":{"production_id":"remove_receiver","nodes":{"A":"A","B":"B"},"edges":{"A_to_B":"A_to_B"}}})).await.unwrap();
    assert_eq!(plan["revision"], before["revision"]);

    // The PID comes from this test's unique data directory and validated
    // server-instance handshake. Never discover or signal unrelated processes.
    let status = client.call("system.status", json!({})).await.unwrap();
    assert_eq!(status["server_id"], client.server_id());
    assert_eq!(
        status["data_dir"],
        serde_json::to_value(&fixture.paths.root).unwrap()
    );
    let process_id = i32::try_from(status["process_id"].as_u64().unwrap()).unwrap();
    assert!(process_id > 1);
    assert_ne!(process_id, i32::try_from(std::process::id()).unwrap());
    let pid = nix::unistd::Pid::from_raw(process_id);
    assert_eq!(nix::unistd::getsid(Some(pid)).unwrap(), pid);
    let killed = Command::new("/bin/kill")
        .args(["-KILL", &process_id.to_string()])
        .status()
        .await
        .unwrap();
    assert!(killed.success());
    tokio::time::timeout(Duration::from_secs(5), async {
        while UnixStream::connect(&fixture.paths.socket).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the killed fixture server must release its socket listener");

    let restarted = fixture.start().await;
    assert_ne!(restarted.server_id(), client.server_id());
    let listed = restarted.call("run.list", json!({})).await.unwrap();
    assert_eq!(listed["runs"].as_array().unwrap().len(), 1);
    assert_eq!(listed["runs"][0]["run_id"], run_id);
    assert_eq!(listed["runs"][0]["status"], "recoverable");
    let dormant = restarted
        .call("run.inspect", json!({"run_id":run_id}))
        .await
        .unwrap();
    assert_eq!(dormant["admission"], Value::Null);
    assert!(dormant.get("revision").is_none());
    assert_eq!(
        restarted
            .call("inspect.frontier", json!({"run_id":run_id}))
            .await
            .unwrap_err()
            .code,
        "run_suspended"
    );
    // Requests addressed to the killed instance must reject before dispatch;
    // its transient outcome records also cannot be recovered from a replacement.
    assert_eq!(
        raw_request(&fixture.paths, &committed)
            .await
            .into_result()
            .unwrap_err()
            .code,
        "server_restarted"
    );
    assert_eq!(
        restarted
            .call(
                "operation.get",
                json!({"client_id":committed.client_id,"request_id":committed.request_id})
            )
            .await
            .unwrap_err()
            .code,
        "unknown_outcome"
    );

    let resumed = restarted
        .call("run.resume", json!({"run_id":run_id}))
        .await
        .unwrap();
    assert_eq!(resumed["admission"], "open");
    assert_eq!(resumed["revision"], before["revision"]);
    assert_eq!(resumed["graph"], before["graph"]);
    assert_eq!(resumed["frontier"], before["frontier"]);
    let records = restarted
        .call("invocation.list", json!({"run_id":run_id}))
        .await
        .unwrap();
    let record = records["invocations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|record| record["invocation_id"] == invocation_id)
        .unwrap();
    assert_eq!(record["status"], "interrupted");
    assert_eq!(
        restarted
            .call(
                "invocation.inspect",
                json!({"run_id":run_id,"invocation_id":invocation_id})
            )
            .await
            .unwrap_err()
            .code,
        "unknown_handle"
    );
    assert_eq!(
        restarted
            .call(
                "rewrite.commit",
                json!({"run_id":run_id,"plan_id":plan["plan_id"]})
            )
            .await
            .unwrap_err()
            .code,
        "unknown_handle"
    );
    let events = restarted
        .call(
            "context.events",
            json!({"run_id":run_id,"invocation_id":invocation_id}),
        )
        .await
        .unwrap();
    assert!(!events["events"].as_array().unwrap().is_empty());
    assert_eq!(
        restarted
            .call("run.inspect", json!({"run_id":run_id}))
            .await
            .unwrap()["frontier"],
        before["frontier"]
    );
    fixture.stop().await;
}
