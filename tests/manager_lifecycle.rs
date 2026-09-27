use ontography_app::{persistence::Paths, protocol::Request, server::Server};
use serde_json::{Value, json};
use std::{os::unix::fs::PermissionsExt, sync::Arc};
use tokio::io::{AsyncWriteExt, BufReader};

async fn wait_mode(server: &Arc<Server>, id: &str, mode: &str) -> Value {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let status = call(server, Some(id), "terminal.status", json!({}))
                .await
                .unwrap();
            if status["manager_mode"] == mode {
                break status;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}

async fn input(writer: &mut tokio::net::unix::OwnedWriteHalf, status: &Value, bytes: &[u8]) {
    let frame = ontography_app::terminal::ClientFrame::Input {
        terminal_id: status["terminal_id"].as_str().unwrap().into(),
        bytes: bytes.to_vec(),
    };
    let mut bytes = serde_json::to_vec(&frame).unwrap();
    bytes.push(b'\n');
    writer.write_all(&bytes).await.unwrap();
}

async fn attach_terminal(
    server: &Arc<Server>,
    id: &str,
    status: &Value,
) -> (
    tokio::net::unix::OwnedWriteHalf,
    tokio::task::JoinHandle<()>,
) {
    let stream = tokio::net::UnixStream::connect(status["socket"].as_str().unwrap())
        .await
        .unwrap();
    let (read, mut write) = stream.into_split();
    let request = ontography_app::terminal::AttachRequest {
        version: ontography_app::terminal::VERSION,
        server_id: server.service.server_id.clone(),
        session_id: id.into(),
        terminal_id: status["terminal_id"].as_str().unwrap().into(),
        rows: 24,
        cols: 80,
    };
    let mut bytes = serde_json::to_vec(&request).unwrap();
    bytes.push(b'\n');
    write.write_all(&bytes).await.unwrap();
    let mut reader = BufReader::new(read);
    let first: ontography_app::terminal::ServerFrame = serde_json::from_slice(
        &ontography_app::protocol::read_frame(&mut reader)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        first,
        ontography_app::terminal::ServerFrame::Attached { .. }
    ));
    let drain = tokio::spawn(async move {
        while matches!(
            ontography_app::protocol::read_frame(&mut reader).await,
            Ok(Some(_))
        ) {}
    });
    (write, drain)
}

#[tokio::test]
async fn foreground_signals_and_lost_launcher_lease_preserve_shell_and_reap_pi() {
    use nix::{
        sys::signal::{Signal, kill},
        unistd::{Pid, getpgid},
    };
    let directory = tempfile::tempdir().unwrap();
    let server = Server::new(Paths::initialize(directory.path().join("store")).unwrap()).unwrap();
    let pi = directory.path().join("pi-fixture");
    let interrupted = directory.path().join("interrupted");
    let ready = directory.path().join("ready");
    std::fs::write(&pi,format!("#!/bin/sh\nif [ \"$1\" = --version ]; then echo 0.85.1; exit; fi\ntrap 'printf interrupted > {}' INT\nprintf ready > {}\nprintf 'ready\\n'\nwhile :; do IFS= read -r line || continue; [ \"$line\" = /quit ] && exit 0; done\n",interrupted.display(),ready.display())).unwrap();
    std::fs::set_permissions(&pi, std::fs::Permissions::from_mode(0o700)).unwrap();
    let session = call(
        &server,
        None,
        "session.create",
        json!({"project":directory.path()}),
    )
    .await
    .unwrap();
    let id = session["session_id"].as_str().unwrap();
    call(&server, Some(id), "terminal.ensure", json!({"pi":pi}))
        .await
        .unwrap();
    let running = wait_mode(&server, id, "pi").await;
    let (mut writer, drain) = attach_terminal(&server, id, &running).await;
    // Process registration precedes the fixture installing its signal handler.
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !ready.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    input(&mut writer, &running, b"\x03").await;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !interrupted.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        call(&server, Some(id), "terminal.status", json!({}))
            .await
            .unwrap()["manager_pid"],
        running["manager_pid"]
    );
    let pi_pid = Pid::from_raw(running["manager_pid"].as_i64().unwrap() as i32);
    let launcher = getpgid(Some(pi_pid)).unwrap();
    assert_ne!(
        launcher,
        Pid::from_raw(running["pid"].as_i64().unwrap() as i32)
    );
    kill(launcher, Signal::SIGKILL).unwrap();
    let shell = wait_mode(&server, id, "shell").await;
    assert_eq!(shell["pid"], running["pid"]);
    assert_eq!(shell["terminal_id"], running["terminal_id"]);
    assert_eq!(shell["running"], true);
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while kill(pi_pid, None).is_ok() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    input(&mut writer, &shell, b"pi\n").await;
    let resumed = wait_mode(&server, id, "pi").await;
    assert_ne!(resumed["manager_pid"], running["manager_pid"]);
    input(&mut writer, &resumed, b"/quit\n").await;
    let shell = wait_mode(&server, id, "shell").await;
    assert!(shell["manager_pid"].is_null());
    server.stop().await.unwrap();
    drain.abort();
}

async fn call(
    server: &Arc<Server>,
    session: Option<&str>,
    operation: &str,
    args: Value,
) -> ontography_app::Result<Value> {
    server
        .request(Request {
            version: ontography_app::protocol::VERSION,
            client_id: "manager-lifecycle-test".into(),
            request_id: uuid::Uuid::new_v4().to_string(),
            operation: operation.into(),
            app_session_id: session.map(str::to_owned),
            expected_server_id: Some(server.service.server_id.clone()),
            args,
        })
        .await
}

#[tokio::test]
async fn session_manager_is_unique_and_survives_its_request_owner() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("store")).unwrap();
    let server = Server::new(paths).unwrap();
    let pi = directory.path().join("pi-fixture");
    std::fs::write(&pi, "#!/bin/sh\nif [ \"$1\" = --version ]; then echo 0.85.1; exit; fi\nprintf 'manager ready\\r\\n'\nwhile IFS= read -r line; do printf '%s\\r\\n' \"$line\"; done\n").unwrap();
    std::fs::set_permissions(&pi, std::fs::Permissions::from_mode(0o700)).unwrap();
    let session = call(
        &server,
        None,
        "session.create",
        json!({"project":directory.path()}),
    )
    .await
    .unwrap();
    let id = session["session_id"].as_str().unwrap();
    let args = json!({"pi":pi,"rows":24,"cols":80});
    let (one, two) = tokio::join!(
        call(&server, Some(id), "terminal.ensure", args.clone()),
        call(&server, Some(id), "terminal.ensure", args)
    );
    let one = one.unwrap();
    let two = two.unwrap();
    let running = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let status = call(&server, Some(id), "terminal.status", json!({}))
                .await
                .unwrap();
            if status["manager_mode"] == "pi" {
                break status;
            }
            assert!(status["manager_error"].is_null(), "{status}");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(running["manager_pid"].as_u64().is_some());
    assert_ne!(running["manager_pid"], running["pid"]);
    assert_eq!(one["pid"], two["pid"]);
    assert_eq!(one["terminal_id"], two["terminal_id"]);
    assert_eq!(
        call(&server, Some(id), "terminal.status", json!({}))
            .await
            .unwrap()["running"],
        true
    );
    // Manager can run before a graph exists, and suspension must still reap it.
    assert!(session["run_id"].is_null());
    assert_eq!(
        call(&server, Some(id), "session.suspend", json!({}))
            .await
            .unwrap()["status"],
        "suspended"
    );
    assert_eq!(
        call(&server, Some(id), "terminal.status", json!({}))
            .await
            .unwrap()["running"],
        false
    );
    assert!(!std::path::Path::new(one["socket"].as_str().unwrap()).exists());
    assert_eq!(
        call(&server, Some(id), "terminal.ensure", json!({"pi":pi}))
            .await
            .unwrap_err()
            .code,
        "session_inactive"
    );
    call(&server, None, "session.resume", json!({"session_id":id}))
        .await
        .unwrap();
    let restarted = call(&server, Some(id), "terminal.ensure", json!({"pi":pi}))
        .await
        .unwrap();
    assert_ne!(one["terminal_id"], restarted["terminal_id"]);
    server.stop().await.unwrap();
    assert!(!std::path::Path::new(restarted["socket"].as_str().unwrap()).exists());
}
