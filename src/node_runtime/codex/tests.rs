use super::*;
use crate::node_runtime::{process::Lifetime, rpc::Rpc};
use crate::terminal::Terminal;
use futures_util::{SinkExt, StreamExt};
use std::{os::unix::fs::PermissionsExt, time::Duration};
use tokio::net::UnixListener;

mod native_delivery;

const THREAD: &str = "6b121953-b116-4a0f-b2dd-a63c293e2c85";

fn node() -> DocumentNode {
    serde_json::from_value(json!({"id":"writer","kind":"agent","config":{"prompt":"Review changes","model":"test-model"}})).unwrap()
}

#[tokio::test]
async fn session_persists_exact_identity_and_removes_only_stale_graph_queue_entries() {
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    let mut definition = node();
    let plan =
        Plan::with_home(&definition, dir.path(), dir.path(), dir.path(), dir.path()).unwrap();
    let listener = UnixListener::bind(&plan.socket).unwrap();
    let server = tokio::spawn(async move {
        let mut seen = Vec::new();
        for connection in 0..3 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            while let Some(Ok(frame)) = socket.next().await {
                let Ok(text) = frame.to_text() else {
                    continue;
                };
                let call: Value = serde_json::from_str(text).unwrap();
                seen.push(call.clone());
                if call["method"] == "initialized" {
                    assert!(call.get("id").is_none());
                    continue;
                }
                if connection == 2 && call["method"] == "thread/resume" {
                    socket.send(tokio_tungstenite::tungstenite::Message::Text(json!({"id":call["id"],"error":{"code":-1,"message":"saved thread missing"}}).to_string().into())).await.unwrap();
                    continue;
                }
                let result = match call["method"].as_str().unwrap() {
                    "thread/start" | "thread/resume" => json!({"thread":{"id":THREAD}}),
                    "thread/queue/list" => {
                        json!({"data":[{"id":"stale","clientUserMessageId":"ontography:old"},{"id":"human","clientUserMessageId":"human-input"}],"nextCursor":null})
                    }
                    _ => json!({}),
                };
                socket
                    .send(tokio_tungstenite::tungstenite::Message::Text(
                        json!({"id":call["id"],"result":result}).to_string().into(),
                    ))
                    .await
                    .unwrap();
            }
        }
        seen
    });
    let rpc = Rpc::connect(&plan.socket).await.unwrap();
    assert_eq!(plan.open(&rpc).await.unwrap(), THREAD);
    plan.attach(THREAD).unwrap();
    assert_eq!(std::fs::read_to_string(&plan.ready).unwrap().trim(), THREAD);
    drop(rpc);
    let next =
        Plan::with_home(&definition, dir.path(), dir.path(), dir.path(), dir.path()).unwrap();
    let rpc = Rpc::connect(&plan.socket).await.unwrap();
    assert_eq!(next.open(&rpc).await.unwrap(), THREAD);
    drop(rpc);
    let rpc = Rpc::connect(&plan.socket).await.unwrap();
    let error = next.open(&rpc).await.unwrap_err();
    assert!(error.message.contains("saved thread missing"));
    let saved: SavedSession = read_json(&dir.path().join("codex-session.json")).unwrap();
    assert_eq!(saved.conversation_id, THREAD);
    drop(rpc);
    let seen = server.await.unwrap();
    assert_eq!(
        seen.iter()
            .filter(|call| call["method"] == "thread/start")
            .count(),
        1
    );
    assert_eq!(
        seen.iter()
            .filter(|call| call["method"] == "thread/resume")
            .count(),
        2
    );
    let deleted: Vec<_> = seen
        .iter()
        .filter(|call| call["method"] == "thread/queue/delete")
        .map(|call| call["params"]["queuedSubmissionId"].clone())
        .collect();
    assert_eq!(deleted, [json!("stale"), json!("stale")]);
    assert!(!seen.iter().any(|call| call["method"] == "turn/start"));
    definition.id = "another-node".into();
    assert!(Plan::with_home(&definition, dir.path(), dir.path(), dir.path(), dir.path()).is_err());
    assert!(
        Plan::with_home(
            &node(),
            dir.path(),
            dir.path(),
            dir.path(),
            Path::new("relative")
        )
        .is_err()
    );
}

#[tokio::test]
#[ignore = "requires installed Codex; verifies native server/TUI with a local-only model provider"]
async fn native_codex_server_and_pane_share_a_thread() {
    let dir = tempfile::Builder::new()
        .prefix("ontography-codex-server-")
        .tempdir_in("/tmp")
        .unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let home = dir.path().join("home");
    let cwd = dir.path().join("workspace");
    std::fs::create_dir(&home).unwrap();
    std::fs::create_dir(&cwd).unwrap();
    std::fs::write(
        home.join("config.toml"),
        format!(
            "[projects.{}]\ntrust_level = \"trusted\"\n",
            serde_json::to_string(&std::fs::canonicalize(&cwd).unwrap()).unwrap()
        ),
    )
    .unwrap();
    let plan = Plan::with_home(&node(), dir.path(), &cwd, dir.path(), &home).unwrap();
    let overrides = vec![
        "model_provider=\"fixture\"".into(),
        "model_providers.fixture={name=\"Fixture\",base_url=\"http://127.0.0.1:1/v1\",wire_api=\"responses\",requires_openai_auth=false,request_max_retries=0,stream_max_retries=0}".into(),
        format!("projects.{}.trust_level=\"trusted\"",serde_json::to_string(&std::fs::canonicalize(&cwd).unwrap()).unwrap()),
    ];
    let env = BTreeMap::from([("CODEX_HOME".into(), home.to_string_lossy().into_owned())]);
    let mut lifetime = Lifetime::new(dir.path()).unwrap();
    let spec = plan.launch(Path::new("codex"), &overrides, env.clone());
    let attach = crate::terminal::AttachRequest {
        version: crate::terminal::VERSION,
        server_id: spec.server_id.clone(),
        session_id: spec.session_id.clone(),
        terminal_id: String::new(),
        rows: 24,
        cols: 80,
    };
    let terminal = Terminal::launch(lifetime.supervise(spec), dir.path().join("terminal.sock"))
        .await
        .unwrap();
    lifetime.permit(&terminal).await.unwrap();
    let rpc = tokio::time::timeout(Duration::from_secs(15), async {
        while !plan.socket.exists() {
            assert!(terminal.status().running, "{}", terminal.snapshot().screen);
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Rpc::connect(&plan.socket).await.unwrap()
    })
    .await
    .unwrap();
    let id = plan.open(&rpc).await.unwrap();
    plan.attach(&id).unwrap();
    let observed = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let screen = terminal.snapshot().screen;
            if screen.contains("Codex")
                && (screen.contains("context") || screen.contains("shortcuts"))
            {
                break;
            }
            assert!(terminal.status().running, "{screen}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    let screen = terminal.snapshot().screen;
    let read = rpc
        .call("thread/read", json!({"threadId":id}))
        .await
        .unwrap();
    assert_eq!(read["thread"]["id"], id);
    assert_eq!(read["thread"]["modelProvider"], "fixture");
    assert_eq!(read["thread"]["status"]["type"], "idle");
    assert!(observed.is_ok(), "native composer did not render: {screen}");
    let mut stream = tokio::net::UnixStream::connect(terminal.status().socket)
        .await
        .unwrap();
    crate::protocol::write_frame(
        &mut stream,
        &crate::terminal::AttachRequest {
            terminal_id: terminal.id().into(),
            ..attach
        },
    )
    .await
    .unwrap();
    let mut attached = tokio::io::BufReader::new(stream);
    let response: crate::terminal::ServerFrame = serde_json::from_slice(
        &crate::protocol::read_frame(&mut attached)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        response,
        crate::terminal::ServerFrame::Attached { .. }
    ));
    let (mut output, mut input) = attached.into_inner().into_split();
    let drain = tokio::spawn(async move {
        let _ = tokio::io::copy(&mut output, &mut tokio::io::sink()).await;
    });
    crate::protocol::write_frame(
        &mut input,
        &crate::terminal::ClientFrame::Input {
            terminal_id: terminal.id().into(),
            bytes: b"/exit".to_vec(),
        },
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    crate::protocol::write_frame(
        &mut input,
        &crate::terminal::ClientFrame::Input {
            terminal_id: terminal.id().into(),
            bytes: b"\r".to_vec(),
        },
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !terminal.snapshot().screen.contains("pane closed") {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("TUI did not close: {}", terminal.snapshot().screen));
    assert_eq!(
        rpc.call("thread/read", json!({"threadId":id}))
            .await
            .unwrap()["thread"]["status"]["type"],
        "idle",
        "closing the TUI must leave the loaded conversation alive"
    );
    drop(input);
    drain.abort();
    drop(rpc);
    lifetime.disconnect();
    terminal.shutdown().await.unwrap();
    observed.unwrap_or_else(|_| {
        panic!(
            "Native pane did not render: {screen}; log: {}",
            std::fs::read_to_string(dir.path().join("codex-server.log")).unwrap()
        )
    });
    crate::workflow::harness::recover_process(dir.path())
        .await
        .unwrap();
    let resumed = Plan::with_home(&node(), dir.path(), &cwd, dir.path(), &home).unwrap();
    let mut lifetime = Lifetime::new(dir.path()).unwrap();
    let terminal = Terminal::launch(
        lifetime.supervise(resumed.launch(Path::new("codex"), &overrides, env)),
        dir.path().join("terminal.sock"),
    )
    .await
    .unwrap();
    lifetime.permit(&terminal).await.unwrap();
    let rpc = tokio::time::timeout(Duration::from_secs(15), async {
        while !resumed.socket.exists() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Rpc::connect(&resumed.socket).await.unwrap()
    })
    .await
    .unwrap();
    assert_eq!(resumed.open(&rpc).await.unwrap(), id);
    drop(rpc);
    lifetime.disconnect();
    terminal.shutdown().await.unwrap();
}
