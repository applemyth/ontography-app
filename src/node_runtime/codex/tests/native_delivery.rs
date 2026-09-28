//! Real Codex runs against a local, deterministic Responses fixture. No model
//! request leaves localhost; Codex itself executes the fixture's MCP tool calls.
use super::*;
use crate::{
    node_mcp::NodeMcp,
    node_tool::tests::{Fixture, document},
};
use std::sync::Arc;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::{Notify, mpsc},
};

#[tokio::test]
#[ignore = "requires installed Codex; runs real turns against a local Responses fixture"]
async fn native_messages_queue_during_work_and_publish_replies_through_mcp() {
    run_delivery(false).await;
}

#[tokio::test]
#[ignore = "requires installed Codex; verifies native approval UI against a local Responses fixture"]
async fn native_terminal_can_approve_a_controller_started_turn() {
    run_delivery(true).await;
}

async fn run_delivery(interactive: bool) {
    let fixture = Fixture::new(document(json!({})), "worker", None).await;
    let dir = tempfile::Builder::new()
        .prefix("onto-native-")
        .tempdir_in("/tmp")
        .unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let home = dir.path().join("home");
    let cwd = dir.path().join("workspace");
    std::fs::create_dir(&home).unwrap();
    std::fs::create_dir(&cwd).unwrap();
    let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = provider.local_addr().unwrap();
    std::fs::write(
        home.join("config.toml"),
        format!(
            r#"
model_provider = "fixture"
model = "fixture"
approval_policy = {policy:?}
[model_providers.fixture]
name = "Fixture"
base_url = "http://{address}/v1"
wire_api = "responses"
requires_openai_auth = false
request_max_retries = 0
stream_max_retries = 0
[projects.{cwd}]
trust_level = "trusted"
[mcp_servers.ontography_node.tools.submit_invocation]
approval_mode = {approval:?}
"#,
            policy = if interactive { "on-request" } else { "never" },
            approval = if interactive { "prompt" } else { "approve" },
            cwd = serde_json::to_string(&std::fs::canonicalize(&cwd).unwrap()).unwrap()
        ),
    )
    .unwrap();
    let release = Arc::new(Notify::new());
    let (requests, mut observed) = mpsc::unbounded_channel();
    let model = tokio::spawn({
        let release = release.clone();
        async move {
            let mut sent = std::collections::HashSet::new();
            loop {
                let (stream, _) = provider.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                let mut header = String::new();
                let mut length = 0;
                loop {
                    header.clear();
                    if reader.read_line(&mut header).await.unwrap() == 0 {
                        break;
                    }
                    if header == "\r\n" {
                        break;
                    }
                    if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:")
                    {
                        length = value.trim().parse::<usize>().unwrap();
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).await.unwrap();
                let request: Value = serde_json::from_slice(&body).unwrap();
                requests.send(request.clone()).unwrap();
                if sent.is_empty() {
                    release.notified().await;
                }
                let delivery = request["input"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .rev()
                    .filter(|item| item["role"] == "user")
                    .flat_map(|item| item["content"].as_array().into_iter().flatten())
                    .filter_map(|part| {
                        part["text"]
                            .as_str()
                            .and_then(|text| serde_json::from_str::<Value>(text).ok())
                    })
                    .find(|value| value["type"] == "ontography_message")
                    .expect("incoming graph message reached the model");
                let attempt = delivery["attempt_id"].as_str().unwrap();
                let item = if sent.insert(attempt.to_owned()) {
                    let namespace = request["tools"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|tool| {
                            tool["tools"].as_array().is_some_and(|tools| {
                                tools.iter().any(|tool| tool["name"] == "submit_invocation")
                            })
                        })
                        .expect("submit_invocation is exposed through MCP");
                    json!({"type":"function_call","id":format!("fc_{}",sent.len()),"call_id":format!("call_{}",sent.len()),"namespace":namespace["name"],"name":"submit_invocation","arguments":json!({"attempt_id":attempt,"result":{"message":format!("Reply to {}",delivery["inputs"][0]["message"].as_str().unwrap())}}).to_string()})
                } else {
                    json!({"type":"message","role":"assistant","id":"msg_done","content":[{"type":"output_text","text":"Delivered.","annotations":[]}]})
                };
                let events = [
                    json!({"type":"response.created","response":{"id":"resp_fixture","status":"in_progress"}}),
                    json!({"type":"response.output_item.done","output_index":0,"item":item}),
                    json!({"type":"response.completed","response":{"id":"resp_fixture","status":"completed","output":[item],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}),
                ];
                let body: String = events
                    .iter()
                    .map(|event| {
                        format!(
                            "event: {}\ndata: {event}\n\n",
                            event["type"].as_str().unwrap()
                        )
                    })
                    .collect();
                reader.get_mut().write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
        }
    });
    let mut mcp = NodeMcp::bind(dir.path(), fixture.tools.clone()).unwrap();
    let scope = fixture.scope.borrow().clone();
    let crate::workflow::Implementation::Codex(config) = &scope.binding.implementation else {
        panic!("the fixture's worker is a Codex agent");
    };
    let plan =
        Plan::with_home(&scope.node.id, config, dir.path(), &cwd, dir.path(), &home).unwrap();
    let overrides = mcp.codex_overrides().unwrap();
    let mut env = mcp.environment();
    env.insert("CODEX_HOME".into(), home.to_string_lossy().into_owned());
    let mut lifetime = Lifetime::new(dir.path()).unwrap();
    let spec = plan.launch(Path::new("codex"), &overrides, env);
    let attach = crate::terminal::AttachRequest {
        version: crate::terminal::VERSION,
        server_id: spec.server_id.clone(),
        session_id: spec.session_id.clone(),
        terminal_id: String::new(),
        rows: 40,
        cols: 140,
    };
    let terminal = Terminal::launch(lifetime.supervise(spec), dir.path().join("terminal.sock"))
        .await
        .unwrap();
    lifetime.permit(&terminal).await.unwrap();
    let mut rpc = tokio::time::timeout(Duration::from_secs(15), async {
        while !plan.socket.exists() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Rpc::connect(&plan.socket).await.unwrap()
    })
    .await
    .unwrap();
    let thread = plan.open(&rpc).await.unwrap();
    plan.attach(&thread).unwrap();
    let (mut input, drain) = if interactive {
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
        let mut reader = BufReader::new(stream);
        let attached: crate::terminal::ServerFrame = serde_json::from_slice(
            &crate::protocol::read_frame(&mut reader)
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            attached,
            crate::terminal::ServerFrame::Attached { .. }
        ));
        let (mut output, input) = reader.into_inner().into_split();
        let drain = tokio::spawn(async move {
            let _ = tokio::io::copy(&mut output, &mut tokio::io::sink()).await;
        });
        tokio::time::timeout(Duration::from_secs(15), async {
            while !(terminal.snapshot().screen.contains("shortcuts")
                || terminal.snapshot().screen.contains("context"))
            {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "Native approval pane did not render: {}",
                terminal.snapshot().screen
            )
        });
        (Some(input), Some(drain))
    } else {
        (None, None)
    };
    let observer = Rpc::connect(&plan.socket).await.unwrap();
    observer
        .call("thread/resume", json!({"threadId":thread}))
        .await
        .unwrap();
    fixture.deliver("first").await;
    let delivery = tokio::spawn({
        let thread = thread.clone();
        let tools = fixture.tools.clone();
        async move { deliver(&mut rpc, &thread, &tools, |_| {}).await }
    });
    let result = tokio::time::timeout(Duration::from_secs(25), async {
        let first = observed.recv().await.unwrap();
        assert!(first.to_string().contains("first"));
        fixture.deliver("second").await;
        loop {
            let queued = observer
                .call("thread/queue/list", json!({"threadId":thread}))
                .await
                .unwrap();
            if queued["data"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item.to_string().contains("second"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            observed.try_recv().is_err(),
            "second message must not steer or interrupt the active model request"
        );
        release.notify_one();
        let mut approved = 0;
        loop {
            if let Some(input) = &mut input {
                let screen = terminal.snapshot().screen;
                if screen.contains("submit_invocation") && screen.contains("1. Allow") {
                    crate::protocol::write_frame(
                        input,
                        &crate::terminal::ClientFrame::Input {
                            terminal_id: terminal.id().into(),
                            bytes: b"\r".to_vec(),
                        },
                    )
                    .await
                    .unwrap();
                    approved += 1;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
            let mut pending = fixture.pending("sink").await;
            if pending.len() == 2 {
                // Core inbox order follows package identity, not publication time.
                pending.sort();
                assert_eq!(pending, ["Reply to first", "Reply to second"]);
                if interactive {
                    assert!(
                        approved > 0,
                        "the native UI must request and receive fixture approval"
                    );
                }
                break;
            }
            if model.is_finished() {
                panic!("local model fixture failed");
            }
            if delivery.is_finished() {
                panic!("delivery loop ended early");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let history = observer
            .call(
                "thread/read",
                json!({"threadId":thread,"includeTurns":true}),
            )
            .await
            .unwrap();
        assert!(history.to_string().contains("first"));
        assert!(history.to_string().contains("second"));
    })
    .await;
    let screen = terminal.snapshot().screen;
    let history = if result.is_err() {
        observer
            .call(
                "thread/read",
                json!({"threadId":thread,"includeTurns":true}),
            )
            .await
            .ok()
    } else {
        None
    };
    delivery.abort();
    drop(input);
    if let Some(drain) = drain {
        drain.abort();
    }
    let _ = delivery.await;
    drop(observer);
    lifetime.disconnect();
    terminal.shutdown().await.unwrap();
    mcp.shutdown().await;
    fixture.stop().await;
    model.abort();
    result.unwrap_or_else(|_| {
        panic!(
            "Native message test timed out. Screen: {screen}; history: {history:?}; log: {}",
            std::fs::read_to_string(dir.path().join("codex-server.log")).unwrap()
        )
    });
}
