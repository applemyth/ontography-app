use super::{Fixture, document};
use crate::{node_mcp::NodeMcp, node_tool::context::context_error, protocol};
use ontography::{ContentDigest, InvocationId, ReceiptState};
use serde_json::{Value, json};
use std::{os::unix::fs::PermissionsExt, process::Stdio, str::FromStr, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    process::{Child, ChildStdin, ChildStdout, Command},
};

async fn hosted(settings: Value) -> (Fixture, tempfile::TempDir, NodeMcp) {
    let fixture = Fixture::new(document(settings), "worker", None).await;
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let server = NodeMcp::bind(directory.path(), fixture.tools.clone()).unwrap();
    (fixture, directory, server)
}

fn initialize() -> Value {
    json!({"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}})
}

async fn read(reader: &mut (impl tokio::io::AsyncBufRead + Unpin)) -> Value {
    let bytes = tokio::time::timeout(Duration::from_secs(5), protocol::read_frame(reader))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

struct Client {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
}

impl Client {
    async fn new(server: &NodeMcp) -> Self {
        let stdio = server.server().unwrap();
        let mut child = Command::new(&stdio.command)
            .args(&stdio.args)
            .envs(&stdio.env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let output = BufReader::new(child.stdout.take().unwrap());
        let mut client = Self {
            child,
            input,
            output,
        };
        client.send(initialize()).await;
        assert_eq!(
            client.read().await["result"]["protocolVersion"],
            "2025-11-25"
        );
        client
            .send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .await;
        client
    }
    async fn send(&mut self, request: Value) {
        protocol::write_frame(&mut self.input, &request)
            .await
            .unwrap();
    }
    async fn read(&mut self) -> Value {
        read(&mut self.output).await
    }
    async fn call(&mut self, id: u32, tool: &str, arguments: Value) -> Value {
        self.send(json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":tool,"arguments":arguments}})).await;
        let reply = self.read().await;
        assert_eq!(reply["id"], id, "{reply}");
        reply
    }
    async fn close(mut self) {
        drop(self.input);
        assert!(
            tokio::time::timeout(Duration::from_secs(3), self.child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
    }
}

struct Raw {
    input: tokio::net::unix::OwnedWriteHalf,
    output: BufReader<tokio::net::unix::OwnedReadHalf>,
}

impl Raw {
    async fn new(server: &NodeMcp) -> Self {
        let env = server.environment();
        let stream = UnixStream::connect(&env["ONTOGRAPHY_NODE_MCP_SOCKET"])
            .await
            .unwrap();
        let (output, mut input) = stream.into_split();
        let mut output = BufReader::new(output);
        protocol::write_frame(
            &mut input,
            &json!({"version":1,"token":env["ONTOGRAPHY_NODE_MCP_TOKEN"]}),
        )
        .await
        .unwrap();
        assert_eq!(read(&mut output).await, json!({"ready":true}));
        let mut client = Self { input, output };
        client.send(initialize()).await;
        assert_eq!(client.read().await["message"]["id"], 0);
        client
            .send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .await;
        client
    }
    async fn send(&mut self, request: Value) {
        protocol::write_frame(&mut self.input, &json!({"message":request}))
            .await
            .unwrap();
    }
    async fn read(&mut self) -> Value {
        read(&mut self.output).await
    }
    async fn call(&mut self, id: u32, name: &str, arguments: Value) -> Value {
        self.send(json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":name,"arguments":arguments}})).await;
        let response = self.read().await;
        assert_eq!(response["message"]["id"], id, "{response}");
        response
    }
    async fn ack(&mut self, response: &Value) {
        protocol::write_frame(&mut self.input, &json!({"delivered":response["delivery"]}))
            .await
            .unwrap();
        self.send(json!({"jsonrpc":"2.0","id":"ack-barrier","method":"ping"}))
            .await;
        assert_eq!(self.read().await["message"]["id"], "ack-barrier");
    }
}

fn tool_value(response: &Value) -> Value {
    assert_eq!(response["result"]["isError"], false, "{response}");
    serde_json::from_str(response["result"]["content"][0]["text"].as_str().unwrap()).unwrap()
}

#[tokio::test]
async fn stdio_catalog_selection_errors_and_live_updates_use_the_node_context() {
    let (fixture, _directory, server) =
        hosted(json!({"tools":["inspect_node","retire_package"]})).await;
    let mut client = Client::new(&server).await;
    client
        .send(json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
        .await;
    let reply = client.read().await;
    assert_eq!(reply["result"]["tools"].as_array().unwrap().len(), 1);
    assert_eq!(reply["result"]["tools"][0]["name"], "inspect_node");
    assert!(reply["result"]["tools"][0]["inputSchema"].is_object());
    let inspected = tool_value(&client.call(2, "inspect_node", json!({})).await);
    assert_eq!(inspected["node"], "worker");
    assert_eq!(
        client.call(3, "begin_invocation", json!({})).await["error"]["code"],
        -32602
    );
    assert_eq!(
        client.call(4, "retire_package", json!({})).await["error"]["code"],
        -32602
    );
    assert_eq!(
        client
            .call(5, "inspect_node", json!({"node":"source"}))
            .await["result"]["isError"],
        true
    );
    fixture.grant(&[crate::workflow::Grant::Retire]);
    assert_eq!(
        client.read().await["method"],
        "notifications/tools/list_changed"
    );
    client
        .send(json!({"jsonrpc":"2.0","id":6,"method":"tools/list"}))
        .await;
    assert_eq!(
        client.read().await["result"]["tools"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    fixture
        .scope
        .send_modify(|scope| scope.node.tools = Some(Default::default()));
    assert_eq!(
        client.read().await["method"],
        "notifications/tools/list_changed"
    );
    assert_eq!(
        client.call(7, "inspect_node", json!({})).await["error"]["code"],
        -32602
    );
    client.input.write_all(b"not-json\n").await.unwrap();
    assert_eq!(client.read().await["error"]["code"], -32700);
    client
        .send(json!({"jsonrpc":"2.0","id":8,"method":"unknown"}))
        .await;
    assert_eq!(client.read().await["error"]["code"], -32601);
    client.close().await;
    drop(server);
    fixture.stop().await;
}

#[tokio::test]
async fn receipt_delivery_waits_for_stdout_ack_and_lost_replies_stay_prepared() {
    let (fixture, _directory, server) = hosted(json!({})).await;
    fixture.deliver("payload through MCP").await;
    let mut client = Raw::new(&server).await;
    let next = client.call(1, "next_trigger", json!({})).await;
    let next_value = tool_value(&next["message"]);
    client.ack(&next).await;
    let begun = client
        .call(
            2,
            "begin_invocation",
            json!({"task_id":next_value["task_id"]}),
        )
        .await;
    let value = tool_value(&begun["message"]);
    let attempt = InvocationId::from_str(value["attempt_id"].as_str().unwrap()).unwrap();
    let text = begun["message"]["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    let events = fixture
        .session
        .invocation_events(attempt, 0, 100)
        .await
        .unwrap();
    let receipt = events
        .iter()
        .find(|event| event.content_digest == ContentDigest::compute(text.as_bytes()))
        .unwrap();
    let sequence = receipt.sequence;
    assert!(
        !events
            .iter()
            .any(|event| event.receipt_sequence == sequence && event.state == ReceiptState::Sent)
    );
    client.ack(&begun).await;
    let events = fixture
        .session
        .invocation_events(attempt, 0, 100)
        .await
        .unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.receipt_sequence == sequence && event.state == ReceiptState::Sent)
    );
    let unsent = client
        .call(
            3,
            "read_package",
            json!({"attempt_id":value["attempt_id"],"handle":value["inputs"][0]["handle"]}),
        )
        .await;
    assert!(
        tool_value(&unsent["message"])
            .to_string()
            .contains("payload through MCP")
    );
    let text = unsent["message"]["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    let digest = ContentDigest::compute(text.as_bytes());
    drop(client);
    let events = fixture
        .session
        .invocation_events(attempt, 0, 100)
        .await
        .unwrap();
    let receipt = events
        .iter()
        .find(|event| event.content_digest == digest)
        .unwrap();
    assert!(!events.iter().any(
        |event| event.receipt_sequence == receipt.sequence && event.state == ReceiptState::Sent
    ));
    drop(server);
    fixture.stop().await;
}

#[tokio::test]
async fn an_acknowledgement_after_its_attempt_ended_keeps_the_proxy_serving() {
    let (fixture, _directory, server) = hosted(json!({})).await;
    // Larger than a pipe holds: the proxy acknowledges reading this input
    // only after the client has read the whole reply.
    fixture.deliver(&"x".repeat(200 * 1024)).await;
    let mut client = Client::new(&server).await;
    let next = tool_value(&client.call(1, "next_trigger", json!({})).await);
    let begun = tool_value(
        &client
            .call(2, "begin_invocation", json!({"task_id":next["task_id"]}))
            .await,
    );
    let attempt = begun["attempt_id"].as_str().unwrap();
    client.send(json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"read_package","arguments":{"attempt_id":attempt,"handle":begun["inputs"][0]["handle"]}}})).await;
    // Once the reply is being written, core ends the attempt before the
    // client has read it, as when an attempt is abandoned.
    tokio::time::timeout(Duration::from_secs(5), client.output.fill_buf())
        .await
        .unwrap()
        .unwrap();
    fixture
        .tools
        .with_attempt(attempt, async |attempt, _| {
            attempt
                .invocation
                .interrupt("ended during delivery")
                .await
                .map_err(context_error)
        })
        .await
        .unwrap();
    let read = client.read().await;
    assert_eq!(read["id"], 3);
    let digest = ContentDigest::compute(
        read["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .as_bytes(),
    );
    // The late acknowledgement leaves the receipt prepared, and the
    // connection keeps serving calls.
    assert_eq!(
        tool_value(&client.call(4, "inspect_node", json!({})).await)["node"],
        "worker"
    );
    let events = fixture
        .session
        .invocation_events(InvocationId::from_str(attempt).unwrap(), 0, 100)
        .await
        .unwrap();
    let receipt = events
        .iter()
        .find(|event| event.content_digest == digest)
        .unwrap();
    assert!(!events.iter().any(
        |event| event.receipt_sequence == receipt.sequence && event.state == ReceiptState::Sent
    ));
    client.close().await;
    drop(server);
    fixture.stop().await;
}

#[tokio::test]
async fn an_unknown_acknowledgement_changes_nothing() {
    let (fixture, _directory, server) = hosted(json!({})).await;
    let mut client = Raw::new(&server).await;
    protocol::write_frame(&mut client.input, &json!({"delivered":42}))
        .await
        .unwrap();
    client
        .send(json!({"jsonrpc":"2.0","id":1,"method":"ping"}))
        .await;
    assert_eq!(client.read().await["message"]["id"], 1);
    drop(client);
    drop(server);
    fixture.stop().await;
}

#[tokio::test]
async fn stdio_tools_read_and_publish_a_real_graph_package() {
    let (fixture, _directory, server) = hosted(json!({})).await;
    fixture.deliver("work item").await;
    let mut client = Client::new(&server).await;
    let next = tool_value(&client.call(1, "next_trigger", json!({})).await);
    let begun = tool_value(
        &client
            .call(2, "begin_invocation", json!({"task_id":next["task_id"]}))
            .await,
    );
    let input = tool_value(
        &client
            .call(
                3,
                "read_package",
                json!({"attempt_id":begun["attempt_id"],"handle":begun["inputs"][0]["handle"]}),
            )
            .await,
    );
    assert!(input.to_string().contains("work item"));
    let submitted = tool_value(
        &client
            .call(
                4,
                "submit_invocation",
                json!({"attempt_id":begun["attempt_id"],"result":{"message":"finished"}}),
            )
            .await,
    );
    assert_eq!(submitted["status"], "accepted");
    assert_eq!(fixture.pending("sink").await, ["finished"]);
    assert!(fixture.pending("worker").await.is_empty());
    client.close().await;
    drop(server);
    fixture.stop().await;
}

#[tokio::test]
async fn accepted_mutation_settles_after_cancellation_and_connection_loss() {
    let (fixture, _directory, server) = hosted(json!({})).await;
    fixture.deliver("work item").await;
    let mut client = Raw::new(&server).await;
    let next = client.call(1, "next_trigger", json!({})).await;
    let task = tool_value(&next["message"])["task_id"].clone();
    client.ack(&next).await;
    let begun = client
        .call(2, "begin_invocation", json!({"task_id":task}))
        .await;
    let attempt = tool_value(&begun["message"])["attempt_id"].clone();
    // Do not acknowledge begin: submission must wait until this unsent reply
    // is either acknowledged or released when the connection disappears.
    client.send(json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"submit_invocation","arguments":{"attempt_id":attempt,"result":{"message":"settled"}}}})).await;
    client
        .send(json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":3}}))
        .await;
    client
        .send(json!({"jsonrpc":"2.0","id":4,"method":"ping"}))
        .await;
    assert_eq!(client.read().await["message"]["id"], 4);
    drop(client);
    tokio::time::timeout(Duration::from_secs(3), async {
        while fixture.pending("sink").await != ["settled"] {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(fixture.pending("worker").await.is_empty());
    drop(server);
    fixture.stop().await;
}

#[tokio::test]
async fn partial_frames_survive_notifications_and_waits_do_not_block_ping() {
    let (fixture, _directory, server) = hosted(json!({})).await;
    let mut client = Raw::new(&server).await;
    let inspected = client.call(1, "inspect_node", json!({})).await;
    let version = tool_value(&inspected["message"])["version"].clone();
    client.ack(&inspected).await;
    client.send(json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"wait_for_change","arguments":{"after":version,"timeout_ms":30000}}})).await;
    client
        .send(json!({"jsonrpc":"2.0","id":3,"method":"ping"}))
        .await;
    assert_eq!(client.read().await["message"]["id"], 3);
    client
        .send(json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":2}}))
        .await;
    client
        .send(json!({"jsonrpc":"2.0","id":"cancel-barrier","method":"ping"}))
        .await;
    assert_eq!(client.read().await["message"]["id"], "cancel-barrier");
    let frame =
        serde_json::to_vec(&json!({"message":{"jsonrpc":"2.0","id":4,"method":"tools/list"}}))
            .unwrap();
    let split = frame.len() / 2;
    client.input.write_all(&frame[..split]).await.unwrap();
    fixture.grant(&[crate::workflow::Grant::Retire]);
    assert_eq!(
        client.read().await["message"]["method"],
        "notifications/tools/list_changed"
    );
    client.input.write_all(&frame[split..]).await.unwrap();
    client.input.write_all(b"\n").await.unwrap();
    assert_eq!(client.read().await["message"]["id"], 4);
    drop(client);
    drop(server);
    fixture.stop().await;
}

#[tokio::test]
async fn proxy_disconnect_keeps_node_alive_and_server_loss_exits_with_stdin_open() {
    let (fixture, _directory, server) = hosted(json!({})).await;
    let mut first = Client::new(&server).await;
    first.child.kill().await.unwrap();
    drop(first);
    assert_eq!(
        fixture.ok("inspect_node", json!({})).await["node"],
        "worker"
    );
    let mut second = Client::new(&server).await;
    let environment = server.environment();
    drop(server);
    assert!(
        !tokio::time::timeout(Duration::from_secs(3), second.child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    // A proxy from this old execution cannot reconnect to a replacement.
    let stale = Command::new(crate::launcher::application_executable().unwrap())
        .arg("node-mcp")
        .envs(environment)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .unwrap();
    assert!(!stale.success());
    fixture.stop().await;
}

#[tokio::test]
async fn a_different_execution_token_cannot_select_this_nodes_tools() {
    let (fixture, directory, server) = hosted(json!({})).await;
    let replacement = NodeMcp::bind(directory.path(), fixture.tools.clone()).unwrap();
    let mut environment = server.environment();
    environment.insert(
        "ONTOGRAPHY_NODE_MCP_TOKEN".into(),
        replacement.environment()["ONTOGRAPHY_NODE_MCP_TOKEN"].clone(),
    );
    let status = Command::new(crate::launcher::application_executable().unwrap())
        .arg("node-mcp")
        .envs(environment)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .unwrap();
    assert!(!status.success());
    assert_eq!(
        fixture.ok("inspect_node", json!({})).await["node"],
        "worker"
    );
    drop(replacement);
    drop(server);
    fixture.stop().await;
}

/// Opt-in interoperability check through Codex's actual MCP client. This
/// starts no model turn and makes only an inspect_node call on fixture data.
#[tokio::test]
#[ignore = "requires installed Codex; verifies its native MCP client without inference"]
async fn native_codex_discovers_and_calls_the_selected_node_tools() {
    async fn rpc(
        input: &mut ChildStdin,
        output: &mut BufReader<ChildStdout>,
        id: u32,
        method: &str,
        params: Value,
    ) -> Value {
        protocol::write_frame(input, &json!({"id":id,"method":method,"params":params}))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let bytes = protocol::read_frame(output)
                    .await
                    .unwrap()
                    .expect("Codex closed its output");
                let response: Value = serde_json::from_slice(&bytes).unwrap();
                if response.get("id") == Some(&json!(id)) {
                    assert!(response.get("error").is_none(), "{response}");
                    return response["result"].clone();
                }
            }
        })
        .await
        .unwrap()
    }
    let (fixture, directory, server) = hosted(json!({"tools":["inspect_node"]})).await;
    let log = std::fs::File::create(directory.path().join("codex.log")).unwrap();
    let mut child = Command::new("codex")
        .args(
            server
                .codex_overrides()
                .unwrap()
                .into_iter()
                .flat_map(|value| ["-c".into(), value]),
        )
        .args(["app-server", "--listen", "stdio://"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(log)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    rpc(&mut input, &mut output, 1, "initialize", json!({"clientInfo":{"name":"ontography_mcp_test","version":"1"},"capabilities":{"experimentalApi":true}})).await;
    protocol::write_frame(&mut input, &json!({"method":"initialized"}))
        .await
        .unwrap();
    let thread = rpc(
        &mut input,
        &mut output,
        2,
        "thread/start",
        json!({"cwd":directory.path(),"ephemeral":true}),
    )
    .await;
    let thread_id = &thread["thread"]["id"];
    let status = rpc(
        &mut input,
        &mut output,
        3,
        "mcpServerStatus/list",
        json!({"threadId":thread_id,"detail":"toolsAndAuthOnly"}),
    )
    .await;
    let node = status["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|server| server["name"] == "ontography_node")
        .unwrap();
    assert_eq!(
        node["tools"]
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        ["inspect_node"],
        "{node}"
    );
    let result = rpc(&mut input, &mut output, 4, "mcpServer/tool/call", json!({"threadId":thread_id,"server":"ontography_node","tool":"inspect_node","arguments":{}})).await;
    assert_eq!(tool_value(&json!({"result":result}))["node"], "worker");
    rpc(
        &mut input,
        &mut output,
        5,
        "thread/unsubscribe",
        json!({"threadId":thread_id}),
    )
    .await;
    drop(input);
    let drained =
        tokio::spawn(async move { tokio::io::copy(&mut output, &mut tokio::io::sink()).await });
    assert!(
        tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    drained.await.unwrap().unwrap();
    drop(server);
    fixture.stop().await;
}
