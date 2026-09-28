//! A bounded, execution-owned connection to Codex's private Unix WebSocket.
//! Requests are never retried: a lost response may already have started work.

use crate::{AppError, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{collections::HashMap, path::Path, time::Duration};
use tokio::{
    net::UnixStream,
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
use tokio_tungstenite::{
    client_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

const LIMIT: usize = 4 * 1024 * 1024;
const TIMEOUT: Duration = Duration::from_secs(30);

struct Call {
    method: String,
    params: Value,
    reply: oneshot::Sender<Result<Value>>,
}

pub(super) struct Rpc {
    calls: mpsc::Sender<Call>,
    pub events: mpsc::Receiver<Value>,
    pub health: watch::Receiver<Option<AppError>>,
    task: JoinHandle<()>,
}

impl Rpc {
    pub async fn connect(path: &Path) -> Result<Self> {
        let stream = UnixStream::connect(path).await?;
        let (socket, _) = client_async_with_config(
            "ws://localhost/",
            stream,
            Some(
                WebSocketConfig::default()
                    .max_message_size(Some(LIMIT))
                    .max_frame_size(Some(LIMIT)),
            ),
        )
        .await
        .map_err(failure)?;
        let (calls, mut requests) = mpsc::channel::<Call>(16);
        let (events, receiver) = mpsc::channel(128);
        let (health, health_rx) = watch::channel(None);
        let task = tokio::spawn(async move {
            let mut socket = socket;
            let mut pending = HashMap::<u64, oneshot::Sender<Result<Value>>>::new();
            let mut next = 0u64;
            let result: Result<()> = async {
                loop {
                    tokio::select! {
                        request = requests.recv(), if pending.len() < 16 => {
                            let Some(call) = request else { return Ok(()); };
                            next += 1;
                            let notification = call.method == "initialized";
                            let mut request = json!({"method":call.method,"params":call.params});
                            if !notification { request["id"] = json!(next); }
                            let request = request.to_string();
                            if request.len() > LIMIT {
                                let _ = call.reply.send(Err(failure("Codex request exceeds 4 MiB")));
                                continue;
                            }
                            if notification {
                                tokio::time::timeout(TIMEOUT, socket.send(Message::Text(request.into())))
                                    .await.map_err(failure)?.map_err(failure)?;
                                let _ = call.reply.send(Ok(json!({})));
                                continue;
                            }
                            pending.insert(next, call.reply);
                            tokio::time::timeout(TIMEOUT, socket.send(Message::Text(request.into())))
                                .await.map_err(failure)?.map_err(failure)?;
                        }
                        frame = socket.next() => {
                            let frame = frame.ok_or_else(|| failure("Codex app-server disconnected"))?.map_err(failure)?;
                            let Message::Text(text) = frame else {
                                if matches!(frame, Message::Close(_)) { return Err(failure("Codex app-server closed")); }
                                // Tungstenite answers ping frames while flushing.
                                socket.flush().await.map_err(failure)?;
                                continue;
                            };
                            let value: Value = serde_json::from_str(&text)?;
                            if value.get("method").is_some() {
                                if value.get("id").is_some() {
                                    // Approval and user-input requests also reach the attached
                                    // native TUI. This controller never answers on the user's behalf.
                                    continue;
                                }
                                // Streaming text is rendered by the TUI. Retain only lifecycle
                                // events needed by the delivery loop, never an unbounded transcript.
                                if matches!(value["method"].as_str(), Some("thread/status/changed" | "thread/closed" | "turn/started" | "turn/completed" | "thread/queue/changed" | "item/started")) {
                                    if value["method"] == "item/started" && value["params"]["item"]["type"] != "userMessage" { continue; }
                                    events.try_send(value).map_err(|_| failure("Codex lifecycle event queue overflowed"))?;
                                }
                            } else if let Some(id) = value["id"].as_u64()
                                && let Some(reply) = pending.remove(&id) {
                                let result = match value.get("error") {
                                    Some(error) => Err(AppError::new("codex_rpc", error.to_string())),
                                    None => value.get("result").cloned().ok_or_else(|| failure("Codex response has no result")),
                                };
                                let _ = reply.send(result);
                            }
                        }
                    }
                }
            }.await;
            let error = result
                .err()
                .unwrap_or_else(|| failure("Codex controller closed"));
            for (_, reply) in pending {
                let _ = reply.send(Err(error.clone()));
            }
            health.send_replace(Some(error));
        });
        let rpc = Self {
            calls,
            events: receiver,
            health: health_rx,
            task,
        };
        rpc.call("initialize", json!({"clientInfo":{"name":"ontography_node","version":env!("CARGO_PKG_VERSION")},"capabilities":{"experimentalApi":true}})).await?;
        rpc.call("initialized", json!({})).await?;
        Ok(rpc)
    }

    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        if let Some(error) = self.health.borrow().clone() {
            return Err(error);
        }
        let (reply, response) = oneshot::channel();
        tokio::time::timeout(TIMEOUT, async {
            self.calls
                .send(Call {
                    method: method.into(),
                    params,
                    reply,
                })
                .await
                .map_err(failure)?;
            response.await.map_err(failure)?
        })
        .await
        .map_err(|_| {
            failure(format!(
                "Timed out during {method}; delivery may have occurred"
            ))
        })?
    }
}

impl Drop for Rpc {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn failure(error: impl std::fmt::Display) -> AppError {
    AppError::new("codex_connection", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

    #[tokio::test]
    async fn lost_queue_ack_keeps_the_exact_message_receipt_prepared_and_stops_delivery() {
        use crate::{
            node_runtime::NodeRuntime,
            node_tool::tests::{Fixture, document},
        };
        let fixture = Fixture::new(document(json!({})), "worker", None).await;
        fixture.deliver("Do this once per attempt").await;
        let directory = tempfile::tempdir_in("/tmp").unwrap();
        let path = directory.path().join("lost.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            loop {
                let text = socket.next().await.unwrap().unwrap().into_text().unwrap();
                let request: Value = serde_json::from_str(&text).unwrap();
                if request["method"] == "initialized" {
                    continue;
                }
                if request["method"] == "thread/queue/add" {
                    return request["params"]["input"][0]["text"]
                        .as_str()
                        .unwrap()
                        .to_owned();
                }
                let result = if request["method"] == "thread/read" {
                    json!({"thread":{"status":{"type":"idle"}}})
                } else {
                    json!({})
                };
                socket
                    .send(Message::Text(
                        json!({"id":request["id"],"result":result})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();
            }
        });
        let mut rpc = Rpc::connect(&path).await.unwrap();
        let runtime = NodeRuntime::new(
            directory.path().into(),
            directory.path().into(),
            fixture.session.clone(),
            fixture.scope.subscribe(),
            fixture.ledger.clone(),
            None,
        );
        let error = tokio::time::timeout(
            Duration::from_secs(3),
            runtime.deliver(&mut rpc, "thread", &fixture.tools),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.code, "codex_connection");
        let message = server.await.unwrap();
        let input: Value = serde_json::from_str(&message).unwrap();
        let events = fixture
            .session
            .invocation_events(
                input["attempt_id"].as_str().unwrap().parse().unwrap(),
                0,
                100,
            )
            .await
            .unwrap();
        let digest = ontography::ContentDigest::compute(message.as_bytes());
        let receipt = events
            .iter()
            .find(|event| event.content_digest == digest)
            .unwrap();
        assert_eq!(receipt.state, ontography::ReceiptState::Prepared);
        assert!(
            !events
                .iter()
                .any(|event| event.receipt_sequence == receipt.sequence
                    && event.state == ontography::ReceiptState::Sent)
        );
        assert!(fixture.tools.next_message().await.unwrap().is_none());
        fixture.stop().await;
    }

    #[tokio::test]
    async fn requests_are_correlated_across_notifications_and_disconnect_is_not_replayed() {
        let directory = tempfile::tempdir_in("/tmp").unwrap();
        let path = directory.path().join("rpc.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let mut calls = Vec::new();
            loop {
                let text = socket.next().await.unwrap().unwrap().into_text().unwrap();
                let request: Value = serde_json::from_str(&text).unwrap();
                calls.push(request["method"].as_str().unwrap().to_owned());
                if request["method"] == "initialized" {
                    assert!(request.get("id").is_none());
                    continue;
                }
                if request["method"] == "lost" {
                    break;
                }
                // A server request may reuse the same numeric ID namespace.
                socket.send(Message::Text(json!({"id":request["id"],"method":"item/commandExecution/requestApproval","params":{}}).to_string().into())).await.unwrap();
                socket.send(Message::Text(json!({"method":"thread/status/changed","params":{"threadId":"thread","status":{"type":"idle"}}}).to_string().into())).await.unwrap();
                socket
                    .send(Message::Text(
                        json!({"id":request["id"],"result":{"echo":request["method"]}})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();
            }
            calls
        });
        let rpc = Rpc::connect(&path).await.unwrap();
        let (a, b) = tokio::join!(rpc.call("one", json!({})), rpc.call("two", json!({})));
        assert_eq!(a.unwrap()["echo"], "one");
        assert_eq!(b.unwrap()["echo"], "two");
        let error = tokio::time::timeout(Duration::from_secs(2), rpc.call("lost", json!({})))
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, "codex_connection");
        assert!(rpc.call("after_loss", json!({})).await.is_err());
        assert_eq!(
            server.await.unwrap(),
            ["initialize", "initialized", "one", "two", "lost"]
        );
    }
}
