//! Pi remains the model/tool harness. This module only speaks its JSONL RPC.

use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    io,
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::mpsc,
    task::JoinHandle,
};

const MAX_RECORD: u64 = 8 * 1024 * 1024;
const MAX_MESSAGE: usize = 64 * 1024;

#[derive(Debug)]
pub enum PiEvent {
    Record(Value),
    Diagnostic(String),
    Closed,
}

pub struct PiRpc {
    child: Child,
    input: Option<ChildStdin>,
    tasks: Vec<JoinHandle<()>>,
    sequence: u64,
}

impl PiRpc {
    pub fn spawn(mut command: Command) -> io::Result<(Self, mpsc::Receiver<PiEvent>)> {
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn()?;
        let input = child.stdin.take();
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("Pi stdout unavailable"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| io::Error::other("Pi stderr unavailable"))?;
        let (tx, rx) = mpsc::channel(256);
        let output_tx = tx.clone();
        let output_task = tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            loop {
                let mut record = Vec::new();
                match (&mut reader)
                    .take(MAX_RECORD + 1)
                    .read_until(b'\n', &mut record)
                    .await
                {
                    Ok(0) => break,
                    Ok(n) if n as u64 > MAX_RECORD => {
                        let _ = output_tx
                            .send(PiEvent::Diagnostic(
                                "Pi RPC record exceeds 8 MiB; restart the client".into(),
                            ))
                            .await;
                        break;
                    }
                    Ok(_) => match serde_json::from_slice(&record) {
                        Ok(value) => {
                            if output_tx.send(PiEvent::Record(value)).await.is_err() {
                                break;
                            }
                        }
                        Err(error) => {
                            let _ = output_tx
                                .send(PiEvent::Diagnostic(format!("Invalid Pi RPC JSON: {error}")))
                                .await;
                        }
                    },
                    Err(error) => {
                        let _ = output_tx.send(PiEvent::Diagnostic(error.to_string())).await;
                        break;
                    }
                }
            }
            let _ = output_tx.send(PiEvent::Closed).await;
        });
        let error_task = tokio::spawn(async move {
            let mut reader = BufReader::new(stderr);
            loop {
                let mut bytes = Vec::new();
                match (&mut reader).take(4096).read_until(b'\n', &mut bytes).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        if tx
                            .send(PiEvent::Diagnostic(
                                String::from_utf8_lossy(&bytes).trim().to_owned(),
                            ))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                }
            }
        });
        Ok((
            Self {
                child,
                input,
                tasks: vec![output_task, error_task],
                sequence: 0,
            },
            rx,
        ))
    }

    pub async fn send(&mut self, mut value: Value) -> io::Result<()> {
        if value["type"] != "extension_ui_response" && value.get("id").is_none() {
            self.sequence += 1;
            value["id"] = json!(format!("ui-{}", self.sequence));
        }
        let mut bytes = serde_json::to_vec(&value)?;
        bytes.push(b'\n');
        let input = self
            .input
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "Pi has exited"))?;
        input.write_all(&bytes).await?;
        input.flush().await
    }

    pub async fn shutdown(mut self) {
        // Pi disposes its runtime when stdin closes. Never stop the graph server.
        self.input.take();
        if tokio::time::timeout(Duration::from_secs(3), self.child.wait())
            .await
            .is_err()
        {
            let _ = self.child.kill().await;
            let _ = self.child.wait().await;
        }
        for task in self.tasks.drain(..) {
            task.abort();
        }
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    pub role: String,
    pub text: String,
}

#[derive(Default)]
pub struct Conversation {
    pub messages: Vec<Message>,
    pub busy: bool,
    pub session: Option<String>,
    pub dialog: Option<Value>,
    pub pending_dialogs: VecDeque<Value>,
    current_assistant: Option<usize>,
    active_tools: BTreeMap<String, usize>,
}

impl Conversation {
    pub fn push(&mut self, role: impl Into<String>, text: impl Into<String>) {
        let mut text = text.into();
        truncate(&mut text, MAX_MESSAGE);
        self.messages.push(Message {
            role: role.into(),
            text,
        });
        while self.messages.len() > 250
            || self
                .messages
                .iter()
                .map(|message| message.text.len())
                .sum::<usize>()
                > 512 * 1024
        {
            self.messages.remove(0);
            self.current_assistant = self.current_assistant.and_then(|i| i.checked_sub(1));
            self.active_tools
                .retain(|_, index| match index.checked_sub(1) {
                    Some(next) => {
                        *index = next;
                        true
                    }
                    None => false,
                });
        }
    }

    pub fn apply(&mut self, record: &Value) {
        match record["type"].as_str().unwrap_or("") {
            "agent_start" => self.busy = true,
            "agent_settled" => {
                self.busy = false;
                self.current_assistant = None;
            }
            "message_start" => {
                let message = &record["message"];
                if message["role"] == "assistant" {
                    self.push("Pi", "");
                    self.current_assistant = Some(self.messages.len() - 1);
                } else if message["role"] == "user" {
                    self.push("You", message_text(message));
                }
            }
            "message_update" => {
                let event = &record["assistantMessageEvent"];
                if event["type"] == "text_delta" {
                    if self.current_assistant.is_none() {
                        self.push("Pi", "");
                        self.current_assistant = Some(self.messages.len() - 1);
                    }
                    if let Some(index) = self.current_assistant {
                        self.messages[index]
                            .text
                            .push_str(event["delta"].as_str().unwrap_or(""));
                        truncate(&mut self.messages[index].text, MAX_MESSAGE);
                    }
                }
            }
            "message_end" => {
                let message = &record["message"];
                if message["role"] == "assistant" {
                    let mut final_text = message_text(message);
                    if let Some(error) = message["errorMessage"].as_str() {
                        final_text.push_str(&format!("\n{error}"));
                    }
                    truncate(&mut final_text, MAX_MESSAGE);
                    if let Some(index) = self.current_assistant.take() {
                        self.messages[index].text = final_text;
                    } else {
                        self.push("Pi", final_text);
                    }
                }
            }
            "tool_execution_start" => self.tool_message(record, "Running…".into(), false),
            "tool_execution_update" => {
                self.tool_message(record, message_text(&record["partialResult"]), false)
            }
            "tool_execution_end" => {
                self.tool_message(record, message_text(&record["result"]), true)
            }
            "response" => {
                if record["success"] == false {
                    self.push(
                        "Pi error",
                        record["error"].as_str().unwrap_or("Command failed"),
                    );
                } else {
                    match record["command"].as_str().unwrap_or("") {
                        "get_state" => {
                            self.busy = record["data"]["isStreaming"].as_bool().unwrap_or(false);
                            self.session =
                                record["data"]["sessionFile"].as_str().map(str::to_owned);
                        }
                        "get_messages" => {
                            self.messages.clear();
                            self.active_tools.clear();
                            self.current_assistant = None;
                            if let Some(messages) = record["data"]["messages"].as_array() {
                                for message in messages {
                                    self.push(
                                        message["role"].as_str().unwrap_or("message"),
                                        message_text(message),
                                    );
                                }
                            }
                        }
                        "new_session" | "switch_session" if record["data"]["cancelled"] != true => {
                            self.messages.clear();
                            self.active_tools.clear();
                            self.current_assistant = None;
                        }
                        _ => {}
                    }
                }
            }
            "extension_ui_request" => match record["method"].as_str().unwrap_or("") {
                "select" | "confirm" | "input" | "editor" => {
                    if self.dialog.is_some() {
                        self.pending_dialogs.push_back(record.clone());
                    } else {
                        self.dialog = Some(record.clone());
                    }
                }
                "notify" => self.push("Notice", record["message"].as_str().unwrap_or("")),
                _ => {}
            },
            _ => {}
        }
    }
    fn tool_message(&mut self, record: &Value, mut text: String, finished: bool) {
        let id = record["toolCallId"].as_str().unwrap_or("").to_owned();
        let role = format!(
            "{} · {}",
            if record["isError"] == true {
                "Tool error"
            } else {
                "Tool"
            },
            record["toolName"].as_str().unwrap_or("operation")
        );
        truncate(&mut text, MAX_MESSAGE);
        if let Some(index) = self.active_tools.get(&id).copied() {
            self.messages[index] = Message { role, text };
        } else {
            self.push(role, text);
            self.active_tools
                .insert(id.clone(), self.messages.len() - 1);
        }
        if finished {
            self.active_tools.remove(&id);
        }
    }
}

fn truncate(text: &mut String, max: usize) {
    if text.len() > max {
        let mut end = max;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        text.push_str("\n[preview truncated]");
    }
}

fn message_text(value: &Value) -> String {
    if let Some(text) = value["content"].as_str() {
        return text.to_owned();
    }
    value["content"]
        .as_array()
        .map(|blocks| {
            blocks
                .iter()
                .filter_map(|block| match block["type"].as_str()? {
                    "text" => Some(block["text"].as_str().unwrap_or("").to_owned()),
                    "toolCall" => Some(format!("[{}]", block["name"].as_str().unwrap_or("tool"))),
                    "image" => Some("[image]".into()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streamed_text_reconciles_once_and_waits_for_settlement() {
        let mut chat = Conversation::default();
        for value in [
            json!({"type":"agent_start"}),
            json!({"type":"message_start","message":{"role":"assistant"}}),
            json!({"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"Hel"}}),
            json!({"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"Hello"}]}}),
            json!({"type":"agent_end"}),
        ] {
            chat.apply(&value);
        }
        assert!(chat.busy);
        assert_eq!(chat.messages.len(), 1);
        assert_eq!(chat.messages[0].text, "Hello");
        chat.apply(&json!({"type":"agent_settled"}));
        assert!(!chat.busy);
    }

    #[test]
    fn restore_and_command_errors_are_visible() {
        let mut chat = Conversation::default();
        chat.apply(&json!({"type":"response","success":true,"command":"get_messages","data":{"messages":[{"role":"user","content":"previous"}]}}));
        chat.apply(&json!({"type":"response","success":false,"error":"No model"}));
        assert_eq!(chat.messages[0].text, "previous");
        assert_eq!(chat.messages[1].text, "No model");
    }

    #[test]
    fn parallel_tool_progress_replaces_accumulated_output_by_call_id() {
        let mut chat = Conversation::default();
        for id in ["a", "b"] {
            chat.apply(&json!({"type":"tool_execution_start","toolCallId":id,"toolName":"bash"}));
        }
        chat.apply(&json!({"type":"tool_execution_update","toolCallId":"b","toolName":"bash","partialResult":{"content":[{"type":"text","text":"first\nsecond"}]}}));
        chat.apply(&json!({"type":"tool_execution_end","toolCallId":"b","toolName":"bash","result":{"content":[{"type":"text","text":"complete"}]},"isError":false}));
        assert_eq!(chat.messages.len(), 2);
        assert_eq!(chat.messages[0].text, "Running…");
        assert_eq!(chat.messages[1].text, "complete");
    }

    #[tokio::test]
    async fn child_framing_handles_unicode_and_cleanup_without_a_model() {
        let mut command = Command::new("/bin/sh");
        command.args(["-c","printf '%s\\n' '{\"type\":\"message_update\",\"assistantMessageEvent\":{\"type\":\"text_delta\",\"delta\":\"a\\u2028b\"}}'; cat >/dev/null"]);
        let (rpc, mut events) = PiRpc::spawn(command).unwrap();
        let record = tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap();
        match record {
            PiEvent::Record(value) => {
                assert_eq!(value["assistantMessageEvent"]["delta"], "a\u{2028}b")
            }
            other => panic!("unexpected {other:?}"),
        }
        rpc.shutdown().await;
    }
}
