//! Claude without a terminal, speaking stream-json on stdin and stdout.
//!
//! Each delivery is one user message whose text is the recorded envelope.
//! Claude acknowledges the message by its uuid once it is queued, and the
//! turn's result names it. Messages sent close together would merge into one
//! turn, so the next is written only after that result.

use super::{Started, Status, attempt_id, error, turn_failed};
use crate::{
    AppError, Result,
    node_tool::{NodeToolContext, Reply},
};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt},
    time::MissedTickBehavior,
};

/// How often delivery looks for work while Claude is idle.
const POLL: Duration = Duration::from_millis(250);
/// Longer output lines, such as a huge tool result, are skipped unparsed.
const MAX_LINE_BYTES: usize = 16 * 1024 * 1024;

/// Delivers graph work through Claude's `input`, following its `output`,
/// until Claude exits. `report` hears every change of status.
pub(crate) async fn deliver(
    mut input: impl AsyncWrite + Unpin,
    output: impl AsyncBufRead + Unpin,
    tools: &NodeToolContext,
    started: Started,
    mut report: impl FnMut(Status),
) -> Result<()> {
    let mut lines = Lines {
        reader: output,
        line: Vec::new(),
        oversized: false,
    };
    let mut delivery = Delivery {
        tools,
        started,
        marked: false,
        pending: None,
    };
    let mut poll = tokio::time::interval(POLL);
    poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut reported = None;
    loop {
        let status = delivery.status();
        if reported != Some(status) {
            reported = Some(status);
            report(status);
        }
        tokio::select! {
            line = lines.next() => {
                let line = line?.ok_or_else(|| error("Claude exited"))?;
                delivery.observe(&line).await?;
            }
            _ = poll.tick(), if delivery.pending.is_none() => {
                if let Some(reply) = tools.next_message().await? {
                    delivery.pending = Some(send(&mut input, reply).await?);
                }
            }
        }
    }
}

struct Delivery<'a> {
    tools: &'a NodeToolContext,
    started: Started,
    marked: bool,
    pending: Option<Pending>,
}

/// A message written to Claude, until its turn ends.
struct Pending {
    uuid: String,
    attempt: String,
    /// Held until Claude takes the message.
    reply: Option<Reply>,
}

impl Delivery<'_> {
    /// Follows one line of Claude's output.
    async fn observe(&mut self, line: &[u8]) -> Result<()> {
        let Some(pending) = &mut self.pending else {
            return Ok(());
        };
        let Ok(event) = serde_json::from_slice::<Value>(line) else {
            return Ok(());
        };
        let Some(progress) = progress(&event, &pending.uuid) else {
            return Ok(());
        };
        // A result shows that Claude took the message, even when it did not
        // acknowledge it first. A dropped message was never taken.
        if !matches!(progress, Progress::Dropped(_))
            && let Some(reply) = pending.reply.take()
        {
            reply.sent().await?;
            if !self.marked {
                self.started.set()?;
                self.marked = true;
            }
        }
        let failure = match progress {
            Progress::Accepted => return Ok(()),
            Progress::Finished(failure) => failure,
            Progress::Dropped(reason) => Some(reason),
        };
        let Pending { attempt, reply, .. } = self.pending.take().expect("a pending message");
        // Ending the attempt would first wait for an unsent reply.
        drop(reply);
        if let Some(reason) = failure {
            self.tools.message_failed(&attempt, &reason).await?;
        }
        Ok(())
    }

    fn status(&self) -> Status {
        match &self.pending {
            None => Status::Idle,
            Some(Pending { reply: Some(_), .. }) => Status::Delivering,
            Some(_) => Status::Working,
        }
    }
}

/// Writes the recorded envelope as one user message.
async fn send(input: &mut (impl AsyncWrite + Unpin), reply: Reply) -> Result<Pending> {
    let attempt = attempt_id(&reply)?;
    let text = std::str::from_utf8(reply.bytes())
        .map_err(|error| AppError::new("claude_delivery", error.to_string()))?;
    let uuid = uuid::Uuid::new_v4().to_string();
    let mut line = serde_json::to_vec(&json!({
        "type": "user",
        "message": {"role": "user", "content": text},
        "parent_tool_use_id": null,
        "uuid": uuid,
        "origin": {"kind": "human"},
    }))?;
    line.push(b'\n');
    input.write_all(&line).await?;
    input.flush().await?;
    Ok(Pending {
        uuid,
        attempt,
        reply: Some(reply),
    })
}

enum Progress {
    /// Claude queued the message, or echoed it back.
    Accepted,
    /// The message's turn ended, with the reason it failed if it did.
    Finished(Option<String>),
    /// Claude will not run the message.
    Dropped(String),
}

/// What one output event says about the message sent as `uuid`. Anything
/// else, such as assistant messages or the `system/init` each turn repeats,
/// is Claude at work.
fn progress(event: &Value, uuid: &str) -> Option<Progress> {
    match event["type"].as_str()? {
        "command_lifecycle" if event["command_uuid"] == uuid => match event["state"].as_str()? {
            "queued" | "started" => Some(Progress::Accepted),
            state @ ("cancelled" | "discarded" | "refused") => Some(Progress::Dropped(format!(
                "Claude {state} the delivered message"
            ))),
            // "completed" follows the turn's result.
            _ => None,
        },
        "user" if event["uuid"] == uuid && event["isReplay"] == true => Some(Progress::Accepted),
        "result"
            if event["user_message_uuid"] == uuid
                || event["user_message_uuids"]
                    .as_array()
                    .is_some_and(|uuids| uuids.iter().any(|id| id == uuid)) =>
        {
            // API errors arrive as successful results marked as errors.
            let failed = event["is_error"] == true;
            Some(Progress::Finished(
                failed.then(|| turn_failed(&result_text(event))),
            ))
        }
        _ => None,
    }
}

/// A failed result's text, or the errors it lists.
fn result_text(event: &Value) -> String {
    if let Some(text) = event["result"].as_str().filter(|text| !text.is_empty()) {
        return text.into();
    }
    let errors: Vec<_> = event["errors"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    if errors.is_empty() {
        event["subtype"].as_str().unwrap_or("unknown error").into()
    } else {
        errors.join("; ")
    }
}

/// Claude's output, line by line. A partly read line survives the select
/// loop's other branches: nothing is consumed while a read waits.
struct Lines<R> {
    reader: R,
    line: Vec<u8>,
    oversized: bool,
}

impl<R: AsyncBufRead + Unpin> Lines<R> {
    /// The next whole line without its newline, or `None` once output ends.
    async fn next(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            let available = self.reader.fill_buf().await?;
            if available.is_empty() {
                return Ok(None);
            }
            let end = available.iter().position(|&byte| byte == b'\n');
            let chunk = &available[..end.unwrap_or(available.len())];
            if !self.oversized {
                self.line.extend_from_slice(chunk);
                if self.line.len() > MAX_LINE_BYTES {
                    self.oversized = true;
                    self.line = Vec::new();
                }
            }
            let consumed = chunk.len() + usize::from(end.is_some());
            self.reader.consume(consumed);
            if end.is_some() {
                let line = std::mem::take(&mut self.line);
                if !std::mem::take(&mut self.oversized) {
                    return Ok(Some(line));
                }
            }
        }
    }
}
