//! Host-delivered conversation input. Like tool replies, the exact input is
//! recorded before transport and marked sent only after Codex accepts it.

use super::{
    NodeToolContext, Reply,
    attempt::{Begin, FailInvocation, begin_attempt},
    context::{Attempt, context_error},
};
use crate::{
    Result,
    workflow::{WorkflowPayload, tasks::Task},
};
use serde_json::{Value, json};

// Keep both the conversation input and its core context charge bounded.
const INLINE_BYTES: u64 = 32 * 1024;
const OPEN_DELIVERIES: usize = 8;

impl NodeToolContext {
    pub(crate) fn message_is_open(&self, attempt: &str) -> bool {
        self.open_attempts().iter().any(|id| id == attempt)
    }

    /// Close a delivered attempt left open when its Codex turn fails. Ordinary
    /// successful turns may deliberately keep work open across conversation.
    pub(crate) async fn message_failed(&self, attempt: &str, reason: &str) -> Result<()> {
        match super::run::<FailInvocation>(
            self,
            json!({"attempt_id":attempt,"reason":reason,"retryable":true}),
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(error) if matches!(error.code.as_str(), "attempt_ended" | "unknown_attempt") => {
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// Delivery is a host input channel, independent of the exposed tool
    /// allowlist. It uses the same grants, joins, retries, and invocation policy
    /// as explicit begin_invocation. No management authority is added.
    pub(crate) async fn next_message(&self) -> Result<Option<Reply>> {
        if self.open_attempts().len() >= OPEN_DELIVERIES {
            return Ok(None);
        }
        let Some(task) = self.tasks(&self.busy())?.next().await? else {
            return Ok(None);
        };
        match begin_attempt(
            self,
            Begin {
                task_id: Some(task.key.to_string()),
                originate: None,
            },
            true,
        )
        .await
        {
            Ok(reply) => Ok(Some(reply)),
            // A tool or graph edit may have claimed/invalidated it first.
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "stale_task" | "task_in_progress" | "task_retrying" | "task_parked"
                ) =>
            {
                Ok(None)
            }
            Err(error)
                if error
                    .details
                    .as_ref()
                    .is_some_and(|details| !details["retry"].is_null()) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
}

pub(super) async fn record(
    _context: &NodeToolContext,
    attempt: &Attempt,
    mut inputs: Vec<Value>,
) -> Result<Reply> {
    let mut remaining = INLINE_BYTES;
    for input in &mut inputs {
        if input["workspace"] == true {
            continue;
        }
        let size = input["bytes"].as_u64().unwrap_or(u64::MAX);
        if size > remaining {
            input["message_omitted"] =
                json!("Input exceeds inline delivery limit; use read_package with this handle.");
            continue;
        }
        // Core authorizes and budgets the read. The internal read receipt stays
        // prepared; only the final envelope below is actually sent to Codex.
        let response = attempt
            .invocation
            .call(
                "package.read",
                json!({"handle":input["handle"],"start":0,"end":size}),
            )
            .await
            .map_err(context_error)?;
        if let Some(text) = response.value["text"].as_str() {
            match WorkflowPayload::decode(text.as_bytes()) {
                Ok(WorkflowPayload::Message { message }) => input["message"] = json!(message),
                _ => input["text"] = json!(text),
            }
        } else {
            input["message_omitted"] = json!("Binary input; use read_package with this handle.");
        }
        remaining = remaining.saturating_sub(size);
    }
    Reply::record(
        attempt,
        "node_message",
        &json!({
            "type":"ontography_message", "node":attempt.node.id,
            "attempt_id":attempt.id, "task_id":attempt.task.as_ref().map(|task| &task.key),
            "initial":attempt.task.as_ref().is_some_and(Task::is_initial), "inputs":inputs,
        }),
    )
    .await
}
