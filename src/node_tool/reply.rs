//! The bytes a transport sends for one call, and the receipt covering them.

use super::context::{Attempt, UnsentGuard, context_error};
use crate::{AppError, Result};
use ontography::{ContextResponse, InvocationHandle, InvocationId, Payload};
use serde::Serialize;
use serde_json::Value;

/// One tool's reply. The transport sends `bytes()` unchanged and then calls
/// `sent()`, so a receipt always names exactly what the worker received.
pub struct Reply {
    bytes: Payload,
    receipt: Option<Receipt>,
}

struct Receipt {
    invocation: InvocationHandle,
    sequence: u64,
    /// Lets the attempt wait for this reply before it ends.
    _unsent: UnsentGuard,
}

impl Reply {
    /// Metadata outside any attempt, or the outcome that ended one. Neither
    /// has an open invocation to record it.
    pub(super) fn plain(value: &impl Serialize) -> Result<Self> {
        Ok(Self {
            bytes: serde_json::to_vec(value)?.into(),
            receipt: None,
        })
    }

    /// A response core recorded itself. Re-encoding its value reproduces the
    /// recorded bytes: core serializes the same value with the same encoder.
    pub(super) fn recorded_by_core(attempt: &Attempt, response: ContextResponse) -> Result<Self> {
        Ok(Self {
            bytes: serde_json::to_vec(&response.value)?.into(),
            receipt: Some(Receipt::new(attempt, response.sequence)),
        })
    }

    /// Records a reply built inside an open attempt, exactly as it will be sent.
    pub(super) async fn record(
        attempt: &Attempt,
        tool: &str,
        value: &impl Serialize,
    ) -> Result<Self> {
        let bytes: Payload = serde_json::to_vec(value)?.into();
        let response = attempt
            .invocation
            .record_tool_response(tool, bytes.clone())
            .await
            .map_err(context_error)?;
        Ok(Self {
            bytes,
            receipt: Some(Receipt::new(attempt, response.sequence)),
        })
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The reply as JSON, for in-process callers.
    pub fn value(&self) -> Result<Value> {
        Ok(serde_json::from_slice(&self.bytes)?)
    }

    /// The attempt and receipt sequence that recorded these bytes, if any.
    pub fn receipt(&self) -> Option<(InvocationId, u64)> {
        self.receipt
            .as_ref()
            .map(|receipt| (receipt.invocation.id(), receipt.sequence))
    }

    /// Call after the transport has written `bytes()`. A reply that was never
    /// written keeps its receipt at "prepared".
    pub async fn sent(self) -> Result<()> {
        let Some(receipt) = self.receipt else {
            return Ok(());
        };
        receipt
            .invocation
            .mark_sent(receipt.sequence)
            .await
            .map_err(|error| {
                AppError::new(
                    "receipt_not_marked",
                    format!("The reply was sent but its receipt stayed prepared: {error}"),
                )
            })
    }
}

impl Receipt {
    fn new(attempt: &Attempt, sequence: u64) -> Self {
        Self {
            invocation: attempt.invocation.clone(),
            sequence,
            _unsent: attempt.unsent.hold(),
        }
    }
}

impl std::fmt::Debug for Reply {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Reply")
            .field("bytes", &self.bytes.len())
            .field("receipt", &self.receipt())
            .finish()
    }
}
