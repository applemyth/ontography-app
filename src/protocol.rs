use crate::{AppError, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

pub const VERSION: u32 = 1;
pub const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub version: u32,
    pub client_id: String,
    pub request_id: String,
    pub operation: String,
    /// Durable application session; omitted only for explicitly global administration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_server_id: Option<String>,
    #[serde(default = "empty_object")]
    pub args: Value,
    /// The client's environment, for the programs of a session this request
    /// activates. Only the command line sends it, and never in a handshake,
    /// which must stay readable by servers of other builds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment: Option<BTreeMap<String, String>>,
}

fn empty_object() -> Value {
    json!({})
}

impl Request {
    pub fn validate(&self) -> Result<()> {
        if self.version != VERSION {
            return Err(AppError::new(
                "protocol_mismatch",
                format!("server protocol is {VERSION}"),
            ));
        }
        for (kind, value) in [
            ("client_id", &self.client_id),
            ("request_id", &self.request_id),
            ("operation", &self.operation),
        ] {
            if value.is_empty() || value.len() > 160 || value.chars().any(char::is_control) {
                return Err(AppError::invalid(format!("invalid {kind}")));
            }
        }
        if !self.args.is_object() {
            return Err(AppError::invalid("args must be an object"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Response {
    pub version: u32,
    pub server_id: String,
    pub request_id: String,
    #[serde(flatten)]
    pub outcome: Outcome,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Outcome {
    Ok { result: Value },
    Error { error: AppError },
}

impl Response {
    pub fn new(server_id: &str, request_id: &str, result: Result<Value>) -> Self {
        Self {
            version: VERSION,
            server_id: server_id.into(),
            request_id: request_id.into(),
            outcome: match result {
                Ok(result) => Outcome::Ok { result },
                Err(error) => Outcome::Error { error },
            },
        }
    }

    pub fn into_result(self) -> Result<Value> {
        match self.outcome {
            Outcome::Ok { result } => Ok(result),
            Outcome::Error { error } => Err(error),
        }
    }
}

/// Read only through the first LF and cap allocation before accepting a frame.
pub async fn read_frame(reader: &mut (impl AsyncBufRead + Unpin)) -> Result<Option<Vec<u8>>> {
    let mut frame = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Err(AppError::new("protocol_error", "incomplete frame"))
            };
        }
        let end = available.iter().position(|&b| b == b'\n').map(|i| i + 1);
        let count = end.unwrap_or(available.len());
        if frame.len() + count > MAX_FRAME_BYTES {
            return Err(AppError::new(
                "frame_too_large",
                format!("frame exceeds {MAX_FRAME_BYTES} bytes"),
            ));
        }
        frame.extend_from_slice(&available[..count]);
        reader.consume(count);
        if end.is_some() {
            frame.pop();
            if frame.last() == Some(&b'\r') {
                frame.pop();
            }
            return Ok(Some(frame));
        }
    }
}

pub async fn write_frame(
    writer: &mut (impl AsyncWrite + Unpin),
    value: &impl Serialize,
) -> Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    if bytes.len() + 1 > MAX_FRAME_BYTES {
        return Err(AppError::new(
            "result_too_large",
            "use bounded reads or export the result to a file",
        ));
    }
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}
