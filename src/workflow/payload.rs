//! What a package holds, read as core reads it: an explicit package envelope,
//! which names a workspace, or ordinary bytes, which are a message when they
//! are text. Core stores `encode`; the app's caches and views keep the JSON form.

use crate::{AppError, Result};
use ontography::{PackageEnvelope, Payload};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::Write;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum WorkflowPayload {
    Message {
        message: String,
    },
    Workspace(PackageEnvelope),
    /// Bytes that are not text, in lowercase hexadecimal.
    Binary {
        hex: String,
    },
}

impl WorkflowPayload {
    /// Read stored bytes as core does. A malformed envelope is an error, never text.
    pub fn read(payload: &Payload) -> Result<Self> {
        if let Some(envelope) = PackageEnvelope::from_payload(payload).map_err(invalid)? {
            return Ok(Self::Workspace(envelope));
        }
        Ok(match std::str::from_utf8(payload) {
            Ok(text) => Self::Message {
                message: text.into(),
            },
            Err(_) => Self::Binary {
                hex: payload.iter().fold(String::new(), |mut hex, byte| {
                    let _ = write!(hex, "{byte:02x}");
                    hex
                }),
            },
        })
    }

    /// The bytes core stores; reading them gives this payload back.
    pub fn encode(&self) -> Result<Payload> {
        let bytes: Payload = match self {
            Self::Message { message } => message.as_bytes().into(),
            Self::Workspace(envelope) => envelope.to_payload().map_err(invalid)?,
            Self::Binary { hex } => unhex(hex)?.into(),
        };
        if Self::read(&bytes)? != *self {
            return Err(invalid(match self {
                Self::Message { .. } => {
                    "This message reads as a package envelope; send it as a workspace"
                }
                _ => "Binary content must be non-text bytes in lowercase hexadecimal",
            }));
        }
        Ok(bytes)
    }

    /// Parse the JSON form kept in caches and views.
    pub fn from_value(value: &Value) -> Result<Self> {
        let payload: Self = serde_json::from_value(value.clone()).map_err(invalid)?;
        payload.encode()?;
        Ok(payload)
    }

    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).expect("a payload serializes")
    }
}

fn unhex(hex: &str) -> Result<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return Err(invalid("Hexadecimal content needs an even length"));
    }
    (0..hex.len())
        .step_by(2)
        .map(|at| {
            hex.get(at..at + 2)
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                .ok_or_else(|| invalid("Binary content must be hexadecimal"))
        })
        .collect()
}

fn invalid(error: impl ToString) -> AppError {
    AppError::new("invalid_payload", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn stored_bytes_are_text_envelopes_or_binary_and_round_trip() {
        let message = WorkflowPayload::Message {
            message: "Hello".into(),
        };
        assert_eq!(&*message.encode().unwrap(), b"Hello");
        let binary = WorkflowPayload::read(&Payload::from(&[0xff, 0x00][..])).unwrap();
        assert_eq!(binary, WorkflowPayload::Binary { hex: "ff00".into() });
        assert_eq!(&*binary.encode().unwrap(), &[0xff, 0x00]);
        for payload in [message, binary] {
            assert_eq!(
                WorkflowPayload::from_value(&payload.to_value()).unwrap(),
                payload
            );
        }
    }

    #[test]
    fn ambiguous_or_malformed_payloads_are_refused() {
        for value in [
            json!({"message": 7}),
            json!({"message": "a", "extra": true}),
            json!({"ontography_package": "wrong"}),
            json!({"hex": "0g"}),
            json!({"hex": "41"}),
            json!({"message": "{\"ontography_package\":\"wrong\"}"}),
        ] {
            assert!(WorkflowPayload::from_value(&value).is_err(), "{value}");
        }
        assert!(
            WorkflowPayload::read(&Payload::from(&br#"{"ontography_package":7}"#[..])).is_err()
        );
    }
}
