//! The server's exported history of a run, as typed records.

use anyhow::{Context, Result, bail};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub enum Trigger {
    Root { node: String },
    Packages(Vec<String>),
}

#[derive(Clone, Debug)]
pub struct Output {
    pub package: String,
    pub edge: Option<String>,
    pub digest: String,
}

#[derive(Clone, Debug)]
pub struct Activation {
    pub trigger: Trigger,
    pub result: Vec<u8>,
    pub outputs: Vec<Output>,
}

#[derive(Clone, Debug)]
pub struct Package {
    pub producer: String,
    /// The node it was sent to.
    pub holder: String,
    /// The connection that delivered it.
    pub edge: Option<String>,
    pub disposition: String,
    pub consumer: Option<String>,
    pub digest: String,
    pub retirement: Option<String>,
}

#[derive(Debug, Default)]
pub struct History {
    pub activations: BTreeMap<String, Activation>,
    pub packages: BTreeMap<String, Package>,
}

/// Bytes as the app's views write them: `{text, length}` when they are
/// UTF-8, `{bytes: [numbers], length}` otherwise.
pub fn bytes(value: &Value) -> Result<Vec<u8>> {
    let bytes = match (&value["text"], &value["bytes"]) {
        (Value::String(text), _) => text.as_bytes().to_vec(),
        (_, Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_u64()
                    .and_then(|n| u8::try_from(n).ok())
                    .context("byte")
            })
            .collect::<Result<_>>()?,
        _ => bail!("not bytes: {value}"),
    };
    if value["length"].as_u64() != Some(bytes.len() as u64) {
        bail!("bytes whose length is not {}: {value}", bytes.len());
    }
    Ok(bytes)
}

fn text(value: &Value) -> Option<String> {
    value.as_str().map(String::from)
}

impl History {
    pub fn parse(export: &Value) -> Result<Self> {
        let mut history = Self::default();
        for record in export["activations"].as_array().context("activations")? {
            let id = text(&record["activation_id"]).context("activation_id")?;
            let trigger = match record["trigger"]["kind"].as_str() {
                Some("root") => Trigger::Root {
                    node: text(&record["trigger"]["node_id"]).context("root node")?,
                },
                Some("packages") => Trigger::Packages(
                    record["trigger"]["package_ids"]
                        .as_array()
                        .context("package_ids")?
                        .iter()
                        .filter_map(text)
                        .collect(),
                ),
                other => bail!("unknown trigger {other:?}"),
            };
            if record["result_truncated"] == true {
                bail!("activation {id} result is truncated in the export");
            }
            let outputs = record["outputs"]
                .as_array()
                .context("outputs")?
                .iter()
                .map(|output| {
                    Ok(Output {
                        package: text(&output["package_id"]).context("package_id")?,
                        edge: text(&output["edge_id"]),
                        digest: text(&output["content_digest"]).context("content_digest")?,
                    })
                })
                .collect::<Result<_>>()?;
            history.activations.insert(
                id,
                Activation {
                    trigger,
                    result: bytes(&record["result"])?,
                    outputs,
                },
            );
        }
        for record in export["packages"].as_array().context("packages")? {
            let fact = &record["package"];
            let id = text(&fact["package_id"]).context("package_id")?;
            history.packages.insert(
                id,
                Package {
                    producer: text(&fact["producer"]).context("producer")?,
                    holder: text(&fact["node_id"]).context("node_id")?,
                    edge: text(&fact["edge_id"]),
                    disposition: text(&record["disposition"]).context("disposition")?,
                    consumer: text(&record["consumer"]),
                    digest: text(&fact["content_digest"]).context("content_digest")?,
                    retirement: text(&record["retirement"]["reason"]),
                },
            );
        }
        Ok(history)
    }

    /// The node an activation worked at.
    pub fn node_of(&self, activation: &Activation) -> Option<String> {
        match &activation.trigger {
            Trigger::Root { node } => Some(node.clone()),
            Trigger::Packages(inputs) => inputs
                .first()
                .and_then(|id| self.packages.get(id))
                .map(|p| p.holder.clone()),
        }
    }
}
