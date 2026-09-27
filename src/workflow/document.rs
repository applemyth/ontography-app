//! The manager's workflow language and its expansion into existing declarations.

use super::grammar;
use crate::declarations::{
    ContractDeclaration, DECLARATION_VERSION, GraphDeclaration, IngressDeclaration,
    SchemaDeclaration, VALIDATOR_VERSION, ValidatorKind,
};
use crate::registry::ExecutionBinding;
use crate::{AppError, Result};
use ontography::{ContractViolation, PackageEnvelope, Payload, content::BlobFormat};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub const NODE_TYPE: &str = "WorkflowNode";
pub const EDGE_TYPE: &str = "WorkflowConnection";
pub const OBJECT_TYPE: &str = "WorkflowPayload";
pub const CONTRACT: &str = "workflow_payload";
pub const AUTHORITY: &str = "workflow";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Document {
    pub name: String,
    pub entry: String,
    pub nodes: Vec<DocumentNode>,
    #[serde(default)]
    pub edges: Vec<DocumentEdge>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DocumentNode {
    pub id: String,
    pub kind: NodeKind,
    #[serde(default = "empty_config")]
    pub config: Value,
    #[serde(default)]
    pub join: JoinMode,
}

fn empty_config() -> Value {
    json!({})
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    Agent,
    Command,
    Human,
    Inbox,
}

impl NodeKind {
    pub const fn implementation(self) -> &'static str {
        match self {
            Self::Agent => "workflow.agent",
            Self::Command => "workflow.command",
            Self::Human => "workflow.human",
            Self::Inbox => "workflow.inbox",
        }
    }
}

#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum JoinMode {
    #[default]
    Any,
    All,
}

impl From<JoinMode> for IngressDeclaration {
    fn from(value: JoinMode) -> Self {
        match value {
            JoinMode::Any => Self::Any,
            JoinMode::All => Self::All,
        }
    }
}

#[derive(
    Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct DocumentEdge {
    pub from: String,
    pub to: String,
}

impl Document {
    pub fn parse(text: &str) -> Result<Self> {
        crate::declarations::parse_json::<Self>(text)
            .map_err(|error| invalid(error.to_string()))?
            .canonicalized()
    }

    /// Array order is presentation only; persisted comparisons use this order.
    pub fn canonicalized(&self) -> Result<Self> {
        if self.name.trim().is_empty() {
            return Err(invalid("Workflow name must not be empty"));
        }
        let mut names = BTreeSet::new();
        for node in &self.nodes {
            if node.id.trim().is_empty() || !names.insert(node.id.as_str()) {
                return Err(invalid(format!(
                    "Node names must be nonempty and unique: {:?}",
                    node.id
                )));
            }
            validate_config(node)?;
        }
        if !names.contains(self.entry.as_str()) {
            return Err(invalid(format!("Entry {:?} must name a node", self.entry)));
        }
        let mut edges = BTreeSet::new();
        for edge in &self.edges {
            if !names.contains(edge.from.as_str()) || !names.contains(edge.to.as_str()) {
                return Err(invalid(format!(
                    "Connection {:?} → {:?} must name existing nodes",
                    edge.from, edge.to
                )));
            }
            if !edges.insert((edge.from.as_str(), edge.to.as_str())) {
                return Err(invalid(format!(
                    "Duplicate connection {:?} → {:?}",
                    edge.from, edge.to
                )));
            }
        }
        let mut document = self.clone();
        document.nodes.sort_by(|a, b| a.id.cmp(&b.id));
        document.edges.sort();
        Ok(document)
    }
}

fn validate_config(node: &DocumentNode) -> Result<()> {
    let error = |message: &str| invalid(format!("Node {:?}: {message}", node.id));
    let config = node
        .config
        .as_object()
        .ok_or_else(|| error("config must be an object"))?;
    let allowed: &[&str] = match node.kind {
        NodeKind::Agent => &["prompt", "harness", "argv", "model", "timeout_secs"],
        NodeKind::Command => &["argv", "timeout_secs"],
        NodeKind::Human => &["prompt"],
        NodeKind::Inbox => &[],
    };
    for key in config.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(error(&format!("unsupported configuration field {key:?}")));
        }
    }
    if node.kind == NodeKind::Agent && !config.get("prompt").is_some_and(Value::is_string) {
        return Err(error("agent prompt must be a string"));
    }
    for key in ["prompt", "model"] {
        if config.get(key).is_some_and(|value| !value.is_string()) {
            return Err(error(&format!("{key} must be a string")));
        }
    }
    if config
        .get("harness")
        .is_some_and(|value| value.as_str() != Some("codex"))
    {
        return Err(error(
            "the supported agent harness is codex; use argv to override its runner",
        ));
    }
    if node.kind == NodeKind::Command || config.contains_key("argv") {
        let argv = config
            .get("argv")
            .and_then(Value::as_array)
            .ok_or_else(|| error("argv must be a nonempty array of strings"))?;
        if argv.is_empty()
            || argv.iter().any(|value| !value.is_string())
            || argv[0].as_str().is_none_or(|value| value.trim().is_empty())
        {
            return Err(error(
                "argv must contain a command followed by string arguments",
            ));
        }
    }
    if config
        .get("timeout_secs")
        .is_some_and(|value| value.as_u64().is_none_or(|n| n == 0))
    {
        return Err(error("timeout_secs must be a positive integer"));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> AppError {
    AppError::new("invalid_workflow_document", message)
}

/// Identity allocation is persisted with the document/edit, never repeated during recovery.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityMap {
    pub nodes: BTreeMap<String, String>,
    pub edges: BTreeMap<String, String>,
}

/// JSON tuple encoding avoids ambiguous names such as `a:b` and `a` + `b:c`.
pub fn edge_key(from: &str, to: &str) -> String {
    json!([from, to]).to_string()
}

impl IdentityMap {
    pub fn fresh(document: &Document) -> Self {
        Self {
            nodes: document
                .nodes
                .iter()
                .map(|node| (node.id.clone(), uuid::Uuid::new_v4().to_string()))
                .collect(),
            edges: document
                .edges
                .iter()
                .map(|edge| {
                    (
                        edge_key(&edge.from, &edge.to),
                        uuid::Uuid::new_v4().to_string(),
                    )
                })
                .collect(),
        }
    }

    fn validate(&self, document: &Document) -> Result<()> {
        let node_names: BTreeSet<_> = document.nodes.iter().map(|node| node.id.clone()).collect();
        let edge_names: BTreeSet<_> = document
            .edges
            .iter()
            .map(|edge| edge_key(&edge.from, &edge.to))
            .collect();
        for (expected, actual) in [(&node_names, &self.nodes), (&edge_names, &self.edges)] {
            if expected != &actual.keys().cloned().collect()
                || actual.values().any(|id| id.trim().is_empty())
                || actual.values().collect::<BTreeSet<_>>().len() != actual.len()
            {
                return Err(AppError::new(
                    "invalid_workflow_identities",
                    "Saved identities must cover the document exactly and be nonempty and unique",
                ));
            }
        }
        Ok(())
    }
}

pub fn expand(
    document: &Document,
    definition_id: &str,
    identities: &IdentityMap,
) -> Result<GraphDeclaration> {
    let document = document.canonicalized()?;
    identities.validate(&document)?;
    if definition_id.trim().is_empty() {
        return Err(invalid("Definition identity must not be empty"));
    }
    let nodes = document
        .nodes
        .iter()
        .map(|node| grammar::node(&identities.nodes[&node.id], node.join))
        .collect();
    let edges = document
        .edges
        .iter()
        .map(|edge| {
            grammar::edge(
                &identities.edges[&edge_key(&edge.from, &edge.to)],
                &identities.nodes[&edge.from],
                &identities.nodes[&edge.to],
            )
        })
        .collect();
    let execution_bindings = document
        .nodes
        .iter()
        .map(|node| ExecutionBinding {
            id: identities.nodes[&node.id].clone(),
            node_id: identities.nodes[&node.id].clone(),
            implementation: node.kind.implementation().into(),
            version: "1".into(),
            configuration: node.config.clone(),
        })
        .collect();
    Ok(GraphDeclaration {
        version: DECLARATION_VERSION,
        id: definition_id.into(),
        schema: SchemaDeclaration {
            node_types: vec![NODE_TYPE.into()],
            object_types: vec![OBJECT_TYPE.into()],
            authority_tags: vec![AUTHORITY.into()],
        },
        contracts: vec![ContractDeclaration {
            id: CONTRACT.into(),
            object_type: OBJECT_TYPE.into(),
            validator: ValidatorKind::WorkflowPayload,
            validator_version: VALIDATOR_VERSION,
        }],
        nodes,
        edges,
        roots: vec![grammar::root(&identities.nodes[&document.entry])],
        authority_transitions: vec![],
        rewrites: grammar::universal(),
        execution_bindings,
    })
}

/// The workspace alternative preserves core's explicit package envelope unchanged.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum WorkflowPayload {
    Message { message: String },
    Workspace(PackageEnvelope),
}

impl WorkflowPayload {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let text = std::str::from_utf8(bytes).map_err(|error| invalid(error.to_string()))?;
        let payload: Self =
            crate::declarations::parse_json(text).map_err(|error| invalid(error.to_string()))?;
        if let Self::Workspace(envelope) = &payload
            && envelope.ontography_package.format() != BlobFormat::Raw
        {
            return Err(invalid("Workspace packages must use raw content format"));
        }
        Ok(payload)
    }

    pub fn encode(&self) -> Result<Payload> {
        let bytes = serde_json::to_vec(self)?;
        Self::decode(&bytes)?;
        Ok(bytes.into())
    }
}

pub fn validate_payload(bytes: &[u8]) -> std::result::Result<(), ContractViolation> {
    WorkflowPayload::decode(bytes)
        .map(|_| ())
        .map_err(|error| ContractViolation::new(error.message))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn document() -> Document {
        Document::parse(r#"{"name":"example","entry":"write","nodes":[{"id":"write","kind":"agent","config":{"prompt":"Write"}},{"id":"test","kind":"command","config":{"argv":["true"]}}],"edges":[{"from":"write","to":"test"}]}"#).unwrap()
    }

    #[test]
    fn array_order_does_not_change_document_or_expansion() {
        let first = document();
        let identities = IdentityMap::fresh(&first);
        let mut second = first.clone();
        second.nodes.reverse();
        second.edges.reverse();
        assert_eq!(first, second.canonicalized().unwrap());
        assert_eq!(
            expand(&first, "example", &identities)
                .unwrap()
                .fingerprint()
                .unwrap(),
            expand(&second, "example", &identities)
                .unwrap()
                .fingerprint()
                .unwrap()
        );
    }

    #[test]
    fn every_kind_has_the_same_core_semantics() {
        let mut document = document();
        let identities = IdentityMap::fresh(&document);
        let expected = expand(&document, "example", &identities)
            .unwrap()
            .compile()
            .unwrap()
            .kernel
            .fingerprint()
            .to_string();
        for (kind, config) in [
            (NodeKind::Agent, json!({"prompt":"Different"})),
            (NodeKind::Command, json!({"argv":["false"]})),
            (NodeKind::Human, json!({"prompt":"Approve?"})),
            (NodeKind::Inbox, json!({})),
        ] {
            document.nodes[0].kind = kind;
            document.nodes[0].config = config;
            let declaration = expand(&document, "example", &identities).unwrap();
            assert_eq!(
                declaration.execution_bindings[0].implementation,
                kind.implementation()
            );
            assert_eq!(
                declaration
                    .compile()
                    .unwrap()
                    .kernel
                    .fingerprint()
                    .to_string(),
                expected
            );
        }
    }

    #[test]
    fn invalid_connections_configuration_and_identities_are_rejected() {
        let original = document();
        let mut bad = original.clone();
        bad.edges.push(bad.edges[0].clone());
        assert!(bad.canonicalized().is_err());
        bad = original.clone();
        bad.edges[0].to = "missing".into();
        assert!(bad.canonicalized().is_err());
        bad = original.clone();
        bad.nodes[0].config["typo"] = json!(true);
        assert!(bad.canonicalized().is_err());
        assert!(expand(&original, "example", &IdentityMap::default()).is_err());
        assert!(Document::parse(r#"{"name":"a","name":"b","entry":"n","nodes":[]}"#).is_err());
        assert_ne!(edge_key("a:b", "c"), edge_key("a", "b:c"));
    }

    #[test]
    fn envelope_validation_rejects_ambiguous_or_malformed_payloads() {
        let message = WorkflowPayload::Message {
            message: "Hello".into(),
        };
        assert_eq!(
            WorkflowPayload::decode(&message.encode().unwrap()).unwrap(),
            message
        );
        for bytes in [
            b"hello".as_slice(),
            br#"{"message":7}"#,
            br#"{"message":"a","extra":true}"#,
            br#"{"message":"a","message":"b"}"#,
            br#"{"ontography_package":"wrong"}"#,
        ] {
            assert!(validate_payload(bytes).is_err());
        }
    }
}
