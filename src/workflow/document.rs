//! The manager's workflow language and its expansion into existing declarations.

use super::{
    components::{Binding, Bindings},
    tasks::RetryPolicy,
};
use crate::declarations::{
    AuthorityMatchDeclaration, ContractDeclaration, DECLARATION_VERSION, EdgeDeclaration, Edits,
    GraphDeclaration, IngressDeclaration, NodeDeclaration, RootDeclaration, SchemaDeclaration,
    VALIDATOR_VERSION, ValidatorKind,
};
use crate::registry::ExecutionBinding;
use crate::{AppError, Result};
use ontography::Kernel;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

/// The one node type every node of a run created before node types has.
pub const SHARED_NODE_TYPE: &str = "WorkflowNode";
pub const EDGE_TYPE: &str = "WorkflowConnection";
/// What connections carry unless a document says otherwise: a message or a workspace.
pub const OBJECT_TYPE: &str = "Payload";
pub const CONTRACT: &str = "payload";
pub const AUTHORITY: &str = "workflow";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Document {
    pub name: String,
    pub entry: String,
    /// This document's own components: specifications that extend a built-in
    /// or library component with default settings.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub components: BTreeMap<String, Value>,
    pub nodes: Vec<DocumentNode>,
    #[serde(default)]
    pub edges: Vec<DocumentEdge>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DocumentNode {
    pub id: String,
    /// The component this node places: `agent`, `codex`, `claude`, `command`,
    /// `human`, `inbox`, or one from the library or this document.
    #[serde(alias = "kind")]
    pub component: String,
    /// This placement's settings, merged over its component's defaults.
    #[serde(default = "empty_config")]
    pub config: Value,
    #[serde(default)]
    pub join: IngressDeclaration,
    /// Retries for failed tasks; the defaults apply when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryPolicy>,
    /// Node-tool operations beyond the base set.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub grants: BTreeSet<Grant>,
    /// Shared node tools to expose. Absent means every tool allowed by grants;
    /// an empty set exposes none. This does not configure harness built-ins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<BTreeSet<String>>,
}

impl DocumentNode {
    pub fn retry_policy(&self) -> RetryPolicy {
        self.retry.unwrap_or_default()
    }
}

fn empty_config() -> Value {
    json!({})
}

/// A node-tool power beyond the base set, given to a node explicitly.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Grant {
    /// Start new work with input the node chooses.
    Originate,
    /// Create packages to send later, list them, and send them.
    SendLater,
    /// Retire packages held at this node.
    Retire,
}

impl Grant {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Originate => "originate",
            Self::SendLater => "send_later",
            Self::Retire => "retire",
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
            validate_node(node)?;
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

/// Checks that need no component. Components check each node's settings
/// when they bind it (`components::Catalog::bind`).
fn validate_node(node: &DocumentNode) -> Result<()> {
    let error = |message: &str| invalid(format!("Node {:?}: {message}", node.id));
    if !node.config.is_object() {
        return Err(error("config must be an object"));
    }
    if let Some(selected) = &node.tools {
        for name in selected {
            if !crate::node_tool::tools()
                .iter()
                .any(|tool| tool.name == name)
            {
                return Err(error(&format!("unknown node tool {name:?}")));
            }
        }
    }
    if let Some(retry) = &node.retry {
        retry.validate().map_err(error)?;
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

/// How a run types its nodes in core.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Typing {
    /// Runs created before node types: every node has one shared type.
    Shared,
    /// Each node has its component's types.
    Component,
}

impl Typing {
    /// Read from core: a run created before node types declares only the
    /// shared type.
    pub fn of(kernel: &Kernel) -> Self {
        if kernel.schema().node_types().eq([SHARED_NODE_TYPE]) {
            Self::Shared
        } else {
            Self::Component
        }
    }

    /// The types core gives a node with this binding. A node keeps its types
    /// for life: different types replace it.
    pub fn node_types(self, binding: &Binding) -> BTreeSet<String> {
        match self {
            Self::Shared => BTreeSet::from([SHARED_NODE_TYPE.to_owned()]),
            Self::Component => binding.types.clone(),
        }
    }

    /// A node as core declares it.
    pub(super) fn node(
        self,
        id: &str,
        binding: &Binding,
        join: IngressDeclaration,
    ) -> NodeDeclaration {
        NodeDeclaration {
            id: id.into(),
            types: self.node_types(binding).into_iter().collect(),
            result_contract: CONTRACT.into(),
            ingress_mode: join,
        }
    }

    /// A connection as core declares it. Any nodes may connect; runs created
    /// before node types require their one type at both ends.
    pub(super) fn edge(self, id: &str, from: &str, to: &str) -> EdgeDeclaration {
        let requirements = match self {
            Self::Shared => vec![SHARED_NODE_TYPE.into()],
            Self::Component => vec![],
        };
        EdgeDeclaration {
            id: id.into(),
            source: from.into(),
            target: to.into(),
            types: vec![EDGE_TYPE.into()],
            source_requirements: requirements.clone(),
            target_requirements: requirements,
            package_contract: CONTRACT.into(),
            authority_tags: vec![AUTHORITY.into()],
            authority_match: AuthorityMatchDeclaration::AnyOf,
        }
    }
}

/// The entry node's root rule: new work there carries workflow authority.
pub(super) fn root(id: &str) -> RootDeclaration {
    RootDeclaration {
        node_id: id.into(),
        ceiling: vec![AUTHORITY.into()],
    }
}

/// Expand a bound document into a new run's declaration. Each node has its
/// component's types in core, and its binding is its execution binding. The
/// run declares `node_types` and every type its nodes have: edits can place
/// nodes only of declared types.
pub fn expand(
    document: &Document,
    bindings: &Bindings,
    node_types: &BTreeSet<String>,
    definition_id: &str,
    identities: &IdentityMap,
) -> Result<GraphDeclaration> {
    let document = document.canonicalized()?;
    identities.validate(&document)?;
    if definition_id.trim().is_empty() {
        return Err(invalid("Definition identity must not be empty"));
    }
    if !bindings
        .keys()
        .eq(document.nodes.iter().map(|node| &node.id))
    {
        return Err(invalid("Every node needs exactly one component binding"));
    }
    let typing = Typing::Component;
    let nodes = document
        .nodes
        .iter()
        .map(|node| typing.node(&identities.nodes[&node.id], &bindings[&node.id], node.join))
        .collect();
    let edges = document
        .edges
        .iter()
        .map(|edge| {
            typing.edge(
                &identities.edges[&edge_key(&edge.from, &edge.to)],
                &identities.nodes[&edge.from],
                &identities.nodes[&edge.to],
            )
        })
        .collect();
    let execution_bindings = document
        .nodes
        .iter()
        .map(|node| {
            let implementation = &bindings[&node.id].implementation;
            ExecutionBinding {
                id: identities.nodes[&node.id].clone(),
                node_id: identities.nodes[&node.id].clone(),
                implementation: implementation.kind().into(),
                version: "1".into(),
                configuration: implementation.configuration(),
            }
        })
        .collect();
    Ok(GraphDeclaration {
        version: DECLARATION_VERSION,
        id: definition_id.into(),
        schema: SchemaDeclaration {
            node_types: node_types
                .iter()
                .chain(bindings.values().flat_map(|binding| &binding.types))
                .cloned()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            object_types: vec![OBJECT_TYPE.into()],
            authority_tags: vec![AUTHORITY.into()],
        },
        contracts: vec![ContractDeclaration {
            id: CONTRACT.into(),
            object_type: OBJECT_TYPE.into(),
            validator: ValidatorKind::Text,
            validator_version: VALIDATOR_VERSION,
        }],
        nodes,
        edges,
        roots: vec![root(&identities.nodes[&document.entry])],
        authority_transitions: vec![],
        // The workflow editor's policy governs edits (`edit::policy`).
        edits: Edits::Fixed,
        rewrites: None,
        execution_bindings,
    })
}

/// Expand a document that places only built-in components.
#[cfg(test)]
pub(crate) fn expand_builtin(
    document: &Document,
    definition_id: &str,
    identities: &IdentityMap,
) -> Result<GraphDeclaration> {
    let catalog = super::Catalog::builtin();
    let bindings = catalog.bind(document)?;
    let node_types = catalog.node_types(document)?;
    expand(document, &bindings, &node_types, definition_id, identities)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::{Catalog, components::definition_digest};

    fn document() -> Document {
        Document::parse(r#"{"name":"example","entry":"write","nodes":[{"id":"write","component":"agent","config":{"prompt":"Write"}},{"id":"test","component":"command","config":{"argv":["true"]}}],"edges":[{"from":"write","to":"test"}]}"#).unwrap()
    }

    fn bind(document: &Document) -> Result<Bindings> {
        Catalog::builtin().bind(document)
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
            expand_builtin(&first, "example", &identities)
                .unwrap()
                .fingerprint()
                .unwrap(),
            expand_builtin(&second, "example", &identities)
                .unwrap()
                .fingerprint()
                .unwrap()
        );
    }

    #[test]
    fn each_node_has_its_component_types_and_its_binding_as_execution_binding() {
        let mut document = document();
        let identities = IdentityMap::fresh(&document);
        let mut fingerprints = BTreeSet::new();
        for (component, config, node_type, implementation) in [
            ("agent", json!({"prompt":"Different"}), "Agent", "codex"),
            ("claude", json!({"prompt":"Different"}), "Agent", "claude"),
            ("command", json!({"argv":["false"]}), "Command", "command"),
            ("human", json!({"prompt":"Approve?"}), "Human", "human"),
            ("inbox", json!({}), "Inbox", "inbox"),
        ] {
            document.nodes[1].component = component.into();
            document.nodes[1].config = config;
            let declaration = expand_builtin(&document, "example", &identities).unwrap();
            assert_eq!(
                declaration.schema.node_types,
                ["Agent", "Command", "Human", "Inbox"]
            );
            let node = declaration
                .nodes
                .iter()
                .find(|node| node.id == identities.nodes["write"])
                .unwrap();
            assert_eq!(node.types, [node_type]);
            let binding = declaration
                .execution_bindings
                .iter()
                .find(|binding| binding.node_id == identities.nodes["write"])
                .unwrap();
            assert_eq!(binding.implementation, implementation);
            let compiled = declaration.compile().unwrap();
            fingerprints.insert(compiled.kernel.fingerprint().to_string());
        }
        // Core records the role: agents share one graph, other roles differ.
        assert_eq!(fingerprints.len(), 4);
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
        assert!(bind(&bad).is_err());
        assert!(expand_builtin(&original, "example", &IdentityMap::default()).is_err());
        assert!(
            expand(
                &original,
                &Bindings::new(),
                &BTreeSet::new(),
                "example",
                &IdentityMap::fresh(&original)
            )
            .is_err()
        );
        assert!(Document::parse(r#"{"name":"a","name":"b","entry":"n","nodes":[]}"#).is_err());
        assert_ne!(edge_key("a:b", "c"), edge_key("a", "b:c"));
    }

    #[test]
    fn documents_saved_with_kind_still_parse() {
        let old = Document::parse(r#"{"name":"example","entry":"write","nodes":[{"id":"write","kind":"agent","config":{"prompt":"Write"}}]}"#).unwrap();
        assert_eq!(old.nodes[0].component, "agent");
        let saved = serde_json::to_value(&old).unwrap();
        assert_eq!(saved["nodes"][0]["component"], "agent");
        assert!(saved["nodes"][0].get("kind").is_none());
        assert!(Document::parse(r#"{"name":"example","entry":"write","nodes":[{"id":"write","kind":"inbox","component":"inbox"}]}"#).is_err());
    }

    #[test]
    fn retry_and_grants_are_optional_validated_and_omitted_when_unset() {
        let original = document();
        let serialized = serde_json::to_value(&original).unwrap();
        assert!(serialized["nodes"][0].get("retry").is_none());
        assert!(serialized["nodes"][0].get("grants").is_none());
        assert_eq!(original.nodes[0].retry_policy(), RetryPolicy::default());

        let configured = Document::parse(r#"{"name":"example","entry":"write","nodes":[{"id":"write","component":"agent","config":{"prompt":"Write"},"retry":{"max_attempts":5},"grants":["send_later","originate"]}]}"#).unwrap();
        let node = &configured.nodes[0];
        assert_eq!(node.retry_policy().max_attempts, 5);
        assert_eq!(
            node.retry_policy().initial_delay_secs,
            RetryPolicy::default().initial_delay_secs
        );
        assert_eq!(
            node.grants.iter().copied().collect::<Vec<_>>(),
            [Grant::Originate, Grant::SendLater]
        );

        for invalid in [
            r#"{"id":"write","component":"agent","config":{"prompt":"Write"},"retry":{"max_attempts":0}}"#,
            r#"{"id":"write","component":"agent","config":{"prompt":"Write"},"retry":{"typo":1}}"#,
            r#"{"id":"write","component":"agent","config":{"prompt":"Write"},"grants":["everything"]}"#,
        ] {
            let text = format!(r#"{{"name":"example","entry":"write","nodes":[{invalid}]}}"#);
            assert!(
                Document::parse(&text).is_err(),
                "{invalid} must be rejected"
            );
        }
        // Whether a node takes tasks depends on its implementation.
        for invalid in [
            r#"{"id":"write","component":"human","retry":{"max_attempts":2}}"#,
            r#"{"id":"write","component":"inbox","grants":["retire"]}"#,
        ] {
            let text = format!(r#"{{"name":"example","entry":"write","nodes":[{invalid}]}}"#);
            assert!(
                bind(&Document::parse(&text).unwrap()).is_err(),
                "{invalid} must be rejected"
            );
        }
    }

    #[test]
    fn persistent_agent_configuration_has_no_task_timeout() {
        let mut configured = document();
        configured.nodes[1].config["timeout_secs"] = json!(30);
        assert!(
            bind(&configured)
                .unwrap_err()
                .message
                .contains("timeout_secs")
        );

        let mut configured = document();
        configured.nodes[0].config["timeout_secs"] = json!(30);
        assert!(bind(&configured).is_ok());
    }

    #[test]
    fn tool_selection_is_validated_and_preserves_omitted_and_empty_semantics() {
        let text = r#"{"name":"tools","entry":"worker","nodes":[{"id":"worker","component":"agent","config":{"prompt":"Observe"},"tools":["inspect_node","inspect_graph"]}]}"#;
        let configured = Document::parse(text).unwrap();
        let mut reversed: Value = serde_json::from_str(text).unwrap();
        reversed["nodes"][0]["tools"] = json!(["inspect_graph", "inspect_node"]);
        assert_eq!(Document::parse(&reversed.to_string()).unwrap(), configured);
        let mut default = configured.clone();
        default.nodes[0].tools = None;
        let mut empty = default.clone();
        empty.nodes[0].tools = Some(BTreeSet::new());
        let binding = &bind(&default).unwrap()["worker"];
        assert_ne!(
            definition_digest(&default.nodes[0], binding),
            definition_digest(&empty.nodes[0], binding)
        );
        assert!(
            serde_json::to_value(default).unwrap()["nodes"][0]
                .get("tools")
                .is_none()
        );
        assert_eq!(
            serde_json::to_value(empty).unwrap()["nodes"][0]["tools"],
            json!([])
        );
        reversed["nodes"][0]["tools"] = json!(["invented_tool"]);
        assert!(
            Document::parse(&reversed.to_string())
                .unwrap_err()
                .message
                .contains("invented_tool")
        );
        reversed["nodes"][0] = json!({"id":"worker","component":"inbox","tools":[]});
        assert!(bind(&Document::parse(&reversed.to_string()).unwrap()).is_err());
    }
}
