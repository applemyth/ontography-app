//! The manager's workflow language and its expansion into core declarations.
//!
//! Typing is optional. Without it, every result and connection carries
//! `payload` (a message or a workspace) under the one `workflow` authority
//! tag, and only the entry starts work. A document refines that where it says
//! so: named contracts, each node's result contract, roots, and authority
//! transitions, and each connection's contract and authority. A connection's
//! contract defaults to its source's result contract; authority is never
//! inferred beyond the default tag.

use super::{
    components::{Binding, Bindings},
    tasks::RetryPolicy,
};
use crate::declarations::{
    AuthorityMatchDeclaration, AuthorityTransitionDeclaration, ContractDeclaration,
    DECLARATION_VERSION, EdgeDeclaration, GraphDeclaration, IngressDeclaration, NodeDeclaration,
    RootDeclaration, SchemaDeclaration, VALIDATOR_VERSION, ValidatorKind,
};
use crate::{AppError, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub const EDGE_TYPE: &str = "WorkflowConnection";
/// What results and connections carry unless a document says otherwise: a
/// message or a workspace.
pub const OBJECT_TYPE: &str = "Payload";
pub const CONTRACT: &str = "payload";
/// The authority of connections and roots that name none.
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
    /// Contracts by name, besides the built-in `payload`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub contracts: BTreeMap<String, ContractSpec>,
    pub nodes: Vec<DocumentNode>,
    #[serde(default)]
    pub edges: Vec<DocumentEdge>,
}

/// What a contract's payloads are, and the trusted validator they pass.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ContractSpec {
    pub object_type: String,
    pub validator: ValidatorKind,
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
    /// The contract of this node's results; `payload` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    /// The most authority work this node starts may carry. Only nodes with a
    /// root start work; the entry always has one, `["workflow"]` unless given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<BTreeSet<String>>,
    /// Authority changes this node may make to what it sends.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transitions: Vec<Transition>,
}

/// Work carrying exactly `from` may leave the node carrying exactly `to`.
#[derive(
    Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct Transition {
    pub from: BTreeSet<String>,
    pub to: BTreeSet<String>,
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
    /// Needed to connect the same two nodes more than once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The contract of what it carries; its source's result contract when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract: Option<String>,
    /// The authority tags it admits; `["workflow"]` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority: Option<BTreeSet<String>>,
    /// Whether work needs any of those tags, or all of them.
    #[serde(
        default,
        rename = "match",
        skip_serializing_if = "AuthorityMatchDeclaration::is_any_of"
    )]
    pub matching: AuthorityMatchDeclaration,
}

impl DocumentEdge {
    /// Its name, or `from:to` for the one unnamed connection between two nodes.
    pub fn key(&self) -> String {
        self.name
            .clone()
            .unwrap_or_else(|| edge_key(&self.from, &self.to))
    }

    /// Whether work carrying exactly `tags` may pass, as core matches them.
    fn admits(&self, tags: &BTreeSet<String>) -> bool {
        let default = BTreeSet::from([AUTHORITY.to_owned()]);
        let admitted = self.authority.as_ref().unwrap_or(&default);
        match self.matching {
            AuthorityMatchDeclaration::AnyOf => !admitted.is_disjoint(tags),
            AuthorityMatchDeclaration::AllOf => admitted.is_subset(tags),
        }
    }
}

/// The key of the unnamed connection from `from` to `to`. Names cannot
/// contain `:`, so no connection's key is another's.
pub fn edge_key(from: &str, to: &str) -> String {
    format!("{from}:{to}")
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
        for (name, contract) in &self.contracts {
            if name == CONTRACT {
                return Err(invalid(format!("Contract {CONTRACT:?} is built in")));
            }
            check_name("Contract", name)?;
            check_name("Object type", &contract.object_type)?;
        }
        let mut names = BTreeSet::new();
        for node in &self.nodes {
            check_name("Node", &node.id)?;
            if !names.insert(node.id.as_str()) {
                return Err(invalid(format!("Node names must be unique: {:?}", node.id)));
            }
            validate_node(node)?;
            if let Some(result) = &node.result {
                self.check_contract(result)?;
            }
            let transitions = node
                .transitions
                .iter()
                .flat_map(|rule| rule.from.iter().chain(&rule.to));
            for tag in node.root.iter().flatten().chain(transitions) {
                check_name("Authority tag", tag)?;
            }
        }
        if !names.contains(self.entry.as_str()) {
            return Err(invalid(format!("Entry {:?} must name a node", self.entry)));
        }
        let mut keys = BTreeSet::new();
        for edge in &self.edges {
            if !names.contains(edge.from.as_str()) || !names.contains(edge.to.as_str()) {
                return Err(invalid(format!(
                    "Connection {:?} → {:?} must name existing nodes",
                    edge.from, edge.to
                )));
            }
            if let Some(name) = &edge.name {
                check_name("Connection", name)?;
            }
            if !keys.insert(edge.key()) {
                return Err(invalid(match &edge.name {
                    Some(name) => format!("Connection name {name:?} is used twice"),
                    None => format!(
                        "Connections {:?} → {:?} need names to connect the same nodes twice",
                        edge.from, edge.to
                    ),
                }));
            }
            if let Some(contract) = &edge.contract {
                self.check_contract(contract)?;
            }
            if let Some(tags) = &edge.authority {
                if tags.is_empty() {
                    return Err(invalid(format!(
                        "Connection {:?} must admit at least one authority tag",
                        edge.key()
                    )));
                }
                for tag in tags {
                    check_name("Authority tag", tag)?;
                }
            }
        }
        let mut document = self.clone();
        document.nodes.sort_by(|a, b| a.id.cmp(&b.id));
        for node in &mut document.nodes {
            node.transitions.sort();
            node.transitions.dedup();
        }
        document.edges.sort();
        Ok(document)
    }

    /// Node tools accept a node's or a connection's name in `to`, so a
    /// connection named like a node would shadow it. Only a name that shadows
    /// in `base` too is kept: documents from before this rule stay usable.
    pub fn check_connection_names(&self, base: Option<&Document>) -> Result<()> {
        let shadows = |document: &Document, name: &str| {
            document.nodes.iter().any(|node| node.id == name)
                && document
                    .edges
                    .iter()
                    .any(|edge| edge.name.as_deref() == Some(name))
        };
        match self
            .edges
            .iter()
            .filter_map(|edge| edge.name.as_deref())
            .find(|name| shadows(self, name) && !base.is_some_and(|base| shadows(base, name)))
        {
            Some(name) => Err(invalid(format!(
                "Connection name {name:?} is already a node's name"
            ))),
            None => Ok(()),
        }
    }

    /// Why core could admit nothing `node` sends with exactly `tags`: they
    /// need a declared transition to them, and each connection from the node
    /// must admit them.
    pub fn check_output_authority(
        &self,
        node: &DocumentNode,
        tags: &[String],
    ) -> std::result::Result<(), String> {
        if let Some(tag) = tags.iter().find(|tag| !is_name(tag)) {
            return Err(format!(
                "authority tag {tag:?} must use only letters, digits, '-' or '_'"
            ));
        }
        let tags: BTreeSet<String> = tags.iter().cloned().collect();
        if !node.transitions.iter().any(|rule| rule.to == tags) {
            return Err(format!(
                "authority {tags:?} needs a declared transition to it"
            ));
        }
        match self
            .edges
            .iter()
            .find(|edge| edge.from == node.id && !edge.admits(&tags))
        {
            Some(edge) => Err(format!(
                "connection {:?} does not admit authority {tags:?}",
                edge.key()
            )),
            None => Ok(()),
        }
    }

    fn check_contract(&self, name: &str) -> Result<()> {
        if name == CONTRACT || self.contracts.contains_key(name) {
            return Ok(());
        }
        Err(invalid(format!(
            "Unknown contract {name:?}; declare it under contracts"
        )))
    }

    /// The contract `node`'s results satisfy.
    pub fn result_contract<'a>(&'a self, node: &'a DocumentNode) -> &'a str {
        node.result.as_deref().unwrap_or(CONTRACT)
    }

    /// The most authority work `node` starts may carry, if it starts work.
    pub fn ceiling(&self, node: &DocumentNode) -> Option<BTreeSet<String>> {
        node.root
            .clone()
            .or_else(|| (node.id == self.entry).then(|| BTreeSet::from([AUTHORITY.to_owned()])))
    }

    /// `node` as core declares it under identity `id`.
    pub(super) fn core_node(&self, node: &DocumentNode, binding: &Binding, id: &str) -> CoreNode {
        let tags = |tags: &BTreeSet<String>| tags.iter().cloned().collect();
        CoreNode {
            node: NodeDeclaration {
                id: id.into(),
                types: binding.types.iter().cloned().collect(),
                result_contract: self.result_contract(node).into(),
                ingress_mode: node.join,
            },
            root: self.ceiling(node).map(|ceiling| RootDeclaration {
                node_id: id.into(),
                ceiling: tags(&ceiling),
            }),
            transitions: node
                .transitions
                .iter()
                .map(|rule| AuthorityTransitionDeclaration {
                    node_id: id.into(),
                    from: tags(&rule.from),
                    to: tags(&rule.to),
                })
                .collect(),
        }
    }

    /// `edge` as core declares it, between the nodes `source` and `target`.
    /// Any nodes may connect.
    pub(super) fn core_edge(
        &self,
        edge: &DocumentEdge,
        id: &str,
        source: &str,
        target: &str,
    ) -> EdgeDeclaration {
        let contract = edge.contract.as_deref().unwrap_or_else(|| {
            self.nodes
                .iter()
                .find(|node| node.id == edge.from)
                .map_or(CONTRACT, |node| self.result_contract(node))
        });
        EdgeDeclaration {
            id: id.into(),
            source: source.into(),
            target: target.into(),
            types: vec![EDGE_TYPE.into()],
            source_requirements: vec![],
            target_requirements: vec![],
            package_contract: contract.into(),
            authority_tags: match &edge.authority {
                Some(tags) => tags.iter().cloned().collect(),
                None => vec![AUTHORITY.into()],
            },
            authority_match: edge.matching,
        }
    }

    /// The contracts a run of this document declares: its own and `payload`.
    fn contract_declarations(&self) -> Vec<ContractDeclaration> {
        let payload = ContractSpec {
            object_type: OBJECT_TYPE.into(),
            validator: ValidatorKind::Text,
        };
        std::iter::once((CONTRACT, &payload))
            .chain(
                self.contracts
                    .iter()
                    .map(|(name, spec)| (name.as_str(), spec)),
            )
            .map(|(id, spec)| ContractDeclaration {
                id: id.into(),
                object_type: spec.object_type.clone(),
                validator: spec.validator,
                validator_version: VALIDATOR_VERSION,
            })
            .collect()
    }

    /// Every authority tag the document names, and the default.
    pub fn authority_tags(&self) -> BTreeSet<String> {
        let mut tags = BTreeSet::from([AUTHORITY.to_owned()]);
        for node in &self.nodes {
            tags.extend(node.root.iter().flatten().cloned());
            for rule in &node.transitions {
                tags.extend(rule.from.iter().chain(&rule.to).cloned());
            }
        }
        for edge in &self.edges {
            tags.extend(edge.authority.iter().flatten().cloned());
        }
        tags
    }
}

/// A node as core declares it: its definition, root rule, and transitions.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct CoreNode {
    pub node: NodeDeclaration,
    pub root: Option<RootDeclaration>,
    pub transitions: Vec<AuthorityTransitionDeclaration>,
}

/// Names become core identities and directory names: letters, digits, `-`, `_`.
pub(crate) fn is_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn check_name(kind: &str, name: &str) -> Result<()> {
    if is_name(name) {
        return Ok(());
    }
    Err(invalid(format!(
        "{kind} name {name:?} must use only letters, digits, '-' or '_'"
    )))
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

impl IdentityMap {
    /// A new run's identities: the document's own names and keys.
    pub fn initial(document: &Document) -> Self {
        Self::allocate(document, str::to_owned)
    }

    /// Identities no run has used, for what an edit adds or replaces.
    pub fn fresh(document: &Document) -> Self {
        Self::allocate(document, |_| uuid::Uuid::new_v4().to_string())
    }

    fn allocate(document: &Document, mut identity: impl FnMut(&str) -> String) -> Self {
        Self {
            nodes: document
                .nodes
                .iter()
                .map(|node| (node.id.clone(), identity(&node.id)))
                .collect(),
            edges: document
                .edges
                .iter()
                .map(|edge| {
                    let key = edge.key();
                    let id = identity(&key);
                    (key, id)
                })
                .collect(),
        }
    }

    fn validate(&self, document: &Document) -> Result<()> {
        let node_names: BTreeSet<_> = document.nodes.iter().map(|node| node.id.clone()).collect();
        let edge_names: BTreeSet<_> = document.edges.iter().map(DocumentEdge::key).collect();
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

/// Expand a bound document into a new run's declaration. Each node has its
/// component's types in core. The run declares `node_types` and every type its nodes have: edits can place
/// nodes only of declared types. Its contracts and authority tags are the
/// document's, and stay fixed for the run.
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
    let contracts = document.contract_declarations();
    let mut declaration = GraphDeclaration {
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
            object_types: contracts
                .iter()
                .map(|contract| contract.object_type.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            authority_tags: document.authority_tags().into_iter().collect(),
        },
        contracts,
        nodes: vec![],
        edges: vec![],
        roots: vec![],
        authority_transitions: vec![],
    };
    for node in &document.nodes {
        let id = &identities.nodes[&node.id];
        let core = document.core_node(node, &bindings[&node.id], id);
        declaration.nodes.push(core.node);
        declaration.roots.extend(core.root);
        declaration.authority_transitions.extend(core.transitions);
    }
    for edge in &document.edges {
        declaration.edges.push(document.core_edge(
            edge,
            &identities.edges[&edge.key()],
            &identities.nodes[&edge.from],
            &identities.nodes[&edge.to],
        ));
    }
    Ok(declaration)
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
    fn each_node_has_its_component_types_and_binding() {
        let mut document = document();
        let identities = IdentityMap::fresh(&document);
        let mut fingerprints = BTreeSet::new();
        for (component, config, node_type, implementation) in [
            ("agent", json!({"prompt":"Different"}), "Agent", "codex"),
            ("claude", json!({"prompt":"Different"}), "Agent", "claude"),
            ("command", json!({"argv":["false"]}), "Command", "command"),
            ("human", json!({"prompt":"Approve?"}), "Human", "human"),
            ("inbox", json!({}), "Inbox", "inbox"),
            ("external", json!({}), "External", "external"),
        ] {
            document.nodes[1].component = component.into();
            document.nodes[1].config = config;
            let declaration = expand_builtin(&document, "example", &identities).unwrap();
            assert_eq!(
                declaration.schema.node_types,
                ["Agent", "Command", "External", "Human", "Inbox"]
            );
            let node = declaration
                .nodes
                .iter()
                .find(|node| node.id == identities.nodes["write"])
                .unwrap();
            assert_eq!(node.types, [node_type]);
            assert_eq!(
                bind(&document).unwrap()["write"].implementation.kind(),
                implementation
            );
            let compiled = declaration.compile().unwrap();
            fingerprints.insert(compiled.fingerprint().to_string());
        }
        // Core records the role: agents share one graph, other roles differ.
        assert_eq!(fingerprints.len(), 5);
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
        // A connection named like a node would shadow it in node tools' `to`.
        // Only a new document is refused: one that already did still loads.
        for shadowed in ["write", "test"] {
            bad = original.clone();
            bad.edges[0].name = Some(shadowed.into());
            let refused = bad.check_connection_names(None).unwrap_err();
            assert!(
                refused.message.contains("already a node's name"),
                "{refused:?}"
            );
            assert!(bad.check_connection_names(Some(&original)).is_err());
            assert_eq!(bad.canonicalized().unwrap(), bad);
            assert!(bad.check_connection_names(Some(&bad)).is_ok());
        }
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
    }

    #[test]
    fn examples_are_documents_whose_names_are_core_identities() {
        for text in [
            include_str!("../../examples/flow.json"),
            include_str!("../../examples/fifteen-node-chain.json"),
        ] {
            let document = Document::parse(text).unwrap();
            let identities = IdentityMap::initial(&document);
            let declaration = expand_builtin(&document, &document.name, &identities).unwrap();
            declaration.compile().unwrap();
            assert!(
                declaration
                    .nodes
                    .iter()
                    .all(|node| identities.nodes[&node.id] == node.id)
            );
        }
    }

    #[test]
    fn typing_is_checked_in_document_terms() {
        let parse = |extra: Value| {
            let mut value = json!({"name":"typed","entry":"a",
                "nodes":[{"id":"a","component":"inbox"},{"id":"b","component":"inbox"}]});
            value
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            Document::parse(&value.to_string())
        };
        for (extra, message) in [
            (
                json!({"nodes":[{"id":"a:b","component":"inbox"}]}),
                "letters",
            ),
            (
                json!({"edges":[{"from":"a","to":"b"},{"from":"a","to":"b"}]}),
                "need names",
            ),
            (
                json!({"edges":[{"from":"a","to":"b","contract":"ore"}]}),
                "Unknown contract",
            ),
            (
                json!({"edges":[{"from":"a","to":"b","authority":[]}]}),
                "at least one",
            ),
            (
                json!({"contracts":{"payload":{"object_type":"Ore","validator":"text"}}}),
                "built in",
            ),
        ] {
            let error = parse(extra.clone()).unwrap_err();
            assert!(
                error.message.contains(message),
                "{extra}: {}",
                error.message
            );
        }
        let document = parse(json!({
            "contracts":{"ore":{"object_type":"Ore","validator":"bytes"}},
            "edges":[{"from":"a","to":"b"},{"from":"a","to":"b","name":"b2","contract":"ore","authority":["red"],"match":"all_of"}]
        }))
        .unwrap();
        let declaration =
            expand_builtin(&document, "typed", &IdentityMap::initial(&document)).unwrap();
        assert_eq!(declaration.schema.object_types, ["Ore", OBJECT_TYPE]);
        assert_eq!(declaration.schema.authority_tags, ["red", AUTHORITY]);
        let edge = |id: &str| declaration.edges.iter().find(|edge| edge.id == id).unwrap();
        assert_eq!(edge("a:b").package_contract, CONTRACT);
        assert_eq!(edge("a:b").authority_tags, [AUTHORITY]);
        assert_eq!(edge("b2").package_contract, "ore");
        // Only the entry starts work unless another node declares a root.
        assert_eq!(declaration.roots.len(), 1);
        declaration.compile().unwrap();
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
