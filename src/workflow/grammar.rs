//! A fixed vocabulary of graph edits, one rule for each node variant.
//!
//! Core matches a rule's nodes exactly, including their node types, so every
//! combination of type, join, and root status needs its own rules. Runs created
//! before node types keep their original rules and names.

use super::NodeType;
use super::document::{AUTHORITY, CONTRACT, EDGE_TYPE, SHARED_NODE_TYPE};
use crate::declarations::{
    AuthorityMatchDeclaration, EdgeDeclaration, GraphFragmentDeclaration, IngressDeclaration,
    NodeDeclaration, RewriteProductionDeclaration, RootDeclaration,
};
use ontography::Kernel;
use std::collections::BTreeSet;

/// How a run's nodes are typed in core.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Typing {
    /// Runs created before node types: every node has the one shared type.
    Shared,
    /// Each node's core type is its role.
    Roles,
}

impl Typing {
    /// Read from core: a run's schema declares the types its nodes can have.
    pub fn of(kernel: &Kernel) -> Self {
        if kernel
            .schema()
            .node_types()
            .any(|name| name == SHARED_NODE_TYPE)
        {
            Self::Shared
        } else {
            Self::Roles
        }
    }

    /// The node types this typing declares in core's schema.
    pub fn node_types(self) -> Vec<&'static str> {
        match self {
            Self::Shared => vec![SHARED_NODE_TYPE],
            Self::Roles => NodeType::ALL.iter().map(|kind| kind.as_str()).collect(),
        }
    }

    /// The core type of a node with this role.
    pub fn core_type(self, node_type: NodeType) -> &'static str {
        match self {
            Self::Shared => SHARED_NODE_TYPE,
            Self::Roles => node_type.as_str(),
        }
    }

    /// The role part of a variant: shared runs don't distinguish roles in core.
    pub fn role(self, node_type: NodeType) -> Option<NodeType> {
        match self {
            Self::Shared => None,
            Self::Roles => Some(node_type),
        }
    }

    fn roles(self) -> Vec<Option<NodeType>> {
        match self {
            Self::Shared => vec![None],
            Self::Roles => NodeType::ALL.into_iter().map(Some).collect(),
        }
    }
}

/// A node as core rules see it. `role` is `None` for the shared type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Variant {
    pub role: Option<NodeType>,
    pub join: IngressDeclaration,
    pub root: bool,
}

impl Variant {
    fn core_type(self) -> &'static str {
        self.role.map_or(SHARED_NODE_TYPE, NodeType::as_str)
    }

    fn name(self) -> String {
        let placement = format!(
            "{}_{}",
            match self.join {
                IngressDeclaration::Any => "any",
                IngressDeclaration::All => "all",
            },
            if self.root { "root" } else { "node" }
        );
        match self.role {
            None => placement,
            Some(role) => format!("{}_{placement}", role.as_str().to_ascii_lowercase()),
        }
    }
}

pub fn node_rule(add: bool, variant: Variant) -> String {
    format!(
        "{}_node_{}",
        if add { "add" } else { "remove" },
        variant.name()
    )
}

pub fn edge_rule(add: bool, source: Variant, target: Variant, self_loop: bool) -> String {
    let action = if add { "add" } else { "remove" };
    if self_loop {
        format!("{action}_loop_{}", source.name())
    } else {
        format!("{action}_edge_{}_{}", source.name(), target.name())
    }
}

pub(crate) fn node(id: &str, core_type: &str, join: IngressDeclaration) -> NodeDeclaration {
    NodeDeclaration {
        id: id.into(),
        types: vec![core_type.into()],
        result_contract: CONTRACT.into(),
        ingress_mode: join,
    }
}

pub(crate) fn root(id: &str) -> RootDeclaration {
    RootDeclaration {
        node_id: id.into(),
        ceiling: vec![AUTHORITY.into()],
    }
}

/// A connection may join nodes of any roles; shared runs require their one type.
pub(crate) fn edge(id: &str, from: &str, to: &str, typing: Typing) -> EdgeDeclaration {
    let requirements = match typing {
        Typing::Shared => vec![SHARED_NODE_TYPE.into()],
        Typing::Roles => vec![],
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

fn fragment(nodes: &[(&str, Variant)]) -> GraphFragmentDeclaration {
    GraphFragmentDeclaration {
        nodes: nodes
            .iter()
            .map(|(id, variant)| node(id, variant.core_type(), variant.join))
            .collect(),
        roots: nodes
            .iter()
            .filter(|(_, variant)| variant.root)
            .map(|(id, _)| root(id))
            .collect(),
        ..Default::default()
    }
}

/// Every role/ingress/root variant; a document has one root, so distinct roots
/// cannot be connected. With V variants there are 2V node rules, 2V loop rules,
/// and 2(V² − R²) distinct-endpoint rules for R root variants: 40 rules for the
/// shared type and 448 with four roles.
pub fn productions(typing: Typing) -> Vec<RewriteProductionDeclaration> {
    let variants: Vec<_> = typing
        .roles()
        .into_iter()
        .flat_map(|role| {
            [IngressDeclaration::Any, IngressDeclaration::All]
                .into_iter()
                .flat_map(move |join| {
                    [false, true]
                        .into_iter()
                        .map(move |root| Variant { role, join, root })
                })
        })
        .collect();
    let mut rules = Vec::new();
    for source in &variants {
        for add in [true, false] {
            let one = fragment(&[("n", *source)]);
            rules.push(RewriteProductionDeclaration {
                id: node_rule(add, *source),
                left: if add {
                    GraphFragmentDeclaration::default()
                } else {
                    one.clone()
                },
                right: if add {
                    one
                } else {
                    GraphFragmentDeclaration::default()
                },
                interface_nodes: BTreeSet::new(),
                interface_edges: BTreeSet::new(),
            });
            for self_loop in [false, true] {
                for target in &variants {
                    if (self_loop && target != source) || (!self_loop && source.root && target.root)
                    {
                        continue;
                    }
                    let mut plain = if self_loop {
                        fragment(&[("n", *source)])
                    } else {
                        fragment(&[("a", *source), ("b", *target)])
                    };
                    let interface_nodes = plain.nodes.iter().map(|node| node.id.clone()).collect();
                    let mut connected = plain.clone();
                    connected.edges.push(if self_loop {
                        edge("e", "n", "n", typing)
                    } else {
                        edge("e", "a", "b", typing)
                    });
                    if !add {
                        std::mem::swap(&mut plain, &mut connected);
                    }
                    rules.push(RewriteProductionDeclaration {
                        id: edge_rule(add, *source, *target, self_loop),
                        left: plain,
                        right: connected,
                        interface_nodes,
                        interface_edges: BTreeSet::new(),
                    });
                }
            }
        }
    }
    rules
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::declarations::{GraphDeclaration, RewriteRequestDeclaration};
    use crate::workflow::{
        components::Catalog,
        document::{Document, IdentityMap, expand},
    };
    use std::{collections::BTreeMap, sync::Arc};

    fn declaration(typing: Typing) -> GraphDeclaration {
        let document = Document::parse(
            r#"{"name":"grammar","entry":"entry","nodes":[{"id":"entry","component":"inbox"}]}"#,
        )
        .unwrap();
        let bindings = Catalog::builtin().bind(&document).unwrap();
        let mut declaration = expand(
            &document,
            &bindings,
            "grammar",
            &IdentityMap::fresh(&document),
        )
        .unwrap();
        declaration.schema.node_types = typing.node_types().into_iter().map(Into::into).collect();
        for node in &mut declaration.nodes {
            node.types = vec![typing.core_type(NodeType::Inbox).into()];
        }
        declaration.rewrites = productions(typing);
        declaration
    }

    #[test]
    fn shared_runs_keep_their_rules_and_roles_get_one_rule_per_variant() {
        let shared: Vec<_> = productions(Typing::Shared)
            .into_iter()
            .map(|rule| rule.id)
            .collect();
        assert_eq!(shared.len(), 40);
        for id in [
            "add_node_any_root",
            "add_edge_any_node_all_root",
            "remove_loop_all_node",
        ] {
            assert!(shared.iter().any(|rule| rule == id), "{id}");
        }
        let roles: Vec<_> = productions(Typing::Roles)
            .into_iter()
            .map(|rule| rule.id)
            .collect();
        assert_eq!(roles.len(), 448);
        assert_eq!(roles.iter().collect::<BTreeSet<_>>().len(), roles.len());
        for id in [
            "add_node_agent_any_root",
            "add_edge_agent_any_node_inbox_all_root",
        ] {
            assert!(roles.iter().any(|rule| rule == id), "{id}");
        }
    }

    #[test]
    fn generated_rules_handle_every_allowed_variant_and_self_loop() {
        for typing in [Typing::Shared, Typing::Roles] {
            check_rules(typing);
        }
    }

    fn check_rules(typing: Typing) {
        let mut base = declaration(typing);
        let compiled = base.compile().unwrap();
        base.rewrites.clear();
        for rule in productions(typing) {
            // Start with this rule's exact left fragment, which admits every
            // combination without relying on a particular example workflow.
            let mut declaration = base.clone();
            declaration.nodes = rule.left.nodes.clone();
            declaration.edges = rule.left.edges.clone();
            declaration.roots = rule.left.roots.clone();
            let left = declaration.compile().unwrap();
            let mut state = left.kernel.empty_state();
            let mut request = RewriteRequestDeclaration {
                production_id: rule.id.clone(),
                nodes: BTreeMap::new(),
                edges: BTreeMap::new(),
                fresh_nodes: BTreeMap::new(),
                fresh_edges: BTreeMap::new(),
            };
            for node in &rule.left.nodes {
                request.nodes.insert(node.id.clone(), node.id.clone());
            }
            for edge in &rule.left.edges {
                request.edges.insert(edge.id.clone(), edge.id.clone());
            }
            for node in &rule.right.nodes {
                if !rule.interface_nodes.contains(&node.id) {
                    request
                        .fresh_nodes
                        .insert(node.id.clone(), format!("fresh-{}", node.id));
                }
            }
            for edge in &rule.right.edges {
                if !rule.interface_edges.contains(&edge.id) {
                    request
                        .fresh_edges
                        .insert(edge.id.clone(), format!("fresh-{}", edge.id));
                }
            }
            let plan = left
                .kernel
                .prepare_rewrite(
                    &state,
                    &compiled.grammar,
                    &request.compile(),
                    &BTreeMap::new(),
                )
                .unwrap_or_else(|error| panic!("{}: {error}", rule.id));
            let after: Arc<_> = left.kernel.commit_rewrite(&mut state, plan).unwrap();
            assert_eq!(
                after.graph().nodes().len(),
                rule.right.nodes.len(),
                "{}",
                rule.id
            );
            assert_eq!(
                after.graph().edges().len(),
                rule.right.edges.len(),
                "{}",
                rule.id
            );
            if rule.id.starts_with("add_loop") {
                let edge = &after.graph().edges()[0];
                assert_eq!(edge.source(), edge.target());
            }
        }
    }
}
