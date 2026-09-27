//! A fixed vocabulary of graph edits, independent of worker kinds.

use super::document::{AUTHORITY, CONTRACT, EDGE_TYPE, JoinMode, NODE_TYPE};
use crate::declarations::{
    AuthorityMatchDeclaration, EdgeDeclaration, GraphFragmentDeclaration, NodeDeclaration,
    RewriteProductionDeclaration, RootDeclaration,
};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Variant {
    pub join: JoinMode,
    pub root: bool,
}

impl Variant {
    fn name(self) -> String {
        format!(
            "{}_{}",
            match self.join {
                JoinMode::Any => "any",
                JoinMode::All => "all",
            },
            if self.root { "root" } else { "node" }
        )
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

pub(crate) fn node(id: &str, join: JoinMode) -> NodeDeclaration {
    NodeDeclaration {
        id: id.into(),
        types: vec![NODE_TYPE.into()],
        result_contract: CONTRACT.into(),
        ingress_mode: join.into(),
    }
}

pub(crate) fn root(id: &str) -> RootDeclaration {
    RootDeclaration {
        node_id: id.into(),
        ceiling: vec![AUTHORITY.into()],
    }
}

pub(crate) fn edge(id: &str, from: &str, to: &str) -> EdgeDeclaration {
    EdgeDeclaration {
        id: id.into(),
        source: from.into(),
        target: to.into(),
        types: vec![EDGE_TYPE.into()],
        source_requirements: vec![NODE_TYPE.into()],
        target_requirements: vec![NODE_TYPE.into()],
        package_contract: CONTRACT.into(),
        authority_tags: vec![AUTHORITY.into()],
        authority_match: AuthorityMatchDeclaration::AnyOf,
    }
}

fn fragment(nodes: &[(&str, Variant)]) -> GraphFragmentDeclaration {
    GraphFragmentDeclaration {
        nodes: nodes
            .iter()
            .map(|(id, variant)| node(id, variant.join))
            .collect(),
        roots: nodes
            .iter()
            .filter(|(_, variant)| variant.root)
            .map(|(id, _)| root(id))
            .collect(),
        ..Default::default()
    }
}

/// Four ingress/root variants; a document has one root, so distinct roots cannot
/// be connected. Eight node rules + 24 distinct-endpoint rules + eight loop rules.
pub fn universal() -> Vec<RewriteProductionDeclaration> {
    let variants: Vec<_> = [JoinMode::Any, JoinMode::All]
        .into_iter()
        .flat_map(|join| {
            [false, true]
                .into_iter()
                .map(move |root| Variant { join, root })
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
                        edge("e", "n", "n")
                    } else {
                        edge("e", "a", "b")
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
    use crate::declarations::RewriteRequestDeclaration;
    use crate::workflow::document::{Document, IdentityMap, expand};
    use std::{collections::BTreeMap, sync::Arc};

    #[test]
    fn generated_rules_handle_every_allowed_variant_and_self_loop() {
        assert_eq!(universal().len(), 40);
        let document = Document::parse(
            r#"{"name":"grammar","entry":"entry","nodes":[{"id":"entry","kind":"inbox"}]}"#,
        )
        .unwrap();
        let identities = IdentityMap::fresh(&document);
        let compiled = expand(&document, "grammar", &identities)
            .unwrap()
            .compile()
            .unwrap();
        for rule in universal() {
            // Start with this rule's exact left fragment, which admits every
            // combination without relying on a particular example workflow.
            let mut declaration = expand(&document, "grammar", &identities).unwrap();
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
