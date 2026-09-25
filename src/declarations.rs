//! Serializable logical graph declarations compiled through public core APIs.
//!
//! This format does not imply executable node implementations. Contract kinds
//! select the small trusted validator catalog below; they are not executable
//! schemas supplied by a client.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use ontography::{
    Authority, AuthorityMatch, AuthorityTag, Contract, ContractViolation, DefinitionError,
    DefinitionId, Edge, EdgeDefinition, Graph, IngressMode, Kernel, Node, NodeDefinition,
    RewriteError, RewriteFragment, RewriteGrammar, RewriteMatch, RewriteProduction, RewriteRequest,
    RootRule, Schema,
};
use serde::de::{DeserializeOwned, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const DECLARATION_VERSION: u32 = 1;
pub const VALIDATOR_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum DeclarationError {
    #[error("invalid declaration JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsupported graph declaration version {0}; expected 1")]
    UnsupportedVersion(u32),
    #[error("contract {contract_id:?} uses unsupported validator version {version}; expected 1")]
    UnsupportedValidatorVersion { contract_id: String, version: u32 },
    #[error("{scope}: {source}")]
    Definition {
        scope: String,
        #[source]
        source: DefinitionError,
    },
    #[error("rewrite {production_id:?}: {source}")]
    Rewrite {
        production_id: String,
        #[source]
        source: RewriteError,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GraphDeclaration {
    pub version: u32,
    pub id: String,
    pub schema: SchemaDeclaration,
    pub contracts: Vec<ContractDeclaration>,
    pub nodes: Vec<NodeDeclaration>,
    #[serde(default)]
    pub edges: Vec<EdgeDeclaration>,
    #[serde(default)]
    pub roots: Vec<RootDeclaration>,
    #[serde(default)]
    pub authority_transitions: Vec<AuthorityTransitionDeclaration>,
    #[serde(default)]
    pub rewrites: Vec<RewriteProductionDeclaration>,
    #[serde(default)]
    pub execution_bindings: Vec<crate::registry::ExecutionBinding>,
}

#[derive(Clone, Debug)]
pub struct CompiledGraph {
    pub kernel: Arc<Kernel>,
    pub grammar: RewriteGrammar,
}

impl GraphDeclaration {
    /// Parse without silently accepting duplicate keys, including nested maps.
    pub fn parse(document: &str) -> Result<Self, DeclarationError> {
        parse_json(document)
    }

    /// Construct the actual core schema, contracts, graph, and fixed grammar.
    /// Both production fragments are admitted against the same schema and
    /// contracts. A concrete rewrite match is checked later by the session.
    pub fn compile(&self) -> Result<CompiledGraph, DeclarationError> {
        if self.version != DECLARATION_VERSION {
            return Err(DeclarationError::UnsupportedVersion(self.version));
        }
        let id = in_scope("id", DefinitionId::new(self.id.as_str()))?;
        let schema = self.schema.compile()?;
        let contracts = self
            .contracts
            .iter()
            .map(ContractDeclaration::compile)
            .collect::<Result<Vec<_>, _>>()?;
        let kernel = FragmentRef {
            nodes: &self.nodes,
            edges: &self.edges,
            roots: &self.roots,
            authority_transitions: &self.authority_transitions,
        }
        .admit(&id, &schema, &contracts, "graph")?;
        let productions = self
            .rewrites
            .iter()
            .map(|production| production.compile(&id, &schema, &contracts))
            .collect::<Result<Vec<_>, _>>()?;
        let grammar =
            RewriteGrammar::new(productions).map_err(|source| DeclarationError::Rewrite {
                production_id: "grammar".into(),
                source,
            })?;
        Ok(CompiledGraph {
            kernel: Arc::new(kernel),
            grammar,
        })
    }

    /// SHA-256 of the typed declaration, including validator versions and grammar.
    /// JSON whitespace and object-key ordering do not affect this identity;
    /// declared list ordering does. This is not the core kernel fingerprint.
    pub fn fingerprint(&self) -> Result<String, DeclarationError> {
        let bytes = serde_json::to_vec(self)?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SchemaDeclaration {
    pub node_types: Vec<String>,
    pub object_types: Vec<String>,
    #[serde(default)]
    pub authority_tags: Vec<String>,
}

impl SchemaDeclaration {
    fn compile(&self) -> Result<Schema, DeclarationError> {
        let tags = authority_tags(&self.authority_tags, "schema.authority_tags")?;
        in_scope(
            "schema",
            Schema::new(
                self.node_types.iter().map(String::as_str),
                self.object_types.iter().map(String::as_str),
                tags,
            ),
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ValidatorKind {
    /// Accept every byte sequence, including empty and non-UTF-8 payloads.
    OpaqueBytes,
    /// Accept precisely byte sequences that are valid UTF-8, including empty text.
    Utf8,
}

#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ContractDeclaration {
    pub id: String,
    pub object_type: String,
    pub validator: ValidatorKind,
    pub validator_version: u32,
}

impl ContractDeclaration {
    fn compile(&self) -> Result<Contract, DeclarationError> {
        if self.validator_version != VALIDATOR_VERSION {
            return Err(DeclarationError::UnsupportedValidatorVersion {
                contract_id: self.id.clone(),
                version: self.validator_version,
            });
        }
        let validator = self.validator;
        in_scope(
            &format!("contract {:?}", self.id),
            Contract::new(
                self.id.as_str(),
                self.object_type.as_str(),
                move |bytes| match validator {
                    ValidatorKind::OpaqueBytes => Ok(()),
                    ValidatorKind::Utf8 => std::str::from_utf8(bytes)
                        .map(|_| ())
                        .map_err(|error| ContractViolation::new(format!("invalid UTF-8: {error}"))),
                },
            ),
        )
    }
}

#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum IngressDeclaration {
    #[default]
    Any,
    All,
}

impl From<IngressDeclaration> for IngressMode {
    fn from(value: IngressDeclaration) -> Self {
        match value {
            IngressDeclaration::Any => Self::Any,
            IngressDeclaration::All => Self::All,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NodeDeclaration {
    pub id: String,
    pub types: Vec<String>,
    pub result_contract: String,
    #[serde(default)]
    pub ingress_mode: IngressDeclaration,
}

#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityMatchDeclaration {
    #[default]
    AnyOf,
    AllOf,
}

impl From<AuthorityMatchDeclaration> for AuthorityMatch {
    fn from(value: AuthorityMatchDeclaration) -> Self {
        match value {
            AuthorityMatchDeclaration::AnyOf => Self::AnyOf,
            AuthorityMatchDeclaration::AllOf => Self::AllOf,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EdgeDeclaration {
    pub id: String,
    pub source: String,
    pub target: String,
    pub types: Vec<String>,
    #[serde(default)]
    pub source_requirements: Vec<String>,
    #[serde(default)]
    pub target_requirements: Vec<String>,
    pub package_contract: String,
    pub authority_tags: Vec<String>,
    #[serde(default)]
    pub authority_match: AuthorityMatchDeclaration,
}

#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RootDeclaration {
    pub node_id: String,
    pub ceiling: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorityTransitionDeclaration {
    pub node_id: String,
    pub from: Vec<String>,
    pub to: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GraphFragmentDeclaration {
    #[serde(default)]
    pub nodes: Vec<NodeDeclaration>,
    #[serde(default)]
    pub edges: Vec<EdgeDeclaration>,
    #[serde(default)]
    pub roots: Vec<RootDeclaration>,
    #[serde(default)]
    pub authority_transitions: Vec<AuthorityTransitionDeclaration>,
}

impl GraphFragmentDeclaration {
    fn as_fragment(&self) -> FragmentRef<'_> {
        FragmentRef {
            nodes: &self.nodes,
            edges: &self.edges,
            roots: &self.roots,
            authority_transitions: &self.authority_transitions,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RewriteProductionDeclaration {
    pub id: String,
    pub left: GraphFragmentDeclaration,
    #[serde(default)]
    pub interface_nodes: BTreeSet<String>,
    #[serde(default)]
    pub interface_edges: BTreeSet<String>,
    pub right: GraphFragmentDeclaration,
}

impl RewriteProductionDeclaration {
    pub(crate) fn compile(
        &self,
        definition_id: &DefinitionId,
        schema: &Schema,
        contracts: &[Contract],
    ) -> Result<RewriteProduction, DeclarationError> {
        let left = self.left.as_fragment().admit(
            definition_id,
            schema,
            contracts,
            &format!("rewrite {:?}.left", self.id),
        )?;
        let right = self.right.as_fragment().admit(
            definition_id,
            schema,
            contracts,
            &format!("rewrite {:?}.right", self.id),
        )?;
        RewriteProduction::new(
            self.id.as_str(),
            RewriteFragment::from_kernel(&left),
            self.interface_nodes
                .iter()
                .map(|value| Arc::from(value.as_str()))
                .collect(),
            self.interface_edges
                .iter()
                .map(|value| Arc::from(value.as_str()))
                .collect(),
            RewriteFragment::from_kernel(&right),
        )
        .map_err(|source| DeclarationError::Rewrite {
            production_id: self.id.clone(),
            source,
        })
    }
}

/// Explicit rule-symbol bindings. Core validates coverage, injectivity, and
/// freshness against the selected production and current run when preparing.
#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RewriteRequestDeclaration {
    pub production_id: String,
    #[serde(default)]
    pub nodes: BTreeMap<String, String>,
    #[serde(default)]
    pub edges: BTreeMap<String, String>,
    #[serde(default)]
    pub fresh_nodes: BTreeMap<String, String>,
    #[serde(default)]
    pub fresh_edges: BTreeMap<String, String>,
}

impl RewriteRequestDeclaration {
    pub fn parse(document: &str) -> Result<Self, DeclarationError> {
        parse_json(document)
    }

    pub fn compile(&self) -> RewriteRequest {
        fn bindings(values: &BTreeMap<String, String>) -> BTreeMap<Arc<str>, Arc<str>> {
            values
                .iter()
                .map(|(symbol, identity)| {
                    (Arc::from(symbol.as_str()), Arc::from(identity.as_str()))
                })
                .collect()
        }
        RewriteRequest::new(
            self.production_id.as_str(),
            RewriteMatch::new(
                bindings(&self.nodes),
                bindings(&self.edges),
                bindings(&self.fresh_nodes),
                bindings(&self.fresh_edges),
            ),
        )
    }
}

struct FragmentRef<'a> {
    nodes: &'a [NodeDeclaration],
    edges: &'a [EdgeDeclaration],
    roots: &'a [RootDeclaration],
    authority_transitions: &'a [AuthorityTransitionDeclaration],
}

impl FragmentRef<'_> {
    fn admit(
        &self,
        id: &DefinitionId,
        schema: &Schema,
        contracts: &[Contract],
        scope: &str,
    ) -> Result<Kernel, DeclarationError> {
        let topology_nodes = self
            .nodes
            .iter()
            .map(|node| in_scope(scope, Node::new(node.id.as_str())))
            .collect::<Result<Vec<_>, _>>()?;
        let topology_edges = self
            .edges
            .iter()
            .map(|edge| {
                in_scope(
                    scope,
                    Edge::new(edge.id.as_str(), edge.source.as_str(), edge.target.as_str()),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let graph = in_scope(scope, Graph::new(topology_nodes, topology_edges))?;
        let nodes = self
            .nodes
            .iter()
            .map(|node| {
                in_scope(
                    scope,
                    NodeDefinition::new(
                        node.id.as_str(),
                        node.types.iter().map(String::as_str),
                        node.result_contract.as_str(),
                    ),
                )
                .map(|definition| definition.with_ingress_mode(node.ingress_mode.into()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let edges = self
            .edges
            .iter()
            .map(|edge| {
                let tags = authority_tags(&edge.authority_tags, scope)?;
                in_scope(
                    scope,
                    EdgeDefinition::new(
                        edge.id.as_str(),
                        edge.types.iter().map(String::as_str),
                        edge.source_requirements.iter().map(String::as_str),
                        edge.target_requirements.iter().map(String::as_str),
                        edge.package_contract.as_str(),
                        tags,
                    ),
                )
                .map(|definition| definition.with_authority_match(edge.authority_match.into()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let roots = self
            .roots
            .iter()
            .map(|root| {
                let ceiling = Authority::new(authority_tags(&root.ceiling, scope)?);
                in_scope(scope, RootRule::new(root.node_id.as_str(), ceiling))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let transitions = self
            .authority_transitions
            .iter()
            .map(|transition| {
                let from = Authority::new(authority_tags(&transition.from, scope)?);
                let to = Authority::new(authority_tags(&transition.to, scope)?);
                in_scope(
                    scope,
                    ontography::AuthorityTransitionRule::new(transition.node_id.as_str(), from, to),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        in_scope(
            scope,
            Kernel::admit(
                id.clone(),
                schema.clone(),
                graph,
                contracts.iter().cloned(),
                nodes,
                edges,
                transitions,
                roots,
            ),
        )
    }
}

fn authority_tags(values: &[String], scope: &str) -> Result<Vec<AuthorityTag>, DeclarationError> {
    values
        .iter()
        .map(|value| in_scope(scope, AuthorityTag::new(value.as_str())))
        .collect()
}

fn in_scope<T>(scope: &str, result: Result<T, DefinitionError>) -> Result<T, DeclarationError> {
    result.map_err(|source| DeclarationError::Definition {
        scope: scope.into(),
        source,
    })
}

/// Validate unique keys before deserializing a typed request. Call this on the
/// original JSON text: converting through `Value` first would lose duplicates.
pub fn parse_json<T: DeserializeOwned>(document: &str) -> Result<T, DeclarationError> {
    let _: UniqueJson = serde_json::from_str(document)?;
    Ok(serde_json::from_str(document)?)
}

struct UniqueJson;

impl<'de> Deserialize<'de> for UniqueJson {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(UniqueJsonVisitor)
    }
}

struct UniqueJsonVisitor;

impl<'de> Visitor<'de> for UniqueJsonVisitor {
    type Value = UniqueJson;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JSON with unique object keys")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut keys = BTreeSet::new();
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key.clone()) {
                return Err(serde::de::Error::custom(format!(
                    "duplicate JSON key {key:?}"
                )));
            }
            let _ = map.next_value::<UniqueJson>()?;
        }
        Ok(UniqueJson)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut values: A) -> Result<Self::Value, A::Error> {
        while values.next_element::<UniqueJson>()?.is_some() {}
        Ok(UniqueJson)
    }

    fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<Self::Value, E> {
        Ok(UniqueJson)
    }
    fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<Self::Value, E> {
        Ok(UniqueJson)
    }
    fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<Self::Value, E> {
        Ok(UniqueJson)
    }
    fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<Self::Value, E> {
        Ok(UniqueJson)
    }
    fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<Self::Value, E> {
        Ok(UniqueJson)
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(UniqueJson)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ontography::{ActivationProposal, Emission, OutputAuthority, PackageId, RetirementReason};

    const FLOW: &str = include_str!("../examples/flow.json");
    const REMOVE_RECEIVER: &str = include_str!("../examples/remove-receiver.json");

    #[test]
    fn example_delivers_and_rewrites_through_core() {
        let compiled = GraphDeclaration::parse(FLOW).unwrap().compile().unwrap();
        let mut state = compiled.kernel.empty_state();
        let mut proposal = ActivationProposal::root(
            "A",
            Authority::new([AuthorityTag::new("work").unwrap()]),
            Arc::from(&b"sent"[..]),
        );
        proposal.emit(Emission::new(
            "A_to_B",
            OutputAuthority::Carry,
            Arc::from(&b"hello"[..]),
        ));
        let accepted = compiled.kernel.activate(&mut state, proposal).unwrap();
        let package = PackageId::from_parts(accepted, 0);
        assert_eq!(state.position(package).unwrap().holder(), "B");

        let request = RewriteRequestDeclaration::parse(REMOVE_RECEIVER)
            .unwrap()
            .compile();
        let plan = compiled
            .kernel
            .prepare_rewrite(&state, &compiled.grammar, &request, &BTreeMap::new())
            .unwrap();
        assert_eq!(
            plan.retirements().get(&package),
            Some(&RetirementReason::HolderRemoved)
        );
        assert!(
            state.position(package).is_some(),
            "preparing does not mutate state"
        );
        let next = compiled.kernel.commit_rewrite(&mut state, plan).unwrap();
        assert!(next.graph().node("B").is_none());
        assert!(next.graph().edge("A_to_B").is_none());
        assert!(state.position(package).is_none());
        assert!(
            state.activation(accepted).is_some(),
            "history survives retirement"
        );
    }

    #[test]
    fn invalid_references_and_versions_are_rejected() {
        let mut declaration = GraphDeclaration::parse(FLOW).unwrap();
        declaration.edges[0].target = "absent".into();
        assert!(matches!(
            declaration.compile(),
            Err(DeclarationError::Definition { .. })
        ));
        declaration = GraphDeclaration::parse(FLOW).unwrap();
        declaration.rewrites[0].right.nodes[0].result_contract = "absent".into();
        let error = declaration.compile().unwrap_err().to_string();
        assert!(
            error.contains("right") && error.contains("absent"),
            "{error}"
        );
        declaration = GraphDeclaration::parse(FLOW).unwrap();
        declaration.version = 2;
        assert!(matches!(
            declaration.compile(),
            Err(DeclarationError::UnsupportedVersion(2))
        ));
        declaration.version = 1;
        declaration.contracts[0].validator_version = 2;
        assert!(matches!(
            declaration.compile(),
            Err(DeclarationError::UnsupportedValidatorVersion { .. })
        ));
    }

    #[test]
    fn validator_kind_changes_real_admission() {
        let mut declaration = GraphDeclaration::parse(FLOW).unwrap();
        let compiled = declaration.compile().unwrap();
        let mut state = compiled.kernel.empty_state();
        let non_utf8 = ActivationProposal::root("A", Authority::new([]), Arc::from(&[0xff][..]));
        assert!(
            compiled
                .kernel
                .activate(&mut state, non_utf8.clone())
                .is_err()
        );
        assert!(state.activations().is_empty());
        declaration.contracts[0].validator = ValidatorKind::OpaqueBytes;
        let compiled = declaration.compile().unwrap();
        let mut state = compiled.kernel.empty_state();
        assert!(compiled.kernel.activate(&mut state, non_utf8).is_ok());
    }

    #[test]
    fn duplicate_keys_and_unknown_fields_are_rejected() {
        let duplicate = r#"{"production_id":"remove_receiver","nodes":{"A":"A","A":"B"}}"#;
        assert!(
            RewriteRequestDeclaration::parse(duplicate)
                .unwrap_err()
                .to_string()
                .contains("duplicate JSON key")
        );
        let unknown = FLOW.replacen(
            "\"version\": 1",
            "\"version\": 1, \"implementation\": \"codex\"",
            1,
        );
        assert!(GraphDeclaration::parse(&unknown).is_err());
    }

    #[test]
    fn declaration_fingerprint_ignores_json_formatting_and_includes_grammar() {
        let declaration = GraphDeclaration::parse(FLOW).unwrap();
        let compact = serde_json::to_string(&declaration).unwrap();
        let fingerprint = declaration.fingerprint().unwrap();
        assert_eq!(fingerprint.len(), 64);
        assert_eq!(
            fingerprint,
            GraphDeclaration::parse(&compact)
                .unwrap()
                .fingerprint()
                .unwrap()
        );
        let mut changed = declaration.clone();
        changed.rewrites.clear();
        assert_ne!(fingerprint, changed.fingerprint().unwrap());
        changed = declaration;
        changed.contracts[0].validator = ValidatorKind::OpaqueBytes;
        assert_ne!(fingerprint, changed.fingerprint().unwrap());
    }
}
