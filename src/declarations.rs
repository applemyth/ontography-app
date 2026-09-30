//! Core graph declarations: what the document compiler produces and a run
//! stores, compiled through public core APIs.
//!
//! Contract kinds select the small trusted validator catalog below; they are
//! not executable schemas supplied by a client.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ontography::{
    Authority, AuthorityMatch, AuthorityTag, Contract, ContractViolation, DefinitionError,
    DefinitionId, Edge, EdgeDefinition, Graph, GraphFragment, IngressMode, Kernel, Node,
    NodeDefinition, PackageEnvelope, Payload, RootRule, Schema,
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
}

impl GraphDeclaration {
    /// Construct the actual core schema, contracts, and graph.
    pub fn compile(&self) -> Result<Arc<Kernel>, DeclarationError> {
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
        Ok(Arc::new(kernel))
    }

    /// SHA-256 of the typed declaration, including validator versions.
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
    /// Any bytes, including empty and non-UTF-8 payloads.
    Bytes,
    /// Valid UTF-8, including empty text: a message, or a workspace envelope.
    Text,
    /// Only a package envelope: a workspace.
    Workspace,
}

impl ValidatorKind {
    /// Whether `bytes` satisfy this validator. Like core, every validator
    /// refuses a malformed package envelope.
    pub fn check(self, bytes: &[u8]) -> Result<(), ContractViolation> {
        // An envelope must be a JSON object; most payloads need no owned copy.
        let object = bytes
            .iter()
            .find(|byte| !matches!(byte, b' ' | b'\t' | b'\r' | b'\n'))
            == Some(&b'{');
        let envelope = if object {
            PackageEnvelope::from_payload(&Payload::from(bytes)).map_err(|error| {
                ContractViolation::new(format!("invalid package envelope: {error}"))
            })?
        } else {
            None
        };
        match self {
            Self::Bytes => Ok(()),
            Self::Text => std::str::from_utf8(bytes)
                .map(|_| ())
                .map_err(|error| ContractViolation::new(format!("invalid UTF-8: {error}"))),
            Self::Workspace if envelope.is_some() => Ok(()),
            Self::Workspace => Err(ContractViolation::new("expected a workspace")),
        }
    }
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
    pub(crate) fn compile(&self) -> Result<Contract, DeclarationError> {
        if self.validator_version != VALIDATOR_VERSION {
            return Err(DeclarationError::UnsupportedValidatorVersion {
                contract_id: self.id.clone(),
                version: self.validator_version,
            });
        }
        let validator = self.validator;
        in_scope(
            &format!("contract {:?}", self.id),
            Contract::new(self.id.as_str(), self.object_type.as_str(), move |bytes| {
                validator.check(bytes)
            }),
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NodeDeclaration {
    pub id: String,
    pub types: Vec<String>,
    pub result_contract: String,
    #[serde(default)]
    pub ingress_mode: IngressDeclaration,
}

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityMatchDeclaration {
    #[default]
    AnyOf,
    AllOf,
}

impl AuthorityMatchDeclaration {
    pub(crate) const fn is_any_of(&self) -> bool {
        matches!(self, Self::AnyOf)
    }
}

impl From<AuthorityMatchDeclaration> for AuthorityMatch {
    fn from(value: AuthorityMatchDeclaration) -> Self {
        match value {
            AuthorityMatchDeclaration::AnyOf => Self::AnyOf,
            AuthorityMatchDeclaration::AllOf => Self::AllOf,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RootDeclaration {
    pub node_id: String,
    pub ceiling: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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
    /// Core's form of this fragment. It is checked only when admitted as part
    /// of a definition.
    pub fn compile(&self) -> Result<GraphFragment, DeclarationError> {
        FragmentRef {
            nodes: &self.nodes,
            edges: &self.edges,
            roots: &self.roots,
            authority_transitions: &self.authority_transitions,
        }
        .compile("fragment")
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
        let fragment = self.compile(scope)?;
        let graph = in_scope(
            scope,
            Graph::new(fragment.nodes().to_vec(), fragment.edges().to_vec()),
        )?;
        in_scope(
            scope,
            Kernel::admit(
                id.clone(),
                schema.clone(),
                graph,
                contracts.iter().cloned(),
                fragment.node_definitions().to_vec(),
                fragment.edge_definitions().to_vec(),
                fragment.authority_transitions().to_vec(),
                fragment.roots().to_vec(),
            ),
        )
    }

    fn compile(&self, scope: &str) -> Result<GraphFragment, DeclarationError> {
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
        Ok(GraphFragment::new(
            topology_nodes,
            topology_edges,
            nodes,
            edges,
            transitions,
            roots,
        ))
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
    use ontography::{ActivationProposal, Emission, OutputAuthority, PackageId};

    /// A root `A` sends text to `B` under `work` authority.
    const FLOW: &str = r#"{
        "version": 1, "id": "logical-flow",
        "schema": {"node_types": ["Logical"], "object_types": ["Text"], "authority_tags": ["work"]},
        "contracts": [{"id": "text", "object_type": "Text", "validator": "text", "validator_version": 1}],
        "nodes": [
            {"id": "A", "types": ["Logical"], "result_contract": "text"},
            {"id": "B", "types": ["Logical"], "result_contract": "text"}
        ],
        "edges": [{"id": "A_to_B", "source": "A", "target": "B", "types": ["Flow"],
            "package_contract": "text", "authority_tags": ["work"]}],
        "roots": [{"node_id": "A", "ceiling": ["work"]}]
    }"#;

    fn flow() -> GraphDeclaration {
        parse_json(FLOW).unwrap()
    }

    #[test]
    fn a_declaration_delivers_through_core() {
        let kernel = flow().compile().unwrap();
        let mut state = kernel.empty_state();
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
        let accepted = kernel.activate(&mut state, proposal).unwrap();
        let package = PackageId::from_parts(accepted, 0);
        assert_eq!(state.position(package).unwrap().holder(), "B");
    }

    #[test]
    fn invalid_references_and_versions_are_rejected() {
        let mut declaration = flow();
        declaration.edges[0].target = "absent".into();
        assert!(matches!(
            declaration.compile(),
            Err(DeclarationError::Definition { .. })
        ));
        declaration = flow();
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
    fn validators_decide_real_admission() {
        let mut declaration = flow();
        let root = |bytes: &'static [u8]| {
            ActivationProposal::root("A", Authority::new([]), Arc::from(bytes))
        };
        let kernel = declaration.compile().unwrap();
        let mut state = kernel.empty_state();
        assert!(kernel.activate(&mut state, root(&[0xff])).is_err());
        assert!(
            kernel
                .activate(&mut state, root(br#"{"ontography_package":7}"#))
                .is_err()
        );
        assert!(
            kernel
                .activate(&mut state, root(b" \n\t{\"ontography_package\":7}"))
                .is_err()
        );
        assert!(kernel.activate(&mut state, root(b"text")).is_ok());
        declaration.contracts[0].validator = ValidatorKind::Bytes;
        let kernel = declaration.compile().unwrap();
        let mut state = kernel.empty_state();
        assert!(kernel.activate(&mut state, root(&[0xff])).is_ok());
        assert!(
            kernel
                .activate(&mut state, root(b" \n\t{\"ontography_package\":7}"))
                .is_err()
        );
        declaration.contracts[0].validator = ValidatorKind::Workspace;
        let kernel = declaration.compile().unwrap();
        let mut state = kernel.empty_state();
        assert!(kernel.activate(&mut state, root(b"text")).is_err());
    }

    #[test]
    fn duplicate_keys_and_unknown_fields_are_rejected() {
        let duplicate = r#"{"nodes":[],"nodes":[]}"#;
        assert!(
            parse_json::<GraphFragmentDeclaration>(duplicate)
                .unwrap_err()
                .to_string()
                .contains("duplicate JSON key")
        );
        let unknown = FLOW.replacen(
            "\"version\": 1",
            "\"version\": 1, \"implementation\": \"codex\"",
            1,
        );
        assert!(parse_json::<GraphDeclaration>(&unknown).is_err());
    }

    #[test]
    fn declaration_fingerprint_ignores_json_formatting() {
        let declaration = flow();
        let compact = serde_json::to_string(&declaration).unwrap();
        let fingerprint = declaration.fingerprint().unwrap();
        assert_eq!(fingerprint.len(), 64);
        assert_eq!(
            fingerprint,
            parse_json::<GraphDeclaration>(&compact)
                .unwrap()
                .fingerprint()
                .unwrap()
        );
        let mut changed = declaration;
        changed.contracts[0].validator = ValidatorKind::Bytes;
        assert_ne!(fingerprint, changed.fingerprint().unwrap());
    }
}
