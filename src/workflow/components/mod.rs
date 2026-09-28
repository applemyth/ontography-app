//! Components: pre-configured nodes that a workflow document places.
//!
//! This follows core's project model (`ontography::project`). A component
//! describes a node, including its core node type, and binds each placement's
//! settings to a trusted implementation and its exact configuration. The
//! built-in components are this app's; the user's library and a document's
//! own `components` add specifications that extend them with defaults.
//!
//! A run keeps the bindings made when its document was started or edited, so
//! changing the library never silently changes a running node.

mod builtin;
mod config;
mod library;
mod preset;

pub use config::{
    AgentConfig, Binding, CommandConfig, HumanConfig, Implementation, InboxConfig, McpServer,
    NODE_TOOLS_SERVER, ProgramConfig,
};
pub use library::{LIBRARY_FILE, Library, PROVIDER};

use super::{Document, DocumentNode, NodeType};
use crate::{AppError, Result};
use ontography::{
    IngressMode,
    project::{ComponentBindings, ComponentDescription, ProjectComponent},
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::Path, sync::Arc};

/// Each node's binding, by node name.
pub type Bindings = BTreeMap<String, Binding>;

/// The components a document can place.
#[derive(Clone)]
pub struct Catalog {
    components: BTreeMap<String, Arc<dyn ProjectComponent>>,
}

impl Catalog {
    /// Only the built-in components.
    pub fn builtin() -> Self {
        Self {
            components: builtin::components(Arc::default()),
        }
    }

    /// Built-in components and those of the library file at `path`.
    pub fn load(path: &Path) -> Result<Self> {
        Self::with_library(&Library::load(path)?)
    }

    pub fn with_library(library: &Library) -> Result<Self> {
        let mut components = builtin::components(Arc::new(library.servers.clone()));
        let loaded = library::Provider {
            scope: "library",
            known: &components,
        }
        .load_specs(&library.components)
        .map_err(|error| AppError::new("invalid_library", error))?;
        components.extend(loaded);
        Ok(Self { components })
    }

    /// Bind every node of `document`, including to its own components.
    pub fn bind(&self, document: &Document) -> Result<Bindings> {
        let loaded = library::Provider {
            scope: "document",
            known: &self.components,
        }
        .load_specs(&document.components)
        .map_err(|error| AppError::new("invalid_workflow_document", error))?;
        let mut components = self.components.clone();
        components.extend(loaded);
        let catalog = Self { components };
        document
            .nodes
            .iter()
            .map(|node| Ok((node.id.clone(), catalog.bind_node(document, node)?)))
            .collect()
    }

    /// What each component is, for the manager choosing one.
    pub fn describe(&self) -> BTreeMap<String, ComponentDescription> {
        self.components
            .iter()
            .map(|(name, component)| (name.clone(), component.description()))
            .collect()
    }

    fn bind_node(&self, document: &Document, node: &DocumentNode) -> Result<Binding> {
        let error = |message: String| {
            AppError::new(
                "invalid_workflow_document",
                format!("Node {:?}: {message}", node.id),
            )
        };
        let component = self
            .components
            .get(&node.component)
            .ok_or_else(|| error(format!("unknown component {:?}", node.component)))?;
        let description = component.description();
        let node_type = match description.types.as_slice() {
            [name] => NodeType::from_core(name),
            _ => None,
        }
        .ok_or_else(|| error("its component must give exactly one workflow node type".into()))?;
        let ingress = IngressMode::from(node.join);
        if !description.ingress_modes.contains(&ingress) {
            return Err(error(format!(
                "{:?} does not support this join",
                node.component
            )));
        }
        let bound = component
            .bind(node.config.clone(), &placement(document, node, ingress))
            .map_err(error)?;
        if !node_type.runs_tasks()
            && (node.retry.is_some() || !node.grants.is_empty() || node.tools.is_some())
        {
            return Err(error(
                "retry, grants, and tools apply only to agent and command nodes".into(),
            ));
        }
        Ok(Binding {
            node_type,
            implementation: Implementation::from_bound(bound)?,
        })
    }
}

/// A node's connections as core's component binder sees them. Workflow
/// connections have no named ports, so each is keyed by the other node.
fn placement(document: &Document, node: &DocumentNode, ingress: IngressMode) -> ComponentBindings {
    let mut inputs = BTreeMap::<String, Vec<String>>::new();
    let mut outputs = BTreeMap::<String, Vec<String>>::new();
    for edge in &document.edges {
        let key = super::edge_key(&edge.from, &edge.to);
        if edge.to == node.id {
            inputs
                .entry(edge.from.clone())
                .or_default()
                .push(key.clone());
        }
        if edge.from == node.id {
            outputs.entry(edge.to.clone()).or_default().push(key);
        }
    }
    ComponentBindings {
        node_id: node.id.clone(),
        inputs,
        outputs,
        ingress_mode: ingress,
        is_entry: node.id == document.entry,
    }
}

/// A placed node and what it is bound to: everything its worker runs by.
#[derive(Clone, Debug, PartialEq)]
pub struct BoundNode {
    pub node: DocumentNode,
    pub binding: Binding,
}

impl BoundNode {
    /// Names this exact definition; see [`definition_digest`].
    pub fn digest(&self) -> String {
        definition_digest(&self.node, &self.binding)
    }

    /// `node` bound by a built-in component.
    #[cfg(test)]
    pub(crate) fn of(node: DocumentNode) -> Self {
        let document = Document {
            name: "test".into(),
            entry: node.id.clone(),
            components: BTreeMap::new(),
            nodes: vec![node.clone()],
            edges: vec![],
        };
        let binding = Catalog::builtin()
            .bind(&document)
            .expect("built-in components bind")
            .remove(&node.id)
            .expect("bound node");
        Self { node, binding }
    }
}

/// Names a node's exact definition: its placement and what it is bound to.
/// Any change to either changes it, which gives failed tasks fresh attempts.
pub fn definition_digest(node: &DocumentNode, binding: &Binding) -> String {
    let encoded = serde_json::to_vec(&(node, binding)).unwrap_or_default();
    format!("{:x}", Sha256::digest(encoded))
}

/// Names shared with other programs' configuration keys.
fn is_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn document(nodes: Value, components: Value) -> Document {
        let mut document = json!({"name":"components","entry":"a","nodes":nodes});
        if !components.is_null() {
            document["components"] = components;
        }
        Document::parse(&document.to_string()).unwrap()
    }

    fn bind_one(catalog: &Catalog, node: Value) -> Result<Binding> {
        let mut node = node;
        node["id"] = json!("a");
        catalog
            .bind(&document(json!([node]), Value::Null))
            .map(|mut bindings| bindings.remove("a").unwrap())
    }

    fn library(value: Value) -> Catalog {
        Catalog::with_library(&serde_json::from_value(value).unwrap()).unwrap()
    }

    fn agent(binding: &Binding) -> (&'static str, &AgentConfig) {
        match &binding.implementation {
            Implementation::Codex(config) => ("codex", config),
            Implementation::Claude(config) => ("claude", config),
            other => panic!("not an agent: {other:?}"),
        }
    }

    #[test]
    fn built_in_components_bind_their_roles_and_implementations() {
        let catalog = Catalog::builtin();
        let codex = bind_one(
            &catalog,
            json!({"component":"agent","config":{"prompt":"p"}}),
        )
        .unwrap();
        assert_eq!(codex.node_type, NodeType::Agent);
        let (harness, config) = agent(&codex);
        assert_eq!((harness, config.pty, config.mcp.len()), ("codex", true, 0));
        let claude = bind_one(
            &catalog,
            json!({"component":"claude","config":{"prompt":"p","pty":false,"permission_mode":"acceptEdits"}}),
        )
        .unwrap();
        let (harness, config) = agent(&claude);
        assert_eq!(harness, "claude");
        assert!(!config.pty);
        assert_eq!(config.permission_mode.as_deref(), Some("acceptEdits"));
        let program = bind_one(
            &catalog,
            json!({"component":"agent","config":{"prompt":"p","argv":["/bin/cat"],"model":"kept"}}),
        )
        .unwrap();
        assert_eq!(
            program.implementation,
            Implementation::Program(ProgramConfig {
                argv: vec!["/bin/cat".into()]
            })
        );
        for (component, node_type) in [
            ("command", NodeType::Command),
            ("human", NodeType::Human),
            ("inbox", NodeType::Inbox),
        ] {
            let config = if component == "command" {
                json!({"argv":["true"]})
            } else {
                json!({})
            };
            let binding =
                bind_one(&catalog, json!({"component":component,"config":config})).unwrap();
            assert_eq!(binding.node_type, node_type);
            assert_eq!(binding.implementation.kind(), component);
        }
    }

    #[test]
    fn settings_that_would_be_ignored_or_invalid_are_rejected() {
        let catalog = Catalog::builtin();
        for (node, message) in [
            (
                json!({"component":"claude","config":{"prompt":"p","argv":["x"]}}),
                "argv",
            ),
            (
                json!({"component":"agent","config":{"prompt":"p","argv":["x"],"pty":false}}),
                "argv",
            ),
            (
                json!({"component":"codex","config":{"prompt":"p","permission_mode":"plan"}}),
                "claude",
            ),
            (
                json!({"component":"claude","config":{"prompt":"p","permission_mode":"yolo"}}),
                "permission_mode",
            ),
            (
                json!({"component":"agent","config":{"prompt":"p","harness":"gemini"}}),
                "gemini",
            ),
            (json!({"component":"agent","config":{}}), "prompt"),
            (
                json!({"component":"agent","config":{"prompt":"p","mcp":["unknown"]}}),
                "library",
            ),
            (
                json!({"component":"agent","config":{"prompt":"p","mcp":{"ontography_node":{"command":"x"}}}}),
                "reserved",
            ),
            (
                json!({"component":"agent","config":{"prompt":"p","mcp":{"bad name":{"command":"x"}}}}),
                "letters",
            ),
            (
                json!({"component":"agent","config":{"prompt":"p","mcp":{"s":{"command":"x","env":{"1X":"v"}}}}}),
                "environment",
            ),
            (
                json!({"component":"command","config":{"argv":[" "]}}),
                "argv",
            ),
            (
                json!({"component":"command","config":{"argv":["x"],"timeout_secs":0}}),
                "timeout_secs",
            ),
            (
                json!({"component":"inbox","config":{"prompt":"p"}}),
                "prompt",
            ),
            (json!({"component":"robot"}), "unknown component"),
        ] {
            let error = bind_one(&catalog, node.clone()).unwrap_err();
            assert!(error.message.contains(message), "{node}: {}", error.message);
        }
    }

    #[test]
    fn library_components_extend_others_and_servers_resolve_by_name() {
        let catalog = library(json!({
            "servers": {
                "github": {"command":"gh-mcp","args":["serve"]},
                "docs": {"command":"docs-mcp","env":{"DOCS_ROOT":"/docs"}}
            },
            "components": {
                "reviewer": {"provider":"ontography","extends":"claude","description":"Reviews changes.",
                    "config":{"prompt":"Review for security.","model":"opus","mcp":["github","docs"]}},
                "strict-reviewer": {"extends":"reviewer","config":{"permission_mode":"plan"}}
            }
        }));
        let binding = bind_one(
            &catalog,
            json!({"component":"strict-reviewer","config":{"mcp":{"docs":null,"local":{"command":"serve"}}}}),
        )
        .unwrap();
        let (harness, config) = agent(&binding);
        assert_eq!(harness, "claude");
        assert_eq!(config.prompt, "Review for security.");
        assert_eq!(config.model.as_deref(), Some("opus"));
        assert_eq!(config.permission_mode.as_deref(), Some("plan"));
        assert_eq!(config.mcp.keys().collect::<Vec<_>>(), ["github", "local"]);
        assert_eq!(config.mcp["github"].args, ["serve"]);
        let descriptions = catalog.describe();
        assert_eq!(descriptions["reviewer"].description, "Reviews changes.");
        assert_eq!(descriptions["reviewer"].identity, "library.reviewer");
        assert_eq!(descriptions["strict-reviewer"].types, ["Agent"]);
    }

    #[test]
    fn invalid_libraries_are_reported() {
        for (value, message) in [
            (
                json!({"components":{"claude":{"extends":"agent"}}}),
                "already exists",
            ),
            (
                json!({"components":{"a":{"extends":"b"},"b":{"extends":"a"}}}),
                "cycle",
            ),
            (
                json!({"components":{"a":{"extends":"missing"}}}),
                "unknown component",
            ),
            (
                json!({"components":{"a":{"provider":"other","extends":"agent"}}}),
                "provider",
            ),
            (
                json!({"components":{"a":{"extends":"agent","config":[]}}}),
                "object",
            ),
            (json!({"components":{"a b":{"extends":"agent"}}}), "letters"),
        ] {
            let library: Library = serde_json::from_value(value.clone()).unwrap();
            let error = Catalog::with_library(&library).err().unwrap();
            assert!(
                error.message.contains(message),
                "{value}: {}",
                error.message
            );
        }
    }

    #[test]
    fn library_files_are_optional_and_strict() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(LIBRARY_FILE);
        assert!(
            Catalog::load(&path)
                .unwrap()
                .describe()
                .contains_key("claude")
        );
        std::fs::write(&path, r#"{"servers":{},"servers":{}}"#).unwrap();
        assert_eq!(Catalog::load(&path).err().unwrap().code, "invalid_library");
        std::fs::write(&path, r#"{"servers":{"s":{"command":""}}}"#).unwrap();
        assert_eq!(Catalog::load(&path).err().unwrap().code, "invalid_library");
        std::fs::write(&path, r#"{"components":{"mine":{"extends":"codex"}}}"#).unwrap();
        assert!(
            Catalog::load(&path)
                .unwrap()
                .describe()
                .contains_key("mine")
        );
    }

    #[test]
    fn documents_define_their_own_components() {
        let catalog = library(
            json!({"components":{"writer":{"extends":"codex","config":{"prompt":"Write."}}}}),
        );
        let components = json!({"careful-writer":{"extends":"writer","config":{"model":"slow"}}});
        let bindings = catalog
            .bind(&document(
                json!([{"id":"a","component":"careful-writer"},{"id":"b","component":"writer"}]),
                components,
            ))
            .unwrap();
        assert_eq!(agent(&bindings["a"]).1.model.as_deref(), Some("slow"));
        assert_eq!(agent(&bindings["b"]).1.model, None);
        let shadowing = document(
            json!([{"id":"a","component":"writer"}]),
            json!({"writer":{"extends":"claude"}}),
        );
        assert!(
            catalog
                .bind(&shadowing)
                .unwrap_err()
                .message
                .contains("already exists")
        );
    }

    #[test]
    fn bindings_round_trip_and_name_core_execution_bindings() {
        let binding = bind_one(
            &Catalog::builtin(),
            json!({"component":"claude","config":{"prompt":"p","mcp":{"s":{"command":"x"}}}}),
        )
        .unwrap();
        let saved = serde_json::to_value(&binding).unwrap();
        assert_eq!(saved["node_type"], "Agent");
        assert_eq!(saved["implementation"]["kind"], "claude");
        assert_eq!(serde_json::from_value::<Binding>(saved).unwrap(), binding);
        assert_eq!(
            binding.implementation.configuration()["mcp"]["s"]["command"],
            "x"
        );
    }
}
