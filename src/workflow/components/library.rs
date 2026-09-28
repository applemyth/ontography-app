//! The user's own components and MCP servers.
//!
//! `library.json` in the app's data directory holds MCP servers that agents
//! load by name and component specifications. Specifications follow core's
//! project format: each names its `provider`, and that provider validates the
//! rest. This app's provider, `ontography`, reads specifications that extend
//! another component with default settings; a document's own `components`
//! use the same format.

use super::{config::McpServer, preset::Preset};
use crate::{AppError, Result, declarations::parse_json};
use ontography::{
    ApplicationRegistry,
    project::{ComponentProvider, ProjectComponent},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, sync::Arc};

pub const LIBRARY_FILE: &str = "library.json";
pub const PROVIDER: &str = "ontography";

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Library {
    /// MCP servers that agents load by name.
    #[serde(default)]
    pub servers: BTreeMap<String, McpServer>,
    /// Component specifications by name.
    #[serde(default)]
    pub components: BTreeMap<String, Value>,
}

impl Library {
    /// Read the library file; without one, only built-in components exist.
    pub fn load(path: &Path) -> Result<Self> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(error) => return Err(error.into()),
        };
        let library: Self = parse_json(&text).map_err(|error| {
            AppError::new("invalid_library", format!("{}: {error}", path.display()))
        })?;
        for (name, server) in &library.servers {
            server.validate(name).map_err(|error| {
                AppError::new(
                    "invalid_library",
                    format!("{}: {}", path.display(), error.message),
                )
            })?;
        }
        Ok(library)
    }
}

/// One component specification for the `ontography` provider.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    /// Defaults to this app's provider.
    #[serde(default = "provider")]
    provider: String,
    /// The component whose node type and implementation this one keeps.
    extends: String,
    /// What the component is for; defaults to its base's description.
    description: Option<String>,
    /// Default settings, which each placement's settings are merged over.
    #[serde(default = "no_settings")]
    config: Value,
}

fn provider() -> String {
    PROVIDER.into()
}

fn no_settings() -> Value {
    json!({})
}

/// Loads specifications that extend already known components or each other.
/// A specification cannot take the name of a known component.
pub(super) struct Provider<'a> {
    /// Prefix for loaded component identities, such as "library".
    pub(super) scope: &'a str,
    pub(super) known: &'a BTreeMap<String, Arc<dyn ProjectComponent>>,
}

impl ComponentProvider for Provider<'_> {
    fn load(
        &self,
        specs: &BTreeMap<String, Value>,
        _project_root: &Path,
        _registry: &mut ApplicationRegistry,
    ) -> std::result::Result<BTreeMap<String, Arc<dyn ProjectComponent>>, String> {
        let specs = specs
            .iter()
            .map(|(name, spec)| {
                if !super::is_name(name) {
                    return Err(format!(
                        "component name {name:?} must use letters, digits, '-' or '_'"
                    ));
                }
                if self.known.contains_key(name) {
                    return Err(format!("component {name:?} already exists"));
                }
                let spec: Spec = serde_json::from_value(spec.clone())
                    .map_err(|error| format!("component {name:?}: {error}"))?;
                if spec.provider != PROVIDER {
                    return Err(format!(
                        "component {name:?} names unknown provider {:?}",
                        spec.provider
                    ));
                }
                Ok((name.clone(), spec))
            })
            .collect::<std::result::Result<BTreeMap<_, _>, _>>()?;
        let mut loaded = BTreeMap::new();
        for name in specs.keys() {
            self.resolve(name, &specs, &mut loaded, &mut Vec::new())?;
        }
        Ok(loaded)
    }
}

impl Provider<'_> {
    /// Load specifications from this app only, without core's native registry.
    pub(super) fn load_specs(
        &self,
        specs: &BTreeMap<String, Value>,
    ) -> std::result::Result<BTreeMap<String, Arc<dyn ProjectComponent>>, String> {
        if specs.is_empty() {
            return Ok(BTreeMap::new());
        }
        self.load(specs, Path::new("/"), &mut ApplicationRegistry::new())
    }

    fn resolve(
        &self,
        name: &str,
        specs: &BTreeMap<String, Spec>,
        loaded: &mut BTreeMap<String, Arc<dyn ProjectComponent>>,
        extending: &mut Vec<String>,
    ) -> std::result::Result<Arc<dyn ProjectComponent>, String> {
        if let Some(component) = self.known.get(name).or_else(|| loaded.get(name)) {
            return Ok(component.clone());
        }
        let spec = specs
            .get(name)
            .ok_or_else(|| format!("unknown component {name:?}"))?;
        if extending.iter().any(|other| other == name) {
            return Err(format!(
                "components extend each other in a cycle: {} → {name}",
                extending.join(" → ")
            ));
        }
        extending.push(name.to_owned());
        let base = self.resolve(&spec.extends, specs, loaded, extending)?;
        extending.pop();
        let description = spec
            .description
            .clone()
            .unwrap_or_else(|| base.description().description);
        let component: Arc<dyn ProjectComponent> = Arc::new(
            Preset::new(
                format!("{}.{name}", self.scope),
                description,
                base,
                spec.config.clone(),
            )
            .map_err(|error| format!("component {name:?}: {error}"))?,
        );
        loaded.insert(name.to_owned(), component.clone());
        Ok(component)
    }
}
