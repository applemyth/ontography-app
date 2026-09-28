//! Components that start from another component's settings.
//!
//! A preset keeps its base component's node type and implementation and adds
//! default settings. A placement's settings are merged over those defaults as
//! a JSON merge patch (RFC 7386): objects merge by key, `null` removes a key,
//! and any other value replaces. MCP server lists are read as maps first, so
//! a placement can add a server to its preset's or remove one with `null`.
//!
//! Presets can extend presets, and each merges in turn. A `null` for a key
//! that this preset doesn't set is kept, so the preset below that does set it
//! can remove it; components treat a remaining `null` as unset.

use ontography::project::{
    BoundComponent, ComponentBindings, ComponentDescription, ProjectComponent,
};
use serde_json::{Map, Value};
use std::sync::Arc;

pub(super) struct Preset {
    identity: String,
    description: String,
    base: Arc<dyn ProjectComponent>,
    defaults: Value,
}

impl Preset {
    pub(super) fn new(
        identity: impl Into<String>,
        description: impl Into<String>,
        base: Arc<dyn ProjectComponent>,
        defaults: Value,
    ) -> Result<Self, String> {
        Ok(Self {
            identity: identity.into(),
            description: description.into(),
            base,
            defaults: settings(defaults)?,
        })
    }
}

impl ProjectComponent for Preset {
    fn description(&self) -> ComponentDescription {
        ComponentDescription {
            identity: self.identity.clone(),
            description: self.description.clone(),
            ..self.base.description()
        }
    }

    fn bind(&self, config: Value, bindings: &ComponentBindings) -> Result<BoundComponent, String> {
        let mut merged = self.defaults.clone();
        merge(&mut merged, settings(config)?);
        self.base.bind(merged, bindings)
    }
}

/// Settings ready to merge: an object whose MCP server list, if any, is a map.
fn settings(mut config: Value) -> Result<Value, String> {
    let object = config
        .as_object_mut()
        .ok_or_else(|| "settings must be an object".to_owned())?;
    if let Some(Value::Array(names)) = object.get("mcp") {
        let servers = names
            .iter()
            .map(|name| {
                name.as_str()
                    .map(|name| (name.to_owned(), Value::Bool(true)))
                    .ok_or_else(|| "mcp server names must be strings".to_owned())
            })
            .collect::<Result<Map<_, _>, _>>()?;
        object.insert("mcp".into(), Value::Object(servers));
    }
    Ok(config)
}

fn merge(target: &mut Value, patch: Value) {
    let Value::Object(patch) = patch else {
        *target = patch;
        return;
    };
    if !target.is_object() {
        *target = Value::Object(Map::new());
    }
    let target = target.as_object_mut().expect("object target");
    for (key, value) in patch {
        if value.is_null() && target.contains_key(&key) {
            target.remove(&key);
        } else if value.is_null() {
            target.insert(key, Value::Null);
        } else {
            merge(target.entry(key).or_insert(Value::Null), value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn placements_merge_over_defaults_and_can_remove_servers() {
        let mut defaults =
            settings(json!({"harness":"claude","mcp":["github","docs"],"model":"a"})).unwrap();
        merge(
            &mut defaults,
            settings(json!({"model":"b","mcp":{"docs":null,"local":{"command":"serve"}}})).unwrap(),
        );
        assert_eq!(
            defaults,
            json!({"harness":"claude","model":"b","mcp":{"github":true,"local":{"command":"serve"}}})
        );
        assert!(settings(json!([])).is_err());
        assert!(settings(json!({"mcp":[1]})).is_err());
    }
}
