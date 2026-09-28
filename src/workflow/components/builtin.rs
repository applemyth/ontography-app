//! The app's own components: what each built-in node is and how it binds.
//!
//! `agent` is the general agent component: its `harness` chooses Codex or
//! Claude, and a legacy `argv` replaces the agent with another program in the
//! node's terminal. `codex` and `claude` are presets of it.

use super::{
    config::{
        AgentConfig, CommandConfig, HumanConfig, Implementation, InboxConfig, McpServer,
        ProgramConfig,
    },
    preset::Preset,
};
use crate::workflow::document::CONTRACT;
use ontography::{
    IngressMode,
    project::{BoundComponent, ComponentBindings, ComponentDescription, ProjectComponent},
};
use schemars::JsonSchema;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Permission modes the installed Claude Code accepts for `--permission-mode`.
const PERMISSION_MODES: [&str; 6] = [
    "acceptEdits",
    "auto",
    "bypassPermissions",
    "dontAsk",
    "manual",
    "plan",
];

/// Built-in components by name. Agents resolve MCP server names in `servers`.
pub(super) fn components(
    servers: Arc<BTreeMap<String, McpServer>>,
) -> BTreeMap<String, Arc<dyn ProjectComponent>> {
    let agent: Arc<dyn ProjectComponent> = Arc::new(Agent { servers });
    let preset = |name: &str, description: &str| -> Arc<dyn ProjectComponent> {
        Arc::new(
            Preset::new(
                identity(name),
                description,
                agent.clone(),
                BTreeSet::new(),
                json!({"harness": name}),
            )
            .expect("built-in preset settings are an object"),
        )
    };
    BTreeMap::from([
        ("agent".to_owned(), agent.clone()),
        (
            "codex".to_owned(),
            preset("codex", "A continuing Codex conversation."),
        ),
        (
            "claude".to_owned(),
            preset("claude", "A continuing Claude Code conversation."),
        ),
        ("command".to_owned(), Arc::new(Command) as Arc<_>),
        ("human".to_owned(), Arc::new(Human) as Arc<_>),
        ("inbox".to_owned(), Arc::new(Inbox) as Arc<_>),
    ])
}

fn identity(name: &str) -> String {
    format!("ontography.{name}")
}

/// A built-in component's description. Each gives its nodes one type, named
/// for what they are; library components can add more.
fn description<T: JsonSchema>(name: &str, text: &str, node_type: &str) -> ComponentDescription {
    ComponentDescription {
        identity: identity(name),
        description: text.into(),
        types: vec![node_type.into()],
        result_contract: CONTRACT.into(),
        // Workflow connections are not ports: any node may connect to any other.
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        dynamic_inputs: true,
        dynamic_outputs: true,
        ingress_modes: vec![IngressMode::Any, IngressMode::All],
        configuration_schema: Some(crate::catalog::schema::<T>()),
    }
}

/// Parse a placement's settings. A `null` a preset left in place means unset.
fn settings<T: DeserializeOwned>(mut config: Value) -> Result<T, String> {
    if let Some(object) = config.as_object_mut() {
        object.retain(|_, value| !value.is_null());
    }
    serde_json::from_value(config).map_err(|error| error.to_string())
}

fn bound(implementation: Implementation) -> Result<BoundComponent, String> {
    Ok(BoundComponent {
        kind: implementation.kind().into(),
        config: implementation.configuration(),
    })
}

fn validate_argv(argv: &[String]) -> Result<(), String> {
    if argv.first().is_none_or(|program| program.trim().is_empty()) {
        return Err("argv must contain a command followed by string arguments".into());
    }
    Ok(())
}

struct Agent {
    servers: Arc<BTreeMap<String, McpServer>>,
}

/// A continuing agent conversation.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AgentSettings {
    /// Standing instructions, followed by the graph delivery instructions.
    prompt: String,
    /// The agent that holds the conversation.
    #[serde(default)]
    harness: Harness,
    /// The model to use; the agent's own default applies when unset.
    model: Option<String>,
    /// Run in a terminal that can be opened from the graph (the default).
    /// `false` runs the agent headless.
    pty: Option<bool>,
    /// MCP servers to load besides the node tools: a list of library server
    /// names, or a map from name to `true` (the library's server) or a
    /// `{command, args, env}` definition.
    mcp: Option<Value>,
    /// Claude's permission mode, such as `acceptEdits` or `plan`.
    permission_mode: Option<String>,
    /// Replace the agent with this interactive program in the node's terminal.
    /// It reaches the graph only through the node tools.
    argv: Option<Vec<String>>,
}

#[derive(Clone, Copy, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
enum Harness {
    #[default]
    Codex,
    Claude,
}

impl ProjectComponent for Agent {
    fn description(&self) -> ComponentDescription {
        description::<AgentSettings>(
            "agent",
            "A continuing agent conversation; harness chooses Codex (default) or Claude.",
            "Agent",
        )
    }

    fn bind(&self, config: Value, _: &ComponentBindings) -> Result<BoundComponent, String> {
        let settings: AgentSettings = settings(config)?;
        if let Some(argv) = settings.argv {
            // The program replaces the agent, so agent settings would be ignored.
            if matches!(settings.harness, Harness::Claude)
                || settings.pty == Some(false)
                || settings.mcp.is_some()
                || settings.permission_mode.is_some()
            {
                return Err("argv runs its program in the node's terminal instead of an agent; remove harness claude, pty, mcp, and permission_mode".into());
            }
            validate_argv(&argv)?;
            return bound(Implementation::Program(ProgramConfig { argv }));
        }
        if let Some(mode) = &settings.permission_mode {
            if matches!(settings.harness, Harness::Codex) {
                return Err("permission_mode applies only to the claude harness".into());
            }
            if !PERMISSION_MODES.contains(&mode.as_str()) {
                return Err(format!(
                    "permission_mode must be one of {}",
                    PERMISSION_MODES.join(", ")
                ));
            }
        }
        let config = AgentConfig {
            prompt: settings.prompt,
            model: settings.model,
            pty: settings.pty.unwrap_or(true),
            mcp: self.servers(settings.mcp)?,
            permission_mode: settings.permission_mode,
        };
        bound(match settings.harness {
            Harness::Codex => Implementation::Codex(config),
            Harness::Claude => Implementation::Claude(config),
        })
    }
}

impl Agent {
    /// Resolve library names and inline definitions to exact servers.
    fn servers(&self, selection: Option<Value>) -> Result<BTreeMap<String, McpServer>, String> {
        let entries = match selection {
            None => return Ok(BTreeMap::new()),
            Some(Value::Array(names)) => names
                .into_iter()
                .map(|name| match name {
                    Value::String(name) => Ok((name, Value::Bool(true))),
                    _ => Err("mcp server names must be strings".to_owned()),
                })
                .collect::<Result<Vec<_>, _>>()?,
            Some(Value::Object(servers)) => servers.into_iter().collect(),
            Some(_) => return Err("mcp must be a list of server names or a map of servers".into()),
        };
        entries
            .into_iter()
            // A server a preset removed, as `null`, is simply not loaded.
            .filter(|(_, entry)| !entry.is_null())
            .map(|(name, entry)| {
                let server = match entry {
                    Value::Bool(true) => self.servers.get(&name).cloned().ok_or_else(|| {
                        format!("MCP server {name:?} is not defined in the library")
                    })?,
                    definition @ Value::Object(_) => {
                        serde_json::from_value::<McpServer>(definition)
                            .map_err(|error| format!("MCP server {name:?}: {error}"))?
                    }
                    _ => {
                        return Err(format!(
                            "MCP server {name:?} must be true (from the library) or a definition"
                        ));
                    }
                };
                server.validate(&name).map_err(|error| error.message)?;
                Ok((name, server))
            })
            .collect()
    }
}

/// A program run once per task, with input messages on stdin.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct CommandSettings {
    /// The command and its arguments.
    argv: Vec<String>,
    /// Seconds before a task's process is stopped (default 300).
    timeout_secs: Option<u64>,
}

struct Command;

impl ProjectComponent for Command {
    fn description(&self) -> ComponentDescription {
        description::<CommandSettings>("command", "Runs a program once per task.", "Command")
    }

    fn bind(&self, config: Value, _: &ComponentBindings) -> Result<BoundComponent, String> {
        let settings: CommandSettings = settings(config)?;
        validate_argv(&settings.argv)?;
        if settings.timeout_secs == Some(0) {
            return Err("timeout_secs must be a positive integer".into());
        }
        bound(Implementation::Command(CommandConfig {
            argv: settings.argv,
            timeout_secs: settings.timeout_secs,
        }))
    }
}

/// A decision a person makes for each task.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HumanSettings {
    /// What the person is asked to decide.
    prompt: Option<String>,
}

struct Human;

impl ProjectComponent for Human {
    fn description(&self) -> ComponentDescription {
        description::<HumanSettings>(
            "human",
            "Waits for a person's decision on each task.",
            "Human",
        )
    }

    fn bind(&self, config: Value, _: &ComponentBindings) -> Result<BoundComponent, String> {
        let settings: HumanSettings = settings(config)?;
        bound(Implementation::Human(HumanConfig {
            prompt: settings.prompt,
        }))
    }
}

/// Holds work; it has no settings.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct InboxSettings {}

struct Inbox;

impl ProjectComponent for Inbox {
    fn description(&self) -> ComponentDescription {
        description::<InboxSettings>(
            "inbox",
            "Holds incoming work for inspection and export.",
            "Inbox",
        )
    }

    fn bind(&self, config: Value, _: &ComponentBindings) -> Result<BoundComponent, String> {
        let InboxSettings {} = settings(config)?;
        bound(Implementation::Inbox(InboxConfig {}))
    }
}
