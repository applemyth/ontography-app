//! What each trusted implementation runs once a component has bound a node.
//!
//! This is the `{kind, config}` half of core's `BoundComponent`: components
//! accept loose placement settings and produce these exact configurations.
//! Every field here is already validated and defaulted.

use super::is_name;
use crate::{AppError, Result};
use ontography::project::BoundComponent;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

/// The node tools' own MCP server name; configured servers cannot take it.
pub const NODE_TOOLS_SERVER: &str = "ontography_node";

/// A node's types in core together with what runs there.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    /// What the node is, as core's schema names it: labels such as `Agent`
    /// or `Reviewer`. They never choose what runs; the implementation does.
    pub types: BTreeSet<String>,
    pub implementation: Implementation,
}

/// A trusted implementation and its exact configuration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "config", rename_all = "snake_case")]
pub enum Implementation {
    /// A Codex conversation hosted by `codex app-server`.
    Codex(AgentConfig),
    /// A Claude Code conversation.
    Claude(AgentConfig),
    /// A program of your choice in the node's terminal. It reaches the graph
    /// only through the node tools it is given.
    Program(ProgramConfig),
    /// A program run once per task.
    Command(CommandConfig),
    /// A person deciding each task.
    Human(HumanConfig),
    /// Incoming work, held.
    Inbox(InboxConfig),
    /// Work an outside client performs with core moves; nothing runs here.
    External(ExternalConfig),
}

impl Implementation {
    /// The implementation kind, as core's execution bindings name it.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Codex(_) => "codex",
            Self::Claude(_) => "claude",
            Self::Program(_) => "program",
            Self::Command(_) => "command",
            Self::Human(_) => "human",
            Self::Inbox(_) => "inbox",
            Self::External(_) => "external",
        }
    }

    /// Read what a component bound. Its configuration must be exact.
    pub fn from_bound(bound: BoundComponent) -> Result<Self> {
        serde_json::from_value(json!({"kind": bound.kind, "config": bound.config})).map_err(
            |error| {
                AppError::new(
                    "invalid_component_binding",
                    format!("Component bound {:?} incorrectly: {error}", bound.kind),
                )
            },
        )
    }

    /// This configuration as core's execution bindings record it.
    pub fn configuration(&self) -> Value {
        serde_json::to_value(self)
            .map(|mut value| value["config"].take())
            .unwrap_or_default()
    }

    /// The command a task harness runs for each task, if this is one.
    pub const fn command(&self) -> Option<&CommandConfig> {
        match self {
            Self::Command(command) => Some(command),
            _ => None,
        }
    }

    /// Whether a worker takes the node's tasks, so that retry policies,
    /// grants, and node tools apply. People decide human tasks, inboxes only
    /// hold work, and outside clients act for external nodes.
    pub const fn runs_tasks(&self) -> bool {
        !matches!(self, Self::Human(_) | Self::Inbox(_) | Self::External(_))
    }

    /// Whether an outside client acts for the node, so that no worker runs.
    pub const fn is_external(&self) -> bool {
        matches!(self, Self::External(_))
    }

    /// Whether a continuing session runs at the node, rather than the task harness.
    pub const fn is_session(&self) -> bool {
        matches!(self, Self::Codex(_) | Self::Claude(_) | Self::Program(_))
    }

    /// Whether the node's session runs in a terminal that can be opened.
    pub const fn has_terminal(&self) -> bool {
        match self {
            Self::Codex(agent) | Self::Claude(agent) => agent.pty,
            Self::Program(_) => true,
            Self::Command(_) | Self::Human(_) | Self::Inbox(_) | Self::External(_) => false,
        }
    }
}

/// A continuing agent conversation at a node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    /// Standing instructions, followed by the graph delivery instructions.
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Whether the agent runs in a terminal that can be opened from the graph.
    /// Without one it runs headless, reachable only through the graph.
    pub pty: bool,
    /// MCP servers loaded alongside the node tools, by name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub mcp: BTreeMap<String, McpServer>,
    /// Claude's permission mode; unset keeps the user's own setting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_mode: Option<String>,
}

/// An MCP server an agent starts over stdio.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct McpServer {
    pub command: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// Stored with each workflow that uses it; keep secrets out of it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

impl McpServer {
    pub fn validate(&self, name: &str) -> Result<()> {
        if !is_name(name) {
            return Err(AppError::invalid(format!(
                "MCP server name {name:?} must use letters, digits, '-' or '_'"
            )));
        }
        if name == NODE_TOOLS_SERVER {
            return Err(AppError::invalid(format!(
                "MCP server name {NODE_TOOLS_SERVER:?} is reserved for the node tools"
            )));
        }
        if self.command.trim().is_empty() {
            return Err(AppError::invalid(format!(
                "MCP server {name:?} needs a command"
            )));
        }
        if let Some(key) = self.env.keys().find(|key| !is_env_name(key)) {
            return Err(AppError::invalid(format!(
                "MCP server {name:?} has an invalid environment variable name {key:?}"
            )));
        }
        Ok(())
    }
}

/// An interactive program that replaces the agent in the node's terminal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramConfig {
    pub argv: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandConfig {
    pub argv: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HumanConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InboxConfig {}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalConfig {}

fn is_env_name(name: &str) -> bool {
    name.bytes()
        .next()
        .is_some_and(|byte| !byte.is_ascii_digit())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}
