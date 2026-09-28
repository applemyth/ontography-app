//! Workflow node types: each node's role, declared in core's schema.
//!
//! Core treats node types as role labels. It admits a node only with declared
//! types, and a rewrite must name a node's exact types, so a node keeps its
//! type for life: changing it replaces the node. What a node runs comes from
//! its component, never from its type.

use serde::{Deserialize, Serialize};

#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    Deserialize,
    schemars::JsonSchema,
)]
pub enum NodeType {
    /// Works on tasks in a continuing session.
    Agent,
    /// Runs a program once per task.
    Command,
    /// Waits for a person's decision on each task.
    Human,
    /// Holds incoming work for inspection and export.
    Inbox,
}

impl NodeType {
    pub const ALL: [Self; 4] = [Self::Agent, Self::Command, Self::Human, Self::Inbox];

    /// The type's name in core's schema.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Agent => "Agent",
            Self::Command => "Command",
            Self::Human => "Human",
            Self::Inbox => "Inbox",
        }
    }

    pub fn from_core(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == name)
    }

    /// Agent and command nodes run workers that take tasks, so only they have
    /// retry policies, grants, and node tools.
    pub const fn runs_tasks(self) -> bool {
        matches!(self, Self::Agent | Self::Command)
    }
}

impl std::fmt::Display for NodeType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}
