//! Components: pre-configured nodes that a workflow document places.
//!
//! This follows core's project model. A component describes a node (its core
//! node type) and binds each placement's settings to a trusted implementation
//! and its exact configuration.

mod config;

pub use config::{
    AgentConfig, Binding, CommandConfig, HumanConfig, Implementation, McpServer,
    NODE_TOOLS_SERVER, ProgramConfig,
};
