//! A process and tool boundary around Ontography's public core API.

pub mod application;
pub mod catalog;
pub mod client;
pub mod declarations;
pub mod definition;
pub mod error;
pub mod launcher;
pub mod logging;
pub mod managed_shell;
pub mod migration;
pub mod node_runtime;
pub mod node_tool;
pub mod persistence;
pub mod protocol;
pub mod registry;
pub mod server;
pub mod session_runtime;
pub mod sessions;
pub mod state;
pub mod terminal;
pub mod terminal_client;
pub mod tools;
pub mod ui;
pub mod views;
pub mod workflow;
pub mod workspace;

pub use error::{AppError, Result};

pub const CORE_BUILD: &str = env!("ONTOGRAPHY_CORE_BUILD");
pub const APP_BUILD: &str = env!("ONTOGRAPHY_APP_BUILD");

#[cfg(test)]
mod state_tests;
