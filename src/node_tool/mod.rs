//! Operations a node's worker calls on its own node.
//!
//! Every call runs for one execution at one node. Nodes and connections appear
//! by their workflow names, and pending work by opaque handles, never by core
//! identity. The base tools let any node read its inputs, run attempts at its
//! tasks, and publish results. A node's grants add the tools that create or
//! discard work.
//!
//! A reply is the exact bytes a transport sends. While an attempt stays open,
//! each of its successful replies is recorded as a core receipt before it is
//! returned, and the transport marks the receipt sent after writing it, so
//! core's evidence names exactly what the worker saw. Replies outside attempts
//! carry metadata only: no payload bytes leave this module unrecorded.

mod attempt;
mod context;
mod granted;
mod node;
mod outputs;
mod read;
mod reply;
#[cfg(test)]
mod tests;
mod workspace;

pub use context::{NodeScope, NodeToolContext};
pub use reply::Reply;

use crate::workflow::Grant;
use crate::{AppError, Result};
use futures_util::future::BoxFuture;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::future::Future;

/// One node tool, implemented once per operation.
trait Tool {
    const NAME: &'static str;
    const DESCRIPTION: &'static str;
    /// The grant a node needs to see and call this tool; `None` for the base set.
    const GRANT: Option<Grant> = None;
    /// Whether a call can change graph, attempt, content, or workspace state.
    const MUTATING: bool;
    type Input: DeserializeOwned + JsonSchema + Send;

    fn run(
        context: &NodeToolContext,
        input: Self::Input,
    ) -> impl Future<Output = Result<Reply>> + Send;
}

/// A tool as a transport lists and calls it.
pub struct NodeTool {
    pub name: &'static str,
    pub description: &'static str,
    pub grant: Option<Grant>,
    pub mutating: bool,
    parameters: fn() -> Value,
    run: for<'a> fn(&'a NodeToolContext, Value) -> BoxFuture<'a, Result<Reply>>,
}

impl NodeTool {
    const fn of<T: Tool>() -> Self {
        Self {
            name: T::NAME,
            description: T::DESCRIPTION,
            grant: T::GRANT,
            mutating: T::MUTATING,
            parameters: crate::catalog::schema::<T::Input>,
            run: run::<T>,
        }
    }

    /// The JSON schema of this tool's arguments.
    pub fn parameters(&self) -> Value {
        (self.parameters)()
    }

    /// Name, description, argument schema, and whether the tool mutates.
    pub fn describe(&self) -> Value {
        json!({"name":self.name,"description":self.description,
            "parameters":self.parameters(),"mutating":self.mutating})
    }
}

fn run<T: Tool>(context: &NodeToolContext, args: Value) -> BoxFuture<'_, Result<Reply>> {
    Box::pin(async move {
        let args = if args.is_null() { json!({}) } else { args };
        let input = serde_json::from_value(args)
            .map_err(|error| AppError::invalid(format!("{}: {error}", T::NAME)))?;
        T::run(context, input).await
    })
}

static TOOLS: [NodeTool; 19] = [
    NodeTool::of::<node::InspectNode>(),
    NodeTool::of::<node::InspectGraph>(),
    NodeTool::of::<node::ListInputs>(),
    NodeTool::of::<node::NextTrigger>(),
    NodeTool::of::<node::WaitForChange>(),
    NodeTool::of::<attempt::BeginInvocation>(),
    NodeTool::of::<read::DescribePackage>(),
    NodeTool::of::<read::ListPackage>(),
    NodeTool::of::<read::ReadPackage>(),
    NodeTool::of::<outputs::ImportContent>(),
    NodeTool::of::<outputs::ComposePackage>(),
    NodeTool::of::<workspace::OpenWorkspace>(),
    NodeTool::of::<workspace::CaptureWorkspace>(),
    NodeTool::of::<workspace::ReleaseWorkspace>(),
    NodeTool::of::<attempt::SubmitInvocation>(),
    NodeTool::of::<attempt::FailInvocation>(),
    NodeTool::of::<granted::ListOutbound>(),
    NodeTool::of::<granted::TransferPackage>(),
    NodeTool::of::<granted::RetirePackage>(),
];

/// Every node tool, including those that need a grant.
pub fn tools() -> &'static [NodeTool] {
    &TOOLS
}

impl NodeToolContext {
    /// The tools this node may call, given its current grants.
    pub fn catalog(&self) -> Vec<&'static NodeTool> {
        tools()
            .iter()
            .filter(|tool| tool.grant.is_none_or(|grant| self.granted(grant)))
            .collect()
    }

    /// Runs one call. Send the reply's bytes unchanged, then call `Reply::sent`.
    pub async fn call(&self, name: &str, args: Value) -> Result<Reply> {
        let tool = tools()
            .iter()
            .find(|tool| tool.name == name)
            .ok_or_else(|| {
                AppError::new(
                    "unknown_tool",
                    format!("There is no node tool named {name:?}"),
                )
            })?;
        if let Some(grant) = tool.grant {
            self.require(grant)?;
        }
        (tool.run)(self, args).await
    }
}
