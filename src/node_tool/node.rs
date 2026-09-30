//! The node itself and the work waiting for it. These tools need no attempt,
//! so they return metadata only, never payload bytes. Each reply carries a
//! `version`; wait_for_change returns at once when it no longer matches.

use super::context::{Names, Successor};
use super::{NodeToolContext, Reply, Tool};
use crate::workflow::tasks::{self, Held, Standing, Task, TaskKey, work_id};
use crate::{AppError, Result};
use ontography::{IngressMode, PackageId, PackageRecord};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use std::time::Duration;
use tokio::time::Instant;

const MAX_WAIT_MS: u64 = 30_000;

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Nothing {}

pub(super) struct InspectNode;

impl Tool for InspectNode {
    const NAME: &'static str = "inspect_node";
    const DESCRIPTION: &'static str = "Read this node's name, types, component, settings, join, grants, retry policy, result contract, root and authority transitions, neighbors, outgoing connections with the contract and authority each accepts, and open attempts.";
    const MUTATING: bool = false;
    type Input = Nothing;

    async fn run(context: &NodeToolContext, _: Nothing) -> Result<Reply> {
        let version = context.version();
        let (kernel, names) = context.graph().await?;
        let scope = context.scope();
        let node = &scope.node;
        let successors = context.successors(&kernel, &names);
        Reply::plain(&json!({
            "node": node.id,
            "types": scope.binding.types,
            "component": node.component,
            "config": node.config,
            "join": node.join,
            "grants": node.grants,
            "tools": context.catalog().iter().map(|tool| tool.name).collect::<Vec<_>>(),
            "retry": node.retry_policy(),
            "entry": scope.document().entry == node.id,
            "result": scope.document().result_contract(node),
            "root": kernel.root_ceiling(context.node_id()).map(tags),
            "transitions": node.transitions,
            "incoming": context.predecessors(&kernel, &names),
            "outgoing": successors.iter().map(|successor| &successor.to).collect::<Vec<_>>(),
            "connections": successors.iter().map(|successor| connection(&kernel, successor)).collect::<Vec<_>>(),
            "open_attempts": context.open_attempts(),
            "initial_pending": context.initial()?.is_some(),
            "version": version,
        }))
    }
}

fn tags(authority: &ontography::Authority) -> Vec<&str> {
    authority.tags().map(ontography::AuthorityTag::id).collect()
}

/// What an outgoing connection accepts: its contract and object type, and the
/// authority tags it admits, any or all of them.
fn connection(kernel: &ontography::Kernel, successor: &Successor) -> Value {
    let definition = kernel.edge_definition(&successor.edge);
    let contract = definition.map(|edge| edge.package_contract());
    json!({
        "name": successor.connection,
        "to": successor.to,
        "contract": contract,
        "object_type": contract.and_then(|id| kernel.contract(id)).map(|contract| contract.object_type()),
        "authority": definition.map(|edge| edge.authority_tags().iter().map(ontography::AuthorityTag::id).collect::<Vec<_>>()),
        "match": definition.map(|edge| match edge.authority_match() {
            ontography::AuthorityMatch::AnyOf => "any_of",
            ontography::AuthorityMatch::AllOf => "all_of",
        }),
    })
}

pub(super) struct InspectGraph;

impl Tool for InspectGraph {
    const NAME: &'static str = "inspect_graph";
    const DESCRIPTION: &'static str =
        "Read the current workflow graph: its nodes and directed connections, by name.";
    const MUTATING: bool = false;
    type Input = Nothing;

    async fn run(context: &NodeToolContext, _: Nothing) -> Result<Reply> {
        let version = context.version();
        let (kernel, names) = context.graph().await?;
        let graph = kernel.graph();
        Reply::plain(&json!({
            "nodes": graph.nodes().iter().map(|node| names.label(node.id())).collect::<Vec<_>>(),
            "edges": graph.edges().iter().map(|edge| json!({
                "name": names.connection(edge.id()),
                "from": names.label(edge.source()),
                "to": names.label(edge.target()),
            })).collect::<Vec<_>>(),
            "version": version,
        }))
    }
}

/// One page of packages this node holds, in core's stable order. Packages
/// added or taken between pages may be missed; read again when that matters.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Page {
    /// Continue after the previous page: its `next_after`.
    #[serde(default)]
    after: Option<String>,
    /// Entries per page, 1 to 100.
    #[serde(default = "default_limit")]
    limit: usize,
}

const fn default_limit() -> usize {
    20
}

/// A page read by `Page::read`.
pub(super) struct Packages {
    pub packages: Vec<(PackageId, PackageRecord)>,
    pub next_after: Option<String>,
}

impl Page {
    pub(super) async fn read(&self, context: &NodeToolContext, held: Held) -> Result<Packages> {
        if !(1..=100).contains(&self.limit) {
            return Err(AppError::invalid("limit must be between 1 and 100"));
        }
        let cursor = self
            .after
            .as_deref()
            .map(|token| context.resume(held, token))
            .transpose()?;
        let node = context.node_id();
        let page = match held {
            Held::Received => {
                context
                    .session
                    .pending_page_at(node, cursor, self.limit + 1)
                    .await
            }
            Held::Outbound => {
                context
                    .session
                    .outbound_page(Some(node), cursor, self.limit + 1)
                    .await
            }
        }
        .map_err(AppError::core)?;
        let mut packages = page.packages().to_vec();
        let next_after = (packages.len() > self.limit).then(|| {
            packages.truncate(self.limit);
            context.cursor(held, packages[self.limit - 1].0)
        });
        Ok(Packages {
            packages,
            next_after,
        })
    }
}

/// The package with this `work_id`, if this node holds it that way.
pub(super) async fn find(
    context: &NodeToolContext,
    held: Held,
    work: &str,
) -> Result<Option<(PackageId, PackageRecord)>> {
    let work = tasks::parse_work_id(work)?;
    tasks::find_held(&context.session, context.node_id(), held, |id, _| {
        work_id(id) == work
    })
    .await
}

pub(super) fn stale_work() -> AppError {
    AppError::new(
        "stale_work",
        "This package is no longer held at this node; list again",
    )
}

pub(super) struct ListInputs;

impl Tool for ListInputs {
    const NAME: &'static str = "list_inputs";
    const DESCRIPTION: &'static str = "Page through the inputs waiting at this node, in stable order. At an `any` node each input is its own task, shown with its retry state.";
    const MUTATING: bool = false;
    type Input = Page;

    async fn run(context: &NodeToolContext, page: Page) -> Result<Reply> {
        let version = context.version();
        let found = page.read(context, Held::Received).await?;
        let (kernel, names) = context.graph().await?;
        let node = context.node_id();
        let tasks_per_input = kernel
            .node_definition(node)
            .is_some_and(|definition| definition.ingress_mode() == IngressMode::Any);
        let busy = context.busy();
        let mut inputs = Vec::with_capacity(found.packages.len());
        for (id, record) in &found.packages {
            let mut input = describe_input(context, &names, id, record).await?;
            if tasks_per_input {
                let key = TaskKey::new(node, &[*id]);
                let (state, retry_in) = if busy.iter().any(|task| task.key == key) {
                    ("in_progress", None)
                } else {
                    match context.ledger.standing(&key) {
                        Standing::Ready => ("ready", None),
                        Standing::Waiting(until) => ("retrying", Some(seconds_until(until))),
                        Standing::Parked => ("parked", None),
                    }
                };
                input["task_id"] = json!(key);
                input["state"] = json!(state);
                if let Some(seconds) = retry_in {
                    input["retry_in_secs"] = json!(seconds);
                }
            }
            inputs.push(input);
        }
        Reply::plain(&json!({
            "inputs": inputs,
            "next_after": found.next_after,
            "version": version,
        }))
    }
}

pub(super) struct NextTrigger;

impl Tool for NextTrigger {
    const NAME: &'static str = "next_trigger";
    const DESCRIPTION: &'static str = "Find the next task this node can begin: the initial input, one waiting input, or a complete join. Skips tasks that are in progress, waiting to retry, or parked.";
    const MUTATING: bool = false;
    type Input = Nothing;

    async fn run(context: &NodeToolContext, _: Nothing) -> Result<Reply> {
        // Both read before choosing, so neither misses what changes meanwhile.
        let version = context.version();
        let wake = context.ledger.next_wake();
        let Some(task) = context.tasks(&context.busy())?.next().await? else {
            return Reply::plain(&json!({
                "task_id": null,
                "retry_in_secs": wake.map(seconds_until),
                "version": version,
            }));
        };
        let (_, names) = context.graph().await?;
        let mut described = describe_task(context, &names, &task).await?;
        described["version"] = json!(version);
        Reply::plain(&described)
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Wait {
    /// The `version` of an earlier reply; the wait ends at once if anything
    /// changed since.
    #[serde(default)]
    after: Option<String>,
    /// Longest wait in milliseconds, 1 to 30000.
    #[serde(default = "default_wait")]
    timeout_ms: u64,
}

const fn default_wait() -> u64 {
    MAX_WAIT_MS
}

pub(super) struct WaitForChange;

impl Tool for WaitForChange {
    const NAME: &'static str = "wait_for_change";
    const DESCRIPTION: &'static str = "Wait up to timeout_ms for a change: to the graph or any pending work in the run (frontier), a retry becoming due or granted (retry), this node's settings (settings), a stop request (stopping), or the end of the run's session (session_ended). Pass `after`, the version of your last reply, to also catch changes made before the wait. Read state again after waking.";
    const MUTATING: bool = false;
    type Input = Wait;

    async fn run(context: &NodeToolContext, wait: Wait) -> Result<Reply> {
        if !(1..=MAX_WAIT_MS).contains(&wait.timeout_ms) {
            return Err(AppError::invalid("timeout_ms must be between 1 and 30000"));
        }
        // Subscribe before comparing, so no change falls between the two.
        let mut frontier = context.session.frontier();
        let mut retries = context.ledger.subscribe();
        let mut settings = context.scope_changes();
        settings.mark_unchanged();
        let mut stop = context.execution.stop();
        let version = context.version();
        if wait.after.as_ref().is_some_and(|after| *after != version) {
            return Reply::plain(&json!({"reason": "changed", "version": version}));
        }
        // Changes win over a timeout that falls due with them.
        let reason = tokio::select! {
            biased;
            _ = frontier.changed() => "frontier",
            _ = retries.changed() => "retry",
            () = tasks::wake_at(context.ledger.next_wake()) => "retry",
            _ = settings.changed() => "settings",
            () = stop.requested() => "stopping",
            _ = context.session.wait_closed() => "session_ended",
            () = tokio::time::sleep(Duration::from_millis(wait.timeout_ms)) => "timeout",
        };
        // A timeout reports nothing, so its version must not absorb a change
        // made since the wait began; the next wait then reports it.
        let version = if reason == "timeout" {
            version
        } else {
            context.version()
        };
        Reply::plain(&json!({"reason": reason, "version": version}))
    }
}

/// A task by its handles and the nodes that sent its inputs.
async fn describe_task(context: &NodeToolContext, names: &Names, task: &Task) -> Result<Value> {
    let mut inputs = Vec::with_capacity(task.inputs.len());
    for (id, record) in &task.inputs {
        inputs.push(describe_input(context, names, id, record).await?);
    }
    Ok(json!({"task_id": task.key, "initial": task.is_initial(), "inputs": inputs}))
}

async fn describe_input(
    context: &NodeToolContext,
    names: &Names,
    id: &PackageId,
    record: &PackageRecord,
) -> Result<Value> {
    let bytes = context
        .execution
        .content_size(record.content_digest())
        .await
        .map_err(AppError::core)?;
    Ok(json!({"work_id": work_id(id), "from": names.label(record.producer_node()), "bytes": bytes}))
}

pub(super) fn seconds_until(until: Instant) -> u64 {
    tasks::ceil_secs(until.saturating_duration_since(Instant::now()))
}
