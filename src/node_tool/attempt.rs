//! Beginning and ending attempts. An attempt is one recorded try at a task, or
//! at work the node originates; it ends when core decides its submission, or
//! when it fails.

use super::context::{Attempt, AttemptState, Names, context_error, successor_edge, warn};
use super::node::seconds_until;
use super::outputs::{Source, resolve};
use super::{NodeToolContext, Reply, Tool};
use crate::workflow::document::OBJECT_TYPE;
use crate::workflow::tasks::{Standing, Task, TaskKey};
use crate::workflow::{BoundNode, Grant, WorkflowPayload};
use crate::{AppError, Result, persistence, views};
use ontography::{
    Authority, ContentId, ContextError, ContextMode, ContextPolicy, Emission, InitialContext,
    InvocationTrigger, OutputAuthority, PackageEnvelope, Payload, ProposalDecision,
    ResolvedEntryKind, SessionStatus, content::BlobFormat, content::Hash,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashSet;

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Begin {
    /// A task from next_trigger or list_inputs.
    #[serde(default)]
    pub(super) task_id: Option<String>,
    /// Instead, start new work with this message. Needs the originate grant.
    #[serde(default)]
    pub(super) originate: Option<String>,
}

pub(super) struct BeginInvocation;

impl Tool for BeginInvocation {
    const NAME: &'static str = "begin_invocation";
    const DESCRIPTION: &'static str = "Begin a recorded attempt at a task, or originate new work with a message. Returns the attempt ID and handles for its inputs; read them with the package tools.";
    const MUTATING: bool = true;
    type Input = Begin;

    async fn run(context: &NodeToolContext, begin: Begin) -> Result<Reply> {
        begin_attempt(context, begin, false).await
    }
}

pub(super) async fn begin_attempt(
    context: &NodeToolContext,
    begin: Begin,
    delivery: bool,
) -> Result<Reply> {
    let _exclusive = context.exclusive().await?;
    // The attempt counts against the definition it begins under, even
    // one the host has not renewed the node's retry ledger with yet.
    let node = context.scope().bound_node();
    context.ledger.renew(&node.digest())?;
    let (trigger, contents, task) = match (begin.task_id, begin.originate) {
        (Some(key), None) => {
            let task = runnable_task(context, &TaskKey::parse(&key)?).await?;
            if task.is_initial() {
                let input = context.initial()?.ok_or_else(stale_task)?;
                let contents =
                    crate::workflow::tools::dependencies(&context.session, &input).await?;
                let authority = root_authority(context).await?;
                (
                    InvocationTrigger::Root { authority, input },
                    contents,
                    Some(task),
                )
            } else {
                (
                    InvocationTrigger::Packages(task.ids()),
                    Vec::new(),
                    Some(task),
                )
            }
        }
        (None, Some(message)) => {
            context.require(Grant::Originate)?;
            // Until the initial input is done, an accepted root at the entry
            // must be that input's: restart recovery reads it that way.
            if context.initial()?.is_some() {
                return Err(AppError::new(
                    "initial_pending",
                    "Begin the initial input before originating new work",
                ));
            }
            let input = WorkflowPayload::Message { message }.encode()?;
            let authority = root_authority(context).await?;
            (
                InvocationTrigger::Root { authority, input },
                Vec::new(),
                None,
            )
        }
        _ => {
            return Err(AppError::invalid(
                "Supply exactly one of task_id or originate",
            ));
        }
    };
    let policy = ContextPolicy {
        mode: ContextMode::Explorable,
        initial: InitialContext::None,
        ..ContextPolicy::default()
    };
    let invocation = match context
        .execution
        .begin_invocation_with_content(trigger, policy, contents)
        .await
    {
        Ok(invocation) => invocation,
        Err(error) => return Err(refused(context, &node, task.as_ref(), error).await?),
    };
    let attempt = context.register(invocation, task, node).await?;
    context
        .with_attempt(&attempt.id, async |attempt, state| {
            let mut reply = async {
                let (_, names) = context.graph().await?;
                let inputs = describe_inputs(context, attempt, &names).await?;
                if delivery {
                    return super::delivery::record(context, attempt, inputs).await;
                }
                Reply::record(
                    attempt,
                    BeginInvocation::NAME,
                    &json!({
                        "attempt_id": attempt.id,
                        "task_id": attempt.task.as_ref().map(|task| &task.key),
                        "initial": attempt.task.as_ref().is_some_and(Task::is_initial),
                        "inputs": inputs,
                    }),
                )
                .await
            }
            .await;
            if let Err(error) = &mut reply {
                if delivery && !context.execution.stop().is_requested() {
                    // Undeliverable input must not crash/restart the whole
                    // agent repeatedly. Persist its task failure first.
                    let retry = context.record_failure(attempt, &error.message, false)?;
                    error.details = Some(json!({"retry":retry}));
                }
                // Nobody learned this attempt's ID, so nobody could end it.
                let _ = attempt.invocation.interrupt("begin reply failed").await;
                context
                    .finish(attempt, state, json!({"status":"interrupted"}))
                    .await;
            }
            reply
        })
        .await
}

/// Core refused to begin a task: it cannot run as delivered, for example
/// because it exceeds the attempt's context budget. Retrying cannot help,
/// unless an input was taken meanwhile and the task no longer exists.
async fn refused(
    context: &NodeToolContext,
    node: &BoundNode,
    task: Option<&Task>,
    error: ContextError,
) -> Result<AppError> {
    let task_level = matches!(
        error,
        ContextError::Denied(_) | ContextError::Budget(_) | ContextError::NotFound
    );
    let error = context_error(error);
    let Some(task) = task.filter(|_| task_level) else {
        return Ok(error);
    };
    if context.tasks(&[])?.current(&task.ids()).await?.is_none() {
        return Ok(stale_task());
    }
    Ok(
        match context.ledger.record_failure(
            task,
            &error.message,
            false,
            &node.node.retry_policy(),
            &node.digest(),
        ) {
            Ok(retry) => error.details(json!({"retry": retry})),
            Err(ledger) => error.details(json!({"warnings": [ledger.to_string()]})),
        },
    )
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Submit {
    attempt_id: String,
    /// The attempt's recorded result.
    result: Content,
    /// Exactly what to send. Omitted, the result goes to every successor; an
    /// empty list sends nothing.
    #[serde(default)]
    outputs: Option<Vec<Output>>,
}

/// A message, or a workspace: the handle of a directory output of this
/// attempt, or of one of its workspace inputs.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Content {
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    workspace: Option<String>,
}

/// One package to send: a message or a workspace, as in `Content`.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Output {
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    workspace: Option<String>,
    /// Deliver to this successor. Omitted, the output goes to every successor.
    #[serde(default)]
    to: Option<String>,
    /// Keep the package here to send later with transfer_package. Needs the
    /// send_later grant.
    #[serde(default)]
    outbound: bool,
}

pub(super) struct SubmitInvocation;

impl Tool for SubmitInvocation {
    const NAME: &'static str = "submit_invocation";
    const DESCRIPTION: &'static str = "Finish an attempt by submitting its result and outputs. Invalid arguments leave the attempt open; once core decides, accepting or rejecting the whole submission, the attempt ends. A rejected task is retried per the node's retry policy.";
    const MUTATING: bool = true;
    type Input = Submit;

    async fn run(context: &NodeToolContext, submit: Submit) -> Result<Reply> {
        let Submit {
            attempt_id,
            result,
            outputs,
        } = submit;
        context
            .with_attempt(&attempt_id, async |attempt, state| {
                let outcome = submit_attempt(context, attempt, state, result, outputs).await?;
                Reply::plain(&outcome)
            })
            .await
    }
}

async fn submit_attempt(
    context: &NodeToolContext,
    attempt: &Attempt,
    state: &mut AttemptState,
    result: Content,
    outputs: Option<Vec<Output>>,
) -> Result<Value> {
    // Validation errors leave the attempt open for a corrected submission.
    let mut contents = Dependencies::default();
    let Content { message, workspace } = result;
    let result = encode(attempt, state, message, workspace, &mut contents).await?;
    let (kernel, names) = context.graph().await?;
    let successors = context.successors(&kernel, &names);
    let mut emissions = Vec::new();
    match outputs {
        None => emissions.extend(route(&successors, None, &result)?),
        Some(outputs) => {
            for output in outputs {
                if output.outbound {
                    context.require(Grant::SendLater)?;
                    if output.to.is_some() {
                        return Err(AppError::invalid(
                            "An outbound output stays here; it has no destination",
                        ));
                    }
                }
                let payload = encode(
                    attempt,
                    state,
                    output.message,
                    output.workspace,
                    &mut contents,
                )
                .await?;
                if output.outbound {
                    emissions.push(Emission::outbound(
                        OBJECT_TYPE,
                        OutputAuthority::Carry,
                        payload,
                    ));
                } else {
                    emissions.extend(route(&successors, output.to.as_deref(), &payload)?);
                }
            }
        }
    }
    let record = json!({
        "invocation_id": attempt.id,
        "node": context.scope().node.id,
        "result": WorkflowPayload::read(&result)?,
    });
    cache_output(context, &record, "prepared")?;
    // From here the attempt ends, whatever core decides. Core records nothing
    // for it afterwards, so replies still on their way are marked sent first.
    attempt.drain_replies().await;
    let submitted = attempt
        .invocation
        .submit(result, emissions, contents.ids)
        .await;
    let outcome = match submitted {
        Ok(ProposalDecision::Committed(activation)) => {
            let mut outcome = json!({"status": "accepted", "attempt_id": attempt.id});
            if let Some(task) = &attempt.task {
                if task.is_initial() {
                    warn(&mut outcome, context.complete_initial());
                }
                warn(&mut outcome, context.ledger.clear(&task.key));
            }
            let mut record = record;
            record["activation_id"] = json!(activation.to_string());
            // Core has committed; the cache is advisory and must never turn an
            // accepted task into a retry.
            let _ = cache_output(context, &record, "committed");
            outcome
        }
        Ok(ProposalDecision::Rejected(reason)) => {
            let reason = format!(
                "Core rejected the submission: {}",
                views::rejection(&reason)
            );
            let mut outcome =
                json!({"status": "rejected", "attempt_id": attempt.id, "reason": reason});
            outcome["retry"] = json!(warn(
                &mut outcome,
                context.record_failure(attempt, &reason, true)
            ));
            outcome
        }
        Err(error) => {
            // Core ended the invocation, or it can no longer be used. A stop
            // interrupts the attempt; anything else is its failure.
            let stopping = context.execution.stop().is_requested()
                || context.session.status() != SessionStatus::Open;
            let error = context_error(error);
            let mut outcome = json!({"attempt_id": attempt.id,
                "status": if stopping { "interrupted" } else { "failed" }});
            if !stopping {
                outcome["retry"] = json!(warn(
                    &mut outcome,
                    context.record_failure(attempt, &error.message, true)
                ));
            }
            return Err(error.details(context.finish(attempt, state, outcome).await));
        }
    };
    Ok(context.finish(attempt, state, outcome).await)
}

/// Emissions carrying `payload` to the named successor, or to every successor.
fn route(
    successors: &[(String, String)],
    to: Option<&str>,
    payload: &Payload,
) -> Result<Vec<Emission>> {
    let emission = |edge: &str| Emission::new(edge, OutputAuthority::Carry, payload.clone());
    Ok(match to {
        Some(to) => vec![emission(successor_edge(successors, to)?)],
        None => successors.iter().map(|(edge, _)| emission(edge)).collect(),
    })
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Fail {
    attempt_id: String,
    /// Why the attempt failed.
    reason: String,
    /// Whether trying again could help; false parks the task immediately.
    #[serde(default = "retryable")]
    retryable: bool,
}

const fn retryable() -> bool {
    true
}

pub(super) struct FailInvocation;

impl Tool for FailInvocation {
    const NAME: &'static str = "fail_invocation";
    const DESCRIPTION: &'static str = "End an attempt as failed. Its task is retried per the node's retry policy, or parked for the manager when retryable is false or attempts are used up.";
    const MUTATING: bool = true;
    type Input = Fail;

    async fn run(context: &NodeToolContext, fail: Fail) -> Result<Reply> {
        context
            .with_attempt(&fail.attempt_id, async |attempt, state| {
                // The ledger comes first: if the process dies next, core
                // interrupts the invocation on restart and the task stays counted.
                let retry = context.record_failure(attempt, &fail.reason, fail.retryable)?;
                let mut outcome =
                    json!({"status": "failed", "attempt_id": attempt.id, "retry": retry});
                attempt.drain_replies().await;
                warn(
                    &mut outcome,
                    attempt
                        .invocation
                        .fail(&fail.reason)
                        .await
                        .map_err(context_error),
                );
                Reply::plain(&context.finish(attempt, state, outcome).await)
            })
            .await
    }
}

/// Resolves a task by key and checks that it may begin now.
async fn runnable_task(context: &NodeToolContext, key: &TaskKey) -> Result<Task> {
    let busy = context.busy();
    if busy.iter().any(|task| task.key == *key) {
        return Err(AppError::new(
            "task_in_progress",
            "This task already has an open attempt",
        ));
    }
    let task = context
        .tasks(&busy)?
        .find(key)
        .await?
        .ok_or_else(stale_task)?;
    match context.ledger.standing(&task.key) {
        Standing::Ready => Ok(task),
        Standing::Waiting(until) => Err(AppError::new(
            "task_retrying",
            format!(
                "This task failed recently; it can be retried in {} seconds",
                seconds_until(until)
            ),
        )),
        Standing::Parked => Err(AppError::new(
            "task_parked",
            "This task is parked until the manager retries or discards it",
        )),
    }
}

async fn root_authority(context: &NodeToolContext) -> Result<Authority> {
    context
        .execution
        .kernel()
        .await
        .map_err(AppError::core)?
        .root_ceiling(context.node_id())
        .cloned()
        .ok_or_else(|| {
            AppError::new(
                "not_a_root",
                "This node has no root rule, so it cannot start new work",
            )
        })
}

/// The input handles of a new attempt: its received packages, or its root input.
async fn describe_inputs(
    context: &NodeToolContext,
    attempt: &Attempt,
    names: &Names,
) -> Result<Vec<Value>> {
    let is_workspace = |handle: &str| {
        attempt.member(handle).is_some_and(|member| {
            member.path.is_empty() && matches!(member.kind, ResolvedEntryKind::Directory)
        })
    };
    let mut inputs = Vec::new();
    for grant in attempt
        .invocation
        .packages()
        .iter()
        .filter(|grant| grant.received)
    {
        let from = attempt
            .task
            .iter()
            .flat_map(|task| &task.inputs)
            .find(|(id, _)| *id == grant.package_id)
            .map(|(_, record)| names.label(record.producer_node()));
        let bytes = context
            .execution
            .content_size(grant.content_digest)
            .await
            .map_err(AppError::core)?;
        inputs.push(json!({
            "handle": grant.handle,
            "from": from,
            "workspace": is_workspace(&grant.handle),
            "bytes": bytes,
        }));
    }
    if let Some((handle, digest)) = &attempt.root_input {
        let bytes = context
            .execution
            .content_size(*digest)
            .await
            .map_err(AppError::core)?;
        inputs.push(json!({"handle": handle, "from": null, "workspace": false, "bytes": bytes}));
    }
    // A root input naming a package appears as the root member of its view.
    for view in attempt
        .invocation
        .views()
        .iter()
        .filter(|view| attempt.grant(&view.owner).is_none())
    {
        let Some(member) = attempt.member(&view.owner) else {
            continue;
        };
        let mut input = json!({"handle": view.owner, "from": null,
            "workspace": matches!(member.kind, ResolvedEntryKind::Directory)});
        if let ResolvedEntryKind::File { content, .. } = &member.kind {
            input["bytes"] = json!(content.size());
        }
        inputs.push(input);
    }
    Ok(inputs)
}

/// Content dependencies declared with a submission, without duplicates.
#[derive(Default)]
struct Dependencies {
    ids: Vec<ContentId>,
    seen: HashSet<(Hash, BlobFormat, u64)>,
}

impl Dependencies {
    fn extend(&mut self, ids: impl IntoIterator<Item = ContentId>) {
        for id in ids {
            if self.seen.insert((id.hash(), id.format(), id.size())) {
                self.ids.push(id);
            }
        }
    }
}

/// Encodes a message or a workspace, collecting what a workspace depends on.
async fn encode(
    attempt: &Attempt,
    state: &AttemptState,
    message: Option<String>,
    workspace: Option<String>,
    dependencies: &mut Dependencies,
) -> Result<Payload> {
    match (message, workspace) {
        (Some(message), None) => WorkflowPayload::Message { message }.encode(),
        (None, Some(handle)) => {
            let source = resolve(attempt, state, &handle)?;
            if !source.is_directory() {
                return Err(AppError::invalid(
                    "Only a directory can be sent as a workspace; compose files into one first",
                ));
            }
            let envelope = PackageEnvelope::new(source.root());
            match source {
                Source::Output(output) => dependencies.extend(output.dependencies.iter().copied()),
                // Core confirms the input is in this attempt's grants.
                Source::Input(_) => dependencies.extend(
                    attempt
                        .invocation
                        .validate_worker_output(&envelope.to_payload().map_err(AppError::core)?)
                        .await
                        .map_err(context_error)?,
                ),
            }
            WorkflowPayload::Workspace(envelope).encode()
        }
        _ => Err(AppError::invalid(
            "Supply exactly one of message or workspace",
        )),
    }
}

/// The node's latest result, as the manager's output view reads it.
fn cache_output(context: &NodeToolContext, record: &Value, status: &str) -> Result<()> {
    let mut record = record.clone();
    record["publication_status"] = json!(status);
    persistence::write_json(&context.directory.join("output.json"), &record)
}

fn stale_task() -> AppError {
    AppError::new(
        "stale_task",
        "This task is no longer waiting at this node; ask next_trigger again",
    )
}
