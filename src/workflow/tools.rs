//! The manager's complete workflow interface. Core identities stay behind it.

use super::{
    Binding, Catalog, Document, DocumentNode, IdentityMap, Implementation, WorkflowPayload,
    artifacts, edit, expand, output, runtime,
    tasks::{self, RetryLedger, TaskKey},
};
use crate::{
    AppError, Result,
    catalog::Operation,
    persistence,
    state::{ManagedRun, Service},
    views,
};
use ontography::{
    ContextPolicy, Emission, InvocationTrigger, OutputAuthority, PackageId, PackageStore, Payload,
    ProposalDecision, Reject, RetireError,
};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

pub fn operations() -> Vec<Operation> {
    let text = json!({"type":"string"});
    let document = crate::catalog::schema::<Document>();
    let run = json!({"run_id":text});
    let task = json!({"run_id":text,"node":text,"task_id":text});
    let source = json!({"enum":["auto","output","pending"]});
    vec![
        Operation::new(
            "flow.library",
            "List the components a document's nodes can place, with their node types and settings, the validators contracts can use, and the library's MCP servers.",
            json!({}),
            &[],
            false,
        ),
        Operation::new(
            "flow.define",
            "Validate and save a reusable workflow document.",
            json!({"document":document}),
            &["document"],
            true,
        ),
        Operation::new(
            "flow.start",
            "Start a document or saved revision. Supply message or a workspace directory as the entry's initial input; an external entry takes none. A start_id makes unscoped retries idempotent.",
            json!({"document":document,"revision":text,"project":text,"message":text,"workspace":text,"start_id":text}),
            &["project"],
            true,
        ),
        Operation::new(
            "flow.status",
            "Read the document, pending edit, workers, ready tasks and failed tasks using workflow names.",
            run.clone(),
            &["run_id"],
            false,
        ),
        Operation::new(
            "flow.output",
            "Read the current human task, latest worker output, or a page of inbox items. Use source:output for a previous result, task_id for a specific ready task, or work_id for an exact pending item. Inbox pages use stable identity order, not arrival order; pass revision with after to continue.",
            json!({"run_id":text,"node":text,"source":source,"task_id":text,"work_id":text,"after":text,"revision":text,"limit":{"type":"integer","minimum":1,"maximum":100}}),
            &["run_id", "node"],
            false,
        ),
        Operation::new(
            "flow.edit",
            "Preview a new document, including the exact pending work it would discard. Saves a preview without applying it.",
            json!({"run_id":text,"document":document}),
            &["run_id", "document"],
            true,
        ),
        Operation::new(
            "flow.commit",
            "Apply a saved preview. Repeating this request recovers or returns the same edit.",
            json!({"run_id":text,"plan_id":text}),
            &["run_id", "plan_id"],
            true,
        ),
        Operation::new(
            "flow.resume",
            "Recover an interrupted edit and restart stopped or failed workers. Failure counts are kept and parked tasks stay parked; retry them with flow.retry. Accepted initial input is never replayed.",
            run.clone(),
            &["run_id"],
            true,
        ),
        Operation::new(
            "flow.decide",
            "Complete a specific human task with a message or captured workspace. The result goes to all connected successors, carrying the task's authority unless authority requests a declared transition.",
            json!({"run_id":text,"node":text,"task_id":text,"message":text,"workspace_id":text,"authority":{"type":"array","items":{"type":"string"}}}),
            &["run_id", "node", "task_id"],
            true,
        ),
        Operation::new(
            "flow.retry",
            "Give a failed agent or command task fresh attempts, starting now; without task_id, every failed task at the node. Take task IDs from status failures.",
            task.clone(),
            &["run_id", "node"],
            true,
        ),
        Operation::new(
            "flow.discard",
            "Discard a parked task, or a human task no decision can satisfy, so it is never attempted again: its pending input is retired, or the initial input is marked complete. Take a parked task_id from status failures, a human one from status tasks.",
            task,
            &["run_id", "node", "task_id"],
            true,
        ),
        Operation::new(
            "flow.workspace",
            "Open a private workspace from a path, saved handle, or node's current task/result. Pin a task_id or choose an inbox work_id from flow.output; source:output selects the previous result. Capture before submitting the workspace_id in a human decision.",
            json!({"run_id":text,"action":{"enum":["open","capture","release"]},"node":text,"path":text,"workspace_id":text,"source":source,"task_id":text,"work_id":text}),
            &["run_id", "action"],
            true,
        ),
        Operation::new(
            "flow.promote",
            "Save the completed run document as a reusable definition revision.",
            run,
            &["run_id"],
            true,
        ),
        Operation::new(
            "flow.export",
            "Export the selected task input or node result as a new file or workspace directory. Choose a work_id when an inbox has several items; source:output selects the previous result. Files use 0644 (0755 if executable), directories 0755.",
            json!({"run_id":text,"node":text,"path":text,"source":source,"task_id":text,"work_id":text}),
            &["run_id", "node", "path"],
            true,
        ),
    ]
}

/// Built-in components and the user's library, read afresh for each request.
pub fn components(service: &Service) -> Result<Catalog> {
    Catalog::load(&service.paths.library())
}

/// The components a document can place, and the MCP servers agents can load
/// by name. Server environments are listed by variable name only.
fn library(service: &Service) -> Result<Value> {
    let path = service.paths.library();
    let library = super::components::Library::load(&path)?;
    let servers: serde_json::Map<_, _> = library
        .servers
        .iter()
        .map(|(name, server)| {
            (
                name.clone(),
                json!({"command":server.command,"args":server.args,"env":server.env.keys().collect::<Vec<_>>()}),
            )
        })
        .collect();
    Ok(json!({
        "library": path,
        "components": Catalog::with_library(&library)?.describe(),
        "validators": {
            "text": "Valid UTF-8: a message, or a workspace envelope",
            "bytes": "Any bytes",
            "workspace": "Only a workspace",
        },
        "servers": servers,
    }))
}

pub fn save_document(service: &Service, document: &Document) -> Result<Value> {
    let document = document.canonicalized()?;
    components(service)?.bind(&document)?;
    let revision = format!("{:x}", Sha256::digest(serde_json::to_vec(&document)?));
    persistence::write_json(&service.paths.workflow_definition(&revision)?, &document)?;
    Ok(json!({"revision":revision,"document":document}))
}

pub fn start_document(service: &Service, args: &Value) -> Result<Document> {
    let document: Document = match (args.get("document"), args.get("revision")) {
        (Some(value), None) => serde_json::from_value(value.clone())?,
        (None, Some(Value::String(revision))) => {
            persistence::read_json(&service.paths.workflow_definition(revision)?)?
        }
        _ => {
            return Err(AppError::invalid(
                "Supply exactly one of document or revision",
            ));
        }
    };
    document.canonicalized()
}

pub(crate) fn optional_str<'a>(args: &'a Value, field: &str) -> Result<Option<&'a str>> {
    args.get(field)
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| AppError::invalid(format!("{field} must be a string")))
        })
        .transpose()
}

/// A new run's declaration and initial workflow: `document` bound by
/// `catalog`, under its own names as core identities.
pub fn plan(
    catalog: &Catalog,
    document: Document,
    id: &str,
    input: Option<Value>,
    workspace: Option<PathBuf>,
) -> Result<(
    crate::declarations::GraphDeclaration,
    runtime::InitialWorkflow,
)> {
    let bindings = catalog.bind(&document)?;
    let identities = IdentityMap::initial(&document);
    let declaration = expand(
        &document,
        &bindings,
        &catalog.node_types(&document)?,
        id,
        &identities,
    )?;
    declaration.compile().map_err(AppError::core)?;
    let state = edit::WorkflowState::new(document, bindings, identities)?;
    Ok((
        declaration,
        runtime::InitialWorkflow {
            state,
            input,
            workspace,
        },
    ))
}

/// A new run's declaration and initial workflow from flow.start's arguments.
pub fn prepare_start(
    service: &Service,
    args: &Value,
    id: &str,
) -> Result<(
    crate::declarations::GraphDeclaration,
    runtime::InitialWorkflow,
)> {
    prepare(service, args, id, true)
}

/// As `prepare_start`, but a retry (`new` false) repeats a start accepted
/// under the rules of its time.
fn prepare(
    service: &Service,
    args: &Value,
    id: &str,
    new: bool,
) -> Result<(
    crate::declarations::GraphDeclaration,
    runtime::InitialWorkflow,
)> {
    let document = start_document(service, args)?;
    // A document given inline is newly written; a saved revision loads as saved.
    if new && args.get("document").is_some() {
        document.check_connection_names(None)?;
    }
    if args.get("message").is_some() && args.get("workspace").is_some() {
        return Err(AppError::invalid("Supply message or workspace, not both"));
    }
    let catalog = components(service)?;
    let (declaration, mut initial) = plan(&catalog, document, id, None, None)?;
    let external = initial.state.bindings[&initial.state.current.entry]
        .implementation
        .is_external();
    if external && (args.get("message").is_some() || args.get("workspace").is_some()) {
        return Err(AppError::invalid(
            "An external entry takes no initial input; its client starts work with root moves",
        ));
    }
    let message = optional_str(args, "message")?.unwrap_or("");
    initial.input = (!external).then(|| json!({"message": message}));
    let workspace = args
        .get("workspace")
        .map(|value| {
            let path = value
                .as_str()
                .ok_or_else(|| AppError::invalid("workspace must be a directory path"))?;
            let path = Path::new(path);
            let path = if path.is_absolute() {
                path.to_owned()
            } else {
                Path::new(views::field(args, "project")?).join(path)
            };
            let path = std::fs::canonicalize(path)?;
            if !path.is_dir() {
                return Err(AppError::invalid("workspace must be a directory"));
            }
            Ok(path)
        })
        .transpose()?;
    initial.workspace = workspace;
    Ok((declaration, initial))
}

pub async fn dispatch(service: &Service, operation: &str, args: &Value) -> Result<Value> {
    if operation == "flow.library" {
        return library(service);
    }
    if operation == "flow.define" {
        let document: Document = serde_json::from_value(
            args.get("document")
                .cloned()
                .ok_or_else(|| AppError::invalid("document is required"))?,
        )?;
        document.check_connection_names(None)?;
        return save_document(service, &document);
    }
    if operation == "flow.start" {
        let id = optional_str(args, "start_id")?
            .map(str::to_owned)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        uuid::Uuid::parse_str(&id).map_err(|_| AppError::invalid("start_id must be a UUID"))?;
        let project = std::fs::canonicalize(views::field(args, "project")?)?;
        let existing = service.run(&id).await.ok();
        let (mut declaration, mut initial) = prepare(service, args, &id, existing.is_none())?;
        if let Some(run) = existing {
            let run = run.lock().await;
            let original = &run.manifest.workflow;
            if original.state.current != initial.state.current
                || original.input != initial.input
                || original.workspace != initial.workspace
                || run.manifest.project != project
            {
                return Err(AppError::new(
                    "initialization_conflict",
                    "This start_id has a different workflow or input",
                ));
            }
            // An incomplete core creation is recovered by the reserved start
            // path, from the declaration this start saved.
            initial = original.clone();
            declaration = run.manifest.declaration.clone();
        }
        service
            .start_reserved(&id, declaration, project, initial, service.environment())
            .await?;
        return applied(&*service.run(&id).await?.lock().await, None).await;
    }
    let id = views::field(args, "run_id")?;
    let handle = service.run(id).await?;
    let mut run = handle.lock().await;
    // Whatever this operation starts, it starts with the run's current
    // environment.
    run.environment = service.run_environment(id).await;
    let state = runtime::load(&run)?;
    match operation {
        "flow.status" => status(&run).await,
        "flow.output" => output::inspect(&run, &state, args).await,
        "flow.resume" => {
            run.resume().await?;
            applied(&run, None).await
        }
        "flow.promote" => {
            if state.pending.is_some() {
                return Err(AppError::new(
                    "pending_edit",
                    "Finish the edit before promoting this workflow",
                ));
            }
            save_document(service, &state.current)
        }
        "flow.edit" => {
            let next: Document = serde_json::from_value(
                args.get("document")
                    .cloned()
                    .ok_or_else(|| AppError::invalid("document is required"))?,
            )?;
            let bindings = components(service)?.bind(&next)?;
            let plan = edit::preview(&run.live()?.session, &state, next, bindings).await?;
            let initial = &run.manifest.workflow;
            let entry = &initial.state.current.entry;
            if runtime::initial_pending(&run).await?
                && (plan.document.entry != *entry
                    || plan.identities.nodes.get(entry)
                        != initial.state.identities.nodes.get(entry))
            {
                return Err(AppError::new(
                    "initial_task_pending",
                    "Complete the initial task before replacing its entry node",
                ));
            }
            persistence::write_json(&plan_path(&run, &plan.id)?, &plan)?;
            Ok(
                json!({"plan_id":plan.id,"version":plan.base_version,"document":plan.document,"changes":plan.changes,
                "retirements":retirements(&run,&state,&plan).await?}),
            )
        }
        "flow.commit" => {
            let plan: edit::Plan =
                persistence::read_json(&plan_path(&run, views::field(args, "plan_id")?)?)?;
            let mut state = state;
            let session = run.live()?.session.clone();
            edit::commit(&session, &mut state, &runtime::state_path(&run), plan)
                .await
                .map_err(public_error)?;
            // The edit is saved; workers that failed to restart for it are a
            // warning, since flow.resume or their own restart applies it.
            let reconciled = runtime::reconcile(&mut run, &state, false).await.err();
            applied(&run, reconciled).await
        }
        "flow.decide" => decide(&mut run, &state, args).await,
        "flow.retry" => {
            let (_, ledger) = failure_ledger(&run, &state, args)?;
            match optional_str(args, "task_id")? {
                Some(task) => {
                    ledger
                        .clear(&TaskKey::parse(task)?)?
                        .ok_or_else(unknown_failure)?;
                }
                None => ledger.retry_all()?,
            }
            applied(&run, None).await
        }
        "flow.discard" => discard(&run, &state, args).await,
        "flow.workspace" => {
            let mut internal = args.clone();
            let node_source = args["action"] == "open"
                && args.get("path").is_none()
                && args.get("workspace_id").is_none();
            if !node_source
                && ["source", "task_id", "work_id"]
                    .iter()
                    .any(|key| args.get(key).is_some())
            {
                return Err(AppError::invalid(
                    "Task/result selectors apply only when opening a workspace from a node",
                ));
            }
            if node_source {
                let payload = output::payload(&run, &state, args, true).await?;
                let WorkflowPayload::Workspace(envelope) = payload else {
                    return Err(AppError::invalid(
                        "This node has no workspace input or output",
                    ));
                };
                internal["root"] = json!(envelope.ontography_package);
            }
            artifacts::workspace(&mut run, &internal).await
        }
        "flow.export" => {
            let payload = output::payload(&run, &state, args, false).await?;
            let path = PathBuf::from(views::field(args, "path")?);
            let path = if path.is_absolute() {
                path
            } else {
                run.manifest.project.join(path)
            };
            artifacts::export(&run, payload, &path).await
        }
        _ => Err(AppError::new("unknown_operation", operation)),
    }
}

fn plan_path(run: &ManagedRun, id: &str) -> Result<PathBuf> {
    uuid::Uuid::parse_str(id).map_err(|_| AppError::invalid("plan_id must be a UUID"))?;
    Ok(run.directory.join("edit-plans").join(format!("{id}.json")))
}

/// A task as the manager sees it: workflow names and a preview of its input.
#[derive(Serialize)]
pub struct TaskView {
    pub node: String,
    pub task_id: String,
    /// The node's types.
    pub types: BTreeSet<String>,
    pub input: Value,
    pub work_ids: Vec<String>,
    #[serde(skip)]
    pub(super) raw_input: Value,
    #[serde(skip)]
    task: tasks::Task,
}

/// A task the manager may select, before its input is read.
pub(super) struct Candidate<'a> {
    pub node: &'a DocumentNode,
    pub binding: &'a Binding,
    pub task: tasks::Task,
}

pub(super) fn payload_view(value: &Value) -> Value {
    if value.get("ontography_package").is_some() {
        return json!({"workspace":true});
    }
    if let Some(hex) = value.get("hex").and_then(Value::as_str) {
        return json!({"binary":true,"bytes":hex.len() / 2});
    }
    if let Some(message) = value.get("message").and_then(Value::as_str) {
        let preview: String = message.chars().take(8192).collect();
        return if preview.len() < message.len() {
            json!({"message":preview,"truncated":true})
        } else {
            value.clone()
        };
    }
    if let Some(values) = value.as_array() {
        return json!(values.iter().map(payload_view).collect::<Vec<_>>());
    }
    value.clone()
}

pub async fn tasks(run: &ManagedRun, state: &edit::WorkflowState) -> Result<Vec<TaskView>> {
    let initial = initial_holder(run).await?;
    let mut views = Vec::new();
    for candidate in ready_tasks(run, state, None, initial).await? {
        views.push(task_view(run, candidate).await?);
    }
    Ok(views)
}

/// Tasks the manager may select at one node: the one it runs or awaits a
/// decision on next, then failed tasks that wait out a backoff or are parked.
pub(super) async fn node_tasks<'a>(
    run: &ManagedRun,
    state: &'a edit::WorkflowState,
    node: &str,
) -> Result<Vec<Candidate<'a>>> {
    run.live()?;
    let initial = initial_holder(run).await?;
    let mut selectable = ready_tasks(run, state, Some(node), initial).await?;
    for failed in failed_tasks(run, state, Some(node), initial).await? {
        if selectable
            .iter()
            .all(|candidate| candidate.task.key != failed.task.key)
        {
            selectable.push(Candidate {
                node: failed.node,
                binding: state.binding(&failed.node.id)?,
                task: failed.task,
            });
        }
    }
    Ok(selectable)
}

/// The core node holding the run's initial input while it is still pending.
async fn initial_holder(run: &ManagedRun) -> Result<Option<&str>> {
    let initial = &run.manifest.workflow;
    let entry = &initial.state.identities.nodes[&initial.state.current.entry];
    Ok((run.live.is_some() && runtime::initial_pending(run).await?).then_some(entry.as_str()))
}

async fn ready_tasks<'a>(
    run: &ManagedRun,
    state: &'a edit::WorkflowState,
    only: Option<&str>,
    initial: Option<&str>,
) -> Result<Vec<Candidate<'a>>> {
    let Some(live) = &run.live else {
        return Ok(vec![]);
    };
    let kernel = live.session.kernel().await.map_err(AppError::core)?;
    let mut ready = Vec::new();
    for node in state
        .current
        .nodes
        .iter()
        .filter(|node| only.is_none_or(|id| node.id == id))
    {
        let id = &state.identities.nodes[&node.id];
        if kernel.graph().node(id).is_none() {
            continue;
        }
        let holds_initial = initial == Some(id.as_str());
        let binding = state.binding(&node.id)?;
        if binding.implementation.is_external() {
            // Its client, not the manager, takes the work there.
            continue;
        }
        let task = if binding.implementation.runs_tasks() {
            // Exactly what its worker runs next; failed tasks that wait or are
            // parked appear among the failures instead.
            let Some(worker) = live.workers.get(id) else {
                continue;
            };
            let next = tasks::TaskSource {
                session: &live.session,
                node_id: id,
                initial: holds_initial,
                ledger: &worker.ledger,
                busy: &[],
            }
            .next()
            .await?;
            let Some(task) = next else {
                continue;
            };
            task
        } else if holds_initial {
            tasks::Task::initial(id)
        } else {
            let frontier = live
                .session
                .next_trigger_at(id.as_str())
                .await
                .map_err(AppError::core)?;
            if frontier.packages().is_empty() {
                continue;
            }
            tasks::Task::packages(id, frontier.packages().to_vec())
        };
        ready.push(Candidate {
            node,
            binding,
            task,
        });
    }
    Ok(ready)
}

pub(super) async fn task_view(run: &ManagedRun, candidate: Candidate<'_>) -> Result<TaskView> {
    let Candidate {
        node,
        binding,
        task,
    } = candidate;
    let raw_input = if task.is_initial() {
        WorkflowPayload::read(&runtime::initial_payload(run).await?)?.to_value()
    } else {
        let mut inputs = Vec::with_capacity(task.inputs.len());
        for (_, record) in &task.inputs {
            inputs.push(output::input(run, record).await?);
        }
        if inputs.len() == 1 {
            inputs.remove(0)
        } else {
            json!(inputs)
        }
    };
    Ok(TaskView {
        node: node.id.clone(),
        task_id: task.key.to_string(),
        types: binding.types.clone(),
        input: payload_view(&raw_input),
        raw_input,
        work_ids: task
            .inputs
            .iter()
            .map(|(id, _)| tasks::work_id(id))
            .collect(),
        task,
    })
}

/// A failed task whose input is still pending at its node.
struct Failed<'a> {
    node: &'a DocumentNode,
    task: tasks::Task,
    record: tasks::FailedTask,
}

impl Failed<'_> {
    fn view(&self) -> Value {
        let failures = &self.record.failures;
        let mut view = json!({"node":self.node.id,"task_id":self.task.key,"attempts":failures.attempts,
            "error":failures.error,"state":if failures.parked {"parked"} else {"retrying"}});
        if let Some(wait) = self.record.retry_in {
            view["retry_in_secs"] = json!(tasks::ceil_secs(wait));
        }
        view
    }
}

/// Failed tasks in document order. A record whose task is gone, because an
/// input was consumed or retired or a join no longer matches its node's
/// connections, is dropped from the ledger once no edit is in progress.
async fn failed_tasks<'a>(
    run: &ManagedRun,
    state: &'a edit::WorkflowState,
    only: Option<&str>,
    initial: Option<&str>,
) -> Result<Vec<Failed<'a>>> {
    let Some(live) = &run.live else {
        return Ok(vec![]);
    };
    let mut failed = Vec::new();
    for node in state
        .current
        .nodes
        .iter()
        .filter(|node| only.is_none_or(|id| node.id == id))
    {
        let id = &state.identities.nodes[&node.id];
        let Some(worker) = live.workers.get(id) else {
            continue;
        };
        let source = tasks::TaskSource {
            session: &live.session,
            node_id: id,
            initial: initial == Some(id.as_str()),
            ledger: &worker.ledger,
            busy: &[],
        };
        for (record, task) in source.failures().await? {
            match task {
                Some(task) => failed.push(Failed { node, task, record }),
                // Only a stale record changes; a failed write just keeps it.
                None if state.pending.is_none() => {
                    let _ = worker.ledger.clear(&record.key);
                }
                None => {}
            }
        }
    }
    Ok(failed)
}

/// The core node and retry ledger of the node a manager request names. Only
/// tasks that workers run fail.
fn failure_ledger<'a>(
    run: &'a ManagedRun,
    state: &'a edit::WorkflowState,
    args: &Value,
) -> Result<(&'a str, &'a RetryLedger)> {
    let node = views::field(args, "node")?;
    if !state.binding(node)?.implementation.runs_tasks() {
        return Err(AppError::invalid(
            "Only tasks that workers run fail and retry",
        ));
    }
    let id = &state.identities.nodes[node];
    let worker = run.live()?.workers.get(id).ok_or_else(|| {
        AppError::new(
            "worker_unavailable",
            "This node has no worker; resume the run",
        )
    })?;
    Ok((id, &worker.ledger))
}

fn unknown_failure() -> AppError {
    AppError::new(
        "stale_task",
        "This task has no recorded failure; read status again",
    )
}

fn stale_task() -> AppError {
    AppError::new(
        "stale_task",
        "This task is no longer ready; read status again",
    )
}

/// Retires a parked task's input before forgetting it. Its worker never
/// selects a parked task, so no attempt can start in between. A human task
/// is discarded while it waits: only a decision would end it otherwise, and
/// one that no decision can satisfy would hold up every task behind it.
async fn discard(run: &ManagedRun, state: &edit::WorkflowState, args: &Value) -> Result<Value> {
    if state.pending.is_some() {
        return Err(AppError::new(
            "pending_edit",
            "Complete the edit before discarding a task",
        ));
    }
    let key = TaskKey::parse(views::field(args, "task_id")?)?;
    let name = views::field(args, "node")?;
    if matches!(
        state.binding(name)?.implementation,
        Implementation::Human(_)
    ) {
        let task = ready_tasks(run, state, Some(name), initial_holder(run).await?)
            .await?
            .into_iter()
            .map(|candidate| candidate.task)
            .find(|task| task.key == key)
            .ok_or_else(stale_task)?;
        end_task(run, &state.identities.nodes[name], &task.ids()).await?;
        return applied(run, None).await;
    }
    let (node, ledger) = failure_ledger(run, state, args)?;
    let failures = ledger.get(&key).ok_or_else(unknown_failure)?;
    if !failures.parked {
        return Err(AppError::new(
            "task_not_parked",
            "Only a parked task can be discarded; retry it or wait for its remaining attempts",
        ));
    }
    let session = &run.live()?.session;
    let source = tasks::TaskSource {
        session,
        node_id: node,
        initial: initial_holder(run).await? == Some(node),
        ledger,
        busy: &[],
    };
    // A task that lost an input, or a join that no longer matches its node's
    // connections, is gone: its inputs may belong to other work by now.
    if source.current(&failures.inputs).await?.is_none() {
        let _ = ledger.clear(&key);
        return Err(unknown_failure());
    }
    end_task(run, node, &failures.inputs).await?;
    // The record is stale now, and status drops a stale record, so failing to
    // forget it here changes nothing: the task is discarded either way.
    let _ = ledger.clear(&key);
    applied(run, None).await
}

/// Ends a task for good: retires its inputs, or marks the run's initial input
/// complete. Inputs retire one at a time, so an error names those already
/// retired.
async fn end_task(run: &ManagedRun, node: &str, inputs: &[PackageId]) -> Result<()> {
    if inputs.is_empty() {
        // The initial input is not a package; completing it ends its attempts.
        return runtime::complete_initial(&runtime::node_directory(run, node));
    }
    let session = &run.live()?.session;
    let mut retired = Vec::new();
    for input in inputs {
        let refused = match session.retire(*input, None).await {
            // Consumed or retired elsewhere: nothing is left to discard.
            Ok(Ok(_) | Err(RetireError::NotLive(_))) => None,
            Ok(Err(error)) => Some(views::retire_refusal(&error)),
            Err(error) => Some(AppError::core(error)),
        };
        if let Some(error) = refused {
            if retired.is_empty() {
                return Err(error);
            }
            let message = format!(
                "{}; {} of the task's inputs were already retired",
                error.message,
                retired.len()
            );
            return Err(AppError::new(error.code, message).details(json!({"retired":retired})));
        }
        retired.push(tasks::work_id(input));
    }
    Ok(())
}

/// How many failed tasks status lists, in document order; `failures_total`
/// counts them all. Each error is already bounded, so the list is too.
const LISTED_FAILURES: usize = 100;

/// The largest status a change's reply carries; status alone may use the
/// whole response budget.
const APPLIED_STATUS_BYTES: usize = crate::protocol::MAX_FRAME_BYTES / 4;

/// The reply to a change that has been applied. Nothing after the change
/// turns into an error, which would invite repeating it: a later step that
/// failed is a `warning`, and a status that cannot be read, or would be too
/// large to send, is replaced by `{"applied":true,"run_id":…,"status_error":…}`.
async fn applied(run: &ManagedRun, warning: Option<AppError>) -> Result<Value> {
    let mut reply = applied_reply(&run.manifest.run_id, status(run).await);
    if let Some(warning) = warning {
        reply["warning"] = json!(warning);
    }
    Ok(reply)
}

fn applied_reply(run_id: &str, status: Result<Value>) -> Value {
    let error = match status {
        Ok(status) => match serde_json::to_vec(&status) {
            Ok(bytes) if bytes.len() <= APPLIED_STATUS_BYTES => return status,
            _ => AppError::new(
                "result_too_large",
                "the run's status is too large for this reply; the change was applied",
            ),
        },
        Err(error) => error,
    };
    json!({"applied":true,"run_id":run_id,"status_error":error})
}

pub async fn status(run: &ManagedRun) -> Result<Value> {
    let state = runtime::load(run)?;
    let initial = initial_holder(run).await?;
    let mut tasks = Vec::new();
    for candidate in ready_tasks(run, &state, None, initial).await? {
        tasks.push(task_view(run, candidate).await?);
    }
    let failed = failed_tasks(run, &state, None, initial).await?;
    let failures: Vec<_> = failed
        .iter()
        .take(LISTED_FAILURES)
        .map(Failed::view)
        .collect();
    let counts = if let Some(live) = &run.live {
        Some(
            live.session
                .frontier_overview(1)
                .await
                .map_err(AppError::core)?,
        )
    } else {
        None
    };
    let nodes: Vec<_> = state.current.nodes.iter().map(|node| {
        let id = &state.identities.nodes[&node.id];
        let worker = run.live.as_ref().and_then(|live| live.workers.get(id));
        let execution = run.live.as_ref().and_then(|live| worker.and_then(|worker| live.executions.get(&worker.execution_id)));
        let binding = &state.bindings[&node.id];
        let mut view = json!({"id":node.id,"types":binding.types,"component":node.component,
            "implementation":binding.implementation.kind(),
            "pending":counts.as_ref().and_then(|view|view.counts().get(id.as_str())).map_or(0,|count|count.received()),
            "execution":execution.map(|handle|execution_status(handle.status()))});
        if let Some(runtime) = worker.and_then(|worker| worker.node.as_ref()) {
            view["session"] = json!(runtime.status());
        }
        view
    }).collect();
    // The existing graph view uses workflow names, including intermediate
    // topology during an unfinished edit. No incarnation IDs leave this view.
    let mut result = json!({"run_id":run.manifest.run_id,"version":state.version,"document":state.current,"status":run.summary()["status"],
        "pending_edit":state.pending.as_ref().map(|plan|json!({"plan_id":plan.id,"document":plan.document})),"nodes":nodes,"tasks":tasks,"failures":failures,
        "failures_total":failed.len()});
    if let Some(view) = counts {
        let names = state.node_names(view.kernel());
        let name = |id: &str| edit::label(&names, id);
        let connections = state.edge_names();
        result["revision"] = json!(view.revision().to_string());
        result["graph"] = json!({"nodes":view.kernel().graph().nodes().iter().map(|node|json!({"id":name(node.id())})).collect::<Vec<_>>(),
            "edges":view.kernel().graph().edges().iter().map(|edge|json!({"id":edit::edge_label(&connections,edge.id()),"source":name(edge.source()),"target":name(edge.target())})).collect::<Vec<_>>()});
        result["frontier"] = json!({"counts":view.counts().iter().map(|(id,count)|(name(id),json!({"received":count.received(),"outbound":count.outbound()}))).collect::<serde_json::Map<_,_>>()});
    }
    Ok(result)
}

fn execution_status(status: ontography::ExecutionStatus) -> Value {
    use ontography::ExecutionStatus;
    match status {
        ExecutionStatus::Running => json!({"state":"running"}),
        ExecutionStatus::Exited => json!({"state":"exited"}),
        ExecutionStatus::Aborted => json!({"state":"aborted"}),
        ExecutionStatus::Failed(error) => {
            json!({"state":"failed","error":{"class":error.class(),"message":error.message()}})
        }
        ExecutionStatus::Panicked(message) => {
            json!({"state":"panicked","error":{"class":"panic","message":message}})
        }
    }
}

async fn decide(run: &mut ManagedRun, state: &edit::WorkflowState, args: &Value) -> Result<Value> {
    if state.pending.is_some() {
        return Err(AppError::new(
            "pending_edit",
            "Complete the edit before deciding a task",
        ));
    }
    let node = views::field(args, "node")?;
    let task = tasks(run, state)
        .await?
        .into_iter()
        .find(|task| task.node == node && task.task_id == args["task_id"])
        .ok_or_else(stale_task)?;
    if !matches!(
        state.binding(node)?.implementation,
        Implementation::Human(_)
    ) {
        return Err(AppError::invalid("Only human tasks accept decisions"));
    }
    let result = match (args.get("message"), args.get("workspace_id")) {
        (Some(Value::String(message)), None) => WorkflowPayload::Message {
            message: message.clone(),
        },
        (None, Some(Value::String(id))) => {
            let saved =
                run.manifest.checkpoints.get(id).ok_or_else(|| {
                    AppError::invalid("Capture the workspace before submitting it")
                })?;
            WorkflowPayload::Workspace(ontography::PackageEnvelope::new(saved.root))
        }
        _ => {
            return Err(AppError::invalid(
                "Supply exactly one of message or workspace_id",
            ));
        }
    };
    let payload = result.encode()?;
    let authority = match args.get("authority") {
        Some(value) => OutputAuthority::Transition(views::authority(&serde_json::from_value::<
            Vec<String>,
        >(value.clone())?)?),
        None => OutputAuthority::Carry,
    };
    let session = &run.live()?.session;
    let core_node = &state.identities.nodes[node];
    let input = if task.task.is_initial() {
        Some(runtime::initial_payload(run).await?)
    } else {
        None
    };
    let contents = if let Some(input) = &input {
        dependencies(session, input).await?
    } else {
        vec![]
    };
    let kernel = session.kernel().await.map_err(AppError::core)?;
    let trigger = match input {
        Some(input) => InvocationTrigger::Root {
            authority: kernel
                .root_ceiling(core_node)
                .cloned()
                .ok_or_else(|| AppError::new("not_a_root", "This node no longer starts work"))?,
            input,
        },
        None => InvocationTrigger::Packages(task.task.ids()),
    };
    let invocation = session
        .begin_invocation_with_content(
            core_node.as_str(),
            trigger,
            ContextPolicy::default(),
            contents,
        )
        .await
        .map_err(AppError::core)?;
    let emissions = kernel
        .graph()
        .edges()
        .iter()
        .filter(|edge| edge.source() == core_node)
        .map(|edge| Emission::new(edge.id(), authority.clone(), payload.clone()))
        .collect();
    let contents = dependencies(session, &payload).await?;
    let directory = runtime::node_directory(run, core_node);
    let path = directory.join("output.json");
    let previous: Option<Value> = persistence::read_json(&path).ok();
    let mut output = json!({"node":node,"invocation_id":invocation.id().to_string(),"result":result,"publication_status":"prepared"});
    persistence::write_json(&path, &output)?;
    match invocation
        .submit(payload, emissions, contents)
        .await
        .map_err(AppError::core)?
    {
        ProposalDecision::Committed(_) => {
            output["publication_status"] = json!("committed");
            // Core has committed; these caches can be reconstructed if either
            // write fails. A cache failure must not invite repeating the work.
            let _ = persistence::write_json(&path, &output);
            if task.task.is_initial() {
                let _ = runtime::complete_initial(&directory);
            }
            applied(run, None).await
        }
        ProposalDecision::Rejected(error) => {
            // The task stays ready for a corrected decision; the node keeps its last result.
            let _ = match &previous {
                Some(previous) => persistence::write_json(&path, previous),
                None => std::fs::remove_file(&path).map_err(AppError::from),
            };
            Err(refusal(state, &error))
        }
    }
}

/// Why core refused a decision, in workflow terms. An authority refusal
/// names its rule, so the decision can be corrected; a task that no decision
/// can satisfy, since every connection must accept the same one, is
/// discarded instead.
fn refusal(state: &edit::WorkflowState, reject: &Reject) -> AppError {
    let message = match reject {
        Reject::AuthorityOutsideSchema { authority, .. } => {
            format!(
                "authority {} has a tag this run does not declare",
                json!(authority)
            )
        }
        Reject::UnauthorizedAuthorityTransition { from, to, .. } => format!(
            "no declared transition changes authority {} to {}",
            json!(from),
            json!(to)
        ),
        Reject::EdgeAuthorityMismatch {
            edge_id, authority, ..
        } => format!(
            "connection {:?} does not admit authority {}",
            edit::edge_label(&state.edge_names(), edge_id),
            json!(authority)
        ),
        _ => views::rejection(reject),
    };
    AppError::new(
        "rejected",
        format!(
            "{message}. The task stays ready: correct the decision, or discard the task with flow.discard if no decision can satisfy every outgoing connection"
        ),
    )
}

/// Saved edit approvals use core package IDs internally; callers identify the
/// same work using the opaque handles returned in tasks and previews.
pub(crate) fn public_error(mut error: AppError) -> AppError {
    if error.code == "retirement_preview_required"
        && let Some(details) = error.details.as_mut()
        && let Some(retirements) = details
            .get("additional_retirements")
            .and_then(Value::as_object)
    {
        // Core reported these identities, so each parses; a handle is never
        // replaced by the identity it hides.
        details["additional_retirements"] = json!(
            retirements
                .iter()
                .map(|(id, reason)| json!({"work_id":views::package_id(id).ok().map(|id| tasks::work_id(&id)),"reason":reason}))
                .collect::<Vec<_>>()
        );
    }
    error
}

pub async fn dependencies(
    session: &ontography::SessionHandle,
    payload: &Payload,
) -> Result<Vec<ontography::ContentId>> {
    match WorkflowPayload::read(payload)? {
        WorkflowPayload::Workspace(envelope) => {
            PackageStore::new(session.content_store().await.map_err(AppError::core)?)
                .dependencies(envelope.ontography_package)
                .await
                .map_err(AppError::core)
        }
        _ => Ok(vec![]),
    }
}

async fn retirements(
    run: &ManagedRun,
    state: &edit::WorkflowState,
    plan: &edit::Plan,
) -> Result<Vec<Value>> {
    let mut result = Vec::new();
    for (id, reason) in &plan.retirements {
        let package = views::package_id(id)?;
        let history = run
            .live()?
            .session
            .package_history(package)
            .await
            .map_err(AppError::core)?;
        let node = history.as_ref().and_then(|history| {
            state
                .identities
                .nodes
                .iter()
                .find(|(_, id)| id.as_str() == history.package().holder())
                .map(|(name, _)| name)
        });
        result.push(json!({"work_id":tasks::work_id(&package),"node":node,"reason":reason}));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_applied_change_replies_with_its_status_or_says_why_not() {
        let status = json!({"run_id":"run","failures":[],"failures_total":0});
        assert_eq!(applied_reply("run", Ok(status.clone())), status);
        let unreadable = applied_reply("run", Err(AppError::core("storage failed")));
        assert_eq!(unreadable["applied"], true);
        assert_eq!(unreadable["run_id"], "run");
        assert_eq!(unreadable["status_error"]["code"], "core_error");
        let large = applied_reply(
            "run",
            Ok(json!({"document":"x".repeat(APPLIED_STATUS_BYTES)})),
        );
        assert_eq!(large["applied"], true);
        assert_eq!(large["run_id"], "run");
        assert_eq!(large["status_error"]["code"], "result_too_large");
    }
}
