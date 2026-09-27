//! The manager's complete workflow interface. Core identities stay behind it.

use super::{
    Document, IdentityMap, NodeKind, WorkflowPayload, artifacts, edit, expand, output, runtime,
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
    ProposalDecision,
};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

pub fn operations() -> Vec<Operation> {
    let text = json!({"type":"string"});
    let document = crate::catalog::schema::<Document>();
    let run = json!({"run_id":text});
    let source = json!({"enum":["auto","output","pending"]});
    vec![
        Operation::new(
            "flow.define",
            "Validate and save a reusable workflow document.",
            json!({"document":document}),
            &["document"],
            true,
        ),
        Operation::new(
            "flow.start",
            "Start a document or saved revision. Supply message or a workspace directory as the initial input. A start_id makes unscoped retries idempotent.",
            json!({"document":document,"revision":text,"project":text,"message":text,"workspace":text,"start_id":text}),
            &["project"],
            true,
        ),
        Operation::new(
            "flow.status",
            "Read the document, pending edit, workers and ready human tasks using workflow names.",
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
            "Recover an interrupted edit and restart stopped or failed workers. Accepted initial input is never replayed.",
            run.clone(),
            &["run_id"],
            true,
        ),
        Operation::new(
            "flow.decide",
            "Complete a specific human task with a message or captured workspace. The result goes to all connected successors.",
            json!({"run_id":text,"node":text,"task_id":text,"message":text,"workspace_id":text}),
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

pub fn save_document(service: &Service, document: &Document) -> Result<Value> {
    let document = document.canonicalized()?;
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

pub fn prepare_start(
    service: &Service,
    args: &Value,
    id: &str,
) -> Result<(
    crate::declarations::GraphDeclaration,
    runtime::InitialWorkflow,
)> {
    let document = start_document(service, args)?;
    if args.get("message").is_some() && args.get("workspace").is_some() {
        return Err(AppError::invalid("Supply message or workspace, not both"));
    }
    let identities = IdentityMap::fresh(&document);
    let declaration = expand(&document, id, &identities)?;
    declaration.compile().map_err(AppError::core)?;
    let state = edit::WorkflowState::new(document, identities)?;
    let input = json!({"message":optional_str(args,"message")?.unwrap_or("")});
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
    Ok((
        declaration,
        runtime::InitialWorkflow {
            state,
            input,
            workspace,
        },
    ))
}

pub async fn dispatch(service: &Service, operation: &str, args: &Value) -> Result<Value> {
    if operation == "flow.define" {
        return save_document(
            service,
            &serde_json::from_value(
                args.get("document")
                    .cloned()
                    .ok_or_else(|| AppError::invalid("document is required"))?,
            )?,
        );
    }
    if operation == "flow.start" {
        let id = optional_str(args, "start_id")?
            .map(str::to_owned)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        uuid::Uuid::parse_str(&id).map_err(|_| AppError::invalid("start_id must be a UUID"))?;
        let project = std::fs::canonicalize(views::field(args, "project")?)?;
        let (declaration, mut initial) = prepare_start(service, args, &id)?;
        if let Ok(run) = service.run(&id).await {
            let run = run.lock().await;
            let original = run.manifest.workflow.as_ref().ok_or_else(|| {
                AppError::new(
                    "initialization_conflict",
                    "This start_id names a different run",
                )
            })?;
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
            initial = original.clone();
            // An incomplete core creation is recovered by the reserved start path.
            let declaration = expand(&initial.state.current, &id, &initial.state.identities)?;
            drop(run);
            service
                .start_workflow_reserved(&id, declaration, project, initial)
                .await?;
        } else {
            service
                .start_workflow_reserved(&id, declaration, project, initial)
                .await?;
        }
        return status(&*service.run(&id).await?.lock().await).await;
    }
    let handle = service.run(views::field(args, "run_id")?).await?;
    let mut run = handle.lock().await;
    let state = runtime::load(&run)?;
    match operation {
        "flow.status" => status(&run).await,
        "flow.output" => output::inspect(&run, &state, args).await,
        "flow.resume" => {
            run.resume().await?;
            status(&run).await
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
            let next = serde_json::from_value(
                args.get("document")
                    .cloned()
                    .ok_or_else(|| AppError::invalid("document is required"))?,
            )?;
            let plan = edit::preview(&run.live()?.session, &state, next).await?;
            let initial = run.manifest.workflow.as_ref().expect("workflow loaded");
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
                json!({"plan_id":plan.id,"version":plan.base_version,"document":plan.document,"changes":plan.steps,
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
            runtime::reconcile(&mut run, &state, false).await?;
            status(&run).await
        }
        "flow.decide" => decide(&mut run, &state, args).await,
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

#[derive(Serialize)]
pub struct Task {
    pub node: String,
    pub task_id: String,
    pub kind: NodeKind,
    pub input: Value,
    pub work_ids: Vec<String>,
    #[serde(skip)]
    pub(super) raw_input: Value,
    #[serde(skip)]
    packages: Vec<PackageId>,
    #[serde(skip)]
    initial: bool,
}

fn task_id(node: &str, packages: &[PackageId]) -> String {
    let ids: Vec<_> = packages.iter().map(ToString::to_string).collect();
    format!("task_{:x}", Sha256::digest(json!([node, ids]).to_string()))
}

pub(super) fn work_id(package: &str) -> String {
    format!("work_{:x}", Sha256::digest(package))
}

pub(super) fn payload_view(value: &Value) -> Value {
    if value.get("ontography_package").is_some() {
        return json!({"workspace":true});
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

pub async fn tasks(run: &ManagedRun, state: &edit::WorkflowState) -> Result<Vec<Task>> {
    ready_tasks(run, state, None).await
}

pub(super) async fn ready_task(
    run: &ManagedRun,
    state: &edit::WorkflowState,
    node: &str,
) -> Result<Option<Task>> {
    run.live()?;
    Ok(ready_tasks(run, state, Some(node)).await?.pop())
}

async fn ready_tasks(
    run: &ManagedRun,
    state: &edit::WorkflowState,
    only: Option<&str>,
) -> Result<Vec<Task>> {
    if run.live.is_none() {
        return Ok(vec![]);
    }
    let session = &run.live()?.session;
    let kernel = session.kernel().await.map_err(AppError::core)?;
    let initial = run.manifest.workflow.as_ref().expect("workflow loaded");
    let original_entry = &initial.state.identities.nodes[&initial.state.current.entry];
    let pending_initial = runtime::initial_pending(run).await?;
    let mut tasks = Vec::new();
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
        if pending_initial && id == original_entry {
            let raw_input = serde_json::from_slice(&runtime::initial_payload(run).await?)?;
            tasks.push(Task {
                node: node.id.clone(),
                task_id: task_id(id, &[]),
                kind: node.kind,
                input: payload_view(&raw_input),
                raw_input,
                work_ids: vec![],
                packages: vec![],
                initial: true,
            });
            continue;
        }
        let frontier = session
            .next_trigger_at(id.as_str())
            .await
            .map_err(AppError::core)?;
        if frontier.packages().is_empty() {
            continue;
        }
        let mut inputs = Vec::new();
        for (_, package) in frontier.packages() {
            let bytes = session
                .content(package.content_digest())
                .await
                .map_err(AppError::core)?
                .ok_or_else(|| AppError::new("missing_content", "Task input is unavailable"))?;
            inputs.push(serde_json::from_slice::<Value>(&bytes)?);
        }
        let packages: Vec<_> = frontier.packages().iter().map(|(id, _)| *id).collect();
        let raw_input = if inputs.len() == 1 {
            inputs.remove(0)
        } else {
            json!(inputs)
        };
        tasks.push(Task {
            node: node.id.clone(),
            task_id: task_id(id, &packages),
            kind: node.kind,
            input: payload_view(&raw_input),
            raw_input,
            work_ids: packages.iter().map(|id| work_id(&id.to_string())).collect(),
            packages,
            initial: false,
        });
    }
    Ok(tasks)
}

pub async fn status(run: &ManagedRun) -> Result<Value> {
    let state = runtime::load(run)?;
    let tasks = tasks(run, &state).await?;
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
        let execution = run.live.as_ref().and_then(|live| live.workers.get(id).and_then(|worker| live.executions.get(&worker.execution_id)));
        json!({"id":node.id,"kind":node.kind,"pending":counts.as_ref().and_then(|view|view.counts().get(id.as_str())).map_or(0,|count|count.received()),
            "execution":execution.map(|handle|execution_status(handle.status()))})
    }).collect();
    // The existing graph view uses workflow names, including intermediate
    // topology during an unfinished edit. No incarnation IDs leave this view.
    let mut result = json!({"run_id":run.manifest.run_id,"version":state.version,"document":state.current,"status":run.summary()["status"],
        "pending_edit":state.pending.as_ref().map(|plan|json!({"plan_id":plan.id,"document":plan.document})),"nodes":nodes,"tasks":tasks});
    if let Some(view) = counts {
        let mut names = std::collections::BTreeMap::new();
        for (name, id) in &state.identities.nodes {
            names.insert(id.as_str(), name.clone());
        }
        if let Some(plan) = &state.pending {
            for (name, id) in &plan.identities.nodes {
                if view.kernel().graph().node(id).is_some() {
                    if let Some(old) = state.identities.nodes.get(name).filter(|old| *old != id) {
                        names.insert(old.as_str(), format!("{name} (previous)"));
                    }
                    names.insert(id.as_str(), name.clone());
                }
            }
        }
        let name = |id: &str| {
            names
                .get(id)
                .cloned()
                .unwrap_or_else(|| "unknown node".into())
        };
        result["revision"] = json!(view.revision().to_string());
        result["graph"] = json!({"nodes":view.kernel().graph().nodes().iter().map(|node|json!({"id":name(node.id())})).collect::<Vec<_>>(),
            "edges":view.kernel().graph().edges().iter().map(|edge|json!({"id":super::edge_key(&name(edge.source()),&name(edge.target())),"source":name(edge.source()),"target":name(edge.target())})).collect::<Vec<_>>()});
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
        .ok_or_else(|| {
            AppError::new(
                "stale_task",
                "This task is no longer ready; read status again",
            )
        })?;
    if task.kind != NodeKind::Human {
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
    let session = &run.live()?.session;
    let core_node = &state.identities.nodes[node];
    let input = if task.initial {
        Some(runtime::initial_payload(run).await?)
    } else {
        None
    };
    let contents = if let Some(input) = &input {
        dependencies(session, input).await?
    } else {
        vec![]
    };
    let trigger = match input {
        Some(input) => InvocationTrigger::Root {
            authority: views::authority(&[super::document::AUTHORITY.into()])?,
            input,
        },
        None => InvocationTrigger::Packages(task.packages),
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
    let payload = result.encode()?;
    let kernel = session.kernel().await.map_err(AppError::core)?;
    let emissions = kernel
        .graph()
        .edges()
        .iter()
        .filter(|edge| edge.source() == core_node)
        .map(|edge| Emission::new(edge.id(), OutputAuthority::Carry, payload.clone()))
        .collect();
    let contents = dependencies(session, &payload).await?;
    let directory = runtime::node_directory(run, core_node);
    let mut output = json!({"node":node,"invocation_id":invocation.id().to_string(),"result":serde_json::from_slice::<Value>(&payload)?,"publication_status":"prepared"});
    persistence::write_json(&directory.join("output.json"), &output)?;
    match invocation
        .submit(payload, emissions, contents)
        .await
        .map_err(AppError::core)?
    {
        ProposalDecision::Committed(_) => {
            output["publication_status"] = json!("committed");
            // Core has committed; these caches can be reconstructed if either
            // write fails. A cache failure must not invite repeating the work.
            let _ = persistence::write_json(&directory.join("output.json"), &output);
            if task.initial {
                let _ = runtime::complete_initial(&directory);
            }
            status(run).await
        }
        ProposalDecision::Rejected(error) => Err(AppError::new("stale_task", error.to_string())),
    }
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
        details["additional_retirements"] = json!(
            retirements
                .iter()
                .map(|(id, reason)| json!({"work_id":work_id(id),"reason":reason}))
                .collect::<Vec<_>>()
        );
    }
    error
}

pub async fn dependencies(
    session: &ontography::SessionHandle,
    payload: &Payload,
) -> Result<Vec<ontography::ContentId>> {
    match WorkflowPayload::decode(payload)? {
        WorkflowPayload::Message { .. } => Ok(vec![]),
        WorkflowPayload::Workspace(envelope) => {
            PackageStore::new(session.content_store().await.map_err(AppError::core)?)
                .dependencies(envelope.ontography_package)
                .await
                .map_err(AppError::core)
        }
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
        result.push(json!({"work_id":work_id(id),"node":node,"reason":reason}));
    }
    Ok(result)
}
