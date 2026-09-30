//! Run ownership and worker reconciliation for document workflows.

use super::{
    BoundNode, WorkflowPayload,
    edit::{self, WorkflowState},
    harness,
    tasks::RetryLedger,
};
use crate::{
    AppError, Result, node_runtime::NodeRuntime, node_tool::NodeScope, persistence,
    state::ManagedRun,
};
use ontography::{ExecutionStatus, InvocationStatus};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::watch;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialWorkflow {
    pub state: WorkflowState,
    pub input: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<PathBuf>,
}

pub struct Worker {
    pub execution_id: String,
    pub settings: watch::Sender<BoundNode>,
    /// Refreshed after every graph edit, including edits to other nodes.
    pub scope: watch::Sender<NodeScope>,
    /// A continuing session for session implementations (agents and
    /// programs). Other implementations use the task harness.
    pub node: Option<Arc<NodeRuntime>>,
    /// Shared with the harness and flow tools. A relaunched worker keeps its
    /// predecessor's instance, so a node never has two writers.
    pub ledger: Arc<RetryLedger>,
}

pub fn state_path(run: &ManagedRun) -> PathBuf {
    run.directory.join("workflow.json")
}

pub fn node_directory(run: &ManagedRun, node_id: &str) -> PathBuf {
    run.directory.join("nodes").join(node_id)
}

pub fn load(run: &ManagedRun) -> Result<WorkflowState> {
    let initial = run.manifest.workflow.as_ref().ok_or_else(|| {
        AppError::new(
            "not_a_workflow",
            "This run was created with the legacy graph interface",
        )
    })?;
    let path = state_path(run);
    if path.exists() {
        edit::load(&path)
    } else {
        let state = initial.state.clone().with_bindings()?;
        edit::store(&path, &state)?;
        Ok(state)
    }
}

/// Settle durable graph intent before any new executable gets custody.
pub async fn resume(run: &mut ManagedRun) -> Result<()> {
    let mut state = load(run)?;
    let session = run.live()?.session.clone();
    edit::recover(&session, &mut state, &state_path(run))
        .await
        .map_err(super::tools::public_error)?;
    reconcile(run, &state, true).await
}

/// A root invocation is accepted in core before its completion cache is written.
/// Read core when that cache is absent; an interrupted root is retried, never an
/// accepted one. Normal tasks recover through their unconsumed package inputs.
pub async fn initial_pending(run: &ManagedRun) -> Result<bool> {
    let initial = run
        .manifest
        .workflow
        .as_ref()
        .ok_or_else(|| AppError::invalid("Workflow required"))?;
    let id = &initial.state.identities.nodes[&initial.state.current.entry];
    let marker = initial_marker(&node_directory(run, id));
    if marker.exists() {
        return persistence::read_json::<bool>(&marker).map(|complete| !complete);
    }
    let mut after = None;
    loop {
        let page = run
            .live()?
            .session
            .invocations_page(Some(id), after.as_deref(), 100)
            .await
            .map_err(AppError::core)?;
        if page.iter().any(|invocation| {
            invocation.packages.is_empty() && invocation.status == InvocationStatus::Accepted
        }) {
            persistence::write_json(&marker, &true)?;
            return Ok(false);
        }
        if page.len() < 100 {
            return Ok(true);
        }
        after = page.last().map(|invocation| invocation.id.to_string());
    }
}

pub fn complete_initial(node_directory: &Path) -> Result<()> {
    persistence::write_json(&initial_marker(node_directory), &true)
}

/// Whether the entry's initial input is known to be done: accepted, or
/// discarded by the manager. Workers re-read this before offering it again.
pub fn initial_complete(node_directory: &Path) -> Result<bool> {
    let marker = initial_marker(node_directory);
    Ok(marker.exists() && persistence::read_json::<bool>(&marker)?)
}

fn initial_marker(node_directory: &Path) -> PathBuf {
    node_directory.join("initial-complete.json")
}

pub async fn initial_payload(run: &ManagedRun) -> Result<ontography::Payload> {
    let initial = run
        .manifest
        .workflow
        .as_ref()
        .ok_or_else(|| AppError::invalid("Workflow required"))?;
    let path = run.directory.join("initial-input.json");
    if path.exists() {
        return WorkflowPayload::from_value(&persistence::read_json(&path)?)?.encode();
    }
    let payload = if let Some(directory) = &initial.workspace {
        let content = run
            .live()?
            .session
            .content_store()
            .await
            .map_err(AppError::core)?;
        let store = crate::workspace::WorkspaceStore::new(content, run.workspace()?);
        let package = store
            .import_directory(directory)
            .await
            .map_err(AppError::core)?;
        WorkflowPayload::Workspace(ontography::PackageEnvelope::new(package.root()))
    } else {
        WorkflowPayload::from_value(&initial.input)?
    };
    let bytes = payload.encode()?;
    persistence::write_json(&path, &payload)?;
    Ok(bytes)
}

/// A session runs one exact implementation, so any change to its binding needs
/// a new process. The task harness reads its settings as it goes and needs a
/// new worker only for a different implementation.
fn needs_new_worker(current: &BoundNode, next: &BoundNode) -> bool {
    let (current, next) = (
        &current.binding.implementation,
        &next.binding.implementation,
    );
    current.kind() != next.kind() || (next.is_session() && current != next)
}

pub async fn reconcile(
    run: &mut ManagedRun,
    state: &WorkflowState,
    retry_failed: bool,
) -> Result<()> {
    if state.pending.is_some() {
        return Err(AppError::new(
            "pending_edit",
            "Finish the saved edit before launching workers",
        ));
    }
    let kernel = run.live()?.session.kernel().await.map_err(AppError::core)?;
    let desired = state
        .current
        .nodes
        .iter()
        .map(|node| {
            Ok((
                &state.identities.nodes[&node.id],
                state.bound_node(&node.id)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let removed: Vec<_> = run
        .live()?
        .workers
        .iter()
        .filter(|(id, worker)| {
            kernel.graph().node(id.as_str()).is_none()
                || desired
                    .get(*id)
                    .is_none_or(|next| needs_new_worker(&worker.settings.borrow(), next))
        })
        .map(|(id, _)| id.clone())
        .collect();
    // A replacement continues its node's retry ledger. Session launch settings
    // require a new process; graph grants and task policy update in place.
    // Each ledger is renewed below with its node's definition, so a changed
    // node gets fresh attempts even if this reconcile stops early.
    let mut ledgers = BTreeMap::new();
    for id in removed {
        let live = run.live_mut()?;
        let Some(worker) = live.workers.remove(&id) else {
            continue;
        };
        if let Some(handle) = live.executions.remove(&worker.execution_id) {
            handle.request_stop();
            if tokio::time::timeout(std::time::Duration::from_secs(3), handle.wait())
                .await
                .is_err()
            {
                handle.abort();
                handle.wait().await;
            }
        }
        ledgers.insert(id, worker.ledger);
    }
    let pending_initial = initial_pending(run).await?;
    let initial = run
        .manifest
        .workflow
        .as_ref()
        .expect("workflow checked")
        .clone();
    let original_entry = &initial.state.identities.nodes[&initial.state.current.entry];
    let workflow = Arc::new(state.clone());
    for (id, node) in desired {
        let definition = node.digest();
        let scope = NodeScope::new(workflow.clone(), &node.node.id)?;
        let mut ledger = ledgers.remove(id);
        let existing = run.live()?.workers.get(id).map(|worker| {
            (
                worker.execution_id.clone(),
                *worker.settings.borrow() != node,
            )
        });
        if let Some((execution_id, changed)) = existing {
            let live = run.live_mut()?;
            let active = live
                .executions
                .get(&execution_id)
                .is_some_and(|execution| matches!(execution.status(), ExecutionStatus::Running));
            // A running worker takes new settings in place; a stopped one is
            // relaunched only for a change or a resume.
            if active || !(changed || retry_failed) {
                let worker = &live.workers[id];
                if changed {
                    worker.settings.send_replace(node);
                }
                // Node tools resolve names and grants from the whole graph.
                // An edge or another node can change without changing this
                // worker's own definition.
                worker.scope.send_replace(scope);
                // After the new settings, so no retried task starts with the old ones.
                worker.ledger.renew(&definition)?;
                continue;
            }
            live.executions.remove(&execution_id);
            ledger = live.workers.remove(id).map(|worker| worker.ledger);
        }
        let directory = node_directory(run, id);
        let ledger = match ledger {
            Some(ledger) => ledger,
            // Parked tasks stay parked across suspension and restart.
            None => RetryLedger::open(directory.join("retry.json"))?,
        };
        ledger.renew(&definition)?;
        let (settings, receiver) = watch::channel(node.clone());
        let (scope, scope_receiver) = watch::channel(scope);
        let input = if pending_initial && id == original_entry {
            Some(initial_payload(run).await?)
        } else {
            None
        };
        std::fs::create_dir_all(&directory)?;
        let project = run.manifest.project.clone();
        let session = run.live()?.session.clone();
        let node_runtime = node.binding.implementation.is_session().then(|| {
            NodeRuntime::new(
                project.clone(),
                directory.clone(),
                run.environment.clone(),
                session.clone(),
                scope_receiver,
                ledger.clone(),
                input.clone(),
            )
        });
        let handle = if let Some(runtime) = &node_runtime {
            let runtime = runtime.clone();
            run.live()?
                .host
                .launch(id.as_str(), move |context| runtime.clone().run(context))
                .await
                .map_err(AppError::core)?
        } else {
            let worker_ledger = ledger.clone();
            let place = harness::Place {
                project: project.clone(),
                directory: directory.clone(),
                environment: run.environment.clone(),
            };
            run.live()?
                .host
                .launch(id.as_str(), move |context| {
                    harness::run(
                        context,
                        receiver.clone(),
                        place.clone(),
                        input.clone(),
                        session.clone(),
                        worker_ledger.clone(),
                    )
                })
                .await
                .map_err(AppError::core)?
        };
        let execution_id = uuid::Uuid::new_v4().to_string();
        let live = run.live_mut()?;
        live.executions.insert(execution_id.clone(), handle);
        live.workers.insert(
            id.clone(),
            Worker {
                execution_id,
                settings,
                scope,
                node: node_runtime,
                ledger,
            },
        );
    }
    Ok(())
}
