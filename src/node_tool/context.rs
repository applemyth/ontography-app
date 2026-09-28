//! One execution's node tools: what they can see, and the attempts they own.

use crate::workflow::{
    DocumentNode, Grant,
    edit::{self, WorkflowState},
    runtime,
    tasks::{Held, RetryLedger, RetryOutcome, Task, TaskSource},
};
use crate::workspace::{AttemptCheckout, WorkspaceCapture, WorkspaceStore};
use crate::{AppError, Result, views};
use futures_util::future::join_all;
use ontography::{
    ContentDigest, ContentId, ContentStore, ContextError, ExecutionContext, InvocationHandle,
    Kernel, PackageGrant, PackageId, PackageMemberGrant, Payload, SessionHandle, SessionStatus,
    content::StagedImports,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    path::PathBuf,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, watch};

/// Outcomes kept for attempts that already ended, so a repeated call is told
/// what happened instead of "unknown attempt".
const ENDED_OUTCOMES: usize = 64;
/// Page cursors kept for continuing listings.
const CURSORS: usize = 64;
/// How long ending an attempt waits for its replies to be marked sent.
const SEND_GRACE: Duration = Duration::from_secs(5);

/// What a node's tools know of the workflow: this node's settings, and the
/// workflow state that names every node. The host refreshes it after each edit.
#[derive(Clone, Debug)]
pub struct NodeScope {
    pub node: DocumentNode,
    workflow: Arc<WorkflowState>,
}

impl NodeScope {
    pub fn new(workflow: Arc<WorkflowState>, node: &str) -> Result<Self> {
        let node = workflow
            .current
            .nodes
            .iter()
            .find(|candidate| candidate.id == node)
            .cloned()
            .ok_or_else(|| AppError::invalid(format!("Unknown workflow node {node:?}")))?;
        Ok(Self { node, workflow })
    }

    /// Workflow names of the core nodes, as `kernel` shows them.
    pub(super) fn names(&self, kernel: &Kernel) -> Names {
        Names(self.workflow.node_names(kernel))
    }
}

/// Workflow names of core nodes, by core identity.
pub(super) struct Names(BTreeMap<String, String>);

impl Names {
    pub(super) fn label(&self, core_id: &str) -> String {
        edit::label(&self.0, core_id)
    }
}

/// Everything one execution's node tools share: its core handles, workflow
/// scope, retry ledger, and open attempts. Build it inside the executable, hand
/// it to a transport, and `close` it when the execution stops.
pub struct NodeToolContext {
    pub(super) execution: ExecutionContext,
    pub(super) session: SessionHandle,
    scope: watch::Receiver<NodeScope>,
    pub(super) ledger: Arc<RetryLedger>,
    pub(super) directory: PathBuf,
    pub(super) workspaces: WorkspaceStore,
    initial: Mutex<Option<Payload>>,
    attempts: Mutex<Attempts>,
    cursors: Mutex<Cursors>,
    /// Serializes the operations that start work or move packages: beginning
    /// attempts, transfers, and retirements. So one task never gets two
    /// attempts, a package is never both sent and retired, and nothing starts
    /// once `close` holds it.
    serial: tokio::sync::Mutex<()>,
}

#[derive(Default)]
struct Attempts {
    closed: bool,
    open: BTreeMap<String, Arc<Attempt>>,
    ended: VecDeque<(String, Value)>,
}

#[derive(Default)]
struct Cursors {
    issued: u64,
    /// Each token continues only the listing that issued it.
    recent: VecDeque<(String, Held, PackageId)>,
}

/// One recorded attempt at work, bound to a core invocation.
pub(super) struct Attempt {
    pub(super) id: String,
    pub(super) invocation: InvocationHandle,
    /// The queued task this attempt works on; `None` for originated work.
    pub(super) task: Option<Task>,
    /// The node's definition when the attempt began. Its failure counts
    /// only while that definition holds.
    pub(super) node: DocumentNode,
    /// The root input's handle and digest, when core triggered the attempt
    /// with bytes rather than packages.
    pub(super) root_input: Option<(String, ContentDigest)>,
    /// Member grants by handle; a view can hold many thousands of members.
    members: HashMap<String, usize>,
    /// Replies recorded for this attempt but not yet marked sent.
    pub(super) unsent: Arc<Unsent>,
    state: tokio::sync::Mutex<AttemptState>,
}

/// Counts replies awaiting their transport, so an ending attempt can let
/// their receipts be marked sent first: core records nothing after it ends.
#[derive(Default)]
pub(super) struct Unsent {
    count: AtomicUsize,
    drained: Notify,
}

/// Held by a recorded reply until it is sent or dropped.
pub(super) struct UnsentGuard(Arc<Unsent>);

impl Unsent {
    pub(super) fn hold(self: &Arc<Self>) -> UnsentGuard {
        self.count.fetch_add(1, Ordering::SeqCst);
        UnsentGuard(Arc::clone(self))
    }

    /// Waits until no reply awaits its transport, for at most `grace`.
    async fn drain(&self, grace: Duration) {
        let _ = tokio::time::timeout(grace, async {
            loop {
                let drained = self.drained.notified();
                if self.count.load(Ordering::SeqCst) == 0 {
                    return;
                }
                drained.await;
            }
        })
        .await;
    }
}

impl Drop for UnsentGuard {
    fn drop(&mut self) {
        if self.0.count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.drained.notify_waiters();
        }
    }
}

/// Resources an attempt owns until it ends.
pub(super) struct AttemptState {
    ended: bool,
    /// Pins new content while the attempt is open. Core retains whatever a
    /// submission publishes; ending the attempt releases the rest.
    staging: Option<StagedImports>,
    pub(super) outputs: BTreeMap<String, OutputRef>,
    pub(super) workspaces: BTreeMap<String, AttemptCheckout>,
    next_handle: u64,
}

/// Content created during an attempt, published by handle.
pub(super) struct OutputRef {
    pub(super) root: ContentId,
    /// A directory view can be sent as a workspace; a file can only be composed.
    pub(super) directory: bool,
    /// The complete representation closure, declared when publishing.
    pub(super) dependencies: Vec<ContentId>,
    /// Keeps a capture's new content pinned, with its validated view.
    pub(super) capture: Option<WorkspaceCapture>,
}

impl AttemptState {
    /// A store whose imports stay pinned until the attempt ends.
    pub(super) fn staging(&self) -> ContentStore {
        self.staging
            .as_ref()
            .expect("an attempt keeps its staging until it ends")
            .store()
    }

    /// A fresh handle, unique within this attempt.
    pub(super) fn handle(&mut self, prefix: &str) -> String {
        self.next_handle += 1;
        format!("{prefix}_{}", self.next_handle)
    }
}

impl Attempt {
    /// A member of one of this attempt's input views.
    pub(super) fn member(&self, handle: &str) -> Option<&PackageMemberGrant> {
        self.members
            .get(handle)
            .map(|&index| &self.invocation.members()[index])
    }

    /// One of this attempt's input packages.
    pub(super) fn grant(&self, handle: &str) -> Option<&PackageGrant> {
        self.invocation
            .packages()
            .iter()
            .find(|grant| grant.handle == handle)
    }

    /// Lets replies already handed to the transport be marked sent before the
    /// invocation ends.
    pub(super) async fn drain_replies(&self) {
        self.unsent.drain(SEND_GRACE).await;
    }

    /// Whether core has ended this attempt's invocation. Core has no status
    /// query, but marking a receipt that cannot exist (receipts count from
    /// 1) records nothing, and fails as closed only once it has ended.
    async fn ended_in_core(&self) -> bool {
        matches!(
            self.invocation.mark_sent(0).await,
            Err(ContextError::Closed)
        )
    }
}

impl NodeToolContext {
    /// `scope` must describe this execution's node. Pass the run's initial
    /// input only while it is pending at this entry node (see
    /// `workflow::runtime::initial_pending`); it is offered as a task until an
    /// attempt at it is accepted. One context serves a node directory at a
    /// time: checkouts found there belong to attempts that ended without
    /// cleanup, such as in a crash, and are removed.
    pub async fn new(
        execution: ExecutionContext,
        session: SessionHandle,
        scope: watch::Receiver<NodeScope>,
        ledger: Arc<RetryLedger>,
        directory: PathBuf,
        initial: Option<Payload>,
    ) -> Result<Self> {
        let kernel = execution.kernel().await.map_err(AppError::core)?;
        let described = {
            let current = scope.borrow();
            current.names(&kernel).label(execution.node_id()) == current.node.id
        };
        if !described {
            return Err(AppError::invalid(
                "The node scope does not describe this execution's node",
            ));
        }
        let workspaces = WorkspaceStore::new(
            execution.content_store().await.map_err(AppError::core)?,
            directory.join("tool-workspaces"),
        );
        AttemptCheckout::remove_abandoned(&workspaces).await;
        Ok(Self {
            execution,
            session,
            scope,
            ledger,
            directory,
            workspaces,
            initial: Mutex::new(initial),
            attempts: Mutex::new(Attempts::default()),
            cursors: Mutex::new(Cursors::default()),
            serial: tokio::sync::Mutex::new(()),
        })
    }

    /// The core identity of this node.
    pub fn node_id(&self) -> &str {
        self.execution.node_id()
    }

    pub fn scope(&self) -> NodeScope {
        self.scope.borrow().clone()
    }

    pub(crate) fn scope_changes(&self) -> watch::Receiver<NodeScope> {
        self.scope.clone()
    }

    /// The current graph, with the workflow names of its nodes.
    pub(super) async fn graph(&self) -> Result<(Arc<Kernel>, Names)> {
        let kernel = self.execution.kernel().await.map_err(AppError::core)?;
        let names = self.scope.borrow().names(&kernel);
        Ok((kernel, names))
    }

    /// Changes whenever something that could make work runnable changes: the
    /// run's graph or pending work, a retry decision or ended backoff, or this
    /// node's settings.
    pub(super) fn version(&self) -> String {
        let frontier = self.session.frontier().revision();
        let (retries, ended) = self.ledger.version();
        let settings = self.scope.borrow().node.digest();
        format!("{frontier}.{retries}.{ended}.{}", &settings[..16])
    }

    pub(super) fn granted(&self, grant: Grant) -> bool {
        self.scope.borrow().node.grants.contains(&grant)
    }

    pub(super) fn require(&self, grant: Grant) -> Result<()> {
        if self.granted(grant) {
            return Ok(());
        }
        Err(AppError::new(
            "not_granted",
            format!(
                "This node lacks the {:?} grant; add it to the node's grants in the workflow document",
                grant.as_str()
            ),
        ))
    }

    /// Waits for every other operation that starts work or moves packages,
    /// and holds off `close`, until the guard drops. Refused once the
    /// execution is stopping.
    pub(super) async fn exclusive(&self) -> Result<tokio::sync::MutexGuard<'_, ()>> {
        let serial = self.serial.lock().await;
        if lock(&self.attempts).closed || self.execution.stop().is_requested() {
            return Err(AppError::new(
                "execution_stopping",
                "This node's execution is stopping; it starts, moves, and retires nothing more",
            ));
        }
        Ok(serial)
    }

    /// The run's initial input, while it still waits at this entry node. The
    /// manager may discard it meanwhile, so its marker is read each time.
    pub(super) fn initial(&self) -> Result<Option<Payload>> {
        let mut initial = lock(&self.initial);
        if initial.is_some() && runtime::initial_complete(&self.directory)? {
            *initial = None;
        }
        Ok(initial.clone())
    }

    /// Never offers the initial input again, even if its marker can't be written.
    pub(super) fn complete_initial(&self) -> Result<()> {
        *lock(&self.initial) = None;
        runtime::complete_initial(&self.directory)
    }

    /// This node's tasks, excluding `busy` ones.
    pub(super) fn tasks<'a>(&'a self, busy: &'a [Task]) -> Result<TaskSource<'a>> {
        Ok(TaskSource {
            session: &self.session,
            node_id: self.node_id(),
            initial: self.initial()?.is_some(),
            ledger: &self.ledger,
            busy,
        })
    }

    /// Tasks with an open attempt; they are not offered again.
    pub(super) fn busy(&self) -> Vec<Task> {
        lock(&self.attempts)
            .open
            .values()
            .filter_map(|attempt| attempt.task.clone())
            .collect()
    }

    pub(super) fn open_attempts(&self) -> Vec<String> {
        lock(&self.attempts).open.keys().cloned().collect()
    }

    /// A token for continuing the `held` listing after `package`, which need
    /// not still be listed when the token is used.
    pub(super) fn cursor(&self, held: Held, package: PackageId) -> String {
        let mut cursors = lock(&self.cursors);
        cursors.issued += 1;
        let token = format!("page_{}", cursors.issued);
        cursors.recent.push_back((token.clone(), held, package));
        if cursors.recent.len() > CURSORS {
            cursors.recent.pop_front();
        }
        token
    }

    pub(super) fn resume(&self, held: Held, token: &str) -> Result<PackageId> {
        lock(&self.cursors)
            .recent
            .iter()
            .find(|(issued, listing, _)| issued == token && *listing == held)
            .map(|(_, _, package)| *package)
            .ok_or_else(|| {
                AppError::new(
                    "stale_cursor",
                    "This page cursor has expired; list again from the start",
                )
            })
    }

    /// Registers an attempt that core has begun under `node`'s definition.
    pub(super) async fn register(
        &self,
        invocation: InvocationHandle,
        task: Option<Task>,
        node: DocumentNode,
    ) -> Result<Arc<Attempt>> {
        let staging = self
            .execution
            .content_store()
            .await
            .map_err(AppError::core)?
            .stage_imports();
        // Core names a root input's digest only in the worker catalog.
        let catalog = invocation.tool_descriptors();
        let root_input = catalog["packages"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|entry| entry["root_input"] == true)
            .map(|entry| {
                Ok::<_, AppError>((
                    entry["handle"].as_str().unwrap_or_default().to_owned(),
                    views::digest(entry["content_digest"].as_str().unwrap_or_default())?,
                ))
            })
            .transpose()?;
        let members = invocation
            .members()
            .iter()
            .enumerate()
            .map(|(index, member)| (member.handle.clone(), index))
            .collect();
        let attempt = Arc::new(Attempt {
            id: invocation.id().to_string(),
            invocation,
            task,
            node,
            root_input,
            members,
            unsent: Arc::default(),
            state: tokio::sync::Mutex::new(AttemptState {
                ended: false,
                staging: Some(staging),
                outputs: BTreeMap::new(),
                workspaces: BTreeMap::new(),
                next_handle: 0,
            }),
        });
        lock(&self.attempts)
            .open
            .insert(attempt.id.clone(), Arc::clone(&attempt));
        Ok(attempt)
    }

    /// Finds an open attempt of this execution.
    fn attempt(&self, id: &str) -> Result<Arc<Attempt>> {
        let attempts = lock(&self.attempts);
        if let Some(attempt) = attempts.open.get(id) {
            return Ok(Arc::clone(attempt));
        }
        if let Some((_, outcome)) = attempts.ended.iter().find(|(ended, _)| ended == id) {
            return Err(ended(id).details(outcome.clone()));
        }
        Err(AppError::new(
            "unknown_attempt",
            "No open attempt of this node has that ID",
        ))
    }

    /// Runs one operation on an open attempt, holding it for the operation's
    /// duration. The attempt ends when core has ended its invocation, or its
    /// context event budget is spent: it failed, unless the execution is
    /// stopping. Other refusals, such as a read too large for the remaining
    /// bytes, leave it open.
    pub(super) async fn with_attempt<T>(
        &self,
        id: &str,
        operation: impl AsyncFnOnce(&Attempt, &mut AttemptState) -> Result<T>,
    ) -> Result<T> {
        let attempt = self.attempt(id)?;
        let mut state = attempt.state.lock().await;
        if state.ended {
            return Err(ended(id));
        }
        let error = match operation(&attempt, &mut state).await {
            Err(error) if !state.ended => error,
            result => return result,
        };
        let error = match error.code.as_str() {
            "attempt_ended" | "budget_exhausted" => error,
            // Core records every refused call, unless the event budget is
            // spent: then it ends the invocation, yet reports the refusal.
            _ if attempt.ended_in_core().await => AppError::new(
                "attempt_ended",
                format!(
                    "Core ended the attempt on refusing this call: {}",
                    error.message
                ),
            ),
            _ => return Err(error),
        };
        Err(self.abandon(&attempt, &mut state, error).await)
    }

    /// Ends an attempt that can record nothing more: core ended its
    /// invocation, or its event budget is spent. Its replies can no longer be
    /// marked sent either, so nothing is waited for.
    async fn abandon(
        &self,
        attempt: &Attempt,
        state: &mut AttemptState,
        error: AppError,
    ) -> AppError {
        let stopping =
            self.execution.stop().is_requested() || self.session.status() != SessionStatus::Open;
        let mut outcome = json!({"attempt_id": attempt.id,
            "status": if stopping { "interrupted" } else { "failed" }, "error": error.message});
        if stopping {
            let _ = attempt.invocation.interrupt(&error.message).await;
        } else {
            outcome["retry"] = json!(warn(
                &mut outcome,
                self.record_failure(attempt, &error.message, true)
            ));
            // A no-op when core has already ended the invocation.
            let _ = attempt.invocation.fail(&error.message).await;
        }
        error.details(self.finish(attempt, state, outcome).await)
    }

    /// Counts a failed attempt against its task, if it has one, under the
    /// definition the attempt began with.
    pub(super) fn record_failure(
        &self,
        attempt: &Attempt,
        reason: &str,
        retryable: bool,
    ) -> Result<Option<RetryOutcome>> {
        let node = &attempt.node;
        attempt
            .task
            .as_ref()
            .map(|task| {
                self.ledger.record_failure(
                    task,
                    reason,
                    retryable,
                    &node.retry_policy(),
                    &node.digest(),
                )
            })
            .transpose()
    }

    /// Ends an attempt and returns its outcome: forgets it, removes its
    /// checkouts, and releases the content it staged. A cleanup failure leaves
    /// core unaffected and is noted in the outcome.
    pub(super) async fn finish(
        &self,
        attempt: &Attempt,
        state: &mut AttemptState,
        mut outcome: Value,
    ) -> Value {
        state.ended = true;
        state.outputs.clear();
        state.staging = None;
        let mut failures = Vec::new();
        for (_, workspace) in std::mem::take(&mut state.workspaces) {
            if let Err(error) = workspace.remove().await {
                failures.push(error.to_string());
            }
        }
        if !failures.is_empty() {
            outcome["cleanup_errors"] = json!(failures);
        }
        let mut attempts = lock(&self.attempts);
        attempts.open.remove(&attempt.id);
        attempts
            .ended
            .push_back((attempt.id.clone(), outcome.clone()));
        if attempts.ended.len() > ENDED_OUTCOMES {
            attempts.ended.pop_front();
        }
        outcome
    }

    /// Stops new attempts, transfers, and retirements, then interrupts every
    /// open attempt and cleans up after it, once operations in progress finish
    /// and their replies are marked sent. Call when the execution stops; the
    /// attempted tasks stay pending in core.
    pub async fn close(&self) {
        let _serial = self.serial.lock().await;
        let open: Vec<_> = {
            let mut attempts = lock(&self.attempts);
            attempts.closed = true;
            attempts.open.values().cloned().collect()
        };
        join_all(open.iter().map(|attempt| async move {
            let mut state = attempt.state.lock().await;
            if state.ended {
                return;
            }
            attempt.drain_replies().await;
            let _ = attempt.invocation.interrupt("execution stopping").await;
            self.finish(attempt, &mut state, json!({"status":"interrupted"}))
                .await;
        }))
        .await;
    }

    /// This node's outgoing edges in the current graph, by target name.
    pub(super) fn successors(&self, kernel: &Kernel, names: &Names) -> Vec<(String, String)> {
        kernel
            .graph()
            .edges()
            .iter()
            .filter(|edge| edge.source() == self.node_id())
            .map(|edge| (edge.id().to_owned(), names.label(edge.target())))
            .collect()
    }

    /// This node's incoming edges in the current graph, by source name.
    pub(super) fn predecessors(&self, kernel: &Kernel, names: &Names) -> Vec<String> {
        kernel
            .graph()
            .edges()
            .iter()
            .filter(|edge| edge.target() == self.node_id())
            .map(|edge| names.label(edge.source()))
            .collect()
    }
}

fn ended(id: &str) -> AppError {
    AppError::new("attempt_ended", format!("Attempt {id} has ended"))
}

/// Keeps a follow-up failure in the outcome instead of hiding the outcome.
pub(super) fn warn<T>(outcome: &mut Value, result: Result<T>) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(error) => {
            if !outcome["warnings"].is_array() {
                outcome["warnings"] = json!([]);
            }
            if let Some(warnings) = outcome["warnings"].as_array_mut() {
                warnings.push(json!(error.to_string()));
            }
            None
        }
    }
}

/// The edge to the successor named `to`.
pub(super) fn successor_edge<'a>(successors: &'a [(String, String)], to: &str) -> Result<&'a str> {
    let mut edges = successors.iter().filter(|(_, name)| name == to);
    match (edges.next(), edges.next()) {
        (Some((edge, _)), None) => Ok(edge),
        (None, _) => Err(AppError::invalid(format!(
            "{to:?} is not a successor of this node; successors: {:?}",
            successors.iter().map(|(_, name)| name).collect::<Vec<_>>()
        ))),
        (Some(_), Some(_)) => Err(AppError::new(
            "ambiguous_successor",
            format!(
                "{to:?} names more than one connection during an edit; retry after it completes"
            ),
        )),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(super) fn context_error(error: ContextError) -> AppError {
    let code = match &error {
        ContextError::Denied(_) => "denied",
        // Core ends an invocation once it cannot record a refusal.
        ContextError::Budget(kind) if kind == "events" => "budget_exhausted",
        ContextError::Budget(_) => "too_large",
        ContextError::Closed => "attempt_ended",
        ContextError::NotFound => "unknown_handle",
        ContextError::Storage(_) => "storage_error",
        ContextError::Submit(_) => "core_error",
    };
    AppError::new(code, error.to_string())
}
