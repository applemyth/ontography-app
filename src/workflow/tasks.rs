//! Task identity, selection, and retries, shared by workers, node tools, and
//! the manager.
//!
//! A task is one unit of work at a node: one input, one joined input set, or
//! the run's initial input at its entry. Each failed attempt is counted in a
//! small ledger beside the node's other state. A failing task waits with
//! exponential backoff while other tasks proceed, and is parked once its
//! attempts are used up or its failure is not worth retrying; a failed join
//! keeps its inputs together meanwhile. Parked tasks stay pending in core until
//! the manager retries or discards them.
//!
//! Counts live in the ledger rather than being derived from core's invocation
//! records, which cannot tell an agent-originated root attempt from an attempt
//! at the initial input. Core's records remain the audit trail.

use crate::{AppError, Result, persistence};
use ontography::{Authority, Delivery, IngressMode, PackageId, PackageRecord, SessionHandle};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};
use tokio::{sync::watch, time::Instant};

const LEDGER_VERSION: u32 = 1;
/// The largest core page a scan for held packages reads.
const MAX_PAGE: usize = 64;
/// Recorded failure text is diagnostic; keep the ledger file small.
const MAX_ERROR_CHARS: usize = 2000;

/// How often a failing task is attempted, and how long it waits in between.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    /// Attempts per task, including the first; 1 disables retries.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Wait before the second attempt; each later wait doubles.
    #[serde(default = "default_initial_delay")]
    pub initial_delay_secs: u64,
    /// Upper bound on any single wait.
    #[serde(default = "default_max_delay")]
    pub max_delay_secs: u64,
}

const fn default_max_attempts() -> u32 {
    3
}
const fn default_initial_delay() -> u64 {
    5
}
const fn default_max_delay() -> u64 {
    300
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            initial_delay_secs: default_initial_delay(),
            max_delay_secs: default_max_delay(),
        }
    }
}

impl RetryPolicy {
    pub const MAX_ATTEMPTS: u32 = 100;
    pub const MAX_DELAY_SECS: u64 = 86_400;

    pub fn validate(&self) -> std::result::Result<(), &'static str> {
        if !(1..=Self::MAX_ATTEMPTS).contains(&self.max_attempts) {
            return Err("retry max_attempts must be between 1 and 100");
        }
        if self.max_delay_secs > Self::MAX_DELAY_SECS {
            return Err("retry max_delay_secs must be at most 86400");
        }
        if self.initial_delay_secs > self.max_delay_secs {
            return Err("retry initial_delay_secs must not exceed max_delay_secs");
        }
        Ok(())
    }

    /// The wait after a task's `failures`-th failed attempt.
    pub fn delay(&self, failures: u32) -> Duration {
        let factor = 1u64
            .checked_shl(failures.saturating_sub(1))
            .unwrap_or(u64::MAX);
        Duration::from_secs(
            self.initial_delay_secs
                .saturating_mul(factor)
                .min(self.max_delay_secs),
        )
    }
}

/// Stable identity of a task: its node and exact trigger packages.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskKey(String);

impl TaskKey {
    /// Trigger order is irrelevant. The initial input has no packages.
    pub fn new(node_id: &str, packages: &[PackageId]) -> Self {
        let mut packages = packages.to_vec();
        packages.sort_unstable();
        let ids: Vec<_> = packages.iter().map(ToString::to_string).collect();
        Self(format!(
            "task_{:x}",
            Sha256::digest(json!([node_id, ids]).to_string())
        ))
    }

    /// Accepts only the exact form this type produces.
    pub fn parse(text: &str) -> Result<Self> {
        if !text.strip_prefix("task_").is_some_and(is_digest) {
            return Err(AppError::invalid("Invalid task_id"));
        }
        Ok(Self(text.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TaskKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Opaque handle for one package, so callers never see core identities.
pub fn work_id(package: &PackageId) -> String {
    format!("work_{:x}", Sha256::digest(package.to_string()))
}

/// Accepts only the exact form `work_id` produces, before any lookup.
pub fn parse_work_id(text: &str) -> Result<&str> {
    if !text.strip_prefix("work_").is_some_and(is_digest) {
        return Err(AppError::invalid("Invalid work_id"));
    }
    Ok(text)
}

fn is_digest(hex: &str) -> bool {
    hex.len() == 64
        && hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Whole seconds, rounded up so nothing is promised sooner than it happens.
pub fn ceil_secs(duration: Duration) -> u64 {
    duration.as_secs() + u64::from(duration.subsec_nanos() > 0)
}

/// Sleeps until `deadline`, or forever without one.
pub async fn wake_at(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// One unit of work at a node, identified by its trigger.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Task {
    pub key: TaskKey,
    /// Trigger packages with core's records, in canonical order; empty for the
    /// initial input. Their digests and producers never change.
    pub inputs: Vec<(PackageId, PackageRecord)>,
}

impl Task {
    pub fn initial(node_id: &str) -> Self {
        Self::packages(node_id, Vec::new())
    }

    pub fn packages(node_id: &str, mut inputs: Vec<(PackageId, PackageRecord)>) -> Self {
        inputs.sort_unstable_by_key(|(id, _)| *id);
        Self {
            key: TaskKey::new(
                node_id,
                &inputs.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            ),
            inputs,
        }
    }

    pub fn ids(&self) -> Vec<PackageId> {
        self.inputs.iter().map(|(id, _)| *id).collect()
    }

    pub fn is_initial(&self) -> bool {
        self.inputs.is_empty()
    }
}

/// Where to look for tasks at one node, and the work already spoken for.
pub struct TaskSource<'a> {
    pub session: &'a SessionHandle,
    pub node_id: &'a str,
    /// Offer the run's initial input first.
    pub initial: bool,
    pub ledger: &'a RetryLedger,
    /// Tasks with an open attempt: never offered again, and their inputs are
    /// not paired into other joins.
    pub busy: &'a [Task],
}

/// How a node takes its inputs in the current graph.
enum Ingress {
    /// Each input alone.
    Any,
    /// One input on every incoming edge, under a common authority.
    All(BTreeSet<Arc<str>>),
}

impl TaskSource<'_> {
    /// The next task the ledger allows now. When there is none,
    /// `RetryLedger::next_wake` says when a waiting task becomes runnable.
    pub async fn next(&self) -> Result<Option<Task>> {
        self.first(|task| self.ledger.standing(&task.key) == Standing::Ready)
            .await
    }

    /// The pending task with this key, whatever its retry state.
    pub async fn find(&self, key: &TaskKey) -> Result<Option<Task>> {
        self.first(|task| task.key == *key).await
    }

    /// The task `inputs` still form, if any: the initial input while it is
    /// offered, or packages waiting here that make a complete trigger in the
    /// current graph. A task that lost an input, or a join whose node gained
    /// or lost a connection since, is no longer a task.
    pub async fn current(&self, inputs: &[PackageId]) -> Result<Option<Task>> {
        match self.ingress().await? {
            Some(ingress) => self.resolve(&ingress, inputs).await,
            None => Ok(None),
        }
    }

    /// Recorded failures, each with the task it still is, if any.
    pub async fn failures(&self) -> Result<Vec<(FailedTask, Option<Task>)>> {
        let ingress = self.ingress().await?;
        let mut failures = Vec::new();
        for failed in self.ledger.failed() {
            let task = match &ingress {
                Some(ingress) => self.resolve(ingress, &failed.failures.inputs).await?,
                None => None,
            };
            failures.push((failed, task));
        }
        Ok(failures)
    }

    /// The first task `accept` takes that no open attempt holds: the initial
    /// input when offered, then inputs in core's stable order, or for `all`
    /// ingress the node's complete joins.
    async fn first(&self, mut accept: impl FnMut(&Task) -> bool) -> Result<Option<Task>> {
        let mut take =
            |task: &Task| !self.busy.iter().any(|busy| busy.key == task.key) && accept(task);
        if self.initial {
            let task = Task::initial(self.node_id);
            if take(&task) {
                return Ok(Some(task));
            }
        }
        match self.ingress().await? {
            None => Ok(None),
            Some(Ingress::Any) => Ok(find_held(
                self.session,
                self.node_id,
                Held::Received,
                |id, record| take(&Task::packages(self.node_id, vec![(*id, record.clone())])),
            )
            .await?
            .map(|input| Task::packages(self.node_id, vec![input]))),
            Some(Ingress::All(incoming)) => self.join(&incoming, take).await,
        }
    }

    /// The first complete join `take` accepts. A failed join keeps its inputs
    /// while they still form one, so its retries use the same set; a busy
    /// join keeps them until its attempt ends. Every other join is the first
    /// free input on each incoming edge under one authority: core's own next
    /// join when it takes nothing held, and otherwise found by a scan.
    async fn join(
        &self,
        incoming: &BTreeSet<Arc<str>>,
        mut take: impl FnMut(&Task) -> bool,
    ) -> Result<Option<Task>> {
        if incoming.is_empty() {
            return Ok(None);
        }
        let mut held: BTreeSet<_> = self.busy.iter().flat_map(Task::ids).collect();
        for failed in self.ledger.failed() {
            let Some(task) = self.joined(incoming, &failed.failures.inputs).await? else {
                continue;
            };
            // An input belongs to one held join at most.
            if task.inputs.iter().any(|(id, _)| held.contains(id)) {
                continue;
            }
            held.extend(task.ids());
            if take(&task) {
                return Ok(Some(task));
            }
        }
        let offered = self
            .session
            .next_trigger_at(self.node_id)
            .await
            .map_err(AppError::core)?;
        if offered.packages().is_empty() {
            // Core offers a join whenever any is complete.
            return Ok(None);
        }
        if offered.packages().iter().all(|(id, _)| !held.contains(id)) {
            let task = Task::packages(self.node_id, offered.packages().to_vec());
            if take(&task) {
                return Ok(Some(task));
            }
        }
        self.scan(incoming, &held, take).await
    }

    /// Reads the node's inputs in core's stable order for the first free
    /// join of each authority, until `take` accepts one. It keeps one input
    /// per edge and authority however long the backlog, which it reads
    /// whole only when no free join is complete.
    async fn scan(
        &self,
        incoming: &BTreeSet<Arc<str>>,
        held: &BTreeSet<PackageId>,
        mut take: impl FnMut(&Task) -> bool,
    ) -> Result<Option<Task>> {
        let mut heads: BTreeMap<Authority, BTreeMap<&str, (PackageId, PackageRecord)>> =
            BTreeMap::new();
        let mut joined = BTreeSet::new();
        let mut after = None;
        loop {
            let page = self
                .session
                .pending_page_at(self.node_id, after, MAX_PAGE)
                .await
                .map_err(AppError::core)?;
            for (id, record) in page.packages() {
                let Some(edge) = record
                    .delivery()
                    .and_then(|delivery| incoming.get(delivery.edge_id()))
                else {
                    continue;
                };
                if held.contains(id) || joined.contains(record.authority()) {
                    continue;
                }
                let edges = heads.entry(record.authority().clone()).or_default();
                edges.entry(edge).or_insert_with(|| (*id, record.clone()));
                if edges.len() < incoming.len() {
                    continue;
                }
                let inputs = heads.remove(record.authority()).unwrap_or_default();
                joined.insert(record.authority().clone());
                let task = Task::packages(self.node_id, inputs.into_values().collect());
                if take(&task) {
                    return Ok(Some(task));
                }
            }
            if page.packages().len() < MAX_PAGE {
                return Ok(None);
            }
            after = page.packages().last().map(|(id, _)| *id);
        }
    }

    async fn ingress(&self) -> Result<Option<Ingress>> {
        let kernel = self.session.kernel().await.map_err(AppError::core)?;
        // A rewrite may have removed this node; its worker is about to be stopped.
        Ok(kernel
            .node_definition(self.node_id)
            .map(|definition| match definition.ingress_mode() {
                IngressMode::Any => Ingress::Any,
                IngressMode::All => Ingress::All(
                    kernel
                        .graph()
                        .incoming_edge_ids(self.node_id)
                        .cloned()
                        .unwrap_or_default(),
                ),
            }))
    }

    /// See `current`.
    async fn resolve(&self, ingress: &Ingress, inputs: &[PackageId]) -> Result<Option<Task>> {
        match ingress {
            _ if inputs.is_empty() => Ok(self.initial.then(|| Task::initial(self.node_id))),
            Ingress::Any if inputs.len() == 1 => Ok(self
                .waiting(inputs)
                .await?
                .map(|records| Task::packages(self.node_id, records))),
            Ingress::Any => Ok(None),
            Ingress::All(incoming) => self.joined(incoming, inputs).await,
        }
    }

    /// `inputs` as a join, if they still form one: each waiting here, one per
    /// incoming edge, under a common authority.
    async fn joined(
        &self,
        incoming: &BTreeSet<Arc<str>>,
        inputs: &[PackageId],
    ) -> Result<Option<Task>> {
        if inputs.len() != incoming.len() {
            return Ok(None);
        }
        let Some(records) = self.waiting(inputs).await? else {
            return Ok(None);
        };
        let edges: BTreeSet<_> = records
            .iter()
            .filter_map(|(_, record)| record.delivery().map(Delivery::edge_id))
            .collect();
        let complete = edges.len() == incoming.len()
            && edges.iter().all(|edge| incoming.contains(*edge))
            && records
                .windows(2)
                .all(|pair| pair[0].1.authority() == pair[1].1.authority());
        Ok(complete.then(|| Task::packages(self.node_id, records)))
    }

    /// The records of `inputs`, if each still waits at this node.
    async fn waiting(
        &self,
        inputs: &[PackageId],
    ) -> Result<Option<Vec<(PackageId, PackageRecord)>>> {
        let mut records = Vec::with_capacity(inputs.len());
        for id in inputs {
            match self
                .session
                .package_history(*id)
                .await
                .map_err(AppError::core)?
            {
                Some(history) if waiting_at(history.package(), self.node_id) => {
                    records.push((*id, history.package().clone()));
                }
                _ => return Ok(None),
            }
        }
        Ok(Some(records))
    }
}

/// Whether a package was delivered to the node and still waits there.
fn waiting_at(record: &PackageRecord, node_id: &str) -> bool {
    record.is_live()
        && record
            .delivery()
            .is_some_and(|delivery| delivery.receiver() == node_id)
}

/// Where a node holds packages: delivered and waiting to be consumed, or
/// created here and waiting to be sent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Held {
    Received,
    Outbound,
}

/// The first package the node holds, in core's stable order, that `accept`
/// takes. Pages start small because the first package usually decides.
pub async fn find_held(
    session: &SessionHandle,
    node_id: &str,
    held: Held,
    mut accept: impl FnMut(&PackageId, &PackageRecord) -> bool,
) -> Result<Option<(PackageId, PackageRecord)>> {
    let mut after = None;
    let mut limit = 1;
    loop {
        let page = match held {
            Held::Received => session.pending_page_at(node_id, after, limit).await,
            Held::Outbound => session.outbound_page(Some(node_id), after, limit).await,
        }
        .map_err(AppError::core)?;
        if let Some((id, record)) = page
            .packages()
            .iter()
            .find(|(id, record)| accept(id, record))
        {
            return Ok(Some((*id, record.clone())));
        }
        if page.packages().len() < limit {
            return Ok(None);
        }
        after = page.packages().last().map(|(id, _)| *id);
        limit = (limit * 2).min(MAX_PAGE);
    }
}

/// Failed attempts recorded for one task.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskFailures {
    /// Failed attempts since the task was last retried by the manager.
    pub attempts: u32,
    /// The most recent failure.
    pub error: String,
    /// No further automatic attempts: retries are used up or not worthwhile.
    pub parked: bool,
    /// Trigger packages, so a parked task can be discarded exactly.
    #[serde(with = "package_ids")]
    pub inputs: Vec<PackageId>,
}

mod package_ids {
    use super::{Deserialize, Deserializer, PackageId, Serializer};
    use serde::de::Error;

    pub fn serialize<S: Serializer>(ids: &[PackageId], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(ids.iter().map(ToString::to_string))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<PackageId>, D::Error> {
        Vec::<String>::deserialize(deserializer)?
            .iter()
            .map(|id| crate::views::package_id(id).map_err(D::Error::custom))
            .collect()
    }
}

/// Whether a task may be attempted now.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Standing {
    Ready,
    Waiting(Instant),
    Parked,
}

/// What follows a failed attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RetryOutcome {
    Retrying {
        attempts: u32,
        retry_in_secs: u64,
    },
    Parked {
        attempts: u32,
    },
    /// The node's definition changed during the attempt, so its failure does
    /// not count: the task starts afresh under the new definition.
    Fresh,
}

/// A failed task as the manager sees it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailedTask {
    pub key: TaskKey,
    pub failures: TaskFailures,
    /// Remaining backoff for a task that is waiting to be retried.
    pub retry_in: Option<Duration>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LedgerFile {
    version: u32,
    #[serde(default)]
    definition: String,
    tasks: BTreeMap<TaskKey, TaskFailures>,
}

struct LedgerState {
    /// The node definition the failures happened under; see `renew`.
    definition: String,
    tasks: BTreeMap<TaskKey, TaskFailures>,
    /// Backoff deadlines are not persisted: after a restart, retries come sooner.
    waits: HashMap<TaskKey, Instant>,
}

/// Durable retry state for one node's tasks, shared by its worker, its node
/// tools, and the manager's flow tools.
pub struct RetryLedger {
    path: PathBuf,
    state: Mutex<LedgerState>,
    changes: watch::Sender<u64>,
}

impl fmt::Debug for RetryLedger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetryLedger")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl RetryLedger {
    pub fn open(path: impl Into<PathBuf>) -> Result<Arc<Self>> {
        let path = path.into();
        let (definition, tasks) = if path.exists() {
            let file: LedgerFile = persistence::read_json(&path)?;
            if file.version != LEDGER_VERSION {
                return Err(AppError::new(
                    "invalid_retry_ledger",
                    format!("Unsupported retry ledger version {}", file.version),
                ));
            }
            (file.definition, file.tasks)
        } else {
            (String::new(), BTreeMap::new())
        };
        Ok(Arc::new(Self {
            path,
            state: Mutex::new(LedgerState {
                definition,
                tasks,
                waits: HashMap::new(),
            }),
            changes: watch::channel(0).0,
        }))
    }

    pub fn standing(&self, key: &TaskKey) -> Standing {
        let state = self.lock();
        if state.tasks.get(key).is_some_and(|record| record.parked) {
            return Standing::Parked;
        }
        match state.waits.get(key) {
            Some(&until) if until > Instant::now() => Standing::Waiting(until),
            _ => Standing::Ready,
        }
    }

    /// Records a failed attempt made under `definition` with its `policy`.
    /// The task is parked when its attempts are used up or `retryable` is
    /// false; otherwise it waits out its backoff. A failure under any other
    /// definition than the current one does not count.
    pub fn record_failure(
        &self,
        task: &Task,
        error: &str,
        retryable: bool,
        policy: &RetryPolicy,
        definition: &str,
    ) -> Result<RetryOutcome> {
        self.update(|state| {
            if state.definition.is_empty() {
                // Never renewed: the first failure names the definition.
                state.definition = definition.to_owned();
            }
            if state.definition != definition {
                return RetryOutcome::Fresh;
            }
            let record = state
                .tasks
                .entry(task.key.clone())
                .or_insert_with(|| TaskFailures {
                    attempts: 0,
                    error: String::new(),
                    parked: false,
                    inputs: task.ids(),
                });
            record.attempts = record.attempts.saturating_add(1);
            record.error = error.chars().take(MAX_ERROR_CHARS).collect();
            record.parked = !retryable || record.attempts >= policy.max_attempts;
            let attempts = record.attempts;
            if record.parked {
                state.waits.remove(&task.key);
                return RetryOutcome::Parked { attempts };
            }
            let delay = policy.delay(attempts);
            state.waits.insert(task.key.clone(), Instant::now() + delay);
            RetryOutcome::Retrying {
                attempts,
                retry_in_secs: delay.as_secs(),
            }
        })
    }

    /// Clears a task's failures: after it is accepted, when the manager gives
    /// it fresh attempts, or once it is discarded. Returns what was recorded.
    pub fn clear(&self, key: &TaskKey) -> Result<Option<TaskFailures>> {
        {
            let state = self.lock();
            if !state.tasks.contains_key(key) && !state.waits.contains_key(key) {
                return Ok(None);
            }
        }
        self.update(|state| {
            state.waits.remove(key);
            state.tasks.remove(key)
        })
    }

    /// Failures count against the node definition they happened under, named
    /// by `definition`: a changed definition gives every failed task fresh
    /// attempts. The definition is durable, so a renewal that cannot be
    /// written now happens on the next call.
    pub fn renew(&self, definition: &str) -> Result<()> {
        {
            let mut state = self.lock();
            if state.definition == definition {
                return Ok(());
            }
            if state.tasks.is_empty() {
                // Nothing to forget; the next write records the definition.
                state.definition = definition.to_owned();
                return Ok(());
            }
        }
        self.update(|state| {
            state.definition = definition.to_owned();
            state.tasks.clear();
            state.waits.clear();
        })
    }

    /// Gives every failed task a fresh set of attempts.
    pub fn retry_all(&self) -> Result<()> {
        self.update(|state| {
            state.waits.clear();
            state.tasks.clear();
        })
    }

    pub fn get(&self, key: &TaskKey) -> Option<TaskFailures> {
        self.lock().tasks.get(key).cloned()
    }

    /// Changes whenever retry state does, including when a backoff runs out:
    /// the count of recorded changes, and of waits that have ended.
    pub fn version(&self) -> (u64, usize) {
        let now = Instant::now();
        let ended = self
            .lock()
            .waits
            .values()
            .filter(|until| **until <= now)
            .count();
        (*self.changes.borrow(), ended)
    }

    /// The earliest moment a task waiting out its backoff becomes runnable.
    pub fn next_wake(&self) -> Option<Instant> {
        let now = Instant::now();
        self.lock()
            .waits
            .values()
            .copied()
            .filter(|until| *until > now)
            .min()
    }

    pub fn failed(&self) -> Vec<FailedTask> {
        let state = self.lock();
        let now = Instant::now();
        state
            .tasks
            .iter()
            .map(|(key, failures)| FailedTask {
                key: key.clone(),
                failures: failures.clone(),
                retry_in: state
                    .waits
                    .get(key)
                    .filter(|until| **until > now)
                    .map(|until| *until - now),
            })
            .collect()
    }

    /// Observes every change, including manager retries that make parked work
    /// runnable again. Backoff expiry is not a change; wait for it separately.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    fn lock(&self) -> MutexGuard<'_, LedgerState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Applies a change and persists it before anyone can observe it. A failed
    /// write leaves the ledger exactly as it was.
    fn update<T>(&self, change: impl FnOnce(&mut LedgerState) -> T) -> Result<T> {
        let mut state = self.lock();
        let definition = state.definition.clone();
        let tasks = state.tasks.clone();
        let waits = state.waits.clone();
        let value = change(&mut state);
        if state.tasks != tasks || state.definition != definition {
            let file = LedgerFile {
                version: LEDGER_VERSION,
                definition: state.definition.clone(),
                tasks: state.tasks.clone(),
            };
            if let Err(error) = persistence::write_json(&self.path, &file) {
                state.definition = definition;
                state.tasks = tasks;
                state.waits = waits;
                return Err(error);
            }
        } else if state.waits == waits {
            return Ok(value);
        }
        drop(state);
        self.changes
            .send_modify(|version| *version = version.wrapping_add(1));
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ontography::ActivationId;

    fn package(n: u128) -> PackageId {
        PackageId::from_parts(ActivationId::from_u128(n), n)
    }

    fn record() -> PackageRecord {
        PackageRecord::new(
            "WorkflowPayload",
            ontography::Authority::new([]),
            ontography::ContentDigest::compute(b"input"),
            "producer",
            None,
            ontography::PackageStatus::Live,
        )
    }

    #[test]
    fn delays_double_up_to_the_cap_without_overflow() {
        let policy = RetryPolicy {
            max_attempts: 10,
            initial_delay_secs: 5,
            max_delay_secs: 60,
        };
        let delays: Vec<_> = (1..=6).map(|n| policy.delay(n).as_secs()).collect();
        assert_eq!(delays, [5, 10, 20, 40, 60, 60]);
        assert_eq!(policy.delay(u32::MAX).as_secs(), 60);
        assert!(RetryPolicy::default().validate().is_ok());
        for invalid in [
            RetryPolicy {
                max_attempts: 0,
                ..RetryPolicy::default()
            },
            RetryPolicy {
                initial_delay_secs: 10,
                max_delay_secs: 5,
                ..RetryPolicy::default()
            },
            RetryPolicy {
                max_delay_secs: RetryPolicy::MAX_DELAY_SECS + 1,
                ..RetryPolicy::default()
            },
        ] {
            assert!(invalid.validate().is_err());
        }
    }

    #[test]
    fn task_keys_ignore_trigger_order_and_parse_only_their_own_form() {
        let forward = TaskKey::new("node", &[package(1), package(2)]);
        assert_eq!(forward, TaskKey::new("node", &[package(2), package(1)]));
        assert_ne!(forward, TaskKey::new("other", &[package(1), package(2)]));
        assert_ne!(TaskKey::new("node", &[]), forward);
        assert_eq!(TaskKey::parse(forward.as_str()).unwrap(), forward);
        for invalid in ["task_", "work_00", &forward.as_str().to_uppercase()] {
            assert!(TaskKey::parse(invalid).is_err());
        }
        let work = work_id(&package(1));
        assert_eq!(parse_work_id(&work).unwrap(), work);
        assert!(parse_work_id(forward.as_str()).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn failures_back_off_park_and_survive_reopening() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("retry.json");
        let ledger = RetryLedger::open(&path).unwrap();
        let task = Task::packages("node", vec![(package(7), record())]);
        let policy = RetryPolicy {
            max_attempts: 2,
            initial_delay_secs: 10,
            max_delay_secs: 60,
        };
        let changes = ledger.subscribe();
        assert_eq!(ledger.standing(&task.key), Standing::Ready);
        assert_eq!(
            ledger
                .record_failure(&task, "first", true, &policy, "node")
                .unwrap(),
            RetryOutcome::Retrying {
                attempts: 1,
                retry_in_secs: 10
            }
        );
        assert!(changes.has_changed().unwrap());
        assert!(matches!(ledger.standing(&task.key), Standing::Waiting(_)));
        tokio::time::advance(Duration::from_secs(11)).await;
        assert_eq!(ledger.standing(&task.key), Standing::Ready);
        assert_eq!(
            ledger
                .record_failure(&task, "second", true, &policy, "node")
                .unwrap(),
            RetryOutcome::Parked { attempts: 2 }
        );
        assert_eq!(ledger.standing(&task.key), Standing::Parked);

        let reopened = RetryLedger::open(&path).unwrap();
        let recorded = reopened.get(&task.key).unwrap();
        assert!(recorded.parked);
        assert_eq!(recorded.error, "second");
        assert_eq!(recorded.inputs, vec![package(7)]);
        assert!(reopened.clear(&task.key).unwrap().is_some());
        assert!(reopened.clear(&task.key).unwrap().is_none());
        assert_eq!(reopened.standing(&task.key), Standing::Ready);
        assert!(RetryLedger::open(&path).unwrap().failed().is_empty());
    }

    #[tokio::test]
    async fn a_failure_not_worth_retrying_parks_immediately() {
        let directory = tempfile::tempdir().unwrap();
        let ledger = RetryLedger::open(directory.path().join("retry.json")).unwrap();
        let task = Task::initial("entry");
        assert_eq!(
            ledger
                .record_failure(&task, "bad input", false, &RetryPolicy::default(), "entry")
                .unwrap(),
            RetryOutcome::Parked { attempts: 1 }
        );
        let failed = ledger.failed();
        assert_eq!(failed.len(), 1);
        assert!(failed[0].failures.inputs.is_empty());
        assert_eq!(ledger.clear(&task.key).unwrap().unwrap().attempts, 1);
        assert_eq!(ledger.standing(&task.key), Standing::Ready);
    }

    #[tokio::test]
    async fn an_unwritable_ledger_keeps_its_previous_state() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("retry.json");
        let ledger = RetryLedger::open(&path).unwrap();
        let task = Task::initial("entry");
        std::fs::create_dir(&path).unwrap();
        assert!(
            ledger
                .record_failure(&task, "lost", true, &RetryPolicy::default(), "entry")
                .is_err()
        );
        assert!(ledger.get(&task.key).is_none());
    }

    #[tokio::test]
    async fn a_failure_under_a_superseded_definition_does_not_count() {
        let directory = tempfile::tempdir().unwrap();
        let ledger = RetryLedger::open(directory.path().join("retry.json")).unwrap();
        let task = Task::initial("entry");
        let policy = RetryPolicy {
            max_attempts: 1,
            ..RetryPolicy::default()
        };
        ledger.renew("new").unwrap();
        assert_eq!(
            ledger
                .record_failure(&task, "old attempt", true, &policy, "old")
                .unwrap(),
            RetryOutcome::Fresh
        );
        assert_eq!(ledger.standing(&task.key), Standing::Ready);
        assert_eq!(
            ledger
                .record_failure(&task, "new attempt", true, &policy, "new")
                .unwrap(),
            RetryOutcome::Parked { attempts: 1 }
        );
    }

    #[tokio::test]
    async fn a_changed_definition_forgets_failures_once_that_is_written() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("retry.json");
        let ledger = RetryLedger::open(&path).unwrap();
        let task = Task::initial("entry");
        ledger.renew("first").unwrap();
        ledger
            .record_failure(&task, "bad input", false, &RetryPolicy::default(), "first")
            .unwrap();
        ledger.renew("first").unwrap();
        assert_eq!(ledger.standing(&task.key), Standing::Parked);

        // A restarted server reads which definition the failures belong to.
        let reopened = RetryLedger::open(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(reopened.renew("second").is_err());
        assert_eq!(reopened.standing(&task.key), Standing::Parked);
        std::fs::remove_dir(&path).unwrap();
        reopened.renew("second").unwrap();
        assert_eq!(reopened.standing(&task.key), Standing::Ready);
        assert!(RetryLedger::open(&path).unwrap().failed().is_empty());
    }
}
