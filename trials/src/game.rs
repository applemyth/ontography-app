//! One trial: a generated world on a real server, driven by players, a
//! person, a manager who also edits the running graph, and chaos until its
//! time is up, then drained, restarted in order, and judged.

use crate::history::History;
use crate::procs::Tracker;
use crate::server::{Reply, Server, send};
use crate::world::{self, Kind, Scale, World};
use crate::{audit, judge};
use anyhow::{Context, Result};
use ontography_app::client::Client;
use rand::{Rng, SeedableRng, seq::IndexedRandom};
use rand_chacha::ChaCha8Rng;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};

#[derive(Clone, Debug)]
pub struct Settings {
    pub scale: Scale,
    /// How long the players act.
    pub play: Duration,
    pub players: usize,
    /// Server crashes and killed commands per trial, at random moments.
    pub crashes: usize,
    pub kills: usize,
    /// Edits the manager makes to the running graph.
    pub edits: usize,
}

/// How many previews an edit may need before it counts as starved.
const EDIT_TRIES: usize = 40;

/// How long a drained run may take to settle.
const SETTLE: Duration = Duration::from_secs(120);
const TICK: Duration = Duration::from_millis(250);

/// What became of a move, once the trial knows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fate {
    Done,
    /// Refused, or never ran: it must not be in history.
    Refused,
    /// Its reply was lost: it may be in history once, or not at all.
    Unknown,
}

impl Fate {
    fn of(reply: &Reply) -> Self {
        match reply {
            Reply::Done(_) => Self::Done,
            Reply::Uncertain(_) => Self::Unknown,
            _ => Self::Refused,
        }
    }
}

/// A player's move at an external node, found in history by its result.
#[derive(Clone, Debug)]
pub struct Submission {
    pub node: String,
    /// For a root: the connection and payload of each emission, in order.
    pub emissions: Vec<(String, Vec<u8>)>,
    /// For a consumption: the packages consumed.
    pub inputs: Vec<String>,
    pub fate: Fate,
}

/// Everything the actors did and saw, for the judges.
#[derive(Debug, Default)]
pub struct Log {
    pub submissions: BTreeMap<String, Submission>,
    /// Packages players asked to retire, and what became of it.
    pub retirements: Vec<(String, Fate)>,
    /// A person's decisions: message to node and fate.
    pub decisions: BTreeMap<String, (String, Fate)>,
    /// Parked tasks the manager retried or discarded, per node.
    pub retried: BTreeMap<String, usize>,
    pub discarded: BTreeMap<String, usize>,
    /// Commands chaos killed, by pid.
    pub kills: BTreeSet<u32>,
    pub crashes: usize,
    /// Every committed version of the world, oldest first.
    pub worlds: Vec<Arc<World>>,
    /// The core identities edits gave nodes and connections, to their names.
    pub names: BTreeMap<String, String>,
    /// Packages the committed edits' previews listed to retire, with why.
    pub edit_retirements: BTreeMap<String, String>,
    /// Edits committed, previews that went stale before their commit, and
    /// the longest an edit took from its first preview to its commit.
    pub edits: usize,
    pub stale: usize,
    pub slowest_edit: Duration,
    pub problems: Vec<String>,
}

#[derive(Debug, Default)]
pub struct Outcome {
    pub counts: BTreeMap<&'static str, usize>,
    pub timing: BTreeMap<&'static str, Duration>,
    pub problems: Vec<String>,
}

struct Table {
    world: std::sync::RwLock<Arc<World>>,
    /// Moves built against an older version may be refused for what an edit
    /// changed; it counts each commit's start and end.
    version: AtomicU64,
    run: String,
    /// The data directory, where the editor reads the plans it committed.
    data: PathBuf,
    program: PathBuf,
    witness: PathBuf,
    ontography: PathBuf,
    client: RwLock<Client>,
    /// Players, the manager and chaos stop; the person keeps deciding.
    stop: AtomicBool,
    /// The person stops too.
    drained: AtomicBool,
    log: Mutex<Log>,
    /// Sink packages a player is using.
    claims: Mutex<BTreeSet<String>>,
}

impl Table {
    fn world(&self) -> Arc<World> {
        self.world
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    async fn send(&self, operation: &str, mut args: Value) -> Reply {
        args["run_id"] = json!(self.run);
        let client = self.client.read().await.clone();
        send(&client, operation, args).await
    }

    async fn problem(&self, text: String) {
        self.log.lock().await.problems.push(text);
    }
}

pub async fn trial(seed: u64, dir: &Path, binary: &Path, settings: &Settings) -> Result<Outcome> {
    let started = Instant::now();
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let witness = dir.join("witness");
    std::fs::create_dir_all(&witness)?;
    let program = std::env::current_exe()?;
    let programs = world::Programs {
        program: &program,
        witness: &witness,
        ontography: binary,
    };
    let world = world::generate(
        &mut rng,
        settings.scale,
        &format!("trial-{seed}"),
        &programs,
    );
    std::fs::write(
        dir.join("document.json"),
        serde_json::to_vec_pretty(&world.document)?,
    )?;
    let data = dir.join("data");
    let mut server = Server::start(binary, &data).await?;
    let mut tracker = Tracker::default();
    tracker.root(server.incarnations[0]);
    record_servers(dir, &server)?;
    let project = dir.join("project");
    std::fs::create_dir_all(&project)?;
    let started_run = server
        .call(
            "flow.start",
            json!({"project": project, "document": world.document}),
        )
        .await?;
    let run = started_run["run_id"].as_str().context("run_id")?.to_owned();
    let world = Arc::new(world);
    let table = Arc::new(Table {
        world: std::sync::RwLock::new(world.clone()),
        version: AtomicU64::new(0),
        run: run.clone(),
        data: data.clone(),
        program: program.clone(),
        witness: witness.clone(),
        ontography: binary.into(),
        client: RwLock::new(server.client.clone()),
        stop: AtomicBool::new(false),
        drained: AtomicBool::new(false),
        log: Mutex::new(Log {
            worlds: vec![world],
            ..Log::default()
        }),
        claims: Mutex::new(BTreeSet::new()),
    });
    let mut outcome = Outcome::default();

    let players: Vec<_> = (0..settings.players)
        .map(|index| tokio::spawn(player(Arc::clone(&table), seed, index)))
        .collect();
    let person = tokio::spawn(person(Arc::clone(&table)));
    let manager = tokio::spawn(manager(Arc::clone(&table), seed));
    let editor = tokio::spawn(editor(
        Arc::clone(&table),
        seed,
        settings.edits,
        settings.play,
    ));

    // Chaos runs here, where the server is owned, at moments fixed by the
    // seed: the first crash or kill comes after the players have begun.
    let begun = Instant::now();
    let deadline = begun + settings.play;
    let mut chaos = ChaCha8Rng::seed_from_u64(seed.wrapping_mul(31).wrapping_add(7));
    let mut moments = |count: usize| {
        let mut at: Vec<Duration> = (0..count)
            .map(|_| settings.play.mul_f64(chaos.random_range(0.1..0.95)))
            .collect();
        at.sort();
        at.reverse();
        at
    };
    let (mut crashes, mut kills) = (moments(settings.crashes), moments(settings.kills));
    while Instant::now() < deadline {
        tokio::time::sleep(TICK).await;
        let _ = tracker.observe();
        let due = |moments: &mut Vec<Duration>| {
            let due = moments.last().is_some_and(|at| begun.elapsed() >= *at);
            if due {
                moments.pop();
            }
            due
        };
        if due(&mut crashes) {
            let mut client = table.client.write().await;
            server.restart().await?;
            tracker.root(*server.incarnations.last().expect("incarnation"));
            record_servers(dir, &server)?;
            server.call("run.resume", json!({"run_id": run})).await?;
            *client = server.client.clone();
            table.log.lock().await.crashes += 1;
        }
        if due(&mut kills)
            && let Some(pid) = kill_a_command(&witness)
        {
            table.log.lock().await.kills.insert(pid);
        }
    }
    table.stop.store(true, Ordering::Relaxed);
    for task in players {
        task.await??;
    }
    manager.await??;
    editor.await??;
    outcome.timing.insert("play", started.elapsed());

    // Drain: the person decides what is left, and the run must settle.
    let clock = Instant::now();
    let settled = settle(&table, &mut tracker).await;
    table.drained.store(true, Ordering::Relaxed);
    person.await??;
    outcome.timing.insert("drain", clock.elapsed());
    let status = match settled {
        Ok(status) => status,
        Err(problem) => {
            outcome.problems.push(problem);
            finish(&mut server, &tracker).await;
            return Ok(collect(outcome, table).await);
        }
    };

    // An orderly restart must leave history exactly as it was.
    let clock = Instant::now();
    let before = export(&server, dir, &run).await?;
    let _ = tracker.observe();
    if let Some(problem) = server.stop().await? {
        outcome.problems.push(problem);
    }
    outcome
        .problems
        .extend(after_stop(&tracker, &data, &server, &run).await);
    let (stored, verified) = audit::store(&data, &run, &before).await;
    outcome
        .problems
        .extend(stored.into_iter().map(|p| format!("store: {p}")));
    outcome
        .counts
        .insert("verified replays", usize::from(verified));
    let mut server = Server::start(binary, &data).await?;
    tracker.root(server.incarnations[0]);
    record_servers(dir, &server)?;
    server.call("run.resume", json!({"run_id": run})).await?;
    *table.client.write().await = server.client.clone();
    let after = export(&server, dir, &run).await?;
    outcome.problems.extend(restart_changes(&before, &after));
    outcome.timing.insert("restart", clock.elapsed());

    let clock = Instant::now();
    let log = table.log.lock().await;
    let history = History::parse(&after, &log.names)?;
    let evidence = judge::Evidence {
        worlds: &log.worlds,
        history: &history,
        log: &log,
        status: &status,
        witness: judge::read_witness(&witness)?,
    };
    outcome.problems.extend(judge::judge(&evidence));
    let (nodes, edges) = crate::history::graph(&after, &log.names);
    outcome
        .problems
        .extend(judge::graph(&table.world(), &nodes, &edges));
    if !status["pending_edit"].is_null() {
        outcome.problems.push(format!(
            "a settled run still has an edit pending: {}",
            status["pending_edit"]["plan_id"]
        ));
    }
    outcome
        .counts
        .insert("activations", history.activations.len());
    outcome.counts.insert("packages", history.packages.len());
    outcome
        .counts
        .insert("parked", status["failures"].as_array().map_or(0, Vec::len));
    drop(log);
    outcome.timing.insert("judge", clock.elapsed());

    let _ = tracker.observe();
    if let Some(problem) = server.stop().await? {
        outcome.problems.push(problem);
    }
    outcome
        .problems
        .extend(after_stop(&tracker, &data, &server, &run).await);
    tracker.kill_survivors();
    outcome.counts.insert("processes", tracker.count());
    Ok(collect(outcome, table).await)
}

async fn collect(mut outcome: Outcome, table: Arc<Table>) -> Outcome {
    let log = table.log.lock().await;
    outcome.problems.extend(log.problems.iter().cloned());
    let fates = |fate: Fate| log.submissions.values().filter(|s| s.fate == fate).count();
    outcome.counts.insert("moves", log.submissions.len());
    outcome.counts.insert("moves lost", fates(Fate::Unknown));
    outcome.counts.insert("decisions", log.decisions.len());
    outcome.counts.insert("retries", log.retried.values().sum());
    outcome
        .counts
        .insert("discards", log.discarded.values().sum());
    outcome.counts.insert("crashes", log.crashes);
    outcome.counts.insert("kills", log.kills.len());
    outcome.counts.insert("edits", log.edits);
    outcome.counts.insert("stale previews", log.stale);
    if log.edits > 0 {
        outcome.timing.insert("slowest edit", log.slowest_edit);
    }
    outcome
}

/// Stops the server however it can, and the processes it left.
async fn finish(server: &mut Server, tracker: &Tracker) {
    let _ = server.stop().await;
    tracker.kill_survivors();
}

/// Servers of this trial, so a later harness can stop any left behind.
pub fn record_servers(dir: &Path, server: &Server) -> Result<()> {
    let lines: String = server
        .incarnations
        .iter()
        .map(|pid| format!("{pid}\n"))
        .collect();
    std::fs::write(dir.join("servers"), lines)?;
    Ok(())
}

async fn export(server: &Server, dir: &Path, run: &str) -> Result<Value> {
    let path = dir.join("export.json");
    let _ = std::fs::remove_file(&path);
    server
        .call("inspect.export", json!({"run_id": run, "path": path}))
        .await?;
    Ok(serde_json::from_slice(&std::fs::read(&path)?)?)
}

/// What a stopped server must not leave behind: processes, its socket
/// directory, the run's node socket directories, or half-written files.
pub async fn after_stop(tracker: &Tracker, data: &Path, server: &Server, run: &str) -> Vec<String> {
    let mut problems = Vec::new();
    // Programs get a moment to notice their server is gone.
    tokio::time::sleep(Duration::from_millis(300)).await;
    match tracker.survivors() {
        Ok(survivors) => problems.extend(survivors.iter().map(|p| {
            format!(
                "process {} outlived the server's orderly stop: {}",
                p.pid, p.command
            )
        })),
        Err(error) => problems.push(format!("could not list processes: {error}")),
    }
    if let Some(directory) = server.socket().parent()
        && directory.exists()
    {
        problems.push(format!(
            "the server left its socket directory {}",
            directory.display()
        ));
    }
    let nodes = data.join("runs").join(run).join("nodes");
    for entry in std::fs::read_dir(&nodes).into_iter().flatten().flatten() {
        if let Some(directory) = node_endpoint(&entry.path())
            && directory.exists()
        {
            problems.push(format!(
                "node {} left its socket directory {}",
                entry.file_name().to_string_lossy(),
                directory.display()
            ));
        }
    }
    for leftover in temporaries(&data.join("runs")) {
        problems.push(format!("half-written file left: {}", leftover.display()));
    }
    problems
}

/// Where the app puts a node's sockets: a short path under /tmp named by a
/// digest of the node's directory.
fn node_endpoint(directory: &Path) -> Option<PathBuf> {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(directory.as_os_str().as_encoded_bytes());
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    Some(PathBuf::from("/tmp").join(format!(
        "ontography-node-{}-{}",
        nix::unistd::geteuid(),
        &hex[..20]
    )))
}

pub fn temporaries(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(directory) = stack.pop() {
        for entry in std::fs::read_dir(&directory)
            .into_iter()
            .flatten()
            .flatten()
        {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            if entry.file_type().is_ok_and(|t| t.is_dir()) {
                // Core's own store keeps its own files.
                if name != "core" {
                    stack.push(path);
                }
            } else if name.starts_with('.') && name.ends_with(".tmp") {
                found.push(path);
            }
        }
    }
    found
}

/// Differences an orderly restart made to history.
pub fn restart_changes(before: &Value, after: &Value) -> Vec<String> {
    let mut problems = Vec::new();
    for field in [
        "revision",
        "activations",
        "packages",
        "retirements",
        "current_fingerprint",
    ] {
        if before[field] != after[field] {
            let size = |v: &Value| v.as_array().map_or(0, Vec::len);
            problems.push(format!(
                "an orderly restart changed {field} (from {} to {} records)",
                size(&before[field]),
                size(&after[field])
            ));
        }
    }
    problems
}

/// Waits until nothing is left to run: no ready task at a command node or a
/// person, nothing retrying, and history unchanged for a while.
async fn settle(table: &Table, tracker: &mut Tracker) -> std::result::Result<Value, String> {
    let deadline = Instant::now() + SETTLE;
    let mut last = String::new();
    let mut quiet = 0;
    let mut status = Value::Null;
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = tracker.observe();
        let Reply::Done(now) = table.send("flow.status", json!({})).await else {
            continue;
        };
        status = now;
        let busy = status["tasks"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|task| {
                let node = task["node"].as_str().unwrap_or_default();
                table
                    .world()
                    .nodes
                    .get(node)
                    .is_some_and(|kind| kind.runs() || matches!(kind, Kind::Human))
            })
            || status["failures"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|failure| failure["state"] != "parked");
        let revision = status["revision"].as_str().unwrap_or_default().to_owned();
        if !busy && revision == last {
            quiet += 1;
            if quiet >= 3 {
                return Ok(status);
            }
        } else {
            quiet = 0;
        }
        last = revision;
    }
    Err(format!(
        "the run did not settle within {}s; last status: tasks {}, failures {}",
        SETTLE.as_secs(),
        status["tasks"],
        status["failures"]
    ))
}

/// Kills one running command of this trial, returning its pid.
fn kill_a_command(witness: &Path) -> Option<u32> {
    let marker = witness.to_string_lossy().into_owned();
    let running: Vec<_> = crate::procs::snapshot()
        .ok()?
        .into_iter()
        .filter(|p| p.command.contains(" node ") && p.command.contains(&marker))
        .collect();
    let target = running.choose(&mut rand::rng())?;
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(target.pid),
        nix::sys::signal::Signal::SIGKILL,
    )
    .ok()?;
    Some(target.pid as u32)
}

/// A player acts for the external nodes: it starts work at sources, and
/// consumes or retires what reaches the sinks.
async fn player(table: Arc<Table>, seed: u64, index: usize) -> Result<()> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed.wrapping_mul(1000).wrapping_add(index as u64 + 1));
    let mut step = 0;
    while !table.stop.load(Ordering::Relaxed) {
        let world = table.world();
        let sources = world.named(|kind| matches!(kind, Kind::Source));
        let sinks = world.named(|kind| matches!(kind, Kind::Sink));
        step += 1;
        let tag = format!("p{index}.m{step}");
        match rng.random_range(0..100) {
            0..70 => root(&table, &mut rng, &sources, tag).await,
            roll => {
                if let Some(sink) = sinks.choose(&mut rng) {
                    at_sink(&table, sink, tag, roll >= 90).await;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(rng.random_range(2..25))).await;
    }
    Ok(())
}

fn payload(rng: &mut impl Rng, tag: &str, index: usize) -> Vec<u8> {
    let mut bytes = format!("{tag}.{index}").into_bytes();
    // Now and then more than the 32 KiB the app delivers inline.
    if rng.random_bool(0.03) {
        while bytes.len() < 40 * 1024 {
            bytes.extend_from_slice(b" filler");
        }
    }
    bytes
}

async fn root(table: &Table, rng: &mut ChaCha8Rng, sources: &[String], tag: String) {
    let Some(node) = sources.choose(rng) else {
        return;
    };
    let version = table.version();
    let chosen: Vec<String> = table
        .world()
        .outgoing(node)
        .filter(|_| rng.random_bool(0.7))
        .map(|(edge, _)| edge.clone())
        .collect();
    let emissions: Vec<(String, Vec<u8>)> = chosen
        .into_iter()
        .enumerate()
        .map(|(i, edge)| (edge, payload(rng, &tag, i)))
        .collect();
    let args = json!({
        "trigger": {"kind": "root", "node_id": node, "authority": ["workflow"]},
        "result": tag,
        "emissions": emissions.iter().map(|(edge, payload)| json!({
            "edge_id": edge,
            "payload": String::from_utf8_lossy(payload),
            "authority": {"kind": "carry"},
        })).collect::<Vec<_>>(),
    });
    let reply = table.send("workflow.submit", args).await;
    // An edit may have removed a connection it named meanwhile.
    if !matches!(
        reply,
        Reply::Done(_) | Reply::Uncertain(_) | Reply::NotRun(_)
    ) && table.version() == version
    {
        table
            .problem(format!(
                "the server refused a legal root at {node}: {reply}"
            ))
            .await;
    }
    table.log.lock().await.submissions.insert(
        tag,
        Submission {
            node: node.clone(),
            emissions,
            inputs: vec![],
            fate: Fate::of(&reply),
        },
    );
}

/// Consumes, or retires, one package that reached `sink`.
async fn at_sink(table: &Table, sink: &str, tag: String, retire: bool) {
    let Reply::Done(page) = table
        .send(
            "inspect.frontier",
            json!({"phase": "received", "node_id": sink, "limit": 50}),
        )
        .await
    else {
        return;
    };
    let package = {
        let mut claims = table.claims.lock().await;
        let free = page["packages"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|p| p["package_id"].as_str())
            .find(|id| !claims.contains(*id))
            .map(String::from);
        let Some(package) = free else { return };
        // A claim is never released: a package once used is spent, or its
        // fate is in doubt.
        claims.insert(package.clone());
        package
    };
    if retire {
        let reply = table
            .send("workflow.retire", json!({"package_id": package}))
            .await;
        table
            .log
            .lock()
            .await
            .retirements
            .push((package, Fate::of(&reply)));
        return;
    }
    let args = json!({
        "trigger": {"kind": "packages", "package_ids": [package]},
        "result": tag,
        "emissions": [],
    });
    let reply = table.send("workflow.submit", args).await;
    table.log.lock().await.submissions.insert(
        tag,
        Submission {
            node: sink.into(),
            emissions: vec![],
            inputs: vec![package],
            fate: Fate::of(&reply),
        },
    );
}

/// A person decides every task that reaches a human node.
async fn person(table: Arc<Table>) -> Result<()> {
    let mut count = 0;
    while !table.drained.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let Reply::Done(status) = table.send("flow.status", json!({})).await else {
            continue;
        };
        for task in status["tasks"].as_array().into_iter().flatten() {
            let node = task["node"].as_str().unwrap_or_default().to_owned();
            if !matches!(table.world().nodes.get(&node), Some(Kind::Human)) {
                continue;
            }
            count += 1;
            let message = format!("{node}.d{count}");
            let reply = table
                .send(
                    "flow.decide",
                    json!({"node": node, "task_id": task["task_id"], "message": message}),
                )
                .await;
            match &reply {
                Reply::Done(_) | Reply::Uncertain(_) | Reply::NotRun(_) => {}
                // The task was completed or replaced meanwhile.
                Reply::Failed(code, _) if code == "stale_task" => {}
                other => {
                    table
                        .problem(format!("deciding a ready task at {node} failed: {other}"))
                        .await
                }
            }
            table
                .log
                .lock()
                .await
                .decisions
                .insert(message, (node, Fate::of(&reply)));
        }
    }
    Ok(())
}

/// The manager retries or discards parked tasks now and then, and pages
/// through inboxes.
async fn manager(table: Arc<Table>, seed: u64) -> Result<()> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed.wrapping_add(99));
    let mut handled = BTreeSet::new();
    while !table.stop.load(Ordering::Relaxed) {
        let inboxes = table.world().named(|kind| matches!(kind, Kind::Inbox));
        tokio::time::sleep(Duration::from_millis(300)).await;
        let Reply::Done(status) = table.send("flow.status", json!({})).await else {
            continue;
        };
        for failure in status["failures"].as_array().into_iter().flatten() {
            let task = failure["task_id"].as_str().unwrap_or_default().to_owned();
            let node = failure["node"].as_str().unwrap_or_default().to_owned();
            if failure["state"] != "parked" || handled.contains(&task) {
                continue;
            }
            let action = match rng.random_range(0..100) {
                0..12 => "flow.retry",
                12..20 => "flow.discard",
                _ => continue,
            };
            handled.insert(task.clone());
            let reply = table
                .send(action, json!({"node": node, "task_id": task}))
                .await;
            let mut log = table.log.lock().await;
            match (&reply, action) {
                (Reply::Done(_) | Reply::Uncertain(_), "flow.retry") => {
                    *log.retried.entry(node).or_default() += 1
                }
                (Reply::Done(_) | Reply::Uncertain(_), _) => {
                    *log.discarded.entry(node).or_default() += 1
                }
                (Reply::NotRun(_), _) => {}
                // It finished, or an edit replaced or retired it, meanwhile.
                (Reply::Failed(code, _), _) if code == "stale_task" => {}
                (other, _) => log.problems.push(format!(
                    "{action} of a parked task at {node} failed: {other}"
                )),
            }
        }
        if let Some(inbox) = inboxes.choose(&mut rng) {
            let version = table.version();
            let reply = table
                .send("flow.output", json!({"node": inbox, "limit": 20}))
                .await;
            if let Reply::Failed(code, message) = reply
                && table.version() == version
            {
                table
                    .problem(format!("reading inbox {inbox} failed: {code}: {message}"))
                    .await;
            }
        }
    }
    Ok(())
}

/// The manager's edits: now and then a change to the running document,
/// previewed and committed while everything else keeps working.
async fn editor(table: Arc<Table>, seed: u64, edits: usize, play: Duration) -> Result<()> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed.wrapping_add(4242));
    let pause = play / (edits as u32 + 1);
    for _ in 0..edits {
        tokio::time::sleep(pause).await;
        if table.stop.load(Ordering::Relaxed) {
            break;
        }
        let current = table.world();
        let programs = world::Programs {
            program: &table.program,
            witness: &table.witness,
            ontography: &table.ontography,
        };
        let Some((next, what)) = (0..10).find_map(|_| world::mutate(&mut rng, &current, &programs))
        else {
            continue;
        };
        edit(&table, next, what).await?;
    }
    Ok(())
}

/// Previews `next` and commits it, previewing again while work keeps
/// making the preview stale, as the documentation says to.
async fn edit(table: &Table, next: World, what: String) -> Result<()> {
    let begun = Instant::now();
    let mut stale = 0;
    for _ in 0..EDIT_TRIES {
        let plan = match table
            .send("flow.edit", json!({"document": next.document}))
            .await
        {
            Reply::Done(plan) => plan,
            Reply::NotRun(_) | Reply::Uncertain(_) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            other => {
                table
                    .problem(format!("previewing an edit ({what}) failed: {other}"))
                    .await;
                return Ok(());
            }
        };
        let plan_id = plan["plan_id"].as_str().unwrap_or_default().to_owned();
        // Moves built before or during the commit may meet the new graph.
        table.version.fetch_add(1, Ordering::AcqRel);
        let mut reply = table.send("flow.commit", json!({"plan_id": plan_id})).await;
        // Repeating a saved commit recovers that edit, as after a lost reply
        // or a crash, or once `flow.resume` finished a busy one.
        for _ in 0..10 {
            match &reply {
                Reply::Uncertain(_) | Reply::NotRun(_) => {}
                Reply::Failed(code, _) if code == "workflow_busy" => {
                    stale += 1;
                    let _ = table.send("flow.resume", json!({})).await;
                }
                _ => break,
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            reply = table.send("flow.commit", json!({"plan_id": plan_id})).await;
        }
        match reply {
            Reply::Done(_) => {
                return committed(table, next, &plan_id, stale, begun.elapsed()).await;
            }
            Reply::Failed(code, _)
                if code == "stale_preview" || code == "retirement_preview_required" =>
            {
                table.version.fetch_add(1, Ordering::AcqRel);
                stale += 1;
            }
            other => {
                table.version.fetch_add(1, Ordering::AcqRel);
                table
                    .problem(format!("committing an edit ({what}) failed: {other}"))
                    .await;
                return Ok(());
            }
        }
    }
    let mut log = table.log.lock().await;
    log.stale += stale;
    log.problems.push(format!(
        "an edit ({what}) never committed: {stale} previews went stale while work kept moving"
    ));
    Ok(())
}

/// Records a committed edit: the identities its plan gave nodes and
/// connections, the pending work its preview said it would retire, and the
/// new version of the world.
async fn committed(
    table: &Table,
    next: World,
    plan_id: &str,
    stale: usize,
    took: Duration,
) -> Result<()> {
    let path = table
        .data
        .join("runs")
        .join(&table.run)
        .join("edit-plans")
        .join(format!("{plan_id}.json"));
    let plan: Value = serde_json::from_slice(&std::fs::read(&path)?)?;
    let next = Arc::new(next);
    let mut log = table.log.lock().await;
    for kind in ["nodes", "edges"] {
        for (name, id) in plan["identities"][kind].as_object().into_iter().flatten() {
            if let Some(id) = id.as_str() {
                log.names.insert(id.to_owned(), name.clone());
            }
        }
    }
    for (package, reason) in plan["retirements"].as_object().into_iter().flatten() {
        if let Some(reason) = reason.as_str() {
            log.edit_retirements
                .insert(package.clone(), reason.to_owned());
        }
    }
    log.edits += 1;
    log.stale += stale;
    log.slowest_edit = log.slowest_edit.max(took);
    log.worlds.push(next.clone());
    *table
        .world
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = next;
    table.version.fetch_add(1, Ordering::AcqRel);
    Ok(())
}
