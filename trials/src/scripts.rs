//! The crash-point scripts: short, fixed sequences against tiny worlds, each
//! built around one kind of durable write the app makes. They play through
//! the driver. After a crash a script finds out from history whether its
//! interrupted request happened, as the documentation asks of a client after
//! a restart, and sends it again only if it did not; after a failure the
//! injected fault explains, it sends it again too, as the server must keep
//! serving. What each move became goes into the log the workflow judge
//! reads, as the trials' players record theirs.

use crate::game::{Fate, Log, Submission};
use crate::history::History;
use crate::recovery::Driver;
use crate::roles::Role;
use crate::server::Reply;
use crate::world::{self, Edge, Kind, Worker, World};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum)]
pub enum Script {
    /// `flow.start` with a `start_id`, roots at an external source, then
    /// consumptions and a retirement at an external sink.
    External,
    /// A command that runs and publishes, beside a flaky one that fails
    /// first: the task harness's ledger, caches and markers.
    Command,
    /// A person's decision.
    Human,
    /// `inspect.export` into the data directory, afresh and over itself.
    Export,
    /// `flow.retry` and `flow.discard` of tasks parked at a broken command.
    Parked,
}

impl Script {
    pub const ALL: [Self; 5] = [
        Self::External,
        Self::Command,
        Self::Human,
        Self::Export,
        Self::Parked,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::External => "external",
            Self::Command => "command",
            Self::Human => "human",
            Self::Export => "export",
            Self::Parked => "parked",
        }
    }

    /// The script's world, whose commands run `program` and record each run
    /// under `witness`.
    pub fn world(self, program: &Path, witness: &Path) -> World {
        let name = format!("crashpoints-{}", self.name());
        let make = |nodes: Vec<(&str, Kind)>, edges: &[(&str, &str, &str)]| {
            let nodes: BTreeMap<String, Kind> = nodes
                .into_iter()
                .map(|(id, kind)| (id.to_owned(), kind))
                .collect();
            let edges: BTreeMap<String, Edge> = edges
                .iter()
                .map(|(id, from, to)| {
                    let edge = Edge {
                        from: (*from).into(),
                        to: (*to).into(),
                    };
                    ((*id).to_owned(), edge)
                })
                .collect();
            // Scripts place no agents, so no `ontography` binary is needed.
            let programs = world::Programs {
                program,
                witness,
                ontography: program,
            };
            let document = world::document(&name, &nodes, &edges, &programs);
            World {
                name: name.clone(),
                document,
                nodes,
                edges,
            }
        };
        let command = |role, attempts| {
            Kind::Worker(Worker {
                role,
                attempts,
                bytes: false,
                all: false,
                layer: 0,
            })
        };
        match self {
            Self::External | Self::Export => make(
                vec![("src0", Kind::Source), ("x0", Kind::Sink)],
                &[("e0", "src0", "x0"), ("e1", "src0", "x0")],
            ),
            Self::Command => make(
                vec![
                    ("src0", Kind::Source),
                    ("w0", command(Role::Digest, 2)),
                    ("w1", command(Role::Flaky, 3)),
                    ("x0", Kind::Sink),
                ],
                &[
                    ("e0", "src0", "w0"),
                    ("e1", "src0", "w1"),
                    ("e2", "w0", "x0"),
                    ("e3", "w1", "x0"),
                ],
            ),
            Self::Human => make(
                vec![
                    ("src0", Kind::Source),
                    ("h0", Kind::Human),
                    ("in0", Kind::Inbox),
                ],
                &[("e0", "src0", "h0"), ("e1", "h0", "in0")],
            ),
            Self::Parked => make(
                vec![
                    ("src0", Kind::Source),
                    ("b0", command(Role::Broken, 1)),
                    ("x0", Kind::Sink),
                ],
                &[("e0", "src0", "b0"), ("e1", "b0", "x0")],
            ),
        }
    }

    /// Plays the script. Where its focus begins, the driver marks; the
    /// points before it are setup other scripts cover.
    pub async fn play(self, d: &mut Driver, world: &World, log: &mut Log) {
        match self {
            // Everything counts, the server's own start included.
            Self::External => {
                if !start(d, world).await {
                    return;
                }
                root(d, log, "r1", &[("e0", "r1.0"), ("e1", "r1.1")]).await;
                root(d, log, "r2", &[("e0", "r2.0")]).await;
                root(d, log, "r3", &[("e1", "r3.1")]).await;
                consume(d, log, "c1").await;
                consume(d, log, "c2").await;
                retire(d, log).await;
            }
            Self::Command => {
                d.focus();
                if !start(d, world).await {
                    return;
                }
                root(d, log, "r1", &[("e0", "r1.0"), ("e1", "r1.1")]).await;
                d.settle(world).await;
                root(d, log, "r2", &[("e0", "r2.0")]).await;
                d.settle(world).await;
            }
            Self::Human => {
                if !start(d, world).await {
                    return;
                }
                d.focus();
                root(d, log, "r1", &[("e0", "r1.0")]).await;
                decide(d, log, "h0", "h0.d1").await;
            }
            Self::Export => {
                if !start(d, world).await {
                    return;
                }
                root(d, log, "r1", &[("e0", "r1.0"), ("e1", "r1.1")]).await;
                d.focus();
                let path = d.data.join("exports").join("history.json");
                exported(d, &path, "export afresh").await;
                exported(d, &path, "export over it").await;
            }
            Self::Parked => parked(d, world, log).await,
        }
    }
}

/// Starts the run with the driver's `start_id`; after a crash, the same
/// `start_id` recovers the same start.
async fn start(d: &mut Driver, world: &World) -> bool {
    d.step("flow.start");
    let project = d.dir.join("project");
    let _ = std::fs::create_dir_all(&project);
    let args = json!({"project": project, "document": world.document, "start_id": d.run});
    for _ in 0..3 {
        let reply = d.send("flow.start", args.clone()).await;
        match &reply {
            Reply::Done(value) => {
                if value["run_id"].as_str() != Some(d.run.as_str()) {
                    let text = format!(
                        "flow.start with start_id {} started run {}",
                        d.run, value["run_id"]
                    );
                    d.problem(text);
                }
                d.started();
                return true;
            }
            Reply::Uncertain(_) | Reply::NotRun(_) => {}
            Reply::Failed(..) | Reply::Rejected(_) if d.excuse("flow.start", &reply) => {}
            other => {
                d.problem(format!("flow.start failed: {other}"));
                return false;
            }
        }
    }
    d.problem("flow.start did not succeed in three tries".into());
    false
}

/// How many activations carry `tag` as their result.
fn tagged(history: &History, tag: &str) -> usize {
    history
        .activations
        .values()
        .filter(|activation| activation.result == tag.as_bytes())
        .count()
}

/// Sends a move until it is known to have happened. After a lost reply, one
/// the server could not run, or a failure the injected fault explains,
/// history says whether it happened, and it is sent again only if not.
async fn deliver(
    d: &mut Driver,
    operation: &str,
    args: Value,
    happened: impl Fn(&History) -> bool,
) -> Fate {
    for _ in 0..3 {
        let reply = d.send(operation, args.clone()).await;
        match &reply {
            Reply::Done(_) => return Fate::Done,
            Reply::Failed(..) | Reply::Rejected(_) if !d.excuse(operation, &reply) => {
                d.problem(format!("{operation} of a legal move failed: {reply}"));
                return Fate::Refused;
            }
            _ => {}
        }
        let Some(history) = d.history().await else {
            return Fate::Unknown;
        };
        if happened(&history) {
            if !matches!(reply, Reply::Uncertain(_)) {
                d.problem(format!("{operation} was {reply}, yet it happened"));
            }
            return Fate::Unknown;
        }
    }
    d.problem(format!("{operation} did not happen in three tries"));
    Fate::Refused
}

/// Starts work at the source with one emission per `(connection, payload)`.
async fn root(d: &mut Driver, log: &mut Log, tag: &str, emissions: &[(&str, &str)]) {
    d.step(&format!("root {tag}"));
    let args = json!({
        "run_id": d.run,
        "trigger": {"kind": "root", "node_id": "src0", "authority": ["workflow"]},
        "result": tag,
        "emissions": emissions.iter().map(|(edge, payload)| json!({
            "edge_id": edge,
            "payload": payload,
            "authority": {"kind": "carry"},
        })).collect::<Vec<_>>(),
    });
    let fate = deliver(d, "workflow.submit", args, |h| tagged(h, tag) > 0).await;
    let emissions = emissions
        .iter()
        .map(|(edge, payload)| ((*edge).to_owned(), payload.as_bytes().to_vec()))
        .collect();
    log.submissions.insert(
        tag.into(),
        Submission {
            node: "src0".into(),
            emissions,
            inputs: vec![],
            fate,
        },
    );
}

/// A package waiting at the sink that no move has used yet.
async fn received(d: &mut Driver, log: &Log) -> Option<String> {
    let args = json!({"run_id": d.run, "phase": "received", "node_id": "x0", "limit": 50});
    let page = d.read("inspect.frontier", args).await?;
    let used: BTreeSet<&String> = log
        .submissions
        .values()
        .flat_map(|s| &s.inputs)
        .chain(log.retirements.iter().map(|(package, _)| package))
        .collect();
    let mut free: Vec<String> = page["packages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|p| p["package_id"].as_str())
        .filter(|id| !used.contains(&id.to_string()))
        .map(String::from)
        .collect();
    free.sort();
    let first = free.into_iter().next();
    if first.is_none() {
        d.problem("no unused package waits at x0".into());
    }
    first
}

/// Consumes one package at the sink.
async fn consume(d: &mut Driver, log: &mut Log, tag: &str) {
    d.step(&format!("consume {tag}"));
    let Some(package) = received(d, log).await else {
        return;
    };
    let args = json!({
        "run_id": d.run,
        "trigger": {"kind": "packages", "package_ids": [package]},
        "result": tag,
        "emissions": [],
    });
    let fate = deliver(d, "workflow.submit", args, |h| tagged(h, tag) > 0).await;
    log.submissions.insert(
        tag.into(),
        Submission {
            node: "x0".into(),
            emissions: vec![],
            inputs: vec![package],
            fate,
        },
    );
}

/// Retires one package at the sink.
async fn retire(d: &mut Driver, log: &mut Log) {
    d.step("retire");
    let Some(package) = received(d, log).await else {
        return;
    };
    let args = json!({"run_id": d.run, "package_id": package});
    let fate = deliver(d, "workflow.retire", args, |h| {
        h.packages
            .get(&package)
            .is_some_and(|p| p.disposition == "retired")
    })
    .await;
    log.retirements.push((package, fate));
}

/// The first task waiting at `node`.
fn task_at(status: &Value, node: &str) -> Option<String> {
    status["tasks"]
        .as_array()?
        .iter()
        .find(|task| task["node"] == node)
        .and_then(|task| task["task_id"].as_str())
        .map(String::from)
}

/// A person decides the task that reaches `node`; after a lost reply,
/// history says whether the decision was taken, and it is made again, on
/// the task then waiting, only if not.
async fn decide(d: &mut Driver, log: &mut Log, node: &str, message: &str) {
    d.step(&format!("decide {message}"));
    let mut fate = Fate::Refused;
    for _ in 0..3 {
        let waiting = format!("a task at {node}");
        let Some(status) = d.wait_until(&waiting, |s| task_at(s, node).is_some()).await else {
            break;
        };
        let task = task_at(&status, node).unwrap_or_default();
        let args = json!({"run_id": d.run, "node": node, "task_id": task, "message": message});
        let reply = d.send("flow.decide", args).await;
        match &reply {
            Reply::Done(_) => {
                fate = Fate::Done;
                break;
            }
            Reply::Failed(..) | Reply::Rejected(_) if !d.excuse("flow.decide", &reply) => {
                d.problem(format!("deciding a ready task at {node} failed: {reply}"));
                break;
            }
            _ => {}
        }
        let Some(history) = d.history().await else {
            fate = Fate::Unknown;
            break;
        };
        if tagged(&history, message) > 0 {
            if !matches!(reply, Reply::Uncertain(_)) {
                d.problem(format!(
                    "flow.decide was {reply}, yet the decision was taken"
                ));
            }
            fate = Fate::Unknown;
            break;
        }
    }
    log.decisions.insert(message.into(), (node.into(), fate));
}

/// Exports history to a path in the data directory. Whatever becomes of the
/// request, what is at the path is a whole export or, before the first one
/// is done, nothing.
async fn exported(d: &mut Driver, path: &Path, step: &str) {
    d.step(step);
    let existed = path.exists();
    for _ in 0..3 {
        let args = json!({"run_id": d.run, "path": path});
        let reply = d.send("inspect.export", args).await;
        let whole = match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice::<Value>(&bytes)
                .is_ok_and(|v| v["activations"].is_array() && v["packages"].is_array()),
            Err(_) => !existed && !matches!(reply, Reply::Done(_)),
        };
        if !whole {
            d.problem(format!(
                "after inspect.export ({reply}), {} holds no whole export",
                path.display()
            ));
        }
        match &reply {
            Reply::Done(_) => return,
            Reply::Uncertain(_) | Reply::NotRun(_) => {}
            Reply::Failed(..) | Reply::Rejected(_) if d.excuse("inspect.export", &reply) => {}
            other => {
                d.problem(format!("inspect.export failed: {other}"));
                return;
            }
        }
    }
    d.problem("inspect.export did not succeed in three tries".into());
}

/// Failed tasks at `node` in a status, and whether each is parked.
fn failed(status: &Value, node: &str) -> BTreeMap<String, bool> {
    status["failures"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|failure| failure["node"] == node)
        .filter_map(|failure| {
            let task = failure["task_id"].as_str()?.to_owned();
            Some((task, failure["state"] == "parked"))
        })
        .collect()
}

/// How often the program at `node` has run, by its witness file.
fn runs(witness: &Path, node: &str) -> usize {
    std::fs::read_to_string(witness.join(format!("{node}.jsonl")))
        .map_or(0, |text| text.lines().count())
}

/// Two tasks park at a broken command; the manager retries one and
/// discards the other.
async fn parked(d: &mut Driver, world: &World, log: &mut Log) {
    if !start(d, world).await {
        return;
    }
    root(d, log, "r1", &[("e0", "r1.0")]).await;
    root(d, log, "r2", &[("e0", "r2.0")]).await;
    let both = |s: &Value| failed(s, "b0").values().filter(|parked| **parked).count() == 2;
    let Some(status) = d.wait_until("both tasks parking at b0", both).await else {
        return;
    };
    d.focus();
    let tasks: Vec<String> = failed(&status, "b0").into_keys().collect();
    let (again, gone) = (tasks[0].clone(), tasks[1].clone());
    let witness = d.dir.join("witness");
    let before = runs(&witness, "b0");
    let error = |s: &Value| {
        s["failures"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|failure| failure["task_id"] == again.as_str())
            .map(|failure| failure["error"].to_string())
    };
    let first = error(&status);

    d.step("flow.retry");
    let mut retried = false;
    for _ in 0..3 {
        let args = json!({"run_id": d.run, "node": "b0", "task_id": again});
        let reply = d.send("flow.retry", args).await;
        match &reply {
            Reply::Done(_) => {
                retried = true;
                break;
            }
            Reply::Failed(..) | Reply::Rejected(_) if !d.excuse("flow.retry", &reply) => {
                d.problem(format!("flow.retry of a parked task failed: {reply}"));
                break;
            }
            _ => {}
        }
        // Did it happen? A retried task leaves the failures until it fails
        // again, with a new run of its program or a new error.
        let Some(status) = d.status().await else {
            break;
        };
        let parked = failed(&status, "b0").get(&again) == Some(&true);
        if !parked || runs(&witness, "b0") > before || error(&status) != first {
            if !matches!(reply, Reply::Uncertain(_)) {
                d.problem(format!("flow.retry was {reply}, yet it happened"));
            }
            retried = true;
            break;
        }
    }
    if retried {
        *log.retried.entry("b0".into()).or_default() += 1;
        // A new attempt shows as a new run of the program, or, when the
        // attempt could not start it, as a new error.
        let ran = |s: &Value| {
            failed(s, "b0").get(&again) == Some(&true)
                && (runs(&witness, "b0") > before || error(s) != first)
        };
        d.wait_until("the retried task running and parking again", ran)
            .await;
    }

    d.step("flow.discard");
    let mut discarded = None;
    for _ in 0..3 {
        let args = json!({"run_id": d.run, "node": "b0", "task_id": gone});
        let reply = d.send("flow.discard", args).await;
        match &reply {
            Reply::Done(_) => {
                discarded = Some(Fate::Done);
                break;
            }
            Reply::Failed(..) | Reply::Rejected(_) if !d.excuse("flow.discard", &reply) => {
                d.problem(format!("flow.discard of a parked task failed: {reply}"));
                break;
            }
            _ => {}
        }
        // A discarded task is gone from the failures.
        let Some(status) = d.status().await else {
            break;
        };
        if !failed(&status, "b0").contains_key(&gone) {
            if !matches!(reply, Reply::Uncertain(_)) {
                d.problem(format!("flow.discard was {reply}, yet the task is gone"));
            }
            discarded = Some(Fate::Unknown);
            break;
        }
    }
    if discarded.is_some() {
        *log.discarded.entry("b0".into()).or_default() += 1;
    }

    // What the manager did shows in the settled run.
    let Some(status) = d.settle(world).await else {
        return;
    };
    let Some(history) = d.history().await else {
        return;
    };
    let retired = history
        .packages
        .values()
        .filter(|p| p.holder == "b0" && p.disposition == "retired")
        .count();
    let failures = failed(&status, "b0");
    if discarded.is_some() {
        if retired != 1 {
            d.problem(format!(
                "discarding a parked task retired {retired} packages at b0, not its one input"
            ));
        }
        if failures.contains_key(&gone) {
            d.problem("the discarded task is still among the failures".into());
        }
    }
    if retried && failures.get(&again) != Some(&true) {
        d.problem("the retried task is not parked again".into());
    }
}
