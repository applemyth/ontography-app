//! The workflow judge: the server's history, final status and the programs'
//! witness files, against what the documentation says each node kind does
//! and what the actors did. Written from docs/WORKFLOWS.md, not the app.
//!
//! - A command runs once per task with its inputs, in package order, joined
//!   by blank lines on stdin, and its result goes to every outgoing
//!   connection. A failed task retries up to its node's `max_attempts`, then
//!   parks; a person's decision goes to every outgoing connection too.
//! - External nodes do only what their players did; inboxes hold work.
//! - After the run settles, work waits only where it may: in inboxes, at
//!   sinks, in an incomplete join, or in a parked task.

use crate::game::{Fate, Log};
use crate::history::{Activation, History, Trigger};
use crate::roles::{self, Role};
use crate::world::{Kind, World};
use anyhow::Result;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

pub struct Evidence<'a> {
    pub world: &'a World,
    pub history: &'a History,
    pub log: &'a Log,
    pub status: &'a Value,
    pub witness: BTreeMap<String, Vec<Run>>,
}

/// One run of a command's program, as it recorded itself.
#[derive(Debug)]
pub struct Run {
    pub pid: u32,
    pub sha: String,
}

pub fn read_witness(dir: &Path) -> Result<BTreeMap<String, Vec<Run>>> {
    let mut runs = BTreeMap::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(node) = path.file_stem().map(|s| s.to_string_lossy().into_owned()) else {
            continue;
        };
        let lines: Vec<Run> = std::fs::read_to_string(&path)?
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .map(|value| Run {
                pid: value["pid"].as_u64().unwrap_or_default() as u32,
                sha: value["sha"].as_str().unwrap_or_default().to_owned(),
            })
            .collect();
        runs.insert(node, lines);
    }
    Ok(runs)
}

pub fn judge(evidence: &Evidence) -> Vec<String> {
    let mut problems = Vec::new();
    let payloads = payloads(evidence, &mut problems);
    let inputs = activations(evidence, &payloads, &mut problems);
    moves(evidence, &mut problems);
    packages(evidence, &mut problems);
    failures(evidence, &mut problems);
    runs(evidence, &inputs, &mut problems);
    problems
}

/// The index of an output within its activation, from its package ID.
fn output_index(package: &str) -> Option<usize> {
    let (_, output) = package.split_once('/')?;
    let index = uuid::Uuid::parse_str(output).ok()?.as_u128();
    usize::try_from(index).ok()
}

/// Every package's payload, rebuilt from its producer: a command's or a
/// person's result, or what a player emitted.
fn payloads(evidence: &Evidence, problems: &mut Vec<String>) -> BTreeMap<String, Vec<u8>> {
    let mut payloads = BTreeMap::new();
    for (id, activation) in &evidence.history.activations {
        let Some(node) = evidence.history.node_of(activation) else {
            problems.push(format!("activation {id} has no node"));
            continue;
        };
        for output in &activation.outputs {
            let payload = match evidence.world.nodes.get(&node) {
                Some(Kind::Source) => {
                    let tag = String::from_utf8_lossy(&activation.result);
                    let emitted = evidence
                        .log
                        .submissions
                        .get(tag.as_ref())
                        .and_then(|s| s.emissions.get(output_index(&output.package)?));
                    match emitted {
                        Some((edge, payload)) if output.edge.as_deref() == Some(edge) => {
                            payload.clone()
                        }
                        other => {
                            problems.push(format!(
                                "{} from {node} matches no emission its player made (found {other:?})",
                                output.package
                            ));
                            continue;
                        }
                    }
                }
                _ => activation.result.clone(),
            };
            match evidence.history.packages.get(&output.package) {
                Some(package) if package.producer == *id && package.digest == output.digest => {}
                other => problems.push(format!(
                    "{} is recorded otherwise as a package than as {id}'s output: {other:?}",
                    output.package
                )),
            }
            if roles::content_digest(&payload) != output.digest {
                problems.push(format!(
                    "{} from {node} carries other bytes than its producer sent",
                    output.package
                ));
            }
            payloads.insert(output.package.clone(), payload);
        }
    }
    payloads
}

/// A command activation's stdin, and whether all its inputs are known.
fn stdin(activation: &Activation, payloads: &BTreeMap<String, Vec<u8>>) -> Option<Vec<u8>> {
    let Trigger::Packages(inputs) = &activation.trigger else {
        return None;
    };
    let mut sorted = inputs.clone();
    sorted.sort();
    let parts: Option<Vec<&[u8]>> = sorted
        .iter()
        .map(|id| payloads.get(id).map(Vec::as_slice))
        .collect();
    Some(parts?.join(&b"\n\n"[..]))
}

/// Checks each activation against its node's kind, returning each command
/// activation's stdin by node.
fn activations(
    evidence: &Evidence,
    payloads: &BTreeMap<String, Vec<u8>>,
    problems: &mut Vec<String>,
) -> BTreeMap<String, Vec<Vec<u8>>> {
    let (world, history) = (evidence.world, evidence.history);
    let mut stdins: BTreeMap<String, Vec<Vec<u8>>> = BTreeMap::new();
    let mut decided = BTreeSet::new();
    for (id, activation) in &history.activations {
        let Some(node) = history.node_of(activation) else {
            continue;
        };
        let broadcast = |problems: &mut Vec<String>| {
            let expected: BTreeSet<&str> = world.outgoing(&node).map(|(e, _)| e.as_str()).collect();
            let actual: BTreeSet<&str> = activation
                .outputs
                .iter()
                .filter_map(|o| o.edge.as_deref())
                .collect();
            if expected != actual || activation.outputs.len() != expected.len() {
                problems.push(format!(
                    "{node}'s result went to {actual:?}, not to each of its connections {expected:?} (activation {id})"
                ));
            }
        };
        match world.nodes.get(&node) {
            Some(Kind::Worker(worker)) => {
                let Trigger::Packages(inputs) = &activation.trigger else {
                    problems.push(format!("command {node} started work of its own ({id})"));
                    continue;
                };
                let edges: BTreeSet<Option<&str>> = inputs
                    .iter()
                    .map(|i| history.packages.get(i).and_then(|p| p.edge.as_deref()))
                    .collect();
                let incoming: BTreeSet<Option<&str>> = world
                    .incoming(&node)
                    .map(|(e, _)| Some(e.as_str()))
                    .collect();
                let joined = if worker.all {
                    inputs.len() == incoming.len() && edges == incoming
                } else {
                    inputs.len() == 1
                };
                if !joined {
                    problems.push(format!(
                        "{node} ran with inputs from {edges:?}, which is not one of its tasks (activation {id})"
                    ));
                }
                if !worker.role.succeeds(worker.bytes) {
                    problems.push(format!(
                        "{node} ({}) published a result, which it never can (activation {id})",
                        worker.role.name()
                    ));
                    continue;
                }
                let Some(input) = stdin(activation, payloads) else {
                    problems.push(format!(
                        "{node}'s inputs are not all in history (activation {id})"
                    ));
                    continue;
                };
                let expected = roles::output(worker.role, &node, &input);
                if activation.result != expected {
                    problems.push(format!(
                        "{node} published {:?}, but its program prints {:?} for that input (activation {id}, {} input bytes)",
                        String::from_utf8_lossy(&activation.result[..activation.result.len().min(80)]),
                        String::from_utf8_lossy(&expected[..expected.len().min(80)]),
                        input.len()
                    ));
                }
                broadcast(problems);
                stdins.entry(node.clone()).or_default().push(input);
            }
            Some(Kind::Human) => {
                let message = String::from_utf8_lossy(&activation.result).into_owned();
                match evidence.log.decisions.get(&message) {
                    Some((at, fate)) if *at == node && *fate != Fate::Refused => {}
                    other => problems.push(format!(
                        "{node} completed a task with {message:?}, which no one decided there ({other:?})"
                    )),
                }
                if !decided.insert(message.clone()) {
                    problems.push(format!("decision {message:?} completed two tasks"));
                }
                broadcast(problems);
            }
            Some(Kind::Inbox) => problems.push(format!("inbox {node} ran something ({id})")),
            Some(Kind::Source) if !matches!(activation.trigger, Trigger::Root { .. }) => {
                problems.push(format!("source {node} consumed work ({id})"))
            }
            Some(Kind::Sink) if !activation.outputs.is_empty() => {
                problems.push(format!("sink {node} sent work on ({id})"))
            }
            Some(_) => {}
            None => problems.push(format!("activation {id} is at unknown node {node}")),
        }
    }
    stdins
}

/// The players' and the person's moves: each done move is in history once,
/// each refused one never, each lost one at most once.
fn moves(evidence: &Evidence, problems: &mut Vec<String>) {
    let history = evidence.history;
    let mut found: BTreeMap<String, Vec<&Activation>> = BTreeMap::new();
    for activation in history.activations.values() {
        let tag = String::from_utf8_lossy(&activation.result).into_owned();
        found.entry(tag).or_default().push(activation);
    }
    let count = |tag: &str| found.get(tag).map_or(0, Vec::len);
    let judge = |what: String, fate: Fate, times: usize, problems: &mut Vec<String>| {
        let fits = match fate {
            Fate::Done => times == 1,
            Fate::Refused => times == 0,
            Fate::Unknown => times <= 1,
        };
        if !fits {
            problems.push(format!("{what} is {fate:?} but in history {times} times"));
        }
    };
    for (tag, submission) in &evidence.log.submissions {
        judge(
            format!("move {tag} at {}", submission.node),
            submission.fate,
            count(tag),
            problems,
        );
        for activation in found.get(tag).into_iter().flatten() {
            let consumed = match &activation.trigger {
                Trigger::Packages(inputs) => inputs.clone(),
                Trigger::Root { .. } => vec![],
            };
            if consumed != submission.inputs {
                problems.push(format!(
                    "move {tag} consumed {consumed:?}, not {:?}",
                    submission.inputs
                ));
            }
        }
        // A refused consumption of a package no one else could touch, which
        // is still there, was a legal move.
        if submission.fate == Fate::Refused
            && let [package] = submission.inputs.as_slice()
            && history
                .packages
                .get(package)
                .is_some_and(|p| p.disposition == "live")
        {
            problems.push(format!(
                "the server refused move {tag}, a legal consumption of {package}"
            ));
        }
    }
    for (message, (node, fate)) in &evidence.log.decisions {
        judge(
            format!("decision {message:?} at {node}"),
            *fate,
            count(message),
            problems,
        );
    }
    for (package, fate) in &evidence.log.retirements {
        let retired = history.packages.get(package).is_some_and(|p| {
            p.disposition == "retired" && p.retirement.as_deref() == Some("explicit")
        });
        let live = history
            .packages
            .get(package)
            .is_some_and(|p| p.disposition == "live");
        match fate {
            Fate::Done if !retired => problems.push(format!(
                "retiring {package} was accepted, but it is not retired"
            )),
            Fate::Refused if live => problems.push(format!(
                "the server refused to retire {package}, which was live"
            )),
            _ => {}
        }
    }
}

/// Where each package ended up.
fn packages(evidence: &Evidence, problems: &mut Vec<String>) {
    let (world, history, log) = (evidence.world, evidence.history, evidence.log);
    let retired_by_players: BTreeSet<&String> = log.retirements.iter().map(|(p, _)| p).collect();
    let mut discarded: BTreeMap<&str, usize> = BTreeMap::new();
    // Live inputs at each command node, by the connection that brought them.
    let mut waiting: BTreeMap<&str, BTreeMap<&str, usize>> = BTreeMap::new();
    for (id, package) in &history.packages {
        let kind = world.nodes.get(&package.holder);
        match package.disposition.as_str() {
            "consumed" => {
                let consumer = package
                    .consumer
                    .as_ref()
                    .and_then(|c| history.activations.get(c));
                let listed = consumer.is_some_and(
                    |a| matches!(&a.trigger, Trigger::Packages(inputs) if inputs.contains(id)),
                );
                if !listed {
                    problems.push(format!(
                        "{id} is consumed by {:?}, which did not take it",
                        package.consumer
                    ));
                }
            }
            "retired" => match (package.retirement.as_deref(), kind) {
                (Some("explicit"), Some(Kind::Sink)) if retired_by_players.contains(id) => {}
                (Some("explicit"), Some(Kind::Worker(_) | Kind::Human)) => {
                    *discarded.entry(package.holder.as_str()).or_default() += 1
                }
                (reason, _) => problems.push(format!(
                    "{id} at {} was retired ({reason:?}) though no one asked",
                    package.holder
                )),
            },
            "live" => match kind {
                Some(Kind::Inbox | Kind::Sink) => {}
                Some(Kind::Worker(_)) => {
                    *waiting
                        .entry(package.holder.as_str())
                        .or_default()
                        .entry(package.edge.as_deref().unwrap_or_default())
                        .or_default() += 1
                }
                Some(Kind::Human) => problems.push(format!(
                    "{id} is still waiting at {} after every task there was decided",
                    package.holder
                )),
                _ => problems.push(format!("{id} is live at {}", package.holder)),
            },
            other => problems.push(format!("{id} has disposition {other}")),
        }
    }
    for (node, count) in discarded {
        let tasks = log.discarded.get(node).copied().unwrap_or_default();
        let inputs = world.incoming(node).count().max(1);
        if count > tasks * inputs {
            problems.push(format!(
                "{count} packages were retired at {node}, but only {tasks} of its tasks were discarded"
            ));
        }
    }
    let parked = parked(evidence.status);
    for (node, by_edge) in waiting {
        let Some(worker) = world.worker(node) else {
            continue;
        };
        // Tasks the waiting inputs form: each input alone, or one per
        // connection at a join.
        let tasks = if worker.all {
            world
                .incoming(node)
                .map(|(edge, _)| by_edge.get(edge.as_str()).copied().unwrap_or_default())
                .min()
                .unwrap_or_default()
        } else {
            by_edge.values().sum()
        };
        let held = parked.get(node).copied().unwrap_or_default();
        if tasks > held {
            problems.push(format!(
                "{tasks} tasks wait at {node} ({}), but only {held} are parked",
                worker.role.name()
            ));
        }
    }
}

fn parked(status: &Value) -> BTreeMap<&str, usize> {
    let mut parked = BTreeMap::new();
    for failure in status["failures"].as_array().into_iter().flatten() {
        if failure["state"] == "parked" {
            *parked
                .entry(failure["node"].as_str().unwrap_or_default())
                .or_default() += 1;
        }
    }
    parked
}

/// The failed tasks the settled run reports.
fn failures(evidence: &Evidence, problems: &mut Vec<String>) {
    let chaos = !evidence.log.kills.is_empty() || evidence.log.crashes > 0;
    for failure in evidence.status["failures"].as_array().into_iter().flatten() {
        let node = failure["node"].as_str().unwrap_or_default();
        let Some(worker) = evidence.world.worker(node) else {
            problems.push(format!(
                "a task failed at {node}, which runs nothing: {failure}"
            ));
            continue;
        };
        if failure["state"] != "parked" {
            problems.push(format!("a settled run still retries at {node}: {failure}"));
        }
        if worker.role.succeeds(worker.bytes) && !chaos {
            problems.push(format!(
                "a task parked at {node} ({}), whose tasks succeed: {}",
                worker.role.name(),
                failure["error"]
            ));
        }
        if failure["attempts"].as_u64() != Some(worker.attempts) {
            problems.push(format!(
                "a task parked at {node} after {} attempts, not its {}",
                failure["attempts"], worker.attempts
            ));
        }
    }
}

/// How often each command's program ran, from its witness file, against how
/// often its tasks succeeded and its role.
fn runs(evidence: &Evidence, stdins: &BTreeMap<String, Vec<Vec<u8>>>, problems: &mut Vec<String>) {
    // Crashes interrupt attempts, which then run again; killed runs fail.
    let chaos = evidence.log.crashes > 0 || !evidence.log.kills.is_empty();
    for (node, runs) in &evidence.witness {
        let Some(worker) = evidence.world.worker(node) else {
            problems.push(format!("a program ran as {node}, which is not a command"));
            continue;
        };
        let mut ran: BTreeMap<&str, usize> = BTreeMap::new();
        for run in runs.iter().filter(|r| !evidence.log.kills.contains(&r.pid)) {
            *ran.entry(run.sha.as_str()).or_default() += 1;
        }
        let mut succeeded: BTreeMap<String, usize> = BTreeMap::new();
        for input in stdins.get(node).into_iter().flatten() {
            *succeeded.entry(roles::sha(input)).or_default() += 1;
        }
        for (sha, times) in &ran {
            let wins = succeeded.get(*sha).copied().unwrap_or_default();
            let fits = match worker.role {
                Role::Digest | Role::Binary if worker.role.succeeds(worker.bytes) => {
                    if chaos {
                        *times >= wins
                    } else {
                        *times == wins
                    }
                }
                Role::Flaky => {
                    if chaos {
                        *times > wins
                    } else {
                        *times == wins + 1
                    }
                }
                _ => {
                    chaos
                        || (*times >= worker.attempts as usize
                            && (*times as u64).is_multiple_of(worker.attempts))
                }
            };
            if !fits {
                problems.push(format!(
                    "{node} ({}) ran an input {times} times for {wins} published results (attempts {})",
                    worker.role.name(),
                    worker.attempts
                ));
            }
        }
        for (sha, wins) in &succeeded {
            if ran.get(sha.as_str()).copied().unwrap_or_default() < *wins && !chaos {
                problems.push(format!(
                    "{node} published {wins} results for an input its program never ran"
                ));
            }
        }
    }
}
