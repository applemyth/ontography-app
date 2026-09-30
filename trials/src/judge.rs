//! The workflow judge: the server's history, final status and the programs'
//! witness files, against what the documentation says each node kind does
//! and what the actors did. Written from docs/WORKFLOWS.md, not the app.
//!
//! - A command runs once per task with its inputs, in package order, joined
//!   by blank lines on stdin, and its result goes to every outgoing
//!   connection. A failed task retries up to its node's `max_attempts`, then
//!   parks, except that output its contract refuses parks at once; a
//!   person's decision goes to every outgoing connection too.
//! - An agent's submission without outputs goes to every successor; with
//!   outputs, exactly those are sent, each along the connection it names.
//! - External nodes do only what their players did; inboxes hold work.
//! - An edit retires exactly the pending work its preview listed, and leaves
//!   the run's graph as its document says. Work done while edits applied is
//!   judged against a version of the graph that explains it.
//! - After the run settles, work waits only where it may: in inboxes, at
//!   sinks, in an incomplete join, or in a parked task.

use crate::game::{Fate, Log};
use crate::history::{Activation, History, Trigger};
use crate::roles::{self, Role, Style};
use crate::world::{Kind, World};
use anyhow::Result;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

pub struct Evidence<'a> {
    /// Every committed version of the world, oldest first.
    pub worlds: &'a [Arc<World>],
    pub history: &'a History,
    pub log: &'a Log,
    pub status: &'a Value,
    pub witness: Witness,
}

impl Evidence<'_> {
    fn last(&self) -> &World {
        self.worlds.last().expect("a first version")
    }

    /// The versions of the world that had `node`.
    fn versions<'b>(&'b self, node: &'b str) -> impl Iterator<Item = &'b World> {
        self.worlds
            .iter()
            .map(AsRef::as_ref)
            .filter(move |world| world.nodes.contains_key(node))
    }

    /// What `node` is, in the latest version that has it.
    fn kind(&self, node: &str) -> Option<&Kind> {
        self.worlds
            .iter()
            .rev()
            .find_map(|world| world.nodes.get(node))
    }
}

/// One run of a command's program, or one attempt an agent began, as the
/// program recorded it.
#[derive(Debug)]
pub struct Run {
    pub pid: u32,
    pub sha: String,
}

/// What the programs recorded: their runs by node, and what an agent's node
/// tools refused it that they should not have.
#[derive(Debug, Default)]
pub struct Witness {
    pub runs: BTreeMap<String, Vec<Run>>,
    pub refusals: Vec<String>,
}

/// Tool errors an agent can meet in a legal course of events: its task was
/// finished, retired or replaced meanwhile, or the run is stopping.
const ORDINARY: [&str; 4] = [
    "stale_task",
    "task_in_progress",
    "stopping",
    "attempt_ended",
];

pub fn read_witness(dir: &Path) -> Result<Witness> {
    let mut witness = Witness::default();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(node) = path.file_stem().map(|s| s.to_string_lossy().into_owned()) else {
            continue;
        };
        let mut runs = Vec::new();
        for value in std::fs::read_to_string(&path)?
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        {
            let event = value["event"].as_str();
            // A command records one line per run; an agent, one per event.
            if event.is_none() || event == Some("begun") {
                runs.push(Run {
                    pid: value["pid"].as_u64().unwrap_or_default() as u32,
                    sha: value["sha"].as_str().unwrap_or_default().to_owned(),
                });
            }
            let error = match event {
                Some("submitted") if value["ok"] == false => &value["reply"],
                Some("begin" | "read" | "next_trigger") => &value["error"],
                _ => continue,
            };
            let code = error["code"].as_str().unwrap_or_default();
            if !ORDINARY.contains(&code) {
                witness
                    .refusals
                    .push(format!("{node}'s node tools refused a legal call: {value}"));
            }
        }
        witness.runs.insert(node, runs);
    }
    Ok(witness)
}

/// What the judge needs of a node that runs a program for its tasks.
struct Program {
    attempts: u64,
    all: bool,
    /// Whether its tasks succeed, given no chaos.
    succeeds: bool,
    /// Whether its first run at each input fails by design.
    flaky: bool,
    label: &'static str,
}

fn program(kind: &Kind) -> Option<Program> {
    match kind {
        Kind::Worker(worker) => Some(Program {
            // Output core refuses for its contract, such as non-UTF-8 stdout
            // under `text`, parks at once; other failures retry.
            attempts: if worker.role == Role::Binary && !worker.bytes {
                1
            } else {
                worker.attempts
            },
            all: worker.all,
            succeeds: worker.role.succeeds(worker.bytes),
            flaky: worker.role == Role::Flaky,
            label: worker.role.name(),
        }),
        Kind::Agent(agent) => Some(Program {
            attempts: agent.attempts,
            all: agent.all,
            succeeds: true,
            flaky: agent.style == Style::Flaky,
            label: agent.style.name(),
        }),
        _ => None,
    }
}

pub fn judge(evidence: &Evidence) -> Vec<String> {
    let mut problems = evidence.witness.refusals.clone();
    let payloads = payloads(evidence, &mut problems);
    let inputs = activations(evidence, &payloads, &mut problems);
    moves(evidence, &mut problems);
    packages(evidence, &mut problems);
    failures(evidence, &mut problems);
    runs(evidence, &inputs, &mut problems);
    problems
}

/// The run's graph after the last edit is the last version's.
pub fn graph(
    world: &World,
    nodes: &BTreeSet<String>,
    edges: &BTreeSet<(String, String, String)>,
) -> Vec<String> {
    let mut problems = Vec::new();
    let expected: BTreeSet<String> = world.nodes.keys().cloned().collect();
    if *nodes != expected {
        problems.push(format!(
            "the run's nodes are {nodes:?}, not its document's {expected:?}"
        ));
    }
    let expected: BTreeSet<(String, String, String)> = world
        .edges
        .iter()
        .map(|(id, edge)| (id.clone(), edge.from.clone(), edge.to.clone()))
        .collect();
    if *edges != expected {
        let extra: Vec<_> = edges.difference(&expected).collect();
        let missing: Vec<_> = expected.difference(edges).collect();
        problems.push(format!(
            "the run's connections differ from its document's: extra {extra:?}, missing {missing:?}"
        ));
    }
    problems
}

/// The index of an output within its activation, from its package ID.
fn output_index(package: &str) -> Option<usize> {
    let (_, output) = package.split_once('/')?;
    let index = uuid::Uuid::parse_str(output).ok()?.as_u128();
    usize::try_from(index).ok()
}

/// Every package's payload, rebuilt from its producer: a command's or a
/// person's result, what a routing agent sent along each connection, or
/// what a player emitted.
fn payloads(evidence: &Evidence, problems: &mut Vec<String>) -> BTreeMap<String, Vec<u8>> {
    let mut payloads = BTreeMap::new();
    for (id, activation) in &evidence.history.activations {
        let Some(node) = evidence.history.node_of(activation) else {
            problems.push(format!("activation {id} has no node"));
            continue;
        };
        for output in &activation.outputs {
            let payload = match evidence.kind(&node) {
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
                Some(Kind::Agent(agent)) if agent.style == Style::Route => {
                    let edge = output.edge.as_deref().unwrap_or_default();
                    let result = String::from_utf8_lossy(&activation.result);
                    format!("{result} via {edge}").into_bytes()
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

/// A command activation's stdin, if all its inputs are known.
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

/// Checks each activation against its node's kind, returning each program
/// activation's input by node.
fn activations(
    evidence: &Evidence,
    payloads: &BTreeMap<String, Vec<u8>>,
    problems: &mut Vec<String>,
) -> BTreeMap<String, Vec<Vec<u8>>> {
    let history = evidence.history;
    let mut stdins: BTreeMap<String, Vec<Vec<u8>>> = BTreeMap::new();
    let mut decided = BTreeSet::new();
    for (id, activation) in &history.activations {
        let Some(node) = history.node_of(activation) else {
            continue;
        };
        let sent: BTreeSet<&str> = activation
            .outputs
            .iter()
            .filter_map(|o| o.edge.as_deref())
            .collect();
        // Whether the outputs went to each connection of some version.
        let broadcast = |problems: &mut Vec<String>| {
            let fits = evidence.versions(&node).any(|world| {
                let expected: BTreeSet<&str> =
                    world.outgoing(&node).map(|(e, _)| e.as_str()).collect();
                expected == sent && activation.outputs.len() == expected.len()
            });
            if !fits {
                problems.push(format!(
                    "{node}'s result went to {sent:?}, not to each of its connections (activation {id})"
                ));
            }
        };
        // A program's task: one input, or one from every connection at a
        // join, in some version.
        let task = |problems: &mut Vec<String>| {
            let Trigger::Packages(inputs) = &activation.trigger else {
                problems.push(format!("{node} started work of its own ({id})"));
                return false;
            };
            let edges: BTreeSet<Option<&str>> = inputs
                .iter()
                .map(|i| history.packages.get(i).and_then(|p| p.edge.as_deref()))
                .collect();
            let joined = evidence.versions(&node).any(|world| {
                let incoming: BTreeSet<Option<&str>> = world
                    .incoming(&node)
                    .map(|(e, _)| Some(e.as_str()))
                    .collect();
                if world.nodes.get(&node).is_some_and(Kind::all) {
                    inputs.len() == incoming.len() && edges == incoming
                } else {
                    inputs.len() == 1
                }
            });
            if !joined {
                problems.push(format!(
                    "{node} ran with inputs from {edges:?}, which is not one of its tasks (activation {id})"
                ));
            }
            true
        };
        match evidence.kind(&node) {
            Some(Kind::Agent(agent)) => {
                if !task(problems) {
                    continue;
                }
                let Trigger::Packages(inputs) = &activation.trigger else {
                    continue;
                };
                let known: Option<Vec<Vec<u8>>> =
                    inputs.iter().map(|i| payloads.get(i).cloned()).collect();
                let Some(known) = known else {
                    problems.push(format!(
                        "{node}'s inputs are not all in history (activation {id})"
                    ));
                    continue;
                };
                let input = roles::agent_input(known);
                let result = roles::agent_result(&node, &input);
                if activation.result != result.as_bytes() {
                    problems.push(format!(
                        "{node} published {:?}, but its program submits {result:?} for that input (activation {id})",
                        String::from_utf8_lossy(&activation.result),
                    ));
                }
                if agent.style == Style::Route {
                    let owned: BTreeSet<String> = sent.iter().map(|e| e.to_string()).collect();
                    let fits = evidence.versions(&node).any(|world| {
                        let to: Vec<String> =
                            world.outgoing(&node).map(|(e, _)| e.clone()).collect();
                        let expected: BTreeSet<String> = roles::routes(&to, &input, &result)
                            .into_iter()
                            .map(|(edge, _)| edge)
                            .collect();
                        expected == owned && activation.outputs.len() == expected.len()
                    });
                    if !fits {
                        problems.push(format!(
                            "{node} routed to {sent:?}, which its program never chose (activation {id})"
                        ));
                    }
                } else {
                    broadcast(problems);
                }
                stdins.entry(node.clone()).or_default().push(input);
            }
            Some(Kind::Worker(_)) => {
                if !task(problems) {
                    continue;
                }
                // The roles the node has had; an edit can change it.
                let roles: Vec<(Role, bool)> = evidence
                    .versions(&node)
                    .filter_map(|world| match world.nodes.get(&node) {
                        Some(Kind::Worker(worker)) => Some((worker.role, worker.bytes)),
                        _ => None,
                    })
                    .collect();
                if !roles.iter().any(|(role, bytes)| role.succeeds(*bytes)) {
                    problems.push(format!(
                        "{node} published a result, which it never can (activation {id})"
                    ));
                    continue;
                }
                let Some(input) = stdin(activation, payloads) else {
                    problems.push(format!(
                        "{node}'s inputs are not all in history (activation {id})"
                    ));
                    continue;
                };
                let matches = roles.iter().any(|(role, bytes)| {
                    role.succeeds(*bytes)
                        && activation.result == roles::output(*role, &node, &input)
                });
                if !matches {
                    let expected = roles::output(roles[roles.len() - 1].0, &node, &input);
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
    let (history, log, last) = (evidence.history, evidence.log, evidence.last());
    let retired_by_players: BTreeSet<&String> = log.retirements.iter().map(|(p, _)| p).collect();
    let mut discarded: BTreeMap<&str, usize> = BTreeMap::new();
    // Live inputs at each program node, by the connection that brought them.
    let mut waiting: BTreeMap<&str, BTreeMap<&str, usize>> = BTreeMap::new();
    for (id, package) in &history.packages {
        let kind = evidence.kind(&package.holder);
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
                (Some("explicit"), Some(Kind::Worker(_) | Kind::Agent(_) | Kind::Human)) => {
                    *discarded.entry(package.holder.as_str()).or_default() += 1
                }
                (Some(reason), _)
                    if log.edit_retirements.get(id).map(String::as_str) == Some(reason) => {}
                (reason, _) => problems.push(format!(
                    "{id} at {} was retired ({reason:?}) though neither an actor nor an edit's preview asked",
                    package.holder
                )),
            },
            "live" if !last.nodes.contains_key(&package.holder) => problems.push(format!(
                "{id} is still live at {}, which an edit removed",
                package.holder
            )),
            "live" => match kind {
                Some(Kind::Inbox | Kind::Sink) => {}
                Some(Kind::Worker(_) | Kind::Agent(_)) => {
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
    // An edit discards exactly the work its preview listed.
    for (id, reason) in &log.edit_retirements {
        let retired = history.packages.get(id).is_some_and(|p| {
            p.disposition == "retired" && p.retirement.as_deref() == Some(reason.as_str())
        });
        if !retired {
            problems.push(format!(
                "an edit's preview listed {id} to retire ({reason}), but it was not"
            ));
        }
    }
    for (node, count) in discarded {
        let tasks = log.discarded.get(node).copied().unwrap_or_default();
        let inputs = evidence
            .versions(node)
            .map(|world| world.incoming(node).count())
            .max()
            .unwrap_or(1)
            .max(1);
        if count > tasks * inputs {
            problems.push(format!(
                "{count} packages were retired at {node}, but only {tasks} of its tasks were discarded"
            ));
        }
    }
    let parked = parked(evidence.status);
    for (node, by_edge) in waiting {
        let Some(program) = last.nodes.get(node).and_then(program) else {
            continue;
        };
        // Tasks the waiting inputs form: each input alone, or one per
        // connection at a join.
        let tasks = if program.all {
            last.incoming(node)
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
                program.label
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

/// Whether chaos or an edit may have run a node's tasks more or less often
/// than its program alone explains.
fn disturbed(evidence: &Evidence, node: &str) -> bool {
    let variants: BTreeSet<String> = evidence
        .versions(node)
        .filter_map(|world| world.nodes.get(node))
        .map(|kind| format!("{kind:?}"))
        .collect();
    evidence.log.crashes > 0 || !evidence.log.kills.is_empty() || variants.len() > 1
}

/// The failed tasks the settled run reports.
fn failures(evidence: &Evidence, problems: &mut Vec<String>) {
    for failure in evidence.status["failures"].as_array().into_iter().flatten() {
        let node = failure["node"].as_str().unwrap_or_default();
        let Some(program) = evidence.last().nodes.get(node).and_then(program) else {
            problems.push(format!(
                "a task failed at {node}, which runs nothing: {failure}"
            ));
            continue;
        };
        if failure["state"] != "parked" {
            problems.push(format!("a settled run still retries at {node}: {failure}"));
        }
        if program.succeeds && !disturbed(evidence, node) {
            problems.push(format!(
                "a task parked at {node} ({}), whose tasks succeed: {}",
                program.label, failure["error"]
            ));
        }
        if failure["attempts"].as_u64() != Some(program.attempts) {
            problems.push(format!(
                "a task parked at {node} after {} attempts, not its {}",
                failure["attempts"], program.attempts
            ));
        }
    }
}

/// How often each program ran, from its witness file, against how often its
/// tasks succeeded and its role.
fn runs(evidence: &Evidence, stdins: &BTreeMap<String, Vec<Vec<u8>>>, problems: &mut Vec<String>) {
    for (node, runs) in &evidence.witness.runs {
        let Some(program) = evidence.kind(node).and_then(program) else {
            problems.push(format!("a program ran as {node}, which runs nothing"));
            continue;
        };
        // Crashes interrupt attempts, which then run again; killed runs
        // fail; an edit may change the program.
        let chaos = disturbed(evidence, node);
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
            let fits = match (program.succeeds, program.flaky) {
                _ if chaos => *times >= wins,
                (true, false) => *times == wins,
                (true, true) => *times == wins + 1,
                (false, _) => {
                    *times >= program.attempts as usize
                        && (*times as u64).is_multiple_of(program.attempts)
                }
            };
            if !fits {
                problems.push(format!(
                    "{node} ({}) ran an input {times} times for {wins} published results (attempts {})",
                    program.label, program.attempts
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
