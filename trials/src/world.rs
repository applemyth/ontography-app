//! A generated workflow of every node kind the app runs: external sources
//! and sinks that players act for, command nodes running this binary in a
//! chosen role, agent nodes running it as a program that pulls work through
//! the node tools, people's decisions, and inboxes. Work flows forward
//! through layers, so it always ends; joins, parallel connections, bytes
//! contracts, and tasks that can never succeed occur by chance.

use crate::roles::{Role, Style};
use rand::{Rng, seq::IndexedRandom};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::Path;

/// How large a world to generate.
#[derive(Clone, Copy, Debug)]
pub struct Scale {
    pub sources: usize,
    pub workers: usize,
    pub layers: usize,
    pub humans: usize,
    pub inboxes: usize,
    pub sinks: usize,
}

/// A command node.
#[derive(Clone, Debug)]
pub struct Worker {
    pub role: Role,
    /// The node's `retry.max_attempts`.
    pub attempts: u64,
    /// Its result contract accepts any bytes.
    pub bytes: bool,
    /// Join `all`: one input from every incoming connection.
    pub all: bool,
    pub layer: usize,
}

/// An agent node whose program is this binary in agent mode.
#[derive(Clone, Debug)]
pub struct Agent {
    pub style: Style,
    pub attempts: u64,
    pub all: bool,
    pub layer: usize,
}

#[derive(Clone, Debug)]
pub enum Kind {
    Source,
    Worker(Worker),
    Agent(Agent),
    Human,
    Inbox,
    Sink,
}

impl Kind {
    /// Whether the app runs a program for this node's tasks.
    pub fn runs(&self) -> bool {
        matches!(self, Self::Worker(_) | Self::Agent(_))
    }

    /// Whether the node joins one input from every incoming connection.
    pub fn all(&self) -> bool {
        match self {
            Self::Worker(worker) => worker.all,
            Self::Agent(agent) => agent.all,
            _ => false,
        }
    }

    fn layer(&self) -> Option<usize> {
        match self {
            Self::Worker(worker) => Some(worker.layer),
            Self::Agent(agent) => Some(agent.layer),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Edge {
    pub from: String,
    pub to: String,
}

#[derive(Clone, Debug)]
pub struct World {
    pub name: String,
    pub document: Value,
    pub nodes: BTreeMap<String, Kind>,
    pub edges: BTreeMap<String, Edge>,
}

impl World {
    pub fn incoming<'a>(&'a self, node: &'a str) -> impl Iterator<Item = (&'a String, &'a Edge)> {
        self.edges.iter().filter(move |(_, edge)| edge.to == node)
    }

    pub fn outgoing<'a>(&'a self, node: &'a str) -> impl Iterator<Item = (&'a String, &'a Edge)> {
        self.edges.iter().filter(move |(_, edge)| edge.from == node)
    }

    pub fn named(&self, pick: fn(&Kind) -> bool) -> Vec<String> {
        self.nodes
            .iter()
            .filter(|(_, kind)| pick(kind))
            .map(|(id, _)| id.clone())
            .collect()
    }
}

fn role(rng: &mut impl Rng) -> Role {
    match rng.random_range(0..100) {
        0..55 => Role::Digest,
        55..70 => Role::Flaky,
        70..80 => Role::Broken,
        80..92 => Role::Binary,
        92..95 => Role::Slow,
        _ => Role::Big,
    }
}

/// Where a world's programs are: this binary, the directory they record
/// their runs in, and the `ontography` binary agents reach their tools with.
pub struct Programs<'a> {
    pub program: &'a Path,
    pub witness: &'a Path,
    pub ontography: &'a Path,
}

pub fn generate(rng: &mut impl Rng, scale: Scale, name: &str, programs: &Programs) -> World {
    let mut nodes = BTreeMap::new();
    let sources: Vec<String> = (0..scale.sources.max(1))
        .map(|i| format!("src{i}"))
        .collect();
    for id in &sources {
        nodes.insert(id.clone(), Kind::Source);
    }
    let mut layers: Vec<Vec<String>> = vec![Vec::new(); scale.layers.max(1)];
    for i in 0..scale.workers {
        // Every layer gets a worker before any gets a second.
        let layer = if i < layers.len() {
            i
        } else {
            rng.random_range(0..layers.len())
        };
        let attempts = rng.random_range(2..=3);
        // About a third of the workers are agents that pull their work.
        let (id, kind) = if rng.random_bool(0.3) {
            let style = *[Style::Broadcast, Style::Route, Style::Flaky]
                .choose(rng)
                .expect("styles");
            let agent = Agent {
                style,
                attempts,
                all: false,
                layer,
            };
            (format!("a{i}"), Kind::Agent(agent))
        } else {
            let role = role(rng);
            let worker = Worker {
                role,
                attempts,
                bytes: role == Role::Binary && rng.random_bool(0.6),
                all: false,
                layer,
            };
            (format!("w{i}"), Kind::Worker(worker))
        };
        layers[layer].push(id.clone());
        nodes.insert(id, kind);
    }
    let terminals: Vec<String> = (0..scale.humans)
        .map(|i| format!("h{i}"))
        .chain((0..scale.inboxes.max(1)).map(|i| format!("in{i}")))
        .chain((0..scale.sinks.max(1)).map(|i| format!("x{i}")))
        .collect();
    for id in &terminals {
        let kind = match &id[..1] {
            "h" => Kind::Human,
            "i" => Kind::Inbox,
            _ => Kind::Sink,
        };
        nodes.insert(id.clone(), kind);
    }

    let mut edges = BTreeMap::new();
    let connect = |edges: &mut BTreeMap<String, Edge>, from: &str, to: &str| {
        let id = format!("e{}", edges.len());
        edges.insert(
            id,
            Edge {
                from: from.into(),
                to: to.into(),
            },
        );
    };
    // Each worker takes work from sources or from workers of earlier layers.
    for (depth, layer) in layers.iter().enumerate() {
        let earlier: Vec<String> = sources
            .iter()
            .cloned()
            .chain(layers[..depth].iter().flatten().cloned())
            .collect();
        for id in layer {
            let all = earlier.len() >= 2 && rng.random_bool(0.25);
            let count = if all {
                rng.random_range(2..=3)
            } else {
                rng.random_range(1..=2)
            };
            for _ in 0..count {
                connect(&mut edges, earlier.choose(rng).expect("a source"), id);
            }
            match nodes.get_mut(id) {
                Some(Kind::Worker(worker)) => worker.all = all,
                Some(Kind::Agent(agent)) => agent.all = all,
                _ => {}
            }
        }
    }
    // Terminals take work from workers, or from sources when there are none.
    let producers: Vec<String> = if scale.workers > 0 {
        layers.iter().flatten().cloned().collect()
    } else {
        sources.clone()
    };
    for id in &terminals {
        for _ in 0..rng.random_range(1..=2) {
            connect(&mut edges, producers.choose(rng).expect("a producer"), id);
        }
    }
    // People's decisions go on to an inbox half the time.
    let inboxes: Vec<&String> = terminals.iter().filter(|id| id.starts_with("in")).collect();
    for id in terminals.iter().filter(|id| id.starts_with('h')) {
        if rng.random_bool(0.5) {
            connect(&mut edges, id, inboxes.choose(rng).expect("an inbox"));
        }
    }
    // Every source and worker sends its work somewhere.
    let senders: Vec<String> = sources
        .iter()
        .chain(layers.iter().flatten())
        .cloned()
        .collect();
    for id in senders {
        if !edges.values().any(|edge| edge.from == id) {
            let depth = nodes.get(&id).and_then(Kind::layer).map_or(0, |l| l + 1);
            let later: Vec<String> = layers
                .iter()
                .skip(depth)
                .flatten()
                .filter(|w| !nodes.get(*w).is_some_and(Kind::all))
                .cloned()
                .chain(terminals.iter().cloned())
                .collect();
            connect(&mut edges, &id, later.choose(rng).expect("a terminal"));
        }
    }

    let document = document(name, &nodes, &edges, programs);
    World {
        name: name.into(),
        document,
        nodes,
        edges,
    }
}

fn terminal(kind: &Kind) -> bool {
    matches!(kind, Kind::Human | Kind::Inbox | Kind::Sink)
}

/// The next free `{prefix}N` name.
fn fresh<T>(names: &BTreeMap<String, T>, prefix: &str) -> String {
    let next = names
        .keys()
        .filter_map(|name| name.strip_prefix(prefix)?.parse::<usize>().ok())
        .max()
        .map_or(0, |n| n + 1);
    format!("{prefix}{next}")
}

/// One edit a manager might make to a running world, and what it does:
/// change a command's role (a settings change), add an inbox, add or remove
/// a connection, remove a sink with its work, or switch a node's join, which
/// replaces it. Work keeps flowing forward, so the world still ends.
pub fn mutate(rng: &mut impl Rng, world: &World, programs: &Programs) -> Option<(World, String)> {
    let (mut nodes, mut edges) = (world.nodes.clone(), world.edges.clone());
    let named = |pick: fn(&Kind) -> bool| -> Vec<String> {
        nodes
            .iter()
            .filter(|(_, kind)| pick(kind))
            .map(|(id, _)| id.clone())
            .collect()
    };
    let workers = named(Kind::runs);
    let outgoing = |edges: &BTreeMap<String, Edge>, node: &str| {
        edges.values().filter(|edge| edge.from == node).count()
    };
    let what = match rng.random_range(0..6) {
        0 => {
            let commands = named(|kind| matches!(kind, Kind::Worker(_)));
            let id = commands.choose(rng)?.clone();
            let Some(Kind::Worker(worker)) = nodes.get_mut(&id) else {
                return None;
            };
            let next = role(rng);
            if next == worker.role {
                return None;
            }
            worker.role = next;
            format!("{id} now runs as {}", next.name())
        }
        1 => {
            let from = workers.choose(rng)?.clone();
            let id = fresh(&nodes, "in");
            nodes.insert(id.clone(), Kind::Inbox);
            let edge = fresh(&edges, "e");
            edges.insert(
                edge.clone(),
                Edge {
                    from: from.clone(),
                    to: id.clone(),
                },
            );
            format!("{from} also sends to a new inbox {id} along {edge}")
        }
        2 => {
            let removable: Vec<String> = edges
                .iter()
                .filter(|(_, edge)| {
                    nodes.get(&edge.from).is_some_and(Kind::runs)
                        && nodes.get(&edge.to).is_some_and(terminal)
                        && outgoing(&edges, &edge.from) >= 2
                })
                .map(|(id, _)| id.clone())
                .collect();
            let id = removable.choose(rng)?.clone();
            let edge = edges.remove(&id).expect("chosen");
            format!("{} no longer sends to {} ({id})", edge.from, edge.to)
        }
        3 => {
            let sinks = named(|kind| matches!(kind, Kind::Sink));
            if sinks.len() < 2 {
                return None;
            }
            let id = sinks.choose(rng)?.clone();
            nodes.remove(&id);
            edges.retain(|_, edge| edge.to != id && edge.from != id);
            format!("sink {id} is removed with its work")
        }
        4 => {
            let joinable: Vec<String> = workers
                .iter()
                .filter(|id| edges.values().filter(|edge| edge.to == **id).count() >= 2)
                .cloned()
                .collect();
            let id = joinable.choose(rng)?.clone();
            let all = match nodes.get_mut(&id) {
                Some(Kind::Worker(worker)) => {
                    worker.all = !worker.all;
                    worker.all
                }
                Some(Kind::Agent(agent)) => {
                    agent.all = !agent.all;
                    agent.all
                }
                _ => return None,
            };
            format!(
                "{id} now joins {}, which replaces it",
                if all { "all" } else { "any" }
            )
        }
        _ => {
            let from = workers.choose(rng)?.clone();
            let targets = named(terminal);
            let to = targets.choose(rng)?.clone();
            let edge = fresh(&edges, "e");
            edges.insert(
                edge.clone(),
                Edge {
                    from: from.clone(),
                    to: to.clone(),
                },
            );
            format!("{from} also sends to {to} along {edge}")
        }
    };
    let document = document(&world.name, &nodes, &edges, programs);
    Some((
        World {
            name: world.name.clone(),
            document,
            nodes,
            edges,
        },
        what,
    ))
}

fn document(
    name: &str,
    nodes: &BTreeMap<String, Kind>,
    edges: &BTreeMap<String, Edge>,
    programs: &Programs,
) -> Value {
    let (program, witness) = (programs.program, programs.witness);
    let bytes = nodes
        .values()
        .any(|kind| matches!(kind, Kind::Worker(worker) if worker.bytes));
    let nodes: Vec<Value> = nodes
        .iter()
        .map(|(id, kind)| match kind {
            Kind::Source if id == "src0" => json!({"id": id, "component": "external"}),
            Kind::Source => json!({"id": id, "component": "external", "root": ["workflow"]}),
            Kind::Sink => json!({"id": id, "component": "external"}),
            Kind::Human => json!({"id": id, "component": "human", "config": {"prompt": "Decide"}}),
            Kind::Inbox => json!({"id": id, "component": "inbox"}),
            Kind::Agent(agent) => {
                let mut argv = vec![
                    json!(program),
                    json!("agent"),
                    json!("--style"),
                    json!(agent.style.name()),
                    json!("--name"),
                    json!(id),
                    json!("--witness"),
                    json!(witness),
                    json!("--ontography"),
                    json!(programs.ontography),
                ];
                let to: Vec<&str> = edges
                    .iter()
                    .filter(|(_, edge)| edge.from == *id)
                    .map(|(name, _)| name.as_str())
                    .collect();
                if !to.is_empty() {
                    argv.extend([json!("--to"), json!(to.join(","))]);
                }
                json!({
                    "id": id,
                    "component": "agent",
                    "config": {"prompt": "A trial agent: pull work through the node tools.", "argv": argv},
                    "retry": {"max_attempts": agent.attempts, "initial_delay_secs": 0, "max_delay_secs": 0},
                    "join": if agent.all { "all" } else { "any" },
                })
            }
            Kind::Worker(worker) => {
                let mut value = json!({
                    "id": id,
                    "component": "command",
                    "config": {
                        "argv": [program, "node", "--role", worker.role.name(), "--name", id, "--witness", witness],
                        "timeout_secs": if worker.role == Role::Slow { 1 } else { 30 },
                    },
                    "retry": {"max_attempts": worker.attempts, "initial_delay_secs": 0, "max_delay_secs": 0},
                    "join": if worker.all { "all" } else { "any" },
                });
                if worker.bytes {
                    value["result"] = json!("blob");
                }
                value
            }
        })
        .collect();
    let mut document = json!({
        "name": name,
        "entry": "src0",
        "nodes": nodes,
        "edges": edges.iter().map(|(id, edge)| json!({"from": edge.from, "to": edge.to, "name": id})).collect::<Vec<_>>(),
    });
    if bytes {
        document["contracts"] = json!({"blob": {"object_type": "Blob", "validator": "bytes"}});
    }
    document
}
