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

pub struct World {
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
        document,
        nodes,
        edges,
    }
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
