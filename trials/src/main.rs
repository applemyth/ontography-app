//! Trials of the whole Ontography app. Each trial generates a workflow of
//! every node kind the app runs — external clients, commands, people and
//! inboxes — starts it on a real `ontography server run`, and drives it with
//! players, a person deciding tasks, a manager, and chaos: server crashes and
//! killed programs. Then it drains the run, restarts the server in order, and
//! judges what the server kept, what its programs did, which processes it
//! left, and its store.

mod agent;
mod audit;
mod game;
mod history;
mod judge;
mod mcp;
mod node;
mod procs;
mod roles;
mod server;
mod world;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use game::Settings;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use world::Scale;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Preset {
    /// About two minutes: fixed seeds, small worlds.
    Quick,
    /// About fifteen minutes: new seeds, larger worlds, more chaos.
    Medium,
    /// About an hour: new seeds, the largest worlds.
    Soak,
}

struct Plan {
    budget: Duration,
    jobs: usize,
    seeds: Option<Vec<u64>>,
    settings: Settings,
}

impl Preset {
    fn plan(self) -> Plan {
        let settings = |scale, play, players, crashes, kills| Settings {
            scale,
            play: Duration::from_secs(play),
            players,
            crashes,
            kills,
        };
        let scale = |sources, workers, layers| Scale {
            sources,
            workers,
            layers,
            humans: 1 + workers / 6,
            inboxes: 1 + workers / 8,
            sinks: 1 + workers / 8,
        };
        match self {
            Self::Quick => Plan {
                budget: Duration::from_secs(120),
                jobs: 4,
                seeds: Some((1..=8).collect()),
                settings: settings(scale(2, 6, 2), 10, 2, 1, 2),
            },
            Self::Medium => Plan {
                budget: Duration::from_secs(15 * 60),
                jobs: 6,
                seeds: None,
                settings: settings(scale(3, 16, 3), 60, 4, 3, 8),
            },
            Self::Soak => Plan {
                budget: Duration::from_secs(60 * 60),
                jobs: 8,
                seeds: None,
                settings: settings(scale(4, 40, 4), 300, 6, 8, 30),
            },
        }
    }
}

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    mode: Option<Mode>,
    #[arg(long, value_enum, default_value = "quick")]
    preset: Preset,
    /// Play only this seed.
    #[arg(long)]
    seed: Option<u64>,
    /// Trials at once, each with its own server.
    #[arg(long)]
    jobs: Option<usize>,
    /// The ontography binary to test. By default the harness builds it from
    /// this tree, so harness and server always match.
    #[arg(long)]
    ontography: Option<PathBuf>,
    /// Where trials keep their data; failed trials are kept.
    #[arg(long, default_value = concat!(env!("CARGO_MANIFEST_DIR"), "/runs"))]
    runs: PathBuf,
}

#[derive(Subcommand)]
enum Mode {
    /// Act as a command node's program, as a document's `argv` places it.
    Node(node::Args),
    /// Act as an agent node's program, pulling work through the node tools.
    Agent(agent::Args),
}

/// Builds the server from the tree this harness was built from.
fn build_server() -> Result<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .context("workspace root")?;
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let status = std::process::Command::new(cargo)
        .args([
            "build",
            "--locked",
            "--quiet",
            "--bin",
            "ontography",
            "--manifest-path",
        ])
        .arg(root.join("Cargo.toml"))
        .status()
        .context("run cargo")?;
    if !status.success() {
        bail!("building the ontography server failed");
    }
    Ok(root.join("target/debug/ontography"))
}

/// Stops servers an earlier, killed harness left running.
fn clear_stale(runs: &Path) {
    let alive = procs::snapshot().unwrap_or_default();
    for entry in std::fs::read_dir(runs).into_iter().flatten().flatten() {
        let Ok(recorded) = std::fs::read_to_string(entry.path().join("servers")) else {
            continue;
        };
        for pid in recorded.lines().filter_map(|l| l.parse::<i32>().ok()) {
            // Only a server of that data directory, not a reused pid.
            let data = entry.path().join("data");
            if alive.iter().any(|p| {
                p.pid == pid
                    && p.command.contains("server run")
                    && p.command.contains(&*data.to_string_lossy())
            }) {
                let _ = nix::sys::signal::killpg(
                    nix::unistd::Pid::from_raw(pid),
                    nix::sys::signal::Signal::SIGKILL,
                );
                println!("stopped a stale server {pid} of {}", entry.path().display());
            }
        }
    }
}

fn report(seed: u64, outcome: &game::Outcome, took: Duration) {
    let counts: Vec<String> = outcome
        .counts
        .iter()
        .map(|(name, count)| format!("{count} {name}"))
        .collect();
    println!(
        "seed {seed}: {}, {:.1}s",
        counts.join(", "),
        took.as_secs_f64()
    );
    let timing: Vec<String> = outcome
        .timing
        .iter()
        .map(|(phase, time)| format!("{phase} {:.1}s", time.as_secs_f64()))
        .collect();
    println!("  time: {}", timing.join(", "));
    for problem in outcome.problems.iter().take(30) {
        println!("  PROBLEM {problem}");
    }
    if outcome.problems.len() > 30 {
        println!("  … {} more", outcome.problems.len() - 30);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    match args.mode {
        Some(Mode::Node(node)) => return node::run(node),
        Some(Mode::Agent(agent)) => return agent::run(agent).await,
        None => {}
    }
    let plan = args.preset.plan();
    std::fs::create_dir_all(&args.runs)?;
    clear_stale(&args.runs);
    let binary = match &args.ontography {
        Some(path) => path.clone(),
        None => build_server()?,
    };
    tokio::spawn(async {
        use tokio::signal::unix::{SignalKind, signal};
        let (Ok(mut interrupt), Ok(mut terminate)) = (
            signal(SignalKind::interrupt()),
            signal(SignalKind::terminate()),
        ) else {
            return;
        };
        tokio::select! {
            _ = interrupt.recv() => {}
            _ = terminate.recv() => {}
        }
        procs::cleanup();
        eprintln!("interrupted: stopped every server and program of this harness");
        std::process::exit(130);
    });

    let started = Instant::now();
    let mut seeds: Box<dyn Iterator<Item = u64>> = match (args.seed, plan.seeds) {
        (Some(seed), _) => Box::new(std::iter::once(seed)),
        (None, Some(fixed)) => Box::new(fixed.into_iter()),
        (None, None) => Box::new(std::iter::repeat_with(|| rand::random::<u32>() as u64)),
    };
    let jobs = args.jobs.unwrap_or(plan.jobs).max(1);
    let mut running = JoinSet::new();
    let (mut trials, mut failures) = (0, 0);
    loop {
        while running.len() < jobs && started.elapsed() < plan.budget {
            let Some(seed) = seeds.next() else { break };
            let dir = args.runs.join(seed.to_string());
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir)?;
            let (binary, settings) = (binary.clone(), plan.settings.clone());
            running.spawn(async move {
                let clock = Instant::now();
                let outcome = game::trial(seed, &dir, &binary, &settings).await;
                (seed, dir, outcome, clock.elapsed())
            });
        }
        let Some(joined) = running.join_next().await else {
            break;
        };
        let (seed, dir, outcome, took) = joined?;
        trials += 1;
        let mut outcome = outcome.unwrap_or_else(|error| game::Outcome {
            problems: vec![format!("the trial could not run: {error:#}")],
            ..Default::default()
        });
        // A leftover seen after each stop is one problem.
        let mut seen = std::collections::BTreeSet::new();
        outcome
            .problems
            .retain(|problem| seen.insert(problem.clone()));
        report(seed, &outcome, took);
        if outcome.problems.is_empty() {
            let _ = std::fs::remove_dir_all(&dir);
        } else {
            failures += 1;
            println!("  kept {}", dir.display());
        }
    }
    println!(
        "{trials} trials, {failures} with problems, {:.0}s",
        started.elapsed().as_secs_f64()
    );
    if failures > 0 {
        std::process::exit(1);
    }
    Ok(())
}
