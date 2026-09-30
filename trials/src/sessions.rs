//! The sessions trial. Several app sessions share one real server, each
//! following a seeded script of what a person does with a session's terminal
//! (driver.rs), all at once, with this binary as their Pi (pi.rs) and as
//! their terminal client (attach.rs). Chaos kills the server while shells and
//! Pi are live, and the sessions resume after the restart. One session binds
//! a graph and plays moves at it. At the end the server stops in order with
//! sessions still live, restarts, and stops again.
//!
//! Judged from docs/SESSIONS.md and docs/SESSION_DESIGN.md:
//! - Session states and `session.list` agree with each script's model, and so
//!   do the terminal's attachment and program (Pi or the shell).
//! - Detach and reattach keep the same shell and Pi, and reattaching shows
//!   the screen as it is; only one client controls a terminal; `/quit`
//!   returns to the same shell, and `pi` there resumes the session's
//!   conversation; exiting the shell suspends the session, attached or not;
//!   a closed session never resumes. A new terminal starts Pi with the
//!   session's saved conversation and the environment of the command that
//!   activated the session.
//! - Every line typed reaches Pi once, in order, and shows on screen once, in
//!   order; resizing reaches Pi; history is a frozen view while Pi runs on.
//! - After a suspension, a close or an orderly stop, no process of the
//!   session survives: not the shell, Pi, or Pi's children, whether or not
//!   they ignore SIGHUP. After a crash, none survives once its session has
//!   resumed or the server has stopped. Terminal sockets (`pty-*.sock`,
//!   `pi-*.sock`), shell rc files and half-written files are removed.
//! - The graph comes back with its session after crashes and stops: moves
//!   the server accepted stay in history once, and the run is active,
//!   suspended or closed as its session is.

use crate::driver::{self, Driver, Ending};
use crate::game::Outcome;
use crate::procs::{Proc, Tracker};
use crate::server::Server;
use anyhow::{Context, Result};
use ontography_app::client::Client;
use ontography_app::persistence::Paths;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, watch};

#[derive(clap::Args, Debug)]
pub struct Args {
    /// Play this seed; by default a new one.
    #[arg(long)]
    pub seed: Option<u64>,
    /// App sessions on the server, played at once.
    #[arg(long, default_value_t = 3)]
    pub sessions: usize,
    /// Rounds of each session's script.
    #[arg(long, default_value_t = 2)]
    pub rounds: usize,
    /// Server crashes, at moments fixed by the seed.
    #[arg(long, default_value_t = 1)]
    pub crashes: usize,
}

/// How long the sessions may play before the trial gives up on them.
const PLAY_LIMIT: Duration = Duration::from_secs(300);
/// How long programs may take to notice that their server has stopped.
const STOPPED: Duration = Duration::from_secs(1);

/// What a process of a session is, from its command line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Shell,
    /// The private launcher the shell's `pi` runs, which starts Pi.
    Launcher,
    Pi,
    /// A program Pi started in its own process group.
    Child {
        ignores_hangup: bool,
    },
    Other,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Shell => "the shell",
            Self::Launcher => "Pi's launcher",
            Self::Pi => "Pi",
            Self::Child {
                ignores_hangup: false,
            } => "Pi's child in its own process group",
            Self::Child {
                ignores_hangup: true,
            } => "Pi's child in its own process group, ignoring SIGHUP",
            Self::Other => "another program",
        })
    }
}

#[derive(Clone, Debug)]
pub struct Label {
    pub session: Option<String>,
    /// The generation of the terminal a shell or launcher belongs to.
    pub generation: Option<String>,
    pub role: Role,
    /// The process's session ID: the pid of the shell that leads its
    /// terminal, for every process that terminal ran.
    pub sid: Option<i32>,
}

impl Label {
    fn observe(process: &Proc) -> Self {
        let mut label = Self::of(&process.command);
        label.sid = nix::unistd::getsid(Some(nix::unistd::Pid::from_raw(process.pid)))
            .ok()
            .map(|sid| sid.as_raw());
        label
    }

    fn of(command: &str) -> Self {
        let words: Vec<&str> = command.split_whitespace().collect();
        let after = |flag: &str| {
            words
                .iter()
                .position(|word| *word == flag)
                .and_then(|i| words.get(i + 1))
                .map(|word| word.to_string())
        };
        if words.contains(&"internal-pi") {
            return Self {
                session: after("--session-id"),
                generation: after("--generation"),
                role: Role::Launcher,
                sid: None,
            };
        }
        if let Some(rc) = after("--rcfile") {
            return Self {
                session: session_in(&rc),
                generation: Path::new(&rc)
                    .file_stem()
                    .map(|stem| stem.to_string_lossy().into_owned()),
                role: Role::Shell,
                sid: None,
            };
        }
        if words.get(1) == Some(&"pi") && words.contains(&"--witness") {
            return match after("--child") {
                Some(hangup) => Self {
                    session: after("--owner"),
                    generation: None,
                    role: Role::Child {
                        ignores_hangup: hangup == "ignore",
                    },
                    sid: None,
                },
                None => Self {
                    session: after("--session-dir").and_then(|dir| session_in(&dir)),
                    generation: None,
                    role: Role::Pi,
                    sid: None,
                },
            };
        }
        Self {
            session: None,
            generation: None,
            role: Role::Other,
            sid: None,
        }
    }
}

/// The session a path under the store's `sessions/` belongs to.
fn session_in(path: &str) -> Option<String> {
    let (_, rest) = path.split_once("/sessions/")?;
    let id = rest.split('/').next()?;
    uuid::Uuid::parse_str(id).ok().map(|_| id.to_owned())
}

fn key(process: &Proc) -> (i32, String) {
    (process.pid, process.start.clone())
}

/// When a process a crash left was found still running.
#[derive(Clone, Copy, Debug)]
pub enum When {
    Resumed,
    Stopped,
}

/// What a crash struck.
struct Crash {
    /// The session processes running then.
    live: Vec<(Proc, Label)>,
    /// Their terminals' shells. Every process a terminal ran has its shell's
    /// pid as its session ID, however late it started.
    shells: BTreeSet<i32>,
    /// The terminal generations whose rc files existed then.
    generations: BTreeSet<String>,
    /// The terminal sockets that existed then, by inode: a session's next
    /// terminal replaces its socket with another.
    sockets: BTreeSet<u64>,
    /// When the server was dead: whatever it wrote was written before.
    at: std::time::SystemTime,
}

struct Leftover {
    process: Proc,
    label: Label,
    crash: usize,
    resumed: bool,
    stopped: bool,
}

/// What the sessions share: the server as it is now, the processes of every
/// incarnation, and what crashes left.
pub struct Stage {
    client: RwLock<Client>,
    /// Bumped once a crashed server has restarted.
    generation: watch::Sender<u64>,
    crashing: AtomicBool,
    progress: AtomicUsize,
    tracker: Mutex<Tracker>,
    /// The fake Pi, as the server is told to run it.
    pub pi: PathBuf,
    pub witness: PathBuf,
    pub dir: PathBuf,
    /// The store, as the server names it.
    pub data: PathBuf,
    /// The server's socket directory.
    pub sockets: PathBuf,
    names: BTreeMap<String, String>,
    started: Instant,
    crashes: Mutex<Vec<Crash>>,
    /// Processes already reported for outliving a stop.
    outlived: Mutex<BTreeSet<(i32, String)>>,
    leftovers: Mutex<BTreeMap<(i32, String), Leftover>>,
}

impl Stage {
    pub async fn client(&self) -> Client {
        self.client.read().await.clone()
    }

    pub fn generation(&self) -> u64 {
        *self.generation.borrow()
    }

    /// Whether a crash struck since `generation`, or strikes now.
    pub fn interrupted(&self, generation: u64) -> bool {
        self.crashing.load(Ordering::SeqCst) || self.generation() != generation
    }

    /// Waits until the server has restarted after `generation`.
    pub async fn restarted(&self, generation: u64) -> u64 {
        let mut watch = self.generation.subscribe();
        loop {
            let now = *watch.borrow_and_update();
            if now != generation || watch.changed().await.is_err() {
                return now;
            }
        }
    }

    pub fn advance(&self) {
        self.progress.fetch_add(1, Ordering::SeqCst);
    }

    pub fn observe(&self) {
        let _ = self
            .tracker
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .observe();
    }

    fn root(&self, pid: u32) {
        self.tracker
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .root(pid);
    }

    /// Tracked processes still running, not zombies, that `keep` selects.
    pub fn alive(&self, keep: impl Fn(&Proc, &Label) -> bool) -> Vec<(Proc, Label)> {
        let survivors = self
            .tracker
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .survivors()
            .unwrap_or_default();
        let chosen: Vec<(Proc, Label)> = survivors
            .into_iter()
            .map(|process| {
                let label = Label::observe(&process);
                (process, label)
            })
            .filter(|(process, label)| keep(process, label))
            .collect();
        let dead = zombies(&chosen.iter().map(|(p, _)| p.pid).collect::<Vec<_>>());
        chosen
            .into_iter()
            .filter(|(process, _)| !dead.contains(&process.pid))
            .collect()
    }

    /// Waits up to `grace` for the processes `keep` selects to exit, and
    /// returns those that did not.
    pub async fn wait_gone(
        &self,
        keep: impl Fn(&Proc, &Label) -> bool,
        grace: Duration,
    ) -> Vec<(Proc, Label)> {
        let deadline = Instant::now() + grace;
        loop {
            let left = self.alive(&keep);
            if left.is_empty() || Instant::now() >= deadline {
                return left;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// The crash, counted from one, that struck this process or its
    /// terminal.
    pub fn crash_of(&self, process: &Proc, label: &Label) -> Option<usize> {
        self.crashes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .position(|crash| {
                crash.live.iter().any(|(p, _)| key(p) == key(process))
                    || label.sid.is_some_and(|sid| crash.shells.contains(&sid))
            })
            .map(|index| index + 1)
    }

    pub fn reported(&self, process: &Proc) -> bool {
        self.outlived
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains(&key(process))
    }

    pub fn report(&self, process: &Proc) {
        self.outlived
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key(process));
    }

    pub fn leftover(&self, process: Proc, label: Label, when: When) {
        let Some(crash) = self.crash_of(&process, &label) else {
            return;
        };
        let mut leftovers = self.leftovers.lock().unwrap_or_else(|p| p.into_inner());
        let entry = leftovers.entry(key(&process)).or_insert(Leftover {
            process,
            label,
            crash,
            resumed: false,
            stopped: false,
        });
        match when {
            When::Resumed => entry.resumed = true,
            When::Stopped => entry.stopped = true,
        }
    }

    /// Adds a line to a session's log in the trial's directory, which a
    /// trial with problems keeps.
    pub fn note(&self, session: &str, text: &str) {
        use std::io::Write;
        let line = format!("{:8.3} {text}\n", self.started.elapsed().as_secs_f64());
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join(format!("{session}.log")))
        {
            let _ = file.write_all(line.as_bytes());
        }
    }

    /// A session's witness records.
    pub fn witness(&self, session: &str) -> Vec<Value> {
        std::fs::read_to_string(self.witness.join(format!("{session}.jsonl")))
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect()
    }

    fn name(&self, session: Option<&str>) -> String {
        session
            .and_then(|id| self.names.get(id))
            .cloned()
            .unwrap_or_else(|| "no session".into())
    }

    /// Whose terminal generation this is, and whether a crash found it live.
    fn terminal_of(&self, drivers: &[Driver], generation: &str) -> String {
        let owner = drivers
            .iter()
            .find(|d| d.generations.contains(generation))
            .map_or("an unknown session", |d| d.name.as_str());
        let crash = self
            .crashes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .position(|crash| crash.generations.contains(generation));
        match crash {
            Some(index) => format!("{owner}'s terminal, live at crash {}", index + 1),
            None => format!("{owner}'s terminal"),
        }
    }
}

/// A session's shell rc files, by the generation of the terminal each
/// starts, wherever under the session's directory the server keeps them.
pub fn rc_files(data: &Path, session: &str) -> BTreeMap<String, PathBuf> {
    let mut found = BTreeMap::new();
    let mut stack = vec![data.join("sessions").join(session)];
    while let Some(directory) = stack.pop() {
        for entry in std::fs::read_dir(&directory)
            .into_iter()
            .flatten()
            .flatten()
        {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            if entry.file_type().is_ok_and(|t| t.is_dir()) {
                stack.push(path);
            } else if let Some(generation) = name.strip_suffix(".bashrc") {
                found.insert(generation.to_owned(), path);
            }
        }
    }
    found
}

fn inode(path: &Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    std::fs::symlink_metadata(path).ok().map(|m| m.ino())
}

/// Processes among `pids` that are zombies: dead, and not yet reaped.
fn zombies(pids: &[i32]) -> BTreeSet<i32> {
    if pids.is_empty() {
        return BTreeSet::new();
    }
    let list: Vec<String> = pids.iter().map(i32::to_string).collect();
    let Ok(output) = std::process::Command::new("/bin/ps")
        .args(["-o", "pid=,stat=", "-p", &list.join(",")])
        .output()
    else {
        return BTreeSet::new();
    };
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let pid = fields.next()?.parse().ok()?;
            fields.next()?.starts_with('Z').then_some(pid)
        })
        .collect()
}

/// The fake Pi as a program the server can run by path: this binary in pi
/// mode, recording to the trial's witness directory.
fn write_pi(dir: &Path, witness: &Path) -> Result<PathBuf> {
    let quote = |path: &Path| format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"));
    let path = dir.join("pi");
    let program = std::env::current_exe()?;
    std::fs::write(
        &path,
        format!(
            "#!/bin/sh\nexec {} pi --witness {} \"$@\"\n",
            quote(&program),
            quote(witness)
        ),
    )?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))?;
    Ok(path)
}

/// Servers of this trial, so a later harness can stop any left behind.
fn record_servers(dir: &Path, pids: &[u32]) -> Result<()> {
    let lines: String = pids.iter().map(|pid| format!("{pid}\n")).collect();
    std::fs::write(dir.join("servers"), lines)?;
    Ok(())
}

/// Endings for each session: one closes and one stays live, when there are
/// two; the rest by chance.
fn endings(rng: &mut impl Rng, sessions: usize) -> Vec<Ending> {
    let all = [Ending::Close, Ending::Exit, Ending::Suspend, Ending::Live];
    let mut endings: Vec<Ending> = (0..sessions)
        .map(|index| match index {
            0 => Ending::Live,
            1 => Ending::Close,
            _ => all[rng.random_range(0..all.len())],
        })
        .collect();
    endings.shuffle(rng);
    endings
}

pub async fn trial(seed: u64, dir: &Path, binary: &Path, args: &Args) -> Result<Outcome> {
    let started = Instant::now();
    let mut outcome = Outcome::default();
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let witness = dir.join("witness");
    std::fs::create_dir_all(&witness)?;
    let pi = write_pi(dir, &witness)?;
    let data = dir.join("data");
    // The socket directory is named by the store's path: an earlier trial of
    // this seed may have left one.
    if let Some(sockets) = Paths::initialize(&data)?.socket.parent() {
        let _ = std::fs::remove_dir_all(sockets);
    }
    let mut server = Server::start(binary, &data).await?;
    let mut incarnations = server.incarnations.clone();
    record_servers(dir, &incarnations)?;
    let paths = Paths::initialize(&data)?;

    // The sessions, each with its own project.
    let mut records = Vec::new();
    for index in 0..args.sessions.max(1) {
        let name = format!("s{index}");
        let project = dir.join("projects").join(&name);
        std::fs::create_dir_all(&project)?;
        let record = server
            .call("session.create", json!({"project": project, "name": name}))
            .await?;
        records.push((name, record));
    }
    let names = records
        .iter()
        .map(|(name, record)| {
            (
                record["session_id"].as_str().unwrap_or_default().to_owned(),
                name.clone(),
            )
        })
        .collect();
    let stage = Arc::new(Stage {
        client: RwLock::new(server.client.clone()),
        generation: watch::channel(0).0,
        crashing: AtomicBool::new(false),
        progress: AtomicUsize::new(0),
        tracker: Mutex::new(Tracker::default()),
        pi,
        witness,
        dir: dir.to_path_buf(),
        data: paths.root.clone(),
        sockets: server
            .socket()
            .parent()
            .context("the server socket's directory")?
            .to_path_buf(),
        names,
        started,
        crashes: Mutex::new(Vec::new()),
        outlived: Mutex::new(BTreeSet::new()),
        leftovers: Mutex::new(BTreeMap::new()),
    });
    stage.root(server.incarnations[0]);

    // Every process of every server, observed while the sessions play.
    let watching = Arc::new(AtomicBool::new(true));
    let observer = {
        let (stage, watching) = (Arc::clone(&stage), Arc::clone(&watching));
        std::thread::spawn(move || {
            while watching.load(Ordering::SeqCst) {
                stage.observe();
                std::thread::sleep(Duration::from_millis(200));
            }
        })
    };

    let endings = endings(&mut rng, records.len());
    let mut total = 0;
    let mut handles = Vec::new();
    for (index, ((name, record), ending)) in records.iter().zip(endings).enumerate() {
        let graph = index == 0;
        let script = driver::script(&mut rng, graph, args.rounds, ending);
        total += script.len();
        let driver = Driver::new(
            Arc::clone(&stage),
            name.clone(),
            record,
            seed.wrapping_mul(1000).wrapping_add(index as u64 + 1),
        );
        handles.push(tokio::spawn(driver.play(script)));
    }
    outcome.counts.insert("sessions", records.len());

    // Chaos: crashes once the scripts are far enough along, while Pi runs.
    let mut chaos = ChaCha8Rng::seed_from_u64(seed.wrapping_mul(31).wrapping_add(7));
    let mut thresholds: Vec<usize> = (0..args.crashes)
        .map(|_| (total as f64 * chaos.random_range(0.2..0.7)) as usize)
        .collect();
    thresholds.sort();
    let limit = Instant::now() + PLAY_LIMIT;
    let mut crashes = 0;
    let mut left_by_crashes = 0;
    'chaos: for threshold in thresholds {
        loop {
            if handles.iter().all(|h| h.is_finished()) || Instant::now() > limit {
                break 'chaos;
            }
            if stage.progress.load(Ordering::SeqCst) >= threshold
                && !stage.alive(|_, l| l.role == Role::Pi).is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        tokio::time::sleep(Duration::from_millis(chaos.random_range(0..150))).await;
        stage.observe();
        let live = stage.alive(|_, l| l.session.is_some());
        stage.crashing.store(true, Ordering::SeqCst);
        server.crash().await?;
        let at = std::time::SystemTime::now();
        crashes += 1;
        for name in stage.names.values() {
            stage.note(name, &format!("CRASH {crashes}"));
        }
        let shells = live
            .iter()
            .filter(|(_, l)| l.role == Role::Shell)
            .map(|(p, _)| p.pid)
            .collect();
        let generations = stage
            .names
            .keys()
            .flat_map(|id| rc_files(&stage.data, id).into_keys())
            .collect();
        let sockets = stage
            .names
            .keys()
            .filter_map(|id| inode(&stage.sockets.join(format!("pty-{id}.sock"))))
            .collect();
        stage
            .crashes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(Crash {
                live: live.clone(),
                shells,
                generations,
                sockets,
                at,
            });
        // What the crash itself did not end, kept with the trial.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let left = stage.alive(|p, l| stage.crash_of(p, l) == Some(crashes));
        left_by_crashes += left.len();
        let struck: String = live
            .iter()
            .map(|(p, l)| {
                let fate = if left.iter().any(|(q, _)| key(q) == key(p)) {
                    "survived"
                } else {
                    "ended"
                };
                format!(
                    "{} {} ({}, {fate}) ppid {} generation {:?}: {}\n",
                    p.pid,
                    stage.name(l.session.as_deref()),
                    l.role,
                    p.ppid,
                    l.generation,
                    p.command
                )
            })
            .collect();
        std::fs::write(dir.join(format!("crash-{crashes}.txt")), struck)?;
        if let Err(error) = server.restart().await {
            handles.iter().for_each(|h| h.abort());
            watching.store(false, Ordering::SeqCst);
            return Err(error.context("restarting the crashed server"));
        }
        let pid = *server.incarnations.last().expect("an incarnation");
        stage.root(pid);
        incarnations.push(pid);
        record_servers(dir, &incarnations)?;
        *stage.client.write().await = server.client.clone();
        stage.generation.send_modify(|g| *g += 1);
        stage.crashing.store(false, Ordering::SeqCst);
    }
    outcome.counts.insert("crashes", crashes);
    outcome.counts.insert("left by crashes", left_by_crashes);

    let mut drivers = Vec::new();
    for handle in handles {
        let remaining = limit.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, handle).await {
            Ok(Ok(driver)) => drivers.push(driver),
            Ok(Err(error)) => outcome
                .problems
                .push(format!("a session's driver failed: {error}")),
            Err(_) => outcome.problems.push(format!(
                "the sessions did not finish within {}s",
                PLAY_LIMIT.as_secs()
            )),
        }
    }
    outcome.timing.insert("play", started.elapsed());

    // Every session as its script left it, in one listing.
    let clock = Instant::now();
    let listing = server.call("session.list", json!({})).await?;
    outcome
        .problems
        .extend(disagreements(&listing, &drivers, "after the play"));
    // An orderly stop with sessions live: nothing of theirs may survive it.
    stage.observe();
    if let Some(problem) = server.stop().await? {
        outcome.problems.push(problem);
    }
    for driver in &mut drivers {
        driver.let_go();
    }
    let mut reported = BTreeSet::new();
    outcome
        .problems
        .extend(after_stop(&stage, &drivers, "the orderly stop", &mut reported).await);
    outcome.timing.insert("stop", clock.elapsed());

    // A restart keeps every record as it was and starts no terminal.
    let clock = Instant::now();
    let mut server = Server::start(binary, &data).await?;
    let pid = server.incarnations[0];
    stage.root(pid);
    incarnations.push(pid);
    record_servers(dir, &incarnations)?;
    *stage.client.write().await = server.client.clone();
    stage.generation.send_modify(|g| *g += 1);
    let listing = server.call("session.list", json!({})).await?;
    outcome
        .problems
        .extend(disagreements(&listing, &drivers, "after a restart"));
    for driver in &drivers {
        let status = server
            .call("terminal.status", json!({"session_id": driver.id}))
            .await?;
        if status["running"] != false {
            outcome.problems.push(format!(
                "{}: a restarted server started its terminal unasked: {status}",
                driver.name
            ));
        }
    }
    // The graph comes back when its session resumes.
    for driver in &mut drivers {
        driver.graph_after_restart().await;
    }
    stage.observe();
    if let Some(problem) = server.stop().await? {
        outcome.problems.push(problem);
    }
    outcome
        .problems
        .extend(after_stop(&stage, &drivers, "the second orderly stop", &mut reported).await);
    outcome.timing.insert("restart", clock.elapsed());
    watching.store(false, Ordering::SeqCst);
    let _ = observer.join();

    // The whole record.
    for driver in &drivers {
        outcome.problems.extend(driver.problems.iter().cloned());
        outcome.problems.extend(driver.judge_witness());
        for (name, count) in &driver.counts {
            *outcome.counts.entry(name).or_default() += count;
        }
        *outcome.counts.entry("lines").or_default() += driver.typed.len();
        if driver.abandoned {
            *outcome.counts.entry("abandoned").or_default() += 1;
        }
    }
    outcome.problems.extend(leftovers(&stage));
    {
        let tracker = stage.tracker.lock().unwrap_or_else(|p| p.into_inner());
        outcome.counts.insert("processes", tracker.count());
        tracker.kill_survivors();
    }
    // Anything of this trial that escaped the tracker, such as a child
    // started between observations of a crashing server.
    tokio::time::sleep(Duration::from_millis(300)).await;
    for process in sweep(dir) {
        outcome.problems.push(format!(
            "untracked process {} of this trial still ran at the end: {}",
            process.pid, process.command
        ));
    }
    Ok(outcome)
}

/// Kills the fake Pi and its children of the trial in `dir` wherever they
/// run, and returns those it found running.
pub fn sweep(dir: &Path) -> Vec<Proc> {
    let marker = dir.join("witness").to_string_lossy().into_owned();
    let found: Vec<Proc> = crate::procs::snapshot()
        .unwrap_or_default()
        .into_iter()
        .filter(|process| process.command.contains(&marker))
        .collect();
    let dead = zombies(&found.iter().map(|p| p.pid).collect::<Vec<_>>());
    let running: Vec<Proc> = found
        .into_iter()
        .filter(|process| !dead.contains(&process.pid))
        .collect();
    for process in &running {
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(process.pid),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
    running
}

/// Where `session.list` disagrees with the scripts' models.
fn disagreements(listing: &Value, drivers: &[Driver], when: &str) -> Vec<String> {
    let mut problems = Vec::new();
    for driver in drivers.iter().filter(|d| !d.abandoned) {
        let Some(record) = listing["sessions"]
            .as_array()
            .and_then(|all| all.iter().find(|s| s["session_id"] == driver.id.as_str()))
        else {
            problems.push(format!(
                "{}: session.list lost the session {when}",
                driver.name
            ));
            continue;
        };
        if record["status"] != driver.status.name() {
            problems.push(format!(
                "{}: session.list says {} {when}, where the script left it {}",
                driver.name,
                record["status"],
                driver.status.name()
            ));
        }
        if record["run_id"] != json!(driver.run) {
            problems.push(format!(
                "{}: its run is {} {when}, not {:?}",
                driver.name, record["run_id"], driver.run
            ));
        }
        if record["pi"]["active_conversation_id"] != driver.conversation.as_str() {
            problems.push(format!(
                "{}: its active conversation is {} {when}",
                driver.name, record["pi"]["active_conversation_id"]
            ));
        }
    }
    problems
}

/// What a stopped server must not leave: processes of any session, its
/// socket directory and the sockets in it, shell rc files, and half-written
/// files. Processes a crash left are judged with the crash. A file already
/// reported after an earlier stop is not reported again.
async fn after_stop(
    stage: &Stage,
    drivers: &[Driver],
    what: &str,
    reported: &mut BTreeSet<PathBuf>,
) -> Vec<String> {
    let mut problems = Vec::new();
    for (process, label) in stage.wait_gone(|_, _| true, STOPPED).await {
        if stage.crash_of(&process, &label).is_some() {
            stage.leftover(process, label, When::Stopped);
        } else if !stage.reported(&process) {
            stage.report(&process);
            problems.push(format!(
                "process {} of {} ({}) outlived {what}: {}",
                process.pid,
                stage.name(label.session.as_deref()),
                label.role,
                process.command
            ));
        }
    }
    let mut report = |path: PathBuf, text: String| {
        if reported.insert(path) {
            problems.push(text);
        }
    };
    if stage.sockets.exists() {
        report(
            stage.sockets.clone(),
            format!(
                "the server left its socket directory {} after {what}",
                stage.sockets.display()
            ),
        );
    }
    // A terminal's files: its launcher socket and its shell's rc file.
    let mut terminals: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();
    for entry in std::fs::read_dir(&stage.sockets)
        .into_iter()
        .flatten()
        .flatten()
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(generation) = name
            .strip_prefix("pi-")
            .and_then(|n| n.strip_suffix(".sock"))
        {
            terminals
                .entry(generation.to_owned())
                .or_default()
                .push(entry.path());
        } else if let Some(id) = name
            .strip_prefix("pty-")
            .and_then(|n| n.strip_suffix(".sock"))
        {
            let crash = inode(&entry.path()).and_then(|inode| {
                stage
                    .crashes
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .iter()
                    .position(|crash| crash.sockets.contains(&inode))
            });
            report(
                entry.path(),
                format!(
                    "{what} left the terminal socket of {}{}: {}",
                    stage.name(Some(id)),
                    crash.map_or(String::new(), |c| format!(
                        "'s terminal, live at crash {}",
                        c + 1
                    )),
                    entry.path().display()
                ),
            );
        } else if name != "server.sock" || stage.sockets.join(&name).exists() {
            report(
                entry.path(),
                format!("{what} left {}", entry.path().display()),
            );
        }
    }
    for driver in drivers {
        for (generation, path) in rc_files(&stage.data, &driver.id) {
            terminals.entry(generation).or_default().push(path);
        }
    }
    for (generation, files) in terminals {
        let names: Vec<String> = files
            .iter()
            .filter_map(|f| f.file_name().map(|n| n.to_string_lossy().into_owned()))
            .collect();
        report(
            files[0].clone(),
            format!(
                "{what} left the files of {}: {}",
                stage.terminal_of(drivers, &generation),
                names.join(" and ")
            ),
        );
    }
    for leftover in temporaries(&stage.data) {
        // A crash can cut a write short; its file was written before.
        let written = std::fs::metadata(&leftover).and_then(|m| m.modified()).ok();
        let crash = written.and_then(|written| {
            stage
                .crashes
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .iter()
                .position(|crash| written <= crash.at)
        });
        report(
            leftover.clone(),
            format!(
                "half-written file left after {what}{}: {}",
                crash.map_or(String::new(), |c| format!(
                    ", written before crash {} cut it short",
                    c + 1
                )),
                leftover.display()
            ),
        );
    }
    problems
}

/// Processes a crash left that were still running once their session had
/// resumed, or the server had stopped.
fn leftovers(stage: &Stage) -> Vec<String> {
    let leftovers = stage.leftovers.lock().unwrap_or_else(|p| p.into_inner());
    leftovers
        .values()
        .map(|leftover| {
            let mut when = Vec::new();
            if leftover.resumed {
                when.push("its session resumed");
            }
            if leftover.stopped {
                when.push("the server stopped in order");
            }
            let session = leftover.label.session.as_deref();
            let hangups = session.map_or(0, |id| {
                stage
                    .witness(id)
                    .iter()
                    .filter(|e| {
                        e["event"] == "hangup ignored"
                            && e["pid"].as_i64() == Some(i64::from(leftover.process.pid))
                    })
                    .count()
            });
            // Its parent when the crash struck: its Pi, or none if Pi had
            // already quit and left it to launchd.
            let parent = stage
                .crashes
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(leftover.crash - 1)
                .and_then(|crash| {
                    crash
                        .live
                        .iter()
                        .find(|(p, _)| key(p) == key(&leftover.process))
                        .map(|(p, _)| p.ppid)
                });
            let why = match (leftover.label.role, parent) {
                (Role::Child { .. }, Some(1)) => "; its Pi had quit before the crash",
                (Role::Child { .. }, Some(_)) => "; its Pi ran until the crash",
                _ => "",
            };
            let hangup = match (leftover.label.role, hangups) {
                (Role::Child { .. }, 0) => "; no SIGHUP reached it".to_owned(),
                (_, 0) => String::new(),
                (_, n) => format!("; it ignored {n} SIGHUP"),
            };
            format!(
                "process {} of {} ({}{why}{hangup}), running when crash {} struck, still ran after {}: {}",
                leftover.process.pid,
                stage.name(session),
                leftover.label.role,
                leftover.crash,
                when.join(" and after "),
                leftover.process.command
            )
        })
        .collect()
}

/// Half-written files: `.NAME.tmp`, outside core's own stores.
fn temporaries(root: &Path) -> Vec<PathBuf> {
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
