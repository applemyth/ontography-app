//! A script's server, driven through a crash. Every request of a script goes
//! through the driver. When the server has died, the driver notes how,
//! starts it again without the crash-point library, and reopens the run, so
//! the script can find out from history what its interrupted request did, as
//! the documentation asks of a client after a restart, and go on. It also
//! makes the manager's documented recoveries: a run whose core session
//! faulted is reopened with `run.resume`, as the error asks, and stopped
//! workers are restarted with `flow.resume` (docs/WORKFLOWS.md). An injected
//! fault explains either; without one, either is a problem. At the end it
//! settles the run, exports its history, and stops the server in order (all
//! again, should it die meanwhile); then it audits the store, and starts and
//! stops the server once more for what a restart and an orderly stop must
//! leave.

use crate::history::History;
use crate::interpose::{self, Action};
use crate::procs::Tracker;
use crate::server::{Exited, Reply, Server, send};
use crate::world::{Kind, World};
use crate::{audit, game};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::time::{Duration, Instant};

/// How long a request may go unanswered; long, as the machine may be busy.
const ANSWER: Duration = Duration::from_secs(60);
/// How long a run may take to settle, or a task to appear.
const SETTLE: Duration = Duration::from_secs(30);
/// Deaths after which the driver gives up on the server.
const DEATHS: usize = 3;
/// Recoveries after which the driver stops making them.
const RECOVERIES: usize = 3;

/// An end of the server other than an orderly stop.
#[derive(Debug)]
pub struct Death {
    /// The step under way.
    pub step: String,
    pub status: ExitStatus,
    /// The driver killed it, as it stopped answering or would not stop.
    pub by_driver: bool,
}

impl Death {
    /// Killed by SIGKILL, as the library kills at a crash point.
    pub fn killed(&self) -> bool {
        self.status.signal() == Some(9) && !self.by_driver
    }
}

enum Stop {
    InOrder,
    /// It died while stopping, and runs again.
    Died,
    Failed,
}

pub struct Driver {
    pub dir: PathBuf,
    pub data: PathBuf,
    /// The crash-point library's log.
    pub log: PathBuf,
    pub armed: Option<(usize, Action)>,
    server: Server,
    tracker: Tracker,
    /// The world's command nodes, whose workers must keep running.
    workers: BTreeSet<String>,
    recoveries: usize,
    /// The run's ID, which the script starts it with as its `start_id`.
    pub run: String,
    started: bool,
    step: String,
    pub deaths: Vec<Death>,
    pub problems: Vec<String>,
    /// Observations that are not problems, such as a clean failure.
    pub notes: Vec<String>,
    /// How many calls the library had counted when the script's focus
    /// began; learnt in a dry run.
    pub focus: usize,
    /// A failure the injected fault explains has been seen.
    pub excused: bool,
    /// The server stopped in order, and is not to be revived.
    stopped: bool,
    /// The driver has killed the server.
    killed: bool,
    /// The server could not be brought back: nothing more can be done.
    broken: bool,
}

impl Driver {
    /// Starts a server for a script in `dir` with the library loaded:
    /// counting only, or armed at a point. One killed while starting is
    /// started again without it.
    pub async fn start(
        binary: &Path,
        dir: &Path,
        library: &Path,
        armed: Option<(usize, Action)>,
        world: &World,
    ) -> Result<Self> {
        let data = dir.join("data");
        std::fs::create_dir_all(&data)?;
        // The server works with the canonical path, and the library sees it.
        let data = data.canonicalize()?;
        let log = dir.join("crashpoints.log");
        let env = interpose::env(library, &data, &log, armed);
        let mut deaths = Vec::new();
        let server = match Server::start_with(binary, &data, env).await {
            Ok(server) => server,
            Err(error) => {
                let exited = error.downcast::<Exited>()?;
                deaths.push(Death {
                    step: "server start".into(),
                    status: exited.status,
                    by_driver: false,
                });
                Server::start(binary, &data)
                    .await
                    .context("the server does not start again after it died while starting")?
            }
        };
        let mut tracker = Tracker::default();
        tracker.root(server.incarnations[0]);
        game::record_servers(dir, &server)?;
        Ok(Self {
            dir: dir.into(),
            data,
            log,
            armed,
            server,
            tracker,
            workers: world
                .named(|kind| matches!(kind, Kind::Worker(_)))
                .into_iter()
                .collect(),
            recoveries: 0,
            run: uuid::Uuid::new_v4().to_string(),
            started: false,
            step: "server start".into(),
            deaths,
            problems: Vec::new(),
            notes: Vec::new(),
            focus: 0,
            excused: false,
            stopped: false,
            killed: false,
            broken: false,
        })
    }

    /// Begins a step of the script, marking it in the library's log.
    pub fn step(&mut self, name: &str) {
        self.step = name.into();
        interpose::mark(&self.log, name);
    }

    /// Marks where the script's focus begins: the points before it are
    /// setup that other scripts cover.
    pub fn focus(&mut self) {
        self.step("focus");
        if self.armed.is_none() {
            self.focus = interpose::read(&self.log).last().map_or(0, |c| c.index);
        }
    }

    pub fn problem(&mut self, text: String) {
        self.problems.push(text);
    }

    /// The run exists: from now on a restarted server reopens it.
    pub fn started(&mut self) {
        self.started = true;
    }

    /// Sends one request. A reply lost to the server's death is noted, and
    /// the server runs again, with the run open, when this returns.
    pub async fn send(&mut self, operation: &str, args: Value) -> Reply {
        self.revive().await;
        if self.broken {
            return Reply::NotRun("the server is down for good".into());
        }
        let reply =
            match tokio::time::timeout(ANSWER, send(&self.server.client, operation, args)).await {
                Ok(reply) => reply,
                Err(_) => {
                    self.problem(format!(
                        "{operation} went unanswered for {}s (step {})",
                        ANSWER.as_secs(),
                        self.step
                    ));
                    self.kill();
                    Reply::Uncertain("no answer".into())
                }
            };
        if matches!(reply, Reply::Uncertain(_) | Reply::NotRun(_)) {
            // The process may still be on its way out.
            let deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline && self.server.exited().is_none() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            self.revive().await;
        }
        // A faulted run serves nothing until it is reopened. A change that
        // faulted it, or met it faulted, has an unknown outcome.
        let faulted = match &reply {
            Reply::Failed(_, message) => message.contains("faulted"),
            Reply::Uncertain(message) => message.contains("storage failed while this change"),
            _ => false,
        };
        if faulted && self.started {
            let what = format!("{operation} found the run's core session faulted");
            self.recover(&what, "run.resume").await;
        }
        let _ = self.tracker.observe();
        reply
    }

    /// Whether the armed call has failed, as the library was told to make
    /// it.
    fn injected(&self) -> bool {
        match self.armed {
            Some((at, action)) if !action.kills() => {
                interpose::read(&self.log).iter().any(|c| c.index == at)
            }
            _ => false,
        }
    }

    /// Whether a failed reply is the one the injected fault explains: the
    /// request that met the failing call may fail cleanly, once.
    pub fn excuse(&mut self, operation: &str, reply: &Reply) -> bool {
        if self.excused || !self.injected() {
            return false;
        }
        self.excused = true;
        self.notes
            .push(format!("{operation} failed at the injected fault: {reply}"));
        true
    }

    /// A request that changes nothing, sent again through a restart or a
    /// failure; None, and a problem, when it keeps failing.
    pub async fn read(&mut self, operation: &str, args: Value) -> Option<Value> {
        let mut last = String::new();
        for _ in 0..4 {
            let reply = self.send(operation, args.clone()).await;
            match reply {
                Reply::Done(value) => return Some(value),
                Reply::Failed(..) | Reply::Rejected(_) => {
                    self.excuse(operation, &reply);
                    last = reply.to_string();
                }
                other => last = other.to_string(),
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        self.problem(format!(
            "{operation} kept failing (step {}): {last}",
            self.step
        ));
        None
    }

    pub async fn status(&mut self) -> Option<Value> {
        self.read("flow.status", json!({"run_id": self.run})).await
    }

    /// Exports the run's history to `path` and reads it back.
    pub async fn export(&mut self, path: &Path) -> Option<Value> {
        let _ = std::fs::remove_file(path);
        self.read("inspect.export", json!({"run_id": self.run, "path": path}))
            .await?;
        let read = std::fs::read(path)
            .map_err(anyhow::Error::from)
            .and_then(|bytes| Ok(serde_json::from_slice(&bytes)?));
        match read {
            Ok(value) => Some(value),
            Err(error) => {
                self.problem(format!(
                    "the export at {} cannot be read: {error:#}",
                    path.display()
                ));
                None
            }
        }
    }

    /// The run's history now, exported outside the data directory.
    pub async fn history(&mut self) -> Option<History> {
        let path = self.dir.join("probe.json");
        let export = self.export(&path).await?;
        match History::parse(&export, &Default::default()) {
            Ok(history) => Some(history),
            Err(error) => {
                self.problem(format!("the export does not parse: {error:#}"));
                None
            }
        }
    }

    /// Waits until the status satisfies `done`; None, and a problem, when it
    /// does not in time.
    pub async fn wait_until(
        &mut self,
        what: &str,
        mut done: impl FnMut(&Value) -> bool,
    ) -> Option<Value> {
        let deadline = Instant::now() + SETTLE;
        let mut status = Value::Null;
        while Instant::now() < deadline && !self.broken {
            if let Reply::Done(now) = self.send("flow.status", json!({"run_id": self.run})).await {
                let stopped = self.stopped_workers(&now);
                if !stopped.is_empty() && self.recoveries <= RECOVERIES {
                    let what = format!("workers stopped: {}", stopped.join("; "));
                    self.recover(&what, "flow.resume").await;
                } else if done(&now) {
                    return Some(now);
                }
                status = now;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        self.problem(format!(
            "{what} did not happen within {}s (step {}); last status: tasks {}, failures {}",
            SETTLE.as_secs(),
            self.step,
            status["tasks"],
            status["failures"]
        ));
        None
    }

    /// Command nodes whose worker has stopped, with its state and error.
    fn stopped_workers(&self, status: &Value) -> Vec<String> {
        status["nodes"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|node| {
                self.workers
                    .contains(node["id"].as_str().unwrap_or_default())
            })
            .filter_map(|node| {
                let execution = &node["execution"];
                let state = execution["state"].as_str()?;
                (state != "running").then(|| {
                    format!(
                        "{} {state} ({})",
                        node["id"].as_str().unwrap_or_default(),
                        execution["error"]["message"].as_str().unwrap_or_default()
                    )
                })
            })
            .collect()
    }

    /// Makes a documented recovery (`run.resume` or `flow.resume`) of a run
    /// that stopped serving. The injected fault explains it once; anything
    /// else is a problem, as is a recovery that does not take.
    async fn recover(&mut self, what: &str, operation: &str) {
        self.recoveries += 1;
        if self.recoveries > RECOVERIES {
            if self.recoveries == RECOVERIES + 1 {
                self.problem(format!(
                    "{what}; after {RECOVERIES} recoveries the driver makes no more (step {})",
                    self.step
                ));
            }
            return;
        }
        if self.injected() {
            self.notes.push(format!(
                "{what} at the injected fault; {operation} recovers it"
            ));
        } else {
            self.problem(format!(
                "{what} (step {}), with no fault injected",
                self.step
            ));
        }
        if let Err(last) = self.resume(operation).await {
            self.problem(format!(
                "{what}, and {operation} does not recover the run (step {}): {last}",
                self.step
            ));
        }
    }

    /// Sends `run.resume` or `flow.resume`, trying three times.
    async fn resume(&mut self, operation: &str) -> std::result::Result<(), String> {
        let mut last = String::new();
        for _ in 0..3 {
            let args = json!({"run_id": self.run});
            match tokio::time::timeout(ANSWER, send(&self.server.client, operation, args)).await {
                Ok(Reply::Done(_)) => return Ok(()),
                Ok(other) => last = other.to_string(),
                Err(_) => last = "no answer".into(),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err(last)
    }

    /// Waits until nothing is left to run at command nodes and history has
    /// stopped changing; returns the last status.
    pub async fn settle(&mut self, world: &World) -> Option<Value> {
        let mut last = Value::Null;
        let mut quiet = 0;
        self.wait_until("settling", |status| {
            let busy = status["tasks"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|task| {
                    matches!(
                        world.nodes.get(task["node"].as_str().unwrap_or_default()),
                        Some(Kind::Worker(_))
                    )
                })
                || status["failures"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|failure| failure["state"] != "parked");
            quiet = if !busy && status["revision"] == last {
                quiet + 1
            } else {
                0
            };
            last = status["revision"].clone();
            quiet >= 2
        })
        .await
    }

    /// The end of every script. The run settles, its history is exported,
    /// and the server stops in order (all again, should it die meanwhile).
    /// The store is audited against the export; then the server starts once
    /// more, as a restarted server must open the run with its history as it
    /// was, and stops in order for the leftover checks. Returns the settled
    /// status and the export, for the workflow judge.
    pub async fn finish(&mut self, world: &World) -> Option<(Value, Value)> {
        if !self.started || self.broken {
            // What a server started afresh makes of the data directory.
            if !self.broken && self.died() {
                self.step("restart");
                self.runs("after the run failed to start").await;
            }
            self.step("stop");
            self.stop().await;
            self.abandon();
            return None;
        }
        let (status, export) = loop {
            self.step("settle");
            let Some(status) = self.settle(world).await else {
                self.abandon();
                return None;
            };
            self.step("export");
            let path = self.dir.join("export.json");
            let Some(export) = self.export(&path).await else {
                self.abandon();
                return None;
            };
            self.step("stop");
            match self.stop().await {
                Stop::InOrder => break (status, export),
                Stop::Died => continue,
                Stop::Failed => {
                    self.abandon();
                    return None;
                }
            }
        };
        let (stored, _) = audit::store(&self.data, &self.run, &export).await;
        self.problems
            .extend(stored.into_iter().map(|p| format!("store: {p}")));

        self.step("restart");
        self.server.set_env(Vec::new());
        if let Err(error) = self.server.restart().await {
            self.problem(format!(
                "the server does not start after its orderly stop: {error:#}"
            ));
            self.abandon();
            return Some((status, export));
        }
        self.stopped = false;
        self.tracker
            .root(*self.server.incarnations.last().expect("incarnation"));
        let _ = game::record_servers(&self.dir, &self.server);
        self.runs("after an orderly restart").await;
        match self.send("run.resume", json!({"run_id": self.run})).await {
            Reply::Done(_) => {
                let path = self.dir.join("export-restarted.json");
                if let Some(after) = self.export(&path).await {
                    self.problems.extend(game::restart_changes(&export, &after));
                }
            }
            other => self.problem(format!(
                "an orderly restart does not reopen the run: run.resume: {other}"
            )),
        }
        self.step("final stop");
        if let Stop::InOrder = self.stop().await {
            let leftovers =
                game::after_stop(&self.tracker, &self.data, &self.server, &self.run).await;
            self.problems.extend(leftovers);
            self.problems.extend(self.temporaries());
        }
        self.abandon();
        Some((status, export))
    }

    /// Kills whatever of the script's servers and programs still runs.
    fn abandon(&mut self) {
        self.kill();
        self.tracker.kill_survivors();
    }

    /// Whether the server has died at any time.
    fn died(&self) -> bool {
        !self.deaths.is_empty()
    }

    /// The runs a freshly started server lists: the script's one, and no
    /// recovery errors.
    async fn runs(&mut self, when: &str) {
        let Some(list) = self.read("run.list", json!({})).await else {
            return;
        };
        if list["recovery_errors"]
            .as_object()
            .is_some_and(|errors| !errors.is_empty())
        {
            self.problem(format!(
                "{when} the server reports recovery errors: {}",
                list["recovery_errors"]
            ));
        }
        let runs: Vec<&str> = list["runs"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|run| run["run_id"].as_str())
            .collect();
        if self.started && runs != [self.run.as_str()] {
            let text = format!(
                "{when} the data directory holds the runs {runs:?}, not just {}",
                self.run
            );
            self.problem(text);
        }
    }

    /// Half-written files outside the runs directory, which the orderly
    /// stop's own checks cover.
    fn temporaries(&self) -> Vec<String> {
        let mut found = Vec::new();
        for entry in std::fs::read_dir(&self.data)
            .into_iter()
            .flatten()
            .flatten()
        {
            let name = entry.file_name().to_string_lossy().into_owned();
            if entry.file_type().is_ok_and(|t| t.is_dir()) {
                if name != "runs" {
                    found.extend(game::temporaries(&entry.path()));
                }
            } else if name.starts_with('.') && name.ends_with(".tmp") {
                found.push(entry.path());
            }
        }
        found
            .into_iter()
            .map(|path| format!("half-written file left: {}", path.display()))
            .collect()
    }

    /// Stops the server in order. One that dies while stopping, at a crash
    /// point in its shutdown, runs again when this returns; a refusal is
    /// asked again, as one failed write may make a stop fail cleanly.
    async fn stop(&mut self) -> Stop {
        for _ in 0..3 {
            self.revive().await;
            if self.broken {
                return Stop::Failed;
            }
            let _ = self.tracker.observe();
            let reply = match tokio::time::timeout(
                ANSWER,
                send(&self.server.client, "server.stop", json!({})),
            )
            .await
            {
                Ok(reply) => reply,
                Err(_) => Reply::Uncertain("no answer".into()),
            };
            if matches!(reply, Reply::Failed(..) | Reply::Rejected(_)) {
                if !self.excuse("server.stop", &reply) {
                    self.problem(format!("server.stop failed: {reply}"));
                }
                continue;
            }
            // Done, or its reply lost: the process ends.
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                if let Some(status) = self.server.exited() {
                    if status.success() {
                        self.stopped = true;
                        return Stop::InOrder;
                    }
                    self.revive().await;
                    return if self.broken {
                        Stop::Failed
                    } else {
                        Stop::Died
                    };
                }
                if Instant::now() > deadline {
                    self.problem(format!(
                        "the server did not exit within 20 s of server.stop ({reply})"
                    ));
                    self.kill();
                    return Stop::Failed;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        self.problem("the server could not be stopped in order".into());
        self.kill();
        Stop::Failed
    }

    /// Kills the server, as one that is not answering, so what it did can
    /// be judged.
    fn kill(&mut self) {
        if self.stopped || self.server.exited().is_some() {
            return;
        }
        self.killed = true;
        if let Some(pid) = self.server.incarnations.last() {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(*pid as i32),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
    }

    /// When the server has died: notes how, starts it again without the
    /// library, and reopens the run.
    async fn revive(&mut self) {
        if self.broken || self.stopped {
            return;
        }
        let Some(status) = self.server.exited() else {
            return;
        };
        self.deaths.push(Death {
            step: self.step.clone(),
            status,
            by_driver: std::mem::take(&mut self.killed),
        });
        if self.deaths.len() > DEATHS {
            self.problem(format!(
                "the server died {} times; giving up",
                self.deaths.len()
            ));
            self.broken = true;
            return;
        }
        self.server.set_env(Vec::new());
        if let Err(error) = self.server.restart().await {
            self.problem(format!(
                "the server does not start again after it died (step {}): {error:#}",
                self.step
            ));
            self.broken = true;
            return;
        }
        self.tracker
            .root(*self.server.incarnations.last().expect("incarnation"));
        let _ = game::record_servers(&self.dir, &self.server);
        if self.started {
            self.reopen().await;
        }
    }

    /// Reopens the run on a new server; an error that a second and a third
    /// try do not clear is a problem.
    async fn reopen(&mut self) {
        if let Err(last) = self.resume("run.resume").await {
            self.problem(format!(
                "the run does not reopen after the server died (step {}): run.resume: {last}",
                self.step
            ));
        }
    }
}
