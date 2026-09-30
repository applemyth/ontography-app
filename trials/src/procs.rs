//! Processes. A trial tracks every process descended from each server it
//! started, by pid and start time, so a pid later reused by someone else is
//! never mistaken for a leftover. None may outlive the server's orderly stop.
//! An interrupted harness kills everything it tracked.

use anyhow::{Context, Result};
use nix::sys::signal::{Signal, kill, killpg};
use nix::unistd::Pid;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

#[derive(Clone, Debug)]
pub struct Proc {
    pub pid: i32,
    pub ppid: i32,
    /// `ps`'s `lstart`: with the pid, the process's identity.
    pub start: String,
    pub command: String,
}

impl Proc {
    fn key(&self) -> (i32, String) {
        (self.pid, self.start.clone())
    }
}

/// Every process now running.
pub fn snapshot() -> Result<Vec<Proc>> {
    let output = std::process::Command::new("/bin/ps")
        .args(["-axo", "pid=,ppid=,lstart=,command="])
        .output()
        .context("run ps")?;
    let text = String::from_utf8_lossy(&output.stdout);
    Ok(text
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let pid = fields.next()?.parse().ok()?;
            let ppid = fields.next()?.parse().ok()?;
            // lstart has five fields, such as `Wed Sep 30 01:02:03 2026`.
            let start = fields.by_ref().take(5).collect::<Vec<_>>().join(" ");
            let command = fields.collect::<Vec<_>>().join(" ");
            Some(Proc {
                pid,
                ppid,
                start,
                command,
            })
        })
        .collect())
}

/// Server process groups and tracked processes, for cleanup on interrupt.
static SERVERS: Mutex<BTreeSet<u32>> = Mutex::new(BTreeSet::new());
static TRACKED: Mutex<BTreeMap<(i32, String), ()>> = Mutex::new(BTreeMap::new());

pub fn register(server: u32) {
    SERVERS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(server);
}

/// Kills every server this harness started and every process it tracked
/// that is still the same process.
pub fn cleanup() {
    for server in SERVERS.lock().unwrap_or_else(|p| p.into_inner()).iter() {
        let _ = killpg(Pid::from_raw(*server as i32), Signal::SIGKILL);
    }
    let tracked = TRACKED.lock().unwrap_or_else(|p| p.into_inner()).clone();
    if let Ok(now) = snapshot() {
        for process in now {
            if tracked.contains_key(&process.key()) {
                let _ = kill(Pid::from_raw(process.pid), Signal::SIGKILL);
            }
        }
    }
}

/// The processes descended from one trial's servers.
#[derive(Default)]
pub struct Tracker {
    roots: BTreeSet<i32>,
    seen: BTreeMap<(i32, String), Proc>,
}

impl Tracker {
    pub fn root(&mut self, pid: u32) {
        self.roots.insert(pid as i32);
    }

    /// Adds every current descendant of a root or of a tracked process.
    pub fn observe(&mut self) -> Result<()> {
        let now = snapshot()?;
        let mut live: BTreeSet<i32> = now
            .iter()
            .filter(|p| self.roots.contains(&p.pid) || self.seen.contains_key(&p.key()))
            .map(|p| p.pid)
            .collect();
        loop {
            let before = live.len();
            for process in &now {
                if live.contains(&process.ppid) {
                    live.insert(process.pid);
                }
            }
            if live.len() == before {
                break;
            }
        }
        let mut tracked = TRACKED.lock().unwrap_or_else(|p| p.into_inner());
        for process in now.into_iter().filter(|p| live.contains(&p.pid)) {
            tracked.insert(process.key(), ());
            self.seen.insert(process.key(), process);
        }
        Ok(())
    }

    /// Tracked processes that are still running.
    pub fn survivors(&self) -> Result<Vec<Proc>> {
        Ok(snapshot()?
            .into_iter()
            .filter(|p| self.seen.contains_key(&p.key()))
            .collect())
    }

    pub fn kill_survivors(&self) {
        if let Ok(survivors) = self.survivors() {
            for process in survivors {
                let _ = kill(Pid::from_raw(process.pid), Signal::SIGKILL);
            }
        }
    }

    pub fn count(&self) -> usize {
        self.seen.len()
    }
}
