#![cfg(unix)]

use nix::{
    errno::Errno,
    sys::signal::kill,
    unistd::{Pid, getpgid},
};
use ontography_app::{client::Client, environment::Environment, persistence::Paths};
use serde_json::{Value, json};
use std::{os::unix::fs::PermissionsExt, path::PathBuf, process::Stdio, time::Duration};

const BIN: &str = env!("CARGO_BIN_EXE_ontography");

/// Stands in for Pi. It and its children run only while the hold file exists,
/// so none outlives the test, even one that fails.
const PI: &str = r#"#!/bin/sh
if [ "$1" = --version ]; then echo 0.85.1; exit; fi
base=$(dirname "$0")
perl -e 'select(undef, undef, undef, 0.05) while -e $ARGV[0]' "$base/hold" &
echo $! > "$base/job.pid"
perl -e '$SIG{HUP} = "IGNORE"; select(undef, undef, undef, 0.05) while -e $ARGV[0]' "$base/hold" &
echo $! > "$base/job-ignoring-hangup.pid"
# Pi runs tool commands in sessions of their own and stops them when asked to
# terminate.
perl -e 'use POSIX; POSIX::setsid() or die; select(undef, undef, undef, 0.05) while -e $ARGV[0]' "$base/hold" &
echo $! > "$base/tool.pid"
# Asked for, a child in the shell's OS session but a process group of its own,
# which is neither the shell's job nor in Pi's group.
if [ -e "$base/own-group" ]; then
  perl -e 'setpgrp(0, 0); select(undef, undef, undef, 0.05) while -e $ARGV[0]' "$base/hold" &
  echo $! > "$base/own-group.pid"
fi
trap 'kill -KILL -- -$(cat "$base/tool.pid"); exit 143' TERM HUP
printf ready > "$base/ready"
while [ -e "$base/hold" ]; do sleep 0.05; done
"#;

/// Removes the endpoint directory a killed server leaves behind.
struct Endpoint(PathBuf);

impl Drop for Endpoint {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A server this test started, and one session whose Pi holds its lease.
/// Fields drop in order: the server, its endpoint, then the hold file.
struct Fixture {
    /// The test's own child, so its PID is safe to signal.
    server: tokio::process::Child,
    session: Client,
    processes: Vec<(&'static str, Pid)>,
    paths: Paths,
    binary: PathBuf,
    _endpoint: Endpoint,
    directory: tempfile::TempDir,
}

/// Starts a server on `paths` and connects to it.
async fn serve(binary: &std::path::Path, paths: &Paths) -> (tokio::process::Child, Client) {
    let server = tokio::process::Command::new(binary)
        .arg("--data-dir")
        .arg(&paths.root)
        .args(["server", "run", "--detach"])
        .env_clear()
        .envs(Environment::current().for_server())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let client = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(client) = Client::connect(&paths.socket).await {
                break client;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("server must start")
    .with_environment(Environment::current());
    (server, client)
}

impl Fixture {
    async fn start() -> Self {
        Self::start_with(false).await
    }

    /// With `own_group`, Pi also starts a child in a process group of its own.
    async fn start_with(own_group: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path();
        let paths = Paths::initialize(base.join("store")).unwrap();
        let endpoint = Endpoint(paths.socket.parent().unwrap().into());
        let binary = base.join("ontography-test");
        std::fs::copy(BIN, &binary).unwrap();
        std::fs::write(base.join("hold"), b"").unwrap();
        if own_group {
            std::fs::write(base.join("own-group"), b"").unwrap();
        }
        let pi = base.join("pi");
        std::fs::write(&pi, PI).unwrap();
        std::fs::set_permissions(&pi, std::fs::Permissions::from_mode(0o700)).unwrap();
        let (server, client) = serve(&binary, &paths).await;
        let created = client
            .call("session.create", json!({"project":base}))
            .await
            .unwrap();
        let session = client.for_session(created["session_id"].as_str().unwrap());
        session
            .call("terminal.ensure", json!({"pi":pi}))
            .await
            .unwrap();
        let status: Value = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let status = session.call("terminal.status", json!({})).await.unwrap();
                if status["manager_mode"] == "pi" && base.join("ready").exists() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("Pi must start under its lease");
        let pid = |value: &Value| Pid::from_raw(value.as_i64().unwrap() as i32);
        let recorded = |name: &str| {
            let text = std::fs::read_to_string(base.join(name)).unwrap();
            Pid::from_raw(text.trim().parse().unwrap())
        };
        let pi = pid(&status["manager_pid"]);
        let processes = vec![
            ("shell", pid(&status["pid"])),
            ("launcher", getpgid(Some(pi)).unwrap()),
            ("Pi", pi),
            ("job", recorded("job.pid")),
            ("job ignoring hangup", recorded("job-ignoring-hangup.pid")),
            ("tool command", recorded("tool.pid")),
        ];
        Self {
            server,
            session,
            processes,
            paths,
            binary,
            _endpoint: endpoint,
            directory,
        }
    }

    /// Waits until the session's shell and everything under it has ended.
    async fn ended(&self, reason: &str) {
        for (name, pid) in &self.processes {
            tokio::time::timeout(Duration::from_secs(10), async {
                while kill(*pid, None) != Err(Errno::ESRCH) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("the {name} outlived {reason}"));
        }
    }
}

#[tokio::test]
async fn killed_server_leaves_nothing_of_its_session_running() {
    let mut fixture = Fixture::start().await;
    fixture.server.start_kill().unwrap();
    fixture.server.wait().await.unwrap();
    fixture.ended("its server").await;
}

#[tokio::test]
async fn suspending_a_session_stops_everything_pi_started() {
    let fixture = Fixture::start().await;
    fixture
        .session
        .call("session.suspend", json!({}))
        .await
        .unwrap();
    fixture.ended("its suspended session").await;
}

#[tokio::test]
async fn the_next_server_reclaims_what_a_crash_left_of_a_session() {
    let mut fixture = Fixture::start_with(true).await;
    let base = fixture.directory.path().to_owned();
    let child = Pid::from_raw(
        std::fs::read_to_string(base.join("own-group.pid"))
            .unwrap()
            .trim()
            .parse()
            .unwrap(),
    );
    fixture.server.start_kill().unwrap();
    fixture.server.wait().await.unwrap();
    fixture.ended("its server").await;
    // Neither the shell's job nor in Pi's group, it survives the crash.
    assert!(kill(child, None).is_ok());
    let shells = |extension: &str| {
        let sessions = std::fs::read_dir(fixture.paths.root.join("sessions")).unwrap();
        sessions
            .flatten()
            .flat_map(|session| {
                std::fs::read_dir(session.path().join("pi/shell"))
                    .into_iter()
                    .flatten()
            })
            .flatten()
            .filter(|entry| entry.path().extension().is_some_and(|e| e == extension))
            .count()
    };
    assert_eq!((shells("bashrc"), shells("shell")), (1, 1));
    let (mut server, _client) = serve(&fixture.binary, &fixture.paths).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while kill(child, None) != Err(Errno::ESRCH) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the next server must end what the crash left of the session");
    assert_eq!((shells("bashrc"), shells("shell")), (0, 0));
    server.start_kill().unwrap();
    server.wait().await.unwrap();
}
