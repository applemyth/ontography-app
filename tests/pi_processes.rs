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
    processes: Vec<(&'static str, Pid)>,
    _endpoint: Endpoint,
    _directory: tempfile::TempDir,
}

impl Fixture {
    async fn start() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path();
        let paths = Paths::initialize(base.join("store")).unwrap();
        let endpoint = Endpoint(paths.socket.parent().unwrap().into());
        let binary = base.join("ontography-test");
        std::fs::copy(BIN, &binary).unwrap();
        std::fs::write(base.join("hold"), b"").unwrap();
        let pi = base.join("pi");
        std::fs::write(&pi, PI).unwrap();
        std::fs::set_permissions(&pi, std::fs::Permissions::from_mode(0o700)).unwrap();
        let server = tokio::process::Command::new(&binary)
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
            processes,
            _endpoint: endpoint,
            _directory: directory,
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
