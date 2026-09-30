//! A node whose previous worker is still exiting waits for it, visibly, and
//! starts its replacement once it has gone, without a manual resume.

use ontography_app::{persistence::Paths, state::Service, tools};
use serde_json::{Value, json};
use std::{io::BufRead, path::Path, process::Stdio, time::Duration};

fn document() -> Value {
    json!({"name":"recovery", "entry":"worker", "nodes":[
        {"id":"worker", "component":"agent", "config":{"prompt":"Work", "argv":["/bin/cat"]}},
        {"id":"archive", "component":"inbox"}
    ], "edges":[{"from":"worker", "to":"archive"}]})
}

async fn call(service: &Service, operation: &str, args: Value) -> Value {
    tools::dispatch(service, operation, &args)
        .await
        .unwrap_or_else(|error| panic!("{operation}: {error}; {:?}", error.details))
}

/// The worker's session once `condition` holds for it.
async fn session(service: &Service, run: &str, condition: impl Fn(&Value) -> bool) -> Value {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let status = call(service, "flow.status", json!({"run_id":run})).await;
            let session = status["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|node| node["id"] == "worker")
                .unwrap()["session"]
                .clone();
            if condition(&session) {
                return session;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the worker did not reach the expected state")
}

/// A process group whose leader is gone while a member runs on, as a crashed
/// server's worker may leave. Removing `hold` ends the member.
fn orphaned_group(hold: &Path) -> i32 {
    std::fs::write(hold, b"").unwrap();
    let mut leader = std::process::Command::new("perl")
        .args([
            "-e",
            r#"setpgrp(0, 0); my $pid = fork() // die; if ($pid) { print "$pid\n"; exit 0 } close STDOUT; close STDERR; select(undef, undef, undef, 0.05) while -e $ARGV[0]"#,
        ])
        .arg(hold)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut member = String::new();
    std::io::BufReader::new(leader.stdout.take().unwrap())
        .read_line(&mut member)
        .unwrap();
    let group = leader.id() as i32;
    assert!(leader.wait().unwrap().success());
    group
}

#[tokio::test]
async fn a_node_waits_for_its_previous_worker_and_then_starts_by_itself() {
    let temporary = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(temporary.path().join("data")).unwrap()).unwrap();
    let started = call(
        &service,
        "flow.start",
        json!({"document":document(),"project":temporary.path(),"message":"begin"}),
    )
    .await;
    let run = started["run_id"].as_str().unwrap().to_owned();
    let running = |session: &Value| session["state"] == "running";
    let directory = session(&service, &run, running).await["directory"]
        .as_str()
        .unwrap()
        .to_owned();
    call(&service, "run.suspend", json!({"run_id":run})).await;
    // The node's lease names a group whose supervisor is gone but whose
    // member runs on: not provably the node's, so never signalled.
    let hold = temporary.path().join("hold");
    let group = orphaned_group(&hold);
    std::fs::write(
        Path::new(&directory).join("worker-process.json"),
        serde_json::to_vec(
            &json!({"pid":group,"token":uuid::Uuid::new_v4().to_string(),"identity":"unproven"}),
        )
        .unwrap(),
    )
    .unwrap();
    call(&service, "run.resume", json!({"run_id":run})).await;
    let waiting = session(&service, &run, |session| session["state"] == "waiting").await;
    assert_eq!(waiting["error"]["code"], "worker_still_exiting");
    // Once it goes, the replacement starts with no manual resume.
    std::fs::remove_file(&hold).unwrap();
    let resumed = session(&service, &run, running).await;
    assert!(resumed["error"].is_null());
    service.shutdown().await.unwrap();
}
