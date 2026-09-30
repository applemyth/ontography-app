use ontography_app::{persistence::Paths, state::Service, tools, workflow::runtime};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

fn document(argv: Value) -> Value {
    json!({"name":"endpoints", "entry":"worker", "nodes":[
        {"id":"worker", "component":"agent", "config":{"prompt":"Work", "argv":argv}},
        {"id":"archive", "component":"inbox"}
    ], "edges":[{"from":"worker", "to":"archive"}]})
}

async fn call(service: &Service, operation: &str, args: Value) -> Value {
    tools::dispatch(service, operation, &args)
        .await
        .unwrap_or_else(|error| panic!("{operation}: {error}; {:?}", error.details))
}

async fn start(service: &Service, project: &Path, argv: Value) -> String {
    call(
        service,
        "flow.start",
        json!({"document":document(argv),"project":project,"message":"begin"}),
    )
    .await["run_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// The worker's status once `condition` holds for it.
async fn worker(service: &Service, run: &str, condition: impl Fn(&Value) -> bool) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = call(service, "flow.status", json!({"run_id":run})).await;
            let worker = status["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|node| node["id"] == "worker")
                .unwrap();
            if condition(worker) {
                return worker.clone();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the worker did not reach the expected state")
}

/// The directory of the worker's sockets, where its terminal listens.
fn endpoint(worker: &Value) -> PathBuf {
    Path::new(worker["session"]["terminal"]["socket"].as_str().unwrap())
        .parent()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn a_node_execution_removes_its_endpoint_directory_however_it_ends() {
    let temporary = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(temporary.path().join("data")).unwrap()).unwrap();
    let running = |worker: &Value| worker["session"]["state"] == "running";

    // Suspending the run stops the node.
    let suspended = start(&service, temporary.path(), json!(["/bin/cat"])).await;
    let directory = endpoint(&worker(&service, &suspended, running).await);
    assert!(directory.is_dir());
    call(&service, "run.suspend", json!({"run_id":suspended})).await;
    assert!(!directory.exists());

    // Core aborts the execution without its orderly teardown.
    let aborted = start(&service, temporary.path(), json!(["/bin/cat"])).await;
    let directory = endpoint(&worker(&service, &aborted, running).await);
    let execution = {
        let run = service.run(&aborted).await.unwrap();
        let run = run.lock().await;
        let id = &runtime::load(&run).unwrap().identities.nodes["worker"];
        let live = run.live().unwrap();
        live.executions[&live.workers[id].execution_id].clone()
    };
    execution.abort();
    execution.wait().await;
    assert!(!directory.exists());

    // The program ends by itself, successfully or not.
    for (argv, state) in [
        (json!(["/usr/bin/true"]), "exited"),
        (json!(["/bin/sh", "-c", "exit 7"]), "failed"),
    ] {
        let run = start(&service, temporary.path(), argv).await;
        let ended = |worker: &Value| worker["execution"]["state"] == state;
        let directory = endpoint(&worker(&service, &run, ended).await);
        assert!(!directory.exists(), "{state}");
    }
    service.shutdown().await.unwrap();
}
