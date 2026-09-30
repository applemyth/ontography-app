//! Lifecycle gates at the public service boundary, using real core ownership.

use crate::persistence::Paths;
use crate::state::Service;
use crate::tools;
use serde_json::{Value, json};
use std::path::PathBuf;

/// An outside client feeds a command worker, the run's one execution.
fn document() -> Value {
    json!({"name":"lifecycle","entry":"feed","nodes":[
        {"id":"feed","component":"external"},
        {"id":"work","component":"command","config":{"argv":["cat"]}}
    ],"edges":[{"from":"feed","to":"work"}]})
}

async fn start(service: &Service, project: &std::path::Path) -> String {
    tools::dispatch(
        service,
        "flow.start",
        &json!({"document":document(),"project":project}),
    )
    .await
    .unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// The run's executions, as `run.inspect` reports them.
async fn executions(service: &Service, run_id: &str) -> Vec<Value> {
    tools::dispatch(service, "run.inspect", &json!({"run_id":run_id}))
        .await
        .unwrap()["executions"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

#[tokio::test]
async fn failed_suspension_preserves_checkout_and_allows_work_after_repair() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("work"), "original").unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let run_id = start(&service, directory.path()).await;
    let first = executions(&service, &run_id).await;
    assert_eq!(first.len(), 1);
    let imported = tools::dispatch(
        &service,
        "workspace.import",
        &json!({"run_id":run_id,"path":"source"}),
    )
    .await
    .unwrap();
    let checkout = tools::dispatch(
        &service,
        "workspace.checkout",
        &json!({"run_id":run_id,"root":imported["root"]}),
    )
    .await
    .unwrap();
    let checkout_id = checkout["checkout_id"].as_str().unwrap();
    let path = PathBuf::from(checkout["path"].as_str().unwrap());
    std::fs::write(path.join("work"), "edited before failed suspension").unwrap();
    let displaced = path.with_extension("displaced");
    std::fs::rename(&path, &displaced).unwrap();

    let failure = tools::dispatch(&service, "run.suspend", &json!({"run_id":run_id}))
        .await
        .unwrap_err();
    let managed = service.run(&run_id).await.unwrap();
    {
        let run = managed.lock().await;
        assert_eq!(run.summary()["status"], "suspension_failed");
        let live = run.live().unwrap();
        assert_eq!(
            live.suspension_error.as_ref().unwrap().message,
            failure.message
        );
        assert!(live.checkouts.contains_key(checkout_id));
        assert_eq!(live.checkouts[checkout_id].checkout.path(), path);
        assert!(live.executions.is_empty());
        assert!(run.manifest.checkpoints.is_empty());
    }
    assert_eq!(
        std::fs::read_to_string(displaced.join("work")).unwrap(),
        "edited before failed suspension"
    );

    // Resuming relaunches the worker through the replacement host. Merely
    // checking a flag would miss a permanently closed host after the failure.
    tools::dispatch(&service, "run.resume", &json!({"run_id":run_id}))
        .await
        .unwrap();
    let relaunched = executions(&service, &run_id).await;
    assert_eq!(relaunched.len(), 1);
    assert_eq!(relaunched[0]["status"], "running");
    assert_ne!(relaunched[0]["execution_id"], first[0]["execution_id"]);
    std::fs::rename(&displaced, &path).unwrap();
    std::fs::write(path.join("more-work"), "still editable after failure").unwrap();
    tools::dispatch(&service, "run.suspend", &json!({"run_id":run_id}))
        .await
        .unwrap();
    let run = managed.lock().await;
    assert!(run.live.is_none());
    assert_eq!(run.manifest.status, "suspended");
    assert!(run.manifest.checkpoints.contains_key(checkout_id));
    assert_ne!(
        serde_json::to_value(run.manifest.checkpoints[checkout_id].root).unwrap(),
        imported["root"]
    );
    assert!(!path.exists());
}

#[tokio::test]
async fn a_closed_run_never_starts_its_workers_again() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let run_id = start(&service, directory.path()).await;
    assert_eq!(executions(&service, &run_id).await.len(), 1);
    tools::dispatch(&service, "run.suspend", &json!({"run_id":run_id}))
        .await
        .unwrap();
    let closed = tools::dispatch(&service, "run.close", &json!({"run_id":run_id}))
        .await
        .unwrap();
    assert_eq!(closed["status"], "closed");
    let readable = tools::dispatch(&service, "run.resume", &json!({"run_id":run_id}))
        .await
        .unwrap();
    assert_eq!(readable["admission"], "closed");
    assert!(readable["executions"].as_array().unwrap().is_empty());
    service.shutdown().await.unwrap();
}
