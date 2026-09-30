//! Lifecycle gates at the public service boundary, using real core ownership.

use crate::persistence::{Paths, read_json};
use crate::state::{RunManifest, Service};
use crate::tools;
use serde_json::{Value, json};
use std::{
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    time::Duration,
};

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

#[tokio::test]
async fn opening_a_closed_run_leaves_its_manifest_closed() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let run_id = start(&service, directory.path()).await;
    tools::dispatch(&service, "run.close", &json!({"run_id":run_id}))
        .await
        .unwrap();
    let path = paths.run(&run_id).unwrap().join("manifest.json");
    let closed = std::fs::metadata(&path).unwrap().ino();
    tools::dispatch(&service, "run.resume", &json!({"run_id":run_id}))
        .await
        .unwrap();
    let manifest: RunManifest = read_json(&path).unwrap();
    assert_eq!(manifest.status, "closed");
    // Nothing changed, so nothing was written.
    assert_eq!(std::fs::metadata(&path).unwrap().ino(), closed);
    // A server stopped now leaves it closed, not recoverable.
    let restarted = Service::new(paths).unwrap();
    let listed = tools::dispatch(&restarted, "run.list", &json!({}))
        .await
        .unwrap();
    assert_eq!(listed["runs"][0]["status"], "closed");
    service.shutdown().await.unwrap();
}

/// Starts the document as the run reserved as `id`, as a retried start does.
async fn start_reserved(service: &Service, project: &Path, id: &str) -> crate::Result<Value> {
    let args = json!({"document":document(),"project":project});
    let (declaration, workflow) = crate::workflow::tools::prepare_start(service, &args, id)?;
    service
        .start_reserved(
            id,
            declaration,
            project.into(),
            workflow,
            service.environment(),
        )
        .await
}

#[tokio::test]
async fn concurrent_starts_with_one_id_share_one_run() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    // Both starts wait for the run map, then look for the id in turn.
    let held = service.runs.lock().await;
    let (first, second, ()) = tokio::join!(
        start_reserved(&service, directory.path(), &id),
        start_reserved(&service, directory.path(), &id),
        async move {
            tokio::task::yield_now().await;
            drop(held);
        }
    );
    // A retry of an unanswered start recovers the same run.
    assert_eq!(first.unwrap()["run_id"], id);
    assert_eq!(second.unwrap()["run_id"], id);
    assert_eq!(service.runs.lock().await.len(), 1);
    let stores = std::fs::read_dir(paths.root.join("runs")).unwrap().count();
    assert_eq!(stores, 1);
    assert!(paths.run(&id).unwrap().join("core").is_dir());
    assert_eq!(executions(&service, &id).await.len(), 1);
    service.shutdown().await.unwrap();
    assert!(!service.has_live_runs());
}

#[tokio::test]
async fn a_start_retry_waits_for_its_run_without_holding_the_others() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    start_reserved(&service, directory.path(), &id)
        .await
        .unwrap();
    // Another operation holds the run when a retry of its start arrives.
    let run = service.run(&id).await.unwrap();
    let busy = run.lock().await;
    let (retried, lookup) = tokio::join!(start_reserved(&service, directory.path(), &id), async {
        let lookup = tokio::time::timeout(Duration::from_secs(1), service.run(&id)).await;
        drop(busy);
        lookup
    });
    assert!(lookup.is_ok(), "the waiting retry kept the run map locked");
    assert_eq!(retried.unwrap()["run_id"], id);
    service.shutdown().await.unwrap();
}
