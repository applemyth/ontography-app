//! Lifecycle gates at the public service boundary, using real core ownership.

use crate::declarations::GraphDeclaration;
use crate::persistence::Paths;
use crate::registry::{ExecutionBinding, ImplementationDescriptor, ImplementationRegistry};
use crate::state::Service;
use crate::tools;
use ontography::ExecutionContext;
use serde_json::json;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Default)]
struct WorkObserved {
    starts: AtomicUsize,
    stops: AtomicUsize,
    started: Notify,
}

fn registry(observed: &Arc<WorkObserved>) -> Arc<ImplementationRegistry> {
    let mut registry = ImplementationRegistry::default();
    let observed = observed.clone();
    registry
        .register_executable(
            ImplementationDescriptor {
                id: "test.lifecycle".into(),
                version: "1".into(),
                description: "Test-only cooperative execution with observed starts".into(),
                configuration_schema: json!({"type":"object","additionalProperties":false}),
            },
            move |_| {
                let observed = observed.clone();
                Ok(Arc::new(move |context: ExecutionContext| {
                    let observed = observed.clone();
                    async move {
                        observed.starts.fetch_add(1, Ordering::SeqCst);
                        observed.started.notify_one();
                        context.stop().requested().await;
                        observed.stops.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                }))
            },
        )
        .unwrap();
    Arc::new(registry)
}

fn declaration() -> GraphDeclaration {
    let mut declaration = GraphDeclaration::parse(include_str!("../examples/flow.json")).unwrap();
    declaration.execution_bindings.push(ExecutionBinding {
        id: "worker".into(),
        node_id: "A".into(),
        implementation: "test.lifecycle".into(),
        version: "1".into(),
        configuration: json!({}),
    });
    declaration
}

async fn started(observed: &WorkObserved) {
    tokio::time::timeout(Duration::from_secs(2), observed.started.notified())
        .await
        .unwrap();
}

#[tokio::test]
async fn failed_suspension_preserves_checkout_and_allows_explicit_work_after_repair() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("work"), "original").unwrap();
    let observed = Arc::new(WorkObserved::default());
    let service = Service::with_registry(
        Paths::initialize(directory.path().join("data")).unwrap(),
        registry(&observed),
    )
    .unwrap();
    let run_id = service
        .start(declaration(), directory.path().to_owned())
        .await
        .unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    started(&observed).await;
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
    assert_eq!(observed.starts.load(Ordering::SeqCst), 1);
    assert_eq!(observed.stops.load(Ordering::SeqCst), 1);
    assert_eq!(
        std::fs::read_to_string(displaced.join("work")).unwrap(),
        "edited before failed suspension"
    );

    // This is a real explicit launch through the replacement host. Merely
    // checking a flag would miss a permanently closed host after the failure.
    tools::dispatch(&service,"execution.launch",&json!({"run_id":run_id,"node_id":"A","implementation":"test.lifecycle","version":"1","configuration":{}})).await.unwrap();
    started(&observed).await;
    assert_eq!(observed.starts.load(Ordering::SeqCst), 2);
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
    assert_eq!(observed.stops.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn closing_a_suspended_declared_binding_never_starts_it_again() {
    let directory = tempfile::tempdir().unwrap();
    let observed = Arc::new(WorkObserved::default());
    let service = Service::with_registry(
        Paths::initialize(directory.path().join("data")).unwrap(),
        registry(&observed),
    )
    .unwrap();
    let run_id = service
        .start(declaration(), directory.path().to_owned())
        .await
        .unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    started(&observed).await;
    tools::dispatch(&service, "run.suspend", &json!({"run_id":run_id}))
        .await
        .unwrap();
    assert_eq!(observed.starts.load(Ordering::SeqCst), 1);
    assert_eq!(observed.stops.load(Ordering::SeqCst), 1);

    let closed = tools::dispatch(&service, "run.close", &json!({"run_id":run_id}))
        .await
        .unwrap();
    assert_eq!(closed["status"], "closed");
    assert_eq!(observed.starts.load(Ordering::SeqCst), 1);
    assert_eq!(observed.stops.load(Ordering::SeqCst), 1);
    let readable = tools::dispatch(&service, "run.resume", &json!({"run_id":run_id}))
        .await
        .unwrap();
    assert_eq!(readable["admission"], "closed");
    assert_eq!(observed.starts.load(Ordering::SeqCst), 1);
    assert!(readable["executions"].as_array().unwrap().is_empty());
    service.shutdown().await.unwrap();
}
