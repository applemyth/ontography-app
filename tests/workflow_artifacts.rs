use ontography_app::{persistence::Paths, state::Service, tools};
use serde_json::{Value, json};
use std::{os::unix::fs::PermissionsExt, path::Path};

async fn call(service: &Service, operation: &str, args: Value) -> Value {
    tools::dispatch(service, operation, &args)
        .await
        .unwrap_or_else(|error| panic!("{operation}: {error}"))
}

#[tokio::test]
async fn captured_workspace_survives_restart_and_becomes_the_human_result() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("draft.txt"), "draft").unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let document = json!({"name":"workspace-review","entry":"review",
        "nodes":[{"id":"review","component":"human","config":{"prompt":"Review the files"}},{"id":"result","component":"inbox"}],
        "edges":[{"from":"review","to":"result"}]});
    let started = call(
        &service,
        "flow.start",
        json!({"document":document,"project":directory.path(),"workspace":source}),
    )
    .await;
    let run_id = started["run_id"].as_str().unwrap().to_owned();
    let opened = call(
        &service,
        "flow.workspace",
        json!({"run_id":run_id,"action":"open","node":"review"}),
    )
    .await;
    assert!(opened.get("root").is_none());
    let workspace_id = opened["workspace_id"].as_str().unwrap().to_owned();
    let checkout = Path::new(opened["path"].as_str().unwrap());
    std::fs::write(checkout.join("draft.txt"), "reviewed").unwrap();
    let captured = call(
        &service,
        "flow.workspace",
        json!({"run_id":run_id,"action":"capture","workspace_id":workspace_id}),
    )
    .await;
    assert_eq!(captured["captured"], true);
    assert!(captured.get("root").is_none());
    call(
        &service,
        "flow.workspace",
        json!({"run_id":run_id,"action":"release","workspace_id":workspace_id}),
    )
    .await;
    assert!(!checkout.exists());
    service.shutdown().await.unwrap();
    drop(service);

    let service = Service::new(paths).unwrap();
    let status = call(&service, "flow.resume", json!({"run_id":run_id})).await;
    let restored = call(
        &service,
        "flow.workspace",
        json!({"run_id":run_id,"action":"open","workspace_id":workspace_id}),
    )
    .await;
    assert_eq!(
        std::fs::read_to_string(Path::new(restored["path"].as_str().unwrap()).join("draft.txt"))
            .unwrap(),
        "reviewed"
    );
    call(
        &service,
        "flow.workspace",
        json!({"run_id":run_id,"action":"release","workspace_id":restored["workspace_id"]}),
    )
    .await;
    let task = status["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|task| task["node"] == "review")
        .unwrap();
    call(&service, "flow.decide", json!({"run_id":run_id,"node":"review","task_id":task["task_id"],"workspace_id":workspace_id})).await;
    let destination = directory.path().join("exported");
    let exported = call(
        &service,
        "flow.export",
        json!({"run_id":run_id,"node":"result","path":destination}),
    )
    .await;
    assert_eq!(exported["kind"], "workspace");
    assert_eq!(
        std::fs::metadata(&destination)
            .unwrap()
            .permissions()
            .mode()
            & 0o7777,
        0o755
    );
    assert_eq!(
        std::fs::metadata(destination.join("draft.txt"))
            .unwrap()
            .permissions()
            .mode()
            & 0o7777,
        0o644
    );
    assert_eq!(
        std::fs::read_to_string(destination.join("draft.txt")).unwrap(),
        "reviewed"
    );
    assert_eq!(
        std::fs::read_to_string(source.join("draft.txt")).unwrap(),
        "draft"
    );
    let conflict = tools::dispatch(
        &service,
        "flow.export",
        &json!({"run_id":run_id,"node":"result","path":destination}),
    )
    .await
    .unwrap_err();
    assert_eq!(conflict.code, "destination_exists");
    assert_eq!(
        std::fs::read_to_string(destination.join("draft.txt")).unwrap(),
        "reviewed"
    );
    service.shutdown().await.unwrap();
}
