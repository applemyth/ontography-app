use ontography_app::{persistence::Paths, state::Service, tools};
use serde_json::{Value, json};

fn declaration() -> Value {
    serde_json::from_str(include_str!("../examples/flow.json")).unwrap()
}

#[tokio::test]
async fn malformed_partial_and_missing_manifests_do_not_block_healthy_runs() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let started = tools::dispatch(
        &service,
        "run.start",
        &json!({"declaration":declaration(),"project":directory.path()}),
    )
    .await
    .unwrap();
    let run_id = started["run_id"].as_str().unwrap();
    tools::dispatch(&service,"workflow.submit",&json!({"run_id":run_id,"trigger":{"kind":"root","node_id":"A","authority":["work"]},"result":"durable","emissions":[{"edge_id":"A_to_B","payload":"healthy"}]})).await.unwrap();
    let before = tools::dispatch(&service, "run.inspect", &json!({"run_id":run_id}))
        .await
        .unwrap();
    service.shutdown().await.unwrap();
    drop(service);

    let malformed_id = uuid::Uuid::new_v4().to_string();
    let partial_id = uuid::Uuid::new_v4().to_string();
    let missing_id = uuid::Uuid::new_v4().to_string();
    let mismatched_id = uuid::Uuid::new_v4().to_string();
    for id in [&malformed_id, &partial_id, &missing_id, &mismatched_id] {
        std::fs::create_dir(paths.run(id).unwrap()).unwrap();
    }
    std::fs::write(
        paths.run(&malformed_id).unwrap().join("manifest.json"),
        b"{\"version\":",
    )
    .unwrap();
    std::fs::write(
        paths.run(&partial_id).unwrap().join("manifest.json"),
        serde_json::to_vec(&json!({"version":1,"run_id":partial_id})).unwrap(),
    )
    .unwrap();
    // A valid manifest under the wrong directory must not shadow the healthy run.
    std::fs::copy(
        paths.run(run_id).unwrap().join("manifest.json"),
        paths.run(&mismatched_id).unwrap().join("manifest.json"),
    )
    .unwrap();

    let recovered = Service::new(paths.clone()).unwrap();
    let status = tools::dispatch(&recovered, "system.status", &json!({}))
        .await
        .unwrap();
    let errors = status["recovery_errors"].as_object().unwrap();
    assert_eq!(errors.len(), 4);
    for id in [&malformed_id, &partial_id, &missing_id, &mismatched_id] {
        assert!(errors.contains_key(id), "missing recovery error for {id}");
    }
    assert_eq!(errors[&mismatched_id]["code"], "invalid_run");
    let listed = tools::dispatch(&recovered, "run.list", &json!({}))
        .await
        .unwrap();
    assert_eq!(listed["runs"].as_array().unwrap().len(), 1);
    assert_eq!(listed["runs"][0]["run_id"], run_id);
    let resumed = tools::dispatch(&recovered, "run.resume", &json!({"run_id":run_id}))
        .await
        .unwrap();
    assert_eq!(resumed["revision"], before["revision"]);
    assert_eq!(resumed["frontier"], before["frontier"]);
    let additional = tools::dispatch(
        &recovered,
        "run.start",
        &json!({"declaration":declaration(),"project":directory.path()}),
    )
    .await
    .unwrap();
    assert_ne!(additional["run_id"], run_id);
    assert_eq!(additional["admission"], "open");
    recovered.shutdown().await.unwrap();
    let _ = std::fs::remove_dir(paths.socket.parent().unwrap());
}
