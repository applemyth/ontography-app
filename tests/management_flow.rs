use ontography_app::{persistence::Paths, state::Service, tools};
use serde_json::{Value, json};

fn declaration() -> Value {
    serde_json::from_str(include_str!("../examples/flow.json")).unwrap()
}

async fn call(service: &Service, operation: &str, args: Value) -> Value {
    tools::dispatch(service, operation, &args)
        .await
        .unwrap_or_else(|error| panic!("{operation} failed: {error}"))
}

async fn start(service: &Service, project: &std::path::Path) -> String {
    call(
        service,
        "run.start",
        json!({"declaration":declaration(),"project":project}),
    )
    .await["run_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn root(run_id: &str, payload: Value) -> Value {
    json!({"run_id":run_id,"trigger":{"kind":"root","node_id":"A","authority":["work"]},
        "result":"sent","emissions":[{"edge_id":"A_to_B","payload":payload}]})
}

fn rewrite_request(run_id: &str) -> Value {
    json!({"run_id":run_id,"request":{"production_id":"remove_receiver",
        "nodes":{"A":"A","B":"B"},"edges":{"A_to_B":"A_to_B"}}})
}

#[tokio::test]
async fn forbidden_root_authority_preserves_revision_and_frontier() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let mut definition = declaration();
    // Both tags are valid vocabulary. Only `work` is granted by A's root rule,
    // so rejection must come from authority governance, not a malformed tag.
    definition["schema"]["authority_tags"] = json!(["work", "admin"]);
    assert_eq!(definition["roots"][0]["ceiling"], json!(["work"]));
    assert_eq!(
        call(
            &service,
            "graph.validate",
            json!({"declaration":definition})
        )
        .await["valid"],
        true
    );
    let run_id = call(
        &service,
        "run.start",
        json!({"declaration":definition,"project":directory.path()}),
    )
    .await["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    call(
        &service,
        "workflow.submit",
        root(&run_id, json!("existing work")),
    )
    .await;
    let before = call(&service, "run.inspect", json!({"run_id":run_id})).await;
    assert_eq!(before["revision"], "1");
    assert_eq!(before["frontier"]["received"].as_array().unwrap().len(), 1);

    let mut forbidden = root(&run_id, json!("otherwise valid text"));
    forbidden["trigger"]["authority"] = json!(["work", "admin"]);
    let rejected = tools::dispatch(&service, "workflow.submit", &forbidden)
        .await
        .unwrap_err();
    assert_eq!(rejected.code, "rejected");
    assert_eq!(
        rejected.message,
        "root authority at node A exceeds its ceiling"
    );
    let after = call(&service, "run.inspect", json!({"run_id":run_id})).await;
    assert_eq!(after["revision"], before["revision"]);
    assert_eq!(after["frontier"], before["frontier"]);

    // Changing only the requested authority makes this proposal admissible.
    forbidden["trigger"]["authority"] = json!(["work"]);
    call(&service, "workflow.submit", forbidden).await;
    let accepted = call(&service, "run.inspect", json!({"run_id":run_id})).await;
    assert_eq!(accepted["revision"], "2");
    assert_eq!(
        accepted["frontier"]["received"].as_array().unwrap().len(),
        2
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn admitted_workflow_survives_suspension_and_closure_remains_terminal() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let mut invalid = declaration();
    invalid["edges"][0]["target"] = json!("missing");
    assert!(
        tools::dispatch(
            &service,
            "run.start",
            &json!({"declaration":invalid,"project":directory.path()})
        )
        .await
        .is_err()
    );
    assert!(
        call(&service, "run.list", json!({})).await["runs"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    let saved = call(&service, "graph.save", json!({"declaration":declaration()})).await;
    assert_eq!(saved["validated"], false);
    let validated = call(
        &service,
        "graph.validate",
        json!({"declaration":declaration()}),
    )
    .await;
    assert_eq!(validated["valid"], true);
    let run_id = start(&service, directory.path()).await;
    let before = call(&service, "run.inspect", json!({"run_id":run_id})).await;
    let rejected = tools::dispatch(&service, "workflow.submit", &root(&run_id, json!([255])))
        .await
        .unwrap_err();
    assert_eq!(rejected.code, "rejected");
    assert_eq!(
        call(&service, "run.inspect", json!({"run_id":run_id})).await["revision"],
        before["revision"]
    );

    let accepted = call(&service, "workflow.submit", root(&run_id, json!("hello"))).await;
    let frontier = call(
        &service,
        "inspect.frontier",
        json!({"run_id":run_id,"node_id":"B"}),
    )
    .await;
    let package_id = frontier["packages"][0]["package_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        frontier["packages"][0]["producer"],
        accepted["activation_id"]
    );
    call(&service, "run.suspend", json!({"run_id":run_id})).await;
    drop(service);

    let service = Service::new(paths).unwrap();
    let resumed = call(&service, "run.resume", json!({"run_id":run_id})).await;
    assert_eq!(resumed["admission"], "open");
    assert_eq!(resumed["frontier"]["received"][0]["package_id"], package_id);
    call(&service, "workflow.submit", json!({"run_id":run_id,"trigger":{"kind":"packages","package_ids":[package_id]},"result":"received"})).await;
    assert!(
        call(&service, "inspect.frontier", json!({"run_id":run_id})).await["packages"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        call(
            &service,
            "inspect.package",
            json!({"run_id":run_id,"package_id":package_id})
        )
        .await["package"]["producer"],
        accepted["activation_id"]
    );

    call(&service, "run.close", json!({"run_id":run_id})).await;
    let closed = call(&service, "run.resume", json!({"run_id":run_id})).await;
    assert_eq!(closed["admission"], "closed");
    assert!(
        tools::dispatch(
            &service,
            "workflow.submit",
            &root(&run_id, json!("too late"))
        )
        .await
        .is_err()
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn stale_rewrites_reject_and_committed_topology_survives_reopening() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let saved = call(&service, "graph.save", json!({"declaration":declaration()})).await;
    let run_id = start(&service, directory.path()).await;
    call(&service, "workflow.submit", root(&run_id, json!("first"))).await;
    let before = call(&service, "run.inspect", json!({"run_id":run_id})).await;
    let stale = call(&service, "rewrite.prepare", rewrite_request(&run_id)).await;
    assert_eq!(stale["retirements"].as_array().unwrap().len(), 1);
    assert_eq!(
        call(&service, "run.inspect", json!({"run_id":run_id})).await["revision"],
        before["revision"]
    );
    call(&service, "workflow.submit", root(&run_id, json!("second"))).await;
    let rejection = tools::dispatch(
        &service,
        "rewrite.commit",
        &json!({"run_id":run_id,"plan_id":stale["plan_id"]}),
    )
    .await
    .unwrap_err();
    assert_eq!(rejection.code, "stale");

    let fresh = call(&service, "rewrite.prepare", rewrite_request(&run_id)).await;
    assert_eq!(fresh["retirements"].as_array().unwrap().len(), 2);
    call(
        &service,
        "rewrite.commit",
        json!({"run_id":run_id,"plan_id":fresh["plan_id"]}),
    )
    .await;
    let changed = call(&service, "run.inspect", json!({"run_id":run_id})).await;
    assert_eq!(changed["graph"]["nodes"], json!([{"id":"A"}]));
    assert_eq!(changed["graph"]["edges"], json!([]));
    assert!(
        changed["frontier"]["received"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        call(&service, "graph.get", json!({"revision":saved["revision"]})).await["nodes"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    service.shutdown().await.unwrap();
    drop(service);

    let service = Service::new(paths).unwrap();
    let reopened = call(&service, "run.resume", json!({"run_id":run_id})).await;
    assert_eq!(reopened["graph"], changed["graph"]);
    assert_eq!(reopened["revision"], changed["revision"]);
    for retired in fresh["retirements"].as_array().unwrap() {
        let history = call(
            &service,
            "inspect.package",
            json!({"run_id":run_id,"package_id":retired["package_id"]}),
        )
        .await;
        assert_eq!(history["package"]["package_id"], retired["package_id"]);
    }
    service.shutdown().await.unwrap();
}
