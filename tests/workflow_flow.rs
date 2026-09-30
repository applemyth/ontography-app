use ontography_app::{persistence::Paths, state::Service, tools, workflow::runtime};
use serde_json::{Value, json};
use std::time::Duration;

fn document() -> Value {
    json!({
        "name":"reviewable", "entry":"draft",
        "nodes":[
            {"id":"draft","component":"command","config":{"argv":["/bin/echo","draft"]}},
            {"id":"review","component":"human","config":{"prompt":"Check the draft"}},
            {"id":"archive","component":"inbox"}
        ],
        "edges":[{"from":"draft","to":"review"},{"from":"review","to":"archive"}]
    })
}

async fn call(service: &Service, operation: &str, arguments: Value) -> Value {
    tools::dispatch(service, operation, &arguments)
        .await
        .unwrap_or_else(|error| panic!("{operation} failed: {error}; details: {:?}", error.details))
}

async fn start(service: &Service, project: &std::path::Path) -> String {
    call(
        service,
        "flow.start",
        json!({"document":document(),"project":project,"message":"begin"}),
    )
    .await["run_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn wait_status(service: &Service, run_id: &str, ready: impl Fn(&Value) -> bool) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = call(service, "flow.status", json!({"run_id":run_id})).await;
            if ready(&status) {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("workflow did not reach the expected state within five seconds")
}

fn task_for<'a>(status: &'a Value, node: &str) -> Option<&'a Value> {
    status["tasks"]
        .as_array()?
        .iter()
        .find(|task| task["node"] == node)
}

async fn core_revision(service: &Service, run_id: &str) -> u64 {
    let run = service.run(run_id).await.unwrap();
    let session = run.lock().await.live().unwrap().session.clone();
    session.try_snapshot().await.unwrap().revision()
}

#[tokio::test]
async fn command_human_inbox_uses_document_names_and_rejects_raw_mutations() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let run_id = start(&service, directory.path()).await;
    let status = wait_status(&service, &run_id, |status| {
        task_for(status, "review").is_some()
    })
    .await;
    let task = task_for(&status, "review").unwrap();
    assert!(task["input"].to_string().contains("draft"));
    for operation in ["workflow.submit", "rewrite.prepare"] {
        let error = tools::dispatch(&service, operation, &json!({"run_id":run_id}))
            .await
            .unwrap_err();
        assert_eq!(
            error.code, "workflow_owned",
            "{operation} must be rejected at the document ownership boundary"
        );
    }
    call(
        &service,
        "flow.decide",
        json!({"run_id":run_id,"node":"review","task_id":task["task_id"],"message":"approved"}),
    )
    .await;
    let status = wait_status(&service, &run_id, |status| {
        status["nodes"].as_array().is_some_and(|nodes| {
            nodes.iter().any(|node| {
                node["id"] == "archive" && node["pending"].as_u64().is_some_and(|n| n > 0)
            })
        })
    })
    .await;
    assert_eq!(status["document"]["entry"], "draft");
    let output = call(
        &service,
        "flow.output",
        json!({"run_id":run_id,"node":"review"}),
    )
    .await;
    assert!(
        output.to_string().contains("approved"),
        "expected human output, got {output}"
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn prompt_edits_preserve_graph_and_added_nodes_launch_with_idempotent_commit() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let run_id = start(&service, directory.path()).await;
    let before = wait_status(&service, &run_id, |status| {
        task_for(status, "review").is_some()
    })
    .await;
    let original_revision = core_revision(&service, &run_id).await;
    let mut updated = before["document"].clone();
    updated["nodes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["id"] == "review")
        .unwrap()["config"]["prompt"] = json!("Check the revised requirements");
    let preview = call(
        &service,
        "flow.edit",
        json!({"run_id":run_id,"document":updated}),
    )
    .await;
    call(
        &service,
        "flow.commit",
        json!({"run_id":run_id,"plan_id":preview["plan_id"]}),
    )
    .await;
    assert_eq!(core_revision(&service, &run_id).await, original_revision);
    let edited = call(&service, "flow.status", json!({"run_id":run_id})).await;
    assert_eq!(
        edited["version"].as_u64().unwrap(),
        before["version"].as_u64().unwrap() + 1
    );
    assert!(edited["pending_edit"].is_null());

    let mut with_node = edited["document"].clone();
    with_node["nodes"]
        .as_array_mut()
        .unwrap()
        .push(json!({"id":"extra","component":"inbox"}));
    let preview = call(
        &service,
        "flow.edit",
        json!({"run_id":run_id,"document":with_node}),
    )
    .await;
    // A prepared plan must remain usable after the server releases and reopens.
    service.shutdown().await.unwrap();
    drop(service);
    let service = Service::new(paths).unwrap();
    call(&service, "flow.resume", json!({"run_id":run_id})).await;
    call(
        &service,
        "flow.commit",
        json!({"run_id":run_id,"plan_id":preview["plan_id"]}),
    )
    .await;
    let committed = call(&service, "flow.status", json!({"run_id":run_id})).await;
    // Retrying after losing the response neither duplicates nodes nor changes
    // the document version a second time.
    call(
        &service,
        "flow.commit",
        json!({"run_id":run_id,"plan_id":preview["plan_id"]}),
    )
    .await;
    let repeated = call(&service, "flow.status", json!({"run_id":run_id})).await;
    assert_eq!(repeated["version"], committed["version"]);
    assert_eq!(repeated["document"], committed["document"]);
    {
        let run = service.run(&run_id).await.unwrap();
        let run = run.lock().await;
        let state = runtime::load(&run).unwrap();
        let node_id = &state.identities.nodes["extra"];
        let worker = run
            .live()
            .unwrap()
            .workers
            .get(node_id)
            .expect("new graph node needs an actual worker");
        assert_eq!(
            run.live().unwrap().executions[&worker.execution_id].status(),
            ontography::ExecutionStatus::Running
        );
    }
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn promoted_documents_start_again_and_resume_does_not_repeat_initial_work() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let defined = call(&service, "flow.define", json!({"document":document()})).await;
    let started = call(
        &service,
        "flow.start",
        json!({"revision":defined["revision"],"project":directory.path(),"message":"begin"}),
    )
    .await;
    let run_id = started["run_id"].as_str().unwrap().to_owned();
    let original = wait_status(&service, &run_id, |status| {
        task_for(status, "review").is_some()
    })
    .await;
    let original_revision = core_revision(&service, &run_id).await;
    let promoted = call(&service, "flow.promote", json!({"run_id":run_id})).await;
    call(&service, "run.suspend", json!({"run_id":run_id})).await;
    service.shutdown().await.unwrap();
    drop(service);

    let service = Service::new(paths).unwrap();
    call(&service, "flow.resume", json!({"run_id":run_id})).await;
    let resumed = wait_status(&service, &run_id, |status| {
        task_for(status, "review").is_some()
    })
    .await;
    assert_eq!(resumed["document"], original["document"]);
    assert_eq!(core_revision(&service, &run_id).await, original_revision);
    {
        let run = service.run(&run_id).await.unwrap();
        let session = run.lock().await.live().unwrap().session.clone();
        let snapshot = session.try_snapshot().await.unwrap();
        assert_eq!(
            snapshot.state().activations().len(),
            1,
            "initial command must execute exactly once in core"
        );
        assert_eq!(snapshot.state().live().count(), 1);
    }
    let restarted = call(
        &service,
        "flow.start",
        json!({"revision":promoted["revision"],"project":directory.path(),"message":"another run"}),
    )
    .await;
    assert_ne!(restarted["run_id"], run_id);
    let restarted = wait_status(&service, restarted["run_id"].as_str().unwrap(), |status| {
        task_for(status, "review").is_some()
    })
    .await;
    assert_eq!(restarted["document"], original["document"]);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn typed_documents_run_programs_under_their_contracts_and_authority() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    // The gate starts red-and-blue work; the counter's result reaches the
    // archive under all of red, and the raw inbox as bytes under blue.
    let document = json!({
        "name":"typed-flow", "entry":"gate",
        "contracts":{"report":{"object_type":"Report","validator":"text"},
            "blob":{"object_type":"Blob","validator":"bytes"}},
        "nodes":[
            {"id":"gate","component":"human","root":["red","blue"],"result":"report"},
            {"id":"count","component":"command","config":{"argv":["/bin/sh","-c","wc -c | tr -d ' '"]},"result":"report"},
            {"id":"archive","component":"inbox"},
            {"id":"raw","component":"inbox"}
        ],
        "edges":[
            {"from":"gate","to":"count","authority":["red"]},
            {"from":"count","to":"archive","authority":["red"],"match":"all_of"},
            {"from":"count","to":"raw","name":"bytes","contract":"blob","authority":["blue"]}
        ]
    });
    let started = call(
        &service,
        "flow.start",
        json!({"document":document,"project":directory.path(),"message":"approve?"}),
    )
    .await;
    let run_id = started["run_id"].as_str().unwrap();
    let edges = started["graph"]["edges"].as_array().unwrap();
    assert!(edges.contains(&json!({"id":"bytes","source":"count","target":"raw"})));
    assert!(edges.contains(&json!({"id":"count:archive","source":"count","target":"archive"})));
    let gate = task_for(&started, "gate").unwrap().clone();
    call(
        &service,
        "flow.decide",
        json!({"run_id":run_id,"node":"gate","task_id":gate["task_id"],"message":"approved"}),
    )
    .await;
    let status = wait_status(&service, run_id, |status| {
        ["archive", "raw"]
            .iter()
            .all(|node| task_for(status, node).is_some())
    })
    .await;
    assert_eq!(
        task_for(&status, "raw").unwrap()["input"],
        json!({"message":"8\n"})
    );
    // Work carries the entry's whole ceiling, as core recorded it.
    let run = service.run(run_id).await.unwrap();
    let session = run.lock().await.live().unwrap().session.clone();
    let pending = session.pending_at("raw").await.unwrap();
    let (_, record) = &pending.packages()[0];
    assert_eq!(record.object_type(), "Blob");
    assert_eq!(
        record
            .authority()
            .tags()
            .map(|tag| tag.id())
            .collect::<Vec<_>>(),
        ["blue", "red"]
    );
    service.shutdown().await.unwrap();
}
