use ontography_app::{
    persistence, persistence::Paths, sessions::SessionRecord, state::Service, tools,
    workflow::runtime,
};
use serde_json::{Value, json};
use std::time::Duration;

fn human_document() -> Value {
    json!({"name":"initial-review","entry":"gate","nodes":[
        {"id":"gate","component":"human","config":{"prompt":"Approve the initial input"}},
        {"id":"archive","component":"inbox"}
    ],"edges":[{"from":"gate","to":"archive"}]})
}

async fn call(service: &Service, operation: &str, args: Value) -> Value {
    tools::dispatch(service, operation, &args)
        .await
        .unwrap_or_else(|error| panic!("{operation}: {error}; {:?}", error.details))
}

async fn activation_count(service: &Service, run_id: &str) -> usize {
    let run = service.run(run_id).await.unwrap();
    let session = run.lock().await.live().unwrap().session.clone();
    session
        .try_snapshot()
        .await
        .unwrap()
        .state()
        .activations()
        .len()
}

async fn start_human(service: &Service, project: &std::path::Path) -> Value {
    call(
        service,
        "flow.start",
        json!({"document":human_document(),"project":project,"message":"seed"}),
    )
    .await
}

async fn accept_initial(service: &Service, started: &Value) {
    let task = started["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|task| task["node"] == "gate")
        .unwrap();
    call(service,"flow.decide",json!({"run_id":started["run_id"],"node":"gate","task_id":task["task_id"],"message":"accepted"})).await;
}

#[tokio::test]
async fn changing_completed_human_entry_to_command_does_not_replay_initial_input() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let started = start_human(&service, directory.path()).await;
    let run_id = started["run_id"].as_str().unwrap();
    accept_initial(&service, &started).await;
    assert_eq!(activation_count(&service, run_id).await, 1);
    let mut document = started["document"].clone();
    let gate = document["nodes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["id"] == "gate")
        .unwrap();
    gate["component"] = json!("command");
    gate["config"] = json!({"argv":["/bin/echo","must not repeat initial task"]});
    let plan = call(
        &service,
        "flow.edit",
        json!({"run_id":run_id,"document":document}),
    )
    .await;
    call(
        &service,
        "flow.commit",
        json!({"run_id":run_id,"plan_id":plan["plan_id"]}),
    )
    .await;
    let duplicate = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            if activation_count(&service, run_id).await > 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    service.shutdown().await.unwrap();
    assert!(
        duplicate.is_err(),
        "the old human worker retained its initial payload and admitted another root after the kind change"
    );
}

#[tokio::test]
async fn accepted_initial_task_is_recovered_from_core_when_completion_cache_is_missing() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let started = start_human(&service, directory.path()).await;
    let run_id = started["run_id"].as_str().unwrap().to_owned();
    accept_initial(&service, &started).await;
    {
        let run = service.run(&run_id).await.unwrap();
        let run = run.lock().await;
        let state = runtime::load(&run).unwrap();
        let path = runtime::node_directory(&run, &state.identities.nodes["gate"])
            .join("initial-complete.json");
        std::fs::remove_file(path).unwrap();
    }
    service.shutdown().await.unwrap();
    drop(service);
    let service = Service::new(paths).unwrap();
    let resumed = call(&service, "flow.resume", json!({"run_id":run_id})).await;
    assert!(
        !resumed["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|task| task["node"] == "gate")
    );
    assert_eq!(activation_count(&service, &run_id).await, 1);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejected_scoped_start_id_does_not_persist_another_sessions_ownership() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let first = call(
        &service,
        "session.create",
        json!({"project":directory.path()}),
    )
    .await;
    let second = call(
        &service,
        "session.create",
        json!({"project":directory.path()}),
    )
    .await;
    let first_id = first["session_id"].as_str().unwrap();
    let second_id = second["session_id"].as_str().unwrap().to_owned();
    let args = json!({"document":human_document(),"message":"seed","start_id":uuid::Uuid::new_v4().to_string()});
    tools::dispatch_scoped(&service, Some(first_id), "flow.start", &args)
        .await
        .unwrap();
    let rejected = tools::dispatch_scoped(&service, Some(&second_id), "flow.start", &args)
        .await
        .unwrap_err();
    assert_eq!(rejected.code, "run_already_owned");
    let path = service
        .sessions
        .directory(&second_id)
        .unwrap()
        .join("session.json");
    let record: SessionRecord = persistence::read_json(&path).unwrap();
    service.shutdown().await.unwrap();
    assert!(
        record.graph_initialization.is_none(),
        "a rejected ownership claim must not become a durable initialization intent"
    );
    drop(service);
    let service = Service::new(paths).unwrap();
    assert!(service.sessions.recovery_errors.is_empty());
    tools::dispatch_scoped(
        &service,
        Some(&second_id),
        "flow.start",
        &json!({"document":human_document(),"message":"own run"}),
    )
    .await
    .unwrap();
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn start_retry_finishes_workers_after_initial_workspace_import_failure() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(directory.path().join("outside"), "outside workspace").unwrap();
    std::os::unix::fs::symlink("../outside", source.join("bad")).unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let args = json!({"document":human_document(),"project":directory.path(),"workspace":source,"start_id":uuid::Uuid::new_v4().to_string()});
    let failure = tools::dispatch(&service, "flow.start", &args)
        .await
        .unwrap_err();
    assert!(
        failure.message.contains("symlink"),
        "expected the initial import to fail, got {failure}"
    );
    std::fs::remove_file(source.join("bad")).unwrap();
    let resumed = call(&service, "flow.start", args).await;
    let gate = resumed["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["id"] == "gate")
        .unwrap();
    service.shutdown().await.unwrap();
    assert_eq!(
        gate["execution"]["state"], "running",
        "the retry must finish launching the entry worker after recovering the input import"
    );
}

#[tokio::test]
async fn accepted_output_is_recovered_from_prepared_cache_and_task_retry_is_stale() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let started = start_human(&service, directory.path()).await;
    let run_id = started["run_id"].as_str().unwrap();
    let task_id = started["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|task| task["node"] == "gate")
        .unwrap()["task_id"]
        .clone();
    accept_initial(&service, &started).await;
    let retry = tools::dispatch(
        &service,
        "flow.decide",
        &json!({"run_id":run_id,"node":"gate","task_id":task_id,"message":"duplicate"}),
    )
    .await
    .unwrap_err();
    assert_eq!(retry.code, "stale_task");
    assert_eq!(activation_count(&service, run_id).await, 1);
    {
        let run = service.run(run_id).await.unwrap();
        let run = run.lock().await;
        let state = runtime::load(&run).unwrap();
        let path =
            runtime::node_directory(&run, &state.identities.nodes["gate"]).join("output.json");
        let mut output: Value = persistence::read_json(&path).unwrap();
        output["publication_status"] = json!("prepared");
        output.as_object_mut().unwrap().remove("activation_id");
        persistence::write_json(&path, &output).unwrap();
    }
    let output = call(
        &service,
        "flow.output",
        json!({"run_id":run_id,"node":"gate"}),
    )
    .await;
    assert_eq!(output["publication_status"], "committed");
    assert!(output.get("activation_id").is_none());
    assert!(output.get("invocation_id").is_none());
    let destination = directory.path().join("accepted.txt");
    call(
        &service,
        "flow.export",
        json!({"run_id":run_id,"node":"gate","path":destination}),
    )
    .await;
    assert_eq!(std::fs::read_to_string(destination).unwrap(), "accepted");
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn manager_status_output_and_preview_use_document_names_and_opaque_work_handles() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let started = start_human(&service, directory.path()).await;
    let run_id = started["run_id"].as_str().unwrap();
    accept_initial(&service, &started).await;
    let private_ids = {
        let handle = service.run(run_id).await.unwrap();
        let run = handle.lock().await;
        let state = runtime::load(&run).unwrap();
        let session = &run.live().unwrap().session;
        let snapshot = session.try_snapshot().await.unwrap();
        // A new run's identities are its names; any other identity is internal.
        let mut ids: Vec<String> = state
            .identities
            .nodes
            .iter()
            .chain(&state.identities.edges)
            .filter(|(name, id)| name != id)
            .map(|(_, id)| id.clone())
            .collect();
        ids.extend(snapshot.state().packages().keys().map(ToString::to_string));
        ids.extend(
            snapshot
                .state()
                .activations()
                .keys()
                .map(ToString::to_string),
        );
        ids.extend(
            session
                .invocations_page(None, None, 100)
                .await
                .unwrap()
                .iter()
                .map(|invocation| invocation.id.to_string()),
        );
        ids
    };
    let status = call(&service, "flow.status", json!({"run_id":run_id})).await;
    let mut node_names: Vec<_> = status["graph"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|node| node["id"].as_str().unwrap())
        .collect();
    node_names.sort();
    assert_eq!(node_names, ["archive", "gate"]);
    assert_eq!(status["graph"]["edges"][0]["source"], "gate");
    assert_eq!(status["graph"]["edges"][0]["target"], "archive");
    let waiting = status["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|task| task["node"] == "archive")
        .unwrap();
    let work_handle = waiting["work_ids"][0].as_str().unwrap();
    assert!(work_handle.starts_with("work_"));
    let output = call(
        &service,
        "flow.output",
        json!({"run_id":run_id,"node":"gate"}),
    )
    .await;
    assert_eq!(output["node"], "gate");
    let mut next = status["document"].clone();
    next["nodes"]
        .as_array_mut()
        .unwrap()
        .retain(|node| node["id"] != "archive");
    next["edges"] = json!([]);
    let preview = call(
        &service,
        "flow.edit",
        json!({"run_id":run_id,"document":next}),
    )
    .await;
    assert_eq!(preview["retirements"][0]["node"], "archive");
    assert_eq!(preview["retirements"][0]["work_id"], work_handle);
    for public in [&status, &output, &preview] {
        let encoded = public.to_string();
        for id in &private_ids {
            assert!(
                !encoded.contains(id),
                "manager response exposed internal identity {id}: {encoded}"
            );
        }
        assert!(!encoded.contains("invocation_id"));
        assert!(!encoded.contains("activation_id"));
        assert!(!encoded.contains("package_id"));
    }

    // A workspace is a public input kind, not its native content-store identity.
    let source = directory.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("draft.txt"), "draft").unwrap();
    let workspace_run = call(
        &service,
        "flow.start",
        json!({"document":human_document(),"project":directory.path(),"workspace":source}),
    )
    .await;
    let workspace_id = workspace_run["run_id"].as_str().unwrap();
    let input = workspace_run["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|task| task["node"] == "gate")
        .unwrap();
    assert_eq!(input["input"], json!({"workspace":true}));
    let output = call(
        &service,
        "flow.output",
        json!({"run_id":workspace_id,"node":"gate"}),
    )
    .await;
    assert_eq!(output["input"], json!({"workspace":true}));
    let raw_workspace = {
        let handle = service.run(workspace_id).await.unwrap();
        let run = handle.lock().await;
        let raw: Value =
            serde_json::from_slice(&runtime::initial_payload(&run).await.unwrap()).unwrap();
        raw["ontography_package"].to_string()
    };
    for public in [&workspace_run, &output] {
        let encoded = public.to_string();
        assert!(!encoded.contains("ontography_package"));
        assert!(!encoded.contains(&raw_workspace));
    }
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_optional_start_fields_fail_before_reserving_a_run() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let created = call(
        &service,
        "session.create",
        json!({"project":directory.path()}),
    )
    .await;
    let session_id = created["session_id"].as_str().unwrap();
    for (field, value) in [
        ("message", json!(7)),
        ("message", Value::Null),
        ("start_id", json!(7)),
        ("start_id", Value::Null),
    ] {
        let mut args = json!({"document":human_document(),"project":directory.path()});
        args[field] = value;
        for scope in [None, Some(session_id)] {
            let error = tools::dispatch_scoped(&service, scope, "flow.start", &args)
                .await
                .unwrap_err();
            assert_eq!(
                error.code, "invalid_arguments",
                "bad {field} should be rejected, not defaulted"
            );
            assert!(
                service.runs.lock().await.is_empty(),
                "bad {field} created a run"
            );
            let record = service.sessions.get(session_id).await.unwrap();
            let record = record.lock().await;
            assert!(record.run_id.is_none());
            assert!(record.graph_initialization.is_none());
        }
    }
    service.shutdown().await.unwrap();
}
