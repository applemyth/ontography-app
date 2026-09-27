use ontography_app::{persistence::Paths, state::Service, tools};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, time::Duration};

async fn call(service: &Service, operation: &str, args: Value) -> Value {
    tools::dispatch(service, operation, &args)
        .await
        .unwrap_or_else(|error| panic!("{operation}: {error}; {:?}", error.details))
}

async fn rejects(service: &Service, operation: &str, args: Value, code: &str) {
    let error = tools::dispatch(service, operation, &args)
        .await
        .unwrap_err();
    assert_eq!(error.code, code, "{operation}: {error}");
}

fn task(status: &Value, node: &str) -> Value {
    status["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|task| task["node"] == node)
        .unwrap_or_else(|| panic!("Missing {node} task: {status}"))
        .clone()
}

fn review_loop() -> Value {
    json!({"name":"review-loop","entry":"review","nodes":[
        {"id":"review","kind":"human"},{"id":"revise","kind":"human"}
    ],"edges":[{"from":"review","to":"revise"},{"from":"revise","to":"review"}]})
}

async fn message(service: &Service, run: &Value, node: &str, text: &str) -> Value {
    call(
        service,
        "flow.decide",
        json!({"run_id":run["run_id"],"node":node,
        "task_id":task(run,node)["task_id"],"message":text}),
    )
    .await
}

async fn open_task(service: &Service, run: &Value, node: &str) -> Value {
    call(
        service,
        "flow.workspace",
        json!({"run_id":run["run_id"],"action":"open",
        "node":node,"task_id":task(run,node)["task_id"]}),
    )
    .await
}

fn workspace_text(opened: &Value) -> String {
    std::fs::read_to_string(Path::new(opened["path"].as_str().unwrap()).join("draft.txt")).unwrap()
}

async fn capture(service: &Service, run_id: &Value, opened: &Value, text: &str) -> Value {
    std::fs::write(
        Path::new(opened["path"].as_str().unwrap()).join("draft.txt"),
        text,
    )
    .unwrap();
    call(
        service,
        "flow.workspace",
        json!({"run_id":run_id,"action":"capture",
        "workspace_id":opened["workspace_id"]}),
    )
    .await;
    call(
        service,
        "flow.workspace",
        json!({"run_id":run_id,"action":"release",
        "workspace_id":opened["workspace_id"]}),
    )
    .await;
    opened["workspace_id"].clone()
}

#[tokio::test]
async fn human_output_prefers_new_task_and_explicit_output_preserves_prior_decision() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let initial = call(
        &service,
        "flow.start",
        json!({"document":review_loop(),
        "project":directory.path(),"message":"initial draft"}),
    )
    .await;
    let run_id = initial["run_id"].clone();
    let first = message(&service, &initial, "review", "first review").await;
    let revised = message(&service, &first, "revise", "revised draft").await;
    assert_ne!(initial["revision"], revised["revision"]);
    assert_ne!(
        task(&initial, "review")["task_id"],
        task(&revised, "review")["task_id"]
    );
    let current = call(
        &service,
        "flow.output",
        json!({"run_id":run_id,"node":"review"}),
    )
    .await;
    assert_eq!(current["input"]["message"], "revised draft");
    assert_eq!(current["task_id"], task(&revised, "review")["task_id"]);
    assert!(current.get("result").is_none());
    let prior = call(
        &service,
        "flow.output",
        json!({"run_id":run_id,"node":"review","source":"output"}),
    )
    .await;
    assert_eq!(prior["result"]["message"], "first review");
    assert_eq!(prior["publication_status"], "committed");
    rejects(
        &service,
        "flow.workspace",
        json!({"run_id":run_id,"action":"open","node":"review",
        "task_id":task(&initial,"review")["task_id"]}),
        "stale_task",
    )
    .await;

    call(&service, "run.suspend", json!({"run_id":run_id})).await;
    rejects(
        &service,
        "flow.output",
        json!({"run_id":run_id,"node":"review"}),
        "run_suspended",
    )
    .await;
    rejects(
        &service,
        "flow.output",
        json!({"run_id":run_id,"node":"review",
        "task_id":current["task_id"]}),
        "run_suspended",
    )
    .await;
    assert_eq!(
        call(
            &service,
            "flow.output",
            json!({"run_id":run_id,"node":"review","source":"output"})
        )
        .await,
        prior
    );
    let resumed = call(&service, "flow.resume", json!({"run_id":run_id})).await;
    let second = message(&service, &resumed, "review", "second review").await;
    assert_eq!(task(&second, "revise")["input"]["message"], "second review");
    let again = message(&service, &second, "revise", "second revised draft").await;
    let current = call(
        &service,
        "flow.output",
        json!({"run_id":run_id,"node":"review"}),
    )
    .await;
    assert_eq!(current["input"]["message"], "second revised draft");
    assert_eq!(current["task_id"], task(&again, "review")["task_id"]);
    assert_eq!(
        call(
            &service,
            "flow.output",
            json!({"run_id":run_id,"node":"review","source":"output"})
        )
        .await["result"]["message"],
        "second review"
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn repeated_workspace_reviews_open_current_files_and_reject_stale_task_selectors() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("draft.txt"), "original").unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let initial = call(
        &service,
        "flow.start",
        json!({"document":review_loop(),
        "project":directory.path(),"workspace":source}),
    )
    .await;
    let run_id = initial["run_id"].clone();
    let opened = open_task(&service, &initial, "review").await;
    assert_eq!(workspace_text(&opened), "original");
    let workspace = capture(&service, &run_id, &opened, "first review").await;
    let first = call(
        &service,
        "flow.decide",
        json!({"run_id":run_id,"node":"review",
        "task_id":task(&initial,"review")["task_id"],"workspace_id":workspace}),
    )
    .await;
    let opened = open_task(&service, &first, "revise").await;
    assert_eq!(workspace_text(&opened), "first review");
    let workspace = capture(&service, &run_id, &opened, "revised draft").await;
    let revised = call(
        &service,
        "flow.decide",
        json!({"run_id":run_id,"node":"revise",
        "task_id":task(&first,"revise")["task_id"],"workspace_id":workspace}),
    )
    .await;
    let current = open_task(&service, &revised, "review").await;
    assert_eq!(workspace_text(&current), "revised draft");
    let prior = call(
        &service,
        "flow.workspace",
        json!({"run_id":run_id,"action":"open",
        "node":"review","source":"output"}),
    )
    .await;
    assert_eq!(workspace_text(&prior), "first review");
    rejects(
        &service,
        "flow.workspace",
        json!({"run_id":run_id,"action":"open","node":"review",
        "task_id":task(&initial,"review")["task_id"]}),
        "stale_task",
    )
    .await;
    for conflicting in [
        json!({"action":"open","path":source,"task_id":task(&revised,"review")["task_id"]}),
        json!({"action":"open","workspace_id":workspace,"source":"output"}),
        json!({"action":"capture","workspace_id":current["workspace_id"],"task_id":task(&revised,"review")["task_id"]}),
        json!({"action":"release","workspace_id":current["workspace_id"],"work_id":"ignored"}),
    ] {
        let mut args = conflicting;
        args["run_id"] = run_id.clone();
        rejects(&service, "flow.workspace", args, "invalid_arguments").await;
    }
    let workspace = capture(&service, &run_id, &current, "second review").await;
    let second = call(
        &service,
        "flow.decide",
        json!({"run_id":run_id,"node":"review",
        "task_id":task(&revised,"review")["task_id"],"workspace_id":workspace}),
    )
    .await;
    let next = open_task(&service, &second, "revise").await;
    assert_eq!(workspace_text(&next), "second review");
    assert_ne!(
        task(&first, "revise")["task_id"],
        task(&second, "revise")["task_id"]
    );
    let visible = call(
        &service,
        "flow.output",
        json!({"run_id":run_id,"node":"revise"}),
    )
    .await;
    assert_eq!(visible["input"], json!({"workspace":true}));
    assert_eq!(
        std::fs::read_to_string(source.join("draft.txt")).unwrap(),
        "original"
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn inbox_items_are_selected_explicitly_paged_consistently_and_stable_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let document = json!({"name":"collect-results","entry":"review","nodes":[
        {"id":"review","kind":"human"},{"id":"results","kind":"inbox"}
    ],"edges":[{"from":"review","to":"review"},{"from":"review","to":"results"}]});
    let initial = call(
        &service,
        "flow.start",
        json!({"document":document,"project":directory.path(),"message":"seed"}),
    )
    .await;
    let run_id = initial["run_id"].clone();
    let first = message(&service, &initial, "review", "one").await;
    let second = message(&service, &first, "review", "two").await;
    let output = call(
        &service,
        "flow.output",
        json!({"run_id":run_id,"node":"results"}),
    )
    .await;
    assert_eq!(output["items"].as_array().unwrap().len(), 2);
    assert!(output.get("input").is_none());
    let expected: BTreeMap<String, String> = output["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| {
            (
                item["work_id"].as_str().unwrap().into(),
                item["input"]["message"].as_str().unwrap().into(),
            )
        })
        .collect();
    assert_eq!(
        expected
            .values()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        ["one".to_owned(), "two".to_owned()].into()
    );
    let ambiguous = directory.path().join("ambiguous");
    rejects(
        &service,
        "flow.export",
        json!({"run_id":run_id,"node":"results","path":ambiguous}),
        "selection_required",
    )
    .await;
    assert!(!ambiguous.exists());
    rejects(
        &service,
        "flow.workspace",
        json!({"run_id":run_id,"action":"open","node":"results"}),
        "selection_required",
    )
    .await;
    for (index, (work, text)) in expected.iter().enumerate() {
        let selected = call(
            &service,
            "flow.output",
            json!({"run_id":run_id,"node":"results","work_id":work}),
        )
        .await;
        assert_eq!(selected["input"]["message"], text.as_str());
        let path = directory.path().join(format!("result-{index}"));
        call(
            &service,
            "flow.export",
            json!({"run_id":run_id,"node":"results","work_id":work,"path":path}),
        )
        .await;
        assert_eq!(std::fs::read_to_string(path).unwrap(), *text);
    }
    let page1 = call(
        &service,
        "flow.output",
        json!({"run_id":run_id,"node":"results","limit":1}),
    )
    .await;
    assert_eq!(page1["items"].as_array().unwrap().len(), 1);
    assert!(page1["next_after"].is_string());
    assert!(page1.get("input").is_none());
    let page2_args = json!({"run_id":run_id,"node":"results","limit":1,"after":page1["next_after"],"revision":page1["revision"]});
    let page2 = call(&service, "flow.output", page2_args.clone()).await;
    assert_eq!(page2["items"].as_array().unwrap().len(), 1);
    assert_ne!(page1["items"][0]["work_id"], page2["items"][0]["work_id"]);
    assert!(page2["next_after"].is_null());
    message(&service, &second, "review", "three").await;
    rejects(&service, "flow.output", page2_args, "stale_page").await;
    service.shutdown().await.unwrap();
    drop(service);

    let service = Service::new(paths).unwrap();
    call(&service, "flow.resume", json!({"run_id":run_id})).await;
    let restored = call(
        &service,
        "flow.output",
        json!({"run_id":run_id,"node":"results"}),
    )
    .await;
    assert_eq!(restored["items"].as_array().unwrap().len(), 3);
    for (work, text) in expected {
        assert!(
            restored["items"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["work_id"] == work && item["input"]["message"] == text)
        );
        assert_eq!(
            call(
                &service,
                "flow.output",
                json!({"run_id":run_id,"node":"results","work_id":work})
            )
            .await["input"]["message"],
            text
        );
    }
    call(&service, "run.suspend", json!({"run_id":run_id})).await;
    rejects(
        &service,
        "flow.output",
        json!({"run_id":run_id,"node":"results"}),
        "run_suspended",
    )
    .await;
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn joined_task_selects_each_input_and_opens_its_unique_workspace() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("draft.txt"), "joined workspace").unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let document = json!({"name":"joined-review","entry":"start","nodes":[
        {"id":"start","kind":"human"},{"id":"files","kind":"human"},
        {"id":"note","kind":"human"},{"id":"review","kind":"human","join":"all"}
    ],"edges":[{"from":"start","to":"files"},{"from":"start","to":"note"},
        {"from":"files","to":"review"},{"from":"note","to":"review"}]});
    let initial = call(
        &service,
        "flow.start",
        json!({"document":document,"project":directory.path(),"message":"seed"}),
    )
    .await;
    let run_id = initial["run_id"].clone();
    let branches = message(&service, &initial, "start", "prepare").await;
    let opened = call(
        &service,
        "flow.workspace",
        json!({"run_id":run_id,"action":"open","path":source}),
    )
    .await;
    let workspace = capture(&service, &run_id, &opened, "joined workspace").await;
    let files = call(
        &service,
        "flow.decide",
        json!({"run_id":run_id,"node":"files",
        "task_id":task(&branches,"files")["task_id"],"workspace_id":workspace}),
    )
    .await;
    let ready = message(&service, &files, "note", "review instructions").await;
    let joined = task(&ready, "review");
    assert_eq!(joined["work_ids"].as_array().unwrap().len(), 2);
    let opened = open_task(&service, &ready, "review").await;
    assert_eq!(workspace_text(&opened), "joined workspace");
    let mut inputs = Vec::new();
    for work in joined["work_ids"].as_array().unwrap() {
        let selected = call(
            &service,
            "flow.output",
            json!({"run_id":run_id,"node":"review",
            "task_id":joined["task_id"],"work_id":work}),
        )
        .await;
        inputs.push(selected["input"].clone());
    }
    assert!(inputs.contains(&json!({"workspace":true})));
    assert!(inputs.contains(&json!({"message":"review instructions"})));
    rejects(
        &service,
        "flow.export",
        json!({"run_id":run_id,"node":"review",
        "task_id":joined["task_id"],"path":directory.path().join("ambiguous")}),
        "selection_required",
    )
    .await;
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_command_exposes_structured_error_without_changing_case() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let document = json!({"name":"command-failure","entry":"fail","nodes":[
        {"id":"fail","kind":"command","config":{"argv":["/bin/sh","-c","printf 'MiXeD Failure' >&2; exit 7"]}}
    ],"edges":[]});
    let started = call(
        &service,
        "flow.start",
        json!({"document":document,"project":directory.path(),"message":"seed"}),
    )
    .await;
    let failed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let status = call(&service, "flow.status", json!({"run_id":started["run_id"]})).await;
            let execution = &status["nodes"][0]["execution"];
            if execution["state"] == "failed" {
                break execution.clone();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("command did not report its failure");
    assert_eq!(failed["error"]["class"], "workflow_worker");
    assert_eq!(
        failed["error"]["message"],
        "Worker exited with status 7: MiXeD Failure"
    );
    let pending = call(
        &service,
        "flow.output",
        json!({"run_id":started["run_id"],"node":"fail","source":"pending"}),
    )
    .await;
    assert_eq!(pending["input"]["message"], "seed");
    service.shutdown().await.unwrap();
}
