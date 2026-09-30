//! Typed command and human execution preserves core authority and payload
//! rules, and document rules refuse only new work.

use ontography::{Payload, SessionHandle};
use ontography_app::{
    persistence::{self, Paths},
    state::Service,
    tools,
};
use serde_json::{Value, json};
use std::{os::unix::fs::PermissionsExt, path::Path, time::Duration};

async fn call(service: &Service, operation: &str, args: Value) -> Value {
    tools::dispatch(service, operation, &args)
        .await
        .unwrap_or_else(|error| panic!("{operation}: {error}; {:?}", error.details))
}

/// A service whose files all stay under `project`. It never listens.
fn service(project: &Path) -> Service {
    let mut paths = Paths::initialize(project.join("data")).unwrap();
    paths.socket = project.join("server.sock");
    Service::new(paths).unwrap()
}

async fn start(service: &Service, project: &Path, document: Value) -> String {
    call(
        service,
        "flow.start",
        json!({"document":document,"project":project}),
    )
    .await["run_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn session(service: &Service, run: &str) -> SessionHandle {
    service
        .run(run)
        .await
        .unwrap()
        .lock()
        .await
        .live()
        .unwrap()
        .session
        .clone()
}

async fn wait_status(service: &Service, run: &str, ready: impl Fn(&Value) -> bool) -> Value {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let status = call(service, "flow.status", json!({"run_id":run})).await;
            if ready(&status) {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the workflow did not reach the expected state")
}

async fn sink_payload(service: &Service, run: &str, node: &str) -> Payload {
    wait_status(service, run, |status| {
        status["frontier"]["counts"][node]["received"] == 1
    })
    .await;
    let session = session(service, run).await;
    let page = session.pending_page_at(node, None, 10).await.unwrap();
    session
        .content(page.packages()[0].1.content_digest())
        .await
        .unwrap()
        .unwrap()
}

fn doc_examples() -> impl Iterator<Item = Value> {
    include_str!("../docs/WORKFLOWS.md")
        .split("```json\n")
        .skip(1)
        .map(|block| serde_json::from_str(block.split("```").next().unwrap()).unwrap())
}

#[tokio::test]
async fn documented_smelting_example_runs_with_its_declared_transition() {
    let directory = tempfile::tempdir().unwrap();
    let script = directory.path().join("smelt");
    let script_text = include_str!("../docs/WORKFLOWS.md")
        .split("```sh\n")
        .skip(1)
        .map(|block| block.split("```").next().unwrap())
        .find(|script| script.starts_with("#!/bin/sh\n"))
        .unwrap();
    std::fs::write(&script, script_text).unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    let service = service(directory.path());
    let document = doc_examples()
        .find(|value| value["name"] == "smelting")
        .unwrap();
    let run = start(&service, directory.path(), document).await;
    let mut submit = doc_examples()
        .find(|value| value["trigger"]["node_id"] == "mine")
        .unwrap();
    submit["run_id"] = json!(run);
    call(&service, "workflow.submit", submit).await;
    assert_eq!(
        &*sink_payload(&service, &run, "vault").await,
        b"Refined: raw ore"
    );
    let page = session(&service, &run)
        .await
        .pending_page_at("vault", None, 10)
        .await
        .unwrap();
    assert_eq!(
        page.packages()[0]
            .1
            .authority()
            .tags()
            .map(|tag| tag.id())
            .collect::<Vec<_>>(),
        ["sealed"]
    );
    service.shutdown().await.unwrap();
}

/// A person reviews what a client sends carrying `red`: `red -> sealed` and
/// `red -> []` are the only transitions, and the sink admits `sink` tags.
fn review_document(sink: Value) -> Value {
    json!({"name":"decision","entry":"client","nodes":[
        {"id":"client","component":"external","root":["red"]},
        {"id":"review","component":"human",
            "transitions":[{"from":["red"],"to":["sealed"]},{"from":["red"],"to":[]}]},
        {"id":"sink","component":"inbox"}
    ],"edges":[{"from":"client","to":"review","authority":["red"]},
        {"from":"review","to":"sink","authority":sink}]})
}

async fn send(service: &Service, run: &str, message: &str) {
    call(
        service,
        "workflow.submit",
        json!({"run_id":run,"trigger":{"kind":"root","node_id":"client","authority":["red"]},
        "result":message,"emissions":[{"edge_id":"client:review","payload":message}]}),
    )
    .await;
}

async fn human_task(service: &Service, run: &str) -> Value {
    let status = call(service, "flow.status", json!({"run_id":run})).await;
    let tasks = status["tasks"].as_array().unwrap();
    let task = tasks.iter().find(|task| task["node"] == "review").unwrap();
    task["task_id"].clone()
}

async fn sink_tags(service: &Service, run: &str) -> Vec<Vec<String>> {
    let session = session(service, run).await;
    let page = session.pending_page_at("sink", None, 10).await.unwrap();
    page.packages()
        .iter()
        .map(|(_, package)| {
            package
                .authority()
                .tags()
                .map(|tag| tag.id().into())
                .collect()
        })
        .collect()
}

#[tokio::test]
async fn human_decisions_apply_only_declared_authority_transitions() {
    let directory = tempfile::tempdir().unwrap();
    let service = service(directory.path());
    // The sink admits `other`, so only the undeclared `red -> other` can refuse it.
    let run = start(
        &service,
        directory.path(),
        review_document(json!(["sealed", "other"])),
    )
    .await;
    send(&service, &run, "first").await;
    send(&service, &run, "second").await;
    let decision = |task: &Value, message: &str, authority: Value| json!({"run_id":run,"node":"review","task_id":task,"message":message,"authority":authority});
    let first = human_task(&service, &run).await;
    call(
        &service,
        "flow.decide",
        decision(&first, "Approved", json!(["sealed"])),
    )
    .await;
    let second = human_task(&service, &run).await;
    assert_ne!(first, second);
    let refused = tools::dispatch(
        &service,
        "flow.decide",
        &decision(&second, "Overreach", json!(["other"])),
    )
    .await
    .unwrap_err();
    assert_eq!(refused.code, "rejected", "{refused}");
    assert!(
        refused
            .message
            .contains(r#"no declared transition changes authority ["red"] to ["other"]"#),
        "{refused}"
    );
    // The task stays ready, and the refusal leaves the node's last result.
    assert_eq!(human_task(&service, &run).await, second);
    assert_eq!(sink_tags(&service, &run).await, [["sealed"]]);
    let output = call(
        &service,
        "flow.output",
        json!({"run_id":run,"node":"review","source":"output"}),
    )
    .await;
    assert_eq!(output["result"]["message"], "Approved");
    assert_eq!(output["publication_status"], "committed");
    let exported = directory.path().join("decision.txt");
    call(
        &service,
        "flow.export",
        json!({"run_id":run,"node":"review","source":"output","path":exported}),
    )
    .await;
    assert_eq!(std::fs::read(exported).unwrap(), b"Approved");
    call(
        &service,
        "flow.decide",
        decision(&second, "Sealed", json!(["sealed"])),
    )
    .await;
    assert_eq!(sink_tags(&service, &run).await, [["sealed"], ["sealed"]]);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn empty_human_output_authority_is_distinct_from_carry() {
    let directory = tempfile::tempdir().unwrap();
    let service = service(directory.path());
    let run = start(&service, directory.path(), review_document(json!(["red"]))).await;
    send(&service, &run, "work").await;
    let mut decision = json!({"run_id":run,"node":"review","task_id":human_task(&service, &run).await,"message":"Approved","authority":[]});
    // `red -> []` is declared, but no connection admits empty authority.
    let refused = tools::dispatch(&service, "flow.decide", &decision)
        .await
        .unwrap_err();
    assert!(
        refused
            .message
            .contains(r#"connection "review:sink" does not admit authority []"#),
        "{refused}"
    );
    decision.as_object_mut().unwrap().remove("authority");
    call(&service, "flow.decide", decision).await;
    assert_eq!(&*sink_payload(&service, &run, "sink").await, b"Approved");
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_command_authority_is_refused_before_the_command_runs() {
    let directory = tempfile::tempdir().unwrap();
    let service = service(directory.path());
    let ran = directory.path().join("ran");
    let document = |authority: &Value| {
        json!({"name":"deploy","entry":"worker","nodes":[
            {"id":"worker","component":"command","root":["red"],
                "config":{"argv":["/usr/bin/touch",ran],"authority":authority},
                "transitions":[{"from":["red"],"to":["sealed"]},{"from":["red"],"to":[]}]},
            {"id":"sink","component":"inbox"}
        ],"edges":[{"from":"worker","to":"sink","authority":["sealed"]}]})
    };
    // Core could admit none of these outputs: a typo, a tag no transition
    // reaches, a malformed tag, and authority no connection admits.
    let invalid = [
        (json!(["seald"]), "needs a declared transition"),
        (json!(["red"]), "needs a declared transition"),
        (json!([""]), "must use only letters"),
        (json!([]), r#"connection "worker:sink" does not admit"#),
    ];
    for (authority, message) in &invalid {
        for operation in ["flow.define", "flow.start"] {
            let error = tools::dispatch(
                &service,
                operation,
                &json!({"document":document(authority),"project":directory.path()}),
            )
            .await
            .unwrap_err();
            assert_eq!(error.code, "invalid_workflow_document", "{error}");
            assert!(error.message.contains(message), "{authority}: {error}");
        }
    }
    assert!(!ran.exists(), "a refused command must never run");
    // A settings-only edit is refused the same way; a valid one is not.
    let run = start(&service, directory.path(), document(&json!(["sealed"]))).await;
    for (authority, message) in &invalid {
        let error = tools::dispatch(
            &service,
            "flow.edit",
            &json!({"run_id":run,"document":document(authority)}),
        )
        .await
        .unwrap_err();
        assert!(error.message.contains(message), "{authority}: {error}");
    }
    let mut edited = document(&json!(["sealed"]));
    edited["nodes"][0]["config"]["timeout_secs"] = json!(60);
    let plan = call(
        &service,
        "flow.edit",
        json!({"run_id":run,"document":edited}),
    )
    .await;
    assert_eq!(plan["changes"], 0);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn binary_command_input_result_and_export_preserve_every_byte() {
    let directory = tempfile::tempdir().unwrap();
    let service = service(directory.path());
    let document = json!({"name":"binary","entry":"source",
        "contracts":{"raw":{"object_type":"Bytes","validator":"bytes"}},
        "nodes":[{"id":"source","component":"external","result":"raw"},
            {"id":"worker","component":"command","result":"raw","config":{"argv":["/bin/cat"]}},
            {"id":"sink","component":"inbox"}],
        "edges":[{"from":"source","to":"worker"},{"from":"worker","to":"sink"}]});
    let run = start(&service, directory.path(), document).await;
    let bytes = [0xff, 0x00, 0xc3, 0x28, 0x80, 0x0a];
    call(
        &service,
        "workflow.submit",
        json!({"run_id":run,
        "trigger":{"kind":"root","node_id":"source","authority":["workflow"]},
        "result":bytes,"emissions":[{"edge_id":"source:worker","payload":bytes}]}),
    )
    .await;
    assert_eq!(&*sink_payload(&service, &run, "sink").await, &bytes);
    let output = directory.path().join("result.bin");
    call(
        &service,
        "flow.export",
        json!({"run_id":run,"node":"worker","path":output}),
    )
    .await;
    assert_eq!(std::fs::read(output).unwrap(), bytes);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn command_stdout_cannot_publish_a_package_outside_its_input_grants() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("ore.txt"), "ore").unwrap();
    let service = service(directory.path());
    let document = json!({"name":"envelope","entry":"client",
        "contracts":{"tree":{"object_type":"Tree","validator":"workspace"}},
        "nodes":[{"id":"client","component":"external"},
            {"id":"worker","component":"command","result":"tree",
                "config":{"argv":["/bin/cat",directory.path().join("envelope.json")]}},
            {"id":"sink","component":"inbox"}],
        "edges":[{"from":"client","to":"worker"},{"from":"worker","to":"sink"}]});
    let run = start(&service, directory.path(), document).await;
    let imported = call(
        &service,
        "workspace.import",
        json!({"run_id":run,"path":"source"}),
    )
    .await;
    let envelope = format!("{{ \"ontography_package\" : {} }}\n", imported["root"]);
    std::fs::write(directory.path().join("envelope.json"), &envelope).unwrap();
    call(
        &service,
        "workflow.submit",
        json!({"run_id":run,
        "trigger":{"kind":"root","node_id":"client","authority":["workflow"]},
        "result":"go","emissions":[{"edge_id":"client:worker","payload":"go"}]}),
    )
    .await;
    let status = wait_status(&service, &run, |status| {
        !status["failures"].as_array().unwrap().is_empty()
    })
    .await;
    // The same command would print the same envelope, so retries cannot help.
    assert_eq!(status["failures"][0]["state"], "parked");
    assert_eq!(status["failures"][0]["attempts"], 1);
    assert!(
        status["failures"][0]["error"]
            .as_str()
            .unwrap()
            .contains("worker output is outside the granted package view")
    );
    let session = session(&service, &run).await;
    assert!(
        session
            .pending_page_at("sink", None, 10)
            .await
            .unwrap()
            .packages()
            .is_empty()
    );
    assert_eq!(
        session
            .pending_page_at("worker", None, 10)
            .await
            .unwrap()
            .packages()
            .len(),
        1,
        "refusing publication must not consume the input"
    );
    assert!(
        session
            .invocations_page(Some("worker"), None, 10)
            .await
            .unwrap()
            .iter()
            .all(|invocation| invocation.activation_id.is_none()),
        "the worker must not commit the ungranted output"
    );

    // Giving the task that workspace permits its existing host capture path.
    call(
        &service,
        "workflow.submit",
        json!({"run_id":run,
        "trigger":{"kind":"root","node_id":"client","authority":["workflow"]},
        "result":envelope,
        "emissions":[{"edge_id":"client:worker","payload":envelope}],
        "contents":imported["dependencies"]}),
    )
    .await;
    sink_payload(&service, &run, "sink").await;
    let exported = directory.path().join("exported");
    call(
        &service,
        "flow.export",
        json!({"run_id":run,"node":"sink","path":exported}),
    )
    .await;
    assert_eq!(
        std::fs::read_to_string(exported.join("ore.txt")).unwrap(),
        "ore"
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_command_stdout_envelopes_are_refused_even_by_bytes_contracts() {
    let directory = tempfile::tempdir().unwrap();
    let service = service(directory.path());
    let document = json!({"name":"malformed","entry":"client",
        "contracts":{"raw":{"object_type":"Bytes","validator":"bytes"}},
        "nodes":[{"id":"client","component":"external"},
            {"id":"worker","component":"command","result":"raw",
            "config":{"argv":["/usr/bin/printf","%s","{\"ontography_package\":7}"]}},
            {"id":"sink","component":"inbox"}],
        "edges":[{"from":"client","to":"worker"},{"from":"worker","to":"sink"}]});
    let run = start(&service, directory.path(), document).await;
    call(
        &service,
        "workflow.submit",
        json!({"run_id":run,
        "trigger":{"kind":"root","node_id":"client","authority":["workflow"]},
        "result":"go","emissions":[{"edge_id":"client:worker","payload":"go"}]}),
    )
    .await;
    let status = wait_status(&service, &run, |status| {
        !status["failures"].as_array().unwrap().is_empty()
    })
    .await;
    // Malformed output is the command's own, so retries cannot help.
    assert_eq!(status["failures"][0]["state"], "parked");
    assert_eq!(status["failures"][0]["attempts"], 1);
    assert!(
        status["failures"][0]["error"]
            .as_str()
            .unwrap()
            .contains("invalid type")
    );
    // Core's check of worker output refused it, and recorded why.
    let session = session(&service, &run).await;
    let invocations = session
        .invocations_page(Some("worker"), None, 10)
        .await
        .unwrap();
    let [invocation] = &invocations[..] else {
        panic!("expected one attempt, got {invocations:?}");
    };
    let events = session
        .invocation_events(invocation.id, 0, 100)
        .await
        .unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.operation == "context_denied"
                && event.source["operation"] == "worker_output"),
        "{events:?}"
    );
    // Nothing is published, and the input waits for the manager.
    assert!(invocation.activation_id.is_none());
    for (node, pending) in [("worker", 1), ("sink", 0)] {
        let page = session.pending_page_at(node, None, 10).await.unwrap();
        assert_eq!(page.packages().len(), pending, "{node}");
    }
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn command_output_refused_for_its_contract_parks_at_once() {
    let directory = tempfile::tempdir().unwrap();
    let service = service(directory.path());
    // Bytes that are not UTF-8, under the default text contract and retries.
    let document = json!({"name":"binary-text","entry":"worker","nodes":[
        {"id":"worker","component":"command","config":{"argv":["/usr/bin/printf","\\377"]}},
        {"id":"sink","component":"inbox"}
    ],"edges":[{"from":"worker","to":"sink"}]});
    let run = start(&service, directory.path(), document).await;
    let status = wait_status(&service, &run, |status| {
        !status["failures"].as_array().unwrap().is_empty()
    })
    .await;
    let failure = &status["failures"][0];
    assert_eq!(failure["state"], "parked", "{failure}");
    assert_eq!(failure["attempts"], 1);
    assert!(
        failure["error"]
            .as_str()
            .unwrap()
            .contains("the result was refused: invalid UTF-8"),
        "{failure}"
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn connection_names_shadowing_nodes_are_refused_only_in_new_documents() {
    let directory = tempfile::tempdir().unwrap();
    let service = service(directory.path());
    // Documents could once name a connection like a node.
    let legacy = json!({"name":"legacy","entry":"a","nodes":[
        {"id":"a","component":"inbox"},{"id":"b","component":"inbox"}
    ],"edges":[{"from":"a","to":"b","name":"b"}]});
    for operation in ["flow.define", "flow.start"] {
        let error = tools::dispatch(
            &service,
            operation,
            &json!({"document":legacy,"project":directory.path()}),
        )
        .await
        .unwrap_err();
        assert!(error.message.contains("already a node's name"), "{error}");
    }
    // A revision saved before the rule still starts, retries by start_id, and promotes.
    let revision = "0".repeat(64);
    let saved = service.paths.workflow_definition(&revision).unwrap();
    persistence::write_json(&saved, &legacy).unwrap();
    let start = |source: Value| {
        let mut args = source;
        args["start_id"] = json!("6f1d0c2e-9a1b-4c3d-8e5f-0a1b2c3d4e5f");
        args["project"] = json!(directory.path());
        call(&service, "flow.start", args)
    };
    let run = start(json!({"revision":revision})).await["run_id"].clone();
    for retry in [json!({"revision":revision}), json!({"document":legacy})] {
        assert_eq!(start(retry).await["run_id"], run);
    }
    call(&service, "flow.promote", json!({"run_id":run})).await;
    // An edit keeps the name; only a name an edit introduces is refused.
    let mut edited = legacy.clone();
    edited["nodes"]
        .as_array_mut()
        .unwrap()
        .push(json!({"id":"c","component":"inbox"}));
    call(
        &service,
        "flow.edit",
        json!({"run_id":run,"document":edited}),
    )
    .await;
    edited["edges"]
        .as_array_mut()
        .unwrap()
        .push(json!({"from":"a","to":"c","name":"c"}));
    let error = tools::dispatch(
        &service,
        "flow.edit",
        &json!({"run_id":run,"document":edited}),
    )
    .await
    .unwrap_err();
    assert!(error.message.contains("already a node's name"), "{error}");
    service.shutdown().await.unwrap();
}
