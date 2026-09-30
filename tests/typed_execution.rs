//! Typed command and human execution preserves core authority and payload rules.

use ontography::{Payload, SessionHandle};
use ontography_app::{persistence::Paths, state::Service, tools};
use serde_json::{Value, json};
use std::{os::unix::fs::PermissionsExt, path::Path, time::Duration};

async fn call(service: &Service, operation: &str, args: Value) -> Value {
    tools::dispatch(service, operation, &args)
        .await
        .unwrap_or_else(|error| panic!("{operation}: {error}; {:?}", error.details))
}

fn service(project: &Path) -> Service {
    Service::new(Paths::initialize(project.join("data")).unwrap()).unwrap()
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

fn human_document(edge_authority: &str) -> Value {
    json!({"name":"decision","entry":"review","nodes":[
        {"id":"review","component":"human","root":["red"],
            "transitions":[{"from":["red"],"to":["sealed"]},{"from":["red"],"to":[]}]},
        {"id":"sink","component":"inbox"}
    ],"edges":[{"from":"review","to":"sink","authority":[edge_authority]}]})
}

async fn human_task(service: &Service, run: &str) -> Value {
    call(service, "flow.status", json!({"run_id":run})).await["tasks"][0]["task_id"].clone()
}

#[tokio::test]
async fn human_decisions_apply_only_declared_authority_transitions() {
    let directory = tempfile::tempdir().unwrap();
    let service = service(directory.path());
    let mut document = human_document("sealed");
    // Declare this tag in the vocabulary without permitting red -> other.
    document["nodes"][0]["transitions"]
        .as_array_mut()
        .unwrap()
        .push(json!({"from":["other"],"to":["sealed"]}));
    let run = start(&service, directory.path(), document).await;
    let task = human_task(&service, &run).await;
    let decision = |authority: Value| json!({"run_id":run,"node":"review","task_id":task,"message":"Approved","authority":authority});
    let refused = tools::dispatch(&service, "flow.decide", &decision(json!(["other"])))
        .await
        .unwrap_err();
    assert!(refused.message.contains("authority"), "{refused}");
    assert_eq!(human_task(&service, &run).await, task);
    assert_eq!(
        session(&service, &run)
            .await
            .pending_page_at("sink", None, 10)
            .await
            .unwrap()
            .packages()
            .len(),
        0
    );
    call(&service, "flow.decide", decision(json!(["sealed"]))).await;
    assert_eq!(&*sink_payload(&service, &run, "sink").await, b"Approved");
    let page = session(&service, &run)
        .await
        .pending_page_at("sink", None, 10)
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

#[tokio::test]
async fn empty_human_output_authority_is_distinct_from_carry() {
    let directory = tempfile::tempdir().unwrap();
    let service = service(directory.path());
    let run = start(&service, directory.path(), human_document("red")).await;
    let mut decision = json!({"run_id":run,"node":"review","task_id":human_task(&service, &run).await,"message":"Approved","authority":[]});
    let refused = tools::dispatch(&service, "flow.decide", &decision)
        .await
        .unwrap_err();
    assert!(refused.message.contains("authority"), "{refused}");
    decision.as_object_mut().unwrap().remove("authority");
    call(&service, "flow.decide", decision).await;
    assert_eq!(&*sink_payload(&service, &run, "sink").await, b"Approved");
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn command_authority_settings_cannot_bypass_transition_or_edge_checks() {
    let directory = tempfile::tempdir().unwrap();
    let service = service(directory.path());
    for (authority, transitions) in [
        (json!(["sealed"]), json!([])),
        (json!([]), json!([{"from":["red"],"to":[]}])),
    ] {
        let document = json!({"name":"refused-command","entry":"worker","nodes":[
            {"id":"worker","component":"command","root":["red"],
                "config":{"argv":["/bin/cat"],"authority":authority},"transitions":transitions,
                "retry":{"max_attempts":1}},
            {"id":"sink","component":"inbox","root":["sealed"]}
        ],"edges":[{"from":"worker","to":"sink","authority":["red"]}]});
        let run = start(&service, directory.path(), document).await;
        let status = wait_status(&service, &run, |status| {
            !status["failures"].as_array().unwrap().is_empty()
        })
        .await;
        assert!(
            session(&service, &run)
                .await
                .pending_page_at("sink", None, 10)
                .await
                .unwrap()
                .packages()
                .is_empty()
        );
        assert!(
            status["failures"][0]["error"]
                .as_str()
                .unwrap()
                .contains("authority")
        );
    }
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
                "config":{"argv":["/bin/cat",directory.path().join("envelope.json")]},
                "retry":{"max_attempts":1}},
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
    assert_eq!(status["failures"][0]["state"], "parked");
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
    let document = json!({"name":"malformed","entry":"worker",
        "contracts":{"raw":{"object_type":"Bytes","validator":"bytes"}},
        "nodes":[{"id":"worker","component":"command","result":"raw",
            "config":{"argv":["/usr/bin/printf","%s","{\"ontography_package\":7}"]},
            "retry":{"max_attempts":1}},
            {"id":"sink","component":"inbox"}],
        "edges":[{"from":"worker","to":"sink"}]});
    let run = start(&service, directory.path(), document).await;
    let status = wait_status(&service, &run, |status| {
        !status["failures"].as_array().unwrap().is_empty()
    })
    .await;
    assert_eq!(status["failures"][0]["state"], "parked");
    assert!(
        status["failures"][0]["error"]
            .as_str()
            .unwrap()
            .contains("invalid type")
    );
    assert!(
        session(&service, &run)
            .await
            .try_snapshot()
            .await
            .unwrap()
            .state()
            .activations()
            .is_empty()
    );
    service.shutdown().await.unwrap();
}
