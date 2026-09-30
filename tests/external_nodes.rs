//! Outside clients act for external nodes with core moves, beside programs.

use ontography_app::{persistence::Paths, state::Service, tools};
use serde_json::{Value, json};
use std::time::Duration;

async fn call(service: &Service, operation: &str, args: Value) -> Value {
    tools::dispatch(service, operation, &args)
        .await
        .unwrap_or_else(|error| panic!("{operation} failed: {error}; {:?}", error.details))
}

#[tokio::test]
async fn an_external_entry_takes_no_initial_input_and_runs_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let document: Value = serde_json::from_str(include_str!("../examples/flow.json")).unwrap();
    let refused = tools::dispatch(
        &service,
        "flow.start",
        &json!({"document":document,"project":directory.path(),"message":"hello"}),
    )
    .await
    .unwrap_err();
    assert!(refused.message.contains("external entry"), "{refused}");
    let started = call(
        &service,
        "flow.start",
        json!({"document":document,"project":directory.path()}),
    )
    .await;
    assert_eq!(started["tasks"], json!([]));
    assert!(
        started["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|node| node["execution"].is_null())
    );
    let inspected = call(&service, "run.inspect", json!({"run_id":started["run_id"]})).await;
    assert_eq!(inspected["revision"], "0");
    assert_eq!(inspected["executions"], json!([]));
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_outside_client_sends_a_workspace_through_a_typed_connection_to_a_program() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("draft.txt"), "draft").unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    // The client's connection infers its result contract: only a workspace.
    let document = json!({"name":"content","entry":"client",
        "contracts":{"tree":{"object_type":"Tree","validator":"workspace"}},
        "nodes":[
            {"id":"client","component":"external","result":"tree"},
            {"id":"stamp","component":"command","result":"tree",
                "config":{"argv":["/bin/sh","-c","printf stamped > stamp.txt"]}},
            {"id":"archive","component":"inbox"}
        ],
        "edges":[{"from":"client","to":"stamp"},{"from":"stamp","to":"archive"}]});
    let started = call(
        &service,
        "flow.start",
        json!({"document":document,"project":directory.path()}),
    )
    .await;
    let run_id = started["run_id"].as_str().unwrap();
    let submit = |payload: Value, contents: Value| {
        json!({"run_id":run_id,"trigger":{"kind":"root","node_id":"client","authority":["workflow"]},
            "result":payload,"emissions":[{"edge_id":"client:stamp","payload":payload}],"contents":contents})
    };
    let text = tools::dispatch(
        &service,
        "workflow.submit",
        &submit(json!("text"), json!([])),
    )
    .await
    .unwrap_err();
    assert_eq!(text.code, "rejected", "{text}");

    let imported = call(
        &service,
        "workspace.import",
        json!({"run_id":run_id,"path":"source"}),
    )
    .await;
    let envelope = json!({"ontography_package":imported["root"]}).to_string();
    call(
        &service,
        "workflow.submit",
        submit(json!(envelope), imported["dependencies"].clone()),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let status = call(&service, "flow.status", json!({"run_id":run_id})).await;
            if status["frontier"]["counts"]["archive"]["received"] == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the program's workspace reaches the archive");
    let exported = directory.path().join("exported");
    call(
        &service,
        "flow.export",
        json!({"run_id":run_id,"node":"archive","path":exported}),
    )
    .await;
    assert_eq!(
        std::fs::read_to_string(exported.join("draft.txt")).unwrap(),
        "draft"
    );
    assert_eq!(
        std::fs::read_to_string(exported.join("stamp.txt")).unwrap(),
        "stamped"
    );
    service.shutdown().await.unwrap();
}
