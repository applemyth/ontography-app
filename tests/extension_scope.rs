use ontography_app::{Result, persistence::Paths, protocol::Request, server::Server};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc};

fn request(server: &Server, session: Option<&str>, operation: &str, args: Value) -> Request {
    Request {
        version: ontography_app::protocol::VERSION,
        client_id: "extension-scope-test".into(),
        request_id: uuid::Uuid::new_v4().to_string(),
        operation: operation.into(),
        app_session_id: session.map(str::to_owned),
        expected_server_id: Some(server.service.server_id.clone()),
        args,
    }
}

async fn call(
    server: &Arc<Server>,
    session: Option<&str>,
    operation: &str,
    args: Value,
) -> Result<Value> {
    server
        .request(request(server, session, operation, args))
        .await
}

async fn create_session(server: &Arc<Server>, project: &Path) -> String {
    call(server, None, "session.create", json!({"project":project}))
        .await
        .unwrap()["session_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn start(server: &Arc<Server>, session: &str) -> String {
    let declaration: Value = serde_json::from_str(include_str!("../examples/flow.json")).unwrap();
    call(
        server,
        Some(session),
        "run.start",
        json!({"declaration":declaration}),
    )
    .await
    .unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn extension_respects_session_binding_lifecycle_and_receipt_scope() {
    let directory = tempfile::tempdir().unwrap();
    let server = Server::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let first = create_session(&server, directory.path()).await;
    let second = create_session(&server, directory.path()).await;
    let extension = json!({"extension":{"node_types":["Worker"]}});
    assert_eq!(
        call(&server, Some(&first), "run.extend", extension.clone())
            .await
            .unwrap_err()
            .code,
        "graph_uninitialized"
    );
    let first_run = start(&server, &first).await;
    let second_run = start(&server, &second).await;
    let before_first = call(&server, Some(&first), "run.inspect", json!({}))
        .await
        .unwrap();
    let before_second = call(&server, Some(&second), "run.inspect", json!({}))
        .await
        .unwrap();
    let mut foreign = extension.clone();
    foreign["run_id"] = json!(second_run);
    assert_eq!(
        call(&server, Some(&first), "run.extend", foreign)
            .await
            .unwrap_err()
            .code,
        "session_scope_conflict"
    );
    assert_eq!(
        call(&server, Some(&first), "run.inspect", json!({}))
            .await
            .unwrap()["revision"],
        before_first["revision"]
    );
    assert_eq!(
        call(&server, Some(&second), "run.inspect", json!({}))
            .await
            .unwrap()["vocabulary"],
        before_second["vocabulary"]
    );

    let accepted_request = request(&server, Some(&first), "run.extend", extension);
    let accepted = server.request(accepted_request.clone()).await.unwrap();
    assert_eq!(accepted["run_id"], first_run);
    assert_eq!(
        accepted["vocabulary"]["node_types"],
        json!(["Logical", "Worker"])
    );
    // A retry of the same accepted request recovers its receipt, rather than
    // attempting a second extension and rejecting its now-duplicate type.
    assert_eq!(
        server.request(accepted_request.clone()).await.unwrap(),
        accepted
    );
    assert_eq!(
        call(&server, Some(&first), "run.inspect", json!({}))
            .await
            .unwrap()["revision"],
        accepted["revision"]
    );
    let receipt =
        json!({"client_id":accepted_request.client_id,"request_id":accepted_request.request_id});
    assert_eq!(
        call(&server, Some(&first), "operation.get", receipt.clone())
            .await
            .unwrap()["result"],
        accepted
    );
    assert_eq!(
        call(&server, Some(&second), "operation.get", receipt)
            .await
            .unwrap_err()
            .code,
        "session_scope"
    );
    assert_eq!(
        call(&server, Some(&second), "run.inspect", json!({}))
            .await
            .unwrap()["vocabulary"],
        before_second["vocabulary"]
    );

    call(&server, Some(&first), "session.suspend", json!({}))
        .await
        .unwrap();
    let next = json!({"extension":{"node_types":["Reviewer"]}});
    assert_eq!(
        call(&server, Some(&first), "run.extend", next.clone())
            .await
            .unwrap_err()
            .code,
        "session_inactive"
    );
    call(&server, Some(&first), "session.resume", json!({}))
        .await
        .unwrap();
    assert_eq!(
        call(&server, Some(&first), "run.inspect", json!({}))
            .await
            .unwrap()["revision"],
        accepted["revision"]
    );
    call(&server, Some(&first), "run.extend", next.clone())
        .await
        .unwrap();
    call(&server, Some(&first), "session.close", json!({}))
        .await
        .unwrap();
    assert_eq!(
        call(&server, Some(&first), "run.extend", next)
            .await
            .unwrap_err()
            .code,
        "session_inactive"
    );
    server.service.shutdown().await.unwrap();
}

#[tokio::test]
async fn extension_wire_schema_rejects_invalid_nested_inputs_before_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let server = Server::new(paths.clone()).unwrap();
    let session = create_session(&server, directory.path()).await;
    let run_id = start(&server, &session).await;
    let before = call(&server, Some(&session), "run.inspect", json!({}))
        .await
        .unwrap();
    for args in [
        json!({}),
        json!({"extension":null}),
        json!({"extension":{"node_types":"Worker"}}),
        json!({"extension":{"node_types":[1]}}),
        json!({"extension":{"nodes":[]}}),
        json!({"extension":{"contracts":[{"id":"bytes","object_type":"Text","validator":"not-installed","validator_version":1}]}}),
        json!({"extension":{"contracts":[{"id":"bytes","object_type":"Text","validator":"utf8"}]}}),
        json!({"extension":{"contracts":[{"id":"bytes","object_type":"Text","validator":"utf8","validator_version":1,"unrecognized":true}]}}),
        json!({"extension":{"node_types":["Worker"]},"unrecognized":true}),
    ] {
        assert_eq!(
            call(&server, Some(&session), "run.extend", args.clone())
                .await
                .unwrap_err()
                .code,
            "invalid_arguments",
            "unexpected rejection for {args}"
        );
        assert!(!paths.run(&run_id).unwrap().join("extensions.json").exists());
    }
    assert_eq!(
        call(&server, Some(&session), "run.inspect", json!({}))
            .await
            .unwrap()["revision"],
        before["revision"]
    );
    let valid = call(&server,Some(&session),"run.extend",json!({"extension":{
        "node_types":["Worker"],"object_types":["Artifact"],"authority_tags":["review"],
        "contracts":[{"id":"artifact","object_type":"Artifact","validator":"opaque_bytes","validator_version":1}]}})).await.unwrap();
    assert_eq!(
        valid["vocabulary"]["object_types"],
        json!(["Artifact", "Text"])
    );
    assert!(
        valid["vocabulary"]["contracts"]
            .as_array()
            .unwrap()
            .contains(&json!({"id":"artifact","object_type":"Artifact"}))
    );
    server.service.shutdown().await.unwrap();
}
