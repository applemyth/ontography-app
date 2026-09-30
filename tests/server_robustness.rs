//! The server keeps the receipts clients may still need, and says when a
//! change applied though its result could not be sent.
#![cfg(unix)]

use ontography_app::{
    persistence::Paths,
    protocol::{self, Request},
    server::Server,
};
use serde_json::{Value, json};
use std::sync::Arc;

fn document() -> Value {
    serde_json::from_str(include_str!("../examples/flow.json")).unwrap()
}

async fn call(
    server: &Arc<Server>,
    request_id: &str,
    operation: &str,
    args: Value,
) -> ontography_app::Result<Value> {
    server
        .request(Request {
            environment: None,
            version: protocol::VERSION,
            client_id: "server-robustness-test".into(),
            request_id: request_id.into(),
            operation: operation.into(),
            app_session_id: None,
            expected_server_id: Some(server.service.server_id.clone()),
            args,
        })
        .await
}

async fn receipt(server: &Arc<Server>, request_id: &str) -> ontography_app::Result<Value> {
    let id = uuid::Uuid::new_v4().to_string();
    let args = json!({"client_id":"server-robustness-test","request_id":request_id});
    call(server, &id, "operation.get", args).await
}

#[tokio::test]
async fn a_full_receipt_table_evicts_the_oldest_completed_receipt() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let server = Server::new(paths.clone()).unwrap();
    // An error is an outcome a receipt keeps, and this one needs no disk.
    let suspend = json!({"run_id":uuid::Uuid::new_v4()});
    // Fill the table, each receipt completing before the next is accepted.
    // The newest has the smallest key and the oldest the largest.
    for index in (0..128).rev() {
        let error = call(
            &server,
            &format!("{index:03}"),
            "run.suspend",
            suspend.clone(),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "not_found");
    }
    call(&server, "new", "run.suspend", suspend)
        .await
        .unwrap_err();
    for kept in ["000", "001", "126", "new"] {
        assert_eq!(
            receipt(&server, kept).await.unwrap()["state"],
            "failed",
            "receipt {kept} must be kept"
        );
    }
    assert_eq!(
        receipt(&server, "127").await.unwrap_err().code,
        "unknown_outcome"
    );
    server.stop().await.unwrap();
    let _ = std::fs::remove_dir(paths.socket.parent().unwrap());
}

#[tokio::test]
async fn a_change_whose_result_is_too_large_says_it_was_applied() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let server = Server::new(paths.clone()).unwrap();
    // A run's status carries its document, here larger than a response may be.
    let mut document = document();
    document["nodes"].as_array_mut().unwrap().push(json!({
        "id":"C","component":"human",
        "config":{"prompt":"x".repeat(protocol::MAX_FRAME_BYTES / 2)}
    }));
    let error = call(
        &server,
        "start",
        "flow.start",
        json!({"document":document,"project":directory.path()}),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "result_too_large");
    assert_eq!(error.details, Some(json!({"committed":true})));
    assert!(error.message.contains("applied"), "{}", error.message);
    let runs = call(&server, "list", "run.list", json!({})).await.unwrap();
    let runs = runs["runs"].as_array().unwrap();
    assert_eq!(runs.len(), 1, "the run started");
    // Reading changes nothing, so says nothing of a change.
    let error = call(
        &server,
        "status",
        "flow.status",
        json!({"run_id":runs[0]["run_id"]}),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "result_too_large");
    assert_eq!(error.details, None);
    server.stop().await.unwrap();
    let _ = std::fs::remove_dir(paths.socket.parent().unwrap());
}
