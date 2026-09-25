use ontography_app::{persistence::Paths, protocol::Request, server::Server};
use serde_json::{Value, json};
use std::{os::unix::fs::PermissionsExt, sync::Arc};

async fn call(
    server: &Arc<Server>,
    session: Option<&str>,
    operation: &str,
    args: Value,
) -> ontography_app::Result<Value> {
    server
        .request(Request {
            version: ontography_app::protocol::VERSION,
            client_id: "manager-lifecycle-test".into(),
            request_id: uuid::Uuid::new_v4().to_string(),
            operation: operation.into(),
            app_session_id: session.map(str::to_owned),
            expected_server_id: Some(server.service.server_id.clone()),
            args,
        })
        .await
}

#[tokio::test]
async fn session_manager_is_unique_and_survives_its_request_owner() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("store")).unwrap();
    let server = Server::new(paths).unwrap();
    let pi = directory.path().join("pi-fixture");
    std::fs::write(&pi, "#!/bin/sh\nif [ \"$1\" = --version ]; then echo 0.85.1; exit; fi\nprintf 'manager ready\\r\\n'\nwhile IFS= read -r line; do printf '%s\\r\\n' \"$line\"; done\n").unwrap();
    std::fs::set_permissions(&pi, std::fs::Permissions::from_mode(0o700)).unwrap();
    let session = call(
        &server,
        None,
        "session.create",
        json!({"project":directory.path()}),
    )
    .await
    .unwrap();
    let id = session["session_id"].as_str().unwrap();
    let args = json!({"pi":pi,"rows":24,"cols":80});
    let (one, two) = tokio::join!(
        call(&server, Some(id), "terminal.ensure", args.clone()),
        call(&server, Some(id), "terminal.ensure", args)
    );
    let one = one.unwrap();
    let two = two.unwrap();
    assert_eq!(one["pid"], two["pid"]);
    assert_eq!(one["terminal_id"], two["terminal_id"]);
    assert_eq!(
        call(&server, Some(id), "terminal.status", json!({}))
            .await
            .unwrap()["running"],
        true
    );
    // Manager can run before a graph exists, and suspension must still reap it.
    assert!(session["run_id"].is_null());
    assert_eq!(
        call(&server, Some(id), "session.suspend", json!({}))
            .await
            .unwrap()["status"],
        "suspended"
    );
    assert_eq!(
        call(&server, Some(id), "terminal.status", json!({}))
            .await
            .unwrap()["running"],
        false
    );
    assert!(!std::path::Path::new(one["socket"].as_str().unwrap()).exists());
    assert_eq!(
        call(&server, Some(id), "terminal.ensure", json!({"pi":pi}))
            .await
            .unwrap_err()
            .code,
        "session_inactive"
    );
    call(&server, None, "session.resume", json!({"session_id":id}))
        .await
        .unwrap();
    let restarted = call(&server, Some(id), "terminal.ensure", json!({"pi":pi}))
        .await
        .unwrap();
    assert_ne!(one["terminal_id"], restarted["terminal_id"]);
    server.stop().await.unwrap();
    assert!(!std::path::Path::new(restarted["socket"].as_str().unwrap()).exists());
}
