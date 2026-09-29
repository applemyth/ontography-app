//! A session's programs start with the environment of the command that
//! activated it, never with the server's or an enclosing agent session's.

use ontography_app::{
    environment::Environment, persistence::Paths, protocol::Request, server::Server,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};

async fn call(
    server: &Arc<Server>,
    session: Option<&str>,
    operation: &str,
    args: Value,
    environment: Option<BTreeMap<String, String>>,
) -> Value {
    server
        .request(Request {
            version: ontography_app::protocol::VERSION,
            client_id: "session-environment-test".into(),
            request_id: uuid::Uuid::new_v4().to_string(),
            operation: operation.into(),
            app_session_id: session.map(str::to_owned),
            expected_server_id: Some(server.service.server_id.clone()),
            args,
            environment,
        })
        .await
        .unwrap_or_else(|error| panic!("{operation} failed: {error}"))
}

/// A command typed inside an agent session, whose own variables must not
/// reach the programs it starts.
fn client(marker: &str) -> BTreeMap<String, String> {
    let mut vars = Environment::current().vars().clone();
    vars.insert("SESSION_MARKER".into(), marker.into());
    vars.insert("CLAUDE_CODE_MESSAGING_TOKEN".into(), "secret".into());
    vars
}

async fn marker(server: &Server, session: &str) -> Option<String> {
    server
        .service
        .session_environment(session)
        .await
        .get("SESSION_MARKER")
        .map(str::to_owned)
}

async fn session(server: &Arc<Server>, project: &Path) -> String {
    call(
        server,
        None,
        "session.create",
        json!({"project":project}),
        None,
    )
    .await["session_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// Starts a workflow, for `session` or for none, whose one command reports
/// what it was started with, and returns that report.
async fn probe(
    server: &Arc<Server>,
    session: Option<&str>,
    project: &Path,
    environment: Option<BTreeMap<String, String>>,
) -> String {
    let probe = r#"printf '%s|%s' "$SESSION_MARKER" "${CLAUDE_CODE_MESSAGING_TOKEN:-none}""#;
    let document = json!({"name":"environment","entry":"probe","nodes":[
        {"id":"probe","component":"command","config":{"argv":["/bin/sh","-c",probe]}}]});
    let started = call(
        server,
        session,
        "flow.start",
        json!({"document":document,"message":"go","project":project}),
        environment,
    )
    .await;
    let run = started["run_id"].clone();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let output = call(
                server,
                session,
                "flow.output",
                json!({"run_id":run,"node":"probe"}),
                None,
            )
            .await
            .to_string();
            if output.contains('|') {
                return output;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the command must run")
}

#[tokio::test]
async fn session_programs_start_with_the_activating_commands_environment() {
    let directory = tempfile::tempdir().unwrap();
    let server = Server::new(Paths::initialize(directory.path().join("store")).unwrap()).unwrap();
    let id = session(&server, directory.path()).await;
    call(
        &server,
        Some(&id),
        "session.resume",
        json!({}),
        Some(client("first")),
    )
    .await;
    assert_eq!(marker(&server, &id).await.as_deref(), Some("first"));

    // The session's command worker runs with it, filtered.
    let output = probe(&server, Some(&id), directory.path(), None).await;
    assert!(output.contains("first|none"), "{output}");

    // Attaching again never changes a running session's environment.
    call(
        &server,
        Some(&id),
        "session.resume",
        json!({}),
        Some(client("other")),
    )
    .await;
    assert_eq!(marker(&server, &id).await.as_deref(), Some("first"));

    // A suspended session forgets it; the next activation brings its own.
    call(&server, Some(&id), "session.suspend", json!({}), None).await;
    assert_eq!(marker(&server, &id).await, None);
    call(
        &server,
        Some(&id),
        "session.resume",
        json!({}),
        Some(client("second")),
    )
    .await;
    assert_eq!(marker(&server, &id).await.as_deref(), Some("second"));
    server.stop().await.unwrap();
}

#[tokio::test]
async fn a_scripted_start_gives_an_active_session_its_commands_environment() {
    let directory = tempfile::tempdir().unwrap();
    let server = Server::new(Paths::initialize(directory.path().join("store")).unwrap()).unwrap();
    let id = session(&server, directory.path()).await;
    let output = probe(
        &server,
        Some(&id),
        directory.path(),
        Some(client("scripted")),
    )
    .await;
    assert!(output.contains("scripted|none"), "{output}");
    server.stop().await.unwrap();
}

#[tokio::test]
async fn reading_or_changing_a_suspended_session_never_gives_it_an_environment() {
    let directory = tempfile::tempdir().unwrap();
    let server = Server::new(Paths::initialize(directory.path().join("store")).unwrap()).unwrap();
    let id = session(&server, directory.path()).await;
    // Reading an active session gives it nothing.
    call(
        &server,
        Some(&id),
        "session.inspect",
        json!({}),
        Some(client("reader")),
    )
    .await;
    assert_eq!(marker(&server, &id).await, None);
    // Nor does changing a suspended one: only resuming it does.
    call(&server, Some(&id), "session.suspend", json!({}), None).await;
    call(
        &server,
        Some(&id),
        "session.preferences",
        json!({"preferences":{"theme":"dark"}}),
        Some(client("agent")),
    )
    .await;
    assert_eq!(marker(&server, &id).await, None);
    call(
        &server,
        Some(&id),
        "session.resume",
        json!({}),
        Some(client("attacher")),
    )
    .await;
    assert_eq!(marker(&server, &id).await.as_deref(), Some("attacher"));
    server.stop().await.unwrap();
}

#[tokio::test]
async fn a_run_no_session_owns_starts_with_the_command_that_started_it() {
    let directory = tempfile::tempdir().unwrap();
    let server = Server::new(Paths::initialize(directory.path().join("store")).unwrap()).unwrap();
    let output = probe(&server, None, directory.path(), Some(client("unowned"))).await;
    assert!(output.contains("unowned|none"), "{output}");
    server.stop().await.unwrap();
}
