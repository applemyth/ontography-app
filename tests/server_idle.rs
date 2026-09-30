//! The server exits once nothing runs and no client is connected.

use ontography_app::{client::Client, persistence::Paths, server};
use serde_json::json;
use std::time::Duration;

async fn connect(paths: &Paths) -> Client {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(client) = Client::connect(&paths.socket).await {
                return client;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the server must start")
}

#[tokio::test]
async fn an_idle_server_exits_while_an_open_run_keeps_it() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("store")).unwrap();
    let serving = tokio::spawn(server::serve(
        paths.clone(),
        Some(Duration::from_millis(300)),
    ));
    let client = connect(&paths).await;
    assert_eq!(
        client.call("system.hello", json!({})).await.unwrap()["idle"],
        true
    );

    // An open run keeps the server past its idle limit.
    let document: serde_json::Value =
        serde_json::from_str(include_str!("../examples/flow.json")).unwrap();
    let run = client
        .call(
            "flow.start",
            json!({"document":document,"project":directory.path()}),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!serving.is_finished());
    assert_eq!(
        client.call("system.hello", json!({})).await.unwrap()["idle"],
        false
    );

    // Once it is suspended nothing runs, and the server exits on its own.
    client
        .call("run.suspend", json!({"run_id":run["run_id"]}))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), serving)
        .await
        .expect("an idle server must exit")
        .unwrap()
        .unwrap();
    assert!(!paths.socket.exists());
}

#[tokio::test]
async fn commands_arriving_between_idle_checks_keep_the_server() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("store")).unwrap();
    let serving = tokio::spawn(server::serve(
        paths.clone(),
        Some(Duration::from_millis(600)),
    ));
    let client = connect(&paths).await;
    // Each command's connection lasts milliseconds, far shorter than the
    // interval between idle checks; the limit counts from the latest one.
    for _ in 0..12 {
        client.call("system.status", json!({})).await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    assert!(!serving.is_finished(), "a server in use must not exit");
    tokio::time::timeout(Duration::from_secs(10), serving)
        .await
        .expect("once commands stop, the server exits")
        .unwrap()
        .unwrap();
}
