//! The server exits once nothing runs and no client is connected.

use ontography_app::{
    client::Client, persistence::Paths, registry::ImplementationRegistry, server,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};

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
    let serving = tokio::spawn(server::serve_until_idle(
        paths.clone(),
        Arc::new(ImplementationRegistry::default()),
        Duration::from_millis(300),
    ));
    let client = connect(&paths).await;
    assert_eq!(
        client.call("system.hello", json!({})).await.unwrap()["idle"],
        true
    );

    // An open run keeps the server past its idle limit.
    let declaration: serde_json::Value =
        serde_json::from_str(include_str!("../examples/flow.json")).unwrap();
    let run = client
        .call(
            "run.start",
            json!({"declaration":declaration,"project":directory.path()}),
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
