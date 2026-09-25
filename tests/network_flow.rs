use ontography_app::{persistence::Paths, state::Service, tools};
use serde_json::{Value, json};

async fn call(service: &Service, operation: &str, args: Value) -> Value {
    tools::dispatch(service, operation, &args)
        .await
        .unwrap_or_else(|error| panic!("{operation}: {error}"))
}
async fn start(service: &Service, project: &std::path::Path) -> String {
    call(service,"run.start",json!({"declaration":serde_json::from_str::<Value>(include_str!("../examples/flow.json")).unwrap(),"project":project})).await["run_id"].as_str().unwrap().into()
}

#[tokio::test]
async fn package_closure_transfers_and_network_owners_release_for_resume() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("hello.txt"), "complete package contents\n").unwrap();
    std::os::unix::fs::symlink("hello.txt", source.join("link")).unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let sender = start(&service, directory.path()).await;
    let receiver = start(&service, directory.path()).await;
    let imported = call(
        &service,
        "workspace.import",
        json!({"run_id":sender,"path":source}),
    )
    .await;
    let root = imported["root"].clone();
    let endpoint = json!({"network":"direct","bind_addresses":["127.0.0.1:0"]});
    let served = call(
        &service,
        "network.serve_package",
        json!({"run_id":sender,"package":root,"endpoint":endpoint}),
    )
    .await;
    let ticket = served["tickets"][0]["ticket"].clone();
    let download = call(
        &service,
        "network.download",
        json!({"run_id":receiver,"ticket":ticket,"endpoint":endpoint}),
    )
    .await;
    let settled = call(
        &service,
        "network.wait",
        json!({"run_id":receiver,"download_id":download["download_id"],"timeout_ms":30000}),
    )
    .await;
    assert_eq!(settled["progress"]["state"], "complete", "{settled}");
    let resolved = call(
        &service,
        "package.resolve",
        json!({"run_id":receiver,"root":root}),
    )
    .await;
    assert!(
        resolved["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["path"] == "hello.txt")
    );
    let checkout = call(
        &service,
        "workspace.checkout",
        json!({"run_id":receiver,"root":root}),
    )
    .await;
    let path = std::path::Path::new(checkout["path"].as_str().unwrap());
    assert_eq!(
        std::fs::read_to_string(path.join("hello.txt")).unwrap(),
        "complete package contents\n"
    );
    assert_eq!(
        std::fs::read_link(path.join("link")).unwrap(),
        std::path::Path::new("hello.txt")
    );
    // Graceful suspension must settle providers, downloads and checkouts before reopening.
    call(&service, "run.suspend", json!({"run_id":sender})).await;
    call(&service, "run.resume", json!({"run_id":sender})).await;
    call(&service, "run.suspend", json!({"run_id":receiver})).await;
    call(&service, "run.resume", json!({"run_id":receiver})).await;
    let restored = call(
        &service,
        "package.resolve",
        json!({"run_id":receiver,"root":root}),
    )
    .await;
    assert_eq!(resolved["items"], restored["items"]);

    // This ticket now names a stopped provider; cancellation settles without that peer.
    let empty = start(&service, directory.path()).await;
    let interrupted = call(
        &service,
        "network.download",
        json!({"run_id":empty,"ticket":ticket,"endpoint":endpoint}),
    )
    .await;
    call(
        &service,
        "network.cancel",
        json!({"run_id":empty,"download_id":interrupted["download_id"]}),
    )
    .await;
    let stopped = call(
        &service,
        "network.wait",
        json!({"run_id":empty,"download_id":interrupted["download_id"],"timeout_ms":30000}),
    )
    .await;
    assert_eq!(stopped["progress"]["state"], "cancelled", "{stopped}");
    call(
        &service,
        "network.release",
        json!({"run_id":empty,"download_id":interrupted["download_id"]}),
    )
    .await;
    call(
        &service,
        "network.discard",
        json!({"run_id":empty,"ticket":ticket}),
    )
    .await;
    service.shutdown().await.unwrap();
}
