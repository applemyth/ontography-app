use ontography_app::{persistence::Paths, state::Service, tools, workflow::runtime};
use serde_json::{Value, json};
use std::{path::Path, time::Duration};

/// Rejects inputs containing "bad" until a `fixed` file exists in the project.
const WORKER: &str = r#"read x; case "$x" in *bad*) [ -e fixed ] || { printf 'rejected %s' "$x" >&2; exit 3; };; esac; printf 'done %s' "$x""#;

/// A human feeder that sends each decision on to the worker and back to itself.
fn document(retry: Value) -> Value {
    json!({"name":"retries","entry":"feed","nodes":[
        {"id":"feed","kind":"human"},
        {"id":"work","kind":"command","config":{"argv":["/bin/sh","-c",WORKER]},"retry":retry},
        {"id":"done","kind":"inbox"}
    ],"edges":[{"from":"feed","to":"feed"},{"from":"feed","to":"work"},{"from":"work","to":"done"}]})
}

async fn call(service: &Service, operation: &str, args: Value) -> Value {
    tools::dispatch(service, operation, &args)
        .await
        .unwrap_or_else(|error| panic!("{operation}: {error}; {:?}", error.details))
}

async fn rejects(service: &Service, operation: &str, args: Value, code: &str) {
    let error = tools::dispatch(service, operation, &args)
        .await
        .unwrap_err();
    assert_eq!(error.code, code, "{operation}: {error}");
}

async fn start(service: &Service, project: &Path, retry: Value) -> String {
    call(
        service,
        "flow.start",
        json!({"document":document(retry),"project":project,"message":"seed"}),
    )
    .await["run_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// Decides the feeder's current task, delivering `message` to the worker.
async fn send(service: &Service, run_id: &str, message: &str) {
    let status = call(service, "flow.status", json!({"run_id":run_id})).await;
    let task = task(&status, "feed").expect("the feeder always has a task");
    call(
        service,
        "flow.decide",
        json!({"run_id":run_id,"node":"feed","task_id":task["task_id"],"message":message}),
    )
    .await;
}

async fn wait_status(service: &Service, run_id: &str, ready: impl Fn(&Value) -> bool) -> Value {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let status = call(service, "flow.status", json!({"run_id":run_id})).await;
            if ready(&status) {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("workflow did not reach the expected state")
}

fn task<'a>(status: &'a Value, node: &str) -> Option<&'a Value> {
    status["tasks"]
        .as_array()?
        .iter()
        .find(|task| task["node"] == node)
}

fn failure(status: &Value) -> Option<&Value> {
    status["failures"].as_array()?.first()
}

fn parked(status: &Value) -> bool {
    failure(status).is_some_and(|failure| failure["state"] == "parked")
}

fn node<'a>(status: &'a Value, name: &str) -> &'a Value {
    status["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["id"] == name)
        .unwrap()
}

fn delivered(status: &Value) -> bool {
    node(status, "done")["pending"] == 1
}

/// Attempts core recorded at a node, including one still in progress.
async fn attempts(service: &Service, run_id: &str, name: &str) -> usize {
    let run = service.run(run_id).await.unwrap();
    let run = run.lock().await;
    let node = runtime::load(&run).unwrap().identities.nodes[name].clone();
    run.live()
        .unwrap()
        .session
        .invocations_page(Some(&node), None, 100)
        .await
        .unwrap()
        .len()
}

async fn result(service: &Service, run_id: &str) -> Value {
    call(
        service,
        "flow.output",
        json!({"run_id":run_id,"node":"done"}),
    )
    .await["input"]["message"]
        .clone()
}

/// A changed node definition gives its failed tasks fresh attempts even when
/// the ledger cannot be written as the edit commits: the ledger keeps the
/// definition its failures belong to, so the next reconcile catches up.
#[tokio::test]
async fn a_definition_change_unparks_even_after_a_failed_ledger_write() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let run_id = start(&service, directory.path(), json!({"max_attempts":1})).await;
    send(&service, &run_id, "bad").await;
    let status = wait_status(&service, &run_id, parked).await;
    let ledger = {
        let run = service.run(&run_id).await.unwrap();
        let run = run.lock().await;
        let id = runtime::load(&run).unwrap().identities.nodes["work"].clone();
        runtime::node_directory(&run, &id).join("retry.json")
    };
    // The ledger cannot be replaced for a moment.
    std::fs::remove_file(&ledger).unwrap();
    std::fs::create_dir(&ledger).unwrap();
    let mut document = status["document"].clone();
    document["nodes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["id"] == "work")
        .unwrap()["config"]["argv"] =
        json!(["/bin/sh", "-c", "read x; printf 'accepted %s' \"$x\""]);
    let plan = call(
        &service,
        "flow.edit",
        json!({"run_id":run_id,"document":document}),
    )
    .await;
    let committed = tools::dispatch(
        &service,
        "flow.commit",
        &json!({"run_id":run_id,"plan_id":plan["plan_id"]}),
    )
    .await;
    // The edit is live, but its failures could not be forgotten yet.
    assert!(committed.is_err(), "{committed:?}");
    // The disk recovers; the manager resumes, as the error suggests.
    std::fs::remove_dir(&ledger).unwrap();
    call(&service, "flow.resume", json!({"run_id":run_id})).await;
    let mut status = Value::Null;
    for _ in 0..100 {
        status = call(&service, "flow.status", json!({"run_id":run_id})).await;
        if delivered(&status) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        delivered(&status),
        "the changed node's task is still {}",
        status["failures"]
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_attempts_park_a_task_and_retry_grants_a_fresh_set() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let run_id = start(
        &service,
        directory.path(),
        json!({"max_attempts":2,"initial_delay_secs":0}),
    )
    .await;
    send(&service, &run_id, "bad").await;
    let status = wait_status(&service, &run_id, parked).await;
    let failed = failure(&status).unwrap().clone();
    assert_eq!(failed["node"], "work");
    assert_eq!(failed["attempts"], 2);
    assert_eq!(failed["error"], "Worker exited with status 3: rejected bad");
    assert!(failed.get("retry_in_secs").is_none());
    assert_eq!(attempts(&service, &run_id, "work").await, 2);
    // The parked input stays pending and selectable, but is not the node's
    // next task, and the worker keeps running.
    assert_eq!(node(&status, "work")["pending"], 1);
    assert_eq!(node(&status, "work")["execution"]["state"], "running");
    assert!(task(&status, "work").is_none());
    let input = call(
        &service,
        "flow.output",
        json!({"run_id":run_id,"node":"work","task_id":failed["task_id"]}),
    )
    .await;
    assert_eq!(input["input"]["message"], "bad");
    let private: Vec<String> = {
        let run = service.run(&run_id).await.unwrap();
        let run = run.lock().await;
        let snapshot = run.live().unwrap().session.try_snapshot().await.unwrap();
        runtime::load(&run)
            .unwrap()
            .identities
            .nodes
            .into_values()
            .chain(snapshot.state().packages().keys().map(ToString::to_string))
            .collect()
    };
    let reported = status["failures"].to_string();
    assert!(
        private.iter().all(|id| !reported.contains(id)),
        "{reported}"
    );

    // A retry removes the record at once, so parking again takes two more.
    call(
        &service,
        "flow.retry",
        json!({"run_id":run_id,"node":"work","task_id":failed["task_id"]}),
    )
    .await;
    let status = wait_status(&service, &run_id, parked).await;
    assert_eq!(failure(&status).unwrap()["attempts"], 2);
    assert_eq!(attempts(&service, &run_id, "work").await, 4);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_parked_task_does_not_block_other_input() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let run_id = start(&service, directory.path(), json!({"max_attempts":1})).await;
    send(&service, &run_id, "bad").await;
    wait_status(&service, &run_id, parked).await;
    send(&service, &run_id, "good").await;
    let status = wait_status(&service, &run_id, delivered).await;
    assert_eq!(result(&service, &run_id).await, "done good");
    assert!(parked(&status));
    assert_eq!(failure(&status).unwrap()["attempts"], 1);
    assert_eq!(node(&status, "work")["pending"], 1);
    assert_eq!(attempts(&service, &run_id, "work").await, 2);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn backoff_is_reported_and_retry_attempts_again_immediately() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let run_id = start(
        &service,
        directory.path(),
        json!({"max_attempts":3,"initial_delay_secs":60}),
    )
    .await;
    send(&service, &run_id, "bad").await;
    let status = wait_status(&service, &run_id, |status| failure(status).is_some()).await;
    let failed = failure(&status).unwrap().clone();
    assert_eq!(failed["state"], "retrying");
    assert_eq!(failed["attempts"], 1);
    let wait = failed["retry_in_secs"].as_u64().unwrap();
    assert!((50..=60).contains(&wait), "{failed}");
    assert!(task(&status, "work").is_none());
    let selected = json!({"run_id":run_id,"node":"work","task_id":failed["task_id"]});
    rejects(
        &service,
        "flow.discard",
        selected.clone(),
        "task_not_parked",
    )
    .await;

    std::fs::write(directory.path().join("fixed"), "").unwrap();
    call(&service, "flow.retry", selected.clone()).await;
    let status = wait_status(&service, &run_id, delivered).await;
    assert_eq!(result(&service, &run_id).await, "done bad");
    assert!(failure(&status).is_none());
    rejects(&service, "flow.retry", selected, "stale_task").await;
    for (arguments, code) in [
        (
            json!({"node":"feed","task_id":failed["task_id"]}),
            "invalid_arguments",
        ),
        (
            json!({"node":"work","task_id":"work_0"}),
            "invalid_arguments",
        ),
        (
            json!({"node":"missing","task_id":failed["task_id"]}),
            "invalid_arguments",
        ),
    ] {
        let mut arguments = arguments;
        arguments["run_id"] = json!(run_id);
        rejects(&service, "flow.retry", arguments, code).await;
    }
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn discard_retires_a_parked_input_or_completes_the_initial_one() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let run_id = start(&service, directory.path(), json!({"max_attempts":1})).await;
    send(&service, &run_id, "bad").await;
    let status = wait_status(&service, &run_id, parked).await;
    let selected =
        json!({"run_id":run_id,"node":"work","task_id":failure(&status).unwrap()["task_id"]});
    let discarded = call(&service, "flow.discard", selected.clone()).await;
    assert!(failure(&discarded).is_none());
    assert_eq!(node(&discarded, "work")["pending"], 0);
    rejects(&service, "flow.discard", selected, "stale_task").await;
    {
        let run = service.run(&run_id).await.unwrap();
        let run = run.lock().await;
        let snapshot = run.live().unwrap().session.try_snapshot().await.unwrap();
        let retired = snapshot
            .state()
            .packages()
            .values()
            .filter(|package| package.retirement().is_some())
            .count();
        assert_eq!(retired, 1);
    }
    assert_eq!(attempts(&service, &run_id, "work").await, 1);

    // The run's initial input is not a package; discarding it completes it.
    let entry = json!({"name":"entry","entry":"work","nodes":[
        {"id":"work","kind":"command","config":{"argv":["/bin/sh","-c",WORKER]},"retry":{"max_attempts":1}}
    ]});
    let run_id = call(
        &service,
        "flow.start",
        json!({"document":entry,"project":directory.path(),"message":"bad"}),
    )
    .await["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let status = wait_status(&service, &run_id, parked).await;
    let discarded = call(
        &service,
        "flow.discard",
        json!({"run_id":run_id,"node":"work","task_id":failure(&status).unwrap()["task_id"]}),
    )
    .await;
    assert!(failure(&discarded).is_none());
    assert_eq!(discarded["tasks"], json!([]));
    {
        let run = service.run(&run_id).await.unwrap();
        assert!(!runtime::initial_pending(&*run.lock().await).await.unwrap());
    }
    assert_eq!(attempts(&service, &run_id, "work").await, 1);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_config_edit_gives_parked_tasks_fresh_attempts_with_the_new_settings() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let run_id = start(&service, directory.path(), json!({"max_attempts":1})).await;
    send(&service, &run_id, "bad").await;
    let status = wait_status(&service, &run_id, parked).await;
    let mut document = status["document"].clone();
    document["nodes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["id"] == "work")
        .unwrap()["config"]["argv"] =
        json!(["/bin/sh", "-c", "read x; printf 'accepted %s' \"$x\""]);
    let plan = call(
        &service,
        "flow.edit",
        json!({"run_id":run_id,"document":document}),
    )
    .await;
    call(
        &service,
        "flow.commit",
        json!({"run_id":run_id,"plan_id":plan["plan_id"]}),
    )
    .await;
    let status = wait_status(&service, &run_id, delivered).await;
    assert_eq!(result(&service, &run_id).await, "accepted bad");
    assert!(failure(&status).is_none());
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn parked_tasks_survive_suspension_restart_and_resume_until_retried() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let run_id = start(&service, directory.path(), json!({"max_attempts":1})).await;
    send(&service, &run_id, "bad").await;
    let before = failure(&wait_status(&service, &run_id, parked).await)
        .unwrap()
        .clone();
    call(&service, "run.suspend", json!({"run_id":run_id})).await;
    let resumed = call(&service, "flow.resume", json!({"run_id":run_id})).await;
    assert_eq!(failure(&resumed), Some(&before));
    service.shutdown().await.unwrap();
    drop(service);

    let service = Service::new(paths).unwrap();
    let resumed = call(&service, "flow.resume", json!({"run_id":run_id})).await;
    assert_eq!(failure(&resumed), Some(&before));
    // Other work proceeds after the restart without retrying the parked task.
    send(&service, &run_id, "good").await;
    let status = wait_status(&service, &run_id, delivered).await;
    assert_eq!(failure(&status), Some(&before));
    assert_eq!(attempts(&service, &run_id, "work").await, 2);

    // Resume restarts workers; only an explicit retry unparks tasks. Without a
    // task_id, it covers every failed task at the node.
    std::fs::write(directory.path().join("fixed"), "").unwrap();
    let resumed = call(&service, "flow.resume", json!({"run_id":run_id})).await;
    assert_eq!(failure(&resumed), Some(&before));
    call(
        &service,
        "flow.retry",
        json!({"run_id":run_id,"node":"work"}),
    )
    .await;
    let status = wait_status(&service, &run_id, |status| {
        node(status, "done")["pending"] == 2
    })
    .await;
    assert!(failure(&status).is_none());
    assert_eq!(node(&status, "work")["pending"], 0);
    service.shutdown().await.unwrap();
}
