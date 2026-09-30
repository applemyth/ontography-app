//! Work that can go no further leaves the rest of the run moving: a human
//! task no decision can satisfy is discarded, and many parked tasks keep
//! status, and so every change's reply, within bounds.

use ontography_app::{persistence::Paths, state::Service, tools};
use serde_json::{Value, json};
use std::{path::Path, time::Duration};

async fn call(service: &Service, operation: &str, args: Value) -> Value {
    tools::dispatch(service, operation, &args)
        .await
        .unwrap_or_else(|error| panic!("{operation} failed: {error}; {:?}", error.details))
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

/// Starts work at the external entry `client`, along `edge`.
async fn submit(service: &Service, run: &str, authority: Value, edge: &str, message: &str) {
    call(
        service,
        "workflow.submit",
        json!({"run_id":run,"trigger":{"kind":"root","node_id":"client","authority":authority},
            "result":message,"emissions":[{"edge_id":edge,"payload":message}]}),
    )
    .await;
}

async fn wait_status(service: &Service, run: &str, ready: impl Fn(&Value) -> bool) -> Value {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let status = call(service, "flow.status", json!({"run_id":run})).await;
            if ready(&status) {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the run did not reach the expected status")
}

fn human_task(status: &Value) -> Option<Value> {
    status["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|task| task["node"] == "review")
        .cloned()
}

/// Frontier counts leave out a node that has received nothing.
fn received(status: &Value, node: &str) -> u64 {
    status["frontier"]["counts"][node]["received"]
        .as_u64()
        .unwrap_or(0)
}

#[tokio::test]
async fn a_human_task_no_decision_can_satisfy_is_discarded_and_the_next_becomes_ready() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    // A decision goes through both connections, which admit no common
    // authority, and no transition changes the task's own.
    let document = json!({"name":"blocked","entry":"client","nodes":[
            {"id":"client","component":"external","root":["red"]},
            {"id":"review","component":"human"},
            {"id":"left","component":"inbox"},
            {"id":"right","component":"inbox"}
        ],"edges":[{"from":"client","to":"review","authority":["red"]},
            {"from":"review","to":"left","authority":["red"]},
            {"from":"review","to":"right","authority":["blue"]}]});
    let run = start(&service, directory.path(), document).await;
    for message in ["first", "second"] {
        submit(&service, &run, json!(["red"]), "client:review", message).await;
    }
    // Pending work is ordered by identity, not arrival.
    let status = call(&service, "flow.status", json!({"run_id":run})).await;
    let first = human_task(&status).expect("a task is ready");
    let next = if first["input"]["message"] == "first" {
        "second"
    } else {
        "first"
    };
    for authority in [None, Some(json!(["blue"]))] {
        let mut decision =
            json!({"run_id":run,"node":"review","task_id":first["task_id"],"message":"Approved"});
        if let Some(authority) = authority {
            decision["authority"] = authority;
        }
        let refused = tools::dispatch(&service, "flow.decide", &decision)
            .await
            .unwrap_err();
        assert_eq!(refused.code, "rejected", "{refused}");
        assert!(refused.message.contains("flow.discard"), "{refused}");
    }

    let discard = json!({"run_id":run,"node":"review","task_id":first["task_id"]});
    let discarded = call(&service, "flow.discard", discard.clone()).await;
    let second = human_task(&discarded).expect("the task behind it is ready");
    assert_eq!(second["input"]["message"], next);
    assert_ne!(second["task_id"], first["task_id"]);
    assert_eq!(received(&discarded, "review"), 1);
    assert_eq!(received(&discarded, "left"), 0);
    assert_eq!(received(&discarded, "right"), 0);
    let again = tools::dispatch(&service, "flow.discard", &discard)
        .await
        .unwrap_err();
    assert_eq!(again.code, "stale_task", "{again}");
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn status_lists_a_bounded_number_of_failures_and_counts_them_all() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let document = json!({"name":"failing","entry":"client","nodes":[
            {"id":"client","component":"external"},
            {"id":"fail","component":"command","config":{"argv":["/bin/sh","-c","exit 3"]},
                "retry":{"max_attempts":1}},
            {"id":"done","component":"inbox"}
        ],"edges":[{"from":"client","to":"fail"},{"from":"fail","to":"done"}]});
    let run = start(&service, directory.path(), document).await;
    const TASKS: usize = 105;
    for index in 0..TASKS {
        let message = format!("task {index}");
        submit(&service, &run, json!(["workflow"]), "client:fail", &message).await;
    }
    let status = wait_status(&service, &run, |status| status["failures_total"] == TASKS).await;
    let failures = status["failures"].as_array().unwrap();
    assert_eq!(failures.len(), 100);
    assert!(failures.iter().all(|failure| failure["state"] == "parked"));

    // A change's reply carries the same bounded status.
    let task = &failures[0]["task_id"];
    let discarded = call(
        &service,
        "flow.discard",
        json!({"run_id":run,"node":"fail","task_id":task}),
    )
    .await;
    assert_eq!(discarded["failures"].as_array().unwrap().len(), 100);
    assert_eq!(discarded["failures_total"], TASKS - 1);
    assert!(
        discarded["failures"]
            .as_array()
            .unwrap()
            .iter()
            .all(|failure| failure["task_id"] != *task)
    );
    service.shutdown().await.unwrap();
}
