//! Observe the executions that run a workflow's programs. Workers belong to
//! their run: flow edits and run suspension start and stop them.

use crate::{AppError, Result, catalog::Operation, state::Service, views};
use ontography::{ExecutionHandle, ExecutionStatus};
use serde_json::{Value, json};
use std::time::Duration;

pub fn operations() -> Vec<Operation> {
    let text = json!({"type":"string"});
    let execution = json!({"run_id":text,"execution_id":text});
    vec![
        Operation::new(
            "execution.list",
            "Inspect actual hosted execution instances independently of core admission status.",
            json!({"run_id":text}),
            &["run_id"],
            false,
        ),
        Operation::new(
            "execution.inspect",
            "Inspect execution status and explicitly self-reported activity.",
            execution.clone(),
            &["run_id", "execution_id"],
            false,
        ),
        Operation::new(
            "execution.activity",
            "Read self-reported activity; this is not a graph commit or proof of external work.",
            execution,
            &["run_id", "execution_id"],
            false,
        ),
        Operation::new(
            "execution.wait",
            "Wait up to timeout_ms for an execution to finish, without holding the run's operation lock.",
            json!({"run_id":text,"execution_id":text,"timeout_ms":{"type":"integer","minimum":1,"maximum":30000}}),
            &["run_id", "execution_id"],
            false,
        ),
    ]
}

pub async fn dispatch(service: &Service, operation: &str, args: &Value) -> Result<Value> {
    let run_id = views::field(args, "run_id")?;
    let run = service.run(run_id).await?;
    let run = run.lock().await;
    if operation == "execution.list" {
        return Ok(
            json!({"executions":run.live()?.executions.iter().map(|(id,handle)|inspect(id,handle)).collect::<Vec<_>>()}),
        );
    }
    let id = views::field(args, "execution_id")?;
    let handle = run.live()?.executions.get(id).cloned().ok_or_else(|| {
        AppError::new(
            "unknown_handle",
            "Execution is absent, expired, or belongs to another run",
        )
    })?;
    match operation {
        "execution.inspect" | "execution.activity" => Ok(inspect(id, &handle)),
        "execution.wait" => {
            let timeout = views::integer(args, "timeout_ms", 1000)?;
            if !(1..=30_000).contains(&timeout) {
                return Err(AppError::invalid("timeout_ms must be between 1 and 30000"));
            }
            // Waiting is observation, so it must not prevent stop/suspend from another client.
            drop(run);
            let timed_out = tokio::time::timeout(Duration::from_millis(timeout), handle.wait())
                .await
                .is_err();
            let mut result = inspect(id, &handle);
            result["timed_out"] = json!(timed_out);
            Ok(result)
        }
        _ => Err(AppError::new("unknown_operation", operation)),
    }
}

pub fn inspect(id: &str, handle: &ExecutionHandle) -> Value {
    let (status, failure) = match handle.status() {
        ExecutionStatus::Running => ("running", Value::Null),
        ExecutionStatus::Exited => ("exited", Value::Null),
        ExecutionStatus::Aborted => ("aborted", Value::Null),
        ExecutionStatus::Failed(error) => (
            "failed",
            json!({"class":error.class(),"message":error.message()}),
        ),
        ExecutionStatus::Panicked(message) => ("panicked", json!({"message":message})),
    };
    let activity = handle.activity();
    json!({"execution_id":id,"host_execution_id":handle.id().get().to_string(),"node_id":handle.node_id(),"status":status,"failure":failure,"activity":{"reported":activity.reported(),"invocations":activity.invocations().iter().map(|invocation|json!({"id":invocation.id().to_string(),"description":invocation.description(),"inputs":invocation.inputs().iter().map(ToString::to_string).collect::<Vec<_>>()})).collect::<Vec<_>>()}})
}

#[cfg(test)]
mod tests {
    use crate::{client::Client, persistence::Paths};
    use serde_json::{Value, json};
    use std::time::Duration;

    /// An outside client feeds a command worker, whose results reach an inbox.
    fn document() -> Value {
        json!({"name":"background","entry":"feed","nodes":[
            {"id":"feed","component":"external"},
            {"id":"worker","component":"command","config":{"argv":["/bin/sh","-c","sleep 0.05; cat"]}},
            {"id":"sink","component":"inbox"}
        ],"edges":[{"from":"feed","to":"worker"},{"from":"worker","to":"sink"}]})
    }

    fn feed(run_id: &str, text: &str) -> Value {
        json!({"run_id":run_id,"trigger":{"kind":"root","node_id":"feed","authority":["workflow"]},
            "result":text,"emissions":[{"edge_id":"feed:worker","payload":text}]})
    }

    fn worker_execution(inspected: &Value) -> Value {
        inspected["executions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|execution| execution["node_id"] == "worker")
            .unwrap()
            .clone()
    }

    #[tokio::test]
    async fn background_work_survives_clients_wait_does_not_block_suspend_and_resume_relaunches() {
        let directory = tempfile::tempdir().unwrap();
        let paths = Paths::initialize(directory.path().join("data")).unwrap();
        let socket = paths.socket.clone();
        let server = tokio::spawn(crate::server::serve(paths, None));
        let client = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match Client::connect(&socket).await {
                    Ok(client) => break client,
                    Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            }
        })
        .await
        .unwrap();
        let started = client
            .call(
                "flow.start",
                json!({"document":document(),"project":directory.path()}),
            )
            .await
            .unwrap();
        let run_id = started["run_id"].as_str().unwrap().to_owned();
        for text in ["one", "two", "three"] {
            client
                .call("workflow.submit", feed(&run_id, text))
                .await
                .unwrap();
        }
        drop(client);
        let client = Client::connect(&socket).await.unwrap();
        let observed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let status = client
                    .call("flow.status", json!({"run_id":run_id}))
                    .await
                    .unwrap();
                if status["frontier"]["counts"]["sink"]["received"] == 3 {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("work continues without clients");
        assert!(observed["nodes"].is_array());
        let inspected = client
            .call("run.inspect", json!({"run_id":run_id}))
            .await
            .unwrap();
        let execution_id = worker_execution(&inspected)["execution_id"].clone();
        let waiting_client = client.clone();
        let target = json!({"run_id":run_id,"execution_id":execution_id,"timeout_ms":5000});
        let waiting =
            tokio::spawn(async move { waiting_client.call("execution.wait", target).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        tokio::time::timeout(
            Duration::from_secs(5),
            client.call("run.suspend", json!({"run_id":run_id})),
        )
        .await
        .unwrap()
        .unwrap();
        let waited = waiting.await.unwrap().unwrap();
        assert_eq!(waited["timed_out"], false);
        let resumed = client
            .call("run.resume", json!({"run_id":run_id}))
            .await
            .unwrap();
        let relaunched = worker_execution(&resumed);
        assert_ne!(relaunched["execution_id"], execution_id);
        assert_eq!(relaunched["status"], "running");
        client.call("server.stop", json!({})).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
