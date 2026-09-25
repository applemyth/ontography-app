//! Bind registered opaque executables to the core execution host.

use crate::{AppError, Result, catalog::Operation, state::Service, views};
use ontography::{ExecutionHandle, ExecutionStatus};
use serde_json::{Value, json};
use std::time::Duration;

pub fn operations() -> Vec<Operation> {
    let text = json!({"type":"string"});
    let mut operations = vec![
        Operation::new(
            "execution.launch",
            "Launch a registered, versioned executable at a current graph node through core's host.",
            json!({"run_id":text,"node_id":text,"implementation":text,"version":text,"configuration":{}}),
            &["run_id", "node_id", "implementation", "version"],
            true,
        ),
        Operation::new(
            "execution.list",
            "Inspect actual hosted execution instances independently of core admission status.",
            json!({"run_id":text}),
            &["run_id"],
            false,
        ),
    ];
    for (name, description, mutating) in [
        (
            "inspect",
            "Inspect execution status and explicitly self-reported activity.",
            false,
        ),
        (
            "activity",
            "Read self-reported activity; this is not a graph commit or proof of external work.",
            false,
        ),
        (
            "stop",
            "Request cooperative stop; external process cleanup belongs to the registered adapter.",
            true,
        ),
        (
            "abort",
            "Abort the core executable future; external effects may require adapter cleanup.",
            true,
        ),
        (
            "release",
            "Release a terminal execution handle; running instances must first stop.",
            true,
        ),
    ] {
        operations.push(Operation::new(
            &format!("execution.{name}"),
            description,
            json!({"run_id":text,"execution_id":text}),
            &["run_id", "execution_id"],
            mutating,
        ));
    }
    operations.push(Operation::new("execution.wait","Wait up to timeout_ms for an execution to finish, without holding the run's operation lock.",json!({"run_id":text,"execution_id":text,"timeout_ms":{"type":"integer","minimum":1,"maximum":30000}}),&["run_id","execution_id"],false));
    operations
}

pub async fn dispatch(service: &Service, operation: &str, args: &Value) -> Result<Value> {
    let run_id = views::field(args, "run_id")?;
    let run = service.run(run_id).await?;
    let mut run = run.lock().await;
    if operation == "execution.launch" {
        let implementation = views::field(args, "implementation")?;
        let version = views::field(args, "version")?;
        let configuration = args
            .get("configuration")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let definition =
            service
                .registry
                .executable(implementation, version, configuration.clone())?;
        let node = views::field(args, "node_id")?;
        let live = run.live_mut()?;
        let handle = live
            .host
            .launch_arc(node, definition)
            .await
            .map_err(AppError::core)?;
        let id = uuid::Uuid::new_v4().to_string();
        let mut value = inspect(&id, &handle);
        value["implementation"] = json!(implementation);
        value["version"] = json!(version);
        value["configuration"] = configuration;
        value["lifetime"] = json!("manual");
        live.bindings.push(json!({"execution_id":id,"node_id":node,"implementation":implementation,"version":version,"configuration":value["configuration"],"lifetime":"manual"}));
        live.executions.insert(id, handle);
        return Ok(value);
    }
    if operation == "execution.list" {
        let live = run.live()?;
        return Ok(
            json!({"executions":live.executions.iter().map(|(id,handle)|with_metadata(inspect(id,handle),live.bindings.iter().find(|record|record["execution_id"].as_str()==Some(id.as_str())))).collect::<Vec<_>>()}),
        );
    }
    let id = views::field(args, "execution_id")?;
    let handle = run.live()?.executions.get(id).cloned().ok_or_else(|| {
        AppError::new(
            "unknown_handle",
            "Execution is absent, expired, or belongs to another run",
        )
    })?;
    let metadata = run
        .live()?
        .bindings
        .iter()
        .find(|record| record["execution_id"].as_str() == Some(id))
        .cloned();
    match operation {
        "execution.inspect" | "execution.activity" => {
            Ok(with_metadata(inspect(id, &handle), metadata.as_ref()))
        }
        "execution.stop" => {
            handle.request_stop();
            Ok(with_metadata(inspect(id, &handle), metadata.as_ref()))
        }
        "execution.abort" => {
            handle.abort();
            Ok(with_metadata(inspect(id, &handle), metadata.as_ref()))
        }
        "execution.release" => {
            if !handle.status().is_terminal() {
                return Err(AppError::new(
                    "execution_running",
                    "Stop the executable before releasing its handle",
                ));
            }
            run.live_mut()?.executions.remove(id);
            run.live_mut()?
                .bindings
                .retain(|record| record["execution_id"].as_str() != Some(id));
            Ok(json!({"released":true}))
        }
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
            let mut result = with_metadata(inspect(id, &handle), metadata.as_ref());
            result["timed_out"] = json!(timed_out);
            Ok(result)
        }
        _ => Err(AppError::new("unknown_operation", operation)),
    }
}

fn with_metadata(mut observation: Value, metadata: Option<&Value>) -> Value {
    if let Some(metadata) = metadata {
        for key in [
            "binding_id",
            "implementation",
            "version",
            "configuration",
            "lifetime",
        ] {
            if let Some(value) = metadata.get(key) {
                observation[key] = value.clone();
            }
        }
    }
    observation
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
    use super::*;
    use crate::{
        client::Client,
        persistence::Paths,
        registry::{ImplementationDescriptor, ImplementationRegistry},
    };
    use ontography::{
        ActivationProposal, Authority, ExecutionContext, ExecutionFailure, ProposalDecision,
    };
    use std::sync::Arc;

    /// This fixture is only registered by this test, never shipped as a node type.
    fn ticker_registry() -> Arc<ImplementationRegistry> {
        let mut registry = ImplementationRegistry::default();
        registry.register_executable(ImplementationDescriptor{id:"test.ticker".into(),version:"1".into(),description:"Test-only root emitter".into(),configuration_schema:json!({"type":"object"})},|_|{
            Ok(Arc::new(|context:ExecutionContext|async move {
                let mut interval=tokio::time::interval(Duration::from_millis(30));
                let mut stop=context.stop();
                loop {tokio::select!{
                    _=stop.requested()=>return Ok(()),
                    _=interval.tick()=>{
                        let proposal=ActivationProposal::root(context.node_id(),Authority::new([]),Arc::from(&b"tick"[..]));
                        match context.submit(proposal).await.map_err(|error|ExecutionFailure::new("fixture",error.to_string()))?{
                            ProposalDecision::Committed(_)=>{},
                            ProposalDecision::Rejected(reason)=>return Err(ExecutionFailure::new("fixture",reason.to_string())),
                        }
                    }
                }}
            }))
        }).unwrap();
        Arc::new(registry)
    }

    #[tokio::test]
    async fn background_core_work_survives_clients_wait_does_not_block_stop_and_resume_relaunches()
    {
        let directory = tempfile::tempdir().unwrap();
        let paths = Paths::initialize(directory.path().join("data")).unwrap();
        let socket = paths.socket.clone();
        let server = tokio::spawn(crate::server::serve_with_registry(paths, ticker_registry()));
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
        let declaration = json!({"version":1,"id":"background-fixture","schema":{"node_types":["Node"],"object_types":["Result"],"authority_tags":[]},"contracts":[{"id":"result","object_type":"Result","validator":"opaque_bytes","validator_version":1}],"nodes":[{"id":"worker","types":["Node"],"result_contract":"result"}],"edges":[],"roots":[{"node_id":"worker","ceiling":[]}],"execution_bindings":[{"id":"ticker","node_id":"worker","implementation":"test.ticker","version":"1","configuration":{}}]});
        let started = client
            .call(
                "run.start",
                json!({"declaration":declaration,"project":directory.path()}),
            )
            .await
            .unwrap();
        let run_id = started["run_id"].as_str().unwrap().to_owned();
        let execution_id = started["executions"][0]["execution_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let before = started["revision"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        drop(client);
        tokio::time::sleep(Duration::from_millis(150)).await;
        let client = Client::connect(&socket).await.unwrap();
        let observed = client
            .call("run.inspect", json!({"run_id":run_id}))
            .await
            .unwrap();
        assert!(
            observed["revision"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
                > before,
            "no-client interval must still commit real core activations"
        );
        let waiting_client = client.clone();
        let target = json!({"run_id":run_id,"execution_id":execution_id,"timeout_ms":5000});
        let waiting =
            tokio::spawn(async move { waiting_client.call("execution.wait", target).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        tokio::time::timeout(
            Duration::from_secs(2),
            client.call(
                "execution.stop",
                json!({"run_id":run_id,"execution_id":execution_id}),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        let waited = waiting.await.unwrap().unwrap();
        assert_eq!(waited["status"], "exited");
        assert_eq!(waited["lifetime"], "declared");
        assert_eq!(waited["implementation"], "test.ticker");
        client
            .call("run.suspend", json!({"run_id":run_id}))
            .await
            .unwrap();
        let resumed = client
            .call("run.resume", json!({"run_id":run_id}))
            .await
            .unwrap();
        assert_ne!(resumed["executions"][0]["execution_id"], execution_id);
        assert_eq!(resumed["executions"][0]["status"], "running");
        client.call("server.stop", json!({})).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
