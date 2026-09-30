use crate::catalog::Operation;
use crate::state::{ManagedRun, Service};
use crate::{AppError, Result, views};
use serde_json::{Value, json};

pub mod content;
pub mod execution;
pub mod workflow;
pub mod workspace;

pub fn operations() -> Vec<Operation> {
    let mut operations = workflow::operations();
    operations.extend(execution::operations());
    operations.extend(content::operations());
    operations.extend(workspace::operations());
    operations.extend(crate::workflow::tools::operations());
    operations.extend(crate::sessions::operations());
    operations.extend(crate::session_runtime::operations());
    operations
}

pub async fn dispatch_scoped(
    service: &Service,
    app_session_id: Option<&str>,
    operation: &str,
    args: &Value,
) -> Result<Value> {
    match app_session_id {
        Some(id) => {
            let mut result = crate::sessions::dispatch_scoped(service, id, operation, args).await?;
            if operation == "system.hello" {
                result["operations"] = serde_json::to_value(crate::catalog::manager_operations())?;
            }
            Ok(result)
        }
        None => dispatch(service, operation, args).await,
    }
}

pub async fn dispatch(service: &Service, operation: &str, args: &Value) -> Result<Value> {
    match operation {
        name if name.starts_with("session.") => {
            crate::sessions::dispatch(service, operation, args).await
        }
        "inspect.wait_frontier" => workflow::dispatch_wait(service, args).await,
        name if name.starts_with("flow.") => {
            crate::workflow::tools::dispatch(service, operation, args).await
        }
        name if name.starts_with("execution.") => {
            execution::dispatch(service, operation, args).await
        }
        "system.hello" => Ok(
            json!({"protocol_version":crate::protocol::VERSION,"server_id":service.server_id,"app_version":env!("CARGO_PKG_VERSION"),"core_version":ontography::VERSION,"app_build":crate::APP_BUILD,"core_build":crate::CORE_BUILD,"operations":crate::catalog::operations()}),
        ),
        "system.status" => Ok(
            json!({"server_id":service.server_id,"process_id":std::process::id(),"runs":service.runs.lock().await.len(),"data_dir":service.paths.root,"recovery_errors":service.recovery_errors}),
        ),
        "run.list" => {
            let limit = views::limit(args)?;
            let after = args.get("after").and_then(Value::as_str).unwrap_or("");
            let runs = service
                .runs
                .lock()
                .await
                .iter()
                .filter(|(id, _)| id.as_str() > after)
                .take(limit)
                .map(|(_, run)| run.clone())
                .collect::<Vec<_>>();
            let mut summaries = Vec::new();
            for run in runs {
                summaries.push(run.lock().await.summary());
            }
            let next = if summaries.len() == limit {
                summaries.last().map(|run| run["run_id"].clone())
            } else {
                None
            };
            Ok(
                json!({"runs":summaries,"next_after":next,"recovery_errors":service.recovery_errors}),
            )
        }
        _ => {
            let id = views::field(args, "run_id")?;
            let run = service.run(id).await?;
            let mut run = run.lock().await;
            // Whatever this operation starts, it starts with the run's
            // current environment.
            run.environment = service.run_environment(id).await;
            dispatch_run(&mut run, operation, args).await
        }
    }
}

async fn dispatch_run(run: &mut ManagedRun, operation: &str, args: &Value) -> Result<Value> {
    match operation {
        "run.inspect" => run.inspect(views::limit(args)?).await,
        "run.resume" => {
            run.resume().await?;
            run.inspect(100).await
        }
        "run.suspend" | "run.close" => {
            run.suspend(operation == "run.close").await?;
            Ok(run.summary())
        }
        name if name.starts_with("workflow.") || name.starts_with("inspect.") => {
            workflow::dispatch(run, operation, args).await
        }
        name if name.starts_with("content.") => content::dispatch(run, operation, args).await,
        name if name.starts_with("workspace.") => workspace::dispatch(run, operation, args).await,
        _ => Err(AppError::new("unknown_operation", operation)),
    }
}
