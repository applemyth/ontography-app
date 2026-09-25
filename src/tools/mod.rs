use crate::catalog::Operation;
use crate::declarations::{GraphDeclaration, RewriteRequestDeclaration};
use crate::persistence::{read_json, write_json};
use crate::state::{ManagedRun, RewriteHandle, Service};
use crate::{AppError, Result, views};
use serde_json::{Value, json};

pub mod content;
pub mod context;
pub mod execution;
pub mod facts;
pub mod network;
pub mod project;
pub mod workflow;
pub mod workspace;

pub fn operations() -> Vec<Operation> {
    let mut operations = workflow::operations();
    operations.extend(context::operations());
    operations.extend(network::operations());
    operations.extend(execution::operations());
    operations.extend(project::operations());
    operations.extend(content::operations());
    operations.extend(workspace::operations());
    operations.extend(facts::operations());
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
        Some(id) => crate::sessions::dispatch_scoped(service, id, operation, args).await,
        None => dispatch(service, operation, args).await,
    }
}

pub async fn dispatch(service: &Service, operation: &str, args: &Value) -> Result<Value> {
    match operation {
        name if name.starts_with("session.") => {
            crate::sessions::dispatch(service, operation, args).await
        }
        "network.wait" => network::wait(service, args).await,
        "inspect.wait_frontier" => workflow::dispatch_wait(service, args).await,
        "run.export_facts" | "run.restore_facts" | "run.verify" => {
            facts::dispatch(service, operation, args).await
        }
        name if name.starts_with("execution.") => {
            execution::dispatch(service, operation, args).await
        }
        name if name.starts_with("project.") => project::dispatch(service, operation, args).await,
        "system.hello" => Ok(
            json!({"protocol_version":crate::protocol::VERSION,"server_id":service.server_id,"app_version":env!("CARGO_PKG_VERSION"),"core_version":ontography::VERSION,"app_build":crate::APP_BUILD,"core_build":crate::CORE_BUILD,"operations":crate::catalog::operations()}),
        ),
        "system.status" => Ok(
            json!({"server_id":service.server_id,"process_id":std::process::id(),"runs":service.runs.lock().await.len(),"data_dir":service.paths.root,"recovery_errors":service.recovery_errors}),
        ),
        "catalog.list" => {
            let mut catalog = service.registry.catalog();
            catalog["validators"] = json!([{"id":"opaque_bytes","version":1,"description":"Accepts arbitrary bytes"},{"id":"utf8","version":1,"description":"Accepts valid UTF-8 bytes"}]);
            catalog["deferred"] = json!([
                "Codex worker executable adapter",
                "semantic Message and Workspace union contracts",
                "node MCP and worker transport receipts"
            ]);
            Ok(catalog)
        }
        "graph.validate" | "graph.save" => {
            let declaration: GraphDeclaration = serde_json::from_value(
                args.get("declaration")
                    .cloned()
                    .ok_or_else(|| AppError::invalid("declaration is required"))?,
            )?;
            let revision = declaration.fingerprint().map_err(AppError::core)?;
            if operation == "graph.validate" {
                let compiled = declaration
                    .compile()
                    .map_err(|e| AppError::new("invalid_definition", e.to_string()))?;
                service
                    .registry
                    .validate_bindings(&declaration.execution_bindings, &compiled.kernel)?;
                Ok(
                    json!({"definition_id":declaration.id,"revision":revision,"graph":views::graph(&compiled.kernel),"valid":true}),
                )
            } else {
                write_json(&service.paths.definition(&revision)?, &declaration)?;
                Ok(json!({"definition_id":declaration.id,"revision":revision,"validated":false}))
            }
        }
        "graph.get" => Ok(serde_json::to_value(read_json::<GraphDeclaration>(
            &service.paths.definition(views::field(args, "revision")?)?,
        )?)?),
        "graph.import" | "graph.export" => {
            let project = std::path::Path::new(views::field(args, "project")?);
            if !project.is_absolute() || !project.is_dir() {
                return Err(AppError::invalid(
                    "project must be an absolute existing directory",
                ));
            }
            let path = std::path::PathBuf::from(views::field(args, "path")?);
            let path = if path.is_absolute() {
                path
            } else {
                project.join(path)
            };
            if operation == "graph.import" {
                if std::fs::metadata(&path)?.len() > 16 * 1024 * 1024 {
                    return Err(AppError::invalid("declaration file exceeds 16MiB"));
                }
                let declaration = GraphDeclaration::parse(&std::fs::read_to_string(&path)?)
                    .map_err(|e| AppError::invalid(e.to_string()))?;
                let revision = declaration.fingerprint().map_err(AppError::core)?;
                write_json(&service.paths.definition(&revision)?, &declaration)?;
                Ok(json!({"definition_id":declaration.id,"revision":revision,"validated":false}))
            } else {
                let declaration: GraphDeclaration =
                    read_json(&service.paths.definition(views::field(args, "revision")?)?)?;
                write_json(&path, &declaration)?;
                Ok(json!({"path":path,"revision":views::field(args,"revision")?}))
            }
        }
        "graph.list" => {
            let limit = views::limit(args)?;
            let after = args.get("after").and_then(Value::as_str).unwrap_or("");
            let mut definitions = Vec::new();
            let mut revisions = std::fs::read_dir(service.paths.root.join("definitions"))?
                .map(|entry| entry.map(|e| e.path()))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            revisions.sort();
            for path in revisions {
                if path.extension().and_then(|s| s.to_str()) != Some("json") {
                    continue;
                }
                let revision = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
                if revision <= after {
                    continue;
                }
                let declaration: GraphDeclaration = read_json(&path)?;
                definitions.push(json!({"definition_id":declaration.id,"revision":revision}));
                if definitions.len() == limit {
                    break;
                }
            }
            let next = if definitions.len() == limit {
                definitions.last().map(|d| d["revision"].clone())
            } else {
                None
            };
            Ok(json!({"definitions":definitions,"next_after":next}))
        }
        "run.start" => {
            let declaration = match (args.get("declaration"), args.get("revision")) {
                (Some(d), None) => serde_json::from_value(d.clone())?,
                (None, Some(Value::String(revision))) => {
                    read_json(&service.paths.definition(revision)?)?
                }
                _ => {
                    return Err(AppError::invalid(
                        "supply exactly one of declaration or revision",
                    ));
                }
            };
            let project = std::path::PathBuf::from(views::field(args, "project")?);
            if !project.is_absolute() {
                return Err(AppError::invalid("project must be an absolute directory"));
            }
            service.start(declaration, project).await
        }
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
            let run = service.run(views::field(args, "run_id")?).await?;
            let mut run = run.lock().await;
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
        "rewrite.list" => Ok(json!({"productions":run.manifest.declaration.rewrites()})),
        "rewrite.prepare" => {
            let run_id = run.manifest.run_id.clone();
            let request: RewriteRequestDeclaration = serde_json::from_value(
                args.get("request")
                    .cloned()
                    .ok_or_else(|| AppError::invalid("request is required"))?,
            )?;
            let id = uuid::Uuid::new_v4().to_string();
            let live = run.live_mut()?;
            let plan = live
                .session
                .prepare_rewrite(&request.compile())
                .await
                .map_err(transition_error)?;
            let summary = json!({"run_id":run_id,"plan_id":id,"revision":plan.revision().to_string(),"base_revision":plan.revision().to_string(),"graph":views::graph(plan.next_kernel()),"retirements":plan.retirements().iter().map(|(id,reason)|json!({"package_id":id.to_string(),"reason":format!("{reason:?}")})).collect::<Vec<_>>()});
            live.rewrites.insert(
                id,
                RewriteHandle {
                    plan,
                    summary: summary.clone(),
                },
            );
            Ok(summary)
        }
        "rewrite.inspect" => run
            .live()?
            .rewrites
            .get(views::field(args, "plan_id")?)
            .map(|p| p.summary.clone())
            .ok_or_else(|| {
                AppError::new(
                    "unknown_handle",
                    "rewrite plan is absent, consumed, expired, or belongs to another run",
                )
            }),
        "rewrite.discard" | "rewrite.commit" => {
            let live = run.live_mut()?;
            let handle = live
                .rewrites
                .remove(views::field(args, "plan_id")?)
                .ok_or_else(|| {
                    AppError::new(
                        "unknown_handle",
                        "rewrite plan is absent, consumed, expired, or belongs to another run",
                    )
                })?;
            if operation == "rewrite.discard" {
                return Ok(json!({"discarded":true}));
            }
            let result = live
                .session
                .commit_rewrite(handle.plan)
                .await
                .map_err(transition_error)?;
            Ok(
                json!({"revision":result.revision().to_string(),"retirements":result.retirements().iter().map(|(id,reason)|json!({"package_id":id.to_string(),"reason":format!("{reason:?}")})).collect::<Vec<_>>()}),
            )
        }
        name if name.starts_with("workflow.") || name.starts_with("inspect.") => {
            workflow::dispatch(run, operation, args).await
        }
        name if name.starts_with("context.") || name.starts_with("invocation.") => {
            context::dispatch(run, operation, args).await
        }
        name if name.starts_with("network.") => network::dispatch(run, operation, args).await,
        name if name.starts_with("content.") || name.starts_with("package.") => {
            content::dispatch(run, operation, args).await
        }
        name if name.starts_with("workspace.") => workspace::dispatch(run, operation, args).await,
        _ => Err(AppError::new("unknown_operation", operation)),
    }
}

fn transition_error(error: ontography::SessionTransitionError) -> AppError {
    use ontography::SessionTransitionError as E;
    let code = match &error {
        E::Stale => "stale",
        E::ForeignSession => "foreign_session",
        E::Rewrite(_) | E::Transfer(_) => "rejected",
        _ => "core_error",
    };
    AppError::new(code, error.to_string())
}
