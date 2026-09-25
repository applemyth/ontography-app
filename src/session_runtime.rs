//! Live manager ownership, separate from durable session records and core runs.
use crate::{
    AppError, Result,
    catalog::Operation,
    launcher::{self, PiSessionLaunch},
    sessions::SessionStatus,
    state::Service,
    terminal::{LaunchSpec, Terminal},
    tools,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

#[derive(Default)]
pub struct Managers {
    /// Serializes launch/stop so one session cannot acquire two manager processes.
    live: Mutex<BTreeMap<String, Arc<Terminal>>>,
}

pub fn operations() -> Vec<Operation> {
    let session = json!({"type":"string"});
    vec![
        Operation::new(
            "terminal.ensure",
            "Start or reuse the session's native Pi terminal. Attachment is separate.",
            json!({"session_id":session,"pi":{"type":"string"},"rows":{"type":"integer","minimum":2,"maximum":200},"cols":{"type":"integer","minimum":10,"maximum":500}}),
            &["session_id"],
            true,
        ),
        Operation::new(
            "terminal.status",
            "Inspect the session's manager process and terminal attachment.",
            json!({"session_id":session}),
            &["session_id"],
            false,
        ),
        Operation::new(
            "terminal.graph",
            "Show the owning session's graph in its controlling terminal client.",
            json!({"session_id":session}),
            &["session_id"],
            true,
        ),
        Operation::new(
            "terminal.detach",
            "Detach the controlling client while preserving the manager process and graph.",
            json!({"session_id":session}),
            &["session_id"],
            true,
        ),
    ]
}

fn session_id<'a>(scope: Option<&'a str>, args: &'a Value) -> Result<&'a str> {
    match (scope, args.get("session_id").and_then(Value::as_str)) {
        (Some(owner), Some(target)) if owner != target => Err(AppError::new(
            "session_scope",
            "operation targets a different Ontography session",
        )),
        (Some(owner), _) => Ok(owner),
        (_, Some(target)) => Ok(target),
        _ => Err(AppError::invalid("session_id is required")),
    }
}

impl Managers {
    pub async fn dispatch(
        &self,
        service: &Service,
        scope: Option<&str>,
        operation: &str,
        args: &Value,
    ) -> Result<Value> {
        match operation {
            "terminal.ensure" => {
                let id = session_id(scope, args)?;
                let mut managers = self.live.lock().await;
                if let Some(terminal) = managers.get(id)
                    && terminal.status().running
                {
                    return Ok(serde_json::to_value(terminal.status())?);
                }
                if let Some(old) = managers.get(id) {
                    old.shutdown().await?;
                }
                managers.remove(id);
                let handle = service.sessions.get(id).await?;
                let record = handle.lock().await.clone();
                if record.status != SessionStatus::Active {
                    return Err(AppError::new(
                        "session_inactive",
                        "resume the Ontography session before starting its manager",
                    ));
                }
                let active = record
                    .pi
                    .conversations
                    .get(&record.pi.active_conversation_id)
                    .ok_or_else(|| {
                        AppError::new("invalid_session", "active Pi conversation is missing")
                    })?;
                let path = match &active.path {
                    Some(path) if path.is_file() => Some(path.clone()),
                    _ if active.materialized => {
                        return Err(AppError::new(
                            "conversation_missing",
                            "saved Pi history is missing; restore it before resuming",
                        ));
                    }
                    _ => None,
                };
                let pi = PathBuf::from(args.get("pi").and_then(Value::as_str).unwrap_or("pi"));
                let command = launcher::pi_session_command(
                    &service.paths,
                    &record.project,
                    &pi,
                    &PiSessionLaunch {
                        session_id: id.into(),
                        conversations_dir: service.sessions.conversations_dir(id)?,
                        conversation_id: active.conversation_id.clone(),
                        conversation_path: path,
                    },
                )
                .await?;
                let command = command.as_std();
                let spec = LaunchSpec {
                    program: command.get_program().into(),
                    args: command
                        .get_args()
                        .map(|s| s.to_string_lossy().into_owned())
                        .collect(),
                    env: command
                        .get_envs()
                        .filter_map(|(key, value)| {
                            value.map(|value| {
                                (
                                    key.to_string_lossy().into_owned(),
                                    value.to_string_lossy().into_owned(),
                                )
                            })
                        })
                        .collect(),
                    cwd: record.project,
                    rows: args.get("rows").and_then(Value::as_u64).unwrap_or(24) as u16,
                    cols: args.get("cols").and_then(Value::as_u64).unwrap_or(80) as u16,
                    server_id: service.server_id.clone(),
                    session_id: id.into(),
                };
                let socket = service
                    .paths
                    .socket
                    .parent()
                    .expect("server socket has a parent")
                    .join(format!("pty-{id}.sock"));
                let terminal = Terminal::launch(spec, socket).await?;
                let status = serde_json::to_value(terminal.status())?;
                managers.insert(id.into(), terminal);
                Ok(status)
            }
            "terminal.status" | "terminal.graph" | "terminal.detach" => {
                let id = session_id(scope, args)?;
                service.sessions.get(id).await?;
                let managers = self.live.lock().await;
                let Some(terminal) = managers.get(id) else {
                    if operation == "terminal.status" {
                        return Ok(json!({"running":false,"session_id":id}));
                    }
                    return Err(AppError::new(
                        "manager_not_running",
                        "attach to this session before opening its graph",
                    ));
                };
                if operation == "terminal.graph" {
                    terminal.request_graph_view()?;
                }
                if operation == "terminal.detach" {
                    terminal.request_detach()?;
                }
                Ok(serde_json::to_value(terminal.status())?)
            }
            "session.suspend" | "session.close" => {
                let id = session_id(scope, args)?;
                let mut managers = self.live.lock().await;
                if let Some(terminal) = managers.get(id) {
                    terminal.shutdown().await?;
                }
                managers.remove(id);
                tools::dispatch_scoped(service, scope, operation, args).await
            }
            _ => tools::dispatch_scoped(service, scope, operation, args).await,
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        let mut managers = self.live.lock().await;
        let mut first = None;
        let ids = managers.keys().cloned().collect::<Vec<_>>();
        for id in ids {
            match managers[&id].shutdown().await {
                Ok(()) => {
                    managers.remove(&id);
                }
                Err(error) => {
                    first.get_or_insert(error);
                }
            }
        }
        first.map_or(Ok(()), Err)
    }
}
