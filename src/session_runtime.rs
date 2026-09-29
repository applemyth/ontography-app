//! Live manager ownership, separate from durable session records and core runs.
use crate::{
    AppError, Result, catalog::Operation, environment::Environment, managed_shell::ManagedShell,
    state::Service, tools,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

#[derive(Default)]
pub struct Managers {
    /// Serializes launch/stop so one session cannot acquire two manager processes.
    live: Mutex<BTreeMap<String, Arc<ManagedShell>>>,
}

impl Drop for Managers {
    fn drop(&mut self) {
        for shell in self.live.get_mut().values() {
            shell.request_stop();
        }
    }
}

pub fn operations() -> Vec<Operation> {
    let session = json!({"type":"string"});
    vec![
        Operation::new(
            "terminal.ensure",
            "Start or reuse the session shell. New shells launch Pi; existing shells retain their current foreground program. Attachment is separate.",
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
            "terminal.node",
            "Resolve the existing terminal for an agent node in this session's workflow. Does not start or resume workers.",
            json!({"session_id":session,"node":{"type":"string"},"rows":{"type":"integer","minimum":2,"maximum":200},"cols":{"type":"integer","minimum":10,"maximum":500}}),
            &["session_id", "node"],
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
    /// `environment` is the command line client's. Activating a session, or
    /// changing an active one that has none, gives it to the session; a change
    /// to work no session owns gives it to that work.
    pub async fn dispatch(
        &self,
        service: &Arc<Service>,
        scope: Option<&str>,
        operation: &str,
        args: &Value,
        environment: Option<Environment>,
    ) -> Result<Value> {
        if operation == "session.resume" {
            let id = session_id(scope, args)?;
            let mut managers = self.live.lock().await;
            // A shell that exited is suspended first, forgetting its session's
            // environment, so this resume brings its own.
            Self::reconcile_one(&mut managers, service, id).await?;
            if let Some(environment) = environment {
                service.offer_environment(id, environment, true).await;
            }
            return tools::dispatch_scoped(service, scope, operation, args).await;
        }
        match operation {
            "terminal.node" => {
                let id = session_id(scope, args)?;
                node_terminal(service, id, args).await
            }
            "terminal.ensure" => {
                let id = session_id(scope, args)?;
                let pi = PathBuf::from(args.get("pi").and_then(Value::as_str).unwrap_or("pi"));
                let running = self
                    .live
                    .lock()
                    .await
                    .get(id)
                    .is_some_and(|shell| shell.terminal.status().running);
                if !running {
                    // Checking Pi can take a while: not under the lock that
                    // every session's manager operations share.
                    ManagedShell::check_pi(service, id, &pi).await?;
                }
                let mut managers = self.live.lock().await;
                Self::reconcile_one(&mut managers, service, id).await?;
                if let Some(environment) = environment {
                    service.offer_environment(id, environment, true).await;
                }
                if let Some(terminal) = managers.get(id) {
                    // It may exit immediately after reconciliation, just as an
                    // attached terminal may exit at any moment. Retain ownership
                    // so the watcher settles that generation before replacement.
                    return terminal.status();
                }
                let terminal = ManagedShell::launch(
                    service.clone(),
                    id,
                    pi,
                    args.get("rows").and_then(Value::as_u64).unwrap_or(24) as u16,
                    args.get("cols").and_then(Value::as_u64).unwrap_or(80) as u16,
                )
                .await?;
                let status = terminal.status()?;
                managers.insert(id.into(), terminal);
                Ok(status)
            }
            "terminal.status" | "terminal.graph" | "terminal.detach" => {
                let id = session_id(scope, args)?;
                service.sessions.get(id).await?;
                let managers = self.live.lock().await;
                let Some(terminal) = managers.get(id) else {
                    if operation == "terminal.status" {
                        return Ok(
                            json!({"running":false,"session_id":id,"manager_mode":"shell","manager_pid":null}),
                        );
                    }
                    return Err(AppError::new(
                        "manager_not_running",
                        "attach to this session before opening its graph",
                    ));
                };
                if operation == "terminal.graph" {
                    terminal.terminal.request_graph_view()?;
                }
                if operation == "terminal.detach" {
                    terminal.terminal.request_detach()?;
                }
                Ok(terminal.status()?)
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
            _ => {
                let mutating = crate::catalog::operations()
                    .iter()
                    .any(|entry| entry.name == operation && entry.mutating);
                if let Some(environment) = environment.filter(|_| mutating) {
                    match session_id(scope, args) {
                        Ok(id) => service.offer_environment(id, environment, false).await,
                        Err(_) => service.set_environment(environment),
                    }
                }
                tools::dispatch_scoped(service, scope, operation, args).await
            }
        }
    }

    /// Called by the server independently of clients. Holding the manager lock
    /// fences replacement terminals until the old shell's graph is suspended.
    pub async fn reconcile(&self, service: &Service) -> Result<()> {
        let mut managers = self.live.lock().await;
        let exited = managers
            .iter()
            .filter(|(_, shell)| !shell.terminal.status().running)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let mut first = None;
        for id in exited {
            if let Err(error) = Self::reconcile_one(&mut managers, service, &id).await {
                first.get_or_insert(error);
            }
        }
        first.map_or(Ok(()), Err)
    }

    async fn reconcile_one(
        managers: &mut BTreeMap<String, Arc<ManagedShell>>,
        service: &Service,
        id: &str,
    ) -> Result<()> {
        let Some(shell) = managers.get(id) else {
            return Ok(());
        };
        if shell.terminal.status().running {
            return Ok(());
        }
        shell.shutdown().await?;
        tools::dispatch_scoped(service, Some(id), "session.suspend", &json!({})).await?;
        managers.remove(id);
        Ok(())
    }

    /// Whether any session's shell is running, or one is being started or
    /// stopped. Never waits: a launch holds the lock while its shell starts.
    pub fn any(&self) -> bool {
        self.live.try_lock().map_or(true, |live| !live.is_empty())
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

async fn node_terminal(service: &Service, session: &str, args: &Value) -> Result<Value> {
    use crate::{sessions::SessionStatus, workflow::runtime};
    let handle = service.sessions.get(session).await?;
    let record = handle.lock().await;
    if record.status != SessionStatus::Active {
        return Err(AppError::new(
            "session_inactive",
            "Resume this session before entering a node",
        ));
    }
    let run_id = record
        .run_id
        .as_ref()
        .ok_or_else(|| AppError::new("graph_uninitialized", "This session has no graph yet"))?;
    let handle = service.run(run_id).await?;
    let run = handle.lock().await;
    let state = runtime::load(&run)?;
    let name = crate::views::field(args, "node")?;
    let binding = state.binding(name).map_err(|_| {
        AppError::new(
            "node_not_found",
            format!("Node {name:?} is no longer in this workflow"),
        )
    })?;
    if !binding.implementation.has_terminal() {
        return Err(AppError::new(
            "node_no_terminal",
            format!("Node {name:?} has no interactive terminal"),
        ));
    }
    let terminal = run.live.as_ref()
        .and_then(|live| live.workers.get(&state.identities.nodes[name]))
        .and_then(|worker| worker.node.as_ref())
        .and_then(|runtime| runtime.terminal())
        .ok_or_else(|| AppError::new("node_not_running", format!("Node {name:?} has no running terminal; inspect its status or resume it through the manager")))?;
    Ok(serde_json::to_value(terminal.attachment(
        args["rows"].as_u64().unwrap_or(24) as u16,
        args["cols"].as_u64().unwrap_or(80) as u16,
    )?)?)
}
