use ontography_app::{persistence::Paths, sessions::GraphInitialization, state::Service, tools};
use serde_json::{Value, json};

fn declaration() -> Value {
    serde_json::from_str(include_str!("../examples/flow.json")).unwrap()
}

async fn create(service: &Service, project: &std::path::Path) -> String {
    tools::dispatch(service, "session.create", &json!({"project":project}))
        .await
        .unwrap()["session_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn scoped(
    service: &Service,
    session: &str,
    operation: &str,
    args: Value,
) -> ontography_app::Result<Value> {
    tools::dispatch_scoped(service, Some(session), operation, &args).await
}

#[tokio::test]
async fn sessions_bind_once_and_scope_every_run_dispatch_path() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let first = create(&service, directory.path()).await;
    let second = create(&service, directory.path()).await;
    let args = json!({"declaration":declaration()});
    let first_run = scoped(&service, &first, "run.start", args.clone())
        .await
        .unwrap();
    let second_run = scoped(&service, &second, "run.start", args.clone())
        .await
        .unwrap();
    assert_ne!(first_run["run_id"], second_run["run_id"]);
    let retried = scoped(&service, &first, "run.start", args.clone())
        .await
        .unwrap();
    assert_eq!(retried["run_id"], first_run["run_id"]);
    assert_eq!(
        tools::dispatch(&service, "run.list", &json!({}))
            .await
            .unwrap()["runs"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        scoped(&service, &first, "run.list", json!({}))
            .await
            .unwrap()["runs"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    for operation in ["run.inspect", "execution.list", "inspect.wait_frontier"] {
        let error = scoped(
            &service,
            &first,
            operation,
            json!({"run_id":second_run["run_id"]}),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "session_scope_conflict", "{operation}");
    }
    let error = scoped(
        &service,
        &first,
        "session.context",
        json!({"session_id":second}),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "session_scope_conflict");
    let other_project = directory.path().join("other");
    std::fs::create_dir(&other_project).unwrap();
    assert_eq!(
        scoped(
            &service,
            &first,
            "flow.start",
            json!({"project":other_project})
        )
        .await
        .unwrap_err()
        .code,
        "session_scope_conflict"
    );
    let mut different = declaration();
    different["id"] = json!("different");
    assert_eq!(
        scoped(
            &service,
            &first,
            "run.start",
            json!({"declaration":different})
        )
        .await
        .unwrap_err()
        .code,
        "graph_already_initialized"
    );
    assert_eq!(
        scoped(&service, &first, "session.suspend", json!({}))
            .await
            .unwrap()["status"],
        "suspended"
    );
    assert_eq!(
        scoped(&service, &first, "run.resume", json!({}))
            .await
            .unwrap_err()
            .code,
        "session_inactive"
    );
    assert_eq!(
        scoped(&service, &first, "session.resume", json!({}))
            .await
            .unwrap()["status"],
        "active"
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn graph_initialization_recovers_reserved_identity_before_and_after_run_creation() {
    for phase in 0..3 {
        let directory = tempfile::tempdir().unwrap();
        let paths = Paths::initialize(directory.path().join("data")).unwrap();
        let service = Service::new(paths.clone()).unwrap();
        let id = create(&service, directory.path()).await;
        let reserved = uuid::Uuid::new_v4().to_string();
        let args = json!({"declaration":declaration(),"project":std::fs::canonicalize(directory.path()).unwrap()});
        {
            let session = service.sessions.get(&id).await.unwrap();
            let mut record = session.lock().await;
            record.graph_initialization = Some(GraphInitialization {
                run_id: reserved.clone(),
                operation: "run.start".into(),
                args: args.clone(),
                definition: ontography_app::definition::RunDefinition::Logical(
                    serde_json::from_value(declaration()).unwrap(),
                ),
                input: None,
                workflow: None,
            });
            service.sessions.save(&record).unwrap();
        }
        if phase == 1 {
            // Crash after reserving the run directory, before writing its manifest.
            std::fs::create_dir(paths.run(&reserved).unwrap()).unwrap();
        }
        if phase == 2 {
            service
                .start_reserved(
                    &reserved,
                    serde_json::from_value(declaration()).unwrap(),
                    directory.path().into(),
                    service.environment.clone(),
                )
                .await
                .unwrap();
        }
        service.shutdown().await.unwrap();
        drop(service);
        let service = Service::new(paths).unwrap();
        let recovered = scoped(&service, &id, "session.resume", json!({}))
            .await
            .unwrap();
        assert_eq!(recovered["run_id"], reserved);
        let retry = scoped(&service, &id, "run.start", args).await.unwrap();
        assert_eq!(retry["run_id"], reserved);
        assert_eq!(
            tools::dispatch(&service, "run.list", &json!({}))
                .await
                .unwrap()["runs"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        service.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn adoption_is_explicit_exclusive_and_preserves_existing_run_identity() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let existing = tools::dispatch(
        &service,
        "run.start",
        &json!({"project":directory.path(),"declaration":declaration()}),
    )
    .await
    .unwrap();
    let first = create(&service, directory.path()).await;
    let second = create(&service, directory.path()).await;
    assert!(
        scoped(&service, &first, "session.context", json!({}))
            .await
            .unwrap()["graph"]
            .is_null()
    );
    let adopted = tools::dispatch(
        &service,
        "session.adopt",
        &json!({"session_id":first,"run_id":existing["run_id"]}),
    )
    .await
    .unwrap();
    assert_eq!(adopted["run_id"], existing["run_id"]);
    let rejected = tools::dispatch(
        &service,
        "session.adopt",
        &json!({"session_id":second,"run_id":existing["run_id"]}),
    )
    .await
    .unwrap_err();
    assert_eq!(rejected.code, "run_already_owned");
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_conversation_changes_preserve_graph_and_missing_history_stays_missing() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let id = create(&service, directory.path()).await;
    let started = scoped(
        &service,
        &id,
        "run.start",
        json!({"declaration":declaration()}),
    )
    .await
    .unwrap();
    let context = scoped(&service, &id, "session.context", json!({}))
        .await
        .unwrap();
    let original = context["session"]["pi"]["active_conversation_id"]
        .as_str()
        .unwrap();
    let initial_path = service
        .sessions
        .conversations_dir(&id)
        .unwrap()
        .join("initial.jsonl");
    scoped(
        &service,
        &id,
        "session.conversation",
        json!({"action":"activate","conversation_id":original,"path":initial_path}),
    )
    .await
    .unwrap();
    let fresh = uuid::Uuid::new_v4().to_string();
    let fresh_path = service
        .sessions
        .conversations_dir(&id)
        .unwrap()
        .join("fresh.jsonl");
    let registered = scoped(
        &service,
        &id,
        "session.conversation",
        json!({"action":"activate","conversation_id":fresh,"path":fresh_path}),
    )
    .await
    .unwrap();
    assert_eq!(registered["conversation"]["materialized"], false);
    std::fs::write(
        &fresh_path,
        format!("{}\n", json!({"type":"session","id":fresh,"version":3})),
    )
    .unwrap();
    let context = scoped(&service, &id, "session.context", json!({}))
        .await
        .unwrap();
    assert_eq!(
        context["session"]["pi"]["conversations"][&fresh]["materialized"],
        true
    );
    assert_eq!(context["graph"]["run_id"], started["run_id"]);
    assert_eq!(
        scoped(
            &service,
            &id,
            "session.conversation",
            json!({"action":"check","path":fresh_path})
        )
        .await
        .unwrap()["allowed"],
        true
    );
    let external = directory.path().join("external.jsonl");
    std::fs::write(
        &external,
        format!("{}\n", json!({"type":"session","id":uuid::Uuid::new_v4()})),
    )
    .unwrap();
    assert_eq!(
        scoped(
            &service,
            &id,
            "session.conversation",
            json!({"action":"check","path":external})
        )
        .await
        .unwrap_err()
        .code,
        "conversation_not_owned"
    );
    scoped(
        &service,
        &id,
        "session.preferences",
        json!({"preferences":{"tool_groups":["graph","run"]}}),
    )
    .await
    .unwrap();
    std::fs::remove_file(&fresh_path).unwrap();
    assert_eq!(
        scoped(
            &service,
            &id,
            "session.conversation",
            json!({"action":"activate","conversation_id":fresh,"path":fresh_path})
        )
        .await
        .unwrap_err()
        .code,
        "conversation_missing"
    );
    service.shutdown().await.unwrap();
    drop(service);
    let service = Service::new(paths).unwrap();
    let context = scoped(&service, &id, "session.context", json!({}))
        .await
        .unwrap();
    assert_eq!(context["session"]["pi"]["active_conversation_id"], fresh);
    assert_eq!(
        context["session"]["pi"]["preferences"]["tool_groups"],
        json!(["graph", "run"])
    );
}

#[tokio::test]
async fn sessions_without_graphs_suspend_close_and_reload_without_spawning_work() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let id = create(&service, directory.path()).await;
    assert_eq!(
        scoped(&service, &id, "session.suspend", json!({}))
            .await
            .unwrap()["status"],
        "suspended"
    );
    assert_eq!(
        scoped(&service, &id, "session.resume", json!({}))
            .await
            .unwrap()["status"],
        "active"
    );
    assert_eq!(
        scoped(&service, &id, "session.close", json!({}))
            .await
            .unwrap()["status"],
        "closed"
    );
    assert!(service.sessions.selected().await.is_none());
    drop(service);
    let service = Service::new(paths).unwrap();
    assert_eq!(
        scoped(&service, &id, "session.inspect", json!({}))
            .await
            .unwrap()["status"],
        "closed"
    );
    assert_eq!(
        scoped(&service, &id, "session.resume", json!({}))
            .await
            .unwrap_err()
            .code,
        "session_closed"
    );
}

#[tokio::test]
async fn scoped_observation_waits_do_not_hold_session_lifecycle_locks() {
    use ontography_app::registry::{ImplementationDescriptor, ImplementationRegistry};
    use std::{sync::Arc, time::Duration};
    let directory = tempfile::tempdir().unwrap();
    let mut registry = ImplementationRegistry::default();
    registry
        .register_executable(
            ImplementationDescriptor {
                id: "test.wait".into(),
                version: "1".into(),
                description: "Cooperative test worker".into(),
                configuration_schema: json!({"type":"object"}),
            },
            |_| {
                Ok(Arc::new(
                    |context: ontography::ExecutionContext| async move {
                        context.stop().requested().await;
                        Ok::<(), ontography::ExecutionFailure>(())
                    },
                ))
            },
        )
        .unwrap();
    let service = Service::with_registry(
        Paths::initialize(directory.path().join("data")).unwrap(),
        Arc::new(registry),
    )
    .unwrap();
    let id = create(&service, directory.path()).await;
    let mut declaration = declaration();
    declaration["execution_bindings"] = json!([{"id":"worker","node_id":"A","implementation":"test.wait","version":"1","configuration":{}}]);
    let run = scoped(
        &service,
        &id,
        "run.start",
        json!({"declaration":declaration}),
    )
    .await
    .unwrap();
    let execution_id = run["executions"][0]["execution_id"].clone();
    let (observed, suspended) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            scoped(
                &service,
                &id,
                "execution.wait",
                json!({"execution_id":execution_id,"timeout_ms":30000})
            ),
            async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                scoped(&service, &id, "session.suspend", json!({})).await
            },
        )
    })
    .await
    .expect("session suspend must complete while an execution observer waits");
    assert_eq!(suspended.unwrap()["status"], "suspended");
    assert_eq!(observed.unwrap()["timed_out"], false);
    scoped(&service, &id, "session.resume", json!({}))
        .await
        .unwrap();
    let current = scoped(&service, &id, "run.inspect", json!({}))
        .await
        .unwrap();
    let (observed, suspended) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            scoped(
                &service,
                &id,
                "inspect.wait_frontier",
                json!({"after_revision":current["revision"],"timeout_ms":30000})
            ),
            async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                scoped(&service, &id, "session.suspend", json!({})).await
            },
        )
    })
    .await
    .expect("session suspend must complete while a frontier observer waits");
    assert_eq!(suspended.unwrap()["status"], "suspended");
    assert_eq!(observed.unwrap()["timed_out"], false);
}

#[tokio::test]
async fn closed_selection_is_rejected_and_stale_selection_does_not_survive_recovery() {
    use ontography_app::{persistence::write_json, sessions::SessionStatus};
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let id = create(&service, directory.path()).await;
    let (selected, closed) = tokio::join!(
        service.sessions.select(&id),
        scoped(&service, &id, "session.close", json!({})),
    );
    if let Err(error) = selected {
        assert_eq!(error.code, "session_closed");
    }
    assert_eq!(closed.unwrap()["status"], "closed");
    assert!(service.sessions.selected().await.is_none());
    assert_eq!(
        service.sessions.select(&id).await.unwrap_err().code,
        "session_closed"
    );

    // A crash can publish the terminal lifecycle state before clearing selection.
    for status in [SessionStatus::Closing, SessionStatus::Closed] {
        let handle = service.sessions.get(&id).await.unwrap();
        let mut record = handle.lock().await;
        record.status = status;
        service.sessions.save(&record).unwrap();
        drop(record);
        write_json(
            &paths.root.join("sessions/selection.json"),
            &json!({"session_id":id}),
        )
        .unwrap();
        let recovered = Service::new(paths.clone()).unwrap();
        assert!(recovered.sessions.selected().await.is_none());
        assert_eq!(
            recovered.sessions.select(&id).await.unwrap_err().code,
            "session_closed"
        );
    }
}

#[tokio::test]
async fn relocated_store_keeps_native_histories_owned_through_its_old_alias() {
    let directory = tempfile::tempdir().unwrap();
    let old = directory.path().join("old");
    let destination = directory.path().join("new");
    let paths = Paths::initialize(&old).unwrap();
    let service = Service::new(paths).unwrap();
    let id = create(&service, directory.path()).await;
    let context = scoped(&service, &id, "session.context", json!({}))
        .await
        .unwrap();
    let conversation_id = context["session"]["pi"]["active_conversation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let saved_path = service
        .sessions
        .conversations_dir(&id)
        .unwrap()
        .join("saved.jsonl");
    std::fs::write(
        &saved_path,
        format!(
            "{}\n",
            json!({"type":"session","id":conversation_id,"version":3})
        ),
    )
    .unwrap();
    scoped(
        &service,
        &id,
        "session.conversation",
        json!({"action":"activate","conversation_id":conversation_id,"path":saved_path}),
    )
    .await
    .unwrap();
    let empty_id = uuid::Uuid::new_v4().to_string();
    let empty_path = service
        .sessions
        .conversations_dir(&id)
        .unwrap()
        .join("unmaterialized.jsonl");
    scoped(
        &service,
        &id,
        "session.conversation",
        json!({"action":"activate","conversation_id":empty_id,"path":empty_path}),
    )
    .await
    .unwrap();
    service.shutdown().await.unwrap();
    drop(service);

    // Exercise the real cutover only under a disposable test directory.
    ontography_app::migration::migrate(&old, &destination).unwrap();
    let service = Service::new(Paths::initialize(&destination).unwrap()).unwrap();
    let restored = scoped(&service, &id, "session.context", json!({}))
        .await
        .unwrap();
    let histories = &restored["session"]["pi"]["conversations"];
    assert_eq!(
        histories[&conversation_id]["path"],
        json!(std::fs::canonicalize(&saved_path).unwrap())
    );
    assert_eq!(histories[&conversation_id]["materialized"], true);
    assert_eq!(histories[&empty_id]["materialized"], false);
    // Native resume may supply either the new name or the retained old alias.
    assert_eq!(
        scoped(
            &service,
            &id,
            "session.conversation",
            json!({"action":"check","path":saved_path})
        )
        .await
        .unwrap()["allowed"],
        true
    );
    assert_eq!(
        scoped(
            &service,
            &id,
            "session.conversation",
            json!({"action":"activate","conversation_id":empty_id,"path":empty_path})
        )
        .await
        .unwrap()["allowed"],
        true
    );
    std::fs::remove_file(&saved_path).unwrap();
    assert_eq!(
        scoped(
            &service,
            &id,
            "session.conversation",
            json!({"action":"activate","conversation_id":conversation_id,"path":saved_path})
        )
        .await
        .unwrap_err()
        .code,
        "conversation_missing"
    );
}

#[tokio::test]
async fn scoped_handshake_advertises_workflow_tools_without_raw_graph_operations() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let session = create(&service, directory.path()).await;
    let hello = scoped(&service, &session, "system.hello", json!({}))
        .await
        .unwrap();
    let names = hello["operations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|operation| operation["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(names.contains(&"flow.start"));
    assert!(names.contains(&"flow.edit"));
    assert!(names.contains(&"session.conversation"));
    assert!(!names.contains(&"graph.save"));
    assert!(!names.contains(&"rewrite.commit"));
    assert!(!names.contains(&"workflow.submit"));
    assert!(!names.contains(&"invocation.begin"));
    assert!(!names.contains(&"content.import_bytes"));
    service.shutdown().await.unwrap();
}
