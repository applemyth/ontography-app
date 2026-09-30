use ontography_app::{
    persistence::{self, Paths},
    sessions::{
        Conversation, GraphInitialization, SavedGraphInitialization, SessionRecord, SessionStatus,
        Sessions,
    },
    state::Service,
    tools,
};
use serde_json::{Value, json};

fn document() -> Value {
    serde_json::from_str(include_str!("../examples/flow.json")).unwrap()
}

fn legacy_intent(run_id: &str, operation: &str) -> Value {
    let application = operation == "project.start";
    let declaration = if application {
        json!({"version":1,"format":"project","document":"{\"id\":\"saved-application\"}",
            "rewrites":[],"definition_id":run_id,"registry":{},"resolution_fingerprint":"saved-provider-resolution"})
    } else {
        json!({"version":1,"id":run_id,
            "schema":{"node_types":["Stage"],"object_types":["Message"],"authority_tags":["work"]},
            "contracts":[{"id":"message","object_type":"Message","validator":"text","validator_version":1}],
            "nodes":[{"id":"A","types":["Stage"],"result_contract":"message","ingress_mode":"any"}],
            "edges":[],"roots":[{"node_id":"A","ceiling":["work"]}],
            "authority_transitions":[],"rewrites":[],"execution_bindings":[]})
    };
    let mut intent = json!({
        "run_id":run_id,"operation":operation,"args":{"project":"/saved/project"},
        "definition":{"kind":if application {"application"} else {"logical"},
            "declaration":declaration},
        "input":if application {json!([104,105])} else {Value::Null},
    });
    if operation == "flow.start" {
        // Old workflow metadata remains opaque, including its superseded field names.
        let document = json!({"name":"old","entry":"A",
            "nodes":[{"id":"A","kind":"human","config":{},"join":"any"}],"edges":[]});
        let identities = json!({"nodes":{"A":"A"},"edges":{}});
        intent["workflow"] = json!({"state":{"version":1,"current":document,
            "identities":identities,"pending":{"id":"saved-edit","base_version":1,
                "base_core_revision":0,"document":document,"identities":identities,
                "retirements":{},"steps":0}},"input":{"message":"retained input"}});
    }
    intent
}

async fn saved_session(paths: &Paths, intent: Option<Value>, bound: bool) -> SessionRecord {
    let sessions = Sessions::open(paths).unwrap();
    let mut record = sessions
        .create(paths.root.clone(), Some("pipeline-test".into()))
        .await
        .unwrap();
    record.status = SessionStatus::Suspended;
    if let Some(intent) = intent {
        if bound {
            record.run_id = Some(intent["run_id"].as_str().unwrap().into());
        }
        record.graph_initialization = Some(serde_json::from_value(intent).unwrap());
    }
    let conversation_id = record.pi.active_conversation_id.clone();
    let path = sessions
        .conversations_dir(&record.session_id)
        .unwrap()
        .join("history.jsonl");
    std::fs::write(&path, format!("{}\n{}\n",
        json!({"type":"session","id":conversation_id,"version":3}),
        json!({"type":"message","id":"retained-message","message":{"role":"user","content":"Keep this history"}}),
    )).unwrap();
    record.pi.conversations.insert(
        conversation_id.clone(),
        Conversation {
            conversation_id,
            path: Some(path),
            materialized: true,
        },
    );
    record.pi.preferences = json!({"model":"saved-model","tool_groups":["graph"]});
    sessions.save(&record).unwrap();
    record
}

#[tokio::test]
async fn old_initialization_formats_preserve_session_selection_history_and_claims() {
    for operation in ["flow.start", "run.start", "project.start"] {
        for bound in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let paths = Paths::initialize(directory.path().join("data")).unwrap();
            let run_id = uuid::Uuid::new_v4().to_string();
            let intent = legacy_intent(&run_id, operation);
            let record = saved_session(&paths, Some(intent.clone()), bound).await;
            let manifest = paths
                .root
                .join("sessions")
                .join(&record.session_id)
                .join("session.json");
            let bytes = std::fs::read(&manifest).unwrap();

            let sessions = Sessions::open(&paths).unwrap();
            assert!(sessions.recovery_errors.is_empty());
            assert_eq!(
                sessions.selected().await.as_deref(),
                Some(record.session_id.as_str())
            );
            assert_eq!(
                sessions.owner(&run_id).await.as_deref(),
                Some(record.session_id.as_str())
            );
            let loaded = sessions.get(&record.session_id).await.unwrap();
            let loaded = loaded.lock().await;
            assert_eq!(loaded.run_id, record.run_id);
            assert_eq!(
                serde_json::to_value(&loaded.pi).unwrap(),
                serde_json::to_value(&record.pi).unwrap()
            );
            assert_eq!(
                serde_json::to_value(&loaded.graph_initialization).unwrap(),
                intent
            );
            assert_eq!(
                std::fs::read(&manifest).unwrap(),
                bytes,
                "loading must not rewrite saved data"
            );
            let inspected = Sessions::inspect_saved(&paths, &record.session_id)
                .await
                .unwrap();
            assert_eq!(inspected["graph_initialization"]["run_id"], run_id);
            assert_eq!(inspected["graph_initialization"]["operation"], operation);
            assert_eq!(inspected["graph_initialization"]["status"], "unavailable");
            let _ = std::fs::remove_dir(paths.socket.parent().unwrap());
        }
    }
}

#[tokio::test]
async fn unavailable_graphs_do_not_hide_pi_or_create_replacement_runs() {
    // Cover old pending starts, completed starts, and adopted runs without an intent.
    for binding in ["pending", "bound", "adopted"] {
        for storage in ["absent", "missing_manifest", "old_manifest"] {
            let directory = tempfile::tempdir().unwrap();
            let paths = Paths::initialize(directory.path().join("data")).unwrap();
            let run_id = uuid::Uuid::new_v4().to_string();
            let intent = (binding != "adopted").then(|| legacy_intent(&run_id, "flow.start"));
            let mut record = saved_session(&paths, intent.clone(), binding == "bound").await;
            if binding == "adopted" {
                record.run_id = Some(run_id.clone());
                Sessions::open(&paths).unwrap().save(&record).unwrap();
            }
            let run_dir = paths.run(&run_id).unwrap();
            let old_manifest = json!({"version":1,"run_id":run_id,
                "definition":{"kind":"logical","declaration":{"id":"old"}},"project":paths.root});
            if storage != "absent" {
                std::fs::create_dir(&run_dir).unwrap();
                if storage == "old_manifest" {
                    persistence::write_json(&run_dir.join("manifest.json"), &old_manifest).unwrap();
                }
            }
            let history = record.pi.conversations[&record.pi.active_conversation_id]
                .path
                .as_ref()
                .unwrap();
            let history_bytes = std::fs::read(history).unwrap();
            let service = Service::new(paths.clone()).unwrap();
            assert!(service.sessions.recovery_errors.is_empty());
            assert_eq!(
                service.sessions.owner(&run_id).await.as_deref(),
                Some(record.session_id.as_str())
            );

            let context = tools::dispatch_scoped(
                &service,
                Some(&record.session_id),
                "session.context",
                &json!({}),
            )
            .await
            .unwrap();
            assert_eq!(
                context["session"]["pi"],
                serde_json::to_value(&record.pi).unwrap()
            );
            assert_eq!(context["graph"]["run_id"], run_id);
            assert_eq!(context["graph"]["status"], "unavailable");
            assert!(context["graph"]["error"]["code"].is_string());
            if binding != "pending" && storage != "absent" {
                assert_eq!(
                    context["graph"]["error"],
                    serde_json::to_value(service.recovery_error(&run_id).unwrap()).unwrap()
                );
            }
            let resumed = tools::dispatch_scoped(
                &service,
                Some(&record.session_id),
                "session.resume",
                &json!({}),
            )
            .await
            .unwrap();
            assert_eq!(resumed["status"], "active");
            assert_eq!(
                resumed["run_id"],
                serde_json::to_value(&record.run_id).unwrap()
            );
            assert_eq!(resumed["graph"]["status"], "unavailable");
            assert!(resumed["graph"]["error"].is_object());
            let checked = tools::dispatch_scoped(&service, Some(&record.session_id), "session.conversation",
                &json!({"action":"check","conversation_id":record.pi.active_conversation_id,"path":history})).await.unwrap();
            assert_eq!(checked["allowed"], true);
            let activated = tools::dispatch_scoped(&service, Some(&record.session_id), "session.conversation",
                &json!({"action":"activate","conversation_id":record.pi.active_conversation_id,"path":history})).await.unwrap();
            assert_eq!(
                activated["active_conversation_id"],
                record.pi.active_conversation_id
            );

            assert!(
                tools::dispatch(&service, "run.resume", &json!({"run_id":run_id}))
                    .await
                    .is_err()
            );
            let start = tools::dispatch_scoped(
                &service,
                Some(&record.session_id),
                "flow.start",
                &json!({"document":document()}),
            )
            .await
            .unwrap_err();
            assert_eq!(
                start.code,
                if binding == "adopted" {
                    "graph_already_initialized"
                } else {
                    "graph_unavailable"
                }
            );
            // Another session cannot steal even an unavailable, intent-only reservation.
            let other = service
                .sessions
                .create(paths.root.clone(), None)
                .await
                .unwrap();
            let stolen = tools::dispatch_scoped(
                &service,
                Some(&other.session_id),
                "flow.start",
                &json!({"document":document(),"start_id":run_id}),
            )
            .await
            .unwrap_err();
            assert_eq!(stolen.code, "run_already_owned");
            assert!(
                tools::dispatch(&service, "run.list", &json!({}))
                    .await
                    .unwrap()["runs"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(std::fs::read(history).unwrap(), history_bytes);
            if storage == "absent" {
                assert!(!run_dir.exists());
            } else if storage == "missing_manifest" {
                assert!(!run_dir.join("manifest.json").exists());
            } else {
                assert_eq!(
                    persistence::read_json::<Value>(&run_dir.join("manifest.json")).unwrap(),
                    old_manifest
                );
            }
            let loaded = service.sessions.get(&record.session_id).await.unwrap();
            assert_eq!(
                serde_json::to_value(&loaded.lock().await.graph_initialization).unwrap(),
                serde_json::to_value(intent).unwrap()
            );
            service.shutdown().await.unwrap();
            let _ = std::fs::remove_dir(paths.socket.parent().unwrap());
        }
    }
}

#[tokio::test]
async fn failed_session_save_does_not_activate_the_manager() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let run_id = uuid::Uuid::new_v4().to_string();
    let record = saved_session(&paths, Some(legacy_intent(&run_id, "flow.start")), true).await;
    let service = Service::new(paths.clone()).unwrap();
    let manifest = paths
        .root
        .join("sessions")
        .join(&record.session_id)
        .join("session.json");
    // Force publication to fail inside this disposable fixture.
    std::fs::remove_file(&manifest).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    let error = tools::dispatch_scoped(
        &service,
        Some(&record.session_id),
        "session.resume",
        &json!({}),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "io_error");
    assert_eq!(
        service
            .sessions
            .get(&record.session_id)
            .await
            .unwrap()
            .lock()
            .await
            .status,
        SessionStatus::Suspended
    );
    service.shutdown().await.unwrap();
    let _ = std::fs::remove_dir(paths.socket.parent().unwrap());
}

#[tokio::test]
async fn compatibility_keeps_current_intent_shape_and_rejects_malformed_records() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let run_id = uuid::Uuid::new_v4().to_string();
    let args = json!({"document":document(),"project":paths.root});
    let (definition, workflow) =
        ontography_app::workflow::tools::prepare_start(&service, &args, &run_id).unwrap();
    let current = GraphInitialization {
        run_id: run_id.clone(),
        args,
        definition,
        workflow,
    };
    let original = serde_json::to_value(&current).unwrap();
    let saved: SavedGraphInitialization = current.into();
    assert_eq!(serde_json::to_value(saved).unwrap(), original);
    let mut missing_workflow = original.clone();
    missing_workflow.as_object_mut().unwrap().remove("workflow");
    let mut bare_legacy = original;
    bare_legacy["operation"] = json!("flow.start");
    let legacy = legacy_intent(&run_id, "flow.start");
    let mutations = [
        ("operation", json!("unknown.start")),
        ("unexpected", json!(true)),
        ("definition", json!({"kind":"logical"})),
        ("definition", json!({"kind":"unknown","declaration":{}})),
        (
            "definition",
            json!({"kind":"logical","declaration":[],"extra":true}),
        ),
        (
            "definition",
            json!({"kind":"logical","declaration":{},"extra":true}),
        ),
    ];
    let mut malformed = vec![missing_workflow, bare_legacy];
    malformed.extend(mutations.into_iter().map(|(field, value)| {
        let mut intent = legacy.clone();
        intent[field] = value;
        intent
    }));
    for intent in malformed {
        assert!(
            serde_json::from_value::<SavedGraphInitialization>(intent.clone()).is_err(),
            "accepted malformed intent: {intent}"
        );
    }
    service.shutdown().await.unwrap();
    let _ = std::fs::remove_dir(paths.socket.parent().unwrap());
}

/// Paths under `directory`, without the server endpoint directory that
/// `Paths::initialize` creates in the system temp directory: no server runs here.
fn paths(directory: &std::path::Path) -> Paths {
    let paths = Paths::initialize(directory.join("data")).unwrap();
    let _ = std::fs::remove_dir(paths.socket.parent().unwrap());
    paths
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

/// A suspended session bound to a run that loads but cannot open: its
/// manifest names another core build, as after rebuilding core.
async fn incompatible_run(directory: &std::path::Path) -> (Paths, String, String) {
    let paths = paths(directory);
    let service = Service::new(paths.clone()).unwrap();
    let session = create(&service, directory).await;
    let run = scoped(
        &service,
        &session,
        "flow.start",
        json!({"document":document()}),
    )
    .await
    .unwrap();
    let run_id = run["run_id"].as_str().unwrap().to_owned();
    scoped(&service, &session, "session.suspend", json!({}))
        .await
        .unwrap();
    service.shutdown().await.unwrap();
    let manifest = paths.run(&run_id).unwrap().join("manifest.json");
    let mut saved: Value = persistence::read_json(&manifest).unwrap();
    saved["core_build"] = json!("another-build");
    persistence::write_json(&manifest, &saved).unwrap();
    (paths, session, run_id)
}

#[tokio::test]
async fn a_graph_that_cannot_resume_stays_reported() {
    let directory = tempfile::tempdir().unwrap();
    let (paths, session, _) = incompatible_run(directory.path()).await;
    let service = Service::new(paths.clone()).unwrap();
    let resumed = scoped(&service, &session, "session.resume", json!({}))
        .await
        .unwrap();
    assert_eq!(resumed["status"], "active");
    assert_eq!(resumed["graph"]["error"]["code"], "incompatible_run");
    // Later views keep the reason instead of showing an idle graph.
    for _ in 0..2 {
        let context = scoped(&service, &session, "session.context", json!({}))
            .await
            .unwrap();
        assert_eq!(context["graph"]["status"], "unavailable");
        assert_eq!(context["graph"]["error"]["code"], "incompatible_run");
    }
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn closing_around_a_run_that_cannot_open_completes() {
    let directory = tempfile::tempdir().unwrap();
    let (paths, session, run_id) = incompatible_run(directory.path()).await;
    let service = Service::new(paths.clone()).unwrap();
    scoped(&service, &session, "session.resume", json!({}))
        .await
        .unwrap();
    let closed = scoped(&service, &session, "session.close", json!({}))
        .await
        .unwrap();
    assert_eq!(closed["status"], "closed");
    // The run is left exactly as it was.
    let manifest: Value =
        persistence::read_json(&paths.run(&run_id).unwrap().join("manifest.json")).unwrap();
    assert_eq!(manifest["core_build"], "another-build");
    assert_eq!(manifest["status"], "suspended");
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn current_sessions_keep_runs_over_legacy_reservations() {
    // Load order follows random identities; the current session wins every time.
    for _ in 0..6 {
        let directory = tempfile::tempdir().unwrap();
        let paths = paths(directory.path());
        let run_id = uuid::Uuid::new_v4().to_string();
        let service = Service::new(paths.clone()).unwrap();
        let current = create(&service, directory.path()).await;
        scoped(
            &service,
            &current,
            "flow.start",
            json!({"document":document(),"start_id":run_id}),
        )
        .await
        .unwrap();
        service.shutdown().await.unwrap();
        // The legacy record was unreadable when the current session bound the run.
        let legacy = saved_session(&paths, Some(legacy_intent(&run_id, "flow.start")), false).await;

        let service = Service::new(paths.clone()).unwrap();
        assert!(service.sessions.recovery_errors.is_empty());
        assert_eq!(
            service.sessions.owner(&run_id).await.as_deref(),
            Some(current.as_str())
        );
        let context = scoped(&service, &legacy.session_id, "session.context", json!({}))
            .await
            .unwrap();
        assert_eq!(context["graph"]["error"]["code"], "graph_unavailable");
        // Closing the legacy session leaves the current session's run running.
        scoped(&service, &current, "session.resume", json!({}))
            .await
            .unwrap();
        scoped(&service, &legacy.session_id, "session.close", json!({}))
            .await
            .unwrap();
        let run = service.run(&run_id).await.unwrap();
        assert!(run.lock().await.live.is_some());
        service.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn reservations_hold_for_starts_outside_sessions() {
    let directory = tempfile::tempdir().unwrap();
    let paths = paths(directory.path());
    let run_id = uuid::Uuid::new_v4().to_string();
    saved_session(&paths, Some(legacy_intent(&run_id, "flow.start")), false).await;
    let service = Service::new(paths.clone()).unwrap();
    let error = tools::dispatch(
        &service,
        "flow.start",
        &json!({"document":document(),"project":directory.path(),"start_id":run_id}),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "run_already_owned");
    assert!(!paths.run(&run_id).unwrap().exists());
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_start_that_cannot_finish_leaves_the_session_resumable() {
    let directory = tempfile::tempdir().unwrap();
    let paths = paths(directory.path());
    let project = directory.path().join("project");
    std::fs::create_dir(&project).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let session = create(&service, &project).await;
    let run_id = uuid::Uuid::new_v4().to_string();
    let args = json!({"document":document(),"start_id":run_id});
    let (definition, workflow) =
        ontography_app::workflow::tools::prepare_start(&service, &args, &run_id).unwrap();
    {
        // A current start saved before its run could be created.
        let handle = service.sessions.get(&session).await.unwrap();
        let mut record = handle.lock().await;
        record.status = SessionStatus::Suspended;
        record.graph_initialization = Some(
            GraphInitialization {
                run_id: run_id.clone(),
                args,
                definition,
                workflow,
            }
            .into(),
        );
        service.sessions.save(&record).unwrap();
    }
    std::fs::remove_dir(&project).unwrap();

    let resumed = scoped(&service, &session, "session.resume", json!({}))
        .await
        .unwrap();
    assert_eq!(resumed["status"], "active");
    assert_eq!(resumed["graph"]["status"], "unavailable");
    let reason = resumed["graph"]["error"].clone();
    assert!(reason["code"].is_string());
    let context = scoped(&service, &session, "session.context", json!({}))
        .await
        .unwrap();
    assert_eq!(context["graph"]["error"], reason);
    // Graph operations report the same reason.
    let error = scoped(&service, &session, "run.inspect", json!({}))
        .await
        .unwrap_err();
    assert_eq!(json!(error.code), reason["code"]);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_failed_activation_starts_no_work() {
    let directory = tempfile::tempdir().unwrap();
    let paths = paths(directory.path());
    let service = Service::new(paths.clone()).unwrap();
    let session = create(&service, directory.path()).await;
    let run = scoped(
        &service,
        &session,
        "flow.start",
        json!({"document":document()}),
    )
    .await
    .unwrap();
    let run_id = run["run_id"].as_str().unwrap().to_owned();
    scoped(&service, &session, "session.suspend", json!({}))
        .await
        .unwrap();
    // Force publication to fail inside this disposable fixture.
    let manifest = paths
        .root
        .join("sessions")
        .join(&session)
        .join("session.json");
    std::fs::remove_file(&manifest).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    let error = scoped(&service, &session, "session.resume", json!({}))
        .await
        .unwrap_err();
    assert_eq!(error.code, "io_error");
    let record = service.sessions.get(&session).await.unwrap();
    assert_eq!(record.lock().await.status, SessionStatus::Suspended);
    let run = service.run(&run_id).await.unwrap();
    assert!(run.lock().await.live.is_none(), "no work starts");
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn graph_operations_report_why_a_graph_is_unavailable() {
    let directory = tempfile::tempdir().unwrap();
    let paths = paths(directory.path());
    // An adopted run that fails to load, and a legacy start that never ran.
    let run_id = uuid::Uuid::new_v4().to_string();
    let mut adopted = saved_session(&paths, None, false).await;
    adopted.run_id = Some(run_id.clone());
    Sessions::open(&paths).unwrap().save(&adopted).unwrap();
    let run_dir = paths.run(&run_id).unwrap();
    std::fs::create_dir(&run_dir).unwrap();
    persistence::write_json(
        &run_dir.join("manifest.json"),
        &json!({"version":1,"run_id":run_id}),
    )
    .unwrap();
    let legacy_run = uuid::Uuid::new_v4().to_string();
    let legacy = saved_session(
        &paths,
        Some(legacy_intent(&legacy_run, "flow.start")),
        false,
    )
    .await;

    let service = Service::new(paths.clone()).unwrap();
    let recovery = service.recovery_error(&run_id).unwrap();
    for session in [&adopted.session_id, &legacy.session_id] {
        scoped(&service, session, "session.resume", json!({}))
            .await
            .unwrap();
    }
    let error = scoped(&service, &adopted.session_id, "run.inspect", json!({}))
        .await
        .unwrap_err();
    assert_eq!(
        (error.code.as_str(), error.message.as_str()),
        (recovery.code.as_str(), recovery.message.as_str())
    );
    let listing = scoped(&service, &adopted.session_id, "run.list", json!({}))
        .await
        .unwrap();
    assert_eq!(
        listing["recovery_errors"][&run_id]["code"],
        json!(recovery.code)
    );
    let error = scoped(&service, &legacy.session_id, "run.inspect", json!({}))
        .await
        .unwrap_err();
    assert_eq!(error.code, "graph_unavailable");
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_saved_starts_report_their_own_error() {
    let directory = tempfile::tempdir().unwrap();
    let paths = paths(directory.path());
    let service = Service::new(paths.clone()).unwrap();
    let run_id = uuid::Uuid::new_v4().to_string();
    let args = json!({"document":document(),"project":paths.root});
    let (definition, workflow) =
        ontography_app::workflow::tools::prepare_start(&service, &args, &run_id).unwrap();
    let mut current = serde_json::to_value(GraphInitialization {
        run_id: run_id.clone(),
        args,
        definition,
        workflow,
    })
    .unwrap();
    current["workflow"] = json!("not a workflow");
    let mut legacy = legacy_intent(&run_id, "flow.start");
    legacy["operation"] = json!("unknown.start");
    for (intent, cause) in [(current, "invalid type"), (legacy, "unknown variant")] {
        let error = serde_json::from_value::<SavedGraphInitialization>(intent)
            .unwrap_err()
            .to_string();
        assert!(error.contains(cause), "{error}");
        assert!(!error.contains("untagged"), "{error}");
    }
    service.shutdown().await.unwrap();
}
