//! Durable vocabulary integration through the app and public core APIs.

use ontography_app::{
    declarations::GraphDeclaration,
    extensions::VocabularyExtension,
    persistence::{Paths, read_json, write_json},
    state::Service,
    tools,
};
use serde_json::{Value, json};

fn extension() -> Value {
    json!({"node_types":["Agent"],"object_types":["Artifact"],"authority_tags":["review"],
        "contracts":[{"id":"artifact","object_type":"Artifact","validator":"opaque_bytes","validator_version":1}]})
}

async fn start(service: &Service, project: &std::path::Path) -> String {
    service
        .start(
            GraphDeclaration::parse(include_str!("../examples/flow.json")).unwrap(),
            project.to_owned(),
        )
        .await
        .unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn call(service: &Service, id: &str, operation: &str, mut args: Value) -> Value {
    args["run_id"] = json!(id);
    tools::dispatch(service, operation, &args).await.unwrap()
}

async fn remove_receiver(service: &Service, id: &str) {
    let plan = call(service, id, "rewrite.prepare", json!({"request":{"production_id":"remove_receiver","nodes":{"A":"A","B":"B"},"edges":{"A_to_B":"A_to_B"}}})).await;
    call(
        service,
        id,
        "rewrite.commit",
        json!({"plan_id":plan["plan_id"]}),
    )
    .await;
}

#[tokio::test]
async fn extension_preserves_frontier_and_validators_and_stales_rewrite_plans() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let id = start(&service, directory.path()).await;
    call(&service, &id, "workflow.submit", json!({"trigger":{"kind":"root","node_id":"A","authority":["work"]},"result":"ok","emissions":[{"object_type":"Text","payload":"waiting"}]})).await;
    let before = call(&service, &id, "run.inspect", json!({})).await;
    let plan = call(&service, &id, "rewrite.prepare", json!({"request":{"production_id":"remove_receiver","nodes":{"A":"A","B":"B"},"edges":{"A_to_B":"A_to_B"}}})).await;
    let result = call(
        &service,
        &id,
        "run.extend",
        json!({"extension":extension()}),
    )
    .await;
    let after = call(&service, &id, "run.inspect", json!({})).await;
    assert_eq!(before["graph"], after["graph"]);
    assert_eq!(before["frontier"], after["frontier"]);
    assert_eq!(before["definition_revision"], after["definition_revision"]);
    assert_eq!(
        result["revision"],
        (before["revision"].as_str().unwrap().parse::<u64>().unwrap() + 1).to_string()
    );
    assert_eq!(
        after["vocabulary"]["object_types"],
        json!(["Artifact", "Text"])
    );
    let stale = tools::dispatch(
        &service,
        "rewrite.commit",
        &json!({"run_id":id,"plan_id":plan["plan_id"]}),
    )
    .await
    .unwrap_err();
    assert_eq!(stale.code, "stale");
    let invalid = tools::dispatch(
        &service,
        "workflow.submit",
        &json!({"run_id":id,"trigger":{"kind":"root","node_id":"A","authority":[]},"result":[255]}),
    )
    .await
    .unwrap_err();
    assert_eq!(invalid.code, "rejected");
    let authority = tools::dispatch(&service,"workflow.submit",&json!({"run_id":id,"trigger":{"kind":"root","node_id":"A","authority":["review"]},"result":"not granted"})).await.unwrap_err();
    assert_eq!(authority.code, "rejected");
    let exported = tools::dispatch(
        &service,
        "run.export_facts",
        &json!({"run_id":id,"path":directory.path().join("facts.json")}),
    )
    .await
    .unwrap_err();
    assert_eq!(exported.code, "unsupported_fact_history");
    assert!(!directory.path().join("facts.json").exists());
    service.shutdown().await.unwrap();
    let verified = tools::dispatch(&service, "run.verify", &json!({"run_id":id}))
        .await
        .unwrap_err();
    assert_eq!(verified.code, "verification_failed");
}

#[tokio::test]
async fn rewritten_graph_reopens_with_all_extensions_and_original_definition_identity() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let id = start(&service, directory.path()).await;
    remove_receiver(&service, &id).await;
    call(
        &service,
        &id,
        "run.extend",
        json!({"extension":extension()}),
    )
    .await;
    call(
        &service,
        &id,
        "run.extend",
        json!({"extension":{"node_types":["Reviewer"]}}),
    )
    .await;
    let before = call(&service, &id, "run.inspect", json!({})).await;
    service.shutdown().await.unwrap();
    drop(service);
    let service = Service::new(paths).unwrap();
    let after = call(&service, &id, "run.resume", json!({})).await;
    assert_eq!(after["graph"], before["graph"]);
    assert_eq!(after["graph"]["nodes"], json!([{"id":"A"}]));
    assert_eq!(after["revision"], before["revision"]);
    assert_eq!(after["vocabulary"], before["vocabulary"]);
    assert_eq!(after["definition_revision"], before["definition_revision"]);
    assert_eq!(after["extensions"]["accepted"].as_array().unwrap().len(), 2);
    assert!(after["extensions"]["pending"].is_null());
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejected_additions_leave_metadata_and_frontier_unchanged() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let id = start(&service, directory.path()).await;
    call(
        &service,
        &id,
        "run.extend",
        json!({"extension":extension()}),
    )
    .await;
    let metadata = paths.run(&id).unwrap();
    let manifest = std::fs::read(metadata.join("manifest.json")).unwrap();
    let journal = std::fs::read(metadata.join("extensions.json")).unwrap();
    let before = call(&service, &id, "run.inspect", json!({})).await;
    for extension in [
        json!({}),
        json!({"node_types":["Logical"]}),
        json!({"node_types":["Duplicate","Duplicate"]}),
        json!({"contracts":[{"id":"text","object_type":"Text","validator":"opaque_bytes","validator_version":1}]}),
        json!({"contracts":[{"id":"missing","object_type":"Missing","validator":"utf8","validator_version":1}]}),
        json!({"contracts":[{"id":"future","object_type":"Text","validator":"utf8","validator_version":2}]}),
        json!({"nodes":[{"id":"Unexpected"}]}),
    ] {
        assert!(
            tools::dispatch(
                &service,
                "run.extend",
                &json!({"run_id":id,"extension":extension})
            )
            .await
            .is_err()
        );
        assert_eq!(
            std::fs::read(metadata.join("manifest.json")).unwrap(),
            manifest
        );
        assert_eq!(
            std::fs::read(metadata.join("extensions.json")).unwrap(),
            journal
        );
        let after = call(&service, &id, "run.inspect", json!({})).await;
        assert_eq!(before["revision"], after["revision"]);
        assert_eq!(before["frontier"], after["frontier"]);
        assert_eq!(before["vocabulary"], after["vocabulary"]);
    }
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn pending_intent_recovers_both_sides_of_core_commit_without_replaying_it() {
    for committed in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let paths = Paths::initialize(directory.path().join("data")).unwrap();
        let service = Service::new(paths.clone()).unwrap();
        let id = start(&service, directory.path()).await;
        remove_receiver(&service, &id).await;
        let revision;
        {
            let run = service.run(&id).await.unwrap();
            let run = run.lock().await;
            // Model process loss after intent publication, with or without the
            // atomic core commit. Only public core APIs touch core storage.
            write_json(&run.directory.join("extensions.json"),&json!({"version":1,"declaration_revision":run.manifest.declaration_revision,"accepted":[],"pending":extension()})).unwrap();
            let session = &run.live().unwrap().session;
            if committed {
                let addition: VocabularyExtension = serde_json::from_value(extension()).unwrap();
                let kernel = session.kernel().await.unwrap();
                session
                    .extend(addition.apply(&kernel).unwrap())
                    .await
                    .unwrap();
            }
            revision = session.frontier().revision();
        }
        service.shutdown().await.unwrap();
        drop(service);
        let service = Service::new(paths.clone()).unwrap();
        let after = call(&service, &id, "run.resume", json!({})).await;
        assert_eq!(after["revision"], revision.to_string());
        assert_eq!(after["graph"]["nodes"], json!([{"id":"A"}]));
        assert_eq!(
            after["extensions"]["accepted"].as_array().unwrap().len(),
            usize::from(committed)
        );
        assert!(after["extensions"]["pending"].is_null());
        let journal: Value = read_json(&paths.run(&id).unwrap().join("extensions.json")).unwrap();
        assert!(journal["pending"].is_null());
        service.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn unresolved_intent_is_preserved_when_neither_vocabulary_matches_storage() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let id = start(&service, directory.path()).await;
    let journal_path = paths.run(&id).unwrap().join("extensions.json");
    {
        let managed = service.run(&id).await.unwrap();
        let run = managed.lock().await;
        write_json(&journal_path,&json!({"version":1,"declaration_revision":run.manifest.declaration_revision,"accepted":[],"pending":extension()})).unwrap();
        let session = &run.live().unwrap().session;
        let kernel = session.kernel().await.unwrap();
        let other = VocabularyExtension {
            node_types: vec!["Unexpected".into()],
            ..Default::default()
        };
        session.extend(other.apply(&kernel).unwrap()).await.unwrap();
    }
    let journal = std::fs::read(&journal_path).unwrap();
    service.shutdown().await.unwrap();
    drop(service);
    let service = Service::new(paths).unwrap();
    let error = tools::dispatch(&service, "run.resume", &json!({"run_id":id}))
        .await
        .unwrap_err();
    assert_eq!(error.code, "extension_recovery_failed");
    assert_eq!(std::fs::read(&journal_path).unwrap(), journal);
    let inspection = call(&service, &id, "run.inspect", json!({})).await;
    assert!(!inspection["extensions"]["pending"].is_null());
}

#[tokio::test]
async fn application_extension_recovers_before_workers_resume_without_replaying_entry_input() {
    use ontography::{
        ApplicationContext, ApplicationRegistry, ApplicationRunMode, Contract, ExecutionFailure,
    };
    use ontography_app::registry::{ImplementationDescriptor, ImplementationRegistry};
    use std::{sync::Arc, time::Duration};

    let (sender, mut launches) = tokio::sync::mpsc::unbounded_channel();
    let mut native = ApplicationRegistry::new();
    native
        .register_contract(Contract::new("result", "Result", |_| Ok(())).unwrap())
        .unwrap();
    native.register_node_implementation("fixture", move |_| {
        let sender = sender.clone();
        Ok::<_, String>(move |context: ApplicationContext| {
            let sender = sender.clone();
            async move {
                let kernel = context.kernel().await.map_err(|error|ExecutionFailure::new("fixture", error.to_string()))?;
                sender.send(json!({"fresh":context.run_mode()==ApplicationRunMode::Fresh,
                    "input":context.initial_input().map(|bytes|String::from_utf8_lossy(bytes).to_string()),
                    "root_authority":context.root_authority().is_some(),"state":context.node_state_dir(),
                    "vocabulary":ontography_app::extensions::vocabulary(&kernel)})).unwrap();
                context.stop().requested().await;
                Ok::<(), ExecutionFailure>(())
            }
        })
    }).unwrap();
    let registry = Arc::new(ImplementationRegistry::new(
        native,
        vec![ImplementationDescriptor {
            id: "fixture".into(),
            version: "1".into(),
            description: "test extension application".into(),
            configuration_schema: json!({"type":"object"}),
        }],
    ));
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::with_registry(paths.clone(), registry.clone()).unwrap();
    let document = json!({"id":"application-extension","entry":"worker","node_definitions":{"worker":{"types":["Node"],"result_contract":"result","root_authority":[],"implementation":{"kind":"fixture"}}},"edge_definitions":{},"nodes":{"worker":{"definition":"worker"}},"edges":{}});
    let started = tools::dispatch(&service,"project.start",&json!({"format":"native","document":document.to_string(),"project":directory.path(),"input":"initial"})).await.unwrap();
    let id = started["run_id"].as_str().unwrap();
    let first = tokio::time::timeout(Duration::from_secs(3), launches.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first["fresh"], true);
    assert_eq!(first["input"], "initial");
    call(&service, id, "run.extend", json!({"extension":extension()})).await;
    {
        let managed = service.run(id).await.unwrap();
        let run = managed.lock().await;
        let path = run.directory.join("extensions.json");
        let mut journal: Value = read_json(&path).unwrap();
        let pending = json!({"node_types":["PendingAtCrash"]});
        journal["pending"] = pending.clone();
        write_json(&path, &journal).unwrap();
        let extension: VocabularyExtension = serde_json::from_value(pending).unwrap();
        let session = &run.live().unwrap().session;
        let kernel = session.kernel().await.unwrap();
        session
            .extend(extension.apply(&kernel).unwrap())
            .await
            .unwrap();
    }
    service.shutdown().await.unwrap();
    drop(service);
    let service = Service::with_registry(paths, registry).unwrap();
    let resumed = call(&service, id, "run.resume", json!({})).await;
    let next = tokio::time::timeout(Duration::from_secs(3), launches.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next["fresh"], false);
    assert!(next["input"].is_null());
    assert_eq!(next["root_authority"], false);
    assert_eq!(next["state"], first["state"]);
    assert_eq!(next["vocabulary"], resumed["vocabulary"]);
    assert_eq!(
        next["vocabulary"]["node_types"],
        json!(["Agent", "Node", "PendingAtCrash"])
    );
    assert_eq!(
        resumed["extensions"]["accepted"].as_array().unwrap().len(),
        2
    );
    assert!(resumed["extensions"]["pending"].is_null());
    call(&service, id, "run.close", json!({})).await;
    let closed = call(&service, id, "run.resume", json!({})).await;
    assert_eq!(closed["admission"], "closed");
    assert!(closed["executions"].as_array().unwrap().is_empty());
    assert!(launches.try_recv().is_err());
    service.shutdown().await.unwrap();
}
