//! Document names and node ownership remain meaningful after graph edits.

use ontography_app::{
    persistence::{self, Paths},
    state::Service,
    tools,
    workflow::{edit, runtime},
};
use serde_json::{Value, json};

async fn call(service: &Service, operation: &str, args: Value) -> Value {
    tools::dispatch(service, operation, &args)
        .await
        .unwrap_or_else(|error| panic!("{operation}: {error}"))
}

fn service(directory: &tempfile::TempDir) -> Service {
    Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap()
}

async fn start(service: &Service, project: &std::path::Path, document: &Value) -> String {
    call(
        service,
        "flow.start",
        json!({"project":project,"document":document}),
    )
    .await["run_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn preview(service: &Service, id: &str, document: &Value) -> edit::Plan {
    let preview = call(
        service,
        "flow.edit",
        json!({"run_id":id,"document":document}),
    )
    .await;
    let handle = service.run(id).await.unwrap();
    persistence::read_json(
        &handle
            .lock()
            .await
            .directory
            .join("edit-plans")
            .join(format!("{}.json", preview["plan_id"].as_str().unwrap())),
    )
    .unwrap()
}

async fn commit(service: &Service, id: &str, plan: &edit::Plan) {
    call(
        service,
        "flow.commit",
        json!({"run_id":id,"plan_id":plan.id}),
    )
    .await;
}

fn root(id: &str, node: &str, emissions: Value) -> Value {
    json!({"run_id":id,"trigger":{"kind":"root","node_id":node,"authority":["workflow"]},
        "result":"done","emissions":emissions})
}

#[tokio::test]
async fn external_moves_and_inspection_resolve_names_after_replacements_and_additions() {
    let directory = tempfile::tempdir().unwrap();
    let service = service(&directory);
    let mut document = json!({"name":"edited","entry":"client","nodes":[
        {"id":"client","component":"external"},
        {"id":"receiver","component":"external"}],
        "edges":[{"from":"client","to":"receiver","name":"route"}]});
    let id = start(&service, directory.path(), &document).await;
    // A changed core definition replaces the node and its connection. The
    // added node also receives a fresh identity, while all authored names stay.
    document["nodes"][0]["join"] = json!("all");
    document["nodes"]
        .as_array_mut()
        .unwrap()
        .push(json!({"id":"added","component":"external","root":["workflow"]}));
    document["edges"]
        .as_array_mut()
        .unwrap()
        .push(json!({"from":"receiver","to":"added"}));
    let plan = preview(&service, &id, &document).await;
    assert_ne!(plan.identities.nodes["client"], "client");
    assert_ne!(plan.identities.edges["route"], "route");
    commit(&service, &id, &plan).await;

    call(
        &service,
        "workflow.submit",
        root(
            &id,
            "client",
            json!([
        {"object_type":"Payload","payload":"later"}]),
        ),
    )
    .await;
    let outbound = call(
        &service,
        "inspect.frontier",
        json!({"run_id":id,"node_id":"client","phase":"outbound"}),
    )
    .await;
    let package = &outbound["packages"][0]["package_id"];
    assert!(package.is_string());
    call(
        &service,
        "workflow.transfer",
        json!({"run_id":id,"package_id":package,"edge_id":"route"}),
    )
    .await;
    let trigger = call(
        &service,
        "inspect.trigger",
        json!({"run_id":id,"node_id":"receiver","edge_id":"route"}),
    )
    .await;
    assert_eq!(trigger["packages"][0]["package_id"], *package);
    call(
        &service,
        "workflow.submit",
        json!({"run_id":id,"trigger":{"kind":"packages","package_ids":[package]},
        "result":"relayed","emissions":[{"edge_id":"receiver:added","payload":"relayed"}]}),
    )
    .await;
    let received = call(
        &service,
        "inspect.frontier",
        json!({"run_id":id,"node_id":"added"}),
    )
    .await;
    assert_eq!(received["packages"].as_array().unwrap().len(), 1);
    call(
        &service,
        "workflow.retire",
        json!({"run_id":id,"package_id":received["packages"][0]["package_id"]}),
    )
    .await;
    assert_eq!(
        call(&service, "workflow.submit", root(&id, "added", json!([]))).await["decision"],
        "committed"
    );
    // Direct emissions use the translated edge identity too.
    call(
        &service,
        "workflow.submit",
        root(
            &id,
            "client",
            json!([{"edge_id":"route","payload":"direct"}]),
        ),
    )
    .await;
    assert_eq!(
        call(
            &service,
            "inspect.trigger",
            json!({"run_id":id,"node_id":"receiver"})
        )
        .await["packages"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn pending_replacement_uses_its_new_binding_for_root_and_package_moves() {
    let directory = tempfile::tempdir().unwrap();
    let service = service(&directory);
    let mut document = json!({"name":"ownership","entry":"source","nodes":[
        {"id":"source","component":"external"},
        {"id":"client","component":"external","root":["workflow"]}],
        "edges":[{"from":"source","to":"client","name":"route"}]});
    let id = start(&service, directory.path(), &document).await;
    let handle = service.run(&id).await.unwrap();
    let mut predecessor = runtime::load(&*handle.lock().await).unwrap();
    document["nodes"][1]["component"] = json!("human");
    let plan = preview(&service, &id, &document).await;
    commit(&service, &id, &plan).await;
    // Reconstruct the recoverable interval after core commits, before the
    // completed document is published. The saved intent is still authoritative.
    predecessor.pending = Some(plan.clone());
    edit::store(&runtime::state_path(&*handle.lock().await), &predecessor).unwrap();
    for node in ["client", plan.identities.nodes["client"].as_str()] {
        let error = tools::dispatch(&service, "workflow.submit", &root(&id, node, json!([])))
            .await
            .unwrap_err();
        assert_eq!(error.code, "not_external", "{error}");
    }
    // Unchanged external nodes continue to accept work while the edit is pending.
    call(
        &service,
        "workflow.submit",
        root(
            &id,
            "source",
            json!([{"edge_id":"route","payload":"review"}]),
        ),
    )
    .await;
    let received = call(
        &service,
        "inspect.frontier",
        json!({"run_id":id,"node_id":"client"}),
    )
    .await;
    let package = &received["packages"][0]["package_id"];
    let error = tools::dispatch(
        &service,
        "workflow.submit",
        &json!({"run_id":id,
        "trigger":{"kind":"packages","package_ids":[package]},"result":"forged"}),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "not_external", "{error}");
    let error = tools::dispatch(
        &service,
        "workflow.retire",
        &json!({"run_id":id,"package_id":package}),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "not_external", "{error}");
    assert_eq!(
        call(
            &service,
            "inspect.frontier",
            json!({"run_id":id,"node_id":"client"})
        )
        .await["packages"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_bindings_and_unknown_names_never_authorize_core_moves() {
    let directory = tempfile::tempdir().unwrap();
    let service = service(&directory);
    let document =
        json!({"name":"missing","entry":"client","nodes":[{"id":"client","component":"external"}]});
    let id = start(&service, directory.path(), &document).await;
    let handle = service.run(&id).await.unwrap();
    let mut state = runtime::load(&*handle.lock().await).unwrap();
    state.bindings.clear();
    edit::store(&runtime::state_path(&*handle.lock().await), &state).unwrap();
    let error = tools::dispatch(&service, "workflow.submit", &root(&id, "client", json!([])))
        .await
        .unwrap_err();
    assert_eq!(error.code, "workflow_drift");
    let error = tools::dispatch(
        &service,
        "workflow.submit",
        &root(&id, "unknown", json!([])),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "rejected");
    assert_eq!(
        handle
            .lock()
            .await
            .live()
            .unwrap()
            .session
            .frontier()
            .revision(),
        0
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn pending_settings_edits_cannot_change_external_ownership_early() {
    let directory = tempfile::tempdir().unwrap();
    let service = service(&directory);
    // Both presets have the same core types, so an implementation change can
    // retain the identity and all pending work without a graph rewrite.
    let mut document = json!({"name":"retained","entry":"source","components":{
        "outside":{"extends":"external","types":["Command"]},
        "inside":{"extends":"command","types":["External"],"config":{"argv":["/bin/cat"]}}
    },"nodes":[{"id":"source","component":"external"},
        {"id":"client","component":"outside","root":["workflow"]}],
        "edges":[{"from":"source","to":"client","name":"route"}]});
    let id = start(&service, directory.path(), &document).await;
    let handle = service.run(&id).await.unwrap();
    for target in ["inside", "outside"] {
        let mut predecessor = runtime::load(&*handle.lock().await).unwrap();
        document["nodes"][1]["component"] = json!(target);
        let plan = preview(&service, &id, &document).await;
        assert_eq!(plan.changes, 0);
        assert_eq!(plan.identities.nodes["client"], "client");
        predecessor.pending = Some(plan.clone());
        edit::store(&runtime::state_path(&*handle.lock().await), &predecessor).unwrap();
        let error = tools::dispatch(&service, "workflow.submit", &root(&id, "client", json!([])))
            .await
            .unwrap_err();
        assert_eq!(error.code, "pending_edit", "{error}");
        commit(&service, &id, &plan).await;
    }
    assert_eq!(
        call(&service, "workflow.submit", root(&id, "client", json!([]))).await["decision"],
        "committed"
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn names_colliding_with_live_core_identities_cannot_redirect_moves() {
    let directory = tempfile::tempdir().unwrap();
    let service = service(&directory);
    let mut document = json!({"name":"ambiguous","entry":"client","nodes":[
        {"id":"client","component":"external"},{"id":"sink","component":"external"}],
        "edges":[{"from":"client","to":"sink","name":"route"}]});
    let id = start(&service, directory.path(), &document).await;
    document["nodes"][0]["join"] = json!("all");
    let replaced = preview(&service, &id, &document).await;
    commit(&service, &id, &replaced).await;
    let node_id = &replaced.identities.nodes["client"];
    let edge_id = &replaced.identities.edges["route"];
    document["nodes"]
        .as_array_mut()
        .unwrap()
        .push(json!({"id":node_id,"component":"external","root":["workflow"]}));
    document["edges"]
        .as_array_mut()
        .unwrap()
        .push(json!({"from":"client","to":node_id,"name":edge_id}));
    let added = preview(&service, &id, &document).await;
    commit(&service, &id, &added).await;
    let error = tools::dispatch(&service, "workflow.submit", &root(&id, node_id, json!([])))
        .await
        .unwrap_err();
    assert_eq!(error.code, "ambiguous_reference");
    call(
        &service,
        "workflow.submit",
        root(
            &id,
            "client",
            json!([{"object_type":"Payload","payload":"later"}]),
        ),
    )
    .await;
    let outbound = call(
        &service,
        "inspect.frontier",
        json!({"run_id":id,"node_id":"client","phase":"outbound"}),
    )
    .await;
    let package = &outbound["packages"][0]["package_id"];
    let error = tools::dispatch(
        &service,
        "workflow.transfer",
        &json!({"run_id":id,"package_id":package,"edge_id":edge_id}),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "ambiguous_reference");
    call(
        &service,
        "workflow.transfer",
        json!({"run_id":id,"package_id":package,"edge_id":"route"}),
    )
    .await;
    let received = call(
        &service,
        "inspect.frontier",
        json!({"run_id":id,"node_id":"sink"}),
    )
    .await;
    assert_eq!(received["packages"][0]["package_id"], *package);
    assert_eq!(
        call(
            &service,
            "inspect.frontier",
            json!({"run_id":id,"node_id":added.identities.nodes[node_id]})
        )
        .await["packages"],
        json!([])
    );
    service.shutdown().await.unwrap();
}
