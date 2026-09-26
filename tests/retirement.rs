use ontography_app::{persistence::Paths, state::Service, tools};
use serde_json::{Value, json};

fn declaration() -> Value {
    serde_json::from_str(include_str!("../examples/flow.json")).unwrap()
}

async fn call(service: &Service, operation: &str, args: Value) -> Value {
    tools::dispatch(service, operation, &args)
        .await
        .unwrap_or_else(|error| panic!("{operation} failed: {error}"))
}

async fn start(service: &Service, project: &std::path::Path, declaration: Value) -> String {
    call(
        service,
        "run.start",
        json!({"declaration":declaration,"project":project}),
    )
    .await["run_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn emit(
    service: &Service,
    run_id: &str,
    node_id: &str,
    edge: Option<&str>,
) -> (String, String) {
    let emission = match edge {
        Some(edge) => json!({"edge_id":edge,"payload":"work"}),
        None => json!({"object_type":"Text","payload":"work"}),
    };
    let activation = call(
        service,
        "workflow.submit",
        json!({"run_id":run_id,
        "trigger":{"kind":"root","node_id":node_id,"authority":["work"]},
        "result":"sent","emissions":[emission]}),
    )
    .await["activation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let package = call(
        service,
        "inspect.activation",
        json!({"run_id":run_id,"activation_id":activation}),
    )
    .await["outputs"][0]["package_id"]
        .as_str()
        .unwrap()
        .to_owned();
    (package, activation)
}

async fn inspect(service: &Service, run_id: &str, package: &str) -> Value {
    call(
        service,
        "inspect.package",
        json!({"run_id":run_id,"package_id":package}),
    )
    .await
}

#[tokio::test]
async fn explicit_retirement_is_observable_bounded_and_durable() {
    let directory = tempfile::tempdir().unwrap();
    let paths = Paths::initialize(directory.path().join("data")).unwrap();
    let service = Service::new(paths.clone()).unwrap();
    let run_id = start(&service, directory.path(), declaration()).await;
    let (received, evidence) = emit(&service, &run_id, "A", Some("A_to_B")).await;
    let (outbound, _) = emit(&service, &run_id, "A", None).await;
    let (consumed, _) = emit(&service, &run_id, "A", Some("A_to_B")).await;
    let (live, _) = emit(&service, &run_id, "A", None).await;
    assert_eq!(
        inspect(&service, &run_id, &received).await["position"],
        json!({"holder":"B","phase":"received"})
    );

    let before = inspect(&service, &run_id, &received).await;
    let error = tools::dispatch(
        &service,
        "workflow.retire",
        &json!({"run_id":run_id,"package_id":received,
        "evidence_activation_id":uuid::Uuid::new_v4().to_string()}),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "rejected");
    assert_eq!(inspect(&service, &run_id, &received).await, before);

    let retirement = call(
        &service,
        "workflow.retire",
        json!({"run_id":run_id,"package_id":received,"evidence_activation_id":evidence}),
    )
    .await;
    assert_eq!(retirement["disposition"], "retired");
    assert_eq!(
        retirement["retirement"],
        json!({"reason":"explicit","holder":"B","phase":"received","revision":"5","evidence_activation_id":evidence})
    );
    let other = call(
        &service,
        "workflow.retire",
        json!({"run_id":run_id,"package_id":outbound}),
    )
    .await;
    assert_eq!(other["retirement"]["phase"], "outbound");
    assert_eq!(other["retirement"]["holder"], "A");
    assert_eq!(other["retirement"]["evidence_activation_id"], Value::Null);

    let committed = call(
        &service,
        "workflow.submit",
        json!({"run_id":run_id,
        "trigger":{"kind":"packages","package_ids":[consumed]},"result":"done",
        "emissions":[{"object_type":"Text","payload":"next"}]}),
    )
    .await;
    let child = call(
        &service,
        "inspect.activation",
        json!({"run_id":run_id,"activation_id":committed["activation_id"]}),
    )
    .await["outputs"][0]["package_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        inspect(&service, &run_id, &child).await["inputs"],
        json!([consumed])
    );
    for package in [&received, &outbound, &consumed] {
        assert_eq!(
            tools::dispatch(
                &service,
                "workflow.retire",
                &json!({"run_id":run_id,"package_id":package})
            )
            .await
            .unwrap_err()
            .code,
            "rejected"
        );
    }
    for (operation, args) in [
        (
            "workflow.submit",
            json!({"run_id":run_id,"trigger":{"kind":"packages","package_ids":[received]},"result":"too late"}),
        ),
        (
            "workflow.transfer",
            json!({"run_id":run_id,"package_id":outbound,"edge_id":"A_to_B"}),
        ),
    ] {
        assert_eq!(
            tools::dispatch(&service, operation, &args)
                .await
                .unwrap_err()
                .code,
            "rejected"
        );
    }
    let retired = inspect(&service, &run_id, &received).await;
    assert_eq!(retired["revision"], "7");
    assert_eq!(retired["disposition"], "retired");
    assert_eq!(retired["retirement"], retirement["retirement"]);
    assert_eq!(retired["position"], Value::Null);
    assert_eq!(retired["consumer"], Value::Null);
    assert_eq!(retired["delivery"]["edge_id"], "A_to_B");
    assert_eq!(
        inspect(&service, &run_id, &consumed).await["disposition"],
        "consumed"
    );
    assert_eq!(
        inspect(&service, &run_id, &consumed).await["consumer"],
        committed["activation_id"]
    );
    assert_eq!(
        inspect(&service, &run_id, &live).await["disposition"],
        "live"
    );

    let first = call(
        &service,
        "inspect.retirements",
        json!({"run_id":run_id,"limit":1}),
    )
    .await;
    assert_eq!(first["retirements"].as_array().unwrap().len(), 1);
    let second = call(
        &service,
        "inspect.retirements",
        json!({"run_id":run_id,"limit":1,"after":first["next_after"]}),
    )
    .await;
    assert_eq!(second["retirements"].as_array().unwrap().len(), 1);
    assert_eq!(second["next_after"], Value::Null);
    assert_eq!(first["revision"], second["revision"]);
    let mut ids = vec![
        first["retirements"][0]["package_id"].as_str().unwrap(),
        second["retirements"][0]["package_id"].as_str().unwrap(),
    ];
    let ordered = ids.clone();
    ids.sort();
    assert_eq!(ordered, ids);
    for args in [
        json!({"run_id":run_id,"limit":0}),
        json!({"run_id":run_id,"limit":1001}),
        json!({"run_id":run_id,"after":"bad"}),
    ] {
        assert!(
            tools::dispatch(&service, "inspect.retirements", &args)
                .await
                .is_err()
        );
    }

    call(
        &service,
        "inspect.export",
        json!({"run_id":run_id,"path":"snapshot.json"}),
    )
    .await;
    let exported: Value =
        serde_json::from_slice(&std::fs::read(directory.path().join("snapshot.json")).unwrap())
            .unwrap();
    assert_eq!(exported["retirements"].as_array().unwrap().len(), 2);
    let exported_retired = exported["packages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["package"]["package_id"] == received)
        .unwrap();
    assert_eq!(exported_retired["disposition"], "retired");
    assert_eq!(exported_retired["retirement"], retirement["retirement"]);
    service.shutdown().await.unwrap();
    drop(service);

    let service = Service::new(paths).unwrap();
    call(&service, "run.resume", json!({"run_id":run_id})).await;
    assert_eq!(inspect(&service, &run_id, &received).await, retired);
    let reopened = call(&service, "inspect.retirements", json!({"run_id":run_id})).await;
    assert_eq!(reopened["retirements"], exported["retirements"]);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_rewrites_report_route_holder_and_acceptance_retirements() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let mut definition = declaration();
    definition["nodes"][1]["ingress_mode"] = json!("all");
    let fragment_nodes = definition["nodes"].clone();
    let edge1 = definition["edges"][0].clone();
    let mut edge2 = edge1.clone();
    edge2["id"] = json!("A_to_B_2");
    definition["edges"].as_array_mut().unwrap().push(edge2);
    let roots = definition["roots"].clone();
    definition["rewrites"] = json!([
        {"id":"drop_route","left":{"nodes":fragment_nodes,"edges":definition["edges"],"roots":roots},
         "interface_nodes":["A","B"],"interface_edges":["A_to_B"],
         "right":{"nodes":fragment_nodes,"edges":[edge1],"roots":roots}},
        {"id":"drop_receiver","left":{"nodes":fragment_nodes,"edges":[edge1],"roots":roots},
         "interface_nodes":["A"],"interface_edges":[],
         "right":{"nodes":[definition["nodes"][0]],"roots":roots}}
    ]);
    definition["nodes"]
        .as_array_mut()
        .unwrap()
        .push(json!({"id":"C","types":["Logical"],"result_contract":"text"}));
    definition["roots"]
        .as_array_mut()
        .unwrap()
        .push(json!({"node_id":"C","ceiling":["work"]}));
    let run_id = start(&service, directory.path(), definition).await;
    let (unrelated, _) = emit(&service, &run_id, "C", None).await;
    let (outbound, _) = emit(&service, &run_id, "A", None).await;
    let (receipt1, _) = emit(&service, &run_id, "A", Some("A_to_B")).await;
    let (receipt2, _) = emit(&service, &run_id, "A", Some("A_to_B_2")).await;
    let plan = call(
        &service,
        "rewrite.prepare",
        json!({"run_id":run_id,"request":{"production_id":"drop_route",
        "nodes":{"A":"A","B":"B"},"edges":{"A_to_B":"A_to_B","A_to_B_2":"A_to_B_2"}}}),
    )
    .await;
    assert_eq!(plan["retirements"].as_array().unwrap().len(), 1);
    assert_eq!(plan["retirements"][0]["package_id"], receipt2);
    assert_eq!(
        inspect(&service, &run_id, &receipt2).await["disposition"],
        "live"
    );
    call(
        &service,
        "rewrite.commit",
        json!({"run_id":run_id,"plan_id":plan["plan_id"]}),
    )
    .await;
    let retired_route = inspect(&service, &run_id, &receipt2).await;
    assert_eq!(retired_route["retirement"]["reason"], "route_removed");
    assert_eq!(retired_route["retirement"]["phase"], "received");
    assert_eq!(
        inspect(&service, &run_id, &unrelated).await["disposition"],
        "live"
    );
    assert_eq!(
        inspect(&service, &run_id, &outbound).await["disposition"],
        "live"
    );
    let trigger = call(
        &service,
        "inspect.trigger",
        json!({"run_id":run_id,"node_id":"B"}),
    )
    .await;
    assert_eq!(trigger["packages"].as_array().unwrap().len(), 1);
    assert_eq!(trigger["packages"][0]["package_id"], receipt1);
    let plan = call(
        &service,
        "rewrite.prepare",
        json!({"run_id":run_id,"request":{"production_id":"drop_receiver",
        "nodes":{"A":"A","B":"B"},"edges":{"A_to_B":"A_to_B"}}}),
    )
    .await;
    assert_eq!(plan["retirements"].as_array().unwrap().len(), 2);
    call(
        &service,
        "rewrite.commit",
        json!({"run_id":run_id,"plan_id":plan["plan_id"]}),
    )
    .await;
    assert_eq!(
        inspect(&service, &run_id, &receipt1).await["retirement"]["reason"],
        "holder_removed"
    );
    assert_eq!(
        inspect(&service, &run_id, &outbound).await["retirement"]["reason"],
        "no_accepting_edge"
    );
    assert_eq!(
        inspect(&service, &run_id, &unrelated).await["disposition"],
        "live"
    );
    assert_eq!(
        call(&service, "inspect.retirements", json!({"run_id":run_id})).await["retirements"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn retirement_obeys_app_session_scope_and_lifecycle() {
    let directory = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
    let record = call(
        &service,
        "session.create",
        json!({"project":directory.path()}),
    )
    .await;
    let session_id = record["session_id"].as_str().unwrap();
    let run = tools::dispatch_scoped(
        &service,
        Some(session_id),
        "run.start",
        &json!({"declaration":declaration()}),
    )
    .await
    .unwrap();
    let run_id = run["run_id"].as_str().unwrap();
    let (package, _) = emit(&service, run_id, "A", None).await;
    assert_eq!(
        tools::dispatch_scoped(
            &service,
            Some(session_id),
            "workflow.retire",
            &json!({"run_id":uuid::Uuid::new_v4().to_string(),"package_id":package})
        )
        .await
        .unwrap_err()
        .code,
        "session_scope_conflict"
    );
    call(
        &service,
        "session.suspend",
        json!({"session_id":session_id}),
    )
    .await;
    assert!(
        tools::dispatch_scoped(
            &service,
            Some(session_id),
            "workflow.retire",
            &json!({"package_id":package})
        )
        .await
        .is_err()
    );
    call(&service, "session.resume", json!({"session_id":session_id})).await;
    let retired = tools::dispatch_scoped(
        &service,
        Some(session_id),
        "workflow.retire",
        &json!({"package_id":package}),
    )
    .await
    .unwrap();
    assert_eq!(retired["disposition"], "retired");
    let inspected = tools::dispatch_scoped(
        &service,
        Some(session_id),
        "inspect.retirements",
        &json!({}),
    )
    .await
    .unwrap();
    assert_eq!(inspected["retirements"][0]["package_id"], package);
    service.shutdown().await.unwrap();
}
