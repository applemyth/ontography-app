// Test fixture implementations are deliberately unavailable in the shipped catalog.
use super::*;
use crate::registry::{ImplementationDescriptor, ImplementationRegistry};
use ontography::{
    ApplicationRegistry, Contract, IngressMode,
    project::{
        BoundComponent, ComponentBindings, ComponentDescription, ComponentProvider,
        ProjectComponent,
    },
};
use std::{collections::BTreeMap, sync::Arc};

struct FixtureProvider;
struct FixtureComponent;

impl ProjectComponent for FixtureComponent {
    fn description(&self) -> ComponentDescription {
        ComponentDescription {
            identity: "fixture-v1".into(),
            description: "Test-only provider".into(),
            types: vec!["Node".into()],
            result_contract: "result".into(),
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            dynamic_inputs: false,
            dynamic_outputs: false,
            ingress_modes: vec![IngressMode::Any],
            configuration_schema: Some(json!({"type":"object"})),
        }
    }
    fn bind(
        &self,
        config: Value,
        _: &ComponentBindings,
    ) -> std::result::Result<BoundComponent, String> {
        Ok(BoundComponent {
            kind: "fixture".into(),
            config,
        })
    }
}
impl ComponentProvider for FixtureProvider {
    fn load(
        &self,
        specs: &BTreeMap<String, Value>,
        _: &Path,
        registry: &mut ApplicationRegistry,
    ) -> std::result::Result<BTreeMap<String, Arc<dyn ProjectComponent>>, String> {
        registry
            .register_contract(Contract::new("result", "Result", |_| Ok(())).unwrap())
            .map_err(|e| e.to_string())?;
        registry
            .register_node_implementation("fixture", |_| {
                Ok::<_, String>(|_: ontography::ApplicationContext| async {
                    Ok::<(), ontography::ExecutionFailure>(())
                })
            })
            .map_err(|e| e.to_string())?;
        Ok(specs
            .keys()
            .map(|alias| {
                (
                    alias.clone(),
                    Arc::new(FixtureComponent) as Arc<dyn ProjectComponent>,
                )
            })
            .collect())
    }
}

#[test]
fn provider_preparation_validates_grammar_and_keeps_factory_registration_isolated() {
    let mut registry = ImplementationRegistry::default();
    registry
        .register_provider(
            ImplementationDescriptor {
                id: "fixture".into(),
                version: "1".into(),
                description: "Test-only".into(),
                configuration_schema: json!({"type":"object"}),
            },
            Arc::new(FixtureProvider),
        )
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let document=json!({"id":"project-test","entry":"worker","components":{"component":{"provider":"fixture"}},"nodes":{"worker":{"component":"component","config":{},"root_authority":[]}},"connections":[]}).to_string();
    let descriptions = registry
        .describe_project(&document, directory.path())
        .unwrap();
    assert_eq!(descriptions["component"].identity, "fixture-v1");
    let prepared = registry
        .prepare_project(&document, directory.path())
        .unwrap();
    assert!(
        prepared
            .application
            .kernel()
            .graph()
            .node("worker")
            .is_some()
    );
    assert!(prepared.application_json.contains("fixture"));
    // Concise providers are isolated preparations, so their factory registrations
    // cannot leak into the base native registry between requests.
    assert!(
        registry
            .native_application(&prepared.application_json)
            .is_err()
    );
    let fragment = json!({"nodes":[{"id":"worker","types":["Node"],"result_contract":"result"}],"roots":[{"node_id":"worker","ceiling":[]}]});
    let (application,grammar)=with_grammar(prepared.application,&json!({"rewrites":[{"id":"identity","left":fragment,"right":fragment,"interface_nodes":["worker"],"interface_edges":[]}]})).unwrap();
    assert!(application.kernel().graph().node("worker").is_some());
    assert_eq!(grammar.len(), 1);
}

#[derive(Debug)]
struct ObservedLaunch {
    fresh: bool,
    input: Option<Vec<u8>>,
    root_authority: bool,
    state_directory: std::path::PathBuf,
}

type LaunchSender = tokio::sync::mpsc::UnboundedSender<ObservedLaunch>;

fn install_observed_native(registry: &mut ApplicationRegistry, launches: LaunchSender) {
    registry
        .register_contract(Contract::new("result", "Result", |_| Ok(())).unwrap())
        .unwrap();
    registry
        .register_node_implementation("fixture", move |_| {
            let launches = launches.clone();
            Ok::<_, String>(move |context: ontography::ApplicationContext| {
                let launches = launches.clone();
                async move {
                    let input = context.initial_input().cloned();
                    if let Some(input) = &input {
                        let proposal = ontography::ActivationProposal::root(
                            context.node_id(),
                            context
                                .root_authority()
                                .cloned()
                                .expect("fresh root authority"),
                            input.clone(),
                        );
                        let decision = context.submit(proposal).await.map_err(|error| {
                            ontography::ExecutionFailure::new("fixture", error.to_string())
                        })?;
                        if let ontography::ProposalDecision::Rejected(reason) = decision {
                            return Err(ontography::ExecutionFailure::new(
                                "fixture",
                                reason.to_string(),
                            ));
                        }
                    }
                    launches
                        .send(ObservedLaunch {
                            fresh: context.run_mode() == ontography::ApplicationRunMode::Fresh,
                            input: input.map(|bytes| bytes.to_vec()),
                            root_authority: context.root_authority().is_some(),
                            state_directory: context
                                .node_state_dir()
                                .expect("persistent application")
                                .to_owned(),
                        })
                        .map_err(|error| {
                            ontography::ExecutionFailure::new("fixture", error.to_string())
                        })?;
                    context.stop().requested().await;
                    Ok(())
                }
            })
        })
        .unwrap();
}

struct ObservedProvider(LaunchSender);
impl ComponentProvider for ObservedProvider {
    fn load(
        &self,
        specs: &BTreeMap<String, Value>,
        _: &Path,
        registry: &mut ApplicationRegistry,
    ) -> std::result::Result<BTreeMap<String, Arc<dyn ProjectComponent>>, String> {
        install_observed_native(registry, self.0.clone());
        Ok(specs
            .keys()
            .map(|alias| {
                (
                    alias.clone(),
                    Arc::new(FixtureComponent) as Arc<dyn ProjectComponent>,
                )
            })
            .collect())
    }
}

async fn observed_launch(
    receiver: &mut tokio::sync::mpsc::UnboundedReceiver<ObservedLaunch>,
) -> ObservedLaunch {
    tokio::time::timeout(std::time::Duration::from_secs(3), receiver.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn native_and_provider_applications_execute_resume_without_input_and_keep_closed_runs_stopped()
 {
    for format in ["native", "project"] {
        let (launches, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let descriptor = ImplementationDescriptor {
            id: "fixture".into(),
            version: "1".into(),
            description: "Test-only persistent application".into(),
            configuration_schema: json!({"type":"object"}),
        };
        let mut registry = if format == "native" {
            let mut native = ApplicationRegistry::new();
            install_observed_native(&mut native, launches.clone());
            ImplementationRegistry::new(native, vec![descriptor.clone()])
        } else {
            ImplementationRegistry::default()
        };
        if format == "project" {
            registry
                .register_provider(descriptor, Arc::new(ObservedProvider(launches)))
                .unwrap();
        }
        let directory = tempfile::tempdir().unwrap();
        let paths = crate::persistence::Paths::initialize(directory.path().join("data")).unwrap();
        let registry = Arc::new(registry);
        let service = Service::with_registry(paths.clone(), registry.clone()).unwrap();
        let document = if format == "native" {
            json!({"id":"native-lifecycle","entry":"worker","node_definitions":{"worker":{"types":["Node"],"result_contract":"result","root_authority":[],"implementation":{"kind":"fixture"}}},"edge_definitions":{},"nodes":{"worker":{"definition":"worker"}},"edges":{}})
        } else {
            json!({"id":"project-lifecycle","entry":"worker","components":{"component":{"provider":"fixture"}},"nodes":{"worker":{"component":"component","config":{},"root_authority":[]}},"connections":[]})
        };
        let fragment = json!({"nodes":[{"id":"worker","types":["Node"],"result_contract":"result"}],"roots":[{"node_id":"worker","ceiling":[]}]});
        let started = crate::tools::dispatch(&service, "project.start", &json!({"format":format,"document":document.to_string(),"project":directory.path(),"input":"first input","rewrites":[{"id":"identity","left":fragment,"right":fragment,"interface_nodes":["worker"],"interface_edges":[]}]})).await.unwrap();
        let run_id = started["run_id"].as_str().unwrap();
        let initial = observed_launch(&mut receiver).await;
        assert!(initial.fresh);
        assert!(initial.root_authority);
        assert_eq!(initial.input.as_deref(), Some(&b"first input"[..]));
        assert!(initial.state_directory.is_dir());
        let observed = crate::tools::dispatch(&service, "run.inspect", &json!({"run_id":run_id}))
            .await
            .unwrap();
        assert_eq!(observed["revision"], "1");
        assert_eq!(observed["executions"][0]["status"], "running");
        // Application runs use exactly the same governed workflow and rewrite tools.
        let submitted = crate::tools::dispatch(&service, "workflow.submit", &json!({"run_id":run_id,"trigger":{"kind":"root","node_id":"worker","authority":[]},"result":"management root"})).await.unwrap();
        assert_eq!(submitted["decision"], "committed");
        let plan = crate::tools::dispatch(&service, "rewrite.prepare", &json!({"run_id":run_id,"request":{"production_id":"identity","nodes":{"worker":"worker"}}})).await.unwrap();
        let rewritten = crate::tools::dispatch(
            &service,
            "rewrite.commit",
            &json!({"run_id":run_id,"plan_id":plan["plan_id"]}),
        )
        .await
        .unwrap();
        let revision = rewritten["revision"].clone();
        crate::tools::dispatch(&service, "run.suspend", &json!({"run_id":run_id}))
            .await
            .unwrap();
        drop(service);
        // Rebuild from manifests and the trusted registry, with no retained application.
        let service = Service::with_registry(paths, registry).unwrap();
        let resumed = crate::tools::dispatch(&service, "run.resume", &json!({"run_id":run_id}))
            .await
            .unwrap();
        let resumed_launch = observed_launch(&mut receiver).await;
        assert!(!resumed_launch.fresh);
        assert!(!resumed_launch.root_authority);
        assert!(resumed_launch.input.is_none());
        assert_eq!(resumed_launch.state_directory, initial.state_directory);
        assert_eq!(resumed["revision"], revision);
        crate::tools::dispatch(&service, "run.close", &json!({"run_id":run_id}))
            .await
            .unwrap();
        let closed = crate::tools::dispatch(&service, "run.resume", &json!({"run_id":run_id}))
            .await
            .unwrap();
        assert_eq!(closed["admission"], "closed");
        assert!(closed["executions"].as_array().unwrap().is_empty());
        assert!(receiver.try_recv().is_err());
        crate::tools::dispatch(&service, "run.suspend", &json!({"run_id":run_id}))
            .await
            .unwrap();
    }
}
