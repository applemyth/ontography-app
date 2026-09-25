//! Trusted executable/provider adapters. Clients select implementations; they cannot upload code.

use crate::{AppError, Result};
use ontography::{
    Application, ApplicationRegistry, ExecutableDefinition, ExecutionHandle, ExecutionHost, Kernel,
    SessionStatus,
    project::{ComponentDescription, ComponentProvider, PreparedProject, ProjectRegistry},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, sync::Arc};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ImplementationDescriptor {
    pub id: String,
    pub version: String,
    pub description: String,
    pub configuration_schema: Value,
}

/// A pinned node implementation in a reusable graph declaration.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExecutionBinding {
    pub id: String,
    pub node_id: String,
    pub implementation: String,
    pub version: String,
    #[serde(default = "empty_configuration")]
    pub configuration: Value,
}

fn empty_configuration() -> Value {
    json!({})
}

pub struct LaunchedBindings {
    pub executions: BTreeMap<String, ExecutionHandle>,
    pub reports: Vec<Value>,
}

type ExecutableFactory = dyn Fn(Value) -> Result<Arc<dyn ExecutableDefinition>> + Send + Sync;
struct ExecutableRegistration {
    descriptor: ImplementationDescriptor,
    factory: Arc<ExecutableFactory>,
}

pub struct ImplementationRegistry {
    executables: BTreeMap<String, ExecutableRegistration>,
    providers: BTreeMap<String, ImplementationDescriptor>,
    native: Vec<ImplementationDescriptor>,
    projects: ProjectRegistry,
}

impl Default for ImplementationRegistry {
    fn default() -> Self {
        Self::new(ApplicationRegistry::new(), vec![])
    }
}

impl ImplementationRegistry {
    /// Native registry metadata is supplied by its owner because core's registry is opaque.
    pub fn new(native: ApplicationRegistry, descriptors: Vec<ImplementationDescriptor>) -> Self {
        Self {
            executables: BTreeMap::new(),
            providers: BTreeMap::new(),
            native: descriptors,
            projects: ProjectRegistry::new(native),
        }
    }

    /// A factory only validates configuration and constructs behavior; launch belongs to core.
    pub fn register_executable<F>(
        &mut self,
        descriptor: ImplementationDescriptor,
        factory: F,
    ) -> Result<()>
    where
        F: Fn(Value) -> Result<Arc<dyn ExecutableDefinition>> + Send + Sync + 'static,
    {
        validate_descriptor(&descriptor)?;
        if self.executables.contains_key(&descriptor.id) {
            return Err(AppError::new("duplicate_implementation", &descriptor.id));
        }
        self.executables.insert(
            descriptor.id.clone(),
            ExecutableRegistration {
                descriptor,
                factory: Arc::new(factory),
            },
        );
        Ok(())
    }

    pub fn register_provider(
        &mut self,
        descriptor: ImplementationDescriptor,
        provider: Arc<dyn ComponentProvider>,
    ) -> Result<()> {
        validate_descriptor(&descriptor)?;
        self.projects
            .register_provider(descriptor.id.clone(), provider)
            .map_err(AppError::core)?;
        self.providers.insert(descriptor.id.clone(), descriptor);
        Ok(())
    }

    pub fn executable(
        &self,
        id: &str,
        version: &str,
        configuration: Value,
    ) -> Result<Arc<dyn ExecutableDefinition>> {
        let entry = self.executables.get(id).ok_or_else(|| {
            AppError::new(
                "unavailable_implementation",
                format!("Executable {id:?} has no registered implementation"),
            )
        })?;
        if entry.descriptor.version != version {
            return Err(AppError::new(
                "incompatible_implementation",
                format!(
                    "Executable {id:?} requires version {:?}, requested {version:?}",
                    entry.descriptor.version
                ),
            ));
        }
        (entry.factory)(configuration)
    }

    /// Validate before creating storage. Constructors may inspect config but cannot launch work.
    pub fn validate_bindings(&self, bindings: &[ExecutionBinding], kernel: &Kernel) -> Result<()> {
        let mut ids = std::collections::BTreeSet::new();
        for binding in bindings {
            if binding.id.trim().is_empty() || !ids.insert(&binding.id) {
                return Err(AppError::invalid(
                    "Execution binding IDs must be nonempty and unique",
                ));
            }
            if kernel.graph().node(&binding.node_id).is_none() {
                return Err(AppError::invalid(format!(
                    "Binding {:?} names absent node {:?}",
                    binding.id, binding.node_id
                )));
            }
            self.executable(
                &binding.implementation,
                &binding.version,
                binding.configuration.clone(),
            )?;
        }
        Ok(())
    }

    /// Restore only declarations whose nodes remain in the current graph.
    /// The report distinguishes missing nodes from executable work actually launched.
    pub async fn launch_bindings(
        &self,
        bindings: &[ExecutionBinding],
        host: &ExecutionHost,
    ) -> Result<LaunchedBindings> {
        let mut launched = LaunchedBindings {
            executions: BTreeMap::new(),
            reports: Vec::new(),
        };
        if host.session().status() != SessionStatus::Open {
            launched.reports=bindings.iter().map(|binding|json!({"binding_id":binding.id,"node_id":binding.node_id,"lifetime":"declared","status":"not_launched","reason":"admission_not_open"})).collect();
            return Ok(launched);
        }
        let kernel = host.session().kernel().await.map_err(AppError::core)?;
        let mut prepared = Vec::new();
        for binding in bindings {
            let definition = self.executable(
                &binding.implementation,
                &binding.version,
                binding.configuration.clone(),
            )?;
            if kernel.graph().node(&binding.node_id).is_none() {
                launched.reports.push(json!({"binding_id":binding.id,"node_id":binding.node_id,"lifetime":"declared","status":"skipped","reason":"node_removed"}));
            } else {
                prepared.push((binding, definition));
            }
        }
        for (binding, definition) in prepared {
            match host.launch_arc(binding.node_id.as_str(), definition).await {
                Ok(handle) => {
                    let id = uuid::Uuid::new_v4().to_string();
                    launched.reports.push(json!({"binding_id":binding.id,"execution_id":id,"node_id":binding.node_id,"implementation":binding.implementation,"version":binding.version,"configuration":binding.configuration,"lifetime":"declared","status":"launched"}));
                    launched.executions.insert(id, handle);
                }
                Err(error) => {
                    for handle in launched.executions.values() {
                        handle.request_stop();
                    }
                    let wait = futures_util::future::join_all(
                        launched.executions.values().map(ExecutionHandle::wait),
                    );
                    if tokio::time::timeout(std::time::Duration::from_secs(3), wait)
                        .await
                        .is_err()
                    {
                        for handle in launched.executions.values() {
                            handle.abort();
                        }
                    }
                    futures_util::future::join_all(
                        launched.executions.values().map(ExecutionHandle::wait),
                    )
                    .await;
                    return Err(AppError::new("execution_launch_failed",error.to_string()).details(json!({"bindings":launched.reports,"committed_workflow_state_preserved":true})));
                }
            }
        }
        Ok(launched)
    }

    pub fn catalog(&self) -> Value {
        json!({"executables":self.executables.values().map(|entry|&entry.descriptor).collect::<Vec<_>>(),"providers":self.providers.values().collect::<Vec<_>>(),"native_implementations":self.native})
    }

    pub fn describe_project(
        &self,
        document: &str,
        root: &Path,
    ) -> Result<BTreeMap<String, ComponentDescription>> {
        crate::declarations::parse_json::<Value>(document).map_err(AppError::core)?;
        self.projects
            .describe(document, root)
            .map_err(|error| AppError::new("invalid_project", error.to_string()))
    }

    pub fn prepare_project(&self, document: &str, root: &Path) -> Result<PreparedProject> {
        crate::declarations::parse_json::<Value>(document).map_err(AppError::core)?;
        self.projects
            .prepare(document, root)
            .map_err(|error| AppError::new("invalid_project", error.to_string()))
    }

    pub fn native_application(&self, document: &str) -> Result<Application> {
        crate::declarations::parse_json::<Value>(document).map_err(AppError::core)?;
        self.projects
            .build_native(document)
            .map_err(|error| AppError::new("invalid_application", error.to_string()))
    }
}

fn validate_descriptor(descriptor: &ImplementationDescriptor) -> Result<()> {
    if descriptor.id.trim().is_empty() || descriptor.version.trim().is_empty() {
        return Err(AppError::invalid(
            "Implementation id and version must be nonempty",
        ));
    }
    if !descriptor.configuration_schema.is_object() {
        return Err(AppError::invalid(
            "Implementation configuration_schema must be an object",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ontography::{
        Authority, Contract, DefinitionId, ExecutionContext, Graph, Node, NodeDefinition,
        ProposalRuntime, RootRule, Schema,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn kernel() -> Arc<Kernel> {
        Arc::new(
            Kernel::admit(
                DefinitionId::new("registry-test").unwrap(),
                Schema::new(["Node"], ["Result"], []).unwrap(),
                Graph::new([Node::new("worker").unwrap()], []).unwrap(),
                [Contract::new("result", "Result", |_| Ok(())).unwrap()],
                [NodeDefinition::new("worker", ["Node"], "result").unwrap()],
                [],
                [],
                [RootRule::new("worker", Authority::new([])).unwrap()],
            )
            .unwrap(),
        )
    }

    fn fixture(starts: Arc<AtomicUsize>) -> ImplementationRegistry {
        let mut registry = ImplementationRegistry::default();
        registry
            .register_executable(
                ImplementationDescriptor {
                    id: "fixture".into(),
                    version: "1".into(),
                    description: "Test-only cooperative executor".into(),
                    configuration_schema: json!({"type":"object"}),
                },
                move |configuration| {
                    if !configuration.is_object() {
                        return Err(AppError::invalid("Fixture config must be an object"));
                    }
                    let starts = starts.clone();
                    Ok(Arc::new(move |context: ExecutionContext| {
                        let starts = starts.clone();
                        async move {
                            starts.fetch_add(1, Ordering::SeqCst);
                            context.stop().requested().await;
                            Ok(())
                        }
                    }))
                },
            )
            .unwrap();
        registry
    }

    fn binding() -> ExecutionBinding {
        ExecutionBinding {
            id: "worker-implementation".into(),
            node_id: "worker".into(),
            implementation: "fixture".into(),
            version: "1".into(),
            configuration: json!({}),
        }
    }

    #[tokio::test]
    async fn validating_does_not_launch_and_registered_host_outlives_launch_call() {
        let starts = Arc::new(AtomicUsize::new(0));
        let registry = fixture(starts.clone());
        let kernel = kernel();
        registry.validate_bindings(&[binding()], &kernel).unwrap();
        assert_eq!(starts.load(Ordering::SeqCst), 0);
        let runtime = ProposalRuntime::new(kernel);
        let host = ExecutionHost::new(runtime.open().unwrap());
        let launched = registry.launch_bindings(&[binding()], &host).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while starts.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let handle = launched.executions.values().next().unwrap();
        assert!(!handle.status().is_terminal());
        handle.request_stop();
        assert!(matches!(
            handle.wait().await,
            ontography::ExecutionStatus::Exited
        ));
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn versions_and_rewritten_away_nodes_do_not_start_unexpected_work() {
        let starts = Arc::new(AtomicUsize::new(0));
        let registry = fixture(starts.clone());
        let mut declaration = binding();
        declaration.version = "2".into();
        assert_eq!(
            registry
                .validate_bindings(&[declaration], &kernel())
                .unwrap_err()
                .code,
            "incompatible_implementation"
        );
        let runtime = ProposalRuntime::new(kernel());
        let host = ExecutionHost::new(runtime.open().unwrap());
        let mut removed = binding();
        removed.node_id = "removed".into();
        let result = registry.launch_bindings(&[removed], &host).await.unwrap();
        assert!(result.executions.is_empty());
        assert_eq!(result.reports[0]["reason"], "node_removed");
        assert_eq!(starts.load(Ordering::SeqCst), 0);
        runtime.shutdown().await;
    }

    #[test]
    fn default_catalog_has_no_fabricated_executables_or_providers() {
        let registry = ImplementationRegistry::default();
        assert!(
            registry.catalog()["executables"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert!(
            registry.catalog()["providers"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            registry
                .executable("codex", "1", json!({}))
                .err()
                .unwrap()
                .code,
            "unavailable_implementation"
        );
    }
}
