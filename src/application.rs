//! Pinned native/project declarations resolved only through trusted core registries.

use crate::declarations::{Edits, SavedProductions, edit_policy};
use crate::registry::ImplementationRegistry;
use crate::{AppError, Result};
use ontography::{Application, EditPolicy, Kernel};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{path::Path, sync::Arc};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationFormat {
    Native,
    Project,
}

#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApplicationDeclaration {
    pub version: u32,
    pub format: ApplicationFormat,
    pub document: String,
    /// Which graph edits the application's run accepts.
    #[serde(default, skip_serializing_if = "Edits::is_fixed")]
    pub edits: Edits,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(skip)]
    pub rewrites: SavedProductions,
    pub definition_id: String,
    /// Exact registered adapter descriptors, including versions and configuration schemas.
    pub registry: Value,
    /// Expanded configuration, component descriptions/specs and kernel identity.
    pub resolution_fingerprint: String,
}

pub struct CompiledApplication {
    pub application: Application,
    pub kernel: Arc<Kernel>,
    pub policy: Arc<dyn EditPolicy>,
}

impl ApplicationDeclaration {
    pub fn new(
        format: ApplicationFormat,
        document: String,
        edits: Edits,
        registry: &ImplementationRegistry,
        project: &Path,
    ) -> Result<Self> {
        let (application, resolution_fingerprint) = resolve(format, &document, registry, project)?;
        Ok(Self {
            version: 1,
            format,
            document,
            edits,
            rewrites: None,
            definition_id: application.kernel().id().to_string(),
            registry: registry.catalog(),
            resolution_fingerprint,
        })
    }

    pub fn fingerprint(&self) -> Result<String> {
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(self)?)))
    }

    pub fn compile(
        &self,
        registry: &ImplementationRegistry,
        project: &Path,
    ) -> Result<CompiledApplication> {
        if self.version != 1 || self.registry != registry.catalog() {
            return Err(AppError::new(
                "incompatible_application",
                "application format or trusted registry identity changed",
            ));
        }
        let (application, resolved) = resolve(self.format, &self.document, registry, project)?;
        if resolved != self.resolution_fingerprint
            || application.kernel().id().to_string() != self.definition_id
        {
            return Err(AppError::new(
                "incompatible_application",
                "provider resolution changed: expanded configuration, component metadata, project identity, or kernel differs from the saved declaration",
            ));
        }
        let policy = edit_policy(self.edits, &self.rewrites);
        let initial = application.kernel();
        // Core exposes the admitted constituents, but does not expose its retained Arc.
        let kernel = Arc::new(
            Kernel::admit(
                initial.id().clone(),
                initial.schema().clone(),
                initial.graph().clone(),
                initial.contracts().to_vec(),
                initial.node_definitions().to_vec(),
                initial.edge_definitions().to_vec(),
                initial.authority_transitions().to_vec(),
                initial.roots().to_vec(),
            )
            .map_err(AppError::core)?,
        );
        Ok(CompiledApplication {
            application: application.with_policy(policy.clone()),
            kernel,
            policy,
        })
    }
}

fn resolve(
    format: ApplicationFormat,
    document: &str,
    registry: &ImplementationRegistry,
    project: &Path,
) -> Result<(Application, String)> {
    if !project.is_absolute() || !project.is_dir() {
        return Err(AppError::invalid(
            "project must be an absolute existing directory",
        ));
    }
    let (application, mut resolution) = match format {
        ApplicationFormat::Native => {
            let application = registry.native_application(document)?;
            (
                application,
                json!({"application":serde_json::from_str::<Value>(document)?}),
            )
        }
        ApplicationFormat::Project => {
            let prepared = registry.prepare_project(document, project)?;
            let evidence = json!({"application":serde_json::from_str::<Value>(&prepared.application_json)?,
                "components":prepared.components,"component_specs":prepared.component_specs,
                "project":prepared.project_root});
            (prepared.application, evidence)
        }
    };
    resolution["definition_id"] = json!(application.kernel().id().to_string());
    resolution["definition_fingerprint"] = json!(application.kernel().fingerprint().to_string());
    let fingerprint = format!("{:x}", Sha256::digest(serde_json::to_vec(&resolution)?));
    Ok((application, fingerprint))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::ImplementationDescriptor;
    use ontography::{
        ApplicationRegistry, Contract, IngressMode,
        project::{
            BoundComponent, ComponentBindings, ComponentDescription, ComponentProvider,
            ProjectComponent,
        },
    };
    use std::{
        collections::BTreeMap,
        sync::atomic::{AtomicUsize, Ordering},
    };

    fn descriptor(id: &str) -> ImplementationDescriptor {
        ImplementationDescriptor {
            id: id.into(),
            version: "1".into(),
            description: "Test-only".into(),
            configuration_schema: json!({"type":"object"}),
        }
    }

    fn register_native(registry: &mut ApplicationRegistry) {
        registry
            .register_contract(Contract::new("result", "Result", |_| Ok(())).unwrap())
            .unwrap();
        registry
            .register_node_implementation("fixture", |_| {
                Ok::<_, String>(|_: ontography::ApplicationContext| async {
                    Ok::<(), ontography::ExecutionFailure>(())
                })
            })
            .unwrap();
    }

    #[test]
    fn native_declaration_reconstructs_identical_kernel_and_rejects_catalog_changes() {
        let mut native = ApplicationRegistry::new();
        register_native(&mut native);
        let registry = ImplementationRegistry::new(native, vec![descriptor("fixture")]);
        let directory = tempfile::tempdir().unwrap();
        let document = json!({"id":"native-test","entry":"a","node_definitions":{"n":{"types":["Node"],"result_contract":"result","root_authority":[],"implementation":{"kind":"fixture","config":{}}}},"edge_definitions":{},"nodes":{"a":{"definition":"n"}},"edges":{}}).to_string();
        let declaration = ApplicationDeclaration::new(
            ApplicationFormat::Native,
            document,
            Edits::Fixed,
            &registry,
            directory.path(),
        )
        .unwrap();
        let compiled = declaration.compile(&registry, directory.path()).unwrap();
        assert_eq!(
            compiled.kernel.fingerprint(),
            compiled.application.kernel().fingerprint()
        );
        assert_eq!(declaration.definition_id, "native-test");
        assert!(
            declaration
                .compile(&ImplementationRegistry::default(), directory.path())
                .is_err()
        );
    }

    struct FixtureProvider(Arc<AtomicUsize>);
    struct FixtureComponent(usize);
    impl ProjectComponent for FixtureComponent {
        fn description(&self) -> ComponentDescription {
            ComponentDescription {
                identity: format!("fixture-v{}", self.0),
                description: "Test-only".into(),
                types: vec!["Node".into()],
                result_contract: "result".into(),
                inputs: BTreeMap::new(),
                outputs: BTreeMap::new(),
                dynamic_inputs: false,
                dynamic_outputs: false,
                ingress_modes: vec![IngressMode::Any],
                configuration_schema: None,
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
            register_native(registry);
            Ok(specs
                .keys()
                .map(|key| {
                    (
                        key.clone(),
                        Arc::new(FixtureComponent(self.0.load(Ordering::SeqCst)))
                            as Arc<dyn ProjectComponent>,
                    )
                })
                .collect())
        }
    }

    #[test]
    fn changed_provider_resolution_rejects_even_with_same_registered_provider_version() {
        let revision = Arc::new(AtomicUsize::new(1));
        let mut registry = ImplementationRegistry::default();
        registry
            .register_provider(
                descriptor("fixture"),
                Arc::new(FixtureProvider(revision.clone())),
            )
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let document = json!({"id":"project-test","entry":"worker","components":{"component":{"provider":"fixture"}},"nodes":{"worker":{"component":"component","config":{},"root_authority":[]}},"connections":[]}).to_string();
        let declaration = ApplicationDeclaration::new(
            ApplicationFormat::Project,
            document,
            Edits::Fixed,
            &registry,
            directory.path(),
        )
        .unwrap();
        declaration.compile(&registry, directory.path()).unwrap();
        revision.store(2, Ordering::SeqCst);
        assert_eq!(
            declaration
                .compile(&registry, directory.path())
                .err()
                .unwrap()
                .code,
            "incompatible_application"
        );
    }
}
