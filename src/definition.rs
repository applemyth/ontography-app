//! Durable construction sources for logical graphs and executable applications.
use crate::{
    AppError, Result,
    application::ApplicationDeclaration,
    declarations::{CompiledGraph, GraphDeclaration, RewriteProductionDeclaration},
    registry::{ExecutionBinding, ImplementationRegistry},
};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "declaration", rename_all = "snake_case")]
pub enum RunDefinition {
    Logical(GraphDeclaration),
    Application(ApplicationDeclaration),
}

impl RunDefinition {
    pub fn id(&self) -> &str {
        match self {
            Self::Logical(d) => &d.id,
            Self::Application(d) => &d.definition_id,
        }
    }
    pub fn fingerprint(&self) -> Result<String> {
        match self {
            Self::Logical(d) => d.fingerprint().map_err(AppError::core),
            Self::Application(d) => d.fingerprint(),
        }
    }
    pub fn compile(
        &self,
        registry: &ImplementationRegistry,
        project: &Path,
    ) -> Result<CompiledGraph> {
        match self {
            Self::Logical(d) => d.compile().map_err(AppError::core),
            Self::Application(d) => {
                let compiled = d.compile(registry, project)?;
                Ok(CompiledGraph {
                    kernel: compiled.kernel,
                    grammar: compiled.grammar,
                })
            }
        }
    }
    pub fn rewrites(&self) -> &[RewriteProductionDeclaration] {
        match self {
            Self::Logical(d) => &d.rewrites,
            Self::Application(d) => &d.rewrites,
        }
    }
    pub fn execution_bindings(&self) -> &[ExecutionBinding] {
        match self {
            Self::Logical(d) => &d.execution_bindings,
            Self::Application(_) => &[],
        }
    }
}
impl From<GraphDeclaration> for RunDefinition {
    fn from(declaration: GraphDeclaration) -> Self {
        Self::Logical(declaration)
    }
}
