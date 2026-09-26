//! Durable app vocabulary additions. Core remains the authority for admission.
//!
//! One atomically replaced journal contains accepted additions and at most one
//! intent. The intent is synced before core mutation. Recovery tries the two
//! possible vocabulary bindings through public core opens; only a successful
//! open proves which side of the core commit survived. No historical replay or
//! direct database access is needed, including after graph rewrites.

use crate::{
    AppError, Result,
    declarations::{CompiledGraph, ContractDeclaration},
    persistence::{MAX_JSON_BYTES, read_json, write_json},
    state::ManagedRun,
};
use ontography::{
    AuthorityTag, Kernel, ProposalRuntime, Schema, SessionHandle, SessionTransitionError,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

#[derive(Clone, Debug, Default, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct VocabularyExtension {
    #[serde(default)]
    pub node_types: Vec<String>,
    #[serde(default)]
    pub object_types: Vec<String>,
    #[serde(default)]
    pub authority_tags: Vec<String>,
    #[serde(default)]
    pub contracts: Vec<ContractDeclaration>,
}

impl VocabularyExtension {
    /// Preserve live validators and topology; compile only newly added contracts.
    pub fn apply(&self, kernel: &Kernel) -> Result<Arc<Kernel>> {
        let node_types = additions(kernel.schema().node_types(), &self.node_types, "node type")?;
        let object_types = additions(
            kernel.schema().object_types(),
            &self.object_types,
            "object type",
        )?;
        let tags = additions(
            kernel.schema().authority_tags().map(AuthorityTag::id),
            &self.authority_tags,
            "authority tag",
        )?;
        let schema = Schema::new(
            node_types,
            object_types,
            tags.iter()
                .map(|tag| AuthorityTag::new(tag.as_str()))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(AppError::core)?,
        )
        .map_err(AppError::core)?;
        let mut contracts = kernel.contracts().to_vec();
        let mut identifiers: BTreeSet<_> = contracts
            .iter()
            .map(|contract| contract.id().to_owned())
            .collect();
        for declaration in &self.contracts {
            if !identifiers.insert(declaration.id.clone()) {
                return Err(AppError::new(
                    "rejected",
                    format!(
                        "contract {:?} already exists; extensions only add vocabulary",
                        declaration.id
                    ),
                ));
            }
            contracts.push(declaration.compile().map_err(AppError::core)?);
        }
        let next = Arc::new(
            Kernel::admit(
                kernel.id().clone(),
                schema,
                kernel.graph().clone(),
                contracts,
                kernel.node_definitions().to_vec(),
                kernel.edge_definitions().to_vec(),
                kernel.authority_transitions().to_vec(),
                kernel.roots().to_vec(),
            )
            .map_err(AppError::core)?,
        );
        // This bounded check proves monotonicity without materializing history.
        // SessionHandle::extend repeats admission against its current state.
        kernel
            .prepare_extension(&kernel.empty_state(), next.clone())
            .map_err(|error| AppError::new("rejected", error.to_string()))?;
        Ok(next)
    }
}

fn additions<'a>(
    existing: impl Iterator<Item = &'a str>,
    added: &[String],
    kind: &str,
) -> Result<BTreeSet<String>> {
    let mut result: BTreeSet<String> = existing.map(str::to_owned).collect();
    for value in added {
        if !result.insert(value.clone()) {
            return Err(AppError::new(
                "rejected",
                format!("{kind} {value:?} already exists; extensions only add vocabulary"),
            ));
        }
    }
    Ok(result)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExtensionJournal {
    version: u32,
    declaration_revision: String,
    accepted: Vec<VocabularyExtension>,
    pending: Option<VocabularyExtension>,
}

impl ExtensionJournal {
    fn apply(&self, mut kernel: Arc<Kernel>) -> Result<Arc<Kernel>> {
        for extension in &self.accepted {
            kernel = extension.apply(&kernel)?;
        }
        Ok(kernel)
    }

    fn check_size(&self, limit: u64) -> Result<()> {
        // Match write_json's pretty serialization and terminating newline.
        let size = serde_json::to_vec_pretty(self)?.len().saturating_add(1);
        if size as u64 > limit {
            return Err(AppError::new(
                "extension_metadata_too_large",
                "extension journal would exceed the persistent metadata reader limit",
            )
            .details(json!({"size":size,"limit":limit})));
        }
        Ok(())
    }
}

pub fn vocabulary(kernel: &Kernel) -> Value {
    json!({"node_types":kernel.schema().node_types().collect::<Vec<_>>(),
        "object_types":kernel.schema().object_types().collect::<Vec<_>>(),
        "authority_tags":kernel.schema().authority_tags().map(AuthorityTag::id).collect::<Vec<_>>(),
        "contracts":kernel.contracts().iter().map(|contract|json!({"id":contract.id(),"object_type":contract.object_type()})).collect::<Vec<_>>()})
}

fn same_vocabulary(left: &Kernel, right: &Kernel) -> bool {
    left.schema() == right.schema()
        && left
            .contracts()
            .iter()
            .map(|c| (c.id(), c.object_type()))
            .eq(right.contracts().iter().map(|c| (c.id(), c.object_type())))
}

impl ManagedRun {
    fn extensions_path(&self) -> PathBuf {
        self.directory.join("extensions.json")
    }

    fn write_extension_journal(&self, journal: &ExtensionJournal) -> Result<()> {
        journal.check_size(MAX_JSON_BYTES)?;
        write_json(&self.extensions_path(), journal)
    }

    fn extension_journal(&self) -> Result<ExtensionJournal> {
        let path = self.extensions_path();
        let journal: ExtensionJournal = match std::fs::metadata(&path) {
            Ok(_) => read_json(&path)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => ExtensionJournal {
                version: 1,
                declaration_revision: self.manifest.declaration_revision.clone(),
                accepted: vec![],
                pending: None,
            },
            Err(error) => return Err(error.into()),
        };
        if journal.version != 1
            || journal.declaration_revision != self.manifest.declaration_revision
        {
            return Err(AppError::new(
                "incompatible_extensions",
                "extension journal does not match the original declaration",
            ));
        }
        Ok(journal)
    }

    pub(crate) fn extension_summary(&self) -> Value {
        match self.extension_journal() {
            Ok(journal) => json!({"accepted":journal.accepted,"pending":journal.pending}),
            Err(error) => json!({"error":error}),
        }
    }

    /// Reconstruct a storage binding from the pinned original and accepted additions.
    pub fn compile_current_definition(&self) -> Result<CompiledGraph> {
        let journal = self.extension_journal()?;
        if journal.pending.is_some() {
            return Err(AppError::new(
                "extension_recovery_required",
                "an extension intent needs reconciliation before compiling the current definition",
            ));
        }
        let mut compiled = self
            .manifest
            .declaration
            .compile(&self.registry, &self.manifest.project)?;
        compiled.kernel = journal.apply(compiled.kernel)?;
        Ok(compiled)
    }

    /// Apply accepted additions to one compilation, preserving its validator Arcs.
    pub(crate) fn extend_compiled_kernel(&self, kernel: Arc<Kernel>) -> Result<Arc<Kernel>> {
        let journal = self.extension_journal()?;
        if journal.pending.is_some() {
            return Err(AppError::new(
                "extension_recovery_required",
                "an extension intent needs reconciliation",
            ));
        }
        journal.apply(kernel)
    }

    pub(crate) fn recover_extension_intent(&self) -> Result<()> {
        if self.extension_journal()?.pending.is_some() {
            // No executable is launched until durable vocabulary is settled.
            let (_runtime, _session) = self.open_extended()?;
        }
        Ok(())
    }

    pub(crate) fn open_extended(&self) -> Result<(ProposalRuntime, SessionHandle)> {
        let mut journal = self.extension_journal()?;
        let compiled = self
            .manifest
            .declaration
            .compile(&self.registry, &self.manifest.project)?;
        let accepted = journal.apply(compiled.kernel)?;
        let open = |kernel: Arc<Kernel>| -> Result<_> {
            self.registry
                .validate_bindings(self.manifest.declaration.execution_bindings(), &kernel)?;
            let runtime = ProposalRuntime::with_grammar(kernel, compiled.grammar.clone());
            let session = runtime
                .open_persistent(self.core_path()?)
                .map_err(AppError::core)?;
            Ok((runtime, session))
        };
        let Some(pending) = journal.pending.clone() else {
            return open(accepted);
        };
        let candidate = pending.apply(&accepted)?;
        let (opened, committed) =
            match open(candidate) {
                Ok(opened) => (opened, true),
                Err(candidate_error) => match open(accepted) {
                    Ok(opened) => (opened, false),
                    Err(accepted_error) => return Err(AppError::new(
                        "extension_recovery_failed",
                        "neither pending nor accepted vocabulary could reopen core storage",
                    )
                    .details(
                        json!({"pending_error":candidate_error,"accepted_error":accepted_error}),
                    )),
                },
            };
        if committed {
            journal.accepted.push(pending);
        }
        journal.pending = None;
        self.write_extension_journal(&journal)?;
        Ok(opened)
    }

    /// Extend one live run. The caller holds the exclusive run management mutex.
    pub async fn extend_vocabulary(&mut self, extension: VocabularyExtension) -> Result<Value> {
        self.extend_vocabulary_with_limit(extension, MAX_JSON_BYTES)
            .await
    }

    async fn extend_vocabulary_with_limit(
        &mut self,
        extension: VocabularyExtension,
        limit: u64,
    ) -> Result<Value> {
        let session = self.live()?.session.clone();
        let current = session.kernel().await.map_err(AppError::core)?;
        let mut journal = self.extension_journal()?;
        if let Some(pending) = journal.pending.clone() {
            let initial = self
                .manifest
                .declaration
                .compile(&self.registry, &self.manifest.project)?;
            let accepted = journal.apply(initial.kernel)?;
            let candidate = pending.apply(&accepted)?;
            if same_vocabulary(&current, &candidate) {
                journal.accepted.push(pending);
            } else if !same_vocabulary(&current, &accepted) {
                return Err(AppError::new(
                    "extension_recovery_failed",
                    "live vocabulary matches neither side of the saved intent",
                ));
            }
            journal.pending = None;
            self.write_extension_journal(&journal)?;
        }
        let next = extension.apply(&current)?;
        journal.pending = Some(extension.clone());
        let mut finalized = journal.clone();
        finalized.accepted.push(extension);
        finalized.pending = None;
        // The accepted form can be larger due to indentation inside its array.
        // Both durable outcomes must be readable before core can commit.
        journal.check_size(limit)?;
        finalized.check_size(limit)?;
        self.write_extension_journal(&journal)?;
        let revision = match session.extend(next.clone()).await {
            Ok(revision) => revision,
            Err(error) => {
                // Storage faults leave commit acknowledgement uncertain; retain
                // the intent for a public-core reopen to settle it.
                if !matches!(error, SessionTransitionError::Faulted(_)) {
                    journal.pending = None;
                    self.write_extension_journal(&journal)?;
                }
                let code = if matches!(error, SessionTransitionError::Extension(_)) {
                    "rejected"
                } else {
                    "core_error"
                };
                return Err(AppError::new(code, error.to_string()));
            }
        };
        self.write_extension_journal(&finalized).map_err(|error| {
            AppError::new(
                "extension_persistence_pending",
                "core committed the extension; its durable intent will be finalized on recovery",
            )
            .details(json!({"committed":true,"revision":revision.to_string(),"cause":error}))
        })?;
        Ok(
            json!({"run_id":self.manifest.run_id,"revision":revision.to_string(),"definition_fingerprint":next.fingerprint().to_string(),"vocabulary":vocabulary(&next)}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{declarations::GraphDeclaration, persistence::Paths, state::Service};

    #[tokio::test]
    async fn unreadable_final_journal_is_rejected_before_intent_or_core_mutation() {
        let directory = tempfile::tempdir().unwrap();
        let service =
            Service::new(Paths::initialize(directory.path().join("data")).unwrap()).unwrap();
        let started = service
            .start(
                GraphDeclaration::parse(include_str!("../examples/flow.json")).unwrap(),
                directory.path().to_owned(),
            )
            .await
            .unwrap();
        let managed = service
            .run(started["run_id"].as_str().unwrap())
            .await
            .unwrap();
        {
            let mut run = managed.lock().await;
            let extension = VocabularyExtension {
                node_types: vec!["NewNode".into()],
                ..Default::default()
            };
            let before = run.live().unwrap().session.frontier().revision();
            let mut pending = run.extension_journal().unwrap();
            pending.pending = Some(extension.clone());
            let pending_bytes = serde_json::to_vec_pretty(&pending).unwrap().len() + 1;
            let mut accepted = pending.clone();
            accepted.accepted.push(extension.clone());
            accepted.pending = None;
            assert!(serde_json::to_vec_pretty(&accepted).unwrap().len() + 1 > pending_bytes);
            let error = run
                .extend_vocabulary_with_limit(extension, pending_bytes as u64)
                .await
                .unwrap_err();
            assert_eq!(error.code, "extension_metadata_too_large");
            assert!(!run.extensions_path().exists());
            assert_eq!(run.live().unwrap().session.frontier().revision(), before);
        }
        service.shutdown().await.unwrap();
    }
}
