//! Explicit fixed-graph fact export, public-core restoration, and verified reopen.
//! These APIs do not promise a backup of artifact stores or invocation context.

use crate::catalog::Operation;
use crate::declarations::parse_json;
use crate::definition::RunDefinition;
use crate::registry::ImplementationRegistry;
use crate::state::{ManagedRun, Service};
use crate::{AppError, Result, persistence, views};
use ontography::{
    Activation, ContentDigest, Output, Payload, ProposalRuntime, StateParts, Trigger,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

const FORMAT: &str = "ontography.graph_facts";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FactArchive {
    format: String,
    version: u32,
    core_version: String,
    core_build: String,
    declaration: RunDefinition,
    declaration_revision: String,
    project: PathBuf,
    definition_id: String,
    definition_fingerprint: String,
    source_run_id: String,
    source_revision: String,
    source_admission: String,
    activations: BTreeMap<String, ActivationFact>,
    /// Exact payload bytes, keyed by the immutable accepted content digest.
    payload_evidence: BTreeMap<String, Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivationFact {
    trigger: TriggerFact,
    result: Vec<u8>,
    outputs: BTreeMap<String, OutputFact>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum TriggerFact {
    Root {
        node_id: String,
        authority: Vec<String>,
    },
    Packages {
        package_ids: Vec<String>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputFact {
    edge_id: Option<String>,
    object_type: String,
    authority: Vec<String>,
    content_digest: String,
}

pub fn operations() -> Vec<Operation> {
    let text = json!({"type":"string"});
    vec![
        Operation::new(
            "run.export_facts",
            "Export complete fixed-graph activation facts and exact output payload evidence to an absolute file. Materializes history. Rewrites, transfers, retirements, and vocabulary extensions reject; excludes artifacts, context, and executable state.",
            json!({"run_id":text,"path":text}),
            &["run_id", "path"],
            true,
        ),
        Operation::new(
            "run.verify",
            "Verify an already suspended persistent fixed-graph run by replaying its complete history and payload evidence through core, then release ownership. Rewrites, transfers, retirements, and vocabulary extensions reject; no executables launch.",
            json!({"run_id":text}),
            &["run_id"],
            true,
        ),
        Operation::new(
            "run.restore_facts",
            "Validate a graph-facts file through core restore and inspect a transient in-memory session. Returns no managed run and persists nothing. Artifacts, context, executions, and source admission lifecycle are not restored.",
            json!({"path":text,"limit":{"type":"integer","minimum":1,"maximum":1000}}),
            &["path"],
            true,
        ),
    ]
}

pub async fn dispatch(service: &Service, operation: &str, args: &Value) -> Result<Value> {
    if operation == "run.restore_facts" {
        return restore(&service.registry, args).await;
    }
    let run = service.run(views::field(args, "run_id")?).await?;
    let run = run.lock().await;
    match operation {
        "run.export_facts" => export(&run, args).await,
        "run.verify" => verify(&run).await,
        _ => Err(AppError::new("unknown_operation", operation)),
    }
}

fn path(args: &Value) -> Result<PathBuf> {
    let path = PathBuf::from(views::field(args, "path")?);
    if !path.is_absolute() {
        return Err(AppError::invalid("path must be absolute"));
    }
    Ok(path)
}

fn scope() -> Value {
    json!({"fixed_graph_facts":true,"exact_package_payloads":true,"activation_results":true,
        "artifact_dependencies":false,"artifact_bytes":false,"invocation_context":false,
        "execution_state":false,"source_admission_lifecycle_restored":false})
}

async fn export(run: &ManagedRun, args: &Value) -> Result<Value> {
    let path = path(args)?;
    let session = &run.live()?.session;
    let snapshot = session.try_snapshot().await.map_err(AppError::core)?;
    let parts = snapshot
        .state()
        .to_parts()
        .map_err(|error| AppError::new("unsupported_fact_history", error.to_string()))?;
    let mut evidence = BTreeMap::new();
    let mut activations = BTreeMap::new();
    for (id, activation) in parts.activations() {
        let trigger = match activation.trigger() {
            Trigger::Orig { node_id, authority } => TriggerFact::Root {
                node_id: node_id.to_string(),
                authority: authority.tags().map(|tag| tag.id().to_owned()).collect(),
            },
            Trigger::Pkgs { package_ids } => TriggerFact::Packages {
                package_ids: package_ids.iter().map(ToString::to_string).collect(),
            },
        };
        let mut outputs = BTreeMap::new();
        for (id, output) in activation.package_outputs() {
            let digest = output.content_digest();
            let key = digest.to_string();
            if !evidence.contains_key(&key) {
                let payload = session
                    .content(digest)
                    .await
                    .map_err(AppError::core)?
                    .ok_or_else(|| {
                        AppError::new(
                            "missing_payload_evidence",
                            format!("accepted payload {digest} is unavailable"),
                        )
                    })?;
                evidence.insert(key.clone(), payload.to_vec());
            }
            outputs.insert(
                id.to_string(),
                OutputFact {
                    edge_id: output.edge_id().map(str::to_owned),
                    object_type: output.object_type().to_owned(),
                    authority: output
                        .authority()
                        .tags()
                        .map(|tag| tag.id().to_owned())
                        .collect(),
                    content_digest: key,
                },
            );
        }
        activations.insert(
            id.to_string(),
            ActivationFact {
                trigger,
                result: activation.result().to_vec(),
                outputs,
            },
        );
    }
    let archive = FactArchive {
        format: FORMAT.into(),
        version: 1,
        core_version: ontography::VERSION.into(),
        core_build: crate::CORE_BUILD.into(),
        declaration: run.manifest.declaration.clone(),
        declaration_revision: run.manifest.declaration_revision.clone(),
        project: run.manifest.project.clone(),
        definition_id: parts.definition_id().to_string(),
        definition_fingerprint: parts.definition_fingerprint().to_string(),
        source_run_id: run.manifest.run_id.clone(),
        source_revision: snapshot.revision().to_string(),
        source_admission: views::status(snapshot.status()).into(),
        activations,
        payload_evidence: evidence,
    };
    persistence::write_json(&path, &archive)?;
    Ok(
        json!({"path":path,"format":FORMAT,"version":1,"revision":archive.source_revision,
        "activations":archive.activations.len(),"payloads":archive.payload_evidence.len(),"scope":scope()}),
    )
}

async fn restore(registry: &ImplementationRegistry, args: &Value) -> Result<Value> {
    let path = path(args)?;
    let limit = views::limit(args)?;
    // This explicitly requested full-history operation is not a bounded preview.
    let document = std::fs::read_to_string(&path)?;
    let archive: FactArchive = parse_json(&document).map_err(AppError::core)?;
    if archive.format != FORMAT
        || archive.version != 1
        || archive.core_version != ontography::VERSION
        || archive.core_build != crate::CORE_BUILD
    {
        return Err(AppError::new(
            "incompatible_facts",
            "fact format/core build does not match this installation",
        ));
    }
    if archive.declaration.fingerprint().map_err(AppError::core)? != archive.declaration_revision {
        return Err(AppError::new(
            "incompatible_facts",
            "declaration revision mismatch",
        ));
    }
    let compiled = archive.declaration.compile(registry, &archive.project)?;
    if compiled.kernel.id().to_string() != archive.definition_id
        || compiled.kernel.fingerprint().to_string() != archive.definition_fingerprint
    {
        return Err(AppError::new(
            "incompatible_facts",
            "definition identity/fingerprint mismatch",
        ));
    }
    let mut activations = BTreeMap::new();
    for (id, activation) in archive.activations {
        let trigger = match activation.trigger {
            TriggerFact::Root { node_id, authority } => Trigger::Orig {
                node_id: Arc::from(node_id),
                authority: views::authority(&authority)?,
            },
            TriggerFact::Packages { package_ids } => {
                let length = package_ids.len();
                let ids: BTreeSet<_> = package_ids
                    .iter()
                    .map(|id| views::package_id(id))
                    .collect::<Result<_>>()?;
                if ids.len() != length {
                    return Err(AppError::invalid("duplicate trigger package ID"));
                }
                Trigger::Pkgs { package_ids: ids }
            }
        };
        let mut outputs = BTreeMap::new();
        for (id, output) in activation.outputs {
            let authority = views::authority(&output.authority)?;
            let digest = views::digest(&output.content_digest)?;
            let value = match output.edge_id {
                Some(edge) => Output::new(edge, output.object_type, authority, digest),
                None => Output::outbound(output.object_type, authority, digest),
            };
            if outputs.insert(views::package_id(&id)?, value).is_some() {
                return Err(AppError::invalid("duplicate output package ID"));
            }
        }
        let value = Activation::new(trigger, Arc::from(activation.result), outputs);
        if activations
            .insert(views::activation_id(&id)?, value)
            .is_some()
        {
            return Err(AppError::invalid("duplicate activation ID"));
        }
    }
    let parts = StateParts::new(
        compiled.kernel.id().clone(),
        *compiled.kernel.fingerprint(),
        activations,
    );
    let mut evidence: BTreeMap<ContentDigest, Payload> = BTreeMap::new();
    for (digest, bytes) in archive.payload_evidence {
        if evidence
            .insert(views::digest(&digest)?, Arc::from(bytes))
            .is_some()
        {
            return Err(AppError::invalid("duplicate payload digest"));
        }
    }
    let runtime = ProposalRuntime::with_grammar(compiled.kernel, compiled.grammar);
    let session = runtime
        .restore(parts, &evidence)
        .map_err(|error| AppError::new("invalid_facts", error.to_string()))?;
    let overview = session
        .frontier_overview(limit)
        .await
        .map_err(AppError::core)?;
    Ok(
        json!({"validated":true,"transient":true,"retained":false,"durable":false,"run_id":null,
        "source_run_id":archive.source_run_id,"source_revision":archive.source_revision,
        "source_admission":archive.source_admission,"admission":views::status(session.status()),
        "revision":overview.revision().to_string(),"graph":views::graph(overview.kernel()),
        "frontier":{"counts":overview.counts().iter().map(|(node,c)|(node.to_string(),json!({"received":c.received(),"outbound":c.outbound()}))).collect::<BTreeMap<_,_>>(),
            "received":overview.received().iter().map(|(id,p)|views::package(*id,p)).collect::<Vec<_>>(),
            "outbound":overview.outbound().iter().map(|(id,p)|views::package(*id,p)).collect::<Vec<_>>()},
        "scope":scope()}),
    )
    // Dropping the session/runtime releases this inspection session; no registry entry exists.
}

async fn verify(run: &ManagedRun) -> Result<Value> {
    if run.live.is_some() {
        return Err(AppError::new(
            "run_active",
            "suspend this run before verified reopen",
        ));
    }
    if run.manifest.version != 1
        || run.manifest.core_version != ontography::VERSION
        || run.manifest.core_build != crate::CORE_BUILD
    {
        return Err(AppError::new(
            "incompatible_run",
            "run format/core build does not match this installation",
        ));
    }
    if run
        .manifest
        .declaration
        .fingerprint()
        .map_err(AppError::core)?
        != run.manifest.declaration_revision
    {
        return Err(AppError::new(
            "incompatible_run",
            "saved definition fingerprint mismatch",
        ));
    }
    let compiled = run.compile_current_definition()?;
    run.registry.validate_bindings(
        run.manifest.declaration.execution_bindings(),
        &compiled.kernel,
    )?;
    let runtime = ProposalRuntime::with_grammar(compiled.kernel, compiled.grammar);
    let session = runtime
        .open_persistent_verified(run.core_path()?)
        .map_err(|error| AppError::new("verification_failed", error.to_string()))?;
    let snapshot = session.try_snapshot().await.map_err(AppError::core)?;
    let result = json!({"verified":true,"run_id":run.manifest.run_id,
        "revision":snapshot.revision().to_string(),"admission":views::status(snapshot.status()),
        "activations":snapshot.state().activations().len(),"packages":snapshot.state().packages().len(),
        "ownership_released":true,"scope":"fixed_graph_activation_history_and_payload_evidence"});
    drop(snapshot);
    drop(session);
    drop(runtime);
    // Runtime shutdown would terminally close admission. Releasing ownership must not.
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::workflow;

    #[tokio::test]
    async fn exact_facts_restore_and_tampered_evidence_rejects() {
        let directory = tempfile::tempdir().unwrap();
        let mut run = workflow::tests::test_run(directory.path());
        let run_id = run.manifest.run_id.clone();
        let accepted = workflow::dispatch(&mut run, "workflow.submit", &json!({"run_id":run_id,"trigger":{"kind":"root","node_id":"A","authority":["work"]},"result":[0,13,10],"emissions":[{"edge_id":"A_to_B","payload":"exact\nbytes"}]})).await.unwrap();
        let snapshot = run.live().unwrap().session.try_snapshot().await.unwrap();
        let package = snapshot
            .state()
            .packages()
            .keys()
            .next()
            .unwrap()
            .to_string();
        drop(snapshot);
        workflow::dispatch(&mut run, "workflow.submit", &json!({"run_id":run_id,"trigger":{"kind":"packages","package_ids":[package]},"result":"consumed"})).await.unwrap();
        let file = directory.path().join("facts.json");
        export(&run, &json!({"path":file})).await.unwrap();
        let mut archive: Value = serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
        assert_eq!(
            archive["activations"][accepted["activation_id"].as_str().unwrap()]["result"],
            json!([0, 13, 10])
        );
        let restored = restore(&run.registry, &json!({"path":file})).await.unwrap();
        assert_eq!(restored["revision"], "2");
        assert_eq!(
            restored["frontier"]["received"].as_array().unwrap().len(),
            0
        );
        assert_eq!(restored["run_id"], Value::Null);
        assert_eq!(restored["retained"], false);
        *archive["payload_evidence"]
            .as_object_mut()
            .unwrap()
            .values_mut()
            .next()
            .unwrap() = json!([99]);
        persistence::write_json(&file, &archive).unwrap();
        assert_eq!(
            restore(&run.registry, &json!({"path":file}))
                .await
                .unwrap_err()
                .code,
            "invalid_facts"
        );
        run.suspend(false).await.unwrap();
    }

    #[tokio::test]
    async fn verified_reopen_releases_ownership_without_closing_admission() {
        let directory = tempfile::tempdir().unwrap();
        let mut run = workflow::tests::test_run(directory.path());
        assert_eq!(verify(&run).await.unwrap_err().code, "run_active");
        run.suspend(false).await.unwrap();
        assert_eq!(verify(&run).await.unwrap()["admission"], "open");
        assert!(run.live.is_none());
        run.resume().await.unwrap();
        assert_eq!(
            run.live().unwrap().session.status(),
            ontography::SessionStatus::Open
        );
        run.suspend(false).await.unwrap();
        run.manifest.core_build = "other".into();
        assert_eq!(verify(&run).await.unwrap_err().code, "incompatible_run");
    }

    #[tokio::test]
    async fn transfer_history_cannot_be_exported_or_verified_as_fixed_facts() {
        let directory = tempfile::tempdir().unwrap();
        let mut run = workflow::tests::test_run(directory.path());
        let run_id = run.manifest.run_id.clone();
        workflow::dispatch(&mut run, "workflow.submit", &json!({"run_id":run_id,"trigger":{"kind":"root","node_id":"A","authority":["work"]},"result":"done","emissions":[{"object_type":"Text","payload":"outbound"}]})).await.unwrap();
        let snapshot = run.live().unwrap().session.try_snapshot().await.unwrap();
        let id = snapshot
            .state()
            .packages()
            .keys()
            .next()
            .unwrap()
            .to_string();
        drop(snapshot);
        workflow::dispatch(
            &mut run,
            "workflow.transfer",
            &json!({"run_id":run_id,"package_id":id,"edge_id":"A_to_B"}),
        )
        .await
        .unwrap();
        let file = directory.path().join("facts.json");
        assert_eq!(
            export(&run, &json!({"path":file})).await.unwrap_err().code,
            "unsupported_fact_history"
        );
        assert!(!file.exists());
        run.suspend(false).await.unwrap();
        assert_eq!(verify(&run).await.unwrap_err().code, "verification_failed");
        run.resume().await.unwrap();
        run.suspend(false).await.unwrap();
    }
}
