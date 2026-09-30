//! The store judge. Once the server has stopped, the trial reopens the run's
//! core store itself, through core's own API and the declaration the run
//! saved, and compares it with the server's last export. A history of
//! activations alone is also replayed from scratch and verified.

use anyhow::{Context, Result, anyhow};
use ontography::ProposalRuntime;
use ontography_app::declarations::{GraphDeclaration, parse_json};
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

pub async fn store(data: &Path, run: &str, export: &Value) -> Vec<String> {
    match check(data, run, export).await {
        Ok(problems) => problems,
        Err(error) => vec![format!("could not reopen the store: {error:#}")],
    }
}

fn ids(export: &Value, field: &str, key: &[&str]) -> BTreeSet<String> {
    export[field]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|record| {
            key.iter()
                .try_fold(record, |value, name| value.get(name))
                .and_then(Value::as_str)
                .map(String::from)
        })
        .collect()
}

async fn check(data: &Path, run: &str, export: &Value) -> Result<Vec<String>> {
    let directory = data.join("runs").join(run);
    let manifest: Value = serde_json::from_slice(&std::fs::read(directory.join("manifest.json"))?)?;
    let declaration = parse_json::<GraphDeclaration>(&manifest["declaration"].to_string())?;
    let kernel = declaration
        .compile()
        .context("compile the saved declaration")?;
    let runtime = ProposalRuntime::with_policy(kernel.clone(), Arc::new(ontography::DenyAll));
    let session = runtime
        .open_persistent(directory.join("core"))
        .map_err(|error| anyhow!("open: {error}"))?;
    let snapshot = session
        .try_snapshot()
        .await
        .map_err(|error| anyhow!("snapshot: {error}"))?;
    let state = snapshot.state();
    let mut problems = Vec::new();
    if Some(snapshot.revision().to_string().as_str()) != export["revision"].as_str() {
        problems.push(format!(
            "the store is at revision {}, the export at {}",
            snapshot.revision(),
            export["revision"]
        ));
    }
    let stored: BTreeSet<String> = state
        .activations()
        .keys()
        .map(ToString::to_string)
        .collect();
    if stored != ids(export, "activations", &["activation_id"]) {
        problems.push(format!(
            "the store holds {} activations, the export {}",
            stored.len(),
            export["activations"].as_array().map_or(0, Vec::len)
        ));
    }
    let stored: BTreeSet<String> = state.packages().keys().map(ToString::to_string).collect();
    if stored != ids(export, "packages", &["package", "package_id"]) {
        problems.push(format!(
            "the store holds {} packages, the export {}",
            stored.len(),
            export["packages"].as_array().map_or(0, Vec::len)
        ));
    }
    let fixed = state.retirements().is_empty();
    runtime.shutdown().await;
    drop(session);
    // Core can check a history of activations alone from its first record.
    if fixed {
        let runtime = ProposalRuntime::with_policy(kernel, Arc::new(ontography::DenyAll));
        match runtime.open_persistent_verified(directory.join("core")) {
            Ok(session) => {
                runtime.shutdown().await;
                drop(session);
            }
            Err(error) => problems.push(format!(
                "core's verified reopening refuses the store: {error}"
            )),
        }
    }
    Ok(problems)
}
