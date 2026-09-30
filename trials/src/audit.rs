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

/// Problems the store shows, and whether core also replayed and verified
/// its history from scratch.
pub async fn store(data: &Path, run: &str, export: &Value) -> (Vec<String>, bool) {
    match check(data, run, export).await {
        Ok(checked) => checked,
        Err(error) => (
            vec![format!("could not reopen the store: {error:#}")],
            false,
        ),
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

async fn check(data: &Path, run: &str, export: &Value) -> Result<(Vec<String>, bool)> {
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
    // Core replays from scratch only a fixed graph's activations: no
    // retirements, and no edit since the run began.
    let initial = kernel.fingerprint().to_string();
    let fixed = state.retirements().is_empty()
        && export["current_fingerprint"].as_str() == Some(initial.as_str());
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
    Ok((problems, fixed))
}
