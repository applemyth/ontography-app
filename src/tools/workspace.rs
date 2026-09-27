//! Managed filesystem views retain their checkout handles independently of clients.

use super::content::{RootPage, local_path, page, resolved_view};
use super::workflow::{check_limit, content_id_schema, default_limit, operation, parse_args};
use crate::catalog::Operation;
use crate::state::{Checkpoint, ManagedRun, WorkspaceHandle};
use crate::{AppError, Result};
use ontography::{
    ContentId, ContentStore,
    package::ResolvedPackage,
    workspace::{WorkspaceError, WorkspaceStore},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::path::PathBuf;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportInput {
    path: PathBuf,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckoutInput {
    root: ContentId,
    #[serde(default)]
    read_only: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CaptureInput {
    checkout_id: String,
    base: ContentId,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointInput {
    checkpoint_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseInput {
    checkout_id: String,
    #[serde(default)]
    discard_changes: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListInput {
    #[serde(default)]
    checkout_offset: usize,
    #[serde(default)]
    checkpoint_offset: usize,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn workspace_error(error: WorkspaceError) -> AppError {
    let code = match &error {
        WorkspaceError::UnsupportedCopyOnWrite { .. } => "unsupported_copy_on_write",
        WorkspaceError::Changed(_) => "workspace_changed",
        WorkspaceError::Limit(_) => "workspace_limit",
        WorkspaceError::Invalid(_) => "invalid_workspace",
        WorkspaceError::Git(_) => "git_error",
        _ => "core_error",
    };
    AppError::new(code, error.to_string())
}

async fn stores(run: &ManagedRun) -> Result<(ContentStore, WorkspaceStore)> {
    let content = run
        .live()?
        .session
        .content_store()
        .await
        .map_err(AppError::core)?;
    let workspace = WorkspaceStore::new(content.clone(), run.workspace()?);
    Ok((content, workspace))
}

fn handle<'a>(run: &'a ManagedRun, id: &str) -> Result<&'a WorkspaceHandle> {
    run.live()?.checkouts.get(id).ok_or_else(|| {
        AppError::new(
            "expired_handle",
            "checkout does not belong to this live run",
        )
    })
}

async fn checkout_destination(run: &ManagedRun) -> Result<(String, PathBuf)> {
    let id = uuid::Uuid::new_v4().to_string();
    let parent = run.directory.join("checkouts");
    tokio::fs::create_dir_all(&parent).await?;
    Ok((id.clone(), parent.join(id)))
}

async fn retain_package(content: &ContentStore, package: &ResolvedPackage) -> Result<()> {
    super::content::retain(content, &package.dependencies()).await
}

/// Persist roots only after the complete closure is retained. Replacing metadata
/// never releases shared core tags that may belong to another checkpoint.
async fn checkpoint(
    run: &mut ManagedRun,
    id: &str,
    base: ContentId,
    content: &ContentStore,
    workspace: &WorkspaceStore,
) -> Result<ResolvedPackage> {
    let path = handle(run, id)?.checkout.path().to_owned();
    let package = workspace
        .capture(path, base)
        .await
        .map_err(workspace_error)?;
    retain_package(content, &package).await?;
    let previous = run.manifest.checkpoints.insert(
        id.to_owned(),
        Checkpoint {
            base,
            root: package.root(),
            dependencies: package.dependencies(),
        },
    );
    if let Err(error) = run.save() {
        if let Some(previous) = previous {
            run.manifest.checkpoints.insert(id.to_owned(), previous);
        } else {
            run.manifest.checkpoints.remove(id);
        }
        return Err(error);
    }
    Ok(package)
}

pub async fn dispatch(run: &mut ManagedRun, name: &str, args: &Value) -> Result<Value> {
    match name {
        "workspace.list" => {
            let input: ListInput = parse_args(run, args)?;
            let checkouts = run.live.as_ref().map(|live|live.checkouts.iter().map(|(id,h)|json!({"checkout_id":id,"base":h.base,"path":h.checkout.path()})).collect::<Vec<_>>()).unwrap_or_default();
            let checkpoints = run.manifest.checkpoints.iter().map(|(id,c)|json!({"checkpoint_id":id,"base":c.base,"root":c.root,"dependency_count":c.dependencies.len()})).collect::<Vec<_>>();
            Ok(
                json!({"checkouts":page(&checkouts,input.checkout_offset,input.limit)?,"checkpoints":page(&checkpoints,input.checkpoint_offset,input.limit)?}),
            )
        }
        // Internal workflow artifact paths; neither operation is registered.
        "workspace.import" => {
            let input: ImportInput = parse_args(run, args)?;
            let (content, workspace) = stores(run).await?;
            let package = workspace
                .import_directory(local_path(run, &input.path))
                .await
                .map_err(workspace_error)?;
            retain_package(&content, &package).await?;
            resolved_view(&package, 0, default_limit())
        }
        "workspace.open" => {
            let input: RootPage = parse_args(run, args)?;
            check_limit(input.limit)?;
            let (_, workspace) = stores(run).await?;
            let package = workspace.open(input.root).await.map_err(workspace_error)?;
            resolved_view(&package, input.offset, input.limit)
        }
        "workspace.checkout" => {
            let input: CheckoutInput = parse_args(run, args)?;
            let (content, workspace) = stores(run).await?;
            let package = workspace.open(input.root).await.map_err(workspace_error)?;
            retain_package(&content, &package).await?;
            let (id, path) = checkout_destination(run).await?;
            let checkout = workspace
                .checkout(&package, &path)
                .await
                .map_err(workspace_error)?;
            if input.read_only {
                checkout.set_read_only().await.map_err(workspace_error)?;
            }
            run.live_mut()?.checkouts.insert(
                id.clone(),
                WorkspaceHandle {
                    checkout,
                    base: input.root,
                },
            );
            Ok(
                json!({"checkout_id":id,"path":path,"base":input.root,"read_only_permissions":input.read_only}),
            )
        }
        "workspace.capture" | "workspace.checkpoint" => {
            let input: CaptureInput = parse_args(run, args)?;
            let (content, workspace) = stores(run).await?;
            let package = if name == "workspace.checkpoint" {
                checkpoint(run, &input.checkout_id, input.base, &content, &workspace).await?
            } else {
                let path = handle(run, &input.checkout_id)?.checkout.path();
                let package = workspace
                    .capture(path, input.base)
                    .await
                    .map_err(workspace_error)?;
                retain_package(&content, &package).await?;
                package
            };
            let mut result = resolved_view(&package, 0, default_limit())?;
            result["base"] = json!(input.base);
            result["checkout_id"] = json!(input.checkout_id);
            if name == "workspace.checkpoint" {
                result["checkpoint_id"] = result["checkout_id"].clone();
            }
            Ok(result)
        }
        "workspace.restore" => {
            let input: CheckpointInput = parse_args(run, args)?;
            let saved = run
                .manifest
                .checkpoints
                .get(&input.checkpoint_id)
                .cloned()
                .ok_or_else(|| AppError::new("not_found", "checkpoint does not exist"))?;
            let (content, workspace) = stores(run).await?;
            let package = workspace.open(saved.root).await.map_err(workspace_error)?;
            retain_package(&content, &package).await?;
            let (id, path) = checkout_destination(run).await?;
            let checkout = workspace
                .checkout(&package, &path)
                .await
                .map_err(workspace_error)?;
            run.live_mut()?.checkouts.insert(
                id.clone(),
                WorkspaceHandle {
                    checkout,
                    base: saved.root,
                },
            );
            Ok(
                json!({"checkout_id":id,"path":path,"base":saved.root,"source_checkpoint":input.checkpoint_id}),
            )
        }
        "workspace.release" => {
            let input: ReleaseInput = parse_args(run, args)?;
            let base = handle(run, &input.checkout_id)?.base;
            let saved = if input.discard_changes {
                None
            } else {
                let (content, workspace) = stores(run).await?;
                let package =
                    checkpoint(run, &input.checkout_id, base, &content, &workspace).await?;
                Some(json!({"checkpoint_id":input.checkout_id,"root":package.root()}))
            };
            let owned = run
                .live_mut()?
                .checkouts
                .remove(&input.checkout_id)
                .expect("handle checked under run mutex");
            owned.checkout.remove().await.map_err(workspace_error)?;
            Ok(
                json!({"released":input.checkout_id,"checkpoint":saved,"discarded_changes":input.discard_changes}),
            )
        }
        _ => Err(AppError::new("unknown_operation", name)),
    }
}

pub fn operations() -> Vec<Operation> {
    let id = content_id_schema();
    let text = json!({"type":"string"});
    let limits = json!({"type":"integer","minimum":1,"maximum":1000});
    let offset = json!({"type":"integer","minimum":0});
    vec![
        operation(
            "workspace.open",
            "Validate a package as a filesystem workspace and page its resolved entries.",
            json!({"root":id,"offset":offset,"limit":limits}),
            &["root"],
            false,
        ),
        operation(
            "workspace.checkout",
            "Create and retain a private OS copy-on-write checkout. Unsupported filesystems fail explicitly. Read-only permissions are advisory.",
            json!({"root":id,"read_only":{"type":"boolean"}}),
            &["root"],
            true,
        ),
        operation(
            "workspace.list",
            "Page retained checkout paths and saved checkpoint metadata, including while suspended.",
            json!({"checkout_offset":offset,"checkpoint_offset":offset,"limit":limits}),
            &[],
            false,
        ),
        operation(
            "workspace.capture",
            "Capture edits against the explicit immutable base, retaining the entire package closure. Stop checkout writers first. Publication is a separate workflow operation.",
            json!({"checkout_id":text,"base":id}),
            &["checkout_id", "base"],
            true,
        ),
        operation(
            "workspace.checkpoint",
            "Capture a stopped checkout against an explicit base and durably protect its complete closure for recovery.",
            json!({"checkout_id":text,"base":id}),
            &["checkout_id", "base"],
            true,
        ),
        operation(
            "workspace.release",
            "Remove a stopped checkout, saving a checkpoint first by default. discard_changes=true explicitly discards uncheckpointed edits.",
            json!({"checkout_id":text,"discard_changes":{"type":"boolean"}}),
            &["checkout_id"],
            true,
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::workflow::tests::test_run;

    #[tokio::test]
    async fn changed_workspaces_checkpoint_gc_suspend_and_restore() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        std::fs::create_dir(&source).unwrap();
        std::fs::write(source.join("keep"), "same").unwrap();
        std::fs::write(source.join("edit"), "before").unwrap();
        std::fs::write(source.join("remove"), "delete").unwrap();
        std::os::unix::fs::symlink("keep", source.join("link")).unwrap();
        let mut run = test_run(dir.path());
        let run_id = run.manifest.run_id.clone();
        let imported = dispatch(
            &mut run,
            "workspace.import",
            &json!({"run_id":run_id,"path":"source"}),
        )
        .await
        .unwrap();
        let base = imported["root"].clone();
        let keep_before = imported["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["path"] == "keep")
            .unwrap()["package"]
            .clone();
        let checkout = dispatch(
            &mut run,
            "workspace.checkout",
            &json!({"run_id":run_id,"root":base}),
        )
        .await
        .unwrap();
        let checkout_id = checkout["checkout_id"].clone();
        let path = PathBuf::from(checkout["path"].as_str().unwrap());
        std::fs::write(path.join("edit"), "after").unwrap();
        std::fs::remove_file(path.join("remove")).unwrap();
        std::fs::write(path.join("added"), "new").unwrap();
        let captured = dispatch(
            &mut run,
            "workspace.checkpoint",
            &json!({"run_id":run_id,"checkout_id":checkout_id,"base":base}),
        )
        .await
        .unwrap();
        assert_ne!(captured["root"], base);
        let keep_after = captured["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["path"] == "keep")
            .unwrap()["package"]
            .clone();
        assert_eq!(keep_before, keep_after);
        let paths = captured["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["path"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(paths.contains(&"added"));
        assert!(!paths.contains(&"remove"));
        let store = run.live().unwrap().session.content_store().await.unwrap();
        let incidental = store.import_bytes(b"incidental".to_vec()).await.unwrap();
        store.release(incidental).await.unwrap();
        store.collect_garbage().await.unwrap();
        drop(store);
        run.suspend(false).await.unwrap();
        assert!(!path.exists());
        run.resume().await.unwrap();
        // Reacquiring in the same server proves every core/store owner released.
        // Reconstructing from disk also proves the saved closure survives a
        // management-server restart, independently of the old manifest object.
        run.suspend(false).await.unwrap();
        drop(run);
        let manifest = crate::persistence::read_json(&dir.path().join("manifest.json")).unwrap();
        let mut run = ManagedRun {
            manifest,
            directory: dir.path().to_owned(),
            live: None,
            recovery_checkouts: std::collections::BTreeMap::new(),
            registry: std::sync::Arc::new(crate::registry::ImplementationRegistry::default()),
        };
        run.resume().await.unwrap();
        let restored = dispatch(
            &mut run,
            "workspace.restore",
            &json!({"run_id":run_id,"checkpoint_id":checkout_id}),
        )
        .await
        .unwrap();
        let restored = PathBuf::from(restored["path"].as_str().unwrap());
        assert_eq!(
            std::fs::read_to_string(restored.join("edit")).unwrap(),
            "after"
        );
        assert_eq!(
            std::fs::read_to_string(restored.join("added")).unwrap(),
            "new"
        );
        assert!(!restored.join("remove").exists());
        assert_eq!(
            std::fs::read_link(restored.join("link")).unwrap(),
            PathBuf::from("keep")
        );
    }
}
