//! Manager workspace handles and publication of workflow output to the filesystem.

use super::WorkflowPayload;
use crate::{AppError, Result, state::ManagedRun, tools, views};
use ontography::{package::ResolvedEntryKind, workspace::WorkspaceStore};
use serde_json::{Value, json};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt, symlink},
    path::{Component, Path, PathBuf},
};

/// Translate manager handles to the app's retained checkout/checkpoint machinery.
pub async fn workspace(run: &mut ManagedRun, args: &Value) -> Result<Value> {
    let run_id = run.manifest.run_id.clone();
    match views::field(args, "action")? {
        "open" => {
            let path = args.get("path").filter(|value| !value.is_null());
            let root = args.get("root").filter(|value| !value.is_null());
            let saved = args.get("workspace_id").filter(|value| !value.is_null());
            if usize::from(path.is_some())
                + usize::from(root.is_some())
                + usize::from(saved.is_some())
                != 1
            {
                return Err(AppError::invalid(
                    "Open a workspace from one path, node output, or saved workspace_id",
                ));
            }
            let checkout = if let Some(id) = saved {
                tools::workspace::dispatch(
                    run,
                    "workspace.restore",
                    &json!({"run_id":run_id,"checkpoint_id":id}),
                )
                .await?
            } else {
                let root = if let Some(path) = path {
                    tools::workspace::dispatch(
                        run,
                        "workspace.import",
                        &json!({"run_id":run_id,"path":path}),
                    )
                    .await?["root"]
                        .clone()
                } else {
                    root.expect("one source checked").clone()
                };
                tools::workspace::dispatch(
                    run,
                    "workspace.checkout",
                    &json!({"run_id":run_id,"root":root}),
                )
                .await?
            };
            Ok(json!({"workspace_id":checkout["checkout_id"],"path":checkout["path"]}))
        }
        "capture" | "release" => {
            let id = views::field(args, "workspace_id")?;
            let action = views::field(args, "action")?;
            let handle = run.live()?.checkouts.get(id);
            if handle.is_none() && action == "release" && run.manifest.checkpoints.contains_key(id)
            {
                return Ok(json!({"workspace_id":id,"released":true,"captured":true}));
            }
            let handle = handle.ok_or_else(|| {
                AppError::new(
                    "unknown_workspace",
                    "This workspace is no longer open; reopen its saved workspace_id",
                )
            })?;
            let path = handle.checkout.path().to_owned();
            let base = handle.base;
            if action == "capture" {
                tools::workspace::dispatch(
                    run,
                    "workspace.checkpoint",
                    &json!({"run_id":run_id,"checkout_id":id,"base":base}),
                )
                .await?;
                Ok(json!({"workspace_id":id,"path":path,"captured":true}))
            } else {
                tools::workspace::dispatch(
                    run,
                    "workspace.release",
                    &json!({"run_id":run_id,"checkout_id":id}),
                )
                .await?;
                Ok(json!({"workspace_id":id,"released":true,"captured":true}))
            }
        }
        _ => Err(AppError::invalid(
            "Workspace action must be open, capture, or release",
        )),
    }
}

/// Export into a new destination. A sibling staging object is published only
/// after all bytes and metadata have been written, using an exclusive rename.
pub async fn export(run: &ManagedRun, payload: WorkflowPayload, path: &Path) -> Result<Value> {
    let path = tools::content::local_path(run, path);
    let name = path
        .file_name()
        .ok_or_else(|| AppError::invalid("Export requires a new file or directory name"))?;
    let parent = fs::canonicalize(
        path.parent()
            .ok_or_else(|| AppError::invalid("Export destination needs a parent directory"))?,
    )?;
    let destination = parent.join(name);
    match fs::symlink_metadata(&destination) {
        Ok(_) => {
            return Err(AppError::new(
                "destination_exists",
                "Export never overwrites an existing file or directory",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let kind = match payload {
        WorkflowPayload::Message { message } => {
            let mut staging = Staging::new(&parent, false)?;
            let mut file = OpenOptions::new().write(true).open(&staging.path)?;
            file.write_all(message.as_bytes())?;
            file.sync_all()?;
            staging.publish(&destination)?;
            "message"
        }
        WorkflowPayload::Workspace(envelope) => {
            let content = run
                .live()?
                .session
                .content_store()
                .await
                .map_err(AppError::core)?;
            let workspaces = WorkspaceStore::new(content.clone(), run.workspace()?);
            // Core validates directory parents, path normalization, case collisions,
            // and symlink chains before any export files are created.
            let package = workspaces
                .open(envelope.ontography_package)
                .await
                .map_err(AppError::core)?;
            let mut staging = Staging::new(&parent, true)?;
            for entry in package.entries() {
                if !entry.path.is_empty() && matches!(entry.kind, ResolvedEntryKind::Directory) {
                    fs::create_dir(entry_path(&staging.path, &entry.path)?)?;
                }
            }
            for entry in package.entries() {
                if let ResolvedEntryKind::File {
                    content: id,
                    executable,
                } = &entry.kind
                {
                    let file = entry_path(&staging.path, &entry.path)?;
                    content
                        .export_file(*id, &file)
                        .await
                        .map_err(AppError::core)?;
                    fs::set_permissions(
                        &file,
                        fs::Permissions::from_mode(if *executable { 0o755 } else { 0o644 }),
                    )?;
                    File::open(&file)?.sync_all()?;
                }
            }
            // Create links last; no later export write can traverse one.
            for entry in package.entries() {
                if let ResolvedEntryKind::Symlink { target } = &entry.kind {
                    symlink(target, entry_path(&staging.path, &entry.path)?)?;
                }
            }
            File::open(&staging.path)?.sync_all()?;
            staging.publish(&destination)?;
            "workspace"
        }
    };
    Ok(json!({"path":destination,"kind":kind,"exported":true}))
}

fn entry_path(root: &Path, entry: &str) -> Result<PathBuf> {
    if entry.is_empty()
        || Path::new(entry)
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(AppError::new(
            "invalid_workspace",
            "An exported entry has an invalid relative path",
        ));
    }
    Ok(root.join(entry))
}

/// Cleanup also runs if an export future is cancelled during a content read.
struct Staging {
    path: PathBuf,
    directory: bool,
    published: bool,
}

impl Staging {
    fn new(parent: &Path, directory: bool) -> Result<Self> {
        let path = parent.join(format!(".ontography-export-{}.tmp", uuid::Uuid::new_v4()));
        if directory {
            fs::DirBuilder::new().mode(0o700).create(&path)?;
        } else {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)?;
        }
        Ok(Self {
            path,
            directory,
            published: false,
        })
    }

    fn publish(&mut self, destination: &Path) -> Result<()> {
        rustix::fs::renameat_with(
            rustix::fs::CWD,
            &self.path,
            rustix::fs::CWD,
            destination,
            rustix::fs::RenameFlags::NOREPLACE,
        )
        .map_err(|error| {
            if error == rustix::io::Errno::EXIST || error == rustix::io::Errno::NOTEMPTY {
                AppError::new(
                    "destination_exists",
                    "Export never overwrites an existing file or directory",
                )
            } else {
                std::io::Error::from(error).into()
            }
        })?;
        self.published = true;
        Ok(())
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        if !self.published {
            if self.directory {
                let _ = fs::remove_dir_all(&self.path);
            } else {
                let _ = fs::remove_file(&self.path);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::workflow::tests::test_run;
    use ontography::{
        PackageEnvelope,
        package::{PackageDocument, PackageStore},
    };

    #[tokio::test]
    async fn workspace_capture_release_and_export_preserve_the_visible_files() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        fs::create_dir_all(source.join("nested/empty")).unwrap();
        fs::write(source.join("nested/script"), "before").unwrap();
        fs::set_permissions(
            source.join("nested/script"),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        symlink("nested/script", source.join("link")).unwrap();
        let mut run = test_run(directory.path());
        let opened = workspace(&mut run, &json!({"action":"open","path":source}))
            .await
            .unwrap();
        assert!(opened.get("root").is_none());
        let id = opened["workspace_id"].as_str().unwrap();
        let path = PathBuf::from(opened["path"].as_str().unwrap());
        fs::write(path.join("nested/script"), "after").unwrap();
        let captured = workspace(&mut run, &json!({"action":"capture","workspace_id":id}))
            .await
            .unwrap();
        assert_eq!(captured["captured"], true);
        assert!(captured.get("root").is_none());
        let root = run.manifest.checkpoints[id].root;
        let destination = directory.path().join("result");
        export(
            &run,
            WorkflowPayload::Workspace(PackageEnvelope::new(root)),
            &destination,
        )
        .await
        .unwrap();
        assert_eq!(
            fs::read_to_string(destination.join("nested/script")).unwrap(),
            "after"
        );
        assert!(destination.join("nested/empty").is_dir());
        assert_ne!(
            fs::metadata(destination.join("nested/script"))
                .unwrap()
                .permissions()
                .mode()
                & 0o111,
            0
        );
        assert_eq!(
            fs::read_link(destination.join("link")).unwrap(),
            Path::new("nested/script")
        );
        workspace(&mut run, &json!({"action":"release","workspace_id":id}))
            .await
            .unwrap();
        assert!(!path.exists());
        assert!(run.manifest.checkpoints.contains_key(id));
        let reopened = workspace(&mut run, &json!({"action":"open","workspace_id":id}))
            .await
            .unwrap();
        assert_eq!(
            fs::read_to_string(Path::new(reopened["path"].as_str().unwrap()).join("nested/script"))
                .unwrap(),
            "after"
        );
        workspace(
            &mut run,
            &json!({"action":"release","workspace_id":reopened["workspace_id"]}),
        )
        .await
        .unwrap();
        run.suspend(false).await.unwrap();
    }

    #[tokio::test]
    async fn export_never_replaces_files_directories_or_dangling_links() {
        let directory = tempfile::tempdir().unwrap();
        let run = test_run(directory.path());
        let message = || WorkflowPayload::Message {
            message: "new message".into(),
        };
        let destination = directory.path().join("message.txt");
        export(&run, message(), &destination).await.unwrap();
        assert_eq!(fs::read_to_string(&destination).unwrap(), "new message");
        fs::write(&destination, "keep me").unwrap();
        assert_eq!(
            export(&run, message(), &destination)
                .await
                .unwrap_err()
                .code,
            "destination_exists"
        );
        assert_eq!(fs::read_to_string(&destination).unwrap(), "keep me");
        let existing = directory.path().join("existing");
        fs::create_dir(&existing).unwrap();
        assert_eq!(
            export(&run, message(), &existing).await.unwrap_err().code,
            "destination_exists"
        );
        let link = directory.path().join("dangling");
        symlink("missing", &link).unwrap();
        assert_eq!(
            export(&run, message(), &link).await.unwrap_err().code,
            "destination_exists"
        );
        // A destination appearing after staging is also protected by the publish syscall.
        let mut stage = Staging::new(directory.path(), true).unwrap();
        let staging_path = stage.path.clone();
        fs::write(stage.path.join("ours"), "ours").unwrap();
        assert_eq!(
            stage.publish(&existing).unwrap_err().code,
            "destination_exists"
        );
        drop(stage);
        assert!(!staging_path.exists());
        assert!(existing.is_dir());
    }

    #[tokio::test]
    async fn export_rejects_workspace_symlinks_that_escape_the_destination() {
        let directory = tempfile::tempdir().unwrap();
        let run = test_run(directory.path());
        let content = run.live().unwrap().session.content_store().await.unwrap();
        let packages = PackageStore::new(content);
        let link = packages
            .put(&PackageDocument::Symlink {
                target: "../outside".into(),
            })
            .await
            .unwrap();
        let root = packages
            .put(&PackageDocument::Collection {
                entries: [("link".into(), link)].into(),
            })
            .await
            .unwrap();
        let destination = directory.path().join("unsafe");
        assert!(
            export(
                &run,
                WorkflowPayload::Workspace(PackageEnvelope::new(root)),
                &destination
            )
            .await
            .is_err()
        );
        assert!(!destination.exists());
        assert!(!directory.path().join("outside").exists());
        assert!(fs::read_dir(directory.path()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".ontography-export-")
        }));
    }
}
