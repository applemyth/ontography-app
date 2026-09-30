use std::fs;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ontography::content::ContentId;
use ontography::content::{Hash, PartialExport};
use ontography::package::{ResolvedEntryKind, ResolvedPackage};

use super::{Checkout, Result, WorkspaceError, WorkspaceStore, filesystem};

/// One root owns both halves of the layout: `bases` holds the immutable
/// verified blob cache and `checkouts` holds private writable directories.
/// Clones share explicit ownership; no process-global pathname registry is needed.
#[derive(Debug)]
pub(super) struct SharedCache {
    bases: PathBuf,
    checkouts: PathBuf,
    /// The whole root, removed on drop, when this store created it for an
    /// ephemeral run rather than being handed a host-owned directory.
    ephemeral_root: Option<PathBuf>,
    /// Serializes checkouts: populating the shared blob cache and cloning from
    /// it run under this lock for the whole blocking job.
    checkout_lock: Arc<tokio::sync::Mutex<()>>,
}
impl SharedCache {
    pub(super) fn new(root: PathBuf, ephemeral: bool) -> Self {
        Self {
            bases: root.join("bases"),
            checkouts: root.join("checkouts"),
            ephemeral_root: ephemeral.then_some(root),
            checkout_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
    pub(super) fn bases(&self) -> &Path {
        &self.bases
    }
    pub(super) fn checkouts(&self) -> &Path {
        &self.checkouts
    }
}

impl Drop for SharedCache {
    fn drop(&mut self) {
        if let Some(root) = &self.ephemeral_root {
            let _ = filesystem::remove_checkout(root);
        }
    }
}

fn permissions(path: &Path, mode: u32) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    Ok(())
}
fn private_directory(path: &Path) -> Result<()> {
    match fs::create_dir(path) {
        Ok(()) => permissions(path, 0o700)?,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(path)?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(WorkspaceError::Invalid(format!(
                    "cache path is not a real directory: {}",
                    path.display()
                )));
            }
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(WorkspaceError::Invalid(format!(
                    "cache directory is not private: {}",
                    path.display()
                )));
            }
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}
fn initialize(bases: &Path) -> Result<()> {
    if let Some(parent) = bases.parent() {
        fs::create_dir_all(parent)?;
    }
    private_directory(bases)?;
    private_directory(&bases.join("blobs"))
}
fn verify_file(path: &Path, id: ContentId) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() != id.size() {
        return Err(WorkspaceError::Invalid(format!(
            "invalid clone file: {}",
            path.display()
        )));
    }
    let mut file = fs::File::open(path)?;
    let mut bytes = Vec::new();
    (&mut file)
        .take(id.size().saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != id.size() || Hash::new(&bytes) != id.hash() {
        return Err(WorkspaceError::Invalid(format!(
            "cache/clone hash mismatch: {}",
            path.display()
        )));
    }
    Ok(())
}

fn cached_blob(store: &WorkspaceStore, id: ContentId) -> Result<PathBuf> {
    let blobs = store.cache.bases().join("blobs");
    let target = blobs.join(id.hash().to_string());
    match fs::symlink_metadata(&target) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let temporary =
                PartialExport::new(blobs.join(format!(".export-{}", uuid::Uuid::new_v4())));
            // This function runs only on the blocking pool. Cancellation leaves
            // the job owning its paths until export/cleanup actually completes.
            tokio::runtime::Handle::current()
                .block_on(store.content.export_file(id, temporary.path()))?;
            permissions(temporary.path(), 0o444)?;
            match fs::hard_link(temporary.path(), &target) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(e.into()),
            }
        }
        Err(e) => return Err(e.into()),
    }
    let metadata = fs::symlink_metadata(&target)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || !metadata.permissions().readonly()
        || metadata.len() != id.size()
    {
        return Err(WorkspaceError::Invalid(format!(
            "invalid immutable cached file: {}",
            target.display()
        )));
    }
    // Verify the actual private clone below, avoiding a second full hash read.
    Ok(target)
}

fn checkout_blocking(
    store: &WorkspaceStore,
    package: &ResolvedPackage,
    destination: &Path,
) -> Result<Checkout> {
    initialize(store.cache.bases())?;
    let name = destination
        .file_name()
        .ok_or_else(|| WorkspaceError::Invalid("checkout destination needs a basename".into()))?;
    let parent = destination
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let path = fs::canonicalize(parent)?.join(name);
    if path.starts_with(fs::canonicalize(store.cache.bases())?) {
        return Err(WorkspaceError::Invalid(
            "checkout destination is inside baseline cache".into(),
        ));
    }
    fs::create_dir(&path)?;
    let checkout = Checkout {
        path: path.clone(),
        removed: false,
        _cache: Arc::clone(&store.cache),
    };
    permissions(&path, 0o700)?;
    for entry in package.entries().iter().filter(|e| !e.path.is_empty()) {
        let target = path.join(&entry.path);
        match &entry.kind {
            ResolvedEntryKind::Directory => {
                // Existing names include aliases on case/normalization-insensitive
                // filesystems. Never silently merge two logical directories.
                fs::create_dir(&target)?;
                permissions(&target, 0o700)?;
            }
            ResolvedEntryKind::File {
                content,
                executable,
            } => {
                let source = cached_blob(store, *content)?;
                reflink_copy::reflink(&source, &target).map_err(|source| {
                    WorkspaceError::UnsupportedCopyOnWrite {
                        path: target.clone(),
                        source,
                    }
                })?;
                let from = fs::metadata(&source)?;
                let to = fs::metadata(&target)?;
                if (from.dev(), from.ino()) == (to.dev(), to.ino()) || to.nlink() != 1 {
                    return Err(WorkspaceError::Invalid(
                        "OS clone did not produce an independent inode".into(),
                    ));
                }
                verify_file(&target, *content)?;
                permissions(&target, if *executable { 0o755 } else { 0o644 })?;
            }
            ResolvedEntryKind::Symlink { .. } => {}
        }
    }
    for entry in package.entries() {
        if let ResolvedEntryKind::Symlink { target } = &entry.kind {
            std::os::unix::fs::symlink(target, path.join(&entry.path))?;
        }
    }
    filesystem::validate_materialized_links(&path, package)?;
    Ok(checkout)
}

pub(super) async fn checkout(
    store: &WorkspaceStore,
    package: &ResolvedPackage,
    destination: &Path,
) -> Result<Checkout> {
    // Move the owned lock into the blocking job: dropping the awaiting future
    // must not unlock the cache while its filesystem job is still running.
    let serialized = Arc::clone(&store.cache.checkout_lock).lock_owned().await;
    let store = store.clone();
    let package = package.clone();
    let destination = destination.to_owned();
    let git = package.entry(".git").is_some();
    let checkout = tokio::task::spawn_blocking(move || {
        let result = checkout_blocking(&store, &package, &destination);
        drop(serialized);
        result
    })
    .await
    .map_err(|error| WorkspaceError::Invalid(error.to_string()))??;
    if git {
        super::git::rebuild_index(checkout.path()).await?;
    }
    Ok(checkout)
}
