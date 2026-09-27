//! Writable filesystem views of immutable composable content packages.
//!
//! File bytes are exported once, verified, into a shared host-owned baseline
//! cache, and every private clone is hashed again after cloning, so a tampered
//! cache file cannot reach a checkout. Every checkout uses the operating
//! system's copy-on-write clone primitive; unsupported filesystems fail instead
//! of falling back to copying. The invocation host owns checkout lifetime and
//! must stop writers before capture. Observable concurrent changes fail
//! capture, but capture is not an operating-system atomic snapshot.

// The store's posture is Unix: mode bits, symlinks, inode identity, and
// reflink clones. It is declared once here instead of in per-call branches.
#[cfg(not(unix))]
compile_error!("the workspace store supports Unix targets only");

mod attempt;
mod cache;
mod filesystem;
mod git;
#[cfg(test)]
mod tests;

pub use attempt::AttemptCheckout;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use thiserror::Error;

use ontography::content::Hash;
use ontography::content::{ContentError, ContentId, ContentStore, StagedImports};
use ontography::package::{
    PackageDocument, PackageError, PackageStore, ResolvedEntryKind, ResolvedPackage, descendants,
};

/// A filesystem operation or package validation failure.
#[derive(Debug, Error)]
pub enum WorkspaceError {
    /// Filesystem access failed.
    #[error("workspace I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Content storage or verification failed.
    #[error(transparent)]
    Content(#[from] ContentError),
    /// Package composition or resolution failed.
    #[error(transparent)]
    Package(#[from] PackageError),
    /// Invalid or unsupported path, entry or cache state.
    #[error("invalid workspace: {0}")]
    Invalid(String),
    /// A source was modified while capture was reading it.
    #[error("workspace changed during capture: {0}")]
    Changed(String),
    /// A read-only checkout's files changed, so its capture was refused.
    #[error("read-only checkout {} changed; open it writable to capture edits", .0.display())]
    ReadOnly(PathBuf),
    /// A configured resource limit was exceeded.
    #[error("workspace limit exceeded: {0}")]
    Limit(String),
    /// Strict OS cloning failed; full-copy fallback is deliberately unavailable.
    #[error("copy-on-write clone failed for {path}: {source}")]
    UnsupportedCopyOnWrite {
        /// Destination that could not be cloned.
        path: PathBuf,
        /// Operating-system clone error.
        source: std::io::Error,
    },
    /// Git was unavailable or rejected an operation.
    #[error("git operation failed: {0}")]
    Git(String),
}

/// Result from filesystem package operations.
pub type Result<T> = std::result::Result<T, WorkspaceError>;

/// Bounded filesystem capture and materialization budgets.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WorkspaceLimits {
    /// Maximum number of filesystem entries, excluding the root directory.
    pub max_entries: usize,
    /// Maximum bytes in one regular file.
    pub max_file_bytes: u64,
    /// Maximum bytes across all regular files.
    pub max_total_bytes: u64,
}
impl Default for WorkspaceLimits {
    fn default() -> Self {
        Self {
            max_entries: 100_000,
            max_file_bytes: 64 * 1024 * 1024,
            max_total_bytes: 256 * 1024 * 1024,
        }
    }
}

/// A private writable directory, removed on drop as a cleanup fallback.
///
/// Its host must stop all writers before removal or drop. Explicit `remove`
/// reports cleanup errors; the drop fallback cannot report them.
#[derive(Debug)]
pub struct Checkout {
    path: PathBuf,
    removed: bool,
    _cache: Arc<cache::SharedCache>,
}
impl Checkout {
    /// Absolute directory suitable as a worker's invocation cwd.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
    /// Apply advisory read-only permissions without following symlinks.
    ///
    /// The directory owner can reverse permissions. Enforcing a read-only grant
    /// also requires an adapter sandbox and host rejection of changed output.
    ///
    /// # Errors
    /// Reports permission or traversal failures.
    pub async fn set_read_only(&self) -> Result<()> {
        let path = self.path().to_owned();
        tokio::task::spawn_blocking(move || filesystem::set_read_only(&path))
            .await
            .map_err(|e| WorkspaceError::Invalid(e.to_string()))?
    }
    /// Remove this checkout after its host has stopped all processes using it.
    ///
    /// # Errors
    /// Reports a replaced checkout root or filesystem cleanup failure.
    pub async fn remove(mut self) -> Result<()> {
        tokio::task::spawn_blocking(move || {
            filesystem::remove_checkout(&self.path)?;
            self.removed = true;
            drop(self);
            Ok(())
        })
        .await
        .map_err(|e| WorkspaceError::Invalid(e.to_string()))?
    }
}

impl Drop for Checkout {
    fn drop(&mut self) {
        if !self.removed {
            let _ = filesystem::remove_checkout(&self.path);
        }
    }
}

/// A changed path; renames are represented by a deletion and an addition.
#[derive(Clone, Debug, Eq, PartialEq)]
struct WorkspaceChange {
    path: String,
    after: Option<ResolvedEntryKind>,
}

/// Shared content packages, filesystem validation, and immutable baseline cache.
#[derive(Clone, Debug)]
pub struct WorkspaceStore {
    content: ContentStore,
    packages: PackageStore,
    cache: Arc<cache::SharedCache>,
    limits: WorkspaceLimits,
}

/// A validated capture whose new content is pinned until accepted or dropped.
pub struct WorkspaceCapture {
    package: ResolvedPackage,
    imports: StagedImports,
}

impl WorkspaceCapture {
    /// Returns the captured view and its dependency closure.
    #[must_use]
    pub const fn package(&self) -> &ResolvedPackage {
        &self.package
    }

    /// Retains the successful capture across restarts.
    ///
    /// # Errors
    /// Reports content durability or retention failures.
    pub async fn retain(self) -> Result<ResolvedPackage> {
        self.imports.retain().await?;
        Ok(self.package)
    }
}

impl WorkspaceStore {
    /// Use a host-owned root shared by all invocations in a run.
    ///
    /// The root owns the whole layout: `bases` is the immutable verified blob
    /// cache and `checkouts` holds private writable directories. Both must be on
    /// a filesystem supporting OS reflinks. Workers must not receive write
    /// access to `bases` through their sandbox. No writable hard links or
    /// full-copy fallback are used.
    #[must_use]
    pub fn new(content: ContentStore, root: impl Into<PathBuf>) -> Self {
        Self::with_cache(content, cache::SharedCache::new(root.into(), false))
    }
    /// The run directory owns `workspaces`; an ephemeral run gets a private
    /// temporary root that the last clone removes.
    #[must_use]
    pub fn for_run(content: ContentStore, run_path: Option<&Path>) -> Self {
        if let Some(run) = run_path {
            return Self::new(content, run.join("workspaces"));
        }
        let root =
            std::env::temp_dir().join(format!("ontography-workspaces-{}", uuid::Uuid::new_v4()));
        Self::with_cache(content, cache::SharedCache::new(root, true))
    }
    fn with_cache(content: ContentStore, cache: cache::SharedCache) -> Self {
        Self {
            packages: PackageStore::new(content.clone()),
            content,
            cache: Arc::new(cache),
            limits: WorkspaceLimits::default(),
        }
    }
    /// Immutable verified blob cache, `<root>/bases`.
    #[must_use]
    pub fn cache_dir(&self) -> &Path {
        self.cache.bases()
    }
    /// Private writable checkouts, `<root>/checkouts`.
    #[must_use]
    pub fn checkouts_dir(&self) -> &Path {
        self.cache.checkouts()
    }
    /// Capture an initial directory as File/Symlink packages inside Collections.
    ///
    /// # Errors
    /// Reports unsafe paths, concurrent mutation, limits, or storage failures.
    pub async fn import_directory(&self, path: impl AsRef<Path>) -> Result<ResolvedPackage> {
        let imports = self.content.stage_imports();
        let store = self.staging_store(&imports);
        let entries = store.capture_entries(path.as_ref(), None).await?;
        let package = store.store_tree(entries).await?;
        imports.retain().await?;
        Ok(package)
    }
    /// Capture edits as one Changes package relative to the explicit base.
    ///
    /// Unchanged files retain their package references. No edits return the base
    /// unchanged. Stop source writers first; this detects observable races, not
    /// changes made and fully hidden by an adversarial writer. The caller links
    /// the returned complete dependency closure during invocation publication.
    ///
    /// # Errors
    /// Reports invalid base, unsafe paths, concurrent mutation, or storage limits.
    pub async fn capture(
        &self,
        path: impl AsRef<Path>,
        base: ContentId,
    ) -> Result<ResolvedPackage> {
        self.capture_staged(path, base).await?.retain().await
    }

    /// Validates a capture under temporary retention for a later policy decision.
    /// Dropping it releases only its own pins, including on failure or cancellation.
    ///
    /// # Errors
    /// Reports the same validation and storage errors as [`Self::capture`].
    pub async fn capture_staged(
        &self,
        path: impl AsRef<Path>,
        base: ContentId,
    ) -> Result<WorkspaceCapture> {
        let base = self.open(base).await?;
        let imports = self.content.stage_imports();
        imports.protect(&base.dependencies()).await?;
        let store = self.staging_store(&imports);
        let entries = store.capture_entries(path.as_ref(), Some(&base)).await?;
        let package = store.store_changes(&base, entries).await?;
        Ok(WorkspaceCapture { package, imports })
    }

    fn staging_store(&self, imports: &StagedImports) -> Self {
        let content = imports.store();
        Self {
            packages: PackageStore::new(content.clone()),
            content,
            cache: Arc::clone(&self.cache),
            limits: self.limits,
        }
    }
    async fn capture_entries(
        &self,
        path: &Path,
        base: Option<&ResolvedPackage>,
    ) -> Result<BTreeMap<String, ResolvedEntryKind>> {
        let path = path.to_owned();
        let limits = self.limits;
        let captured = tokio::task::spawn_blocking(move || filesystem::capture(&path, limits))
            .await
            .map_err(|e| WorkspaceError::Invalid(e.to_string()))??;
        let previous = base.map(entry_map).unwrap_or_default();
        let mut entries = BTreeMap::new();
        for item in captured {
            let kind = match item.kind {
                filesystem::CapturedKind::Directory => ResolvedEntryKind::Directory,
                filesystem::CapturedKind::File { bytes, executable } => {
                    let reused = match previous.get(&item.path) {
                        Some(ResolvedEntryKind::File { content, .. })
                            if content.size() == bytes.len() as u64
                                && content.hash() == Hash::new(&bytes) =>
                        {
                            Some(*content)
                        }
                        _ => None,
                    };
                    ResolvedEntryKind::File {
                        content: if let Some(id) = reused {
                            id
                        } else {
                            self.content.import_bytes(bytes).await?
                        },
                        executable,
                    }
                }
                filesystem::CapturedKind::Symlink { target } => {
                    ResolvedEntryKind::Symlink { target }
                }
            };
            entries.insert(item.path, kind);
        }
        Ok(entries)
    }
    /// Resolve and validate a package as a filesystem directory view.
    ///
    /// # Errors
    /// Reports invalid composition, unsafe paths/symlinks, incomplete content or limits.
    pub async fn open(&self, root: ContentId) -> Result<ResolvedPackage> {
        let package = self.packages.resolve(root).await?;
        self.validate_view(&package)?;
        Ok(package)
    }
    fn validate_view(&self, package: &ResolvedPackage) -> Result<()> {
        if !package
            .entries()
            .iter()
            .any(|e| e.path.is_empty() && e.kind == ResolvedEntryKind::Directory)
        {
            return Err(WorkspaceError::Invalid(
                "filesystem package root must be a collection or directory changes view".into(),
            ));
        }
        filesystem::validate_entries(&entry_map(package), self.limits)?;
        Ok(())
    }
    /// Clone a package's shared immutable baseline into a new private directory.
    ///
    /// The destination's parent directory must already exist and be controlled
    /// by the host, and workers must not access the directory before
    /// completion. Every regular file is cloned by the OS and verified; clone
    /// errors fail without falling back to copies.
    ///
    /// # Errors
    /// Reports existing destination, unsafe cache, unavailable COW, or Git failure.
    pub async fn checkout(
        &self,
        package: &ResolvedPackage,
        destination: impl AsRef<Path>,
    ) -> Result<Checkout> {
        self.validate_view(package)?;
        cache::checkout(self, package, destination.as_ref()).await
    }
    async fn store_tree(
        &self,
        entries: BTreeMap<String, ResolvedEntryKind>,
    ) -> Result<ResolvedPackage> {
        filesystem::validate_entries(&entries, self.limits)?;
        let root = self.store_subtree("", &entries).await?;
        self.open(root).await
    }

    // Serialize only this replacement subtree. Existing directory identities in
    // a resolved Changes view are path-bound and must never be reused as if they
    // were independent collections.
    async fn store_subtree(
        &self,
        root: &str,
        entries: &BTreeMap<String, ResolvedEntryKind>,
    ) -> Result<ContentId> {
        let kind = if root.is_empty() {
            &ResolvedEntryKind::Directory
        } else {
            entries.get(root).ok_or_else(|| {
                WorkspaceError::Invalid(format!("missing replacement subtree {root}"))
            })?
        };
        let mut children: BTreeMap<String, BTreeMap<String, ContentId>> = BTreeMap::new();
        for (path, kind) in descendants(entries, root)
            .rev()
            .map(|(path, kind)| (path.as_str(), kind))
            .chain(std::iter::once((root, kind)))
        {
            let document = match kind {
                ResolvedEntryKind::Directory => PackageDocument::Collection {
                    entries: children.remove(path).unwrap_or_default(),
                },
                ResolvedEntryKind::File {
                    content,
                    executable,
                } => PackageDocument::File {
                    content: *content,
                    executable: *executable,
                },
                ResolvedEntryKind::Symlink { target } => PackageDocument::Symlink {
                    target: target.clone(),
                },
            };
            let id = self.packages.put(&document).await?;
            if path == root {
                return Ok(id);
            }
            let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
            children
                .entry(parent.to_owned())
                .or_default()
                .insert(name.to_owned(), id);
        }
        unreachable!("the iterator always includes the subtree root")
    }

    async fn store_changes(
        &self,
        base: &ResolvedPackage,
        after: BTreeMap<String, ResolvedEntryKind>,
    ) -> Result<ResolvedPackage> {
        filesystem::validate_entries(&after, self.limits)?;
        let mut changes = BTreeMap::new();
        for change in tree_difference(&entry_map(base), &after) {
            let path = change.path;
            if path
                .match_indices('/')
                .any(|(index, _)| changes.contains_key(&path[..index]))
            {
                continue;
            }
            let replacement = if change.after.is_some() {
                Some(self.store_subtree(&path, &after).await?)
            } else {
                None
            };
            changes.insert(path, replacement);
        }
        if changes.is_empty() {
            return Ok(base.clone());
        }
        let root = self
            .packages
            .put(&PackageDocument::Changes {
                base: base.root(),
                changes,
            })
            .await?;
        self.open(root).await
    }
}

fn entry_map(package: &ResolvedPackage) -> BTreeMap<String, ResolvedEntryKind> {
    package
        .entries()
        .iter()
        .filter(|e| !e.path.is_empty())
        .map(|e| (e.path.clone(), e.kind.clone()))
        .collect()
}

fn tree_difference(
    before: &BTreeMap<String, ResolvedEntryKind>,
    after: &BTreeMap<String, ResolvedEntryKind>,
) -> Vec<WorkspaceChange> {
    before
        .keys()
        .chain(after.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter_map(|path| {
            let old = before.get(path);
            let new = after.get(path);
            (old != new).then(|| WorkspaceChange {
                path: path.clone(),
                after: new.cloned(),
            })
        })
        .collect()
}
