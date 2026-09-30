//! The rules for a checkout an attempt opens, shared by node tools and
//! builtin workers.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use serde_json::{Value, json};

use ontography::content::ContentId;
use ontography::package::ResolvedPackage;

use super::{Checkout, Result, WorkspaceCapture, WorkspaceError, WorkspaceStore};

/// A private checkout opened for one attempt, and the rules it was opened under.
#[derive(Debug)]
pub struct AttemptCheckout {
    checkout: Checkout,
    base: ContentId,
    writable: bool,
}

impl AttemptCheckout {
    /// Checks out `view` as `name` among the store's checkouts, read-only
    /// unless `writable`. A checkout that cannot be made read-only is removed,
    /// never left behind.
    ///
    /// # Errors
    /// Reports checkout failures, including permissions that could not be set.
    pub async fn open(
        store: &WorkspaceStore,
        view: &ResolvedPackage,
        name: &str,
        writable: bool,
    ) -> Result<Self> {
        tokio::fs::create_dir_all(store.checkouts_dir()).await?;
        let checkout = store
            .checkout(view, store.checkouts_dir().join(name))
            .await?;
        Self::adopt(checkout, view.root(), writable).await
    }

    /// Takes charge of a new checkout of `base`, read-only unless `writable`.
    async fn adopt(checkout: Checkout, base: ContentId, writable: bool) -> Result<Self> {
        if !writable && let Err(error) = checkout.set_read_only().await {
            let _ = checkout.remove().await;
            return Err(error);
        }
        Ok(Self {
            checkout,
            base,
            writable,
        })
    }

    /// Absolute directory suitable as a worker's working directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.checkout.path()
    }

    /// The view checked out, which a capture's changes are relative to.
    #[must_use]
    pub const fn base(&self) -> ContentId {
        self.base
    }

    /// What the worker is given, as its attempt's evidence records it: the
    /// exact files by content ID, where they are, and whether edits may be
    /// captured.
    #[must_use]
    pub fn exposure(&self) -> Value {
        json!({"root": self.base, "path": self.path().to_string_lossy(), "writable": self.writable})
    }

    /// Captures the files as a staged package; a changed read-only checkout is
    /// refused. Stop anything writing to it first.
    ///
    /// # Errors
    /// Reports the store's capture failures, and changes to a read-only checkout.
    pub async fn capture(&self, store: &WorkspaceStore) -> Result<WorkspaceCapture> {
        let capture = store.capture_staged(self.path(), self.base).await?;
        if !self.writable && capture.package().root() != self.base {
            return Err(WorkspaceError::ReadOnly(self.path().to_owned()));
        }
        Ok(capture)
    }

    /// Removes the checkout now; dropping it removes it too.
    ///
    /// # Errors
    /// Reports a replaced checkout root or filesystem cleanup failure.
    pub async fn remove(self) -> Result<()> {
        self.checkout.remove().await
    }

    /// Removes the checkouts in the store's checkouts directory, as far as it
    /// can. Call it before opening any: those found then belong to attempts
    /// that ended without cleanup, such as in a crash. One that cannot be
    /// removed, say because it holds a locked file, stays behind and only
    /// takes up space: no new checkout reuses its name.
    pub async fn remove_abandoned(store: &WorkspaceStore) {
        let directory = store.checkouts_dir().to_owned();
        // Cleanup is best-effort; a failed job leaves only what it left.
        let _ = tokio::task::spawn_blocking(move || remove_checkouts(&directory)).await;
    }
}

/// Removes every checkout below `directory` that it can.
fn remove_checkouts(directory: &Path) {
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let _ = remove_tree(&entry.path());
    }
}

/// Removes `root` and everything below it. Checkouts may be read-only, so
/// directories are made writable first; links are never followed, and what
/// another process removes meanwhile is skipped.
fn remove_tree(root: &Path) -> std::io::Result<()> {
    let mut directories = vec![root.to_owned()];
    while let Some(path) = directories.pop() {
        let Some(metadata) = present(fs::symlink_metadata(&path))? else {
            continue;
        };
        if !metadata.is_dir() {
            continue;
        }
        present(fs::set_permissions(
            &path,
            fs::Permissions::from_mode(0o700),
        ))?;
        let Some(children) = present(fs::read_dir(&path))? else {
            continue;
        };
        for child in children {
            if let Some(child) = present(child)?
                && present(child.file_type())?.is_some_and(|kind| kind.is_dir())
            {
                directories.push(child.path());
            }
        }
    }
    match present(fs::symlink_metadata(root))? {
        Some(metadata) if metadata.is_dir() => present(fs::remove_dir_all(root)).map(drop),
        Some(_) => present(fs::remove_file(root)).map(drop),
        None => Ok(()),
    }
}

/// `None` for a path that is already gone.
fn present<T>(result: std::io::Result<T>) -> std::io::Result<Option<T>> {
    match result {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        result => result.map(Some),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    use ontography::ProposalRuntime;

    use super::*;
    use crate::workflow::{Document, IdentityMap, document::expand_builtin as expand};

    /// A store in `directory`, and its view of `files`.
    async fn imported(
        directory: &Path,
        files: &[(&str, &str)],
    ) -> (ProposalRuntime, WorkspaceStore, ResolvedPackage) {
        let document = Document::parse(
            r#"{"name":"store","entry":"inbox","nodes":[{"id":"inbox","component":"inbox"}]}"#,
        )
        .unwrap();
        let compiled = expand(&document, "store", &IdentityMap::fresh(&document))
            .unwrap()
            .compile()
            .unwrap();
        let runtime = ProposalRuntime::with_policy(compiled, crate::workflow::edit::policy());
        let content = runtime.open().unwrap().content_store().await.unwrap();
        let store = WorkspaceStore::new(content, directory.join("store"));
        let source = directory.join("source");
        for (path, text) in files {
            let path = source.join(path);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, text).unwrap();
        }
        let view = store.import_directory(&source).await.unwrap();
        (runtime, store, view)
    }

    #[tokio::test]
    async fn only_a_writable_checkout_captures_changes() {
        let directory = tempfile::tempdir().unwrap();
        let (runtime, store, view) = imported(directory.path(), &[("a.txt", "one")]).await;
        let read_only = AttemptCheckout::open(&store, &view, "read-only", false)
            .await
            .unwrap();
        assert_eq!(
            read_only.exposure(),
            json!({"root": view.root(), "path": read_only.path(), "writable": false})
        );
        let file = read_only.path().join("a.txt");
        assert!(fs::metadata(&file).unwrap().permissions().readonly());
        assert!(matches!(
            read_only.capture(&store).await,
            Ok(capture) if capture.package().root() == view.root()
        ));
        // Permissions are advisory: the owner can lift them, so capture checks.
        fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();
        fs::write(&file, "edited").unwrap();
        assert!(matches!(
            read_only.capture(&store).await,
            Err(WorkspaceError::ReadOnly(path)) if path == read_only.path()
        ));
        let writable = AttemptCheckout::open(&store, &view, "writable", true)
            .await
            .unwrap();
        fs::write(writable.path().join("a.txt"), "edited").unwrap();
        assert_ne!(
            writable.capture(&store).await.unwrap().package().root(),
            view.root()
        );
        let paths = [read_only.path().to_owned(), writable.path().to_owned()];
        read_only.remove().await.unwrap();
        drop(writable);
        assert!(paths.iter().all(|path| !path.exists()));
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn a_checkout_that_cannot_be_made_read_only_is_removed() {
        // Root can read any directory, so this failure cannot be provoked.
        if nix::unistd::geteuid().is_root() {
            return;
        }
        let directory = tempfile::tempdir().unwrap();
        let (runtime, store, view) = imported(directory.path(), &[("sealed/a.txt", "one")]).await;
        fs::create_dir_all(store.checkouts_dir()).unwrap();
        let checkout = store
            .checkout(&view, store.checkouts_dir().join("attempt"))
            .await
            .unwrap();
        let path = checkout.path().to_owned();
        fs::set_permissions(path.join("sealed"), fs::Permissions::from_mode(0o000)).unwrap();
        let refused = AttemptCheckout::adopt(checkout, view.root(), false).await;
        assert!(matches!(refused, Err(WorkspaceError::Io(_))), "{refused:?}");
        assert!(!path.exists());
        runtime.shutdown().await;
    }

    #[test]
    fn stale_checkouts_are_removed_even_read_only_without_following_links() {
        let directory = tempfile::tempdir().unwrap();
        let outside = directory.path().join("outside");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("keep.txt"), "keep").unwrap();
        let checkouts = directory.path().join("checkouts");
        let checkout = checkouts.join("attempt-ws_1");
        fs::create_dir_all(checkout.join("nested")).unwrap();
        fs::write(checkout.join("nested/file.txt"), "old").unwrap();
        std::os::unix::fs::symlink(&outside, checkout.join("link")).unwrap();
        for path in [checkout.join("nested"), checkout.clone()] {
            fs::set_permissions(path, fs::Permissions::from_mode(0o500)).unwrap();
        }
        remove_checkouts(&checkouts);
        assert_eq!(fs::read_dir(&checkouts).unwrap().count(), 0);
        assert_eq!(
            fs::read_to_string(outside.join("keep.txt")).unwrap(),
            "keep"
        );
        remove_checkouts(&directory.path().join("absent"));
    }

    /// Removal tolerates another process removing the same checkout, as an
    /// aborted worker's background removal may still be doing at startup.
    #[test]
    fn removal_tolerates_a_checkout_being_removed_concurrently() {
        for round in 0..20 {
            let directory = tempfile::tempdir().unwrap();
            let checkout = directory.path().join("checkouts/aborted-attempt");
            for index in 0..200 {
                let nested = checkout.join(format!("d{index}/e"));
                fs::create_dir_all(&nested).unwrap();
                fs::write(nested.join("f"), "x").unwrap();
            }
            let aborted = std::thread::spawn({
                let checkout = checkout.clone();
                move || super::super::filesystem::remove_checkout(&checkout)
            });
            // Start once that removal is under way.
            while fs::read_dir(&checkout).map_or(0, Iterator::count) == 200 {
                std::hint::spin_loop();
            }
            let removed = remove_tree(&checkout);
            let _ = aborted.join().unwrap();
            assert!(removed.is_ok(), "round {round}: {removed:?}");
            assert!(!checkout.exists());
        }
    }
}
