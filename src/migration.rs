//! Exclusive, restartable relocation of an application store.
use crate::{
    AppError, Result,
    persistence::{read_json, write_json},
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Identity {
    device: u64,
    inode: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Migration {
    pub source: PathBuf,
    pub destination: PathBuf,
    pub archive: Option<PathBuf>,
    pub complete: bool,
    source_identity: Identity,
    destination_identity: Option<Identity>,
}

fn identity(path: &Path) -> Result<Option<Identity>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(Identity {
            device: metadata.dev(),
            inode: metadata.ino(),
        })),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

// Normalize parent aliases, retaining the final entry so the old root's alias
// remains distinct from its destination when a migration is retried.
fn normalized_entry(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let name = absolute
        .file_name()
        .ok_or_else(|| AppError::invalid("store path requires a final directory name"))?;
    let mut parent = absolute
        .parent()
        .ok_or_else(|| AppError::invalid("store path requires a parent"))?
        .to_path_buf();
    let mut missing = Vec::new();
    while !parent.exists() {
        let name = parent
            .file_name()
            .ok_or_else(|| AppError::invalid("store parent cannot be resolved"))?
            .to_os_string();
        missing.push(name);
        parent = parent
            .parent()
            .ok_or_else(|| AppError::invalid("store parent cannot be resolved"))?
            .to_path_buf();
    }
    let mut parent = fs::canonicalize(parent)?;
    for component in missing.into_iter().rev() {
        parent.push(component);
    }
    Ok(parent.join(name))
}

/// Called before app directory creation and again when a server acquires its
/// store lock. An interrupted cutover must finish before either name is opened.
pub fn check_root_available(root: &Path) -> Result<()> {
    let root = normalized_entry(root)?;
    for parent in root.ancestors().skip(1) {
        let journal = parent.join(".ontography-migration.json");
        if journal.exists() {
            let plan: Migration = read_json(&journal)?;
            if !plan.complete && (plan.source == root || plan.destination == root) {
                return Err(AppError::new(
                    "migration_required",
                    "An interrupted store migration must finish before this data directory can be opened. Run ontography migrate with the recorded paths.",
                ));
            }
        }
    }
    Ok(())
}

fn lock(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    file.try_lock().map_err(|e| {
        AppError::new(
            "store_in_use",
            format!("{} is owned by another process: {e}", path.display()),
        )
    })?;
    Ok(file)
}

pub fn is_store(path: &Path) -> bool {
    path.join("runs").is_dir() && path.join("definitions").is_dir()
}

pub fn default_root(home: &Path) -> Result<PathBuf> {
    let destination = home.join(".ontography");
    let old = home.join(".local/share/ontography");
    check_root_available(&destination)?;
    if old.is_symlink() {
        if is_store(&destination)
            && fs::canonicalize(&old).is_ok_and(|old| {
                fs::canonicalize(&destination).is_ok_and(|destination| old == destination)
            })
        {
            return Ok(destination);
        }
        return Err(AppError::new(
            "migration_inconsistent",
            "The old data-directory alias does not resolve to the current Ontography store. Select a data directory explicitly or repair the migration.",
        ));
    }
    if old.exists() && is_store(&old) {
        return Err(AppError::new(
            "migration_required",
            "Existing runs require migration. Stop the old server with its matching client, then run ontography migrate.",
        ));
    }
    Ok(destination)
}

/// Call after gracefully stopping the old server. File locks reject live owners.
/// The source becomes an alias so old paths cannot silently select an empty store.
pub fn migrate(source: &Path, destination: &Path) -> Result<Migration> {
    if !source.is_absolute() || !destination.is_absolute() || source == destination {
        return Err(AppError::invalid(
            "migration requires distinct absolute source and destination paths",
        ));
    }
    let source = normalized_entry(source)?;
    let destination = normalized_entry(destination)?;
    if source.starts_with(&destination) || destination.starts_with(&source) {
        return Err(AppError::invalid(
            "source and destination must not contain one another",
        ));
    }
    let source = source.as_path();
    let destination = destination.as_path();
    let parent = destination
        .parent()
        .ok_or_else(|| AppError::invalid("destination requires a parent"))?;
    fs::create_dir_all(parent)?;
    let _migration_lock = lock(&parent.join(".ontography-migration.lock"))?;
    let journal = parent.join(".ontography-migration.json");
    let new_plan = !journal.exists();
    let mut plan = if !new_plan {
        let plan: Migration = read_json(&journal)?;
        if plan.source != source || plan.destination != destination {
            return Err(AppError::new(
                "migration_conflict",
                "a migration journal exists for different paths",
            ));
        }
        plan
    } else {
        if source.is_symlink() || !is_store(source) {
            return Err(AppError::invalid(
                "source must be an existing Ontography store",
            ));
        }
        if destination.is_symlink() || is_store(destination) {
            return Err(AppError::new(
                "destination_in_use",
                "destination already contains an app store or alias; stores are never merged implicitly",
            ));
        }
        let archive = destination
            .exists()
            .then(|| parent.join(format!(".ontography.archive-{}", uuid::Uuid::new_v4())));
        Migration {
            source: source.into(),
            destination: destination.into(),
            archive,
            complete: false,
            source_identity: identity(source)?
                .ok_or_else(|| AppError::invalid("source store is unavailable"))?,
            destination_identity: identity(destination)?,
        }
    };
    if plan.complete {
        if source.is_symlink()
            && fs::canonicalize(source)? == fs::canonicalize(destination)?
            && is_store(destination)
            && identity(destination)?.as_ref() == Some(&plan.source_identity)
        {
            return Ok(plan);
        }
        return Err(AppError::new(
            "migration_inconsistent",
            "completed migration paths no longer agree",
        ));
    }
    // Retain locks across rename: the file descriptions continue to own the store.
    let _source_lock = if is_store(source) && !source.is_symlink() {
        Some(lock(&source.join("server.lock"))?)
    } else {
        None
    };
    let _destination_lock = if destination.is_dir() {
        Some(lock(&destination.join("server.lock"))?)
    } else {
        None
    };
    // A persisted plan authorizes moving the original objects only. A server or
    // another process may have created a new directory since a failed attempt.
    validate_cutover(&plan)?;
    if new_plan {
        write_json(&journal, &plan)?;
    }
    if let Some(archive) = &plan.archive
        && !archive.exists()
        && destination.exists()
    {
        fs::rename(destination, archive)?;
        File::open(parent)?.sync_all()?;
    }
    if !destination.exists() {
        if !is_store(source) || source.is_symlink() {
            return Err(AppError::new(
                "migration_inconsistent",
                "source store is unavailable before relocation",
            ));
        }
        fs::rename(source, destination)?;
        File::open(parent)?.sync_all()?;
        if let Some(parent) = source.parent() {
            File::open(parent)?.sync_all()?;
        }
    }
    if !is_store(destination) {
        return Err(AppError::new(
            "migration_inconsistent",
            "destination is not a complete app store",
        ));
    }
    if !source.exists() {
        std::os::unix::fs::symlink(destination, source)?;
        if let Some(parent) = source.parent() {
            File::open(parent)?.sync_all()?;
        }
    }
    if !source.is_symlink() || fs::canonicalize(source)? != fs::canonicalize(destination)? {
        return Err(AppError::new(
            "migration_inconsistent",
            "source is not an alias to the relocated store",
        ));
    }
    plan.complete = true;
    write_json(&journal, &plan)?;
    Ok(plan)
}

fn validate_cutover(plan: &Migration) -> Result<()> {
    let source = identity(&plan.source)?;
    let destination = identity(&plan.destination)?;
    let archive = plan.archive.as_deref().map(identity).transpose()?.flatten();
    let inconsistent = || {
        AppError::new(
            "migration_inconsistent",
            "Migration paths contain unexpected objects; no directory was moved. Preserve both stores and resolve the recorded migration before retrying.",
        )
    };
    if archive.is_some() && archive != plan.destination_identity {
        return Err(inconsistent());
    }
    if plan.destination.is_symlink() {
        return Err(inconsistent());
    }
    let source_original =
        !plan.source.is_symlink() && source.as_ref() == Some(&plan.source_identity);
    let relocated = destination.as_ref() == Some(&plan.source_identity);
    if relocated {
        if plan.destination_identity.is_some() && archive != plan.destination_identity {
            return Err(inconsistent());
        }
        if source.is_some()
            && (!plan.source.is_symlink()
                || fs::canonicalize(&plan.source)? != fs::canonicalize(&plan.destination)?)
        {
            return Err(inconsistent());
        }
    } else if source_original {
        if !is_store(&plan.source) {
            return Err(inconsistent());
        }
        if destination.is_some() {
            if destination != plan.destination_identity
                || archive.is_some()
                || is_store(&plan.destination)
            {
                return Err(inconsistent());
            }
        } else if plan.destination_identity.is_some() && archive != plan.destination_identity {
            return Err(inconsistent());
        }
    } else {
        return Err(inconsistent());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn store(path: &Path) {
        fs::create_dir_all(path.join("runs/run/core")).unwrap();
        fs::create_dir_all(path.join("definitions")).unwrap();
        fs::write(path.join("runs/run/core/evidence"), b"committed").unwrap();
    }

    fn pending_plan(source: &Path, destination: &Path) -> Migration {
        let source = normalized_entry(source).unwrap();
        let destination = normalized_entry(destination).unwrap();
        let plan = Migration {
            archive: destination
                .exists()
                .then(|| destination.parent().unwrap().join("legacy-archive")),
            source_identity: identity(&source).unwrap().unwrap(),
            destination_identity: identity(&destination).unwrap(),
            source,
            destination,
            complete: false,
        };
        write_json(
            &plan
                .destination
                .parent()
                .unwrap()
                .join(".ontography-migration.json"),
            &plan,
        )
        .unwrap();
        plan
    }
    #[test]
    fn migration_preserves_runs_archives_legacy_and_retries() {
        let home = tempfile::tempdir().unwrap();
        let source = home.path().join("old");
        let dest = home.path().join(".ontography");
        fs::create_dir_all(source.join("runs/run/core")).unwrap();
        fs::create_dir_all(source.join("definitions")).unwrap();
        fs::write(source.join("runs/run/core/evidence"), b"committed").unwrap();
        fs::create_dir_all(&dest).unwrap();
        fs::write(dest.join("legacy"), b"preserved").unwrap();
        let report = migrate(&source, &dest).unwrap();
        assert_eq!(
            fs::read(dest.join("runs/run/core/evidence")).unwrap(),
            b"committed"
        );
        assert_eq!(
            fs::read(report.archive.unwrap().join("legacy")).unwrap(),
            b"preserved"
        );
        assert_eq!(
            fs::canonicalize(&source).unwrap(),
            fs::canonicalize(&dest).unwrap()
        );
        assert!(migrate(&source, &dest).unwrap().complete);
    }

    #[test]
    fn live_store_owner_prevents_move_and_retry_recovers() {
        let home = tempfile::tempdir().unwrap();
        let source = home.path().join("old");
        let dest = home.path().join(".ontography");
        fs::create_dir_all(source.join("runs")).unwrap();
        fs::create_dir_all(source.join("definitions")).unwrap();
        let owner = lock(&source.join("server.lock")).unwrap();
        assert_eq!(migrate(&source, &dest).unwrap_err().code, "store_in_use");
        assert!(source.join("runs").exists());
        assert!(!home.path().join(".ontography-migration.json").exists());
        released(owner, &source.join("server.lock"));
        assert!(migrate(&source, &dest).unwrap().complete);
    }

    /// Closes the owner's lock and waits until nothing holds it. A process
    /// another test forks shares this one's open files until it execs, and
    /// holds the lock that long; a real owner has exited by then.
    fn released(owner: File, path: &Path) {
        drop(owner);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while lock(path).is_err() {
            assert!(
                std::time::Instant::now() < deadline,
                "the lock was never released"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    #[test]
    fn default_never_silently_selects_a_second_store_or_wrong_alias() {
        let home = tempfile::tempdir().unwrap();
        let old = home.path().join(".local/share/ontography");
        let destination = home.path().join(".ontography");
        store(&old);
        fs::create_dir_all(destination.join("runs")).unwrap();
        fs::create_dir_all(destination.join("definitions")).unwrap();
        assert_eq!(
            default_root(home.path()).unwrap_err().code,
            "migration_required"
        );
        let actual = home.path().join("actual");
        fs::rename(&old, &actual).unwrap();
        std::os::unix::fs::symlink(&actual, &old).unwrap();
        assert_eq!(
            default_root(home.path()).unwrap_err().code,
            "migration_inconsistent"
        );
        fs::remove_file(&old).unwrap();
        std::os::unix::fs::symlink(&destination, &old).unwrap();
        assert_eq!(default_root(home.path()).unwrap(), destination);
    }

    #[test]
    fn nested_paths_reject_before_archival_or_journal_creation() {
        let home = tempfile::tempdir().unwrap();
        let source = home.path().join("old");
        store(&source);
        assert!(migrate(&source, &source.join("inside")).is_err());
        assert!(migrate(&source, home.path()).is_err());
        assert_eq!(
            fs::read(source.join("runs/run/core/evidence")).unwrap(),
            b"committed"
        );
        assert!(!home.path().join(".ontography-migration.json").exists());
    }

    #[test]
    fn interrupted_cutovers_resume_after_each_rename_and_alias_step() {
        for step in 0..=3 {
            let home = tempfile::tempdir().unwrap();
            let source = home.path().join("old");
            let destination = home.path().join(".ontography");
            store(&source);
            fs::create_dir_all(&destination).unwrap();
            fs::write(destination.join("legacy"), b"preserved").unwrap();
            let plan = pending_plan(&source, &destination);
            if step >= 1 {
                fs::rename(&destination, plan.archive.as_ref().unwrap()).unwrap();
            }
            if step >= 2 {
                fs::rename(&source, &destination).unwrap();
            }
            if step >= 3 {
                std::os::unix::fs::symlink(&destination, &source).unwrap();
            }
            assert_eq!(
                check_root_available(&destination).unwrap_err().code,
                "migration_required"
            );
            assert_eq!(
                check_root_available(&source).unwrap_err().code,
                "migration_required"
            );
            assert!(migrate(&source, &destination).unwrap().complete);
            assert_eq!(
                fs::read(destination.join("runs/run/core/evidence")).unwrap(),
                b"committed"
            );
            assert_eq!(
                fs::read(plan.archive.unwrap().join("legacy")).unwrap(),
                b"preserved"
            );
            check_root_available(&destination).unwrap();
            check_root_available(&source).unwrap();
        }
    }

    #[test]
    fn retry_does_not_archive_an_unrelated_destination() {
        let home = tempfile::tempdir().unwrap();
        let source = home.path().join("old");
        let destination = home.path().join(".ontography");
        store(&source);
        fs::create_dir_all(&destination).unwrap();
        fs::write(destination.join("legacy"), b"original").unwrap();
        let plan = pending_plan(&source, &destination);
        let original = home.path().join("original");
        fs::rename(&destination, &original).unwrap();
        store(&destination);
        fs::write(destination.join("new-data"), b"keep this store").unwrap();
        assert_eq!(
            migrate(&source, &destination).unwrap_err().code,
            "migration_inconsistent"
        );
        assert_eq!(
            fs::read(destination.join("new-data")).unwrap(),
            b"keep this store"
        );
        assert_eq!(fs::read(original.join("legacy")).unwrap(), b"original");
        assert!(!plan.archive.unwrap().exists());
        assert!(source.join("runs/run/core/evidence").is_file());
    }

    #[test]
    fn retry_rejects_original_legacy_directory_if_it_became_an_app_store() {
        let home = tempfile::tempdir().unwrap();
        let source = home.path().join("old");
        let destination = home.path().join(".ontography");
        store(&source);
        fs::create_dir_all(&destination).unwrap();
        let plan = pending_plan(&source, &destination);
        store(&destination);
        assert_eq!(
            migrate(&source, &destination).unwrap_err().code,
            "migration_inconsistent"
        );
        assert!(!plan.archive.unwrap().exists());
        assert!(source.join("runs").is_dir());
        assert!(destination.join("runs").is_dir());
    }

    #[test]
    fn legacy_destination_owner_prevents_archival() {
        let home = tempfile::tempdir().unwrap();
        let source = home.path().join("old");
        let destination = home.path().join(".ontography");
        store(&source);
        fs::create_dir_all(&destination).unwrap();
        fs::write(destination.join("legacy"), b"preserved").unwrap();
        let owner = lock(&destination.join("server.lock")).unwrap();
        assert_eq!(
            migrate(&source, &destination).unwrap_err().code,
            "store_in_use"
        );
        assert_eq!(fs::read(destination.join("legacy")).unwrap(), b"preserved");
        assert!(!home.path().join(".ontography-migration.json").exists());
        released(owner, &destination.join("server.lock"));
        assert!(migrate(&source, &destination).unwrap().complete);
    }
}
