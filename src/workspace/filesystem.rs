use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, Metadata};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path};
use std::time::SystemTime;

use super::{Result, WorkspaceError, WorkspaceLimits};
use ontography::content::BlobFormat;
use ontography::package::{ResolvedEntryKind as EntryKind, validate_package_path};

pub(super) struct CapturedEntry {
    pub path: String,
    pub kind: CapturedKind,
}
pub(super) enum CapturedKind {
    Directory,
    File { bytes: Vec<u8>, executable: bool },
    Symlink { target: String },
}

#[derive(Debug, Eq, PartialEq)]
struct Fingerprint {
    kind: u8,
    len: u64,
    modified: Option<SystemTime>,
    readonly: bool,
    unix: (u64, u64, u32, i64, i64, i64, i64),
}
fn fingerprint(m: &Metadata) -> Fingerprint {
    Fingerprint {
        kind: if m.is_dir() {
            1
        } else if m.is_file() {
            2
        } else if m.file_type().is_symlink() {
            3
        } else {
            4
        },
        len: m.len(),
        modified: m.modified().ok(),
        readonly: m.permissions().readonly(),
        unix: (
            m.dev(),
            m.ino(),
            m.mode(),
            m.mtime(),
            m.mtime_nsec(),
            m.ctime(),
            m.ctime_nsec(),
        ),
    }
}
fn executable(m: &Metadata) -> bool {
    m.permissions().mode() & 0o111 != 0
}

/// Paths are validated for the Unix posture of this store, which also needs
/// Unix symlinks, mode bits and reflink clones. Windows reserved device names,
/// trailing dots or spaces and drive-letter colons are therefore not rejected;
/// a Windows port must add those rules here.
pub(super) fn validate_path(path: &str) -> Result<()> {
    validate_package_path(path)?;
    // Case-insensitive hosts must not reinterpret a nested Git control directory.
    if path
        .split('/')
        .skip(1)
        .any(|p| p.eq_ignore_ascii_case(".git"))
        || path
            .split('/')
            .next()
            .is_some_and(|p| p.eq_ignore_ascii_case(".git") && p != ".git")
    {
        return Err(WorkspaceError::Invalid(format!(
            "unsupported Git path {path:?}"
        )));
    }
    Ok(())
}

fn git_path_allowed(path: &str) -> bool {
    path == ".git"
        || path == ".git/HEAD"
        || path == ".git/packed-refs"
        || path == ".git/shallow"
        || path == ".git/objects"
        || path.starts_with(".git/objects/")
        || path == ".git/refs"
        || path.starts_with(".git/refs/")
}

fn target_path(path: &str, target: &str) -> Result<String> {
    if target.is_empty()
        || target.len() > 4096
        || target.starts_with('/')
        || target.contains(['\\', '\0'])
        || target.chars().any(char::is_control)
    {
        return Err(WorkspaceError::Invalid(format!(
            "unsafe symlink {path:?} -> {target:?}"
        )));
    }
    let mut parts: Vec<_> = path.split('/').collect();
    parts.pop();
    for part in target.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.pop().is_none() {
                    return Err(WorkspaceError::Invalid(format!(
                        "escaping symlink {path:?}"
                    )));
                }
            }
            value => parts.push(value),
        }
    }
    let result = parts.join("/");
    if !result.is_empty() {
        validate_path(&result)?;
    }
    if result == ".git" || result.starts_with(".git/") {
        return Err(WorkspaceError::Invalid("symlink into Git metadata".into()));
    }
    Ok(result)
}

pub(super) fn validate_entries(
    source: &BTreeMap<String, EntryKind>,
    limits: WorkspaceLimits,
) -> Result<()> {
    if source.len() > limits.max_entries {
        return Err(WorkspaceError::Limit("filesystem entries".into()));
    }
    let mut folded = BTreeSet::new();
    let mut total = 0_u64;
    for (path, kind) in source {
        validate_path(path)?;
        if !folded.insert(
            unicode_normalization::UnicodeNormalization::nfc(path.to_lowercase().as_str())
                .collect::<String>(),
        ) {
            return Err(WorkspaceError::Invalid(
                "case- or Unicode-normalization-colliding paths".into(),
            ));
        }
        if let Some((parent, _)) = path.rsplit_once('/')
            && source.get(parent) != Some(&EntryKind::Directory)
        {
            return Err(WorkspaceError::Invalid(format!(
                "missing directory parent for {path}"
            )));
        }
        if path == ".git" && *kind != EntryKind::Directory {
            return Err(WorkspaceError::Invalid(
                "linked Git worktrees are unsupported; import a standalone repository".into(),
            ));
        }
        if path.starts_with(".git/")
            && (!git_path_allowed(path)
                || path.eq_ignore_ascii_case(".git/objects/info/alternates")
                || path.eq_ignore_ascii_case(".git/objects/info/http-alternates")
                || matches!(kind, EntryKind::Symlink { .. }))
        {
            return Err(WorkspaceError::Invalid(format!(
                "unsafe Git metadata {path}"
            )));
        }
        match kind {
            EntryKind::File { content, .. } => {
                if content.format() != BlobFormat::Raw || content.size() > limits.max_file_bytes {
                    return Err(WorkspaceError::Limit(format!("file {path}")));
                }
                total = total
                    .checked_add(content.size())
                    .ok_or_else(|| WorkspaceError::Limit("total bytes".into()))?;
                if total > limits.max_total_bytes {
                    return Err(WorkspaceError::Limit("total bytes".into()));
                }
            }
            EntryKind::Symlink { target } => {
                target_path(path, target)?;
            }
            EntryKind::Directory => {}
        }
    }
    // Resolve every symlink chain in actual filesystem order. In particular,
    // `link/..` must expand `link` before processing the parent component.
    for (path, kind) in source {
        if let EntryKind::Symlink { target } = kind {
            let mut resolved = path.split('/').map(str::to_owned).collect::<Vec<_>>();
            resolved.pop();
            let mut remaining = target
                .split('/')
                .map(str::to_owned)
                .collect::<VecDeque<_>>();
            let mut expansions = 0;
            while let Some(part) = remaining.pop_front() {
                match part.as_str() {
                    "" | "." => continue,
                    ".." => {
                        if resolved.pop().is_none() {
                            return Err(WorkspaceError::Invalid(
                                "symlink chain escapes root".into(),
                            ));
                        }
                        continue;
                    }
                    _ => {}
                }
                resolved.push(part);
                let candidate = resolved.join("/");
                if candidate == ".git" || candidate.starts_with(".git/") {
                    return Err(WorkspaceError::Invalid(
                        "symlink chain enters Git metadata".into(),
                    ));
                }
                if let Some(EntryKind::Symlink { target }) = source.get(candidate.as_str()) {
                    expansions += 1;
                    if expansions > 40 {
                        return Err(WorkspaceError::Invalid(
                            "cyclic or excessive symlink chain".into(),
                        ));
                    }
                    resolved.pop();
                    for component in target.split('/').rev() {
                        remaining.push_front(component.to_owned());
                    }
                }
            }
        }
    }
    Ok(())
}

fn scan(root: &Path, limits: WorkspaceLimits) -> Result<BTreeMap<String, Fingerprint>> {
    let root_meta = fs::symlink_metadata(root)?;
    if !root_meta.is_dir() || root_meta.file_type().is_symlink() {
        return Err(WorkspaceError::Invalid(
            "capture root must be a real directory".into(),
        ));
    }
    let mut found = BTreeMap::from([(String::new(), fingerprint(&root_meta))]);
    let mut directories = vec![String::new()];
    while let Some(relative) = directories.pop() {
        for child in fs::read_dir(root.join(&relative))? {
            let child = child?;
            let name = child
                .file_name()
                .into_string()
                .map_err(|_| WorkspaceError::Invalid("non-UTF-8 filename".into()))?;
            let path = if relative.is_empty() {
                name
            } else {
                format!("{relative}/{name}")
            };
            validate_path(&path)?;
            if path == ".git/commondir"
                || path.eq_ignore_ascii_case(".git/objects/info/alternates")
                || path.eq_ignore_ascii_case(".git/objects/info/http-alternates")
            {
                return Err(WorkspaceError::Invalid(
                    "repository links to external Git data".into(),
                ));
            }
            if path.starts_with(".git/") && !git_path_allowed(&path) {
                continue;
            }
            let meta = fs::symlink_metadata(child.path())?;
            if path == ".git" && !meta.is_dir() {
                return Err(WorkspaceError::Invalid(
                    "linked Git worktrees are unsupported".into(),
                ));
            }
            if path.starts_with(".git/") && meta.file_type().is_symlink() {
                return Err(WorkspaceError::Invalid("Git metadata symlink".into()));
            }
            if !meta.is_dir() && !meta.is_file() && !meta.file_type().is_symlink() {
                return Err(WorkspaceError::Invalid(format!(
                    "special filesystem entry {path}"
                )));
            }
            if found.len() > limits.max_entries {
                return Err(WorkspaceError::Limit("entry count".into()));
            }
            if meta.is_dir() {
                directories.push(path.clone());
            }
            found.insert(path, fingerprint(&meta));
        }
    }
    Ok(found)
}

pub(super) fn capture(root: &Path, limits: WorkspaceLimits) -> Result<Vec<CapturedEntry>> {
    let before = scan(root, limits)?;
    let mut result = Vec::new();
    let mut total = 0_u64;
    for (path, expected) in &before {
        if path.is_empty() {
            continue;
        }
        let full = root.join(path);
        let metadata = fs::symlink_metadata(&full)?;
        if fingerprint(&metadata) != *expected {
            return Err(WorkspaceError::Changed(path.clone()));
        }
        let kind = if metadata.is_dir() {
            CapturedKind::Directory
        } else if metadata.file_type().is_symlink() {
            let target = fs::read_link(&full)?
                .into_os_string()
                .into_string()
                .map_err(|_| WorkspaceError::Invalid("non-UTF-8 symlink target".into()))?;
            target_path(path, &target)?;
            CapturedKind::Symlink { target }
        } else {
            if metadata.len() > limits.max_file_bytes {
                return Err(WorkspaceError::Limit(format!("file {path}")));
            }
            if metadata.len() > limits.max_total_bytes.saturating_sub(total) {
                return Err(WorkspaceError::Limit("total bytes".into()));
            }
            let mut file = fs::File::open(&full)?;
            if fingerprint(&file.metadata()?) != *expected {
                return Err(WorkspaceError::Changed(path.clone()));
            }
            let mut bytes = Vec::new();
            (&mut file)
                .take(limits.max_file_bytes.saturating_add(1))
                .read_to_end(&mut bytes)?;
            if fingerprint(&file.metadata()?) != *expected || bytes.len() as u64 != metadata.len() {
                return Err(WorkspaceError::Changed(path.clone()));
            }
            total = total
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| WorkspaceError::Limit("total bytes".into()))?;
            if total > limits.max_total_bytes {
                return Err(WorkspaceError::Limit("total bytes".into()));
            }
            CapturedKind::File {
                bytes,
                executable: executable(&metadata),
            }
        };
        result.push(CapturedEntry {
            path: path.clone(),
            kind,
        });
    }
    if scan(root, limits)? != before {
        return Err(WorkspaceError::Changed("directory tree".into()));
    }
    Ok(result)
}

/// Resolve through the actual host filesystem before exposure. This catches
/// case and Unicode aliases which a portable package-path map cannot predict.
/// Nonexistent suffixes are processed component by component, so a later `..`
/// cannot hide a symlink expansion in the longest existing prefix.
pub(super) fn validate_materialized_links(
    root: &Path,
    package: &ontography::package::ResolvedPackage,
) -> Result<()> {
    let root = fs::canonicalize(root)?;
    for entry in package.entries() {
        let EntryKind::Symlink { target } = &entry.kind else {
            continue;
        };
        let parent = Path::new(&entry.path).parent().unwrap_or(Path::new(""));
        let mut current = fs::canonicalize(root.join(parent))?;
        let mut remaining = Path::new(target)
            .components()
            .map(|c| c.as_os_str().to_owned())
            .collect::<VecDeque<_>>();
        let mut expansions = 0;
        while let Some(part) = remaining.pop_front() {
            match Path::new(&part).components().next() {
                Some(Component::CurDir) | None => continue,
                Some(Component::ParentDir) => {
                    current.pop();
                }
                Some(Component::Normal(_)) => {
                    let candidate = current.join(&part);
                    match fs::symlink_metadata(&candidate) {
                        Ok(metadata) if metadata.file_type().is_symlink() => {
                            expansions += 1;
                            if expansions > 40 {
                                return Err(WorkspaceError::Invalid(
                                    "cyclic or excessive host symlink chain".into(),
                                ));
                            }
                            let target = fs::read_link(&candidate)?;
                            for component in target.components().rev() {
                                remaining.push_front(component.as_os_str().to_owned());
                            }
                            continue;
                        }
                        Ok(_) => {
                            current = fs::canonicalize(candidate)?;
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            current = candidate;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                _ => {
                    return Err(WorkspaceError::Invalid(
                        "absolute host symlink target".into(),
                    ));
                }
            }
            if !current.starts_with(&root) {
                return Err(WorkspaceError::Invalid(format!(
                    "host symlink escapes workspace: {}",
                    entry.path
                )));
            }
        }
    }
    Ok(())
}

/// Advisory permissions only; never follows checkout symlinks.
pub(super) fn set_read_only(root: &Path) -> Result<()> {
    let mut directories = vec![root.to_owned()];
    let mut next = 0;
    while next < directories.len() {
        for child in fs::read_dir(&directories[next])? {
            let child = child?;
            let metadata = fs::symlink_metadata(child.path())?;
            if metadata.file_type().is_symlink() {
                continue;
            }
            if metadata.is_dir() {
                directories.push(child.path());
            } else {
                let mut permissions = metadata.permissions();
                permissions.set_readonly(true);
                fs::set_permissions(child.path(), permissions)?;
            }
        }
        next += 1;
    }
    for directory in directories.into_iter().rev() {
        let mut permissions = fs::metadata(&directory)?.permissions();
        permissions.set_readonly(true);
        fs::set_permissions(directory, permissions)?;
    }
    Ok(())
}

pub(super) fn remove_checkout(root: &Path) -> Result<()> {
    let mut directories = vec![root.to_owned()];
    while let Some(path) = directories.pop() {
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(WorkspaceError::Invalid("checkout root replaced".into()));
        }
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
        for child in fs::read_dir(&path)? {
            let child = child?;
            let metadata = fs::symlink_metadata(child.path())?;
            if metadata.is_dir() && !metadata.file_type().is_symlink() {
                directories.push(child.path());
            }
        }
    }
    match fs::remove_dir_all(root) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}
