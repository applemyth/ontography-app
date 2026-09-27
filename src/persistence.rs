use crate::{AppError, Result};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

/// Persisted JSON writers that accumulate records must remain readable by read_json.
pub const MAX_JSON_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct Paths {
    pub root: PathBuf,
    pub socket: PathBuf,
}

impl Paths {
    pub fn initialize(root: impl AsRef<Path>) -> Result<Self> {
        crate::migration::check_root_available(root.as_ref())?;
        let root = if root.as_ref().is_symlink() {
            fs::canonicalize(root)?
        } else {
            root.as_ref().to_path_buf()
        };
        if (root.join("profiles").exists() || root.join("session-roots").exists())
            && !crate::migration::is_store(&root)
        {
            return Err(AppError::new(
                "migration_required",
                "Legacy Ontography data must be archived through ontography migrate before this directory is used.",
            ));
        }
        private_directory(&root)?;
        let root = fs::canonicalize(root)?;
        for name in ["definitions", "runs", "logs", "client", "workspace-cache"] {
            private_directory(&root.join(name))?;
        }
        let identity = format!("{:x}", Sha256::digest(root.as_os_str().as_encoded_bytes()));
        let endpoint_dir = PathBuf::from("/tmp").join(format!(
            "ontography-{}-{}",
            nix::unistd::geteuid(),
            &identity[..20]
        ));
        private_directory(&endpoint_dir)?;
        Ok(Self {
            root,
            socket: endpoint_dir.join("server.sock"),
        })
    }

    pub fn run(&self, id: &str) -> Result<PathBuf> {
        uuid::Uuid::parse_str(id).map_err(|_| AppError::invalid("run_id must be a UUID"))?;
        Ok(self.root.join("runs").join(id))
    }

    pub fn definition(&self, fingerprint: &str) -> Result<PathBuf> {
        if fingerprint.len() != 64 || !fingerprint.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(AppError::invalid(
                "definition revision must be a SHA-256 hex string",
            ));
        }
        Ok(self
            .root
            .join("definitions")
            .join(format!("{fingerprint}.json")))
    }

    pub fn workflow_definition(&self, fingerprint: &str) -> Result<PathBuf> {
        // Reuse revision validation while keeping documents separate from raw
        // graph declarations, whose listing has a different schema.
        self.definition(fingerprint)?;
        Ok(self
            .root
            .join("definitions/workflows")
            .join(format!("{fingerprint}.json")))
    }
}

pub fn default_data_dir() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("ONTOGRAPHY_DATA_DIR") {
        return Ok(path.into());
    }
    if let Some(path) = std::env::var_os("XDG_DATA_HOME") {
        return Ok(PathBuf::from(path).join("ontography"));
    }
    let home = std::env::var_os("HOME")
        .ok_or_else(|| AppError::new("configuration", "set --data-dir or ONTOGRAPHY_DATA_DIR"))?;
    crate::migration::default_root(Path::new(&home))
}

fn private_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.uid() != nix::unistd::geteuid().as_raw() {
        return Err(AppError::new(
            "invalid_data_directory",
            format!(
                "{} must be a directory owned by the current user",
                path.display()
            ),
        ));
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let metadata = fs::metadata(path)?;
    if metadata.len() > MAX_JSON_BYTES {
        return Err(AppError::new("file_too_large", path.display().to_string()));
    }
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

/// Publish complete app metadata atomically; core stores have their own commits.
pub fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| AppError::invalid("metadata path has no parent"))?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(&serde_json::to_vec_pretty(value)?)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}
