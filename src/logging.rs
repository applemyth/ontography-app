//! Bounded diagnostics for the detached process, independent of terminal streams.
use std::{
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::Path,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn record(root: &Path, message: &str) {
    static WRITER: Mutex<()> = Mutex::new(());
    let _guard = WRITER.lock().unwrap_or_else(|poison| poison.into_inner());
    let mut end = message.len().min(16 * 1024);
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let line = format!(
        "{timestamp} {}{}\n",
        &message[..end],
        if end < message.len() {
            " [truncated]"
        } else {
            ""
        }
    );
    let path = root.join("logs/server.log");
    if fs::metadata(&path).is_ok_and(|m| m.len() + line.len() as u64 > 1024 * 1024) {
        let _ = fs::rename(&path, root.join("logs/server.previous.log"));
    }
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)
    {
        let _ = file.write_all(line.as_bytes());
    }
}

/// Install only in the dedicated CLI server process, never an embedding application.
pub fn install_panic_hook(root: &Path) {
    let root = root.to_path_buf();
    std::panic::set_hook(Box::new(move |info| record(&root, &info.to_string())));
}
