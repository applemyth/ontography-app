//! The lease that names a live supervisor, and the recovery that verifies it
//! before signalling the process group it names.

use super::failure;
use crate::{Result, persistence::write_json};
use nix::{
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::process::Command;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProcessLease {
    pub(super) pid: i32,
    pub(super) token: String,
    pub(super) identity: String,
}

pub(super) fn lease_path(directory: &Path) -> PathBuf {
    directory.join("worker-process.json")
}

/// Record an owned terminal supervisor before permitting it to launch user code.
/// Terminal workers use the same verified recovery as command-task workers.
pub(crate) async fn lease_process(directory: &Path, pid: u32, token: &str) -> Result<()> {
    let pid = i32::try_from(pid).map_err(failure)?;
    let identity = startup_identity(
        || inspect_process_identity(pid, token),
        Duration::from_secs(3),
    )
    .await?;
    write_json(
        &lease_path(directory),
        &ProcessLease {
            pid,
            token: token.into(),
            identity,
        },
    )
    .map_err(failure)
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ProcessIdentity {
    Gone,
    ArgumentsUnavailable,
    Verified(String),
}

fn classify_identity(pid: i32, token: &str, output: &[u8]) -> Result<ProcessIdentity> {
    use sha2::{Digest, Sha256};
    let text = String::from_utf8_lossy(output);
    let mut fields = text.split_whitespace().skip(5); // lstart has five fields.
    if fields.next().and_then(|value| value.parse::<i32>().ok()) != Some(pid) {
        return Err(failure(
            "Saved worker process ownership is uncertain; no process was signalled",
        ));
    }
    let command = fields.collect::<Vec<_>>().join(" ");
    if command == "(sh)" {
        // macOS can report the process name before argv becomes readable.
        return Ok(ProcessIdentity::ArgumentsUnavailable);
    }
    if !command.contains(&format!("workflow-worker-{token}")) {
        return Err(failure(
            "Saved worker process ownership is uncertain; no process was signalled",
        ));
    }
    Ok(ProcessIdentity::Verified(format!(
        "{:x}",
        Sha256::digest(output)
    )))
}

pub(super) async fn inspect_process_identity(pid: i32, token: &str) -> Result<ProcessIdentity> {
    use nix::{errno::Errno, sys::signal::kill, unistd::getpgid};
    match kill(Pid::from_raw(pid), None) {
        Err(Errno::ESRCH) => return Ok(ProcessIdentity::Gone),
        Err(error) => {
            return Err(failure(format!(
                "Cannot inspect saved worker process: {error}"
            )));
        }
        Ok(()) => {}
    }
    let observed = Command::new("/bin/ps")
        .args([
            "-ww",
            "-p",
            &pid.to_string(),
            "-o",
            "lstart=",
            "-o",
            "pgid=",
            "-o",
            "command=",
        ])
        .kill_on_drop(true)
        .output()
        .await
        .map_err(failure)?;
    if !observed.status.success() {
        if kill(Pid::from_raw(pid), None) == Err(Errno::ESRCH) {
            return Ok(ProcessIdentity::Gone);
        }
        return Err(failure("Cannot verify the saved worker process identity"));
    }
    match getpgid(Some(Pid::from_raw(pid))) {
        Ok(group) if group == Pid::from_raw(pid) => {}
        Err(Errno::ESRCH) => return Ok(ProcessIdentity::Gone),
        _ => {
            return Err(failure(
                "Saved worker process ownership is uncertain; no process was signalled",
            ));
        }
    }
    classify_identity(pid, token, &observed.stdout)
}

async fn process_identity(pid: i32, token: &str) -> Result<Option<String>> {
    match inspect_process_identity(pid, token).await? {
        ProcessIdentity::Gone => Ok(None),
        ProcessIdentity::Verified(identity) => Ok(Some(identity)),
        ProcessIdentity::ArgumentsUnavailable => Err(failure(
            "Saved worker process ownership is uncertain; no process was signalled",
        )),
    }
}

pub(super) async fn startup_identity<F, Fut>(mut inspect: F, timeout: Duration) -> Result<String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<ProcessIdentity>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let identity = tokio::time::timeout_at(deadline, inspect())
            .await
            .map_err(|_| failure("Worker supervisor arguments were unavailable at startup"))??;
        match identity {
            ProcessIdentity::Verified(identity) => return Ok(identity),
            ProcessIdentity::Gone => {
                return Err(failure(
                    "Worker supervisor exited before its lease was saved",
                ));
            }
            ProcessIdentity::ArgumentsUnavailable => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(failure(
                        "Worker supervisor arguments were unavailable at startup",
                    ));
                }
                tokio::time::sleep_until(
                    deadline.min(tokio::time::Instant::now() + Duration::from_millis(10)),
                )
                .await;
            }
        }
    }
}

async fn wait_group_gone(pid: i32) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        match killpg(Pid::from_raw(pid), None) {
            Err(nix::errno::Errno::ESRCH) => return Ok(()),
            // macOS can briefly deny this probe while the group's members
            // are exiting. Keep waiting within the same bound; only ESRCH
            // establishes that cleanup finished.
            Err(nix::errno::Errno::EPERM) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => {
                return Err(failure(format!(
                    "Cannot verify worker group cleanup: {error}"
                )));
            }
            Ok(()) if tokio::time::Instant::now() >= deadline => {
                return Err(failure(
                    "Previous worker process group has not exited; refusing to launch a replacement",
                ));
            }
            Ok(()) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
}

/// Called before giving any replacement worker inputs. Never signal a saved PID
/// until its process group, random marker and complete start/command fingerprint
/// have been verified. The supervisor's lifetime pipe normally cleans it first.
pub async fn recover_process(directory: &Path) -> Result<()> {
    let path = lease_path(directory);
    if !path.exists() {
        return Ok(());
    }
    let lease: ProcessLease = crate::persistence::read_json(&path).map_err(failure)?;
    uuid::Uuid::parse_str(&lease.token).map_err(failure)?;
    if lease.pid <= 1 {
        return Err(failure("Invalid saved worker process identity"));
    }
    if let Some(identity) = process_identity(lease.pid, &lease.token).await? {
        if identity != lease.identity {
            return Err(failure(
                "Saved worker process identity changed; no process was signalled",
            ));
        }
        // Let the verified supervisor finish its own cleanup before removing
        // the owner group. PTY agents may have jobs in other process groups;
        // killing the supervisor first would interrupt their cleanup helper.
        // If cleanup stalls, retain the lease and refuse to overlap a new
        // worker rather than bypassing that ownership boundary.
        match nix::sys::signal::kill(Pid::from_raw(lease.pid), Signal::SIGTERM) {
            Ok(()) | Err(nix::errno::Errno::ESRCH) => {}
            Err(error) => return Err(failure(error)),
        }
    }
    wait_group_gone(lease.pid).await?;
    for suffix in ["input", "status"] {
        match tokio::fs::remove_file(directory.join(format!("{}.{}", lease.token, suffix))).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(failure(error)),
        }
    }
    tokio::fs::remove_file(path).await.map_err(failure)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn startup_retries_only_unavailable_arguments_with_a_deadline() {
        let pid = 123;
        let token = "fixture-token";
        let bare = format!("Sat Sep 26 12:00:00 2026 {pid} (sh)\n");
        let full =
            format!("Sat Sep 26 12:00:00 2026 {pid} /bin/sh -c script workflow-worker-{token}\n");
        let mut attempts = 0;
        let identity = startup_identity(
            || {
                attempts += 1;
                std::future::ready(classify_identity(
                    pid,
                    token,
                    if attempts < 3 {
                        bare.as_bytes()
                    } else {
                        full.as_bytes()
                    },
                ))
            },
            Duration::from_millis(100),
        )
        .await
        .unwrap();
        assert_eq!(attempts, 3);
        assert_eq!(
            classify_identity(pid, token, full.as_bytes()).unwrap(),
            ProcessIdentity::Verified(identity),
        );
        // A readable but different command or a different group is not the
        // macOS metadata race, even if an expected value might appear later.
        for output in [
            format!("Sat Sep 26 12:00:00 2026 {pid} /bin/sh unrelated\n"),
            "Sat Sep 26 12:00:00 2026 456 (sh)\n".into(),
        ] {
            let mut attempts = 0;
            let result = startup_identity(
                || {
                    attempts += 1;
                    std::future::ready(classify_identity(pid, token, output.as_bytes()))
                },
                Duration::from_millis(100),
            )
            .await;
            assert!(
                result
                    .unwrap_err()
                    .message
                    .contains("ownership is uncertain")
            );
            assert_eq!(attempts, 1);
        }
        let result = startup_identity(
            || std::future::ready(classify_identity(pid, token, bare.as_bytes())),
            Duration::from_millis(20),
        )
        .await;
        assert!(
            result
                .unwrap_err()
                .message
                .contains("unavailable at startup")
        );
    }
}
