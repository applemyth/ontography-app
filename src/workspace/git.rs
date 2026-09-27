//! Git invocations for checkouts that contain a repository.

use std::path::Path;
use std::process::{Output, Stdio};
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::process::Command;

use super::{Result, WorkspaceError};

fn command(cwd: &Path) -> Command {
    let mut command = Command::new("git");
    // The index rebuild must never inherit repository redirection, injected
    // config, credential helpers or hooks from the host.
    command
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", cwd)
        .env("XDG_CONFIG_HOME", cwd.join(".no-config"))
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("LC_ALL", "C")
        .env("GIT_CEILING_DIRECTORIES", cwd.parent().unwrap_or(cwd))
        .current_dir(cwd)
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.attributesFile=/dev/null",
            "-c",
            "core.pager=cat",
        ]);
    command
}

/// Run without unbounded output allocation or an unbounded child lifetime.
async fn run_git(command: &mut Command, stdout_limit: u64) -> Result<Output> {
    async fn read_limited(pipe: impl tokio::io::AsyncRead + Unpin, limit: u64) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        pipe.take(limit.saturating_add(1))
            .read_to_end(&mut bytes)
            .await?;
        if bytes.len() as u64 > limit {
            return Err(WorkspaceError::Limit("Git output".into()));
        }
        Ok(bytes)
    }
    let mut child = command
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    let result = tokio::time::timeout(Duration::from_mins(1), async {
        tokio::try_join!(
            read_limited(stdout, stdout_limit),
            read_limited(stderr, 64 * 1024),
            async { child.wait().await.map_err(WorkspaceError::from) }
        )
    })
    .await;
    match result {
        Ok(Ok((stdout, stderr, status))) => Ok(Output {
            status,
            stdout,
            stderr,
        }),
        failed => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            match failed {
                Ok(Err(error)) => Err(error),
                _ => Err(WorkspaceError::Git("operation exceeded 60 seconds".into())),
            }
        }
    }
}

/// Rebuild a local staging index without changing any working files.
pub(super) async fn rebuild_index(path: &Path) -> Result<()> {
    let tree = run_git(
        command(path).args(["rev-parse", "--verify", "--quiet", "HEAD^{tree}"]),
        64 * 1024,
    )
    .await?;
    let output = if tree.status.success() {
        run_git(command(path).args(["read-tree", "HEAD"]), 64 * 1024).await?
    } else {
        let branch = run_git(
            command(path).args(["symbolic-ref", "--quiet", "HEAD"]),
            64 * 1024,
        )
        .await?;
        if !branch.status.success() {
            return Err(WorkspaceError::Git(
                "package repository has no valid HEAD".into(),
            ));
        }
        let branch = std::str::from_utf8(&branch.stdout)
            .map_err(|_| WorkspaceError::Git("invalid HEAD reference".into()))?
            .trim();
        if !branch.starts_with("refs/") {
            return Err(WorkspaceError::Git("invalid HEAD reference".into()));
        }
        let exists = run_git(
            command(path).args(["show-ref", "--verify", "--quiet", "--", branch]),
            64 * 1024,
        )
        .await?;
        if exists.status.code() != Some(1) {
            return Err(WorkspaceError::Git(
                "HEAD references a missing or invalid object".into(),
            ));
        }
        run_git(command(path).args(["read-tree", "--empty"]), 64 * 1024).await?
    };
    if !output.status.success() {
        return Err(WorkspaceError::Git(
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn excessive_stdout_and_stderr_are_stopped_before_process_exit() {
        for redirect in ["", " >&2"] {
            let mut command = Command::new("sh");
            command.args([
                "-c",
                &format!("while :; do printf 'unbounded output\n'{redirect}; done"),
            ]);
            let result = tokio::time::timeout(Duration::from_secs(5), run_git(&mut command, 1024))
                .await
                .unwrap();
            assert!(matches!(result, Err(WorkspaceError::Limit(_))));
        }
    }
}
