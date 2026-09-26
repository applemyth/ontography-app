use crate::{AppError, Result, client::Client, persistence::Paths};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::process::Command;

pub const PI_VERSION: &str = "0.85.1";

/// Integration tests host a server in a test executable under `deps`; its shell
/// children must still enter the actual application's hidden launcher command.
pub fn application_executable() -> Result<PathBuf> {
    let executable = std::env::current_exe()?;
    if executable
        .parent()
        .and_then(Path::file_name)
        .is_some_and(|name| name == "deps")
    {
        let binary = executable
            .parent()
            .and_then(Path::parent)
            .expect("deps has a build root")
            .join("ontography");
        if binary.is_file() {
            return Ok(binary);
        }
    }
    Ok(executable)
}
const ASSETS: &[(&str, &str)] = &[
    ("index.ts", include_str!("../pi/index.ts")),
    ("client.ts", include_str!("../pi/client.ts")),
    ("tools.ts", include_str!("../pi/tools.ts")),
    ("session.ts", include_str!("../pi/session.ts")),
    ("autocomplete.ts", include_str!("../pi/autocomplete.ts")),
    ("terminal.ts", include_str!("../pi/terminal.ts")),
    ("instructions.md", include_str!("../pi/instructions.md")),
];

/// Pi owns conversation contents; Ontography selects their storage and active identity.
/// Global Pi credentials/settings continue to use the user's existing Pi home.
pub struct PiSessionLaunch {
    pub session_id: String,
    pub conversations_dir: PathBuf,
    pub conversation_id: String,
    pub conversation_path: Option<PathBuf>,
}

/// Build native Pi's command for a server-owned PTY. The caller retains the child
/// and decides whether an absent history is a new conversation or a recovery error.
pub async fn pi_session_command(
    paths: &Paths,
    project: &Path,
    executable: &Path,
    session: &PiSessionLaunch,
) -> Result<Command> {
    let mut command = pi_command(paths, project, executable, false).await?;
    command
        .env("ONTOGRAPHY_SESSION_ID", &session.session_id)
        .env("ONTOGRAPHY_TERMINAL", "1")
        .arg("--session-dir")
        .arg(&session.conversations_dir);
    if let Some(path) = &session.conversation_path {
        command.arg("--session").arg(path);
    } else {
        command.arg("--session-id").arg(&session.conversation_id);
    }
    Ok(command)
}

pub async fn ensure_server(paths: &Paths) -> Result<Client> {
    match Client::connect(&paths.socket).await {
        Ok(client) => return Ok(client),
        Err(error) if error.code == "io_error" => {}
        Err(error) => return Err(error),
    }
    let log = paths.root.join("logs/server.log");
    let mut child = Command::new(std::env::current_exe()?)
        .arg("--data-dir")
        .arg(&paths.root)
        .args(["server", "run", "--detach"])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .stdout(Stdio::null())
        .spawn()?;
    for _ in 0..100 {
        match Client::connect(&paths.socket).await {
            Ok(client) => return Ok(client),
            Err(error) if error.code == "io_error" => {}
            Err(error) => return Err(error),
        }
        if let Some(status) = child.try_wait()? {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if let Ok(client) = Client::connect(&paths.socket).await {
                return Ok(client);
            }
            return Err(AppError::new(
                "server_start_failed",
                format!("server exited {status}; inspect {}", log.display()),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(AppError::new(
        "server_start_timeout",
        format!("server did not become ready; inspect {}", log.display()),
    ))
}

pub async fn pi_command(
    paths: &Paths,
    project: &Path,
    executable: &Path,
    rpc: bool,
) -> Result<Command> {
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        Command::new(executable)
            .arg("--version")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| AppError::new("pi_version", "Pi version check timed out"))?
    .map_err(|e| {
        AppError::new(
            "pi_missing",
            format!(
                "cannot run {}: {e}; install @earendil-works/pi-coding-agent@{PI_VERSION}",
                executable.display()
            ),
        )
    })?;
    let version = String::from_utf8_lossy(&output.stdout);
    if !output.status.success() || version.trim() != PI_VERSION {
        return Err(AppError::new(
            "pi_version",
            format!(
                "expected Pi {PI_VERSION}; {} reported {:?}",
                executable.display(),
                version.trim()
            ),
        ));
    }
    let directory = materialize(paths)?;
    let instructions = format!(
        "{}\n\nThis client's project directory is {}. Supply this absolute project directory when starting a run unless the user selects another project.",
        include_str!("../pi/instructions.md"),
        serde_json::to_string(project)?
    );
    let mut command = Command::new(executable);
    command
        .current_dir(project)
        .arg("--extension")
        .arg(directory.join("index.ts"))
        .arg("--append-system-prompt")
        .arg(instructions)
        .env("ONTOGRAPHY_SOCKET", &paths.socket)
        .env("ONTOGRAPHY_PROJECT", project)
        .env("ONTOGRAPHY_APP_BUILD", crate::APP_BUILD)
        .env("ONTOGRAPHY_CORE_BUILD", crate::CORE_BUILD)
        .env("ONTOGRAPHY_CLIENT_ID", uuid::Uuid::new_v4().to_string());
    if rpc {
        command.args(["--mode", "rpc"]);
    } else {
        command
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
    }
    Ok(command)
}

fn materialize(paths: &Paths) -> Result<PathBuf> {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    for (name, source) in ASSETS {
        hash.update(name);
        hash.update(source);
    }
    let directory = paths
        .root
        .join("client")
        .join(format!("{:x}", hash.finalize()));
    if directory.is_dir() {
        return Ok(directory);
    }
    let temporary = paths
        .root
        .join("client")
        .join(format!(".{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&temporary)?;
    for (name, source) in ASSETS {
        std::fs::write(temporary.join(name), source)?;
    }
    if let Err(error) = std::fs::rename(&temporary, &directory) {
        let _ = std::fs::remove_dir_all(&temporary);
        if !directory.is_dir() {
            return Err(error.into());
        }
    }
    Ok(directory)
}
