use crate::{AppError, Result, client::Client, persistence::Paths};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::process::Command;

pub const PI_VERSION: &str = "0.85.1";
const ASSETS: &[(&str, &str)] = &[
    ("index.ts", include_str!("../pi/index.ts")),
    ("client.ts", include_str!("../pi/client.ts")),
    ("tools.ts", include_str!("../pi/tools.ts")),
    ("instructions.md", include_str!("../pi/instructions.md")),
];

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
