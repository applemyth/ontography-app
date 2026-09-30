//! The server under test: a real `ontography server run` in the trial's own
//! data directory and process group, reached through the app's own client.

use anyhow::{Context, Result, bail};
use ontography_app::{client::Client, persistence::Paths};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};

/// What became of one request.
pub enum Reply {
    Done(Value),
    /// Core refused the move.
    Rejected(String),
    /// It never ran: nothing was sent, or the server refused it before
    /// starting, as a restarted or stopping server does.
    NotRun(String),
    /// It ran and failed with this code; history tells what it changed.
    Failed(String, String),
    /// The reply was lost: it may or may not have happened.
    Uncertain(String),
}

impl std::fmt::Display for Reply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Done(value) => write!(f, "done: {value}"),
            Self::Rejected(message) => write!(f, "rejected: {message}"),
            Self::NotRun(message) => write!(f, "not run: {message}"),
            Self::Failed(code, message) => write!(f, "{code}: {message}"),
            Self::Uncertain(message) => write!(f, "uncertain: {message}"),
        }
    }
}

/// Sends one request and says what became of it.
pub async fn send(client: &Client, operation: &str, args: Value) -> Reply {
    let request_id = uuid::Uuid::new_v4().to_string();
    let response = match client.request(operation, args, request_id).await {
        Ok(response) => response,
        // A lost reply is uncertain; any other client-side failure, such as
        // a refused connection, happened before anything was sent.
        Err(error) if error.code == "unknown_outcome" => return Reply::Uncertain(error.message),
        Err(error) => return Reply::NotRun(format!("{}: {}", error.code, error.message)),
    };
    match response.into_result() {
        Ok(value) => Reply::Done(value),
        Err(error) => match error.code.as_str() {
            "rejected" => Reply::Rejected(error.message),
            "server_restarted" | "server_stopping" => Reply::NotRun(error.message),
            // An oversized result replaces the reply of a change that happened.
            "result_too_large" => Reply::Uncertain(error.message),
            code => Reply::Failed(code.into(), error.message),
        },
    }
}

pub struct Server {
    binary: PathBuf,
    data: PathBuf,
    socket: PathBuf,
    child: Option<Child>,
    pub client: Client,
    /// Every process this server has been, by pid.
    pub incarnations: Vec<u32>,
}

async fn launch(binary: &Path, data: &Path, socket: &Path) -> Result<(Child, Client)> {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(data.join("server.log"))?;
    let child = Command::new(binary)
        .arg("--data-dir")
        .arg(data)
        .args(["server", "run"])
        .stdin(std::process::Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log)
        // Its own group, so an interrupted trial can stop it and its workers.
        .process_group(0)
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("start {}", binary.display()))?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match Client::connect(socket).await {
            Ok(client) => return Ok((child, client)),
            Err(error) if error.code == "incompatible_server" => bail!(
                "{} was built from other sources than this harness; build both from one tree (the harness does so unless --ontography is given): {}",
                binary.display(),
                error.message
            ),
            Err(error) if Instant::now() > deadline => {
                bail!("server did not start: {}: {}", error.code, error.message)
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
}

impl Server {
    pub async fn start(binary: &Path, data: &Path) -> Result<Self> {
        std::fs::create_dir_all(data)?;
        let socket = Paths::initialize(data)?.socket;
        let (child, client) = launch(binary, data, &socket).await?;
        let pid = child.id().context("server pid")?;
        crate::procs::register(pid);
        Ok(Self {
            binary: binary.into(),
            data: data.into(),
            socket,
            child: Some(child),
            client,
            incarnations: vec![pid],
        })
    }

    /// Kills the server outright, as a crash or power cut would; its
    /// programs are left to fend for themselves.
    pub async fn crash(&mut self) -> Result<()> {
        if let Some(mut child) = self.child.take() {
            if let Some(pid) = child.id() {
                let _ = nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(pid as i32),
                    nix::sys::signal::Signal::SIGKILL,
                );
            }
            child.wait().await?;
        }
        Ok(())
    }

    pub async fn restart(&mut self) -> Result<()> {
        self.crash().await?;
        let (child, client) = launch(&self.binary, &self.data, &self.socket).await?;
        let pid = child.id().context("server pid")?;
        crate::procs::register(pid);
        self.incarnations.push(pid);
        self.child = Some(child);
        self.client = client;
        Ok(())
    }

    /// Stops the server in order; a server that does not exit within the
    /// grace is killed, and that is a problem.
    pub async fn stop(&mut self) -> Result<Option<String>> {
        let said = match send(&self.client, "server.stop", json!({})).await {
            Reply::Done(_) => None,
            other => Some(format!("server.stop: {other}")),
        };
        let Some(mut child) = self.child.take() else {
            return Ok(said);
        };
        match tokio::time::timeout(Duration::from_secs(20), child.wait()).await {
            Ok(status) => {
                status?;
                Ok(said)
            }
            Err(_) => {
                child.start_kill()?;
                child.wait().await?;
                Ok(Some(format!(
                    "the server did not exit within 20 s of server.stop{}",
                    said.map(|s| format!(" ({s})")).unwrap_or_default()
                )))
            }
        }
    }

    pub async fn call(&self, operation: &str, args: Value) -> Result<Value> {
        match send(&self.client, operation, args).await {
            Reply::Done(value) => Ok(value),
            other => bail!("{operation}: {other}"),
        }
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }
}
