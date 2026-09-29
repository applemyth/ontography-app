//! Where a node's session runs: in a terminal the graph can open, or headless
//! with its input and output on pipes. Either way the process group is
//! supervised: it stops when the node stops, and when the server dies.

use super::process::Lifetime;
use crate::{
    AppError, Result,
    process::{Stdin, SupervisedProcess, spawn_supervised},
    terminal::{LaunchSpec, Terminal},
};
use std::{
    collections::BTreeMap,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{net::unix::pipe, process::ChildStdout, task::JoinHandle};

pub(super) enum Host {
    Terminal {
        terminal: Arc<Terminal>,
        lifetime: Lifetime,
    },
    Headless {
        /// `None` once the program has ended and been reaped.
        process: Option<Box<SupervisedProcess>>,
        logging: Vec<JoinHandle<()>>,
    },
}

/// A headless session's input and output, for the driver that speaks to it.
pub(super) struct Pipes {
    pub input: Option<pipe::Sender>,
    pub output: Option<ChildStdout>,
}

impl Host {
    /// Start `spec` in a new terminal; clients attach through `socket`.
    pub(super) async fn terminal(
        spec: LaunchSpec,
        directory: &Path,
        socket: PathBuf,
    ) -> Result<Self> {
        let mut lifetime = Lifetime::new(directory)?;
        let terminal = Terminal::launch(lifetime.supervise(spec), socket).await?;
        lifetime.permit(&terminal).await?;
        Ok(Self::Terminal { terminal, lifetime })
    }

    /// Start `argv` without a terminal. Its stderr, and its stdout unless its
    /// driver reads it, are appended to `log`.
    pub(super) async fn headless(
        argv: &[String],
        cwd: &Path,
        directory: &Path,
        env: &BTreeMap<String, String>,
        stdin: Stdin<'_>,
        log: &Path,
        read_output: bool,
    ) -> Result<(Self, Pipes)> {
        let mut process = spawn_supervised(argv, cwd, directory, env, stdin).await?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(log)?;
        let mut logging = vec![append(process.stderr.take(), file.try_clone()?)];
        let output = process.stdout.take();
        let output = if read_output {
            output
        } else {
            logging.push(append(output, file));
            None
        };
        let pipes = Pipes {
            input: process.stdin.take(),
            output,
        };
        process.permit().await?;
        Ok((
            Self::Headless {
                process: Some(Box::new(process)),
                logging,
            },
            pipes,
        ))
    }

    pub(super) fn terminal_handle(&self) -> Option<&Arc<Terminal>> {
        match self {
            Self::Terminal { terminal, .. } => Some(terminal),
            Self::Headless { .. } => None,
        }
    }

    /// The program's exit code once it has ended. A terminal fault is an error.
    pub(super) async fn ended(&mut self) -> Result<Option<u32>> {
        match self {
            Self::Terminal { terminal, lifetime } => {
                let status = terminal.status();
                if let Some(fault) = status.fault {
                    return Err(AppError::new("node_terminal", fault));
                }
                if status.running {
                    return Ok(None);
                }
                lifetime.exit_code().map(Some)
            }
            Self::Headless { process, .. } => {
                let Some(running) = process.as_mut() else {
                    return Ok(None);
                };
                if !running.exited()? {
                    return Ok(None);
                }
                let code = process.take().expect("running process").finish().await?;
                Ok(Some(u32::from(code)))
            }
        }
    }

    /// Stop the process group without waiting; safe in a drop guard.
    pub(super) fn request_stop(&mut self) {
        match self {
            Self::Terminal { terminal, lifetime } => {
                lifetime.disconnect();
                terminal.request_stop();
            }
            Self::Headless { process, .. } => {
                if let Some(process) = process.as_mut() {
                    process.terminate();
                }
            }
        }
    }

    /// Stop the process group and wait for it to end.
    pub(super) async fn close(&mut self) -> Result<()> {
        self.request_stop();
        match self {
            Self::Terminal { terminal, .. } => terminal.shutdown().await,
            Self::Headless { process, logging } => {
                if let Some(process) = process.take() {
                    // A stopped program has no exit record; only cleanup matters.
                    let _ = process.finish().await;
                }
                for task in logging.drain(..) {
                    task.abort();
                }
                Ok(())
            }
        }
    }
}

/// Copy a pipe into the session log until the program closes it.
fn append<R>(reader: Option<R>, file: std::fs::File) -> JoinHandle<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        if let Some(mut reader) = reader {
            let mut file = tokio::fs::File::from_std(file);
            let _ = tokio::io::copy(&mut reader, &mut file).await;
        }
    })
}
