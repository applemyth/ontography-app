//! A terminal client driven by a program: it attaches to a session's
//! terminal over the terminal socket as `ontography attach` does, and keeps
//! what the server sends. Each snapshot's screen, vt100-formatted text, is
//! parsed into rows with vt100; graph and detach notices and errors are kept
//! too. Protocol faults the client sees are kept as problems: a first frame
//! other than `attached`, a snapshot of another terminal, or snapshots out of
//! order.

use anyhow::{Context, Result, bail};
use ontography_app::terminal::{AttachRequest, ClientFrame, HistoryAction, ServerFrame, Snapshot};
use ontography_app::{AppError, protocol};
use std::path::Path;
use std::time::Duration;
use tokio::io::BufReader;
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::watch;
use tokio::time::Instant;

/// How an attachment ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum End {
    /// The server sent `detach`.
    Detached,
    /// The connection closed.
    Closed,
    /// The server sent an error, or the connection failed.
    Error(String),
}

/// What the client has seen so far.
#[derive(Clone, Debug, Default)]
pub struct View {
    /// The latest snapshot's screen, one string per row.
    pub rows: Vec<String>,
    pub size: (u16, u16),
    pub sequence: u64,
    pub snapshots: usize,
    pub graphs: usize,
    /// Set while the snapshot is a frozen copy of history.
    pub history: bool,
    pub exit_code: Option<u32>,
    pub end: Option<End>,
    pub problems: Vec<String>,
}

impl View {
    fn apply(&mut self, terminal: &str, snapshot: Snapshot) {
        if snapshot.terminal_id != terminal {
            self.problems.push(format!(
                "a snapshot of terminal {} reached the client of {terminal}",
                snapshot.terminal_id
            ));
            return;
        }
        if self.snapshots > 0 && snapshot.sequence < self.sequence {
            self.problems.push(format!(
                "snapshot {} arrived after snapshot {}",
                snapshot.sequence, self.sequence
            ));
        }
        let mut parser = vt100::Parser::new(snapshot.rows, snapshot.cols, 0);
        parser.process(snapshot.screen.as_bytes());
        self.rows = parser
            .screen()
            .rows(0, snapshot.cols)
            .map(|row| row.trim_end().to_owned())
            .collect();
        self.size = (snapshot.rows, snapshot.cols);
        self.sequence = snapshot.sequence;
        self.snapshots += 1;
        self.history = snapshot.history.is_some();
        self.exit_code = snapshot.exit_code;
    }

    /// Whether a row of the screen is exactly `text`.
    pub fn shows(&self, text: &str) -> bool {
        self.rows.iter().any(|row| row == text)
    }

    /// The rows that begin with `prefix`, without it, top to bottom.
    pub fn lines(&self, prefix: &str) -> Vec<String> {
        self.rows
            .iter()
            .filter_map(|row| row.strip_prefix(prefix).map(String::from))
            .collect()
    }
}

pub enum Opened {
    Attached(Attachment),
    /// The server answered the handshake with an error.
    Refused(AppError),
}

pub struct Attachment {
    terminal: String,
    write: OwnedWriteHalf,
    view: watch::Receiver<View>,
    reader: tokio::task::JoinHandle<()>,
}

impl Drop for Attachment {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

impl Attachment {
    /// Connects and sends the handshake, which the server wants within three
    /// seconds; the first frame back says whether this client controls the
    /// terminal.
    pub async fn open(socket: &Path, request: AttachRequest) -> Result<Opened> {
        let stream = UnixStream::connect(socket)
            .await
            .with_context(|| format!("connect to {}", socket.display()))?;
        let (read, mut write) = stream.into_split();
        protocol::write_frame(&mut write, &request).await?;
        let mut reader = BufReader::new(read);
        let first = tokio::time::timeout(Duration::from_secs(5), protocol::read_frame(&mut reader))
            .await
            .context("no answer to the terminal handshake within 5 s")??
            .context("the terminal closed the connection before answering")?;
        match serde_json::from_slice::<ServerFrame>(&first)? {
            ServerFrame::Attached { terminal_id } if terminal_id == request.terminal_id => {}
            ServerFrame::Error { error } => return Ok(Opened::Refused(error)),
            other => bail!("the terminal answered the handshake with {other:?}"),
        }
        let (sender, view) = watch::channel(View::default());
        let terminal = request.terminal_id;
        let reader = tokio::spawn(read_frames(reader, terminal.clone(), sender));
        Ok(Opened::Attached(Self {
            terminal,
            write,
            view,
            reader,
        }))
    }

    pub fn view(&self) -> View {
        self.view.borrow().clone()
    }

    pub async fn input(&mut self, bytes: &[u8]) -> Result<()> {
        self.send(&ClientFrame::Input {
            terminal_id: self.terminal.clone(),
            bytes: bytes.to_vec(),
        })
        .await
    }

    pub async fn resize(&mut self, rows: u16, cols: u16) -> Result<()> {
        self.send(&ClientFrame::Resize {
            terminal_id: self.terminal.clone(),
            rows,
            cols,
        })
        .await
    }

    pub async fn detach(&mut self) -> Result<()> {
        self.send(&ClientFrame::Detach {
            terminal_id: self.terminal.clone(),
        })
        .await
    }

    pub async fn history(&mut self, action: HistoryAction) -> Result<()> {
        self.send(&ClientFrame::History {
            terminal_id: self.terminal.clone(),
            action,
        })
        .await
    }

    async fn send(&mut self, frame: &ClientFrame) -> Result<()> {
        protocol::write_frame(&mut self.write, frame).await?;
        Ok(())
    }

    /// Waits until `done` holds of what the client has seen. Otherwise, once
    /// the attachment has ended or `limit` has passed, returns what it saw.
    pub async fn wait(
        &mut self,
        limit: Duration,
        done: impl Fn(&View) -> bool,
    ) -> std::result::Result<View, View> {
        let deadline = Instant::now() + limit;
        loop {
            {
                let view = self.view.borrow_and_update();
                if done(&view) {
                    return Ok(view.clone());
                }
                if view.end.is_some() {
                    return Err(view.clone());
                }
            }
            match tokio::time::timeout_at(deadline, self.view.changed()).await {
                Ok(Ok(())) => {}
                _ => return Err(self.view.borrow().clone()),
            }
        }
    }
}

async fn read_frames(
    mut reader: BufReader<OwnedReadHalf>,
    terminal: String,
    view: watch::Sender<View>,
) {
    loop {
        let frame = match protocol::read_frame(&mut reader).await {
            Ok(Some(bytes)) => serde_json::from_slice::<ServerFrame>(&bytes),
            Ok(None) => {
                view.send_modify(|v| {
                    v.end.get_or_insert(End::Closed);
                });
                return;
            }
            Err(error) => {
                view.send_modify(|v| {
                    v.end.get_or_insert(End::Error(error.to_string()));
                });
                return;
            }
        };
        match frame {
            Ok(ServerFrame::Snapshot { snapshot }) => {
                view.send_modify(|v| v.apply(&terminal, snapshot));
            }
            Ok(ServerFrame::Graph) => view.send_modify(|v| v.graphs += 1),
            Ok(ServerFrame::Detach) => {
                view.send_modify(|v| {
                    v.end.get_or_insert(End::Detached);
                });
                return;
            }
            Ok(ServerFrame::Error { error }) => {
                view.send_modify(|v| {
                    v.end.get_or_insert(End::Error(error.to_string()));
                });
                return;
            }
            Ok(ServerFrame::Attached { terminal_id }) => view.send_modify(|v| {
                v.problems
                    .push(format!("a second `attached` frame, for {terminal_id}"))
            }),
            Err(error) => {
                view.send_modify(|v| v.problems.push(format!("an unreadable frame: {error}")))
            }
        }
    }
}
