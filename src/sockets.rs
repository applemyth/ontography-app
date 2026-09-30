//! The app's local sockets. An accept error, such as running out of file
//! descriptors, never closes a socket: it is logged unless it repeats the
//! last one, and accepting resumes after a pause, so it cannot spin. A client
//! connects only to a socket served by its own user: the directories under
//! /tmp that hold them go away while nothing serves them, so another user
//! could make one in the meantime.

use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::net::UnixStream;

/// Connects to the socket at `path` if a process of this user serves it.
pub async fn connect(path: impl AsRef<Path>) -> std::io::Result<UnixStream> {
    let stream = UnixStream::connect(path.as_ref()).await?;
    let owner = stream.peer_cred()?.uid();
    if owner != nix::unistd::geteuid().as_raw() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "{} is served by another user ({owner})",
                path.as_ref().display()
            ),
        ));
    }
    Ok(stream)
}

/// How long a socket waits after an accept error to accept again.
const ACCEPT_PAUSE: Duration = Duration::from_millis(100);

/// What a socket has logged of its accept errors.
pub(crate) struct AcceptLog {
    socket: PathBuf,
    /// The data directory whose server log records them.
    root: Option<PathBuf>,
    last: Option<String>,
}

impl AcceptLog {
    pub(crate) fn new(socket: &Path, root: Option<&Path>) -> Self {
        Self {
            socket: socket.to_owned(),
            root: root.map(Path::to_owned),
            last: None,
        }
    }
}

/// The next connection `accept` yields, across any accept errors.
pub(crate) async fn next_connection<T, F: Future<Output = std::io::Result<T>>>(
    mut accept: impl FnMut() -> F,
    log: &mut AcceptLog,
) -> T {
    loop {
        match accept().await {
            Ok(connection) => {
                if let Some(error) = log.last.take()
                    && let Some(root) = &log.root
                {
                    // Recording the failure may itself have needed a descriptor.
                    let socket = log.socket.display();
                    let message = format!("{socket} is accepting connections again after: {error}");
                    crate::logging::record(root, &message);
                }
                return connection;
            }
            Err(error) => {
                let error = error.to_string();
                if log.last.as_ref() != Some(&error) {
                    if let Some(root) = &log.root {
                        let socket = log.socket.display();
                        let message = format!("{socket} could not accept a connection: {error}");
                        crate::logging::record(root, &message);
                    }
                    log.last = Some(error);
                }
                tokio::time::sleep(ACCEPT_PAUSE).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nix::errno::Errno;
    use std::collections::VecDeque;
    use tokio::time::Instant;

    #[tokio::test]
    async fn connects_to_a_socket_its_own_user_serves() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("s.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let (connected, accepted) = tokio::join!(connect(&path), listener.accept());
        connected.unwrap();
        accepted.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn accept_errors_pause_and_are_logged_unless_repeated() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("logs")).unwrap();
        let mut log = AcceptLog::new(Path::new("mcp.sock"), Some(root.path()));
        let failed = |errno| Err(std::io::Error::from(errno));
        let mut results = VecDeque::from([
            failed(Errno::EMFILE),
            failed(Errno::EMFILE),
            failed(Errno::ECONNABORTED),
            Ok(1),
            failed(Errno::ECONNABORTED),
            Ok(2),
        ]);
        let started = Instant::now();
        let mut accept = || std::future::ready(results.pop_front().unwrap());
        assert_eq!(next_connection(&mut accept, &mut log).await, 1);
        assert_eq!(started.elapsed(), 3 * ACCEPT_PAUSE);
        assert_eq!(next_connection(&mut accept, &mut log).await, 2);
        let logged = std::fs::read_to_string(root.path().join("logs/server.log")).unwrap();
        let logged: Vec<_> = logged.lines().collect();
        // Each distinct failure once, and each recovery.
        assert_eq!(logged.len(), 5, "{logged:?}");
        assert!(logged[0].contains(&format!(
            "mcp.sock could not accept a connection: {}",
            std::io::Error::from(Errno::EMFILE)
        )));
        let aborted = std::io::Error::from(Errno::ECONNABORTED).to_string();
        assert!(logged[1].contains(&aborted));
        assert!(logged[2].contains("mcp.sock is accepting connections again"));
        assert!(logged[3].contains(&aborted));
        assert!(logged[4].contains("mcp.sock is accepting connections again"));
    }
}
