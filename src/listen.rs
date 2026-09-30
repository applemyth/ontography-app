//! Accepting connections on the app's local sockets. An accept error, such
//! as running out of file descriptors, never closes a socket: it is logged
//! unless it repeats the last one, and accepting resumes after a pause, so it
//! cannot spin.

use std::path::{Path, PathBuf};
use std::time::Duration;

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
            Ok(connection) => return connection,
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
        assert_eq!(logged.len(), 2, "{logged:?}");
        assert!(logged[0].contains(&format!(
            "mcp.sock could not accept a connection: {}",
            std::io::Error::from(Errno::EMFILE)
        )));
        assert!(logged[1].contains(&std::io::Error::from(Errno::ECONNABORTED).to_string()));
    }
}
