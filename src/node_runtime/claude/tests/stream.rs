//! The headless driver against a scripted Claude on in-memory pipes, with
//! real node tools.
use super::*;
use crate::node_runtime::claude::stream::deliver;
use ontography::InvocationStatus;
use tokio::{
    io::{AsyncBufReadExt, BufReader, DuplexStream, Lines},
    sync::watch,
    task::JoinHandle,
};

/// Claude's side of the pipes.
struct Claude {
    stdin: Lines<BufReader<DuplexStream>>,
    stdout: DuplexStream,
}

impl Claude {
    /// The next message written to Claude.
    async fn read(&mut self) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(10), self.stdin.next_line())
            .await
            .expect("a message for Claude")
            .unwrap()
            .expect("an open input");
        serde_json::from_str(&line).unwrap()
    }

    /// Nothing more is written for a while.
    async fn quiet(&mut self) {
        let line = tokio::time::timeout(Duration::from_millis(400), self.stdin.next_line()).await;
        assert!(line.is_err(), "unexpected message: {line:?}");
    }

    async fn write(&mut self, line: &str) {
        self.stdout.write_all(line.as_bytes()).await.unwrap();
        self.stdout.write_all(b"\n").await.unwrap();
    }
}

fn start(
    fixture: &Fixture,
    started: &Path,
) -> (Claude, JoinHandle<Result<()>>, watch::Receiver<Status>) {
    let (input, stdin) = tokio::io::duplex(1 << 20);
    let (stdout, output) = tokio::io::duplex(1 << 20);
    let (report, status) = watch::channel(Status::Starting);
    let tools = fixture.tools.clone();
    let started = Started(started.into());
    let driver = tokio::spawn(async move {
        let report = move |status| {
            report.send_replace(status);
        };
        deliver(input, BufReader::new(output), &tools, started, report).await
    });
    let claude = Claude {
        stdin: BufReader::new(stdin).lines(),
        stdout,
    };
    (claude, driver, status)
}

/// A delivered message's envelope text, after checking the message's shape.
fn content(message: &Value) -> &str {
    assert_eq!(message["type"], "user");
    assert_eq!(message["message"]["role"], "user");
    assert!(message["parent_tool_use_id"].is_null());
    assert_eq!(message["origin"], json!({"kind": "human"}));
    let uuid = uuid::Uuid::parse_str(message["uuid"].as_str().unwrap()).unwrap();
    assert_eq!(uuid.get_version_num(), 4);
    message["message"]["content"].as_str().unwrap()
}

fn envelope(message: &Value) -> Value {
    let envelope: Value = serde_json::from_str(content(message)).unwrap();
    assert_eq!(envelope["type"], "ontography_message");
    envelope
}

#[tokio::test]
async fn messages_carry_their_envelope_and_wait_for_their_turn_to_end() {
    let fixture = fixture().await;
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("claude-session.started");
    fixture.deliver("first").await;
    fixture.deliver("second").await;
    let (mut claude, driver, mut status) = start(&fixture, &marker);

    let first = claude.read().await;
    let uuid = first["uuid"].as_str().unwrap().to_owned();
    let attempt = envelope(&first)["attempt_id"].as_str().unwrap().to_owned();
    assert!(!receipt_sent(&fixture, &attempt, content(&first).as_bytes()).await);
    for line in [
        json!({"type": "command_lifecycle", "command_uuid": uuid, "state": "queued"}).to_string(),
        json!({"type": "command_lifecycle", "command_uuid": uuid, "state": "started"}).to_string(),
        json!({"type": "system", "subtype": "init", "session_id": "s"}).to_string(),
        json!({"type": "assistant", "message": {"content": [{"type": "text", "text": "Working."}]}})
            .to_string(),
        json!({"type": "rate_limit_event"}).to_string(),
        "not json".into(),
    ] {
        claude.write(&line).await;
    }
    eventually(async || {
        receipt_sent(&fixture, &attempt, content(&first).as_bytes())
            .await
            .then_some(())
    })
    .await;
    tokio::time::timeout(
        Duration::from_secs(10),
        status.wait_for(|status| *status == Status::Working),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(marker.exists());
    // Messages sent close together would merge into one turn.
    claude.quiet().await;
    claude
        .write(
            &json!({"type": "result", "subtype": "success", "is_error": false, "result": "Done.",
                "user_message_uuid": uuid, "user_message_uuids": [uuid]})
            .to_string(),
        )
        .await;
    claude
        .write(
            &json!({"type": "command_lifecycle", "command_uuid": uuid, "state": "completed"})
                .to_string(),
        )
        .await;

    // Echoed back rather than queued, then failed by an API error.
    let second = claude.read().await;
    let uuid = second["uuid"].as_str().unwrap().to_owned();
    let retried = envelope(&second);
    let attempt = retried["attempt_id"].as_str().unwrap().to_owned();
    let replay =
        json!({"type": "user", "message": second["message"], "uuid": uuid, "isReplay": true});
    claude.write(&replay.to_string()).await;
    eventually(async || {
        receipt_sent(&fixture, &attempt, content(&second).as_bytes())
            .await
            .then_some(())
    })
    .await;
    claude
        .write(
            &json!({"type": "result", "subtype": "success", "is_error": true,
                "result": "API Error: 529 Overloaded", "user_message_uuid": uuid, "user_message_uuids": [uuid]})
            .to_string(),
        )
        .await;
    let retry = claude.read().await;
    assert_eq!(
        envelope(&retry)["inputs"][0]["message"],
        retried["inputs"][0]["message"]
    );
    assert_eq!(
        fixture.invocation_status(&attempt).await,
        InvocationStatus::Failed
    );
    // A successful turn may leave its attempt open.
    let first = envelope(&first)["attempt_id"].as_str().unwrap().to_owned();
    assert_eq!(
        fixture.invocation_status(&first).await,
        InvocationStatus::Open
    );
    driver.abort();
    fixture.stop().await;
}

#[tokio::test]
async fn a_refused_message_fails_and_claude_exiting_ends_delivery() {
    let fixture = fixture().await;
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("claude-session.started");
    fixture.deliver("first").await;
    let (mut claude, driver, _status) = start(&fixture, &marker);
    let message = claude.read().await;
    let uuid = message["uuid"].as_str().unwrap();
    let attempt = envelope(&message)["attempt_id"]
        .as_str()
        .unwrap()
        .to_owned();
    claude
        .write(
            &json!({"type": "command_lifecycle", "command_uuid": uuid, "state": "refused"})
                .to_string(),
        )
        .await;
    eventually(async || {
        (fixture.invocation_status(&attempt).await == InvocationStatus::Failed).then_some(())
    })
    .await;
    // Claude never took it.
    assert!(!receipt_sent(&fixture, &attempt, content(&message).as_bytes()).await);
    assert!(!marker.exists());
    let Claude { stdin, stdout } = claude;
    drop(stdout);
    let error = driver.await.unwrap().unwrap_err();
    assert_eq!(error.message, "Claude exited");
    drop(stdin);
    fixture.stop().await;
}
