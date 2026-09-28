//! The terminal driver against a scripted console and hook events, with real
//! node tools: what it types and when, and what its attempts record.
use super::*;
use crate::{
    node_hooks::HookEvent,
    node_runtime::claude::session::{Console, Timing, run},
    terminal::ClientInput,
};
use ontography::InvocationStatus;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, watch};

const TIMING: Timing = Timing {
    trust: Duration::from_millis(300),
    quiet: Duration::from_millis(400),
    accept: Duration::from_millis(500),
    keystroke: Duration::from_millis(1),
    paste: Duration::from_millis(1),
    poll: Duration::from_millis(10),
};
const REQUEST: &str = "Handle the graph work pasted here: ";
const STASH: &[u8] = b"\x13";

/// A terminal whose typing and screen the test sets.
#[derive(Default)]
struct FakeConsole(Mutex<Screen>);

#[derive(Default)]
struct Screen {
    sent: Vec<Vec<u8>>,
    typing: ClientInput,
    text: String,
}

impl Console for FakeConsole {
    fn send(&self, bytes: &[u8]) -> Result<()> {
        self.0.lock().unwrap().sent.push(bytes.to_vec());
        Ok(())
    }

    fn typing(&self) -> ClientInput {
        self.0.lock().unwrap().typing
    }

    fn screen(&self) -> String {
        self.0.lock().unwrap().text.clone()
    }
}

struct Harness {
    fixture: Fixture,
    console: Arc<FakeConsole>,
    hooks: mpsc::Sender<HookEvent>,
    status: watch::Receiver<Status>,
    driver: tokio::task::JoinHandle<Result<()>>,
    started: PathBuf,
    _directory: tempfile::TempDir,
}

impl Harness {
    async fn new() -> Self {
        let fixture = fixture().await;
        let directory = tempfile::tempdir().unwrap();
        let started = directory.path().join("claude-session.started");
        let console = Arc::new(FakeConsole::default());
        let (hooks, events) = mpsc::channel(64);
        let (report, status) = watch::channel(Status::Starting);
        let driver = tokio::spawn({
            let (console, tools) = (console.clone(), fixture.tools.clone());
            let marker = Started(started.clone());
            async move {
                let report = move |status| {
                    report.send_replace(status);
                };
                run(&*console, events, &tools, marker, report, TIMING).await
            }
        });
        Self {
            fixture,
            console,
            hooks,
            status,
            driver,
            started,
            _directory: directory,
        }
    }

    async fn hook(&self, name: &str, input: Value) {
        self.hooks
            .send(HookEvent {
                name: name.into(),
                input,
            })
            .await
            .unwrap();
    }

    /// Someone typed `count` frames in all, the last one `ago`.
    fn typed(&self, count: u64, ago: Duration) {
        self.console.0.lock().unwrap().typing = ClientInput {
            count,
            last: Some(std::time::Instant::now() - ago),
        };
    }

    fn show(&self, screen: &str) {
        self.console.0.lock().unwrap().text = screen.into();
    }

    fn sent_now(&self) -> Vec<Vec<u8>> {
        self.console.0.lock().unwrap().sent.clone()
    }

    /// Everything sent, once there are `count` writes.
    async fn sent(&self, count: usize) -> Vec<Vec<u8>> {
        eventually(async || Some(self.sent_now()).filter(|sent| sent.len() >= count)).await
    }

    async fn status(&mut self, expected: Status) {
        tokio::time::timeout(
            Duration::from_secs(10),
            self.status.wait_for(|status| *status == expected),
        )
        .await
        .unwrap_or_else(|_| panic!("status never became {expected:?}"))
        .unwrap();
    }

    /// Nothing more is typed for a while.
    async fn quiet(&self) {
        let before = self.sent_now().len();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(self.sent_now().len(), before, "{:?}", self.sent_now());
    }

    /// Claude takes the delivered prompt, reporting a long paste as it does.
    async fn submit(&self, delivery: &[u8]) {
        let envelope = std::str::from_utf8(pasted(delivery)).unwrap();
        let prompt = format!(
            "{REQUEST}\n\n<pasted_content id=\"a5ab\">\n{envelope}\n</pasted_content id=\"a5ab\">\n"
        );
        self.hook("UserPromptSubmit", json!({ "prompt": prompt }))
            .await;
    }

    async fn stop(self) {
        self.driver.abort();
        let _ = self.driver.await;
        self.fixture.stop().await;
    }
}

/// The envelope a delivery pastes: exactly the request, then the envelope as
/// a bracketed paste.
fn pasted(delivery: &[u8]) -> &[u8] {
    delivery
        .strip_prefix(format!("{REQUEST}\x1b[200~").as_bytes())
        .and_then(|rest| rest.strip_suffix(b"\x1b[201~"))
        .unwrap_or_else(|| panic!("not a delivery: {:?}", String::from_utf8_lossy(delivery)))
}

fn envelope(delivery: &[u8]) -> Value {
    let envelope: Value = serde_json::from_slice(pasted(delivery)).unwrap();
    assert_eq!(envelope["type"], "ontography_message");
    envelope
}

fn attempt(delivery: &[u8]) -> String {
    envelope(delivery)["attempt_id"].as_str().unwrap().into()
}

#[tokio::test]
async fn delivers_after_session_start_and_marks_the_envelope_sent_once_claude_takes_it() {
    let mut harness = Harness::new().await;
    harness.fixture.deliver("first").await;
    // Hooks do not run while Claude asks whether to trust the folder.
    harness.status(Status::AwaitingTrust).await;
    assert!(harness.sent_now().is_empty());
    harness
        .hook("SessionStart", json!({"source": "startup"}))
        .await;
    let sent = harness.sent(2).await;
    assert_eq!(sent[1], b"\r");
    assert_eq!(envelope(&sent[0])["inputs"][0]["message"], "first");
    let first = attempt(&sent[0]);
    harness.status(Status::Delivering).await;
    assert!(!receipt_sent(&harness.fixture, &first, pasted(&sent[0])).await);
    assert!(!harness.started.exists());

    harness.submit(&sent[0]).await;
    harness.status(Status::Working).await;
    assert!(receipt_sent(&harness.fixture, &first, pasted(&sent[0])).await);
    assert!(harness.started.exists());
    // One message at a time, and only while Claude is idle.
    harness.fixture.deliver("second").await;
    harness
        .hook("SessionStart", json!({"source": "compact"}))
        .await;
    harness
        .hook("PermissionRequest", json!({"tool_name": "Bash"}))
        .await;
    harness.status(Status::AwaitingApproval).await;
    harness.quiet().await;
    harness.hook("Stop", json!({})).await;
    let sent = harness.sent(4).await;
    assert_eq!(envelope(&sent[2])["inputs"][0]["message"], "second");
    assert_eq!(sent[3], b"\r");
    // A successful turn may leave its attempt open.
    assert_eq!(
        harness.fixture.invocation_status(&first).await,
        InvocationStatus::Open
    );
    harness.stop().await;
}

#[tokio::test]
async fn waits_while_someone_types_and_stashes_what_the_input_box_may_hold() {
    let mut harness = Harness::new().await;
    harness
        .hook("SessionStart", json!({"source": "startup"}))
        .await;
    harness.status(Status::Idle).await;
    harness.typed(3, Duration::ZERO);
    harness.fixture.deliver("first").await;
    harness.status(Status::Attended).await;
    harness.quiet().await;

    harness.typed(3, Duration::from_secs(60));
    let sent = harness.sent(3).await;
    assert_eq!(sent[0], STASH);
    assert_eq!(sent[2], b"\r");
    harness.submit(&sent[1]).await;
    harness.hook("Stop", json!({})).await;
    // Submitting restored the stashed draft, so it is stashed again.
    harness.fixture.deliver("second").await;
    let sent = harness.sent(6).await;
    assert_eq!(sent[3], STASH);
    assert_eq!(envelope(&sent[4])["inputs"][0]["message"], "second");
    harness.stop().await;
}

#[tokio::test]
async fn never_stashes_over_a_stashed_draft() {
    let mut harness = Harness::new().await;
    harness
        .hook("SessionStart", json!({"source": "startup"}))
        .await;
    let stashed = "❯ \n                         ◐ medium · /effort · \u{203a} stashed\n";
    harness.show(stashed);
    // With nothing typed since the last prompt, no stash is needed.
    harness.fixture.deliver("first").await;
    let sent = harness.sent(2).await;
    envelope(&sent[0]);
    harness.submit(&sent[0]).await;
    harness.status(Status::Working).await;
    harness.hook("Stop", json!({})).await;
    harness.status(Status::Idle).await;
    // Stashing a new draft would replace the stashed one.
    harness.typed(1, Duration::from_secs(60));
    harness.fixture.deliver("second").await;
    harness.status(Status::Attended).await;
    harness.quiet().await;
    harness.show("❯ \n                         ◐ medium · /effort\n");
    let sent = harness.sent(5).await;
    assert_eq!(sent[2], STASH);
    assert_eq!(envelope(&sent[3])["inputs"][0]["message"], "second");
    harness.stop().await;
}

#[tokio::test]
async fn unaccepted_input_fails_its_attempt_and_waits_for_claude_to_go_idle() {
    let mut harness = Harness::new().await;
    harness
        .hook("SessionStart", json!({"source": "startup"}))
        .await;
    harness.fixture.deliver("first").await;
    let sent = harness.sent(2).await;
    let first = attempt(&sent[0]);
    // Claude never reports the prompt.
    harness.status(Status::Working).await;
    assert_eq!(
        harness.fixture.invocation_status(&first).await,
        InvocationStatus::Failed
    );
    assert!(!receipt_sent(&harness.fixture, &first, pasted(&sent[0])).await);
    harness.quiet().await;
    harness
        .hook("Notification", json!({"notification_type": "idle_prompt"}))
        .await;
    // The unaccepted input may remain in the box, so it is stashed first.
    let sent = harness.sent(5).await;
    assert_eq!(sent[2], STASH);
    let retried = envelope(&sent[3]);
    assert_eq!(retried["inputs"][0]["message"], "first");
    assert_ne!(retried["attempt_id"], first.as_str());
    harness.stop().await;
}

#[tokio::test]
async fn a_failed_turn_fails_the_attempts_it_carried() {
    let mut harness = Harness::new().await;
    harness
        .hook("SessionStart", json!({"source": "startup"}))
        .await;
    harness.fixture.deliver("first").await;
    let sent = harness.sent(2).await;
    let first = attempt(&sent[0]);
    harness.submit(&sent[0]).await;
    harness.status(Status::Working).await;
    harness
        .hook(
            "StopFailure",
            json!({"error": "overloaded", "last_assistant_message": "API Error: 529 Overloaded"}),
        )
        .await;
    // The retry policy offers the task again at once.
    let sent = harness.sent(4).await;
    let retried = envelope(&sent[2]);
    assert_eq!(retried["inputs"][0]["message"], "first");
    assert_ne!(retried["attempt_id"], first.as_str());
    assert_eq!(
        harness.fixture.invocation_status(&first).await,
        InvocationStatus::Failed
    );
    harness.stop().await;
}

#[tokio::test]
async fn nothing_is_delivered_while_claude_is_exited() {
    let mut harness = Harness::new().await;
    harness
        .hook("SessionStart", json!({"source": "startup"}))
        .await;
    harness
        .hook("SessionEnd", json!({"reason": "prompt_input_exit"}))
        .await;
    harness.status(Status::Exited).await;
    harness.fixture.deliver("first").await;
    harness.quiet().await;
    // Enter in the terminal resumes the conversation.
    harness
        .hook("SessionStart", json!({"source": "resume"}))
        .await;
    let sent = harness.sent(2).await;
    let first = attempt(&sent[0]);
    // Input Claude had not taken when it exited is lost with it.
    harness.hook("SessionEnd", json!({"reason": "other"})).await;
    harness.status(Status::Exited).await;
    assert_eq!(
        harness.fixture.invocation_status(&first).await,
        InvocationStatus::Failed
    );
    harness.stop().await;
}
