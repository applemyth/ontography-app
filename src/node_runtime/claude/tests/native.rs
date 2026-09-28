//! The installed Claude Code, with a configuration of its own and a hook that
//! blocks every prompt, so no model request is made.
use super::*;
use crate::{
    node_mcp::NodeMcp,
    node_runtime::claude::{session, stream},
    terminal::Terminal,
};
use ontography::InvocationStatus;
use tokio::{io::BufReader, sync::watch};

/// Never used: Claude asks before using an API key it has not seen, so the
/// test's configuration approves this one, and blocked prompts never reach
/// the API.
const API_KEY: &str =
    "sk-ant-api03-ontography-native-test-00000000000000000000000000000000-AAAAAAAA";

/// A node, and Claude configured for it alone: onboarded, trusting the node's
/// workspace, taking the key without asking, and blocking every prompt.
struct Native {
    directory: tempfile::TempDir,
    home: PathBuf,
    env: BTreeMap<String, String>,
    plan: Plan,
}

impl Native {
    fn new() -> Self {
        let directory = tempfile::Builder::new()
            .prefix("onto-claude-")
            .tempdir_in("/tmp")
            .unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let cwd = directory.path().join("workspace");
        let home = directory.path().join("claude-config");
        std::fs::create_dir(&cwd).unwrap();
        std::fs::create_dir(&home).unwrap();
        let mut state = json!({
            "hasCompletedOnboarding": true,
            "customApiKeyResponses": {"approved": [&API_KEY[API_KEY.len() - 20..]], "rejected": []},
        });
        let trusted = std::fs::canonicalize(&cwd).unwrap();
        state["projects"][trusted.to_str().unwrap()] = json!({"hasTrustDialogAccepted": true});
        std::fs::write(home.join(".claude.json"), state.to_string()).unwrap();
        let blocked = json!({"hooks": {"UserPromptSubmit": [
            {"hooks": [{"type": "command", "command": "exit 2"}]},
        ]}});
        std::fs::write(home.join("settings.json"), blocked.to_string()).unwrap();
        let env = BTreeMap::from([
            (
                "CLAUDE_CONFIG_DIR".into(),
                home.to_string_lossy().into_owned(),
            ),
            ("ANTHROPIC_API_KEY".into(), API_KEY.into()),
            (
                "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC".into(),
                "1".into(),
            ),
        ]);
        let config = AgentConfig {
            prompt: "Handle graph work.".into(),
            model: None,
            pty: true,
            mcp: BTreeMap::new(),
            permission_mode: None,
        };
        let plan = new_plan(directory.path(), &config).unwrap();
        Self {
            directory,
            home,
            env,
            plan,
        }
    }

    /// Claude took its first prompt in the saved conversation.
    async fn began(&self) {
        assert!(
            self.directory
                .path()
                .join("claude-session.started")
                .exists()
        );
        let transcript = format!("{}.jsonl", self.plan.session_id());
        eventually(async || {
            let mut projects = std::fs::read_dir(self.home.join("projects"))
                .ok()?
                .flatten();
            projects
                .any(|project| project.path().join(&transcript).exists())
                .then_some(())
        })
        .await;
    }
}

/// The worker's one attempt, once its delivery is marked sent.
async fn delivered(fixture: &Fixture) -> InvocationId {
    let node = fixture.tools.node_id().to_owned();
    eventually(async || {
        let attempts = fixture
            .session
            .invocations_page(Some(&node), None, 100)
            .await
            .unwrap();
        let [attempt] = &attempts[..] else {
            return None;
        };
        let receipts = fixture
            .session
            .invocation_events(attempt.id, 0, 100)
            .await
            .unwrap();
        receipts
            .iter()
            .any(|receipt| receipt.state == ReceiptState::Sent)
            .then_some(attempt.id)
    })
    .await
}

#[tokio::test]
#[ignore = "requires installed Claude Code; blocks every prompt, so no model request is made"]
async fn native_claude_takes_delivered_work_in_its_terminal() {
    let fixture = fixture().await;
    let native = Native::new();
    let mut mcp = NodeMcp::bind(native.directory.path(), fixture.tools.clone()).unwrap();
    let (hooks, events) = NodeHooks::bind(native.directory.path()).unwrap();
    let spec = native
        .plan
        .session(
            Path::new("claude"),
            mcp.server().unwrap(),
            &hooks,
            native.env.clone(),
        )
        .unwrap();
    fixture.deliver("native work").await;
    let terminal = Terminal::launch(spec, native.directory.path().join("terminal.sock"))
        .await
        .unwrap();
    let (report, mut status) = watch::channel(Status::Starting);
    let driver = tokio::spawn({
        let (terminal, tools) = (terminal.clone(), fixture.tools.clone());
        let started = native.plan.started();
        async move {
            let report = move |status| {
                report.send_replace(status);
            };
            session::deliver(&*terminal, events, &tools, started, report).await
        }
    });
    let taken = tokio::time::timeout(
        Duration::from_secs(60),
        status.wait_for(|status| *status == Status::Working),
    )
    .await
    .is_ok_and(|waited| waited.is_ok());
    assert!(
        taken,
        "Claude never took the delivery ({:?}): {}",
        *status.borrow(),
        terminal.screen_text()
    );
    delivered(&fixture).await;
    native.began().await;
    // The configured hook kept the prompt from the model.
    eventually(async || {
        terminal
            .screen_text()
            .contains("blocked by hook")
            .then_some(())
    })
    .await;
    driver.abort();
    terminal.shutdown().await.unwrap();
    drop(hooks);
    mcp.shutdown().await;
    fixture.stop().await;
}

#[tokio::test]
#[ignore = "requires installed Claude Code; blocks every prompt, so no model request is made"]
async fn native_headless_claude_takes_delivered_work() {
    let fixture = fixture().await;
    let native = Native::new();
    let mut mcp = NodeMcp::bind(native.directory.path(), fixture.tools.clone()).unwrap();
    let argv = native
        .plan
        .headless(Path::new("claude"), mcp.server().unwrap());
    let mut claude = Command::new(&argv[0])
        .args(&argv[1..])
        .envs(&native.env)
        .current_dir(native.directory.path().join("workspace"))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    fixture.deliver("native work").await;
    let (report, mut status) = watch::channel(Status::Starting);
    let driver = tokio::spawn({
        let input = claude.stdin.take().unwrap();
        let output = BufReader::new(claude.stdout.take().unwrap());
        let (tools, started) = (fixture.tools.clone(), native.plan.started());
        async move {
            let report = move |status| {
                report.send_replace(status);
            };
            stream::deliver(input, output, &tools, started, report).await
        }
    });
    let attempt = delivered(&fixture).await;
    native.began().await;
    // A blocked prompt ends its turn at once, and not as a failure.
    tokio::time::timeout(
        Duration::from_secs(60),
        status.wait_for(|status| *status == Status::Idle),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        fixture.invocation_status(&attempt.to_string()).await,
        InvocationStatus::Open
    );
    driver.abort();
    claude.kill().await.unwrap();
    mcp.shutdown().await;
    fixture.stop().await;
}
