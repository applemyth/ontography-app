use super::*;
use crate::{
    node_hooks::EVENTS,
    node_tool::tests::{Fixture, document},
};
use ontography::{ContentDigest, InvocationId, ReceiptState};
use serde_json::Value;
use std::{os::unix::fs::PermissionsExt, str::FromStr, time::Duration};
use tokio::{io::AsyncWriteExt, process::Command};

mod native;
mod session;
mod stream;

/// Node tools at `worker`, whose failed tasks are offered again at once.
async fn fixture() -> Fixture {
    let retry = json!({"max_attempts": 5, "initial_delay_secs": 0, "max_delay_secs": 0});
    Fixture::new(document(json!({ "retry": retry })), "worker", None).await
}

/// Waits a few seconds at most for `probe` to find something.
async fn eventually<T>(mut probe: impl AsyncFnMut() -> Option<T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(found) = probe().await {
                return found;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the expected state was never reached")
}

/// Whether `attempt` recorded exactly `bytes`, and marked them sent.
async fn receipt_sent(fixture: &Fixture, attempt: &str, bytes: &[u8]) -> bool {
    let events = fixture
        .session
        .invocation_events(InvocationId::from_str(attempt).unwrap(), 0, 100)
        .await
        .unwrap();
    let digest = ContentDigest::compute(bytes);
    let recorded = events
        .iter()
        .find(|event| event.content_digest == digest)
        .expect("the delivered bytes are the recorded envelope");
    events.iter().any(|event| {
        event.receipt_sequence == recorded.sequence && event.state == ReceiptState::Sent
    })
}

// Records how a launch script started it: `<mode> <id>|<Claude variables>`.
// Beginning creates $FAKE_MARK and exits with $FAKE_BEGIN; resuming exits
// with $FAKE_RESUME.
const FAKE_CLAUDE: &str = r#"#!/bin/sh
previous=
for arg in "$@"; do
    case "$previous" in --session-id|--resume) run="$previous $arg" ;; esac
    previous=$arg
done
names=$(env | grep -oE '^(CLAUDE|ANTHROPIC)[A-Z_]*=' | tr -d '=' | sort | tr '\n' ' ')
printf '%s|%s\n' "$run" "$names" >> "$FAKE_LOG"
case "$run" in
    --session-id*)
        if [ -n "$FAKE_MARK" ]; then : > "$FAKE_MARK"; fi
        exit "${FAKE_BEGIN:-0}" ;;
    --resume*) exit "${FAKE_RESUME:-0}" ;;
esac
"#;

fn config() -> AgentConfig {
    AgentConfig {
        prompt: "Review changes".into(),
        model: Some("opus".into()),
        pty: true,
        mcp: BTreeMap::from([(
            "docs".into(),
            McpServer {
                command: "docs-server".into(),
                args: vec!["--stdio".into()],
                env: BTreeMap::from([("DOCS_REGION".into(), "eu".into())]),
            },
        )]),
        permission_mode: Some("acceptEdits".into()),
    }
}

fn node_tools() -> McpServer {
    McpServer {
        command: "/opt/ontography".into(),
        args: vec!["node-mcp".into()],
        env: BTreeMap::from([("ONTOGRAPHY_NODE_MCP_TOKEN".into(), "token".into())]),
    }
}

/// A private node directory with its workspace.
fn node_directory() -> tempfile::TempDir {
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::create_dir(directory.path().join("workspace")).unwrap();
    directory
}

fn new_plan(directory: &Path, config: &AgentConfig) -> Result<Plan> {
    Plan::new(
        "writer",
        config,
        directory,
        &directory.join("workspace"),
        &directory.join("tool-workspaces/checkouts"),
    )
}

/// The value that follows `name`.
fn option<'a>(options: &'a [String], name: &str) -> &'a str {
    let index = options
        .iter()
        .position(|option| option == name)
        .unwrap_or_else(|| panic!("{name} is missing from {options:?}"));
    &options[index + 1]
}

#[test]
fn a_conversation_is_saved_once_and_continues_only_at_its_node_and_workspace() {
    let directory = node_directory();
    let plan = new_plan(directory.path(), &config()).unwrap();
    uuid::Uuid::parse_str(plan.session_id()).unwrap();
    let saved: SavedSession = read_json(&directory.path().join("claude-session.json")).unwrap();
    assert_eq!(saved.session_id, plan.session_id());
    assert_eq!(saved.node_id, "writer");
    assert_eq!(
        saved.cwd,
        std::fs::canonicalize(directory.path().join("workspace")).unwrap()
    );
    let checkouts = directory.path().join("tool-workspaces/checkouts");
    assert_eq!(
        std::fs::metadata(&checkouts).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        new_plan(directory.path(), &config()).unwrap().session_id(),
        plan.session_id()
    );

    let elsewhere = directory.path().join("elsewhere");
    std::fs::create_dir(&elsewhere).unwrap();
    let error = Plan::new(
        "writer",
        &config(),
        directory.path(),
        &elsewhere,
        &checkouts,
    )
    .err()
    .unwrap();
    assert!(error.message.contains("different node or workspace"));
    assert!(
        Plan::new(
            "reviewer",
            &config(),
            directory.path(),
            &directory.path().join("workspace"),
            &checkouts
        )
        .is_err()
    );

    // A marker left without its conversation must not resume a new one.
    plan.started().set().unwrap();
    std::fs::remove_file(directory.path().join("claude-session.json")).unwrap();
    let fresh = new_plan(directory.path(), &config()).unwrap();
    assert_ne!(fresh.session_id(), plan.session_id());
    assert!(!directory.path().join("claude-session.started").exists());
}

#[tokio::test]
async fn the_terminal_launch_passes_hooks_tools_and_instructions_as_options() {
    let directory = node_directory();
    let plan = new_plan(directory.path(), &config()).unwrap();
    let (hooks, _events) = NodeHooks::bind(directory.path()).unwrap();
    let spec = plan
        .session(
            Path::new("claude"),
            node_tools(),
            &hooks,
            BTreeMap::from([("ONTOGRAPHY_NODE_NAME".into(), "writer".into())]),
        )
        .unwrap();
    assert_eq!(spec.program, Path::new("/bin/sh"));
    assert_eq!(spec.args[0], "-c");
    assert!(spec.args[1].contains("Press Enter to resume"));
    let marker = directory.path().join("claude-session.started");
    assert_eq!(
        spec.args[2..6],
        [
            "ontography-claude",
            "claude",
            plan.session_id(),
            marker.to_str().unwrap()
        ]
    );
    let options = &spec.args[6..];
    // Only options and their values: nothing positional follows a variadic option.
    for pair in options.chunks(2) {
        assert!(pair[0].starts_with("--"), "{pair:?}");
        assert!(pair.len() == 2 && !pair[1].starts_with("--"), "{pair:?}");
    }
    let settings: Value = serde_json::from_str(option(options, "--settings")).unwrap();
    for event in EVENTS {
        let command = settings["hooks"][event][0]["hooks"][0]["command"]
            .as_str()
            .unwrap();
        assert!(
            command.ends_with(&format!(" node-hook {event}")),
            "{command}"
        );
    }
    let servers: Value = serde_json::from_str(option(options, "--mcp-config")).unwrap();
    assert_eq!(
        servers,
        json!({"mcpServers": {
            "ontography_node": {"command": "/opt/ontography", "args": ["node-mcp"],
                "env": {"ONTOGRAPHY_NODE_MCP_TOKEN": "token"}},
            "docs": {"command": "docs-server", "args": ["--stdio"], "env": {"DOCS_REGION": "eu"}},
        }})
    );
    assert!(!options.iter().any(|option| option == "--strict-mcp-config"));
    let prompt = option(options, "--append-system-prompt");
    assert!(prompt.starts_with("Review changes\n\nYou are graph node \"writer\"."));
    for words in [
        "ontography_message",
        "attempt_id",
        "ontography_node MCP tools",
        "submit_invocation",
        "fail_invocation",
        "not published",
        "invalid",
    ] {
        assert!(prompt.contains(words), "{words}");
    }
    assert_eq!(option(options, "--system-prompt-snapshot"), "off");
    assert_eq!(
        Path::new(option(options, "--add-dir")),
        std::fs::canonicalize(directory.path().join("tool-workspaces/checkouts")).unwrap()
    );
    assert_eq!(option(options, "--model"), "opus");
    assert_eq!(option(options, "--permission-mode"), "acceptEdits");
    assert_eq!(spec.env["ONTOGRAPHY_NODE_NAME"], "writer");
    for (name, value) in hooks.environment() {
        assert_eq!(spec.env[&name], value);
    }
    assert_eq!(
        spec.cwd,
        std::fs::canonicalize(directory.path().join("workspace")).unwrap()
    );
    assert_eq!(spec.session_id, "writer");
}

#[test]
fn the_headless_launch_speaks_stream_json_without_hooks() {
    let directory = node_directory();
    let config = AgentConfig {
        model: None,
        permission_mode: None,
        ..config()
    };
    let plan = new_plan(directory.path(), &config).unwrap();
    let argv = plan.headless(Path::new("claude"), node_tools());
    assert_eq!(argv[..2], ["/bin/sh", "-c"]);
    assert!(!argv[2].contains("Press Enter"));
    assert_eq!(argv[3..5], ["ontography-claude", "claude"]);
    assert_eq!(argv[5], plan.session_id());
    let options = &argv[7..];
    assert_eq!(
        options[..9],
        [
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
            "--replay-user-messages",
            "--permission-prompts",
            "none"
        ]
    );
    let options = &options[9..];
    for pair in options.chunks(2) {
        assert!(pair[0].starts_with("--") && pair.len() == 2, "{pair:?}");
    }
    assert!(option(options, "--mcp-config").contains("ontography_node"));
    assert!(option(options, "--append-system-prompt").contains("submit_invocation"));
    for absent in ["--settings", "--model", "--permission-mode"] {
        assert!(!options.iter().any(|option| option == absent), "{absent}");
    }
}

/// Runs a launch command against the fake Claude, as an enclosing Claude Code
/// session would start it, and returns its output and what the fake recorded.
async fn launch(
    mut command: Command,
    directory: &Path,
    fake: &[(&str, &str)],
    input: &[u8],
) -> (std::process::Output, Vec<String>) {
    let log = directory.join("fake.log");
    let _ = std::fs::remove_file(&log);
    command
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .envs(INHERITED.map(|name| (name, "inherited")))
        .env("CLAUDE_CONFIG_DIR", directory.join("claude-config"))
        .env("ANTHROPIC_MODEL", "fixture")
        .env("FAKE_LOG", &log)
        .envs(fake.iter().copied())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped());
    let mut child = command.spawn().unwrap();
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(input).await.unwrap();
    drop(stdin);
    let output = child.wait_with_output().await.unwrap();
    let runs = std::fs::read_to_string(&log)
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect();
    (output, runs)
}

#[tokio::test]
async fn launch_scripts_begin_or_resume_the_conversation_without_inherited_variables() {
    let directory = node_directory();
    let fake = directory.path().join("claude");
    std::fs::write(&fake, FAKE_CLAUDE).unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let plan = new_plan(directory.path(), &config()).unwrap();
    let id = plan.session_id().to_owned();
    let marker = directory.path().join("claude-session.started");
    let marker = marker.to_str().unwrap();
    let variables = "ANTHROPIC_MODEL CLAUDE_CONFIG_DIR ";
    let begin = format!("--session-id {id}|{variables}");
    let resume = format!("--resume {id}|{variables}");
    let (begin, resume) = (begin.as_str(), resume.as_str());
    let headless = || {
        let argv = plan.headless(&fake, node_tools());
        let mut command = Command::new(&argv[0]);
        command.args(&argv[1..]);
        command
    };

    let (output, runs) = launch(headless(), directory.path(), &[], b"").await;
    assert!(output.status.success());
    assert_eq!(runs, [begin]);
    // Claude refuses to begin an ID whose transcript exists.
    let (output, runs) = launch(headless(), directory.path(), &[("FAKE_BEGIN", "1")], b"").await;
    assert!(output.status.success());
    assert_eq!(runs, [begin, resume]);
    // A conversation that began before failing is not started again.
    let (output, runs) = launch(
        headless(),
        directory.path(),
        &[("FAKE_BEGIN", "3"), ("FAKE_MARK", marker)],
        b"",
    )
    .await;
    assert_eq!(output.status.code(), Some(3));
    assert_eq!(runs, [begin]);
    let (output, runs) = launch(headless(), directory.path(), &[], b"").await;
    assert!(output.status.success());
    assert_eq!(runs, [resume]);
    // Nor can Claude resume a conversation whose transcript it never wrote.
    let (output, runs) = launch(headless(), directory.path(), &[("FAKE_RESUME", "1")], b"").await;
    assert!(output.status.success());
    assert_eq!(runs, [resume, begin]);

    std::fs::remove_file(marker).unwrap();
    let (hooks, _events) = NodeHooks::bind(directory.path()).unwrap();
    let spec = plan
        .session(&fake, node_tools(), &hooks, BTreeMap::new())
        .unwrap();
    let mut terminal = Command::new(&spec.program);
    terminal.args(&spec.args).current_dir(&spec.cwd);
    // Each exit waits for Enter; the end of input ends the node.
    let (output, runs) = launch(terminal, directory.path(), &[("FAKE_BEGIN", "1")], b"\n").await;
    assert_eq!(runs, [begin, resume, begin, resume]);
    let shown = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        shown
            .matches("Claude exited; this node is still running. Press Enter to resume.")
            .count(),
        2
    );
}
