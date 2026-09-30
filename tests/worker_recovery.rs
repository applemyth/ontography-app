//! A node whose previous worker is still exiting waits for it, visibly, and
//! starts its replacement once it has gone, without a manual resume. A
//! resume of a node whose program just died waits for its cleanup, then
//! starts it again.

use ontography_app::{persistence::Paths, state::Service, tools};
use serde_json::{Value, json};
use std::{io::BufRead, path::Path, process::Stdio, time::Duration};

fn document() -> Value {
    program_document(json!(["/bin/cat"]))
}

fn program_document(argv: Value) -> Value {
    json!({"name":"recovery", "entry":"worker", "nodes":[
        {"id":"worker", "component":"agent", "config":{"prompt":"Work", "argv":argv}},
        {"id":"archive", "component":"inbox"}
    ], "edges":[{"from":"worker", "to":"archive"}]})
}

async fn call(service: &Service, operation: &str, args: Value) -> Value {
    tools::dispatch(service, operation, &args)
        .await
        .unwrap_or_else(|error| panic!("{operation}: {error}; {:?}", error.details))
}

/// The worker's session once `condition` holds for it.
async fn session(service: &Service, run: &str, condition: impl Fn(&Value) -> bool) -> Value {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let status = call(service, "flow.status", json!({"run_id":run})).await;
            let session = status["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|node| node["id"] == "worker")
                .unwrap()["session"]
                .clone();
            if condition(&session) {
                return session;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the worker did not reach the expected state")
}

/// A process group whose leader is gone while a member runs on, as a crashed
/// server's worker may leave. Removing `hold` ends the member.
fn orphaned_group(hold: &Path) -> i32 {
    std::fs::write(hold, b"").unwrap();
    let mut leader = std::process::Command::new("perl")
        .args([
            "-e",
            r#"setpgrp(0, 0); my $pid = fork() // die; if ($pid) { print "$pid\n"; exit 0 } close STDOUT; close STDERR; select(undef, undef, undef, 0.05) while -e $ARGV[0]"#,
        ])
        .arg(hold)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut member = String::new();
    std::io::BufReader::new(leader.stdout.take().unwrap())
        .read_line(&mut member)
        .unwrap();
    let group = leader.id() as i32;
    assert!(leader.wait().unwrap().success());
    group
}

#[tokio::test]
async fn a_node_waits_for_its_previous_worker_and_then_starts_by_itself() {
    let temporary = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(temporary.path().join("data")).unwrap()).unwrap();
    let started = call(
        &service,
        "flow.start",
        json!({"document":document(),"project":temporary.path(),"message":"begin"}),
    )
    .await;
    let run = started["run_id"].as_str().unwrap().to_owned();
    let running = |session: &Value| session["state"] == "running";
    let directory = session(&service, &run, running).await["directory"]
        .as_str()
        .unwrap()
        .to_owned();
    call(&service, "run.suspend", json!({"run_id":run})).await;
    // The node's lease names a group whose supervisor is gone but whose
    // member runs on: not provably the node's, so never signalled.
    let hold = temporary.path().join("hold");
    let group = orphaned_group(&hold);
    std::fs::write(
        Path::new(&directory).join("worker-process.json"),
        serde_json::to_vec(
            &json!({"pid":group,"token":uuid::Uuid::new_v4().to_string(),"identity":"unproven"}),
        )
        .unwrap(),
    )
    .unwrap();
    call(&service, "run.resume", json!({"run_id":run})).await;
    let waiting = session(&service, &run, |session| session["state"] == "waiting").await;
    assert_eq!(waiting["error"]["code"], "worker_still_exiting");
    // Once it goes, the replacement starts with no manual resume.
    std::fs::remove_file(&hold).unwrap();
    let resumed = session(&service, &run, running).await;
    assert!(resumed["error"].is_null());
    service.shutdown().await.unwrap();
}

/// Every process as (pid, parent).
fn processes() -> Vec<(i32, i32)> {
    let listed = std::process::Command::new("/bin/ps")
        .args(["-A", "-o", "pid=,ppid="])
        .output()
        .unwrap();
    String::from_utf8_lossy(&listed.stdout)
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            Some((fields.next()?.parse().ok()?, fields.next()?.parse().ok()?))
        })
        .collect()
}

/// Whether `directory` holds a file anywhere below it.
fn holds_a_file(directory: &Path) -> bool {
    std::fs::read_dir(directory)
        .into_iter()
        .flatten()
        .flatten()
        .any(|entry| {
            entry.file_type().is_ok_and(|kind| kind.is_file()) || holds_a_file(&entry.path())
        })
}

/// A node program's MCP client: this application's `node-mcp` proxy.
struct Mcp {
    input: tokio::process::ChildStdin,
    output: tokio::io::Lines<tokio::io::BufReader<tokio::process::ChildStdout>>,
    child: tokio::process::Child,
}

impl Mcp {
    async fn connect(socket: &str, token: &str) -> Self {
        use tokio::io::AsyncBufReadExt;
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_ontography"))
            .arg("node-mcp")
            .env("ONTOGRAPHY_NODE_MCP_SOCKET", socket)
            .env("ONTOGRAPHY_NODE_MCP_TOKEN", token)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut mcp = Self {
            input: child.stdin.take().unwrap(),
            output: tokio::io::BufReader::new(child.stdout.take().unwrap()).lines(),
            child,
        };
        mcp.send(json!({"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}})).await;
        mcp.reply(0).await;
        mcp.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .await;
        mcp
    }

    async fn send(&mut self, message: Value) {
        use tokio::io::AsyncWriteExt;
        let line = format!("{message}\n");
        self.input.write_all(line.as_bytes()).await.unwrap();
        self.input.flush().await.unwrap();
    }

    async fn reply(&mut self, id: i64) -> Value {
        loop {
            let line = tokio::time::timeout(Duration::from_secs(20), self.output.next_line())
                .await
                .expect("no MCP reply")
                .unwrap()
                .expect("node-mcp ended");
            let message: Value = serde_json::from_str(&line).unwrap();
            if message["id"] == id {
                return message;
            }
        }
    }

    /// Sends a tool call without waiting for its result.
    async fn start(&mut self, id: i64, tool: &str, arguments: Value) {
        self.send(json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":tool,"arguments":arguments}}))
            .await;
    }

    async fn call(&mut self, id: i64, tool: &str, arguments: Value) -> Value {
        self.start(id, tool, arguments).await;
        let reply = self.reply(id).await;
        assert_eq!(reply["result"]["isError"], false, "{reply}");
        serde_json::from_str(reply["result"]["content"][0]["text"].as_str().unwrap()).unwrap()
    }
}

#[tokio::test]
async fn a_resume_while_a_killed_programs_work_settles_starts_it_again() {
    let temporary = tempfile::tempdir().unwrap();
    // A workspace large enough that checking it out takes a while.
    let workspace = temporary.path().join("workspace");
    for index in 0..300 {
        let directory = workspace.join(format!("d{}", index % 30));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join(format!("f{index}.txt")),
            format!("{index}\n").repeat(64),
        )
        .unwrap();
    }
    // The program leaves its node tools to this test, which acts for it.
    let program = r#"printf '%s\n%s\n' "$ONTOGRAPHY_NODE_MCP_SOCKET" "$ONTOGRAPHY_NODE_MCP_TOKEN" > "$ONTOGRAPHY_NODE_DIRECTORY/mcp.tmp" && mv "$ONTOGRAPHY_NODE_DIRECTORY/mcp.tmp" "$ONTOGRAPHY_NODE_DIRECTORY/mcp"; exec sleep 60"#;
    let service = Service::new(Paths::initialize(temporary.path().join("data")).unwrap()).unwrap();
    let started = call(
        &service,
        "flow.start",
        json!({"document":program_document(json!(["/bin/sh","-c",program])),"project":temporary.path(),"workspace":workspace}),
    )
    .await;
    let run = started["run_id"].as_str().unwrap().to_owned();
    let running = session(&service, &run, |session| {
        session["state"] == "running" && session["terminal"]["pid"].is_i64()
    })
    .await;
    let supervisor = running["terminal"]["pid"].as_i64().unwrap() as i32;
    let node = Path::new(running["directory"].as_str().unwrap()).to_owned();
    let endpoint = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Ok(text) = std::fs::read_to_string(node.join("mcp")) {
                return text;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the program did not start");
    let (socket, token) = endpoint.trim_end().split_once('\n').unwrap();
    let mut mcp = Mcp::connect(socket, token).await;
    let next = mcp.call(1, "next_trigger", json!({})).await;
    let begun = mcp
        .call(2, "begin_invocation", json!({"task_id":next["task_id"]}))
        .await;
    // A checkout, once begun, settles even if its program dies.
    mcp.start(
        3,
        "open_workspace",
        json!({"attempt_id":begun["attempt_id"],"handle":begun["inputs"][0]["handle"]}),
    )
    .await;
    let checkouts = node.join("tool-workspaces/checkouts");
    tokio::time::timeout(Duration::from_secs(20), async {
        while !holds_a_file(&checkouts) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the checkout did not begin");
    // The program dies, killed from outside with its supervisor and the
    // supervisor's lifetime watcher; its proxy goes with it.
    for (pid, parent) in processes() {
        if pid == supervisor || parent == supervisor {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(pid),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
    }
    mcp.child.kill().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The checkout still settles, so the node has not yet stopped; the
    // resume must not pass it over.
    call(&service, "flow.resume", json!({"run_id":run})).await;
    let resumed = session(&service, &run, |session| {
        session["state"] == "running"
            && session["terminal"]["pid"].is_i64()
            && session["terminal"]["pid"] != supervisor
    })
    .await;
    assert!(resumed["error"].is_null(), "{resumed}");
    service.shutdown().await.unwrap();
}
