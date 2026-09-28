use super::*;
use ontography_app::{
    protocol,
    terminal::{Attachment, ServerFrame},
};
use tokio::{io::BufReader, net::UnixStream};

fn document() -> Value {
    json!({"name":"panes","entry":"a-worker","nodes":[
        {"id":"a-worker","component":"agent","config":{"prompt":"Test pane attachment","argv":["/bin/sh","-c",
            "stty -echo; printf 'worker ready\\n'; while IFS= read -r line; do printf '%s\\n' \"$line\" >> input; [ \"$line\" = quit ] && exit; printf 'reply:%s\\n' \"$line\"; done"]}},
        {"id":"z-inbox","component":"inbox"}],"edges":[{"from":"a-worker","to":"z-inbox"}]})
}

async fn worker(client: &Client, predicate: impl Fn(&Value) -> bool) -> Value {
    let mut last = Value::Null;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let status = client.call("flow.status", json!({})).await.unwrap();
            let node = status["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|node| node["id"] == "a-worker")
                .unwrap();
            if predicate(&node["session"]) {
                return node["session"].clone();
            }
            last = node["session"].clone();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Worker state did not converge: {last}"))
}

async fn screen(terminal: &mut TerminalClient, text: &str) {
    let mut contents = String::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            terminal.still_running();
            contents = terminal.screen.lock().unwrap().screen().contents();
            if contents.contains(text) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Missing {text:?}: {contents}"));
}

fn send_fresh(terminal: &mut TerminalClient, bytes: &[u8]) {
    terminal.output.lock().unwrap().clear();
    terminal.send(bytes);
}

async fn enter(terminal: &mut TerminalClient, client: &Client) {
    send_fresh(terminal, b"\r");
    worker(client, |node| node["terminal"]["attached"] == true).await;
    screen(terminal, "worker ready").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graph_enters_existing_worker_and_returns_without_restarting_it() {
    let fixture = Fixture::new();
    let mut terminal = TerminalClient::start(&fixture, &[]);
    terminal.ready_before_selection().await;
    let session = fixture.selected().await;
    let id = session["session_id"].as_str().unwrap();
    let manager = fixture.mode(id, "pi").await;
    let client = Client::connect(&fixture.paths.socket)
        .await
        .unwrap()
        .for_session(id);
    client
        .call(
            "flow.start",
            json!({"document":document(),"message":"begin"}),
        )
        .await
        .unwrap();
    let original = worker(&client, |node| node["state"] == "running").await;
    let input = std::path::Path::new(original["cwd"].as_str().unwrap()).join("input");

    terminal.send(b"\x02g");
    screen(&mut terminal, "Node a-worker").await;
    enter(&mut terminal, &client).await;
    terminal.send(b"first\r");
    screen(&mut terminal, "reply:first").await;
    assert_eq!(std::fs::read_to_string(&input).unwrap(), "first\n");
    terminal
        .screen
        .lock()
        .unwrap()
        .screen_mut()
        .set_size(35, 120);
    terminal
        ._master
        .resize(PtySize {
            rows: 35,
            cols: 120,
            pixel_width: 0,
            pixel_height: 0,
        })
        .unwrap();
    worker(&client, |node| {
        node["terminal"]["rows"] == 35 && node["terminal"]["cols"] == 120
    })
    .await;

    for shortcut in [b"\x02d", b"\x02g"] {
        send_fresh(&mut terminal, shortcut);
        screen(&mut terminal, "Returned from a-worker").await;
        let detached = worker(&client, |node| node["terminal"]["attached"] == false).await;
        assert_eq!(
            detached["terminal"]["terminal_id"],
            original["terminal"]["terminal_id"]
        );
        assert_eq!(detached["terminal"]["pid"], original["terminal"]["pid"]);
        enter(&mut terminal, &client).await;
    }

    // Parent detach cancels a nested worker pane, releasing both controlling
    // sockets. Neither process belongs to the client and both keep running.
    fixture.cli(&["detach", id]).await;
    terminal.exited().await;
    worker(&client, |node| node["terminal"]["attached"] == false).await;
    assert_eq!(fixture.terminal(id).await["pid"], manager["pid"]);

    let mut terminal = TerminalClient::start(&fixture, &["attach", id, "--ui"]);
    screen(&mut terminal, "Node a-worker").await;
    send_fresh(&mut terminal, b"\t\r");
    screen(&mut terminal, "has no interactive terminal").await;
    send_fresh(&mut terminal, b"\x1b[A");
    enter(&mut terminal, &client).await;
    terminal.send(b"second\r");
    screen(&mut terminal, "reply:second").await;
    assert_eq!(std::fs::read_to_string(&input).unwrap(), "first\nsecond\n");
    let current = worker(&client, |node| node["terminal"]["attached"] == true).await;
    assert_eq!(
        current["terminal"]["terminal_id"],
        original["terminal"]["terminal_id"]
    );

    send_fresh(&mut terminal, b"quit\r");
    screen(&mut terminal, "Returned from a-worker").await;
    worker(&client, |node| node["state"] == "exited").await;
    send_fresh(&mut terminal, b"\r");
    screen(&mut terminal, "has no running terminal").await;
    send_fresh(&mut terminal, b"q");
    screen(&mut terminal, "manager ready").await;
    terminal.send(b"manager-again\r");
    screen(&mut terminal, "manager-again").await;
    terminal.send(b"\x02d");
    terminal.exited().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_lookup_is_session_scoped_and_stale_attachments_cannot_enter_replacements() {
    let fixture = Fixture::new();
    let session = fixture.cli(&["new", "first", "--no-attach"]).await;
    let id = session["session_id"].as_str().unwrap();
    let other = fixture.cli(&["new", "other", "--no-attach"]).await;
    let client = Client::connect(&fixture.paths.socket)
        .await
        .unwrap()
        .for_session(id);
    assert_eq!(
        client
            .call("terminal.node", json!({"node":"a-worker"}))
            .await
            .unwrap_err()
            .code,
        "graph_uninitialized"
    );
    client
        .call(
            "flow.start",
            json!({"document":document(),"message":"begin"}),
        )
        .await
        .unwrap();
    worker(&client, |node| node["state"] == "running").await;
    assert_eq!(
        client
            .call(
                "terminal.node",
                json!({"session_id":other["session_id"],"node":"a-worker"})
            )
            .await
            .unwrap_err()
            .code,
        "session_scope"
    );
    assert_eq!(
        client
            .call("terminal.node", json!({"node":"unknown"}))
            .await
            .unwrap_err()
            .code,
        "node_not_found"
    );
    assert_eq!(
        client
            .call("terminal.node", json!({"node":"z-inbox"}))
            .await
            .unwrap_err()
            .code,
        "node_no_terminal"
    );
    let old: Attachment = serde_json::from_value(
        client
            .call("terminal.node", json!({"node":"a-worker"}))
            .await
            .unwrap(),
    )
    .unwrap();
    let mut changed = document();
    // A new argument ($0 of the script) changes the program, so it restarts.
    changed["nodes"][0]["config"]["argv"]
        .as_array_mut()
        .unwrap()
        .push(json!("replacement"));
    let plan = client
        .call("flow.edit", json!({"document":changed}))
        .await
        .unwrap();
    client
        .call("flow.commit", json!({"plan_id":plan["plan_id"]}))
        .await
        .unwrap();
    worker(&client, |node| {
        node["state"] == "running" && node["terminal"]["terminal_id"] != old.request.terminal_id
    })
    .await;
    let current: Attachment = serde_json::from_value(
        client
            .call("terminal.node", json!({"node":"a-worker"}))
            .await
            .unwrap(),
    )
    .unwrap();
    let mut socket = UnixStream::connect(&current.socket).await.unwrap();
    protocol::write_frame(&mut socket, &old.request)
        .await
        .unwrap();
    let response: ServerFrame = serde_json::from_slice(
        &protocol::read_frame(&mut BufReader::new(socket))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert!(matches!(response, ServerFrame::Error{error} if error.code == "stale_terminal"));
    fixture.cli(&["suspend", id]).await;
    assert_eq!(
        client
            .call("terminal.node", json!({"node":"a-worker"}))
            .await
            .unwrap_err()
            .code,
        "session_inactive"
    );
}
