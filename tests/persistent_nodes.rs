use ontography_app::{
    node_runtime::NodeRuntime,
    persistence::Paths,
    state::Service,
    tools,
    workflow::{runtime, tasks::RetryLedger},
};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, time::Duration};

fn document() -> Value {
    json!({"name":"persistent", "entry":"worker", "nodes":[
        {"id":"worker", "component":"agent", "config":{"prompt":"Work together", "argv":["/bin/cat"]}},
        {"id":"archive", "component":"inbox"}
    ], "edges":[{"from":"worker", "to":"archive"}]})
}

async fn call(service: &Service, operation: &str, args: Value) -> Value {
    tools::dispatch(service, operation, &args)
        .await
        .unwrap_or_else(|error| panic!("{operation}: {error}; {:?}", error.details))
}

async fn start(service: &Service, project: &Path, document: Value) -> String {
    call(
        service,
        "flow.start",
        json!({"document":document,"project":project,"message":"begin"}),
    )
    .await["run_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn node<'a>(status: &'a Value, id: &str) -> &'a Value {
    status["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["id"] == id)
        .unwrap()
}

async fn wait_status(service: &Service, run: &str, condition: impl Fn(&Value) -> bool) -> Value {
    let mut last = Value::Null;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = call(service, "flow.status", json!({"run_id":run})).await;
            if condition(&status) {
                return status;
            }
            last = status;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("node did not reach expected state; last status: {last}"))
}

async fn running(service: &Service, run: &str) -> Value {
    wait_status(service, run, |status| {
        node(status, "worker")["session"]["state"] == "running"
    })
    .await
}

async fn resources(
    service: &Service,
    run_id: &str,
) -> (String, Arc<NodeRuntime>, Arc<RetryLedger>) {
    let run = service.run(run_id).await.unwrap();
    let run = run.lock().await;
    let id = runtime::load(&run).unwrap().identities.nodes["worker"].clone();
    let worker = &run.live().unwrap().workers[&id];
    (
        id,
        worker.node.as_ref().unwrap().clone(),
        worker.ledger.clone(),
    )
}

async fn edit(service: &Service, run: &str, document: Value) {
    let preview = call(
        service,
        "flow.edit",
        json!({"run_id":run,"document":document}),
    )
    .await;
    call(
        service,
        "flow.commit",
        json!({"run_id":run,"plan_id":preview["plan_id"]}),
    )
    .await;
}

#[tokio::test]
async fn agent_keeps_initial_work_pending_and_resumes_with_its_identity_and_workspace() {
    let temporary = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(temporary.path().join("data")).unwrap()).unwrap();
    let run_id = start(&service, temporary.path(), document()).await;
    let started = running(&service, &run_id).await;
    let (identity, worker, _) = resources(&service, &run_id).await;
    let terminal = worker.terminal().unwrap();
    let context = worker.tools().unwrap();
    assert!(terminal.status().running);
    let inspect = context
        .call("inspect_node", json!({}))
        .await
        .unwrap()
        .value()
        .unwrap();
    assert_eq!(inspect["initial_pending"], true);
    {
        let run = service.run(&run_id).await.unwrap();
        let run = run.lock().await;
        assert!(runtime::initial_pending(&run).await.unwrap());
        assert!(
            run.live()
                .unwrap()
                .session
                .invocations_page(Some(&identity), None, 10)
                .await
                .unwrap()
                .is_empty()
        );
    }
    let cwd =
        std::path::PathBuf::from(node(&started, "worker")["session"]["cwd"].as_str().unwrap());
    assert_ne!(
        cwd.canonicalize().unwrap(),
        temporary.path().canonicalize().unwrap(),
        "an agent's workspace must be separate from the project directory"
    );
    std::fs::write(cwd.join("kept.txt"), "persistent work").unwrap();
    call(&service, "run.suspend", json!({"run_id":run_id})).await;
    assert!(!terminal.status().running);
    // The retired context must not start new attempts after its process stops.
    let error = context
        .call("begin_invocation", json!({}))
        .await
        .unwrap_err();
    assert_eq!(error.code, "execution_stopping");
    // Scoped contexts retain core session handles. Release those stopped
    // owners before reopening the run's persistent store; terminal views can
    // remain attached to the old execution without retaining that store.
    drop(context);
    drop(worker);
    call(&service, "flow.resume", json!({"run_id":run_id})).await;
    let resumed = running(&service, &run_id).await;
    let (resumed_identity, resumed_worker, _) = resources(&service, &run_id).await;
    assert_eq!(identity, resumed_identity);
    assert_ne!(terminal.id(), resumed_worker.terminal().unwrap().id());
    assert_eq!(
        node(&started, "worker")["session"]["cwd"],
        node(&resumed, "worker")["session"]["cwd"]
    );
    assert_eq!(
        std::fs::read_to_string(cwd.join("kept.txt")).unwrap(),
        "persistent work"
    );
    assert_eq!(
        resumed_worker
            .tools()
            .unwrap()
            .call("inspect_node", json!({}))
            .await
            .unwrap()
            .value()
            .unwrap()["initial_pending"],
        true
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn graph_and_grant_edits_refresh_tools_without_restarting_the_session() {
    let temporary = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(temporary.path().join("data")).unwrap()).unwrap();
    let run_id = start(&service, temporary.path(), document()).await;
    let status = running(&service, &run_id).await;
    let (_, worker, ledger) = resources(&service, &run_id).await;
    let terminal = worker.terminal().unwrap();
    let context = worker.tools().unwrap();
    let mut changed = status["document"].clone();
    changed["nodes"]
        .as_array_mut()
        .unwrap()
        .push(json!({"id":"new-recipient", "component":"inbox"}));
    changed["edges"]
        .as_array_mut()
        .unwrap()
        .push(json!({"from":"worker", "to":"new-recipient"}));
    edit(&service, &run_id, changed.clone()).await;
    let graph = context
        .call("inspect_graph", json!({}))
        .await
        .unwrap()
        .value()
        .unwrap();
    assert!(
        graph["nodes"]
            .as_array()
            .unwrap()
            .contains(&json!("new-recipient")),
        "scope must refresh even when the agent definition does not change: {graph}"
    );
    let inspect = context
        .call("inspect_node", json!({}))
        .await
        .unwrap()
        .value()
        .unwrap();
    assert!(
        inspect["outgoing"]
            .as_array()
            .unwrap()
            .contains(&json!("new-recipient"))
    );
    changed["nodes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["id"] == "worker")
        .unwrap()["grants"] = json!(["originate", "retire"]);
    edit(&service, &run_id, changed).await;
    assert!(
        context
            .catalog()
            .iter()
            .any(|tool| tool.name == "retire_package")
    );
    let (_, current, current_ledger) = resources(&service, &run_id).await;
    assert!(Arc::ptr_eq(&worker, &current));
    assert!(Arc::ptr_eq(&ledger, &current_ledger));
    assert_eq!(terminal.id(), current.terminal().unwrap().id());
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn config_restarts_and_removal_stop_the_old_process_before_replacement() {
    let temporary = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(temporary.path().join("data")).unwrap()).unwrap();
    let mut definition = document();
    // Keep initial input at a separate entry so the worker can be removed.
    definition["entry"] = json!("archive");
    let run_id = start(&service, temporary.path(), definition).await;
    let status = running(&service, &run_id).await;
    let (identity, original, ledger) = resources(&service, &run_id).await;
    let original_terminal = original.terminal().unwrap();
    let mut changed = status["document"].clone();
    // The program is all a session with argv runs, so only argv restarts it.
    changed["nodes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["id"] == "worker")
        .unwrap()["config"]["argv"] = json!(["/bin/cat", "-u"]);
    edit(&service, &run_id, changed.clone()).await;
    running(&service, &run_id).await;
    let (current_identity, current, current_ledger) = resources(&service, &run_id).await;
    let current_terminal = current.terminal().unwrap();
    assert_eq!(identity, current_identity);
    assert!(!original_terminal.status().running);
    assert_ne!(original_terminal.id(), current_terminal.id());
    assert!(Arc::ptr_eq(&ledger, &current_ledger));
    // A different node type is a different core node: the edit replaces it.
    let replacement = changed["nodes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["id"] == "worker")
        .unwrap();
    replacement["component"] = json!("human");
    replacement["config"] = json!({"prompt":"Manual work"});
    edit(&service, &run_id, changed.clone()).await;
    assert!(!current_terminal.status().running);
    let run = service.run(&run_id).await.unwrap();
    let human = {
        let run = run.lock().await;
        let human = runtime::load(&run).unwrap().identities.nodes["worker"].clone();
        assert_ne!(human, identity);
        let workers = &run.live().unwrap().workers;
        assert!(!workers.contains_key(&identity));
        assert!(workers[&human].node.is_none());
        human
    };
    let replacement = changed["nodes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["id"] == "worker")
        .unwrap();
    replacement["component"] = json!("agent");
    replacement["config"] = json!({"prompt":"Restored session", "argv":["/bin/cat"]});
    edit(&service, &run_id, changed.clone()).await;
    running(&service, &run_id).await;
    let (restored_identity, restored, _) = resources(&service, &run_id).await;
    assert_ne!(restored_identity, human);
    let restored_terminal = restored.terminal().unwrap();
    changed["nodes"]
        .as_array_mut()
        .unwrap()
        .retain(|node| node["id"] != "worker");
    changed["edges"] = json!([]);
    edit(&service, &run_id, changed).await;
    assert!(!restored_terminal.status().running);
    assert!(
        !run.lock()
            .await
            .live()
            .unwrap()
            .workers
            .contains_key(&restored_identity)
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn hard_abort_stops_the_process_even_with_retained_terminal_and_tools() {
    let temporary = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(temporary.path().join("data")).unwrap()).unwrap();
    let run_id = start(&service, temporary.path(), document()).await;
    running(&service, &run_id).await;
    let (identity, worker, _) = resources(&service, &run_id).await;
    let terminal = worker.terminal().unwrap();
    let context = worker.tools().unwrap();
    let pid = nix::unistd::Pid::from_raw(terminal.status().pid.unwrap() as i32);
    let handle = {
        let run = service.run(&run_id).await.unwrap();
        let run = run.lock().await;
        let live = run.live().unwrap();
        live.executions[&live.workers[&identity].execution_id].clone()
    };
    handle.abort();
    assert_eq!(handle.wait().await, ontography::ExecutionStatus::Aborted);
    assert!(!terminal.status().running);
    assert_eq!(
        context
            .call("begin_invocation", json!({}))
            .await
            .unwrap_err()
            .code,
        "execution_stopping"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(
                nix::sys::signal::killpg(pid, None),
                Err(nix::errno::Errno::ESRCH)
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("aborted execution must leave no running process group");
    call(&service, "flow.resume", json!({"run_id":run_id})).await;
    running(&service, &run_id).await;
    let (resumed_id, resumed, _) = resources(&service, &run_id).await;
    assert_eq!(identity, resumed_id);
    assert_ne!(terminal.id(), resumed.terminal().unwrap().id());
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn successful_exit_and_missing_program_are_reported_without_accepting_input() {
    let temporary = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(temporary.path().join("data")).unwrap()).unwrap();
    for (program, state, code) in [
        ("/usr/bin/true", "exited", 0),
        ("/no/such/ontography-agent", "failed", 127),
    ] {
        let mut definition = document();
        definition["nodes"][0]["config"]["argv"] = json!([program]);
        let run_id = start(&service, temporary.path(), definition).await;
        let status = wait_status(&service, &run_id, |status| {
            node(status, "worker")["execution"]["state"] == state
        })
        .await;
        assert_eq!(node(&status, "worker")["session"]["state"], state);
        assert_eq!(node(&status, "worker")["session"]["exit_code"], code);
        let run = service.run(&run_id).await.unwrap();
        assert!(runtime::initial_pending(&*run.lock().await).await.unwrap());
    }
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn process_exit_is_visible_and_resume_retries_without_consuming_work() {
    let temporary = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(temporary.path().join("data")).unwrap()).unwrap();
    let mut definition = document();
    definition["nodes"][0]["config"]["argv"] = json!(["/bin/sh", "-c", "exit 7"]);
    let run_id = start(&service, temporary.path(), definition).await;
    let failed = wait_status(&service, &run_id, |status| {
        node(status, "worker")["execution"]["state"] == "failed"
    })
    .await;
    let (identity, worker, ledger) = resources(&service, &run_id).await;
    assert_eq!(node(&failed, "worker")["session"]["state"], "failed");
    call(&service, "flow.resume", json!({"run_id":run_id})).await;
    wait_status(&service, &run_id, |status| {
        node(status, "worker")["execution"]["state"] == "failed"
    })
    .await;
    let (resumed_identity, resumed, resumed_ledger) = resources(&service, &run_id).await;
    assert_eq!(identity, resumed_identity);
    assert!(!Arc::ptr_eq(&worker, &resumed));
    assert!(Arc::ptr_eq(&ledger, &resumed_ledger));
    let run = service.run(&run_id).await.unwrap();
    assert!(runtime::initial_pending(&*run.lock().await).await.unwrap());
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn runtime_provisions_mcp_refreshes_selection_and_revokes_it_on_suspend() {
    use ontography_app::protocol;
    use std::process::Stdio;
    use tokio::io::BufReader;
    async fn read(reader: &mut (impl tokio::io::AsyncBufRead + Unpin)) -> Value {
        let bytes = tokio::time::timeout(Duration::from_secs(5), protocol::read_frame(reader))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }
    let temporary = tempfile::tempdir().unwrap();
    let service = Service::new(Paths::initialize(temporary.path().join("data")).unwrap()).unwrap();
    let mut definition = document();
    definition["nodes"][0]["tools"] = json!(["inspect_node"]);
    definition["nodes"][0]["config"]["argv"] = json!([
        "/bin/sh",
        "-c",
        "umask 077; printf '%s\n%s\n' \"$ONTOGRAPHY_NODE_MCP_SOCKET\" \"$ONTOGRAPHY_NODE_MCP_TOKEN\" > mcp-endpoint; exec /bin/cat"
    ]);
    let run_id = start(&service, temporary.path(), definition.clone()).await;
    let status = running(&service, &run_id).await;
    let (_, worker, _) = resources(&service, &run_id).await;
    let terminal_id = worker.terminal().unwrap().id().to_owned();
    let path =
        Path::new(node(&status, "worker")["session"]["cwd"].as_str().unwrap()).join("mcp-endpoint");
    let endpoint = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(value) = std::fs::read_to_string(&path)
                && value.lines().count() == 2
            {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut lines = endpoint.lines();
    let socket = lines.next().unwrap();
    let token = lines.next().unwrap();
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
    let mut input = child.stdin.take().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    protocol::write_frame(&mut input, &json!({"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}})).await.unwrap();
    assert_eq!(
        read(&mut output).await["result"]["serverInfo"]["name"],
        "ontography-node"
    );
    protocol::write_frame(
        &mut input,
        &json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
    )
    .await
    .unwrap();
    protocol::write_frame(
        &mut input,
        &json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}),
    )
    .await
    .unwrap();
    assert_eq!(
        read(&mut output).await["result"]["tools"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    definition["nodes"][0]["tools"] = json!(["inspect_node", "inspect_graph"]);
    edit(&service, &run_id, definition).await;
    assert_eq!(
        read(&mut output).await["method"],
        "notifications/tools/list_changed"
    );
    protocol::write_frame(
        &mut input,
        &json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
    )
    .await
    .unwrap();
    assert_eq!(
        read(&mut output).await["result"]["tools"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(worker.terminal().unwrap().id(), terminal_id);
    call(&service, "run.suspend", json!({"run_id":run_id})).await;
    assert!(!Path::new(socket).exists());
    assert!(
        !tokio::time::timeout(Duration::from_secs(3), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    service.shutdown().await.unwrap();
}
