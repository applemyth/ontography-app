//! Node tools hosted in a real core execution, as a transport would host them.

mod mcp;

use super::{NodeScope, NodeToolContext};
use crate::workflow::{
    Document, Grant, IdentityMap, WorkflowPayload, edge_key,
    edit::WorkflowState,
    expand,
    tasks::{RetryLedger, TaskKey},
};
use crate::workspace::WorkspaceStore;
use ontography::{
    ActivationProposal, ContentDigest, ContentId, Emission, ExecutionContext, ExecutionHandle,
    ExecutionHost, InvocationId, InvocationStatus, OutputAuthority, PackageEnvelope, PackageStore,
    ProposalDecision, ProposalRuntime, ReceiptState, ResolvedEntryKind, SessionHandle,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, str::FromStr, sync::Arc};
use tokio::sync::{mpsc, watch};

/// `source` feeds `worker`, which feeds `sink`. Tools run at `worker`, or at
/// the entry when a test needs to originate work.
pub(super) fn document(worker: Value) -> Document {
    let mut worker = worker;
    worker["id"] = json!("worker");
    worker["kind"] = json!("agent");
    if worker.get("config").is_none() {
        worker["config"] = json!({"prompt": "Work"});
    }
    serde_json::from_value(json!({
        "name": "tools", "entry": "source",
        "nodes": [{"id": "source", "kind": "inbox"}, worker, {"id": "sink", "kind": "inbox"}],
        "edges": [{"from": "source", "to": "worker"}, {"from": "worker", "to": "sink"}],
    }))
    .unwrap()
}

/// A single `worker` entry feeding `sink`, for originating work.
fn entry_document(grants: Value) -> Document {
    serde_json::from_value(json!({
        "name": "entry", "entry": "worker",
        "nodes": [{"id": "worker", "kind": "agent", "config": {"prompt": "Start"}, "grants": grants},
            {"id": "sink", "kind": "inbox"}],
        "edges": [{"from": "worker", "to": "sink"}],
    }))
    .unwrap()
}

pub(super) struct Fixture {
    pub directory: tempfile::TempDir,
    pub runtime: ProposalRuntime,
    pub session: SessionHandle,
    pub identities: IdentityMap,
    pub tools: Arc<NodeToolContext>,
    pub ledger: Arc<RetryLedger>,
    pub scope: watch::Sender<NodeScope>,
    execution: ExecutionHandle,
    _host: ExecutionHost,
}

impl Fixture {
    /// Hosts tools at `node`, offering `initial` as the run's initial input.
    pub async fn new(document: Document, node: &str, initial: Option<&str>) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let identities = IdentityMap::fresh(&document);
        let compiled = expand(&document, "tools", &identities)
            .unwrap()
            .compile()
            .unwrap();
        let runtime = ProposalRuntime::with_grammar(compiled.kernel, compiled.grammar);
        let session = runtime.open().unwrap();
        let host = ExecutionHost::new(session.clone());
        let state = Arc::new(WorkflowState::new(document, identities.clone()).unwrap());
        let (scope, receiver) = watch::channel(NodeScope::new(state, node).unwrap());
        let node_directory = directory.path().join("nodes").join(node);
        std::fs::create_dir_all(&node_directory).unwrap();
        let ledger = RetryLedger::open(node_directory.join("retry.json")).unwrap();
        let initial = initial.map(message);
        let (sender, mut hosted) = mpsc::unbounded_channel();
        let launch = {
            let (session, ledger) = (session.clone(), Arc::clone(&ledger));
            move |context: ExecutionContext| {
                let (session, ledger, receiver) =
                    (session.clone(), Arc::clone(&ledger), receiver.clone());
                let (directory, initial, sender) =
                    (node_directory.clone(), initial.clone(), sender.clone());
                async move {
                    let mut stop = context.stop();
                    let tools = Arc::new(
                        NodeToolContext::new(
                            context, session, receiver, ledger, directory, initial,
                        )
                        .await
                        .unwrap(),
                    );
                    let _ = sender.send(Arc::clone(&tools));
                    stop.requested().await;
                    tools.close().await;
                    Ok(())
                }
            }
        };
        let execution = host
            .launch(identities.nodes[node].clone(), launch)
            .await
            .unwrap();
        let tools = hosted.recv().await.unwrap();
        Self {
            directory,
            runtime,
            session,
            identities,
            tools,
            ledger,
            scope,
            execution,
            _host: host,
        }
    }

    /// Calls a tool the way a transport does: send the bytes, then mark them.
    pub async fn call(&self, name: &str, args: Value) -> crate::Result<Value> {
        let reply = self.tools.call(name, args).await?;
        let value = reply.value()?;
        reply.sent().await?;
        Ok(value)
    }

    pub async fn ok(&self, name: &str, args: Value) -> Value {
        self.call(name, args)
            .await
            .unwrap_or_else(|error| panic!("{name} failed: {error} {:?}", error.details))
    }

    pub async fn error(&self, name: &str, args: Value) -> crate::AppError {
        match self.call(name, args).await {
            Ok(value) => panic!("{name} unexpectedly succeeded: {value}"),
            Err(error) => error,
        }
    }

    /// Delivers a payload from `source` to `worker`, as a finished task there would.
    async fn send(&self, payload: ontography::Payload, contents: Vec<ContentId>) {
        let source = &self.identities.nodes["source"];
        let kernel = self.session.kernel().await.unwrap();
        let mut proposal = ActivationProposal::root(
            source.as_str(),
            kernel.root_ceiling(source).unwrap().clone(),
            payload.clone(),
        );
        proposal.emit(Emission::new(
            self.identities.edges[&edge_key("source", "worker")].as_str(),
            OutputAuthority::Carry,
            payload,
        ));
        assert!(matches!(
            self.session
                .submit_with_content(proposal, contents)
                .await
                .unwrap(),
            ProposalDecision::Committed(_)
        ));
    }

    pub async fn deliver(&self, text: &str) {
        self.send(message(text), Vec::new()).await;
    }

    /// Messages waiting at `node`, in core's order.
    pub async fn pending(&self, node: &str) -> Vec<String> {
        let frontier = self
            .session
            .pending_at(self.identities.nodes[node].as_str())
            .await
            .unwrap();
        let mut messages = Vec::new();
        for (_, record) in frontier.packages() {
            let bytes = self
                .session
                .content(record.content_digest())
                .await
                .unwrap()
                .unwrap();
            messages.push(match WorkflowPayload::decode(&bytes).unwrap() {
                WorkflowPayload::Message { message } => message,
                WorkflowPayload::Workspace(_) => "<workspace>".into(),
            });
        }
        messages
    }

    pub fn grant(&self, grants: &[Grant]) {
        self.scope
            .send_modify(|scope| scope.node.grants = grants.iter().copied().collect());
    }

    pub async fn invocation_status(&self, attempt: &str) -> InvocationStatus {
        let node = self.tools.node_id().to_owned();
        let records = self
            .session
            .invocations_page(Some(&node), None, 100)
            .await
            .unwrap();
        records
            .iter()
            .find(|record| record.id.to_string() == attempt)
            .unwrap()
            .status
    }

    pub async fn stop(self) {
        self.execution.request_stop();
        self.execution.wait().await;
        self.runtime.shutdown().await;
    }
}

pub(super) fn message(text: &str) -> ontography::Payload {
    WorkflowPayload::Message {
        message: text.into(),
    }
    .encode()
    .unwrap()
}

#[tokio::test]
async fn selected_tools_limit_catalog_and_dispatch_without_adding_grants() {
    let fixture = Fixture::new(
        document(json!({"tools":["inspect_node", "retire_package"]})),
        "worker",
        None,
    )
    .await;
    assert_eq!(
        fixture
            .tools
            .catalog()
            .iter()
            .map(|tool| tool.name)
            .collect::<Vec<_>>(),
        ["inspect_node"]
    );
    let inspected = fixture.ok("inspect_node", json!({})).await;
    assert_eq!(inspected["tools"], json!(["inspect_node"]));
    assert_eq!(
        fixture.error("begin_invocation", json!({})).await.code,
        "tool_disabled"
    );
    assert_eq!(
        fixture.error("retire_package", json!({})).await.code,
        "not_granted"
    );
    assert!(
        fixture
            .session
            .invocations_page(Some(fixture.tools.node_id()), None, 10)
            .await
            .unwrap()
            .is_empty()
    );
    fixture.grant(&[Grant::Retire]);
    assert!(
        fixture
            .tools
            .catalog()
            .iter()
            .any(|tool| tool.name == "retire_package")
    );
    fixture
        .scope
        .send_modify(|scope| scope.node.tools = Some(Default::default()));
    assert!(fixture.tools.catalog().is_empty());
    assert_eq!(
        fixture.error("inspect_node", json!({})).await.code,
        "tool_disabled"
    );
    fixture.scope.send_modify(|scope| scope.node.tools = None);
    assert!(
        fixture
            .tools
            .catalog()
            .iter()
            .any(|tool| tool.name == "begin_invocation")
    );
    fixture.stop().await;
}

/// Begins an attempt at the next task.
pub(super) async fn begin(fixture: &Fixture) -> Value {
    let next = fixture.ok("next_trigger", json!({})).await;
    fixture
        .ok("begin_invocation", json!({"task_id": next["task_id"]}))
        .await
}

/// Calls a tool as `Fixture::ok` does, checking that its reply is receipted.
pub(super) async fn recorded(fixture: &Fixture, name: &str, args: Value) -> Value {
    let reply = fixture
        .tools
        .call(name, args)
        .await
        .unwrap_or_else(|error| panic!("{name} failed: {error} {:?}", error.details));
    assert!(
        reply.receipt().is_some(),
        "{name} replied without a receipt"
    );
    let value = reply.value().unwrap();
    reply.sent().await.unwrap();
    value
}

/// Delivers a directory of `files` from `source` to `worker` as a workspace.
/// Returns the view's root.
pub(super) async fn deliver_workspace(fixture: &Fixture, files: &[(&str, &str)]) -> ContentId {
    let directory = tempfile::tempdir().unwrap();
    for (path, text) in files {
        let path = directory.path().join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }
    let package = WorkspaceStore::new(
        fixture.session.content_store().await.unwrap(),
        fixture.directory.path().join("imports"),
    )
    .import_directory(directory.path())
    .await
    .unwrap();
    let payload = WorkflowPayload::Workspace(PackageEnvelope::new(package.root()))
        .encode()
        .unwrap();
    fixture.send(payload, package.dependencies()).await;
    package.root()
}

/// The text of each file in the one workspace waiting at `sink`, by path.
/// An executable's path ends in `*`, as `ls -F` marks it.
pub(super) async fn sink_files(fixture: &Fixture) -> BTreeMap<String, String> {
    let frontier = fixture
        .session
        .pending_at(fixture.identities.nodes["sink"].as_str())
        .await
        .unwrap();
    let [(_, record)] = frontier.packages() else {
        panic!("expected one package at the sink");
    };
    let payload = fixture
        .session
        .content(record.content_digest())
        .await
        .unwrap()
        .unwrap();
    let WorkflowPayload::Workspace(envelope) = WorkflowPayload::decode(&payload).unwrap() else {
        panic!("expected a workspace at the sink");
    };
    // Collecting first reads only what core retained with the publication,
    // not what an ended attempt had pinned.
    let content = fixture.session.content_store().await.unwrap();
    content.collect_garbage().await.unwrap();
    let view = PackageStore::new(content.clone())
        .resolve(envelope.ontography_package)
        .await
        .unwrap();
    let mut files = BTreeMap::new();
    for entry in view.entries() {
        if let ResolvedEntryKind::File {
            content: id,
            executable,
        } = entry.kind
        {
            let text = content.read_range(id, 0..id.size()).await.unwrap();
            files.insert(
                format!("{}{}", entry.path, if executable { "*" } else { "" }),
                String::from_utf8(text.to_vec()).unwrap(),
            );
        }
    }
    files
}

/// Expected `sink_files`, from paths and texts.
pub(super) fn files(files: &[(&str, &str)]) -> BTreeMap<String, String> {
    files
        .iter()
        .map(|(path, text)| ((*path).to_owned(), (*text).to_owned()))
        .collect()
}

#[tokio::test]
async fn every_reply_inside_an_attempt_is_the_receipted_bytes_and_marked_sent() {
    let fixture = Fixture::new(document(json!({})), "worker", None).await;
    fixture.deliver("hello").await;
    let begun = begin(&fixture).await;
    let attempt = begun["attempt_id"].as_str().unwrap().to_owned();
    let handle = begun["inputs"][0]["handle"].as_str().unwrap().to_owned();
    let mut replies = Vec::new();
    for (tool, args) in [
        (
            "describe_package",
            json!({"attempt_id": attempt, "handle": handle}),
        ),
        (
            "read_package",
            json!({"attempt_id": attempt, "handle": handle}),
        ),
    ] {
        let reply = fixture.tools.call(tool, args).await.unwrap();
        let (id, sequence) = reply.receipt().expect("attempt replies are receipted");
        replies.push((id, sequence, ContentDigest::compute(reply.bytes())));
        if tool == "read_package" {
            assert!(String::from_utf8_lossy(reply.bytes()).contains("hello"));
        }
        reply.sent().await.unwrap();
    }
    let unsent = fixture
        .tools
        .call(
            "describe_package",
            json!({"attempt_id": attempt, "handle": handle}),
        )
        .await
        .unwrap();
    let (_, unsent_sequence) = unsent.receipt().unwrap();
    drop(unsent);
    let events = fixture
        .session
        .invocation_events(InvocationId::from_str(&attempt).unwrap(), 0, 100)
        .await
        .unwrap();
    for (id, sequence, digest) in replies {
        assert_eq!(id.to_string(), attempt);
        let prepared = events
            .iter()
            .find(|event| event.sequence == sequence)
            .expect("the reply's receipt exists");
        assert_eq!(prepared.content_digest, digest, "{}", prepared.operation);
        assert!(
            events.iter().any(
                |event| event.receipt_sequence == sequence && event.state == ReceiptState::Sent
            )
        );
    }
    assert!(!events.iter().any(
        |event| event.receipt_sequence == unsent_sequence && event.state == ReceiptState::Sent
    ));
    // The begin reply itself was recorded before it was returned.
    assert!(
        events
            .iter()
            .any(|event| event.operation == "tool_response")
    );
    fixture.stop().await;
}

#[tokio::test]
async fn nothing_outside_an_attempt_reveals_a_payload() {
    let fixture = Fixture::new(document(json!({})), "worker", None).await;
    fixture.deliver("secret text").await;
    for tool in [
        "inspect_node",
        "inspect_graph",
        "list_inputs",
        "next_trigger",
    ] {
        let reply = fixture.tools.call(tool, json!({})).await.unwrap();
        assert!(reply.receipt().is_none(), "{tool}");
        assert!(
            !String::from_utf8_lossy(reply.bytes()).contains("secret"),
            "{tool} leaked a payload"
        );
    }
    let node = fixture.ok("inspect_node", json!({})).await;
    assert_eq!(node["node"], "worker");
    assert_eq!(node["incoming"], json!(["source"]));
    assert_eq!(node["outgoing"], json!(["sink"]));
    assert_eq!(node["open_attempts"], json!([]));
    let graph = fixture.ok("inspect_graph", json!({})).await;
    assert!(
        graph["edges"]
            .as_array()
            .unwrap()
            .contains(&json!({"from":"worker","to":"sink"}))
    );
    let inputs = fixture.ok("list_inputs", json!({})).await;
    assert_eq!(inputs["inputs"][0]["from"], "source");
    assert_eq!(inputs["inputs"][0]["state"], "ready");
    let next = fixture.ok("next_trigger", json!({})).await;
    assert_eq!(next["task_id"], inputs["inputs"][0]["task_id"]);
    assert_eq!(next["inputs"][0]["work_id"], inputs["inputs"][0]["work_id"]);
    fixture.stop().await;
}

#[tokio::test]
async fn pages_continue_after_their_listed_inputs_are_processed() {
    let fixture = Fixture::new(document(json!({})), "worker", None).await;
    for text in ["one", "two", "three"] {
        fixture.deliver(text).await;
    }
    let first = fixture.ok("list_inputs", json!({"limit": 1})).await;
    let begun = fixture
        .ok(
            "begin_invocation",
            json!({"task_id": first["inputs"][0]["task_id"]}),
        )
        .await;
    fixture
        .ok(
            "submit_invocation",
            json!({"attempt_id": begun["attempt_id"], "result": {"message": "done"}}),
        )
        .await;
    // The cursor outlives the input it follows.
    let second = fixture
        .ok(
            "list_inputs",
            json!({"after": first["next_after"], "limit": 5}),
        )
        .await;
    assert_eq!(second["inputs"].as_array().unwrap().len(), 2);
    assert!(second["next_after"].is_null());
    assert_eq!(
        fixture
            .error("list_inputs", json!({"after": "page_999"}))
            .await
            .code,
        "stale_cursor"
    );
    fixture.stop().await;
}

#[tokio::test]
async fn submitting_routes_results_and_ends_the_attempt() {
    let fixture = Fixture::new(document(json!({})), "worker", None).await;
    fixture.deliver("one").await;
    fixture.deliver("two").await;
    let first = begin(&fixture).await;
    let second = begin(&fixture).await;
    let accepted = fixture
        .ok(
            "submit_invocation",
            json!({"attempt_id": first["attempt_id"], "result": {"message": "done one"}}),
        )
        .await;
    assert_eq!(
        accepted,
        json!({"status": "accepted", "attempt_id": first["attempt_id"]})
    );
    assert_eq!(fixture.pending("sink").await, ["done one"]);
    let routed = fixture
        .ok(
            "submit_invocation",
            json!({"attempt_id": second["attempt_id"], "result": {"message": "summary"},
                "outputs": [{"message": "for the sink", "to": "sink"}]}),
        )
        .await;
    assert_eq!(routed["status"], "accepted");
    let mut sink = fixture.pending("sink").await;
    sink.sort();
    assert_eq!(sink, ["done one", "for the sink"]);
    assert!(fixture.pending("worker").await.is_empty());
    let ended = fixture
        .error(
            "submit_invocation",
            json!({"attempt_id": first["attempt_id"], "result": {"message": "again"}}),
        )
        .await;
    assert_eq!(ended.code, "attempt_ended");
    assert_eq!(ended.details.unwrap()["status"], "accepted");
    let cached: Value =
        crate::persistence::read_json(&fixture.directory.path().join("nodes/worker/output.json"))
            .unwrap();
    assert_eq!(cached["publication_status"], "committed");
    fixture.stop().await;
}

#[tokio::test]
async fn invalid_submissions_keep_the_attempt_open() {
    let fixture = Fixture::new(document(json!({})), "worker", None).await;
    fixture.deliver("input").await;
    let begun = begin(&fixture).await;
    for (args, code) in [
        (
            json!({"result": {"message": "x"}, "outputs": [{"message": "x", "to": "nowhere"}]}),
            "invalid_arguments",
        ),
        (
            json!({"result": {"message": "x", "workspace": "ws"}}),
            "invalid_arguments",
        ),
        (
            json!({"result": {"workspace": "no such handle"}}),
            "unknown_handle",
        ),
        (
            json!({"result": {"message": "x"}, "outputs": [{"message": "x", "outbound": true}]}),
            "not_granted",
        ),
    ] {
        let mut args = args;
        args["attempt_id"] = begun["attempt_id"].clone();
        assert_eq!(fixture.error("submit_invocation", args).await.code, code);
    }
    let accepted = fixture
        .ok(
            "submit_invocation",
            json!({"attempt_id": begun["attempt_id"], "result": {"message": "fine"}}),
        )
        .await;
    assert_eq!(accepted["status"], "accepted");
    fixture.stop().await;
}

#[tokio::test(start_paused = true)]
async fn failed_tasks_wait_park_and_never_block_other_inputs() {
    let fixture = Fixture::new(
        document(json!({"retry": {"max_attempts": 2, "initial_delay_secs": 30}})),
        "worker",
        None,
    )
    .await;
    fixture.deliver("first").await;
    fixture.deliver("second").await;
    let first = begin(&fixture).await;
    let first_task = first["task_id"].clone();
    let retrying = fixture
        .ok(
            "fail_invocation",
            json!({"attempt_id": first["attempt_id"], "reason": "flaky"}),
        )
        .await;
    assert_eq!(retrying["retry"]["state"], "retrying");
    assert_eq!(retrying["retry"]["retry_in_secs"], 30);
    assert_eq!(
        fixture
            .invocation_status(first["attempt_id"].as_str().unwrap())
            .await,
        InvocationStatus::Failed
    );
    // The waiting task is skipped, not retried at once and not blocking.
    let other = fixture.ok("next_trigger", json!({})).await;
    assert_ne!(other["task_id"], first_task);
    let waiting = fixture
        .error("begin_invocation", json!({"task_id": first_task}))
        .await;
    assert_eq!(waiting.code, "task_retrying");
    let second = fixture
        .ok("begin_invocation", json!({"task_id": other["task_id"]}))
        .await;
    assert_eq!(
        fixture.ok("next_trigger", json!({})).await["retry_in_secs"],
        30
    );
    fixture
        .ok(
            "submit_invocation",
            json!({"attempt_id": second["attempt_id"], "result": {"message": "ok"}}),
        )
        .await;
    tokio::time::advance(std::time::Duration::from_secs(31)).await;
    let again = begin(&fixture).await;
    assert_eq!(again["task_id"], first_task);
    let parked = fixture
        .ok(
            "fail_invocation",
            json!({"attempt_id": again["attempt_id"], "reason": "still flaky"}),
        )
        .await;
    assert_eq!(parked["retry"], json!({"state": "parked", "attempts": 2}));
    assert!(fixture.ok("next_trigger", json!({})).await["task_id"].is_null());
    let listed = fixture.ok("list_inputs", json!({})).await;
    assert_eq!(listed["inputs"][0]["state"], "parked");
    // The ledger is the manager's view as well.
    let key = TaskKey::parse(first_task.as_str().unwrap()).unwrap();
    assert!(fixture.ledger.get(&key).unwrap().parked);
    assert!(fixture.ledger.clear(&key).unwrap().is_some());
    assert_eq!(
        fixture.ok("next_trigger", json!({})).await["task_id"],
        first_task
    );
    fixture.stop().await;
}

#[tokio::test]
async fn a_failure_not_worth_retrying_parks_at_once_and_one_task_has_one_attempt() {
    let fixture = Fixture::new(document(json!({})), "worker", None).await;
    fixture.deliver("bad").await;
    let begun = begin(&fixture).await;
    let duplicate = fixture
        .error("begin_invocation", json!({"task_id": begun["task_id"]}))
        .await;
    assert_eq!(duplicate.code, "task_in_progress");
    assert!(fixture.ok("next_trigger", json!({})).await["task_id"].is_null());
    let parked = fixture
        .ok(
            "fail_invocation",
            json!({"attempt_id": begun["attempt_id"], "reason": "malformed", "retryable": false}),
        )
        .await;
    assert_eq!(parked["retry"], json!({"state": "parked", "attempts": 1}));
    assert_eq!(
        fixture
            .error("begin_invocation", json!({"task_id": begun["task_id"]}))
            .await
            .code,
        "task_parked"
    );
    assert_eq!(fixture.pending("worker").await, ["bad"]);
    fixture.stop().await;
}

#[tokio::test]
async fn a_rejected_submission_ends_the_attempt_and_counts_as_a_failure() {
    let fixture = Fixture::new(document(json!({})), "worker", None).await;
    fixture.deliver("input").await;
    let begun = begin(&fixture).await;
    let frontier = fixture
        .session
        .pending_at(fixture.tools.node_id())
        .await
        .unwrap();
    let (package, _) = frontier.packages()[0];
    fixture
        .session
        .retire(package, None)
        .await
        .unwrap()
        .unwrap();
    let rejected = fixture
        .ok(
            "submit_invocation",
            json!({"attempt_id": begun["attempt_id"], "result": {"message": "late"}}),
        )
        .await;
    assert_eq!(rejected["status"], "rejected");
    assert_eq!(rejected["retry"]["state"], "retrying");
    assert_eq!(
        fixture
            .invocation_status(begun["attempt_id"].as_str().unwrap())
            .await,
        InvocationStatus::Rejected
    );
    fixture.stop().await;
}

#[tokio::test]
async fn closing_interrupts_attempts_without_counting_a_failure() {
    let fixture = Fixture::new(document(json!({})), "worker", None).await;
    fixture.deliver("input").await;
    let begun = begin(&fixture).await;
    fixture.tools.close().await;
    assert_eq!(
        fixture
            .invocation_status(begun["attempt_id"].as_str().unwrap())
            .await,
        InvocationStatus::Interrupted
    );
    assert!(fixture.ledger.failed().is_empty());
    assert_eq!(fixture.pending("worker").await, ["input"]);
    fixture.stop().await;
}

#[tokio::test]
async fn the_initial_input_is_offered_until_accepted_and_never_again() {
    let fixture = Fixture::new(entry_document(json!([])), "worker", Some("kickoff")).await;
    let next = fixture.ok("next_trigger", json!({})).await;
    assert_eq!(next["initial"], true);
    let begun = fixture
        .ok("begin_invocation", json!({"task_id": next["task_id"]}))
        .await;
    assert_eq!(begun["initial"], true);
    let read = fixture
        .ok(
            "read_package",
            json!({"attempt_id": begun["attempt_id"], "handle": begun["inputs"][0]["handle"]}),
        )
        .await;
    assert!(read["text"].as_str().unwrap().contains("kickoff"));
    fixture
        .ok(
            "submit_invocation",
            json!({"attempt_id": begun["attempt_id"], "result": {"message": "started"}}),
        )
        .await;
    assert!(fixture.ok("next_trigger", json!({})).await["task_id"].is_null());
    assert_eq!(
        fixture.ok("inspect_node", json!({})).await["initial_pending"],
        false
    );
    assert!(
        fixture
            .directory
            .path()
            .join("nodes/worker/initial-complete.json")
            .exists()
    );
    assert_eq!(fixture.pending("sink").await, ["started"]);
    fixture.stop().await;
}

#[tokio::test]
async fn grants_gate_originating_sending_later_and_retiring() {
    let fixture = Fixture::new(entry_document(json!([])), "worker", None).await;
    let names = |fixture: &Fixture| {
        fixture
            .tools
            .catalog()
            .iter()
            .map(|tool| tool.name)
            .collect::<Vec<_>>()
    };
    assert!(!names(&fixture).contains(&"transfer_package"));
    let work = format!("work_{}", "0".repeat(64));
    for (tool, args) in [
        ("begin_invocation", json!({"originate": "new work"})),
        ("list_outbound", json!({})),
        ("transfer_package", json!({"work_id": work, "to": "sink"})),
        ("retire_package", json!({"work_id": work})),
    ] {
        assert_eq!(
            fixture.error(tool, args).await.code,
            "not_granted",
            "{tool}"
        );
    }

    fixture.grant(&[Grant::Originate, Grant::SendLater, Grant::Retire]);
    assert!(names(&fixture).contains(&"transfer_package"));
    let originated = fixture
        .ok("begin_invocation", json!({"originate": "new work"}))
        .await;
    assert!(originated["task_id"].is_null());
    fixture
        .ok(
            "submit_invocation",
            json!({"attempt_id": originated["attempt_id"], "result": {"message": "held"},
                "outputs": [{"message": "later", "outbound": true}, {"message": "discard me", "outbound": true}]}),
        )
        .await;
    assert!(fixture.pending("sink").await.is_empty());
    let outbound = fixture.ok("list_outbound", json!({})).await;
    let packages = outbound["packages"].as_array().unwrap().clone();
    assert_eq!(packages.len(), 2);
    let delivered = fixture
        .ok(
            "transfer_package",
            json!({"work_id": packages[0]["work_id"], "to": "sink"}),
        )
        .await;
    assert_eq!(delivered["status"], "delivered");
    let retired = fixture
        .ok("retire_package", json!({"work_id": packages[1]["work_id"]}))
        .await;
    assert_eq!(retired["status"], "retired");
    assert_eq!(fixture.pending("sink").await.len(), 1);
    assert!(
        fixture.ok("list_outbound", json!({})).await["packages"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fixture
            .error("retire_package", json!({"work_id": packages[1]["work_id"]}))
            .await
            .code,
        "stale_work"
    );
    fixture.stop().await;
}

#[tokio::test]
async fn a_discarded_initial_input_is_not_offered_and_gates_originating_until_done() {
    let fixture = Fixture::new(
        entry_document(json!(["originate"])),
        "worker",
        Some("kickoff"),
    )
    .await;
    // An accepted root at the entry must be the initial input's until it is done.
    let early = fixture
        .error("begin_invocation", json!({"originate": "side work"}))
        .await;
    assert_eq!(early.code, "initial_pending");
    assert_eq!(fixture.ok("next_trigger", json!({})).await["initial"], true);
    // The manager discards the parked initial input, as flow.discard does.
    crate::workflow::runtime::complete_initial(&fixture.directory.path().join("nodes/worker"))
        .unwrap();
    assert!(fixture.ok("next_trigger", json!({})).await["task_id"].is_null());
    assert_eq!(
        fixture.ok("inspect_node", json!({})).await["initial_pending"],
        false
    );
    let originated = fixture
        .ok("begin_invocation", json!({"originate": "side work"}))
        .await;
    assert!(originated["task_id"].is_null());
    fixture.stop().await;
}

#[tokio::test]
async fn waiting_wakes_for_new_input_and_reports_why() {
    let fixture = Arc::new(Fixture::new(document(json!({})), "worker", None).await);
    let waiter = {
        let fixture = Arc::clone(&fixture);
        tokio::spawn(async move {
            fixture
                .ok("wait_for_change", json!({"timeout_ms": 5000}))
                .await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    fixture.deliver("wake up").await;
    let woke = waiter.await.unwrap();
    assert_eq!(woke["reason"], "frontier");
    let stale = fixture
        .ok(
            "wait_for_change",
            json!({"after": "0.0.0.0", "timeout_ms": 5000}),
        )
        .await;
    assert_eq!(stale["reason"], "changed");
    let timed_out = fixture
        .ok("wait_for_change", json!({"timeout_ms": 1}))
        .await;
    assert_eq!(timed_out["reason"], "timeout");
    Arc::try_unwrap(fixture).ok().unwrap().stop().await;
}

/// Workers see workflow names and opaque handles, never core package or
/// activation identities, including in core's rejection reasons.
#[tokio::test]
async fn replies_keep_core_package_and_activation_identities_hidden() {
    let fixture = Fixture::new(document(json!({})), "worker", None).await;
    fixture.deliver("hello").await;
    let frontier = fixture
        .session
        .pending_at(fixture.tools.node_id())
        .await
        .unwrap();
    let (package, _) = frontier.packages()[0];
    let key = format!(
        "{:032x}:{:032x}",
        package.producer().as_u128(),
        package.output()
    );
    let begun = begin(&fixture).await;
    let described = recorded(
        &fixture,
        "describe_package",
        json!({"attempt_id": begun["attempt_id"], "handle": begun["inputs"][0]["handle"]}),
    )
    .await;
    assert_eq!(described["kind"], "message");
    let accepted = fixture
        .ok(
            "submit_invocation",
            json!({"attempt_id": begun["attempt_id"], "result": {"message": "done"}}),
        )
        .await;
    fixture.deliver("again").await;
    let retried = begin(&fixture).await;
    let frontier = fixture
        .session
        .pending_at(fixture.tools.node_id())
        .await
        .unwrap();
    let (second, _) = frontier.packages()[0];
    fixture.session.retire(second, None).await.unwrap().unwrap();
    let rejected = fixture
        .ok(
            "submit_invocation",
            json!({"attempt_id": retried["attempt_id"], "result": {"message": "late"}}),
        )
        .await;
    assert_eq!(rejected["status"], "rejected");
    for (reply, identity) in [
        (&described, key.as_str()),
        (&accepted, &package.producer().to_string()),
        (&rejected, &second.to_string()),
    ] {
        assert!(!reply.to_string().contains(identity), "{reply}");
    }
    let recorded = fixture.ledger.failed();
    assert!(!recorded[0].failures.error.contains(&second.to_string()));
    fixture.stop().await;
}

/// A wait passed an earlier reply's version ends at once when something that
/// could make work runnable changed in between: a retry granted by the
/// manager, a backoff that ran out, or the node's settings.
#[tokio::test(start_paused = true)]
async fn waiting_sees_changes_made_before_the_wait_began() {
    let fixture = Fixture::new(document(json!({})), "worker", None).await;
    fixture.deliver("input").await;
    let begun = begin(&fixture).await;
    let failed = fixture
        .ok(
            "fail_invocation",
            json!({"attempt_id": begun["attempt_id"], "reason": "flaky"}),
        )
        .await;
    assert_eq!(failed["retry"]["retry_in_secs"], 5);
    let wait = |version: &Value| json!({"after": version, "timeout_ms": 30000});

    // A backoff that ends before the wait begins.
    let next = fixture.ok("next_trigger", json!({})).await;
    assert!(next["task_id"].is_null());
    tokio::time::advance(std::time::Duration::from_secs(6)).await;
    let woke = fixture.ok("wait_for_change", wait(&next["version"])).await;
    assert_eq!(woke["reason"], "changed");

    // A settings change before the wait begins.
    let version = fixture.ok("inspect_node", json!({})).await["version"].clone();
    fixture.grant(&[Grant::Retire]);
    let woke = fixture.ok("wait_for_change", wait(&version)).await;
    assert_eq!(woke["reason"], "changed");

    // A manager retry before the wait begins.
    let again = begin(&fixture).await;
    fixture
        .ok(
            "fail_invocation",
            json!({"attempt_id": again["attempt_id"], "reason": "flaky", "retryable": false}),
        )
        .await;
    let next = fixture.ok("next_trigger", json!({})).await;
    assert!(next["task_id"].is_null());
    let key = TaskKey::parse(again["task_id"].as_str().unwrap()).unwrap();
    assert!(fixture.ledger.clear(&key).unwrap().is_some());
    let woke = fixture.ok("wait_for_change", wait(&next["version"])).await;
    assert_eq!(woke["reason"], "changed");
    // Nothing changed since this wait's own version.
    let quiet = fixture
        .ok(
            "wait_for_change",
            json!({"after": woke["version"], "timeout_ms": 10}),
        )
        .await;
    assert_eq!(quiet["reason"], "timeout");
    fixture.stop().await;
}

/// A reply the transport writes while its attempt is being submitted is still
/// marked sent: ending the attempt waits for it.
#[tokio::test]
async fn a_reply_written_as_its_attempt_ends_is_marked_sent() {
    let fixture = Arc::new(Fixture::new(document(json!({})), "worker", None).await);
    fixture.deliver("hello").await;
    let begun = begin(&fixture).await;
    let attempt = begun["attempt_id"].as_str().unwrap().to_owned();
    let read = fixture
        .tools
        .call(
            "read_package",
            json!({"attempt_id": attempt, "handle": begun["inputs"][0]["handle"]}),
        )
        .await
        .unwrap();
    let (_, sequence) = read.receipt().unwrap();
    let submit = {
        let (fixture, attempt) = (Arc::clone(&fixture), attempt.clone());
        tokio::spawn(async move {
            fixture
                .ok(
                    "submit_invocation",
                    json!({"attempt_id": attempt, "result": {"message": "done"}}),
                )
                .await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    read.sent().await.unwrap();
    assert_eq!(submit.await.unwrap()["status"], "accepted");
    let events = fixture
        .session
        .invocation_events(InvocationId::from_str(&attempt).unwrap(), 0, 100)
        .await
        .unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.receipt_sequence == sequence && event.state == ReceiptState::Sent)
    );
    Arc::try_unwrap(fixture).ok().unwrap().stop().await;
}

/// Once an attempt's event budget is spent, core records nothing more and
/// ends the invocation. The attempt ends with it and counts as failed.
#[tokio::test]
async fn an_attempt_whose_event_budget_is_spent_ends_and_counts_as_failed() {
    let fixture = Fixture::new(
        document(json!({"retry": {"max_attempts": 1}})),
        "worker",
        None,
    )
    .await;
    fixture.deliver("input").await;
    let begun = begin(&fixture).await;
    let attempt = begun["attempt_id"].as_str().unwrap().to_owned();
    let read = json!({"attempt_id": attempt, "handle": begun["inputs"][0]["handle"]});
    let mut reads = 0;
    let refusal = loop {
        match fixture.call("read_package", read.clone()).await {
            Ok(_) => reads += 1,
            Err(error) => break error,
        }
        assert!(reads < 5000, "the event budget was never reached");
    };
    assert_eq!(refusal.code, "budget_exhausted", "after {reads} reads");
    assert_eq!(refusal.details.unwrap()["status"], "failed");
    assert_eq!(
        fixture.invocation_status(&attempt).await,
        InvocationStatus::Failed
    );
    assert_eq!(
        fixture.ok("inspect_node", json!({})).await["open_attempts"],
        json!([])
    );
    let failed = fixture.ledger.failed();
    assert!(failed.len() == 1 && failed[0].failures.parked, "{failed:?}");
    fixture.stop().await;
}

/// An input core refuses to begin, here larger than an attempt's context
/// budget, is recorded against its task and parked, so the node's other
/// input is offered next.
#[tokio::test]
async fn an_input_core_refuses_to_begin_is_parked_not_offered_forever() {
    let fixture = Fixture::new(document(json!({})), "worker", None).await;
    fixture.deliver(&"x".repeat(9 * 1024 * 1024)).await;
    fixture.deliver("small").await;
    let mut refused = Vec::new();
    for _ in 0..4 {
        let next = fixture.ok("next_trigger", json!({})).await;
        if next["task_id"].is_null() {
            break;
        }
        match fixture
            .call("begin_invocation", json!({"task_id": next["task_id"]}))
            .await
        {
            Ok(begun) => {
                fixture
                    .ok(
                        "submit_invocation",
                        json!({"attempt_id": begun["attempt_id"], "result": {"message": "done"}}),
                    )
                    .await;
            }
            Err(error) => {
                assert_eq!(error.details.unwrap()["retry"]["state"], "parked");
                refused.push(error.code);
            }
        }
    }
    assert_eq!(refused, ["too_large"]);
    assert_eq!(fixture.pending("sink").await, ["done"]);
    fixture.stop().await;
}

/// After close, no attempt begins, so nothing is left for close to clean up.
#[tokio::test]
async fn no_attempt_begins_after_close() {
    let fixture = Fixture::new(document(json!({})), "worker", None).await;
    fixture.deliver("input").await;
    let next = fixture.ok("next_trigger", json!({})).await;
    fixture.tools.close().await;
    let late = fixture
        .error("begin_invocation", json!({"task_id": next["task_id"]}))
        .await;
    assert_eq!(late.code, "execution_stopping");
    assert_eq!(
        fixture.ok("inspect_node", json!({})).await["open_attempts"],
        json!([])
    );
    fixture.stop().await;
}

/// Staged content of an attempt that ends unpublished is released, while an
/// open attempt keeps it pinned.
#[tokio::test]
async fn unpublished_imports_are_released_when_the_attempt_fails() {
    use ontography::content::Hash;
    let fixture = Fixture::new(document(json!({})), "worker", None).await;
    fixture.deliver("input").await;
    let begun = begin(&fixture).await;
    let text = "unpublished draft 7c1f0e";
    recorded(
        &fixture,
        "import_content",
        json!({"attempt_id": begun["attempt_id"], "text": text}),
    )
    .await;
    let id: ContentId = serde_json::from_value(json!({
        "hash": Hash::new(text.as_bytes()).to_string(), "format": "Raw", "size": text.len(),
    }))
    .unwrap();
    let content = fixture.session.content_store().await.unwrap();
    content.collect_garbage().await.unwrap();
    assert!(
        content.verify_content(&[id]).await.is_ok(),
        "pinned while open"
    );
    fixture
        .ok(
            "fail_invocation",
            json!({"attempt_id": begun["attempt_id"], "reason": "abandon"}),
        )
        .await;
    content.collect_garbage().await.unwrap();
    assert!(
        content.verify_content(&[id]).await.is_err(),
        "released at end"
    );
    fixture.stop().await;
}

/// Submits an activation at `node`, a root or one consuming `trigger`, that
/// emits `text` on the connection to `to`.
async fn emit(
    fixture: &Fixture,
    ids: &IdentityMap,
    node: &str,
    trigger: Option<ontography::PackageId>,
    to: &str,
    text: &str,
) {
    let id = ids.nodes[node].clone();
    let kernel = fixture.session.kernel().await.unwrap();
    let payload = message(text);
    let mut proposal = match trigger {
        Some(package) => ActivationProposal::join([package], payload.clone()),
        None => ActivationProposal::root(
            id.as_str(),
            kernel.root_ceiling(&id).unwrap().clone(),
            payload.clone(),
        ),
    };
    proposal.emit(Emission::new(
        ids.edges[&edge_key(node, to)].as_str(),
        OutputAuthority::Carry,
        payload,
    ));
    assert!(matches!(
        fixture.session.submit(proposal).await.unwrap(),
        ProposalDecision::Committed(_)
    ));
}

/// Sends `text` from the root `root` through `side` to `worker`.
async fn relay(fixture: &Fixture, ids: &IdentityMap, root: &str, side: &str, text: &str) {
    emit(fixture, ids, root, None, side, text).await;
    let page = fixture
        .session
        .pending_page_at(ids.nodes[side].clone(), None, 1)
        .await
        .unwrap();
    let (package, _) = page.packages()[0].clone();
    emit(fixture, ids, side, Some(package), "worker", text).await;
}

/// A parked join keeps its inputs, while a complete join of other inputs
/// runs: here core's own next join takes a parked input, and the only free
/// input on its connection lies pages deep in a backlog.
#[tokio::test]
async fn a_parked_join_keeps_its_inputs_while_a_later_join_runs() {
    let document: Document = serde_json::from_value(json!({
        "name": "parked", "entry": "left",
        "nodes": [{"id": "left", "kind": "inbox"}, {"id": "right", "kind": "inbox"},
            {"id": "worker", "kind": "agent", "config": {"prompt": "Join"}, "join": "all"},
            {"id": "sink", "kind": "inbox"}],
        "edges": [{"from": "left", "to": "worker"}, {"from": "left", "to": "right"},
            {"from": "right", "to": "worker"}, {"from": "worker", "to": "sink"}],
    }))
    .unwrap();
    let fixture = Fixture::new(document, "worker", None).await;
    let ids = fixture.identities.clone();
    emit(&fixture, &ids, "left", None, "worker", "a1").await;
    relay(&fixture, &ids, "left", "right", "b1").await;
    let first = fixture.ok("next_trigger", json!({})).await;
    let held: Vec<_> = first["inputs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|input| input["work_id"].clone())
        .collect();
    let parked = fixture
        .ok("begin_invocation", json!({"task_id": first["task_id"]}))
        .await;
    fixture
        .ok(
            "fail_invocation",
            json!({"attempt_id": parked["attempt_id"], "reason": "bad", "retryable": false}),
        )
        .await;
    for n in 0..150 {
        emit(&fixture, &ids, "left", None, "worker", &format!("a{n}")).await;
    }
    // Place one more input from `right` after the parked one and past the
    // first two pages of the node's inputs.
    let worker = ids.nodes["worker"].clone();
    let from_right = ids.edges[&edge_key("right", "worker")].clone();
    let mut placed = false;
    for _ in 0..500 {
        relay(&fixture, &ids, "left", "right", "b2").await;
        let pending = fixture
            .session
            .pending_page_at(worker.clone(), None, 1000)
            .await
            .unwrap();
        let rank = |free: bool| {
            pending.packages().iter().position(|(id, record)| {
                record
                    .delivery()
                    .is_some_and(|delivery| delivery.edge_id() == from_right.as_str())
                    && held.contains(&json!(crate::workflow::tasks::work_id(id))) != free
            })
        };
        let (Some(parked_rank), Some(free_rank)) = (rank(false), rank(true)) else {
            panic!("both inputs from `right` wait at the node");
        };
        if parked_rank < free_rank && free_rank >= 128 {
            placed = true;
            break;
        }
        let (free, _) = pending.packages()[free_rank];
        fixture.session.retire(free, None).await.unwrap().unwrap();
    }
    assert!(placed, "could not place the free input");
    let next = fixture.ok("next_trigger", json!({})).await;
    let inputs = next["inputs"].as_array().unwrap();
    assert_eq!(inputs.len(), 2, "{next}");
    assert!(
        inputs.iter().all(|input| !held.contains(&input["work_id"])),
        "{next}"
    );
    let begun = fixture
        .ok("begin_invocation", json!({"task_id": next["task_id"]}))
        .await;
    let submitted = fixture
        .ok(
            "submit_invocation",
            json!({"attempt_id": begun["attempt_id"], "result": {"message": "joined"}}),
        )
        .await;
    assert_eq!(submitted["status"], "accepted");
    let failed = fixture.ledger.failed();
    assert!(failed.len() == 1 && failed[0].failures.parked, "{failed:?}");
    fixture.stop().await;
}

/// A failed join that no longer covers every connection into its node, here
/// because one was added, is no longer a task: its inputs join afresh.
#[tokio::test]
async fn a_failed_join_is_released_when_its_node_gains_a_connection() {
    let mut document = json!({
        "name": "stale", "entry": "feed",
        "nodes": [{"id": "feed", "kind": "inbox"}, {"id": "left", "kind": "inbox"},
            {"id": "right", "kind": "inbox"}, {"id": "third", "kind": "inbox"},
            {"id": "worker", "kind": "agent", "config": {"prompt": "Join"}, "join": "all",
                "retry": {"max_attempts": 3, "initial_delay_secs": 0}},
            {"id": "sink", "kind": "inbox"}],
        "edges": [{"from": "feed", "to": "left"}, {"from": "feed", "to": "right"},
            {"from": "feed", "to": "third"}, {"from": "left", "to": "worker"},
            {"from": "right", "to": "worker"}, {"from": "worker", "to": "sink"}],
    });
    let fixture = Fixture::new(
        serde_json::from_value(document.clone()).unwrap(),
        "worker",
        None,
    )
    .await;
    let ids = fixture.identities.clone();
    relay(&fixture, &ids, "feed", "left", "a1").await;
    relay(&fixture, &ids, "feed", "right", "b1").await;
    let begun = begin(&fixture).await;
    assert_eq!(begun["inputs"].as_array().unwrap().len(), 2);
    let failed = fixture
        .ok(
            "fail_invocation",
            json!({"attempt_id": begun["attempt_id"], "reason": "flaky"}),
        )
        .await;
    assert_eq!(failed["retry"]["state"], "retrying");

    // The manager connects `third` to the join; nothing pending is retired.
    let path = fixture.directory.path().join("workflow.json");
    let mut state = WorkflowState::new(
        serde_json::from_value(document.clone()).unwrap(),
        ids.clone(),
    )
    .unwrap();
    crate::workflow::edit::store(&path, &state).unwrap();
    document["edges"]
        .as_array_mut()
        .unwrap()
        .push(json!({"from": "third", "to": "worker"}));
    let plan = crate::workflow::edit::preview(
        &fixture.session,
        &state,
        serde_json::from_value(document).unwrap(),
    )
    .await
    .unwrap();
    assert!(plan.retirements.is_empty(), "{:?}", plan.retirements);
    crate::workflow::edit::commit(&fixture.session, &mut state, &path, plan)
        .await
        .unwrap();
    let ids = state.identities.clone();
    fixture
        .scope
        .send_replace(NodeScope::new(Arc::new(state), "worker").unwrap());
    relay(&fixture, &ids, "feed", "third", "c1").await;
    let worker = ids.nodes["worker"].clone();
    // Core's own next join takes one input from each of the three connections.
    assert_eq!(
        fixture
            .session
            .next_trigger_at(worker.clone())
            .await
            .unwrap()
            .packages()
            .len(),
        3
    );
    // Run whatever the node offers, as a worker would, until it offers nothing.
    let mut runs = Vec::new();
    for _ in 0..5 {
        let next = fixture.ok("next_trigger", json!({})).await;
        if next["task_id"].is_null() {
            break;
        }
        let begun = fixture
            .ok("begin_invocation", json!({"task_id": next["task_id"]}))
            .await;
        let submitted = fixture
            .ok(
                "submit_invocation",
                json!({"attempt_id": begun["attempt_id"], "result": {"message": "joined"}}),
            )
            .await;
        runs.push(json!({"inputs": next["inputs"].as_array().unwrap().len(),
            "status": submitted["status"], "reason": submitted["reason"],
            "retry": submitted["retry"]}));
    }
    let still_offered_by_core = fixture
        .session
        .next_trigger_at(worker)
        .await
        .unwrap()
        .packages()
        .len();
    assert_eq!(
        runs,
        [json!({"inputs": 3, "status": "accepted", "reason": null, "retry": null})],
        "core still offers a {still_offered_by_core}-input join"
    );
    fixture.stop().await;
}

/// A `next_after` token continues only the listing that issued it.
#[tokio::test]
async fn a_cursor_continues_only_the_listing_that_issued_it() {
    let fixture = Fixture::new(document(json!({"grants": ["send_later"]})), "worker", None).await;
    for text in ["one", "two", "three"] {
        fixture.deliver(text).await;
    }
    let begun = begin(&fixture).await;
    fixture
        .ok(
            "submit_invocation",
            json!({"attempt_id": begun["attempt_id"], "result": {"message": "held"},
                "outputs": [{"message": "o1", "outbound": true}, {"message": "o2", "outbound": true},
                    {"message": "o3", "outbound": true}, {"message": "o4", "outbound": true}]}),
        )
        .await;
    let inputs = fixture.ok("list_inputs", json!({"limit": 1})).await;
    assert!(inputs["next_after"].is_string());
    let misused = fixture
        .call("list_outbound", json!({"after": inputs["next_after"]}))
        .await;
    assert!(
        misused
            .as_ref()
            .is_err_and(|error| error.code == "stale_cursor"),
        "an inputs cursor was accepted for outbound packages: {misused:?}"
    );
    fixture.stop().await;
}

/// Calls racing to retire the same input: the losers' errors never name its
/// core identity.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_retirements_never_reveal_a_package_identity() {
    let fixture =
        Arc::new(Fixture::new(document(json!({"grants": ["retire"]})), "worker", None).await);
    let mut leaks = Vec::new();
    for round in 0..40 {
        fixture.deliver(&format!("input {round}")).await;
        let frontier = fixture
            .session
            .pending_at(fixture.tools.node_id())
            .await
            .unwrap();
        let (package, _) = frontier.packages()[0];
        let work = crate::workflow::tasks::work_id(&package);
        let barrier = Arc::new(tokio::sync::Barrier::new(4));
        let calls: Vec<_> = (0..4)
            .map(|_| {
                let (fixture, barrier, work) =
                    (Arc::clone(&fixture), Arc::clone(&barrier), work.clone());
                tokio::spawn(async move {
                    barrier.wait().await;
                    fixture
                        .call("retire_package", json!({"work_id": work}))
                        .await
                })
            })
            .collect();
        for call in calls {
            if let Err(error) = call.await.unwrap()
                && error.message.contains(&package.to_string())
            {
                leaks.push(error.message);
            }
        }
    }
    assert!(
        leaks.is_empty(),
        "{} leaks, e.g. {:?}",
        leaks.len(),
        leaks[0]
    );
    Arc::try_unwrap(fixture).ok().unwrap().stop().await;
}

/// A worker told `retry_in_secs: 5` waits 5000 ms, so its backoff and the
/// timeout fall due together. A reply never counts a change it does not
/// report, so the next wait reports the ended backoff.
#[tokio::test(start_paused = true)]
async fn a_timeout_reply_never_absorbs_an_unreported_change() {
    let fixture = Fixture::new(
        document(
            json!({"retry": {"max_attempts": 100, "initial_delay_secs": 5, "max_delay_secs": 5}}),
        ),
        "worker",
        None,
    )
    .await;
    fixture.deliver("input").await;
    for _ in 0..40 {
        let begun = begin(&fixture).await;
        fixture
            .ok(
                "fail_invocation",
                json!({"attempt_id": begun["attempt_id"], "reason": "flaky"}),
            )
            .await;
        let next = fixture.ok("next_trigger", json!({})).await;
        assert_eq!(next["retry_in_secs"], 5);
        let woke = fixture
            .ok(
                "wait_for_change",
                json!({"after": next["version"], "timeout_ms": 5000}),
            )
            .await;
        if woke["reason"] == "timeout" {
            let again = fixture
                .ok(
                    "wait_for_change",
                    json!({"after": woke["version"], "timeout_ms": 1}),
                )
                .await;
            assert_eq!(
                again["reason"],
                "changed",
                "the ended backoff was absorbed by a timeout reply's version; the task is runnable: {}",
                fixture.ok("next_trigger", json!({})).await
            );
        }
    }
    fixture.stop().await;
}

/// Closing waits for replies already handed to the transport before it
/// interrupts their attempt, so a reply written meanwhile is marked sent.
#[tokio::test]
async fn a_reply_written_while_closing_is_marked_sent() {
    let fixture = Arc::new(Fixture::new(document(json!({})), "worker", None).await);
    fixture.deliver("hello").await;
    let begun = begin(&fixture).await;
    let attempt = begun["attempt_id"].as_str().unwrap().to_owned();
    let read = fixture
        .tools
        .call(
            "read_package",
            json!({"attempt_id": attempt, "handle": begun["inputs"][0]["handle"]}),
        )
        .await
        .unwrap();
    let closing = {
        let fixture = Arc::clone(&fixture);
        tokio::spawn(async move { fixture.tools.close().await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let marked = read.sent().await;
    closing.await.unwrap();
    assert!(
        marked.is_ok(),
        "the written reply's receipt stayed prepared: {marked:?}"
    );
    Arc::try_unwrap(fixture).ok().unwrap().stop().await;
}

/// A read beyond the attempt's byte budget is refused, and the attempt stays
/// open: a reply written before is still marked sent, and the worker can
/// still submit.
#[tokio::test]
async fn a_read_beyond_the_byte_budget_is_refused_and_the_attempt_stays_open() {
    const CHUNK: u64 = 256 * 1024;
    let fixture = Fixture::new(document(json!({})), "worker", None).await;
    fixture.deliver(&"x".repeat(3 * 1024 * 1024)).await;
    let begun = begin(&fixture).await;
    let attempt = begun["attempt_id"].as_str().unwrap().to_owned();
    let handle = begun["inputs"][0]["handle"].clone();
    let read = |end: u64| json!({"attempt_id": attempt, "handle": handle, "start": 0, "end": end});
    let returned = async || {
        let node = fixture.tools.node_id().to_owned();
        fixture
            .session
            .invocations_page(Some(&node), None, 10)
            .await
            .unwrap()
            .iter()
            .find(|record| record.id.to_string() == attempt)
            .unwrap()
            .returned_bytes
    };
    // Spend the byte budget until less than one more full read fits.
    while 8 * 1024 * 1024 - returned().await >= CHUNK + 4096 {
        fixture.ok("read_package", read(CHUNK)).await;
    }
    // A small read the transport has not written yet.
    let small = fixture.tools.call("read_package", read(16)).await.unwrap();
    let (_, sequence) = small.receipt().unwrap();
    let refusal = fixture.error("read_package", read(CHUNK)).await;
    assert_eq!(refusal.code, "too_large");
    small.sent().await.unwrap();
    let events = fixture
        .session
        .invocation_events(InvocationId::from_str(&attempt).unwrap(), 0, 200)
        .await
        .unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.receipt_sequence == sequence && event.state == ReceiptState::Sent)
    );
    let submitted = fixture
        .ok(
            "submit_invocation",
            json!({"attempt_id": attempt, "result": {"message": "partial"}}),
        )
        .await;
    assert_eq!(submitted["status"], "accepted");
    fixture.stop().await;
}

/// Once an attempt's event budget is spent, core ends its invocation on the
/// next refused call, yet reports that call's own refusal. The attempt ends
/// with it and counts as failed.
#[tokio::test]
async fn a_refusal_once_the_events_are_spent_ends_the_attempt() {
    let fixture = Fixture::new(
        document(json!({"retry": {"max_attempts": 1}})),
        "worker",
        None,
    )
    .await;
    fixture.deliver("input").await;
    let begun = begin(&fixture).await;
    let attempt = begun["attempt_id"].as_str().unwrap().to_owned();
    // An empty range is refused, and each refusal spends one event. The
    // call that core ends the invocation on must itself say so.
    let refused = json!({"attempt_id": attempt, "handle": begun["inputs"][0]["handle"],
        "start": 2, "end": 1});
    let mut refusals = 0;
    let ended = loop {
        let error = fixture.error("read_package", refused.clone()).await;
        if fixture.invocation_status(&attempt).await != InvocationStatus::Open {
            break error;
        }
        assert_eq!(error.code, "denied");
        refusals += 1;
        assert!(refusals <= 4096, "the event budget was never spent");
    };
    assert_eq!(ended.code, "attempt_ended", "after {refusals} refusals");
    assert_eq!(ended.details.unwrap()["status"], "failed");
    assert_eq!(
        fixture.invocation_status(&attempt).await,
        InvocationStatus::Failed
    );
    assert_eq!(
        fixture.ok("inspect_node", json!({})).await["open_attempts"],
        json!([])
    );
    let failed = fixture.ledger.failed();
    assert!(failed.len() == 1 && failed[0].failures.parked, "{failed:?}");
    fixture.stop().await;
}

/// Once its execution stops, a node's tools begin, move, and discard nothing
/// more, whoever still holds them.
#[tokio::test]
async fn a_stopped_execution_moves_and_discards_nothing() {
    let fixture = Fixture::new(
        document(json!({"grants": ["send_later", "retire"]})),
        "worker",
        None,
    )
    .await;
    fixture.deliver("one").await;
    fixture.deliver("two").await;
    let begun = begin(&fixture).await;
    fixture
        .ok(
            "submit_invocation",
            json!({"attempt_id": begun["attempt_id"], "result": {"message": "held"},
                "outputs": [{"message": "later", "outbound": true}]}),
        )
        .await;
    let outbound = fixture.ok("list_outbound", json!({})).await["packages"][0]["work_id"].clone();
    let waiting = fixture.ok("list_inputs", json!({})).await["inputs"][0].clone();
    // The host stops the execution, which closes its tools; a transport may
    // still hold them.
    fixture.execution.request_stop();
    fixture.execution.wait().await;
    for (tool, args) in [
        ("begin_invocation", json!({"task_id": waiting["task_id"]})),
        (
            "transfer_package",
            json!({"work_id": outbound, "to": "sink"}),
        ),
        ("retire_package", json!({"work_id": outbound})),
        ("retire_package", json!({"work_id": waiting["work_id"]})),
    ] {
        assert_eq!(
            fixture.error(tool, args).await.code,
            "execution_stopping",
            "{tool}"
        );
    }
    assert!(fixture.pending("sink").await.is_empty());
    assert_eq!(fixture.pending("worker").await.len(), 1);
    let still = fixture.ok("list_outbound", json!({})).await;
    assert_eq!(still["packages"].as_array().unwrap().len(), 1);
    fixture.stop().await;
}

/// Sending and retiring one package at once never does both: it is either
/// delivered or retired, never delivered and then retired at its receiver.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_package_sent_and_retired_at_once_is_never_both() {
    let fixture = Arc::new(
        Fixture::new(
            document(json!({"grants": ["send_later", "retire"]})),
            "worker",
            None,
        )
        .await,
    );
    for round in 0..100 {
        fixture.deliver(&format!("input {round}")).await;
        let begun = begin(&fixture).await;
        fixture
            .ok(
                "submit_invocation",
                json!({"attempt_id": begun["attempt_id"], "result": {"message": "held"},
                    "outputs": [{"message": format!("later {round}"), "outbound": true}]}),
            )
            .await;
        let work = fixture.ok("list_outbound", json!({})).await["packages"][0]["work_id"].clone();
        // One transfer races several retirements of the same package.
        let calls: Vec<_> =
            std::iter::once(("transfer_package", json!({"work_id": work, "to": "sink"})))
                .chain(std::iter::repeat_n(
                    ("retire_package", json!({"work_id": work})),
                    7,
                ))
                .collect();
        let barrier = Arc::new(tokio::sync::Barrier::new(calls.len()));
        let calls: Vec<_> = calls
            .into_iter()
            .map(|(tool, args)| {
                let (fixture, barrier) = (Arc::clone(&fixture), Arc::clone(&barrier));
                tokio::spawn(async move {
                    barrier.wait().await;
                    fixture.call(tool, args).await
                })
            })
            .collect();
        let mut succeeded = Vec::new();
        for call in calls {
            succeeded.push(call.await.unwrap().is_ok());
        }
        // Exactly one call wins; the others find the package gone.
        assert_eq!(
            succeeded.iter().filter(|ok| **ok).count(),
            1,
            "round {round}: transfer, then retirements, succeeded {succeeded:?}"
        );
    }
    Arc::try_unwrap(fixture).ok().unwrap().stop().await;
}
