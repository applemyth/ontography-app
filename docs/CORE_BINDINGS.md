# Core bindings and coverage

The workflow layer uses the existing public core API. It adds an app document
language compiled to core's typed model, edit recovery, a task harness, and
persistent node sessions; it does not change core.
[WORKFLOWS.md](WORKFLOWS.md) is the usage guide. [ARCHITECTURE.md](../ARCHITECTURE.md)
retains the agreed scope and outstanding live-model project acceptance work.

## Ownership

One Ontography app session owns Pi manager state, a native manager terminal,
and one graph run after initialization. Pi owns conversation semantics. The
server retains processes, accepted requests, core runtimes, and workspace
handles independently of attached clients. Core owns graph transitions,
package custody/history, content, and context records. See
[session ownership](SESSIONS.md).

A document compiles into a `GraphDeclaration`, core's graph, which its run
stores; component bindings stay in the app's workflow state. Every run starts
this way. The harness handles initial input and workspace preparation.

Nodes place components, using core's project model (`ontography::project`).
Built-in and library components implement `ProjectComponent`: each
`ComponentDescription` gives the node's core node types, and `bind` turns the
placement's settings into a `BoundComponent`, the trusted implementation kind
and exact configuration. Library and document specifications load through a
`ComponentProvider` named `ontography`. The bound kind and configuration are
the node's binding. A run stores its bindings with its document, so a later
library change reaches a node only through an edit.

A binding carries the node's types: labels such as `Agent` or `Reviewer` that
core records on the node. Built-in components give one each (`Agent`,
`Command`, `Human`, `Inbox`, or `External`); library and document specifications may add
more, which accumulate along `extends` chains. Types never choose behavior;
the implementation does. A new run's schema declares every type any component
in its catalog can give, placed or not, so later edits can place any of them;
an edit that needs an undeclared type fails with `unknown_node_type`. Nodes
and connections take the document's contracts and authority, or the defaults:
contract `payload` (object type `Payload`, validator `text`) and tag
`workflow`. The run's schema declares every contract, object type, and tag the
document uses; they are fixed for the run. Connections require no endpoint
type. A node keeps its core definition for life: an edit that changes its
types, join, result contract, root, or transitions replaces the node, and one
that changes a connection's contract or authority replaces the connection. A
new run's core identities are the document's names; replacements take fresh
UUIDs. Reusable documents live under `definitions/workflows/<revision>.json`.

A workflow edit is one explicit core `GraphEdit`: it adds the target's nodes
and connections that core lacks and removes those the target lacks. Workflow
runs admit edits under the policy in [`edit.rs`](../src/workflow/edit.rs):
only principal `workflow`, the app's editor, may edit, and it changes the graph
only to match a document the compiler has checked. Other principals are
denied.

The app persists its document, identity map, and edit intentions alongside
core storage. It never reads or modifies core's private SQLite representation.
Core's current graph and accepted history determine what actually committed;
app output files are caches, not publication evidence. Reopening checks the
stored core build and declaration/catalog identities.

## Workflow surface

Pi exposes the following fourteen workflow tools. Bound requests default to the
session's run and project; explicit targets must agree with that binding.
Schemas are available through the session-scoped `system.hello` handshake.

| Operation | Manager input and result | Existing capability used |
| --- | --- | --- |
| `flow.library` | → components with node types and settings schemas; library MCP servers | Built-in `ProjectComponent`s and library specifications loaded through the `ontography` `ComponentProvider`. |
| `flow.define` | Document → reusable revision | App validation, component binding, and immutable document storage; declaration admission checks on start. |
| `flow.start` | Document or revision, message or workspace directory → run | `GraphDeclaration`, persistent runtime/session, reserved initialization, app worker bindings. |
| `flow.status` | Run → document, named graph, pending edit, worker state, tasks, failures | Current kernel/frontier, app edit state, hosted execution status, per-node retry ledgers. |
| `flow.output` | Node and optional task/work selection → ready input, last result, or inbox page | Accepted invocation/publication evidence, bounded pending queries, and content reads. |
| `flow.edit` | Updated document → plan, count of graph changes, and exact retirement preview | `SessionHandle::prepare_rewrite` of one `GraphEdit`, without committing it. |
| `flow.commit` | Plan ID → completed or recoverable edit | Revision-fenced `SessionHandle::prepare_rewrite` / `commit_rewrite`, persisted target and identities. |
| `flow.resume` | Run → recovered edit and restarted workers | Core reopening, saved target recovery, worker reconciliation. |
| `flow.decide` | Human node/task ID, message or saved workspace → publication | Node-bound invocation, accepted result and outgoing emissions. |
| `flow.retry` | Worker node, optional failed task ID → fresh attempts | App retry ledger, which wakes the worker. |
| `flow.discard` | Worker node/parked task ID → discarded input | Package retirement through `SessionHandle::retire`; initial input completion. |
| `flow.workspace` | Open/capture/release; node, path, or workspace handle | The app's `WorkspaceStore`, retained checkouts, durable checkpoints/dependency retention. |
| `flow.promote` | Run → reusable document revision | Save the current document after any pending edit completes. |
| `flow.export` | Node and new destination → exported file/directory | Message bytes or resolved native package entries and content export. |

Pi also exposes `session.context`, `session.inspect`, and `operation.get`.
There is no group activation/bootstrap tool. Its visible schemas do not
include raw graph, rewrite, content, package, or context tools. Native workspace
payloads appear as `{"workspace":true}`; tools accept workspace handles rather
than content roots. Status/output previews bound text; artifact export obtains
the complete result. Human reads prefer their ready task; `source:"output"`
selects the prior result. Inboxes expose every item with a `work_id`, which
also selects the exact input for workspace opening/export. Pages use stable
identity order and require the same revision, not inferred chronological order.
Worker status is structured as `{state,error?}` with original error text.

The session handshake separately advertises `session.*`, `terminal.*`,
`system.hello`, `system.status`, and `operation.get` for CLI/extension lifecycle
and transport. Session and manager-terminal control remain infrastructure.

## Internal capabilities and removed wrappers

Unused raw tool wrappers are deleted: package, invocation, context, project,
network, fixed-fact export/restore/verification, and live vocabulary extension.
Content keeps only read/metadata operations and shared helpers. Graph import,
rewrite list/inspect, and unused workspace operations are removed. Internal
workspace restore remains available to `flow.workspace` without a public
registration.

Raw declarations, graph drafts, operator rewrites, native application runs,
and the executable registry are removed: every run starts from a document and
changes only through its editor. The backend retains run lifecycle
(`run.list`, `run.inspect`, `run.resume`, `run.suspend`, `run.close`), core
moves and inspection (`workflow.*`, `inspect.*`), observation of hosted
executions, and seven workspace operations (import, open, checkout, capture,
checkpoint, release, list). Core moves act only for `external` nodes, so they
cannot publish for a node a program or person owns. `workspace.import` returns
the dependencies a submission that sends the workspace declares.

Package resolution/retention, workspace checkout/capture, invocation context,
and execution supervision remain necessary capabilities. Checkout/capture is
the app's own workspace store ([`src/workspace`](../src/workspace)); the rest
is core's. The harness calls core and that store directly; manager workspace
actions also reuse internal workspace helpers. Their continued use should not
be counted as deleted functionality.
The native application module also remains available internally; its project
tool wrappers do not.

Implementation: [document](../src/workflow/document.rs),
[components](../src/workflow/components/mod.rs),
[editor and edit policy](../src/workflow/edit.rs),
[runtime ownership](../src/workflow/runtime.rs),
[persistent node runtime](../src/node_runtime/mod.rs),
[Codex session launcher](../src/node_runtime/codex.rs),
[task harness](../src/workflow/harness.rs),
[workflow tools](../src/workflow/tools.rs),
[artifacts](../src/workflow/artifacts.rs),
[manager allowlist](../src/catalog.rs), and [Pi tools](../pi/tools.ts).

## Wire and resource behavior

- JSONL frames are bounded to 4 MiB including the newline. Rust validates
  published argument schemas. Larger artifacts use file/content references.
- UUIDs and compound identities are strings. Potentially wide core revisions,
  receipt sequences, and content sizes preserve exact decimal strings.
- Requests carry the expected server instance. A replacement server rejects
  stale requests before dispatch. Session-bound requests also carry
  `app_session_id`; the server fills omitted targets and rejects conflicts.
- Resuming and attaching requests, and `call`, also carry the client's
  environment. The server gives it, in memory only, to a session they activate,
  or to an active session without one that they change; a change to work no
  session owns gives it to that work. `system.hello` reports whether the server
  is `idle`, with nothing running and no other connection, so a client of
  another build can replace it.
- Accepted mutations retain `(client_id, request_id)` outcomes in a bounded
  server table. Identical retries return the original outcome; conflicting
  arguments reject. `operation.get` reports the receipt. Restart loses this
  transient table.
- A missing response means an unknown outcome. Neither Rust nor Pi blindly
  replays it. Client cancellation stops waiting, while accepted server work
  continues. Read receipts and current workflow state before retrying.
- Socket requests are multiplexed. Run mutations hold the run ownership
  mutex; waits for hosted work or frontier changes leave controls available.
- Workflow initialization and edits additionally save durable intentions.
  Session-bound startup reserves its run identity. Unscoped callers can supply
  a stable UUID `start_id` to recover the same start after a lost response.
- Relative paths resolve against the stored workflow project, not the
  server's launch directory.

## Edit recovery and artifact export

Before changing core, the app saves the target document, its allocated
identities, and approved retirements. The edit is one atomic core transition,
and existing workers continue while it is prepared and committed. Recovery
computes the edit again from the actual core graph and finds either the whole
edit or nothing left to do. A commit that worker activity makes stale is
prepared again; after eight stale attempts, recovery stops with
`workflow_busy` and keeps the saved intention. It cannot undo retirements.
Additional retirements stop recovery for a fresh preview of the same target
(`retirement_preview_required`). A settings-only edit never repairs unexpected
graph changes (`workflow_drift`). The last completed document is replaced only
after the edit succeeds; worker reconciliation then applies the new bindings.
Plans saved with the earlier `steps` count still load.

Accepted initial input is not replayed merely because an app completion cache
was lost. Publication/output recovery checks accepted core invocations. This
does not make subprocess effects outside core exactly once: an interrupted
uncommitted command may run again after explicit recovery.

`flow.export` writes a message to a new text file or a workspace to a new
directory, preserving files, directories, symlinks, and executable bits. It
validates package paths, builds in a private sibling staging location, and
publishes without overwriting an existing destination. Failure cleans up its
partial staging output. This is result export, not a portable backup of the
run, invocation history, or live processes.

## Node runtimes and remaining limits

Agent nodes use `NodeRuntime`: one core execution owns a persistent private
working directory, a supervised process in a managed PTY or headless on pipes,
a continuing conversation, and `NodeToolContext`. For Codex the runtime hosts a
persistent app-server on a private Unix socket and, with a terminal, connects
the native terminal UI to the same saved conversation with `--remote`. For
Claude it runs Claude Code in the terminal, reporting through hooks, or headless
over stream JSON. Restart reopens the same conversation rather than selecting a
global latest conversation. An `argv` program bypasses agent setup and does not
provide managed conversation recovery. Live process state is never recovered
from conversation history.

The runtime begins attempts for runnable tasks and queues recorded conversation
input through app-server. Packages remain pending until core accepts a result.
A private [MCP adapter](NODE_MCP.md) exposes the selected scoped tools for
reading attachments and publishing replies. App-server acceptance marks an
incoming message receipt sent; MCP stdout acknowledgement marks tool replies.

Command tasks invoke the configured argv with input messages on stdin. Without
a workspace, the result is stdout; with a workspace, the result is the captured
checkout. Each result broadcasts to every outgoing
connection. Human tasks wait for an explicit decision; inboxes keep work pending.

Command config changes are sampled before the next task. Agent config changes
stop and replace the process, preserving its node directory and conversation;
tool selection, grants, and graph scope refresh in place. A kind change stops and
waits for the previous worker before launching the new kind. A failed task
retries with capped backoff while the worker continues with other tasks, then
parks until `flow.retry` or `flow.discard`. Counts persist beside the node's
state, with the definition they were counted under; core's invocation records
remain the audit trail. A change to the node's definition grants fresh
attempts. One task may receive at most one
workspace, and a workspace worker currently requires an outgoing connection
(an inbox can hold its result). The harness checks out a task's workspace only
if it is a directory the attempt received, under the same rules as node tools'
checkouts ([`src/workspace/attempt.rs`](../src/workspace/attempt.rs)). The
attempt's receipts record the checkout's `root`, `path`, `writable`, and input
`handle` as a `workspace_exposure` tool response. The command is given the
checkout as its working directory rather than those bytes, and the receipt is
marked sent when it starts there. A worker that starts removes the checkouts a
crash left behind, as node tools do; one it cannot remove stays and is never
reused.

Node tools ([NODE_TOOLS.md](NODE_TOOLS.md)) bind a worker to its own node:
execution-bound invocations with explorable context, core's package reads and
receipts, staged content, private workspaces, and, when granted, transfer and
retirement through the session. The MCP adapter lists their schemas, forwards
calls, and marks replies sent only after its stdio proxy flushes them to Codex.

Worker and manager PTYs use the same terminal backend. Agent lifecycle includes
cooperative stop, forced cancellation, exit reporting, and explicit resume.
Automatic incoming conversation delivery and MCP package handoffs are available.
The graph UI resolves the selected agent through `terminal.node` and attaches
using its exact terminal identity. Detaching returns to the graph; no process
or graph mutation is part of opening a view. A full interactive project
acceptance run with live models remains outstanding.

Other existing limits remain:

- Changing a run's declared `edits` and live vocabulary extension are not app
  operations. A run's vocabulary, including its node types, is fixed at
  creation; an edit that needs an undeclared type fails with
  `unknown_node_type`. Previously extended runs are not supported by the
  fixed-definition reopening path.
- Some historical inspection adapters materialize a full snapshot before
  returning bounded results.
- Workspace checkout needs supported OS copy-on-write behavior. Read-only
  permissions do not provide process confinement; writers must stop before
  capture.
- Trusted implementation identities are supplied by adapters, not inferred
  from executable code hashes. Local Codex installation/authentication is an
  execution prerequisite.
- This layer introduces no core storage migration or durable full-run import.
  Stored runs must be compatible with the selected core build. The earlier
  integration is recorded in [CORE_UPDATE.md](CORE_UPDATE.md).

## Verification evidence

| Boundary | Tests |
| --- | --- |
| Document validation, common annotations, binding expansion | [document tests](../src/workflow/document.rs) |
| Node types, identity across type changes, declared vocabulary, and the workflow edit policy | [editor tests](../src/workflow/edit.rs), [component tests](../src/workflow/components/mod.rs) |
| Retirement previews, stale plans, interrupted edits, saved-target recovery | [editor tests](../src/workflow/edit.rs), [workflow recovery](../tests/workflow_recovery.rs) |
| Declared `edits`, explicit graph edits, declarations saved with productions | [declaration tests](../src/declarations.rs), [retirement tests](../tests/retirement.rs) |
| Command → human → inbox, config changes, added workers, repeated commits | [workflow flow](../tests/workflow_flow.rs) |
| Repeated reviews, exact inbox selection, pagination, structured failure status | [workflow outputs](../tests/workflow_outputs.rs) |
| Task execution, cancellation, failures, process supervision, workspace constraints | [harness tests](../src/workflow/harness.rs), [supervisor tests](../src/process/) |
| Retry backoff, parking, join sets, manager retry/discard, definition changes, durability | [task tests](../src/workflow/tasks.rs), [workflow retry](../tests/workflow_retry.rs) |
| Node tools: receipts equal sent bytes, metadata-only views, attempts, retries, grants, content and checkouts | [node tool tests](../src/node_tool/tests.rs), [content](../src/node_tool/outputs.rs), [workspaces](../src/node_tool/workspace.rs) |
| Persistent agent startup, scope updates, config replacement, exit, suspension, and resume | [persistent node tests](../tests/persistent_nodes.rs), [node runtime](../src/node_runtime/mod.rs), [Codex launcher](../src/node_runtime/codex.rs) |
| Incoming messages, exact receipts, lost acknowledgements, native queueing, and MCP replies | [delivery tests](../src/node_tool/tests/delivery.rs), [controller tests](../src/node_runtime/rpc.rs), [native Codex fixture](../src/node_runtime/codex/tests/native_delivery.rs) |
| Workspace capture/reopen/export and no-clobber/path validation | [artifact tests](../src/workflow/artifacts.rs), [workspace flow](../tests/workflow_artifacts.rs) |
| Session ownership, scoped targets, manager catalog, initialization recovery | [session ownership](../tests/session_ownership.rs), [catalog tests](../src/catalog.rs) |
| Detached server lifetime, idle exit, abrupt death, receipts, stale instances, multiplexing | [server process tests](../tests/server_lifecycle.rs), [idle exit](../tests/server_idle.rs) |
| Session environments, the variables programs never inherit, reading sessions without a server | [session environments](../tests/session_environment.rs), [environment rule](../src/environment.rs), [session CLI](../tests/session_cli.rs) |
| Malformed manifests isolated from healthy runs | [manifest recovery](../tests/state_recovery.rs) |
| Manager PTY ownership, detach/reattach, graph control | [terminal backend](../src/terminal.rs), [terminal client](../src/terminal_client.rs) |
| Retained core adapters, bounded content reads, workspace checkpoints | [tool module tests](../src/tools/), [management flow](../tests/management_flow.rs) |
| Manager allowlist, session defaults, no replay, receipts, bounded output | [Pi tests](../pi/test/) |

[VERIFICATION.md](VERIFICATION.md) records release checks. Native Codex terminal
and agent-loop checks use a localhost Responses fixture; they do not establish
the quality or completion of a project run against a live model provider.
