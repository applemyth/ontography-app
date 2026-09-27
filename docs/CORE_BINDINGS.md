# Core bindings and coverage

The workflow layer uses the existing public core API. It adds an app document,
fixed vocabulary, edit recovery, and a worker harness; it does not change core.
[WORKFLOWS.md](WORKFLOWS.md) is the usage guide. [ARCHITECTURE.md](../ARCHITECTURE.md)
retains the agreed scope and unfinished interactive worker work.

## Ownership

One Ontography app session owns Pi manager state, a native manager terminal,
and one graph run after initialization. Pi owns conversation semantics. The
server retains processes, accepted requests, core runtimes, and workspace
handles independently of attached clients. Core owns graph transitions,
package custody/history, content, and context records. See
[session ownership](SESSIONS.md).

A document expands into the existing `GraphDeclaration` and app-owned
`ExecutionBinding` settings. Workflow startup uses this graph directly; the
harness handles initial input and workspace preparation. The older native
`ApplicationDeclaration` path remains a separate compatibility path.

All worker kinds share one core node type, result contract, edge rule, and
authority tag. Only ingress (`any`/`all`) and root status affect the graph
variants. The fixed grammar contains 40 productions, including self loops.
Kind and process settings belong to the harness. Reusable documents live under
`definitions/workflows/<revision>.json`, separately from legacy graph drafts.

The app persists its document, identity map, and edit intentions alongside
core storage. It never reads or modifies core's private SQLite representation.
Core's current graph and accepted history determine what actually committed;
app output files are caches, not publication evidence. Reopening checks the
stored core build and declaration/catalog identities.

## Workflow surface

Pi exposes the following thirteen workflow tools. Bound requests default to the
session's run and project; explicit targets must agree with that binding.
Schemas are available through the session-scoped `system.hello` handshake.

| Operation | Manager input and result | Existing capability used |
| --- | --- | --- |
| `flow.define` | Document → reusable revision | App validation and immutable document storage; declaration admission checks on start. |
| `flow.start` | Document or revision, message or workspace directory → run | `GraphDeclaration`, persistent runtime/session, reserved initialization, app worker bindings. |
| `flow.status` | Run → document, named graph, pending edit, worker state, tasks, failures | Current kernel/frontier, app edit state, hosted execution status, per-node retry ledgers. |
| `flow.output` | Node and optional task/work selection → ready input, last result, or inbox page | Accepted invocation/publication evidence, bounded pending queries, and content reads. |
| `flow.edit` | Updated document → plan and exact retirement preview | Cloned state and configured core rewrite preparation. |
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
workspace import/restore remain available to `flow.workspace` without public
registrations.

The backend retains graph declaration, run lifecycle, configured rewrite
prepare/commit/discard, workflow/inspection, hosted execution, and six workspace
operations (open, checkout, capture, checkpoint, release, list). They serve
existing infrastructure and tests. Raw mutations reject document-owned runs,
so they cannot bypass a saved edit or publish outside the workflow harness.

Package resolution/retention, workspace checkout/capture, invocation context,
and execution supervision remain necessary capabilities. Checkout/capture is
the app's own workspace store ([`src/workspace`](../src/workspace)); the rest
is core's. The harness calls core and that store directly; manager workspace
actions also reuse internal workspace helpers. Their continued use should not
be counted as deleted functionality.
The native application module also remains available internally; its project
tool wrappers do not.

Implementation: [document](../src/workflow/document.rs),
[grammar](../src/workflow/grammar.rs), [editor](../src/workflow/edit.rs),
[runtime ownership](../src/workflow/runtime.rs),
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

Before the first edit transition, the app saves the target document, its
allocated identities, and approved retirements. Each core transition is
atomic; the complete edit is not. Existing workers continue while steps apply.
Recovery compares the saved target with the actual core graph, prepares the
next step at the current revision, and resumes forward. It cannot undo
retirements. Additional retirements stop recovery for a fresh preview of the
same target. The last completed document is replaced only after all steps
succeed; worker reconciliation then applies the new bindings.

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

## Harness and remaining limits

Agent tasks currently invoke noninteractive `codex exec`; command tasks invoke
the configured argv. Input messages go to stdin. Without a workspace, the
result is the command's stdout or Codex's last message; with a workspace, the
result is the captured checkout. Each result broadcasts to every outgoing
connection. Human tasks wait for an explicit decision; inboxes keep work pending.

Config changes are sampled before the next task. A kind change stops and
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
retirement through the session. No transport exposes them yet.

Worker PTYs, continuing interactive Codex conversations, node-scoped MCP, and
node panes remain unimplemented. Manager PTYs are already implemented. The
current layer does not complete architecture items 1, 7, and 12.

Other existing limits remain:

- Grammar replacement and live vocabulary extension are not app operations.
  The vocabulary and grammar are fixed at creation. Previously extended runs
  are not supported by the fixed-definition reopening path.
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
| Every allowed ingress/root variant and self loop in the fixed grammar | [grammar tests](../src/workflow/grammar.rs) |
| Retirement previews, stale plans, partial edits, saved-target recovery | [editor tests](../src/workflow/edit.rs), [workflow recovery](../tests/workflow_recovery.rs) |
| Command → human → inbox, config changes, added workers, repeated commits | [workflow flow](../tests/workflow_flow.rs) |
| Repeated reviews, exact inbox selection, pagination, structured failure status | [workflow outputs](../tests/workflow_outputs.rs) |
| Task execution, cancellation, failures, process supervision, workspace constraints | [harness tests](../src/workflow/harness.rs) |
| Retry backoff, parking, join sets, manager retry/discard, definition changes, durability | [task tests](../src/workflow/tasks.rs), [workflow retry](../tests/workflow_retry.rs) |
| Node tools: receipts equal sent bytes, metadata-only views, attempts, retries, grants, content and checkouts | [node tool tests](../src/node_tool/tests.rs), [content](../src/node_tool/outputs.rs), [workspaces](../src/node_tool/workspace.rs) |
| Workspace capture/reopen/export and no-clobber/path validation | [artifact tests](../src/workflow/artifacts.rs), [workspace flow](../tests/workflow_artifacts.rs) |
| Session ownership, scoped targets, manager catalog, initialization recovery | [session ownership](../tests/session_ownership.rs), [catalog tests](../src/catalog.rs) |
| Detached server lifetime, abrupt death, receipts, stale instances, multiplexing | [server process tests](../tests/server_lifecycle.rs) |
| Malformed manifests isolated from healthy runs | [manifest recovery](../tests/state_recovery.rs) |
| Manager PTY ownership, detach/reattach, graph control | [terminal backend](../src/terminal.rs), [terminal client](../src/terminal_client.rs) |
| Retained core adapters, bounded content reads, workspace checkpoints | [tool module tests](../src/tools/), [management flow](../tests/management_flow.rs) |
| Manager allowlist, session defaults, no replay, receipts, bounded output | [Pi tests](../pi/test/) |

[VERIFICATION.md](VERIFICATION.md) records the earlier manager/session release
checks. That history and deterministic worker fixtures do not establish live
Codex authentication, model execution, or interactive worker-terminal acceptance.
