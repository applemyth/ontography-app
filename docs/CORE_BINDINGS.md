# Core bindings and coverage

This inventory maps [PLAN.md](../PLAN.md) to the implemented public core adapters. The authoritative operation names and JSON schemas are returned by `ontography call system.hello --args '{}'`. Pi exposes model-facing management operations; internal session/terminal controls remain extension or CLI responsibilities; `ontography_tools` activates capability groups.

## Ownership

One Ontography app session owns Pi manager state, a native manager terminal, and one graph run after initialization. Pi owns native conversation semantics; the app records membership, active selection, tool preferences, and graph association. The detached Rust server retains processes, PTYs, accepted requests, core runtimes, executable hosts, and handles. Core owns admission, graph transitions, occurrence history, content, and context records. Client detach preserves these resources. See [session ownership](SESSIONS.md).

The app imports the sibling core through its public Rust API. It creates manifests and graph declarations; it never reads or modifies core's private SQLite representation. The binary fingerprints app/core source. Handshakes require the exact app/core build, and run reopening requires the stored core build and original declaration/catalog identities.

## Capability matrix

Operation names in a row share the indicated prefix. Argument schemas define the precise required fields, defaults, and bounds.

| Planned family | App operations / inputs and results | Public core binding / ownership |
| --- | --- | --- |
| System and catalog | `system.hello`, `system.status`, `catalog.list`; versions/builds, server identity, schemas, registered implementation descriptors | App metadata and `ImplementationRegistry`; validators are explicitly versioned opaque-byte and UTF-8 implementations. |
| App sessions | `session.create/list/inspect/select/adopt/resume/suspend/close/context/conversation/preferences` | App-owned records, conversation membership, lifecycle admission, graph binding, and durable initialization intents around existing core runs. |
| Manager terminals | `terminal.ensure/status/graph/detach` plus attachment protocol | Server-owned native Pi through `portable-pty`, screen state through `vt100`, and one controlling client. Concrete node terminals remain deferred. |
| Definitions | `graph.save/get/list/validate/import/export`; versioned declaration, immutable revision hash, admitted topology, explicit file paths | `Schema`, `Graph`, contract/definition/rule constructors, `Kernel::admit`, `RewriteGrammar`; drafts persist independently from runs. Import parses and saves a draft without admitting it; export writes a saved revision to the requested file. |
| Durable runs | `run.start/list/inspect/suspend/resume/close`; explicit project directory and run ID | `ProposalRuntime::create_persistent/open_persistent`, `SessionHandle`, `ExecutionHost`; retained `ManagedRun`. Suspension drops owners and preserves admission. Closure calls the selected session's `close`. |
| Fixed graph facts | `run.export_facts`, `run.restore_facts`, `run.verify`; absolute files, exact activation/result/output data, verification summary | `State::to_parts`, public `Activation`/`Output` constructors, `ProposalRuntime::restore/open_persistent_verified`. Restore is transient validation/inspection with no managed run; see scope below. |
| Executable hosting | `execution.launch/list/inspect/activity/stop/abort/wait/release`; registered ID/version/config, node ID, execution handle | `ExecutionHost::launch`, `ExecutionHandle` status/activity/stop/abort/wait. Declaration bindings restart on resume; manually launched bindings have live-run lifetime. Initial production catalog has no worker implementation. |
| Rewrite grammar and live graph | `rewrite.list/prepare/inspect/commit/discard`; production/match, plan ID, base revision, exact resulting topology and retirements | Retained `SessionRewrite` from `SessionHandle::prepare_rewrite`, applied by `commit_rewrite`. Preparation has no graph mutation. Intervening transitions make a plan stale. |
| Workflow | `workflow.submit`, `workflow.transfer`; root/package/join triggers, results, emissions, authority, content dependencies | `ActivationProposal`, `Emission`, session `submit_with_content` and `transfer`; core enforces contracts, authority, consumed inputs, custody, and one delivery. |
| Bounded observations | `inspect.frontier/trigger/package/activation/activation_content/wait_frontier`, `run.inspect` | Session frontier overview/pages, trigger selection, package history, artifact references, and frontier watch. Activation inspection currently materializes a complete snapshot before returning a bounded result preview. |
| Explicit history export | `inspect.export`; full graph/occurrence snapshot to an absolute file | `SessionHandle::try_snapshot`; diagnostic export includes occurrences, not a restorable complete session backup. |
| Content import and reads | `content.import_bytes/import_file/import_stream/metadata/resolve/read/export`; content identity or local path | `ContentStore`; file-backed stream import uses 64 KiB buffers. Bounded reads are at most 64 KiB. Digest resolution verifies the payload commitment; it is distinct from an iroh hash. |
| Retention and collections | `content.retain/release/gc/hash_sequence_put/hash_sequence_read/collection_put/collection_read` | Public content retention, GC, hash-sequence and native named-collection APIs. Checkpoint and live checkout references remain protected. Native collections differ from semantic package documents. |
| Semantic package documents | `package.put/get/resolve/dependencies/retain/envelope/parse_envelope`; File, Collection, Changes, Symlink documents | `PackageStore`, package envelope helpers, complete representation dependency closure. `Changes` preserves base references and changed full blobs. Content IDs identify bytes; occurrence package IDs identify workflow deliveries. |
| Workspaces | `workspace.import/open/checkout/list/read_only/capture/checkpoint/restore/checkpoint_delete/release/diff/git_diff/git_merge` | `WorkspaceStore`, retained `Checkout`, diff/Git helpers. Capture has an explicit immutable base. Checkpoints retain the full dependency closure and persist in the run manifest. Merge conflicts remain explicit. |
| Invocation capability | `invocation.begin/list/inspect/submit/interrupt/fail/release`; node-bound trigger, context policy, retained invocation ID | Session `begin_invocation_with_content`, retained `InvocationHandle`, bounded durable invocation readers. Identical accepted bound publication retries return their original activation. Reopening interrupts unfinished invocations. |
| Context and receipts | `context.prepare/call/record/events/read/workspace/validate_output`; scoped grants, receipt sequence, bounded byte ranges | `InvocationHandle` initial context, granted package describe/parents/list/read, budget enforcement, output validation, durable context readers. Management-created receipts stay prepared. Trusted transport callbacks advance only observed delivery/tool-response states. |
| Network providers | `network.serve/serve_package/providers/ticket/stop`; explicit roots, endpoint config, allowed peers | Core provider builder and iroh endpoint; retained provider handles. Package serving publishes its complete dependency closure. Direct networking is default; public relay/discovery is explicit. |
| Network transfers | `network.download/downloads/progress/wait/cancel/release/status/discard`; ticket and retained download ID | Core verified download handles and local availability/pin APIs. Wait releases the run mutex; cancel settles work; release closes its endpoint and preserves resumable verified partial bytes. Transfer does not submit workflow occurrences. |
| Native/project configurations | `project.providers/describe/prepare/native_validate/start`; source document, format, project root, initial input, optional rewrite grammar | `ApplicationRegistry`, `ProjectRegistry`, trusted providers and native factories. `ApplicationDeclaration` pins catalog and resolved configuration/component metadata. Start calls `Application::start_in`; managed resume uses `Application::resume`; suspension consumes `RunningApplication::suspend`. Initial input is supplied only at creation. |

Implementation: [sessions](../src/sessions.rs), [manager ownership](../src/session_runtime.rs), [declarations](../src/declarations.rs), [catalog](../src/catalog.rs), [run ownership](../src/state.rs), [tools](../src/tools/), [trusted registry](../src/registry.rs), [application declarations](../src/application.rs).

## Wire and resource behavior

- JSONL frames are bounded to 4 MiB including the newline. Rust validates the published argument schemas. Large content and explicit history exports use file/content references.
- UUIDs and compound occurrence IDs are strings. Revisions, receipt sequences, content sizes, and other potentially wide counters preserve exact decimal strings across JavaScript.
- Every non-handshake request includes the expected server instance. A replacement server rejects a stale request before dispatch. Transient resource IDs must also resolve within the named run and resource kind.
- Session-bound requests also carry `app_session_id`. The server fills omitted session/run/project targets and rejects conflicts, including specialized execution/project/facts paths. Unscoped calls retain the administration API.
- Accepted mutations retain `(client_id, request_id)` outcomes in a bounded server table. Repeating the same request identity with the same operation/arguments returns its existing outcome; conflicting arguments reject. `operation.get` reports running, completed, failed, or absent/expired. Restart loses this transient table.
- A missing response is an unknown outcome. Neither Rust nor Pi automatically replays it. Pi cancellation stops waiting; accepted server work continues unless an explicit capability such as download cancellation or executable stop requests a change.
- Socket requests are multiplexed. Run resource mutations hold that run's ownership mutex. Waiting for execution, network progress, or frontier changes must leave controls available to other requests.
- Graph initialization additionally persists its reserved run identity and resolved source before core creation; retries reconcile that same run. General operation receipts still expire at restart.
- File targets are absolute or explicitly relative to the target run's stored project, as documented by each operation. The server's launch directory does not decide a client's target.

## Fact export and restore scope

`run.export_facts` writes a versioned file containing the original declaration, core identity, fixed-graph accepted activations, exact result bytes, package output metadata, and the exact payload evidence for every historical output, including consumed packages. IDs and digests are preserved. This intentionally materializes the complete history.

`run.restore_facts` reconstructs untrusted public `StateParts` and passes them with payload evidence to core's checked restore API. It returns a bounded frontier summary from a temporary in-memory session, then drops the session. It returns `run_id: null`, `durable: false`, and `retained: false`. Source admission status is reported as provenance; it is not restored.

`run.verify` requires released run ownership. It checks the same core/declaration/catalog compatibility as reopening, calls `open_persistent_verified`, then releases all owners without terminal runtime shutdown. The original suspended run remains resumable. This verifies fixed-graph activation history and payloads; it is not a claim of a complete artifact/context audit.

Core's checked fact export and historical verification reject histories with rewrites or explicit transfers. Ordinary `run.resume` restores those histories through core's trusted current-state loader. Core currently exposes no durable import of `StateParts`; adding durable imported runs requires an upstream public API.

Fact files exclude artifact dependency records and artifact bytes, invocation/context records, retained executable resources, and source admission lifecycle. Content/package exports or transfer tools handle artifact content separately. Neither fact export nor `inspect.export` is advertised as a complete run backup.

## Dependencies and remaining adapters

### Concrete worker layer, intentionally deferred

The production registry starts without concrete Codex workers, direct-message injection, app Message/Workspace/union contracts, node MCP, and worker transport acknowledgements. The management Pi PTY/session infrastructure is implemented separately. These are architecture items 1–7. Tests register deterministic implementations locally; those fixtures do not appear in the shipped catalog. Future adapters must record actual observed delivery and reconcile external processes after committed rewrites or restart.

### Core API limits

- Durable fact import and verified replay of dynamic rewrite/transfer histories are absent from the public core API.
- Live grammar/schema/trusted validator replacement is absent. Use the configured grammar or construct a new definition/run.
- Single-activation inspection currently requires materializing a full snapshot. The response preview is bounded; the underlying history read is not.
- Workspace checkout requires supported OS copy-on-write behavior. Advisory read-only permissions do not establish execution confinement. Writers must be stopped before capture.
- A trusted provider's description identity is not a code hash. The app pins expanded application configuration, component descriptions/specs, project identity, catalog versions, and core build. Registered adapters remain responsible for truthful immutable implementation identities and referenced executable resources.

### Release verification

The original release checked native Pi, the historical Rust/Pi RPC UI, and live graph tool calls. The session foundation adds manager PTYs, attachment, conversation ownership, scoped dispatch, and graph view switching. See [verification results](VERIFICATION.md) for completed checks and outstanding acceptance/cutover work; earlier evidence does not by itself validate the new session path.

## Verification evidence

| Boundary | Tests |
| --- | --- |
| Detached lifetime, simultaneous startup, abrupt process death, explicit recovery, expired handles, receipt recovery, stale server rejection, partial-frame multiplexing | [server process tests](../tests/server_lifecycle.rs) |
| Malformed/partial/missing manifests isolated from healthy run recovery and creation | [manifest recovery tests](../tests/state_recovery.rs) |
| App-session ownership, scoped targets, initialization recovery, adoption, conversation materialization, lifecycle waits | [session ownership tests](../tests/session_ownership.rs) |
| Manager terminal lifetime, exclusive controller, snapshots, query/input handling, detach and graph control | [terminal backend](../src/terminal.rs), [terminal client](../src/terminal_client.rs) |
| Governed delivery/consume/rejection, suspend/reopen, terminal closure, rewrite staleness and persisted topology | [management flow](../tests/management_flow.rs), workflow unit tests |
| Fixed fact restoration, tampered evidence rejection, verification ownership release, unsupported dynamic history | [fact adapter tests](../src/tools/facts.rs) |
| Content/package formats, closure retention, checkpoint/GC/reopen, merge conflicts | content and workspace module tests |
| Context grants/budgets, exact retry, truthful receipt progression | context module tests |
| Registered hosted work after disconnect, executable control; provider/native resolution | execution/project/application module tests |
| Local endpoint package transfer, cancellation and resource release | [network integration test](../tests/network_flow.rs) |
| Reconnect, no replay, cancellation, build mismatch, wide IDs, typed tools and bounded output | [Pi tests](../pi/test/) |
| Graph identity/rendering and Pi stream/child cleanup | Rust UI module tests |

The core has a known ignored orphan-retention regression probe after failed SQLite commit. App tests do not establish cleanup beyond the demonstrated retention/GC paths; see [core probe](../../ontography-core/tests/orphan_retention_probe.rs).
