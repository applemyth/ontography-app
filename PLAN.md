# Management layer implementation plan

Status: implementation plan; no application code implemented by this document. Scope: item 8 in [ARCHITECTURE.md](ARCHITECTURE.md). Items 1–7 supply the later concrete agent nodes, package semantics, edges, harnesses, and node MCP.

## 1. Outcome and ownership

`ontography` opens a Pi management interface connected to a persistent local Rust server. The server retains core runs and executes accepted operations independently of management clients. A user can define a graph, start a durable run, inspect it, submit permitted work, and rewrite its live graph through Pi tools. Graph visualization follows the working management interface.

The three pieces remain:

| Piece | Implementation responsibility |
| --- | --- |
| **1. CLI + Pi harness** | Installable `ontography` command; server discovery/startup; native Pi launch; management instructions; extension loading and reconnection. |
| **2. Ontography tools** | Typed tools; local Rust transport; retained core handles; declaration conversion; capability discovery; graph/run/content operations; durable reconstruction metadata. The detached server is the process hosting this integration. |
| **3. Graph TUI, later** | Rust client using Ratatui; present graph, frontier, and execution observations; invoke the same server operations for actions. |

```text
ontography                         detached Rust server
  └─ Pi management interface         ├─ definition / implementation catalog
      └─ Ontography extension ──────►├─ retained core runtimes and sessions
                  local socket       ├─ retained operations and resource handles
                                     └─ ontography-core
                                         ├─ graph admission and rewriting
                                         ├─ occurrence state and context records
                                         ├─ content / packages / workspaces
                                         └─ execution hosts when implementations exist
```

**Authoritative ownership:** Pi owns the management conversation. Core owns graph validity, workflow state, commits, content, and context evidence. The Rust integration owns process lifetime, wire conversion, implementation lookup, and the references needed to use core across tool calls.

**Confirmed lifetime requirement:** graph/node execution continues after the last management client exits; Pi reconnects when needed. Pi's own active turn has client lifetime. Initial implementation targets a local server on macOS/Unix.

**Language boundary:** the application, server, and graph TUI are Rust. Pi is the selected external management harness; its extension supplies the necessary tool bindings. Initial management uses Pi's existing terminal client. The later unified Rust UI can drive Pi over its RPC interface, with conversation rendering implemented in the Rust client.

## 2. Invariants

1. **Definition ≠ run.** A saved definition describes future runs. A run owns a particular history and current graph. Editing a saved definition never silently changes an existing run.
2. **Open ≠ executing.** Core admission status, attached executable status, and pending work are separate facts. An open logical graph may have no executables and no work.
3. **Client exit ≠ run suspension.** A socket disconnect removes subscriptions; it leaves runs and accepted server operations alive.
4. **Suspension ≠ closure.** Suspension releases active resources while preserving resumability. Closure terminally ends core admission.
5. **Live graph mutation uses the session API.** Mutating a detached `Kernel`/`State` does not mutate the owned run. All accepted live changes go through core session methods.
6. **Rewriting is governed.** A run's schema, trusted contracts, and rewrite grammar are fixed at construction. A rewrite applies an existing production to a valid match and commits against its prepared revision.
7. **Authority and validation remain core decisions.** Tool schemas validate transport structure; trusted core contracts decide admissible payloads. Management access does not bypass graph rules.
8. **Identifiers preserve their domain.** App run IDs, node IDs, package occurrences, content IDs, activations, invocations, and transient handle tokens remain distinguishable. Large integers cross JavaScript as decimal strings. Public run identity belongs to the app; core's private session registration ID is not a durable external identifier.
9. **Content storage and delivery are separate.** Importing/downloading bytes establishes content availability; workflow submission/transfer establishes governed occurrence and delivery.
10. **Observations state their scope.** Frontier revisions, execution activity, and context receipt sequences are distinct. Refresh after notification gaps; never fabricate a universal event sequence.
11. **Accepted operations belong to the server.** Losing a client response does not cancel or undo a committed operation. Mutation retry requires a known outcome or operation-specific retry semantics.
12. **Execution descriptions reflect implementations.** Logical graphs work immediately. Codex/tmux execution, direct message insertion, and node-scoped MCP become available when items 1–7 supply those implementations.

## 3. Process and lifecycle design

### 3.1 Startup and attachment

1. Resolve the selected data directory and project directory; canonicalize their identities. Use one server per OS user and data directory. Each client keeps its own project context.
2. Attempt a local socket handshake. If a compatible server responds, attach.
3. If absent, coordinate startup, recheck, start the server in an independent OS process session, and wait for readiness. The server holds its exclusive ownership lock for its entire lifetime; any client startup lock is separate and temporary. Concurrent clients must converge on one owner.
4. Give the server detached standard streams and a bounded log destination. Its lifetime must survive terminal closure and client signals. Use a short socket path derived from data-directory identity where Unix path limits require it.
5. Protect the local endpoint with user-only filesystem permissions. Preserve core's exclusive per-run storage ownership in addition to the server lock.
6. Handshake on protocol version, server instance ID, app/core build identity, supported operations, and capabilities. Report incompatible versions; preserve the existing server and its runs.
7. Launch native Pi in the client's project directory with inherited terminal stdin/stdout/stderr, the Ontography extension, and appended management instructions. Tool protocol traffic uses the separate socket. Pi owns terminal rendering, authentication, and model selection. [Pi CLI](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/cli.md)
8. Extension startup connects and refreshes state. Extension shutdown releases its connection and subscriptions. Conversation replacement, reload, resume, and fork must reconnect without resetting runs. [Pi lifecycle types](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/src/core/extensions/types.ts)

Proposed CLI surface:

| Command | Meaning |
| --- | --- |
| `ontography [--data-dir …] [--project …]` | Ensure a server exists; open the management interface. |
| `ontography server start` | Ensure the server exists without opening Pi. |
| `ontography server status` | Inspect discovery/version/liveness without implicitly starting it. |
| `ontography server stop` | Gracefully suspend resources, preserve durable runs, and exit the server. |
| `ontography server run` | Run the server in the foreground for development and supervision. |

### 3.2 Lifecycle operations

| Action | Effect |
| --- | --- |
| Exit Pi / disconnect / close terminal | Client connection ends; graph runs, invocations, and accepted operations continue in the server. |
| Cancel a Pi turn | Cancel eligible requested work; report committed or uncertain outcomes accurately. An already-started run remains alive. |
| `run.start` | Create a durable core session from an admitted definition; launch only explicitly registered executable bindings. |
| `run.suspend` | Stop owned executions, checkpoint managed workspaces, release transient resources and storage ownership; preserve open core admission. |
| `run.resume` | Reconstruct the fixed definition environment, open stored current state, and restore supported execution bindings. |
| `run.close` | Stop executions and terminally close admission; preserve readable state/history. |
| `server.stop` | Stop accepting new operations; finish or explicitly settle accepted work; suspend runs; finalize resources; release locks and exit. |
| Server crash / machine restart | Durable committed state remains recoverable. Transient handles expire. Initial policy: list recoverable runs and resume explicitly. |

Core's `ProposalRuntime::shutdown` closes admission terminally for every session it tracks. `run.close` targets the selected `SessionHandle::close`; it must preserve unrelated runs. Ordinary server stop uses resumable release semantics; `RunningApplication::suspend` supplies executable-future stopping and release for the high-level path. Workspace checkpointing is additional integration work. Lower-level hosting requests cooperative stop, waits a bounded grace period, aborts remaining hosted futures, and releases retained objects without closing the session. External-process cleanup requires its registered execution adapter. [Core lifecycle](../ontography-core/src/runtime/session.rs), [application suspension](../ontography-core/src/application.rs)

An intentional client disconnect needs no recovery: the server still owns the same objects. Server restart reconstructs objects from durable data. Automatic login startup and automatic process resurrection can be added separately.

### 3.3 Multiple clients and resource ownership

- Every run operation names its run. Selected graph/run IDs belong to each Pi client; the server has no shared mutable selection.
- Core serializes session operations. Preserve stale rewrite rejection and other admission checks when clients race. Add expected revisions only where the app can enforce their stated semantics.
- Before suspending or closing a run, block new management mutations/resource creation for that run, settle in-flight tool work, stop its executions/writers, then checkpoint and release resources. Keep inspection available with an explicit transition status. This guards app-resource teardown in addition to core's transaction serialization.
- Retain runtimes, sessions, execution hosts, invocations, rewrite plans, checkouts, imports, downloads, and providers independently of socket handlers.
- Expose list/inspect/release operations for retained resources. Bind every token to its server instance, owning run/store, and resource kind. Reject foreign or expired tokens explicitly.
- Complete suspension only after owned tasks finish and all run-scoped session/store owners are released, including workspace stores, readers, providers, and downloads. Releasing the primary session alone can leave a storage engine alive. Confirm immediate resume in the same server can reacquire the run cleanly.
- Prepared plans may be discarded after staleness or bounded idle retention. Active invocations and editable checkouts require explicit lifecycle handling; connection loss cannot release them.
- Stop all managed writers before workspace capture or release. Keep server-owned checkouts alive while execution futures stop; future node adapters must stop real writers before releasing their workspace handles. On graceful suspension, capture retained managed edits, retain their dependency closure, and save checkpoint roots before dropping checkouts. A checkpoint saves work; publication remains a separate workflow operation. If checkpointing fails, report suspension failure and keep the server and affected recovery resources accessible.
- Checkpoints record the original base, captured root, and full dependency closure. Track those app-owned references and protect them from unrelated release/GC operations: core `retain` is idempotent, while `release` removes a shared artifact tag rather than decrementing a reference count. Release that tag only when no app-owned checkpoint/resource still requires it; core retains committed ledger dependencies separately. Persisted checkpoint metadata alone does not pin bytes. [Content retention](../ontography-core/src/content.rs)
- Core removes a checkout directory when its handle drops and interrupts an unfinished invocation when its final handle drops. These are resource semantics the integration must preserve. [Checkout ownership](../ontography-core/src/workspace/mod.rs), [invocation ownership](../ontography-core/src/context.rs)

## 4. Definitions, implementations, and persistence

### 4.1 Saved graph declaration

Implement a versioned serializable declaration that maps directly to core constructors:

| Field group | Content |
| --- | --- |
| Identity | Format version, definition ID, immutable saved revision/hash. |
| Schema | Node types, object types, authority vocabulary. |
| Contracts | Contract ID/object type plus registered validator identity, version, and supported configuration. |
| Rules | Node definitions, edge definitions, roots, authority transitions. |
| Initial topology | Concrete nodes and directed edges. |
| Rewrite grammar | Productions, left/right fragments, preserved interface, required schema references. |
| Optional execution bindings | Concrete node → registered implementation/configuration, when available. |

Draft operations load/save/replace declarations; validation constructs the actual core types and calls `Kernel::admit`. Preserve field-specific failures; reject duplicate keys, invalid references, unsupported versions, and unknown implementations. Save revisions atomically; do not treat a successful file write as successful admission.

Core's `ApplicationConfig`/`ApplicationRegistry` and `ProjectConfig`/`ProjectRegistry` support executable application composition. Expose them when registered providers exist. Their required implementations make them unsuitable as the only path for logical graphs. The logical declaration is a wire/storage adapter for public core constructors. Both existing configuration paths need a grammar envelope because neither serializes rewrite grammar. [Core configuration](../ontography-core/src/config.rs), [projects](../ontography-core/src/project.rs)

### 4.2 Trusted catalog

- Implement an explicit catalog of validator and executable/provider adapters. Each entry identifies implementation, semantic version, supported configuration, and availability.
- Initial validators: opaque bytes and UTF-8 text, with truthful semantics and stable IDs. They enable real logical graph operations before semantic Message/Workspace contracts exist.
- A contract declaration selects trusted validator code; a JSON schema alone does not become a core validator. Preserve the accepted payload set under an existing validator identity/version.
- Register future Codex/message/union/edge semantics through this catalog when implemented. Reject missing adapters and describe the missing dependency.
- Store catalog descriptors used for construction and discovery. Core does not publicly enumerate every schema vocabulary, registry entry, or rewrite fragment. Present retained construction metadata with its source/version; query current topology from the live session.

### 4.3 Durable state

Proposed storage shape:

```text
<data-dir>/
  server.lock
  logs/
  definitions/<definition-id>/<saved-revision>.json
  runs/<app-run-id>/
    manifest.json
    core/                         created and owned through core APIs
      state.sqlite3
      objects/
    checkpoints.json             app-owned references to retained content
  workspace-cache/
```

The manifest stores app run ID, format version, exact admitted declaration and grammar reference/hash, validator/provider identities and versions, core compatibility identity, project root, and core run path. A saved definition update never retargets an existing manifest. Core owns current graph, workflow state, payloads, and context records; the app records only reconstruction/configuration/resource metadata.

Create the app run directory and manifest around a new `core/` path; core's persistent creation requires a new run directory. Detect partially completed creation and report/reconcile it. App metadata writes and core commits are separate transactions; design explicit reconciliation rather than claiming cross-store atomicity.

On resume: verify compatible catalog → reconstruct initial admitted environment and grammar → call `open_persistent` → use the stored current graph → report actual admission and execution state. A mismatch blocks that run's opening with an actionable error; it does not reinterpret old contracts.

Core marks unfinished invocations interrupted on reopen. Old invocation capabilities and prepared rewrite handles cannot be resurrected. Verified historical replay currently supports fixed-graph activation histories; routine reopening of rewritten/transferred runs uses core's trusted current-state loader. `restore(StateParts, …)` is a graph-fact operation, not a complete artifact backup. [Persistent session APIs](../ontography-core/src/runtime/session.rs), [reopen behavior](../ontography-core/src/runtime/sqlite.rs)

## 5. Bindings to the existing core API

### 5.1 Capability inventory

`ontography-core` already establishes the graph/runtime API. Implement Pi tool and transport bindings to those public operations, preserving their semantics. Maintain `docs/CORE_BINDINGS.md` alongside implementation: record the core calls, serialized arguments/results, retained handles, and connection-specific behavior for each binding. Names below organize tool coverage; core remains the API authority.

| Family | Operations to expose | Core mapping / qualification |
| --- | --- | --- |
| `system`, `catalog` | Capabilities, versions, server status, resource discovery, supported validators/providers. | App metadata and registered adapters. |
| `graph` | Draft import/export/save, inspect, validate/admit; roots/rules/topology; grammar declaration. | `Schema`, `Graph`, definitions, `Contract`, `Kernel::admit`; configuration/project adapters where available. |
| `run` | Start/list/inspect/resume/suspend/close; explicit fact restore and supported verification. | `ProposalRuntime`, `SessionHandle`; high-level `Application` where bindings exist. |
| `execution` | Launch/list/status/activity/stop/abort/wait. | `ExecutionHost` and handles; only registered implementations. Production Codex adapter is deferred. |
| `rewrite` | List configured productions; prepare match; inspect next graph and exact retirements; commit/discard. | `RewriteGrammar`, `RewriteRequest`, `SessionRewrite`, session prepare/commit. |
| `workflow` | Root/package/join proposals, result and emission submission, outbound transfer. | Session submit/submit-with-content/transfer and core proposal types; authority/contract rejection preserved. |
| `content` | Import bytes/file/stream, metadata, bounded read/export, resolve digest, retain/release/GC; native collections. | `ContentStore`; distinguish native content collections from semantic package documents. |
| `package` | Compose/read File/Collection/Changes/Symlink documents; resolve full view and dependency closure. | `PackageStore`; workflow occurrences and content roots remain distinct. |
| `workspace` | Import/open/checkout/capture/diff/three-way merge/checkpoint/release. | `WorkspaceStore` and retained `Checkout`; surface conflicts and filesystem constraints. |
| `network` | Serve selected content roots, issue tickets, download/progress/cancel/release/discard. | Iroh endpoint configuration plus core provider/download APIs. Blob transfer does not imply workflow delivery. |
| `invocation`, `context` | Begin/inspect/list, grants, prepare/read/list context, call/receipt lifecycle, bound submit, interrupt/fail. | Retained scoped `InvocationHandle`; describe/package/list/read/parents and context records. |
| `inspect` | Current topology/frontier, pending/outbound pages, package history, invocations/events, execution activity; explicit full export. | Core bounded queries and snapshots; current graph comes from session. |
| `project` | Provider/component discovery, validate/prepare supported application configurations. | Core project registry and trusted providers; expose availability explicitly. |

“Full core tooling” means every public capability family has a usable adapter or an explicit documented limitation/dependency. Rust closures and resource handles require registered implementations and retained objects. Internal helpers need not each become model tools.

### 5.2 Wire behavior

- Use a versioned, bounded JSONL protocol over the local socket. Requests carry client/request identity, operation, typed arguments, and explicit target IDs; responses carry typed success/rejection/error and relevant core revisions.
- Distinguish invalid arguments, unavailable capability, core admission rejection, stale/foreign handle, storage/runtime fault, cancellation, and unknown outcome. Preserve useful core details; keep model-facing explanations short.
- Validate serialized inputs in Rust as well as Pi. Derive wire/tool schemas from the existing core inputs and results; add representations for retained handles where needed. Keep Rust and TS bindings consistent through generated schemas or shared fixtures.
- Store accepted mutations/long operations in a server-owned request table. A reconnecting client can inspect the original outcome by request ID while that server instance survives. Bound retention and report expiration.
- Correlation IDs alone do not make operations idempotent. Never automatically replay a mutation after an unknown outcome. Core's exact accepted invocation retry can be exposed under its own narrow rules. Server-crash ambiguity requires state reconciliation.
- Cancellation is operation-specific: cancel a download when supported, stop waiting when appropriate, and report a completed commit if already accepted. Client EOF only removes that connection.
- Include server instance ID in tokens and request receipts. After restart, invalidate transient capabilities explicitly.
- Bound reads, pages, previews, and message frames. Return content IDs, operation IDs, or local export paths for large results; avoid injecting complete repositories or histories into Pi context by default.
- Keep observations typed: frontier revision change → query state; context events use their sequence; execution activity uses its own observation channel. Reconnect obtains a fresh snapshot.
- All paths are absolute or explicitly relative to the named project root. The server's launch directory must never determine another client's filesystem target.

### 5.3 Pi integration

Use a native Pi extension. Register tools in the factory; open connections during `session_start`; clean up idempotently during `session_shutdown`. Register all supported tool definitions, then activate useful capability groups through a compact discovery tool. Preserve the user's native tool selection. Return concise `content` plus structured `details`; throw from `execute()` for failed operations, preserving structured error information in the mapping. An error-shaped successful result is insufficient. [Pi extensions](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/extensions.md)

Append instructions explaining definitions/runs, explicit targets, lifecycle verbs, core rejections, rewrite prepare/commit, capability discovery, and bounded context inspection. Pi conversation restoration reloads server state; it never replays graph mutations.

Pin and verify an exact Pi version and its extension API. Package the extension and instructions with the Rust command using a stable install-relative lookup; validate prerequisite/runtime versions at startup. The installer must place these assets explicitly; `cargo install` alone installs the binary. Development installation can use a locally installed compatible Pi; do not silently install or upgrade global dependencies during ordinary launch.

## 6. Implementation sequence

Start with the server and Pi connection, binding existing core operations incrementally. For each capability, inspect its public signature and ownership requirements, implement the required conversion, and verify the binding against core. Record coverage and limitations as implementation proceeds.

### Phase 1 — Persistent server and native Pi

**Deliver:** Cargo executable using sibling core, server entry point, discovery/start/stop/status, locked local socket, versioned handshake, Pi extension, capability/status tools, install/dev launch instructions.

**Work:** depend on the existing core package; detach server from terminal; isolate logs; retain server state independently of connections; add request correlation; make Pi connection lifecycle idempotent; keep authentication/model settings native. Record dependency versions and the initial core bindings alongside this work.

**Gate:** cold launch opens Pi; a tool reaches Rust; two simultaneous clients find one server; Pi exit and terminal closure leave it alive; a new client reconnects; `/new`, reload, resume, and fork preserve server identity. Version mismatch and stale endpoint failures are actionable.

### Phase 2 — Author graphs and retain durable runs

**Deliver:** declaration constructors/converters, validator discovery, draft save/load/admit, persistent `run.start/inspect/suspend/resume/close`, reconstruction manifest, initial bounded inspection.

**Work:** use lower-level `Kernel` + `ProposalRuntime` for logical graphs. Retain actual session objects. Start with no production executors; expose that fact. Preserve the original admitted declaration separately from current runtime topology.

**Gate:** author and admit a small graph → start an idle durable run → inspect across independent calls → exit/reconnect → observe the same live run → explicitly suspend/resume → observe the same committed state. Invalid graphs mutate nothing. A competing storage owner fails cleanly. Closing one run preserves unrelated runs and remains terminal after restart. A concurrent client cannot create new run resources during suspension.

**First usable milestone:** the management agent can create and manage a real persistent core graph. No Codex node implementation is required.

### Phase 3 — Governed work and observable continuity

**Deliver:** proposal/emission/transfer tools, package occurrence inspection, server-owned operation receipts, execution-host adapter with test-only executables, concurrent-client behavior.

**Work:** support permitted root/package/join triggers and content-bearing submissions; preserve authority, contract, custody, and one-delivery semantics. Retain accepted operations across disconnect. Add small deterministic test executables solely to exercise background hosting.

**Gate:** root activation → emission → delivery → consumption produces the expected history; rejected payload/authority leaves the frontier unchanged. A fixture continues producing observable work after the last client exits. Reconnect during a lost response resolves the same operation without a second submission while the server survives.

### Phase 4 — Live graph rewrites

**Deliver:** grammar declaration/discovery, match input, prepare/inspect/commit/discard tools, exact resulting topology and retirement reports.

**Work:** retain `SessionRewrite` objects in the server. Return plan ID, owning run, base revision, proposed graph, and every package retirement/reason. Commit through `SessionHandle`; report resulting revision. Retire consumed/stale plan tokens. Preparation/commit are technical protocol steps; they do not add a mandatory human approval step.

**Gate:** prepare has no mutation; valid commit changes current topology and expected frontier together; another client's intervening mutation yields stale rejection; foreign plans reject; draft definition remains unchanged; server restart restores rewritten topology from core storage.

**Boundary:** introducing a production/type/validator absent at run construction requires a new definition/run or a deliberate core feature addition. Existing APIs do not provide arbitrary grammar replacement on a live run.

### Phase 5 — Packages and workspace control

**Deliver:** content/package/workspace families, explicit retention and release, bounded exports, checkout ownership, capture/checkpoint, diff/merge and conflict results.

**Work:** route storage through core; retain complete dependency closure; keep IDs distinct; carry explicit capture bases; preserve full changed file blobs and unchanged references. Capture after stopping managed writers. Verify OS copy-on-write support and surface unsupported filesystems honestly. Preserve modified managed workspaces across graceful suspension.

**Gate:** import directory → package → checkout → edit → capture → reopen yields expected files, deletions, symlinks, and unchanged references. A reconnect preserves the same live checkout. Checkpoint → release incidental pins → GC → graceful server stop/restart restores checkpointed contents. A run with workspace/content owners can suspend and immediately resume in the same server. Merge conflicts remain explicit. Releasing incidental pins cannot remove accepted publication dependencies.

**Boundary:** this exposes core's complete content/context carrier. Semantic direct-message delivery and Codex workspace installation remain implementations of items 2–7.

### Phase 6 — Invocation/context, networking, and remaining core coverage

**Deliver:** scoped invocation/context and receipt tools; content serve/download controls; configuration/project adapters; complete audited capability matrix.

**Work:** retain invocation capabilities; expose explicit grants and bounded context operations; preserve bound publication and exact retry behavior; record only evidence the host actually observes. Create/configure iroh endpoints and retain provider/download handles. Surface progress/cancellation. Expose provider/application paths only for registered components.

**Gate:** wrong-run/invocation use rejects; context budgets/grants hold; bound submit consumes the correct inputs; exact accepted retry returns the accepted result; receipt ordering is valid; restart marks unfinished invocations interrupted. A two-endpoint local test transfers a retained package dependency closure and resolves it. Download cancellation/status and provider shutdown work independently of workflow transfer. Suspending with active readers/providers/downloads releases ownership sufficiently for immediate same-server resume.

**Coverage gate:** every planned in-scope adapter is implemented and tested. Unavailable capabilities identify a concrete dependency on items 1–7 or an absent core API; documentation cannot substitute for unfinished adapter work. Desired APIs absent from core become small explicit upstream proposals, never private SQLite access or duplicated semantics in the app.

### Phase 7 — Complete the management experience

**Deliver:** concise instructions and tool descriptions, progressive tool activation, selected-run metadata, lifecycle/status commands, installation docs, troubleshooting, meaningful failure messages.

**Work:** exercise a complete user flow through native Pi; tune bounded results and discovery; make shutdown/reconnect/resource cleanup coherent; package the binary plus extension; document crash recovery and supported versions.

**Gate:** a user can author, start, inspect, operate, rewrite, detach, reconnect, suspend, resume, and close a supported graph through the management interface. Most verification uses deterministic Rust/transport fixtures; one interactive Pi smoke test verifies integration with the user's existing model configuration.

### Phase 8 — Graph TUI

**Deliver:** Rust Ratatui client with topology/frontier display, selected node/package details, admission/execution status, rewrite preview, and actions through existing server bindings. [Ratatui](https://ratatui.rs/)

**Graph drawing:** evaluate the Rust `tuiflow::GraphCanvas` component, which provides node boxes, orthogonal edge routing, selection, and viewport integration with Ratatui. Treat it as a candidate until a prototype verifies cycles, parallel edges, stable ID mapping, node placement, resize, and live topology replacement. Its view document represents server state; graph edits must invoke the existing core bindings. Any missing rendering behavior belongs in a Rust widget. [tuiflow](https://github.com/kraemahz/tuiflow)

**Pi presentation:** for a unified graph/conversation screen, the proposed Rust client owns the terminal and launches Pi in RPC mode. Implement prompt input, streamed messages/tool results, cancellation, session controls, and supported extension interactions in Rust. Pi continues to own model/tool execution and conversation semantics. This is additional frontend work in this phase. [Pi RPC](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/rpc.md)

**State flow:** consume the same server observations; coalesce refreshes; recover from gaps. Distinguish current graph from saved definition and live execution from open admission. Keep Pi RPC and the Ontography server protocol as separate connections with distinct purposes.

**Gate:** display matches authoritative core state, including another client's rewrite; selecting a node resolves its real pending work/history; graph and conversation remain responsive; display actions use the same validation/commit path. Verify Pi RPC streaming, cancellation, session restoration, child exit, and terminal cleanup. Closing the Rust client ends its Pi process while server-owned graph execution continues.

Phase order gives early proof of the hardest boundary: persistent server + real core runs. Phases 5 and 6 can be split by capability family once transport/ownership semantics are stable. Visualization follows the working management tools.

## 7. Repository shape

```text
Cargo.toml
src/
  main.rs                  CLI and server entry points
  server.rs                ownership, connections, readiness, shutdown
  protocol.rs              request/result/error schemas and dispatch
  state.rs                 retained core objects and resource lookup
  declarations.rs          serializable inputs → core constructors
  catalog.rs               trusted validator/provider adapters
  persistence.rs           app manifests and reconstruction metadata
  tools/
    graph.rs  runs.rs  rewrites.rs  workflow.rs
    content.rs  workspace.rs  context.rs  inspect.rs
  ui/                      later Rust frontend
    mod.rs  graph.rs  pi_rpc.rs
pi/
  package.json             exact compatible dependency versions
  index.ts                 extension registration and lifecycle
  client.ts                socket protocol and reconnect
  tools.ts                 typed Pi tool bindings
  instructions.md          management-agent instructions
tests/
  management_flow.rs        core/bridge lifecycle and workflow scenarios
  server_lifecycle.rs      real process detach/reconnect/concurrency
docs/
  CORE_BINDINGS.md
  INSTALL.md
```

These are initial module boundaries; split networking/project support when needed. Begin with a single Rust package and one Pi extension. Dependency:

```toml
ontography = { package = "ontography-core", path = "../ontography-core" }
```

Pin a compatible Rust toolchain and commit the app lockfile. Keep code using public core APIs; update the sibling core only through a separately identified change if a required capability is missing.

## 8. Verification and completion

Prioritize integration boundaries rather than restating core unit tests:

| Boundary | Required evidence |
| --- | --- |
| Client/server lifetime | Last client exits; deterministic hosted work continues; new client sees the same run. |
| Startup ownership | Simultaneous launches yield one server; core rejects another owner of an active run. |
| Transport/concurrency | Wide IDs survive JSON; accepted work survives socket loss; uncertain mutations are not replayed; stale plans reject. |
| Definitions/persistence | Invalid admission leaves no run; exact trusted catalog reconstructs; rewrites survive reopen; incompatible versions fail clearly. |
| Workflow/governance | Accepted activation/delivery/consume sequence; contract and authority rejection preserve state. |
| Resources/content | Handle lifetimes preserve checkouts/invocations; checkpoints survive graceful stop; dependency closure remains available. |
| Recovery | Explicit suspension is resumable; closure terminal; crash recovery exposes interrupted invocations and expired plans. |
| Pi | Native launch, tool invocation, lifecycle reconnect, bounded results, version/install diagnostics. |
| Later display | Current graph/frontier remains correct across external mutation and reconnect. |

Use core fixtures and local endpoints for deterministic tests. Run focused tests per phase; run compilation/lint and the full relevant integration suite at the release gate. Model calls are unnecessary for automated graph semantics tests.

Current core exposes a known ignored test for orphan retention after a failed SQLite commit. Track this when qualifying GC/failure behavior; do not claim failure cleanup beyond demonstrated behavior. [Core regression probe](../ontography-core/tests/orphan_retention_probe.rs)

**Management completion:** phases 1–7 pass their gates; every in-scope planned adapter is implemented and tested; client exit preserves ongoing graph work. Only explicitly named dependencies on unfinished items 1–7 or absent core APIs remain unavailable. **Item 8 completion:** graph visualization also passes phase 8. **Agent-network completion:** items 1–7 subsequently provide the actual Codex nodes, package semantics, delivery harnesses, and scoped MCP.

## 9. Integration after items 1–7 exist

1. Register real Codex/tmux executable bindings and semantic Message/Workspace/union validators with stable versions.
2. Add their precise management controls and discovery metadata to the existing catalog/tool surface.
3. Expose node/invocation-scoped operations through the node MCP; management tools retain graph/run scope.
4. Implement process reconciliation after committed graph rewrites: stop/remove obsolete executions, retain valid ones, launch new bindings. Report failures separately from the already-committed graph transition.
5. Implement message injection and workspace/context installation in the receiving harness; record actual transport evidence and publication through core.
6. Implement restart reconciliation for external tmux/Codex processes and their managed workspaces. Preserve node identity independently of process identity.

The existing lower-level `ExecutionHost` can launch registered executables at current graph positions. `RunningApplication` launches its compiled bindings and does not expose generic dynamic launch; choose the lower-level host for eventual rewrite reconciliation. [Core execution hosting](../ontography-core/src/runtime/hosting.rs)
