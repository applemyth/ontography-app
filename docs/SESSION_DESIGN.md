# Session design and implementation

**Status:** the persistent manager-session foundation is implemented and verified, including the local user-home migration. Concrete worker nodes and default app editing productions remain deferred. [VERIFICATION.md](VERIFICATION.md) records the evidence; [SESSIONS.md](SESSIONS.md) gives commands and usage.

## Ownership invariant

One Ontography session owns:

- Pi manager state: its conversation collection, active conversation, native saved histories/context, app tool preferences, and resume references.
- One core graph run/runtime once initialized; otherwise explicit pending initialization.
- A fixed project association and the live manager terminal when running.

Several sessions share a Rust server but retain separate manager state and graph identities. Pi owns native conversation semantics; core owns graph/workflow semantics. Ontography records their association. Pi conversation branching does not merge histories, copy the graph, or rewind committed state.

A process ID, connection, or terminal-instance ID is not a durable session identity. Native history supports conversation resumption after process loss; it does not restore arbitrary process memory or in-flight work.

## Agreed session, graph, and terminal ownership

| Piece | Responsibility | Implementation |
| --- | --- | --- |
| Ontography server | Own app sessions, core runtimes, manager processes, and terminals across client attachments. | Implemented. |
| Ontography session | Group Pi state, project, optional graph binding, and manager terminal. | Implemented. |
| Graph runtime | Govern nodes, edges, contracts, packages, workflow, authority, and rewrites. Coordinate nodes collectively. | Existing core bindings implemented. |
| Manager execution | Run native Pi before and after graph initialization. | Implemented with `portable-pty`. |
| Node execution | Run a concrete agent associated with a durable graph-node ID. | Registered execution infrastructure exists; concrete Codex adapter deferred. |
| PTY and terminal state | Host terminal bytes/dimensions; continuously read output and retain a current screen. | `portable-pty` and `vt100` implemented for the manager. |
| Pane/view | Display existing execution and route controlling-client input. | Manager and graph view switching implemented; worker panes deferred. |

The intended worker relationship remains node → execution → PTY → client view. Each node has an execution inside one collective graph runtime. Node identity persists across process restart and view closure.

## Implemented launch and initialization

1. `ontography` connects to or starts the server for its canonical data directory.
2. Bare launch always creates a session with a reserved initial Pi conversation UUID and pending graph initialization. `new NAME` names it. `attach NAME_OR_ID` or bare `--session NAME_OR_ID` explicitly resumes an existing session. IDs take precedence; names must match exactly and uniquely. Persisted selection does not control default launch.
3. Attachment resumes the selected graph when present and launches/reuses its native Pi manager. Only one controlling attachment is admitted per terminal.
4. Pi's first scoped `run.start` or `project.start` persists a run identity and resolved initialization source before creating core storage. The resulting run is bound to the app session. Retry/recovery reuses that identity.
5. `/graph` switches the attached client's view to the bound run, or pending initialization. Returning restores the same Pi terminal.
6. Ctrl-B, then D detaches the client; Pi, graph resources, and terminal output handling remain server-owned.

`session new --no-attach` creates records without starting Pi. Existing runs require explicit adoption into an uninitialized session with the same project; no old Pi ownership is inferred.

Future node creation will commit through core's configured grammar, then reconcile/provision agent execution. Nodes present in initial graphs also need provisioning. A startup failure preserves committed graph state and remains visible for recovery. Opening a worker view must not create a node, edge, or package delivery. These worker mechanisms are not part of the current manager foundation.

## Pi integration

Native Pi runs with the owning session's conversation directory and active history or reserved UUID. Extension hooks register startup/new/fork/resume state, check resume membership, refresh current graph context, and persist tool-group preferences. Completion callbacks record history materialization. The extension's shutdown event closes its bridge; it does not close the app session or graph.

| Action | Current relationship |
| --- | --- |
| `/new` | Add a conversation; retain the app session and graph. |
| `/resume` | Select owned history; retain the current graph. |
| `/fork`, `/clone`, `/tree` | Apply native conversation branching/navigation; retain graph state. |
| Pi exit | End its process; preserve manager history, app-session identity, and graph. |
| Attach afterward | Resume the active saved conversation in a new process if necessary. |
| Switch app sessions | Select that session's own manager state and graph together. |

Ontography tool groups are shared app-session preferences. Model/thinking history, compaction, native settings, and credentials retain Pi's own scope. The preferences record does not turn every native setting into a session-wide default.

Pi can report a history path before the file exists. Records distinguish an unmaterialized conversation from missing previously saved history. Foreign histories require explicit import; loading one does not silently rebind the graph. Older transcript context never proves the current graph revision.

## Durable storage and recovery

The default is `~/.ontography/`; explicit `--data-dir`, `ONTOGRAPHY_DATA_DIR`, and XDG overrides retain precedence. Records are stored under `sessions/SESSION_UUID/session.json`, native histories under `sessions/SESSION_UUID/pi/conversations/`, selection in `sessions/selection.json`, and existing core runs under `runs/RUN_UUID/`.

The migration command implements an exclusive journaled cutover: archive existing legacy home contents, relocate the stopped old app store, and alias its old path to the new root. Run IDs and content are preserved. The local migration completed on 2026-09-25 and restored all three previously active runs; consult [VERIFICATION.md](VERIFICATION.md).

Initialization persists the chosen run ID, operation arguments, resolved definition, and initial application input before core creation. Recovery handles an intent without a run, an empty reserved directory, or an existing run awaiting session binding. Existing application state resumes without replaying its fresh input. Unopenable partial core storage remains an explicit error.

Accepted vocabulary additions are stored in each run's `extensions.json`, with one pending intent persisted before the core commit. Recovery resolves whether that commit occurred before launching workers and reconstructs the extended binding. The original declaration and rewrite grammar remain fixed. See [CORE_UPDATE.md](CORE_UPDATE.md).

General accepted-operation receipts remain bounded and server-instance-local. Session initialization has a durable intent; arbitrary mutations do not gain automatic durable replay. After server replacement, refresh graph state and reconcile unknown outcomes. Prepared rewrites, invocation capabilities, and other live handles expire.

| Action | Ownership outcome |
| --- | --- |
| Detach or close client terminal | Server retains manager process, PTY state, and graph resources. |
| Session suspend | Stop manager; suspend/checkpoint graph resources; preserve resumability. |
| Session close | Stop manager; close graph admission; retain durable history and clear default selection if needed. |
| Server stop | Settle accepted work, stop managers, suspend runs, retain records. |
| Server restart | Load records; explicitly resume a selected session or attach to restore its graph and manager. |

Session lifecycle admission serializes graph mutations with suspend/close. Observation waits release session-record/run locks so stop controls remain available. Manager launch/stop is serialized separately; final Pi metadata callbacks can finish without waiting on a held session-record lock.

## Terminal implementation

`portable-pty` hosts the native process; `vt100` interprets output, maintains screen state, and supplies serialization. Ontography supplies the background server, control protocol, snapshots, input/resize routing, terminal queries, graph switching, process supervision, and cleanup.

Output is read continuously without a client. Reattachment receives current screen state rather than requiring an unbounded replay. A single controller determines input and dimensions. Graph rendering is a client view change; Pi keeps running and producing output while hidden.

The current backend hosts the management terminal. Worker terminal creation and views remain unimplemented. Terminal compatibility claims are limited to recorded automated/native-Pi checks, not every terminal extension.

## Remaining work

1. Define concrete Codex nodes, workspace/message/union contracts, edge rules, node harnesses, and scoped MCP.
2. Supply their app editing productions at run creation; implement worker process reconciliation and views.
3. Specify explicit app-session fork/archive/delete, complete artifact-backed export, and optional viewer/takeover behavior.
4. Runs need the desired grammar at creation. Live grammar replacement remains unavailable; `run.extend` only adds vocabulary and trusted contracts. This WIP uses fresh runs instead of migrating empty-grammar runs.

## Historical baseline

The initial management implementation (`389076c`) kept graphs in the detached server but launched Pi with client lifetime, without durable Pi/run ownership. Its `--ui` path used Pi RPC and an independent run picker. The present native PTY/session path supersedes those behaviors; the previous UI code and dated verification remain historical implementation evidence.

The tmux session/pane analogy still explains interaction. Actual tmux hosting was considered and replaced by the Rust `portable-pty` backend. A node never became a separate graph runtime as a consequence of that backend choice.
