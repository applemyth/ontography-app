# Session design and implementation

**Status:** the persistent manager-session foundation is implemented and verified, including the local user-home migration. Workflow definitions and edits, persistent Codex worker sessions, and node-scoped MCP are implemented. Incoming work is delivered through persistent Codex app-servers; Enter in the graph opens the selected agent's terminal. [VERIFICATION.md](VERIFICATION.md) records release evidence; [SESSIONS.md](SESSIONS.md) gives commands and usage.

## Ownership invariant

One Ontography session owns:

- Pi manager state: its conversation collection, active conversation, native saved histories/context, app tool preferences, and resume references.
- One core graph run/runtime once initialized; otherwise explicit pending initialization.
- A fixed project association and a persistent shell terminal; its managed Pi process can exit and relaunch independently.

Several sessions share a Rust server but retain separate manager state and graph identities. Pi owns native conversation semantics; core owns graph/workflow semantics. Ontography records their association. Pi conversation branching does not merge histories, copy the graph, or rewind committed state.

A process ID, connection, or terminal-instance ID is not a durable session identity. Native history supports conversation resumption after process loss; it does not restore arbitrary process memory or in-flight work.

## Agreed session, graph, and terminal ownership

| Piece | Responsibility | Implementation |
| --- | --- | --- |
| Ontography server | Own app sessions, core runtimes, manager processes, and terminals across client attachments. | Implemented. |
| Ontography session | Group Pi state, project, optional graph binding, and manager terminal. | Implemented. |
| Graph runtime | Govern nodes, edges, contracts, packages, workflow, authority, and rewrites. Coordinate nodes collectively. | Existing core bindings implemented. |
| Manager execution | Run native Pi before and after graph initialization; resume the latest active conversation on shell reentry. | Managed launcher inside a persistent Bash PTY. |
| Node execution | Run a concrete agent associated with a durable graph-node ID. | Persistent Codex app-server owns the native conversation; the node runtime binds execution, working directory, attached native TUI, queued input, and scoped tools. |
| PTY and terminal state | Host terminal bytes/dimensions; continuously read output and retain a current screen. | `portable-pty` and `vt100` shared by manager and worker sessions. |
| Pane/view | Display existing execution and route controlling-client input. | Full Pi, shell + session panel, graph view, and selected worker terminal. |

The worker relationship is node → execution → PTY → client view. Each node has an execution inside one collective graph runtime. Node identity persists across process restart and view closure; worker client views are still pending.

## Implemented launch and initialization

1. `ontography` connects to or starts the server for its canonical data directory, replacing an idle server of another build.
2. Bare launch always creates a session with a reserved initial Pi conversation UUID and pending graph initialization. `new NAME` names it. `attach NAME_OR_ID` or bare `--session NAME_OR_ID` explicitly resumes an existing session. IDs take precedence; names must match exactly and uniquely. Persisted selection does not control default launch.
3. Attachment resumes the selected graph when present and reuses its live shell terminal as-is. Creating a new terminal starts Pi with the saved active conversation. Only one controlling attachment is admitted per terminal.
4. Pi's first scoped `flow.start` persists a run identity, its compiled declaration, and its initial workflow before creating core storage. The resulting run is bound to the app session. Retry/recovery reuses that identity.
5. `/graph` in Pi or Ctrl-B G switches the attached client's view to the bound run, or pending initialization. Returning restores the same terminal.
6. Pi `/quit` reveals the managed shell and session panel. Shell `pi` resolves the latest saved conversation and launches Pi again; `/new` remains the explicit new-conversation operation.
7. Ctrl-B, then D detaches the client; the shell, any managed Pi, graph resources, and terminal output handling remain server-owned.
8. Shell `exit` causes server-side session suspension, including while detached. The session remains resumable and `ls` displays it as inactive; the API status is `suspended`.

`session new --no-attach` creates records without starting Pi. Existing runs require explicit adoption into an uninitialized session with the same project; no old Pi ownership is inferred.

The background server outlives the terminal that started it, so it keeps only a few basic variables such as `HOME` and `PATH` and never hands that terminal's environment to later sessions. A session's shell, Pi, agents, and command tasks start with the environment of the command that activated it, such as attaching or a scripted start; runs no session owns start with the latest command's that changed them. One rule drops the variables of the terminal or agent session a command was typed in, so a session attached from inside Claude Code does not pass on that session's messaging socket and token. Environments stay in memory and are forgotten when the session stops. [SESSIONS.md](SESSIONS.md#environment) gives the rule.

Node creation commits through one core graph edit under the workflow edit policy, then reconciles agent execution. Initial graphs provision their agent sessions on startup. A startup failure preserves committed graph state and remains visible for recovery with `flow.resume`. Agent config changes restart its process; graph grants and scope update in place. Opening a worker view does not create a node, edge, or package delivery. See [WORKFLOWS.md](WORKFLOWS.md) for session configuration and delivery limits.

## Pi integration

Native Pi runs with the owning session's conversation directory and active history or reserved UUID. Extension hooks register startup/new/fork/resume state, check resume membership, refresh current graph context, and persist tool-group preferences. Completion callbacks record history materialization. The extension's shutdown event closes its bridge; it does not close the app session or graph.

| Action | Current relationship |
| --- | --- |
| `/new` | Add a conversation; retain the app session and graph. |
| `/resume` | Select owned history; retain the current graph. |
| `/fork`, `/clone`, `/tree` | Apply native conversation branching/navigation; retain graph state. |
| Pi exit | End its process and reveal shell + session panel; preserve manager history, app-session identity, and graph. |
| Shell `pi` | Resolve and resume the latest active saved conversation with the session extension and graph binding. |
| Attach afterward | Reuse the terminal as-is; after suspension or terminal loss, create a terminal and resume Pi. |
| Switch app sessions | Select that session's own manager state and graph together. |

Ontography tool groups are shared app-session preferences. Model/thinking history, compaction, native settings, and credentials retain Pi's own scope. The preferences record does not turn every native setting into a session-wide default.

Pi can report a history path before the file exists. Records distinguish an unmaterialized conversation from missing previously saved history. Foreign histories require explicit import; loading one does not silently rebind the graph. Older transcript context never proves the current graph revision.

## Durable storage and recovery

The default is `~/.ontography/`; explicit `--data-dir`, `ONTOGRAPHY_DATA_DIR`, and XDG overrides retain precedence. Records are stored under `sessions/SESSION_UUID/session.json`, native histories under `sessions/SESSION_UUID/pi/conversations/`, selection in `sessions/selection.json`, and existing core runs under `runs/RUN_UUID/`.

The migration command implements an exclusive journaled cutover: archive existing legacy home contents, relocate the stopped old app store, and alias its old path to the new root. Run IDs and content are preserved. The local migration completed on 2026-09-25 and restored all three previously active runs; consult [VERIFICATION.md](VERIFICATION.md).

Initialization persists the chosen run ID, operation arguments, resolved definition, and initial application input before core creation. Recovery handles an intent without a run, an empty reserved directory, or an existing run awaiting session binding. Existing application state resumes without replaying its fresh input. Unopenable partial core storage remains an explicit error.

A run's vocabulary, including its node types, is fixed when it is created. The earlier `extensions.json` journal for vocabulary additions was removed; [CORE_UPDATE.md](CORE_UPDATE.md) records it.

General accepted-operation receipts remain bounded and server-instance-local. Session initialization has a durable intent; arbitrary mutations do not gain automatic durable replay. After server replacement, refresh graph state and reconcile unknown outcomes. Prepared rewrites, invocation capabilities, and other live handles expire.

| Action | Ownership outcome |
| --- | --- |
| Detach or close client terminal | Server retains manager process, PTY state, and graph resources. |
| Shell exit / session suspend | Stop terminal and managed processes; suspend/checkpoint graph resources; preserve resumability. |
| Session close | Stop manager; close graph admission; retain durable history and clear default selection if needed. |
| Server stop | Settle accepted work, stop managers, suspend runs, retain records. |
| Server restart | Load records; explicitly resume a selected session or attach to restore its graph and manager. |
| Nothing runs and no client is connected for 30 seconds | The server exits. Records remain; the next command that needs a server starts one. |

Session lifecycle admission serializes graph mutations with suspend/close. Observation waits release session-record/run locks so stop controls remain available. Manager launch/stop is serialized separately; final Pi metadata callbacks can finish without waiting on a held session-record lock.

## Terminal implementation

`portable-pty` hosts an interactive Bash shell; a session-local function launches native Pi through a private, generation-fenced process lease. `vt100` interprets output, maintains screen state, and supplies serialization. Ontography supplies the background server, control protocol, snapshots, input/resize routing, terminal queries, graph switching, process supervision, and cleanup.

Output is read continuously without a client. Reattachment receives current screen state rather than requiring an unbounded replay. A single controller determines input and dimensions. Graph rendering is a client view change; Pi keeps running and producing output while hidden.

Pi runs full screen. After it exits, the client renders the shell beside a session status panel, with a compact layout on narrow terminals. Explicit manager mode drives layout and PTY resizing; rendered terminal text is never treated as process-state evidence. The private launcher resolves conversation selection on each invocation and rejects stale terminal generations. Server supervision suspends the session when its shell ends, without depending on an attached client.

The same backend hosts management and worker terminals. Worker view integration remains unimplemented. Terminal compatibility claims are limited to recorded automated/native-Pi checks, not every terminal extension.

## Remaining work

1. Wake continuing agent sessions when graph work arrives; tools already support discovering and processing it.
2. Verify a full interactive multi-agent project flow against live models.
3. Specify explicit app-session fork/archive/delete, complete artifact-backed export, and optional viewer/takeover behavior.
4. Runs need their accepted edits and node types declared at creation; neither can be replaced on a live run. This WIP uses fresh runs instead of migrating fixed-graph runs.

## Historical baseline

The initial management implementation (`389076c`) kept graphs in the detached server but launched Pi with client lifetime, without durable Pi/run ownership. Its `--ui` path used Pi RPC and an independent run picker. The present native PTY/session path supersedes those behaviors; the previous UI code and dated verification remain historical implementation evidence.

The tmux session/pane analogy still explains interaction. Actual tmux hosting was considered and replaced by the Rust `portable-pty` backend. A node never became a separate graph runtime as a consequence of that backend choice.
