# Ontography sessions

The persistent session foundation is implemented and verified. Automated gates, native Pi checks, and the completed local home-directory migration are recorded in [VERIFICATION.md](VERIFICATION.md).

## Ownership

**One Ontography session owns Pi manager state and, once initialized, one core graph run.** Manager state contains multiple native conversations, one active conversation reference, and app-scoped tool preferences. Session IDs, conversation IDs, graph-run IDs, and live terminal IDs are separate identities.

```text
Rust server for one canonical data directory
├── Session A
│   ├── Pi conversations + active selection + tool preferences
│   ├── Manager process ↔ portable-pty ↔ terminal screen state
│   └── Graph run A ↔ core runtime and retained resources
└── Session B
    ├── Separate Pi state and manager terminal
    └── Graph run B, or graph initialization pending
```

Clients attach to running terminals; detachment preserves server resources. A new session can host Pi before its graph exists. The graph view always resolves the owning session's run.

Concrete worker nodes and their terminal views are subsequent work. Their intended model remains **graph node → agent execution → PTY → client view**. Each node has an execution within the graph's collective runtime; another process does not imply another graph runtime.

## Commands

Commands accept an exact session ID or a unique exact name. IDs take precedence over names; ambiguous names return candidate IDs and perform no action. Closed sessions remain listed with their history.

| Command | Effect |
| --- | --- |
| `ontography` | Always create a new session and attach Pi; graph initialization starts pending. |
| `ontography new NAME` | Create a named session with independent Pi state and attach. `create` aliases `new`. |
| `ontography new NAME --no-attach` | Create the session without launching Pi; print its record. |
| `ontography ls` | Show session ID/name, session state, terminal state, and graph state. `list` is equivalent. |
| `ontography ls --json` | Print records enriched with terminal state and graph summaries; include recovery errors. |
| `ontography show NAME_OR_ID` | Inspect the durable record. |
| `ontography attach NAME_OR_ID` | Resume/select and attach to the existing or restored manager. |
| `ontography --session NAME_OR_ID` | Explicit attachment shorthand; with `call`, scope that API operation. |
| `ontography detach NAME_OR_ID` | Disconnect the controller while retaining Pi and graph execution. |
| `ontography resume NAME_OR_ID` | Resume graph/session state without attaching or starting Pi. |
| `ontography suspend NAME_OR_ID` | Stop the manager and suspend the graph, preserving saved state. |
| `ontography close NAME_OR_ID` | Stop the manager and terminally close the session and graph; preserve history. |
| `ontography adopt NAME_OR_ID RUN_UUID` | Bind an existing unowned run to an uninitialized session with the same project. |

The existing `ontography session …` forms remain supported. `session list` continues returning raw session records and selection as JSON. API calls themselves use durable IDs; name resolution belongs to the CLI.

`--project` chooses the project only during session creation. Existing attachments use the saved project. `--data-dir` selects the server/store. `--pi` selects an executable when starting a manager; an existing manager is reused. Interactive creation requires a terminal before creating any session record; scripts use `new --no-attach`.

Persisted selection records the most recently selected session. Closing that session clears selection. Every bare launch creates a new session regardless of selection, cwd, or list order. Each session owns its Pi state and optional graph; the background server hosts multiple sessions.

### Exit behavior

| Action | Pi | App session and graph |
| --- | --- | --- |
| Ctrl-B, then D | Keeps running; client returns to shell. | Keep running. |
| Pi `/quit` | Exits; client returns to shell. | Keep running. Explicit attachment starts Pi with the saved active conversation. |
| `ontography close NAME_OR_ID` | Stops, including any attached client. | Close permanently; retain saved history. |

Pi `/new` starts another conversation inside the same app session and keeps its graph. Neither detach nor `/quit` makes the next bare launch reattach. Use `attach` explicitly.

### Graph display

Enter `/graph` in Pi. The Rust client displays its graph, or an initialization-pending view. Arrow keys select nodes/pan; `q` or Escape returns to Pi. `ontography attach NAME_OR_ID --ui` opens this view first for an existing session; bare `ontography --ui` creates a new session with its pending graph view.

Pi keeps running while hidden; the server keeps reading its output. Returning displays its current screen. Graph display does not launch another Pi instance.

### Terminal scrollback

Press **Ctrl-B, then `[`** to browse the terminal's retained history. Use Up/Down for single lines, Page Up/Page Down for pages, Home/End for the oldest/newest position, or the mouse wheel. **`q` or Escape returns to live Pi.** Ctrl-B, then D still detaches.

History is a frozen view captured on entry, with up to 1,000 retained lines plus the current screen. Pi continues executing, receiving terminal-query replies, and producing output while this view is open. Navigation and paste in history mode are consumed by the viewer. Return to live and reopen history to include newer output.

The history view belongs to the attachment; it does not modify Pi's cursor, input modes, or live screen. Resizing the terminal, opening the graph, or reconnecting returns to live mode. In live mode Pi retains its own Page Up/Page Down and mouse behavior. History is bounded terminal output, not a durable substitute for saved Pi conversations; applications using an alternate screen may expose no scrollback.

### Detach and control ownership

From the Pi terminal, press **Ctrl-B, then D**. Reattach with `ontography attach NAME_OR_ID`.

One controlling client owns a manager terminal's input and dimensions. A second controller is rejected. From another terminal, `session detach SESSION_UUID` disconnects the first controller, including an open graph view. Shared read-only viewers and takeover controls are deferred.

## Conversations and Pi state

| Native Pi operation | Ontography behavior |
| --- | --- |
| `/new` | Register another conversation within this session; retain its graph. |
| `/resume` | Select registered history in this session; retain the current graph. |
| `/fork`, `/clone` | Add a conversation branch/copy within this session; retain its graph. |
| `/tree` | Navigate conversation history; retain current graph state. |
| Pi exit | End the manager process; preserve the app session, conversations, and graph. |

On a fresh turn or conversation change, the extension obtains current session/run context. Older transcripts may describe older graph revisions; navigating them does not rewind the graph or replay accepted mutations.

Native Pi owns transcript structure, model/thinking history, compaction, and global/project settings and credentials. Ontography owns membership, active selection, graph binding, and **shared tool-group preferences**. The generic app preferences record does not replace Pi's native settings or make all model settings shared across conversations.

Resume hooks reject unregistered/foreign histories. Explicit administrative import is available:

```sh
ontography call session.conversation --args '{"session_id":"SESSION_UUID","action":"import","path":"/absolute/path/to/history.jsonl"}'
```

Import copies/registers history and selects it as the active saved conversation; it preserves the graph. Perform administrative conversation changes with its manager stopped, then attach to launch the recorded selection.

Pi writes new histories lazily. Ontography reserves the initial UUID before launch, records its eventual path, and tracks materialization. Missing previously saved history is an error; it does not silently become an empty conversation.

## Initialize or adopt a graph

Pi can save/validate reusable definitions before starting a graph. The first scoped `run.start` or `project.start` binds its resulting run. Logical runs can remain idle and need not contain executable implementations.

Initialization persists a reserved run ID, operation arguments, resolved definition, and initial application input before constructing core storage. Per-session serialization prevents competing initialization; retries recover the same identity. Application recovery does not reinject fresh input. Partial state that core cannot reopen remains an explicit recovery error.

To associate an existing run:

```sh
ontography call run.list --args '{}'
ontography --project /absolute/path/to/the/run-project session new existing --no-attach
ontography session adopt SESSION_UUID RUN_UUID
ontography session attach SESSION_UUID
```

Adoption preserves its run ID and history. Pi state begins with the new app session unless history is explicitly imported. Ownership is never inferred from project paths, names, or old transcripts.

Each run retains the rewrite grammar supplied at creation. An empty grammar still prevents topology edits. The default app grammar is deferred until concrete node/edge/package definitions exist. Editing a saved declaration cannot retrofit a runtime's grammar.

## Lifecycle and recovery

| Action | Pi process | Graph | Durable session |
| --- | --- | --- | --- |
| Detach / close attached terminal | Continues | Continues | Retained |
| Open/close graph view | Continues | Continues | Retained |
| Exit Pi | Ends | Continues | Retained; attach restores active history |
| Suspend session | Stopped | Suspended and resumable | Retained |
| Resume session | Starts on subsequent attachment | Reopened | Same identities |
| Close session | Stopped | Admission terminally closed | History retained; cannot resume |
| Stop server | Managers stopped | Runs suspended | Records retained |
| Restart server | Starts only on attachment | Reopened through resume/attach | Saved identities retained |

Server startup does not relaunch every saved session. Attachment explicitly resumes the selected graph and starts Pi with its recorded conversation if no manager is alive.

Server/machine failure loses PTY resources and process memory. Recovery uses core's durable state and Pi history. Prepared rewrite plans and other transient handles expire across server replacement. General operation receipts are bounded and server-instance-local; reconcile uncertain accepted work against current graph state after restart.

App-session fork/clone, archive/delete, complete portable backups, and multiple-controller/viewer attachment remain deferred. Native Pi conversation forks already operate within the existing graph.

## Storage

Without overrides, the app home is `~/.ontography/`. Selection precedence is `--data-dir`, `ONTOGRAPHY_DATA_DIR`, `$XDG_DATA_HOME/ontography`, then the default home.

```text
~/.ontography/
├── sessions/
│   ├── selection.json
│   └── SESSION_UUID/
│       ├── session.json
│       └── pi/conversations/*.jsonl
├── runs/RUN_UUID/
│   ├── manifest.json
│   └── core/                 # logical run's core-owned stores
├── definitions/REVISION.json
├── client/ASSET_HASH/         # embedded Pi extension materialization
├── logs/
└── server.lock
```

Native applications retain core-owned stores beneath the managed run's `application/` directory. Project working files and Pi global credentials/settings remain outside the session records. Portable export requires explicit artifact/dependency capture; directory organization alone does not provide it.

Migration archives legacy home contents, relocates the stopped previous store, and leaves an alias at the old location. See [installation/migration](INSTALL.md#storage-and-migration). The local cutover completed on 2026-09-25 with all three prior runs preserved and resumed; existing runs require explicit session adoption.

## Implementation map

| Module | Responsibility |
| --- | --- |
| [`sessions.rs`](../src/sessions.rs) | Durable records, ownership, initialization intents, conversation registration, scoped dispatch, lifecycle admission. |
| [`session_runtime.rs`](../src/session_runtime.rs) | Retain/reuse managers; serialize start/stop; coordinate terminal and session lifecycle. |
| [`terminal.rs`](../src/terminal.rs) | `portable-pty`, `vt100` screen state, terminal queries, continuous output draining, snapshots, controller ownership. |
| [`terminal_client.rs`](../src/terminal_client.rs) | Attach, display native terminal state, input/resize, detach, graph view switching. |
| [`pi/session.ts`](../pi/session.ts) | Conversation hooks, current graph context, tool preferences, `/graph`. |
| [`ui/session_graph.rs`](../src/ui/session_graph.rs) | Graph-only Ratatui view bound to the app session. |
| [`state.rs`](../src/state.rs) | Core run ownership and reserved-identity start/recovery. |
| [`migration.rs`](../src/migration.rs) | Exclusive, journaled archive/relocation/alias cutover. |

Scoped requests carry `app_session_id`. The server resolves omitted session/run/project targets and rejects conflicting explicit targets before dispatch. Unscoped administrative calls retain the full core API. Handles continue to resolve within the selected run. Lifecycle admission locks serialize mutations with suspend/close; observation waits release record/run locks so controls stay available.

Terminal snapshots use a bounded protocol, and output is drained without clients. Terminal parsing/input compatibility is tested for the supported native Pi flow; this does not establish complete emulation of every terminal extension.
