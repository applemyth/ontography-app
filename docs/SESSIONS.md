# Ontography sessions

The persistent session foundation is implemented and verified. Automated gates, native Pi checks, and the completed local home-directory migration are recorded in [VERIFICATION.md](VERIFICATION.md).

## Ownership

**One Ontography session owns Pi manager state and, once initialized, one core graph run.** Manager state contains multiple native conversations, one active conversation reference, and app-scoped tool preferences. Session IDs, conversation IDs, graph-run IDs, and live terminal IDs are separate identities.

```text
Rust server for one canonical data directory
├── Session A
│   ├── Pi conversations + active selection + tool preferences
│   ├── Managed shell ↔ portable-pty ↔ terminal screen state
│   │   └── Pi process, initially launched and resumable from the shell
│   └── Graph run A ↔ core runtime and retained resources
└── Session B
    ├── Separate Pi state and manager terminal
    └── Graph run B, or graph initialization pending
```

Clients attach to running terminals; detachment preserves server resources. A new session can host Pi before its graph exists. The graph view always resolves the owning session's run.

Agent nodes own persistent worker terminals. Their model is **graph node → agent execution → PTY → client view**. Each node has an execution within the graph's collective runtime; another process does not imply another graph runtime.

## Commands

Commands accept an exact session ID or a unique exact name. IDs take precedence over names; ambiguous names return candidate IDs and perform no action. Closed sessions remain listed with their history.

| Command | Effect |
| --- | --- |
| `ontography` | Always create a new session and attach Pi; graph initialization starts pending. |
| `ontography new NAME` | Create a named session with independent Pi state and attach. `create` aliases `new`. |
| `ontography new NAME --no-attach` | Create the session without launching Pi; print its record. |
| `ontography ls` | Show session ID/name, session state, terminal attachment, current program, and graph state. `list` is equivalent. |
| `ontography ls --json` | Print records enriched with terminal state and graph summaries; include recovery errors. |
| `ontography show NAME_OR_ID` | Inspect the durable record. |
| `ontography attach NAME_OR_ID` | Attach to the existing terminal as it is, or resume an inactive session with its saved Pi conversation. |
| `ontography --session NAME_OR_ID` | Explicit attachment shorthand; with `call`, scope that API operation. |
| `ontography detach NAME_OR_ID` | Disconnect the controller while retaining Pi and graph execution. |
| `ontography resume NAME_OR_ID` | Resume graph/session state without attaching or starting Pi. |
| `ontography suspend NAME_OR_ID` | Stop the session terminal and suspend the graph, preserving saved state. |
| `ontography close NAME_OR_ID` | Stop the session terminal and terminally close the session and graph; preserve history. |
| `ontography adopt NAME_OR_ID RUN_UUID` | Bind an existing unowned run to an uninitialized session with the same project. |

The existing `ontography session …` forms remain supported. `session list` continues returning raw session records and selection as JSON. API calls themselves use durable IDs; name resolution belongs to the CLI.

`ls`, `show`, and `session list` never start the server. With none running, they read saved sessions from disk, as they were saved, and show that nothing runs: terminals and programs as `stopped`, and a graph left open as `recoverable`. Other session commands and `call` start the server when none is running.

`--project` chooses the project only during session creation. Existing attachments use the saved project. `--data-dir` selects the server/store. `--pi` selects the executable when creating a session terminal; a live terminal retains that choice for Pi reentry. Interactive creation requires a terminal before creating any session record; scripts use `new --no-attach`.

Persisted selection records the most recently selected session. Closing that session clears selection. Every bare launch creates a new session regardless of selection, cwd, or list order. Each session owns its Pi state and optional graph; the background server hosts multiple sessions.

### Exit behavior

| Action | Terminal | App session and graph |
| --- | --- | --- |
| Ctrl-B, then D | Keep Pi or the shell running; client returns to the outer shell. | Keep running. |
| Pi `/quit` | Pi exits; show the managed shell and session status panel. | Keep running. |
| `pi` in the managed shell | Start Pi with the latest active saved conversation; show native Pi full screen. | Keep the same session and graph. |
| `exit` in the managed shell | Shell exits; client returns to the outer shell. | Suspend live execution and retain resumable state, even when detached. |
| `ontography close NAME_OR_ID` | Stop the shell and its managed Pi, including the attached client. | Close permanently; retain saved history. |

Pi `/new` starts another conversation inside the same app session and keeps its graph. Neither detach nor `/quit` makes the next bare launch reattach. Use `attach` explicitly.

The managed terminal uses Bash. Its session-local startup script provides the `pi`
launcher and does not edit your global shell configuration. Bare `pi` resumes the
latest active conversation; use Pi's native commands to start a new conversation
or change model settings. Extra arguments to the managed `pi` command are rejected
so they cannot override the session binding. A missing previously saved history
is reported as an error and leaves the shell usable.

`ls` displays a successfully suspended session as `inactive`; its API status stays
`suspended`. Terminal attachment (`attached`/`detached`/`stopped`), program
(`pi`/`shell`/`starting`/`stopped`), and graph status are independent. An active
graph may be idle. Suspension failures remain visible and require recovery;
they are not reported as a successful inactive session.

### Shell and session panel

Native Pi occupies the full terminal. On Pi exit, the Rust client displays the
shell alongside a session panel with session state, manager state, graph state,
worker execution count, and pending packages. Missing graphs and zero workers
are explicit. Narrow terminals use a compact layout. The panel reads structured
server state; terminal output does not decide whether Pi is running.

The server owns the shell independently of the viewing client. Detach and attach
preserve a shell prompt without restarting Pi. A fresh terminal after suspension
or process loss starts Pi using the session's saved active conversation.

### Graph display

Enter `/graph` in Pi, or press Ctrl-B then G from the terminal. The Rust client displays its graph, or an initialization-pending view. Up/Down or Tab selects a node; Left/Right pans. **Enter attaches to the selected agent's existing terminal.** `q` or Escape in the graph returns to the manager. `ontography attach NAME_OR_ID --ui` opens this view first for an existing session; bare `ontography --ui` creates a new session with its pending graph view.

Pi keeps running while hidden; the server keeps reading its output. Returning displays its current screen. Graph display does not launch another Pi instance.

Inside a node terminal, type directly into its running Codex session, including
answers to approval prompts. **Ctrl-B, then D or G returns to the graph**, keeping
the worker running. Scrollback uses the same Ctrl-B `[` controls as the manager.
Selecting another node and pressing Enter switches to that node's terminal.
Command, human, and inbox nodes have no interactive terminal. A stopped node
shows an error; opening its pane does not restart it. For native Codex, `/exit`
closes its interface while its app-server keeps running; Enter reconnects it.

Only one controller can attach to a worker terminal at a time. Session detach
from another terminal closes any nested node view too; manager and worker
processes remain alive. Suspension, node replacement, or process exit releases
the attachment. Lookup is session-scoped and uses the current worker identity,
so an old attachment cannot enter a replacement execution.

### Terminal scrollback

Press **Ctrl-B, then `[`** to browse the terminal's retained history. Use Up/Down for single lines, Page Up/Page Down for pages, Home/End for the oldest/newest position, or the mouse wheel. **`q` or Escape returns to the live terminal.** Ctrl-B, then D still detaches.

History is a frozen view captured on entry, with up to 1,000 retained lines plus the current screen. Pi continues executing, receiving terminal-query replies, and producing output while this view is open. Navigation and paste in history mode are consumed by the viewer. Return to live and reopen history to include newer output.

The history view belongs to the attachment; it does not modify Pi's cursor, input modes, or live screen. Resizing the terminal, opening the graph, or reconnecting returns to live mode. In live mode Pi retains its own Page Up/Page Down and mouse behavior. History is bounded terminal output, not a durable substitute for saved Pi conversations; applications using an alternate screen may expose no scrollback.

### Detach and control ownership

From Pi or the shell, press **Ctrl-B, then D**. Reattach with `ontography attach NAME_OR_ID`.

One controlling client owns a manager terminal's input and dimensions. A second controller is rejected. From another terminal, `session detach SESSION_UUID` disconnects the first controller, including an open graph view. Shared read-only viewers and takeover controls are deferred.

## Conversations and Pi state

| Native Pi operation | Ontography behavior |
| --- | --- |
| `/new` | Register another conversation within this session; retain its graph. |
| `/resume` | Select registered history in this session; retain the current graph. |
| `/fork`, `/clone` | Add a conversation branch/copy within this session; retain its graph. |
| `/tree` | Navigate conversation history; retain current graph state. |
| Pi exit | End the manager process; return to the managed shell; preserve conversations and graph. |

On a fresh turn or conversation change, the extension obtains current session/run context. Older transcripts may describe older graph revisions; navigating them does not rewind the graph or replay accepted mutations.

Native Pi owns transcript structure, model/thinking history, compaction, and global/project settings and credentials. Ontography owns membership, active selection, graph binding, and **shared tool-group preferences**. The generic app preferences record does not replace Pi's native settings or make all model settings shared across conversations.

Resume hooks reject unregistered/foreign histories. Explicit administrative import is available:

```sh
ontography call session.conversation --args '{"session_id":"SESSION_UUID","action":"import","path":"/absolute/path/to/history.jsonl"}'
```

Import copies/registers history and selects it as the active saved conversation; it preserves the graph. Perform administrative conversation changes with Pi stopped, then run `pi` in the managed shell to launch the recorded selection.

Pi writes new histories lazily. Ontography reserves the initial UUID before launch, records its eventual path, and tracks materialization. Missing previously saved history is an error; it does not silently become an empty conversation.

## Initialize or adopt a graph

Pi can save reusable documents with `flow.define` before starting a graph. The first scoped `flow.start` binds its resulting run. A run whose nodes are all `external` runs no process and waits for an outside client's moves.

Initialization persists a reserved run ID, operation arguments, compiled declaration, and initial workflow before constructing core storage. Per-session serialization prevents competing initialization; retries recover the same identity. Recovery does not reinject fresh input. Partial state that core cannot reopen remains an explicit recovery error.

To associate an existing run:

```sh
ontography call run.list --args '{}'
ontography --project /absolute/path/to/the/run-project session new existing --no-attach
ontography session adopt SESSION_UUID RUN_UUID
ontography session attach SESSION_UUID
```

Adoption preserves its run ID and history. Pi state begins with the new app session unless history is explicitly imported. Ownership is never inferred from project paths, names, or old transcripts.

A run's graph changes only through its document: `flow.edit` previews the new document as one explicit core graph edit and `flow.commit` applies it; see [Edit and recover](WORKFLOWS.md#edit-and-recover). A run keeps the document revision it started from, so editing a saved document does not change a running graph.

## Lifecycle and recovery

| Action | Pi process | Graph | Durable session |
| --- | --- | --- | --- |
| Detach / close attached terminal | Continues | Continues | Retained |
| Open/close graph view | Continues | Continues | Retained |
| Exit Pi | Ends; shell remains | Continues | Retained; shell `pi` restores active history |
| Exit managed shell | Ends | Suspended and resumable | Inactive (`suspended` in the API) |
| Suspend session | Stopped | Suspended and resumable | Retained |
| Resume session | Starts on subsequent attachment | Reopened | Same identities |
| Close session | Stopped | Admission terminally closed | History retained; cannot resume |
| Stop server | Managers stopped | Runs suspended | Records retained |
| Restart server | Starts only on attachment | Reopened through resume/attach | Saved identities retained |

Server startup does not relaunch every saved session. Attachment explicitly resumes the selected graph. A live shell terminal is reused; only creation of a new terminal automatically starts Pi with its recorded conversation.

The server keeps sessions running after their clients detach. After 30 seconds in which nothing runs (no session shell, active graph, or operation in progress) and no client is connected, it exits by itself. Saved sessions stay on disk, and the next command that needs the server starts the current build.

Server/machine failure loses PTY resources and process memory. Recovery uses core's durable state and Pi history. Prepared rewrite plans and other transient handles expire across server replacement. General operation receipts are bounded and server-instance-local; reconcile uncertain accepted work against current graph state after restart.

Older session initialization records remain readable even when their graph format is unsupported. Session inspection, selection, and Pi conversations remain available. Resume can activate the manager while reporting its graph as `unavailable`, with the run ID and recovery error. The saved initialization, run ownership, and conversation files are retained; an unsupported old graph is not replayed or replaced. Graph operations continue to report its recovery failure.

App-session fork/clone, archive/delete, complete portable backups, and multiple-controller/viewer attachment remain deferred. Native Pi conversation forks already operate within the existing graph.

## Environment

The background server outlives the terminal that started it, so it keeps only `HOME`, `USER`, `LOGNAME`, `PATH`, `SHELL`, `TMPDIR`, `LANG`, and `LC_*` for itself. Each session brings its own environment instead. Resuming or attaching a session gives it the environment of the terminal that did so, and its shell, Pi, agents, and command tasks start with it. So does a command that changes an active session without one, such as a scripted `--session NAME call flow.start` after `new --no-attach`. Commands that only read a session never give it one, and neither does changing a suspended session. Later commands do not change it. Suspending or closing the session forgets it; the next activation brings its own.

One rule drops the variables that describe the terminal or agent session a command was typed in:

- terminal variables: `TERM`, `TERM_*`, `COLORTERM`, `TMUX*`, `ITERM_*`, `KITTY_*`, `VSCODE_*`, and those of other terminal programs. Each terminal the server creates sets its own `TERM`.
- shell state: `PWD`, `OLDPWD`, `SHLVL`, and `_`.
- variables Claude Code and Codex set for their own children, such as `AI_AGENT`, `CLAUDECODE`, `CLAUDE_CODE_SESSION_ID`, `CLAUDE_CODE_MESSAGING_SOCKET`, `CLAUDE_CODE_MESSAGING_TOKEN`, and `CODEX_SANDBOX`. From a command typed inside Claude Code, the settings it gives its tools, such as `GIT_EDITOR=true`, are dropped too.
- every `ONTOGRAPHY_*` variable; the server sets its own.

Configuration such as `CLAUDE_CONFIG_DIR`, `CODEX_HOME`, API keys, and `SSH_AUTH_SOCK` is kept.

Environments are kept in memory only, never saved. Runs no session owns, started with an unscoped `call`, start with the environment of the latest command that changed such a run; before any, with the server's own. Every program also gets `ONTOGRAPHY_DATA_DIR`, so `ontography` commands it runs reach the same store.

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
| [`session_runtime.rs`](../src/session_runtime.rs) | Retain/reuse managed shells; serialize start/stop; reconcile shell exit with session suspension. |
| [`managed_shell.rs`](../src/managed_shell.rs) | Session-local shell startup, fresh Pi launch context, process leases, and manager lifecycle. |
| [`terminal.rs`](../src/terminal.rs) | `portable-pty`, `vt100` screen state, terminal queries, continuous output draining, snapshots, controller ownership. |
| [`terminal_client.rs`](../src/terminal_client.rs) | Attach, display native terminal state, input/resize, detach, graph view switching. |
| [`pi/session.ts`](../pi/session.ts) | Conversation hooks, current graph context, tool preferences, `/graph`. |
| [`ui/session_graph.rs`](../src/ui/session_graph.rs) | Graph-only Ratatui view bound to the app session. |
| [`state.rs`](../src/state.rs) | Core run ownership and reserved-identity start/recovery. |
| [`migration.rs`](../src/migration.rs) | Exclusive, journaled archive/relocation/alias cutover. |

Scoped requests carry `app_session_id`. The server resolves omitted session/run/project targets and rejects conflicting explicit targets before dispatch. Unscoped administrative calls retain the full core API. Handles continue to resolve within the selected run. Lifecycle admission locks serialize mutations with suspend/close; observation waits release record/run locks so controls stay available.

Terminal snapshots use a bounded protocol, and output is drained without clients. Terminal parsing/input compatibility is tested for the supported native Pi flow; this does not establish complete emulation of every terminal extension.
