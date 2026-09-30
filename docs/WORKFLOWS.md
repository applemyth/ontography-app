# Workflows

A workflow document names the steps, their worker settings, how they connect,
and where the first input goes. It is the one way to start a run: the app
compiles it into core's graph, core records accepted work, and the app harness
runs the workers. Typing is optional; a document without it carries messages
and workspaces everywhere, and a document can declare as much of core's typed
model as it needs (see [Typing](#typing)).

## Start a small workflow

Save this as `start.json`. It is the complete argument object for `flow.start`:

```json
{
  "document": {
    "name": "review-a-draft",
    "entry": "draft",
    "nodes": [
      {"id": "draft", "component": "command", "config": {"argv": ["/bin/echo", "Draft ready"]}},
      {"id": "review", "component": "human", "config": {"prompt": "Check the draft"}},
      {"id": "result", "component": "inbox"}
    ],
    "edges": [
      {"from": "draft", "to": "review"},
      {"from": "review", "to": "result"}
    ]
  },
  "message": "Begin"
}
```

From the project directory:

```sh
ontography new demo --no-attach
ontography --session demo call flow.start --file start.json
ontography --session demo call flow.status
```

The session supplies the project and run. `flow.status` lists the pending
`review` task after the command finishes. Copy its `task_id` into this call:

```sh
ontography --session demo call flow.decide --args '{"node":"review","task_id":"TASK_ID","message":"Approved"}'
ontography --session demo call flow.output --args '{"node":"result"}'
ontography --session demo call flow.export --args '{"node":"result","path":"result.txt"}'
```

The inbox holds the published result without running a process. Export creates
a new file; it refuses to overwrite an existing path. Pi uses these same tools
with the same session defaults. Unscoped API calls must supply `project` at
start and `run_id` afterward; a stable UUID `start_id` makes startup retryable.

## Document pieces

| Piece | Meaning |
| --- | --- |
| `name` | Name for the reusable workflow. |
| `entry` | The node receiving the initial message or workspace. |
| `components` | Optional: this document's own components; see [The library](#the-library). |
| `contracts` | Optional: named contracts, each `{object_type, validator}`; see [Typing](#typing). |
| `nodes[].id` | Unique name used by tools and connections. |
| `nodes[].component` | The component the node places: a built-in, one from the library, or one from `components`. Documents saved with `kind` still load. |
| `nodes[].config` | This placement's settings, merged over its component's defaults; defaults to `{}`. |
| `nodes[].join` | `any` by default, or `all`; belongs to the receiving node. |
| `nodes[].retry` | Agent and command nodes only; see [Failed tasks](#failed-tasks). |
| `nodes[].grants` | Agent and command nodes only: node-tool powers beyond the base set (`originate`, `send_later`, `retire`); see [Node tools](NODE_TOOLS.md). |
| `nodes[].tools` | Optional shared node-tool allowlist for agent/command nodes. Omitted uses the grant-filtered default; `[]` exposes none. |
| `nodes[].result` | Optional: the contract of the node's results; `payload` by default. |
| `nodes[].root` | Optional: the authority tags work the node starts may carry. Only nodes with a root start work; the entry has `["workflow"]` unless given. |
| `nodes[].transitions` | Optional: `[{from, to}]` authority changes the node may make to what it sends. |
| `edges` | Directed `{from,to}` connections; defaults to `[]`. |
| `edges[].name` | Optional; needed to connect the same two nodes twice. Otherwise a connection is named `from:to`. It cannot be a node's name, since node tools accept either in `to`. Documents saved or started before this rule keep such names; an edit may keep them but not add one. |
| `edges[].contract` | Optional: what it carries; its source's `result` by default. |
| `edges[].authority` | Optional: the tags it admits; `["workflow"]` by default. |
| `edges[].match` | `any_of` by default, or `all_of`: whether work needs any or all of those tags. |

`any` takes one available incoming package. `all` waits for one package on
every incoming connection. Cycles and self loops are supported. Names of
nodes, connections, contracts, object types, and tags use letters, digits,
`-`, and `_`: a new run uses the document's names as core's identities.

## Typing

Every part of core's typed model is optional, and what a document leaves out
takes a default:

- **Contracts.** `payload` is built in: object type `Payload`, validator
  `text`, which accepts a message or a workspace. A
  document names more under `contracts`, each an object type and one of three
  trusted validators: `text` (valid UTF-8, including a workspace), `bytes`
  (anything), or `workspace` (only a workspace).
- **Results and connections.** A node's `result` names the contract its results
  satisfy. A connection carries its source's result contract unless it names
  its own, so a type is written once.
- **Authority.** Connections admit `["workflow"]` and the entry starts work
  under it, unless the document says otherwise. Authority is never inferred
  beyond that default: a connection that needs other tags names them, a node
  that starts work declares its `root`, and a node that changes authority
  declares its `transitions`.

```json
{"name": "smelting", "entry": "mine",
 "contracts": {"ore": {"object_type": "Ore", "validator": "text"}},
 "components": {"mine": {"extends": "external", "types": ["Mine"]}},
 "nodes": [
   {"id": "mine", "component": "mine", "result": "ore", "root": ["red"]},
   {"id": "smelter", "component": "command",
    "config": {"argv": ["./smelt"], "authority": ["sealed"]},
    "transitions": [{"from": ["red"], "to": ["sealed"]}]},
   {"id": "vault", "component": "inbox"}],
 "edges": [
   {"from": "mine", "to": "smelter", "authority": ["red"]},
   {"from": "smelter", "to": "vault", "authority": ["sealed"], "match": "all_of"}]}
```

Save this executable script as `smelt` in the project:

```sh
#!/bin/sh
printf 'Refined: '
cat
```

After starting the document, submit this to `workflow.submit`, supplying the
run's `run_id`:

```json
{"trigger": {"kind": "root", "node_id": "mine", "authority": ["red"]},
 "result": "raw ore", "emissions": [{"edge_id": "mine:smelter", "payload": "raw ore"}]}
```

The vault receives `Refined: raw ore` carrying `sealed`. Command
`config.authority` and human `flow.decide`'s `authority` request exactly those
output tags; a declared transition must permit the change. Omit the setting
to carry the task's authority. An explicit `[]` requests empty authority;
connections still check whether they accept it. Defining, starting, or editing
a document refuses a command `authority` that no transition of its node
targets, or that a connection from the node does not admit, so no task runs
with it; core still checks each task's transition. `flow.decide` reports a
refusal as `rejected`, naming the rule; the task stays ready and the node keeps
its last result.

Core checks every result and every package against its contract and every
move against authority. A command's result and a human's decision go through
every outgoing connection; if one refuses it, core rejects the whole
submission. Commands retry under their task policy, except that output core
refuses, for its contract or as a stdout package envelope, parks at once;
storage faults still retry. Human tasks remain ready
for a corrected decision. Agents choose connections, authority transitions, and
outbound object types with the node tools; see [Node tools](NODE_TOOLS.md).
A run's contracts and authority tags are fixed when it starts: an edit that
changes a contract or uses a new tag is refused, and needs a new run.

## External nodes

An `external` node runs nothing: an outside client acts for it with core
moves, `workflow.submit`, `workflow.transfer`, and `workflow.retire`, which
name nodes and connections by their document names. Those moves act only for
external nodes; programs and people own the rest. An external entry takes no
initial input. `workspace.import` puts a directory from the project into the
run's store and returns its `root`, to send as the envelope
`{"ontography_package": ROOT}`, and the `dependencies` the submission declares
in `contents`. `inspect.*` and `run.inspect` read what happened.

```json
{"trigger": {"kind": "root", "node_id": "client", "authority": ["workflow"]},
 "result": "sent", "emissions": [{"edge_id": "client:worker", "payload": "hello"}]}
```

## Components

A node places a component, as in core's project model: the component gives
the node its types and binds the placement's settings to a trusted
implementation and its exact configuration. Node types are labels, such as
`Agent` or `Reviewer`, that core records on the node. A node keeps its types
for life, so switching it to a component with other types replaces it (see
[Edit and recover](#edit-and-recover)). Types never choose behavior; the
implementation does: agent and command nodes run tasks, people decide human
tasks, inboxes hold work, and outside clients act for external nodes. Any node
may connect to any other.

| Component | Node types | Settings | What runs |
| --- | --- | --- | --- |
| `agent` | Agent | Required `prompt`; optional `harness` (`codex`, the default, or `claude`), `model`, `pty`, `mcp`, `permission_mode`, or an `argv` program | A continuing agent conversation |
| `codex` | Agent | As `agent`, with `harness:"codex"` | A Codex conversation |
| `claude` | Agent | As `agent`, with `harness:"claude"` | A Claude Code conversation |
| `command` | Command | Required nonempty `argv`; optional `timeout_secs`, `authority` | The command once per task, with input bytes on stdin |
| `human` | Human | Optional `prompt` | Waits for `flow.decide` on each task |
| `inbox` | Inbox | None | Holds incoming work for inspection and export |
| `external` | External | None | Nothing; an outside client acts for it (see [External nodes](#external-nodes)) |

`flow.library` lists every component a document can place, with its node types,
description, and settings schema, and the library's MCP servers. `flow.status`
shows `types` for each node and task.

An agent node looks like this:

```json
{"id":"draft","component":"claude","config":{"prompt":"Review the input and produce a concise draft.","mcp":["github"]}}
```

Agent settings:

- `prompt` supplies standing instructions, followed by the graph delivery
  instructions.
- `pty` (default `true`) runs the agent in a terminal you can open from the
  graph. `false` runs it headless, reachable only through the graph.
- `mcp` loads MCP servers besides the node tools: a list of library server
  names, or a map from a name to `true` (the library's server) or a
  `{command, args, env}` definition. Your own configured servers still load.
- `permission_mode` (Claude only) sets Claude's permission mode: `acceptEdits`,
  `auto`, `bypassPermissions`, `dontAsk`, `manual`, or `plan`. Unset keeps
  your own setting.
- `argv` replaces the agent with that interactive program in the node's
  terminal. It receives MCP connection variables but no managed conversation
  or automatic delivery, so only `argv` changes restart it.

### The library

`library.json` in the data directory (`~/.ontography/library.json` by default)
holds MCP servers that agents load by name, and components that extend others
with node types and default settings:

```json
{
  "servers": {
    "github": {"command": "github-mcp-server", "args": ["stdio"]}
  },
  "components": {
    "reviewer": {
      "provider": "ontography",
      "extends": "claude",
      "description": "Reviews changes for security issues.",
      "types": ["Reviewer"],
      "config": {"prompt": "Review the change for security issues.", "mcp": ["github"], "permission_mode": "plan"}
    }
  }
}
```

A component specification names its `provider` (`ontography`, the default)
and the component it `extends`, which may be another library component.
`types` adds node types to those of the component it extends, so a `reviewer`
node has the types `Agent` and `Reviewer` and still runs Claude. A component
that extends `reviewer` keeps both and may add more. Type names use letters,
digits, `-`, or `_`. A node's settings are merged
over its component's `config` as a JSON merge patch: objects merge by key,
`null` removes a key, and other values replace.
A list of MCP server names is read as a map first, so a node can add a server
to its component's (`"mcp":{"docs":true}`) or remove one
(`"mcp":{"github":null}`). Library components cannot reuse a built-in name.
Server `env` values are stored in each workflow that uses them, so keep
secrets out of them.

A document's own `components` use the same format and may extend library
components; their names cannot reuse an existing component's.

A run keeps the bindings made when it was started or last edited, so changing
the library does not change a running node. Submit the document again with
`flow.edit` to apply library changes.

A new run declares every node type its components can give: the built-in
components', the library's, and the document's own, placed or not. It keeps
that vocabulary for life, so later edits can place any of those components.
An edit that needs a type the run never declared, such as one added to the
library later, fails with `unknown_node_type`; start a new run to use it.

## Agent sessions

Each agent owns a persistent private working directory under its node's
storage directory, separate from the workflow project and temporary package
checkouts. Restarts retain this directory and resume the recorded
conversation. `timeout_secs` belongs to command tasks and is rejected for
agents.

Agents and command tasks start with their session's environment: that of the
terminal that activated the session, such as with `attach` or a scripted `call
flow.start`, less the variables that describe that terminal or an enclosing
agent session. So `PATH`, API keys, `CODEX_HOME`, and `CLAUDE_CONFIG_DIR` come
from that terminal. A run no session owns starts with the environment of the
latest command that changed such a run. See [Environment](SESSIONS.md#environment).

Initial and incoming graph work is delivered automatically as user messages in
that conversation. The host begins an attempt using the existing join and retry
rules, then supplies its `attempt_id`, `task_id`, sender names, input handles,
and message text in an `ontography_message` envelope. Up to 32 KiB of input
payloads is included inline; larger messages retain handles for `read_package`.
Workspaces retain handles for `open_workspace`. Delivery is independent of the
node's tool allowlist; graph operations still enforce that allowlist and grants.
At most eight attempts are open before automatic delivery waits for capacity.
Agents use the [node MCP adapter](NODE_MCP.md) to work on the supplied attempt
and publish results with `submit_invocation`. Terminal text and ordinary chat
answers are not automatically published.

In the graph UI, select a node with a terminal and press Enter to use it.
Ctrl-B D or G returns to the graph without stopping the agent; see
[Session controls](SESSIONS.md#graph-display). Suspending or replacing the node
stops its processes and their child jobs.

### Codex

The server needs Codex installed and authenticated. Codex finds its home
through the session's environment: `CODEX_HOME`, which must be absolute, or
`$HOME/.codex`. A node's saved conversation belongs to that home; starting the
node later with a different one fails. Each Codex agent owns a long-lived
`codex app-server` on a private Unix socket. With a terminal, a Codex client
joins that server with `--remote` and resumes the exact saved conversation;
closing that client leaves the server running, and Enter reconnects it.
Headless, the server runs alone and logs to `codex-server.log` in the node's
directory.

Codex queues new messages while a turn is active and starts queued work when
idle. A completed turn may leave its attempt open for later conversation;
failed/interrupted turns fail their still-open delivered attempts under the
normal retry policy.

Codex's native approval settings still apply, including MCP tool approvals.
A tool with a destructive annotation can require approval even when
`approval_policy="never"` is configured (that policy refuses the call).
Use Codex's per-server/per-tool approval configuration when configuring unattended
workers; this app does not automatically approve requests. The native terminal
handles interactive requests. See [Codex MCP settings](https://learn.chatgpt.com/docs/extend/mcp).

Delivery receipts are marked sent after app-server accepts the queued input.
A lost acknowledgement stops the execution without replaying the request.
On explicit resume, pending graph tasks receive fresh attempts; old graph queue
entries are withdrawn because their attempt handles expired. History remains,
so a task may be presented again after restart. This is not an exactly-once
model execution guarantee.

### Claude

The server needs Claude Code installed and signed in. Each Claude agent keeps
one conversation for life: its ID is saved in the node's directory, and every
launch continues it. Claude loads the node tools with `--mcp-config`,
alongside your own MCP servers, and the node's prompt with
`--append-system-prompt`, re-read on every request so a changed prompt applies
after a restart. It works in the node's private folder and may also use the
node tools' checkouts. Variables an enclosing Claude Code session exports are
dropped from the session's environment, so a session resumed or attached from
inside Claude Code still runs independent agents.

With a terminal, Claude runs in the node's terminal and reports what it is
doing through hooks. Graph work is typed into its prompt only while it is idle:
a short request, the recorded envelope as a paste, then Enter. The delivery is
marked sent when Claude's prompt hook reports it. While someone attached to the
terminal is typing, delivery waits until they have stopped for ten seconds. A
draft left in the input box is stashed (Ctrl+S) first, and Claude restores it
after the delivery; if a draft is already stashed, delivery waits until you
restore it. Input Claude does not take within twenty seconds fails its attempt,
which retries under the node's policy. A turn that fails (an API error) or is
interrupted fails the attempts it carried. Exiting Claude leaves the node
running; press Enter in its terminal to resume the conversation.

Claude asks whether to trust a folder it has not seen and runs no hooks until
you answer; its default answer exits. Node folders live in the data directory,
so trust it once: run `claude` in `~/.ontography` and accept. Folders inside a
trusted folder are trusted. Until then the node's status says it is waiting.

Headless, Claude runs with `-p`, reading and writing stream JSON. Each message
is sent once the previous turn has finished, and a turn that ends in an error
fails its attempt. Nothing can answer a permission prompt, so whatever would
prompt is denied: allow the node tools in your Claude settings (for example
`"permissions": {"allow": ["mcp__ontography_node"]}`), or set
`permission_mode`. In a terminal, Claude's permission prompts appear there as
usual; `bypassPermissions` also shows Claude's own confirmation when it starts.

## Edit and recover

1. Read `flow.status` and change its document.
2. Call `flow.edit` with the updated `document`. Inspect `changes`, the number
   of nodes and connections the edit adds or removes (0 when the graph stays
   the same), and `retirements`: pending work the edit would discard.
3. Call `flow.commit` with the returned `plan_id`.
4. Read status. Use `flow.resume` to recover an interrupted edit or restart
   stopped or failed workers.

A command config change takes effect on the next task. A change to what an
agent node is bound to (its settings over its component's defaults) stops and
replaces its process, retaining its node directory and recorded conversation.
Tool selection, grants, retry policy, and changes elsewhere in the graph refresh
the agent's scoped tooling without restarting its process. Changing what core
declares about a node, its types, join, result contract, root (the entry's
default one included), or transitions, replaces its core node, which retires
its pending work; the preview reports that work, and the old worker stops
before its replacement starts. Likewise, changing a connection's contract,
authority, or match replaces it. A replaced node or connection takes a new
core identity. The initial entry cannot be replaced before its initial task
completes.

Each edit is one atomic core graph edit: it adds the target's nodes and
connections that core lacks and removes those the target lacks. Existing
workers continue meanwhile. The app saves the target and its new identities
before changing core. Recovery computes the edit again from core's graph and
finds either the whole edit or nothing left to do. If new work would be
retired, it stops with `retirement_preview_required`; preview the same target
again and review the additional retirements. If ongoing work makes the edit
stale eight times, it stops with `workflow_busy`; `flow.resume` continues it.
There is no rollback or substitution of an unrelated target mid-recovery.
Core accepts edits to a run only from this editor.

A stale topology preview before the edit starts needs a new preview. Config-only
edits tolerate unrelated tasks completing, but still reject a changed document
version or unexpected graph changes. If a response is
lost, read `operation.get` or current status before retrying. Repeating a saved
commit recovers that edit; accepted initial input is not replayed on resume.

`flow.define` saves a reusable document revision. `flow.start` accepts that
`revision` instead of a document. `flow.promote` saves the current run document
as a reusable revision after any pending edit finishes.

## Workspaces

Start with `"workspace":"path/to/directory"` instead of `message` to import
an initial workspace. A command task executes in a private checkout and
publishes its captured changes. The original directory is preserved. Without a
workspace, the command runs in the workflow project and publishes stdout's
exact bytes. Non-UTF-8 output needs a `bytes` contract; a run keeps the
contracts it started with, so a run without one parks such a command. Stdout
that is a package envelope, well-formed or not, is always refused and its task
parks with the input unconsumed: only a workspace input grants a task a package
view, and a task with one publishes its checkout, not stdout.
Persistent agents use the selected node tools to open workspace packages in
attempt checkouts, capture changes, and submit them. Their persistent session
working directory is not automatically captured as graph output.

For a human task, call `flow.workspace` with `action:"open"`, its `node`, and
the `task_id` from status. This opens that task's current input and rejects
a task that has already been replaced or completed.
The result gives a private `path` and `workspace_id`. Edit files there, then
call `action:"capture"` with that handle. Supply the captured `workspace_id`
to `flow.decide` instead of a message. Release with `action:"release"` when
finished. Release retains a captured checkpoint; `action:"open"` with the
saved handle reopens it, including after restart. Opening an explicit local
`path` is also supported.

`flow.export` writes a workspace result to a new directory, preserving
directories, files, symlinks, and executable bits. Output/status represent a
workspace as `{"workspace":true}`; use workspace/export tools to obtain files.
Exported message and ordinary files use mode `0644`, executable files `0755`,
and directories `0755`. Staging remains private until publication.

## Selecting inputs and results

`flow.output` on a human node shows its ready task before any earlier decision.
Use `task_id` to pin that task. `source:"output"` explicitly reads the node's
last execution/decision result; `source:"pending"` reads pending inputs.
These selectors also apply to `flow.workspace` open and `flow.export`.
Resume a suspended run to inspect current tasks or inbox items; the explicit
`source:"output"` selector can still read a cached result while suspended.

An inbox returns `items`, each with an opaque `work_id` and an input preview.
Pass that `work_id` to read, open, or export the exact item. A single item can
be selected implicitly; several items require a selection for open/export.
Items are ordered by stable identity, with no claim of arrival order. To page,
set `limit` (1–100, default 20), then pass `next_after` as `after` along with
the returned `revision`. A changed revision rejects the page; restart listing.
Handles remain valid across restart while the item remains pending at the node.

A joined task returns `work_ids` matching its input array. Select a `work_id`
to export one input. Workspace open selects the unique workspace in that task,
allowing accompanying messages to remain visible in status/output.

Worker status uses `execution.state` (`running`, `exited`, `failed`, `panicked`,
or `aborted`) and, on failure, `execution.error.class` and `.message`. Error
messages preserve their original text. When no worker handle exists, `execution`
is null. A failed task does not fail its worker.

Agent nodes also report `session`: its lifecycle state, terminal status,
persistent directory and working directory, native conversation ID when managed
by Codex, its observed `agent_state` (`idle`, `active`, etc.), and any startup or process error. A failed or exited agent session
stays stopped until `flow.resume` or a change to that node's definition; its
process is not restarted by the task retry policy. Opening a node terminal in
the graph does not start or resume its process.

## Failed tasks

A command task fails when its process exits nonzero, times out, or
exceeds an output limit, when core rejects its result, or when the task cannot
start as delivered. Its worker keeps running: the task waits out a backoff and
is attempted again, while other tasks at the node proceed. At an `all` node, a
failed join keeps its inputs together for its retries, and the node's other
inputs form joins of their own. A node's `retry` sets the policy:

```json
{"id":"draft","component":"command","config":{"argv":["./draft.sh"]},
 "retry":{"max_attempts":3,"initial_delay_secs":5,"max_delay_secs":300}}
```

These are the defaults, and each field is optional. `max_attempts` includes the
first attempt (1–100; 1 disables retries). The first wait is
`initial_delay_secs`; each later wait doubles, up to `max_delay_secs` (at most
86400, and not below the first wait). Human and inbox nodes reject `retry`.

A task whose attempts run out is parked, as is one that retrying cannot help,
such as a task that received two workspaces or a workspace that is not a
directory, an input core refuses to begin, or output core refuses, like
non-UTF-8 stdout under `text` or a package envelope on stdout. A parked task
stays pending, and
no attempt starts until the manager intervenes. `flow.status` lists failed
tasks in `failures`:

```json
{"node":"draft","task_id":"task_…","attempts":1,"state":"retrying","retry_in_secs":5,
 "error":"Worker exited with status 3: …"}
```

`state` is `retrying` or `parked`; `retry_in_secs` appears while a backoff
remains. `tasks` shows what each worker runs next, so a task that is waiting
or parked appears only in `failures`. Its `task_id` also selects its input in
`flow.output`, `flow.workspace`, and `flow.export`.

- `flow.retry` with `node` and `task_id` gives a failed task a fresh set of
  attempts, starting now. Without `task_id`, it does so for every failed task
  at the node.
- `flow.discard` with `node` and `task_id` retires a parked task's pending
  input; the run's initial input is marked complete instead. Either way, the
  task is never attempted again.
- Any change to a node's definition (its config, retry, grants, join, or kind)
  gives its failed tasks fresh attempts. The node's worker, or `flow.resume`,
  applies it even if the edit reported an error, and an attempt already
  running under the old definition does not count against the new one. A
  change elsewhere in the graph, such as adding the connection a task was
  missing, does not; retry the task. At an `all` node, though, a failed join
  that no longer has one input per connection is no longer a task, and its
  inputs join afresh.

Failure counts are durable. Suspension, restart, and `flow.resume` keep them
and keep parked tasks parked; only an unfinished backoff is cut short. A crash
between core rejecting a result and the ledger counting it grants one extra
attempt.

## Current policies and limits

- A command's result and a human's decision go through every outgoing
  connection. Agents route per connection with the node tools.
- Each delivery is a message, a workspace, or, on a `bytes` connection, other
  bytes. A joined task may receive several messages but at most one workspace. Workspace workers currently
  need an outgoing connection; connect the final worker to an inbox.
- A failed or timed-out task retries with capped exponential backoff, then
  parks until the manager retries or discards it. An interrupted attempt is not
  counted. External command effects may repeat if a previous attempt ran but
  did not publish to core.
- Command config updates do not interrupt a task already running. A change to
  an agent's binding restarts its session process. Removing or replacing a
  node stops its old worker during reconciliation.
- Command task timeout defaults to five minutes. Captured stdout/stderr are bounded
  to 1 MiB each; status/output previews truncate long text. Export retrieves
  the complete accepted message or workspace.
- `flow.decide` completes human tasks; it is not arbitrary message injection
  into any node. Inbox inputs stay pending until the graph or workflow uses
  them.
- Result export and document promotion do not produce a complete portable
  run backup. Core remains unchanged.

The manager uses workflow terms. Internal package, context, and workspace
capabilities still do the underlying work; their necessity is separate from
which tools the manager sees. See [CORE_BINDINGS.md](CORE_BINDINGS.md).
