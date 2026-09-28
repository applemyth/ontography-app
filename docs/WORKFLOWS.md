# Workflows

A workflow document names the steps, their worker settings, how they connect,
and where the first input goes. The app translates it into the existing core
graph. Core records accepted work; the app harness runs the workers.

## Start a small workflow

Save this as `start.json`. It is the complete argument object for `flow.start`:

```json
{
  "document": {
    "name": "review-a-draft",
    "entry": "draft",
    "nodes": [
      {"id": "draft", "kind": "command", "config": {"argv": ["/bin/echo", "Draft ready"]}},
      {"id": "review", "kind": "human", "config": {"prompt": "Check the draft"}},
      {"id": "result", "kind": "inbox"}
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
| `nodes[].id` | Unique name used by tools and connections. |
| `nodes[].kind` | `agent`, `command`, `human`, or `inbox`. |
| `nodes[].config` | Worker settings; defaults to `{}`. |
| `nodes[].join` | `any` by default, or `all`; belongs to the receiving node. |
| `nodes[].retry` | Agent and command nodes only; see [Failed tasks](#failed-tasks). |
| `nodes[].grants` | Agent and command nodes only: node-tool powers beyond the base set (`originate`, `send_later`, `retire`); see [Node tools](NODE_TOOLS.md). |
| `nodes[].tools` | Optional shared node-tool allowlist for agent/command nodes. Omitted uses the grant-filtered default; `[]` exposes none. |
| `edges` | Directed `{from,to}` connections; defaults to `[]`. |

`any` takes one available incoming package. `all` waits for one package on
every incoming connection. Cycles and self loops are supported; duplicate
connections are rejected. Worker kind does not change core graph rules.

| Kind | Config | What it does |
| --- | --- | --- |
| `agent` | Required `prompt`; optional `model`, `harness:"codex"`, or `argv` runner override | Hosts a continuing interactive Codex session in a managed PTY. |
| `command` | Required nonempty `argv`; optional `timeout_secs` | Runs the command with input messages on stdin. |
| `human` | Optional `prompt` | Waits for `flow.decide` on the specific task. |
| `inbox` | Empty config | Holds incoming work for inspection/export. |

An agent node definition looks like this:

```json
{"id":"draft","kind":"agent","config":{"prompt":"Review the input and produce a concise draft."}}
```

The server needs Codex installed and authenticated; a configured `CODEX_HOME`
must be an absolute path. It establishes a saved
conversation through Codex's app server without starting a model turn, then
launches the interactive CLI with that conversation ID. `prompt` supplies its
standing instructions. The session starts when the node starts and remains
alive independently of individual graph tasks. Native Codex trust, onboarding,
and approval prompts retain their normal behavior.

Each agent owns a persistent private working directory under its node's storage
directory, separate from the workflow project and temporary package checkouts.
Restarts retain this directory and resume the recorded Codex conversation.
An `argv` override is the complete interactive command, passed without a shell;
it bypasses Codex setup and has no managed Codex conversation reference.
`timeout_secs` belongs to command tasks and is rejected for agent sessions.

The [node MCP adapter](NODE_MCP.md) exposes the selected shared tools to Codex
automatically, bound to this execution. The agent can use those tools to inspect
and process graph packages. Initial and incoming packages remain pending until
the agent begins and submits work; startup does not start a model turn, inject
packages into terminal input, or publish terminal output. Automatic incoming-work
wakeups and attachable node panes remain future work.
Use command nodes for the end-to-end task workflow above.

## Edit and recover

1. Read `flow.status` and change its document.
2. Call `flow.edit` with the updated `document`. Inspect `retirements`: pending
   work the proposed graph change would discard.
3. Call `flow.commit` with the returned `plan_id`.
4. Read status. Use `flow.resume` to recover an interrupted edit or restart
   stopped or failed workers.

A command config change takes effect on the next task. An agent config change
stops and replaces its process, retaining its node directory and recorded
conversation. Tool selection, grants, retry policy, and changes elsewhere in the graph refresh
the agent's scoped tooling without restarting its process. A kind change stops
and waits for the old worker before launching the replacement. Changing joins or
entry status can require replacing core identities and retiring pending work;
the preview reports that work. The initial entry cannot be replaced before
its initial task completes.

Each graph transition is atomic. A multi-step edit is not: existing workers
continue, and a failed edit may leave some graph changes committed. The app
saves the target and identities before applying steps. Recovery reads the
actual graph and proceeds toward that target. If new work would be retired,
it stops; preview the same target again and review the additional retirements.
There is no rollback or substitution of an unrelated target mid-recovery.

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
workspace, the command runs in the workflow project and publishes stdout.
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
by Codex, and any startup or process error. A failed or exited agent session
stays stopped until `flow.resume` or a change to that node's definition; its
process is not restarted by the task retry policy. Status is available even
though the graph UI does not yet expose node terminal panes.

## Failed tasks

A command task fails when its process exits nonzero, times out, or
exceeds an output limit, when core rejects its result, or when the task cannot
start as delivered. Its worker keeps running: the task waits out a backoff and
is attempted again, while other tasks at the node proceed. At an `all` node, a
failed join keeps its inputs together for its retries, and the node's other
inputs form joins of their own. A node's `retry` sets the policy:

```json
{"id":"draft","kind":"command","config":{"argv":["./draft.sh"]},
 "retry":{"max_attempts":3,"initial_delay_secs":5,"max_delay_secs":300}}
```

These are the defaults, and each field is optional. `max_attempts` includes the
first attempt (1–100; 1 disables retries). The first wait is
`initial_delay_secs`; each later wait doubles, up to `max_delay_secs` (at most
86400, and not below the first wait). Human and inbox nodes reject `retry`.

A task whose attempts run out is parked, as is one that retrying cannot help,
such as a task that received two workspaces or a workspace that is not a
directory, or an input core refuses to begin. A parked task stays pending, and
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

- Each successful task broadcasts the same result through every outgoing
  connection. There is no per-edge routing choice in the document.
- Each delivery is a message or a workspace. A joined task may receive
  several messages but at most one workspace. Workspace workers currently
  need an outgoing connection; connect the final worker to an inbox.
- A failed or timed-out task retries with capped exponential backoff, then
  parks until the manager retries or discards it. An interrupted attempt is not
  counted. External command effects may repeat if a previous attempt ran but
  did not publish to core.
- Command config updates do not interrupt a task already running. Agent config
  updates restart its session process. Removing a node or changing its kind
  stops its old worker during reconciliation.
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
