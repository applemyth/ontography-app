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
| `edges` | Directed `{from,to}` connections; defaults to `[]`. |

`any` takes one available incoming package. `all` waits for one package on
every incoming connection. Cycles and self loops are supported; duplicate
connections are rejected. Worker kind does not change core graph rules.

| Kind | Config | What it does |
| --- | --- | --- |
| `agent` | Required `prompt`; optional `model`, `timeout_secs`, `harness:"codex"`, or `argv` runner override | Runs noninteractive `codex exec` for each task; supplies prompt and input messages on stdin. |
| `command` | Required nonempty `argv`; optional `timeout_secs` | Runs the command with input messages on stdin. |
| `human` | Optional `prompt` | Waits for `flow.decide` on the specific task. |
| `inbox` | Empty config | Holds incoming work for inspection/export. |

Use an agent by replacing the `draft` node with, for example:

```json
{"id":"draft","kind":"agent","config":{"prompt":"Review the input and produce a concise draft."}}
```

The server needs Codex installed and authenticated. Workers currently run
tasks with piped input/output; they do not have interactive terminals, node
MCP tools, persistent Codex conversations, or attachable node panes.

## Edit and recover

1. Read `flow.status` and change its document.
2. Call `flow.edit` with the updated `document`. Inspect `retirements`: pending
   work the proposed graph change would discard.
3. Call `flow.commit` with the returned `plan_id`.
4. Read status. Use `flow.resume` to recover an interrupted edit or explicitly
   retry failed workers.

A prompt/config change takes effect on the next task. A kind change stops and
waits for the old worker before launching the replacement. Changing joins or
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
an initial workspace. A worker executes in a private checkout and publishes
its captured changes. The original directory is preserved. Without a workspace,
the task runs in the workflow project and publishes stdout (or Codex's last
message).

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
is null.

## Current policies and limits

- Each successful task broadcasts the same result through every outgoing
  connection. There is no per-edge routing choice in the document.
- Each delivery is a message or a workspace. A joined task may receive
  several messages but at most one workspace. Workspace workers currently
  need an outgoing connection; connect the final worker to an inbox.
- Failed, timed-out, or interrupted workers do not automatically retry in a
  loop. Resume or changed configuration enables a retry. External command
  effects may repeat if a previous attempt ran but did not publish to core.
- Config updates do not interrupt a task already running. Removing a node or
  changing its kind stops its old worker during reconciliation.
- Task timeout defaults to five minutes. Captured stdout/stderr are bounded
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
