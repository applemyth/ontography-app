# Node tools

Node tools are the operations a node's worker calls on its own node: read its
inputs, run attempts at its tasks, and publish results. They live in
[`src/node_tool`](../src/node_tool) and run against core through one
execution's context. Workers name nodes and connections by their workflow
names and pending work by opaque handles; the tools never ask for or report
core package, activation, node, or edge identities. The [node MCP adapter](NODE_MCP.md)
lists selected tools and relays calls from Codex. The persistent Codex node
runtime owns and refreshes the context and its private transport endpoint.

Managed Codex nodes also receive graph work as conversation messages. The host
begins the attempt and supplies its `attempt_id` and input handles; the agent
uses those handles directly instead of beginning the delivered task again.
This input channel is independent of tool selection. Its exact message envelope
is recorded as a `node_message` response and marked sent when app-server accepts
the queued input. The shared tools still govern reading attachments, submitting
results, and failing attempts.

## Hosting

One `NodeToolContext` serves one execution at one node:

```text
executable launched at a node
└── NodeToolContext::new(execution, session, scope, ledger, node directory, initial input)
    ├── catalog()           selected tools this node may call, given its grants
    ├── call(name, args)    → Reply
    │   └── transport sends reply.bytes() unchanged, then reply.sent()
    └── close()             when the execution stops
```

The scope carries the node's document settings and the workflow state that
names every node; the host refreshes it after each edit. The retry ledger is
the node's, shared with its worker and the manager's flow tools. Pass the
initial input only while it is pending at the entry node. One context serves a
node directory at a time; at startup it removes checkouts that a crash left
behind. Once the execution is stopping, the tools begin no attempt and send or
retire no package. `close` lets operations in progress finish and their
replies be marked sent, then interrupts the open attempts; their tasks stay
pending.

## Tools

| Tool | What it does | Grant |
| --- | --- | --- |
| `inspect_node` | This node's name, types, component, settings, join, grants, retry policy, neighbors, and open attempts. | |
| `inspect_graph` | Current nodes and connections, by name. | |
| `list_inputs` | Pages the inputs waiting here. At an `any` node each input is a task, shown with its retry state. | |
| `next_trigger` | The next task that may begin: the initial input, one input, or a complete join. | |
| `wait_for_change` | Waits up to 30 seconds for a change that could make work runnable, or for a stop. | |
| `begin_invocation` | Begins an attempt at a task and returns its input handles. | |
| `describe_package`, `list_package`, `read_package` | Describe, list, and read the attempt's inputs. Reads return at most 256 KiB; collections of more than 1000 entries are refused, to be opened as a workspace instead. | |
| `import_content` | Stores text as a new file output. | |
| `compose_package` | Builds a directory output from named entries, or from changes to a directory. | |
| `open_workspace`, `capture_workspace`, `release_workspace` | Check out a workspace input or directory output, capture edits as an output, remove a checkout early. | |
| `submit_invocation` | Submits the result and outputs. | |
| `fail_invocation` | Ends the attempt as failed, retryable or not. | |
| `begin_invocation` with `originate` | Starts new work at a root node with the worker's own message. | `originate` |
| `submit_invocation` outbound outputs, `list_outbound`, `transfer_package` | Create packages to send later, list them, and deliver them to a successor. | `send_later` |
| `retire_package` | Discards an input waiting here or an outbound package; core records the retirement. | `retire` |

Grants are set per agent or command node in the workflow document:
`"grants": ["send_later"]`. Optional `"tools": ["inspect_node", "list_inputs"]`
restricts the shared tool catalog and calls to those names; omitted means all
tools allowed by grants, and `[]` means none. The list never adds grants. Live
edits refresh the context and notify connected MCP clients. A node
cannot originate work while the run's initial input is still pending at it.
Beginning attempts, transfers, and retirements run one at a time, so a package
is never both sent and retired.

Handles are named for what they identify: `attempt_id`, `task_id`, `work_id`,
`workspace_id`, and `output_id`, plus core's input and member `handle`s. A
member's handle is its input's handle, then `/` and its path inside that input.
Listings return `next_after` to continue the same listing after its last entry;
entries added or taken between pages may be missed. Replies of the first five tools carry a
`version`; pass it to `wait_for_change` as `after`, and the wait ends at once
if the graph, pending work, retry state (including a backoff that ran out), or
the node's settings changed since.

## Attempts

An attempt is one recorded try at a task, bound to a core invocation. A task is
one input, one complete join, or the initial input at the entry. Each task has
at most one open attempt. `submit_invocation` takes a `result` (a `message`, or
a `workspace` handle) and optional `outputs`. Without `outputs`, the result
goes to every successor; with them, exactly those are sent: each to a
successor named by `to`, to every successor, or, with the `send_later` grant,
kept here with `outbound: true`. Invalid arguments leave the attempt open.
Once core decides, accepting or rejecting the whole submission, the attempt
ends.

A failed or rejected attempt counts against the node's retry policy. The task
waits out its backoff while other tasks proceed, and parks once its attempts
are used up or `fail_invocation` says `retryable: false`. A task core refuses
to begin, such as an input larger than an attempt's context budget, parks at
once. The manager retries or discards parked tasks; see
[Workflows](WORKFLOWS.md). Attempts interrupted by a stop don't count, nor do
attempts begun before a change to the node's definition: their `retry` state
is `fresh`, and the task starts afresh under the new definition.

Each attempt has core's default context budget: 8 MiB returned to the worker,
and 4096 receipt events, which every recorded reply and sent mark draws on. A
call too large for what remains of the bytes is refused (`too_large`), and the
attempt stays open to read less, submit, or fail. Once the events are spent,
core records nothing more: a call that needs a record fails the attempt
(`budget_exhausted`), and core ends the invocation on the next refused call.
An attempt fails whenever core ends its invocation (`attempt_ended`).

At an `all` node, a failed join keeps its inputs together, so its retries use
the same set; the node's other inputs form joins of their own. A failed join
that no longer has one input per connection into its node, for example after
a connection is added, is no longer a task: its inputs join afresh.

## Receipts

While an attempt stays open, each successful reply is exactly what core
recorded: core records its own list and read replies, and the tools record the
rest before returning them. The transport must send `Reply::bytes` unchanged
and call `Reply::sent` after writing, which marks the receipt sent. Ending an
attempt, by submission, failure, or a stop, first waits up to five seconds for
its replies to be marked, since core records nothing after an invocation ends;
a reply never written stays prepared. Tools bound their own replies (ranges, pages) instead of relying on
truncation. Replies outside attempts carry metadata only, never payload bytes;
replies that end an attempt, and errors, carry only outcomes.

## Content and workspaces

New files and directories are staged for the attempt that created them. A
submission declares their complete dependency closure, and core retains what it
publishes when it accepts; anything unpublished is released when the attempt
ends. `open_workspace` accepts a workspace input or a directory output. A
directory nested inside an input has an identity that is meaningful only at its
path, so it can be used only through its input: open the whole input, or
compose its files. Checkouts are private directories, removed when their
attempt ends or, after a crash, when the node's next context starts. A
read-only checkout that was modified cannot be captured.

## Omitted

`package_parents` is not offered. Core grants ancestry only as a whole and
charges it to each attempt's package budget, so in a looping workflow
beginning an attempt would eventually fail.
