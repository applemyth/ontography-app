# Architecture

The app builds on `../ontography-core`. This document separates the agreed
implementation scope from [Tentative ideas](#tentative-ideas). Item numbers
preserve references from other documents; build order is stated separately.

## Goal

Run Codex nodes in managed terminals, exchange message and workspace packages
through governed edges, and edit the live graph through core's rewrite rules.
Pi manages the graph using Ontography tools. Core supplies admission, contracts,
authority, package/workflow state, and persistence.

## Current scope

- The original pieces 1–8: Codex node, workspace/message/union definitions,
  edges, node harness, node MCP, and the Pi management/session foundation.
- A document and compiler to assemble those pieces, with the app edit grammar
  and a diff/retirement preview.
- Process reconciliation and node terminal views.

The document/compiler, edit recovery, task harness, and persistent Codex node
runtime are implemented. Agent nodes own interactive Codex sessions in managed
PTYs; command nodes run individual tasks, human nodes wait for decisions, and
inboxes hold results. Node-scoped MCP exposes selected graph tools to Codex;
automatic incoming-work wakeups and node panes remain unfinished. Starting an
agent starts an idle conversation. Pi remains the management harness. Core is
unchanged by this work.

## The picture

```text
manager (Pi)
 └─ workflow document          named nodes, settings, joins, connections
     ├─ translator → core      graph declaration + fixed rewrite grammar
     ├─ agent → node runtime   managed PTY + continuing Codex session + scoped tools
     ├─ other kinds → harness command tasks, human decisions, inboxes
     └─ editor → core          preview changes, save target, apply/recover

core commits graph and package state; workflow runtime reconciles worker processes
```

| Piece | Job | Items | Status |
| --- | --- | --- | --- |
| Shell + UI | Where you sit. Attach, detach, look at the graph and the panes. | 8, 12 | Manager terminal and graph view exist. Node panes missing. |
| Manager | Creates documents, starts runs, reads status, and requests edits. | 8, 11 | Document tools and Pi allowlist implemented. |
| Workflow document | Named nodes, kind/config, joins, directed edges, and entry. | 10 | Implemented. |
| Compiler + library | Fixed vocabulary, declaration expansion, edit grammar, and diff. | 2–5, 9 | Implemented using existing core APIs. |
| Core | Enforces graph and package/workflow semantics. | — | Existing dependency; no additions in scope. |
| Nodes | Host continuing agents or execute tasks using scoped graph tools. | 1, 6, 7 | Codex PTY runtime, task harness, selected node tools, and MCP implemented; automatic incoming-work wakeups remain. |

## Workflow document

The manager writes it. The compiler checks it and returns errors in these terms.

```json
{
  "name": "review-and-fix",
  "entry": "triage",
  "nodes": [
    {"id": "triage", "kind": "agent", "config": {"prompt": "Review the input and describe the changes needed."}},
    {"id": "fix", "kind": "agent", "config": {"prompt": "Apply the requested changes and report the result."}},
    {"id": "result", "kind": "inbox"}
  ],
  "edges": [
    {"from": "triage", "to": "fix"},
    {"from": "fix", "to": "result"}
  ]
}
```

Fields: `name`, `entry`, `nodes` (`id`, `kind`, `config`, `join`), and directed
`edges` (`from`, `to`). Join belongs to the receiving node: `any` (the default)
takes one available input; `all` waits for one from every incoming edge. There
is one entry. Cycles and self loops are supported; duplicate connections are
rejected. Kinds are `agent`, `command`, `human`, and `inbox`. See
[WORKFLOWS.md](docs/WORKFLOWS.md) for configuration and a working example.

## Library

The app supplies the initial vocabulary and defaults.

- Worker kinds: `agent` (Codex), `command`, `human`, and `inbox`.
- Package types: `workspace` (item 2), `message` (item 3), `union` (item 4).
- One edge rule accepting `union` (item 5).
- One implicit authority tag on every edge and root.
- One fixed grammar installed on every document run: add/remove nodes and
  add/remove connections, including self loops. Replacement uses removal and
  addition (item 9).

Every kind uses the same core node type, result contract, edge rule, and tag.
Kind and worker config live only in app execution bindings. The four core
variants come from join (`any`/`all`) and root status, not worker kind. The
generated grammar has 40 productions: eight node rules, 24 connection rules,
and eight self-loop rules.

Command config changes take effect on the next task. Agent config changes stop
and replace the running session process, preserving its recorded conversation
and working directory. Grants and topology refresh its tooling scope in place.
Changing kind stops and waits for the old worker before launching its replacement.
A prompt or kind change alone does not rewrite the core graph.

## Process

Define, compile, run, inspect, edit.

1. **Describe.** Say what the workflow should do. The manager writes the document.
2. **Compile.** The document becomes a definition, or plain errors.
3. **Run.** Start with an input. Command tasks run when inputs arrive; agent
   sessions start with the graph and remain running. Agents can discover and
   process work through MCP tools; automatic wakeups remain unfinished.
4. **Watch.** Read graph status, pending tasks, and committed outputs. Node
   terminal panes remain future work.
5. **Edit.** Change the document, inspect the edit steps and retirement preview,
   and commit. Reconcile node processes with the committed graph.

## Pieces

Checkboxes indicate implementation, not completion of final release gates.

- [x] **1. Codex node definition**

   An `agent` binding launches a persistent interactive Codex session in a server-owned `portable-pty` terminal. The node runtime binds its core identity, persistent private working directory, configuration, native conversation reference, and `NodeToolContext`. Startup establishes a durable Codex conversation through its app server, then launches the interactive CLI with that exact conversation ID and its scoped MCP adapter. Stop, process exit, definition changes, and resume reconcile these resources. An `argv` override hosts another interactive command without managed Codex conversation recovery. Node panes belong to item 12; automatic incoming-work wakeups remain follow-up work.

- [x] **2. Workspace package**

   Uses core's existing package format: collections, files, symlinks, and changes against a base. Core resolves a complete directory view; the app's workspace store ([`src/workspace`](src/workspace)) prepares a private copy-on-write checkout and captures edits as a changes package. The app supports manager checkouts/checkpoints and export to a new directory. A task can receive at most one workspace.

- [x] **3. Message package definition**

   A message is `{"message":"text"}`. The contract validates it; the command task harness supplies its text on stdin and records delivery receipts. Delivering graph messages to a continuing interactive Codex conversation remains unfinished.

- [x] **4. Union package definition**

   One core `Contract` and object type accept either `Message` or core's native `WorkspaceEnvelope`. Each delivery carries one of these forms. There is no combined message/workspace payload form; a joining node may receive separate messages and one workspace.

- [x] **5. Edge definitions**

   One shared directed edge rule accepts the union contract with the fixed authority tag. A successful task broadcasts its result through every outgoing connection. Replies require a reverse connection. Worker kinds introduce no additional routing or authority rules.

- [x] **6. Node harness**

   Prepares invocation context and private workspaces, delivers messages, runs command tasks, captures outputs, and publishes through core. Human nodes wait for `flow.decide`; inboxes hold input without executing. Core validates publication and input consumption. Workflow runtime reconciles workers after edits and restart: stop, keep, update settings, or launch. A failed command task retries with capped backoff while other tasks proceed, then parks until the manager retries or discards it; failure counts are durable. A change to the node's definition grants fresh attempts. Agent bindings use the persistent node runtime instead of the per-task harness.

- [x] **7. Node-scoped MCP interface**

   Shared tools in [`src/node_tool`](src/node_tool) serve one execution at one node. The [MCP adapter](docs/NODE_MCP.md) exposes the node's selected tools, with grants enforced in both catalog and dispatch. Managed Codex sessions receive a stdio adapter connected to a private execution socket; they cannot select another node by name or ID. Tool text preserves the exact receipt bytes, and delivery is acknowledged after stdout flush. Live selection/grant edits notify clients without restarting Codex. Automatic turns for incoming work remain separate follow-up work.

- [x] **8. Graph TUI and native management harness**

   The management layer and persistent session foundation are implemented. Final verification and rollout are tracked separately in [VERIFICATION.md](docs/VERIFICATION.md).

   | Piece | Responsibility |
   | --- | --- |
   | CLI + Pi harness | Select or create an Ontography session, resume its graph, and attach to its server-owned native Pi terminal. |
   | Ontography tools | Expose supported core APIs through session-scoped requests. The server owns accepted operations and retained core resources independently of clients. |
   | Graph TUI | `/graph` opens the owning session's run in the Rust Ratatui renderer; closing it returns to the same manager process. `--ui` opens this view first. |

   Native Pi owns its conversations and model execution. Ontography owns conversation membership/selection, app tool preferences, graph binding, process supervision, and terminal attachment. Core owns graph/workflow semantics, packages, contracts, authority, and durable graph history.

   The initial implementation's separate Rust/Pi RPC conversation UI remains in source for its earlier tests; it is no longer the CLI's `--ui` path. Current graph display reuses the existing Ratatui graph widget, which supports cycles, self loops, and parallel edges. The [renderer decision](src/ui/README.md#renderer-decision) records that evaluation.

   The workflow layer reuses these internal core capabilities:

   | Core capability | Responsibility |
   | --- | --- |
   | Definitions/admission | Validate the generated schema, graph, contracts, authority rules, and rewrite grammar. |
   | Run lifecycle | Create/open, inspect, suspend, resume, and terminally close runs; supervise executable tasks. |
   | Graph rewriting | Prepare and commit configured productions with revision checks and retirement evidence. |
   | Workflow | Commit results and governed deliveries; consume or retire pending work with recorded reasons. |
   | Content/packages | Import/read/retain, compose/resolve, and export artifacts. |
   | Invocation/context | Issue scoped invocations, prepare grants, enforce budgets, and record observed exposure/delivery evidence. |
   | Inspection | Inspect topology, frontier, package history, executions, and revision notifications. |

   Workspace checkout and capture are the app's own, in [`src/workspace`](src/workspace), built on core's content and packages. The workflow harness launches concrete workers from app bindings. These internal content, package, workspace, and context capabilities remain necessary even though Pi no longer sees their raw tool families.

- [x] **9. Vocabulary and compiler**

   Expands the document into the existing `GraphDeclaration` with app-owned `ExecutionBinding` settings and the fixed grammar. It does not add another graph representation or depend on native `ApplicationDeclaration` factories. The workflow harness owns initial input, workspace setup, and process reconciliation. Existing non-document runs retain their original declarations and grammar.

   The editor compares documents and previews ordered rewrites against cloned core state. It saves the target document, allocated identities, and the exact approved retirements before applying changes. Stable names map to persisted core identities; removed identities are never reused. The last completed document and any pending target are explicit app metadata; core's actual graph records which transitions committed.

   Each rewrite is atomic; a whole edit can span several transitions. Workers continue during that edit. Recovery reads the actual graph, prepares the next step at the current revision, and proceeds toward the saved target. It stops if a step would retire work absent from the preview. The manager must review that same target again before continuing; recovery never rolls back committed transitions or silently approves additional retirements. Process reconciliation follows successful completion.

- [x] **10. Workflow document**

   The schema in [Workflow document](#workflow-document) and its validation. Defines named nodes, worker kinds/settings, per-node joins, connections, and one entry.

- [x] **11. Manager surface**

   Pi exposes `flow.define`, `flow.start`, `flow.status`, `flow.output`, `flow.edit`, `flow.commit`, `flow.resume`, `flow.decide`, `flow.retry`, `flow.discard`, `flow.workspace`, `flow.promote`, and `flow.export`, plus `session.context`, `session.inspect`, and `operation.get`. There is no capability-group activation tool. The session handshake also advertises session/terminal lifecycle and system controls for CLI/extension use.

   Unused raw package, invocation, context, project, network, and fact tool wrappers are deleted, along with content mutation/export tools and unused rewrite/workspace operations. Remaining declaration, lifecycle, inspection, execution, and workspace adapters serve existing infrastructure and tests. Raw mutations reject document-owned runs, including through unscoped calls. The manager uses document names, task IDs, and workspace handles rather than core incarnations or content roots. Core package/context APIs and the app's workspace store still provide necessary internal plumbing; deletion of their tool wrappers does not mean deletion of those capabilities. No total code-size reduction is claimed here.

- [ ] **12. Node panes**

   Attach a client to a node's terminal the way the manager terminal works today. Opening or closing a pane has no graph effect.

**Build status.** The document, fixed vocabulary, translator/editor, task
harness, persistent Codex runtime (1), and Pi surface are implemented. Remaining
execution/UI work includes automatic incoming-work wakeups, verification of
interactive package handoffs between agents, and node panes (12).
Core remains an existing dependency; no composite-production API was added.

## Session foundation

```text
Rust server
└── Ontography session
    ├── Pi manager state: conversations, active selection, tool preferences
    ├── Server-owned shell, PTY, and terminal state → managed Pi process
    └── One graph run/runtime after initialization
```

A new session can start Pi before the graph exists. The first scoped start operation persists a reserved run identity and resolved definition, constructs core storage, and binds that run. Retries recover the same initialization. Existing runs are adopted explicitly; no Pi ownership is inferred from cwd or old transcripts.

Bare `ontography` always creates an independent app session. `attach NAME_OR_ID` (or `--session NAME_OR_ID`) explicitly attaches; persisted selection is metadata and never an implicit launch target. Pi `/new`, `/resume`, `/fork`, `/clone`, and `/tree` preserve its graph. One controlling client owns the manager terminal's input/dimensions. Ctrl-B, then D detaches while manager and graph continue. Pi `/quit` returns to the managed shell and session panel. Shell `pi` resolves the latest saved active conversation and launches it with the same graph binding. Shell `exit` suspends the session and graph even while detached. Explicit suspension also stops the terminal; closure is terminal. Server restart loads records; attachment explicitly resumes the named session's graph and native history.

`portable-pty` hosts manager and worker processes; `vt100` maintains terminal state; the Rust client renders native Pi, shell + session status, or the bound graph. Worker execution reconciliation provisions persistent Codex terminals and scoped MCP. Worker terminal views remain subsequent work.

The [session guide](docs/SESSIONS.md) documents commands and storage. [SESSION_DESIGN.md](docs/SESSION_DESIGN.md) records ownership and recovery details. [CORE_BINDINGS.md](docs/CORE_BINDINGS.md) maps the existing core API. [PLAN.md](PLAN.md) is the original item-8 implementation plan.

## Management status

- [x] **Per-user app-home support.** Default storage is `~/.ontography/`, with existing explicit/environment/XDG overrides. A journaled migration archives legacy home contents, moves the stopped previous store, and leaves an alias at its old path. It preserves existing run identities; storage migration does not replace a core build or grammar.
- [x] **Live home migration and final rollout verification.** Automated gates and native Pi/terminal checks passed. The legacy home was archived, the current store moved to `~/.ontography`, and all three prior runs preserved and resumed. [VERIFICATION.md](docs/VERIFICATION.md) records the evidence.
- [x] **Graph display inside the Pi interaction.** `/graph` and `--ui` use the same server-owned native Pi manager and the session's bound run; there is no second management conversation.
- [x] **Unified Ontography session.** Durable manager state, graph initialization/adoption, conversation ownership, selection, lifecycle commands, and scoped dispatch are implemented. App-session fork/archive/delete remain deferred.
- [x] **Rust manager terminal backend.** Server-owned PTY, terminal parsing, attachment snapshots, input/resize ownership, detach/reconnect, and manager supervision are implemented. This replaces the proposed tmux backend.
- [x] **Persistent session shell.** Launch into Pi, return to shell + session status on `/quit`, resume the active conversation with shell `pi`, and suspend the session when its shell exits. Structured process leases distinguish manager state from terminal state.

The app's dynamic vocabulary extension adapter is removed. Document workflows keep their vocabulary and grammar fixed. Previously extended runs are not supported by the fixed-definition reopening path; stored runs must match the selected core build and declaration. This layer adds no storage migration. [CORE_UPDATE.md](docs/CORE_UPDATE.md) records earlier integration work, including operations that are no longer exposed.

A definition is reusable configuration; starting it creates a run. Rewriting changes that run's current graph and preserves its history under the configured grammar. Core commits graph/package state; the worker supervisor reconciles external processes separately and exposes partial failures. Opening or closing a terminal view has no graph-topology effect.

## Policies and remaining work

The command task harness makes explicit choices: broadcast each result to all successors,
allow one workspace per task, retry a failed task with capped backoff and then
park it for the manager, and apply new config at the next task boundary.
Workspace workers currently need an outgoing edge; an inbox provides a terminal
result holder. These are app policies, not restrictions imposed by worker kinds
or new core semantics.

Remaining within the agreed scope: automatic incoming-work wakeups, interactive
verification of package handoffs between continuing sessions, and node panes.
[WORKFLOWS.md](docs/WORKFLOWS.md) describes current behavior and recovery limits.

## Tentative ideas

Retained for discussion. These are not implementation commitments or
prerequisites for the current scope; each needs a separate decision before
being added to the plan.

- **More worker implementations:** additional agent harnesses beyond Codex.
- **Workflow conveniences:** skills delivered as content packages, dynamic
  fan-out/collectors, and nested or child workflows.
- **Portable runs:** complete run backup/import. Promoting a document revision
  and exporting a result file/workspace are already implemented.
- **Interchangeable managers:** a standalone management MCP server, other
  harnesses holding the manager seat, and generic manager-kind/launch/resume
  records. The node-scoped MCP in item 7 remains in the current scope.
- **Additional policy:** human-gate authority tags, restrictions between new
  node kinds, and a dedicated operator node for manager messages.
- **Session conveniences:** app-session fork/archive/delete, shared viewers,
  and controller takeover.
- **Core investigation:** verified replay across rewrites/transfers/retirements,
  frontier-local rewrite preparation, retention after failed commits,
  tag-free edges, and schema-parameterised validators. None is required by
  this app scope.

## Proof

Automated coverage exercises document validation, all grammar variants,
preview/recovery, command → human → inbox execution, workspace capture/export,
config changes, and worker reconciliation. See
[CORE_BINDINGS.md](docs/CORE_BINDINGS.md#verification-evidence).

The remaining full acceptance flow is interactive: create two Codex nodes,
deliver messages and workspaces through core, edit with a retirement preview,
and attach to each worker terminal. Verify detach/restart and resumed execution.
Persistent-process fixtures establish lifecycle behavior, but do not complete
that graph-communication and interactive-terminal gate.
