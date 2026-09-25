# App implementation todo

The app builds on `../ontography-core`. These are the eight agreed pieces to implement or integrate. Their numbers identify the pieces; they do not prescribe implementation order.

- [ ] **1. Codex node definition**

   An Ontography node whose app implementation hosts a Codex session in a server-owned terminal using `portable-pty`. Core supplies the node's identity and workflow rules. The node's identity persists across terminal or Codex restarts. A graph change admits the node; the app supervisor provisions its execution. A pane displays that execution, and opening or closing the pane preserves graph topology. This replaces the earlier proposed tmux backend; implementation is pending.

- [ ] **2. Workspace package**

   Uses core's existing package format: collections, files, symlinks, and changes against a base. Core resolves a complete directory view, prepares a private copy-on-write checkout, and captures edits as a changes package. The app defines the semantic layout of its contents.

- [ ] **3. Message package definition**

   Defines message content and its delivery behavior. Once Ontography accepts the delivery, the receiving harness inserts the text directly into Codex's conversation input, as though pasted to the agent. Receiving the message requires no separate package lookup. The contract validates the payload; the harness performs and records delivery.

- [ ] **4. Union package definition**

   An app-level package type permitting `Message | WorkspaceEnvelope`. It uses one core `Contract` with one associated object type; the validator accepts either payload form. Each delivery carries one package. A handoff containing both a message and a workspace can use one composed content package.

- [ ] **5. Edge definitions**

   Specify permitted endpoint types, authority requirements, and the accepted package contract. Concrete edges connect a source node to a target node. Connections are directed; replies require a corresponding reverse connection.

- [ ] **6. Node harness**

   Prepares context and workspaces, delivers messages, captures outputs, and submits operations through core. Agent requests are bound to the node and its active invocation. Core validates and commits workflow operations.

- [ ] **7. Node-scoped MCP interface**

   Implemented by the node's harness. Exposes the node's identity, outgoing connections, accepted package definitions, received inputs, and permitted context. Provides operations to read packages, construct messages or artifact packages, and request publication through an edge.

- [x] **8. Graph TUI and native management harness**

   The management layer and persistent session foundation are implemented. Final verification and rollout are tracked separately in [VERIFICATION.md](docs/VERIFICATION.md).

   | Piece | Responsibility |
   | --- | --- |
   | CLI + Pi harness | Select or create an Ontography session, resume its graph, and attach to its server-owned native Pi terminal. |
   | Ontography tools | Expose supported core APIs through session-scoped requests. The server owns accepted operations and retained core resources independently of clients. |
   | Graph TUI | `/graph` opens the owning session's run in the Rust Ratatui renderer; closing it returns to the same manager process. `--ui` opens this view first. |

   Native Pi owns its conversations and model execution. Ontography owns conversation membership/selection, app tool preferences, graph binding, process supervision, and terminal attachment. Core owns graph/workflow semantics, packages, contracts, authority, and durable graph history.

   The initial implementation's separate Rust/Pi RPC conversation UI remains in source for its earlier tests; it is no longer the CLI's `--ui` path. Current graph display reuses the existing Ratatui graph widget, which supports cycles, self loops, and parallel edges. The [renderer decision](src/ui/README.md#renderer-decision) records that evaluation.

   Core capability groups already bound to management tools:

   | Core capability | Operations |
   | --- | --- |
   | Definitions/admission | Validate schemas, graphs, contracts, authority rules, and rewrite grammars; save/import/export reusable declarations. |
   | Run lifecycle | Create/open, inspect, suspend, resume, and terminally close runs; manage registered executable implementations. |
   | Graph rewriting | Prepare, inspect, commit, and discard configured productions. |
   | Workflow | Submit activations/emissions and transfer packages through permitted edges. |
   | Content/packages/workspaces | Import/read/retain, compose/resolve, checkout/capture, diff/merge, transfer through iroh. |
   | Invocation/context | Issue scoped invocations, prepare grants, enforce budgets, and record observed exposure/delivery evidence. |
   | Inspection | Inspect topology, frontier, package history, executions, and revision notifications. |

   Concrete worker definitions remain items 1–7. The production executable registry has no Codex worker. A logical graph node currently does not create an agent process or terminal.

## Session foundation

```text
Rust server
└── Ontography session
    ├── Pi manager state: conversations, active selection, tool preferences
    ├── Server-owned Pi process, PTY, and terminal state
    └── One graph run/runtime after initialization
```

A new session can start Pi before the graph exists. The first scoped start operation persists a reserved run identity and resolved definition, constructs core storage, and binds that run. Retries recover the same initialization. Existing runs are adopted explicitly; no Pi ownership is inferred from cwd or old transcripts.

The last selected app session is the default attachment target. Pi `/new`, `/resume`, `/fork`, `/clone`, and `/tree` preserve its graph. One controlling client owns the manager terminal's input/dimensions. Ctrl-B, then D detaches while manager and graph continue. Pi exit ends the manager process but retains session/graph state. Session suspension stops the manager and suspends graph resources; closure is terminal. Server restart loads records; attachment explicitly resumes the selected graph and native history.

`portable-pty` hosts processes; `vt100` maintains terminal state; the Rust client renders the terminal or the bound graph. The manager terminal is implemented. Worker terminal provisioning, views, package delivery, and execution reconciliation remain subsequent work.

The [session guide](docs/SESSIONS.md) documents commands and storage. [SESSION_DESIGN.md](docs/SESSION_DESIGN.md) records ownership and recovery details. [CORE_BINDINGS.md](docs/CORE_BINDINGS.md) maps the existing core API. [PLAN.md](PLAN.md) is the original item-8 implementation plan.

## Follow-up status

Checkboxes indicate implementation, not completion of final release gates.

- [x] **Per-user app-home support.** Default storage is `~/.ontography/`, with existing explicit/environment/XDG overrides. A journaled migration archives legacy home contents, moves the stopped previous store, and leaves an alias at its old path. It preserves existing run identities; storage migration does not replace a core build or grammar.
- [x] **Live home migration and final rollout verification.** Automated gates and native Pi/terminal checks passed. The legacy home was archived, the current store moved to `~/.ontography`, and all three prior runs preserved and resumed. [VERIFICATION.md](docs/VERIFICATION.md) records the evidence.
- [x] **Graph display inside the Pi interaction.** `/graph` and `--ui` use the same server-owned native Pi manager and the session's bound run; there is no second management conversation.
- [x] **Unified Ontography session.** Durable manager state, graph initialization/adoption, conversation ownership, selection, lifecycle commands, and scoped dispatch are implemented. App-session fork/archive/delete remain deferred.
- [x] **Rust manager terminal backend.** Server-owned PTY, terminal parsing, attachment snapshots, input/resize ownership, detach/reconnect, and manager supervision are implemented. This replaces the proposed tmux backend.
- [ ] **Concrete worker layer.** Implement items 1–7, then connect admitted graph nodes to supervised executions and node terminal views. Worker package delivery remains a governed core/harness operation.
- [ ] **Default app rewrite grammar.** Define versioned productions alongside the concrete node/edge/package definitions. Cover supported add/remove/connect/disconnect/rewire operations with core validation. Existing runtimes keep their original grammar; editing a saved declaration does not retrofit an empty-grammar run.

A definition is reusable configuration; starting it creates a run. Rewriting changes that run's current graph and preserves its history under the configured grammar. Core commits graph/package state; the future worker supervisor reconciles external processes separately and exposes partial failures. Opening or closing a terminal view has no graph-topology effect.
