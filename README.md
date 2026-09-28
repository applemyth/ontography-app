# Ontography

A Rust application for persistent agent sessions and graphs, built on [`ontography-core`](../ontography-core). Native Pi manages each session; the Rust server owns its persistent shell terminal and graph runtime.

```sh
cargo build --locked
./target/debug/ontography               # always create a new session
./target/debug/ontography new work      # create a named session
./target/debug/ontography ls            # list sessions, terminals, and graphs
./target/debug/ontography attach work   # explicitly reattach
./target/debug/ontography attach work --ui  # open its graph first
./target/debug/ontography close work    # stop its manager and close its graph
```

Inside Pi, `/graph` opens the graph; select an agent node and press Enter to enter its terminal. **Ctrl-B, then D or G returns from a node to the graph**, and `q` or Escape returns from the graph to Pi. From Pi or the shell, **Ctrl-B, then D detaches while Pi and the graph continue running.** Pi `/quit` returns to the managed shell with a session status panel. Type `pi` there to resume the latest active conversation. Shell `exit` suspends the session and graph; `attach` resumes it. Detach and reattach preserve an existing shell prompt without restarting Pi. Pi `/new` starts another conversation within the same Ontography session and retains its graph.

Keep the sibling core checkout available when building. Install Pi **0.85.1** separately; the binary embeds its Ontography extension. See [installation](docs/INSTALL.md) and the [session guide](docs/SESSIONS.md). Existing stores under `~/.local/share/ontography/` require explicit migration before the new default `~/.ontography/` is used.

## Ownership

```text
Rust server
└── Ontography session
    ├── Pi manager state: conversations, active selection, tool preferences
    ├── Server-owned shell and PTY → native Pi when launched
    └── Optional graph run and core runtime
        └── Agent nodes → persistent Codex app-servers + attached terminal clients
```

| Component | Responsibility |
| --- | --- |
| Rust CLI/client | Select a session; attach input/output; display Pi, shell + session panel, or graph. |
| Pi | Native conversations, model execution, and management tools. |
| Rust server | Own sessions, manager and worker processes, terminal screen state, graph runtimes, and accepted operations. |
| Core | Enforce graph admission, contracts, authority, workflow commits, rewrites, packages, and durable graph history. |
| Rust graph view | Render the bound run using Ratatui; closing the view preserves the manager. |

A new session can start Pi before its graph exists. Its first scoped `run.start` or `project.start` durably binds the resulting run. Each session owns separate Pi state and, once initialized, one graph run. Saved graph definitions remain separate from the current graph of a run.

Server stop suspends graph resources and stops managers. Explicit attachment after restart restores that session's graph and saved Pi conversation. Transcripts do not restore process memory or in-flight agent work.

## Use without a model

```sh
./target/debug/ontography new work --no-attach
./target/debug/ontography ls --json
./target/debug/ontography call system.hello --args '{}'
./target/debug/ontography --session SESSION_UUID call run.inspect --args '{}'
./target/debug/ontography call run.list --args '{}'
```

CLI targets accept an exact session ID or a unique exact name. Ambiguous names require an ID. The `session …` command forms remain supported; `session list` retains its raw JSON output.

Scoped calls resolve the session's graph and reject conflicting targets. Unscoped calls remain the explicit administration interface. The handshake publishes operation schemas. [examples/flow.json](examples/flow.json) is a logical graph with a configured rewrite; logical nodes can exist before worker implementations are installed.

Pi manages workflow documents, runs, edits, task decisions, and artifacts through the workflow tools. Shared node tooling supplies scoped graph and package operations for worker implementations. [The workflow guide](docs/WORKFLOWS.md) describes current behavior; [the binding inventory](docs/CORE_BINDINGS.md) maps it to core's public API.

## Development and status

```sh
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --check
cd pi
npm ci --ignore-scripts
npm run check
npm test
```

The persistent session foundation is implemented. Final verification and the actual home-directory cutover are recorded separately in [VERIFICATION.md](docs/VERIFICATION.md). [ARCHITECTURE.md](ARCHITECTURE.md) tracks remaining pieces; [SESSION_DESIGN.md](docs/SESSION_DESIGN.md) explains ownership and recovery.

**Persistent Codex node foundations are implemented:** an agent binding owns a persistent Codex app-server, an attached native terminal client in a managed PTY, a continuing conversation, a private working directory, and scoped node tools. Workflow startup, edits, suspension, and resume reconcile its process. Command, human, and inbox nodes retain their task behavior; message/workspace contracts and the fixed editing grammar already exist.

The [node MCP adapter](docs/NODE_MCP.md) exposes shared graph tools to managed Codex sessions, with an optional per-node `tools` allowlist and existing grants enforced. Initial and incoming work is queued into the Codex conversation automatically; agents publish graph replies through tools. Enter in the graph attaches to an agent's existing terminal. See [WORKFLOWS.md](docs/WORKFLOWS.md) for configuration.
