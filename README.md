# Ontography

A Rust application for persistent agent sessions and graphs, built on [`ontography-core`](../ontography-core). Native Pi manages each session; the Rust server owns its terminal and graph runtime.

```sh
cargo build --locked
./target/debug/ontography               # attach to the last selected session
./target/debug/ontography session new work
./target/debug/ontography --ui          # open that session's graph first
```

Inside Pi, `/graph` opens the graph; `q` or Escape returns to the same Pi process. **Ctrl-B, then D detaches while Pi and the graph continue running.** Pi `/new` starts another conversation within the same Ontography session and retains its graph.

Keep the sibling core checkout available when building. Install Pi **0.85.1** separately; the binary embeds its Ontography extension. See [installation](docs/INSTALL.md) and the [session guide](docs/SESSIONS.md). Existing stores under `~/.local/share/ontography/` require explicit migration before the new default `~/.ontography/` is used.

## Ownership

```text
Rust server
└── Ontography session
    ├── Pi manager state: conversations, active selection, tool preferences
    ├── Native Pi process and server-owned PTY
    └── Optional graph run and core runtime
```

| Component | Responsibility |
| --- | --- |
| Rust CLI/client | Select a session; attach input/output; display Pi or its graph. |
| Pi | Native conversations, model execution, and management tools. |
| Rust server | Own sessions, manager processes, terminal screen state, graph runtimes, and accepted operations. |
| Core | Enforce graph admission, contracts, authority, workflow commits, rewrites, packages, and durable graph history. |
| Rust graph view | Render the bound run using Ratatui; closing the view preserves the manager. |

A new session can start Pi before its graph exists. Its first scoped `run.start` or `project.start` durably binds the resulting run. Each session owns separate Pi state and, once initialized, one graph run. Saved graph definitions remain separate from the current graph of a run.

Server stop suspends graph resources and stops managers. Attaching after restart restores the selected session's graph and saved Pi conversation. Transcripts do not restore process memory or in-flight agent work.

## Use without a model

```sh
./target/debug/ontography session new work --no-attach
./target/debug/ontography session list
./target/debug/ontography call system.hello --args '{}'
./target/debug/ontography --session SESSION_UUID call run.inspect --args '{}'
./target/debug/ontography call run.list --args '{}'
```

Scoped calls resolve the session's graph and reject conflicting targets. Unscoped calls remain the explicit administration interface. The handshake publishes operation schemas. [examples/flow.json](examples/flow.json) is a logical graph with a configured rewrite; logical nodes can exist before worker implementations are installed.

The management bindings expose definitions, runs, execution, rewrites, workflow, packages, workspaces, iroh transfers, scoped invocations/context, inspection, and trusted project applications. [The binding inventory](docs/CORE_BINDINGS.md) maps these to core's public API.

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

**Concrete worker nodes remain deferred:** Codex execution adapters, node terminal views, message/workspace/union contracts, edge definitions, node harnesses/MCP, and process reconciliation after rewrites. The app's default editing grammar must accompany those definitions. Existing runs retain their original grammar.
