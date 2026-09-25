# Management layer verification

Verified on macOS on 2026-09-24, using Rust 1.97.1, Node.js 22.22.3, Pi 0.85.1,
and the sibling `ontography-core` checkout. The application uses public core APIs;
this implementation made no changes to the sibling core.

## Automated checks

```sh
cargo test --locked --offline
cargo clippy --locked --offline --all-targets -- -D warnings
cargo fmt --check
cd pi
npm run check
npm test
```

The release checks passed 59 Rust tests and 14 Pi tests, TypeScript checking,
formatting, and Clippy with warnings denied. Local socket and iroh tests require
permission to create listeners. Tests register deterministic workers and providers;
the production registry does not ship those fixtures.

| Plan phase | Evidence |
| --- | --- |
| 1. Server and Pi | [Process tests](../tests/server_lifecycle.rs) cover concurrent startup, detached ownership, reconnect, build mismatch, response loss, and partial-frame multiplexing. Real native Pi loaded the embedded extension. |
| 2. Definitions and durable runs | [Management tests](../tests/management_flow.rs) cover admission, suspension, reopening, terminal closure, and persisted topology. [Recovery tests](../tests/state_recovery.rs) isolate damaged manifests from healthy runs. |
| 3. Governed work | Workflow tests exercise delivery, consumption, and contract rejection. [Management tests](../tests/management_flow.rs) reject schema-valid authority above the node's ceiling without changing revision or frontier; the same payload succeeds with permitted authority. [Execution tests](../src/tools/execution.rs) prove registered core work continues after clients disconnect and waits leave stop controls usable. |
| 4. Rewrites | Management tests check preparation, stale rejection, commit, and persistence. The live model flow independently confirms the committed topology through a second client. |
| 5. Packages and workspaces | [Content](../src/tools/content.rs) and [workspace](../src/tools/workspace.rs) tests cover package closure, edits/deletions/symlinks, unchanged references, checkpoints, GC, reopening, and merge conflicts. [Suspension tests](../src/state_tests.rs) preserve edits and recovery access after capture failure. |
| 6. Context, network, and applications | [Context tests](../src/tools/context.rs) cover grants, invocation/run scope, budgets, exact publication retry, and observed receipts. [Network tests](../tests/network_flow.rs) transfer a complete package closure between local endpoints and release resources for immediate resume. [Application lifecycle tests](../src/tools/project_tests.rs) verify native/provider start, suspension, and reconstruction in a fresh service. |
| 7. Management experience | [Pi tests](../pi/test/) cover schemas, wide identifiers, cancellation, reconnection, no mutation replay, build identity, and bounded previews. Installed Pi performed actual management tool calls; copied-binary launch verified embedded extension assets. |
| 8. Rust TUI | [UI tests](../src/ui/) cover graph identity, loops/parallel edges, resizing, selection, stale observations/previews, Pi streaming, and child cleanup. [Node-page tests](../src/ui/frontier.rs) use 400 real outbound packages to inspect a node absent from the global first 100, reach subsequent pages and history, and reject mixed revisions and obsolete responses. Actual terminal checks cover rendering, model tools, detach, session restoration, and terminal restoration. |

## Live management and terminal checks

Both native Pi and `ontography --ui` launched with the installed Pi configuration.
A live model read the example declaration, called graph validation and run start,
prepared and committed `remove_receiver`, and inspected the result. An independent
CLI inspection after UI detach confirmed node A, zero edges, revision 1, and open
admission. This checks the generated nested tool schemas against the real harness.

Client exit preserved the same server process and live run. Reattachment restored
the graph; `/session` restored the earlier Pi transcript. The Rust UI rendered at
80×24 and repainted at 120×35. Node selection updated the displayed details.
Ctrl-Q restored the original terminal attributes, cursor, alternate screen, and
bracketed-paste mode. Only explicit test cleanup stopped the isolated servers.
See [UI integration evidence](../src/ui/README.md).

Crash tests separately killed a server, reconstructed committed state, required
explicit resumption, and rejected expired transient handles. Failed workspace
capture retained recovery resources. Closing a suspended run did not relaunch its
declared executable.

## Scope of these results

- Architecture items 1–7 remain subsequent work: concrete Codex/tmux nodes,
  app-specific package contracts and edge definitions, receiving harnesses, and
  node-scoped MCP. The management layer exposes existing core package/workspace
  functionality and registered execution/application integrations now.
- Fact restore is checked transient inspection. Core has no public durable fact
  import or verified replay for rewrite/transfer histories. Ordinary reopening of
  those histories uses core's current-state loader.
- Workspace checks ran on the available macOS filesystem. Checkout requires
  supported copy-on-write behavior; these results do not establish support for
  other filesystems or execution confinement.
- The core's ignored failed-SQLite-commit orphan-retention probe remains an upstream
  limitation. App tests establish only the retention/GC paths they exercise.

The [binding inventory](CORE_BINDINGS.md) records precise operation coverage,
ownership, and additional public-core limitations.
