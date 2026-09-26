# Verification

## Core extension and retirement integration — 2026-09-25

Passed: **104 app Rust tests, 51 core Rust tests, and 31 Pi tests**, TypeScript
checking, Clippy across all targets with warnings denied in both Rust crates,
formatting, and the locked offline build. Core retains one previously ignored
failed-SQLite-commit probe; it was not counted as passing.

- [Extension tests](../tests/extensions.rs) cover monotone admission, retained
  authority and topology, stale rewrite plans, persistent recovery, both sides
  of an interrupted journal commit, executable resume without replaying entry
  input, and rejection by fixed-graph fact APIs. An internal boundary test
  verifies both journal forms fit the metadata reader limit before core mutates.
- [Request tests](../tests/extension_scope.rs) exercise actual server dispatch,
  nested schema validation, session ownership, lifecycle gates, and receipt
  deduplication.
- [Retirement tests](../tests/retirement.rs) cover explicit retirement, all four
  reasons, local rewrite cleanup, `All` receiver route removal, invalid evidence,
  consumed/retired rejection, pagination, exports, reopening, and session scope.
- Pi tests verify capability discovery, generated schemas, default run-group
  availability, retirement group activation, scoped dispatch, and exact decimal
  revisions. Independent review also checked crash recovery and core application
  reconstruction.

A real compiled-CLI check used an isolated server: create a session/run, extend
all four vocabulary categories, submit and retire a package, inspect its evidence,
stop/restart the server, explicitly resume, and compare the recovered vocabulary,
unchanged graph, journal, package disposition, and retirement page. It passed;
the isolated server was stopped. Evidence is retained temporarily under
`/private/tmp/ontography-core-update-opz9g2tm/`.

Core's public API gained schema iterators and an application reconstruction
helper that retains executables and grammar. Its new integration test verifies
live extension followed by application resume without a second initial input.
The original core additions were committed separately before this integration.
See [CORE_UPDATE.md](CORE_UPDATE.md) for semantics, limits, and examples. Core
schema 9 requires fresh runs; this update implements no run migration.

The default server now runs the verified update at `~/.ontography`, with zero
runs, zero sessions, and no recovery errors. Application fingerprint:
`ff2cc187f5ac9b34697e2b024091d53ddb4ffea5a43d79b9aaaeb6ea58e60412`;
core fingerprint:
`0b5f4f316927115ac62d75b85bafa76438130d060a4a39c84db2e2307ba659e3`.
The previous manager was stopped and no executable worker bindings were active.
The old server was gracefully stopped, then its store was renamed intact to
`/Users/hershybar/.ontography.archive-core8-20260926T055318Z-e450eb46`.
The archive includes `previous-ontography`, the matching old binary. Directory
identity and hashes of all 12 retained JSON metadata files were checked after
rename. The earlier legacy archive was untouched. An initial replacement attempt
safely rolled back because the legacy alias required an initialized store; the
successful retry initialized the new store explicitly, then checked default-path
commands. `cutover.json` in the evidence directory records the result.

## Explicit attachment and new-session CLI — 2026-09-25

Bare `ontography` now always creates a session. Top-level `new`, `ls`, `attach`,
`close`, and the other session controls accept exact IDs or unique exact names.
`ls` separates session, terminal, and graph state; the existing nested command
forms and raw `session.list` API remain available.

Passed: **92 Rust tests**, Clippy across all targets with warnings denied,
formatting, `git diff --check`, and the locked build. The two new
[CLI integration tests](../tests/session_cli.rs) exercise real subprocesses,
server sockets, and PTYs: independent bare launches, explicit attachment,
manager reuse/replacement, graph survival after manager exit, scoped closure,
name ambiguity, ID precedence, listing, and noninteractive creation preflight.
The existing concurrent frontier test now accepts `run_suspended` when suspension
wins before observation begins; it still requires status/receipt replies before
the waiting request and prompt completion after suspension.

Native Pi **0.85.1** independently passed the following checks in temporary
storage, using the compiled application fingerprint
`f0aecd11551541253c899177c2a38cb30874948323ceee5475a45ba8c3170218`:

- Bare launch, detach, and another bare launch create two independent sessions
  and managers; the first graph remains active.
- Named `attach --ui` displays the original graph and reuses its Pi terminal.
- Native `/quit` returns to the shell, restores terminal attributes, and keeps
  the session, graph, and active conversation identity.
- Explicit attachment creates a replacement Pi process and renders saved
  history. The history was seeded with Pi's own session writer; no model call
  was needed. Another `/quit` and attachment retain that history.
- `close NAME` closes only that session and graph, leaving the other manager
  detached and running. `ls --json` reports both states correctly.

Evidence and terminal captures are retained temporarily under
`/private/tmp/ontography-cli-native-07n_yc29/`; its isolated server was stopped.
Pi extension code and sibling core were unchanged in this CLI update.

The idle default server was gracefully replaced with this build. All four run
summaries and the valid closed session record were preserved exactly: three
graphs remain suspended and one remains closed. Restart discovered two older
directories lacking `session.json` (`d5607719-44b0-42c9-a7df-7561b94bacdd` and
`f9957648-e9fc-4717-a4a8-c5ad44001bd2`). They contain legacy runtime artifacts,
were left untouched, and appear in session recovery errors. Upgrade evidence is
retained at `/private/tmp/ontography-cli-update-54lfbnk7/upgrade.json`.

## Persistent sessions — 2026-09-25

The persistent manager-session milestone is implemented and verified on macOS,
with native Pi 0.85.1 and the unchanged sibling core. The checked application
fingerprint is `64fdb11944f63fd5ec52acd36c89cefa983d33199189fae12172a71f301b6d34`;
the core fingerprint is
`819006a6fa15f139ccaee99ea2c17b7f1f51fc5f2270eafeeb95758c06904cda`.
All seven embedded Pi assets participate in the application fingerprint.

### Automated gates

Passed: **90 Rust tests, 30 Pi tests**, TypeScript checking, Clippy across all
targets with warnings denied, formatting, and `git diff --check`. The locked
offline build succeeds. Socket, PTY, and local iroh tests ran with permission to
create their required local resources.

| Requirement | Evidence |
| --- | --- |
| Durable session, optional graph, exclusive run ownership | Eight [session tests](../tests/session_ownership.rs): bind once, scoped dispatch, explicit adoption, saved conversations, close/selection recovery, and initialization retries before/after core creation. |
| Lifecycle controls remain usable | Session tests cover observation waits during suspension; [manager lifecycle](../tests/manager_lifecycle.rs) covers concurrent ensure, manager before graph initialization, suspension, explicit resume, and shutdown. |
| Detached native terminal | Fourteen tests across [terminal backend](../src/terminal.rs) and [client](../src/terminal_client.rs): detached draining, ordered input, exclusive controller, stale identities, terminal queries, process reaping, and history navigation isolated from live input/state. |
| Pi conversations retain graph identity | [Extension tests](../pi/test/session.test.ts) cover native lifecycle hooks, membership checks, preferences, failed registration, replacement, and saved-history materialization. Native checks below exercise the real harness. |
| Typed tools and connection recovery | [Bridge/tool tests](../pi/test/) verify session envelopes, reconnection, cancellation, no mutation replay, bounded output, schemas, and exact wide identifiers. |
| Fast slash commands | Installed-provider regressions reproduce stale-prefix and obsolete-candidate corruption; the public completion wrapper preserves current command text and arguments. Native checks exercise the correction. |
| Migration and recovered paths | Eight migration tests cover exclusive ownership, archival, interrupted rename/alias stages, ambiguous stores, and substituted directories. A session integration test relocates real native-history files and checks ownership through the old path alias. |

### Native acceptance

Two independent PTY harnesses exercised the same compiled application build.
Both isolated servers were explicitly stopped after verification.

- Native Pi made an actual `ontography_run_inspect({})` call using its bound run.
  Its first saved history became durably marked materialized. `/new`, `/resume`,
  `/clone`, and `/reload` preserved the app session and graph; `/graph` continued
  to work after those transitions. Burst slash input arrived and executed intact.
- `--ui` displayed the session's graph first, then returned to the same native Pi
  process. `/hotkeys` produced 93 retained history rows; Ctrl-B `[` and Page Up
  displayed older output, and `q`/Escape restored live input. Resizing from 80×24
  to 120×35 reached the server-owned terminal.
- Detach restored the original terminal attributes, alternate screen, and paste
  mode. Reattachment retained the same Pi PID and terminal identity. A second app
  session had a different manager and graph. Server restart retained the original
  session, conversation, and run IDs while creating a replacement terminal.

Native evidence was retained at
`/private/tmp/ontography-native-session-fncoalu3/` and
`/private/tmp/ontography-native-terminal-ublseek2/` during verification. Those
directories contain assertion records and terminal captures; they are temporary
verification artifacts. Native `/fork` was not separately exercised; its extension
hooks are covered by automated tests. Terminal presentation supports text and
color; inline images and hyperlinks are disabled.

### Completed local storage migration

The old server was gracefully stopped with its matching client. The verified new
binary then completed the authorized migration:

- Current store: `/Users/hershybar/.ontography`.
- Legacy archive:
  `/Users/hershybar/.ontography.archive-e0fa8fb7-5475-4a9e-bd4a-2c3a4777a00e`.
- `/Users/hershybar/.local/share/ontography` is an alias to the current store.
- Directory identities prove both trees were renamed intact. SHA-256 comparisons
  verified all 13 stopped run/definition files before and after relocation.
- All three previously active runs were resumed. Their run IDs, declarations,
  projects, checkpoints, admission, revisions, topology, and package frontiers
  matched pre-migration inspection. Both store paths connect to the same server;
  recovery errors are empty.

| Preserved run | Definition | Nodes / edges | Revision |
| --- | --- | --- | --- |
| `12dccebd-f344-4482-a0b8-4006f7e472ea` | fifteen-node-chain | 15 / 14 | 0 |
| `6c2d1977-4fd0-473a-b11d-448e92536b2a` | fifteen-node-chain-mixed | 15 / 21 | 0 |
| `a380573c-a8f0-4ff6-bdf0-8a1250ea99d7` | logical-flow | 2 / 1 | 0 |

Existing runs remain available for explicit session adoption; the migration does
not invent an earlier Pi association. Cutover evidence is retained under
`/private/tmp/ontography-cutover-6e7urwx1/`.

### Scope

This completes the manager-session foundation: native Pi ownership, multiple
conversations, session-bound graph tooling, persistent terminal attachment,
graph display, recovery, and storage migration. Concrete Codex worker nodes,
their message/workspace contracts, node MCP, default editing productions, and
worker process reconciliation remain the following implementation phase.

## Historical management release — 2026-09-24

Verified on macOS on 2026-09-24, using Rust 1.97.1, Node.js 22.22.3, Pi 0.85.1,
and the sibling `ontography-core` checkout. The application uses public core APIs;
this implementation made no changes to the sibling core.

### Automated checks

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

### Live management and terminal checks

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

### Scope of these results

- Architecture items 1–7 remain subsequent work: concrete Codex nodes,
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
