# Ontography trials

Trials of the whole app, run from outside. Each trial generates a workflow
document with every node kind the app runs, starts it on a real
`ontography server run` in its own data directory, and drives it the way
people and programs do. Then it drains the run, restarts the server in order,
and judges what happened. Judges are written from the documentation
(docs/WORKFLOWS.md, docs/SESSIONS.md), never from the app's code.

```sh
cargo run -p ontography-trials --release -- --preset quick   # about two minutes, seeds 1–8
cargo run -p ontography-trials --release -- --preset medium  # about fifteen minutes, new seeds
cargo run -p ontography-trials --release -- --preset soak    # about an hour
cargo run -p ontography-trials --release -- --seed 42        # replay one seed
```

The harness builds the server (`target/debug/ontography`, the debug build
people run) from the same tree before it starts, so the two always match;
`--ontography PATH` tests another binary. Trials run in parallel, each with
its own server (`--jobs N`). A trial with a problem keeps its directory under
`trials/runs/<seed>`: the document, the server's data and log, the last
export, and the programs' witness files. Interrupting the harness stops every
server and program it started; servers a killed harness left behind are
stopped at the next start.

## A trial

| Actor | What it does | Surfaces |
| --- | --- | --- |
| Players | Start work at external sources (`workflow.submit` roots), consume or retire what reaches external sinks | core moves, `inspect.frontier` |
| Commands | This binary in node mode, placed by the document's `argv`, in a role: `digest`, `flaky` (fails once per input), `broken`, `binary` (non-UTF-8 output), `slow` (outlives its timeout), `big` (more output than the app keeps) | the task harness, joins, broadcast, retries and parking |
| Agents | This binary as an agent node's `argv` program, pulling work through the node tools over `ontography node-mcp`: it broadcasts, routes to connections its input picks, or fails its first attempt at each input | node tools, attempts, receipts, the MCP proxy, agent processes |
| A person | Decides every task at human nodes | `flow.decide`, `flow.status` |
| The manager | Retries or discards parked tasks, pages inboxes, and edits the running graph: changes a command's role, adds an inbox, adds or removes a connection, removes a sink with its work, or switches a node's join, which replaces it | `flow.retry`, `flow.discard`, `flow.output`, `flow.edit`, `flow.commit`, `flow.resume` |
| Chaos | Crashes the server (`kill -9`, restart, `run.resume`) and kills running commands, at moments fixed by the seed | crash recovery |

Every command records each run of its program, and every agent each attempt
and submission, in a witness file: the only outside record of how often the
app ran a task and what a program decided.

## The judges

- **Workflow** (`judge.rs`): every package's payload is rebuilt from its
  producer and checked against its content digest. Every command activation
  consumed one of its node's tasks and published exactly what its program
  prints for those inputs, in package order, joined by blank lines; its
  result went to each outgoing connection. Every agent activation published
  what its program submitted, to every successor or exactly the connections
  it chose, and its node tools refused none of its legal calls. Tasks that
  can never succeed never published. Work done while edits applied is judged
  against a version of the graph that explains it; an edit retires exactly
  the pending work its preview listed, and the run's final graph is its last
  document's. An edit whose previews keep going stale is reported as
  starved. Every player move and decision that was done is in
  history once, every refused one never, every lost one at most once. After
  the run settles, work waits only in inboxes, at sinks, in incomplete joins,
  or in parked tasks, parked after exactly their node's `max_attempts`; the
  witness files agree with how often each input ran.
- **Restart**: an orderly restart leaves history exactly as it was.
- **Store** (`audit.rs`): once the server has stopped, the run's core store is
  reopened through core's own API and compared with the last export; a
  history of activations alone is also replayed and verified from scratch.
- **Processes and leftovers** (`procs.rs`): every process descended from any
  of the trial's servers is tracked by pid and start time; none may outlive
  the orderly stop. The server's socket directory, its nodes' socket
  directories, and half-written files must not be left behind.

Crashes widen what the judges accept only where the documentation says a
crash may: a lost reply may or may not have committed, and an interrupted
program may run again.

## The sessions trial

```sh
cargo run -p ontography-trials -- sessions --seed 1                   # about ten seconds
cargo run -p ontography-trials -- sessions --sessions 5 --rounds 3 --crashes 2
```

Several app sessions share one server, each following a seeded script of
what a person does with a session's terminal, all at once: attach (resume,
`terminal.ensure`, then the terminal socket), type lines, resize, browse
history, detach three ways (`detach`, closing the connection, `ontography
detach` from another client), try a second controller, `/graph`, `/quit` and
`pi` at the shell, `exit` at the shell with or without a client, suspend,
resume and close. This binary is their Pi (`pi` mode, which the server runs
through a small `pi` script): it registers its conversation as Pi's
extension does, echoes each line as `ECHO <line>`, reports its size, starts
children in their own process groups (`/child`, and `/child-hup`, which
ignores SIGHUP), and records all it sees in a witness file per session. One
session binds a graph of external nodes with a scoped `flow.start` and plays
moves at it. Chaos kills the server while shells and Pi are live, once the
scripts are far enough along; the sessions attach again, which resumes them.
Then the server stops in order with sessions still live, restarts, and stops
again.

The judges, from docs/SESSIONS.md and docs/SESSION_DESIGN.md:

- **States**: `session.list` and `terminal.status` agree with each script's
  model after every step (state, run, conversation, whether the terminal
  runs, is attached, and shows Pi or the shell), after the play, and after a
  restart, which starts no terminal.
- **Terminals**: detaching keeps the shell and Pi; reattaching shows the
  screen as it is; a second controller is refused; `/quit` keeps the shell,
  and `pi` there, or a new terminal, resumes the session's saved
  conversation with the environment of the command that activated the
  session; history stays frozen while Pi runs on; a closed session never
  resumes.
- **Lines**: every line typed shows on screen once, in order, and reached
  Pi once, in order (the witness); a resize, or a reattach from a terminal of
  another size, reaches Pi, which reports it on SIGWINCH and on `/size`.
- **Processes**: after a suspension, a close or an orderly stop, nothing of
  the session runs: shell, launcher, Pi, or Pi's children. Processes are told
  apart by session ID, so whatever a crash struck, however late it started,
  is judged with the crash: none may run once its session has resumed or the
  server has stopped. Each crash's processes are kept in `crash-N.txt`.
- **Files**: terminal sockets (`pty-*.sock`, `pi-*.sock`), shell rc files and
  half-written files are gone after each stop, and the server's socket
  directory too.
- **Graph**: moves the server accepted are in history once after crashes and
  restarts, and the run is active, suspended or closed with its session.

A trial with problems keeps `trials/runs/sessions-<seed>`, with a log of
each session's steps (`s0.log`, …).

## Not yet played

Agent node terminals, Pi's `/new` and `/resume`, workspaces, the agent
grants (`originate`, `send_later`, `retire`), systematic crash points, and
scripted models for the real Pi, Codex and Claude.
