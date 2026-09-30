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
| The manager | Retries or discards parked tasks, pages inboxes | `flow.retry`, `flow.discard`, `flow.output` |
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
  it chose, and its node tools refused none of its legal calls. Tasks that can never succeed
  never published. Every player move and decision that was done is in
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

## Not yet played

Graph edits, sessions and terminals, workspaces, the agent grants
(`originate`, `send_later`, `retire`), systematic crash points, and scripted
models for the real Pi, Codex and Claude.
