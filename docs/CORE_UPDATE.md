# Core vocabulary and retirement integration

Core governs three distinct operations: extension adds vocabulary; rewriting
changes topology under configured productions; retirement removes pending work
with a recorded reason. The app exposes these through the management API and Pi.

## Add vocabulary to a run

```sh
ontography --session work call run.extend --args '{
  "extension": {
    "node_types": ["Reviewer"],
    "object_types": ["ReviewNote"],
    "authority_tags": ["review"],
    "contracts": [{
      "id": "review-note-v1",
      "object_type": "ReviewNote",
      "validator": "utf8",
      "validator_version": 1
    }]
  }
}'
```

Each field is optional; an empty extension rejects. Additions must be new names.
New contracts use the trusted `utf8` or `opaque_bytes` validators, version 1.
Existing validators retain their live identities. The result includes the new
revision, definition fingerprint, and vocabulary; revisions are decimal strings.

Extension preserves graph topology, existing contracts, roots, authority rules,
and rewrite grammar. It grants no additional authority to an existing node and
launches no executable. Actual node/edge changes still require a permitted
production. An empty-grammar run remains unable to rewrite its graph.

The original declaration stays immutable. Each run has an `extensions.json`
journal containing accepted additions and at most one pending intent:

1. Validate additions against the current kernel, retaining existing validators.
2. Atomically persist and sync the intent.
3. Let core commit the vocabulary extension.
4. Atomically record the accepted addition and clear the intent.

After interruption, the app tests the candidate and previous bindings through
core's public reopen API. The binding that opens identifies whether the commit
survived. An unresolved storage failure stays visible. Workers start only after
reconciliation. Logical and executable application runs both reconstruct their
extended vocabulary; application resume retains bindings and does not inject
fresh entry input. This journal does not add general mutation replay.

If core committed but final journal persistence failed, the API reports
`extension_persistence_pending` with `committed: true`. Inspect/recover that run;
do not automatically repeat the extension. Ordinary request receipts still have
server-instance lifetime. `run.inspect` shows current vocabulary and accepted or
pending extension records.

## Retire pending work

```sh
ontography --session work call workflow.retire --args '{
  "package_id": "PRODUCER_UUID/OUTPUT_UUID",
  "evidence_activation_id": "ACTIVATION_UUID"
}'
ontography --session work call inspect.package --args '{
  "package_id": "PRODUCER_UUID/OUTPUT_UUID"
}'
ontography --session work call inspect.retirements --args '{"limit": 100}'
```

Evidence is optional. When supplied, it must identify an accepted activation in
that run; core checks its existence, not the semantic justification for retiring
the package. Unknown, consumed, and already retired packages cannot be retired.

Each package is exactly one of `live`, `consumed`, or `retired`. Retirement
records contain `reason`, `holder`, `phase` (`received`/`outbound`), `revision`,
and optional `evidence_activation_id`. Reasons are `explicit`, `holder_removed`,
`no_accepting_edge`, and `route_removed`. Rewrites also create these records.

Retirement preserves package history and artifact bytes. Agent process shutdown
uses separate lifecycle controls. An agent holding now-retired input cannot
successfully publish a proposal consuming that input.

`inspect.package` includes disposition and retirement details. Retirement pages
use `after` and `next_after` package IDs. `inspect.export` includes retirement
records and extended vocabulary. These diagnostic APIs currently materialize a
full core snapshot internally; bounded responses do not imply bounded history
reads. Export is not a complete restorable backup.

## Rewrite effects

- Outbound work is reconsidered when its holder's outgoing edge identities
  change. This includes adding an edge: packages with no accepting resulting
  edge are retired. Work at unrelated holders is preserved.
- At an `All` receiver, removing an incoming route retires receipts tied to that
  route. Other receipts remain. `Any` receivers retain receipts when their
  holder survives.
- Removed holders retire their pending packages.

Always inspect the prepared rewrite's `retirements` before committing. Existing
rewrite results retain their `reason` spelling and additionally expose canonical
snake_case `reason_code`. Extension or retirement advances the core revision and
makes an earlier prepared rewrite stale.

## Compatibility

Core stores use schema version 9. This WIP update uses fresh runs; no migration
from version 8 is implemented. Build identity checks continue to reject older
runs rather than silently interpreting them as current data.

Fixed-graph fact export and verified historical replay reject runs containing
rewrites, transfers, retirements, or extensions. Ordinary reopen supports these
operations. Concrete Codex workers, node MCP, and the default app rewrite grammar
remain separate work.
