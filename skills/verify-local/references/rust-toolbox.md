# Verification design and agent interface

## Scope

`verify-local` is a Rust control plane for bounded, explicit state-machine exploration. Any language can participate by reading requests and writing responses on standard input/output. The bundled Python and JavaScript adapters demonstrate that boundary; the Rust application example lives in the separate [S3 append proxy repository](https://github.com/s1liconcow/s3-append-proxy).

Each execution starts a new process, resets the target to its declared initial state, chooses legal actions, collects property checks, and replays the complete action tape in another fresh process. Exact replay compares every canonical JSON response, including observations, enabled action objects, checks, witnesses, and extension fields. A matching final error alone is insufficient.

The default search uses SplitMix64 choices weighted by the inverse frequency of adapter state/action pairs. Guided mode also queues unvisited alternatives offered by the adapter. The state digest should distinguish future behavior that matters to the oracle. Coarser digests can merge distinct histories.

## Reuse exploration and revisit a decision

```bash
verify-local run verify.json --guided --out .verify/first --runs 20 --steps 80
verify-local run verify.json --corpus-from .verify/first/frontier.json \
  --out .verify/next --runs 20 --steps 80 --seed 21 --max-findings 4
verify-local report .verify/first/sample-trace.json --events --from 2 --limit 1
```

A guided campaign alternates fresh reset seeds with queued branches while eligible branches remain. Importing a nonempty frontier gives a branch the first turn. The adapter receives the original workload seed for a branch. The campaign records the separate seed used to choose its continuation. The full prefix counts toward the step bound.

Only matched traces contribute new branches. The queue retains up to 128 untried state/action-ID pairs at events 0 through 63, within 2 MiB of serialized entries. A separately bounded 2 MiB set remembers previously exercised pairs across imports. The report exposes queue and visited-set capacity flags. Once a branch is selected, the controller reconstructs its original actions, verifies the digest of all canonical responses through the branch point, and sends the full saved action. Changed identities or reconstructed prefixes make the attempt inconclusive. Importing a corpus contributes search hints only. Every property and witness needed by the new campaign must be observed again.

For a controlled experiment at a specific event, use `verify-local fork MANIFEST TRACE --at N --action ID --steps K --out FILE`. Event zero is reset. The command matches the prefix, takes the action that the adapter offered at event N, and chooses up to K further actions with the original seed and fresh frequency counts. The default K is zero. It replays the entire resulting trace in a second fresh process and records the parent hash and branch point. A fork can reproduce a violation, complete cleanly within its scenario obligations, or be inconclusive if it omits required checks, witnesses, or a required terminal state. Inspect the observations and the declared boundary when interpreting a changed outcome.

Each run stops at its first failing response. By default the campaign stops at its first replayed failure. With `--max-findings N` (1–64), the campaign retains one trace per distinct primary failing property ID and signature and counts repeat sightings. The run and wall-clock bounds still apply. A later execution error can leave the overall campaign incomplete with confirmed findings in `campaign.json`.

## Small control plane, full evidence files

Commands return one compact JSON object. Normal output contains the verdict, counts, target failure, and artifact paths. The implementation caps stdout summaries at 12 KiB; lists are paged or truncated with explicit omitted counts. Detailed evidence remains local:

| Artifact | Contents |
| --- | --- |
| `campaign.json` | Bounds, identity, property counts, witnesses, each run's seed/count/stop reason, evidence references |
| `sample-trace.json` | First completed clean trace |
| `failure-NNNN.json` | First exactly replayed counterexample for a distinct finding |
| `frontier.json` | Guided campaign’s pending decisions, visited pairs, and execution identity digest |
| `unconfirmed-failure.json` | Observed failure whose replay could not finish |
| `divergence.json` | Original and diverging execution plus reason |
| User-selected minimized file | Reduced valid action tape, full responses, reduction bounds and provenance |

Every completed trace is replayed. Only a sampled clean trace and failure evidence are retained in full. Artifacts use compact canonical JSON, atomic exclusive creation, and a 32 MiB file limit. Output directories must be new. Exhausted evidence budgets are visible outcomes.

Use `verify-local report PATH` before opening raw JSON. Use `report TRACE --events --from N --limit K` for the relevant event page; K is 1-32. For unusually large pages, reduce K. The command does not spill the entire trace into agent context.

## Commands

```bash
verify-local doctor verify.json
verify-local run verify.json --out .verify/run-001 --runs 100 --steps 80 --seconds 30 --seed 1
verify-local replay verify.json .verify/run-001/failure-0001.json
verify-local shrink verify.json .verify/run-001/failure-0001.json \
  --out .verify/minimized.json --attempts 200 --seconds 30
verify-local regression verify-fixed.json .verify/minimized.json --out .verify/regression.json
verify-local report .verify/run-001/campaign.json
verify-local report .verify/minimized.json --events --from 0 --limit 8
```

`doctor` checks configuration and file identity without executing the adapter. It cannot establish determinism or production correspondence. Commands accept `--json` for explicit callers; JSON is also the default.

| Outcome | Exit | Meaning |
| --- | --- | --- |
| `READY` | 0 | Configuration-only validation succeeded |
| `NO_FAILURE_WITHIN_BOUNDS` | 0 | Requested execution completed with required evidence and matched replay |
| `FOUND` | 1 | A property violation was reproduced |
| `INCONCLUSIVE` | 2 | Missing evidence, timeout, disabled tape, identity change, or divergence prevents the intended conclusion |
| `HARNESS_ERROR` | 3 | Invalid configuration, malformed protocol, or adapter failure |

`report` is informational and exits zero when it successfully reads evidence, even when the reported artifact contains a failure. Check its structured outcome. `replay` and `regression` summarize only the retained scenario. A clean replay alone does not satisfy a campaign's full coverage obligations.

## Manifest

Keep the manifest in a directory containing all its declared files. Paths in identity lists must be relative, resolve within that directory, and name regular files. See `examples/python/verify.json` and `examples/javascript/verify.json`.

- `version`: 1 for the manifest and JSONL protocol.
- `adapter`: executable and argv array; no shell expansion. Executable resolution is recorded.
- `identity_files`: production logic, adapter, oracle, fixtures, dependency locks, and relevant configuration. Include every behavior-affecting input; omitted dependencies cannot be discovered automatically.
- `contract_files`: unchanged oracle/contract files required for cross-version regression. They must also appear in `identity_files`.
- `required_properties`: property IDs that must be evaluated at least once across a clean campaign.
- `required_witnesses`: reachable scenario IDs needed to support the campaign conclusion.
- `require_terminal`: optional boolean, default `false`. When true, each clean run must reach `terminal: true` within its action budget. A cut-off run is inconclusive even if another run satisfied every required property and witness. This rule also applies to replay and regression and is pinned in the property contract.
- `boundary`: explicit `controlled` and `uncontrolled` lists.

The identity includes source hashes, exact manifest bytes, adapter binary, runner binary, argv, selected environment, OS/architecture/kernel, and the pinned contract. Exact replay rejects identity changes. Cross-version regression permits changed application code while requiring the same declared contract, witnesses, boundary, and per-event evaluation opportunities; it replays the candidate twice. The runner cannot establish the semantic independence of an oracle or prevent a dishonest adapter from lying.

The process environment is cleared and rebuilt from PATH plus deterministic locale, timezone, and Python settings. Credentials and unrelated environment values are not passed through. Dynamic libraries, system executables invoked by an adapter, and files outside declared identity remain the adapter author's responsibility.

## JSONL v1 protocol

Each request occupies one line. Responses must have the same protocol version and request ID. Reset starts at request ID 0; subsequent steps use 1, 2, and so on.

```json
{"protocol":1,"request_id":0,"op":"reset","seed":1}
{"protocol":1,"request_id":1,"op":"step","action":{"id":"deliver:A","kind":"workload","operation":"A"}}
```

An adapter response:

```json
{"protocol":1,"request_id":1,"state":"stable-state-digest","enabled":[{"id":"checkpoint","kind":"schedule"},{"id":"crash","kind":"fault"}],"checks":[],"witnesses":[],"terminal":false,"observation":{"value":0}}
```

Action kinds are `workload`, `fault`, `schedule`, and `time`. The controller sends the complete enabled action object unchanged. Action IDs are unique within a response. Response extension fields participate in replay. Duplicate JSON keys, duplicate property/action/witness IDs, invalid request IDs, and terminal states with enabled actions are rejected.

Check eligibility is explicit: omit a check while its predicate is inapplicable. Emit `{id, ok, signature?, details?}` when it is meaningful. Keep stable IDs and signatures. Prefer compact observations and failure-only details. A missing required property or witness makes a clean campaign inconclusive. A nonterminal state with no enabled actions is also inconclusive, including at the last permitted step. Temporal recovery deadlines belong in the adapter's virtual-time model.

The default request timeout is 2 seconds, response-line cap 256 KiB, and combined per-process stdout/stderr budget 4 MiB. JSON depth and collection limits prevent accepted responses from exceeding evidence-envelope parser constraints. Options can tighten these limits. Artifact and global novelty/evidence cardinality limits are fixed in this version.

## Trusted process boundary

The subprocess backend runs on POSIX systems and uses process groups for cleanup. It bounds protocol I/O, campaign time, and individual retained traces. It does not enforce a filesystem sandbox, network denial, aggregate RSS limit, or containment of a deliberately escaping process. Run trusted synthetic adapters here; select an appropriate disposable OS sandbox before executing hostile code. Native Windows is currently unsupported. Linux is the exercised CI target; macOS needs platform qualification.

## Reduction and repairs

Shrinking performs bounded action deletion. Every accepted candidate must retain enabled-action validity, the same property/signature, and exact repeatability. Invalid schedules and unrelated failures are rejected. A fully completed reducer establishes one-action-deletion minimality under this action representation. Budget interruption reports weaker minimality. Keep the original artifact and preserve both builds if future exact replay matters.

A repaired implementation should pass the retained scenario under an unchanged independent contract, then complete a fresh bounded campaign. Changed action availability or removed checks produces an explicit inconclusive regression. Requirements changes deserve a separate review from an application repair.

## Exercise an external service

The toolbox's `examples/sqlite-http` adapter owns a private database per execution, launches a separate HTTP server on a dynamically allocated loopback port, and drives the public client API. Control pipes expose checkpoints immediately before transaction commit and between commit and reply. A fault action kills the owned child, waits for the observed client disconnect, and permits restart against the same SQLite WAL. The independent oracle checks recovered state on restart and audits the acknowledged credit over HTTP. The runtime version and logical observations enter replay evidence. Port numbers and process IDs are allocation details. The example qualifies process crash recovery on the host filesystem. Kernel crashes, power cuts, packet-level scheduling, and hardware durability are outside its boundary.

For another service, give each run fresh persistent state, synchronize fault actions with observed readiness or I/O checkpoints, include the service and oracle in the manifest identity, and keep child processes in the adapter process group for cleanup. Verify both a correct build and a defect that reaches the intended fault window.

## Worked application

The direct-S3 service campaign in [s3-append-proxy](https://github.com/s1liconcow/s3-append-proxy) sends POST, GET, and HEAD through the production router and consumes their whole response streams. It uses the real S3 client to generate signed conditional requests. An adapter-owned HTTP connector controls request transfer, atomic S3 publication, and response delivery. The signed HTTP boundary excludes real DNS, TCP, TLS, and S3 durability. Run the app's `scripts/service-demo.py` after building and installing its pinned controller.

`verify-async::scheduler::Scheduler` exposes stable IDs for runnable top-level futures, single polls, explicit cancellation, and paused Tokio time. Keep I/O completions and wakeups inside the adapter. Tokio spawning is rejected by the scheduler. Register controllable background futures as separate scheduled tasks.

`verify-async::history::linearize` takes complete operation intervals, an initial model state, a state budget, and your independent transition function. Return all allowed successors for the observed outcome, including both unchanged and committed states for ambiguous effects when the evidence permits either. The search preserves response-before-invocation order, memoizes explored combinations, and returns a serialization, a contradiction, or budget exhaustion. Use a complete, bounded history and model the possible end of pending effects. Record raw evidence and final reads so later operations can rule out invalid interpretations of unknown outcomes.
