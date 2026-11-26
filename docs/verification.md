# Verification design and agent interface

## Scope

`verify-local` is a Rust control plane for bounded, explicit state-machine exploration. Any language can participate by reading requests and writing responses on standard input/output. The bundled Rust, Python, and JavaScript adapters demonstrate that boundary.

Each execution starts a new process, resets synthetic state, chooses legal actions, collects property checks, and replays the complete action tape in another fresh process. Exact replay compares every canonical JSON response, including observations, enabled action objects, checks, witnesses, and extension fields. A matching final error alone is insufficient.

Search uses deterministic SplitMix64 choices weighted by the inverse frequency of adapter state/action pairs. State hashes and witnesses are adapter-defined features. Code coverage, native scheduling, OS virtualization, persistent corpus management, distributed workers, and automatic fault interception are outside this implementation.

## Small control plane, full evidence files

Commands return one compact JSON object. Normal output contains the verdict, counts, target failure, and artifact paths. The implementation caps stdout summaries at 12 KiB; lists are paged or truncated with explicit omitted counts. Detailed evidence remains local:

| Artifact | Contents |
| --- | --- |
| `campaign.json` | Bounds, identity, property counts, witnesses, each run's seed/count/stop reason, evidence references |
| `sample-trace.json` | First completed clean trace |
| `failure-0001.json` | First exactly replayed counterexample |
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

Keep the manifest in a directory containing all its declared files. Paths in identity lists must be relative, resolve within that directory, and name regular files. See `verify.append.json` and `examples/python/verify.json`.

- `version`: 1 for the manifest and JSONL protocol.
- `adapter`: executable and argv array; no shell expansion. Executable resolution is recorded.
- `identity_files`: production logic, adapter, oracle, fixtures, dependency locks, and relevant configuration. Include every behavior-affecting input; omitted dependencies cannot be discovered automatically.
- `contract_files`: unchanged oracle/contract files required for cross-version regression. They must also appear in `identity_files`.
- `required_properties`: property IDs that must be evaluated at least once across a clean campaign.
- `required_witnesses`: reachable scenario IDs needed to support the campaign conclusion.
- `boundary`: explicit `controlled` and `uncontrolled` lists.

The identity includes source hashes, exact manifest bytes, adapter binary, runner binary, argv, selected environment, OS/architecture/kernel, and the pinned contract. Exact replay rejects identity changes. Cross-version regression permits changed application code while requiring the same declared contract, witnesses, boundary, and per-event evaluation opportunities; it replays the candidate twice. The runner cannot establish the semantic independence of an oracle or prevent a dishonest adapter from lying.

The process environment is cleared and rebuilt from PATH plus deterministic locale, timezone, and Python settings. Credentials and unrelated environment values are not passed through. Dynamic libraries, system executables invoked by an adapter, and files outside declared identity remain the adapter author's responsibility.

## JSONL v1 protocol

Each request occupies one line. Responses must have the same protocol version and request ID. Reset starts at request ID 0; subsequent steps use 1, 2, and so on.

```json
{"protocol":1,"request_id":0,"op":"reset","seed":1}
{"protocol":1,"request_id":1,"op":"step","action":{"id":"publish:0","kind":"schedule","actor":0,"op":"publish"}}
```

An adapter response:

```json
{"protocol":1,"request_id":1,"state":"stable-state-digest","enabled":[{"id":"restart:0","kind":"fault","actor":0,"op":"restart"}],"checks":[{"id":"append.committed_prefix","ok":true,"signature":"committed-prefix-lost"}],"witnesses":["lost_commit_response"],"terminal":false,"observation":{"length":6}}
```

Action kinds are `workload`, `fault`, `schedule`, and `time`. The controller sends the complete enabled action object unchanged. Action IDs are unique within a response. Response extension fields participate in replay. Duplicate JSON keys, duplicate property/action/witness IDs, invalid request IDs, and terminal states with enabled actions are rejected.

Check eligibility is explicit: omit a check while its predicate is inapplicable. Emit `{id, ok, signature?, details?}` when it is meaningful. Keep stable IDs and signatures. Prefer compact observations and failure-only details. A missing required property or witness makes a clean campaign inconclusive. A nonterminal state with no enabled actions is also inconclusive, including at the last permitted step. Temporal recovery deadlines belong in the adapter's virtual-time model.

The default request timeout is 2 seconds, response-line cap 256 KiB, and combined per-process stdout/stderr budget 4 MiB. JSON depth and collection limits prevent accepted responses from exceeding evidence-envelope parser constraints. Options can tighten these limits. Artifact and global novelty/evidence cardinality limits are fixed in this version.

## Trusted process boundary

The subprocess backend runs on POSIX systems and uses process groups for cleanup. It bounds protocol I/O, campaign time, and individual retained traces. It does not enforce a filesystem sandbox, network denial, aggregate RSS limit, or containment of a deliberately escaping process. Run trusted synthetic adapters here; select an appropriate disposable OS sandbox before executing hostile code. Native Windows is currently unsupported. Linux is the exercised CI target; macOS needs platform qualification.

## Reduction and repairs

Shrinking performs bounded action deletion. Every accepted candidate must retain enabled-action validity, the same property/signature, and exact repeatability. Invalid schedules and unrelated failures are rejected. A fully completed reducer establishes one-action-deletion minimality under this action representation. Budget interruption reports weaker minimality. Keep the original artifact and preserve both builds if future exact replay matters.

A repaired implementation should pass the retained scenario under an unchanged independent contract, then complete a fresh bounded campaign. Changed action availability or removed checks produces an explicit inconclusive regression. Requirements changes deserve a separate review from an application repair.

## S3 demonstration boundary

`append-sim` calls the same `append-core::plan_append` function as the HTTP proxy. Its independently maintained commit history checks contiguous valid storage, preserved committed content, durable acknowledgments, and stable snapshot reads. Exposed transitions cover reads, immutable segment upload, manifest publication, CAS conflicts, upload crashes, lost replies, restarts, retries, and range/snapshot reads.

The `lost-update` mutant deliberately replaces the simulator's conditional publication with an unconditional write. This demonstrates that the oracle detects a broken store protocol. It leaves the production proxy's conditional path intact. Production HTTP and S3-client behavior receive separate integration tests, including the actual AmazonS3 client against an embedded wire-contract emulator. Real AWS performance and provider qualification require a dedicated bucket run.

## Sources

- [S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).
- [S3 consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel).
- [object_store 0.12.5 PutMode](https://docs.rs/object_store/0.12.5/object_store/enum.PutMode.html).

See [S3 semantics](s3-semantics.md) for the storage contract and operational limits.
