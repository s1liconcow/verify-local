---
name: verify-local
description: Run bounded on-device correctness verification with a Rust toolbox and language-neutral adapters. Use for deterministic production service scheduling, stateful and concurrency bugs, fault injection at I/O boundaries, linearizability, exact replay, reduction, and evidence-driven repair. Choose native backends when the target needs thread, memory, or OS control.
---

# Verify Local

Find reproducible violations of explicit properties. Keep execution, evidence, and verdicts in the Rust toolbox; use the agent for specifications, adapter integration, triage, and authorized repair.

## Load only what is needed

Use the target project's documented pinned `verify-local` binary, a PATH installation, or a toolbox checkout's `target/release/verify-local`. The Rust toolbox lives in `https://github.com/s1liconcow/verify-local`. The worked application and its verification evidence live in `https://github.com/s1liconcow/s3-append-proxy`. Build with `cargo build --release --workspace --locked`; install the controller with `cargo install --path crates/verify-local --locked`. Read `--help` for supported options. Use [rust-toolbox.md](references/rust-toolbox.md) only when writing an adapter or interpreting evidence. Rust produces evidence v2 and uses JSONL protocol v1.

Keep adapters, manifests, contracts, and retained regressions in the target project. Put campaigns in a new ignored `.verify/` subdirectory. Follow the user's project and Git workflow.

## Define the target and contract

1. Identify production logic, state ownership, relevant effects, and existing tests. State the controlled decisions, modeled dependencies, remaining nondeterminism, and finite bounds.
2. For service-level claims, drive normal production entry points through response-body consumption. Keep admission, retries, serialization, storage clients, and streaming in the exercised path. Intercept the lowest practical I/O boundary and enumerate any production work the adapter bypasses. Use a native backend when the property depends on native threads, memory ordering, disk, or OS scheduling.
3. Derive stable property IDs from requirements or an independent reference history. Define eligibility, observation points, failure signatures, and meaningful reachability witnesses. Observe committed effects when checking idempotency. Model recovery deadlines explicitly; unfinished temporal obligations cannot pass.
4. Pin oracle and contract files before changing implementation. Preserve their intent during repairs. Explain requirements changes separately.

## Integrate a language-neutral adapter

Use one JSON request and response per line on stdin/stdout. Reset fresh modeled state; return exact enabled action objects, a deterministic state digest, eligible checks, witnesses, and compact observations. Reserve stderr for diagnostics. Keep details small and emit large diagnostics only on failure.

Place the manifest above every declared identity file. Include executable argv, behavior-affecting source/fixture/lock files, nonempty `required_properties`, required witnesses, and explicit boundaries. Include unchanged oracle files in both `identity_files` and `contract_files` to support regression.

## Verify a production async service

1. Schedule invocation, runnable future polls, I/O delivery, cancellation, and virtual time as explicit choices. Use `verify-async::scheduler::Scheduler` for Rust futures with Tokio timers. Route background tasks and entropy through controllable boundaries where needed. Keep real-time and provider assumptions in the manifest.
2. At the storage boundary, separate request transmission, staged transfer, atomic commit, and reply delivery. For an atomic backend, interrupted transfer can leave staged bytes and no visible change. After commit, drop or delay the reply while preserving the durable effect. If accepted I/O can finish after cancellation, model that possibility and drain it before sealing the history.
3. Generate seeded workloads with overlapping writers and streaming readers, exact retries, conflicting idempotency intents, capacity exhaustion, cancellation, and retries after unknown outcomes with and without a durable commit. Require witnesses for actual faults, competing operations, and observed recoveries.
4. Record each invocation and its observable outcome, including receipts, range metadata, bytes, partial responses, timeouts, and cancelled calls. Add unfaulted reads after in-flight work settles. Check the whole history against a separate sequential specification that never calls application planning, range, or receipt code. Preserve real-time precedence and model effects of indeterminate calls within their possible intervals. Compare durable effects with independently decoded observations.
5. Set `require_terminal: true` in the manifest. Emit a clean terminal result only after the history is complete and its oracle finishes. Step bounds, dead ends, unresolved work, and search exhaustion leave the campaign inconclusive. `verify-async::history::linearize` provides bounded serialization search for up to 64 operations. The S3 append example uses `verify.service.json` and `scripts/service-demo.py` in the application repository.

Use trusted adapters. The subprocess runner bounds transport and process lifetime. Filesystem and network confinement and aggregate memory isolation require another backend.

## Explore, reproduce, reduce

```bash
verify-local doctor verify.json
verify-local run verify.json --out .verify/run-001 --runs 100 --steps 80 --seconds 30 --seed 1
verify-local report .verify/run-001/campaign.json
verify-local replay verify.json .verify/run-001/failure-0001.json
verify-local shrink verify.json .verify/run-001/failure-0001.json \
  --out .verify/minimized.json --attempts 200 --seconds 30
verify-local report .verify/minimized.json --events --from 0 --limit 8
```

Exercise a baseline and a relevant seeded defect to validate oracle sensitivity. Doctor checks configuration only. Every completed exploration trace receives an exact fresh-process replay. Require identical full responses and declared identity; investigate the first divergence.

Read compact command results first. Request a bounded event page for the relevant step. Full identities and observations remain in evidence files; avoid dumping traces into context. Expand a budget only to resolve a concrete gap. Preserve original counterexamples and report interrupted reduction; claim minimality only as reported by the reducer.

## Repair and demonstrate

When authorized, fix production code under the unchanged property contract. Keep both original evidence and build identity. Use `regression` for changed code:

```bash
verify-local regression verify-fixed.json .verify/minimized.json --out .verify/regression.json
```

Require the candidate to replay, preserve the original per-event checks and scenario witnesses, and pass the retained scenario. Changed action availability or omitted checks must remain inconclusive. Run a fresh campaign and the targeted real-dependency integration tests needed to establish model correspondence. The adapter and oracle remain part of the trusted verification boundary.

## Report precise evidence

Report the outcome, production code exercised, modeled assumptions, run/action/replay counts, property and witness coverage, failure signature, original/reduced action counts, artifact references, and remaining qualification gaps.

| Outcome | Exit | Meaning |
| --- | --- | --- |
| `NO_FAILURE_WITHIN_BOUNDS` | 0 | Requested execution and required evidence completed |
| `FOUND` | 1 | A property violation reproduced exactly |
| `INCONCLUSIVE` | 2 | Evidence, execution, identity, or replay was insufficient |
| `HARNESS_ERROR` | 3 | Configuration, protocol, or adapter failure prevented an experiment |

Doctor/report are informational; inspect their structured fields. Scope a replay or regression result to that retained scenario. Describe finite exploration as bounded evidence. Keep native scheduling, real storage durability, and provider performance claims tied to separately observed tests.
