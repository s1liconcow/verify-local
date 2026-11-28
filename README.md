# verify-local

A Rust toolbox for bounded local correctness verification, with compact agent-facing results and a language-neutral JSONL adapter protocol.

Specify properties, expose controlled scheduling and fault decisions, explore difficult histories, and keep reproducible counterexamples. The controller provides exact replay, failure reduction, contract-preserving regression, and evidence files. Its deterministic boundary is the application adapter; native scheduling, weak memory, and real provider behavior require their own verification backends.

The [S3 append proxy implementation, verification reports, and evidence](https://github.com/s1liconcow/s3-append-proxy) live in their own repository. This repository owns the reusable controller, async scheduler, history checker, protocol, and skill.

## Quick start

Requirements: Rust 1.100.0, Python 3.10+, and Node.js 22+. Linux is the exercised target. The toolchain file pins Rust, rustfmt, and Clippy. The controller itself has no Python or JavaScript runtime dependency; those interpreters run the example adapters and demonstration script.

```bash
cargo build --release --workspace --locked
python3 scripts/demo.py --skip-build
```

The demonstration runs 100 executions of 80 actions in each language and checks a separate HTTP/SQLite service through crash, restart, retry, and audit. Each completed execution is replayed in a fresh process. Evidence stays in a new ignored `.verify/demo-*` directory.

To install just the controller:

```bash
cargo install --path crates/verify-local --locked
```

## Try a real process crash

The ledger example sends an HTTP credit request over loopback to a separate process using an on-disk SQLite WAL. The verifier can kill the service with an open transaction or after commit while the HTTP reply is still pending. It observes the client disconnect, restarts the service on the same database, retries the same credit, and checks the balance through the public HTTP API. Every new execution gets its own database.

```bash
target/release/verify-local run examples/sqlite-http/verify.json \
  --guided --out .verify/ledger --runs 8 --steps 12 --seed 1
target/release/verify-local run examples/sqlite-http/verify-mutant.json \
  --guided --out .verify/volatile --runs 8 --steps 12 --seed 1
```

The second command should exit 1 with a replayed `ledger.once` violation. Its deliberately defective service forgets idempotency receipts on restart. The credit survives, and retrying duplicates the durable effect. The full demonstration also shrinks the failure, tests the fixed service against the same trace, and revisits the commit to let its reply through.

## Guided exploration and debugging

- `run --guided` alternates fresh workloads with unexplored decisions from matched traces. Each saved branch carries its original workload seed and a digest of every response up to the decision. Resume from a campaign with `--corpus-from .verify/ledger/frontier.json` and a new output directory. Property and witness counts start fresh on each campaign.
- `run --max-findings 4` keeps exploring until four distinct property/signature pairs reproduce or the campaign reaches its bounds. The campaign records the number of sightings and one replayable trace per pair. The default is one finding.
- `report TRACE --events --from N --limit 1` shows action IDs offered at event N. `fork MANIFEST TRACE --at N --action ID --steps 8 --out FILE` reconstructs that event, takes the selected action, explores up to eight more steps, and verifies the new trace by exact replay.

These features follow the guided exploration and replay workflow described by Antithesis. Local control extends to decisions the adapter exposes. The HTTP example uses real sockets, process death, and SQLite recovery. Instruction scheduling, disk power loss, and arbitrary container workloads require separate backends. [Antithesis product overview](https://antithesis.com/product/)

## Components

| Component | Responsibility |
| --- | --- |
| `crates/verify-local` | Compact CLI, bounded subprocess transport, exploration, exact replay, reduction, regression, paged reports |
| `crates/verify-protocol` | Typed JSONL v1 messages, strict JSON parsing, canonicalization and protocol validation |
| `crates/verify-async` | Explicit future polling and virtual Tokio time, plus bounded linearizability search |
| `examples/python`, `examples/javascript` | Equivalent standard-library adapters demonstrating language-neutral operation |
| `examples/sqlite-http` | HTTP ledger, process fault controls, independent client oracle, and a seeded defect |
| `skills/verify-local` | Compact agent instructions and on-demand protocol reference |

The Python and JavaScript counter examples each model a durable idempotent counter with a checkpoint, lost reply, process restart, and retry. They check one committed effect per operation, agreement with a commit history, and durability of acknowledged operations. Durability is modeled in memory. These examples establish protocol interoperability and bounded behavior within that model.

## Context-efficient tooling

```bash
target/release/verify-local doctor examples/python/verify.json
target/release/verify-local run examples/python/verify.json \
  --out .verify/my-run --runs 100 --steps 80 --seconds 30
target/release/verify-local report .verify/my-run/campaign.json
```

Default output is one compact JSON object containing the outcome, counts, failure identity, missing evidence, and artifact paths. Full identities and event histories stay in files. `report TRACE --events --from 0 --limit 8` reads a bounded event page. Summary output is capped at 12 KiB.

| Operation | Purpose |
| --- | --- |
| `doctor` | Validate manifest and execution identity without launching code |
| `run` | Explore legal actions within explicit run, step, time and I/O limits |
| `replay` | Require the original identity and compare every response |
| `fork` | Try a different enabled action at a recorded event and retain the replayed result |
| `shrink` | Retain a shorter valid, reproducible failure of the same signature |
| `regression` | Exercise a preserved failure scenario against changed code with the same pinned contract |
| `report` | Read a concise campaign or trace summary, with optional event pagination |

A clean campaign reports `NO_FAILURE_WITHIN_BOUNDS`. Failure, missing evidence, and harness problems have distinct outcomes and exit codes. Every completed execution is replayed; full clean traces are sampled for retention. Finite campaigns provide evidence within their stated bounds.

See [verification.md](docs/verification.md) for the protocol, manifests, identity rules, resource boundaries, and adapter authoring. The [historical validation](docs/validation.json) records the checks performed when the application moved to its own repository.

## Application integration

An adapter reads one JSON request per line and returns one JSON response. It resets fresh modeled state and exposes legal action objects, deterministic state identity, eligible property checks, and scenario witnesses. The runner chooses actions and retains complete evidence outside agent context.

Include production logic, the adapter, independent oracle, fixtures, and dependency locks in the declared identity. Pin unchanged oracle/contract files for cross-version regression. Missing required properties or witnesses make a clean campaign inconclusive. The subprocess backend bounds protocol handling and process cleanup; adapters execute as trusted host processes.

For a production service example, [s3-append-proxy](https://github.com/s1liconcow/s3-append-proxy) schedules the direct-S3 HTTP router and signed storage client, injects faults at the HTTP connector, and checks complete operation histories against an independent model. Its `verify.service.json` requires a terminal history on every clean run. The app also retains its original planner and local-tier simulations.

## Development

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

The toolbox tests cover deterministic future scheduling, real-time history constraints, terminal evidence, protocol validation, exact replay, shrinking, regression, identity drift, and subprocess and evidence bounds. CI runs the gates above, builds release binaries, and executes the language and service demonstrations. Each workflow run publishes the demonstration evidence as the `verification-evidence` artifact for seven days.

The `verify-local-skill` ZIP on a successful GitHub Actions run contains the installable skill directory. The source is also available under `skills/verify-local`. Keep property implementations and production code changes separately reviewable.

Apache-2.0 licensed. This project is an independent local implementation inspired by property-driven deterministic testing. Antithesis is a separate commercial system.
