# verify-local

A Rust toolbox for bounded local correctness verification, with compact agent-facing results and a language-neutral JSONL adapter protocol.

Specify properties, expose controlled scheduling and fault decisions, explore difficult histories, and keep reproducible counterexamples. The controller provides exact replay, failure reduction, contract-preserving regression, and evidence files. Its deterministic boundary is the application adapter; native scheduling, weak memory, and real provider behavior require their own verification backends.

The [S3 append proxy implementation, verification reports, and evidence](https://github.com/s1liconcow/s3-append-proxy) now live in their own repository. This repository owns the reusable controller, protocol, skill, and generic language examples.

## Quick start

Requirements: Rust 1.100.0, Python 3.10+, and Node.js 22+. Linux is the exercised target. The toolchain file pins Rust, rustfmt, and Clippy. The controller itself has no Python or JavaScript runtime dependency; those interpreters run the example adapters and demonstration script.

```bash
cargo build --release --workspace --locked
python3 scripts/demo.py --skip-build
```

The demonstration validates both example manifests and runs 100 executions of 80 actions in each language, replaying every completed execution in a fresh process. It checks the expected outcomes and prints compact results. Evidence stays in a new ignored `.verify/demo-*` directory.

To install just the controller:

```bash
cargo install --path crates/verify-local --locked
```

## Components

| Component | Responsibility |
| --- | --- |
| `crates/verify-local` | Compact CLI, bounded subprocess transport, exploration, exact replay, reduction, regression, paged reports |
| `crates/verify-protocol` | Typed JSONL v1 messages, strict JSON parsing, canonicalization and protocol validation |
| `examples/python`, `examples/javascript` | Equivalent standard-library adapters demonstrating language-neutral operation |
| `skills/verify-local` | Compact agent instructions and on-demand protocol reference |

Each example models a durable idempotent counter with a checkpoint, lost reply, process restart, and retry. It checks one committed effect per operation, agreement with a commit history, and durability of acknowledged operations. Durability is modeled in memory. These examples establish protocol interoperability and bounded behavior within that model.

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
| `shrink` | Retain a shorter valid, reproducible failure of the same signature |
| `regression` | Exercise a preserved failure scenario against changed code with the same pinned contract |
| `report` | Read a concise campaign or trace summary, with optional event pagination |

A clean campaign reports `NO_FAILURE_WITHIN_BOUNDS`. Failure, missing evidence, and harness problems have distinct outcomes and exit codes. Every completed execution is replayed; full clean traces are sampled for retention. Finite campaigns provide evidence within their stated bounds.

See [verification.md](docs/verification.md) for the protocol, manifests, identity rules, resource boundaries, and adapter authoring. The [recorded validation](docs/validation.json) distinguishes current toolbox checks from the earlier combined-repository evidence.

## Application integration

An adapter reads one JSON request per line and returns one JSON response. It resets fresh modeled state and exposes legal action objects, deterministic state identity, eligible property checks, and scenario witnesses. The runner chooses actions and retains complete evidence outside agent context.

Include production logic, the adapter, independent oracle, fixtures, and dependency locks in the declared identity. Pin unchanged oracle/contract files for cross-version regression. Missing required properties or witnesses make a clean campaign inconclusive. The subprocess backend bounds protocol handling and process cleanup; adapters execute as trusted host processes.

For a Rust application that shares actual production logic with its simulator, see [s3-append-proxy](https://github.com/s1liconcow/s3-append-proxy). That repository contains its append implementation, intentional lost-update mutant, reduced scenario, benchmarks, and application-specific evidence.

## Development

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

The 16 toolbox tests cover protocol validation, duplicate JSON keys, exact replay, shrinking, contract-preserving regression, per-event property eligibility, missing evidence, hidden entropy, timeouts, malformed traces, identity drift, output bounds, and exclusive artifact creation. CI runs the gates above, builds release binaries, and executes the Python and JavaScript demonstration.

Use the installed `verify-local` skill or the exported skill directory to guide integration into a target application. Keep property implementations and production code changes separately reviewable.

Apache-2.0 licensed. This project is an independent local implementation inspired by property-driven deterministic testing. Antithesis is a separate commercial system.
