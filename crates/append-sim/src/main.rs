//! A deterministic transition adapter around the production append-core planner.
//! S3 atomic conditional updates and durable writes are modeled assumptions.
mod oracle;

use anyhow::{bail, Context, Result};
use append_core::{plan_append, AppendError, AppendInput, AppendPlan, Manifest, Receipt};
use clap::Parser;
use oracle::History;
use serde::Serialize;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, BufRead, Read, Write},
};
use verify_protocol::{Action, ActionKind, Request, Response};

#[derive(Parser)]
struct Args {
    /// Intentionally replace manifest compare-and-swap with an unconditional write.
    #[arg(long, value_parser = ["lost-update"])]
    mutant: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
enum Phase {
    Read,
    Upload {
        version: u64,
        next: Manifest,
        receipt: Receipt,
    },
    Publish {
        version: u64,
        next: Manifest,
        receipt: Receipt,
    },
    Reply {
        receipt: Receipt,
        replayed: bool,
    },
    Crashed,
    Done,
    Rejected,
}

#[derive(Clone, Debug, Serialize)]
struct Actor {
    input: AppendInput,
    body: Vec<u8>,
    phase: Phase,
    lost_reply: bool,
}
impl Actor {
    fn new(id: &str, body: &[u8], expected: Option<u64>) -> Self {
        Self {
            input: AppendInput::from_bytes("demo/log", id, body, expected),
            body: body.into(),
            phase: Phase::Read,
            lost_reply: false,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct Reader {
    snapshot: Manifest,
    expected: Vec<u8>,
    range: bool,
    version: u64,
}

#[derive(Clone, Debug, Serialize)]
struct Simulation {
    mutant: bool,
    manifest: Manifest,
    version: u64,
    segments: BTreeMap<String, Vec<u8>>,
    actors: Vec<Actor>,
    reader: Option<Reader>,
    history: History,
    witnesses: BTreeSet<String>,
}

impl Simulation {
    fn new(mutant: bool) -> Self {
        Self {
            mutant,
            manifest: Manifest::default(),
            version: 0,
            segments: BTreeMap::new(),
            actors: vec![
                Actor::new("request-a", b"alpha\n", None),
                Actor::new("request-b", b"beta\n", None),
                Actor::new("request-offset", b"gamma\n", Some(0)),
                Actor::new("request-a", b"different\n", None),
            ],
            reader: None,
            history: History::new(),
            witnesses: BTreeSet::new(),
        }
    }
    fn witness(&mut self, value: &str) {
        self.witnesses.insert(value.into());
    }

    fn actions(&self) -> Vec<Action> {
        let mut out = Vec::new();
        for (i, actor) in self.actors.iter().enumerate() {
            let choices: &[(&str, ActionKind)] = match actor.phase {
                Phase::Read => &[("read", ActionKind::Schedule)],
                Phase::Upload { .. } => &[("upload", ActionKind::Schedule)],
                Phase::Publish { .. } => &[
                    ("publish", ActionKind::Schedule),
                    ("publish_lost_reply", ActionKind::Fault),
                    ("crash", ActionKind::Fault),
                    ("transient_error", ActionKind::Fault),
                ],
                Phase::Reply { .. } => &[
                    ("reply", ActionKind::Schedule),
                    ("crash", ActionKind::Fault),
                ],
                Phase::Crashed => &[("restart", ActionKind::Fault)],
                Phase::Done | Phase::Rejected => &[("retry", ActionKind::Workload)],
            };
            for (op, kind) in choices {
                out.push(
                    Action::new(format!("{op}:{i}"), kind.clone())
                        .with("actor", json!(i))
                        .with("op", json!(op)),
                );
            }
        }
        if self.reader.is_some() {
            out.push(Action::new("read_finish", ActionKind::Schedule));
        } else {
            out.push(Action::new("read_begin", ActionKind::Workload));
            out.push(Action::new("range_begin", ActionKind::Workload));
        }
        out
    }

    fn step(&mut self, action: &Action) -> Result<()> {
        if !self.actions().contains(action) {
            bail!("action is not currently enabled");
        }
        if action.id == "read_begin" || action.id == "range_begin" {
            self.reader = Some(Reader {
                snapshot: self.manifest.clone(),
                expected: self.history.bytes(),
                range: action.id == "range_begin",
                version: self.version,
            });
            return Ok(());
        }
        if action.id == "read_finish" {
            let reader = self.reader.take().context("missing reader")?;
            let mut bytes = Vec::new();
            for e in &reader.snapshot.entries {
                match self.segments.get(&e.segment) {
                    Some(segment) => bytes.extend_from_slice(segment),
                    None => self.history.snapshot_ok = false,
                }
            }
            if reader.range {
                let start = reader.expected.len() / 3;
                let end = (reader.expected.len() * 2 / 3).max(start);
                self.history.snapshot_ok &= bytes.get(start..end)
                    == reader.expected.get(start..end)
                    && bytes.len() == reader.expected.len();
                if end > start {
                    self.witness("range_read");
                }
            } else {
                self.history.snapshot_ok &= bytes == reader.expected;
            }
            if reader.version != self.version {
                self.witness("snapshot_overlapped_append");
            }
            self.witness("snapshot_read");
            return Ok(());
        }
        let i = action.fields["actor"]
            .as_u64()
            .context("actor must be integer")? as usize;
        let op = action.fields["op"].as_str().context("missing operation")?;
        let phase = self.actors[i].phase.clone();
        match op {
            "read" => match plan_append(&self.manifest, &self.actors[i].input, 32) {
                Ok(AppendPlan::Existing(receipt)) => {
                    self.witness("deduplicated_retry");
                    if self.actors[i].lost_reply {
                        self.witness("recovered_committed_retry");
                    }
                    self.actors[i].phase = Phase::Reply {
                        receipt,
                        replayed: true,
                    };
                }
                Ok(AppendPlan::Commit { manifest, receipt }) => {
                    self.actors[i].phase = Phase::Upload {
                        version: self.version,
                        next: manifest,
                        receipt,
                    };
                }
                Err(AppendError::IdempotencyConflict) => {
                    self.witness("idempotency_conflict");
                    self.actors[i].phase = Phase::Rejected;
                }
                Err(AppendError::OffsetConflict { .. }) => {
                    self.witness("offset_conflict");
                    self.actors[i].phase = Phase::Rejected;
                }
                Err(e) => return Err(e.into()),
            },
            "upload" => {
                let Phase::Upload {
                    version,
                    next,
                    receipt,
                } = phase
                else {
                    bail!("upload phase");
                };
                let actor = &self.actors[i];
                if let Some(old) = self.segments.get(&actor.input.segment) {
                    if old != &actor.body {
                        bail!("immutable segment collision");
                    }
                } else {
                    self.segments
                        .insert(actor.input.segment.clone(), actor.body.clone());
                }
                self.actors[i].phase = Phase::Publish {
                    version,
                    next,
                    receipt,
                };
            }
            "publish" | "publish_lost_reply" => {
                let Phase::Publish {
                    version,
                    next,
                    receipt,
                } = phase
                else {
                    bail!("publish phase");
                };
                if self.mutant || version == self.version {
                    self.manifest = next;
                    self.version += 1;
                    let actor = &self.actors[i];
                    self.history.record_commit(
                        &actor.input.request_id,
                        &actor.body,
                        actor.input.expected_offset,
                    );
                    self.witness("append_committed");
                    if op == "publish_lost_reply" {
                        self.actors[i].phase = Phase::Crashed;
                        self.actors[i].lost_reply = true;
                        self.witness("lost_commit_response");
                    } else {
                        self.actors[i].phase = Phase::Reply {
                            receipt,
                            replayed: false,
                        };
                    }
                } else {
                    self.witness("cas_conflict");
                    self.actors[i].phase = Phase::Read;
                }
            }
            "transient_error" => {
                self.actors[i].phase = Phase::Read;
                self.witness("write_error_before_commit");
            }
            "crash" => {
                if matches!(phase, Phase::Publish { .. }) {
                    self.witness("crash_after_upload");
                }
                if matches!(
                    phase,
                    Phase::Reply {
                        replayed: false,
                        ..
                    }
                ) {
                    self.actors[i].lost_reply = true;
                    self.witness("lost_commit_response");
                }
                self.actors[i].phase = Phase::Crashed;
            }
            "restart" | "retry" => {
                self.actors[i].phase = Phase::Read;
            }
            "reply" => {
                let Phase::Reply { receipt, .. } = phase else {
                    bail!("reply phase");
                };
                self.history.acknowledgments.push(receipt);
                self.witness("append_acknowledged");
                self.actors[i].phase = Phase::Done;
            }
            _ => bail!("unknown action"),
        }
        let versions: Vec<u64> = self
            .actors
            .iter()
            .filter_map(|a| match a.phase {
                Phase::Publish { version, .. } => Some(version),
                _ => None,
            })
            .collect();
        if versions
            .iter()
            .enumerate()
            .any(|(i, v)| versions[i + 1..].contains(v))
        {
            self.witness("concurrent_writers");
        }
        Ok(())
    }

    fn response(&self, request_id: u64) -> Result<Response> {
        let mut r = Response::new(request_id, verify_protocol::digest(self)?);
        r.enabled = self.actions();
        r.checks = self.history.checks(&self.manifest, &self.segments);
        r.witnesses = self.witnesses.iter().cloned().collect();
        r.observation = Some(
            json!({ "length":self.manifest.length, "generation":self.manifest.generation,
            "stored_segments":self.segments.len(), "commits":self.history.commits.len() }),
        );
        Ok(r)
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mutant = args.mutant.is_some();
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let mut output = io::BufWriter::new(io::stdout().lock());
    let mut simulation: Option<Simulation> = None;
    let mut expected_id = 0;
    loop {
        let mut line = Vec::new();
        let count = input.by_ref().take(262_145).read_until(b'\n', &mut line)?;
        if count == 0 {
            break;
        }
        if count > 262_144 || !line.ends_with(b"\n") {
            bail!("invalid or oversized JSONL request");
        }
        let raw: Value = verify_protocol::parse_json(&line)?;
        let request: Request = serde_json::from_value(raw)?;
        if request.protocol != 1 || request.request_id != expected_id {
            bail!("protocol/request id mismatch");
        }
        match request.op.as_str() {
            "reset" if expected_id == 0 && request.seed.is_some() && request.action.is_none() => {
                simulation = Some(Simulation::new(mutant));
            }
            "step" if expected_id > 0 && request.seed.is_none() => {
                simulation
                    .as_mut()
                    .context("reset required")?
                    .step(request.action.as_ref().context("action required")?)?;
            }
            _ => bail!("invalid request operation"),
        }
        let response = simulation
            .as_ref()
            .context("reset required")?
            .response(request.request_id)?;
        serde_json::to_writer(&mut output, &response)?;
        output.write_all(b"\n")?;
        output.flush()?;
        expected_id += 1;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn step(sim: &mut Simulation, id: &str) {
        let action = sim.actions().into_iter().find(|a| a.id == id).unwrap();
        sim.step(&action).unwrap();
    }
    fn healthy(sim: &Simulation) -> bool {
        sim.history
            .checks(&sim.manifest, &sim.segments)
            .iter()
            .all(|c| c.ok)
    }
    #[test]
    fn cas_preserves_concurrent_appends_and_mutant_loses_one() {
        for mutant in [false, true] {
            let mut sim = Simulation::new(mutant);
            for id in [
                "read:0",
                "read:1",
                "upload:0",
                "upload:1",
                "publish:0",
                "publish:1",
            ] {
                step(&mut sim, id);
            }
            assert_eq!(healthy(&sim), !mutant);
            if !mutant {
                for id in ["read:1", "upload:1", "publish:1", "reply:0", "reply:1"] {
                    step(&mut sim, id);
                }
                assert_eq!(sim.manifest.length, 11);
                assert!(healthy(&sim));
                assert!(sim.witnesses.contains("cas_conflict"));
            }
        }
    }
    #[test]
    fn lost_commit_reply_recovers_original_receipt_after_another_append() {
        let mut sim = Simulation::new(false);
        for id in [
            "read:0",
            "upload:0",
            "publish_lost_reply:0",
            "read:1",
            "upload:1",
            "publish:1",
            "restart:0",
            "read:0",
            "reply:0",
        ] {
            step(&mut sim, id);
        }
        assert_eq!(sim.history.acknowledgments[0].total_length, 6);
        assert_eq!(sim.manifest.length, 11);
        assert!(sim.witnesses.contains("recovered_committed_retry"));
        assert!(healthy(&sim));
    }
    #[test]
    fn snapshot_pins_content_across_append_and_crash() {
        let mut sim = Simulation::new(false);
        for id in [
            "read:0",
            "upload:0",
            "publish:0",
            "read_begin",
            "read:1",
            "upload:1",
            "publish_lost_reply:1",
            "read_finish",
        ] {
            step(&mut sim, id);
        }
        assert!(sim.witnesses.contains("snapshot_overlapped_append"));
        assert!(healthy(&sim));
    }
}
