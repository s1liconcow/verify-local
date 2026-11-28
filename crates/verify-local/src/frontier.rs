//! A queue of alternative decisions from exactly replayed executions.
use crate::{model, Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::Path,
};
use verify_protocol::{canonical_json, validate_id, Action, Response};

pub type Visits = BTreeMap<(String, String), u32>;
const MAX_ENTRIES: usize = 128;
const MAX_DEPTH: usize = 64;
const MAX_QUEUE_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Branch {
    pub seed: u64,
    pub prefix: Vec<Action>,
    pub state: String,
    pub next: Action,
    pub responses_sha256: String,
}

// Length prefixes make response boundaries unambiguous.
fn hash_response(hash: &mut Sha256, response: &Value) -> Result<()> {
    let bytes = canonical_json(response)?;
    hash.update((bytes.len() as u64).to_le_bytes());
    hash.update(bytes);
    Ok(())
}

impl Branch {
    pub fn at(trace: &model::Trace, event: usize, id: &str) -> Result<Self> {
        let raw = trace
            .responses
            .get(event)
            .ok_or_else(|| Error::harness("event outside trace"))?;
        let response: Response = serde_json::from_value(raw.clone())?;
        if model::failure(&response, event).is_some() {
            return Err(Error::harness("cannot branch after a property violation"));
        }
        let next = response
            .enabled
            .into_iter()
            .find(|a| a.id == id)
            .ok_or_else(|| Error::harness("action ID is not enabled at the selected event"))?;
        let mut hash = Sha256::new();
        for raw in &trace.responses[..=event] {
            hash_response(&mut hash, raw)?;
        }
        Ok(Self {
            seed: trace.seed,
            prefix: trace.actions[..event].to_vec(),
            state: response.state,
            next,
            responses_sha256: hex::encode(hash.finalize()),
        })
    }

    pub fn check_prefix(&self, trace: &model::Trace) -> Result<()> {
        let mut hash = Sha256::new();
        for raw in &trace.responses {
            hash_response(&mut hash, raw)?;
        }
        if hex::encode(hash.finalize()) != self.responses_sha256
            || trace.responses.last().and_then(|r| r["state"].as_str()) != Some(&self.state)
        {
            return Err(Error::divergence(format!(
                "recorded prefix diverged at or before event {}",
                self.prefix.len()
            )));
        }
        Ok(())
    }

    fn key(&self) -> (String, String) {
        (self.state.clone(), self.next.id.clone())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Corpus {
    format: String,
    version: u32,
    identity_sha256: String,
    branches: Vec<Branch>,
    visited: Vec<(String, String)>,
    visited_capped: bool,
}

#[derive(Default)]
pub struct Frontier {
    // Cached entry sizes avoid serializing the entire queue on each insertion.
    queue: VecDeque<(Branch, usize)>,
    queued: BTreeSet<(String, String)>,
    bytes: usize,
    pub capped: bool,
    visited: BTreeSet<(String, String)>,
    visited_bytes: usize,
    visited_capped: bool,
}

impl Frontier {
    pub fn load(path: &Path, identity: &model::Identity) -> Result<Self> {
        let corpus: Corpus =
            serde_json::from_value(model::read_value(path, 2 * MAX_QUEUE_BYTES + 4096)?)?;
        if corpus.format != "verify-local-frontier"
            || corpus.version != 1
            || corpus.branches.len() > MAX_ENTRIES
        {
            return Err(Error::harness("invalid frontier format or queue length"));
        }
        if corpus.identity_sha256 != verify_protocol::digest(identity)? {
            return Err(Error::inconclusive("frontier execution identity changed"));
        }
        let mut frontier = Self {
            visited_capped: corpus.visited_capped,
            ..Self::default()
        };
        for (state, action) in corpus.visited {
            validate_id(&state, "visited state")?;
            validate_id(&action, "visited action")?;
            if !frontier.remember((state, action))? {
                return Err(Error::harness("duplicate or oversized visited set"));
            }
        }
        for branch in corpus.branches {
            validate_id(&branch.state, "frontier state")?;
            validate_id(&branch.next.id, "frontier action")?;
            if branch.prefix.len() >= MAX_DEPTH
                || hex::decode(&branch.responses_sha256).map_or(true, |v| v.len() != 32)
                || frontier.visited.contains(&branch.key())
                || frontier.queued.contains(&branch.key())
                || !frontier.insert(branch)?
            {
                return Err(Error::harness("invalid or oversized frontier entry"));
            }
        }
        Ok(frontier)
    }

    fn insert(&mut self, branch: Branch) -> Result<bool> {
        let size = canonical_json(&branch)?.len();
        if self.bytes + size > MAX_QUEUE_BYTES {
            self.capped = true;
            return Ok(false);
        }
        self.bytes += size;
        self.queued.insert(branch.key());
        self.queue.push_back((branch, size));
        Ok(true)
    }

    fn remember(&mut self, key: (String, String)) -> Result<bool> {
        if self.visited.contains(&key) {
            return Ok(false);
        }
        let bytes = canonical_json(&key)?.len() + 1;
        if self.visited_bytes + bytes > MAX_QUEUE_BYTES
            || self.visited.len() == model::EVIDENCE_CARDINALITY_LIMIT
        {
            self.visited_capped = true;
            return Ok(false);
        }
        self.visited_bytes += bytes;
        self.visited.insert(key);
        Ok(true)
    }

    pub fn collect(&mut self, trace: &model::Trace, visits: &Visits) -> Result<()> {
        for (raw, action) in trace.responses.iter().zip(&trace.actions) {
            let state = raw["state"].as_str().expect("validated state");
            self.remember((state.to_owned(), action.id.clone()))?;
        }
        let mut hash = Sha256::new();
        for (index, raw) in trace.responses.iter().take(MAX_DEPTH).enumerate() {
            if self.queue.len() == MAX_ENTRIES {
                self.capped = true;
                break;
            }
            hash_response(&mut hash, raw)?;
            let response: Response = serde_json::from_value(raw.clone())?;
            if model::failure(&response, index).is_some() {
                break;
            }
            for next in response.enabled {
                let key = (response.state.clone(), next.id.clone());
                if visits.contains_key(&key)
                    || self.visited.contains(&key)
                    || self.queued.contains(&key)
                {
                    continue;
                }
                if self.queue.len() == MAX_ENTRIES {
                    self.capped = true;
                    return Ok(());
                }
                if !self.insert(Branch {
                    seed: trace.seed,
                    prefix: trace.actions[..index].to_vec(),
                    state: response.state.clone(),
                    next,
                    responses_sha256: hex::encode(hash.clone().finalize()),
                })? {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    pub fn discard_visited(&mut self, visits: &Visits, selected: Option<&Branch>) {
        self.queue.retain(|(branch, size)| {
            let key = branch.key();
            if visits.contains_key(&key)
                || selected.is_some_and(|b| b.state == branch.state && b.next.id == branch.next.id)
            {
                self.queued.remove(&key);
                self.bytes -= size;
                false
            } else {
                true
            }
        });
    }

    // Retire a choice only after its execution has matched a fresh replay.
    pub fn candidate(&self, steps: usize) -> Option<&Branch> {
        self.queue
            .iter()
            .find(|(branch, _)| branch.prefix.len() < steps)
            .map(|(b, _)| b)
    }

    pub fn visited_capped(&self) -> bool {
        self.visited_capped
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn save(&self, path: &Path, identity: &model::Identity) -> Result<()> {
        model::write_artifact(
            path,
            &serde_json::json!({"format":"verify-local-frontier","version":1,
            "identity_sha256":verify_protocol::digest(identity)?,
            "visited":self.visited, "visited_capped":self.visited_capped,
            "branches":self.queue.iter().map(|(branch, _)| branch).collect::<Vec<_>>() }),
        )
    }
}
