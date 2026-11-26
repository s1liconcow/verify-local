//! Independent history oracle. This module never calls the production append planner.
use append_core::{Manifest, Receipt};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use verify_protocol::Check;

#[derive(Clone, Debug, Serialize)]
pub struct Commit {
    pub id: String,
    pub body: Vec<u8>,
    pub expected_offset: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct History {
    pub commits: Vec<Commit>,
    pub acknowledgments: Vec<Receipt>,
    pub snapshot_ok: bool,
    pub conflicting_commit: bool,
}

impl History {
    pub fn new() -> Self {
        Self {
            snapshot_ok: true,
            ..Self::default()
        }
    }

    pub fn record_commit(&mut self, id: &str, body: &[u8], expected_offset: Option<u64>) {
        if let Some(old) = self.commits.iter().find(|c| c.id == id) {
            if old.body != body || old.expected_offset != expected_offset {
                self.conflicting_commit = true;
            }
        } else {
            self.commits.push(Commit {
                id: id.into(),
                body: body.into(),
                expected_offset,
            });
        }
    }

    pub fn bytes(&self) -> Vec<u8> {
        self.commits
            .iter()
            .flat_map(|c| c.body.iter().copied())
            .collect()
    }

    pub fn checks(&self, manifest: &Manifest, segments: &BTreeMap<String, Vec<u8>>) -> Vec<Check> {
        let mut seen = BTreeSet::new();
        let mut offset = 0u64;
        let mut structurally_valid =
            manifest.format == 1 && manifest.generation == manifest.entries.len() as u64;
        let mut actual = Vec::new();
        for entry in &manifest.entries {
            structurally_valid &= seen.insert(&entry.request_id)
                && entry.length > 0
                && entry.offset == offset
                && entry.expected_offset.is_none_or(|n| n == offset);
            if let Some(body) = segments.get(&entry.segment) {
                structurally_valid &= body.len() as u64 == entry.length
                    && append_core::digest_hex(body) == entry.sha256;
                actual.extend_from_slice(body);
            } else {
                structurally_valid = false;
            }
            match offset.checked_add(entry.length) {
                Some(n) => offset = n,
                None => structurally_valid = false,
            }
        }
        structurally_valid &= manifest.length == offset;
        let committed_prefix = !self.conflicting_commit
            && actual == self.bytes()
            && manifest.entries.len() == self.commits.len()
            && manifest
                .entries
                .iter()
                .zip(&self.commits)
                .all(|(e, c)| e.request_id == c.id && e.expected_offset == c.expected_offset);
        let durable = self.acknowledgments.iter().all(|ack| {
            manifest.entries.iter().enumerate().any(|(i, entry)| {
                entry.request_id == ack.request_id
                    && entry.offset == ack.offset
                    && entry.length == ack.length
                    && ack.total_length == entry.offset + entry.length
                    && ack.generation == i as u64 + 1
                    && self.commits.iter().any(|c| {
                        c.id == ack.request_id && segments.get(&entry.segment) == Some(&c.body)
                    })
            })
        });
        vec![
            Check::new("append.manifest_valid", structurally_valid)
                .signature("invalid-manifest-or-segment"),
            Check::new("append.committed_prefix", committed_prefix)
                .signature("committed-prefix-lost"),
            Check::new("append.acknowledged_durable", durable)
                .signature("acknowledged-append-lost"),
            Check::new("append.snapshot_consistent", self.snapshot_ok)
                .signature("snapshot-content-changed"),
        ]
    }
}
