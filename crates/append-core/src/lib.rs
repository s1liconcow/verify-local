//! Pure production append planning shared by the HTTP proxy and deterministic simulator.
//! Durability and serialization come from publishing the returned manifest with a
//! compare-and-swap against the version read, after its immutable segment exists.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use thiserror::Error;

pub const FORMAT_VERSION: u32 = 1;
pub const DEFAULT_MAX_ENTRIES: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub format: u32,
    pub generation: u64,
    pub length: u64,
    pub entries: Vec<Entry>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            format: FORMAT_VERSION,
            generation: 0,
            length: 0,
            entries: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    pub request_id: String,
    pub offset: u64,
    pub length: u64,
    pub sha256: String,
    pub segment: String,
    pub expected_offset: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendInput {
    pub request_id: String,
    pub length: u64,
    pub sha256: String,
    pub segment: String,
    pub expected_offset: Option<u64>,
}

impl AppendInput {
    pub fn from_bytes(
        key: &str,
        request_id: &str,
        body: &[u8],
        expected_offset: Option<u64>,
    ) -> Self {
        let sha256 = digest_hex(body);
        Self {
            request_id: request_id.to_owned(),
            length: body.len() as u64,
            segment: segment_key(key, request_id, &sha256),
            sha256,
            expected_offset,
        }
    }

    pub fn validate(&self) -> Result<(), AppendError> {
        validate_request_id(&self.request_id)?;
        if self.length == 0 {
            return Err(AppendError::Invalid(
                "an append must contain at least one byte".into(),
            ));
        }
        if !valid_digest(&self.sha256) {
            return Err(AppendError::Invalid(
                "payload digest must be lowercase SHA-256".into(),
            ));
        }
        validate_segment(&self.segment)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub request_id: String,
    pub offset: u64,
    pub length: u64,
    /// Length immediately after this append committed, including on an old retry.
    pub total_length: u64,
    /// Generation of this append's commit, including on an old retry.
    pub generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppendPlan {
    Existing(Receipt),
    Commit {
        manifest: Manifest,
        receipt: Receipt,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum AppendError {
    #[error("the idempotency key is already bound to a different append intent")]
    IdempotencyConflict,
    #[error("expected append offset {expected}, current length is {actual}")]
    OffsetConflict { expected: u64, actual: u64 },
    #[error("object reached its {limit}-append metadata limit; rotate to a new object")]
    Full { limit: usize },
    #[error("{0}")]
    Invalid(String),
    #[error("object length or generation overflow")]
    Overflow,
}

pub fn validate_request_id(id: &str) -> Result<(), AppendError> {
    if id.is_empty() || id.len() > 128 || id.bytes().any(|c| !(0x21..=0x7e).contains(&c)) {
        return Err(AppendError::Invalid(
            "idempotency key must be 1-128 visible ASCII bytes".into(),
        ));
    }
    Ok(())
}

pub fn validate_object_key(key: &str) -> Result<(), AppendError> {
    if key.is_empty() || key.len() > 1024 || key.chars().any(char::is_control) {
        return Err(AppendError::Invalid(
            "object key must be 1-1024 bytes without control characters".into(),
        ));
    }
    Ok(())
}

fn valid_digest(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

fn validate_segment(s: &str) -> Result<(), AppendError> {
    if !s.starts_with("segments/")
        || s.len() > 256
        || !s
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || b"/-".contains(&c))
        || s.contains("//")
    {
        return Err(AppendError::Invalid(
            "invalid immutable segment path".into(),
        ));
    }
    Ok(())
}

impl Manifest {
    pub fn validate(&self) -> Result<(), AppendError> {
        if self.format != FORMAT_VERSION {
            return Err(AppendError::Invalid("unsupported manifest format".into()));
        }
        if self.generation != self.entries.len() as u64 {
            return Err(AppendError::Invalid(
                "generation does not match append count".into(),
            ));
        }
        let mut next = 0u64;
        let mut seen = BTreeSet::new();
        for entry in &self.entries {
            validate_request_id(&entry.request_id)?;
            validate_segment(&entry.segment)?;
            if entry.length == 0 || !valid_digest(&entry.sha256) {
                return Err(AppendError::Invalid(
                    "invalid segment length or digest".into(),
                ));
            }
            if !seen.insert(&entry.request_id) {
                return Err(AppendError::Invalid(
                    "duplicate committed request ID".into(),
                ));
            }
            if entry.offset != next {
                return Err(AppendError::Invalid(
                    "manifest offsets are not contiguous".into(),
                ));
            }
            if entry
                .expected_offset
                .is_some_and(|offset| offset != entry.offset)
            {
                return Err(AppendError::Invalid(
                    "committed offset differs from append precondition".into(),
                ));
            }
            next = next
                .checked_add(entry.length)
                .ok_or(AppendError::Overflow)?;
        }
        if next != self.length {
            return Err(AppendError::Invalid(
                "manifest length differs from its segments".into(),
            ));
        }
        Ok(())
    }

    pub fn receipt(&self, index: usize) -> Receipt {
        let entry = &self.entries[index];
        Receipt {
            request_id: entry.request_id.clone(),
            offset: entry.offset,
            length: entry.length,
            total_length: entry.offset + entry.length,
            generation: index as u64 + 1,
        }
    }
}

/// Plan one operation against one immutable manifest snapshot.
///
/// Existing receipts are resolved before offset checks and capacity checks. The
/// caller must upload the immutable payload first, then conditionally publish
/// the returned manifest with the exact ETag associated with this snapshot.
/// On conditional conflict or ambiguous response, reread and call this again.
pub fn plan_append(
    current: &Manifest,
    input: &AppendInput,
    limit: usize,
) -> Result<AppendPlan, AppendError> {
    current.validate()?;
    input.validate()?;
    if let Some((index, entry)) = current
        .entries
        .iter()
        .enumerate()
        .find(|(_, entry)| entry.request_id == input.request_id)
    {
        if entry.length != input.length
            || entry.sha256 != input.sha256
            || entry.expected_offset != input.expected_offset
            || entry.segment != input.segment
        {
            return Err(AppendError::IdempotencyConflict);
        }
        return Ok(AppendPlan::Existing(current.receipt(index)));
    }
    if let Some(expected) = input.expected_offset {
        if expected != current.length {
            return Err(AppendError::OffsetConflict {
                expected,
                actual: current.length,
            });
        }
    }
    if current.entries.len() >= limit {
        return Err(AppendError::Full { limit });
    }
    let length = current
        .length
        .checked_add(input.length)
        .ok_or(AppendError::Overflow)?;
    let generation = current
        .generation
        .checked_add(1)
        .ok_or(AppendError::Overflow)?;
    let receipt = Receipt {
        request_id: input.request_id.clone(),
        offset: current.length,
        length: input.length,
        total_length: length,
        generation,
    };
    let mut manifest = current.clone();
    manifest.entries.push(Entry {
        request_id: input.request_id.clone(),
        offset: current.length,
        length: input.length,
        sha256: input.sha256.clone(),
        segment: input.segment.clone(),
        expected_offset: input.expected_offset,
    });
    manifest.length = length;
    manifest.generation = generation;
    Ok(AppendPlan::Commit { manifest, receipt })
}

pub fn digest_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub fn manifest_key(object_key: &str) -> String {
    format!(
        "objects/{}/manifest.json",
        digest_hex(object_key.as_bytes())
    )
}

pub fn segment_key(object_key: &str, request_id: &str, payload_digest: &str) -> String {
    format!(
        "segments/{}/{}-{}",
        digest_hex(object_key.as_bytes()),
        digest_hex(request_id.as_bytes()),
        payload_digest
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    fn input(id: &str, body: &[u8], expected: Option<u64>) -> AppendInput {
        AppendInput::from_bytes("log", id, body, expected)
    }
    fn commit(m: &Manifest, i: &AppendInput) -> (Manifest, Receipt) {
        match plan_append(m, i, 8).unwrap() {
            AppendPlan::Commit { manifest, receipt } => (manifest, receipt),
            _ => panic!("expected commit"),
        }
    }

    #[test]
    fn retry_resolves_original_receipt_before_stale_offset_and_full_limit() {
        let first = input("a", b"hello", Some(0));
        let (m, receipt) = commit(&Manifest::default(), &first);
        let (m, _) = commit(&m, &input("b", b"world", None));
        assert_eq!(
            plan_append(&m, &first, 0).unwrap(),
            AppendPlan::Existing(receipt)
        );
    }

    #[test]
    fn idempotency_binds_payload_and_offset_intent() {
        let first = input("a", b"one", None);
        let (m, _) = commit(&Manifest::default(), &first);
        assert_eq!(
            plan_append(&m, &input("a", b"two", None), 8),
            Err(AppendError::IdempotencyConflict)
        );
        assert_eq!(
            plan_append(&m, &input("a", b"one", Some(0)), 8),
            Err(AppendError::IdempotencyConflict)
        );
    }

    #[test]
    fn stale_offset_is_rejected_for_new_operation() {
        let (m, _) = commit(&Manifest::default(), &input("a", b"abc", None));
        assert_eq!(
            plan_append(&m, &input("b", b"d", Some(0)), 8),
            Err(AppendError::OffsetConflict {
                expected: 0,
                actual: 3
            })
        );
    }

    #[test]
    fn concurrent_snapshots_require_conditional_publication() {
        let initial = Manifest::default();
        let (a, _) = commit(&initial, &input("a", b"A", None));
        let (b_stale, _) = commit(&initial, &input("b", b"B", None));
        assert_eq!(a.generation, b_stale.generation);
        // The store rejects b_stale's outdated ETag; planning against a keeps both.
        let (both, _) = commit(&a, &input("b", b"B", None));
        assert_eq!(both.length, 2);
        assert_eq!(
            both.entries
                .iter()
                .map(|e| e.request_id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
    }

    #[test]
    fn malformed_manifest_fails_before_planning() {
        let (mut m, _) = commit(&Manifest::default(), &input("a", b"x", None));
        m.entries[0].offset = 8;
        assert!(matches!(
            plan_append(&m, &input("b", b"y", None), 8),
            Err(AppendError::Invalid(_))
        ));
    }

    #[test]
    fn limits_never_silently_evict_idempotency_receipts() {
        let (m, _) = commit(&Manifest::default(), &input("a", b"x", None));
        assert_eq!(
            plan_append(&m, &input("b", b"y", None), 1),
            Err(AppendError::Full { limit: 1 })
        );
        assert!(matches!(
            plan_append(&m, &input("a", b"x", None), 1),
            Ok(AppendPlan::Existing(_))
        ));
    }

    #[test]
    fn key_namespace_is_collision_resistant_and_does_not_embed_user_paths() {
        let key = manifest_key("../../a/☃");
        assert!(key.starts_with("objects/"));
        assert!(!key.contains(".."));
        assert_ne!(key, manifest_key("../../a"));
        assert_ne!(
            segment_key("a", "id", &digest_hex(b"x")),
            segment_key("b", "id", &digest_hex(b"x"))
        );
    }

    #[test]
    fn empty_append_and_control_ids_are_rejected() {
        assert!(input("a", b"", None).validate().is_err());
        assert!(input("bad\nid", b"x", None).validate().is_err());
    }
}
