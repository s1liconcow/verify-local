//! Language-neutral, bounded JSONL protocol for controlled simulation adapters.
use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

pub const VERSION: u32 = 1;
pub const MAX_COLLECTION: usize = 1024;
pub const MAX_ID_BYTES: usize = 256;
/// Leaves room for trace/campaign envelopes below serde_json's parser depth limit.
pub const MAX_RESPONSE_DEPTH: usize = 64;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ActionKind {
    Workload,
    Fault,
    Schedule,
    Time,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Action {
    pub id: String,
    pub kind: ActionKind,
    #[serde(flatten)]
    pub fields: BTreeMap<String, Value>,
}
impl Action {
    pub fn new(id: impl Into<String>, kind: ActionKind) -> Self {
        Self {
            id: id.into(),
            kind,
            fields: BTreeMap::new(),
        }
    }
    pub fn with(mut self, key: impl Into<String>, value: Value) -> Self {
        self.fields.insert(key.into(), value);
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Check {
    pub id: String,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}
impl Check {
    pub fn new(id: impl Into<String>, ok: bool) -> Self {
        Self {
            id: id.into(),
            ok,
            signature: None,
            details: None,
            extra: BTreeMap::new(),
        }
    }
    pub fn signature(mut self, signature: impl Into<String>) -> Self {
        self.signature = Some(signature.into());
        self
    }
    pub fn details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Response {
    pub protocol: u32,
    pub request_id: u64,
    pub state: String,
    pub enabled: Vec<Action>,
    pub checks: Vec<Check>,
    pub witnesses: Vec<String>,
    pub terminal: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation: Option<Value>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}
impl Response {
    pub fn new(request_id: u64, state: impl Into<String>) -> Self {
        Self {
            protocol: VERSION,
            request_id,
            state: state.into(),
            enabled: Vec::new(),
            checks: Vec::new(),
            witnesses: Vec::new(),
            terminal: false,
            observation: None,
            extra: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Request {
    pub protocol: u32,
    pub request_id: u64,
    pub op: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<Action>,
}
impl Request {
    pub fn reset(seed: u64) -> Self {
        Self {
            protocol: VERSION,
            request_id: 0,
            op: "reset".into(),
            seed: Some(seed),
            action: None,
        }
    }
    pub fn step(request_id: u64, action: Action) -> Self {
        Self {
            protocol: VERSION,
            request_id,
            op: "step".into(),
            seed: None,
            action: Some(action),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ProtocolError(pub String);

pub fn validate_id(value: &str, name: &str) -> Result<(), ProtocolError> {
    if value.is_empty() || value.len() > MAX_ID_BYTES {
        return Err(ProtocolError(format!(
            "{name} must contain 1..={MAX_ID_BYTES} bytes"
        )));
    }
    Ok(())
}

pub fn validate_response(r: &Response, expected_id: u64) -> Result<(), ProtocolError> {
    if r.protocol != VERSION || r.request_id != expected_id {
        return Err(ProtocolError("protocol/request_id mismatch".into()));
    }
    validate_id(&r.state, "state")?;
    if r.enabled.len() > MAX_COLLECTION
        || r.checks.len() > MAX_COLLECTION
        || r.witnesses.len() > MAX_COLLECTION
    {
        return Err(ProtocolError(
            "response collection exceeds 1024 entries".into(),
        ));
    }
    if r.terminal && !r.enabled.is_empty() {
        return Err(ProtocolError(
            "terminal response has enabled actions".into(),
        ));
    }
    let mut ids = BTreeSet::new();
    for a in &r.enabled {
        validate_id(&a.id, "action id")?;
        if !ids.insert(&a.id) {
            return Err(ProtocolError("duplicate action id".into()));
        }
        if a.fields.contains_key("id") || a.fields.contains_key("kind") {
            return Err(ProtocolError(
                "action extra fields shadow reserved fields".into(),
            ));
        }
    }
    ids.clear();
    for c in &r.checks {
        validate_id(&c.id, "property id")?;
        if !ids.insert(&c.id) {
            return Err(ProtocolError("duplicate check id".into()));
        }
        if let Some(s) = &c.signature {
            validate_id(s, "failure signature")?;
        }
    }
    ids.clear();
    for witness in &r.witnesses {
        validate_id(witness, "witness id")?;
        if !ids.insert(witness) {
            return Err(ProtocolError("duplicate witness id".into()));
        }
    }
    validate_depth(
        &serde_json::to_value(r).map_err(|e| ProtocolError(e.to_string()))?,
        0,
    )?;
    Ok(())
}

fn validate_depth(value: &Value, depth: usize) -> Result<(), ProtocolError> {
    if depth > MAX_RESPONSE_DEPTH {
        return Err(ProtocolError(format!(
            "response JSON exceeds nesting depth {MAX_RESPONSE_DEPTH}"
        )));
    }
    match value {
        Value::Object(fields) => {
            for nested in fields.values() {
                validate_depth(nested, depth + 1)?;
            }
        }
        Value::Array(items) => {
            for nested in items {
                validate_depth(nested, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Preserve every response field in artifacts, and reject duplicate keys at any depth.
pub fn parse_json(bytes: &[u8]) -> Result<Value, serde_json::Error> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = StrictValue::deserialize(&mut deserializer)?.0;
    deserializer.end()?;
    Ok(value)
}

struct StrictValue(Value);
impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = StrictValue;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("finite JSON without duplicate keys")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
                serde_json::Number::from_f64(v)
                    .map(|n| StrictValue(Value::Number(n)))
                    .ok_or_else(|| E::custom("non-finite number"))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(StrictValue(Value::Null))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(StrictValue(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut out = Vec::new();
                while let Some(v) = seq.next_element::<StrictValue>()? {
                    out.push(v.0);
                }
                Ok(StrictValue(Value::Array(out)))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut out = serde_json::Map::new();
                while let Some((k, v)) = map.next_entry::<String, StrictValue>()? {
                    if out.insert(k.clone(), v.0).is_some() {
                        return Err(de::Error::custom(format!("duplicate key: {k}")));
                    }
                }
                Ok(StrictValue(Value::Object(out)))
            }
        }
        d.deserialize_any(V)
    }
}

fn sorted(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .map(|(k, v)| (k, sorted(v)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.into_iter().map(sorted).collect()),
        other => other,
    }
}
pub fn canonical_json<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&sorted(serde_json::to_value(value)?))
}
pub fn digest<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    Ok(hex::encode(Sha256::digest(canonical_json(value)?)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn duplicate_keys_are_rejected() {
        assert!(parse_json(br#"{"a":{"x":1,"x":2}}"#).is_err());
    }
    #[test]
    fn canonical_order_is_recursive() {
        let a = parse_json(br#"{"z":{"b":2,"a":1},"a":0}"#).unwrap();
        assert_eq!(
            String::from_utf8(canonical_json(&a).unwrap()).unwrap(),
            r#"{"a":0,"z":{"a":1,"b":2}}"#
        );
    }
    #[test]
    fn response_checks_identity_and_dead_end_shape() {
        let mut r = Response::new(1, "state");
        assert!(validate_response(&r, 0).is_err());
        r.request_id = 0;
        r.terminal = true;
        r.enabled.push(Action::new("x", ActionKind::Workload));
        assert!(validate_response(&r, 0).is_err());
    }
}
