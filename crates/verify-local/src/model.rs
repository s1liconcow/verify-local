use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
};
use verify_protocol::{canonical_json, digest, parse_json, validate_id, Action, Response};

pub const ARTIFACT_LIMIT: usize = 32 * 1024 * 1024;
pub const EVIDENCE_CARDINALITY_LIMIT: usize = 100_000;
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Boundary {
    pub controlled: Vec<String>,
    #[serde(default)]
    pub uncontrolled: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u32,
    pub name: String,
    pub adapter: Vec<String>,
    pub identity_files: Vec<String>,
    #[serde(default)]
    pub contract_files: Vec<String>,
    pub required_properties: Vec<String>,
    #[serde(default)]
    pub required_witnesses: Vec<String>,
    #[serde(default)]
    pub require_terminal: bool,
    pub boundary: Boundary,
}
#[derive(Clone, Debug)]
pub struct Manifest {
    pub path: PathBuf,
    pub root: PathBuf,
    pub config: Config,
    pub argv: Vec<String>,
    pub environment: BTreeMap<String, String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Identity {
    pub manifest: String,
    pub files: BTreeMap<String, String>,
    pub contract: String,
    pub contract_files: BTreeMap<String, String>,
    pub adapter: Vec<String>,
    pub adapter_executable: String,
    pub runner: String,
    pub platform: Value,
    pub environment: BTreeMap<String, String>,
}
impl Manifest {
    pub fn load(path: &Path) -> Result<Self> {
        let path = fs::canonicalize(path)?;
        let root = path
            .parent()
            .ok_or_else(|| Error::harness("manifest has no parent"))?
            .to_path_buf();
        let config: Config = serde_json::from_value(read_value(&path, 1024 * 1024)?)?;
        if config.version != 1 {
            return Err(Error::harness("unsupported manifest version"));
        }
        validate_id(&config.name, "manifest name")?;
        validate_list(&config.identity_files, "identity_files", false)?;
        validate_list(&config.contract_files, "contract_files", true)?;
        validate_list(&config.required_properties, "required_properties", false)?;
        validate_list(&config.required_witnesses, "required_witnesses", true)?;
        validate_list(&config.boundary.controlled, "boundary.controlled", false)?;
        validate_list(&config.boundary.uncontrolled, "boundary.uncontrolled", true)?;
        for f in &config.identity_files {
            contained(&root, f)?;
        }
        for f in &config.contract_files {
            if !config.identity_files.contains(f) {
                return Err(Error::harness(
                    "contract_files must also be listed in identity_files",
                ));
            }
        }
        if config.adapter.is_empty()
            || config.adapter.len() > 128
            || config
                .adapter
                .iter()
                .any(|v| v.is_empty() || v.len() > 4096 || v.contains('\0'))
        {
            return Err(Error::harness(
                "adapter must be a nonempty bounded argv array",
            ));
        }
        let path_env = std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".into());
        let mut argv = config.adapter.clone();
        if argv[0] == "{python}" {
            argv[0] = "python3".into();
        }
        let executable = resolve_executable(&root, &argv[0], &path_env)?;
        argv[0] = executable.to_string_lossy().into_owned();
        let environment = [
            ("PATH".into(), path_env),
            ("LANG".into(), "C".into()),
            ("LC_ALL".into(), "C".into()),
            ("TZ".into(), "UTC".into()),
            ("PYTHONHASHSEED".into(), "0".into()),
            ("PYTHONDONTWRITEBYTECODE".into(), "1".into()),
        ]
        .into_iter()
        .collect();
        Ok(Self {
            path,
            root,
            config,
            argv,
            environment,
        })
    }
    pub fn identity(&self) -> Result<Identity> {
        let mut files = BTreeMap::new();
        for f in &self.config.identity_files {
            files.insert(f.clone(), hash_file(&contained(&self.root, f)?)?);
        }
        let contract_files = self
            .config
            .contract_files
            .iter()
            .map(|f| (f.clone(), files[f].clone()))
            .collect::<BTreeMap<_, _>>();
        let mut contract = json!({
            "properties":self.config.required_properties, "witnesses":self.config.required_witnesses,
            "boundary":self.config.boundary, "files":contract_files
        });
        if self.config.require_terminal {
            contract["require_terminal"] = json!(true);
        }
        let contract = digest(&contract)?;
        Ok(Identity {
            manifest: hash_file(&self.path)?,
            files,
            contract,
            contract_files,
            adapter: self.argv.clone(),
            adapter_executable: hash_file(Path::new(&self.argv[0]))?,
            runner: hash_file(&std::env::current_exe()?)?,
            platform: json!({"os":std::env::consts::OS,"arch":std::env::consts::ARCH,
                "kernel":fs::read_to_string("/proc/sys/kernel/osrelease").ok().map(|s|s.trim().to_owned())}),
            environment: self.environment.clone(),
        })
    }
    pub fn require_identity(&self, expected: &Identity) -> Result<Identity> {
        let actual = self.identity()?;
        if &actual != expected {
            return Err(Error::inconclusive("execution identity changed; exact replay requires original source, config, binary, runner and environment"));
        }
        Ok(actual)
    }
}
fn resolve_executable(root: &Path, value: &str, path_env: &str) -> Result<PathBuf> {
    let candidate = Path::new(value);
    if candidate.is_absolute() || candidate.components().count() > 1 {
        let p = fs::canonicalize(root.join(candidate))?;
        if p.is_file() {
            return Ok(p);
        }
    } else {
        for directory in std::env::split_paths(path_env) {
            let p = directory.join(candidate);
            if p.is_file() {
                return Ok(fs::canonicalize(p)?);
            }
        }
    }
    Err(Error::harness(format!(
        "adapter executable unavailable: {value}"
    )))
}
fn validate_list(items: &[String], label: &str, empty: bool) -> Result<()> {
    if items.len() > 1024 || (!empty && items.is_empty()) {
        return Err(Error::harness(format!("{label} has invalid length")));
    }
    let mut seen = BTreeSet::new();
    for item in items {
        validate_id(item, label)?;
        if !seen.insert(item) {
            return Err(Error::harness(format!("{label} contains duplicates")));
        }
    }
    Ok(())
}
pub fn contained(root: &Path, relative: &str) -> Result<PathBuf> {
    let relative = Path::new(relative);
    if relative.is_absolute()
        || relative
            .components()
            .any(|c| matches!(c, Component::ParentDir | Component::Prefix(_)))
    {
        return Err(Error::harness(
            "identity paths must be relative and contain no parent traversal",
        ));
    }
    let p = fs::canonicalize(root.join(relative))?;
    if !p.starts_with(root) || !p.is_file() {
        return Err(Error::harness(
            "identity path escapes manifest directory or is not a file",
        ));
    }
    Ok(p)
}
pub fn hash_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut sha = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        sha.update(&buffer[..n]);
    }
    Ok(hex::encode(sha.finalize()))
}
pub fn read_value(path: &Path, limit: usize) -> Result<Value> {
    let mut data = Vec::new();
    File::open(path)?
        .take(limit as u64 + 1)
        .read_to_end(&mut data)?;
    if data.len() > limit {
        return Err(Error::harness(format!(
            "JSON file exceeds {limit}-byte limit"
        )));
    }
    Ok(parse_json(&data)?)
}
pub fn write_artifact<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let mut bytes = canonical_json(value)?;
    bytes.push(b'\n');
    if bytes.len() > ARTIFACT_LIMIT {
        return Err(Error::inconclusive("artifact size budget exhausted"));
    }
    let parent = path
        .parent()
        .ok_or_else(|| Error::harness("artifact requires a parent directory"))?;
    let mut tmp = tempfile::NamedTempFile::new_in(parent)?;
    tmp.write_all(&bytes)?;
    tmp.as_file().sync_all()?;
    tmp.persist_noclobber(path)
        .map_err(|e| Error::harness(format!("artifact write refused: {}", e.error)))?;
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Failure {
    pub property: String,
    pub signature: String,
    pub step: usize,
    pub details: Option<Value>,
}
pub fn failure(response: &Response, step: usize) -> Option<Failure> {
    response.checks.iter().find(|c| !c.ok).map(|c| Failure {
        property: c.id.clone(),
        signature: c.signature.clone().unwrap_or_else(|| c.id.clone()),
        step,
        details: c.details.clone(),
    })
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Trace {
    pub format: String,
    pub version: u32,
    pub identity: Identity,
    pub boundary: Boundary,
    pub seed: u64,
    pub actions: Vec<Action>,
    /// Raw JSON preserves absent fields, nulls, and adapter extension fields.
    pub responses: Vec<Value>,
    pub failure: Option<Failure>,
    pub stop: String,
    pub reproducible: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reduction: Option<Value>,
}
impl Trace {
    pub fn new(m: &Manifest, identity: &Identity, seed: u64) -> Self {
        Self {
            format: "verify-local-trace".into(),
            version: 2,
            identity: identity.clone(),
            boundary: m.config.boundary.clone(),
            seed,
            actions: Vec::new(),
            responses: Vec::new(),
            failure: None,
            stop: String::new(),
            reproducible: None,
            provenance: None,
            reduction: None,
        }
    }
    pub fn load(path: &Path) -> Result<Self> {
        let trace: Self = serde_json::from_value(read_value(path, ARTIFACT_LIMIT)?)?;
        if trace.format != "verify-local-trace"
            || trace.version != 2
            || trace.actions.len() > 100_000
            || trace.responses.len() != trace.actions.len() + 1
        {
            return Err(Error::harness(
                "invalid trace format or action/response count",
            ));
        }
        for (i, raw) in trace.responses.iter().enumerate() {
            let response: Response = serde_json::from_value(raw.clone())?;
            verify_protocol::validate_response(&response, i as u64)?;
            if i < trace.actions.len() {
                if failure(&response, i).is_some()
                    || response.terminal
                    || !response.enabled.contains(&trace.actions[i])
                {
                    return Err(Error::harness(
                        "trace continues after failure/termination or contains a disabled action",
                    ));
                }
            } else if failure(&response, i) != trace.failure {
                return Err(Error::harness(
                    "trace failure summary disagrees with observations",
                ));
            }
        }
        Ok(trace)
    }
    pub fn compare(&self, other: &Self) -> Result<()> {
        if self.actions != other.actions {
            return Err(Error::divergence("action sequence diverged"));
        }
        if self.responses.len() != other.responses.len() {
            return Err(Error::divergence("response count diverged"));
        }
        for (i, (a, b)) in self.responses.iter().zip(&other.responses).enumerate() {
            if canonical_json(a)? != canonical_json(b)? {
                return Err(Error::divergence(format!(
                    "canonical response diverged at event {i}"
                )));
            }
        }
        if self.failure != other.failure {
            return Err(Error::divergence("failure signature diverged"));
        }
        Ok(())
    }
}
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct PropertyCount {
    pub evaluations: u64,
    pub failures: u64,
}
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct Evidence {
    pub properties: BTreeMap<String, PropertyCount>,
    pub witnesses: BTreeSet<String>,
    pub states: BTreeSet<String>,
    pub actions: u64,
}
impl Evidence {
    pub fn add(&mut self, trace: &Trace) -> Result<()> {
        self.actions += trace.actions.len() as u64;
        for raw in &trace.responses {
            let r: Response = serde_json::from_value(raw.clone())?;
            if self.states.len() < EVIDENCE_CARDINALITY_LIMIT {
                self.states.insert(r.state);
            }
            self.witnesses.extend(r.witnesses);
            for c in r.checks {
                let count = self.properties.entry(c.id).or_default();
                count.evaluations += 1;
                count.failures += u64::from(!c.ok);
            }
            if self.properties.len() > EVIDENCE_CARDINALITY_LIMIT
                || self.witnesses.len() > EVIDENCE_CARDINALITY_LIMIT
            {
                return Err(Error::inconclusive(
                    "global property/witness evidence limit exhausted",
                ));
            }
        }
        Ok(())
    }
}
