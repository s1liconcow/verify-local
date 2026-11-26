//! Black-box acceptance tests drive the Rust CLI through a Python adapter.
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::Instant,
};
use tempfile::TempDir;

const ADAPTER: &str = r#"import hashlib,json,os,sys,time
mode=sys.argv[1]
count=0
remembered=False
ever_applied=False
crashed_after_apply=False
witnesses=set()
for line in sys.stdin:
    q=json.loads(line)
    if mode=='hang': time.sleep(60)
    if q['op']=='reset':
        count=0;remembered=False;ever_applied=False;crashed_after_apply=False;witnesses=set()
    else:
        a=q['action']['id']
        if a=='apply':
            if crashed_after_apply: witnesses.add('retry_after_crash')
            if not remembered: count+=1
            remembered=True;ever_applied=True
        elif a=='crash':
            if ever_applied: crashed_after_apply=True
            if mode!='fixed': remembered=False
        elif a!='advance': raise ValueError('unknown action')
    state={'count':count,'remembered':remembered,'ever_applied':ever_applied,
           'crashed':crashed_after_apply,'witnesses':sorted(witnesses)}
    enabled=[{'id':'apply','kind':'workload'},{'id':'crash','kind':'fault'}]
    terminal=False
    if mode in ('terminal','entropy','malformed','missing','nested','too_deep','extra'):
        enabled=[];terminal=True
    if mode=='deadend': enabled=[]
    if mode=='finaldeadend':
        enabled=[{'id':'advance','kind':'schedule'}] if q['op']=='reset' else []
    checks=[{'id':'once','ok':count<=1,'signature':'duplicate_effect','details':{'count':count}}]
    if mode=='missing': checks=[]
    if mode=='omit_at_failure' and count>1: checks=[]
    if mode=='malformed': checks[0]['ok']='yes'
    response={'protocol':1,'request_id':q['request_id'],
              'state':hashlib.sha256(json.dumps(state,sort_keys=True).encode()).hexdigest(),
              'enabled':enabled,'checks':checks,'witnesses':sorted(witnesses),
              'terminal':terminal,'observation':state}
    if mode=='entropy':response['observation']['noise']=os.urandom(16).hex()
    if mode in ('nested','too_deep'):
        nested='leaf'
        for _ in range(60 if mode=='nested' else 70): nested={'nested':nested}
        response['observation']=nested
    if mode=='extra':
        sys.stdout.write(json.dumps(response)+'\n'+json.dumps(response)+'\n');sys.stdout.flush()
    else: print(json.dumps(response),flush=True)
"#;

struct Fixture {
    _temp: TempDir,
    root: PathBuf,
    manifest: PathBuf,
}
impl Fixture {
    fn new(mode: &str, witness: bool) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        fs::write(root.join("adapter.py"), ADAPTER).unwrap();
        fs::write(
            root.join("contract.json"),
            r#"{"property":"once","meaning":"one effect per logical request"}"#,
        )
        .unwrap();
        let manifest = root.join("verify.json");
        let fixture = Self {
            _temp: temp,
            root,
            manifest,
        };
        fixture.configure(mode, witness);
        fixture
    }
    fn configure(&self, mode: &str, witness: bool) {
        fs::write(
            &self.manifest,
            serde_json::to_vec(&json!({
                "version":1,"name":"python-idempotency",
                "adapter":["{python}","adapter.py",mode],
                "identity_files":["adapter.py","contract.json"],
                "contract_files":["contract.json"],
                "required_properties":["once"],
                "required_witnesses":if witness{vec!["retry_after_crash"]}else{vec![]},
                "boundary":{"controlled":["operations","crashes"],"uncontrolled":["OS threads"]}
            }))
            .unwrap(),
        )
        .unwrap();
    }
    fn run(&self, out: &str, runs: &str, steps: &str) -> (i32, Value) {
        cli(&[
            "run",
            self.manifest.to_str().unwrap(),
            "--out",
            self.root.join(out).to_str().unwrap(),
            "--runs",
            runs,
            "--steps",
            steps,
            "--seconds",
            "10",
            "--seed",
            "1",
        ])
    }
    fn trace(&self) -> PathBuf {
        self.root.join("run/failure-0001.json")
    }
}
fn cli(args: &[&str]) -> (i32, Value) {
    let output = Command::new(env!("CARGO_BIN_EXE_verify-local"))
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.stdout.len() <= 12 * 1024,
        "stdout exceeded context budget"
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "{e}: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output.status.code().unwrap(), value)
}
fn strpath(path: &Path) -> &str {
    path.to_str().unwrap()
}

#[test]
fn python_failure_replays_and_shrinks() {
    let f = Fixture::new("buggy", true);
    let (code, run) = f.run("run", "20", "40");
    assert_eq!(code, 1, "{run}");
    assert_eq!(run["outcome"], "FOUND");
    assert_eq!(run["runs"], run["replays"]);
    let (code, replay) = cli(&["replay", strpath(&f.manifest), strpath(&f.trace())]);
    assert_eq!(code, 1, "{replay}");
    assert_eq!(replay["replay_match"], true);
    let minimized = f.root.join("run/minimized.json");
    let (code, shrink) = cli(&[
        "shrink",
        strpath(&f.manifest),
        strpath(&f.trace()),
        "--out",
        strpath(&minimized),
    ]);
    assert_eq!(code, 1, "{shrink}");
    assert!(
        shrink["reduction"]["actions"].as_u64().unwrap() <= 3,
        "{shrink}"
    );
    let (code, replay) = cli(&["replay", strpath(&f.manifest), strpath(&minimized)]);
    assert_eq!(code, 1, "{replay}");
    let (_, report) = cli(&["report", strpath(&minimized), "--events", "--limit", "2"]);
    assert_eq!(report["events"]["items"].as_array().unwrap().len(), 2);
}
#[test]
fn fixed_component_has_required_witnesses() {
    let f = Fixture::new("fixed", true);
    let (code, result) = f.run("run", "10", "40");
    assert_eq!(code, 0, "{result}");
    assert_eq!(result["replays"], 10);
    assert_eq!(result["missing_properties"]["count"], 0);
    assert_eq!(result["missing_witnesses"]["count"], 0);
}
#[test]
fn terminal_policy_rejects_clean_truncated_histories() {
    for (mode, expected) in [("fixed", (2, "step_bound")), ("terminal", (0, "terminal"))] {
        let f = Fixture::new(mode, false);
        let mut config: Value = serde_json::from_slice(&fs::read(&f.manifest).unwrap()).unwrap();
        config["require_terminal"] = json!(true);
        fs::write(&f.manifest, serde_json::to_vec(&config).unwrap()).unwrap();
        let (code, run) = f.run("run", "1", "1");
        assert_eq!(code, expected.0, "{run}");
        assert_eq!(run["missing_properties"]["count"], 0);
        assert_eq!(run["replays"], 1);
        let (code, replay) = cli(&[
            "replay",
            strpath(&f.manifest),
            strpath(&f.root.join("run/sample-trace.json")),
        ]);
        assert_eq!(code, expected.0, "{replay}");
        assert_eq!(replay["stop"], expected.1);
        assert_eq!(replay["replay_match"], true);
    }
}
#[test]
fn cross_version_regression_preserves_contract() {
    let f = Fixture::new("buggy", true);
    let (code, result) = f.run("run", "10", "40");
    assert_eq!(code, 1, "{result}");
    f.configure("fixed", true);
    let (code, result) = cli(&["replay", strpath(&f.manifest), strpath(&f.trace())]);
    assert_eq!(code, 2, "{result}");
    let (code, result) = cli(&["regression", strpath(&f.manifest), strpath(&f.trace())]);
    assert_eq!(code, 0, "{result}");
    assert_eq!(result["candidate_replayed"], true);
    assert_eq!(result["exact_original_replay"], false);
    fs::write(f.root.join("contract.json"), "weakened property").unwrap();
    let (code, result) = cli(&["regression", strpath(&f.manifest), strpath(&f.trace())]);
    assert_eq!(code, 2, "{result}");
    assert!(result["reason"].as_str().unwrap().contains("contract"));
}
#[test]
fn regression_requires_property_at_original_failing_checkpoint() {
    let f = Fixture::new("buggy", true);
    let (code, result) = f.run("run", "10", "40");
    assert_eq!(code, 1, "{result}");
    f.configure("omit_at_failure", true);
    let (code, result) = cli(&["regression", strpath(&f.manifest), strpath(&f.trace())]);
    assert_eq!(code, 2, "{result}");
    assert_eq!(result["missing_evaluations"]["count"], 1);
}
#[test]
fn nested_json_round_trips_through_trace_envelopes() {
    let f = Fixture::new("nested", false);
    let (code, result) = f.run("run", "1", "1");
    assert_eq!(code, 0, "{result}");
    let path = f.root.join("run/sample-trace.json");
    let (code, result) = cli(&["replay", strpath(&f.manifest), strpath(&path)]);
    assert_eq!(code, 0, "{result}");
    let f = Fixture::new("too_deep", false);
    let (code, result) = f.run("run", "1", "1");
    assert_eq!(code, 3, "{result}");
}
#[test]
fn trailing_unsolicited_response_is_rejected() {
    let f = Fixture::new("extra", false);
    let (code, result) = f.run("run", "1", "1");
    assert_eq!(code, 3, "{result}");
    assert!(result["reason"].as_str().unwrap().contains("unsolicited"));
}
#[test]
fn missing_checks_and_witnesses_are_inconclusive() {
    let f = Fixture::new("missing", true);
    let (code, result) = f.run("run", "1", "1");
    assert_eq!(code, 2, "{result}");
    assert_eq!(result["missing_properties"]["count"], 1);
    assert_eq!(result["missing_witnesses"]["count"], 1);
}
#[test]
fn clean_hidden_entropy_is_detected() {
    let f = Fixture::new("entropy", false);
    let (code, result) = f.run("run", "1", "1");
    assert_eq!(code, 2, "{result}");
    assert!(f.root.join("run/divergence.json").is_file());
}
#[test]
fn final_step_dead_end_is_inconclusive() {
    let f = Fixture::new("finaldeadend", false);
    let (code, result) = f.run("run", "1", "1");
    assert_eq!(code, 2, "{result}");
    assert!(result["reason"].as_str().unwrap().contains("no enabled"));
    let (code, result) = cli(&[
        "replay",
        strpath(&f.manifest),
        strpath(&f.root.join("run/sample-trace.json")),
    ]);
    assert_eq!(code, 2, "{result}");
    assert_eq!(result["replay_match"], true);
}
#[test]
fn malformed_check_is_harness_error() {
    let f = Fixture::new("malformed", false);
    let (code, result) = f.run("run", "1", "1");
    assert_eq!(code, 3, "{result}");
}
#[test]
fn timeout_is_bounded_and_inconclusive() {
    let f = Fixture::new("hang", false);
    let start = Instant::now();
    let (code, result) = cli(&[
        "run",
        strpath(&f.manifest),
        "--out",
        strpath(&f.root.join("run")),
        "--runs",
        "1",
        "--steps",
        "1",
        "--timeout",
        "0.15",
        "--seconds",
        "10",
    ]);
    assert_eq!(code, 2, "{result}");
    assert!(
        result["reason"]
            .as_str()
            .unwrap()
            .contains("adapter request timeout"),
        "{result}"
    );
    // The request budget excludes process cleanup and final identity hashing.
    // Parallel debug tests add scheduler/hash contention; retain enough headroom
    // while still catching the adapter's otherwise unbounded 60-second sleep.
    assert!(start.elapsed().as_secs_f64() < 15.0, "{result}");
}
#[test]
fn tampering_and_source_drift_are_refused() {
    let f = Fixture::new("buggy", true);
    let (code, result) = f.run("run", "10", "40");
    assert_eq!(code, 1, "{result}");
    let mut trace: Value = serde_json::from_slice(&fs::read(f.trace()).unwrap()).unwrap();
    trace["failure"]["property"] = json!("different");
    let tampered = f.root.join("tampered.json");
    fs::write(&tampered, serde_json::to_vec(&trace).unwrap()).unwrap();
    let (code, result) = cli(&["replay", strpath(&f.manifest), strpath(&tampered)]);
    assert_eq!(code, 3, "{result}");
    fs::write(
        f.root.join("adapter.py"),
        format!("{ADAPTER}\n# changed source\n"),
    )
    .unwrap();
    let (code, result) = cli(&["replay", strpath(&f.manifest), strpath(&f.trace())]);
    assert_eq!(code, 2, "{result}");
}
#[test]
fn existing_evidence_and_escaping_identity_are_refused() {
    let f = Fixture::new("fixed", false);
    let existing = f.root.join("run");
    fs::create_dir(&existing).unwrap();
    fs::write(existing.join("sentinel"), "keep").unwrap();
    let (code, result) = f.run("run", "1", "1");
    assert_eq!(code, 3, "{result}");
    assert_eq!(
        fs::read_to_string(existing.join("sentinel")).unwrap(),
        "keep"
    );
    let mut config: Value = serde_json::from_slice(&fs::read(&f.manifest).unwrap()).unwrap();
    config["identity_files"] = json!(["../outside.py"]);
    fs::write(&f.manifest, serde_json::to_vec(&config).unwrap()).unwrap();
    let (code, result) = cli(&["doctor", strpath(&f.manifest)]);
    assert_eq!(code, 3, "{result}");
}
