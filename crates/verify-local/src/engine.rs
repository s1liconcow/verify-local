use crate::{
    model::{self, Evidence, Failure, Identity, Manifest, Trace},
    transport::Session,
    Error, Limits, Result,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};
use tokio::time::{Duration, Instant};
use verify_protocol::{canonical_json, digest, Action, Request, Response};

const NOVELTY_LIMIT: usize = 100_000;
type Visits = BTreeMap<(String, String), u32>;
struct Random(u64);
impl Random {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
    fn fraction(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[derive(Clone, Copy)]
struct Execution<'a> {
    manifest: &'a Manifest,
    identity: &'a Identity,
    limits: &'a Limits,
    deadline: Instant,
}

async fn execute(
    execution: &Execution<'_>,
    seed: u64,
    steps: usize,
    tape: Option<&[Action]>,
    mut visits: Option<&mut Visits>,
) -> Result<Trace> {
    let Execution {
        manifest,
        identity,
        limits,
        deadline,
    } = *execution;
    let mut session = Session::start(manifest, limits, deadline)?;
    let mut trace = Trace::new(manifest, identity, seed);
    let result = async {
        let (mut response, raw) = session.request(&Request::reset(seed)).await?;
        let mut evidence_bytes = canonical_json(&raw)?.len();
        trace.responses.push(raw);
        trace.failure = model::failure(&response, 0);
        let mut random = Random(seed);
        let count = tape.map_or(steps, |a| a.len());
        for index in 0..count {
            if trace.failure.is_some() {
                trace.stop = "property_violation".into();
                break;
            }
            if response.terminal {
                if tape.is_some() {
                    return Err(Error::invalid("action tape continues after terminal state"));
                }
                trace.stop = "terminal".into();
                break;
            }
            if response.enabled.is_empty() {
                if tape.is_some() {
                    return Err(Error::invalid("no enabled action at replay step"));
                }
                trace.stop = "no_enabled_actions".into();
                break;
            }
            let action = if let Some(tape) = tape {
                let action = &tape[index];
                if !response.enabled.contains(action) {
                    return Err(Error::invalid(format!(
                        "action {index} is disabled in current state"
                    )));
                }
                action.clone()
            } else {
                let weights: Vec<f64> = response
                    .enabled
                    .iter()
                    .map(|a| {
                        let n = visits
                            .as_ref()
                            .and_then(|v| v.get(&(response.state.clone(), a.id.clone())))
                            .copied()
                            .unwrap_or(0);
                        1.0 / (1.0 + f64::from(n))
                    })
                    .collect();
                let mut threshold = random.fraction() * weights.iter().sum::<f64>();
                let mut selected = weights.len() - 1;
                for (i, w) in weights.iter().enumerate() {
                    threshold -= w;
                    if threshold < 0.0 {
                        selected = i;
                        break;
                    }
                }
                let action = response.enabled[selected].clone();
                if let Some(v) = visits.as_mut() {
                    let key = (response.state.clone(), action.id.clone());
                    if v.len() < NOVELTY_LIMIT {
                        *v.entry(key).or_default() += 1;
                    }
                }
                action
            };
            evidence_bytes = evidence_bytes.saturating_add(canonical_json(&action)?.len());
            trace.actions.push(action.clone());
            let (next, raw) = session
                .request(&Request::step((index + 1) as u64, action))
                .await?;
            evidence_bytes = evidence_bytes.saturating_add(canonical_json(&raw)?.len());
            if evidence_bytes > model::ARTIFACT_LIMIT / 2 {
                return Err(Error::inconclusive("trace evidence budget exhausted"));
            }
            response = next;
            trace.responses.push(raw);
            trace.failure = model::failure(&response, index + 1);
        }
        trace.stop = if trace.failure.is_some() {
            "property_violation"
        } else if response.terminal {
            "terminal"
        } else if response.enabled.is_empty() {
            "no_enabled_actions"
        } else {
            "step_bound"
        }
        .into();
        if let Some(tape) = tape {
            if tape.len() != trace.actions.len() {
                return Err(Error::invalid(
                    "tape continues after first property violation",
                ));
            }
        }
        Ok(trace)
    }
    .await;
    let closed = session.close().await;
    match result {
        Ok(trace) => closed.map(|()| trace),
        Err(error) => Err(error),
    }
}

fn output_dir(path: &Path) -> Result<PathBuf> {
    let path = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let parent = path
        .parent()
        .ok_or_else(|| Error::harness("output requires a parent"))?;
    fs::create_dir_all(parent)?;
    fs::create_dir(&path)
        .map_err(|e| Error::harness(format!("new output directory required: {e}")))?;
    Ok(fs::canonicalize(path)?)
}
pub fn absolute_file(path: &Path) -> Result<PathBuf> {
    let path = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let parent = path
        .parent()
        .ok_or_else(|| Error::harness("output requires a parent"))?;
    if !parent.is_dir() {
        return Err(Error::harness("artifact parent directory does not exist"));
    }
    if path.exists() {
        return Err(Error::harness("artifact output already exists"));
    }
    Ok(fs::canonicalize(parent)?.join(
        path.file_name()
            .ok_or_else(|| Error::harness("missing output filename"))?,
    ))
}
fn missing(manifest: &Manifest, evidence: &Evidence) -> (Vec<String>, Vec<String>) {
    (
        manifest
            .config
            .required_properties
            .iter()
            .filter(|p| !evidence.properties.contains_key(*p))
            .cloned()
            .collect(),
        manifest
            .config
            .required_witnesses
            .iter()
            .filter(|w| !evidence.witnesses.contains(*w))
            .cloned()
            .collect(),
    )
}
fn short_list(items: &[String]) -> Value {
    json!({"count":items.len(),"items":items.iter().take(12).collect::<Vec<_>>(),"omitted":items.len().saturating_sub(12)})
}
fn compact_failure(failure: &Option<Failure>) -> Value {
    failure
        .as_ref()
        .map(|f| json!({"property":f.property,"signature":f.signature,"step":f.step}))
        .unwrap_or(Value::Null)
}
pub async fn run(
    manifest: &Manifest,
    out: &Path,
    runs: usize,
    steps: usize,
    seed: u64,
    limits: &Limits,
) -> Result<Value> {
    let started = Instant::now();
    let deadline = started + Duration::from_secs_f64(limits.seconds);
    let identity = manifest.identity()?;
    let execution = Execution {
        manifest,
        identity: &identity,
        limits,
        deadline,
    };
    let out = output_dir(out)?;
    let mut evidence = Evidence::default();
    let mut visits = Visits::new();
    let mut records = Vec::<Value>::new();
    let mut artifacts = BTreeMap::<String, String>::new();
    let mut replayed_count = 0usize;
    let mut observed: Option<Trace> = None;
    let mut found = None;
    let result: Result<()> = async {
        for index in 0..runs {
            if Instant::now() >= deadline {
                return Err(Error::inconclusive(
                    "campaign wall-clock budget exhausted before requested runs",
                ));
            }
            let run_seed = seed
                .checked_add(index as u64)
                .ok_or_else(|| Error::harness("seed overflow"))?;
            let mut trace = execute(&execution, run_seed, steps, None, Some(&mut visits)).await?;
            evidence.add(&trace)?;
            records.push(json!({
                "seed":run_seed, "actions":trace.actions.len(), "stop":trace.stop,
                "failure":compact_failure(&trace.failure)
            }));
            observed = Some(trace.clone());
            let replay = execute(&execution, run_seed, steps, Some(&trace.actions), None).await?;
            if let Err(error) = trace.compare(&replay) {
                trace.reproducible = Some(false);
                model::write_artifact(
                    &out.join("divergence.json"),
                    &json!({
                        "format":"verify-local-divergence", "original":trace,
                        "replay":replay, "reason":error.to_string()
                    }),
                )?;
                artifacts.insert(
                    "divergence".into(),
                    out.join("divergence.json").display().to_string(),
                );
                return Err(error);
            }
            replayed_count += 1;
            trace.reproducible = Some(true);
            if trace.failure.is_some() {
                model::write_artifact(&out.join("failure-0001.json"), &trace)?;
                artifacts.insert(
                    "failure".into(),
                    out.join("failure-0001.json").display().to_string(),
                );
                found = trace.failure.clone();
                observed = None;
                break;
            }
            if index == 0 {
                model::write_artifact(&out.join("sample-trace.json"), &trace)?;
                artifacts.insert(
                    "sample".into(),
                    out.join("sample-trace.json").display().to_string(),
                );
            }
            if trace.stop == "no_enabled_actions" {
                return Err(Error::inconclusive(
                    "nonterminal state has no enabled actions",
                ));
            }
            observed = None;
        }
        Ok(())
    }
    .await;
    let (mut outcome, mut reason) = match result {
        Ok(()) if found.is_some() => (
            "FOUND".to_owned(),
            "property violation reproduced with identical observations".to_owned(),
        ),
        Ok(()) => (
            "NO_FAILURE_WITHIN_BOUNDS".into(),
            "all requested runs completed and replayed".into(),
        ),
        Err(error) => (error.outcome().into(), error.to_string()),
    };
    if let Some(trace) = observed.filter(|t| t.failure.is_some()) {
        if !artifacts.contains_key("failure") {
            match model::write_artifact(&out.join("unconfirmed-failure.json"), &trace) {
                Ok(()) => {
                    artifacts.insert(
                        "unconfirmed_failure".into(),
                        out.join("unconfirmed-failure.json").display().to_string(),
                    );
                }
                Err(error) => {
                    reason.push_str(&format!(
                        "; failure evidence could not be retained: {error}"
                    ));
                }
            }
        }
    }
    let (missing_properties, missing_witnesses) = missing(manifest, &evidence);
    if outcome == "NO_FAILURE_WITHIN_BOUNDS"
        && (!missing_properties.is_empty() || !missing_witnesses.is_empty())
    {
        outcome = "INCONCLUSIVE".into();
        reason = "required properties or scenario witnesses were never observed".into();
    }
    if let Err(error) = manifest.require_identity(&identity) {
        outcome = "INCONCLUSIVE".into();
        reason = error.to_string();
    }
    let elapsed_ms = started.elapsed().as_millis();
    let campaign = json!({
        "format":"verify-local-campaign","version":2,"name":manifest.config.name,
        "outcome":outcome,"reason":reason,"identity":identity,"boundary":manifest.config.boundary,
        "bounds":{"runs":runs,"steps":steps,"seed":seed,"limits":limits},
        "runs_completed":records.len(),"actions":evidence.actions,"replays_matched":replayed_count,
        "elapsed_ms":elapsed_ms,"properties":evidence.properties,"witnesses":evidence.witnesses,
        "missing_properties":missing_properties,"missing_witnesses":missing_witnesses,
        "unique_states":evidence.states.len(),"unique_transitions":visits.len(),
        "novelty_capped":evidence.states.len()>=NOVELTY_LIMIT||visits.len()>=NOVELTY_LIMIT,
        "search":"seeded state/action frequency weighting","runs":records,"artifacts":artifacts,
        "isolation":"trusted host process; no filesystem, network or aggregate memory confinement",
        "coverage":"adapter-defined properties, witnesses and state features; no code coverage claim"
    });
    model::write_artifact(&out.join("campaign.json"), &campaign)?;
    Ok(
        json!({"outcome":outcome,"reason":reason,"runs":records.len(),"actions":evidence.actions,
        "replays":replayed_count,"elapsed_ms":elapsed_ms,"failure":compact_failure(&found),
        "missing_properties":short_list(&missing_properties),"missing_witnesses":short_list(&missing_witnesses),
        "campaign":out.join("campaign.json"),"artifacts":artifacts}),
    )
}

pub fn doctor(manifest: &Manifest) -> Result<Value> {
    let identity = manifest.identity()?;
    Ok(
        json!({"outcome":if cfg!(unix){"READY"}else{"HARNESS_ERROR"},"operation":"configuration_only",
        "name":manifest.config.name,"identity":digest(&identity)?,
        "declared_files":identity.files.len(),"contract_files":identity.contract_files.len(),
        "properties":manifest.config.required_properties.len(),"witnesses":manifest.config.required_witnesses.len(),
        "transport":if cfg!(unix){"posix_process_group"}else{"unsupported"},
        "isolation":"trusted host process",
        "next":"run a bounded campaign; doctor does not execute or validate adapter determinism"}),
    )
}
pub async fn replay(manifest: &Manifest, path: &Path, limits: &Limits) -> Result<Value> {
    let trace = Trace::load(path)?;
    let identity = manifest.require_identity(&trace.identity)?;
    let deadline = Instant::now() + Duration::from_secs_f64(limits.seconds);
    let execution = Execution {
        manifest,
        identity: &identity,
        limits,
        deadline,
    };
    let replay = execute(
        &execution,
        trace.seed,
        trace.actions.len(),
        Some(&trace.actions),
        None,
    )
    .await?;
    trace.compare(&replay)?;
    manifest.require_identity(&identity)?;
    let mut evidence = Evidence::default();
    evidence.add(&replay)?;
    let (missing_properties, missing_witnesses) = missing(manifest, &evidence);
    let outcome = if replay.failure.is_some() {
        "FOUND"
    } else if replay.stop == "no_enabled_actions"
        || !missing_properties.is_empty()
        || !missing_witnesses.is_empty()
    {
        "INCONCLUSIVE"
    } else {
        "NO_FAILURE_WITHIN_BOUNDS"
    };
    Ok(json!({"outcome":outcome,
        "replay_match":true,"actions":trace.actions.len(),"failure":compact_failure(&trace.failure),
        "stop":replay.stop,"missing_properties":short_list(&missing_properties),
        "missing_witnesses":short_list(&missing_witnesses),
        "scope":"exact recorded trace only; no new state-space exploration","trace":path}))
}

pub async fn shrink(
    manifest: &Manifest,
    path: &Path,
    out: &Path,
    attempt_budget: usize,
    limits: &Limits,
) -> Result<Value> {
    let out = absolute_file(out)?;
    let original = Trace::load(path)?;
    let identity = manifest.require_identity(&original.identity)?;
    let target = original
        .failure
        .as_ref()
        .ok_or_else(|| Error::harness("shrinking requires a property failure"))?;
    let deadline = Instant::now() + Duration::from_secs_f64(limits.seconds);
    let execution = Execution {
        manifest,
        identity: &identity,
        limits,
        deadline,
    };
    let baseline = execute(
        &execution,
        original.seed,
        original.actions.len(),
        Some(&original.actions),
        None,
    )
    .await?;
    original.compare(&baseline)?;
    let mut best = baseline;
    best.reproducible = Some(true);
    let mut attempts = 0usize;
    let mut invalid = 0usize;
    let mut exhausted = false;
    let mut diverged = false;
    let mut interruption = None;
    let mut granularity = 2usize;
    let mut try_empty = true;
    'reduce: loop {
        if !try_empty && best.actions.is_empty() {
            break;
        }
        let was_empty_pass = try_empty;
        try_empty = false;
        if !was_empty_pass && best.actions.len() == 1 {
            break;
        }
        let length = best.actions.len();
        let chunk = if was_empty_pass {
            length.max(1)
        } else {
            length.div_ceil(granularity)
        };
        let candidates = if was_empty_pass {
            1
        } else {
            length.div_ceil(chunk)
        };
        let mut reduced = false;
        for candidate in 0..candidates {
            if attempts >= attempt_budget || Instant::now() >= deadline {
                exhausted = true;
                break 'reduce;
            }
            attempts += 1;
            // Allocate only the candidate being tried, after its budget check.
            // Eagerly cloning all complements makes long traces quadratic in RAM.
            let start = candidate * chunk;
            let actions: Vec<Action> = if was_empty_pass {
                Vec::new()
            } else {
                best.actions[..start]
                    .iter()
                    .chain(best.actions[(start + chunk).min(length)..].iter())
                    .cloned()
                    .collect()
            };
            let test = match execute(
                &execution,
                original.seed,
                actions.len(),
                Some(&actions),
                None,
            )
            .await
            {
                Ok(test) => test,
                Err(Error::Invalid(_)) => {
                    invalid += 1;
                    continue;
                }
                Err(Error::Inconclusive(reason)) => {
                    exhausted = true;
                    interruption = Some(reason);
                    break 'reduce;
                }
                Err(error) => return Err(error),
            };
            let matches = test
                .failure
                .as_ref()
                .is_some_and(|f| f.property == target.property && f.signature == target.signature);
            if !matches {
                continue;
            }
            let repeated = match execute(
                &execution,
                test.seed,
                test.actions.len(),
                Some(&test.actions),
                None,
            )
            .await
            {
                Ok(repeated) => repeated,
                Err(Error::Harness(reason)) => return Err(Error::harness(reason)),
                Err(error) => {
                    exhausted = true;
                    diverged = matches!(error, Error::Invalid(_) | Error::Divergence(_));
                    interruption = Some(error.to_string());
                    break 'reduce;
                }
            };
            if let Err(error) = test.compare(&repeated) {
                exhausted = true;
                diverged = true;
                interruption = Some(error.to_string());
                break 'reduce;
            }
            best = test;
            best.reproducible = Some(true);
            granularity = 2;
            reduced = true;
            break;
        }
        if was_empty_pass && !reduced {
            continue;
        }
        if !reduced {
            if granularity >= best.actions.len() {
                break;
            }
            granularity = (granularity * 2).min(best.actions.len());
        }
    }
    manifest.require_identity(&identity)?;
    let reduction = json!({"original_actions":original.actions.len(),"actions":best.actions.len(),
        "attempts":attempts,"invalid_candidates":invalid,"budget_exhausted":exhausted,
        "candidate_diverged":diverged,"interruption":interruption,
        "minimality":if exhausted{"unproven"}else{"one-action-deletion-minimal"}});
    best.provenance = Some(
        json!({"source_trace_sha256":model::hash_file(path)?,"operation":"valid_action_deletion"}),
    );
    best.reduction = Some(reduction.clone());
    model::write_artifact(&out, &best)?;
    Ok(
        json!({"outcome":if diverged{"INCONCLUSIVE"}else{"FOUND"},"artifact":out,
        "failure":compact_failure(&best.failure),"reduction":reduction}),
    )
}

pub async fn regression(
    manifest: &Manifest,
    path: &Path,
    out: Option<&Path>,
    limits: &Limits,
) -> Result<Value> {
    let original = Trace::load(path)?;
    let target = original
        .failure
        .as_ref()
        .ok_or_else(|| Error::harness("regression requires an original property failure"))?;
    let identity = manifest.identity()?;
    if identity.contract_files.is_empty() || original.identity.contract_files.is_empty() {
        return Err(Error::inconclusive(
            "regression requires nonempty contract_files pinned in both manifests",
        ));
    }
    if identity.contract != original.identity.contract {
        return Err(Error::inconclusive(
            "property contract identity changed; cross-version regression refused",
        ));
    }
    let deadline = Instant::now() + Duration::from_secs_f64(limits.seconds);
    let execution = Execution {
        manifest,
        identity: &identity,
        limits,
        deadline,
    };
    let mut candidate = execute(
        &execution,
        original.seed,
        original.actions.len(),
        Some(&original.actions),
        None,
    )
    .await?;
    let repeated = execute(
        &execution,
        original.seed,
        original.actions.len(),
        Some(&original.actions),
        None,
    )
    .await?;
    candidate.compare(&repeated)?;
    candidate.reproducible = Some(true);
    manifest.require_identity(&identity)?;
    let mut before = Evidence::default();
    before.add(&original)?;
    let mut after = Evidence::default();
    after.add(&candidate)?;
    let missing_properties: Vec<_> = before
        .properties
        .keys()
        .filter(|p| !after.properties.contains_key(*p))
        .cloned()
        .collect();
    let missing_witnesses: Vec<_> = before
        .witnesses
        .difference(&after.witnesses)
        .cloned()
        .collect();
    // An initial successful check cannot stand in for the original failing
    // checkpoint. Preserve every original per-event evaluation opportunity.
    let mut missing_evaluation_count = 0usize;
    let mut missing_evaluations = Vec::new();
    for (event, (before, after)) in original
        .responses
        .iter()
        .zip(&candidate.responses)
        .enumerate()
    {
        let before: Response = serde_json::from_value(before.clone())?;
        let after: Response = serde_json::from_value(after.clone())?;
        for check in before.checks {
            if !after
                .checks
                .iter()
                .any(|candidate| candidate.id == check.id)
            {
                missing_evaluation_count += 1;
                if missing_evaluations.len() < 12 {
                    missing_evaluations.push(json!({"event":event,"property":check.id}));
                }
            }
        }
    }
    let outcome = if candidate.failure.is_some() {
        "FOUND"
    } else if !missing_properties.is_empty()
        || !missing_witnesses.is_empty()
        || missing_evaluation_count > 0
        || !after.properties.contains_key(&target.property)
    {
        "INCONCLUSIVE"
    } else {
        "NO_FAILURE_WITHIN_BOUNDS"
    };
    candidate.provenance = Some(json!({"source_trace_sha256":model::hash_file(path)?,
        "operation":"cross_version_regression","original_identity":digest(&original.identity)?,
        "contract":identity.contract,"original_property":target.property}));
    if let Some(out) = out {
        model::write_artifact(&absolute_file(out)?, &candidate)?;
    }
    Ok(
        json!({"outcome":outcome,"operation":"cross_version_regression","exact_original_replay":false,
        "candidate_replayed":true,"original_property":target.property,"failure":compact_failure(&candidate.failure),
        "actions":candidate.actions.len(),"missing_properties":short_list(&missing_properties),
        "missing_witnesses":short_list(&missing_witnesses),"artifact":out,
        "missing_evaluations":{"count":missing_evaluation_count,"items":missing_evaluations,
            "omitted":missing_evaluation_count.saturating_sub(12)},
        "scope":"retained scenario against candidate build under unchanged declared property contract"}),
    )
}

pub fn report(path: &Path, events: bool, from: usize, limit: usize) -> Result<Value> {
    let value = model::read_value(path, model::ARTIFACT_LIMIT)?;
    match value.get("format").and_then(Value::as_str) {
        Some("verify-local-campaign") => {
            let properties = value
                .get("properties")
                .and_then(Value::as_object)
                .ok_or_else(|| Error::harness("invalid campaign properties"))?;
            let selected = properties
                .iter()
                .skip(from)
                .take(limit)
                .map(|(k, v)| json!({"id":k,"counts":v}))
                .collect::<Vec<_>>();
            Ok(
                json!({"outcome":value["outcome"],"reason":value["reason"],"runs":value["runs_completed"],
                "actions":value["actions"],"replays":value["replays_matched"],"elapsed_ms":value["elapsed_ms"],
                "artifacts":value["artifacts"],"property_page":{"from":from,"limit":limit,"total":properties.len(),"items":selected},
                "missing_properties":short_list(&serde_json::from_value::<Vec<String>>(value["missing_properties"].clone())?),
                "missing_witnesses":short_list(&serde_json::from_value::<Vec<String>>(value["missing_witnesses"].clone())?)}),
            )
        }
        Some("verify-local-trace") => {
            let trace = Trace::load(path)?;
            let page = if events {
                trace.responses.iter().enumerate().skip(from).take(limit).map(|(i,v)|{
                    let r:Response=serde_json::from_value(v.clone()).expect("validated response");
                    json!({"event":i,"action":if i==0{Value::Null}else{json!({"id":trace.actions[i-1].id,"kind":trace.actions[i-1].kind})},
                        "state":r.state,"checks":r.checks.iter().take(8).map(|c|json!({"id":c.id,"ok":c.ok,"signature":c.signature})).collect::<Vec<_>>(),
                        "checks_omitted":r.checks.len().saturating_sub(8),
                        "witnesses":r.witnesses.iter().take(8).collect::<Vec<_>>(),
                        "witnesses_omitted":r.witnesses.len().saturating_sub(8),"enabled_actions":r.enabled.len()})
                }).collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            Ok(
                json!({"operation":"artifact_inspection","actions":trace.actions.len(),"failure":compact_failure(&trace.failure),
                "reproducible":trace.reproducible,"stop":trace.stop,"reduction":trace.reduction,
                "events":{"from":from,"limit":limit,"total":trace.responses.len(),"items":page},
                "hint":"use report --events --from N --limit 8 for bounded event summaries; raw observations remain in artifact"}),
            )
        }
        _ => Err(Error::harness(
            "report requires a campaign or trace artifact",
        )),
    }
}
