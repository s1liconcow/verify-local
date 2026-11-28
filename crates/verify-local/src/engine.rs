use crate::{
    frontier::{Branch, Frontier, Visits},
    model::{self, Evidence, Failure, Identity, Manifest, Trace},
    transport::Session,
    Error, ForkOptions, Limits, Result, SearchOptions,
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

enum Decisions<'a> {
    Tape(&'a [Action]),
    Explore {
        steps: usize,
        branch: Option<&'a Branch>,
        choice_seed: u64,
        visits: &'a mut Visits,
    },
}

fn choose(response: &Response, visits: &Visits, random: &mut Random) -> Action {
    let weights: Vec<f64> = response
        .enabled
        .iter()
        .map(|action| {
            let count = visits
                .get(&(response.state.clone(), action.id.clone()))
                .copied()
                .unwrap_or(0);
            1.0 / (1.0 + f64::from(count))
        })
        .collect();
    let mut threshold = random.fraction() * weights.iter().sum::<f64>();
    let index = weights
        .iter()
        .position(|weight| {
            threshold -= weight;
            threshold < 0.0
        })
        .unwrap_or(weights.len() - 1);
    response.enabled[index].clone()
}

async fn execute(
    execution: &Execution<'_>,
    seed: u64,
    mut decisions: Decisions<'_>,
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
        let (count, choice_seed, required) = match &decisions {
            Decisions::Tape(actions) => (actions.len(), seed, actions.len()),
            Decisions::Explore {
                steps,
                choice_seed,
                branch,
                ..
            } => (
                *steps,
                *choice_seed,
                branch.map_or(0, |b| b.prefix.len() + 1),
            ),
        };
        let mut random = Random(choice_seed);
        for index in 0..count {
            if trace.failure.is_some() || response.terminal || response.enabled.is_empty() {
                break;
            }
            let action = match &mut decisions {
                Decisions::Tape(tape) => tape[index].clone(),
                Decisions::Explore { branch, visits, .. } => {
                    let action = match branch {
                        Some(b) if index < b.prefix.len() => b.prefix[index].clone(),
                        Some(b) if index == b.prefix.len() => {
                            b.check_prefix(&trace)?;
                            b.next.clone()
                        }
                        _ => choose(&response, visits, &mut random),
                    };
                    if visits.len() < NOVELTY_LIMIT {
                        let count = visits
                            .entry((response.state.clone(), action.id.clone()))
                            .or_default();
                        *count = count.saturating_add(1);
                    }
                    action
                }
            };
            if !response.enabled.contains(&action) {
                return Err(Error::invalid(format!(
                    "action {index} is disabled in current state"
                )));
            }
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
        if trace.actions.len() < required {
            return Err(Error::invalid(
                "recorded decisions continue after failure, termination, or dead end",
            ));
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
#[derive(serde::Serialize)]
struct Finding {
    property: String,
    signature: String,
    occurrences: usize,
    artifact: PathBuf,
}

pub async fn run(
    manifest: &Manifest,
    out: &Path,
    runs: usize,
    steps: usize,
    seed: u64,
    search: &SearchOptions,
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
    let guided = search.guided || search.corpus_from.is_some();
    let mut frontier = match &search.corpus_from {
        Some(path) => Frontier::load(path, &identity)?,
        None => Frontier::default(),
    };
    let imported = frontier.len();
    let mut branches_run = 0;
    let out = output_dir(out)?;
    let mut evidence = Evidence::default();
    let mut visits = Visits::new();
    let mut records = Vec::<Value>::new();
    let mut artifacts = BTreeMap::<String, String>::new();
    let mut replayed_count = 0usize;
    let mut observed: Option<Trace> = None;
    let mut found = None;
    let mut findings = Vec::<Finding>::new();
    let result: Result<()> = async {
        for index in 0..runs {
            if Instant::now() >= deadline {
                return Err(Error::inconclusive(
                    "campaign wall-clock budget exhausted before requested runs",
                ));
            }
            let choice_seed = seed
                .checked_add(index as u64)
                .ok_or_else(|| Error::harness("seed overflow"))?;
            // Reserve alternating runs for new workloads even when the frontier is deep.
            let try_branch = if imported > 0 {
                index.is_multiple_of(2)
            } else {
                !index.is_multiple_of(2)
            };
            let branch = if guided && try_branch {
                frontier.candidate(steps).cloned()
            } else {
                None
            };
            let run_seed = branch.as_ref().map_or(choice_seed, |b| b.seed);
            let mut trace = execute(
                &execution,
                run_seed,
                Decisions::Explore {
                    steps,
                    branch: branch.as_ref(),
                    choice_seed,
                    visits: &mut visits,
                },
            )
            .await?;
            evidence.add(&trace)?;
            records.push(json!({
                "seed":run_seed, "actions":trace.actions.len(), "stop":trace.stop,
                "failure":compact_failure(&trace.failure), "choice_seed":choice_seed,
                "branch":branch.as_ref().map(|b| json!({"event":b.prefix.len(),"action":b.next.id}))
            }));
            observed = Some(trace.clone());
            let replay = execute(&execution, run_seed, Decisions::Tape(&trace.actions)).await?;
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
            if guided {
                branches_run += usize::from(branch.is_some());
                frontier.discard_visited(&visits, branch.as_ref());
                frontier.collect(&trace, &visits)?;
            }
            if let Some(failure) = &trace.failure {
                if let Some(existing) = findings
                    .iter_mut()
                    .find(|f| f.property == failure.property && f.signature == failure.signature)
                {
                    existing.occurrences += 1;
                } else {
                    let path = out.join(format!("failure-{:04}.json", findings.len() + 1));
                    model::write_artifact(&path, &trace)?;
                    if findings.is_empty() {
                        found = trace.failure.clone();
                        artifacts.insert("failure".into(), path.display().to_string());
                    }
                    findings.push(Finding {
                        property: failure.property.clone(),
                        signature: failure.signature.clone(),
                        occurrences: 1,
                        artifact: path,
                    });
                }
                observed = None;
                if findings.len() == search.max_findings as usize {
                    break;
                }
                continue;
            }
            if !artifacts.contains_key("sample") {
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
            if manifest.config.require_terminal && trace.stop != "terminal" {
                return Err(Error::inconclusive(
                    "step budget exhausted before required terminal state",
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
    if guided {
        let path = out.join("frontier.json");
        frontier.save(&path, &identity)?;
        artifacts.insert("frontier".into(), path.display().to_string());
    }
    let guidance = if guided {
        Some(
            json!({"imported":imported, "branches_replayed":branches_run,
        "pending":frontier.len(), "queue_capped":frontier.capped,"visited_capped":frontier.visited_capped()}),
        )
    } else {
        None
    };
    let elapsed_ms = started.elapsed().as_millis();
    let campaign = json!({
        "format":"verify-local-campaign","version":2,"name":manifest.config.name,
        "outcome":outcome,"reason":reason,"identity":identity,"boundary":manifest.config.boundary,
        "findings":findings, "bounds":{"runs":runs,"steps":steps,"seed":seed,"limits":limits,"max_findings":search.max_findings,
                  "require_terminal":manifest.config.require_terminal},
        "runs_completed":records.len(),"actions":evidence.actions,"replays_matched":replayed_count,
        "elapsed_ms":elapsed_ms,"properties":evidence.properties,"witnesses":evidence.witnesses,
        "missing_properties":missing_properties,"missing_witnesses":missing_witnesses,
        "unique_states":evidence.states.len(),"unique_transitions":visits.len(),
        "novelty_capped":evidence.states.len()>=NOVELTY_LIMIT||visits.len()>=NOVELTY_LIMIT,
        "search":if guided {"replayed frontier and fresh seeded frequency weighting"} else {"seeded state/action frequency weighting"},
        "guidance":guidance,"runs":records,"artifacts":artifacts,
        "isolation":"trusted host process; no filesystem, network or aggregate memory confinement",
        "coverage":"adapter-defined properties, witnesses and state features; no code coverage claim"
    });
    model::write_artifact(&out.join("campaign.json"), &campaign)?;
    Ok(
        json!({"outcome":outcome,"reason":reason,"runs":records.len(),"actions":evidence.actions,
        "replays":replayed_count,"elapsed_ms":elapsed_ms,"failure":compact_failure(&found),
        "missing_properties":short_list(&missing_properties),"missing_witnesses":short_list(&missing_witnesses),
        "distinct_findings":findings.len(), "campaign":out.join("campaign.json"),"artifacts":artifacts,"guidance":guidance}),
    )
}

pub fn doctor(manifest: &Manifest) -> Result<Value> {
    let identity = manifest.identity()?;
    Ok(
        json!({"outcome":if cfg!(unix){"READY"}else{"HARNESS_ERROR"},"operation":"configuration_only",
        "name":manifest.config.name,"identity":digest(&identity)?,
        "declared_files":identity.files.len(),"contract_files":identity.contract_files.len(),
        "properties":manifest.config.required_properties.len(),"witnesses":manifest.config.required_witnesses.len(),
        "require_terminal":manifest.config.require_terminal,
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
    let replay = execute(&execution, trace.seed, Decisions::Tape(&trace.actions)).await?;
    trace.compare(&replay)?;
    manifest.require_identity(&identity)?;
    let mut result = scenario_result(manifest, &replay)?;
    result["trace"] = json!(path);
    Ok(result)
}

fn scenario_result(manifest: &Manifest, replay: &Trace) -> Result<Value> {
    let mut evidence = Evidence::default();
    evidence.add(replay)?;
    let (missing_properties, missing_witnesses) = missing(manifest, &evidence);
    let outcome = if replay.failure.is_some() {
        "FOUND"
    } else if replay.stop == "no_enabled_actions"
        || (manifest.config.require_terminal && replay.stop != "terminal")
        || !missing_properties.is_empty()
        || !missing_witnesses.is_empty()
    {
        "INCONCLUSIVE"
    } else {
        "NO_FAILURE_WITHIN_BOUNDS"
    };
    Ok(json!({"outcome":outcome,
        "replay_match":true,"actions":replay.actions.len(),"failure":compact_failure(&replay.failure),
        "stop":replay.stop,"missing_properties":short_list(&missing_properties),
        "missing_witnesses":short_list(&missing_witnesses),
        "scope":"retained scenario only"}))
}

pub async fn fork(
    manifest: &Manifest,
    path: &Path,
    options: &ForkOptions,
    limits: &Limits,
) -> Result<Value> {
    let out = absolute_file(&options.out)?;
    let original = Trace::load(path)?;
    let identity = manifest.require_identity(&original.identity)?;
    let branch = Branch::at(&original, options.at, &options.action)?;
    let steps = options.at + 1 + options.steps as usize;
    if steps > 100_000 {
        return Err(Error::harness("fork exceeds 100000 total actions"));
    }
    let execution = Execution {
        manifest,
        identity: &identity,
        limits,
        deadline: Instant::now() + Duration::from_secs_f64(limits.seconds),
    };
    let mut trace = execute(
        &execution,
        original.seed,
        Decisions::Explore {
            steps,
            branch: Some(&branch),
            choice_seed: original.seed,
            visits: &mut Visits::new(),
        },
    )
    .await?;
    let repeated = execute(&execution, original.seed, Decisions::Tape(&trace.actions)).await?;
    trace.compare(&repeated)?;
    manifest.require_identity(&identity)?;
    trace.reproducible = Some(true);
    trace.provenance = Some(
        json!({"operation":"fork","source_trace_sha256":model::hash_file(path)?,
        "event":options.at, "action":options.action,"original_next":original.actions.get(options.at)}),
    );
    let mut result = scenario_result(manifest, &trace)?;
    model::write_artifact(&out, &trace)?;
    result["artifact"] = json!(out);
    result["operation"] = json!("fork");
    result["branch_event"] = json!(options.at);
    Ok(result)
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
        Decisions::Tape(&original.actions),
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
            let test = match execute(&execution, original.seed, Decisions::Tape(&actions)).await {
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
            let repeated =
                match execute(&execution, test.seed, Decisions::Tape(&test.actions)).await {
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
        Decisions::Tape(&original.actions),
    )
    .await?;
    let repeated = execute(
        &execution,
        original.seed,
        Decisions::Tape(&original.actions),
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
        || (manifest.config.require_terminal && candidate.stop != "terminal")
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
                "artifacts":value["artifacts"],"guidance":value["guidance"],
                "distinct_findings":value["findings"].as_array().map(Vec::len).unwrap_or_else(|| usize::from(value["artifacts"]["failure"].is_string())),
                "property_page":{"from":from,"limit":limit,"total":properties.len(),"items":selected},
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
                        "witnesses_omitted":r.witnesses.len().saturating_sub(8),"enabled_actions":r.enabled.len(),
                        "choices":r.enabled.iter().take(8).map(|a| &a.id).collect::<Vec<_>>(),
                        "choices_omitted":r.enabled.len().saturating_sub(8)})
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
