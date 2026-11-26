//! A small control plane: compact summaries on stdout, complete evidence on disk.
mod engine;
mod model;
mod transport;

use clap::{Args, Parser, Subcommand};
use serde::Serialize;
use serde_json::{json, Value};
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Harness(String),
    #[error("{0}")]
    Inconclusive(String),
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Divergence(String),
}
pub type Result<T> = std::result::Result<T, Error>;
impl Error {
    pub fn harness(v: impl Into<String>) -> Self {
        Self::Harness(v.into())
    }
    pub fn inconclusive(v: impl Into<String>) -> Self {
        Self::Inconclusive(v.into())
    }
    pub fn invalid(v: impl Into<String>) -> Self {
        Self::Invalid(v.into())
    }
    pub fn divergence(v: impl Into<String>) -> Self {
        Self::Divergence(v.into())
    }
    pub fn outcome(&self) -> &'static str {
        match self {
            Self::Harness(_) => "HARNESS_ERROR",
            _ => "INCONCLUSIVE",
        }
    }
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::harness(e.to_string())
    }
}
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::harness(e.to_string())
    }
}
impl From<verify_protocol::ProtocolError> for Error {
    fn from(e: verify_protocol::ProtocolError) -> Self {
        Self::harness(e.to_string())
    }
}

#[derive(Args, Clone, Debug, Serialize)]
pub struct Limits {
    #[arg(long,default_value_t=30.0,value_parser=seconds)]
    pub seconds: f64,
    #[arg(long,default_value_t=2.0,value_parser=seconds)]
    pub timeout: f64,
    #[arg(long,default_value_t=262_144,value_parser=byte_limit)]
    pub max_response_bytes: usize,
    #[arg(long,default_value_t=4_194_304,value_parser=byte_limit)]
    pub max_output_bytes: usize,
}
fn seconds(v: &str) -> std::result::Result<f64, String> {
    let x = v.parse::<f64>().map_err(|_| "expected seconds")?;
    if !x.is_finite() || x <= 0.0 || x > 86400.0 {
        return Err("seconds must be finite and in (0, 86400]".into());
    }
    Ok(x)
}
fn positive(v: &str) -> std::result::Result<usize, String> {
    let x = v
        .parse::<usize>()
        .map_err(|_| "expected positive integer")?;
    if !(1..=100_000).contains(&x) {
        return Err("must be between 1 and 100000".into());
    }
    Ok(x)
}
fn byte_limit(v: &str) -> std::result::Result<usize, String> {
    let x = v.parse::<usize>().map_err(|_| "expected byte limit")?;
    if !(1024..=33_554_432).contains(&x) {
        return Err("byte limit must be between 1024 and 33554432".into());
    }
    Ok(x)
}
fn page_limit(v: &str) -> std::result::Result<usize, String> {
    let x = v.parse::<usize>().map_err(|_| "expected page size")?;
    if !(1..=32).contains(&x) {
        return Err("page size must be between 1 and 32".into());
    }
    Ok(x)
}

#[derive(Parser)]
#[command(
    name = "verify-local",
    version,
    about = "Bounded language-neutral verification with compact agent-facing output"
)]
struct Cli {
    /// Output is always compact JSON; retained for explicit machine-client invocation.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Operation,
}
#[derive(Subcommand)]
enum Operation {
    /// Validate declared configuration and input identity without running code.
    Doctor { manifest: PathBuf },
    /// Explore controlled executions and replay every completed trace.
    Run {
        manifest: PathBuf,
        #[arg(long)]
        out: PathBuf,
        #[arg(long,default_value_t=100,value_parser=positive)]
        runs: usize,
        #[arg(long,default_value_t=80,value_parser=positive)]
        steps: usize,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[command(flatten)]
        limits: Limits,
    },
    /// Replay under exactly the original declared build, runtime, and environment.
    Replay {
        manifest: PathBuf,
        trace: PathBuf,
        #[command(flatten)]
        limits: Limits,
    },
    /// Delete actions while preserving the valid, reproducible failure signature.
    Shrink {
        manifest: PathBuf,
        trace: PathBuf,
        #[arg(long)]
        out: PathBuf,
        #[arg(long,default_value_t=200,value_parser=positive)]
        attempts: usize,
        #[command(flatten)]
        limits: Limits,
    },
    /// Apply an original failure scenario to a new build with unchanged pinned contract.
    Regression {
        manifest: PathBuf,
        trace: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
        #[command(flatten)]
        limits: Limits,
    },
    /// Read bounded evidence summaries without loading full traces into agent context.
    Report {
        artifact: PathBuf,
        #[arg(long)]
        events: bool,
        #[arg(long, default_value_t = 0)]
        from: usize,
        #[arg(long,default_value_t=8,value_parser=page_limit)]
        limit: usize,
    },
}

async fn dispatch(operation: Operation) -> Result<Value> {
    match operation {
        Operation::Doctor { manifest } => engine::doctor(&model::Manifest::load(&manifest)?),
        Operation::Run {
            manifest,
            out,
            runs,
            steps,
            seed,
            limits,
        } => {
            engine::run(
                &model::Manifest::load(&manifest)?,
                &out,
                runs,
                steps,
                seed,
                &limits,
            )
            .await
        }
        Operation::Replay {
            manifest,
            trace,
            limits,
        } => engine::replay(&model::Manifest::load(&manifest)?, &trace, &limits).await,
        Operation::Shrink {
            manifest,
            trace,
            out,
            attempts,
            limits,
        } => {
            engine::shrink(
                &model::Manifest::load(&manifest)?,
                &trace,
                &out,
                attempts,
                &limits,
            )
            .await
        }
        Operation::Regression {
            manifest,
            trace,
            out,
            limits,
        } => {
            engine::regression(
                &model::Manifest::load(&manifest)?,
                &trace,
                out.as_deref(),
                &limits,
            )
            .await
        }
        Operation::Report {
            artifact,
            events,
            from,
            limit,
        } => engine::report(&artifact, events, from, limit),
    }
}
fn exit_code(outcome: Option<&str>) -> i32 {
    match outcome {
        Some("FOUND") => 1,
        Some("INCONCLUSIVE") => 2,
        Some("HARNESS_ERROR") => 3,
        _ => 0,
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();
    let informational = matches!(
        &cli.command,
        Operation::Doctor { .. } | Operation::Report { .. }
    );
    let outcome = tokio::select! {
        result=dispatch(cli.command)=>result,
        _=tokio::signal::ctrl_c()=>Err(Error::inconclusive("interrupted; active adapter process group terminated")),
    };
    let result = match outcome {
        Ok(value) => value,
        Err(error) => {
            json!({"outcome":error.outcome(),"reason":error.to_string().chars().take(1024).collect::<String>()})
        }
    };
    let code = if informational && result["outcome"] != "HARNESS_ERROR" {
        0
    } else {
        exit_code(result["outcome"].as_str())
    };
    let serialized = serde_json::to_vec(&result).expect("valid JSON summary");
    // A pathological report remains bounded even if every identifier is long.
    if serialized.len() >= 12 * 1024 {
        println!(
            "{}",
            json!({"outcome":result["outcome"],"summary_truncated":true,
            "reason":"summary exceeds 12 KiB context budget; request a smaller report page",
            "campaign":result.get("campaign"),"artifact":result.get("artifact"),
            "runs":result.get("runs"),"actions":result.get("actions"),"failure":result.get("failure")})
        );
    } else {
        println!("{}", String::from_utf8(serialized).expect("JSON UTF-8"));
    }
    std::process::exit(code);
}
