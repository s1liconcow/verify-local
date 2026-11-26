use append_proxy::{router, AppendService, Config};
use clap::Parser;
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    memory::InMemory,
    ClientOptions, ObjectStore, RetryConfig,
};
use std::{net::SocketAddr, sync::Arc, time::Duration};

#[derive(Parser)]
#[command(about = "Append-only virtual objects over conditional S3 writes")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8787")]
    listen: SocketAddr,
    #[arg(long, help = "Ephemeral in-memory demo; no S3 credentials required")]
    memory: bool,
    #[arg(long, env = "APPEND_S3_BUCKET")]
    bucket: Option<String>,
    #[arg(long, env = "APPEND_S3_ENDPOINT")]
    endpoint: Option<String>,
    #[arg(long, env = "AWS_REGION", default_value = "us-east-1")]
    region: String,
    #[arg(long, env = "APPEND_PREFIX", default_value = "verify-local-append")]
    prefix: String,
    #[arg(long, env = "APPEND_BEARER_TOKEN", hide_env_values = true)]
    bearer_token: Option<String>,
    #[arg(long, default_value_t = 8 * 1024 * 1024)]
    max_append_bytes: usize,
    #[arg(long, default_value_t = 4096)]
    max_entries: usize,
    #[arg(long, default_value_t = 4)]
    prefetch: usize,
    #[arg(long, default_value_t = 16)]
    max_inflight: usize,
    #[arg(
        long,
        help = "Permit an explicitly configured HTTP S3 endpoint for local testing"
    )]
    allow_http: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "append_proxy=info".into()),
        )
        .init();
    anyhow::ensure!(
        args.listen.ip().is_loopback() || args.bearer_token.as_ref().is_some_and(|s| !s.is_empty()),
        "a bearer token is required when binding beyond loopback"
    );
    anyhow::ensure!(
        args.max_inflight > 0 && args.max_inflight <= 1024,
        "max-inflight must be 1-1024"
    );
    anyhow::ensure!(
        args.bearer_token.as_ref().is_none_or(|s| !s.is_empty()),
        "bearer token must not be empty"
    );
    let store: Arc<dyn ObjectStore> = if args.memory {
        Arc::new(InMemory::new())
    } else {
        let bucket = args.bucket.as_deref().ok_or_else(|| {
            anyhow::anyhow!(
                "--bucket (or APPEND_S3_BUCKET) is required unless --memory is selected"
            )
        })?;
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_region(&args.region)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_client_options(
                ClientOptions::new()
                    .with_allow_http(args.allow_http)
                    .with_timeout(Duration::from_secs(10))
                    .with_connect_timeout(Duration::from_secs(5)),
            )
            .with_retry(RetryConfig {
                max_retries: 1,
                retry_timeout: Duration::from_secs(10),
                ..Default::default()
            });
        if let Some(endpoint) = args.endpoint {
            builder = builder
                .with_endpoint(endpoint)
                .with_virtual_hosted_style_request(false);
        }
        Arc::new(builder.build()?)
    };
    let config = Config {
        prefix: args.prefix,
        max_append_bytes: args.max_append_bytes,
        max_entries: args.max_entries,
        prefetch: args.prefetch,
        ..Default::default()
    };
    let service = Arc::new(AppendService::new(store, config)?);
    let app = router(service, args.bearer_token, args.max_inflight);
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    tracing::info!(address = %listener.local_addr()?, ephemeral = args.memory, "append proxy listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
