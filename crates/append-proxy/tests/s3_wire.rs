//! Wire-contract emulator exercised through the real object_store AmazonS3
//! client. It checks request signing/header presence and CAS semantics, but is
//! not an AWS service qualification or a cryptographic SigV4 implementation.
use append_proxy::{AppendService, Config};
use axum::{
    body::{to_bytes, Body},
    extract::{Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Router,
};
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    ClientOptions, RetryConfig,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{Barrier, Mutex};

#[derive(Default)]
struct WireState {
    objects: Mutex<BTreeMap<String, (Bytes, String)>>,
    generation: AtomicU64,
    create_conditions: AtomicUsize,
    update_conditions: AtomicUsize,
    rejected_conditions: AtomicUsize,
    signed_requests: AtomicUsize,
    fail_after_manifest_commit: AtomicBool,
    fail_resolution_after_commit: AtomicBool,
    fail_next_manifest_read: AtomicBool,
    read_gate: Mutex<Option<Arc<Barrier>>>,
    gated_reads_remaining: AtomicUsize,
}
fn xml(status: StatusCode, code: &str) -> Response {
    (status, [(header::CONTENT_TYPE,"application/xml")], format!("<?xml version=\"1.0\"?><Error><Code>{code}</Code><Message>wire fixture</Message></Error>")).into_response()
}
async fn wire(State(state): State<Arc<WireState>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    if !parts
        .headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("AWS4-HMAC-SHA256 "))
    {
        return xml(StatusCode::FORBIDDEN, "AccessDenied");
    }
    state.signed_requests.fetch_add(1, Ordering::Relaxed);
    let key = parts.uri.path().to_owned();
    if !key.starts_with("/test-bucket/") {
        return xml(StatusCode::NOT_FOUND, "NoSuchBucket");
    }
    if parts.method == Method::PUT {
        let bytes = match to_bytes(body, 16 * 1024 * 1024).await {
            Ok(bytes) => bytes,
            Err(_) => return xml(StatusCode::PAYLOAD_TOO_LARGE, "EntityTooLarge"),
        };
        let mut objects = state.objects.lock().await;
        let current = objects.get(&key);
        let accepted = if let Some(condition) = parts.headers.get(header::IF_NONE_MATCH) {
            state.create_conditions.fetch_add(1, Ordering::Relaxed);
            condition == "*" && current.is_none()
        } else if let Some(condition) = parts.headers.get(header::IF_MATCH) {
            state.update_conditions.fetch_add(1, Ordering::Relaxed);
            current.is_some_and(|(_, etag)| condition.as_bytes() == etag.as_bytes())
        } else {
            return xml(StatusCode::BAD_REQUEST, "MissingConditionalWrite");
        };
        if !accepted {
            state.rejected_conditions.fetch_add(1, Ordering::Relaxed);
            return xml(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
        }
        // Deliberately opaque, non-MD5 ETags exercise exact version transport.
        let etag = format!(
            "\"opaque-generation-{}\"",
            state.generation.fetch_add(1, Ordering::Relaxed)
        );
        objects.insert(key.clone(), (bytes, etag.clone()));
        drop(objects);
        if key.ends_with("/manifest.json")
            && state
                .fail_after_manifest_commit
                .swap(false, Ordering::Relaxed)
        {
            if state
                .fail_resolution_after_commit
                .swap(false, Ordering::Relaxed)
            {
                state.fail_next_manifest_read.store(true, Ordering::Relaxed);
            }
            return xml(StatusCode::SERVICE_UNAVAILABLE, "InternalError");
        }
        let mut response = StatusCode::OK.into_response();
        response
            .headers_mut()
            .insert(header::ETAG, HeaderValue::from_str(&etag).unwrap());
        return response;
    }
    if parts.method == Method::GET || parts.method == Method::HEAD {
        if key.ends_with("/manifest.json")
            && state.fail_next_manifest_read.swap(false, Ordering::Relaxed)
        {
            return xml(StatusCode::SERVICE_UNAVAILABLE, "InternalError");
        }
        let objects = state.objects.lock().await;
        let response = match objects.get(&key) {
            Some((bytes, etag)) => {
                let mut response = Response::new(if parts.method == Method::HEAD {
                    Body::empty()
                } else {
                    Body::from(bytes.clone())
                });
                response
                    .headers_mut()
                    .insert(header::ETAG, HeaderValue::from_str(etag).unwrap());
                response.headers_mut().insert(
                    header::CONTENT_LENGTH,
                    HeaderValue::from_str(&bytes.len().to_string()).unwrap(),
                );
                response.headers_mut().insert(
                    header::LAST_MODIFIED,
                    HeaderValue::from_static("Wed, 21 Oct 2015 07:28:00 GMT"),
                );
                response
            }
            None => xml(StatusCode::NOT_FOUND, "NoSuchKey"),
        };
        drop(objects);
        // A test can pin two independent clients to the exact same version,
        // making the stale CAS race deterministic without changing the service.
        if key.ends_with("/manifest.json")
            && state
                .gated_reads_remaining
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            let gate = state.read_gate.lock().await.clone().unwrap();
            gate.wait().await;
        }
        return response;
    }
    xml(StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed")
}
struct Fixture {
    endpoint: String,
    state: Arc<WireState>,
    server: tokio::task::JoinHandle<()>,
}
impl Fixture {
    async fn start() -> Self {
        let state = Arc::new(WireState::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let router = Router::new().fallback(wire).with_state(state.clone());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Self {
            endpoint,
            state,
            server,
        }
    }
    fn service(&self, config: Config) -> Arc<AppendService> {
        let store = AmazonS3Builder::new()
            .with_bucket_name("test-bucket")
            .with_region("us-east-1")
            .with_access_key_id("test-access")
            .with_secret_access_key("test-secret")
            .with_endpoint(&self.endpoint)
            .with_virtual_hosted_style_request(false)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_client_options(
                ClientOptions::new()
                    .with_allow_http(true)
                    .with_timeout(Duration::from_secs(3)),
            )
            .with_retry(RetryConfig {
                max_retries: 0,
                retry_timeout: Duration::from_secs(3),
                ..Default::default()
            })
            .build()
            .unwrap();
        Arc::new(AppendService::new(Arc::new(store), config).unwrap())
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
async fn read(service: &AppendService, range: Option<&str>) -> Vec<u8> {
    service
        .read("shared/log", range)
        .await
        .unwrap()
        .stream
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .concat()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_s3_client_conditional_writes_concurrency_snapshot_and_range() {
    let fixture = Fixture::start().await;
    let a = fixture.service(Config::default());
    let b = fixture.service(Config::default());
    let first = a
        .append("shared/log", "a", Bytes::from_static(b"abc"), Some(0))
        .await
        .unwrap();
    let pinned = a.snapshot("shared/log").await.unwrap();
    b.append("shared/log", "b", Bytes::from_static(b"defgh"), None)
        .await
        .unwrap();
    assert_eq!(read(&a, Some("bytes=2-5")).await, b"cdef");
    let old = a
        .read_snapshot(pinned, None)
        .unwrap()
        .stream
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .concat();
    assert_eq!(old, b"abc");
    assert_eq!(
        a.append("shared/log", "a", Bytes::from_static(b"abc"), Some(0))
            .await
            .unwrap()
            .receipt,
        first.receipt
    );
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..12u32 {
        let service = if index % 2 == 0 { a.clone() } else { b.clone() };
        tasks.spawn(async move {
            service
                .append(
                    "shared/log",
                    &format!("parallel-{index}"),
                    Bytes::from_static(b"x"),
                    None,
                )
                .await
        });
    }
    while let Some(task) = tasks.join_next().await {
        task.unwrap().unwrap();
    }
    let manifest = a.snapshot("shared/log").await.unwrap().manifest;
    assert_eq!(manifest.entries.len(), 14);
    assert_eq!(manifest.length, 20);
    assert_eq!(read(&a, None).await, b"abcdefghxxxxxxxxxxxx");
    assert!(fixture.state.create_conditions.load(Ordering::Relaxed) > 0);
    assert!(fixture.state.update_conditions.load(Ordering::Relaxed) > 0);
    assert!(fixture.state.signed_requests.load(Ordering::Relaxed) > 0);
}
#[tokio::test]
async fn committed_write_with_lost_success_response_resolves_on_final_retry() {
    let fixture = Fixture::start().await;
    let service = fixture.service(Config {
        max_attempts: 1,
        ..Default::default()
    });
    fixture
        .state
        .fail_after_manifest_commit
        .store(true, Ordering::Relaxed);
    let first = service
        .append(
            "shared/log",
            "request-1",
            Bytes::from_static(b"once"),
            Some(0),
        )
        .await
        .unwrap();
    assert!(first.replayed);
    let repeated = service
        .append(
            "shared/log",
            "request-1",
            Bytes::from_static(b"once"),
            Some(0),
        )
        .await
        .unwrap();
    assert_eq!(first.receipt, repeated.receipt);
    assert_eq!(read(&service, None).await, b"once");
    assert_eq!(
        service
            .snapshot("shared/log")
            .await
            .unwrap()
            .manifest
            .entries
            .len(),
        1
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn competing_expected_offsets_have_one_success() {
    let fixture = Fixture::start().await;
    let a = fixture.service(Config::default());
    let b = fixture.service(Config::default());
    *fixture.state.read_gate.lock().await = Some(Arc::new(Barrier::new(2)));
    fixture
        .state
        .gated_reads_remaining
        .store(2, Ordering::SeqCst);
    let (left, right) = tokio::join!(
        a.append("shared/log", "left", Bytes::from_static(b"L"), Some(0)),
        b.append("shared/log", "right", Bytes::from_static(b"R"), Some(0))
    );
    assert_ne!(left.is_ok(), right.is_ok());
    assert_eq!(a.snapshot("shared/log").await.unwrap().manifest.length, 1);
    assert_eq!(fixture.state.rejected_conditions.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn unresolved_commit_returns_retryable_error_and_same_id_recovers() {
    let fixture = Fixture::start().await;
    let service = fixture.service(Config {
        max_attempts: 1,
        ..Default::default()
    });
    fixture
        .state
        .fail_after_manifest_commit
        .store(true, Ordering::Relaxed);
    fixture
        .state
        .fail_resolution_after_commit
        .store(true, Ordering::Relaxed);
    let first = service
        .append(
            "shared/log",
            "request-1",
            Bytes::from_static(b"once"),
            Some(0),
        )
        .await;
    assert!(matches!(first, Err(append_proxy::ProxyError::Unavailable)));
    let recovered = service
        .append(
            "shared/log",
            "request-1",
            Bytes::from_static(b"once"),
            Some(0),
        )
        .await
        .unwrap();
    assert!(recovered.replayed);
    assert_eq!(recovered.receipt.offset, 0);
    assert_eq!(read(&service, None).await, b"once");
    assert_eq!(
        service
            .snapshot("shared/log")
            .await
            .unwrap()
            .manifest
            .entries
            .len(),
        1
    );
}
