use append_proxy::{router, AppendService, Config, ProxyError};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use bytes::Bytes;
use futures::TryStreamExt;
use http_body_util::BodyExt;
use object_store::{memory::InMemory, ObjectStore};
use std::sync::Arc;
use tower::ServiceExt;

fn service(store: Arc<dyn ObjectStore>) -> Arc<AppendService> {
    Arc::new(AppendService::new(store, Config::default()).unwrap())
}
async fn contents(service: &AppendService, key: &str, range: Option<&str>) -> Vec<u8> {
    service
        .read(key, range)
        .await
        .unwrap()
        .stream
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .flatten()
        .collect()
}
#[tokio::test]
async fn retry_binds_payload_offset_and_returns_original_receipt() {
    let service = service(Arc::new(InMemory::new()));
    let first = service
        .append("log", "a", Bytes::from_static(b"hello"), Some(0))
        .await
        .unwrap();
    service
        .append("log", "b", Bytes::from_static(b"world"), None)
        .await
        .unwrap();
    let repeated = service
        .append("log", "a", Bytes::from_static(b"hello"), Some(0))
        .await
        .unwrap();
    assert_eq!(first.receipt, repeated.receipt);
    assert!(repeated.replayed);
    assert!(matches!(
        service
            .append("log", "a", Bytes::from_static(b"other"), Some(0))
            .await,
        Err(ProxyError::Conflict(_))
    ));
    assert!(matches!(
        service
            .append("log", "a", Bytes::from_static(b"hello"), None)
            .await,
        Err(ProxyError::Conflict(_))
    ));
    assert!(matches!(
        service
            .append("log", "c", Bytes::from_static(b"!"), Some(0))
            .await,
        Err(ProxyError::Conflict(_))
    ));
    assert_eq!(contents(&service, "log", None).await, b"helloworld");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_instances_preserve_concurrent_appends_and_offsets() {
    let store = Arc::new(InMemory::new());
    let a = service(store.clone());
    let b = service(store);
    let mut workers = tokio::task::JoinSet::new();
    for index in 0..32u32 {
        let service = if index % 2 == 0 { a.clone() } else { b.clone() };
        workers.spawn(async move {
            let reply = service
                .append(
                    "shared",
                    &format!("r-{index}"),
                    Bytes::copy_from_slice(&index.to_be_bytes()),
                    None,
                )
                .await
                .unwrap();
            (index, reply.receipt.offset)
        });
    }
    let mut receipts = Vec::new();
    while let Some(result) = workers.join_next().await {
        receipts.push(result.unwrap());
    }
    let bytes = contents(&a, "shared", None).await;
    assert_eq!(bytes.len(), 128);
    for (index, offset) in receipts {
        assert_eq!(
            &bytes[offset as usize..offset as usize + 4],
            &index.to_be_bytes()
        );
    }
}
#[tokio::test]
async fn pinned_reads_and_cross_segment_ranges_are_exact() {
    let service = service(Arc::new(InMemory::new()));
    service
        .append("log", "a", Bytes::from_static(b"abc"), None)
        .await
        .unwrap();
    let pinned = service.snapshot("log").await.unwrap();
    service
        .append("log", "b", Bytes::from_static(b"defgh"), None)
        .await
        .unwrap();
    let old = service
        .read_snapshot(pinned, None)
        .unwrap()
        .stream
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(old.concat(), b"abc");
    assert_eq!(contents(&service, "log", Some("bytes=2-5")).await, b"cdef");
    assert_eq!(contents(&service, "log", Some("bytes=-3")).await, b"fgh");
    assert_eq!(contents(&service, "log", Some("bytes=6-")).await, b"gh");
    assert!(matches!(
        service.read("log", Some("bytes=8-")).await,
        Err(ProxyError::RangeNotSatisfiable(8))
    ));
}
#[tokio::test]
async fn http_contract_auth_limits_head_and_ranges() {
    let service = service(Arc::new(InMemory::new()));
    let app = router(service, Some("test-token".into()), 2);
    let denied = app
        .clone()
        .oneshot(
            Request::post("/v1/objects/log")
                .header("idempotency-key", "a")
                .body(Body::from("abc"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    let make = |method: &str| {
        Request::builder()
            .method(method)
            .uri("/v1/objects/log")
            .header("authorization", "Bearer test-token")
    };
    let created = app
        .clone()
        .oneshot(
            make("POST")
                .header("idempotency-key", "a")
                .body(Body::from("abcdef"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let replay = app
        .clone()
        .oneshot(
            make("POST")
                .header("idempotency-key", "a")
                .body(Body::from("abcdef"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    let head = app
        .clone()
        .oneshot(
            make("HEAD")
                .header("range", "bytes=0-1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(head.headers()["content-length"], "6");
    assert_eq!(head.status(), StatusCode::OK);
    assert!(head
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .is_empty());
    let get = app
        .clone()
        .oneshot(
            make("GET")
                .header("range", "bytes=1-3")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(get.headers()["content-range"], "bytes 1-3/6");
    assert_eq!(
        get.into_body().collect().await.unwrap().to_bytes(),
        Bytes::from_static(b"bcd")
    );
    let invalid = app
        .oneshot(
            make("GET")
                .header("range", "bytes=99-")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(invalid.headers()["content-range"], "bytes */6");
}
#[tokio::test]
async fn manifest_and_payload_limits_are_explicit() {
    let service = Arc::new(
        AppendService::new(
            Arc::new(InMemory::new()),
            Config {
                max_append_bytes: 3,
                max_entries: 1,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    assert!(matches!(
        service
            .append("log", "big", Bytes::from_static(b"abcd"), None)
            .await,
        Err(ProxyError::TooLarge)
    ));
    service
        .append("log", "a", Bytes::from_static(b"abc"), None)
        .await
        .unwrap();
    assert!(matches!(
        service
            .append("log", "b", Bytes::from_static(b"a"), None)
            .await,
        Err(ProxyError::Full)
    ));
    assert!(
        service
            .append("log", "a", Bytes::from_static(b"abc"), None)
            .await
            .unwrap()
            .replayed
    );
    assert!(matches!(
        service
            .append("", "a", Bytes::from_static(b"a"), None)
            .await,
        Err(ProxyError::Invalid(_))
    ));
    assert!(matches!(
        service.append("log", "empty", Bytes::new(), None).await,
        Err(ProxyError::Invalid(_))
    ));
    let app = router(service, None, 1);
    let response = app
        .oneshot(
            Request::post("/v1/objects/log")
                .header("idempotency-key", "large")
                .body(Body::from("abcd"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}
#[tokio::test]
async fn corrupt_segment_is_detected_before_stream_delivery() {
    let store = Arc::new(InMemory::new());
    let service = service(store.clone());
    service
        .append("log", "a", Bytes::from_static(b"hello"), None)
        .await
        .unwrap();
    let snapshot = service.snapshot("log").await.unwrap();
    let location = object_store::path::Path::from(format!(
        "verify-local-append/{}",
        snapshot.manifest.entries[0].segment
    ));
    store
        .put(&location, Bytes::from_static(b"wrong").into())
        .await
        .unwrap();
    let result = service
        .read("log", None)
        .await
        .unwrap()
        .stream
        .try_collect::<Vec<_>>()
        .await;
    assert!(matches!(result, Err(ProxyError::Corrupt)));
}

#[tokio::test]
async fn stalled_upload_times_out_and_saturation_sheds_load() {
    let service = Arc::new(
        AppendService::new(
            Arc::new(InMemory::new()),
            Config {
                io_timeout: std::time::Duration::from_millis(100),
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let app = router(service, None, 1);
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let stalled = Body::from_stream(futures::stream::once(async move {
        let _ = started_tx.send(());
        futures::future::pending::<Result<Bytes, std::io::Error>>().await
    }));
    let request = Request::post("/v1/objects/log")
        .header("idempotency-key", "stalled")
        .body(stalled)
        .unwrap();
    let pending = tokio::spawn(app.clone().oneshot(request));
    started_rx.await.unwrap();
    let overloaded = app
        .clone()
        .oneshot(Request::get("/v1/objects/log").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(overloaded.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(overloaded.headers()["retry-after"], "1");
    let timed_out = tokio::time::timeout(std::time::Duration::from_secs(2), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(timed_out.status(), StatusCode::REQUEST_TIMEOUT);
    let recovered = app
        .oneshot(Request::get("/v1/objects/log").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(recovered.status(), StatusCode::NOT_FOUND);
}
