//! Virtual append objects backed by immutable S3 segments and a conditional
//! manifest. The successful manifest CAS is the only commit point.
//!
//! Backend requirements: strongly consistent GET, atomic create/update
//! preconditions, and an exclusively owned prefix. Direct bucket GET returns
//! the storage representation; the proxy presents concatenated virtual objects.
use append_core::{
    digest_hex, manifest_key, plan_append, validate_object_key, validate_request_id, AppendError,
    AppendInput, AppendPlan, Manifest, Receipt,
};
use axum::{
    body::{to_bytes, Body},
    extract::{Path as HttpPath, Request, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use bytes::{Bytes, BytesMut};
use futures::{
    stream::{self, BoxStream},
    StreamExt,
};
use object_store::{path::Path, GetResult, ObjectStore, PutMode, PutOptions, UpdateVersion};
use serde::Serialize;
use std::{
    ops::Range,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::Semaphore,
    time::{sleep, timeout},
};

#[derive(Clone, Debug)]
pub struct Config {
    pub prefix: String,
    pub max_append_bytes: usize,
    pub max_manifest_bytes: usize,
    pub max_entries: usize,
    pub prefetch: usize,
    pub max_attempts: usize,
    pub io_timeout: Duration,
    pub append_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            prefix: "verify-local-append".into(),
            max_append_bytes: 8 * 1024 * 1024,
            max_manifest_bytes: 4 * 1024 * 1024,
            max_entries: 4096,
            prefetch: 4,
            max_attempts: 32,
            io_timeout: Duration::from_secs(10),
            append_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug)]
pub enum ProxyError {
    Invalid(String),
    TooLarge,
    NotFound,
    Conflict(String),
    Full,
    RangeNotSatisfiable(u64),
    Backend,
    Corrupt,
    Unavailable,
    RequestTimeout,
    Unauthorized,
}
impl std::fmt::Display for ProxyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::Invalid(s) | Self::Conflict(s) => s.as_str(),
            Self::TooLarge => "append exceeds the configured byte limit",
            Self::NotFound => "virtual object does not exist",
            Self::Full => "manifest capacity reached; create a new object",
            Self::RangeNotSatisfiable(_) => "requested byte range is not satisfiable",
            Self::Backend => "object storage operation failed",
            Self::Corrupt => "stored data failed integrity or manifest validation",
            Self::Unavailable => "append outcome may be unknown; retry with the same Idempotency-Key and identical body and offset",
            Self::RequestTimeout => "request body exceeded the configured receive deadline",
            Self::Unauthorized => "a valid bearer token is required",
        };
        f.write_str(message)
    }
}
impl std::error::Error for ProxyError {}
impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        let (status, code) = match &self {
            Self::Invalid(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
            Self::TooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large"),
            Self::NotFound => (StatusCode::NOT_FOUND, "not_found"),
            Self::Conflict(_) => (StatusCode::CONFLICT, "conflict"),
            Self::Full => (StatusCode::INSUFFICIENT_STORAGE, "manifest_full"),
            Self::RangeNotSatisfiable(_) => (StatusCode::RANGE_NOT_SATISFIABLE, "invalid_range"),
            Self::Backend | Self::Corrupt => (StatusCode::BAD_GATEWAY, "backend_error"),
            Self::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "retry_same_request"),
            Self::RequestTimeout => (StatusCode::REQUEST_TIMEOUT, "request_timeout"),
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized"),
        };
        let mut response = (
            status,
            Json(serde_json::json!({"error":code,"message":self.to_string()})),
        )
            .into_response();
        if let Self::RangeNotSatisfiable(length) = self {
            response.headers_mut().insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes */{length}")).expect("numeric header"),
            );
        }
        if status == StatusCode::SERVICE_UNAVAILABLE {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        response
    }
}

fn core_error(error: AppendError) -> ProxyError {
    match error {
        AppendError::IdempotencyConflict => ProxyError::Conflict(
            "Idempotency-Key is already bound to a different append intent".into(),
        ),
        AppendError::OffsetConflict { expected, actual } => ProxyError::Conflict(format!(
            "Append-Offset {expected} does not match current length {actual}"
        )),
        AppendError::Full { .. } => ProxyError::Full,
        AppendError::Invalid(reason) => ProxyError::Invalid(reason),
        AppendError::Overflow => ProxyError::TooLarge,
    }
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub manifest: Manifest,
    pub version: UpdateVersion,
    pub etag: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct AppendReply {
    pub receipt: Receipt,
    pub replayed: bool,
}

pub struct ReadResult {
    pub full_length: u64,
    pub range: Range<u64>,
    pub partial: bool,
    pub etag: String,
    pub stream: BoxStream<'static, Result<Bytes, ProxyError>>,
}

#[derive(Clone)]
pub struct AppendService {
    store: Arc<dyn ObjectStore>,
    config: Config,
}

impl AppendService {
    pub fn new(store: Arc<dyn ObjectStore>, config: Config) -> Result<Self, ProxyError> {
        if config.prefix.trim_matches('/').is_empty()
            || Path::parse(&config.prefix).is_err()
            || config.max_append_bytes == 0
            || config.max_append_bytes > 256 * 1024 * 1024
            || config.max_manifest_bytes < 1024
            || config.max_manifest_bytes > 32 * 1024 * 1024
            || config.max_entries == 0
            || config.max_entries > 100_000
            || config.prefetch == 0
            || config.prefetch > 64
            || config.max_attempts == 0
            || config.max_attempts > 1024
            || config.io_timeout.is_zero()
            || config.append_timeout.is_zero()
        {
            return Err(ProxyError::Invalid(
                "invalid service limits or storage prefix".into(),
            ));
        }
        Ok(Self { store, config })
    }
    pub fn config(&self) -> &Config {
        &self.config
    }
    fn location(&self, relative: &str) -> Path {
        Path::from(format!(
            "{}/{}",
            self.config.prefix.trim_end_matches('/'),
            relative
        ))
    }
    fn validate_key(key: &str) -> Result<(), ProxyError> {
        validate_object_key(key).map_err(core_error)
    }
    async fn load(&self, key: &str) -> Result<Option<Snapshot>, ProxyError> {
        let result = match timeout(
            self.config.io_timeout,
            self.store.get(&self.location(&manifest_key(key))),
        )
        .await
        {
            Err(_) => return Err(ProxyError::Backend),
            Ok(Err(object_store::Error::NotFound { .. })) => return Ok(None),
            Ok(Err(_)) => return Err(ProxyError::Backend),
            Ok(Ok(result)) => result,
        };
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        if version.e_tag.is_none() && version.version.is_none() {
            return Err(ProxyError::Corrupt);
        }
        let bytes = timeout(
            self.config.io_timeout,
            read_bounded(result, self.config.max_manifest_bytes),
        )
        .await
        .map_err(|_| ProxyError::Backend)??;
        let manifest: Manifest = serde_json::from_slice(&bytes).map_err(|_| ProxyError::Corrupt)?;
        manifest.validate().map_err(|_| ProxyError::Corrupt)?;
        if manifest.entries.len() > self.config.max_entries
            || manifest
                .entries
                .iter()
                .any(|e| e.length > self.config.max_append_bytes as u64)
        {
            return Err(ProxyError::Corrupt);
        }
        // A virtual object's validator refers to the complete pinned manifest.
        let etag = format!("\"{}\"", digest_hex(&bytes));
        Ok(Some(Snapshot {
            manifest,
            version,
            etag,
        }))
    }
    pub async fn snapshot(&self, key: &str) -> Result<Snapshot, ProxyError> {
        Self::validate_key(key)?;
        self.load(key).await?.ok_or(ProxyError::NotFound)
    }
    pub async fn append(
        &self,
        key: &str,
        request_id: &str,
        payload: Bytes,
        expected_offset: Option<u64>,
    ) -> Result<AppendReply, ProxyError> {
        Self::validate_key(key)?;
        validate_request_id(request_id).map_err(core_error)?;
        if payload.len() > self.config.max_append_bytes {
            return Err(ProxyError::TooLarge);
        }
        let input = AppendInput::from_bytes(key, request_id, &payload, expected_offset);
        input.validate().map_err(core_error)?;
        timeout(
            self.config.append_timeout,
            self.append_inner(key, payload, &input),
        )
        .await
        .map_err(|_| ProxyError::Unavailable)?
    }
    async fn append_inner(
        &self,
        key: &str,
        payload: Bytes,
        input: &AppendInput,
    ) -> Result<AppendReply, ProxyError> {
        let mut uploaded = false;
        for attempt in 0..self.config.max_attempts {
            let snapshot = match self.load(key).await {
                Ok(snapshot) => snapshot,
                Err(ProxyError::Backend) => {
                    backoff(attempt).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let empty = Manifest::default();
            let current = snapshot.as_ref().map(|s| &s.manifest).unwrap_or(&empty);
            // Receipt resolution intentionally precedes expected-offset validation.
            let (manifest, receipt) =
                match plan_append(current, input, self.config.max_entries).map_err(core_error)? {
                    AppendPlan::Existing(receipt) => {
                        return Ok(AppendReply {
                            receipt,
                            replayed: true,
                        })
                    }
                    AppendPlan::Commit { manifest, receipt } => (manifest, receipt),
                };
            let encoded = serde_json::to_vec(&manifest).map_err(|_| ProxyError::Corrupt)?;
            if encoded.len() > self.config.max_manifest_bytes {
                return Err(ProxyError::Full);
            }
            if !uploaded {
                let options = PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                };
                match timeout(
                    self.config.io_timeout,
                    self.store.put_opts(
                        &self.location(&input.segment),
                        payload.clone().into(),
                        options,
                    ),
                )
                .await
                {
                    Ok(Ok(_)) => uploaded = true,
                    Ok(Err(
                        object_store::Error::AlreadyExists { .. }
                        | object_store::Error::Precondition { .. },
                    )) => {
                        // A hash-named segment is immutable. Verify existing bytes
                        // before allowing it into a manifest (including a retry).
                        let get = timeout(
                            self.config.io_timeout,
                            self.store.get(&self.location(&input.segment)),
                        )
                        .await;
                        let existing = match get {
                            Ok(Ok(value)) => value,
                            _ => {
                                backoff(attempt).await;
                                continue;
                            }
                        };
                        let bytes = timeout(
                            self.config.io_timeout,
                            read_bounded(existing, self.config.max_append_bytes),
                        )
                        .await
                        .map_err(|_| ProxyError::Unavailable)??;
                        if bytes.len() as u64 != input.length || digest_hex(&bytes) != input.sha256
                        {
                            return Err(ProxyError::Corrupt);
                        }
                        uploaded = true;
                    }
                    _ => {
                        backoff(attempt).await;
                        continue;
                    }
                }
            }
            let mode = snapshot
                .map(|s| PutMode::Update(s.version))
                .unwrap_or(PutMode::Create);
            let options = PutOptions {
                mode,
                ..Default::default()
            };
            match timeout(
                self.config.io_timeout,
                self.store.put_opts(
                    &self.location(&manifest_key(key)),
                    Bytes::from(encoded).into(),
                    options,
                ),
            )
            .await
            {
                Ok(Ok(_)) => {
                    return Ok(AppendReply {
                        receipt,
                        replayed: false,
                    })
                }
                // All uncertain outcomes resolve from durable state on retry.
                // Persistent auth/config failures eventually become a bounded 503.
                _ => {
                    backoff(attempt).await;
                }
            }
        }
        // Final resolution allows the last attempted CAS to have committed even
        // when its response was lost. Never issue a new append after this point.
        if let Ok(Some(snapshot)) = self.load(key).await {
            if let Ok(AppendPlan::Existing(receipt)) =
                plan_append(&snapshot.manifest, input, self.config.max_entries)
            {
                return Ok(AppendReply {
                    receipt,
                    replayed: true,
                });
            }
        }
        Err(ProxyError::Unavailable)
    }
    pub async fn read(
        &self,
        key: &str,
        range_header: Option<&str>,
    ) -> Result<ReadResult, ProxyError> {
        let snapshot = self.snapshot(key).await?;
        self.read_snapshot(snapshot, range_header)
    }
    pub fn read_snapshot(
        &self,
        snapshot: Snapshot,
        range_header: Option<&str>,
    ) -> Result<ReadResult, ProxyError> {
        let full_length = snapshot.manifest.length;
        let range = parse_range(range_header, full_length)?;
        let partial = range_header.is_some();
        let selected: Vec<_> = snapshot
            .manifest
            .entries
            .into_iter()
            .filter_map(|entry| {
                let start = range.start.max(entry.offset);
                let end = range.end.min(entry.offset + entry.length);
                let slice =
                    (start - entry.offset) as usize..end.saturating_sub(entry.offset) as usize;
                (start < end).then_some((entry, slice))
            })
            .collect();
        let store = self.store.clone();
        let prefix = self.config.prefix.trim_end_matches('/').to_string();
        let max = self.config.max_append_bytes;
        let duration = self.config.io_timeout;
        let stream = stream::iter(selected)
            .map(move |(entry, slice)| {
                let store = store.clone();
                let location = Path::from(format!("{prefix}/{}", entry.segment));
                async move {
                    let operation = async {
                        let result = store
                            .get(&location)
                            .await
                            .map_err(|_| ProxyError::Backend)?;
                        let bytes = read_bounded(result, max).await?;
                        if bytes.len() as u64 != entry.length || digest_hex(&bytes) != entry.sha256
                        {
                            return Err(ProxyError::Corrupt);
                        }
                        Ok(bytes.slice(slice))
                    };
                    timeout(duration, operation)
                        .await
                        .map_err(|_| ProxyError::Backend)?
                }
            })
            .buffered(self.config.prefetch)
            .boxed();
        Ok(ReadResult {
            full_length,
            range,
            partial,
            etag: snapshot.etag,
            stream,
        })
    }
}

async fn read_bounded(result: GetResult, max: usize) -> Result<Bytes, ProxyError> {
    if result.meta.size > max as u64 {
        return Err(ProxyError::Corrupt);
    }
    let expected = result.meta.size as usize;
    let mut body = result.into_stream();
    let first = body
        .next()
        .await
        .transpose()
        .map_err(|_| ProxyError::Backend)?
        .unwrap_or_default();
    if first.len() > expected {
        return Err(ProxyError::Corrupt);
    }
    let Some(second) = body.next().await else {
        if first.len() != expected {
            return Err(ProxyError::Corrupt);
        }
        // In-memory and single-frame responses retain their existing buffer.
        return Ok(first);
    };
    let second = second.map_err(|_| ProxyError::Backend)?;
    if second.len() > expected - first.len() {
        return Err(ProxyError::Corrupt);
    }
    let mut buffer = BytesMut::with_capacity(expected);
    buffer.extend_from_slice(&first);
    buffer.extend_from_slice(&second);
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|_| ProxyError::Backend)?;
        if chunk.len() > expected - buffer.len() {
            return Err(ProxyError::Corrupt);
        }
        buffer.extend_from_slice(&chunk);
    }
    if buffer.len() != expected {
        return Err(ProxyError::Corrupt);
    }
    Ok(buffer.freeze())
}

async fn backoff(attempt: usize) {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let tick = COUNTER.fetch_add(1, Ordering::Relaxed);
    let jitter = tick.wrapping_mul(6364136223846793005).rotate_left(17) % 7;
    sleep(Duration::from_millis((1u64 << attempt.min(6)) + jitter)).await;
}

pub fn parse_range(header: Option<&str>, length: u64) -> Result<Range<u64>, ProxyError> {
    let Some(header) = header else {
        return Ok(0..length);
    };
    let value = header
        .strip_prefix("bytes=")
        .ok_or_else(|| ProxyError::Invalid("only bytes ranges are supported".into()))?;
    if value.contains(',') {
        return Err(ProxyError::Invalid(
            "only one byte range is supported".into(),
        ));
    }
    let (start, end) = value
        .split_once('-')
        .ok_or_else(|| ProxyError::Invalid("invalid byte range".into()))?;
    if length == 0 {
        return Err(ProxyError::RangeNotSatisfiable(length));
    }
    if start.is_empty() {
        let suffix: u64 = end
            .parse()
            .map_err(|_| ProxyError::Invalid("invalid suffix range".into()))?;
        if suffix == 0 {
            return Err(ProxyError::RangeNotSatisfiable(length));
        }
        return Ok(length.saturating_sub(suffix)..length);
    }
    let start: u64 = start
        .parse()
        .map_err(|_| ProxyError::Invalid("invalid range start".into()))?;
    if start >= length {
        return Err(ProxyError::RangeNotSatisfiable(length));
    }
    let end = if end.is_empty() {
        length
    } else {
        let inclusive: u64 = end
            .parse()
            .map_err(|_| ProxyError::Invalid("invalid range end".into()))?;
        if inclusive < start {
            return Err(ProxyError::RangeNotSatisfiable(length));
        }
        inclusive.saturating_add(1).min(length)
    };
    Ok(start..end)
}

#[derive(Clone)]
struct AppState {
    service: Arc<AppendService>,
    bearer: Option<Arc<str>>,
    permits: Arc<Semaphore>,
}

pub fn router(service: Arc<AppendService>, bearer: Option<String>, max_inflight: usize) -> Router {
    let state = AppState {
        service,
        bearer: bearer.map(Into::into),
        permits: Arc::new(Semaphore::new(max_inflight.max(1))),
    };
    Router::new()
        .route(
            "/healthz",
            get(|| async { Json(serde_json::json!({"status":"ok"})) }),
        )
        .route(
            "/v1/objects/{*key}",
            get(object_request)
                .head(object_request)
                .post(object_request),
        )
        .with_state(state)
}

fn authenticate(headers: &HeaderMap, token: &Option<Arc<str>>) -> Result<(), ProxyError> {
    let Some(expected) = token else {
        return Ok(());
    };
    let supplied = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    let mut difference = supplied.len() ^ expected.len();
    for (index, byte) in expected.bytes().enumerate() {
        difference |= usize::from(byte ^ supplied.as_bytes().get(index).copied().unwrap_or(0));
    }
    if difference == 0 {
        Ok(())
    } else {
        Err(ProxyError::Unauthorized)
    }
}

async fn object_request(
    State(state): State<AppState>,
    HttpPath(key): HttpPath<String>,
    request: Request,
) -> Result<Response, ProxyError> {
    authenticate(request.headers(), &state.bearer)?;
    // Acquire before reading POST bodies. GET retains its permit until the body
    // is consumed or dropped, bounding aggregate prefetch buffers.
    let permit = state
        .permits
        .clone()
        .try_acquire_owned()
        .map_err(|_| ProxyError::Unavailable)?;
    let (parts, body) = request.into_parts();
    if parts.method == Method::POST {
        let id = parts
            .headers
            .get("idempotency-key")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| ProxyError::Invalid("Idempotency-Key is required".into()))?;
        validate_request_id(id).map_err(core_error)?;
        AppendService::validate_key(&key)?;
        let offset = parts
            .headers
            .get("append-offset")
            .map(|v| {
                v.to_str()
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .ok_or_else(|| {
                        ProxyError::Invalid("Append-Offset must be an unsigned integer".into())
                    })
            })
            .transpose()?;
        let payload = timeout(
            state.service.config.io_timeout,
            to_bytes(body, state.service.config.max_append_bytes),
        )
        .await
        .map_err(|_| ProxyError::RequestTimeout)?
        .map_err(|_| ProxyError::TooLarge)?;
        let result = state.service.append(&key, id, payload, offset).await?;
        let status = if result.replayed {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        };
        return Ok((status, Json(result)).into_response());
    }
    let snapshot = state.service.snapshot(&key).await?;
    if parts
        .headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(',')
                .map(str::trim)
                .any(|tag| tag == "*" || tag == snapshot.etag)
        })
    {
        let mut response = StatusCode::NOT_MODIFIED.into_response();
        response.headers_mut().insert(
            header::ETAG,
            HeaderValue::from_str(&snapshot.etag).expect("digest etag"),
        );
        return Ok(response);
    }
    // HEAD ignores Range as required for a full representation's metadata.
    let range_header = if parts.method == Method::HEAD {
        None
    } else {
        parts
            .headers
            .get(header::RANGE)
            .map(|v| {
                v.to_str()
                    .map_err(|_| ProxyError::Invalid("invalid Range header".into()))
            })
            .transpose()?
    };
    let read = state.service.read_snapshot(snapshot, range_header)?;
    let status = if read.partial {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    let body = if parts.method == Method::HEAD {
        Body::empty()
    } else {
        let stream = stream::unfold((read.stream, permit), |(mut stream, permit)| async move {
            stream.next().await.map(|value| (value, (stream, permit)))
        });
        Body::from_stream(stream)
    };
    let mut response = (status, body).into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&(read.range.end - read.range.start).to_string())
            .expect("numeric header"),
    );
    headers.insert(
        header::ETAG,
        HeaderValue::from_str(&read.etag).expect("digest etag"),
    );
    if read.partial {
        headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!(
                "bytes {}-{}/{}",
                read.range.start,
                read.range.end - 1,
                read.full_length
            ))
            .expect("numeric header"),
        );
    }
    Ok(response)
}
