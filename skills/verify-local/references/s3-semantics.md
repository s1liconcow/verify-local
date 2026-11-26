# S3 append semantics

## Storage and API boundary

The proxy exposes virtual append objects at `/v1/objects/{key}`. POST appends a nonempty body; GET reads a snapshot, HEAD returns its metadata, and GET supports a single byte range. The proxy supports an optional bearer token and binds to loopback by default. A non-loopback listener requires a bearer token.

The backing bucket stores one mutable manifest and immutable payload segments under the configured prefix. Direct bucket GET returns that storage representation. Clients obtain concatenated object bytes through the proxy. This release does not implement the complete S3 API or native S3 Express append, object deletion, compaction, or garbage collection.

Use an exclusively owned storage prefix. External overwrite, deletion, lifecycle expiration, or mutation of manifests/segments invalidates the persistence assumptions. Each proxy instance must share the same configured size limits and storage prefix for a given virtual object.

## Commit and retry protocol

The shared `append-core` planner is called by both the HTTP service and the deterministic simulator. The HTTP service performs these storage operations:

1. Read the current manifest and its opaque version/ETag.
2. Resolve an existing idempotency receipt before checking the current append offset or manifest capacity.
3. For a new request, check its optional expected offset and prepare a manifest containing the new segment descriptor and durable receipt.
4. Create the immutable segment, verifying its full length and SHA-256 if it already exists.
5. Publish the manifest with an atomic create/update precondition against the version just read.
6. Acknowledge the append after the manifest commit, or after a subsequent read confirms its receipt.

The successful conditional manifest write is the append's linearization point. Payload publication and the idempotency receipt occur in the same logical manifest transition. The manifest contains a generation in its body, contiguous offsets, payload lengths/digests, immutable segment paths, request IDs, and their original offset intent.

`Idempotency-Key` identifies one append intent within one virtual object. A successful retry returns its original receipt, including the original commit generation and total length immediately after that append. Reusing the ID with a different body, length, or `Append-Offset` returns a conflict. Two concurrent blind appends may both succeed after CAS retries. Two new requests requiring the same offset can yield one success and one offset conflict.

The current append planner accepts 1-128 visible ASCII bytes for an idempotency key and rejects zero-byte appends. Object names contain 1-1024 bytes without control characters. Internal object and segment names are hash-derived.

A timeout or a lost storage response can leave the commit outcome unknown. The service rereads durable state and resolves the receipt before attempting further work. If the retry/time budget expires, HTTP 503 instructs the caller to retry with the same key, identical body, and identical offset intent. A 503 does not establish that no append occurred. New appends normally return HTTP 201; resolved retries return HTTP 200; conflicting IDs or offsets return HTTP 409.

## Snapshot reads and integrity

GET reads one current manifest, pins that snapshot, and fetches only its referenced segments. Concurrent appends cannot change the snapshot being streamed. Segment requests run with bounded ordered prefetch. Each selected segment is fully fetched, bounded, and verified against its stored SHA-256 and length before its bytes are emitted. Range requests slice those verified segment buffers, including ranges crossing segment boundaries.

This design trades some small-range read amplification for whole-segment verification: a range intersecting a segment still fetches that whole segment. The proxy returns a validator derived from the complete pinned manifest, handles `If-None-Match`, and ignores Range for HEAD. Multiple ranges and the wider S3 conditional/header surface are outside this release's HTTP contract. A later backend failure during an HTTP stream terminates the body; consumers must treat an incomplete body as a failed read.

## Backend requirements and primary sources

A compatible backend must provide strongly consistent manifest reads and atomic conditional creation/replacement. These requirements need validation for each S3-compatible provider.

| Requirement | AWS documentation |
| --- | --- |
| Conditional creation uses `If-None-Match: *`; replacement uses `If-Match` with the observed ETag. Conditional writes use Signature Version 4. | [Conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html) |
| A failed condition can return 412; concurrent conditional conflicts can return 409. Re-read current state before choosing the next logical append transition. | [PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html) and [conditional write behavior](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html#conditional-write-behavior) |
| S3 supplies strong read-after-write consistency and atomic single-key updates. Multi-object publication remains the proxy's responsibility. | [S3 consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel) |
| ETags have differing checksum semantics across upload and encryption modes. Preserve them as opaque conditional-write tokens and use a separate payload SHA-256. | [Object integrity and ETags](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html) |
| Native append applies to S3 Express One Zone directory-bucket objects, uses a matching write offset, and has a 10,000-part limit. | [Directory-bucket append](https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-objects-append.html) |

The production S3 driver uses `object_store` with ETag-match conditional writes. The service has no correctness-critical local lock or mutable manifest cache. Separate proxy processes serialize through the backend's single-key CAS. The in-memory mode is ephemeral and intended for the local demonstration.

## Operation cost and bounds

| Operation | Uncontended storage requests |
| --- | --- |
| New append | One manifest GET, one segment PUT, one conditional manifest PUT |
| Existing successful request retry | One manifest GET |
| Whole-object GET | One manifest GET plus one GET per referenced segment |
| Range GET | One manifest GET plus one full GET per intersecting segment |
| HEAD | One manifest GET |

Conflicts, ambiguous failures, and segment-existence verification add requests. Each append uploads only its new payload bytes. The v1 vector manifest is fully read, validated, cloned, and rewritten, making metadata work and metadata transfer O(number of committed appends). A single hot object also serializes at the manifest CAS. Benchmarks with the in-memory backend quantify local implementation overhead; they do not measure remote S3 service throughput or cost.

Defaults are 8 MiB per append, 4 MiB per manifest, 4,096 committed appends per virtual object, four prefetched segments, 32 application retry attempts, a 10-second I/O timeout, a 30-second append deadline, and 16 active HTTP requests. The manifest capacity is explicit: rotate to a new object when it fills. Existing matching receipts remain resolvable when capacity is reached. The entry limit and byte limit both apply.

Read memory scales with selected-segment prefetch and the maximum segment size, plus manifest, HTTP, and downstream buffers. Aggregate memory also scales with concurrent requests. POST bodies are bounded and buffered before upload. Configure concurrency and payload bounds together for the target device.

The implementation retains upload orphans. Age alone is insufficient to prove a segment is safe to reclaim: a stalled append may still publish it, and an existing reader may hold an older manifest snapshot. Future garbage collection needs a protocol for outstanding writers and readers. Future long-log scaling can use immutable paged indexes, retained idempotency indexes, and batched manifest commits, with separately verified publication and retention rules.

## Verification scope

The deterministic simulator explores storage-operation scheduling, CAS conflicts, response loss, and process crashes around the production planner. The suite should preserve every acknowledged append, prevent duplicate request effects, maintain contiguous offsets, and expose the deliberately unsafe unconditional-write mutant. HTTP integration exercises actual service routing, request limits, ranges, authentication, concurrent appends, and storage-driver behavior where configured.

Simulation establishes evidence under the modeled storage contract. Live-provider compatibility and real remote durability require the corresponding backend integration run. The Python and JavaScript examples demonstrate the language-neutral verification protocol using a separate synthetic state machine; they make no production S3 correctness claim.

## HTTP admission and stalled clients

POST body ingestion uses the configured I/O timeout (10 seconds by default). An incomplete body returns 408 before append processing starts. Admission uses immediate load shedding: saturation returns 503 with Retry-After, without an unbounded queue of semaphore waiters. GET streams retain their permit through completion or cancellation. A deployment edge should enforce downstream idle/write deadlines so a stalled reader cannot retain a bounded slot indefinitely. The append operation itself has a 30-second deadline and at most 32 attempts by default.
