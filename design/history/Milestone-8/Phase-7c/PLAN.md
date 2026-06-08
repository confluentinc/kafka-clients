# Phase 7c: `TopicMetadataRequestManager`

## Goal

Translate the request manager that serves the consumer's `list_topics()`
and `partitions_for(topic)` API calls by issuing `MetadataRequest`
requests against the cluster and routing the responses back through
oneshot futures.

This is the smallest of the three parallel 7b/c/d sub-phases — single
production file (~283 LOC) + single test file (~305 LOC).

## Branch / worktree

Runs on a **worktree** of `consumer-impl` for parallel execution with
Phase 7b and 7d. Base SHA: `14f18b7` (Phase 7a closed).

## Java sources

- `org/apache/kafka/clients/consumer/internals/TopicMetadataRequestManager.java` (283)

Tests:

- `clients/consumer/internals/TopicMetadataRequestManagerTest.java` (305)
  → inline `#[cfg(test)] mod tests` (Phase 4/5/6/7a precedent for
  `pub(crate)` types)

## Out of scope

- **`TopicMetadataFetcher.java`** (167 LOC) — used only by
  `ClassicKafkaConsumer`. Out per `consumer-threading.md` §20.
- **`TopicMetadataFetcherTest.java`** (260 LOC) — same.
- **Metrics / Sensor / ClientTelemetry parameters** — no Rust analog.

## Module structure produced by this phase

```
src/consumer/internals/
└── topic_metadata_request_manager.rs   # NEW
```

`src/consumer/internals/mod.rs` gets a new `pub(crate) mod` line.
Phase 6's `RequestManagers` skeleton gets one new `Option<TopicMetadataRequestManager>`
slot — the comment-placeholder already exists per Phase 6 #5.

## Type-by-type spec

### `TopicMetadataRequestManager` (`src/consumer/internals/topic_metadata_request_manager.rs`)

`pub(crate)`. Java: `TopicMetadataRequestManager.java:53-283`.

```rust
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::oneshot;

use crate::common::{KafkaError, PartitionInfo};
use crate::consumer::internals::network_client_delegate::{PollResult, UnsentRequest};
use crate::consumer::internals::request_manager::RequestManager;
use crate::consumer::internals::request_state::RequestState;
use crate::consumer::ConsumerConfig;

pub(crate) struct TopicMetadataRequestManager {
    /// Pending requests, keyed by topic (Some(topic)) or None (all topics).
    inflight_requests: Vec<TopicMetadataRequestState>,
    allow_auto_topic_creation: bool,
    request_timeout_ms: i32,
    closing: bool,
}

struct TopicMetadataRequestState {
    topic: Option<String>,                // None = all topics
    request_state: RequestState,
    ack: oneshot::Sender<Result<HashMap<String, Vec<PartitionInfo>>, KafkaError>>,
    deadline_ms: i64,
}

impl TopicMetadataRequestManager {
    pub(crate) fn new(config: &ConsumerConfig) -> Self;

    /// Java: `requestTopicMetadata(Optional<String> topic, long expirationTimeMs)`.
    /// Enqueue a new metadata request, return a Receiver the caller awaits.
    pub(crate) fn request_topic_metadata(
        &mut self,
        topic: Option<String>,
        deadline_ms: i64,
    ) -> oneshot::Receiver<Result<HashMap<String, Vec<PartitionInfo>>, KafkaError>>;

    /// Java: `onResponse(...)`. Called by the bg task when a metadata
    /// response arrives.
    pub(crate) fn on_response(
        &mut self,
        request_index: usize, // or some other handle
        current_time_ms: i64,
        response: MetadataResponse,
    );
    pub(crate) fn on_failure(&mut self, request_index: usize, current_time_ms: i64, error: KafkaError);
}

impl RequestManager for TopicMetadataRequestManager {
    fn poll(&mut self, current_time_ms: i64) -> PollResult {
        if self.closing { return PollResult::empty(); }
        // Walk inflight_requests; for each that can_send_request, build a
        // MetadataRequest::Builder and create an UnsentRequest. Return them.
    }

    fn signal_close(&mut self) { self.closing = true; }
}
```

Notes:

- **`MetadataRequest::Builder`** already exists from the producer-side
  translation (`src/common/requests/metadata_request.rs`). Reuse — the
  consumer's metadata-request shape is the same.
- **`request_topic_metadata`** returns a `oneshot::Receiver` directly,
  not the `CompletableEventHandle<T>` pattern. The Phase 5
  `ApplicationEvent::TopicMetadata` / `AllTopicsMetadata` variants
  already carry the handle; this manager is the bg-task-side endpoint
  that fulfills them.
- **`inflight_requests: Vec<TopicMetadataRequestState>`** — not a
  HashMap, because multiple concurrent requests for the same topic
  must each get their own response. Java uses `LinkedList<TopicMetadataRequestState>`.
- **Per-request `RequestState`** — independent backoff per request
  (Java has this; mirrors classic-protocol heartbeat retry semantics).
- **Response dispatch**: when a response arrives, find the matching
  inflight request (Java tracks by request-ID; Rust can use index or
  a request handle). On success, complete the oneshot with the
  partition info. On failure, complete with `KafkaError`. On retriable
  failure (e.g. `LEADER_NOT_AVAILABLE`), update `RequestState` and
  re-poll.
- **`allow_auto_topic_creation`**: from `ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG`.
  Threaded through to `MetadataRequest::Builder`.

## Cross-cutting requirements

- **License header**: Apache 2.0 (CLAUDE.md §7).
- **No `#[async_trait]`** per DoD §11.
- **No `panic!` / `unimplemented!` / `todo!`** in production code.
- **No new dependencies**.
- **`oneshot::Sender` idempotent completion** mirrors Phase 5's
  `CompletableEventHandle` Arc-Mutex-Option pattern. Same shape:
  `inflight_request.ack.take()` inside a lock guard, then send
  outside.

## Verification

1. `cargo build` clean on the 7c worktree
2. `cargo test --lib` — 1208 baseline + new tests
3. `cargo test --test consumer` — 36 baseline holds
4. `cargo xtask format-check` clean
5. `cargo xtask lint` clean
6. `cargo test --lib -- --test-threads=1` no hangs

## Commit plan

Suggested:

1. `Phase 7c (1/2): TopicMetadataRequestManager + RequestState dispatch`
2. `Phase 7c (2/2): TopicMetadataRequestManagerTest translation`

## Workflow

Same Actor → Critic loop on the 7c worktree. Comments at
`design/history/Milestone-8/Phase-7c/COMMENTS.1.md`. After close, merge
back to `consumer-impl`.
