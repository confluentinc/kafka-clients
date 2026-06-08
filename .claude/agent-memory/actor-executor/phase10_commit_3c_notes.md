---
name: phase10-commit-3c-notes
description: Phase 10 (3c/N) — OffsetsRequestManager::fetch_offsets + cluster-listener replay; shared-inner pattern, deferred replay via flag to avoid metadata reentrant-lock deadlock
metadata:
  type: project
---

Phase 10 commit 3c (Milestone-8) translated
`OffsetsRequestManager.fetchOffsets(Map<TopicPartition, Long>, boolean)`
plus the cluster-listener replay path
(`OffsetsRequestManager.onUpdate(ClusterResource)`). Reusable patterns:

## Pattern 1: shared-inner `Arc<OffsetsManagerShared>` for `&self` listener access

The `ClusterResourceListener` trait gives `on_update(&self, ...)`. Anything
the listener needs to mutate must live in `Arc<Mutex<...>>`. To match
Java's `OffsetsRequestManager implements ClusterResourceListener` pattern
without putting every manager field behind a mutex, I split the manager:

- `OffsetsRequestManager` — owns `&mut self` work (drain pending
  completions, drain pending followups, signal_close).
- `Arc<OffsetsManagerShared>` — fields read by the listener and by
  spawned per-response forwarder tasks (`metadata`, `subscription_state`,
  `isolation_level`, `request_timeout_ms`, `offset_fetcher_utils`,
  `pending_completions_tx`, `Mutex<Vec<UnsentRequest>> requests_to_send`,
  `Mutex<Vec<Arc<Mutex<ListOffsetsRequestState>>>> requests_to_retry`,
  `AtomicBool metadata_updated`).

The manager holds `shared: Arc<OffsetsManagerShared>` and exposes the
shared fields via `self.shared.foo`.

## Pattern 2: defer listener work via `AtomicBool` flag — Mutex reentrancy

**Critical Rust-vs-Java pitfall**: Java's `ClusterResourceListener.onUpdate`
is fired from inside `Metadata::update` while holding `Metadata`'s lock.
Java `synchronized` is reentrant, so the listener can call
`metadata.currentLeader(tp)` (which re-acquires the same lock) without
deadlock. Rust `std::sync::Mutex` is NOT reentrant — calling
`metadata.current_leader(tp)` from inside `on_update` deadlocks.

**Solution**: the listener sets an `AtomicBool metadata_updated` flag and
returns. The actual replay (which calls `metadata.current_leader(tp)`) is
deferred to the next `poll()` call, which holds no metadata locks.

Observable behaviour matches Java: the retried requests appear on
`requests_to_send` *before the next network poll*, so any caller that
observes through the network-poll surface sees the same sequence.

Test accessors (`requests_to_send_count`, `requests_to_retry_count`)
also drain the flag so tests asserting immediately after `metadata.update()`
work — the cost is a `swap` per accessor call, which is negligible.

## Pattern 3: `ListOffsetsRequestState` accumulator with single Arc<Mutex>

Java's per-fetch state has the global `CompletableFuture` and a
multi-node `expectedResponses` counter. Rust collapses both onto the
state struct itself and tracks waiters as `Vec<oneshot::Sender<...>>`.

The waiters slot allows the (future) callers that piggy-back on the
same in-flight `ListOffsets` to be added — though for `fetch_offsets`
there's no piggy-back path (each call is independent), the
`Vec<Sender>` shape mirrors the `PendingFetchCommittedRequest` from
commit 3a so future composition is uniform.

Once the global outcome is routed (`completed = true`), further
`apply_partial_result` calls are silently dropped — mirrors
`CompletableFuture::complete` idempotency.

## Pattern 4: `ConcreteRequest::ListOffsets` downcast for test assertions

Tests asserting on the wire request properties (e.g. `timeout_ms`) need
to downcast from the `RequestBuilder` trait. The `RequestBuilder` trait
does NOT expose `as_any` — so the test calls `builder.build()` (returns
`ConcreteRequest`) and matches on `ConcreteRequest::ListOffsets(r)`.

## Pattern 5: `Option<OffsetAndTimestamp>` deviation from Java's
`OffsetAndTimestampInternal`

Java has a separate `OffsetAndTimestampInternal` type that permits
negative offsets/timestamps (the broker may omit timestamps when
querying for `EARLIEST_TIMESTAMP` / `LATEST_TIMESTAMP`). Rust uses the
public `OffsetAndTimestamp` (enforces non-negative) wrapped in `Option`.
A negative timestamp from the broker maps to `None`.

**Test impact**: tests can't assert on the offset value when timestamp
is -1; they need to supply a positive timestamp (e.g. 100) in the
synthetic response so the OAT construction succeeds. Java tests use
timestamp=-1 freely because their internal type accepts it.

## Pattern 6: metadata test bootstrap requires `add_transient_topics` first

`ConsumerMetadata::retain_topic_fn` filters topics out of the cluster
snapshot unless they're in the transient set, the subscription, or
matched by a pattern. Test helpers that bootstrap metadata for a topic
that's not subscribed must first call `metadata.add_transient_topics()`
then `metadata.update_with_current_request_version(...)`.

## What's deferred to 3d (and beyond)

- `try_connect` plumbing for `validate_positions_if_needed`. Currently
  a debug log fires when `NodeApiVersions` are missing; the bg task
  (Phase 10 commit 4+) will own the network client and needs to expose
  a `try_connect` surface on `PollResult` or as a separate hint.
- `clear_transient_topics` on `fetch_offsets` completion — Java does
  this inside the `whenComplete` chain. The Rust translation skips it;
  the topic just remains in the transient set one refresh longer, which
  is benign (the next metadata refresh covers it anyway).
- Multi-node `fetch_offsets` is supported by the code shape but not
  exercised by tests (Java's `testRequestPartiallyFailsWithRetriableError_RetrySucceeds`
  uses two brokers). The accumulator path handles it; defer the
  explicit test to a later phase if there's tooling appetite.
