# Root cause: spurious NetworkException from poll() during rebalance

> Status: fixed in this branch. Two follow-ups remain open — see "Remaining work".

## Summary

`AsyncKafkaConsumer::poll()` can return a spurious, retriable
`Errors::NetworkException` during a KIP-848 rebalance, even though nothing
disconnected. The Java client cannot produce this error at this point, so any
application that treats a `poll()` error as fatal will see a phantom failure.

Observed rate: **~9% of isolated runs** of an affected test; 3 occurrences across
one 25-run full-suite sweep.

## Affected tests (both intermittent)

- `plaintext_consumer_callback_test::test_on_partitions_assigned_called_with_new_partitions_only`
- `plaintext_consumer_subscription_test::test_async_consumer_re2j_pattern_expand_subscription`

```
poll should succeed: Generic(KafkaGenericError { error: NetworkException, custom_message: None, fatal: false })
```

## Root cause

`OffsetsRequestManager` keeps a single in-flight committed-offset fetch so that
short `poll()` timeouts don't spam `OffsetFetch` RPCs. Reuse requires an exact
partition-set match (mirroring Java's
`canReusePendingOffsetFetchEvent`, which is `requestedPartitions.equals(partitions)`).

The Rust port stores the *waiting callers* inside that replaceable slot:

```rust
struct PendingFetchCommittedRequest {
    requested_partitions: HashSet<TopicPartition>,
    waiters: Vec<oneshot::Sender<Result<(), KafkaError>>>,   // inside the replaceable slot
}
```

When KIP-848 reconciliation changes the assignment, `initializing_partitions`
differs between two overlapping `AsyncPollEvent`s, the equality check fails, and
`*guard = Some(PendingFetchCommittedRequest { .. })` replaces the slot — dropping
the previous request's senders. The orphaned receivers `RecvError`, and the code
converted that purely in-process channel drop into a fabricated wire error:

```rust
Err(_) => Err(KafkaError::new(Errors::NetworkException)),
```

`src/consumer/internals/offsets_request_manager.rs`, two sites (in
`init_with_committed_offsets_if_needed`'s driver task and in
`spawn_committed_offsets_followup`).

## Why the error variant made it user-visible

```
RecvError
  -> fabricated NetworkException
  -> cached in cached_update_positions_exception  (Java: cacheExceptionIfEventExpired)
  -> applied to the next live AsyncPollEvent
  -> is_ignorable_async_poll_error(err)?
  -> AsyncPollState::complete_exceptionally
  -> poll() returns Err
```

The gate ignores only timeouts:

```rust
fn is_ignorable_async_poll_error(err: &KafkaError) -> bool {
    matches!(err, KafkaError::Timeout(_))
}
```

That is a faithful translation of Java's
`ApplicationEventProcessor.maybeCompleteAsyncPollEventExceptionally`, which also
ignores only `TimeoutException`. The predicate was correct; the error handed to
it was not. Note that `NetworkException.is_retriable()` is true but irrelevant —
nothing on this path consults it.

## Why Java is structurally immune

`OffsetsRequestManager.initWithCommittedOffsetsIfNeeded` (Kafka 4.2,
`a18251bae0b825c69794a50dffd4c3100cf5ca5b`):

```java
CompletableFuture<...> fetchOffsetsAndRefresh = fetchOffsets.whenComplete((offsets, error) -> {
    pendingOffsetFetchEvent = null;
    refreshOffsets(offsets, error, result);      // completes THIS caller's future
});
pendingOffsetFetchEvent = new PendingFetchCommittedRequest(initializingPartitions, fetchOffsetsAndRefresh);
```

Each caller's `result` is completed by a `whenComplete` chain attached to
`fetchOffsets`, independent of the `pendingOffsetFetchEvent` field. Reassigning
the field swaps only the reuse-lookup entry; the earlier caller's continuation is
still attached to a live future, which
`CommitRequestManager.fetchOffsetsWithRetries` retries for every
`RetriableException` until `fetchCommittedDeadlineMs` and then wraps via
`maybeWrapAsTimeoutException`. A superseded Java caller therefore sees **success
or `TimeoutException`** — and `TimeoutException` is exactly what the async-poll
layer swallows.

There is no orphaned-waiter state in Java. The in-tree comment claiming "Java
would never complete the future" is incorrect.

## Fix applied

Both sites now complete the orphaned waiter with the outcome Java produces for an
abandoned fetch:

```rust
fn superseded_committed_fetch_error() -> KafkaError {
    KafkaError::timeout("Committed-offset fetch was superseded before it completed")
}
```

This routes into the existing ignore-and-retry path (CLAUDE.md §5 is still
satisfied — the caller is completed, not left hanging). `poll()` returns empty
records and the next `update_fetch_positions` re-initializes.

Verification: 14/14 iterations of both affected tests (vs ~9% failure before),
plus a unit test asserting the exact message
(`superseded_committed_offset_fetch_completes_with_timeout_not_network_error`).

Three other `NetworkException`-on-dropped-sender sites in the same file were
deliberately left unchanged — they await real network responses via
`UnsentRequest::take_response_receiver()`, where a dropped sender genuinely is a
transport failure (Java's `DisconnectException`).

## Remaining work (not addressed)

1. **The orphaning itself.** Waiters still live in the replaceable slot, so a
   superseded request's partitions are not initialized in that cycle. It
   self-heals via repeated `poll()`, but diverges from Java, where the old chain
   still runs `refreshOffsets`. Java parity: hoist the waiter list out of the
   slot (e.g. `Arc<Mutex<Vec<Sender>>>` owned by the driver task) so replacement
   drops only the lookup entry.

2. **Unconditional `guard.take()`** in the driver task: after a slot
   replacement, a *stale* fetch's completion takes and resolves the *newer*
   event's waiters with the stale result. Java clears
   `pendingOffsetFetchEvent` the same way, but its newer caller is completed only
   by its own chain, so there is no cross-contamination. The take should be
   conditional on the slot still holding this task's own generation.
