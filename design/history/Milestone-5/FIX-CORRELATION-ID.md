# Fix Correlation ID Mismatch Bug

## Context

The Rust producer consistently fails with:
```
Correlation id for response (442) does not match request (441)
```
Every response's correlation ID is exactly +1 ahead of the expected request, cascading for all subsequent requests. This is a data-loss bug caused by unsafe future cancellation in the sender's poll loop.

## Root Cause

In `Sender::run_once()` (`src/producer/internals/sender.rs:224-227`):
```rust
let responses = tokio::select! {
    responses = self.client.poll(poll_timeout, current_time_ms) => responses,
    _ = self.wakeup.notified() => Vec::new(),
};
```

When the producer wakes up the sender (new batch, flush, close), `tokio::select!` cancels the `client.poll()` future mid-execution. If `selector.poll()` has already read a response from the socket into `completed_receives` but `handle_completed_receives` hasn't run yet, the response is lost:

1. `selector.poll()` reads response N from socket → adds to `completed_receives`
2. Wakeup fires → `tokio::select!` drops the `client.poll()` future
3. `handle_completed_receives` never runs → in-flight entry for request N stays
4. Next `client.poll()` → `selector.clear()` discards the unprocessed response N
5. Response N+1 arrives → `complete_next` pops request N → correlation ID mismatch (N+1 ≠ N)

This doesn't happen in Java because `Sender.runOnce()` calls `client.poll()` synchronously — no cancellation is possible. Java's `wakeup()` goes through `nioSelector.wakeup()` which makes `select()` return immediately but the poll processing still completes.

## Fix: Three coordinated changes

### Step 1: Selector — break on wakeup (`src/common/network/selector.rs`)

**File:** `src/common/network/selector.rs:813-829`

Change the notify branch in the poll loop to `break` instead of continuing. This matches Java's `Selector.wakeup()` contract: the poll returns immediately.

```rust
// BEFORE:
tokio::select! {
    biased;
    _ = notify.notified() => {},        // continues loop
    _ = select_all(readiness_futs) => {},
    _ = tokio::time::sleep_until(dl) => {},
}

// AFTER:
tokio::select! {
    biased;
    _ = notify.notified() => { break; },  // wakeup → return immediately
    _ = select_all(readiness_futs) => {},  // I/O ready → continue loop
    _ = tokio::time::sleep_until(dl) => { break; },  // timeout → return
}
```

Also add a public method to expose the notify handle:
```rust
pub fn wakeup_notify(&self) -> Arc<Notify> {
    self.notify.clone()
}
```

### Step 2: Wire wakeup through NetworkClient/KafkaClient

**Files:**
- `src/common/network/selector.rs` — add `wakeup_notify()` method (see above)
- `src/network_client.rs` — add `wakeup_notify()` that delegates to `selector.wakeup_notify()`
- `src/kafka_client.rs` — add `wakeup_notify()` to the `KafkaClient` trait returning `Arc<Notify>`

### Step 3: Producer/Sender — unify wakeup, remove tokio::select!

**File:** `src/producer/kafka_producer.rs:360`

In `with_client()`, get the selector's notify BEFORE moving the client into the Sender:
```rust
// BEFORE:
let wakeup = Arc::new(Notify::new());

// AFTER:
let wakeup = client.wakeup_notify();
```

**File:** `src/producer/internals/sender.rs:223-227`

Remove the `tokio::select!` and just await `client.poll()` directly:
```rust
// BEFORE:
let responses = tokio::select! {
    responses = self.client.poll(poll_timeout, current_time_ms) => responses,
    _ = self.wakeup.notified() => Vec::new(),
};

// AFTER:
let responses = self.client.poll(poll_timeout, current_time_ms).await;
```

The `wakeup` field on the Sender struct is no longer needed (the producer's wakeup goes directly to the selector). Remove it from the Sender struct and constructor. Keep the `wakeup()` method which calls `self.client.wakeup()`.

**File:** `src/producer/kafka_producer.rs`

All existing `self.wakeup.notify_one()` calls (lines 603, 699, 820, 935) continue to work unchanged — they now trigger the selector's notify directly, causing `selector.poll()` to return and `client.poll()` to finish normally.

### Critical files to modify

1. `src/common/network/selector.rs` — break on notify, add `wakeup_notify()`
2. `src/network_client.rs` — add `wakeup_notify()` delegation
3. `src/kafka_client.rs` — add `wakeup_notify()` to trait
4. `src/producer/kafka_producer.rs` — use client's notify as wakeup
5. `src/producer/internals/sender.rs` — remove `tokio::select!`, remove `wakeup` field

### Flow after fix

1. Producer appends batch → `wakeup.notify_one()` (same Arc<Notify> as selector's)
2. Selector's poll loop: `notify.notified()` fires → **breaks** out of loop
3. `selector.poll()` returns normally
4. `client.poll()` continues → `handle_completed_sends/receives` processes all I/O
5. `client.poll()` returns → `run_once()` returns
6. Sender loop calls `run_once()` again → `send_producer_data()` picks up new batches

## Verification

1. `cargo build`
2. `cargo test`
3. `make verify` — format, lint, all tests including Python/C bindings
4. Performance test: `TEST_DURATION_SECONDS=10 CLIENT_VERSION=3 python3 bindings/python/test/performance/producer_performance_test.py`
5. Verify no correlation ID mismatch errors in the log output during the performance test
