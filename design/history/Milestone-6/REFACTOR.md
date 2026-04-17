# Plan: Make Producer trait methods async

## Context

The CLAUDE.md rule states: "If a method is blocking in Java it should be async in Rust." An audit found that all six `Producer` trait methods (`send`, `send_with_callback`, `flush`, `partitions_for`, `close`, `close_timeout`) are synchronous `fn` despite corresponding to blocking Java methods. Internally they use `Condvar::wait_timeout` (metadata) and `tokio::task::block_in_place` + `Handle::block_on` bridges (flush, close). This refactor converts them to `async fn` throughout the stack.

Additionally, the FFI layer currently creates throwaway tokio runtimes for `FutureRecordMetadata_get`/`get_all`. The user requires a single tokio runtime per client (producer).

## Files to Modify

1. `src/metadata.rs` — Replace `Condvar` with `tokio::sync::Notify` in `await_update`
2. `src/producer/producer_trait.rs` — All 6 methods become `async fn`
3. `src/producer/kafka_producer.rs` — Async impl + internal helpers (`do_send`, `wait_on_metadata`, `await_sender_handle*`)
4. `src/producer/mock_producer.rs` — Async impl (bodies stay synchronous)
5. `src/ffi/producer.rs` — Bridge async→sync via `runtime.block_on()`, single runtime per producer, `FfiFuture` wraps runtime handle
6. `tests/producer/mock_producer_test.rs` — `#[test]` → `#[tokio::test]` + `.await`
7. `tests/integration/producer_test.rs` — Add `.await` to producer method calls

## Implementation Steps

### Step 1: Metadata — Replace Condvar with Notify

**File: `src/metadata.rs`**

- Replace `use std::sync::Condvar` with `use tokio::sync::Notify` (line 29)
- Replace field `update_condvar: Condvar` → `update_notify: Notify` (line 106)
- Update both constructors (`new` ~line 283, `with_overrides` ~line 341): `Condvar::new()` → `Notify::new()`
- Convert `await_update` (line 1115) from `pub fn` to `pub async fn`:
  ```rust
  pub async fn await_update(&self, last_version: i32, timeout_ms: i64) -> Result<(), KafkaError> {
      let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms as u64);
      loop {
          // Must create notified() BEFORE checking condition to avoid race
          let notified = self.update_notify.notified();
          {
              let inner = self.inner.lock().unwrap();
              if inner.update_version > last_version {
                  return Ok(());
              }
          }
          let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
          if remaining.is_zero() {
              // Final check under lock
              let inner = self.inner.lock().unwrap();
              if inner.update_version > last_version { return Ok(()); }
              return Err(KafkaError::timeout(...));
          }
          if tokio::time::timeout(remaining, notified).await.is_err() {
              let inner = self.inner.lock().unwrap();
              if inner.update_version > last_version { return Ok(()); }
              return Err(KafkaError::timeout(...));
          }
      }
  }
  ```
- Replace `self.update_condvar.notify_all()` → `self.update_notify.notify_waiters()` at lines 681 and 1163
- In `close()` method (line 1157): same replacement

### Step 2: KafkaProducer internal helpers → async

**File: `src/producer/kafka_producer.rs`**

- `wait_on_metadata` (line 588): `fn` → `async fn`. Only change: `self.metadata.await_update(version, remaining_wait_ms)` becomes `.await`
- `do_send` (line 458): `fn` → `async fn`. Only change: `self.wait_on_metadata(...)` becomes `.await`
- `await_sender_handle` (line 768): `fn` → `async fn`. Remove `block_in_place`/`block_on`. Body becomes:
  ```rust
  async fn await_sender_handle(&self, timeout: Duration) -> bool {
      let handle = self.sender_handle.lock().unwrap().take();
      match handle {
          None => true,
          Some(join_handle) => tokio::time::timeout(timeout, join_handle).await.is_ok(),
      }
  }
  ```
- `await_sender_handle_indefinitely` (line 783): `fn` → `async fn`. Remove `block_in_place`/`block_on`. Body: `if let Some(jh) = handle { let _ = jh.await; }`
- Remove `use tokio::task::block_in_place` if it becomes unused (check)

### Step 3: Producer trait → async

**File: `src/producer/producer_trait.rs`**

All 6 methods become `async fn`. No other trait changes. Native async fn in traits is supported (edition 2024). No `dyn Producer` usage exists.

### Step 4: KafkaProducer Producer impl → async

**File: `src/producer/kafka_producer.rs`** (lines 793-905)

- `send` → `async fn`, body: `self.do_send(record, None).await`
- `send_with_callback` → `async fn`, body: `self.do_send(record, callback).await`
- `flush` → `async fn`. Remove all `block_in_place`/`Handle::try_current`/`block_on` bridging. Body:
  ```rust
  async fn flush(&self) -> Result<(), KafkaError> {
      self.accumulator.begin_flush();
      self.wakeup.notify_one();
      self.accumulator.await_flush_completion().await;
      Ok(())
  }
  ```
- `partitions_for` → `async fn`, add `.await` to `self.wait_on_metadata(...)`
- `close` → `async fn`, add `.await` to `self.close_timeout(...)`
- `close_timeout` → `async fn`, add `.await` to `self.await_sender_handle(...)` and `self.await_sender_handle_indefinitely()`. Remove blocking bridge comments.

### Step 5: MockProducer Producer impl → async

**File: `src/producer/mock_producer.rs`** (lines 288-381)

All 6 methods become `async fn`. Bodies stay synchronous (pure Mutex operations).
- `send` → `async fn`, calls `self.send_with_callback(record, None).await`
- `send_with_callback` → `async fn`, same body (no `.await` inside)
- `flush` → `async fn`, same body
- `partitions_for` → `async fn`, same body
- `close` → `async fn`, same body
- `close_timeout` → `async fn`, calls `self.close().await`

### Step 6: FFI layer — single runtime per producer

**File: `src/ffi/producer.rs`**

**6a. Add runtime to MockProducer variant:**
```rust
enum ProducerKind {
    Mock(MockProducer<Vec<u8>, Vec<u8>>, tokio::runtime::Runtime),
    Kafka(KafkaProducer<Vec<u8>, Vec<u8>>, tokio::runtime::Runtime),
}
```

Add helper:
```rust
impl ProducerKind {
    fn runtime(&self) -> &tokio::runtime::Runtime {
        match self {
            ProducerKind::Mock(_, rt) | ProducerKind::Kafka(_, rt) => rt,
        }
    }
}
```

**6b. Update MockProducer_new** (line 356): Create `current_thread` runtime.

**6c. Introduce `FfiFuture` struct** to pair future with runtime handle:
```rust
struct FfiFuture {
    future: KafkaFuture<RecordMetadata>,
    runtime_handle: tokio::runtime::Handle,
}
```

Update `box_future` to accept runtime handle. Update `future_ref` to return `&FfiFuture`. Update all destroy functions to drop `Box<FfiFuture>`.

**6d. Update `producer_send`** — use `runtime.block_on(producer.send(record))`:
```rust
fn producer_send(kind: &ProducerKind, record: ...) -> Result<...> {
    match kind {
        ProducerKind::Mock(mock, rt) => rt.block_on(mock.send(record)),
        ProducerKind::Kafka(producer, rt) => rt.block_on(producer.send(record)),
    }
}
```

**6e. Update `Producer_send`** — pass runtime handle to `box_future`:
```rust
box_future(future, guard.runtime().handle().clone())
```

Same for `send_batch_inner`.

**6f. Update `Producer_flush`** — `rt.block_on(mock.flush())` / `rt.block_on(kafka.flush())`

**6g. Update `Producer_close`** — `rt.block_on(mock.close())` / `rt.block_on(kafka.close())`. Remove `runtime.enter()` pattern.

**6h. Update `FutureRecordMetadata_get`** — remove temporary runtime. Use `f.runtime_handle.block_on(f.future.get())`.

**6i. Update `FutureRecordMetadata_get_all`** — remove temporary runtime. Each future carries its own handle.

**6j. Update `FutureRecordMetadata_is_done`** — access `f.future.is_done()`.

**6k. Update mock-specific ops** (`complete_next`, `error_next`, `history_count`, `clear`) — these access `Mock(mock, _rt)` now.

### Step 7: Update all tests

**Tests that need `#[test]` → `#[tokio::test]` and `.await`:**

`src/producer/mock_producer.rs` internal tests:
- `should_throw_on_send_if_producer_is_closed`, `should_throw_on_flush_if_producer_is_closed`
- `should_be_flushed_with_auto_complete_if_buffered_records`, `should_not_be_flushed_with_no_auto_complete_if_buffered_records`, `should_be_flushed_after_flush`
- `test_default`, `test_set_send_error`, `test_set_flush_error`, `test_set_partitions_for_error`, `test_set_close_error`
- `test_partitions_for`, `test_close_timeout`, `test_history_returns_clone`

`src/producer/kafka_producer.rs` internal tests:
- `test_close_should_be_idempotent`, `test_close_with_zero_timeout`, `test_partitions_for_returns_partitions`
- `test_send_after_close_returns_error`, `test_send_appends_to_accumulator`, `test_flush_with_no_pending_records`
- All `wait_on_metadata` tests, and any test calling `send`/`flush`/`close`/`partitions_for`/`close_timeout`

**Tests already `#[tokio::test]` that need `.await` added:**

`src/producer/mock_producer.rs`: All `#[tokio::test]` tests calling `send()`, `flush()`, `close()`
`src/producer/kafka_producer.rs`: All `#[tokio::test]` tests calling trait methods
`tests/producer/mock_producer_test.rs`: Add `.await` to `send()`, `flush()`, `close()` calls; convert `#[test]` to `#[tokio::test]` where needed
`tests/integration/producer_test.rs`: Add `.await` to all `producer.send()`, `producer.flush()`, `producer.close()` calls

**FFI tests (`src/ffi/producer.rs`):** No changes needed — they call `extern "C"` functions which remain synchronous.

## Key Design Decisions

1. **`tokio::sync::Notify` over `watch`** — maps directly to Java's `wait()/notifyAll()` pattern. The `Mutex<MetadataInner>` is held only briefly (no `.await` while locked).
2. **Race-free Notify pattern** — `let notified = notify.notified()` must be created *before* checking the version under lock, to avoid missing notifications between check and await.
3. **Single runtime per producer** — Both `ProducerKind::Mock` and `::Kafka` carry a `tokio::runtime::Runtime`. Futures store `Handle` for `get()`/`get_all()`.
4. **No `dyn Producer`** — Native `async fn` in traits works (edition 2024, no `async_trait` crate needed).
5. **FFI `block_on` safety** — The FFI calls `runtime.block_on(...)` from the C caller's thread (outside the runtime). The sender task runs on a separate runtime thread. No deadlock risk.
6. **`Drop` unchanged** — `KafkaProducer::Drop` calls `force_close()` which is synchronous (sets atomics, notifies). Does NOT call the async `close()`.

## Verification

1. `cargo build --features ffi` succeeds
2. `cargo test --features ffi` passes (unit + FFI + mock + integration + doctests)
3. `cargo xtask format-check` passes
4. `cargo xtask lint` passes
5. C tests build and pass: `cd bindings/c/build && cmake .. && make && ./test_mock_producer && ./test_kafka_producer`
