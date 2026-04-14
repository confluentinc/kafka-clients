# Phase 7 Review -- Critic 0 -- Resolved

## Issue 1: ProducerMetadata.errors never populated in production metadata update path (RESOLVED)

- **File**: `src/clients/producer/producer_metadata.rs`
- **Severity**: Bug
- **Fix**: Changed `errors` field from `Mutex<...>` to `Arc<Mutex<...>>` and shared it with the `update_listener_fn` closure. The closure now stores `response.errors()` on every metadata update, regardless of whether the update comes through `update_with_current_request_version()` or directly through `Metadata.update()` (via `DefaultMetadataUpdater`). Removed the redundant error storage from `update_with_current_request_version()`.

## Issue 2: wait_on_metadata does not refresh topic expiry inside the loop (RESOLVED)

- **File**: `src/clients/producer/kafka_producer.rs`
- **Severity**: Behavior Mismatch
- **Fix**: Added `self.inner.metadata.add(topic, current_time_ms())` inside the loop body, after the partition-count check, matching Java's `metadata.add(topic, nowMs + elapsed)` at KafkaProducer.java:1127. This refreshes the topic's expiry timestamp on each iteration so it won't be evicted while waiting.

## Issue 3: No sender wakeup mechanism in wait_on_metadata (RESOLVED)

- **File**: `src/clients/producer/kafka_producer.rs`, `src/clients/producer/sender.rs`
- **Severity**: Behavior Mismatch
- **Fix**: Added `Arc<tokio::sync::Notify>` (`sender_wakeup`) shared between `KafkaProducer` and `Sender`. `wait_on_metadata()` now calls `self.inner.wakeup_sender()` after `request_update_for_topic()`, matching Java's `sender.wakeup()`. The sender's `run_once()` uses `tokio::select!` to wait on either `client.poll()` or the wakeup notify, so metadata requests are processed immediately rather than waiting for the current poll timeout to expire.
