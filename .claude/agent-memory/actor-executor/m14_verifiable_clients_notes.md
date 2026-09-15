---
name: m14-verifiable-clients-notes
description: Milestone 14 verifiable-clients tools crate — VerifiableProducer/Consumer translation gotchas (listener/callback Arc split, Box<dyn Consumer>, StringDeserializer placement)
metadata:
  type: project
---

Milestone 14 = Rust translations of Kafka's system-test tools in the `tools/verifiable-clients/` workspace crate (deliberate one-off scope exception to "client only"). Phase 1 = VerifiableProducer + ThroughputThrottler; Phase 2 = VerifiableConsumer + StringDeserializer.

**Why / how to apply:**

- **JSON stdout IS the wire.** Every event mirrors Java's Jackson output (event `name`, field names, order). Pin with exact-string `serde_json::to_string` vector tests; overwrite `timestamp` to 42 first. Java's base `@JsonPropertyOrder({"timestamp","name"})` → each event struct declares timestamp, name first. Getter-derived names are camelCase → `#[serde(rename="minOffset")]` etc. `@JsonInclude(NON_NULL)` → `#[serde(skip_serializing_if="Option::is_none")]` (only OffsetsCommitted.error). TopicPartition custom serializer = `{"topic":..,"partition":..}` → dedicated `PartitionJson` helper (do NOT rely on the client's `TopicPartition` Serialize).

- **Listener/callback Arc split (Rust-necessitated, not in Java).** Java `VerifiableConsumer implements OffsetCommitCallback, ConsumerRebalanceListener` — one object is driver + callback. In Rust the driver needs `&mut self` (poll/commit/close) but `subscribe_with_listener`/`commit_async_offsets_with_callback` take `Arc<dyn …>`. So split off a stateless `EventReporter` unit struct that impls both traits (they only print JSON). `VerifiableConsumer::commit_sync` calls `self.reporter.on_complete(...)` directly (matches Java calling onComplete). Document as a justified deviation (DoD §7).

- **`Box<dyn Consumer<String,String>>` as a field, NOT a generic** (unlike VerifiableProducer's `P: Producer`). The consumer trait is `#[async_trait]` → dyn-compatible; the producer trait uses native async fn → not dyn-compatible (that's why the producer had to be generic). Tests box a `MockConsumer`.

- **StringDeserializer** goes in the MAIN crate (`src/common/serialization/string_deserializer.rs`, genuine client class), UTF-8 only via `String::from_utf8_lossy` (= Java `new String(bytes, UTF_8)` which replaces malformed with U+FFFD). Re-exported from `common::serialization`; replaced the inline copy in `src/bin/consumer_test.rs`.

- **commit_sync wakeup recursion** (Java 215-228): translate literally with `Box::pin(self.commit_sync(offsets)).await?` (async recursion needs the box), then `Err(wakeup)` after the retry. FencedInstanceId → rethrow; other → onComplete(Some(e)) + Ok. Detect via `matches!(e, Error::Wakeup(_))` / `Error::FencedInstanceId(_)`.

- **Shutdown**: no `shutdownLatch` needed — `#[tokio::main]` awaits `run()`. signal task holds `consumer.handle()` (ConsumerHandle, cross-task wakeup) + `consumer.reporter()`; on signal prints `shutdown_requested` + `handle.wakeup()` (= Java close()); run()'s finally closes + prints `shutdown_complete`.
  - **MUST wait on SIGINT OR SIGTERM, not ctrl_c() alone** (Critic P2-1). ducktape's clean shutdown of a verifiable client sends **SIGTERM by default** (`kafka/tests/kafkatest/services/verifiable_client.py` `kill_signal` = `signal.SIGTERM`) then waits for `shutdown_complete`; SIGINT-only makes SIGTERM kill the process abruptly → harness hangs. Java's JVM hook fires on both. Shared helper `verifiable_clients::wait_for_shutdown_signal()` (lib.rs) races `ctrl_c()` vs `tokio::signal::unix::signal(SignalKind::terminate())` in a `select!`; SIGTERM arm is `#[cfg(unix)]` only (falls back to ctrl_c on non-Unix, and if SIGTERM can't register). Both bins call it. Select is cancel-safe (arms only detect, no side effects).

- **Defaults**: `DEFAULT_GROUP_REMOTE_ASSIGNOR` is `null` in Java → `Option<None>`; `DEFAULT_GROUP_PROTOCOL="classic"` → tool default is classic, which then **fails at new_consumer** with unsupported_version (KIP-848-only, §20) — faithful/documented. `--assignment-strategy` default is the literal string `"org.apache.kafka.clients.consumer.RangeAssignor"` (assignor types NOT translated). createFromArgs applies config files first, then explicit args override.

- **Test deps**: added `async-trait` (real dep, for the trait impls) and `indexmap` (dev-dep only, to build `ConsumerRecords::new(IndexMap, HashMap)` fixtures for on_records_received offset-math tests). No JUnit tests exist for these tools (ducktape only) — the Rust unit tests are net-new.

- **DoD notes**: §10 N/A (system-test tool, not send path); §11 N/A (consumes the Consumer trait, defines none). Callbacks run on caller task (§31) since they're passed as Arc<dyn> and commit_sync's onComplete is inline.
