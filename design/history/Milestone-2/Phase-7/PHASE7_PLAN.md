# Phase 7 -- ProducerMetadata and Integration Test Update

## Goal

Translate Java's `ProducerMetadata` class and update `KafkaProducer` to use it
for topic metadata management.  Update the integration test and performance test
to use the refactored producer with `NetworkClient`.

## Java Reference

### ProducerMetadata (extends Metadata)

```java
public class ProducerMetadata extends Metadata {
    private final long metadataIdleMs;
    private final Map<String, Long> topics;     // topic -> expiry timestamp
    private final Set<String> newTopics;         // topics needing first fetch
    
    void add(String topic, long nowMs)           // add topic to tracked set
    boolean containsTopic(String topic)          // check if topic is tracked
    boolean retainTopic(topic, isInternal, now)  // override: expire idle topics
    void awaitUpdate(lastVersion, timeoutMs)     // block until metadata version advances
    MetadataRequest.Builder newMetadataRequestBuilder()          // request for all tracked topics
    MetadataRequest.Builder newMetadataRequestBuilderForNewTopics()  // request for new topics only
}
```

### How KafkaProducer uses ProducerMetadata

```java
// KafkaProducer.doSend():
ClusterAndWaitTime clusterAndWaitTime = waitOnMetadata(record.topic(), record.partition(), nowMs, maxBlockTimeMs);

// waitOnMetadata():
metadata.add(topic, nowMs);                        // ensure topic is tracked
do {
    metadata.requestUpdateForTopic(topic);
    int version = metadata.requestUpdateForTopic(topic);
    sender.wakeup();                               // wake sender to drive poll
    metadata.awaitUpdate(version, remainingMs);     // block until version advances
    partitionCount = metadata.fetch().partitionCountForTopic(topic);
} while (partitionCount == null || partition >= partitionCount);
```

### How Sender uses ProducerMetadata

```java
// Sender.sendProducerData():
MetadataSnapshot metadataSnapshot = metadata.fetchMetadataSnapshot();
ReadyCheckResult result = accumulator.ready(metadataSnapshot, now);
if (!result.unknownLeaderTopics.isEmpty()):
    for (topic : result.unknownLeaderTopics):
        metadata.add(topic, now)
    metadata.requestUpdate(false)
```

## Design Decisions

### 1. ProducerMetadata as a Rust struct wrapping Metadata

Since Java's `ProducerMetadata extends Metadata`, in Rust we use composition:

```rust
pub struct ProducerMetadata {
    metadata: Arc<Metadata>,    // the existing Metadata from Layer 6
    metadata_idle_ms: i64,
    topics: Mutex<HashMap<String, i64>>,   // topic -> expiry time
    new_topics: Mutex<HashSet<String>>,
}
```

The existing `Metadata` class already has `request_update()`, `update_version()`,
`fetch_metadata_snapshot()`, and topic retention hooks.  `ProducerMetadata` wraps
it and adds topic-idle-expiry and the new-topics optimization.

### 2. KafkaProducer.waitOnMetadata uses ProducerMetadata.awaitUpdate

Replace the current polling loop in `wait_on_metadata()` (which calls
`client.partitions_for()` directly) with:

```rust
async fn wait_on_metadata(&self, topic: &str, partition: Option<i32>) -> Result<i32> {
    self.metadata.add(topic, now);
    loop {
        let version = self.metadata.request_update_for_topic(topic);
        // Wake the sender to drive a metadata update
        self.sender_wakeup.notify_one();
        // Wait for metadata version to advance
        self.metadata.await_update(version, remaining_ms).await?;
        let count = self.metadata.partition_count_for_topic(topic);
        if count.is_some() && (partition.is_none() || partition.unwrap() < count.unwrap()) {
            return Ok(count.unwrap());
        }
    }
}
```

### 3. Sender drives metadata updates via NetworkClient

`Sender` already calls `client.poll()` which triggers `NetworkClient`'s internal
`DefaultMetadataUpdater`.  When `ProducerMetadata.request_update()` is called,
the next `client.poll()` will send a metadata request and update the snapshot.
This is exactly how Java works -- no separate metadata fetch path needed.

### 4. Shared Metadata between KafkaProducer and Sender

```
KafkaProducer  ----Arc<ProducerMetadata>----> ProducerMetadata
                                                    |
Sender         ----Arc<ProducerMetadata>----> (same instance)
```

Both `KafkaProducer` (for `waitOnMetadata`) and `Sender` (for `sendProducerData`)
share the same `ProducerMetadata` instance via `Arc`.  Thread safety comes from
`Metadata`'s internal `Mutex`, matching Java's `synchronized` blocks.

## Files to Add

| File | Java Class | Purpose |
|------|------------|---------|
| `src/clients/producer/producer_metadata.rs` | `ProducerMetadata` | Topic tracking, idle expiry, new-topic optimization |

## Files to Modify

| File | Change |
|------|--------|
| `src/clients/producer/kafka_producer.rs` | Replace `metadata_cache: Mutex<HashMap<String, i32>>` with `Arc<ProducerMetadata>`. Rewrite `wait_on_metadata()` to use `ProducerMetadata.await_update()`. Remove `partitions_for()` method that called `ProduceClient`. |
| `src/clients/producer/sender.rs` | Use `Arc<ProducerMetadata>` for `metadata` field. In `send_producer_data()`, call `metadata.fetch_metadata_snapshot()` for ready-check and `metadata.add(topic, now)` for unknown leader topics. |
| `src/clients/producer/mod.rs` | Add `producer_metadata` module. Export `ProducerMetadata`. |

## Files to Update (tests)

| File | Change |
|------|--------|
| `src/clients/producer/kafka_producer.rs` (tests) | Update unit tests to use `ProducerMetadata` |
| `performance_tests/src/producer_perf.rs` | Full update: construct `NetworkClient` + `ProducerMetadata` + `KafkaProducer`. Remove `KafkaProduceClient` usage. |

## Java Tests to Translate

From `kafka/clients/src/test/java/org/apache/kafka/clients/producer/internals/ProducerMetadataTest.java`:

| Java Test | Purpose |
|-----------|---------|
| `testMetadata` | Basic add/contains/retain |
| `testMetadataExpiry` | Topics expire after idle period |
| `testNewTopicTracking` | New topics tracked separately |
| `testAwaitUpdate` | Blocking until version advances |

## Scope Limits

- No consumer metadata (ConsumerMetadata is separate)
- No metrics for metadata updates
- Topic retention is time-based only (no LRU)

## Definition of Done

1. `ProducerMetadata` class translated with all methods
2. `KafkaProducer` uses `ProducerMetadata` instead of `metadata_cache`
3. `wait_on_metadata()` uses `ProducerMetadata.await_update()` 
4. `Sender` uses `ProducerMetadata.fetch_metadata_snapshot()` 
5. All `ProducerMetadata` Java tests translated and passing
6. Integration tests pass against a real broker
7. Performance test compiles and runs
8. `cargo build` succeeds
9. `cargo test` passes
10. `cargo xtask format-check` passes
11. `cargo xtask lint` passes
