# Layer 7: Metadata & Version Management - Implementation Plan

## 1. Overview

Layer 7 is the final layer of the Basic Connection Roadmap (Phase 4: Metadata Management). It provides cluster metadata caching and API version negotiation. Two of the four roadmap classes are already implemented (`NodeApiVersions`, `ApiVersions`). This plan covers the remaining work: `ClusterResourceListeners`, `MetadataSnapshot`, and `Metadata`.

## 2. Current State

| Class | Status | Location |
|-------|--------|----------|
| `NodeApiVersions` | **DONE** | `src/clients/node_api_versions.rs` |
| `ApiVersions` | **DONE** | `src/clients/api_versions.rs` |
| `ClusterResourceListeners` | NOT STARTED | Dependency of Metadata |
| `MetadataSnapshot` | NOT STARTED | Dependency of Metadata |
| `Metadata` | NOT STARTED | Main metadata cache |

All dependencies are in place: `ExponentialBackoff`, `SupportedVersionRange`, `Cluster`, `Node`, `TopicPartition`, `ClusterResource`, `PartitionInfo`, `Uuid`, `ApiKeys`, `Errors`, `MetadataRequest`, `MetadataResponse` (with `TopicMetadata`, `PartitionMetadata`).

Layer 6 Phase B is also complete: `ClusterConnectionStates`, `ClientRequest`, `ClientResponse`, `InFlightRequests`.

## 3. Classes to Translate (Dependency Order)

### Phase A: ClusterResourceListeners

| # | Java Class | Rust Module | Lines | Complexity |
|---|-----------|-------------|-------|------------|
| A1 | `ClusterResourceListeners` | `common::internals::cluster_resource_listeners` | 60 | Low |

### Phase B: MetadataSnapshot

| # | Java Class | Rust Module | Lines | Complexity |
|---|-----------|-------------|-------|------------|
| B1 | `MetadataSnapshot` | `clients::metadata_snapshot` | 261 | Medium-High |

### Phase C: Metadata

| # | Java Class | Rust Module | Lines | Complexity |
|---|-----------|-------------|-------|------------|
| C1 | `Metadata` | `clients::metadata` | 854 | High |

## 4. Detailed Design

### Phase A: ClusterResourceListeners (~30 lines Rust)

**Java source:** `kafka/clients/src/main/java/org/apache/kafka/common/internals/ClusterResourceListeners.java`

**Files to create:**
- `src/common/internals/mod.rs`
- `src/common/internals/cluster_resource_listeners.rs`

**Files to modify:**
- `src/common/mod.rs` - add `pub mod internals;`

**Design:**
- Define `ClusterResourceListener` trait: `fn on_update(&self, cluster_resource: &ClusterResource)`
- `ClusterResourceListeners` struct: holds `Vec<Box<dyn ClusterResourceListener + Send>>`
- Java's `maybeAdd(Object)` with `instanceof` check becomes `add(impl ClusterResourceListener)` - Rust's type system enforces the trait at compile time
- `on_update(&self, cluster: &ClusterResource)` iterates and notifies all listeners
- The trait must be `Send` since Metadata can be shared across threads
- No dedicated test file exists in Java; tested through MetadataTest

### Phase B: MetadataSnapshot (~200 lines Rust)

**Java source:** `kafka/clients/src/main/java/org/apache/kafka/clients/MetadataSnapshot.java`

**Files to create:**
- `src/clients/metadata_snapshot.rs`

**Files to modify:**
- `src/clients/mod.rs` - add `#[cfg(not(feature = "skip-generated"))] pub mod metadata_snapshot;` + re-export

**Struct design:**
```rust
pub struct MetadataSnapshot {
    cluster_id: Option<String>,
    nodes: HashMap<i32, Node>,
    unauthorized_topics: HashSet<String>,
    invalid_topics: HashSet<String>,
    internal_topics: HashSet<String>,
    controller: Option<Node>,
    metadata_by_partition: HashMap<TopicPartition, PartitionMetadata>,
    topic_ids: HashMap<String, Uuid>,
    topic_names: HashMap<Uuid, String>,
    cluster_instance: Cluster,  // computed eagerly
}
```

**Key methods:**
- `new()` - constructor, eagerly computes `Cluster` via `MetadataResponse::to_partition_info()`
- `cluster() -> &Cluster`
- `leader_epoch_for(tp) -> Option<i32>`
- `partition_metadata(tp) -> Option<&PartitionMetadata>`
- `topic_ids()`, `topic_names()`, `node_by_id()`, `cluster_resource()`
- `merge_with(...)` - incremental merge with `retain_topic: impl Fn(&str, bool) -> bool`
- `bootstrap(addresses) -> Self`, `empty() -> Self` - factory methods

**Rust-specific adaptations:**
- Java lazy `clusterInstance` -> compute eagerly (avoids `OnceLock`; the Java ctor always computes it anyway)
- Java `BiPredicate<String, Boolean>` -> `impl Fn(&str, bool) -> bool`
- `computeClusterView()` reuses existing `MetadataResponse::to_partition_info()`

### Phase C: Metadata (~600 lines Rust)

**Java source:** `kafka/clients/src/main/java/org/apache/kafka/clients/Metadata.java`

**Files to create:**
- `src/clients/metadata.rs`

**Files to modify:**
- `src/clients/mod.rs` - add `#[cfg(not(feature = "skip-generated"))] pub mod metadata;` + re-export

**Struct design:**
```rust
pub struct Metadata {
    inner: Mutex<MetadataInner>,
}

struct MetadataInner {
    refresh_backoff: ExponentialBackoff,
    metadata_expire_ms: i64,
    update_version: i32,
    request_version: i32,
    last_refresh_ms: i64,
    last_successful_refresh_ms: i64,
    attempts: i64,
    fatal_exception: Option<MetadataError>,
    invalid_topics: HashSet<String>,
    unauthorized_topics: HashSet<String>,
    metadata_snapshot: Arc<MetadataSnapshot>,
    need_full_update: bool,
    need_partial_update: bool,
    equivalent_response_count: i64,
    cluster_resource_listeners: ClusterResourceListeners,
    is_closed: bool,
    last_seen_leader_epochs: HashMap<TopicPartition, i32>,
    bootstrap_addresses: Vec<SocketAddr>,
    // Customization closures (replaces Java protected method overrides)
    retain_topic: Box<dyn Fn(&str, bool, i64) -> bool + Send>,
    new_request_builder_for_new_topics: Option<Box<dyn Fn() -> Option<MetadataRequestBuilder> + Send>>,
}
```

**Inner types (Java inner classes -> module-level structs):**
- `LeaderAndEpoch { leader: Option<Node>, epoch: Option<i32> }` + `no_leader_or_epoch()`
- `LeaderIdAndEpoch { leader_id: Option<i32>, epoch: Option<i32> }`
- `MetadataRequestAndVersion { request_builder: MetadataRequestBuilder, request_version: i32, is_partial_update: bool }`
- `MetadataError` enum: `TopicAuthorization { topics }`, `InvalidTopic { topics }`, `Fatal(String)`

**Thread safety:**
- All Java `synchronized` methods -> acquire `self.inner.lock()`
- Java `volatile metadataSnapshot` -> `Arc<MetadataSnapshot>` inside Mutex, cloned out for reads
- `fetch()` and `fetch_metadata_snapshot()` acquire lock briefly, clone Arc, release

**Java `protected` overrides -> closures:**
- `retainTopic(String, boolean, long)` -> `retain_topic: Box<dyn Fn(&str, bool, i64) -> bool + Send>`, default returns `true`
- `newMetadataRequestBuilderForNewTopics()` -> optional closure, default returns `None`
- Constructor takes optional closures via `with_overrides()` method

**Key methods to implement:**

Core public API:
- `new()`, `with_overrides()` - constructors
- `fetch() -> Cluster`, `fetch_metadata_snapshot() -> Arc<MetadataSnapshot>`
- `time_to_allow_update(now_ms) -> i64`, `time_to_next_update(now_ms) -> i64`
- `request_update(reset) -> i32`, `request_update_for_new_topics() -> i32`
- `update_last_seen_epoch_if_newer(tp, epoch) -> bool`
- `last_seen_leader_epoch(tp) -> Option<i32>`
- `update_requested() -> bool`
- `partition_metadata_if_current(tp) -> Option<PartitionMetadata>`
- `topic_ids()`, `topic_names()`, `current_leader(tp) -> LeaderAndEpoch`
- `bootstrap(addresses)`, `rebootstrap()`
- `update(request_version, response, is_partial, now_ms)` - core update logic
- `update_with_current_request_version(response, is_partial, now_ms)` - testing convenience
- `failed_update(now)`, `fatal_error(err)`
- `update_version() -> i32`, `last_successful_update() -> i64`
- `close()`, `is_closed() -> bool`
- `new_metadata_request_and_version(now_ms) -> MetadataRequestAndVersion`
- `add_cluster_update_listener(listener)`
- `maybe_throw_any_exception() -> Result<()>`, `maybe_throw_exception_for_topic(topic) -> Result<()>`, `maybe_throw_fatal_exception() -> Result<()>`

Private methods:
- `handle_metadata_response()` - transforms MetadataResponse into MetadataSnapshot
- `update_latest_metadata()` - epoch comparison logic
- `maybe_set_metadata_error()`, `check_invalid_topics()`, `check_unauthorized_topics()`
- `clear_recoverable_errors()`

**Error handling:**
```rust
pub enum MetadataError {
    TopicAuthorization { topics: HashSet<String> },
    InvalidTopic { topics: HashSet<String> },
    Fatal(String),
}
```

## 5. Rust-Specific Adaptations Summary

| Java Pattern | Rust Solution |
|-------------|---------------|
| `synchronized` methods | `Mutex<MetadataInner>` |
| `volatile MetadataSnapshot` | `Arc<MetadataSnapshot>` inside Mutex, cloned for reads |
| `BiPredicate<String, Boolean>` | `impl Fn(&str, bool) -> bool` |
| Java inner classes | Module-level structs |
| `protected` method overrides | Closures stored in struct, customizable at construction |
| `instanceof` check in `maybeAdd` | Compile-time trait enforcement |
| Lazy `clusterInstance` field | Eagerly computed (ctor always computes it) |
| `LogContext` / `Logger` | `log` crate macros (`log::debug!`, etc.) |
| `KafkaException` throws | `Result<(), MetadataError>` returns |
| `Optional` / `OptionalInt` | `Option<T>` / `Option<i32>` |
| Java `Time` / `MockTime` | Explicit `now_ms: i64` parameters |

## 6. Tests to Translate

### MetadataSnapshotTest (5 tests, Java: 235 lines)

| # | Test | Description |
|---|------|-------------|
| 1 | `test_missing_leader_endpoint` | Leader node ID not in nodes map |
| 2 | `test_merge_with_pre_existing_partition_retained` | Merge preserves old partitions |
| 3 | `test_topic_names_cache_built_from_topic_ids` | Reverse map verification |
| 4 | `test_empty_topic_names_cache` | Empty reverse map |
| 5 | `test_leader_epoch_for` | Epoch queries with present/absent/missing partitions |

All 5 are translatable with current infrastructure.

### MetadataTest (~25 tests, Java: 1233 lines)

| # | Test | Category |
|---|------|----------|
| 1 | `test_metadata_update_after_close` | Lifecycle |
| 2 | `test_time_to_next_update` | Timing/Backoff |
| 3 | `test_update_metadata_allowed_immediately_after_bootstrap` | Timing |
| 4 | `test_time_to_next_update_retry_backoff` | Backoff |
| 5 | `test_failed_update` | Error handling |
| 6 | `test_request_update` | Update flags |
| 7 | `test_cluster_listener_gets_notified_of_update` | Listeners |
| 8 | `test_update_last_epoch` | Epoch tracking |
| 9 | `test_epoch_update_after_topic_deletion` | Epoch tracking |
| 10 | `test_epoch_update_on_changed_topic_ids` | Epoch tracking |
| 11 | `test_reject_old_metadata` | Epoch tracking |
| 12 | `test_out_of_band_epoch_update` | Epoch tracking |
| 13 | `test_no_epoch` | Epoch tracking |
| 14 | `test_cluster_copy` | Cluster state |
| 15 | `test_request_version` | Version tracking |
| 16 | `test_partial_metadata_update` | Partial updates |
| 17 | `test_invalid_topic_error` | Error handling |
| 18 | `test_topic_authorization_error` | Error handling |
| 19 | `test_metadata_topic_errors` | Error handling |
| 20 | `test_ignore_leader_epoch_in_older_metadata_response` | Epoch reliability |
| 21 | `test_stale_metadata` | Staleness detection |
| 22 | `test_leader_metadata_inconsistent_with_broker_metadata` | Edge cases |
| 23 | `test_metadata_merge` | Merge logic |
| 24 | `test_metadata_merge_on_id_downgrade` | Merge with ID removal |
| 25 | `test_node_if_offline` | Node state |
| 26 | `test_node_if_online_when_not_in_replica_set` | Node state |
| 27 | `test_node_if_online_non_existent_topic_partition` | Node state |
| 28 | `test_concurrent_update_and_fetch` | Thread safety |

**Deferred test (1):**
- `test_topic_metadata_on_update_partition_leadership` - depends on `update_partition_leadership()` which is deferred

**Test utilities needed:**
- `RequestTestUtils` equivalent: helper functions to construct `MetadataResponse` from simple parameters. Place as `#[cfg(test)]` functions.
- `MockClusterResourceListener`: simple struct implementing `ClusterResourceListener` with update count tracking. Place in test module.
- Time: pass explicit `now_ms: i64` timestamps directly (no MockTime needed).

## 7. Module Structure

```
src/
  common/
    mod.rs                              # Add: pub mod internals;
    internals/
      mod.rs                            # NEW
      cluster_resource_listeners.rs     # A1 - NEW
  clients/
    mod.rs                              # Add: metadata_snapshot, metadata
    metadata_snapshot.rs                # B1 - NEW
    metadata.rs                         # C1 - NEW
```

## 8. Implementation Sequence

1. Create `common/internals` module with `ClusterResourceListeners`
2. Build and verify
3. Create `MetadataSnapshot` + tests
4. Build and verify all tests pass
5. Create `Metadata` + test utilities + tests
6. Build and verify all tests pass
7. Run `cargo xtask format-check` and `cargo xtask lint`

## 9. Deferred (Out of Scope)

- `update_partition_leadership()` - producer/consumer specific
- `MetadataRecoveryStrategy::Rebootstrap` handling in `rebootstrap()`
- Metrics/Sensor integration
- `test_topic_metadata_on_update_partition_leadership` test
