# Plan: Integration Testing with Real Kafka Broker (Phase 5)

## Context

After Layer 7 (Metadata & Version Management) completes, the Rust client will have all the infrastructure needed for basic Kafka broker communication: TCP transport, channel management, request/response framing, and metadata discovery. Phase 5 is the first point where end-to-end testing against a real broker becomes meaningful.

The Java Kafka client uses an annotation-driven framework (`@ClusterTest` + `ClusterInstance`) backed by an in-process embedded broker (`KafkaClusterTestKit`). Each Java test method gets its own fresh cluster instance — affordable because the broker is in-process. Docker containers are far heavier (seconds to start vs. milliseconds), so we **share a single container across all tests that need the same cluster configuration**, achieving the same test semantics with acceptable startup cost.

## Approach: testcontainers-rs with Shared Cluster Instances

### Why testcontainers-rs (not docker-compose or binary spawn)

| Criteria | testcontainers-rs | docker-compose | Binary spawn |
|---|---|---|---|
| Shared instances | Yes (via pool) | Yes (shared) | Yes |
| Automatic cleanup | Yes (Drop trait) | Manual | Manual |
| Parallel tests | Yes (unique ports) | Port conflicts | Port conflicts |
| No JVM required | Yes | Yes | No |
| CI-friendly | Yes | Needs docker-compose | Needs JVM |

### Why Shared Clusters (diverging from Java's per-test model)

The Java framework starts an in-process embedded broker per test method (~100ms). Docker containers take 5-15 seconds to start. With 20+ integration tests, per-test containers would make the suite unbearably slow. Instead:

- Tests with **identical cluster requirements** (same broker count, same server properties) share **one container**.
- Test **isolation** is achieved through **unique topic/group names per test** and **resource cleanup after each test**, not through separate brokers.
- Tests **run in parallel** safely because they operate on non-overlapping resources within the shared cluster.

### Docker Image

Use `apache/kafka` official image (KRaft mode, no Zookeeper). This is the same image used by the Java project's docker examples in `kafka/docker/examples/docker-compose-files/`.

## Implementation Plan

### Step 1: Add dev-dependencies

**File: `Cargo.toml`**

```toml
[dev-dependencies]
testcontainers = { version = "0.23", features = ["blocking"] }
testcontainers-modules = { version = "0.11", features = ["kafka"] }
```

### Step 2: Create `ClusterConfig` — cluster requirements descriptor

**File: `tests/common/cluster_config.rs`**

Describes the cluster shape a test needs. Used as the key for the shared cluster pool.

```rust
/// Describes cluster requirements for a group of tests.
/// Tests with identical ClusterConfig share one container.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct ClusterConfig {
    /// Number of brokers (default: 1)
    pub brokers: u16,
    /// Extra server properties (key=value pairs set as env vars)
    pub server_properties: BTreeMap<String, String>,
}

impl ClusterConfig {
    /// Default single-broker cluster with no extra properties.
    pub fn default() -> Self {
        Self { brokers: 1, server_properties: BTreeMap::new() }
    }

    /// Single broker with custom server properties.
    pub fn with_properties(props: BTreeMap<String, String>) -> Self {
        Self { brokers: 1, server_properties: props }
    }
}
```

### Step 3: Create `ClusterPool` — shared instance registry

**File: `tests/common/cluster_pool.rs`**

A process-global registry that maps `ClusterConfig` → running `KafkaCluster`. All tests requesting the same config get the same instance. Initialization is lazy and thread-safe.

```rust
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::OnceCell;

/// Global pool of shared Kafka cluster instances.
/// Each unique ClusterConfig gets at most one running container.
static CLUSTER_POOL: Mutex<HashMap<ClusterConfig, Arc<OnceCell<KafkaCluster>>>> =
    Mutex::new(HashMap::new());

/// Get or create a shared KafkaCluster for the given config.
/// First caller triggers container startup; subsequent callers await
/// the same OnceCell and receive a reference to the already-running cluster.
pub async fn get_or_create(config: &ClusterConfig) -> Arc<KafkaCluster> {
    let cell = {
        let mut pool = CLUSTER_POOL.lock().unwrap();
        pool.entry(config.clone())
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone()
    };
    cell.get_or_init(|| async {
        KafkaCluster::start_with_config(config).await
    }).await;
    // ...
}
```

The pool lives for the duration of the test process. Containers are cleaned up when the `KafkaCluster` values are dropped at process exit (testcontainers default behavior).

### Step 4: Create `KafkaCluster` — container wrapper

**File: `tests/common/kafka_cluster.rs`**

Wraps a single testcontainers Kafka container. Created by `ClusterPool`, shared across tests.

```rust
/// Manages a real Kafka broker in Docker for integration tests.
/// Shared across tests with the same ClusterConfig.
pub struct KafkaCluster {
    container: ContainerAsync<kafka::Kafka>,
    bootstrap_servers: String,
    config: ClusterConfig,
}

impl KafkaCluster {
    /// Start a cluster matching the given config.
    /// Called by ClusterPool, not by tests directly.
    pub(crate) async fn start_with_config(config: &ClusterConfig) -> Self { ... }

    /// Bootstrap servers connection string.
    pub fn bootstrap_servers(&self) -> &str { &self.bootstrap_servers }

    /// The config this cluster was started with.
    pub fn config(&self) -> &ClusterConfig { &self.config }
}
```

### Step 5: Create `TestContext` — per-test isolation and cleanup

**File: `tests/common/test_context.rs`**

Each test creates a `TestContext` that provides unique resource names and tracks created resources for cleanup. This is the key to parallel test safety on a shared cluster.

```rust
/// Per-test context providing unique resource names and cleanup.
/// Each test creates one of these; they share the underlying KafkaCluster.
pub struct TestContext {
    cluster: Arc<KafkaCluster>,
    /// Unique prefix for this test's resources (e.g., "test_api_versions_a3f9")
    prefix: String,
    /// Topics created during this test, cleaned up on drop
    created_topics: Vec<String>,
}

impl TestContext {
    /// Create a new context for a test, sharing the given cluster.
    /// Generates a unique prefix from the test name + random suffix.
    pub async fn new(config: ClusterConfig) -> Self {
        let cluster = ClusterPool::get_or_create(&config).await;
        let prefix = format!(
            "{}_{}",
            std::thread::current().name().unwrap_or("test"),
            random_suffix(4)
        );
        Self { cluster, prefix, created_topics: Vec::new() }
    }

    /// Bootstrap servers string (delegates to cluster).
    pub fn bootstrap_servers(&self) -> &str { self.cluster.bootstrap_servers() }

    /// Generate a unique topic name for this test.
    /// e.g., "test_api_versions_a3f9_my_topic"
    pub fn topic(&mut self, base_name: &str) -> String {
        let name = format!("{}_{}", self.prefix, base_name);
        self.created_topics.push(name.clone());
        name
    }

    /// Generate a unique consumer group ID for this test.
    pub fn group_id(&self, base_name: &str) -> String {
        format!("{}_{}", self.prefix, base_name)
    }

    /// Clean up all resources created during this test.
    /// Deletes topics via admin client or docker exec.
    pub async fn cleanup(&mut self) {
        for topic in self.created_topics.drain(..) {
            // Delete topic via docker exec or admin client
            self.delete_topic(&topic).await;
        }
    }

    async fn delete_topic(&self, topic: &str) {
        // Use docker exec to call kafka-topics.sh --delete
        // or use our admin client once available
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        // Best-effort synchronous cleanup hint.
        // Primary cleanup should happen via explicit cleanup() call.
        if !self.created_topics.is_empty() {
            eprintln!(
                "WARN: TestContext dropped with {} uncleaned topics (prefix: {})",
                self.created_topics.len(), self.prefix
            );
        }
    }
}
```

### Step 6: Create integration test files

**Directory: `tests/integration/`**

Mirror the Java `clients-integration-tests` structure, starting with the tests that validate the basic connection flow from Phase 5 of the roadmap:

#### 6a. `tests/integration/connection_test.rs` — Basic Connection Flow

Tests from roadmap Phase 5:
1. Connect to broker via TCP
2. Send `ApiVersionsRequest` → receive `ApiVersionsResponse`
3. Send `MetadataRequest` → receive `MetadataResponse`
4. Verify parsed response data matches broker state

This maps to Java's `MetadataVersionIntegrationTest` and `BootstrapControllersIntegrationTest`.

#### 6b. `tests/integration/api_versions_test.rs` — Version Negotiation

Test that the client correctly negotiates API versions with the broker:
- Query all supported API versions
- Verify expected APIs are present (Produce, Fetch, Metadata, etc.)
- Verify version ranges are within expected bounds

Maps to Java's `NodeApiVersions` integration coverage.

#### 6c. `tests/integration/metadata_test.rs` — Metadata Discovery

Test cluster metadata operations:
- Fetch metadata for non-existent topic
- Fetch metadata for auto-created topic
- Verify broker/node information matches
- Verify partition leader assignment

Maps to Java's `MetadataVersionIntegrationTest`.

### Step 7: Test runner configuration

**File: `tests/integration/mod.rs`** or use `#[cfg(feature = "integration")]`

Integration tests should be gated behind a cargo feature or test attribute so they don't run on every `cargo test` (they require Docker):

```toml
# Cargo.toml
[features]
integration-tests = []
```

```rust
// Each integration test file
#![cfg(feature = "integration-tests")]
```

Run with: `cargo test --features integration-tests`

Alternatively, use the `#[ignore]` attribute and run with `cargo test -- --ignored`.

### Step 8: CI configuration

Add a CI step that:
1. Ensures Docker is available
2. Pulls `apache/kafka` image
3. Runs `cargo test --features integration-tests`

## Test Pattern (shared cluster with per-test isolation)

Each Rust integration test follows this pattern. Tests sharing the same `ClusterConfig` reuse one container:

```rust
#[tokio::test]
async fn test_api_versions_request() {
    // Get a shared cluster (first test to run triggers container startup;
    // subsequent tests with same config reuse it immediately)
    let mut ctx = TestContext::new(ClusterConfig::default()).await;

    // Use our Rust client to connect and send request
    let client = build_test_client(ctx.bootstrap_servers()).await;

    // Send ApiVersionsRequest, receive response
    let response = client.send_api_versions_request().await.unwrap();

    // Verify response
    assert!(!response.api_keys().is_empty());
    assert_eq!(response.error_code(), 0);

    // Clean up any resources this test created
    ctx.cleanup().await;
}

#[tokio::test]
async fn test_metadata_for_topic() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;

    // Each test gets a unique topic name — no collision with parallel tests
    let topic = ctx.topic("metadata_check");

    let client = build_test_client(ctx.bootstrap_servers()).await;
    let response = client.send_metadata_request(&[&topic]).await.unwrap();

    assert_eq!(response.topics().len(), 1);

    // Cleanup deletes the topic so the shared cluster stays clean
    ctx.cleanup().await;
}
```

### Parallel safety guarantees

1. **Unique resource names**: `TestContext::topic()` and `TestContext::group_id()` generate names with a per-test prefix + random suffix, preventing collisions even when tests run concurrently.
2. **No shared mutable state**: Each test creates its own `TestContext`. The shared `KafkaCluster` is read-only after initialization (only `bootstrap_servers()` is called).
3. **Resource cleanup**: Each test cleans up its own topics/groups, so one test's leftovers cannot affect another.
4. **Thread-safe pool**: `ClusterPool` uses `Mutex` + `OnceCell` so concurrent first-access from multiple tests safely initializes exactly one container per config.

## Mapping Java → Rust Test Infrastructure

| Java | Rust |
|---|---|
| `@ClusterTest` annotation | `#[tokio::test]` + `TestContext::new(config)` |
| `@ClusterTestDefaults` | `ClusterConfig::default()` / `ClusterConfig::with_properties()` |
| `ClusterInstance` (injected) | `TestContext` (created in test) |
| `ClusterInstance.bootstrapServers()` | `ctx.bootstrap_servers()` |
| `ClusterInstance.createTopic()` | `ctx.topic("name")` (auto-tracked for cleanup) |
| `ClusterInstance.producer()` | Our Rust KafkaProducer (future milestone) |
| `ClusterInstance.consumer()` | Our Rust KafkaConsumer (future milestone) |
| `KafkaClusterTestKit` (embedded, per-test) | `KafkaCluster` (Docker, shared per-config) |
| One cluster per test method | One cluster per unique `ClusterConfig` |
| Isolation via separate brokers | Isolation via unique topic/group name prefixes |
| `ClientsTestUtils` helpers | `tests/common/` helper functions |
| `@ClusterTest(brokers=3)` | `ClusterConfig { brokers: 3, .. }` → shared multi-broker container (future) |

## Phased Rollout

### Phase 5a (after Layer 7) — Connection & Protocol Tests
- `ClusterConfig`, `ClusterPool`, `KafkaCluster`, `TestContext` harness
- `connection_test.rs`: TCP connect + ApiVersions + Metadata
- `api_versions_test.rs`: version negotiation
- `metadata_test.rs`: cluster discovery

### Phase 5b (future, after Producer implementation)
- `producer_test.rs`: produce records to broker
- Maps to Java's `ProducerCompressionTest`, `ProducerFailureHandlingTest`

### Phase 5c (future, after Consumer implementation)
- `consumer_test.rs`: consume records from broker
- Maps to Java's `ConsumerIntegrationTest`, `PlaintextConsumer*Test`

### Phase 5d (future, multi-broker)
- `ClusterConfig { brokers: 3, .. }` → shared multi-broker container
- Broker failure/recovery tests (these may need per-test clusters since they mutate broker state)
- Maps to Java's `ConsumerBounceTest`, `ClientRebootstrapTest`

## Files to Create/Modify

| File | Action |
|---|---|
| `Cargo.toml` | Add testcontainers dev-dependencies, integration-tests feature |
| `tests/common/cluster_config.rs` | New: ClusterConfig descriptor |
| `tests/common/cluster_pool.rs` | New: shared cluster instance registry |
| `tests/common/kafka_cluster.rs` | New: KafkaCluster container wrapper |
| `tests/common/test_context.rs` | New: per-test isolation and cleanup |
| `tests/common/mod.rs` | Modify: export new modules |
| `tests/integration/mod.rs` | New: integration test module |
| `tests/integration/connection_test.rs` | New: basic connection tests |
| `tests/integration/api_versions_test.rs` | New: version negotiation tests |
| `tests/integration/metadata_test.rs` | New: metadata discovery tests |

## Verification

1. `docker pull apache/kafka` succeeds
2. `cargo test --features integration-tests` — all integration tests pass
3. `cargo test` (without feature) — integration tests are skipped
4. Tests run in parallel safely: no topic/group name collisions, no cross-test interference
5. Only **one** container starts per unique `ClusterConfig`, regardless of how many tests share it
6. Topics created during tests are deleted after each test completes
7. Containers are cleaned up at process exit (no orphaned containers)

## Key Reference Files

- Java ClusterInstance: `kafka/test-common/test-common-runtime/src/main/java/org/apache/kafka/common/test/ClusterInstance.java`
- Java ClusterTest annotation: `kafka/test-common/test-common-internal-api/src/main/java/org/apache/kafka/common/test/api/ClusterTest.java`
- Java client integration tests: `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/`
- Docker examples: `kafka/docker/examples/docker-compose-files/`
- Roadmap Phase 5: `design/history/Milestone-1/BASIC_CONNECTION_ROADMAP.md` (lines 117-124)
