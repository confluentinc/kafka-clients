# Layer 6: NetworkClient Implementation Plan

## 1. Overview

Layer 6 ties together the network transport (Selector) with the request/response framework to provide a high-level client for sending Kafka RPCs. The goal is a working `NetworkClient` that can perform ApiVersions and Metadata RPCs.

## 2. Classes to Translate (Dependency Order)

### Phase A: Leaf Types and Utilities

| # | Java Class | Rust Module | Lines | Complexity |
|---|-----------|-------------|-------|------------|
| A1 | `ExponentialBackoff` | `common::utils::exponential_backoff` | 80 | Low |
| A2 | `ConnectionState` | `clients::connection_state` | 39 | Trivial |
| A3 | `MetadataRecoveryStrategy` | `clients::metadata_recovery_strategy` | 44 | Trivial |
| A4 | `LeastLoadedNode` | `clients::least_loaded_node` | 43 | Trivial |
| A5 | `RequestCompletionHandler` | Type alias in `clients::mod` | - | Trivial |
| A6 | `SupportedVersionRange` | `common::feature::supported_version_range` | 56 | Trivial |
| A7 | `HostResolver` / `DefaultHostResolver` | `clients::host_resolver` | ~40 | Low |

### Phase B: State Management

| # | Java Class | Rust Module | Lines | Complexity |
|---|-----------|-------------|-------|------------|
| B1 | `NodeApiVersions` | `clients::node_api_versions` | 268 | Medium |
| B2 | `ApiVersions` | `clients::api_versions` | 69 | Low |
| B3 | `ClusterConnectionStates` | `clients::cluster_connection_states` | 567 | High |
| B4 | `ClientRequest` | `clients::client_request` | 119 | Low |
| B5 | `ClientResponse` | `clients::client_response` | 173 | Low |
| B6 | `InFlightRequests` + `InFlightRequest` | `clients::in_flight_requests` | 186+95 | Medium |

### Phase C: Metadata Infrastructure

| # | Java Class | Rust Module | Lines | Complexity |
|---|-----------|-------------|-------|------------|
| C1 | `ClusterResourceListeners` | `common::internals::cluster_resource_listeners` | 60 | Low |
| C2 | `MetadataSnapshot` | `clients::metadata_snapshot` | 261 | High |
| C3 | `Metadata` (core subset) | `clients::metadata` | 854 | High |
| C4 | `MetadataUpdater` (trait) | `clients::metadata_updater` | 110 | Low |

### Phase D: NetworkClient

| # | Java Class | Rust Module | Lines | Complexity |
|---|-----------|-------------|-------|------------|
| D1 | `KafkaClient` (trait) | `clients::kafka_client` | ~100 | Medium |
| D2 | `NetworkClient` + `DefaultMetadataUpdater` | `clients::network_client` | 1607 | Very High |
| D3 | `NetworkClientUtils` | `clients::network_client_utils` | 154 | Medium |

## 3. Rust-Specific Adaptations

### Async Integration
- `NetworkClient::poll()` is async (Selector is already async)
- `KafkaClient` trait has async methods
- Per CLAUDE.md rule 9: callbacks become `Box<dyn FnOnce(&ClientResponse) + Send>`

### DefaultMetadataUpdater (Inner Class → Separate Struct)
Java inner class freely accesses `NetworkClient` fields. In Rust:
- `DefaultMetadataUpdater` owns `Metadata`
- Methods that need `NetworkClient` state receive it as parameters
- Or: `maybe_update()` returns an enum of actions (send request, initiate connect, nothing) that `NetworkClient::poll()` executes

### Java Inheritance → Enums
`ConcreteRequest`/`ConcreteResponse` enums already exist from Layer 3. NetworkClient dispatches on these.

### Thread Safety
- `NetworkClient` is single-threaded (same as Java: "not thread-safe")
- `ApiVersions` uses `RwLock` for cross-thread access (matching Java `synchronized`)
- `Metadata` uses `Mutex` for shared access between threads

### DNS Resolution
`ClusterConnectionStates::current_address()` uses async `tokio::net::lookup_host()` instead of blocking DNS.

## 4. Module Structure

```
src/
  lib.rs                              # Add: pub mod clients;
  common/
    mod.rs                            # Add: pub mod feature; pub mod internals; pub mod utils;
    utils/
      mod.rs
      exponential_backoff.rs          # A1
    feature/
      mod.rs
      supported_version_range.rs      # A6
    internals/
      mod.rs
      cluster_resource_listeners.rs   # C1
  clients/
    mod.rs                            # RequestCompletionHandler type alias (A5)
    connection_state.rs               # A2
    metadata_recovery_strategy.rs     # A3
    least_loaded_node.rs              # A4
    host_resolver.rs                  # A7
    node_api_versions.rs              # B1
    api_versions.rs                   # B2
    cluster_connection_states.rs      # B3
    client_request.rs                 # B4
    client_response.rs                # B5
    in_flight_requests.rs             # B6 + InFlightRequest
    metadata_snapshot.rs              # C2
    metadata.rs                       # C3
    metadata_updater.rs               # C4
    kafka_client.rs                   # D1
    network_client.rs                 # D2 + DefaultMetadataUpdater
    network_client_utils.rs           # D3
```

## 5. Tests to Translate

| Java Test File | Lines | Priority | Notes |
|---|---|---|---|
| `NodeApiVersionsTest.java` | 196 | HIGH | Version intersection, unknown APIs |
| `ApiVersionsTest.java` | 66 | HIGH | Finalized features tracking |
| `InFlightRequestsTest.java` | 129 | HIGH | Add/complete/timeout/canSendMore |
| `ClusterConnectionStatesTest.java` | 469 | HIGH | Backoff, timeouts, address rotation |
| `MetadataTest.java` | 1233 | MEDIUM | Translate core subset first |
| `NetworkClientTest.java` | 1482 | MEDIUM | Requires MockSelector; translate core subset |

### Test Infrastructure Needed
- **MockSelector**: Test-only `Selectable` implementation simulating connections/sends/receives/disconnections (~200 lines)
- **MockTime**: Controllable time source with `advance(ms)` method

## 6. Implementation Sequence

1. `ExponentialBackoff` + tests
2. `ConnectionState` + `MetadataRecoveryStrategy` + `LeastLoadedNode` (trivial)
3. `SupportedVersionRange`
4. `HostResolver` + `DefaultHostResolver`
5. `NodeApiVersions` + tests
6. `ApiVersions` + tests
7. `ClusterConnectionStates` + tests
8. `RequestCompletionHandler` type alias
9. `ClientRequest` + `ClientResponse`
10. `InFlightRequest` + `InFlightRequests` + tests
11. `ClusterResourceListeners` (stub)
12. `MetadataSnapshot`
13. `Metadata` (core subset) + selected tests
14. `MetadataUpdater` trait
15. `KafkaClient` trait
16. `MockSelector` (test utility)
17. `NetworkClient` + `DefaultMetadataUpdater` + selected tests
18. `NetworkClientUtils`

## 7. Deferred (Out of Scope)

- Telemetry (`TelemetrySender`, telemetry API handling)
- SASL correlation ID reservation
- `MetadataRecoveryStrategy::Rebootstrap` logic
- `Metadata::updatePartitionLeadership()` (producer/consumer)
- Leader epoch tracking (simplified for now)
- Metrics/Sensor integration
- `Metadata::maybeThrowAnyException` / `maybeThrowExceptionForTopic`
- Full `MetadataTest` and `NetworkClientTest` coverage

## 8. Key Challenges

1. **Async poll loop**: `NetworkClient::poll()` must be async, calling `Selector::poll().await`
2. **DefaultMetadataUpdater**: Extract from inner class to separate struct with parameter passing
3. **DNS resolution**: `ClusterConnectionStates` needs async DNS via tokio
4. **MetadataSnapshot merge**: Complex logic for updating cluster state
5. **MockSelector**: Essential test infrastructure for NetworkClientTest
6. **Callback execution**: Synchronous within `poll()`, matching Java's `completeResponses()` pattern
