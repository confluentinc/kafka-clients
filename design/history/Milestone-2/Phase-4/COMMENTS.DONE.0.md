# Phase 4 Review — Critic 0

## Issue 1: ApiVersions handshake performed on every produce request

- **File**: `src/clients/producer/kafka_produce_client.rs`
- **Severity**: Bug (Performance / Behavioral deviation)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java` — caches API versions per node in `NodeApiVersions`
- **Description**: Both `send_produce()` (line 316) and `fetch_partitions()` (line 412) call `handshake_api_versions()` on every single invocation, even when the connection is already established and API versions have already been negotiated. This adds an extra full network roundtrip (ApiVersions request + response) before every produce request.
- **Expected**: Cache the `ApiVersionsResponse` per node_id on first handshake, and reuse the cached result on subsequent calls. Java's `NetworkClient` does this via `NodeApiVersions` — the handshake happens once per connection, not once per request.
- **Actual**: Every call to `send_produce` or `fetch_partitions` performs a redundant ApiVersions roundtrip, doubling the latency of every produce operation. On a sustained workload, this means 2x the number of network requests.

## Issue 2: Reconnection fails because stale channel is not closed before re-connecting

- **File**: `src/clients/producer/kafka_produce_client.rs`
- **Severity**: Bug
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/ClusterConnectionStates.java` — manages connection lifecycle with proper state transitions
- **Description**: When a connection is lost (detected by `poll_for_response` at line 247, which sets `connected_nodes[node_id] = false`), the next call to `ensure_connected()` falls through the early-return check (line 137) and attempts to call `selector.connect(node_id, ...)`. However, the old channel is still registered in the selector — `Selector::connect()` calls `ensure_not_registered()` which checks `self.channels` and `self.closing_channels`. If the stale channel has not been cleaned up (which requires a `poll()` + `clear()` cycle), the connect call will fail with `AlreadyExists`, making the node permanently unreachable until the client is recreated.
- **Expected**: Before attempting to reconnect, close the existing channel via `self.selector.close_channel(node_id)` (and poll to process the close) so the selector slot is freed. Alternatively, perform a `poll()` before the reconnection attempt to clear stale state.
- **Actual**: `ensure_connected()` directly calls `selector.connect()` without cleaning up the old channel, causing an `AlreadyExists` error on reconnection attempts.

## Issue 3: Missing planned test `test_produce_to_nonexistent_topic`

- **File**: `tests/integration_producer_test.rs`
- **Severity**: Missing Requirement
- **Java Reference**: N/A (plan-specified test)
- **Description**: The Phase 4 plan (`PHASE4_PLAN.md`) specifies 4 integration tests, including `test_produce_to_nonexistent_topic` to verify error handling. Only 3 tests are implemented. The error-handling test is important because it validates the error propagation path from broker error codes through `KafkaProduceClient` back to the `ProducerBatch` completion.
- **Expected**: Implement `test_produce_to_nonexistent_topic` that produces to a non-existent topic (with auto-create disabled) and verifies that an appropriate `KafkaError` is returned.
- **Actual**: Only 3 of 4 planned tests are implemented; the error-path test is missing.

## Issue 4: Double clone of batch data in `build_produce_request_data`

- **File**: `src/clients/producer/kafka_produce_client.rs`
- **Severity**: Design Flaw (Performance)
- **Description**: In `build_produce_request_data()`, each batch's `Vec<u8>` data is cloned twice: once at line 273 when inserting into the `topics` HashMap, and again at line 282 when calling `pd.set_records(Some(data.clone()))`. The second clone is caused by iterating over `&topics` (borrowing) instead of consuming the HashMap.
- **Expected**: Either (a) take `batches` by value and consume the data into the HashMap, then `into_iter()` the HashMap to avoid the second clone, or (b) at minimum iterate the HashMap with `into_iter()` / `drain()` so the second clone at line 282 is unnecessary. Since `send_produce` already owns `batches` (it takes `Vec<(TopicPartition, Vec<u8>)>` by value), the data can be moved all the way through without any clones.
- **Actual**: Two unnecessary copies of each record batch's bytes are created. For large batches this doubles memory pressure.
