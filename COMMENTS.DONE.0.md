
# Resolved Issues -- Critic 0 Review

## Issue 30: default_maybe_update_with_node never calls selector.connect() [RESOLVED]
- **File**: `src/clients/network_client.rs`
- **Fix**: Replaced inline sync-only state change with `block_on(self.initiate_connect(node, now))` call, matching Java's `initiateConnect()`.
- **Commit**: 9a0f588

## Issue 31: poll() uses pre-poll timestamp instead of post-poll timestamp [RESOLVED]
- **File**: `src/clients/network_client.rs`
- **Fix**: Added `time_provider` field (mirrors Java's `Time time`) and `poll_time_store` for mock time support. After `selector.poll()`, calls `(self.time_provider)()` to get `updated_now`. Tests use mock time via `set_mock_time()`.
- **Commit**: 9a0f588

## Issue 32: is_invalid_metadata_error includes wrong errors and misses others [RESOLVED]
- **File**: `src/clients/metadata.rs`
- **Fix**: Updated match to include exactly the error codes whose Java exceptions extend `InvalidMetadataException`. Removed `InvalidTopicException` and `TopicAuthorizationFailed`, added `ListenerNotFound`, `FencedLeaderEpoch`, `UnknownTopicId`, `NetworkException`, `KafkaStorageError`, `InconsistentTopicId`, `PreferredLeaderNotAvailable`, `EligibleLeadersNotAvailable`, `ElectionNotNeeded`.
- **Commit**: 9a0f588

## Issue 33: least_loaded_node returns None instead of panicking when nodes is empty [RESOLVED]
- **File**: `src/clients/network_client.rs`
- **Fix**: Changed to `panic!("There are no nodes in the Kafka cluster")` matching Java's `IllegalStateException`. No existing tests depend on the previous behavior.
- **Commit**: 9a0f588
