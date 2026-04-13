# Resolved Issues from Critic 1 Review: Commit 5ac9cbb

## Issue 1: RESOLVED — Metadata caching added to KafkaProducer

Added `metadata_cache: Mutex<HashMap<String, i32>>` to `ProducerInner` storing topic → partition count. `wait_on_metadata()` now checks the cache first and returns immediately if the topic is already known and the requested partition is within range. Only makes a network call when the topic is missing or the partition count is insufficient.

## Issue 2: RESOLVED — wait_on_metadata now accepts partition parameter

Changed `wait_on_metadata(topic)` to `wait_on_metadata(topic, partition: Option<i32>)`. The retry loop now checks whether the requested partition falls within the known count, matching Java's `while (partitionsCount == null || (partition != null && partition >= partitionsCount))`. This supports online partition expansion.

## Issue 3: RESOLVED — topic_ids cleared on disconnect in poll_for_response

Added `self.topic_ids.clear()` in `poll_for_response()` at the disconnect handler, alongside the existing `cached_api_versions.remove()` call. Both disconnect paths now clear topic_ids consistently.

## Issue 4: RESOLVED — Dead topic_ids() trait method removed

Removed the unused `topic_ids()` method from the `ProduceClient` trait and its implementation in `KafkaProduceClient`. The internal `self.topic_ids` field is still used directly by `KafkaProduceClient`.

## ~~Issue 5~~ RETRACTED — false positive (already retracted in original review)

## Issue 6: RESOLVED — Integration test updated for wait_on_metadata flow

Updated `test_produce_to_nonexistent_topic` to:
- Use `max_block_ms(3000)` for fast failure instead of the 60s default
- Expect `send()` to return `Err(TimedOut)` instead of expecting a `SendFuture` (since `wait_on_metadata` now loops until timeout for retriable metadata errors like `UNKNOWN_TOPIC_OR_PARTITION`)

## Issue 7: RESOLVED — fetch_partitions now checks topic-level error codes

Added topic-level error code checking in `fetch_partitions()`. For retriable errors (e.g. `LEADER_NOT_AVAILABLE`), the topic is skipped and empty partitions are returned so `wait_on_metadata` retries. For non-retriable errors (e.g. `TOPIC_AUTHORIZATION_FAILED`, `INVALID_TOPIC_EXCEPTION`), an error is returned immediately. Also updated `wait_on_metadata` to propagate non-retriable errors instead of retrying.
