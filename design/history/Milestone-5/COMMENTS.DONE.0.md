# Resolved Comments for Actor 0

## RESOLVED: About naming, whenever we're talking about "thread" in Java let's use the "task" term in Rust
- Changed "Starting Kafka producer I/O thread." to "Starting Kafka producer I/O task." in sender.rs
- Changed "Beginning shutdown of Kafka producer I/O thread" to "Beginning shutdown of Kafka producer I/O task" in sender.rs
- Changed "Shutdown of Kafka producer I/O thread has completed." to "Shutdown of Kafka producer I/O task has completed." in sender.rs
- Changed example in log_macros.rs to use "task" instead of "thread"

## RESOLVED: NetworkClient always uses LogContext::empty() instead of receiving it from caller
- **File**: `src/network_client.rs`
- **Fix**: Added `log_context: LogContext` parameter to both `with_metadata` and `with_metadata_updater` constructors. The log_context is now forwarded to `ClusterConnectionStates::new()` via `log_context.clone()`. Updated `KafkaProducer::from_config()` to pass `log_context.clone()` when constructing NetworkClient. Updated all test call sites to pass `LogContext::empty()`.

## RESOLVED: Bare log::error! used instead of kafka_error! for "Uncaught error in request completion"
- **File**: `src/network_client.rs`
- **Fix**: Changed `complete_responses` to accept a `&LogContext` parameter, replaced `log::error!(...)` with `kafka_error!(log_context, ...)`. Updated both call sites to pass `&self.log_context`.

## RESOLVED: SaslChannelBuilder passes LogContext::empty() to SaslClientAuthenticator
- **File**: `src/common/network/sasl_channel_builder.rs`
- **Fix**: Added `log_context: LogContext` field to `SaslChannelBuilder` struct, added it as a parameter to `SaslChannelBuilder::new()`, and now passes `self.log_context.clone()` to `SaslClientAuthenticator::new()`. Also updated `client_channel_builder()` in `channel_builders.rs` to accept and forward `LogContext`. Updated all test call sites and the integration test file.

## RESOLVED: "Allocating a new buffer" log message missing remaining timeout parameter
- **File**: `src/producer/internals/record_accumulator.rs`
- **Fix**: Added `"with remaining timeout {}ms"` and `max_time_to_block` to the trace log message, matching the Java message format.

## RESOLVED: "Sent produce request" trace log missing request builder details
- **File**: `src/producer/internals/sender.rs`
- **Fix**: Captured `request_builder` debug representation before it is moved into `Box::new(...)`, guarded by `log::log_enabled!(log::Level::Trace)` to avoid formatting overhead when trace is disabled. The trace log now includes the request details.

## RESOLVED: Missing "received error with leaderIdAndEpoch" debug log in Sender
- **File**: `src/producer/internals/sender.rs`
- **Fix**: Added `kafka_debug!(self.log_context, "For {}, received error {}, with leaderIdAndEpoch {:?}", batch.topic_partition, error, response.current_leader)` before the leader info update block, guarded by the same error type check as Java (`NotLeaderOrFollower || FencedLeaderEpoch`).
