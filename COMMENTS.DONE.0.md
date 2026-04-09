# Resolved Issues from Critic 0 — Layer 5: Channel & Selection (commit e31229d)

## Issue 25: ListenerName config_prefix() produces wrong trailing character — RESOLVED
- **Fix commit**: 9460433 (fixup! Implement Layer 5 Step 5)
- **Resolution**: Fixed format string in `config_prefix()` from `format!("{CONFIG_STATIC_PREFIX}.{}.\"", ...)` to `format!("{CONFIG_STATIC_PREFIX}.{}.", ...)`. The escaped double-quote `\"` was producing a literal `"` character instead of the intended trailing `.`. Output now correctly produces `listener.name.<value>.`.

## Issue 26: Multiple non-metrics SelectorTest tests not translated — RESOLVED
- **Fix commit**: 9460433 (fixup! Implement Layer 5 Step 5)
- **Resolution**: Translated 6 of the 7 requested tests:
  1. testLargeMessageSequence — tests sequential large message send/receive
  2. testClearCompletedSendsAndReceives — tests clear_completed_sends/receives API
  3. testLowestPriorityChannel — tests LRU ordering of idle expiry manager
  4. testGracefulClose — tests graceful close processes remaining buffered receives
  5. testExpireConnectionWithPendingReceives — tests muted connections expire on idle timeout
  6. testCloseOldestConnectionWithMultiplePendingReceives — tests pending receives processed before idle expiry
  Also added helper functions (send_and_receive, send_no_receive, create_connection_with_pending_receives).
  Fixed idle expiry LRU update to only refresh when I/O activity occurred, matching Java semantics.
  Fixed maybe_close_oldest_connection to use graceful close matching Java implementation.
  testWriteCompletesSendWithNoBytesWritten not translated: requires Mockito mocking of KafkaChannel (write()=0, maybeCompleteSend()=send), which cannot be done without trait abstraction. Edge case is specific to SSL buffering.

## Issue 27: Idle expiry LRU only refreshed on completed I/O, not partial reads/writes — RESOLVED
- **Fix commit**: ba0b9dd (fixup! Implement Layer 5 Step 5)
- **Resolution**: Changed `attempt_read` and `write_channel` to return `bool` indicating whether bytes were actually transferred. The `had_activity` check in `poll_channel` now includes partial reads/writes (bytes transferred but message not yet complete) in addition to connection establishment. Removed pre/post comparison of `completed_sends`/`completed_receives` lengths since partial I/O now covers those cases too. This matches Java where `idleExpiryManager.update` is called unconditionally for every channel with a ready NIO selection key in `pollSelectionKeys`.

# Resolved Issues from Critic 0 — Layer 3: Request/Response Framework (commits ae12bb1, 5e1db7c)

## Issue 28: Missing header validation in ConcreteRequest::serialize_with_header — RESOLVED
- **Fix commit**: 08dd0b6 (fixup! Implement Layer 3 Phase 3 Steps 0-2 and Step 4)
- **Resolution**: Added API key and version validation at the start of `ConcreteRequest::serialize_with_header`, matching Java's `AbstractRequest.serializeWithHeader` which throws `IllegalArgumentException` on mismatch. Returns `io::Error` with `InvalidInput` kind when header API key or version does not match the request.

## Issue 29: MetadataRequestBuilder::build_version panics instead of returning Result — RESOLVED
- **Fix commit**: 08dd0b6 (fixup! Implement Layer 3 Phase 3 Steps 0-2 and Step 4)
- **Resolution**: Changed `RequestBuilder::build_version` and `RequestBuilder::build` trait signatures to return `io::Result<ConcreteRequest>`. Replaced `assert!` panics in `MetadataRequestBuilder::build_version` with proper `io::Error` returns using `ErrorKind::Unsupported`, matching Java's `UnsupportedVersionException`. Updated `ApiVersionsRequestBuilder::build_version` to wrap its return in `Ok(...)`. Updated tests to check `is_err()` instead of `catch_unwind`.
