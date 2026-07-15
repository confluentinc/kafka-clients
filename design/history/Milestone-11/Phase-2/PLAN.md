# Phase 2: Acknowledgement core types

## Goal

Translate the acknowledgement value/callback types and the in-flight batch
bookkeeping that the Phase 5 `ShareConsumeRequestManager` and Phase 6
`ShareConsumerImpl` orchestrate. No networking, no managers.

## Branch

`milestone9-share-consumer`.

## Java sources

All paths relative to `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/`,
submodule commit `a18251bae0b825c69794a50dffd4c3100cf5ca5b`.

- `AcknowledgeType.java` (public enum)
- `AcknowledgementCommitCallback.java` (callback interface)
- `internals/Acknowledgements.java`
- `internals/AcknowledgementCommitCallbackHandler.java`
- `internals/ShareAcknowledgementMode.java`
- `internals/ShareInFlightBatch.java` (+ `ShareInFlightBatchException`)

Tests:

- `AcknowledgementsTest` (translated with the Phase 1 support types — see
  Phase 1 `COMMENTS.DONE.1.md`)
- `ShareAcknowledgementModeTest`, `ShareAcquireModeTest`

## Rust output

- `src/consumer/acknowledge_type.rs` (public `AcknowledgeType` enum)
- `src/consumer/acknowledgement_commit_callback.rs`
  (`AcknowledgementCommitCallback` trait)
- `src/consumer/internals/acknowledgements.rs` (`Acknowledgements`)
- `src/consumer/internals/acknowledgement_commit_callback_handler.rs`
- `src/consumer/internals/share_acknowledgement_mode.rs`
- `src/consumer/internals/share_in_flight_batch.rs` (+
  `share_in_flight_batch_exception.rs`)

## Deviations (documented in code / carried to later phases)

`ShareInFlightBatch` consumes records by value because `ConsumerRecord` is not
`Clone` (§27 zero-copy). Two mechanical consequences were flagged for Phase 5 to
handle explicitly:

1. `ShareFetch.add` must read `getAcquisitionLockTimeoutMs()` *before* the
   consuming `merge` (Java reads it after; Rust's `merge` consumes `other`).
2. Delivering owned `ConsumerRecord`s to the user needs a drain/take path — a
   borrow-only getter cannot transfer ownership.

## Commits

- `b81267d` — acknowledgement core types (KIP-932)

## Verification

- `cargo build`, `cargo test`, `cargo xtask format-check`, `cargo xtask lint` — clean.
- `ShareAcknowledgementModeTest`, `ShareAcquireModeTest`, and the
  `AcknowledgementsTest` translations pass.
