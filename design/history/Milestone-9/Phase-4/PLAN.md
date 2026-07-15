# Phase 4: Share membership + heartbeat + metadata

## Goal

Translate the three share-group managers that drive the KIP-932 membership state
machine and heartbeat, plus the share-scoped metadata wrapper. These mirror the
KIP-848 consumer cousins landed in Milestone 8 (`ConsumerMembershipManager`,
`ConsumerHeartbeatRequestManager`, `ConsumerMetadata`).

## Branch

`milestone9-share-consumer`.

## Java sources

All paths relative to
`kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/`,
submodule commit `a18251bae0b825c69794a50dffd4c3100cf5ca5b`.

- `ShareMembershipManager.java`
- `ShareHeartbeatRequestManager.java` (over `AbstractHeartbeatRequestManager`)
- `ShareConsumerMetadata.java`

Tests:

- `ShareMembershipManagerTest`, `ShareHeartbeatRequestManagerTest`
  (`ShareConsumerMetadata` has no Java test; hand-written tests added.)

## Rust output

- `src/consumer/internals/share_membership_manager.rs`
- `src/consumer/internals/share_heartbeat_request_manager.rs`
- `src/consumer/internals/share_consumer_metadata.rs`

Metrics omitted (KIP-714); `// metrics: deferred to KIP-714` at omitted sites.

## Design notes

- The reconcile pipeline is a **hand-duplicated copy** (like the consumer cousin);
  `AbstractMembershipManager` has no `reconcile` method in the Rust tree. The
  share copy carries its own revoked-vs-added set-difference,
  `mark_pending_revocation`, the `on_partitions_revoked` await, and the
  same-assignment short-circuit.
- Share membership passes `autoCommitEnabled = false` and does not override
  `signalReconciliationStarted` — so the reconcile correctly omits the consumer's
  `can_commit` gate and auto-commit flush steps.
- Member epoch is `i32` (Java `memberEpoch` is `int`).
- `UNSUPPORTED_VERSION` routes to `SHARE_PROTOCOL_NOT_SUPPORTED_MSG` (broker-side)
  and `SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG` (client-side).

## Commits

- `44b8d40` — share membership + heartbeat + metadata
- `9737fdb` — fixup: correct reconcile-duplication rationale + translate the
  revoked / other-partitions-owned / same-assignment reconcile tests

## Verification

- `cargo build`, `cargo test`, `cargo xtask format-check`, `cargo xtask lint` — clean.
- `ShareMembershipManagerTest` / `ShareHeartbeatRequestManagerTest` translated;
  lib tests 2165 → 2168 after the fixup.
