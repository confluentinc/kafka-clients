# Consumer test-translation fidelity review — summary

Date: 2026-06-18. Scope: KIP-848 consumer (`org.apache.kafka.clients.consumer.*`)
unit + integration tests, Java 4.2 → Rust. Classic-protocol, Share, Streams,
assignors, and metrics-manager classes are out of scope per
`.claude/rules/consumer-threading.md` §20.

Method: every Java `@Test` read against its Rust counterpart (inline `#[cfg(test)]`
+ `tests/`), classified PRESERVED / REDUCED / CHANGED / MISSING (in-scope) /
OUT_OF_SCOPE. Per-subsystem detail in files 01–07 in this directory.

## Verdict

Translation fidelity is **high and uneven**. Value types, `SubscriptionState`,
the event/reaper plumbing, the network thread, and the top-level consumer
behavioral suite are faithful — often *stronger* than Java. The gaps cluster in
four areas, all on the request-manager response paths and at the integration
layer.

The §31 mandatory rebalance-listener regression tests **exist and are faithful**
(`async_kafka_consumer.rs:5706`, `:5816`).

## Subsystem scorecard

| # | Subsystem | Java in-scope | Verdict |
|---|---|---|---|
| 01 | Fetch path (FRM/collector/buffer/completed/config) | ~124 | ⚠️ decode layer strong; ~37–48 integration-level behaviors untested |
| 02 | Membership & heartbeat | ~120 | ⚠️ state-machine core preserved; reconciliation/STALE/HB-field-diff untested |
| 03 | Commit / offsets / coordinator | ~147 | ⚠️ reset-positions + LogTruncation untested; error matrices collapsed |
| 04 | Events / network thread / request plumbing | ~96 | ✅ no genuine in-scope gap; WakeupTrigger redesigned but guarantees retested |
| 05 | SubscriptionState / metadata / value types | ~116 | ✅ no MISSING in-scope; SubscriptionState exemplary |
| 06 | AsyncKafkaConsumer / KafkaConsumer / MockConsumer | ~221 | ✅ behaviorally faithful; metrics block absent; few error-message reductions |
| 07 | Integration | ~70 | ⚠️ 3 large Java files unmirrored; §31 reentrancy + wakeup untested at integ |

## Cross-cutting themes

1. **Metrics are entirely untranslated (deliberate deferral).** Accounts for a
   large share of every file's OUT_OF_SCOPE/MISSING count (FetchMetricsManager,
   AsyncConsumerMetrics, rebalance/heartbeat/commit metrics, lead/lag,
   clientInstanceId/telemetry). Single biggest sign-off decision: confirm the
   metrics deferral is acceptable, or schedule it.

2. **Request-manager *response-path* coverage is the real gap.** Fine-grained
   decode/build is well unit-tested, but the behaviors that need a fetch/commit
   round-trip are thin or absent:
   - **Reset-positions state machine** (offsets_request_manager): zero
     behavioral/response-path assertions — can regress silently. *Largest single gap.*
   - **OffsetValidation → LogTruncation** (`auto.offset.reset=none` contract): untested.
   - **Metadata-driven reconciliation** (membership): Rust tests pre-seed the
     topic-name cache, bypassing unresolved-assignment / delayed-result-discard.
   - **STALE-member path**: state exists, never driven.
   - **Fetch-session / topic-id negotiation, buffered-partition exclusion,
     KIP-951 leadership-change**: missing.

3. **Parameterized error matrices collapsed to one representative.** Commit
   (~32 error cases → few), OffsetsRequestManager (10→1), OFLE (1 of 7),
   FetchWithOtherErrors (3 of all). The retriable-vs-fatal classification and
   exact-exception-class mapping required by DoD §3 is the most pervasive loss —
   round-trip tests do **not** catch a wrong classification.

4. **Heartbeat request-field diff unpinned** (`testFirstHeartbeatIncludes…`,
   `testHeartbeatState`): wire-field omission logic untested; a wrong diff is
   wire-incompatible and round-trips won't catch it (DoD §3 wire-vector rule).

5. **Integration coverage holes:** `PlaintextConsumerCommitTest` (12),
   `PlaintextConsumerCallbackTest` (9), `PlaintextConsumerTest`/Base (~40) have
   no Rust counterpart. Rebalance-listener reentrancy (§31 beyond
   commit-in-revoke), `wakeup()` at integration level, pause/resume,
   offsets_for_times/beginning/end at integration are untested.

6. **Stale in-source doc claims (correctness-of-docs bug).** Several test-module
   headers cite functions that don't exist or stale counts:
   - `commit_request_manager.rs` header: cites
     `test_fail_all_with_error_via_coordinator_fatal` — does not exist.
   - membership/heartbeat headers: stale "~26/93", "12/31" counts (Phase-12.5
     additions not reflected).

## Recommended follow-ups (priority order)

1. Decide metrics: accept deferral for sign-off, or schedule translation.
2. Add reset-positions + LogTruncation response-path tests (offsets_request_manager).
3. Restore parameterized error-classification matrices (commit, offsets, OFLE) —
   assert retriable/fatal + exact error code per `Errors` variant.
4. Add heartbeat request-field-diff byte/field assertions.
5. Add metadata-driven reconciliation + STALE-member membership tests.
6. Port the three unmirrored integration files (Commit/Callback/Plaintext), at
   least the KIP-848 arms; add §31 reentrancy + wakeup integration tests.
7. Fix stale test-module doc headers (cheap, do alongside).

## Notes on legitimate divergences (not gaps)

- WakeupTrigger 18→10: Rust uses a rotating `CancellationToken` (§11); the
  `Wakeupable` state-machine tests have no analog by design, both guarantees
  (interrupt + rotate-after-one-throw) are retested.
- `ConcurrentModification` tests: `&mut self` makes concurrent calls a compile
  error — no runtime analog needed.
- Null-argument / `Serializable` / `ConfigDef.Validator` tests: Rust type system
  removes the failure mode.
- Abstract*Manager Rust unit tests: legitimate bonus (Java has no such classes).
