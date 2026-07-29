# Tier 3 Phase 6 — Producers & transactions — Review record

**Status: COMPLETE, Critic-CLEAN on first pass (no fix cycle).** Agent N=1.
Date: 2026-07-29.

## RPCs translated
`describeProducers`, `abortTransaction`, `describeTransactions`,
`fenceProducers`, `listTransactions`, `forceTerminateTransaction`.

## Commits
- `f03a133` — prerequisite: `ProducerIdAndEpoch` + transaction wire types
  (`describe_producers`, `describe_transactions`, `list_transactions`,
  `init_producer_id`, `write_txn_markers` — req+resp).
- `4ea7a74` — Phase 6 (a): describeProducers + abortTransaction.
- `a482f01` — Phase 6 (b): describeTransactions + fenceProducers.
- `41e5c13` — Phase 6 (c): listTransactions + forceTerminateTransaction.
- `3c7ceca` — Phase 6: transaction admin RPC integration tests.

## DoD verification (independently confirmed by the Critic)
- `cargo build`: clean.
- `cargo test --lib`: **2984 passed, 0 failed**.
- `cargo xtask format-check`: clean.
- `cargo xtask lint` (clippy -D warnings): clean.
- Integration (`--features integration-tests`, real 4.2 broker): 5 passed, 1 ignored.

## AllBrokersStrategy design-gap verdict — FITS CLEANLY
The first-ever use of "fan out to all brokers, no per-key lookup" (dynamic key
discovery) required **no changes** to the Tier-1 `AdminApiLookupStrategy` trait,
`LookupResult`, or `AdminApiDriver`. Discovered per-broker keys flow through
`LookupResult` as a sentinel key in `completed_keys` plus one brand-new
`mapped_keys` entry per broker; the driver already inserts these into the
fulfillment map. Proven end-to-end by the translated `AllBrokersStrategyTest`
+ `AllBrokersStrategyIntegrationTest` (5 cases) driving the real driver.
`CoordinatorStrategy` was confirmed genuinely generic over `CoordinatorType`
(used with `::Transaction`, not GROUP-hardcoded).

Critic clarification: the Actor's `fence_producers_handler.rs` comment framing
the "one fulfillment request per broker per poll cycle" as Rust-specific is
imprecise — Java's `AdminApiDriver.java:354-356` does the identical thing. Fully
faithful; comment wording only, not a defect.

## Documented skips (both Critic-confirmed legitimate, not scope-dodging)
1. `ListTransactionsHandlerTest.testBuildRequestWithDurationFilter` **case 3**
   only (`build((short)0)` with a set duration filter): Java throws
   `UnsupportedVersionException`; the Rust message generator silently omits
   below-min-version fields at serialization instead of throwing. Pre-existing,
   generator-wide. All other cases of that test are translated. **Follow-up
   ticket worth opening:** the omit-vs-throw generator behavior is a real
   wire-safety divergence from Java (a field set on an unsupported version is
   silently dropped rather than failing loudly) — pre-existing and out of Phase
   6 scope, but should be tracked separately.
2. Ongoing-transaction integration scenarios (in-flight producer state,
   `Ongoing` txn, `abort→CompleteAbort`, `ProducerFenced` on the old producer):
   require a transactional `KafkaProducer`, which the Rust `Producer` trait does
   not implement. Kept as one compiling `#[ignore]`d skeleton for re-enabling
   when the transactional producer lands.

## Notes
- The `tests/integration/main.rs` developer-local `mod admin_smoke_test_manual;`
  line was verified excluded from every commit; only `mod admin_transactions_test;`
  is committed.
- The full resolved-issues archive for the milestone is in this directory's
  `COMMENTS.DONE.1.md` snapshot.
