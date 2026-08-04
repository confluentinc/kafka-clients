---
name: review-m11-phase2-admin-driver
description: M11 Tier1 Phase2 (createPartitions/deleteRecords + AdminApiDriver engine) review findings and driver-parity heuristics
metadata:
  type: project
---

# M11 Phase 2: Partitions & records + AdminApiDriver engine pull-forward

Commits ac67326..555cd92 on `dev/admin-client-implementation`. Verdict: clean
except ONE should-fix. See [[review-m11-phase1-admin]] for Phase 1.

## The one real finding: driver coverage gap
`AdminApiDriverTest.java` (917 lines, 17 tests) was untranslated, substituted by
`KafkaAdminClientTest` deleteRecords slices. Slices cover lookup-retry-on-topic-error,
fulfillment success/fatal, sanity check, fan-out. They MISS the driver's two
signature bidirectional branches:
1. fulfillment-response → `retry_lookup(unmapped_keys)` → re-lookup → re-fulfillment
   (Java `testFulfillmentUnmapping`). Handler unit test only checks *classification*
   into unmapped_keys, not the driver transition.
2. disconnect → `on_failure(NetworkException)` retry-lookup via the NEW Phase-2
   `Call::set_maybe_retry_fn`/`MaybeRetryOutcome` hook (Java
   `testRetryLookupAfterDisconnect`). The new machinery is the raison d'être of
   the phase and had ZERO coverage.
`admin_api_driver.rs` has no `#[cfg(test)]` module at all. Driver is pure sync
poll/on_response/on_failure — direct unit tests with fake handler/future are easy.

## Heuristic: when a "generic engine" is pulled forward, check its OWN test file
If a phase pulls forward a foundational multi-stage engine (AdminApiDriver), the
component tests (HandlerTest, StrategyTest) being fully translated does NOT mean
the engine is covered — those test classification/build in isolation. The
engine's orchestration test (DriverTest) covers the state transitions. Grep the
Java *DriverTest for `test*` and map each to a Rust test; the bidirectional /
retry-after-disconnect / static-mapping / bookkeeping ones are the easiest to
silently drop because the happy path "looks" covered by end-to-end slices.

## Adjudications that were NOT bugs (accept-with-rationale)
- `ApiRequestScope` closed enum (SingleLookup|Fulfillment(i32)) vs Java open
  interface: does NOT block Tier2 CoordinatorStrategy — pub(crate), add a variant
  non-breakingly; derived Eq/Hash give the BiMultimap keying semantics.
- `Batched`/`Unbatched` fold: `AdminApiHandler::build_request -> Vec<RequestAndKeys>`
  keeps unbatched representable; deleteRecords batches by topic, one req/broker.
- `NodeProvider::ConstantNodeId(i32)`: faithful ConstantNodeIdProvider (node-by-id
  else request_update + no node).
- `testDeleteRecordsMultipleSends` substitution: MockClient::authentication_error
  (mock_client.rs) ALWAYS returns None → can't reproduce createPendingAuthenticationError.
  Per-partition fatal error substitute exercises fan-out + independent completion. OK.

## Driver parity points verified correct (no issue)
- `clear_inflight_request` backoff: fulfillment `now + backoff((tries-1).max(0))`,
  lookup `now` (no backoff) — matches Java clearInflightAndBackoff/clearInflight.
- spec.tries read BEFORE set_inflight increments (matches Java collectRequests order).
- driver call createRequest: prebuilt.take() first send, build_request_for_spec on
  retry (Rust can't return same boxed builder twice; Java returns same spec.request).
- Arc<Mutex<AdminApiDriver>> locked only in sync closures, guard dropped per-statement,
  no re-entrant deadlock in maybe_send_requests send-fail path.
- deadline = calc_deadline_ms(now, options.timeout(), default); driver ExponentialBackoff
  built with RETRY_BACKOFF_EXP_BASE + RETRY_BACKOFF_JITTER (matches invokeDriver).
- DeleteRecords handlePartitionError order is_invalid_metadata → is_retriable → failed
  (matches Java InvalidMetadataException → RetriableException → fatal).
- MockAdminClient createPartitions/deleteRecords return per-key unsupported_version
  error (deviation from Java UnsupportedOperationException throw, documented per
  admin-client.md §9); deleteRecords empty→empty matches Java.
- Byte-level known-vector encoding tests exist for CreatePartitions v3 + DeleteRecords v2
  requests; responses round-trip only (acceptable — client decodes, not encodes).

## Note on errors.rs is_invalid_metadata (Phase 1, not Phase 2)
`is_invalid_metadata()` includes NetworkException (errors.rs:447) which is NOT a
Java InvalidMetadataException. Harmless for deleteRecords (partition results never
carry NetworkException code; driver checks NetworkException first in on_failure).
Did not flag — Phase 1 code, no Phase-2 behavioral impact. Watch if a later RPC
routes NetworkException through a partition-level classifier.
