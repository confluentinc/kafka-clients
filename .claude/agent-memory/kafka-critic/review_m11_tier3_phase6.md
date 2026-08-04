---
name: review-m11-tier3-phase6
description: M11 Tier3 Phase6 producers/transactions review — AllBrokersStrategy driver fit, per-poll serialization is Java-faithful, generator omit-vs-throw skip legit
metadata:
  type: project
---

# M11 Tier 3 Phase 6 — Producers & transactions (CLEAN)

Reviewed commits 4ea7a74/a482f01/41e5c13/3c7ceca. No real issues. Build/2984-tests/format/lint all pass.

**AllBrokersStrategy debut adjudication (design-gap question):** The Tier-1
`AdminApiLookupStrategy`/`AdminApiDriver` needed NO changes to express
"no per-key lookup, just discover the broker list." Dynamic-key discovery is
expressed via `LookupResult { completed_keys: [any_broker], mapped_keys:
{BrokerKey(id)->id}, failed_keys:{} }` — the sentinel key completes the lookup,
each discovered broker id is a brand-new mapped fulfillment key. Integration
tests (`multi_broker_completion` etc.) prove the driver fulfills dynamically-
discovered per-broker keys end-to-end. The claim "Tier-1 needed no changes"
holds; it does NOT mask a gap.

**Per-poll "serialize same-broker requests" is Java behavior, NOT a Rust
deviation.** The Actor's `fence_producers_handler.rs` doc comment frames "one
request per broker per poll cycle" as a Rust driver limitation vs Java's
parallel fan-out. FALSE FRAMING but not a bug: Java's `AdminApiDriver.java:354-356`
does exactly the same ("Only process the first request; ... we don't want to
issue more than one fulfillment request per broker at a time"). When auditing
Unbatched handlers, check the Java driver's collectRequests before treating
per-broker serialization as a divergence.

**Generator omit-vs-throw skip is legitimate (recurring pattern).** Skips of
Java `build((short)N)` → `assertThrows(UnsupportedVersionException)` for a
set-below-min-version field are legit: the Rust generated `write()` does
`if version >= N { write field }` and silently omits when below min-version,
with no non-default-value check. Java's generated code throws. This is a
generator-WIDE, pre-existing limitation — verify by reading the generated
`*_data.rs` write() body (target/debug/build/*/out/generated/). Worth a future
generator ticket (real wire-safety divergence) but never a per-phase defect.

**StaticBrokerStrategy** build_request/handle_response panic == Java
`throw new UnsupportedOperationException()` (both never called since lookupScope
returns a fulfillment scope). Faithful.

**Error-message divergence that is NOT reportable:** DescribeProducers
InvalidTopic drops Java's rich message ("Failed to fetch metadata ... due to
invalid topic error: <fallback>") using `KafkaError::invalid_topics(set)`. Java's
own `testInvalidTopic` asserts only exception type + invalidTopics set (not the
message), and the programmatic contract matches — so not a behavioral bug. When
a message differs, check whether the Java test actually asserts it before filing.

**Transactional integration skip legit:** no init/begin/commit/abort_transaction
on Rust `Producer` (only mock_producer comments mention them). Ongoing-txn
scenarios kept as `#[ignore]` skeletons.
