---
name: review-m8-phase37
description: Phase 37 fetch round-trip — PLAN-vs-impl test bookkeeping drift, control-record gap provenance, KIP-951 leadership fix audit heuristics
metadata:
  type: project
---

Phase 37 (fetch round-trip harness + 57 FetchRequestManagerTest behaviors). Patterns worth reusing:

**PLAN-lists-it-but-code-omits-it is a high-yield check.** The PLAN enumerated 7b tests (pause/seek family x4, testFetchResultNotProcessed..., testSeekBeforeException) and the buffered-partition family. Cross-check: `awk '/mod round_trip/{f=1} f&&/fn test_/' | sed | sort` the actual test names vs the PLAN's Java-name list. Phase 37 silently dropped 5+ in-scope manager-level tests that were translatable (all primitives existed in subscription_state). PLAN bullets are a contract; treat any listed-but-absent test as a Missing Requirement.

**Test reuses another test's mutation under a different name = WRONG-scenario smell.** `test_..._missing_position` used `request_offset_reset_default` — identical to `test_..._reset_offset`. Java's missing-position test sets a NULL position and asserts `assertFutureThrows(IllegalStateException)`. The Rust test dropped the error assertion entirely. When two "different reason" tests share a byte-identical mutation, open both Java sources — they almost certainly diverge.

**Control-record gap provenance.** completed_fetch.rs:909 returns UnsupportedVersion for READ_COMMITTED control-batch-from-aborted-producer (containsAbortMarker NOT translated; ControlRecordType::parse_key missing). REAL prod gap (READ_COMMITTED + aborted txn breaks) but PRE-EXISTING from Phase 7a (8da37b5), not the phase under review. Use `git log -S "<string from the bail-out>"` to date the provenance before blaming the current phase. The two passing control tests don't hit it: one is READ_UNCOMMITTED, the other uses a non-control aborted DATA batch (control=false) → is_batch_aborted path, not the marker path. No contradiction.

**KIP-951 leadership fix audit (AbstractFetch.java:180-251).** Genuine bug — new-leader info was dropped pre-fix. Verify: (a) error-path-gated accumulation (only NOT_LEADER/FENCED + leaderId/epoch != -1); (b) post-loop guarded by is_empty() so happy path pays only an i16 match — no alloc/lock/clone; (c) node_endpoints.clone() per RESPONSE is free (empty Vec in steady state, populated only on errors); (d) §27 alloc-budget test still exercises the full path and wasn't weakened (re-run it). Node.noNode() filter: Java compares ALL fields via equals; Rust filtering on node_id==-1 only is a benign divergence for malformed endpoints.

**ApiVersions re-add wiring check:** confirm production passes the SHARED Arc<ApiVersions> (the one also given to NetworkClient — grep all api_versions usages in async_kafka_consumer.rs), not a fresh `ApiVersions::new()`. Test fixtures legitimately use a fresh one.

**Harness faithfulness confirmed good pattern:** RoundTrip drove prepare_fetch_requests → create_fetch_request().build() → handle_fetch_success/_failure → FetchCollector::collect_fetch with no short-circuit. That is the bar — a harness that calls the real network-callback entry points, not a reimplementation.
