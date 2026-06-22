---
name: review-m8-phase7b-patterns
description: M8 Phase 7b review — FetchCollector/FetchRequestManager translation pitfalls
metadata:
  type: feedback
---

Patterns recorded during review of Milestone-8 Phase-7b (commits 7580296..f332706).

## Java CompletableFuture single-slot-with-chaining → Rust queue mismatch
Java's `FetchRequestManager.createFetchRequests()` holds a single `pendingFetchRequestFuture` slot; multiple concurrent callers all get chained futures that complete TOGETHER on the next `pollInternal`. A direct Rust translation that uses a `VecDeque<oneshot::Sender>` and pops ONE per poll changes the semantics (N callers need N polls instead of 1). **Why:** subtle — looks like "FIFO" but Java is "chain + clear-after-one-poll". **How to apply:** when a Java method takes the pattern `if (existing != null) existing.whenComplete(chain); else existing = new`, expect a single-slot translation, not a queue.

## IllegalStateException vs KafkaException scope-of-catch
Java's `try { ... } catch (KafkaException e)` does NOT catch `IllegalStateException`. When the Rust port wraps both as `KafkaError::illegal_state(...)`, the outer "empty-then-defer" check swallows what Java would have propagated. **Why:** Java exception hierarchy is load-bearing here, not incidental. **How to apply:** if Java throws `IllegalStateException` inside a `try { ... } catch (KafkaException e)`, the Rust translation must either propagate immediately, use a separate `Err` variant, or panic — not stuff it into a generic `KafkaError`.

## New method without test ≈ "claim was a lie, replacement also untested"
Phase 7a's docstring claimed `prepare_fetch_requests` was implemented; the diff showed only the docstring mention with no fn definition. Phase 7b added the actual implementation (~140 LOC) but with ZERO direct tests — the higher-level `FetchRequestManager` tests only exercise the empty-partition / no-pending-ack paths. **Why:** when the previous phase's docstring overclaims, the next phase tends to add the implementation but inherit the missing-test gap. **How to apply:** when reviewing a "promotes pre-existing private method to public" or "adds method whose claim was deferred", explicitly check the test count delta for the new function. If zero, flag as DoD §3 violation.

## "MockClient required" defense — partially true but masks `prepare_*` tests
Actor's deferral of 78/88 `FetchRequestManagerTest` tests with the "all rely on MockClient" defense holds for the network-IO tests (sampled 5, all use `client.prepareResponse` + `networkClientDelegate.poll`). **But** the `prepare_*` level methods (`prepareFetchRequests`, `prepareCloseFetchSessionRequests`) can be exercised directly without MockClient — only `SubscriptionState` and `ConsumerMetadata` are needed plus the two `is_unavailable`/`maybe_throw_auth_failure` closures. **How to apply:** when the Actor defers tests with a "MockClient required" justification, identify which tests actually require network-driven state and which can use direct state manipulation. The latter should still be translated.

## `_param` underscore-prefix unused parameter = design smell
`compute_buffered_nodes(_is_unavailable: &impl Fn(...))` — parameter never used, prefixed with `_` to silence the warning. This signals an incomplete translation: either Java does use it and Rust missed the use site, or it shouldn't be a parameter at all. **How to apply:** when a Rust closure-taking function has an `_unused`-prefixed function parameter, double-check the Java equivalent. If Java doesn't use it either, remove the parameter rather than masking the warning.

## `with_first` orphan helper / dead code
A new `pub(crate) fn with_first<R>(...)` was added to `FetchBuffer` with the justification "needed for the peek pattern" but is never called. CLAUDE.md DoD §7 forbids structs/traits not in Java. **How to apply:** when the Actor lists "added helpers" in a commit message, grep for usage of each helper. If a helper is never called from production code, flag as dead code.

## §27 budget docstring vs empirical disagreement
Budget docstring enumerated "2 deserializer + 1 RecordHeaders::from_slice = 3 allocs/record", but empirical was 4.2 with test fixture using `vec![]` headers (where from_slice allocates 0). The 1.2 difference is unexplained. **How to apply:** when reviewing an allocation-budget test that claims "exactly N allocs/record from these sources", verify by re-running with RECORD_COUNT=1 or use the fixture that exercises each enumerated source.

## FetchResponse / inconsistent `set_throttle_time_ms` vs `maybe_set_throttle_time_ms`
Every variant of `ConcreteResponse::maybe_set_throttle_time_ms` dispatches to a method named `maybe_set_throttle_time_ms` EXCEPT Fetch, which calls `set_throttle_time_ms`. Java's `AbstractResponse` declares `public abstract void maybeSetThrottleTimeMs(int)` and all impls (including FetchResponse) override it with the `maybe_` name. **How to apply:** when reviewing Concrete enum dispatch, scan for asymmetric method-name dispatch — usually a rename oversight.

## get_error_response v<13 partition-walk skip
Java's `FetchRequest.getErrorResponse` walks topic/partition data and sets per-partition error code for v<13. Rust port unconditionally sets only top-level. Phase 7b's intended use is transport error, where v13+ is standard — but the v<13 codepath is still in the Java codebase. **How to apply:** when a Java method has an `if (version() < N) { walk-data }` branch and the Rust port skips it, flag as a behavior gap even if "unlikely in practice".

## §27 zero-copy audit — verification heuristics
The §27 contract holds if:
- `ConsumerRecord::topic` is `Arc<str>` (not `String`).
- `Deserializer::deserialize(&self, &str, &[u8])` is sync (no `#[async_trait]`).
- Per-record path doesn't `Bytes::copy_from_slice` (use `slice`).
- No `String::from_utf8(topic_bytes.clone())` per record.
- No per-record `tokio::spawn`.
- No `DefaultRecord::clone()` on the happy path.
- Allocation tracker test exists with `≤6 allocs/record` budget AND `≥record_count` lower bound.

Phase 7b satisfies all of these. Phase 7c/7d should be audited against this checklist.
