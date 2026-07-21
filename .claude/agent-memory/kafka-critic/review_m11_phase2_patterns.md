---
name: review-m9-phase2-patterns
description: M11 Phase 2 share-consumer acknowledgement core types — review patterns for move-semantics rewrites, standalone exception structs, ownership-consuming deviations
metadata:
  type: project
---

M11 Phase 2 (`b81267d`) = ShareInFlightBatch, ShareInFlightBatchException,
ShareAcknowledgementMode, AcknowledgementCommitCallback(+Handler). Reviewed clean.

**Reusable audit heuristics that paid off here:**

- **Single-pass move rewrite vs Java copy-then-remove**: When ConsumerRecord (non-Clone)
  forces the Actor to fold Java's two-step (copy refs to renewing set, then clear/remove
  from in-flight) into one loop, verify the *routing predicate* is unchanged. Key check:
  which offsets land in `acknowledged_records`. Only `acknowledge()`/`acknowledge_all()`
  add to it; `add_acknowledgement()` (RELEASE-on-exception) does NOT. So RENEW routing
  and removal are driven by the same set as Java. Java's `clear()`-when-all-acked branch
  is a pure optimisation — identical end state to per-offset remove.

- **Standalone-struct-for-Java-exception safety**: When a Java exception extending
  SerializationException/KafkaException is modeled as a plain struct (not a KafkaError
  variant), the risk is `is_retriable`/type-based classification loss. Resolve by grepping
  the ONE caller: here `ShareFetchCollector.java:115` does `getException().cause()` and
  rewraps — never relies on the wrapper's own type. Rust `cause() -> &KafkaError` carries
  retriability. Safe. Always find the caller before flagging.

- **Ownership-consuming deviation (merge(self)/get_x()->Vec<&T>)**: correct for the
  current phase but leaves forward risks for the wiring phase. Record them as non-blocking
  Phase-N notes, not defects: (1) callers that read a field AFTER Java's `merge` must
  reorder before Rust's consuming merge (ShareFetch.add reads getAcquisitionLockTimeoutMs
  after merge); (2) borrow-only accessors can't transfer owned non-Clone records to the
  user — a drain/take path will be needed where Java returned a shared-reference list.

- **retryOnExceptionWithTimeout dropped in test**: legitimate when Java needed it because
  the callback fired on the bg thread but the Rust callback fires synchronously on the
  caller's task (§31). State is observable immediately after the await returns.

- **Validator/ConfigDef deferral**: accepted precedent (ShareAcquireMode). OK as long as
  invalid values are still rejected at from_string parse time and the deferral is
  documented on the type + the deferred test is named.
