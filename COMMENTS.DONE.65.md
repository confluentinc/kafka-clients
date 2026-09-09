# Critic 65 — P2 review: resolved / deferred

Findings from `COMMENTS.65.md` (Critic 65, review of commits 7049b57e /
3d790a2d / 9da263bf / 7551bc68). F1–F3 fixed; F4–F5 deferred to P4 per the
Manager's instruction (they are P4 wiring/spec-reconciliation items, not P2
defects). The four notes N1–N4 are owner/rule-text items (no Actor change).

---

## F1 — RESOLVED — `ConsumerRecordsTest` rate-limit & no-log branches translated
- **Fix**: `bindings/python/test/unit/test_consumer_types.py` — replaced the
  single empty-map test with the three Java `ConsumerRecordsTest` `nextOffsets`
  tests, now asserting the log side via `caplog` on the
  `confluent_kafka.consumer.consumer_records` logger:
  - `test_next_offsets_logs_error_periodically_when_tainted` — mirrors Java's
    `testNextOffsetsLogsErrorPeriodicallyWhenConstructedWithDeprecatedConstructor`:
    forces the window elapsed (sets the module global
    `_tainted_next_offsets_last_log_s` into the past, as Java sets the static
    `TAINTED_NEXT_OFFSETS_LAST_LOG_NS`), asserts exactly ONE ERROR, then that
    repeated calls and a new tainted instance within the window log nothing
    (throttle), then that re-elapsing the window logs a second time. This is the
    first coverage of the production `_maybe_log_tainted` rate-limiter.
  - `test_next_offsets_does_not_log_when_supplied` — Java
    `testNextOffsetsDoesNotLogErrorWhenConstructedWithNextOffsets`.
  - `test_next_offsets_does_not_log_for_empty_records` — Java
    `testNextOffsetsDoesNotLogErrorForEmptyRecords`.
- **Verify**: 3 tests pass; the rate-limiter and both no-log branches are covered.

## F2 — RESOLVED — `MetricName` description-excluded equality + tag-order-independent hash asserted
- **Fix**: `bindings/python/test/unit/test_common_types.py` — added two tests per
  `MetricName.java` `equals`/`hashCode` (over group, name, tags — description
  excluded):
  - `test_metric_name_equality_excludes_description` — two instances differing
    ONLY in `description()` are equal with equal hash.
  - `test_metric_name_hash_is_tag_insertion_order_independent` — two instances
    with the same tags in different insertion order are equal with equal hash
    (the `__hash__` sorts `tags.items()`).
- **Verify**: 2 tests pass.

## F3 — RESOLVED — write-path header validator made binding-internal
- **Fix**: renamed `validate_written_headers` -> `_validate_written_headers`
  (leading underscore) in `bindings/python/confluent_kafka/common/headers.py`
  and dropped it from `confluent_kafka.common.__all__` and the `common/__init__.py`
  re-export, with a docstring note that Java has no free header-validation
  function (`RecordHeaders(Iterable<Header>)` normalizes internally) so the helper
  is deliberately off the public `common` surface (rule 2 / DoD #7). The
  intra-package imports in `consumer_record.py` / `producer_record.py` now use the
  private name. Added `test_validate_written_headers_is_not_public` asserting the
  name is absent from `common.__all__` and `hasattr(common, ...)` is False; the
  three existing validator tests use the private import.
- **Verify**: the public `confluent_kafka.common` surface is now fully
  Java-mirrored; tests pass.

---

## F4 — DEFERRED to P4 — receive-path zero-copy vs the write-path copy validator
- Critic's own classification: "a P4 wiring hazard to flag now, not a P2
  behavioural bug (no receive path exists yet)." `ConsumerRecord` constructing
  headers through `_validate_written_headers` (which copies byte-likes to owned
  `bytes`) is correct for a user-constructed record in P2. The P4 brief must add a
  no-copy fetch construction path (a private `_from_fetch` that skips the
  validator and hands back `memoryview`s borrowing the fetch batch) per
  consumer-threading.md §27 / spec §5.2 / CLAUDE.md §12. No P2 change.

## F5 — DEFERRED to P4 — `ProducerRecord` "C extension type" spec text vs pure-Python choice
- Critic's own classification: "for the P4 brief." C10 already records the
  pure-Python choice and its DoD #10 reasoning (owned `bytes`, no per-record copy
  in the value type; the native `_confluentkafka.ProducerRecord` untouched), which
  holds for P2. P4 must either make `send()` adopt this value type into the native
  struct without a per-record copy, or update the spec text; P4's Critic checks
  the reconciliation avoids a per-record copy (DoD #10). No P2 change.

---

## Notes (owner / rule-text — no Actor change, recorded for the owner)
- **N1** — rule 10a / P2-task "raises" wording contradicts Java 4.3.1
  (`ConsumerRecords.nextOffsets()` logs-and-returns-empty, does not throw). The
  Actor followed Java (C6); the **rule text** should be amended. Owner item.
- **N2** — spec §5.3 lists `OffsetResetStrategy` members out of Java order; the
  Actor followed Java (`LATEST, EARLIEST, NONE`). Fix the spec, not the code.
- **N3** — spec §5.1 types `TopicIdPartition.topic()` as `str`; the Actor widened
  to `str | None` (C7), Java-faithful. Owner to reconcile the spec stub.
- **N4** — `TimestampType.for_name` raises `KeyError` where Java raises
  `NoSuchElementException` (no JDK analog on the Python surface, C8). Owner to
  confirm `KeyError` or mint a mapped class.
