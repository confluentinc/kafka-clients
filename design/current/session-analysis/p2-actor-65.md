# P2 — Actor 65 session analysis

Phase **P2** of `PLAN-python-interface-implementation.md`: the pure-Python value
types of the new `confluent_kafka` package (common, consumer, producer), one Java
class per file, re-exported from each package `__init__`. Branch
`dev_python-interface-implementation`. Committed, not pushed.

## What was built

### 1. `confluent_kafka/common/` value types
- `topic_partition.py` — `TopicPartition` (keyword-only, value equality + hash,
  Java `toString` repr).
- `uuid.py` — `Uuid`. Ported Java's algorithm faithfully: URL-safe base64
  `__str__` / `from_string` round-trip (max-24-char / 16-byte rejects →
  `IllegalArgumentError`), signed 64-bit `(msb, lsb)` `compareTo` ordering (rich
  comparisons), `RESERVED` = {ZERO, ONE}, `METADATA_TOPIC_ID == ONE_UUID`,
  `random_uuid()` reserved-range + leading-dash rejection, and Java's exact
  `hashCode` (`(int)(xor>>32) ^ (int)xor`). Java `testHashCode` vectors match
  exactly (23 / 19064 / -2011255899); `vDiRhkpVQgmtSLnsAZx7lA` round-trip matches.
- `timestamp_type.py` — `TimestampType(IntEnum)`, member value == Java `id`
  (-1/0/1); Java's label `name` field via `label()` / `__str__`, `for_name`
  lookup (C8).
- `node.py` — `Node` (3→1 ctor collapse, `no_node`/`id_string`/`has_rack`/
  `is_fenced`/`is_empty`).
- `topic_id_partition.py` — `TopicIdPartition`, two `@overload` ctor stubs via
  `_args.exactly_one`; defined for parity (share-consumer-only surface, rule 10a);
  `topic() -> str | None` Java-faithful (C7).
- `partition_info.py` — `PartitionInfo` (6-arg/5-arg collapse, `Node[]` → tuple,
  Java `toString`).
- `metric_name.py` — `MetricName` (equality over name/group/tags; description
  excluded, per Java).
- `metric.py` — `Metric` / `KafkaMetric` `Protocol`s; `config()`/`measurable()`
  opaque `object` (C12).
- `headers.py` — `Headers` read-form alias + `validate_written_headers` write-side
  validator.
- `__init__.py` re-exports all ten (the P1 `common.errors`/`common.config`
  sub-packages untouched — Actor/Critic 64's territory).

### 2. `confluent_kafka/consumer/` value types
- `offset_and_metadata.py` — `OffsetAndMetadata` (3→1 ctor, negative offset →
  `IllegalArgumentError`; `leader_epoch()` filters null-or-negative; equals over
  the **filtered** epoch — Java parity).
- `offset_and_timestamp.py` — `OffsetAndTimestamp` (equals over the **raw** stored
  epoch, unlike `OffsetAndMetadata` — Java difference preserved).
- `consumer_group_metadata.py` — `ConsumerGroupMetadata` (deprecated ctor kept,
  defaults = one-arg overload's -1/""/None; group_id/member_id null → `TypeError`).
- `close_options.py` — `CloseOptions` Java builder shape: private ctor
  (`CloseOptions()` → `TypeError`), static `timeout`/`group_membership_operation`
  factories, fluent `with_*` → Self, nested `GroupMembershipOperation` enum. A
  `_StaticOrInstance` descriptor dispatches the two Java-overloaded names
  (static factory vs instance getter) — C9.
- `subscription_pattern.py`, `offset_reset_strategy.py` (deprecated, Java member
  order, lowercase `__str__`).
- `consumer_record.py` — `ConsumerRecord` (3→1 ctor, null topic/headers →
  `IllegalArgumentError`; no equals in Java, so none here).
- `consumer_records.py` — `ConsumerRecords` (`records(*, partition|topic)` 2-stub
  collapse, `partitions`/`next_offsets`/`empty`; the deprecated records-only ctor's
  `next_offsets()` LOGS and returns `{}` as Java does — it does NOT raise, C6).
- `__init__.py` re-exports the eight new types alongside the P1 error re-exports.

### 3. `confluent_kafka/producer/` value types
- `producer_record.py` — `ProducerRecord` pure-Python value type (6→1 ctor, null
  topic / negative partition|timestamp → `IllegalArgumentError` with Java's exact
  messages, null value legal, headers normalized, value equality + hash over all
  fields). Kept pure-Python rather than modifying the native
  `_confluentkafka.ProducerRecord` send-path struct; DoD #10 preserved (C10).
- `record_metadata.py` — `RecordMetadata` (Java's 8 methods incl.
  `has_offset()`/`has_timestamp()` -1 sentinel + `baseOffset==-1` batch-index
  rule; no equals in Java, none here).
- `__init__.py` re-exports both.

### 4. Typing gate
- `test/unit/test_typing.py` — `typing.assert_type` assertions that
  machine-verify the `@overload` stubs (`TopicIdPartition`,
  `ConsumerRecords.records`), the `Generic[K, V]` inference on the record types,
  and the `long + hasX() -> int` (not `int | None`) sentinel pairs.
- `bindings/python/Makefile` `typecheck` target extended to also
  `mypy --strict`-check `test/unit/test_typing.py`, so a broken stub fails
  `make verify` (spec §11 / rule 11).

## Java tests translated / skipped

| Java test | Status | Notes |
|---|---|---|
| `UuidTest` | translated | `@RepeatedTest(100)` → parametrized loop. `testToArray`/`testToList` **skipped** — `Uuid.toArray`/`toList` are static Java array↔List helpers with no Python surface. |
| `TopicPartitionTest` | replaced | Its two tests are Java-`Serializable` round-trips (`ObjectOutputStream` / a checked-in blob) — no Python analog; replaced with value-type tests. |
| `TopicIdPartitionTest` | translated | `testEquals`/`testHashCode`/`testToString` + accessor/combination tests. |
| `PartitionInfoTest` | translated | `testToString` + offline-default/equality/null-leader tests. |
| `OffsetAndMetadataTest` | translated | `testInvalidNegativeOffset`, `testEqualsWithNullAndNegativeLeaderEpoch`, `testEqualsWithNullAndEmptyMetadata` translated; the 3 `Serializable`/blob-deserialization cases **skipped** (no Python analog). |
| `ConsumerGroupMetadataTest` | translated | All 4 (null-arg cases → `TypeError`, Python's `NullPointerException` analog). |
| `CloseOptionsTest` | translated | All 4. |
| `ConsumerRecordTest` | translated | `testShortConstructor`/`testLongConstructor`. |
| `ConsumerRecordsTest` | translated | iterator / by-partition / by-topic / immutability / next-offsets tainted-vs-supplied. Null-topic `records()` raises the collapsed combination error, not Java's "Topic must be non-null." (C11). |
| `ProducerRecordTest` | translated | `testEqualsAndHashCode`/`testInvalidRecords`. |
| `RecordMetadataTest` | translated | both ctor cases + sentinel coverage. |
| `SubscriptionPatternTest`, `OffsetAndTimestampTest`, `NodeTest`, `TimestampTypeTest`, `MetricNameTest` | no Java test | Behavioural tests added for parity. |

## Test counts

- `test_common_types.py` **131** items, `test_consumer_types.py` **33**,
  `test_producer_types.py` **10**, `test_typing.py` **1** (backed by static
  `assert_type` checks) — **175 new**.
- Full unit suite: **573 passed, 2 skipped** (398 pre-existing incl. the P1 error
  suite + 175 new).
- `mypy --strict confluent_kafka test/unit/test_typing.py`: clean over **33**
  source files.

## Clarifications logged (`design/current/implementation-clarifications.md`, C6–C12)

- **C6** — `ConsumerRecords.next_offsets()` on the deprecated records-only form
  LOGS (rate-limited) and returns `{}`, it does NOT raise — Java 4.3.1 source
  contradicts rule 10a's "raises" wording; followed Java (rules §1).
- **C7** — `TopicIdPartition.topic()` typed `str | None` (Java returns null), not
  spec's `str`.
- **C8** — `TimestampType` exposes Java's `name` label via `label()`/`__str__`/
  `for_name` (Python's `.name` is the member name); value == Java `id`.
- **C9** — `CloseOptions.timeout` / `group_membership_operation` overload one
  Python name (static factory + instance getter) via a `_StaticOrInstance`
  descriptor.
- **C10** — `ProducerRecord` is a pure-Python value type in the package; the
  native `_confluentkafka.ProducerRecord` send-path struct is reconciled in P4
  (DoD #10 reasoning recorded).
- **C11** — `ConsumerRecords.records(topic=None)` raises the collapsed combination
  error (`_args.exactly_one`), not Java's literal "Topic must be non-null."
- **C12** — `KafkaMetric` typed as a `Protocol`; `config()`/`measurable()` return
  opaque `object` (metrics design pass).

## Verification

`mypy --strict` clean; `pytest test/unit` → 573 passed / 2 skipped. Each of the
three commits' pre-commit hook (`make verify-sandbox`: release build, C build,
format-check, lint, Docker integration + C tests) passed. The producer commit's
first attempt hit an unrelated Rust integration flake
(`plaintext_consumer_commit_test::test_async_consumer_async_commit`, no
Python/P2 code involved); retried once per the phase's known-flake instruction and
it passed.

## Concurrency

Actor/Critic 64 worked on `xtask/` and `confluent_kafka/common/errors/`
concurrently; those paths were not touched. All commits used path-limited
`git commit -- <paths>` so the shared index was never swept, and each waited for
any in-flight commit / pre-commit hook to clear first.

## Commits

1. `7049b57e` — P2: confluent_kafka.common value types (TopicPartition, Uuid, Node, …)
2. `3d790a2d` — P2: confluent_kafka.consumer value types (records, offsets, CloseOptions, …)
3. `9da263bf` — P2: confluent_kafka.producer value types + typing gate + clarifications

`COMMENTS.65.md` was empty at start and end.
