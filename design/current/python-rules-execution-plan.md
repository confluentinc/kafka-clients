# Python client — execution plan for the decided rules

Date: 2026-10-06. Branch `dev_python-interface-on-master`, worktree `/home/prathi/work/ecfk-py-master`.
Java source: `kafka/` (Apache Kafka 4.3.1). Rules: `CLAUDE.md`, section **Python Binding Conventions**
(round 1 below, already committed when an Actor reads this).

This file is the briefing for the Actor agents and the record the Critic reviews against. The rules
are the contract; this file says which code has to change to meet them, and nothing else.

## Process (set by the owner)

- **Minimal diff.** Touch only the files listed for your round. No refactors, no renames, no
  reformatting of untouched code, no new abstractions, no "while I am here" fixes. If a listed change
  needs a file that is not listed, make the smallest edit there and name it in the commit message.
- **Tests before each commit, only the touched ones:** `pytest <files>` from `python/`;
  `cargo test -p xtask <filter>` for generator tests; `cargo xtask generate-error-codes` then
  `cargo xtask check-generated` when the generator changed; the mypy gate
  `python -m mypy --strict confluent_kafka test/unit/test_typing.py` (from `python/`) when any
  stub or signature changed. Do **not** run `make verify` or the full pre-commit hook: commit with
  `git commit --no-verify`. The Manager runs `make verify` once at the end of the round.
- **Commit messages:** imperative subject, a body saying what changed and which rule it meets. No
  `Co-Authored-By` trailer and no "Generated with Claude Code" line.
- **Do not edit** `CLAUDE.md`, `.claude/rules/*`, the spec or design notes under `design/`, or
  anything on Confluence. Do not push.
- **Critic loop:** findings arrive in `COMMENTS.<N>.md` at the repo root. Fix each, then move it to
  `COMMENTS.DONE.<N>.md` with a line saying what was done. Commit fixes as `fixup!` of the commit
  that introduced the issue.
- Line numbers below are from a read on 2026-10-06; locate by identifier when they have drifted.
- Java messages are asserted verbatim in tests. When a Java detail is needed, read
  `kafka/clients/src/main/java/org/apache/kafka/clients/...`.

## Round 1 — rules (done by the Manager)

`CLAUDE.md` edits: §4 `_async`-form sentence; Scope errors sentence; Class family async-oracle bullet;
Signatures: defaults clause, explicit-`None` deviation, serde exception, stub principle, stub examples,
binding-stub `None` wording, exact `java_forms` matching, `UNSET` rule, timeout overflow, private-overload
nullability; Errors `cause` clause; Implementation: FFI mocks clause, merged "no check of its own" bullet
with five exceptions and the **Order** paragraph, module-docstring listing, `_async` wait rule; Behaviour:
closed-client deviation. Commit: `docs: apply the decided Python binding rules`.

## Round 2 — Python side (Actor 2)

Files allowed: `python/confluent_kafka/**`, `python/_confluentkafka.c`, `python/test/**`,
`python/grpc_server_async.py` (call sites only), `rust/xtask/src/error_hierarchy.rs`, and the files
`cargo xtask generate-error-codes` writes. Nothing else.

### A. Code catch-up with rules already in CLAUDE.md (merge-review decisions)

- **A1 Deprecated Java API is not generated** (Class family: "A class, method or overload Java
  deprecates … is not generated").
  - Consumer `close(timeout)`: remove `Form("timeout", deprecated=CLOSE_DEPRECATED)` from `CLOSE_FORMS`
    (`consumer/consumer.py:92`), the `timeout` parameter, its stub and the `DeprecationWarning` path, in
    `consumer.py`, `async_consumer.py`, `mock_consumer.py`, `async_mock_consumer.py` (and a shared
    helper in `consumer/_base.py` if one exists). `CLOSE_DEPRECATED` goes if unused.
  - `ConsumerGroupMetadata.__init__` (`consumer_group_metadata.py:46`): verify it raises `TypeError`
    naming `group_metadata()`; no change if so.
  - `ConsumerRecords(records)` records-only constructor (`consumer_records.py:67`): `next_offsets`
    becomes required; delete the rate-limited-error fallback in `next_offsets()` (around line 133).
    `ConsumerRecords.EMPTY` / `empty()` keep working with `next_offsets={}`.
  - `OffsetResetStrategy`: delete `consumer/offset_reset_strategy.py`; remove it from
    `consumer/__init__.py` (import and `__all__`); `MockConsumer` and `AsyncMockConsumer` take
    `offset_reset_strategy: str` only — drop the union type, the `isinstance` dispatch and the second
    stub. Internal users (`_auto_offset_reset_strategy.py`, `_subscription_state.py`, `_mock_core.py`)
    are `_`-private: keep a private equivalent of what they need, smallest change.
  - Generator: a Java constructor annotated `@Deprecated` is not generated (`error_hierarchy.rs`, where
    constructors are collected). Known case: `RecordDeserializationError`'s deprecated 4-argument
    constructor. Regenerate.
  - Tests of the removed forms go: look in `test/unit/test_kafka_consumer.py`, `test_mock_consumer.py`,
    `test_consumer_records.py`, `test_typing.py`, `test_args.py`, `test_errors.py`.
- **A2 Config** (Configuration bullets 1–3). In `_config.py`: delete `log_unused` (definition at 224,
  `__all__`, imports and calls in `producer/_base.py:35,416` and `consumer/_base.py:70,394`); delete the
  `interceptor.classes` and `partitioner.class` `ConfigError` checks (constants 64–66, checks 210–216,
  docstring lines 26–28). Keep the key-type check, the coercion and the serde-key handling. Verify
  `partitioner.type` is passed verbatim. Tests of the removed checks go; tests that unknown keys are
  accepted stay.
- **A3 `None` config value** (Configuration bullet 1: "reaches the core as not set, so the core's default
  applies"). `prepare` (`_config.py`, around 218) leaves `None` values out of the native map. Verify the
  core applies its default for an absent key; if so, no code change — say so in the commit message. Only
  if absent and NULL differ in the core, pass NULL.
- **A4 Transaction-manager check** (Behaviour: the closed-client bullet no longer has a transaction clause;
  Implementation: the binding repeats no core check). Delete `has_transaction_manager`
  (`producer/_base.py:169`), the `_transaction_manager` field (415), `_check_transaction_manager` (418),
  the comments at 80 and 346, and the ten call sites (`producer.py:111,127,168,196,221`;
  `async_producer.py:100,111,121,132,145`). The core reports a missing `transactional.id` itself. Tests
  that asserted the Python message now assert the core's error (run the test to read it), or go if an
  FFI test already covers it.
- **A5 `check_group_metadata`** (`producer/_base.py:486–496`): drop the
  `generation_id() > 0 and member_id() == _UNKNOWN_MEMBER_ID` arm and `_UNKNOWN_MEMBER_ID` if unused; keep
  the `None` check with `IllegalArgumentError("Consumer group metadata could not be null")`. Tests
  accordingly.

### B. `None` / `UNSET` and the seven null checks

Rules: Signatures "'Given' means …" (the two `UNSET` cases), Implementation exception 4, **Order**.

- **B1 `commit_nowait`** (`consumer.py:220–244`, `async_consumer.py:147–153`): `callback` defaults to
  `UNSET`; forms `(Form(), Form("callback", defaults={"callback": None}), Form("offsets", "callback"))`;
  second stub `callback: OffsetCommitCallback | None` with no default. Expected:
  `commit_nowait(offsets=o, callback=None)` is accepted and calls Java's `commitAsync(offsets, null)`.
- **B2 `TopicIdPartition.topic`** (`common/topic_id_partition.py`): default `UNSET`; stub unchanged.
  Expected: `TopicIdPartition(topic_id=u, partition=0, topic=None)` accepted.
- **B3 `ProducerRecord.key` and `.partition`** (`producer/producer_record.py:101–103`): defaults `UNSET`.
  Expected: `ProducerRecord(topic=t, partition=0, key=None, value=v)` is Java's `(topic, partition, key,
  value)`.
- **B4 Other flips the rule implies:** `Node.rack` (`common/node.py`; nullable, and
  `Node(id, host, port, rack=None, is_fenced=True)` must select the 5-argument constructor) and
  `ConsumerRecord.leader_epoch` (`consumer/consumer_record.py`; `Optional`, and the long form with
  `leader_epoch=None` must still match the 11/12-parameter constructor). Derive the complete list from the
  rule's second case and name every flipped parameter in the commit message; do not flip a parameter the
  rule does not cover (`ProducerRecord.timestamp`, `OffsetAndMetadata.leader_epoch`,
  `ConsumerRecord.delivery_count`, `subscribe.callback` stay `None`).
- **B5 Seven null checks** (exception 4), in the sync and async real consumers (`consumer.py`,
  `async_consumer.py`, or the shared code in `consumer/_base.py`), after the closed check, with Java's
  message: `pause` → `NullPointerError("The partitions to pause must be nonnull")` and `resume` →
  `NullPointerError("The partitions to resume must be nonnull")` (`AsyncKafkaConsumer.java:1364,1377`);
  `offsets_for_times` → `NullPointerError("Timestamps to search cannot be null")` (`:1397`);
  `seek_to_beginning` / `seek_to_end` → `IllegalArgumentError("Partitions collection cannot be null")`
  (`:1201`, the private `seek(Collection, strategy)`); `beginning_offsets` / `end_offsets` →
  `NullPointerError("Partitions cannot be null")` (`:1461`, the private `beginningOrEndOffset`). Java's
  `MockConsumer` makes none of these checks, so the mocks do not change. One test per site asserting the
  message, next to the existing `assign` null test.
- **B6 Closed first** (**Order**): `producer.py` `partitions_for` (383–395): `_check_not_closed()` before
  the `topic is None` check; `send_offsets_to_transaction` (sync 140–169, and the async peer): closed
  check first, then `check_group_metadata`; consumer `poll` (sync and async): closed check before
  `poll_timeout_ms`; producer `close` (sync and async): closed test before `close_timeout_ms`. Adjust any
  test that called these on a closed client with a bad argument and expected the argument's error.

### C. Exact matching

Rules: Signatures "`java_forms` works on Java's overloads …", "A stub admits …", "A serializer /
deserializer parameter is the one exception …"; Errors "Every `Throwable` parameter …".

- **C1 `_args.py`** (table builder ~190–236): keep only the given-name sets that equal a form's parameter
  set — drop the subset enumeration over `defaults`; the fill for a matched form is its `defaults`,
  unchanged. Serde exception: a `Form` option naming parameters that may be absent from the given set
  (then filled with `None`), used by the constructors of `KafkaProducer`, `AsyncKafkaProducer`,
  `KafkaConsumer`, `AsyncKafkaConsumer`, `KafkaShareConsumer`, `AsyncKafkaShareConsumer`, `MockProducer`,
  `AsyncMockProducer` for `key_serializer`/`value_serializer` (`key_deserializer`/`value_deserializer`).
  Error text unchanged. `test/unit/test_args.py`: update the machinery tests (the `Node(...)` cases at
  134–138 and any that relied on fill-in).
- **C2 Stubs** (`@overload`, in the `.py` files, stub order per the rule): `ProducerRecord` 22 — the six
  Java constructors times the key/value binding combinations (`(topic, value)` × 2 for `value: V` /
  `value: None`; each of the five constructors with `key` × 4); `MockProducer` and `AsyncMockProducer` 9
  each — `()`, `(auto_complete, partitioner, key_serializer, value_serializer)` × 4 serde combinations,
  `(cluster, auto_complete, partitioner, key_serializer, value_serializer)` × 4; `Node` 3; `OffsetAndMetadata`
  2 — `(offset, metadata: str = "")` and `(offset, leader_epoch: int | None, metadata: str)`;
  `ConsumerRecord` unchanged. A throwaway script may write the regular ones; the committed result is plain
  source. The mypy gate must pass.
- **C3 Call sites** that are not Java overloads today. `ProducerRecord`: six `(topic, partition, value)`
  calls gain `key=None` (`test/integration/test_kafka_consumer_broker.py:81,385,747,791`,
  `test/unit/test_producer_family.py:436,486`); eight `(topic, [key,] value, headers)` calls gain
  `partition=None` (`test_producer_family.py:402,513,1600`, `test_producer_types.py:72,139,160,201`,
  `test_kafka_producer.py:268`) — verify each. `MockProducer`: five calls (`test_typing.py:330–331` and
  others) move to Java's 4- or 5-argument form. `Node`: three in `test_args.py:134–138`.
  `RecordDeserializationError(...)` at `test_errors.py:523` gains `cause=None`. Then scan `python/` for
  any other constructor or method call that no Java overload admits (an AST walk over keyword sets) and
  fix those too.
- **C4 Generator** (`rust/xtask/src/error_hierarchy.rs`): (i) default choice (~1687–1707): `Unset` when a
  parameter is required in one constructor and defaulted in another, **or** is nullable and dropping it
  from a constructor that has it leaves a set that is no constructor; the `p == "cause"` special case
  goes; (ii) `stub_param` (~2094): `cause` is optional only when a shorter constructor of its stub group
  omits it, else `cause: BaseException | None` with no default; (iii) grouping (`is_prefix`, ~1648–1664):
  two constructors share a stub only if every set the shared stub admits is a constructor; (iv) the
  replicated match table (~1727–1733): exact sets only. Regenerate, then `check-generated`. Expected
  change: exactly six classes — `AuthenticationError`, `InterruptError`, `LogDirNotFoundError`,
  `ReplicaNotAvailableError`, `RetriableCommitFailedError` (the `(cause)` stub requires `cause`),
  `TransactionAbortedError` (`()` and `(message, cause=None)`), `RecordDeserializationError` (`cause`
  required once A1 drops the deprecated constructor). If any other class changes, stop and report before
  committing. Extend the generator's model tests (~3210) for the new defaults.
- **C5 New unit test** `test/unit/test_stub_runtime_consistency.py`: for every public class and method
  with stubs, enumerate the keyword subsets of the implementation signature and assert "some stub admits
  it" ⇔ "`java_forms` accepts it". Oracle: `IllegalArgumentError` → refused; `TypeError` for a missing
  required keyword-only argument or an unexpected keyword → refused; abstract and non-instantiable bases
  skipped. Parse `.pyi` files and inline `@overload`s tolerantly (multi-line signatures).

### D. Async oracle

Rule: Class family "On the async peer a method is `async def` iff …"; Implementation "A method whose
entry point has an `_async` completion form …".

- `AsyncProducer.begin_transaction` / `AsyncKafkaProducer`: `async def`, through
  `kafka_producer_Producer_begin_transaction_async`. New C wrapper `py_Producer_begin_transaction_async`
  in `python/_confluentkafka.c`, copied from `py_Producer_init_transactions_async` (~2016) and registered
  in the method table (~7396). `AsyncMockProducer.begin_transaction`: `async def`.
- Call sites gain `await`: `test/integration/test_kafka_producer_broker.py:123,181,187`,
  `grpc_server_async.py:271`, `test/unit/test_producer_family.py:1635`; the test at `:1469`
  (`test_async_begin_transaction_is_plain_and_does_not_block_the_loop`) inverts: it asserts a coroutine.
- `current_lag` added as a plain `def` to `AsyncConsumer` / `AsyncKafkaConsumer` (`async_consumer.py`;
  pattern `consumer.py:375` and `_c_current_lag`) and `AsyncMockConsumer` (through
  `_mock_core._c_current_lag`, 395–400); remove it from the "not generated" list in
  `async_consumer.py:32–34`; tests mirroring the sync ones.

### E. Module docstrings

Rule: Implementation "Generation lists each overload it leaves out, with the entry-point name it looked
for, in the module docstring." The consumer base module's docstring names the entry points it looked
for, as `producer.py`'s does. The seven files citing the untracked `ffi-overload-gaps.md` (`consumer.py`,
`async_consumer.py`, `producer.py`, `_poll.py`, `consumer_records.py`, `test/unit/test_kafka_consumer.py`,
`test/integration/test_kafka_consumer_broker.py`) drop the citation; no tracked file has that name.

### Commits for round 2

Four, each after its targeted tests: (1) A1–A5; (2) B1–B6; (3) C1–C5 with the regenerated files;
(4) D–E. Report the list of commits, the tests run, and anything the plan got wrong.

## Round 3 — FFI and C (Actor 2 continues, or Actor 3)

Files allowed: `rust/src/ffi/consumer.rs`, its tests, `c/` tests, `python/_confluentkafka.c`,
`python/confluent_kafka/consumer/_poll.py`, `python/test/integration/test_kafka_consumer_broker.py`.

- **F1** `kafka_consumer_ConsumerRecords_next_offsets` in `rust/src/ffi/consumer.rs`: returns the
  records' `next_offsets()` as a `kafka_consumer_OffsetMap_t *` through `box_offset_map` (~2677), owned
  by the caller and released with the existing `OffsetMap` destroy function; rustdoc in the style of
  `kafka_consumer_ConsumerRecords_get`. The header regenerates on build (cbindgen in `rust/build.rs`,
  feature `ffi`). A Rust test beside the other `ConsumerRecords_*` tests; a C test if `c/` tests
  `ConsumerRecords`.
- **F2** C wrapper in `_confluentkafka.c`; `_poll.py:81–89` reads `next_offsets` from the FFI and the
  recomputation (last offset + 1) goes; un-skip `test_next_offsets_skip_the_transaction_marker`
  (`test_kafka_consumer_broker.py:757`).
- Tests before the commit: `cargo test --features ffi <filter>` for the touched tests, the C tests,
  `pytest` for the touched Python tests.

## Round 4 — design notes (Actor 4, after the code rounds; briefing to follow)

Gap log and design notes only: extend gap 26 with the seven sites and the revisit condition; close gap
30; rows for the FFI naming (four-layer table, options A/B), the FFI `Mock*` entry points, the three
non-Public errors, the `None`/`UNSET` revisit, the `commit_async` note; the eleven dangling references.

## Critic checklist (the Manager, per round)

- The diff touches only the round's files; no refactor, rename or reformat rode along.
- Each rule is applied as written: `java_forms` admits exactly the Java sets; every stub admits only Java
  sets; `UNSET` on exactly the parameters the two cases cover; Java messages verbatim; closed first on
  FFI-backed classes, Java's order on the mocks; `async def` iff the `_async` form exists.
- Tests assert messages, not `is_err`; the new consistency test runs over every class with stubs.
- The regenerated error output differs in the six named classes only; `check-generated` is clean.
- The mypy gate and the touched pytest files pass; `make verify` passes at the round's end.
