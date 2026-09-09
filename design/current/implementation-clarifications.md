# Python interface implementation — clarifications for the owner

Branch: `dev_python-interface-implementation` (from `origin/dev_interface_consolidation`).
Per the owner's instruction (2026-09-09) the Actor/Critic loop does not stop for input; every
question is logged here with the assumption taken, and the owner is asked once at the end.

Format: `### C<n> — <title>` · **Where** · **Question** · **Assumption taken** · **Alternatives** · **Status**.

### C1 — Share consumer is out of implementation scope
- **Where:** spec §6.3 / §9 (`ShareConsumer`, `KafkaShareConsumer`, `MockShareConsumer`, `Async*`).
- **Question:** the spec defines the share consumer, but the Rust core has no share-consumer
  translation (`consumer-threading.md` §20 lists every share-consumer file as out of scope) and the
  FFI has no share entry points.
- **Assumption taken:** not implemented in this pass; the owner's instruction names "consumer and
  producer". No stub module is added (a stub raising errors would be a public surface with no core).
- **Alternatives:** translate the share consumer core first (a separate milestone).
- **Status:** open.

### C2 — Overlap fallback of the union naming rule (rule 3.4)
- **Where:** `.claude/rules/python-binding-interface.md` §3.4.
- **Question:** the owner asked that the "non-disjoint Python types → `name_type` parameters" case be
  reviewed explicitly before it is encoded as a rule.
- **Assumption taken:** written into the rule file **marked "NOT yet reviewed by the owner"**; no site
  in the producer/consumer surface needs it, so it is not applied anywhere.
- **Status:** open — owner to confirm or replace the fallback.

### C3 — Module placement of the six non-`common.errors` Kafka exception classes (P1)
- **Where:** `bindings/python/confluent_kafka/common/errors/_generated.py`; the generator's
  `module_for` in `xtask/src/error_hierarchy.rs`.
- **Question:** the spec's module table (§4) and D1's generation ruling name exactly four output
  modules for the hierarchy: `common.errors`, `common.config`, `consumer`, and the root JDK analogs.
  But six concrete Kafka classes live in sibling Java packages with no module row of their own:
  `common.InvalidRecordException`, `common.requests.CorrelationIdMismatchException`,
  `common.network.InvalidReceiveException`, `common.metrics.QuotaViolationException`,
  `common.protocol.types.SchemaException`, and `clients.producer.BufferExhaustedException`.
- **Assumption taken:** all six are emitted into `confluent_kafka.common.errors._generated` (the
  home the spec gives the Kafka common hierarchy). They all descend from `KafkaException`, are
  catch-only/rarely-caught leaves, and none has a dedicated in-scope module.
  `CorrelationIdMismatchError` keeps Java's parent — it extends `IllegalStateException`, so it
  subclasses the root JDK analog `IllegalStateError` (cross-module import), not `KafkaError`.
  `BufferExhaustedError` (Java `clients.producer.BufferExhaustedException extends TimeoutException`)
  therefore also lands in `common.errors`, beside the `TimeoutError` it extends.
- **Alternatives:** mint a `common.requests` / `common.network` / `common.metrics` /
  `common.protocol.types` / `producer` errors module each mirroring the exact Java package; or move
  `BufferExhaustedError` under a future `producer` errors module.
- **Status:** open — owner to confirm the fold-into-`common.errors` placement.

### C4 — Typed payload accessors (RecordDeserializationError.topic_partition(), …) deferred (P1)
- **Where:** `bindings/python/confluent_kafka/common/errors/`; the FFI accessors
  `kafka_common_Error_topic_authorization` / `_group_authorization` / `_invalid_topic` /
  `_record_deserialization` / `_throttling_quota_exceeded` / `_quota_violation` / `_record_too_large` /
  `_consumer_*` in `target/include/confluent_kafka.h`.
- **Question:** P1's runtime-mapping deliverable lists typed payload accessors as METHODS on the
  classes whose FFI exposes them (e.g. `TopicAuthorizationError.unauthorized_topics()`). Each FFI
  accessor returns an opaque sub-handle (`kafka_common_TopicAuthorizationError_t *`) whose own
  sub-accessors return collections/strings that must be marshalled through NEW native functions in
  `_confluentkafka.c` (only `KafkaError_code`/`_message`/`_destroy` are exposed to Python today).
- **Assumption taken:** P1 ships the error hierarchy, the `id -> class` table, `from_ffi_error`
  (code + message + cause), and `to_ffi_id` — but NOT the per-class payload accessor methods.
  Wiring each payload type end-to-end (new C-extension natives + marshalling + per-type tests) is a
  large, mechanical C task that fits a later phase (P4/P5, when the producer/consumer surfaces that
  actually raise these errors land). No stubbed accessor is added (a method returning `None` would be
  a false surface); the classes carry the correct Java-mirrored parent chain and `_ffi_id` now.
- **Alternatives:** wire all ten payload types in P1 (substantial C-extension work with no
  producer/consumer caller to exercise it yet).
- **Status:** open — payload accessors tracked for P4/P5.

### C5 — Temporary duplication: legacy flat `KafkaError` in producer.py/consumer.py (P1)
- **Where:** `bindings/python/producer.py`, `bindings/python/consumer.py` (top-level legacy modules).
- **Question:** the legacy modules keep their own flat `KafkaError` (with `code`/`is_retriable`/…)
  and are NOT rewired to the new typed hierarchy in P1 (the task scopes that to P4/P5).
- **Assumption taken:** left as-is; the new `confluent_kafka.common.errors.KafkaError` and the flat
  legacy `producer.KafkaError` coexist for now. Removal of the legacy flat error is a P4/P5 item as
  those surfaces move into the package.
- **Status:** open — remove the duplication when producer/consumer move into `confluent_kafka` (P4/P5).

### C6 — `ConsumerRecords.next_offsets()` on the deprecated records-only form logs, does not raise (P2)
- **Where:** `bindings/python/confluent_kafka/consumer/consumer_records.py`
  (`next_offsets`); rule 10a and the P2 task both say the records-only instance "raises from
  `next_offsets()` as Java does".
- **Question:** what does Java's `ConsumerRecords.nextOffsets()` do when the instance was built
  with the deprecated records-only constructor (KIP-1094 / KAFKA-20660)?
- **Finding:** Java does NOT raise. It returns the empty map and logs a **rate-limited** ERROR
  (every 5 min via a static `AtomicLong`); `ConsumerRecordsTest`
  (`testNextOffsetsLogsErrorPeriodicallyWhenConstructedWithDeprecatedConstructor`) asserts exactly
  this (`assertTrue(consumerRecords.nextOffsets().isEmpty())` + one ERROR logged, throttled). So
  rule 10a's / the task's "raises" wording contradicts the Java 4.3.1 source.
- **Assumption taken:** follow Java (the functional contract, rules §1) — the tainted instance's
  `next_offsets()` returns an empty `dict` and logs a rate-limited error via Python `logging` on
  `confluent_kafka.consumer.consumer_records`. The tests assert the empty-map behaviour, not a
  raise.
- **Alternatives:** raise (as the spec/rule text literally says) — rejected because it diverges from
  Java and would break the two Java tests that assert the empty-map return.
- **Status:** open — owner to reconcile the rule 10a "raises" wording with Java's log-and-return.

### C7 — `TopicIdPartition.topic()` returns `str | None` (Java can return null), not spec's `str` (P2)
- **Where:** `bindings/python/confluent_kafka/common/topic_id_partition.py` (`topic`).
- **Question:** the spec §5.1 stub types `def topic(self) -> str`, but Java's `TopicIdPartition.topic()`
  returns the underlying `TopicPartition.topic()`, which is `null` when the topic name is unknown —
  `TopicIdPartitionTest` constructs instances with a null topic and asserts `toString()` renders it
  as `null`.
- **Assumption taken:** typed `topic() -> str | None` (Java-faithful, rule R1). The type appears only
  on the share-consumer surface (out of scope, C1), so no producer/consumer method depends on the
  narrower `str`.
- **Status:** open — owner to confirm the `str | None` widening (spec stub says `str`).

### C8 — `TimestampType` exposes Java's label via `label()`/`__str__`/`for_name`, not `.name` (P2)
- **Where:** `bindings/python/confluent_kafka/common/timestamp_type.py`.
- **Question:** the Java enum `TimestampType` has two fields: `id` (`-1`/`0`/`1`) and `name` (a label
  string: `"NoTimestampType"`/`"CreateTime"`/`"LogAppendTime"`), plus a static `forName(String)`.
  Python's `IntEnum` already owns `.name` (the member name, e.g. `"NO_TIMESTAMP_TYPE"`) and `.value`.
- **Assumption taken:** member value == Java `id` (spec §5.3). Java's `id` field is exposed as
  `id()`, Java's label `name` field as `label()` and `__str__` (Java `toString` returns the label),
  and `forName` as the static `for_name(*, name=...)` (raising `KeyError` where Java raises
  `NoSuchElementException`). Python's own `.name` is left untouched to avoid clobbering the enum
  machinery.
- **Alternatives:** override `.name` to return Java's label (fights `IntEnum`, risks breaking
  `repr`/lookup); expose only the numeric `id` and drop the label (loses Java surface).
- **Status:** open — owner to confirm `label()`/`for_name` names for the Java `name`/`forName` pair.

### C9 — `CloseOptions.timeout` / `group_membership_operation` overload one Python name via a descriptor (P2)
- **Where:** `bindings/python/confluent_kafka/consumer/close_options.py` (`_StaticOrInstance`).
- **Question:** Java overloads each of `timeout` and `groupMembershipOperation` as BOTH a static
  factory (`CloseOptions.timeout(Duration)`) and an instance getter (`opts.timeout()`). Python cannot
  bind one class attribute name to both a `@staticmethod` and an instance method.
- **Assumption taken:** a small descriptor dispatches on `obj is None`: `CloseOptions.timeout(d)`
  (accessed on the class) calls the static factory; `opts.timeout()` (accessed on an instance) calls
  the getter. This keeps both Java forms under the single Java name and passes `mypy --strict`
  (the static-factory return type degrades to `Any` through the descriptor — the only typing
  weakness). `CloseOptionsTest` passes verbatim against it.
- **Alternatives:** rename one form (e.g. `set_timeout`/`get_timeout`) — violates R1 (Java's names);
  a `.pyi` overload set for the descriptor — deferred (the descriptor already type-checks clean).
- **Status:** open — owner to confirm the descriptor approach for the overloaded static/instance name.

### C10 — `ProducerRecord` is a pure-Python value type in the package; native FFI struct reconciled in P4 (P2)
- **Where:** `bindings/python/confluent_kafka/producer/producer_record.py` vs the existing native
  `_confluentkafka.ProducerRecord` (a C type carrying `kafka_producer_ProducerRecord_t` on the send
  path, with positional init + attribute getters).
- **Question:** the P2 task says to keep the native type and give it the spec's keyword-only ctor +
  accessor methods. Modifying the C type's `tp_init`/getters would change the send path that the
  current `producer.py`/`test_producer.py` (P4's territory) depend on, and risk the existing tests.
- **Assumption taken:** implement `ProducerRecord` as a **pure-Python value type** in the new package
  (keyword-only ctor in Java order, accessor methods, headers, value equality) — the public type
  users construct. The native `_confluentkafka.ProducerRecord` is left untouched. **DoD #10 (hot-path
  allocation):** the value type holds owned `bytes` for key/value and performs no per-record copy; the
  zero-copy send path (P4) reads `key()`/`value()` (already `bytes`) and writes them directly into the
  batch buffer, so no intermediate buffer is introduced by this value type. P4 reconciles the two —
  either by converting the pure-Python record into the native FFI struct at `send()`, or by making the
  native type accept/produce this value type — without a per-record copy.
- **Alternatives:** modify the native C `ProducerRecord` now (touches the send path and P4's
  `producer.py`/`test_producer.py` under concurrent ownership; higher risk, out of P2's isolated
  value-type scope).
- **Status:** open — P4 to reconcile the public value type with the native send-path struct.

### C11 — `ConsumerRecords.records(topic=None)` raises the combination error, not Java's "Topic must be non-null." (P2)
- **Where:** `bindings/python/confluent_kafka/consumer/consumer_records.py` (`records`).
- **Question:** Java's `records(String topic)` with a null topic raises `IllegalArgumentException`
  ("Topic must be non-null."). In the collapsed Python API (`records(*, partition=None, topic=None)`
  with the rule-3.5 convention that `None` == "argument not given"), `records(topic=None)` is
  indistinguishable from `records()`.
- **Assumption taken:** the `_args.exactly_one` combination check raises `IllegalArgumentError`
  naming both alternatives ("records() takes exactly one of partition, topic; got none") — still an
  `IllegalArgumentError`, still on a null-topic call, just with the collapsed-signature message rather
  than Java's literal text. This is the unavoidable consequence of overload collapse + None-as-absent
  (rule 3.5), which the owner flagged for re-discussion at implementation.
- **Status:** open — owner to confirm the combination-error message is acceptable in place of Java's
  "Topic must be non-null." for the null-topic call.

### C12 — `KafkaMetric` typed as a `Protocol`; `config()`/`measurable()` return opaque `object` (P2)
- **Where:** `bindings/python/confluent_kafka/common/metric.py`.
- **Question:** spec §5.1 defers `KafkaMetric.config()` (`MetricConfig`) and `measurable()`
  (`Measurable`) return types to the metrics/plugin design pass.
- **Assumption taken:** `Metric`/`KafkaMetric` are `Protocol`s (not user-constructed; instances come
  from `metrics()`), and `config()`/`measurable()` are typed `-> object` for now, per the spec's
  deferral. `metric_value()` is `Any` (Java `Object`).
- **Status:** open — the two return types land in the metrics design pass.

### C13 — Spec text lags Java in two P2 details (Critic 65 notes N2/N3)
- **Where:** spec §5.3 `OffsetResetStrategy` member order; §5.1 `TopicIdPartition.topic()` return type.
- **Question:** the spec's enum member order and the `topic()` type differ from Java 4.3.1
  (`OffsetResetStrategy.java`, `TopicIdPartition.java:…` — `topic()` may return `null` in Java).
- **Assumption taken:** the code follows Java (rule 2 outranks the spec's code blocks); the spec is to
  be corrected in its next revision, not the code.
- **Status:** open — spec edit for the owner's next spec pass.

### C14 — `TimestampType.for_name` raises `KeyError` where Java throws `NoSuchElementException`
- **Where:** `confluent_kafka/common/timestamp_type.py`.
- **Question:** Java's `TimestampType.forName` throws `java.util.NoSuchElementException`; the spec
  defines no JDK analog for it (only `IllegalState`, `IllegalArgument`, `ConcurrentModification`,
  `Timeout`).
- **Assumption taken:** Python's `KeyError` (the closest builtin; a lookup failure). Alternative: add a
  `NoSuchElementError` JDK analog at the root (would need an FFI id — none exists).
- **Status:** open — owner to confirm `KeyError` or request a new JDK analog.

### C15 — `uuid_serializer`/`uuid_deserializer` use this binding's `Uuid` (base64), not `java.util.UUID` (dashed) (P3)
- **Where:** `bindings/python/confluent_kafka/common/serialization/uuid_serializer.py`,
  `uuid_deserializer.py`.
- **Question:** Java's `UUIDSerializer`/`UUIDDeserializer` operate on `java.util.UUID`, serializing
  `UUID.toString()` (the canonical dashed form, e.g. `123e4567-e89b-...`) and parsing via
  `UUID.fromString`. The spec (§5.4) types the factory as `Serializer[Uuid]` / `Deserializer[Uuid]`
  where `Uuid` is `confluent_kafka.common.Uuid` (`org.apache.kafka.common.Uuid`), whose string form is
  a **URL-safe base64** encoding — not the dashed form. These produce different wire bytes.
- **Assumption taken:** faithful to the Java *mechanism* (serialize the type's string form, parse it
  back on the deserializer) using our `Uuid`'s `str()` / `Uuid.from_string`. The wire bytes are
  therefore the base64 string, not `java.util.UUID`'s dashed string. An unparseable string wraps the
  `IllegalArgumentError` from `from_string` as `SerializationError("Error parsing data into UUID")`,
  matching Java. Encoding config keys (`key.serializer.encoding` etc.) are honoured exactly as Java's
  UUID serdes honour them (stored, not validated at configure time — unlike `StringSerializer`).
- **Alternatives:** (a) introduce a separate `java.util.UUID`-compatible path (would require a second
  UUID type on the surface — none exists); (b) serialize our `Uuid`'s dashed-equivalent form (our
  `Uuid` has no dashed form).
- **Status:** open — owner to confirm base64 wire form for our `Uuid` is acceptable, since it diverges
  from Java's `java.util.UUID` bytes for cross-language interop.

### C16 — Java serdes with no spec factory: Short / Long / Void / ByteBuffer / Bytes / List (P3)
- **Where:** `bindings/python/confluent_kafka/common/serialization/` (factory roster in `_factories.py`).
- **Question:** the P3 deliverable lists all Java built-in serdes; the spec §5.4 collapses the sized
  integer serdes into `int_serializer(*, size=4)` (allowed sizes **4|8**) and the float serdes into
  `float_serializer(*, size=8)` (**8|4**), and names an exact factory set that omits several Java
  serdes. Which Java serdes have no factory, and what happens to them?
- **Assumption taken (per D6 built-ins table + rule 2 = implement exactly the spec's roster):**
  - **`ShortSerializer`/`ShortDeserializer` (size 2)** — NOT offered. `int_serializer`/`int_deserializer`
    accept only size 4|8 (spec), so `size=2` is rejected with `IllegalArgumentError`. Java's Short
    testData row has no home; a `test_short_range_via_int_size2_is_not_offered` documents the rejection.
    (Long IS offered as `int_serializer(size=8)`.)
  - **`VoidSerializer`/`VoidDeserializer`** — NOT offered (D6: "not in v1 — no demand; `bytes_*` +
    None-passthrough covers it").
  - **`ByteBufferSerializer`** — NOT offered as a distinct factory; the producer default
    `bytes_serializer()` (passthrough) covers the `bytes`-like producer path. `ByteBufferDeserializer`
    IS offered as `memoryview_deserializer()` (the zero-copy opt-in).
  - **`BytesSerializer`/`BytesDeserializer`** (Java's `org.apache.kafka.common.utils.Bytes` wrapper) —
    NOT offered; `bytes_*` covers the same role (there is no `Bytes` wrapper type on this surface).
  - **`ListSerializer`/`ListDeserializer`** — NOT offered (D6: "not in v1 — needs an inner-serde story
    first"). All `listSerde…` Java tests are skipped with this reason in `test_serialization.py`.
- **Status:** resolved by D6's built-ins table; recorded here so the omissions are explicit and the
  skipped Java tests are accounted for.

### C17 — float serde does not preserve raw signaling-NaN payloads (Python-float limitation) (P3)
- **Where:** `bindings/python/confluent_kafka/common/serialization/float_serializer.py`.
- **Question:** Java's `FloatSerializer` uses `Float.floatToRawIntBits`, which preserves the exact NaN
  bit pattern; `floatSerdeShouldPreserveNaNValues` constructs a signaling NaN from `0x7f800001` and
  asserts the raw int bits round-trip. Python's `struct.pack(">f", x)` canonicalizes a signaling NaN to
  a quiet NaN (`0x7f800001` -> `0x7fc00001`), and a Python `float` cannot carry the raw payload through
  the C double it materializes as.
- **Assumption taken:** implement standard big-endian IEEE-754 packing (`struct`), which matches Java
  for all non-NaN values and for canonical (quiet) NaN. The `floatSerdeShouldPreserveNaNValues` test is
  translated as a **canonical-NaN round-trip** (still a NaN; the bits our own encoder emits round-trip
  exactly), NOT the raw-payload assertion — which is unrepresentable in pure Python. If native C-ext
  execution of the built-ins lands (see C18), the raw-payload path could be recovered there.
- **Status:** open — a genuine language limitation; owner to acknowledge the canonical-NaN adaptation.

### C18 — built-ins run in Python for P3; native C-ext execution is a P4/P5 item (P3)
- **Where:** `bindings/python/confluent_kafka/common/serialization/` (all built-in serde classes).
- **Question:** spec §5.4 / D10 say "built-ins run natively — no per-record Python call". The FFI /
  C extension (`bindings/python/_confluentkafka.c`, `src/ffi/*.rs`) currently exposes NO serde hooks
  (only the `RecordDeserializationError` accessors); there is no native serialize/deserialize entry
  point to wire the built-ins to.
- **Assumption taken:** implement the built-ins in Python for P3 (correct behavior, byte-faithful with
  Java). Native execution (recognizing a built-in by identity at construction and running it in the C
  layer, zero per-record Python call) is deferred to the client-integration phases (P4/P5) where the
  poll/send FFI path exists — no FFI is added in P3 (per the phase instruction).
- **Status:** open — native-execution wiring tracked for P4/P5; the Python built-ins are the fallback
  D6 placement option 1 ("Hybrid — built-ins native, customs Python") permits.

### C19 — serde/config error classes imported from their concrete generated modules, not the package re-export (P3)
- **Where:** `bindings/python/confluent_kafka/common/serialization/*` and
  `bindings/python/confluent_kafka/_config.py` (imports of `SerializationError`, `ConfigError`).
- **Question:** CLAUDE.md's naming rule prefers importing a type via its parent-module re-export
  (`from confluent_kafka.common.errors import SerializationError`). Under `mypy --strict` (rule 12),
  that re-export resolves `SerializationError` as `object` (a variable, not a type): the errors package
  `__init__` re-exports via `from ._generated import *` with a runtime-computed `__all__` and ships no
  `__init__.pyi`, so mypy cannot statically see the class through the package.
- **Assumption taken:** import these error classes from their **concrete generated modules**
  (`confluent_kafka.common.errors._generated`, `confluent_kafka.common.config._generated_errors`),
  which carry proper `.pyi` types, so `mypy --strict` stays clean. Runtime behavior is identical (the
  package re-export works at runtime). The parent-re-export typing gap belongs to the errors-package
  owner (Actor/Critic 64); this is a workaround on the consumer side, not a fix to that package.
- **Status:** RESOLVED for `common/errors` (Actor 64). `cargo xtask generate-error-codes` now also
  emits `confluent_kafka/common/errors/__init__.pyi` — explicit `from ._generated import X as X`
  typed re-exports for every generated error class, plus `KafkaError`, `from_ffi_error`, `to_ffi_id`
  and `__all__` — so `from confluent_kafka.common.errors import TopicAuthorizationError` now types as
  the class, not `object` (a `test/unit/test_typing.py` `assert_type` pins this under `mypy --strict`;
  `check-generated` covers the new stub). `common/config` and the root already re-export **explicitly**
  (`from ._generated_errors import ConfigError`, etc.), which mypy already types correctly, so they
  need no stub. The `consumer` package re-exports its errors with a star import in its **hand-written**
  `__init__.py` (owned by the value-types/P5 Actor, who also adds many non-error exports there): the
  durable fix on that file is to switch its `from ._generated_errors import *` to explicit
  `from ._generated_errors import X as X` lines (a generated `consumer/__init__.pyi` would shadow and
  hide that Actor's other typed exports, so it is NOT generated). Consumers of the errors package
  (e.g. C19's serde/config imports) can now move back to the `common.errors` parent re-export.

### C20 — `float_serializer(size=8)` canonicalizes NaN to match Java `DoubleSerializer` (P3, Critic 66 F2)
- **Where:** `bindings/python/confluent_kafka/common/serialization/float_serializer.py`.
- **Question:** Java's numeric-serde pair is asymmetric — `FloatSerializer` uses
  `Float.floatToRawIntBits` (raw, preserves the NaN payload) but `DoubleSerializer` uses
  `Double.doubleToLongBits`, which **canonicalizes** every NaN to `0x7ff8000000000000`. The initial P3
  implementation used `struct.pack(">f" | ">d")` for both sizes; `>d` is a raw pack
  (`doubleToRawLongBits`), so a non-canonical double NaN (e.g. raw `0x7ff0000000000001`) was emitted
  verbatim — a wire divergence from Java, and one that IS representable in a Python float (it is a C
  double), unlike the float32 signaling-NaN case (C17).
- **Assumption taken (now Java-faithful):** for `size=8`, when the value is NaN, emit the canonical
  `0x7ff8000000000000` big-endian (Java `doubleToLongBits`); all finite values, infinities and the
  already-canonical NaN go through `struct.pack(">d")` unchanged. `size=4` keeps `struct.pack(">f")`
  (raw), matching `FloatSerializer.floatToRawIntBits`. A test builds a non-canonical double NaN from
  raw bits and asserts the serializer emits Java's canonical bytes.
- **Status:** resolved (fixed to match Java). Recorded because the fix reproduces a subtle asymmetry a
  future Actor collapsing sized serdes could re-break — candidate for a rule note in
  `python-binding-interface.md`'s serialization section (Critic 66 suggestion).

### C21 — The pre-commit hook scans the whole working tree, so parallel actors block each other
- **Where:** `.githooks/pre-commit` → `make verify-sandbox` (release build, C build, `format-check`,
  `lint`, Rust integration tests, C tests) on every commit.
- **Question:** with several actors sharing one working tree, any actor's uncommitted Rust/C WIP
  (unformatted, non-compiling, or lint-dirty) fails every other actor's commit deterministically
  (Actor 66's fixup was blocked by Actor 67's `src/ffi/*.rs` WIP on 2026-09-09).
- **Assumption taken (process, not code):** actors that touch Rust/C work in their own `git worktree`
  on a per-phase branch and the Manager merges; Python-only actors stay in the main tree. The hook is
  NOT changed (that is the owner's call).
- **Alternatives:** scope `format-check`/`lint` in the hook to staged paths; or a lighter hook
  (`format-check` + `lint` + unit tests) with `verify-sandbox` in CI only. Each commit currently costs
  ~15 min of hook time, which also serialises the whole team.
- **Flake evidence (2026-09-09):** Python-only fix commits were rejected by the hook's Rust integration
  stage on `test_re2j_pattern_subscription_and_topic_subscription` (UnknownMemberId),
  `test_async_consumer_async_commit` (twice), `test_async_consumer_max_poll_records` (timestamp) — none
  touched by the change; 204/205 pass each time. The hook makes every commit pay ~15 min and a flake
  lottery; retries were authorised up to three per commit.
- **Owner decision (2026-09-10): option 2.** `.githooks/pre-commit` is TEMPORARILY reduced to
  `format-check` + `lint` + Python `typecheck`; the full `make verify-sandbox` runs once before each
  phase-branch merge into `dev_python-interface-implementation` and at P7; the original hook
  (`exec make verify-sandbox`) is restored when P7 closes.
- **Status:** decided; revert-at-end tracked in the plan file.

### C22 — `MockProducer` is a pure-Python synchronous translation of `MockProducer.java`, not FFI-backed (P4)
- **Where:** `bindings/python/confluent_kafka/producer/mock_producer.py` (the whole file).
- **Question:** should the Python `MockProducer` wrap the Rust core's `MockProducer` over the FFI, or
  translate `MockProducer.java` directly to Python?
- **Finding:** Java's `MockProducer` is a **self-contained, synchronous** in-memory mock — it has no
  network and no I/O thread, keeps records/offsets/transaction-state in its own fields, and completes
  sends **synchronously** (auto-complete) or on `completeNext`/`flush`. The Rust core, by contrast, is
  fundamentally async: the C extension's send path stages a record on a background batching task and
  the FFI dispatcher delivers completions on its own thread, so a Python `send()` backed by it cannot
  return an already-`done()` future the way Java's auto-complete `MockProducer` does. That breaks the
  canonical `MockProducerTest` asserts (`md.isDone()` / `md.get().offset()` immediately after `send`,
  `completeNext` right after `send`). It also stores serialized `Vec<u8>` records, so `history()`
  round-tripped through the FFI would return `bytes` records that do not compare equal to the original
  `ProducerRecord(topic=…, key=…, value=…)` a test constructed.
- **Assumption taken:** translate `MockProducer.java` **directly to Python** (each Java class → one
  Python class, DoD #2) as a synchronous pure-Python mock (`_MockCore` mirrors Java's fields and every
  method), holding the **original** `ProducerRecord[K, V]` objects in `sent`/`uncommittedSends`. It
  does not touch the FFI or the Rust core mock at all; `MockProducer`/`AsyncMockProducer` subclass
  `Producer`/`AsyncProducer` only for the type hierarchy and `isinstance`, and override every method.
  This is the most faithful translation (Java's mock IS pure and synchronous) and passes all 51
  translated `MockProducerTest` cases synchronously. **Consequence:** the mock-surface FFI functions
  drafted earlier (`MockProducer_*` scalar accessors, setters, `error_next_with_code`, and the core
  `error_from_ffi_code` inverse) were **reverted** — they had no Python caller and would violate DoD
  ("no new FFI without a Python test"). Only `RecordMetadata_copy_full`,
  `Producer_close_timeout_async`, and `Producer_send_offsets_to_transaction_fields_async` (used by the
  real `KafkaProducer`) were kept.
- **Alternatives:** wrap the core mock over the FFI (rejected: the async dispatcher cannot express
  Java's synchronous completion, and serialized-bytes history breaks equality); pump the dispatcher
  synchronously inside the mock's `send`/`complete_next` (rejected: complex, and still would not match
  Java's `send`-returns-a-done-future for auto-complete).
- **Status:** open — owner to confirm the pure-Python `MockProducer` translation.

### C23 — `error_next(*, error)` (mock) accepts any exception and delivers it directly (P4)
- **Where:** `bindings/python/confluent_kafka/producer/mock_producer.py` (`error_next`).
- **Question:** Java's `MockProducer.errorNext(RuntimeException e)` fails the next send with the
  **same exception instance** `e` (`MockProducerTest.testManualCompletion` /
  `testMetadataOnException` assert `future.get()`'s cause **== e**). What type does the Python
  `error_next(*, error)` accept, and does it preserve identity?
- **Assumption taken:** because the mock is now pure-Python (C22), `error_next(*, error)` stores and
  delivers the **exact** Python exception instance — Java's identity contract is preserved (the
  translated tests assert type+message, which the identity also satisfies). The parameter is typed
  `BaseException` (not the spec's narrower `KafkaError | None`), because Java's `errorNext` takes a
  `RuntimeException` and `MockProducerTest` injects `IllegalArgumentException` (a JDK analog, not a
  `KafkaError`); typing it `KafkaError` would reject the canonical Java test. Likewise the nine
  `set_<field>_exception(*, error)` setters accept `BaseException | None` (Java's fields are
  `RuntimeException`), a widening of the spec's `KafkaError | None` for the same reason — flagged
  because it deviates from the spec §6.1 signature.
- **Alternatives:** keep the spec's `KafkaError | None` (rejected: cannot express the Java test's
  `IllegalArgumentException` injection).
- **Status:** open — owner to confirm the `BaseException` widening for `error_next` / the mock
  exception setters vs the spec's `KafkaError | None`.

### C24 — Legacy `bindings/python/producer.py` kept: dependency list for P5/P7 (P4)
- **Where:** `bindings/python/producer.py` (legacy flat-error module).
- **Question:** deliverable 5 retires `producer.py` only if nothing else imports it. It IS still
  imported.
- **Finding (grep):** `admin.py`, `consumer.py`, `grpc_server.py`, `grpc_server_async.py`,
  `grpc_translate.py` import from `producer` (mostly `KafkaError`; the gRPC servers import
  `KafkaProducer` / `AsyncKafkaProducer` / `MockProducer` / `AsyncMockProducer` / `ProducerRecord`);
  tests `test/unit/test_consumer.py`, `test_consumer_callbacks.py`, `test_admin.py`,
  `test/unit/test_producer.py`, `test/performance/producer_performance_test.py` import from it too.
- **Assumption taken:** `producer.py` is **kept, unmodified**, and the legacy
  `test/unit/test_producer.py` (111 cases) stays alongside the new
  `test/unit/test_producer_family.py`. It is retired only when `consumer.py` (P5) and the admin/gRPC
  surfaces (P7) move into the package. Extends C5.
- **Status:** open — remove when P5/P7 land; dependency list recorded here for those phases.

### C25 — Real `KafkaProducer` telemetry/metric-subscription methods raise (core gap); the mock implements them (P4)
- **Where:** `bindings/python/confluent_kafka/producer/producer.py` (real) vs
  `mock_producer.py` (mock).
- **Question:** the Rust core's `Producer` trait (`src/producer/producer_trait.rs`) exposes neither
  `client_instance_id` nor metric-subscription registration (KIP-714 telemetry). What do the Python
  methods do?
- **Assumption taken:** two behaviours, split by class:
  - **Real `KafkaProducer` / `AsyncKafkaProducer`:** per rule 10, `client_instance_id`,
    `register_metric_for_subscription` and `unregister_metric_from_subscription` exist with the spec's
    signatures and raise the mapped Java error (a base `KafkaError` explaining the core gap) rather
    than silently no-op'ing — except `client_instance_id`'s **negative-timeout** validation runs
    Python-side first and raises `IllegalArgumentError("The timeout cannot be negative.")` (Java's
    exact message, `KafkaProducerTest.testClientInstanceIdInvalidTimeout`), so that test translates.
  - **`MockProducer` / `AsyncMockProducer`:** because the mock is a pure-Python translation of Java's
    `MockProducer` (C22), it **fully implements** `client_instance_id`, `inject_timeout_exception`,
    `disable_telemetry`, `set_client_instance_id`, `set_mock_metrics`, `added_metrics`, `metrics`, and
    `register/unregister_metric_for_subscription` exactly as `MockProducer.java` does (in-memory, no
    core dependency), so they are real and Java-faithful there.
- **Alternatives:** implement client telemetry in the core — a separate milestone (would let the real
  client implement these too).
- **Status:** open — real-client telemetry/metric-subscription tracked for a future core phase; the
  mock implements the full Java surface now.
- **Addendum (Critic 67 F1):** `MockProducer.client_instance_id` when the id is unset — Java throws
  `UnsupportedOperationException("clientInstanceId not set")` (`MockProducer.java:406`). There is no
  `UnsupportedOperationError` JDK analog on this surface (the defined analogs are `IllegalStateError`
  / `IllegalArgumentError` / `ConcurrentModificationError` / `TimeoutError`) and no FFI id for one,
  so Python's **semantic counterpart `NotImplementedError`** carries Java's exact message. **Owner
  choice:** confirm `NotImplementedError`, or add a hand-written `UnsupportedOperationError` JDK analog
  at the package root (no FFI id, mirroring the other root JDK analogs). The `disable_telemetry` path
  is already faithful (Java throws bare `IllegalStateException()`; Python raises `IllegalStateError()`).

### C26 — Legacy `bindings/python/consumer.py` NOT retired (P5)
- **Where:** `bindings/python/consumer.py` (the flat legacy module) vs the new
  `bindings/python/confluent_kafka/consumer/` package.
- **Question:** P5 deliverable 6 says to retire the legacy `consumer.py` "ONLY if nothing else
  imports it".
- **Finding:** `producer.py`, `admin.py`, `grpc_server.py`, `grpc_server_async.py`,
  `grpc_translate.py`, and `test/performance/consumer_performance_test.py` all still
  `import consumer` (verified 2026-09-09). Retiring it would break those.
- **Assumption taken:** left the legacy `consumer.py` untouched; the new package is added
  alongside it (this extends the P1 C5 duplication note). The legacy `test_consumer.py` /
  `test_consumer_callbacks.py` stay while the legacy module lives; the new API's tests are in
  `test/unit/test_consumer_family.py`.
- **Status:** open — retire the legacy module once the gRPC servers / admin / producer / perf
  tests move onto the new package.

### C27 — Caller-thread rebalance-callback delivery: new FFI mechanism (P5, D25 gaps 1/8)
- **Where:** `src/ffi/consumer.rs` (`CallerThreadRebalanceListener`, the pending-callback queue,
  `kafka_consumer_Consumer_{set_pending_callback_notify,subscribe_caller_thread_listener_async,
  next_pending_callback,ack_pending_callback}`, `kafka_consumer_PendingCallback_*`,
  `kafka_consumer_MockConsumer_rebalance_async`);
  `bindings/python/confluent_kafka/consumer/_engine.py`.
- **Question:** §31 requires the listener / `on_commit` to run on the poll/commit/rebalance
  CALLER's thread, and D25 gaps 1/8 require the async-listener reentrancy deadlock to be fixed
  (not documented). The existing FFI `subscribe_with_listener` runs the C callback on the
  consumer's *dispatcher* thread (via `dispatch_and_wait`), which violates §31 and deadlocks a
  reentrant `await consumer.commit()`.
- **Assumption taken:** added a new FFI path. `subscribe_caller_thread_listener_async` installs a
  core listener that, when invoked, enqueues a `(method, partitions, ack)` record on the consumer
  handle and parks on the ack; a one-shot C notify (`set_pending_callback_notify`) wakes the
  Python blocking-op wait loop, which drains the queue on its OWN thread
  (`Consumer_next_pending_callback` / `PendingCallback_method` / `_partitions` /
  `ack_pending_callback`), runs the user listener, and acks. The op that triggered the callback
  (poll / rebalance / subscribe-driven reconcile) is parked on the ack on a runtime worker, so
  the bg task keeps spinning and a reentrant `ConsumerHandle` op completes (§41). `MockConsumer`
  needed a `rebalance_async` variant (its sync `rebalance` `block_on`s on the caller thread, which
  cannot also drain). The notify slot is shared (`Arc<Mutex<Option<..>>>`) and read at callback
  time so the async client's per-op loop-hop notify takes effect.
- **Alternatives:** a dedicated per-subscription dispatcher thread (breaks the single-FIFO
  deadlock but still runs the listener off the caller's thread — a §31 violation); restructure
  poll to run on a background thread (larger change).
- **Status:** open — owner to confirm the caller-thread drain/ack FFI shape.

### C28 — `MockConsumer.add_record` carries serialized (bytes) key/value (P5)
- **Where:** `bindings/python/confluent_kafka/consumer/_mock_driver.py` (`add_record`).
- **Question:** Java's `MockConsumer<K,V>.addRecord(ConsumerRecord<K,V>)` takes the already-typed
  key/value; the spec (§6.2) says the mock applies the deserializers on `poll()`.
- **Assumption taken:** the record passed to `add_record` carries the **serialized** (`bytes`)
  key/value; `poll()` runs the configured deserializers on them (with the byte defaults this is a
  pass-through). This is the only way to honour "the mock applies serdes on poll" over the FFI's
  byte-oriented `MockConsumer_add_record`, and it makes a `MockConsumer(value_deserializer=
  json_deserializer())` yield decoded values (spec §6.2).
- **Alternatives:** store the typed value and skip deserialization on the mock (diverges from the
  spec's "applies serdes on poll").
- **Status:** open — owner to confirm the serialized-bytes `add_record` contract.

### C29 — `schedule_poll_task` (general form) raises unsupported; `schedule_nop_poll_task` wired (P5)
- **Where:** `bindings/python/confluent_kafka/consumer/_mock_driver.py` (`schedule_poll_task`).
- **Question:** Java's `MockConsumer.schedulePollTask(Runnable)` runs a task on the next poll; the
  Rust core's `schedule_poll_task` takes `Box<dyn FnOnce(&mut MockConsumer<K,V>)>`, which cannot
  bridge a bare Python callable across the FFI safely (the task would need `&mut MockConsumer`).
- **Assumption taken:** `schedule_nop_poll_task()` is fully wired (the only form `MockConsumerTest`
  exercises); the general `schedule_poll_task(task=...)` raises `UnsupportedVersionError` (rule 10)
  rather than a wrong bridge. No in-scope Java test uses the general form.
- **Status:** open — owner to confirm; a future FFI could expose a bare-callable task form.

### C30 — KIP-714 client telemetry gaps raise `UnsupportedVersionError` (P5, extends P4 gap)
- **Where:** `bindings/python/confluent_kafka/consumer/{consumer,async_consumer,_mock_driver,
  _unsupported}.py`.
- **Question:** `Consumer.clientInstanceId`, `registerMetricForSubscription`,
  `unregisterMetricFromSubscription`, and `MockConsumer.{setClientInstanceId,
  injectTimeoutException,disableTelemetry,addedMetrics}` are all KIP-714 telemetry, which the Rust
  core explicitly defers (`src/consumer/mod.rs:142`).
- **Assumption taken:** each Python method exists with the spec's signature and raises
  `UnsupportedVersionError` (rule 10 — an explicit `Error`, never a silent no-op), with the
  negative-timeout check on `client_instance_id` done first so
  `KafkaConsumerTest.testClientInstanceIdInvalidTimeout` is translatable. Same pattern as the P4
  producer (its C4 note).
- **Status:** open — implemented when the core lands KIP-714.

### C31 — `set_poll_exception` / `set_offsets_exception` inject via wire-code round-trip (P5)
- **Where:** `src/ffi/consumer.rs` (`error_from_code_and_message`), `_mock_driver.py`.
- **Question:** the mock exception setters take a Python `KafkaError`; its `_ffi_id` is the CLASS
  enum (`kafka_common_ErrorCode_t`), which diverges from the wire code for 4 inheritance classes
  (BufferExhausted, Authentication, Authorization, SslAuthentication) — the same P4 producer trap.
- **Assumption taken:** the FFI reuses `kafka_common_Error_new(code, message)` (= `Errors::for_code`),
  which round-trips the wire code and collapses those 4 classes to `UnknownServerError`. Acceptable
  for the mock test surface (no in-scope test injects one of the 4); a faithful class-enum inverse
  (`error_from_ffi_code`) is P4/shared work in `src/ffi/common.rs` (owned by the producer actor).
- **Status:** open — tracked with the P4 error-injection-inverse item.

### C32 — `RecordDeserializationError` payload accessors attached at raise time (P5, closes C4)
- **Where:** `bindings/python/confluent_kafka/consumer/_poll.py`
  (`_attach_deserialization_payload`).
- **Question:** the generated `RecordDeserializationError` is a plain leaf (P1 deferred the typed
  payload accessors, C4). The spec (§5.5 / rule 5) requires `topic_partition()`, `offset()`,
  `key_buffer()`, `value_buffer()`, `origin()` + `__cause__` so the poison-pill recovery
  `seek(partition=e.topic_partition(), offset=e.offset()+1)` works.
- **Assumption taken:** the deserialize path builds the error and attaches the payload accessors +
  `__cause__` at raise time (the raw key/value are the batch memoryviews, which pin the batch so
  the buffers stay valid). This is per-instance, over the record being decoded — it does not need
  the FFI `kafka_common_Error_record_deserialization` sub-handle (which is for a core-produced
  error, not a Python-side serde throw). The position is not advanced (§5.4).
- **Status:** open — owner to confirm attaching accessors at raise time (vs. baking them into the
  generated class), which closes C4 for the deserialize path.

### C33 — `metrics()` values are a small `MetricValue` (P5)
- **Where:** `bindings/python/confluent_kafka/consumer/_conversions.py` (`MetricValue`).
- **Question:** `metrics()` returns `dict[MetricName, Metric]`, but `Metric`/`KafkaMetric` are
  `Protocol`s (P2 C12) with no concrete class.
- **Assumption taken:** the binding materialises each FFI metric snapshot as a small `MetricValue`
  satisfying the `Metric` protocol (`metric_name()` / `metric_value()`). Not a Java class (Java's
  `metrics()` returns `KafkaMetric` instances); a binding-internal value type, like the producer's
  metric handling.
- **Status:** open — folds into the metrics design pass (C12).

### C34 — Consumer construction error surfaces the outer wrapper, not the cause (P5)
- **Where:** `bindings/python/confluent_kafka/consumer/_config_resolve.py`;
  `src/ffi/consumer.rs` (`kafka_consumer_KafkaConsumer_new`).
- **Question:** Java's `KafkaConsumer` constructor wraps every failure in
  `KafkaException("Failed to construct kafka consumer", cause)` (the Rust core does this
  unconditionally too, `async_kafka_consumer.rs:1687`). `KafkaConsumerTest.testEmptyGroupId`
  asserts `e.getCause() instanceof InvalidGroupIdException`.
- **Finding:** the FFI's error handle exposes only `code` + `message` (no cause/source chain);
  `from_ffi_error` reads those two. So the binding raises the OUTER error —
  `KafkaError("Failed to construct kafka consumer")` (code `UnknownServerError`) — and cannot see
  the `InvalidGroupIdException` cause.
- **Assumption taken:** raise the outer `KafkaError` with the wrapper message (Java-faithful for
  the outer exception); the translated tests assert that, not the buried cause. A new
  `Consumer_KafkaConsumer_new_typed` FFI hands the construction error back as a handle (rather
  than the legacy `RuntimeError`) so at least the outer typed error surfaces.
- **Alternatives:** add a `kafka_common_Error_source`/`_cause` FFI accessor so the binding can
  chain the cause into `__cause__` (would let the tests assert the `InvalidGroupIdError` cause,
  matching Java exactly) — a shared FFI addition (`src/ffi/common.rs`, producer-actor territory).
- **Status:** open — owner to decide whether to add an error-cause FFI accessor.

### C35 — Rust core does not trim `group.id` before the empty-check (P5)
- **Where:** `src/consumer/consumer_config.rs` / `async_kafka_consumer.rs` (group.id validation);
  test `test_group_id_with_whitespace_rejected` (skipped).
- **Question:** Java's `ConsumerConfig` trims `group.id`, so both `""` and `" "` reach
  `groupId.isEmpty()` and are rejected with `InvalidGroupIdException`
  (`KafkaConsumerTest.testGroupIdWithWhitespace`).
- **Finding:** the Rust core rejects an EMPTY `group.id` but accepts a whitespace-only one
  (`" "`) — it does not trim before the empty check. So the whitespace case is not rejected at
  construction.
- **Assumption taken:** the empty-string test is translated and passes; the whitespace test is
  SKIPPED with this reason (a core behaviour gap, not a binding issue — the binding must not add
  a Python-side trim/reject that the core lacks, per the parity rule).
- **Status:** open — core fix (trim `group.id` before the empty check) would close it.
### C38 — `MockProducer` is pure Python; the Rust core's `MockProducer` + FFI mock surface back only the legacy module (Critic 67 note on C22)
- **Where:** `confluent_kafka/producer/mock_producer.py` vs `src/producer/mock_producer.rs` + `kafka_producer_MockProducer_*`.
- **Question:** the spec's motivation is "one Rust core serves every binding". Critic 67 verified the Rust
  mock IS synchronous, but the FFI dispatcher forces asynchronous completion delivery and there is no
  synchronous FFI mock surface, so Java's synchronous `MockProducer` contract (which `MockProducerTest`
  depends on: `send(...).isDone()` immediately with autoComplete, synchronous `completeNext()`) is only
  reachable today via a Python re-implementation of `MockProducer.java`.
- **Assumption taken:** pure-Python `MockProducer` (option a). The FFI mock stays for the legacy
  `producer.py` until that is retired (C24).
- **Alternatives:** (b) add a synchronous FFI mock surface and wrap it from Python; (c) keep both.
- **Status:** open — owner to confirm (a) or request (b).
- **Status:** open — owner to decide whether to change the hook.
### C39 — `uuid_serializer` wire bytes are not interoperable with Java's `UUIDSerializer` (Critic 66 N1 on C15)
- **Where:** `confluent_kafka/common/serialization/uuid_serializer.py` / `uuid_deserializer.py`.
- **Question:** the spec says `uuid_serializer() -> Serializer[Uuid]` with "Java's UUID serdes". Java's
  `UUIDSerializer` serializes `java.util.UUID.toString()` (dashed hex); Kafka's `common.Uuid` (our
  `Uuid`) has no Java serde and its string form is URL-safe base64.
- **Assumption taken:** serialize our `Uuid` in its own base64 string form — the faithful reading of
  `Serializer<Uuid>` for the type the spec names. Consequence: bytes are NOT readable by a Java
  consumer using `UUIDDeserializer`, and vice versa.
- **Alternatives:** emit the dashed `java.util.UUID` form (interoperable, but `Uuid` ↔ `java.util.UUID`
  bit mapping must be defined), or offer both.
- **Status:** open — owner to decide interop vs type-faithfulness.

- **Status:** open — the package-level typing gap should be closed by adding an `__init__.pyi` (or
  explicit re-exports) to `common/errors` / `common/config`, after which these imports can move back to
  the parent re-export per CLAUDE.md.

