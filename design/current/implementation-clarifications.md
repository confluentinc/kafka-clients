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

### C20 — `uuid_serializer` wire bytes are not interoperable with Java's `UUIDSerializer` (Critic 66 N1 on C15)
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

### C22 — MockProducer record/offset history is mirrored Python-side, not read from the core (P4)
- **Where:** `bindings/python/confluent_kafka/producer/mock_producer.py` (`history`,
  `uncommitted_records`, `uncommitted_offsets`, `consumer_group_offsets_history`).
- **Question:** Java's `MockProducer` stores the **original** `ProducerRecord<K, V>` objects in its
  `sent` / `uncommittedSends` lists (`MockProducer.java:328-331`, `commitTransaction` moves
  `uncommittedSends` → `sent` at `:224`). The Rust core mock stores **serialized** records
  (`Vec<ProducerRecord<Vec<u8>, Vec<u8>>>`), so reading history back through the FFI would hand back
  `bytes` keys/values, and `history()` would not compare equal to the original
  `ProducerRecord(topic=…, key=…, value=…)` the test constructed (Java's canonical
  `assertEquals(singletonList(record1), producer.history())`).
- **Assumption taken:** the Python `MockProducer` wrapper mirrors Java's four lists Python-side,
  holding the **original** `ProducerRecord[K, V]` objects and the offset maps, and applies Java's
  exact `send`/`beginTransaction`/`sendOffsetsToTransaction`/`commitTransaction`/`abortTransaction`/
  `clear` bookkeeping (translated from `MockProducer.java:154-260, 287-338, 490`) **after** the
  corresponding core call succeeds (so a core-side raise leaves the lists untouched). The core
  remains authoritative for the transaction flags (`transaction_initialized/in_flight/committed/
  aborted`, `sent_offsets`, `commit_count`, `closed`, `flushed`), read through the new scalar FFIs.
  This is MORE faithful to Java (which also keeps the originals) than round-tripping serialized bytes,
  and avoids four complex record/map-marshaling FFI functions.
- **Alternatives:** add FFI to read the core's serialized history and reconstruct — rejected
  (bytes≠original, breaks equality); make the core store originals — out of scope (core change).
- **Status:** open — owner to confirm mirroring the record/offset lists in the Python wrapper.

### C23 — `error_next(*, error)` reconstructs by class-code, not identity (P4)
- **Where:** `bindings/python/confluent_kafka/producer/mock_producer.py` (`error_next`); FFI
  `kafka_producer_MockProducer_error_next_with_code` + core `error_from_ffi_code`
  (`src/ffi/common.rs`, `src/ffi/producer.rs`).
- **Question:** Java's `MockProducer.errorNext(RuntimeException e)` fails the next send with the
  **same exception instance** `e` (`MockProducerTest.testManualCompletion` / `testMetadataOnException`
  assert `future.get()`'s cause **== e**). Across the FFI the error crosses as a
  `(class-code, message)` pair (there is no way to carry a live Python object into the Rust core),
  so identity cannot be preserved.
- **Assumption taken:** `error_next(*, error)` passes `to_ffi_id(error)` (the class code) + `str(error)`
  through the new `error_next_with_code` FFI; the core reconstructs the matching `Error` via
  `error_from_ffi_code` (the inverse of `error_code_of`), and it re-crosses back to Python as the
  same typed **class** with the same message. The four inheritance classes that the pre-existing
  `error_next(error_code: i16)` collapsed to `UnknownServerError` (buffer-exhausted / authentication /
  authorization / ssl-authentication — that path decodes wire codes) now round-trip, because the new
  path uses the class-enum numbering that matches `_ffi_id`. The translated tests assert the error's
  **type and message**, not Python object identity.
- **Alternatives:** keep a Python-side table mapping an injected id back to the original object — would
  restore identity but only for errors injected on the same producer, and diverges from how a real
  completion error arrives (it comes from the core as a fresh object); rejected.
- **Status:** open — owner to confirm type+message round-trip is acceptable in place of Java's instance
  identity for injected mock errors.

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

### C25 — `client_instance_id` / `register|unregister_metric_for_subscription`: core capability gap (P4)
- **Where:** `bindings/python/confluent_kafka/producer/producer.py`.
- **Question:** the Rust core's `Producer` trait (`src/producer/producer_trait.rs`) exposes neither
  `client_instance_id` nor metric-subscription registration (KIP-714 telemetry); the mock has no
  `inject_timeout` / `disable_telemetry` / `set_client_instance_id` / `added_metrics` either.
- **Assumption taken:** per rule 10, the Python methods exist with the spec's signatures and raise the
  mapped Java error (a base `KafkaError` explaining the core gap) rather than silently no-op'ing —
  except that `client_instance_id`'s **negative-timeout** validation runs Python-side first and raises
  `IllegalArgumentError("The timeout cannot be negative.")` (Java's exact message,
  `KafkaProducerTest.testClientInstanceIdInvalidTimeout`), so that test translates. The mock's
  `inject_timeout_exception` / `disable_telemetry` / `set_client_instance_id` / `added_metrics`
  raise the mapped error (core lacks the capability). `set_mock_metrics` DOES exist in the core, but
  wiring a Python `Metric` (a `Protocol`, §5.1) into a native `KafkaMetric` handle requires
  marshaling machinery with no in-scope caller (no MockProducerTest case exercises it), so it too
  raises the mapped error for now — a wiring gap, not a missing core capability. Gaps logged here.
- **Alternatives:** implement client telemetry in the core — a separate milestone.
- **Status:** open — telemetry/metric-subscription tracked for a future core phase.

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
