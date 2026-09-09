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
