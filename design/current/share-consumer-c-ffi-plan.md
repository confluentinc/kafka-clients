# Plan: Share Consumer C FFI layer (KIP-932)

**Status:** Reviewed — decisions locked (§12). This is the **living C-FFI design
reference** for **Milestone-10** (share consumer C + Python bindings); the
milestone-level plan is [`design/history/Milestone-10/PLAN.md`](../history/Milestone-10/PLAN.md).
No code yet.
**Branch:** `milestone9-share-consumer`.
**Scope of this doc:** the **C FFI layer only** — `src/ffi/share_consumer.rs` + supporting
in-crate seams + cbindgen/build wiring + FFI-level tests (Milestone-10 phases 1–6). The
`_confluentkafka.c` extension, `share_consumer.py`, and the gRPC/Python test harness are
Milestone-10 phases 7–9, detailed when they start.

---

## 1. Objective

Expose the already-translated Rust KIP-932 share consumer
(`KafkaShareConsumer` / `ShareConsumerImpl` / `MockShareConsumer`) across a C ABI so
non-Rust callers (the CPython extension, C tests, etc.) can drive the full
subscribe → poll → acknowledge → commit → close flow.

## 2. Ground truth (verified on this branch)

- `src/ffi/` contains **only** `producer.rs` + `mod.rs`. There is **no** consumer FFI
  and **no** `common.rs` here. `src/ffi/mod.rs` declares only `pub(crate) mod producer;`.
- The reference branch `origin/dev/c_and_python_consumer_bindings` has a complete
  consumer FFI (`src/ffi/consumer.rs` ~130 KB, `src/ffi/common.rs` ~278 lines) but
  **no share Rust code**. It is our template, not a merge source.
- Build wiring (this branch): `Cargo.toml` `ffi = ["dep:cbindgen", "dep:env_logger"]`;
  `build.rs` (cfg `ffi`) runs cbindgen → `target/include/confluent_kafka.h`, gated by
  `cbindgen.toml`'s `[export] include` list (currently **producer types only**).
- The share consumer emits the **same** `crate::consumer::ConsumerRecord<K,V>` /
  `ConsumerRecords<K,V>` as the regular consumer. On this branch `ConsumerRecord`
  already carries `delivery_count: Option<i16>` (accessor `delivery_count()`),
  so the KIP-932 delivery-count field is available for free.
- `WakeupHandle` (in `src/consumer/async_kafka_consumer.rs`) already exists with
  `pub(crate) fn for_async(WakeupTrigger, Arc<dyn Fn()+Send+Sync>)` and
  `pub(crate) fn for_mock(Arc<AtomicBool>)`. The regular `Consumer` trait exposes
  `fn wakeup_handle(&self) -> WakeupHandle`.
- `TopicIdPartition` exposes `topic() -> &str`, `topic_id() -> Uuid`,
  `partition() -> i32`, `topic_partition() -> &TopicPartition`.
- `AcknowledgeType` = `Accept`(1) / `Release`(2) / `Reject`(3) / `Renew`(4).
  GAP is **not** a public variant (internal sentinel only).
- `AcknowledgementCommitCallback` (async trait):
  `async fn on_complete(&self, offsets: &HashMap<TopicIdPartition, HashSet<i64>>, error: Option<&KafkaError>)`.
- Construction entry points: `KafkaShareConsumer::new(config, key_deser, value_deser)`
  and `new_share_consumer(...)`, both requiring `K: Clone, V: Clone` (RENEW retention).
  `MockShareConsumer::new()` (parameterless).

## 3. Key design decisions (locked — see §12)

1. **K/V type = `bytes::Bytes`** (mirror the reference consumer FFI). `Bytes: Clone`
   satisfies the share consumer's `K/V: Clone` bound, and — critically — lets us reuse
   the reference's `box_records` + `ConsumerRecord(s)_*` accessors **verbatim**, since
   the share `poll` returns the identical `ConsumerRecords<Bytes,Bytes>` type.
   Requires porting a `BytesDeserializer` to this branch (this branch's
   `serialization/deserializer.rs` has the trait but **no concrete impl**).
2. **Naming prefix = `kafka_consumer_ShareConsumer_*`.** The share consumer lives in
   `org.apache.kafka.clients.consumer`, so per CLAUDE.md §3 the FFI namespace is
   `kafka_consumer_` (drop `clients`), **not** a new `kafka_share_*` root. Shared
   cross-client types keep the `kafka_common_*` prefix (`KafkaError`, `Node`,
   `TopicIdPartition`).
3. **FFI is in-crate**, so it uses `pub(crate)` internals directly. The only new Rust
   seams needed are (a) a `BytesDeserializer` and (b) a wakeup-handle-returning share
   constructor — both small (§4).
4. **Ack-commit callback = a persistent registered callback** stored on the handle and
   fired on the single dispatcher thread. This is the one pattern with no precedent in
   the reference FFI (§8).

## 4. Rust-side prerequisites (must land before/with the FFI)

These are the only production-code changes outside `src/ffi/`. Each is small and additive.

### 4.1 `BytesDeserializer`
Add `src/common/serialization/bytes_deserializer.rs`:
```rust
pub struct BytesDeserializer;
impl Deserializer<bytes::Bytes> for BytesDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<bytes::Bytes, KafkaError> {
        Ok(bytes::Bytes::copy_from_slice(data))
    }
}
```
Re-export from `serialization/mod.rs`. (If the share receive buffer is already
`Bytes`-backed, a later optimization can borrow instead of `copy_from_slice`; not
required for correctness and not on a hot path the FFI benchmarks target.)
**Verify:** `bytes` is (or becomes) a normal dependency for non-ffi builds, or gate the
impl behind the `ffi` feature if `bytes` is ffi-only today.

### 4.2 Wakeup-handle-returning share constructor
`wakeup()` must fire without touching the guarded consumer (an in-flight `poll` holds
`&mut`). Mirror `AsyncKafkaConsumer::wakeup_handle()`: capture a `WakeupHandle` at
construction, before the consumer is type-erased to `Box<dyn ShareConsumer>`.

Add to `src/consumer/mod.rs` a `pub(crate)` variant of the factory that also returns the
handle (the public `new_share_consumer` calls it and drops the handle):
```rust
pub(crate) fn new_share_consumer_with_wakeup<K, V>(
    config: ShareConsumerConfig,
    key_deserializer: Box<dyn Deserializer<K>>,
    value_deserializer: Box<dyn Deserializer<V>>,
) -> Result<(Box<dyn ShareConsumer<K, V>>, WakeupHandle), KafkaError>
where K: Send + Sync + Clone + 'static, V: Send + Sync + Clone + 'static;
```
Inside `build_share_consumer`, construct
`WakeupHandle::for_async(wakeup_trigger.clone(), Arc::new({ let n = Arc::clone(&event_notify); move || n.notify_one() }))`
(reusing the `wakeup_trigger` + `event_notify` already built there).
For the mock: expose `pub(crate) fn wakeup_handle(&self) -> WakeupHandle` on
`MockShareConsumer` returning `WakeupHandle::for_mock(self.wakeup.clone())`.

### 4.3 (verify) `MockShareConsumer::poll` returns added records
The C smoke tests drive a mock without a broker. Confirm `MockShareConsumer::poll`
drains its `records` map into `ConsumerRecords` (it holds
`records: HashMap<TopicPartition, Vec<ConsumerRecord<K,V>>>` + `add_record`, so this is
expected — confirm it returns and clears them, matching Java's `MockShareConsumer`).

## 5. cbindgen / build changes

1. `cbindgen.toml`: add `"typedefs"` to `item_types` (this branch has only
   `["functions", "structs"]`; callback typedefs won't emit without it).
2. `cbindgen.toml` `[export] include`: append every new opaque type, result-container
   type, the `AcknowledgeType` enum, and every callback typedef (full list in §6).
   `kafka_common_KafkaError_t` is already present (producer uses it).
3. No `build.rs` change needed — it already regenerates the header under `--features ffi`.
4. `src/ffi/mod.rs`: add `pub(crate) mod common;` and `pub(crate) mod share_consumer;`
   (both `#[cfg(feature = "ffi")]`, matching the reference `mod.rs`).

## 6. C type inventory (what cbindgen must emit)

**Ported from reference `common.rs` (reused as-is):**
- `kafka_common_KafkaError_t` (opaque; null = success) + `_code` / `_message` /
  `_is_retriable` / `_is_fatal` / `_destroy`.

**Reused verbatim from reference consumer FFI (record marshaling):**
- `kafka_consumer_ConsumerRecords_t` + `_count` / `_is_empty` / `_get` / `_destroy`.
- `kafka_consumer_ConsumerRecord_t` + `_partition` / `_offset` / `_timestamp` /
  `_timestamp_type` / `_topic` / `_key` / `_value` / `_serialized_key_size` /
  `_serialized_value_size` / `_leader_epoch` / `_delivery_count` / `_header_count` /
  `_header_key` / `_header_value`.

**New opaque handles:**
- `kafka_consumer_ShareConsumer_t` — the share consumer handle.
- `kafka_consumer_ShareConsumerProperties_t` — config map (`HashMap<String,String>`).
- `kafka_common_TopicIdPartition_t` — `{topic: CString, topic_id: 16 bytes, partition: i32}`;
  accessors `_topic` (`*const c_char`), `_topic_id` (`*const u8` len 16), `_partition`.

**New result-container handles (owned, `_count`/`_get*`/`_destroy`):**
- `kafka_consumer_ShareCommitResult_t` — the `commit_sync` return
  `HashMap<TopicIdPartition, Option<KafkaError>>`; per-entry accessors
  `_get_partition(i) -> *const TopicIdPartition_t`, `_get_error(i) -> *const KafkaError_t`
  (null = that partition committed OK).
- `kafka_consumer_ShareAcknowledgeOffsets_t` — the ack-callback payload
  `HashMap<TopicIdPartition, HashSet<i64>>`; accessors `_partition_count`,
  `_get_partition(i)`, `_offset_count(i)`, `_get_offset(i, j)`.

**New enum:**
- `kafka_consumer_AcknowledgeType_t` (`#[repr(i32)]`, cbindgen `prefix_with_name`):
  `ACCEPT=1, RELEASE=2, REJECT=3, RENEW=4`.

**New callback typedefs (`unsafe extern "C" fn`):**
- Poll: reuse `kafka_consumer_Consumer_poll_callback_t` shape →
  `(*mut ConsumerRecords_t, *mut KafkaError_t, *mut c_void)`.
- Void ops (unsubscribe/commit_async/close): reuse the op-callback shape →
  `(*mut KafkaError_t, *mut c_void)`.
- Commit-sync value op:
  `kafka_consumer_ShareConsumer_commit_callback_t = (*mut ShareCommitResult_t, *mut KafkaError_t, *mut c_void)`.
- Registered ack-commit callback:
  `kafka_consumer_ShareConsumer_AcknowledgementCommitCallback_t = (*const ShareAcknowledgeOffsets_t, *const KafkaError_t, *mut c_void)`.

## 7. Full extern-C function surface

Naming: `#[unsafe(no_mangle)] pub [unsafe] extern "C" fn`. Errors: void ops return
`*mut KafkaError_t` (null = ok); constructors/value-returning ops write
`out_error: *mut *mut KafkaError_t` and return the data handle (or null). Every async
`_async` fn takes a trailing `(callback, user_data: *mut c_void)`.

**Lifecycle / config**
- `ShareConsumerProperties_new` / `_from_configs` / `_put` / `_destroy`.
- `KafkaShareConsumer_new(props, out_error) -> *mut ShareConsumer_t` (capture wakeup,
  reject blank `group.id`, seed `share.acknowledgement.mode`).
- `MockShareConsumer_new() -> *mut ShareConsumer_t`.
- `ShareConsumer_destroy(consumer)` — 3-step teardown (runtime.shutdown_background →
  drop consumer → drop completion_tx + detach dispatcher); does **not** take the guard.
- `ShareConsumer_wakeup(consumer)` — fires the captured `WakeupHandle`; bypasses guard.

**Subscription (async — network-blocking)**
- `ShareConsumer_subscribe` / `_subscribe_async` (topics = NULL-terminated `*const *const c_char`).
- `ShareConsumer_unsubscribe` / `_unsubscribe_async`.
- `ShareConsumer_subscription(consumer, out_error) -> *mut StringList_t` (sync state read).

**Poll (async — network-blocking)**
- `ShareConsumer_poll(consumer, timeout_ms, out_error) -> *mut ConsumerRecords_t`.
- `ShareConsumer_poll_async(consumer, timeout_ms, callback, user_data)`.

**Acknowledge (sync, guarded, no dispatcher — records intent only)**
- `ShareConsumer_acknowledge(consumer, record: *const ConsumerRecord_t) -> *mut KafkaError_t`.
- `ShareConsumer_acknowledge_with_type(consumer, record, ack_type: AcknowledgeType_t) -> *mut KafkaError_t`.
- `ShareConsumer_acknowledge_by_offset(consumer, topic: *const c_char, partition: i32, offset: i64, ack_type) -> *mut KafkaError_t`.

**Commit / close (async)**
- `ShareConsumer_commit_sync(consumer, out_error) -> *mut ShareCommitResult_t` +
  `_commit_sync_async(consumer, commit_callback, user_data)`.
- `ShareConsumer_commit_sync_timeout(consumer, timeout_ms, out_error) -> *mut ShareCommitResult_t` +
  `_commit_sync_timeout_async(...)`.
- `ShareConsumer_commit_async(consumer) -> *mut KafkaError_t` +
  `_commit_async_async(consumer, op_callback, user_data)`
  (Java `commitAsync` does not block on the network but drains the callback queue).
- `ShareConsumer_close(consumer) -> *mut KafkaError_t` +
  `_close_timeout(consumer, timeout_ms)` + `_close_async(...)`.

**Ack-commit callback + misc (sync)**
- `ShareConsumer_set_acknowledgement_commit_callback(consumer, callback_or_null, user_data)`
  (NULL clears — matches `set_acknowledgement_commit_callback(None)`).
- `ShareConsumer_acquisition_lock_timeout_ms(consumer, out_ms: *mut i32, out_error) -> bool`
  (returns presence; writes the value when present).
- `client_instance_id` — **omitted from the FFI** this milestone (decision §12.2); the
  underlying Rust method returns a telemetry-disabled `illegal_state`. Revisit when
  KIP-714 telemetry lands.

**Mock drivers (inherent methods, not on the trait)**
- `MockShareConsumer_add_record(consumer, topic, partition, key_ptr, key_len, value_ptr, value_len, offset, out_error)`.
- `MockShareConsumer_set_client_instance_id(consumer, id_ptr /*16 bytes*/)`.

## 8. The async bridge (reuse) + the ack-commit callback (new)

**Reuse from `common.rs` + reference patterns:** `ConsumerHandle` → `ShareConsumerHandle`
(same fields: `UnsafeCell<ShareConsumerKind>`, `owner: AtomicU64`, embedded
multi-thread `Runtime`, `runtime_handle`, `completion_tx`, `dispatcher`,
`wakeup_handle`, `is_mock`). `ShareConsumerKind = { Kafka(Box<dyn ShareConsumer<Bytes,Bytes>>), Mock(Box<MockShareConsumer<Bytes,Bytes>>) }`.
- Sync path: `acquire` → `ReleaseGuard` → `runtime.block_on(consumer_mut(h).method())`.
- Async path: `acquire` (fire callback inline with `box_error` on failure) →
  `runtime_handle.spawn` the awaited op → build owned result handles **after** the await
  → enqueue a `CompletionJob` that **releases the guard, then fires the callback** on the
  dispatcher thread. Poll mirrors the reference's bespoke `poll_async`; void/value ops
  reuse `async_void_op` / `async_value_op`.

**Acknowledge-by-record correctness note (why this works zero-copy):** `poll` moves the
record objects out via `current_fetch.take_records()`, but the share consumer's
`current_fetch` retains **offset-level in-flight tracking** (`in_flight_offsets`,
Phase-6 blocker 1). `ShareConsumer_acknowledge(record_ptr)` dereferences a
`*const ConsumerRecord<Bytes,Bytes>` borrowed from the boxed `ConsumerRecords` batch and
calls `consumer.acknowledge(&record)`, which matches `record.offset()` against
`in_flight_offsets`. **C contract:** the `ConsumerRecords_t` batch must outlive the
`acknowledge` calls, and the record must come from this consumer's most recent poll
(else the Rust side returns `IllegalState "The record cannot be acknowledged."`, exactly
like Java).

**Registered ack-commit callback (the one genuinely new pattern):**
1. `set_acknowledgement_commit_callback` stores `Option<(C fn ptr, SendUserData)>` on the
   handle (behind the guard model; setting is a sync, guarded op) and, on first
   registration, wraps it in a Rust type implementing `AcknowledgementCommitCallback`:
   ```rust
   struct FfiAckCommitCallback { cb: AckCb, user_data: SendUserData, completion_tx: Sender<CompletionJob> }
   #[async_trait] impl AcknowledgementCommitCallback for FfiAckCommitCallback {
       async fn on_complete(&self, offsets: &HashMap<TopicIdPartition, HashSet<i64>>, error: Option<&KafkaError>) {
           // Marshal the BORROWED map into an owned ShareAcknowledgeOffsets_t + optional
           // boxed KafkaError_t (the borrow ends when this returns), then enqueue a job
           // that invokes the C fn on the dispatcher thread. One alloc per commit; off
           // the per-record hot path.
       }
   }
   ```
   Register it via `consumer.set_acknowledgement_commit_callback(Some(Arc::new(...)))`.
2. Because §31 already guarantees `on_complete` runs on the **app task** during the
   `poll`/`commit_*` drain (which the FFI drives on the runtime via `block_on`/spawn),
   routing the C invocation onto the shared dispatcher thread keeps callback threading
   consistent with all other FFI callbacks (single predictable thread), and honors the
   "never the bg task, never per-callback spawn" rule.
3. Clearing (NULL) calls `set_acknowledgement_commit_callback(None)` and drops the stored
   C pointers.

## 9. Memory ownership rules (documented per function)

- Rust allocates (`Box::into_raw`); **C frees** via the matching `_destroy`. All
  `_destroy` are null-safe no-ops.
- Borrowed sub-handles (records from a batch; key/value byte slices; `TopicIdPartition`
  from a result container) are **not** separately freed — invalidated when the owning
  handle is destroyed.
- Async completion callbacks **take ownership** of whichever non-null result/error handle
  they receive and must free it.
- Byte returns: `*const u8` + `out_len` (`-1` absent). Topic/header strings:
  `*const c_char` + len, **not** NUL-terminated. Owned strings (none expected on the
  share path beyond `TopicIdPartition._topic`, which is a cached NUL-terminated `CString`
  inside the container).

## 10. Phased implementation (each phase: build+test+lint+format green, then commit)

| Phase | Deliverable | Exit criteria |
|---|---|---|
| **1. Foundation** | Port `src/ffi/common.rs`; add `BytesDeserializer` (§4.1); wire `mod.rs`; cbindgen `item_types += typedefs` + seed `[export]`. | `cargo build --features ffi` clean; header emits `kafka_common_KafkaError_t`. |
| **2. Handle + lifecycle** | `ShareConsumerHandle`, `ShareConsumerKind`, wakeup seam (§4.2); `ShareConsumerProperties_*`; `KafkaShareConsumer_new`, `MockShareConsumer_new`, `_destroy`, `_wakeup`. | Rust test: create/destroy/wakeup a mock over the ABI; no leaks under the guard. |
| **3. Poll + records** | `_poll` / `_poll_async`; reuse `box_records` + all `ConsumerRecord(s)_*` accessors (incl. `_delivery_count`). | Poll a mock; read record key/value/offset/delivery_count in a Rust FFI test. |
| **4. Acknowledge** | `AcknowledgeType_t`; `_acknowledge` / `_acknowledge_with_type` / `_acknowledge_by_offset`. | Acknowledge polled records; `IllegalState` surfaced for a non-in-flight record. |
| **5. Commit + close + ack callback** | `TopicIdPartition_t`, `ShareCommitResult_t`, `ShareAcknowledgeOffsets_t`; `_commit_sync[_timeout]` (+async), `_commit_async` (+async), `_close[_timeout]` (+async), `set_acknowledgement_commit_callback`, `acquisition_lock_timeout_ms`. | Full subscribe→poll→ack→commit→close round-trip against a mock, with the registered ack callback firing on the dispatcher thread. |
| **6. Tests** | Rust `#[cfg(test)]` FFI tests (drive `MockShareConsumer` through the extern-C fns; assert error messages, not just presence); C smoke test `bindings/c/tests/test_mock_share_consumer.c` modeled on the reference `test_mock_consumer.c`; wire into `bindings/c/Makefile` / CMake behind `--features ffi`. | `cargo test --features ffi` green; C smoke test builds+passes against the generated header. |

Each phase commits incrementally (Actor role). Phases 1–2 are the highest-risk (new
infra + the wakeup seam); 3 is nearly free (accessor reuse); 5 carries the only novel
design (ack callback).

## 11. Testing strategy

- **Rust FFI unit tests** (`#[cfg(test)]` in `share_consumer.rs`): construct a
  `MockShareConsumer` via `MockShareConsumer_new`, `add_record`, then exercise
  `poll`/`acknowledge`/`commit_sync`/`close` through the raw extern-C fns using a
  Rust-defined `extern "C"` callback for the async paths. Assert **error message
  content** (DoD §3), guard rejection on concurrent access, and that `_destroy` after an
  in-flight async op is safe (teardown ordering).
- **C smoke test**: mirror `test_mock_consumer.c` — include the generated header, create
  a mock share consumer, poll, acknowledge, commit, close; assert non-null/handle counts.
- **No broker / AdminClient dependency** in these tests (same constraint that
  `#[ignore]`-gates the integration tests). Full end-to-end against a live share-group
  broker is a Python-layer / harness concern, deferred.
- **DoD §11 (consumer trait surface):** confirm the ack callback path is not
  `tokio::spawn`-per-callback and does not run on the bg task; confirm no `block_on`
  sync façade is added to the Rust consumer itself (the runtime lives only in the FFI
  handle).

## 12. Resolved decisions (locked in review, 2026-07-16)

1. **K/V = `bytes::Bytes`.** ✅ Confirmed. Port `BytesDeserializer`; verify `bytes` is
   available in the non-ffi build config, or gate the impl behind the `ffi` feature.
2. **`client_instance_id` — omit.** ✅ Not exposed in the FFI this milestone; revisit
   when KIP-714 telemetry lands. (§7 updated: no FFI entry point is generated.)
3. **Two bespoke result containers.** ✅ `ShareCommitResult_t` +
   `ShareAcknowledgeOffsets_t` (not a single generic `TopicIdPartition`-keyed map).
4. **New milestone.** ✅ The whole C + Python share-bindings effort is **Milestone-10**,
   with per-phase `PLAN.md` + `COMMENTS.N.md` under `design/history/Milestone-10/`
   following the M8/M9 convention. This doc is the living C-FFI design reference; the
   milestone-level plan is `design/history/Milestone-10/PLAN.md`.
5. **Shared `common.rs` from day one.** ✅ `share_consumer.rs` depends on a shared
   `common.rs` so a later consumer-FFI port de-duplicates cleanly.

## 13. Non-goals (this plan)

- `bindings/python/_confluentkafka.c` share glue, `share_consumer.py`, Python unit tests,
  gRPC harness wiring — follow-on layers.
- Any change to the share consumer's Rust behavior/semantics beyond the two additive
  seams in §4.
- Live-broker / AdminClient integration tests.
