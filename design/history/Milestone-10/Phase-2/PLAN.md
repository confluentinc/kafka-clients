# Phase 2: Share consumer FFI — handle + read + acknowledge

*(Merged phase: was original C-FFI phases 2+3+4. Split at the read/write seam —
this phase is everything up to and including sync acknowledge-intent; the async
commit/close write path + the registered ack-commit callback are Phase 3.)*

## Goal

Build the share consumer's **construct + read + acknowledge** C ABI in a new
`src/ffi/share_consumer.rs`, on top of the Phase-1 shared `common.rs`:

1. **Handle + lifecycle + config** — the `ShareConsumerHandle` (embedded Tokio
   runtime, single-owner guard, dispatcher, captured `WakeupHandle`), properties,
   construct/destroy/wakeup, subscribe/unsubscribe/subscription.
2. **Read** — `poll` (sync + async) and the record-accessor surface, reusing the
   reference's zero-copy `box_records` + `ConsumerRecord(s)_*` marshaling.
3. **Acknowledge** — `AcknowledgeType_t` + the three sync `acknowledge*` entry
   points (record intent only, no network).

No commit/close, no ack-commit callback, no result-container types (all Phase 3).

## Branch

`milestone9-share-consumer`. All commits land here.

## Why merged (review decision)

The three slices are cohesive and comparatively low `unsafe`-risk: acknowledge
depends on poll (it takes a polled record pointer), and all three exercise the
same handle/guard/dispatcher plumbing. Merging halves the Actor→Critic
round-trips. Defect localization is preserved by the **three internal
incremental commits** below (2a/2b/2c), each independently green. The
higher-risk async-commit completions + the novel registered-callback marshaling
are isolated in Phase 3.

## Sources / template

- **Template:** `src/ffi/consumer.rs` on `origin/dev/c_and_python_consumer_bindings`
  (scratchpad `ref_consumer_ffi.rs`) — `ConsumerHandle`, `acquire`/`release`/
  `ReleaseGuard`, the bespoke `poll` async bridge, `box_records`/
  `ConsumerRecordsInner`, and every `ConsumerRecord(s)_*` accessor.
- **Phase-1 output (this branch):** `src/ffi/common.rs` (error handles, logger,
  and the staged dispatcher/callback machinery this phase now *consumes*).
- **Rust API (M9):** `KafkaShareConsumer`, `MockShareConsumer`,
  `ShareConsumer<Bytes, Bytes>`, `new_share_consumer`.
- **Design reference:** `design/current/share-consumer-c-ffi-plan.md` §6–§9.

## Rust-side seams (in-crate; small, additive)

- `pub(crate) fn new_share_consumer_with_wakeup<K, V>(...) -> Result<(Box<dyn ShareConsumer<K, V>>, WakeupHandle), KafkaError>`
  in `src/consumer/mod.rs`; the public `new_share_consumer` delegates and drops
  the handle. Build the handle inside `build_share_consumer` via
  `WakeupHandle::for_async(wakeup_trigger.clone(), Arc::new({ let n = Arc::clone(&event_notify); move || n.notify_one() }))`.
- `pub(crate) fn wakeup_handle(&self) -> WakeupHandle` on `MockShareConsumer`,
  returning `WakeupHandle::for_mock(self.wakeup.clone())`.

## Output — C surface (all under `kafka_consumer_` per CLAUDE.md §3)

**New opaque types** (add to `cbindgen.toml` `[export] include`):
`kafka_consumer_ShareConsumer_t`, `kafka_consumer_ShareConsumerProperties_t`,
the shared `kafka_consumer_ConsumerRecords_t` / `kafka_consumer_ConsumerRecord_t`,
and the enum `kafka_consumer_AcknowledgeType_t` (`#[repr(i32)]`
`ACCEPT=1/RELEASE=2/REJECT=3/RENEW=4`, cbindgen `prefix_with_name`). Plus the poll
callback typedef `kafka_consumer_ShareConsumer_poll_callback_t` and the op-callback
typedef for subscribe/unsubscribe async.

**Shared record marshaling — placement (decision #5):** the generic
`box_records` + `ConsumerRecordsInner` + all `ConsumerRecord(s)_*` accessors are
consumer-generic (a future regular-consumer FFI reuses them verbatim), so they
live in the **shared** module (`common.rs`, or a `common`-adjacent `records`
submodule if `common.rs` grows unwieldy — Actor's discretion, but **shared, not
share-private**). Only `ShareConsumer`-specific fns live in `share_consumer.rs`.

**Lifecycle / config:** `ShareConsumerProperties_{new,from_configs,put,destroy}`;
`KafkaShareConsumer_new` (capture wakeup before boxing; reject blank `group.id`;
seed `share.acknowledgement.mode`); `MockShareConsumer_new`; `ShareConsumer_destroy`
(3-step teardown: `runtime.shutdown_background()` → drop consumer → drop
`completion_tx` + detach dispatcher; no guard); `ShareConsumer_wakeup` (fires the
captured `WakeupHandle`; bypasses the guard).

**Subscription (async):** `ShareConsumer_subscribe` / `_subscribe_async`,
`_unsubscribe` / `_unsubscribe_async`, `subscription` (sync state read). Required
this phase so the mock poll test can subscribe before `add_record`.

**Read (async poll):** `ShareConsumer_poll` (sync, `block_on`) / `_poll_async`
(bespoke completion, mirroring the reference's `poll_async`); the reused
`ConsumerRecords_{count,is_empty,get,destroy}` and `ConsumerRecord_{partition,
offset,timestamp,timestamp_type,topic,key,value,serialized_key_size,
serialized_value_size,leader_epoch,delivery_count,header_count,header_key,
header_value}`.

**Acknowledge (sync, guarded — intent only):** `ShareConsumer_acknowledge`
(`ACCEPT`), `_acknowledge_with_type`, `_acknowledge_by_offset`. `_acknowledge`
takes a `*const ConsumerRecord_t` borrowed from the last poll batch.

**Mock drivers:** `MockShareConsumer_add_record(consumer, topic, partition,
key_ptr, key_len, value_ptr, value_len, offset, out_error)`,
`MockShareConsumer_set_client_instance_id`.

## Implementation steps (each an independent, green commit)

- **2a — handle + lifecycle + config + subscription + wakeup seam.**
  `ShareConsumerHandle`/`ShareConsumerKind`, `acquire`/`release`/`ReleaseGuard`,
  `build_share_consumer_handle`, the two Rust seams, `ShareConsumerProperties_*`,
  `new`/`mock_new`/`destroy`/`wakeup`, subscribe/unsubscribe/subscription, mock
  drivers; wire `pub(crate) mod share_consumer;` in `mod.rs`; add the lifecycle
  types to `cbindgen.toml`. **Test:** create → subscribe → wakeup → destroy a
  mock; guard rejects a concurrent op. **Commit.**
- **2b — poll + shared record marshaling.** `ShareConsumer_poll` / `_poll_async`;
  move/port `box_records` + `ConsumerRecord(s)_*` into the shared module; add
  those types + the poll callback typedef to `cbindgen.toml`. **Test:**
  `add_record` → poll → read `key`/`value`/`offset`/`delivery_count`. **Commit.**
- **2c — acknowledge.** `AcknowledgeType_t` + the three `acknowledge*` fns.
  **Test:** poll then acknowledge; a non-in-flight record surfaces the correct
  `IllegalState` message (where the mock supports it). **Commit.**

## Tests (ship with the code, DoD §3)

Rust `#[cfg(test)]` tests in `share_consumer.rs` that drive `MockShareConsumer`
through the raw `extern "C"` fns (a Rust-defined `extern "C"` callback exercises
the `_async` paths): full create → subscribe → `add_record` → poll → read fields
→ acknowledge → wakeup → destroy. Assert **error-message content** (not just
non-null), guard rejection on a concurrent acquire, and that `_destroy` after an
in-flight `_async` op is safe (teardown ordering). The consolidated **C smoke
test is Phase 3.**

## Definition of Done

- `cargo build` (default) and `cargo build --features ffi` clean.
- `cargo test --features ffi` green (no regressions).
- `cargo xtask lint` (now ffi-covering) and `cargo xtask format-check` clean.
- Header regenerates with the new types present in `[export] include`; the
  `AcknowledgeType_t` enum emits with `prefix_with_name`.
- No leaks under the single-owner guard; `_destroy` teardown ordering verified.

## Risks / watch-items

- **`_poll_async` guard/release timing** is the main `unsafe` risk this phase:
  acquire at submit, build result handles *after* the await, and **release inside
  the completion job before firing the callback** (per the reference) — otherwise
  aliasing / use-after-free. Capture the `&'static ShareConsumerHandle`, not a raw
  `*mut` (keeps the future `Send`).
- **Record-pointer lifetime:** `box_records`' flat index holds `*const
  ConsumerRecord<Bytes,Bytes>` into a boxed batch that must not move; document the
  C contract that record pointers (and their key/value slices) are valid only
  until `ConsumerRecords_destroy`.
- **Acknowledge contract:** the record must come from *this* consumer's most
  recent poll and its batch must outlive the `acknowledge` call; relies on the M9
  offset-level in-flight tracking. Document it.
- **Mock preconditions:** `MockShareConsumer::add_record` requires the topic
  already subscribed — tests must subscribe first.
- **Shared-vs-share placement:** keep the generic record marshaling shared so a
  future consumer FFI reuses it (decision #5); do not bury it in
  `share_consumer.rs`.

## Not in this phase (→ Phase 3)

`commit_sync`/`_timeout`/`commit_async`/`close` (async completions),
`set_acknowledgement_commit_callback` (the registered callback + its marshaling),
`acquisition_lock_timeout_ms`, the result-container types (`TopicIdPartition_t`,
`ShareCommitResult_t`, `ShareAcknowledgeOffsets_t`), and the C smoke test.
`client_instance_id` is omitted for the whole milestone (KIP-714).
