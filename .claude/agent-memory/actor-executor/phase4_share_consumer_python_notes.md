---
name: phase4-share-consumer-python
description: M10 Phase 4 share consumer Python binding — callback lifecycle, borrowed/owned error, mock behaviors
metadata:
  type: project
---

Milestone 10 Phase 4 = the Python binding for the KIP-932 share consumer
(`bindings/python/`: extension glue in `_confluentkafka.c`, wrapper
`share_consumer.py`, tests `test/unit/test_share_consumer.py`). Sync-first.
See [[python-bindings-macos-build]] for how to build/test it on macOS.

**Non-obvious decisions/gotchas worth keeping:**

- **Persistent ack-commit callback refcount.** Extension INCREFs the callable on
  set, DECREFs only on clear/replace (never per invocation — a per-call DECREF
  use-after-frees it). There is no per-consumer C struct (handle is a raw int),
  so the extension can't retrieve the previously-registered callable on clear:
  the wrapper tracks it in `self._ack_commit_bridge` and passes it as `old_cb`
  to `set_acknowledgement_commit_callback(h, new_cb_or_None, old_cb_or_None)`.
  On Rust rejection the extension undoes the new INCREF and leaves old alone.
  `close()` clears it first so the INCREF isn't leaked at destroy.

- **Non-callable validation must be wrapper-side.** The wrapper wraps the user
  callback in an always-callable bridge closure, so the extension's
  `PyCallable_Check` never sees the user object — validate `callable()` in the
  wrapper.

- **Borrowed vs owned KafkaError.** `ShareCommitResult_get_error` is BORROWED
  (read-only, freed by the result) → extract (code,msg,retriable,fatal) eagerly
  in C, do NOT destroy. The ack-commit callback's error is OWNED → destroy after
  reading. Both marshal to a field tuple; the wrapper rebuilds via
  `_make_kafka_error` (can't use `KafkaError._from_c`, which owns+destroys a
  handle). Eager extraction avoids handing Python a pointer that dangles once
  the owning container is destroyed.

- **`MockShareConsumer_add_record` FFI arg order is key/value BEFORE offset**
  (differs from the regular consumer). The wrapper's `add_record(topic,
  partition, offset, key, value)` does the reorder so the C entry point is a
  straight pass-through.

- **Reuse:** the share poll returns the same `ConsumerRecords_t`, so
  `consumer_poll_trampoline`/`consumer_op_trampoline` are reused (byte-identical
  typedefs); `share_commit_trampoline` forwards to `fire_handle_cb` with the
  exact typed signature (no function-pointer cast). `TopicIdPartition` marshals
  to a `(topic, topic_id[16 bytes], partition)` hashable tuple.

**MockShareConsumer behaviors (learned empirically; drove the tests):**
- Never fires the ack-commit callback end-to-end (Rust setter is a no-op) — same
  limitation as the C-FFI mock. Test the register/replace/clear path + drive the
  wrapper's bridge closure directly with synthetic raw payloads.
- `wakeup()` does NOT interrupt mock poll (returns empty, no Wakeup error) —
  UNLIKE the regular MockConsumer which raises Wakeup. So no wakeup-raises test.
- `acknowledge*` accepts unconditionally (bogus offset = no error).
- `add_record` to an unsubscribed topic → KafkaError "Cannot add records for a
  topics that is not subscribed by the consumer".
- `delivery_count`, `acquisition_lock_timeout_ms` → None; `commit_sync` → `{}`.
