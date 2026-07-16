---
name: review-m10-phase4-python
description: M10 Phase 4 Python share-consumer binding — CPython-extension refcount-lifecycle audit heuristics, persistent-callback balance, owned/borrowed frees, shim build recipe
metadata:
  type: project
---

M10 Phase 4 = Python KIP-932 share consumer (`bindings/python/_confluentkafka.c`
share block + `share_consumer.py` wrapper + `test/unit/test_share_consumer.py`).
Reviewed clean, NOT blocking. Related: [[review-m10-phase3]],
[[review-m10-phase2-share-ffi]], [[review-m10-phase1-ffi-foundation]].

**Persistent ack-commit callback refcount — the one novel CPython lifecycle.**
`set_acknowledgement_commit_callback(h, new_cb, old_cb)` (`_confluentkafka.c`):
INCREF new on set; on Rust success DECREF `old_cb`; on Rust rejection undo the
INCREF and DON'T touch `old_cb`; trampoline NEVER DECREFs per call. Balance
depends on the WRAPPER passing the right `old_cb` (it tracks
`self._ack_commit_bridge`, sets it AFTER success). All six paths (set / replace /
clear(None) / reject-on-{set,replace,clear}) verified balanced.

**Reusable heuristic — assert the C refcount, don't trust wrapper attributes.**
Register/replace/clear tests that only check `self._ack_commit_bridge is
None/not None` have NO teeth for the actual INCREF/DECREF: the wrapper attribute
is independent of the extension's refcount, so a leaked INCREF or an over-DECREF
(UAF) passes them. Give teeth with a Python-only probe:
`sys.getrefcount(bridge)` while registered == attribute(1) + extension INCREF(1)
+ getrefcount's temp(1) = **3** (double-INCREF→4, missing→2), and `weakref.ref`
must die after clear / replace(+drop local) / close / 200 set-clear cycles. This
probe is the definitive check when the mock can't fire the callback.

**Test-teeth gap that recurs (filed as should-fix, not a bug):** the C
trampoline `ack_commit_callback_trampoline` (owned-handle destroys + persistent
refcount) runs in ZERO tests — the mock's Rust setter is a no-op (matches C-FFI
mock, see [[review-m10-phase2-share-ffi]]), and the firing test drives the
*Python* `bridge` closure directly (good teeth for `_to_share_ack_offsets` /
`_make_kafka_error`, none for the C `share_ack_offsets_to_dict` /
`topic_id_partition_to_py` / the two `destroy`s). No `ShareAcknowledgeOffsets_t`
constructor is exposed to synthesize input, so the C trampoline is genuinely
hard to test without new FFI scaffolding or a fire-capable mock.

**Owned/borrowed free audit (against the header doc comments):**
- ack trampoline: `ShareAcknowledgeOffsets_t` is **owned-by-callback** → destroy
  once after marshal; its error is **owned** → `kafka_error_to_py_fields(e,
  owned=1)` destroys. Both freed even on the dict-build-fail branch.
- `ShareCommitResult_drain`: per-partition error is **borrowed** →
  `kafka_error_to_py_fields(e, owned=0)` KEEPS it; the result itself is **owned**
  → destroy once on every path. `TopicIdPartition_t` is borrowed (no standalone
  destructor). Never free a borrowed handle (= UAF).
- commit trampoline hands result+error ints to Python; wrapper's
  `_resolve_value`/`_free_value` drain(destroy) result + `_from_c`/destroy error.
  Exactly-one-of resolve/free runs per op ⇒ no double-free.

**`_make_kafka_error` vs `KafkaError._from_c`:** `_from_c` OWNS+destroys the
handle int (use only for owned error ints: poll/commit/subscribe/subscription/
acquisition-lock). `_make_kafka_error(code,msg,retriable,fatal)` builds from
eagerly-extracted fields (use for borrowed / already-consumed C errors). Field
names must match `producer.KafkaError`: `_code/_message/_is_retriable/_is_fatal`.

**Non-findings (don't re-report):** `MockShareConsumer_add_record` arg order is
CORRECT end-to-end — Python `add_record(topic,partition,offset,key,value)` →
wrapper reorders to `(topic,partition,key,value,offset)` → C `"KsiOOL"` → FFI
`key,key_len,value,value_len,offset`. No `__del__` on the wrapper = handle leak
without close(), but the sibling `consumer.py` has none either (established
"must close explicitly" contract); callback INCREF leak in that path co-occurs
with the handle leak, bounded, no UAF. SIGINT test absent = consistent (sibling
`@pytest.mark.skip`s it; mock poll never blocks). `_confluentkafka.c` has no
Apache-2.0 header but PREDATES this branch (share block purely appended) — accept,
recommend separate whole-file hygiene commit.

**Build/run recipe (macOS, this box):** release lib + header already at
`target/release/libconfluent_kafka.{a,dylib}` + `target/include/`. venv (system
python lacks setuptools/pytest); `CONFLUENT_KAFKA_LIB_DIR=<root>/target/release
CFLAGS=-I<scratchpad>/shim pip install -e .` (shim supplies C11 `<threads.h>` the
PRODUCER half needs; share code uses no threads). Then `pytest test/unit`. 33/33
green; 12x stress-run clean (refcount bugs surface only under repetition/GC).
Whole 3-commit diff is `bindings/python/**` only ⇒ Rust/C layer provably
unaffected, no rebuild needed.
