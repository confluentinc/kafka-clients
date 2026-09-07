---
name: review-cext-tp-new-owned-handle
description: Audit checklist for a Python C-extension tp_new that builds and owns an FFI handle (ConsumerGroupMetadata field-constructor, PR #168 comment #2 Python half) — CLEAN
metadata:
  type: project
---

Reviewed `204f13c1` (`ConsumerGroupMetadata_new` tp_new) + `9cbfa95d` (Python gRPC
`SendOffsetsToTransaction` handler). **CLEAN, 0 findings.** Companion to
[[review-ffi-send-offsets-multilang]] (C++ half, same PR #168 comment #2).

**tp_new owned-handle audit — the 4-path table (reusable for any ext type whose
`tp_new` calls an FFI constructor returning an owned handle):**
1. `PyArg_ParseTupleAndKeywords` fails → return NULL *before* any handle/alloc. Check
   the format: `s` rejects None/non-str (good for required non-null fields); `z` maps
   None/absent → C NULL (good for nullable → `Optional::empty`); `i`→C `int`→`i32`.
2. FFI ctor returns NULL → set error, return NULL *before* `tp_alloc`. (Often dead —
   e.g. `kafka_consumer_ConsumerGroupMetadata_new` always `box_..`s — but faithful.)
3. **Ordering trap:** handle created BEFORE `tp_alloc`, so if `tp_alloc` fails the
   handle MUST be `_destroy`'d before returning NULL (else leak). This is the one to
   check first.
4. Success: `self->handle = handle` set with NO fallible step between `tp_alloc` and
   the assignment → no partially-constructed/garbage-handle object ever reachable.

**Also verify:** arg order vs the FFI decl (grep the header + `src/ffi/`); the FFI
copies strings synchronously (so borrowed `PyArg` `const char*` don't dangle);
**two creation paths, one dealloc** — a field-ctor `tp_alloc` path AND a
`PyObject_New` producer path (`py_Consumer_group_metadata`) must both set `->handle`
and both be freed by the single `tp_dealloc` (which needs an `if(handle!=NULL)`
guard + NULL-after-destroy). FFI pairing sound = `Box::into_raw` (1 alloc) ↔
`_destroy`/`Box::from_raw` (1 free, null-safe). No `tp_traverse`/GC flag needed when
the struct holds only a raw pointer (no PyObject refs → no cycle). `object.__init__`
tolerates ctor args iff `tp_new` is overridden (CPython `object_init`:
`tp_new != object_new` ⇒ no excess-args error) — so a custom `tp_new` + default
`tp_init` "just works". Non-blocking struct-builder FFI → NO `Py_BEGIN_ALLOW_THREADS`.

**gRPC harness handler parity checklist (grpc_server.py / grpc_server_async.py):**
new handler must mirror the Commit/Abort siblings: `_take_producer` is a `.get()`
LOOKUP not a pop (multi-RPC txn flow relies on this); same `producer is None`
ILLEGAL_STATE guard; translate outside the `try` (helpers don't raise for valid
proto); `except kp.KafkaError` only (non-KafkaError propagates identically); async
variant genuinely `await`s. Translate helpers (`_proto_*`) have NO Java counterpart
and NO dedicated unit test — that matches the existing harness convention (siblings
untested at unit level, covered by the Docker `multilanguage_test!` suite); don't
flag the missing unit test.

**Sentinel three-path parity (native __rust / C++ server / Python server all rebuild
the same `OffsetAndMetadata`):** flat producer `OffsetEntry` (proto `optional`
leader_epoch/metadata) → `_proto_offset_entries_to_dict`: absent epoch→None,
absent metadata→"" → `Producer._offsets_to_spec` None→-1, None→"" → C
`offsets_to_arrays` → FFI `read_offset_map` (epoch<0→None, non-null metadata→that
string). `leader_epoch=0` survives as `Some(0)` (not collapsed). Matches the C++
`has_..()? : -1/nullptr` path exactly.

**Build/run recipe (this host) — SUPERSEDES the hand-shim note in
[[review-python-bindings-callbacks]]:** the repo now VENDORS `tinycthread.h`/`.c`
(setup.py appends `tinycthread.c` on darwin), so NO hand-written `threads.h` shim is
needed. Just: `python3 -m venv`, `pip install pytest pytest-asyncio setuptools`,
then `CONFLUENT_KAFKA_LIB_DIR=<root>/target/release venv/bin/python setup.py
build_ext --inplace` (needs the prebuilt `target/release/libconfluent_kafka.dylib`),
run `PYTHONPATH=. DYLD_LIBRARY_PATH=<root>/target/release venv/bin/python -m pytest
test/unit -q`. Baseline here: 179 passed / 2 skipped. Build warns only about
`timespec_get` availability + a universal2 x86_64-slice-ignored note (dylib is
arm64-only) — both benign on arm64.
