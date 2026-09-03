---
name: review-ffi-send-offsets-multilang
description: M11 CFFI send_offsets_to_transaction multilang wiring — clean; parallel-array/proto sentinel-equivalence audit + multilanguage_test! backend set
metadata:
  type: project
---

Reviewed `c97e78a4` (public `kafka_consumer_ConsumerGroupMetadata_new`) + `e7771624`
(wire `send_offsets_to_transaction` through the multilang harness; CTP test →
`_inner<F>`). **Verdict CLEAN, 0 findings.** Complements [[review-ffi-async-txn-control]]
and [[review_ffi_txn_drain]] (same PR #168).

**Highest-risk pattern — parallel-array flatten across a proto boundary into a SHARED
FFI reader.** The C++ `SendOffsetsToTransaction` handler flattens `repeated OffsetEntry`
into the parallel arrays `read_offset_map` (`src/ffi/consumer.rs`) reads. Audit method
that mattered: verify the C++ sentinels against the READER's PER-ELEMENT contract, not
the outer-pointer one.
- `read_offset_map`: outer-null OR per-element `leader_epochs[i] < 0` → `None` (absent),
  `>= 0` → `Some`; outer-null OR per-element `metadata[i]==null` → `""`.
- C++ passes `has_leader_epoch() ? epoch : -1` and `has_metadata() ? c_str() : nullptr`,
  and ALWAYS passes non-null OUTER arrays (`vec.data()`), so only the per-element branches
  fire; `n==0` skips the loop so an empty-vector `data()` is never deref'd. Matches. So
  `-1` is read as "no epoch", NOT a literal epoch `-1`. No wrong-epoch / null-deref path.
- Three-path equivalence check: native builds `OffsetAndMetadata` directly; wire path uses
  the NORMALIZED getter `oam.leader_epoch()` (negative→None) so a raw negative can't leak,
  and `metadata: Some(oam.metadata())` (always present). Rebuilt == original under the
  type's `Eq`. **proto3 `optional` presence-tracking makes it robust either way**: `Some(0)`
  survives as `Some(0)` (0 is a valid epoch, not collapsed), and empty metadata lands as
  `""` whether prost emits `Some("")` or elides it (both server branches yield `""`).

**Harness fact — `multilanguage_test!` (`tests/common/multilanguage_test_macro.rs`)
generates exactly FOUR targets: `__rust` (native `RustNativeFactory`, no container/gRPC),
`__grpc_python`, `__grpc_python_async`, `__grpc_c`. There is NO `__grpc_rust`.** So a new
producer RPC needs a handler ONLY in `bindings/c/grpc_server/server.cc` (C++); the "rust"
backend is native. Python servers lack txn handlers → `__grpc_python*` fail UNIMPLEMENTED
by design (PR #175) — NOT a regression. Corollary: **tonic server-trait methods are
required**, so "the `multilanguage-test-server` crate compiled clean" is proof no Rust
`ProducerService` server impl was left stale by the new RPC (there is none — grep confirms).

**Other reusable checks (all passed here):** cbindgen `[export] include` holds ONLY `_t`
types + `_callback_t`/`_user_data_destroy_t` typedefs — `#[no_mangle]` fns auto-export, so
a new FFI fn is NOT added there (justified deviation, verify it's in
`target/include/confluent_kafka.h`). C++ error path: `fill_proto_error` FREES the `err`
handle (`server.cc:153`); the new handler `_destroy`s `group_meta` unconditionally before
the error check (only early return is `producer==nullptr`, before `_new`). `ConsumerGroupMetadata`
has exactly 4 fields, all set by `with_details` — no dropped field.
