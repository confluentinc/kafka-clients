---
name: FFI Producer patterns
description: Out-parameter null-initialization on error paths, batch partial-failure cleanup patterns in C FFI layer
type: project
---

FFI layer uses opaque handles (CProducer, CFutureRecordMetadata, CRecordMetadata) with Box::into_raw/Box::from_raw. Key pattern found: out-parameters must be explicitly null-initialized on ALL error paths, not just some.

Fixup 3166af8 added null-init on `build_record` and `producer_send` error paths, plus null-fill loops for batch partial failure. But the early null-parameter guard still skips null-init of `*out_future` when `producer` or `topic` is null. Recurring pattern: compound null-checks that short-circuit before reaching the fixed error paths. Tests that pre-initialize out-params to null hide this -- sentinel pattern catches it.

**Why:** C callers typically check out-parameters for NULL after error returns. If the function doesn't write NULL, the caller gets a stale/garbage pointer, potentially leading to use-after-free.

**How to apply:** When reviewing FFI functions with out-parameters, verify that the FIRST thing after confirming the out-parameter itself is non-null is to write NULL to it (fail-safe default), then overwrite with the real value on success. This avoids needing to null-init on every individual error path. Also verify tests use non-null sentinels rather than pre-initialization to null.

The `crate-type = ["lib", "cdylib"]` in Cargo.toml is unconditional -- always builds a shared library even without `ffi` feature. This matches the plan but adds build overhead.
