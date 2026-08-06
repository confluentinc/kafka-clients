---
name: ffi-callback-bridging-phase2
description: FFI callback-bridging Phase 2 — producer delivery-callback semantics (both handles non-null on API failure, callback dropped unfired on Err), C-only test convention for callback FFI
metadata:
  type: project
---

Phase 2 of the FFI/Python callback-bridging plan (`~/.claude/plans/wiggly-bouncing-babbage.md`)
landed as `d6b6674` + fixup `4bc1b7b` on `ffi-callback-bridging`:
`kafka_producer_Producer_send_with_callback` (future AND callback, Java's
`send(record, Callback)` shape).

**Why:** the C surface previously forced a choice between the future
(`Producer_send`) and the callback (`Producer_send_async`).

**How to apply** — three findings that generalize to the later callback phases (3, 5, 6):

1. **The core producer's `Callback` can be invoked with BOTH arguments non-null.**
   `KafkaProducer::handle_api_exception` fires `cb(Some(&null_metadata),
   Some(&error))` — a placeholder `RecordMetadata` (offset/partition `-1`) plus
   the error — and then returns `Ok(failed_future)`, not `Err`. This is
   Java-faithful (`KafkaProducer.send` does
   `callback.onCompletion(nullMetadata, e)`). `MockProducer::Completion::complete`
   is strictly one-or-the-other, so C/Python tests driven by the mock never
   exercise it. Any binding that documents "the other argument is null" (the
   pre-existing `send_async` doc did) is wrong; adapters must test `error` first
   and free every non-null handle. Python's `on_delivery` in Phase 6 must not
   assume `metadata is None` implies success.

2. **`Err` from the core send never fires the callback; the `Callback` is just
   dropped.** Checked every path: `ensure_not_closed`, non-API `wait_on_metadata`
   errors, and `RecordAccumulator::append` errors all drop it unfired. That makes
   the "on synchronous failure `out_error` is set and the callback is NOT invoked"
   FFI contract accurate. **Pre-existing core gap (not FFI, out of Phase 2 scope):**
   `do_send_bytes`'s `Err(e) if e.is_api_exception()` arm after `append` returns
   `Ok(failed_future)` while the callback (already moved into `append`) is dropped
   unfired — a CLAUDE.md §9.5 callback-obligation divergence that also affects the
   existing `send_async`. Worth a separate fix/report, not a Phase-2 edit.

3. **Callback-based producer FFI functions are tested in C only** — there are
   ~57 Rust `#[cfg(test)]` tests in `src/ffi/producer.rs` but zero touch any
   `*_async` entry point. Follow that split: sync/validation surface gets Rust
   unit tests, dispatcher-thread callback behavior gets Unity tests in
   `bindings/c/tests/`. Don't add a Rust test that spins on a dispatcher thread.

Also: reusing an existing `*_callback_t` typedef for a new function (instead of
adding an identically-shaped alias per CLAUDE.md §3) is acceptable, but say so in
the typedef's rustdoc — the file already has that precedent for
`kafka_producer_Producer_partitions_for_callback_t`.

Environment notes from Phase 1 (no cmake, no venv, `cargo xtask lint` skips the
`ffi` feature) all still held — see [[ffi-callback-bridging-phase1]].
