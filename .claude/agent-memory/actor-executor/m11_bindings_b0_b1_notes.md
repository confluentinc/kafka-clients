---
name: m11-bindings-b0-b1-notes
description: M11 admin bindings slices B0+B1 — admin FFI has no access guard, finish_sync opaque-type trap, per-key result flattening, macOS cannot build the Python ext
metadata:
  type: project
---

Admin C FFI + Python bindings, slices B0 (foundation) + B1 (topic RPCs), landed
on `dev/admin-bindings`. Non-obvious findings worth carrying forward.

**Why:** the remaining slices B2–B6 repeat this exact shape 40+ more times, and
several of these traps are silent (a leak, a panic across FFI) rather than
compile errors.

**How to apply:** read before starting any further admin binding slice.

## Admin FFI is deliberately *simpler* than the consumer FFI

`Admin` is `Send + Sync` with `&self` methods, so `src/ffi/admin.rs` has no
`UnsafeCell`, no `owner: AtomicU64` guard, no `acquire`/`release`, no `wakeup()`
abort path — and needs **no** `unsafe impl Send/Sync for AdminHandle`, because
the spawned task captures a `&'static dyn Admin` (`Send`, since `Admin: Sync`)
rather than the handle. Do not copy the consumer's guard machinery.

## The trap that cost real debugging: freeing an opaque handle generically

A generic helper that takes `*mut R` where `R` is the `#[repr(C)] { _private:
[u8;0] }` marker type **cannot** free it — `Box::from_raw(handle)` drops a ZST,
leaking the inner state and deallocating with the wrong layout. The fix was to
never build the handle when the caller passes a NULL out-param. Any future
"free it if the caller doesn't want it" branch needs the concrete `*_destroy`
fn passed in, or must not allocate at all. A test that just passes NULL and
checks the return value will pass either way — the bug is silent.

## Rust-core panics reachable from the FFI

`MockAdminClient::create(0)` succeeds but its `create_topics` then panics on
`state.brokers[0]`. Java throws from `Builder.build()` instead. Before exposing
any `MockAdminClient` inherent method through the FFI, check it for
`panic!`/`assert!`/indexing — `add_topic` and `mark_topic_for_deletion` both
panic (duplicate / missing topic) and were deliberately left unexposed for that
reason. Translate Java's throw into a NULL/error return; never let a panic
unwind into C.

## Per-key results across C

`KafkaFuture::join_map` short-circuits, so `join_map_results` was added.
`CreateTopicsResult` needed a `pub(crate) futures()` accessor — its public
`values()` maps the per-key future to `KafkaFuture<Void>` and cannot carry
`TopicMetadataAndConfig`. Expect the same for any `*Result` whose public
accessors erase the payload. Feature-gate such accessors with
`#[cfg_attr(not(feature = "ffi"), allow(dead_code))]`: the crate is
`#![deny(warnings)]` and plain `cargo build` / `cargo test` run without `ffi`.

Borrowed per-key errors (`*const kafka_common_KafkaError_t` into the result's
`Vec<Option<KafkaErrorInner>>`) die with the parent handle. The Python drain
must copy code/message/retriable/fatal out **before** destroying it — hence
`KafkaError._from_parts` in `producer.py`; `_from_c` would double-free because
it destroys the handle it is given.

Result entries are sorted by key so C's index addressing is reproducible.

## Environment gotchas

- `cargo xtask lint` does **not** enable the `ffi` feature, so the FFI modules
  escape the standard lint gate. Run
  `cargo clippy --all-targets --features ffi -- -D warnings` separately.
- `_confluentkafka.c` includes `<threads.h>`, which the macOS SDK does not have,
  so **the Python extension cannot be built on this host at all**. Run the
  Python suite in a Linux container (rust:1.95-bookworm + python3-dev, rsync the
  tree excluding `target/`, `pip install --no-build-isolation -e .`).
- Host `pip` points at an authenticated CodeArtifact index that 401s; use
  `--index-url https://pypi.org/simple` or build inside Docker.
- `cmake` is not on PATH; use `nix shell nixpkgs#cmake --command cmake ...`.
  `bindings/c/tests/unity` needs `git submodule update --init` first.
- cbindgen emitted the new admin types even before they were added to
  `cbindgen.toml`'s `[export] include` (they are reachable from exported
  functions), but they were added anyway per the plan's checklist.

## Behaviour verified against Java, not assumed

`MockAdminClient.createTopics` (MockAdminClient.java:363-422 at kafka
`a18251bae0b8`) **ignores its `options` argument entirely and ignores
`replicasAssignments`**, substituting `defaultPartitions` /
`defaultReplicationFactor` for -1. Two tests initially asserted the intuitive
behaviour and failed; the Rust mock was right. Fetch the Java file before
asserting mock behaviour.
