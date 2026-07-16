---
name: milestone10-phase3-panic-safety
description: Panic-safe async FFI dispatch — PanicCompletionGuard disarm-return pattern, disjoint-capture Send pitfall, teeth-proof by neutering Drop
metadata:
  type: project
---

Hardening fix (commit be6ac2e) making the share-consumer async FFI helpers
(`async_void_op` / `async_value_op` / `poll_async` in `src/ffi/share_consumer.rs`)
panic-safe. Resolves Critic COMMENTS.1 #2 (see [[milestone10-phase3-share-ffi-writepath]]).
Reusable patterns for any future async FFI dispatch (Python layer Phases 4-6):

- **The gap:** a panic (not `Err`) inside `op(...).await` on the spawned tokio task
  unwinds past all post-await code, so the single-owner guard (`Arc<AtomicU64>`) is
  never released (consumer locked forever) and no completion is enqueued (C callback
  never fires, caller hangs).

- **Precondition:** Cargo.toml is `panic = unwind` (release explicitly NOT panic=abort
  — std Mutex poisoning needs unwinding; debug/test default to unwind). Tokio catches a
  panicking task at the task boundary via internal catch_unwind — the worker thread
  survives, the process does NOT abort. During unwind, in-scope RAII `Drop`s run.

- **`PanicCompletionGuard<P>` (disarm-returns-payload):** one-shot RAII "bomb" armed
  before the `.await`. `armed: Option<(P, PanicAction<P>)>` where
  `type PanicAction<P> = Box<dyn FnOnce(P) + Send>`. On armed `drop` (only reachable via
  panic unwind) runs `on_panic(payload)`. Normal path calls `disarm(self) -> P` which
  `.take()`s the Option (returns payload, drops the boxed action WITHOUT calling it) —
  so guard released once + callback fires once, never both. Needed the payload-return
  form because value-op's `complete: FnOnce` + `ud` are move-only and consumed by
  exactly one of {panic path, normal path}; disarm hands them back. Void/poll payload =
  `()` (their `target` is `Copy`, so bomb + normal path each hold a copy).

- **Panic path mirrors each helper's normal completion** (poll reuses `PollCompletion`
  whose `fire()` releases+fires; void/value release in the enqueued job before firing) —
  release happens via the owner Arc clone (teardown-safe, never derefs the handle) and
  the error is `KafkaError::illegal_state("KafkaShareConsumer operation failed unexpectedly.")`.

- **Rust-2021 disjoint-capture Send pitfall:** a `move ||` closure that reads
  `target.user_data` captures the `*mut c_void` FIELD disjointly (→ closure `!Send`),
  not the whole `Send` wrapper. Fix: `let target = target;` as the first line forces
  whole-struct capture. (SendUserData::into_ptr doc already warns of this.)

- **clippy `type_complexity`** fires on `Option<(P, Box<dyn FnOnce(P)+Send>)>` under
  `#![deny(warnings)]` — extract the boxed-closure into a `type` alias.

- **Teeth-proof technique:** temporarily neuter the guard's `Drop` body (`if false { ... }`)
  to reproduce pre-fix behavior, run the panic tests → they must fail (callback `Timeout`
  via `recv_timeout`), then restore. Test double = `PanicConsumer` whose poll/commit_sync/
  commit_async `panic!`; probe guard-freedom with a direct `acquire(h)/release(h)`.
