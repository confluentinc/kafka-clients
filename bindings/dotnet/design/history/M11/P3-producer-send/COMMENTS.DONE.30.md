# COMMENTS.DONE.30 — resolved dotnet-critic items (M11/P3 producer SEND path)

**Original commit reviewed:** `23c3ec66` — `dotnet(M11/P3): producer SEND path`
**Fixup commit:** see `git log` (fixup! referencing `23c3ec66`)
**Actor:** dotnet-actor (N=30)

---

## Issue 1 — [MEDIUM] Teardown joined the pump before graceful close → manual-mock `Dispose` hung forever — RESOLVED

**Fix (Mode-A reorder, flush-before-join):** `NativeProducer.StopPump` now runs `Producer_flush`
**before** `_thread.Join()` (and before `Producer_close`), so the completion pump's blocking
`FutureRecordMetadata_get_all` is unblocked before the join.

**Close-semantics investigation (read-only, per the Manager's charge):** I read the Rust close path.
The finding branches the fix:

- `src/ffi/producer.rs:1945-1971` `Producer_close` → `rt.block_on(mock.close())` /
  `kafka.close()`.
- `src/producer/mock_producer.rs:374-383` `MockProducer::close()` **only sets `inner.closed = true`
  and returns `Ok`** — it does **NOT** touch `inner.completions`. So **`Producer_close` does NOT
  resolve pending sends** → the Manager's literal option-(a) ("close resolves the in-flight
  `get_all`") does **not** hold, and reordering around `close` alone would still hang.
- **BUT** `src/producer/mock_producer.rs:346-362` `MockProducer::flush()` **drains
  `inner.completions` and calls `completion.complete(None)` on each**, and the `completion`'s
  `Arc<ProduceRequestResult>` is the **same** one the future awaits
  (`send_with_callback` line 318-335 shares `result` between the future and the completion). So
  **`Producer_flush` DOES resolve pending sends** → the in-flight `get_all` returns.

So this is **NOT** the STOP case: there IS a clean Mode-A way to make `get_all` return
(`Producer_flush`, an existing ABI at `confluent_kafka.h:2792`), it just is not `close`. Flushing
pending records on teardown is exactly Java's `close()` semantics (Java `close()` flushes then
closes; the Rust `Producer_close` does not flush, so the flush is made explicit here). For a real
producer `flush` delivers-or-times-out — the **accepted Option-C bounded residual** the Critic
already declined to file (Java's `close()` blocks on `flush()` too), unchanged by this fix.

**Recorded deviation:** for a `MockProducer(autoComplete:false)` with an uncompleted send, the
teardown flush completes that send (its `Task` resolves) rather than leaving it incomplete as Java's
`MockProducer.close()` does. This is **forced by the pull-pump architecture** (the pump must resolve
every future or the join hangs) and is strictly better than the previous forever-hang; it is
Java-faithful for the real producer (`close` flushes). Documented in `StopPump` / `SendCompletionPump`
docstrings.

**Docstring corrected:** the `SendCompletionPump` "for a real producer the graceful close/flush
drives it" claim (architecturally impossible under join-before-close) is replaced with the accurate
"the in-flight `get_all` is unblocked by a flush, NOT by close" rationale.

**Regression tests added** (`PublicProducerSendTests`): `Dispose_WithUncompletedManualSendInFlight_Returns`,
`DisposeAsync_WithUncompletedManualSendInFlight_Returns`, `Close_WithUncompletedManualSendInFlight_Returns`,
`Dispose_WithManyUncompletedManualSendsInFlight_Returns` — each fires an uncompleted
`autoComplete:false` send and asserts teardown returns under the hang guard (and the send `Task`
settled).

**Files:** `Internal/NativeProducer.cs` (`StopPump`), `Internal/SendCompletionPump.cs` (docstring),
`Internal/Interop/NativeMethods.cs` (new sync `ProducerFlush` DllImport — Mode A, wired only by
`StopPump`).

---

## Issue 2 — [LOW] `NativeProducer.Send` called `Producer_send` on a raw handle without a span-the-op ref → destroy-vs-in-flight-send UAF — RESOLVED

**Fix:** `Send` now takes `_handle.DangerousAddRef(ref handleRefAdded)` before the
`ProducerSendMarshal.Send` (`Producer_send`) P/Invoke and `DangerousRelease()` in a `finally`,
mirroring the peripherals (`SubmitVoidOperation` / `CloseWithCallbackInternal`). The ref spans only
the `Producer_send` call (the native borrow is call-scoped — the core copies key/value during the
call, ffi §A4), so `ReleaseHandle → Producer_destroy` cannot drop the runtime out from under an
in-flight send. Closes the multi-writer `Send`-vs-`Dispose` window.

**Regression test added** (`PublicProducerSendTests.ConcurrentSendAndDispose_DoesNotCrash`): churns a
producer whose sends race its own `Dispose` across threads under GC pressure; a racing send may throw
`ObjectDisposedException` (rejected after the latch) but must never crash.

**Files:** `Internal/NativeProducer.cs` (`Send`).

---

## Post-review refinement (user-approved, round-2 review clean — NOT a COMMENTS fix) — async teardown flush

The Issue-1 fix flushed via the blocking sync `Producer_flush` in `StopPump`, which was called by
`Dispose` (sync — fine) AND by `DisposeAsync` / `CloseWithCallback` (async) — a blocking sync flush
on an async path is sync-over-async (ffi §A7). Refinement: `StopPump` split into a shared
`PumpToStop()` reader + the sync `StopPump()` (Dispose — sync `Producer_flush`) + a new
`StopPumpAsync()` (DisposeAsync / CloseWithCallback — `await`s `Producer_flush_async` via the new
latch-free `FlushInternal` bridge, twin of `CloseWithCallbackInternal`). Both share the same
join+destroy tail (`pump.Stop()`).

**Scope (deliberate):** ONLY the flush swaps to async on the async paths. The pump join
(`_thread.Join()`) and `Producer_destroy` STAY BLOCKING — making them awaitable is out of scope
(over-engineering). Sync `Dispose` unchanged. No new public API, no new DllImport (reuses the
already-wired `Producer_flush_async`). Issue-1's no-hang property holds on the async path: the async
flush runs BEFORE the join, resolving pending sends so the pump's blocking `get_all` returns. Commit
`14d83dc8` (fixup).

---

## Post-close refinement (user-approved, round-3 review clean — NOT a COMMENTS fix) — sync→SafeHandle-param convention

Adopted the **sync-native-call → `SafeHandle`-param** convention, starting with producer `Send`.
`NativeMethods.ProducerSend`'s `producer` param changed `IntPtr` → `SafeProducerHandle` (single
caller confirmed: only `ProducerSendMarshal.Send` ← only `NativeProducer.Send`), so the P/Invoke
marshaler auto-`DangerousAddRef`/`Release`s it around the synchronous `Producer_send` — replacing the
manual span-the-call `DangerousAddRef` added for Issue 2 in `2560bdc7`. `NativeProducer.Send` drops
the manual ref and passes `_handle` straight through; `ProducerSendMarshal.Send` threads the
`SafeProducerHandle` through (the `fixed` key/value pins + absent/empty/present sentinels + the
`out_error` handling all unchanged inside that `unsafe` helper). Post-`Dispose` `Send` still throws
`ObjectDisposedException` (via `ThrowIfClosed` + SafeHandle-marshals-closed-to-ODE); the
concurrent-`Send`-vs-`Dispose` churn test still passes.

The principled split is now documented in `ffi-marshalling.md §A2`: **sync = pass the `SafeHandle`
(auto ref, call-scoped — exactly right for an op whose native use ends when the call returns) /
async `*_async` = manual `DangerousAddRef` held submit→callback** (the auto marshaler releases its
ref *before* the completion callback fires, so async can never use the auto form —
`SubmitVoidOperation` / `CloseWithCallbackInternal` / `FlushInternal` keep the manual span-the-op
ref). `Send` is the first adopter. **No new public surface, no new DllImport** (this modifies an
existing DllImport's signature). Mode A (edits under `bindings/dotnet/{src,.claude/rules,design}`;
no `src/ffi`/header/`cbindgen` delta).

**Follow-up task (tracked here as the in-repo copy; also pinned in Manager session memory):** migrate
the consumer **sync-op** DllImports to a `SafeConsumerHandle` parameter to match — `ConsumerSeek`,
`ConsumerSeekWithMetadata`, `ConsumerCurrentLag`, `ConsumerEnforceRebalance`, `ConsumerCommitAsync`,
and the sync state reads / poll / commit / query family (`ConsumerAssignment` / `Subscription` /
`Paused` / `ConsumerPoll` / `ConsumerCommitSync` / `Committed` / `OffsetsForTimes` /
`BeginningOffsets` / `EndOffsets` / `PartitionsFor` / `ListTopics`). Leave the consumer `*_async` ops
on manual `DangerousAddRef` (they can never use the auto ref). This retires the consumer's
"accepted single-owner residual" TOCTOU on those sync ops (`NativeConsumer` class remark).

---

## Round-2 deferred observation — RESOLVED (verified safe by core inspection)

The round-2 review surfaced (but did NOT file) a low-confidence observation: on the
`Enqueue`-races-`Stop` path, a send whose thread stalls between `Producer_send` returning and
`pump.Enqueue` could — if a concurrent `Dispose` fully tears down first — reach the `_stopped`
branch and call `FutureRecordMetadata_destroy_all` on its future **after** `Producer_destroy` has
run. It was left "deferred / needs kafka-critic confirmation" (Rust-core lifetime, outside
dotnet-critic scope). A core-code investigation now **resolves it as memory-safe** — no
kafka-critic routing needed:

- `FfiFuture` owns its `KafkaFuture<RecordMetadata>` **by value** (`src/ffi/producer.rs:121-128`);
  `KafkaFuture<T>` is **Arc-backed** (`src/common/kafka_future.rs:65-66`; state
  `Arc<Completable<T>>` :355); the send future's completion state is an `Arc<ProduceRequestResult>`
  **shared as Arc clones** between the future and the completion (`src/producer/mock_producer.rs:318`).
- `FutureRecordMetadata_destroy_all` is **drop-only** (`src/ffi/producer.rs:1662-1673` — no runtime
  use), so it touches **no producer memory**; and `Producer_destroy` **explicitly tolerates
  outstanding futures** (`:863-868`, "detach the dispatcher… must not deadlock").
- ⇒ Destroying a future after `Producer_destroy` merely **decrements the future's own Arc** — memory
  safe. Empirically, `ConcurrentSendAndDispose_DoesNotCrash` passes under churn + GC pressure.

**Deliberate cross-binding divergence, both safe:** Python never destroys a future after
`Producer_destroy` — it front-gates (`closed=1`) and drains/destroys all futures **before** destroy
(`_confluentkafka.c:825`, `:514-515`, `:832`). .NET Option C instead relies on the core's
Arc-independence (the future outlives the producer safely). Both are correct; the difference is
mechanism, not safety.

**Ordering-parity option (recorded, NOT recommended):** a minimal change *could* make .NET match
Python's "never destroy after destroy" ordering — widen `Send`'s span-the-op producer ref to span
through `pump.Enqueue` (not just the `Producer_send` call). It is **not recommended**: **zero safety
gain** (the core is already Arc-independent), and it is **mutually exclusive with the
`SafeProducerHandle`-param cleanup** above — it would require reverting `Send` to an `IntPtr`
producer param + a manual `DangerousAddRef`/`Release`, undoing the sync→SafeHandle-param adoption.
The full snippet lives in the Manager's session memory; only the pointer + the tradeoff are recorded
here.

---

## DoD after fix

`cargo build --features ffi` — no header delta (Mode A; header hash unchanged). `dotnet build`
0W/0E across net462 · net8.0 · net10.0. `dotnet test` net10.0 — 507 passed / 0 failed (net8.0 +
net462 build-verified). `dotnet format --verify-no-changes` clean. No TODO/FIXME.
