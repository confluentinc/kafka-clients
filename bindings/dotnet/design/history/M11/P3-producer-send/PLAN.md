# M11 / Phase 3 (P3) — .NET producer SEND path — DRAFT PLAN (for review)

**Status: DRAFT — plan only, not approved, no code, no agents spawned.**
This file is written to `design/current/` for the user's review. On approval it is
archived to `design/history/M11/P3-producer-send/PLAN.md` (per the root
`project-manager` mechanics, CLAUDE.md §8.4) and the Actor/Critic loop begins.

---

## 1 · Scope, branch, roadmap position, agent number

- **Phase:** M11/P3 — the **producer SEND path**. In the producer roadmap laid out in
  M11/P1 this is **"Phase C"** (A foundation → B async peripherals → **C send** →
  D sync producer). The binding keeps its own milestone/phase numbering, independent
  of the root Rust `design/` (CLAUDE.md §8.4).
- **Branch:** `prashah_dev_producer_send`, stacked on
  `prashah_dev_producer_async_peripherals` (the M11/P2 + P2.1 branch, HEAD carries the
  collapsed `NativeProducer` teardown). Already checked out.
- **Agent number for the later loop: `N = 30`** (the binding's own sequence — N=27 P1,
  N=28 P2, N=29 P2.1, **N=30 P3**). *Noted here, not run.* `COMMENTS.30.md` /
  `COMMENTS.DONE.30.md` are the working files once the loop starts.
- **Mode:** **Mode A only** — binding-only over the existing C ABI. Hard invariant:
  **no** change to `src/**`, `src/ffi/**`, `target/include/confluent_kafka.h`, or
  `cbindgen.toml`. Verified Mode-A-sufficient (see §8). Any missing ABI feature must be
  flagged as a Mode B / Rust-core dependency, **not** invented.

**What Phase C delivers (public, namespace `Confluent.Kafka`, additive to P2):**

- `IAsyncProducer.Send(ProducerRecord, CancellationToken = default) -> Task<RecordMetadata>`
  — grows the interface additively (P2 shipped `Flush`/`Close`/`PartitionsFor`).
- `ProducerRecord` value type (clipped to today's ABI — no `Headers`).
- `RecordMetadata` value type (clipped — no serialized-size / `has*` accessors).
- `MockProducer` send-control helpers, **inherent on the concrete type** (not on
  `IAsyncProducer`): `CompleteNext()`, `ErrorNext(int, string?)`, `HistoryCount`, `Clear()`.
- Bytes-only interim (`ReadOnlyMemory<byte>?`); the typed generic producer stays deferred.

**Internal:** the send-completion machinery (the chosen option — §3),
`NativeProducer.Send` (the internal send worker — §6.2 explains why it takes **no**
`WithCallback` suffix), the `NativeMethods` send /
completion / metadata / mock DllImports, and integration of the completion-teardown into
`NativeProducer`'s existing P2.1-collapsed `Dispose`/`DisposeAsync`.

---

## 2 · Parity anchor (mandatory — the guardrail from the M11/P2 retro)

> **Why this section leads.** M11/P2 + P2.1 needed a whole cleanup phase and fixups
> because a green build + green tests + a clean Critic still missed invented public
> surface, a divergence from the consumer precedent, and dead code. The saved feedback
> is explicit: *pin a parity anchor up front and review against it.* The Critic for N=30
> reviews the delivered surface **against (a) below** and flags anything beyond it.

### (a) The Python-sibling send surface — the CEILING on public surface

Source of truth: `bindings/python/producer.py` + `bindings/python/_confluentkafka.c`
(verified while planning).

- **`ProducerRecord`** — a C-extension type (`_confluentkafka.c:57-216`). Constructor
  kwlist `{"topic", "value", "key", "partition", "timestamp"}`, arg format `"sO|OiL"`:
  `topic` (str, **required**), `value` (object, required positionally, **nullable**),
  then optional `key` (object), `partition` (`int32`), `timestamp` (`int64`). Getters:
  `topic`, `partition`, `timestamp`, `key`, `value`. **No headers getter.**
- **`send(producer_record) -> Future[RecordMetadata]`** (sync `Producer`) /
  `async send(...) -> asyncio.Future[RecordMetadata]` (async `AsyncProducer`). Returns a
  future; the result is a `RecordMetadata`, a failure raises `KafkaError`.
- **`RecordMetadata`** (`producer.py:48-92`) — `offset()`, `topic()`, `partition()`,
  `timestamp()` (lazily populated via `RecordMetadata_copy`). **No** serialized-size /
  `has*` accessors.
- **`MockProducer` helpers** (`_MockProducerMixin`, `producer.py:177-208`):
  `complete_next()` → bool, `error_next(error_code, error_message=None)` → bool,
  `history_count()` → int, `clear()`. (Java `MockProducer.history()` returns the record
  list; the ABI exposes only a count → `history_count()` — hence .NET `HistoryCount`.)

**Mapping to the .NET surface** (the CLAUDE.md §3 sketch, matched to (a)):

| Python sibling | .NET P3 surface |
|---|---|
| `ProducerRecord(topic, value, key=None, partition, timestamp)` | `ProducerRecord(string topic, ReadOnlyMemory<byte>? value, ReadOnlyMemory<byte>? key = null, int? partition = null, long? timestamp = null)` |
| `RecordMetadata.offset/topic/partition/timestamp()` | props `Topic`/`Partition`/`Offset`/`Timestamp` (getter → property idiom) |
| `Producer.send(rec) -> Future[RecordMetadata]` | `IAsyncProducer.Send(ProducerRecord, CancellationToken) -> Task<RecordMetadata>` |
| `complete_next()` / `error_next(code, msg?)` / `history_count()` / `clear()` | `CompleteNext()` / `ErrorNext(int, string?)` / `HistoryCount` / `Clear()` (inherent on `MockProducer`) |

### (b) The in-repo precedents to mirror (no reinvention)

- **P2.1 `NativeProducer` teardown shape** — one layer, three flavors
  (`Dispose` sync-close→destroy swallow, `DisposeAsync` async-close→destroy swallow,
  `CloseWithCallback` async-close→destroy surface), one atomic `_closed` latch, the
  span-the-op `SafeProducerHandle` ref. Mirrors `NativeConsumer`.
- **`OperationCompletionSource<TResult>`** (`Internal/OperationCompletionSource.cs`) —
  already producer-ready: `RunContinuationsAsynchronously`, `SetHandleRef` (span-the-op),
  `RegisterCancellation` + `CancelAwaiter` (the **no-abort producer** path), `FreeGcHandle`
  (exactly-once). Reuse verbatim where the chosen option needs a per-op callback bridge.
- **`ProducerCallbacks`** (`Internal/Interop/ProducerCallbacks.cs`) — the kept-alive Cdecl
  delegate + no-throw boundary + copy-out-on-dispatcher template (§A6/§B6).
- **`Utf8Marshal.Pin`** (topic in), the flat `KafkaException.FromHandle` error model (§A5),
  the `SafeProducerHandle` ownership pattern (§A2).
- **ffi-marshalling.md Part A §A1–§A7** — the producer boundary contracts, esp. **§A4**
  (call-scoped send pin) and **§A7** (the pull-vs-push completion decision this phase makes).

### (c) NOT-adding list (explicit — the Critic flags any of these appearing)

- **`Headers`** on `ProducerRecord` — the ABI's `ProducerRecord_t` has no headers field.
- **Typed generic producer** (`Producer<K,V>` / `ISerializer<T>` wiring on send) — deferred;
  bytes-only interim this phase.
- **Serialized-size / `has*`** accessors on `RecordMetadata` — not in the ABI.
- **Transactions / metrics / `clientInstanceId`** — Mode B, deferred.
- **`Close(TimeSpan)`** — dropped in P2.1 (Python producer close has no timeout); do not
  reintroduce.
- **`send_batch` / `send_batch_async` machinery + the `ProducerRecord_t` blittable mirror
  struct** — needed **only** if Option A is chosen (it is not — §3). Under Option C the
  singular `Producer_send` is used and no mirror struct is added.
- **A managed bound in front of the accumulator** (the Python `on_space_available` /
  `record_batches` bound) — Python needs it because its C extension batches; Option C sends
  inline so the core's `buffer.memory` backpressure applies directly (§3, decision 4).

**Critic charge for N=30 (state in the review brief):** flag (1) any public surface not in
(a); (2) any divergence from the consumer/producer precedent (b) without a written
rationale; (3) any dead / unused DllImport; (4) §A4/§A7 memory-safety anti-patterns.

**Recorded execution-time parity deviation (added at close, N=30 — Phase-D / typed-producer
discoverability):** the Option-C pull-pump teardown **flushes** pending sends before joining the
pump, so for a `MockProducer(autoComplete:false)` with an uncompleted in-flight send the teardown
**completes** that send — whereas Java's `MockProducer.close()` leaves it incomplete. It is forced
by the pull-pump (the pump must resolve every future or the join hangs), is Java-faithful for the
*real* producer (Java `close()` flushes), and is strictly better than a forever-hang. Full
rationale in `COMMENTS.DONE.30.md` (Issue 1). Phase D (sync producer) and the typed producer
should carry this deviation forward.

---

## 3 · The central decision — §A7 completion model (A / B / C)

The producer ABI exposes **both** a pull surface (`Producer_send` → a future you block /
poll via `get` / `get_all` / `is_done`) and a push surface (`Producer_send_async(…,
callback, user_data)` fired on the core's dispatcher thread). So the completion model is a
**binding choice**, not ABI-forced (ffi §A7 — OPEN). Three options; all bridge to
`Task<RecordMetadata>` via a `TaskCompletionSource` built with
`RunContinuationsAsynchronously`.

### 3.1 Flow diagrams

**Option A — Python-style pull** (1 batch thread + 1 poll thread, `Producer_send_batch` +
`FutureRecordMetadata_get_all`; k/v pinned via long-lived `GCHandle.Alloc(Pinned)` across
the caller→batch-thread handoff; the core copies inside `send_batch`; TWO managed queues,
Python-style 2-mutex/2-condvar):

```
 caller thread(s)         │ batch thread              │ poll thread            │ C ABI / Rust core
 ────────────────         │ ─────────────             │ ───────────            │ ──────────────────
 Send(rec):               │                           │                        │
  GCHandle.Alloc(Pinned)  │                           │                        │
   k/v (LONG pin ─────────┼─► record_batches Q        │                        │
   across handoff)        │   (pin still held)        │                        │
  new TCS; return Task    │  drain N → ProducerRecord_t[]                       │
                          │  ─────────────────────────┼────────────────────────┼─► Producer_send_batch(N)
                          │  (core COPIES k/v here)    │                        │    → out_futures[N]
                          │  unpin the N GCHandles     │                        │    RecordAccumulator.append
                          │  push (futures,TCS[]) ─────┼─► pending_batches Q     │
                          │                            │  get_all(futures) ─────┼─► BLOCKS → md[]/err[]
                          │                            │  TCS[i].SetResult/Exc   │
                          │                            │  destroy_all(futures)   │
 Dispose: signal + join BOTH threads ─────────────────┴────────────────────────┘
```

**Option B — push callback** (0 managed threads, `Producer_send_async`; the callback fires
on the core's native dispatcher thread; reuses the consumer's proven callback→TCS template
§A6/§B7; **but** `send_async` **borrows** k/v **until the callback fires**, so the pin is a
long-lived per-send `GCHandle.Alloc(Pinned)` freed on every completion path; no managed
queue):

```
 caller thread(s)             │ C ABI                     │ Rust core (dispatcher thread, native)
 ────────────────            │ ──────                    │ ─────────────────────────────────────
 Send(rec):                   │                           │
  GCHandle.Alloc(Pinned) k/v  │                           │
   (per-send pin, held ───────┼─► Producer_send_async(    │  submission task BORROWS k/v
    UNTIL callback fires)     │      …, cb, userData) ────┼─► RecordAccumulator.append (later; copy here)
  GCHandle(TCS) rooted        │      + out_error (sync)    │       …network round-trip…
  return Task (send_async     │  (does NOT block caller)   │
   is non-blocking)           │                           │
       ◄── cb(md*, err*, ud) ─┼───────────────────────────┤  fires on dispatcher (foreign) thread
  (on foreign thread):        │                           │
    TCS.SetResult/Exception   │                           │
    free k/v pin + TCS GCH    │                           │
    destroy md* / err*        │                           │
 Dispose: reconcile N pending callbacks + N pins on the foreign thread (§A2 parent-outlives-children)
```

*Why not a `SafeHandle` for key/value instead of pinning?* It cannot substitute for the pin,
and its only working variant is B's other horn:

- **`SafeHandle` and pinning solve different problems.** Pinning keeps a **managed** `byte[]`
  from being **moved by the GC** (so a raw pointer stays valid); `SafeHandle` manages the
  **lifetime of an unmanaged handle** and does nothing about GC movement. A `SafeHandle`
  wrapped around the user's *managed* bytes cannot stop them from moving — that is a category
  error.
- **The version that does work reintroduces a copy.** Copy key+value into **unmanaged** memory
  (`AllocHGlobal` / `NativeMemory.Alloc`), wrap that in a `SafeHandle` (or a plain `IntPtr`
  freed in the callback), pass its pointer to `send_async`, free on callback. That avoids the
  GC pin and its fragmentation — **but reintroduces a per-send copy of key+value**, exactly the
  zero-copy violation §A4 / CLAUDE.md §12 forbids on the send path. So it is **B's other horn
  (copy vs pin), not a third way out.**
- **There is no no-copy-and-no-pin option** for managed user bytes that native code holds until
  the callback: pinning is the only way to fix managed memory in place, so skipping the pin
  *requires* copying to memory that does not move.
- *(Footnote — §A2.)* Even the copy-to-unmanaged variant would **not** use a *per-message*
  `SafeHandle`: §A2 says don't wrap per-message transients in a `SafeHandle` (a finalizable
  object per record is hot-path waste); you would `AllocHGlobal` + free in the callback's
  `finally`.
- **This is precisely why Option C avoids both costs:** the ABI's `Producer_send` copies
  key/value *synchronously during the call*, so C needs **neither** a long pin **nor** a
  binding-side copy — a call-scoped `fixed` suffices and the core does the copy for free inside
  the call. B cannot get this because `send_async` defers the copy to after the call returns.

**Option C — inline pull-pump (OUR PREFERENCE)** (1 pump thread, `Producer_send` singular
inline on the caller + batched `get_all` on the pump; k/v pinned **only until the P/Invoke
returns** via `fixed`, because the core copies during the call; ONE managed MPSC completion
queue):

```
 caller thread(s)             │ C ABI / Rust core                     │ pump thread (1 managed)
 ────────────────            │ ──────────────────                    │ ───────────────────────
 Send(rec):                   │                                       │
  fixed(k,v) ── CALL-SCOPED ──┼─► Producer_send(…, out_error) ───────►│
   pin (this frame only)      │     block_on(send()) COPIES k/v       │
  unpin at end of fixed  ◄────┼──── returns future handle             │
  new TCS; enqueue            │   (buffer.memory backpressure: send    │  loop:
   (future, TCS) on ──────────┼──── BLOCKS caller ≤ max.block.ms       │   drain a batch of (future,TCS)
   ConcurrentQueue + signal   │      when the core buffer is full)     │   get_all(futures[]) ──► BLOCKS
  return Task                 │                                       │   per i: read md accessors
                              │                                       │     (or RecordMetadata_copy)
                              │                                       │     TCS[i].SetResult/Exception
                              │                                       │     (RunContinuationsAsynchronously)
                              │                                       │   destroy_all(futures)
 Dispose: stop sends → drain/fault pending → join pump → flush/close → Producer_destroy
```

### 3.2 Comparison table

| Axis | A — Python-style pull | B — push callback | C — inline pull-pump (PREFERRED) |
|---|---|---|---|
| §A7 model | pull | push | pull |
| Managed threads | **2** (batch + poll) | **0** | **1** (pump) |
| Uses core's native dispatcher | no | **yes** | no |
| Send FFI fn | `Producer_send_batch` (N/call) | `Producer_send_async` (1/call) | `Producer_send` (singular, inline) |
| Completion mechanism | pull `get_all` (batched) | push per-send callback | pull `get_all` (batched) |
| FFI crossings / record | ≪1 send (batched) + batched `get_all` | ~2 (1 send_async + 1 callback), no batching | 1 send + batched `get_all` (+ ≤4 accessor reads or 1 `RecordMetadata_copy`) |
| Producer-side lock | serialized on the coarse `Mutex<ProducerKind>` (append) — send runs on the **batch thread** | same coarse mutex — send runs on the **submission task** | same coarse mutex — callers serialize on it **inline** during `Producer_send`. **All three share the identical serialized-append ceiling** (the mutex is Mode B; the binding cannot change it — verified `src/ffi/producer.rs:965`) |
| Where the byte copy happens | inside `send_batch` (batch thread, during the call) | **asynchronously** on the submission task (after `send_async` returns) | **synchronously** during `Producer_send` (`block_on(send)`→append; verified L262–281) |
| Pin mechanism | `GCHandle.Alloc(Pinned)` | `GCHandle.Alloc(Pinned)` per send | **`fixed`** (stack-scoped) |
| Pin window | long — Send → `send_batch` copy (cross-thread) | **longest** — Send → callback fires (completion) | **call-scoped** — the `Producer_send` P/Invoke frame only |
| Spans a GC? | yes (cross-thread handoff) | yes (until completion) | **no** (frame-local) |
| Heap-fragmentation risk | moderate (N held pins) | **high** (per-send pins held until completion; unbounded in flight → §backpressure) | **negligible** (call-scoped) |
| `RunContinuationsAsynchronously` | required (pump completes TCS) | required (foreign thread completes TCS) | required (pump completes TCS) |
| §A6 per-send Cdecl callback | no (pull) | **yes** (send_callback + rooting + no-throw per send) | no per-send callback (optional sync `RecordMetadata_copy` only) |
| Backpressure source | managed queue bound **+** core `buffer.memory` (on batch thread) | **none on the caller** — `send_async` is non-blocking → unbounded in-flight + unbounded pins | **core `buffer.memory`** directly (`Producer_send` blocks ≤ `max.block.ms`) → queue practically bounded |
| Wire batching | core (`RecordAccumulator`) | core | core |
| **New code to write** | **HIGH** — 2 threads, 2 queues (2 mutex/condvar), `ProducerRecord_t` mirror struct, long-pin bookkeeping | **LOW** — reuses the shipped callback→TCS template; add `send_async` DllImport + delegate + per-send pin/TCS GCHandle | **MEDIUM** — new pump + 1 MPSC queue + `send`/`get_all`/`destroy_all`/metadata DllImports + `fixed` pinning |
| **Correctness / memory-safety hazard surface** (the Critic's lens) | **HIGH** | **HIGH** | **LOW** |
| In-repo precedent | Python `_confluentkafka.c` batch+poll threads (C-ext, not a shipped .NET pattern) | shipped consumer push bridge (§B7) + shipped producer peripherals over `OperationCompletionSource` | ffi §A7 Option A (documented); Python poll-thread is the conceptual sibling |

### 3.3 Preference: Option C — rationale

We recommend **Option C**, and record **B** and **A** as documented alternatives.

1. **Call-scoped `fixed` pinning is trivially, provably correct.** The core copies k/v
   **synchronously** during `Producer_send` (`src/ffi/producer.rs` L262–281:
   `rt.block_on(producer.send(record, None))` → `RecordAccumulator::append` →
   `DefaultRecord::write_to`'s `out.write_all(k/v)`; and the header Safety clause requires
   the buffers valid only "for `key_len`/`value_len` bytes" — with **no** "until callback"
   language). So the pin can end at the frame: it can't leak, can't fragment, can't span a
   GC. **B's `send_async` borrows k/v *until the callback fires*** (header, verbatim: the
   buffers "are **not** copied; they are borrowed by the submission task. The caller **must
   keep them valid until `callback` fires**"). That forces B into a lose-lose: either a
   **long fragmenting per-send pin** held until completion, or a **per-send copy** that
   violates the §A4 / CLAUDE.md §12 zero-copy contract. **That asymmetry is the crux.**
2. **Completion runs on *our* pump thread, not a foreign dispatcher.** No per-send
   "managed exception must not unwind into native" tightrope, and no per-send dual-`GCHandle`
   (pin + TCS) that must be freed on every completion path. B carries both, per send.
3. **The producer push is many-in-flight.** Unbounded concurrent sends are the norm, whereas
   the shipped consumer callback precedent we would "reuse" for B is **single-op-in-flight**.
   B's teardown must therefore reconcile **N pending callbacks + N pins on a foreign thread**
   (the §A2 "parent must outlive children" hazard) — genuinely harder than the consumer
   precedent covers. **C's teardown is deterministic:** stop sends → drain/fault pending →
   **signal + join the pump** → flush/close → destroy.
4. **B has no caller backpressure.** `send_async` returns without blocking (header), so a
   fast producer accumulates unbounded in-flight sends, each holding a long-lived pin → memory
   blow-up + heap fragmentation. **C** rides the core's `buffer.memory` backpressure directly
   (`Producer_send` blocks the caller ≤ `max.block.ms` when the buffer is full), which
   naturally bounds the completion queue.
5. **Honest counter-point (recorded).** **B writes the least new code** — it reuses the
   shipped callback bridge; C writes a pump + an MPSC queue. We **weight the memory-safety
   hazard axis (the dotnet-critic's lens) over code volume**, so we lean C — but it is a
   judgment call, and **B is a legitimate alternative** if the team prefers reusing the
   proven callback path and accepts the long-pin / foreign-thread-teardown cost.
6. **Workload input to record (explicit decision input).** If the expected workload is
   *many threads sharing one producer at a high produce rate*, B's channel-push (callers never
   block on the coarse mutex, zero managed threads) becomes more attractive. **We ask the
   reviewer to name the "expected produce concurrency"** as a decision input before locking C.
   (Note the serialized-append ceiling is identical across A/B/C — the coarse mutex is Mode B
   — so this is about *where the caller waits*, not throughput.)
7. **A only with profiling evidence.** A optimizes FFI crossings + lock acquisitions — both
   cheap in .NET (blittable P/Invoke ≈ ns; uncontended `Interlocked` ≈ ns) — at the cost of
   long cross-thread pins + two managed queues + two threads. Over-built unless a profile shows
   boundary crossings dominating. Not recommended now.

---

## 4 · Enumerated decisions (statement → pick → why)

1. **Completion model A/B/C** → **C (inline pull-pump).** Why: call-scoped `fixed` pin is
   provably correct (core copies during the call); completion on our own pump avoids the
   per-send foreign-thread / dual-GCHandle hazard; deterministic signal+join teardown for a
   many-in-flight producer. B/A recorded as alternatives (§3.3).
2. **Send FFI fn** → **`Producer_send` (singular, inline) + `FutureRecordMetadata_get_all`
   on the pump.** Why: singular send returns a future handle after a fast, synchronous
   copy-and-enqueue; the pump batches many completions per `get_all`; no `ProducerRecord_t`
   blittable mirror struct is needed (that is for `send_batch` / Option A).
3. **Pinning (§A4)** → **call-scoped `fixed` + explicit sentinels.** Absent key/value →
   `IntPtr.Zero` + len `-1`; **empty (Length 0) → a non-null stack sentinel byte + len 0**
   (NOT the `fixed` null — `fixed` over an empty array yields null and the core rejects
   `(null, len ≥ 0)`); a mutation-after-send test proves the copy happened during the call.
4. **Completion queue** → **an unbounded `ConcurrentQueue<(IntPtr future, TCS)>` + a
   lightweight signal (`SemaphoreSlim` or `ManualResetEventSlim`).** In-box on the
   ns2.0/net462 floor (avoid a `System.Threading.Channels` package dependency). Structurally
   unbounded, **practically bounded by the core's `buffer.memory` backpressure** (decision 3
   above): `Producer_send` blocks up to `max.block.ms` when the core buffer is full, so
   callers cannot outrun the drain. Steady-state depth ≈ one broker round-trip's in-flight
   window; worst case ≈ `buffer.memory / record_size` tiny `(handle, TCS)` entries (a few MB).
   **Do NOT hand-cap it, and do NOT add a Python-style managed bound in front of the
   accumulator** — C sends inline, so the core's backpressure applies directly (Python needs
   its bound only because its C extension batches; `on_space_available` is Python-C-ext-only —
   0 occurrences in the ABI header, verified).
5. **Pump lifecycle + teardown integration** → **exactly one pump thread**; batched drain →
   `get_all` → per result read the metadata accessors (or `RecordMetadata_copy`) → complete
   each TCS with **`RunContinuationsAsynchronously`** (mandatory — user continuations never run
   on / stall the pump) → `destroy_all` the futures; free every handle on every path;
   completion exactly-once. **Teardown folds into the P2.1-collapsed `NativeProducer`
   Dispose/DisposeAsync:** **stop sends → drain/fault pending → join the pump → flush/close →
   `Producer_destroy`.** Preserve the P2.1 single-layer teardown shape (mirror `NativeConsumer`);
   `Producer_destroy` blocks/joins the Sender.
6. **`ProducerRecord` shape** → **clipped value type** (`Topic`, `Partition?`, `Timestamp?`,
   `Key: ReadOnlyMemory<byte>?`, `Value: ReadOnlyMemory<byte>?`), ctor
   `(topic, value, key=null, partition=null, timestamp=null)` — exact Python-ctor order.
   **No `Headers`** (ABI has no field).
7. **`RecordMetadata` shape** → **clipped value type** (`Topic`, `Partition`, `Offset`,
   `Timestamp`). **No** serialized-size / `has*` accessors (ABI doesn't expose them).
8. **Cancellation (§A7)** → **best-effort — cancels the *wait*, never aborts an enqueued
   send** (the producer has no `wakeup()`). A canceled `CancellationToken` → the returned
   `Task` faults with `OperationCanceledException`; the native send runs to completion; the
   pump's later `SetResult`/`SetException` on the already-canceled TCS is a safe no-op. Reuse
   `OperationCompletionSource`'s `CancelAwaiter` wiring where a per-op bridge is used; for the
   pull-pump the TCS is canceled directly on token fire.
9. **Error model (§A5)** → **operational vs precondition, two surfaces.** Operational
   failures → flat `KafkaException` via `KafkaException.FromHandle` (a **sync** `out_error`
   from `Producer_send` throws; an **async** send failure faults the `Task` from the pump —
   same `FromHandle`). Preconditions validated **before** any pin / marshal / P/Invoke:
   null topic / record → `ArgumentNullException`; **negative partition** →
   `ArgumentOutOfRangeException` (the ABI maps negative → "unset", so the binding must reject
   an explicitly-negative partition); post-Dispose → `ObjectDisposedException`.
   Assert **error-message content**, not just the type.
10. **`MockProducer` helpers** → **inherent on the concrete `MockProducer`** (not on
    `IAsyncProducer`): `bool CompleteNext()`, `bool ErrorNext(int code, string? message = null)`,
    `int HistoryCount { get; }`, `void Clear()`. Exact Java `MockProducer` / Python
    `_MockProducerMixin` parity; `HistoryCount` (property) because the ABI gives only a count.
11. **`IAsyncProducer` growth** → **additive** — add `Send(ProducerRecord, CancellationToken
    = default) -> Task<RecordMetadata>` to the existing interface (which already carries
    `Flush`/`Close`/`PartitionsFor`), exactly as `IConsumer` grew across M5/P8a→P8b. Safe:
    pre-publish, no external implementers.
12. **Serializer** → **bytes-only interim** (`ReadOnlyMemory<byte>?` both fields); the typed
    generic producer + `ISerializer<T>` wiring stay **deferred** (they were gated on this §A7
    decision; the follow-up phase can layer them non-breakingly over `Send`).

---

## 5 · Public API sketch (namespace `Confluent.Kafka`)

```csharp
public sealed class ProducerRecord            // Java ProducerRecord (clipped to today's ABI)
{
    public string Topic { get; }
    public int? Partition { get; }             // null → let the producer choose
    public long? Timestamp { get; }            // null → the producer stamps it
    public ReadOnlyMemory<byte>? Key { get; }  // null → no key
    public ReadOnlyMemory<byte>? Value { get; }// null → tombstone
    public ProducerRecord(string topic, ReadOnlyMemory<byte>? value,
        ReadOnlyMemory<byte>? key = null, int? partition = null, long? timestamp = null);
    // NO Headers (ABI ProducerRecord_t has no headers field)
}

public sealed class RecordMetadata            // Java RecordMetadata (clipped)
{
    public string Topic { get; }
    public int Partition { get; }
    public long Offset { get; }
    public long Timestamp { get; }
    // NO serialized-size / has* accessors (not in the ABI)
}

public interface IAsyncProducer : IAsyncDisposable, IDisposable   // additive growth
{
    // NEW in P3:
    Task<RecordMetadata> Send(ProducerRecord record, CancellationToken cancellationToken = default);
    // already shipped (P2 / P2.1):
    Task Flush(CancellationToken cancellationToken = default);
    Task Close(CancellationToken cancellationToken = default);
    Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic, CancellationToken cancellationToken = default);
}

public sealed class AsyncKafkaProducer : IAsyncProducer { /* Send forwards to _native */ }

public sealed class AsyncMockProducer : IAsyncProducer     // Java MockProducer
{
    public AsyncMockProducer(bool autoComplete = true);
    // Send forwards to _native; plus the inherent send-control helpers (NOT on IAsyncProducer):
    public bool CompleteNext();                            // MockProducer_complete_next
    public bool ErrorNext(int code, string? message = null);// MockProducer_error_next
    public int  HistoryCount { get; }                       // MockProducer_history_count
    public void Clear();                                    // MockProducer_clear
}
```

*(Naming note: the shipped mock type is `AsyncMockProducer`; the CLAUDE.md §3 sketch's bare
`MockProducer` is the same role. The helpers land on `AsyncMockProducer` — the concrete
mock — per decision 10. The Critic should confirm the helper names match (a) exactly.)*

---

## 6 · Internal design (the chosen Option C)

### 6.1 `NativeMethods` additions (all Mode A — symbols already in the header)

DllImports to add (none currently present — verified against `NativeMethods.cs`):

- `kafka_producer_Producer_send(IntPtr producer, IntPtr topic, int partition, long timestamp,
  IntPtr key, int keyLen, IntPtr value, int valueLen, out IntPtr outError) -> IntPtr` (future).
- `kafka_producer_FutureRecordMetadata_get_all(IntPtr[] futures, int count,
  IntPtr[] outMetadata, IntPtr[] outErrors) -> void`.
- `kafka_producer_FutureRecordMetadata_destroy_all(IntPtr[] futures, int count) -> void`.
- Metadata accessors: `RecordMetadata_offset` (long), `RecordMetadata_partition` (int),
  `RecordMetadata_topic` (IntPtr → `Utf8Marshal.PtrToString`), `RecordMetadata_timestamp`
  (long), `RecordMetadata_destroy` (void). *(Alternative: `RecordMetadata_copy` via a
  kept-alive Cdecl §A6 callback — decide at build time; the per-field accessors avoid the
  callback and are the default.)*
- Mock: `MockProducer_complete_next` (bool, `[return: MarshalAs(I1)]`),
  `MockProducer_error_next(IntPtr, int, IntPtr) -> bool` (`[return: MarshalAs(I1)]`; message
  pinned UTF-8 or `IntPtr.Zero` for the default message),
  `MockProducer_history_count` (int), `MockProducer_clear` (void).
- *(Optional fast-path)* `FutureRecordMetadata_is_done` (bool) — for a synchronous
  completion shortcut when a mock resolved before enqueue.

All `Cdecl`, full `EntryPoint`, `bool` via `[MarshalAs(I1)]`, sizes `int`/`long`, opaque
handles `IntPtr` (§0.1 type map).

### 6.2 `NativeProducer.Send` (the internal send worker)

**Name — `Send`, not `SendWithCallback`.** The P2 peripherals are named
`FlushWithCallback` / `PartitionsForWithCallback` / `CloseWithCallback` because they genuinely
use the push `_async`→`OperationCompletionSource` **callback** bridge (§A7 push). Option C — the
preferred pull-pump — has **no callback**, so a `*WithCallback` suffix would be inaccurate: the
completion arrives by the pump's `get_all`, not a native callback. `NativeProducer` is internal,
so the bare `Send` doesn't collide with the public `AsyncKafkaProducer.Send` (clean forwarding:
`public Send => _native.Send(...)`), and the **absence** of a `WithCallback` suffix next to the
peripherals honestly signals "different mechanism — pull pump, not the callback bridge." *(If
Option B were chosen instead, the name would revert to a `*WithCallback`-style worker, since B
**is** callback-based — the name is coupled to the Option-C choice.)*

Send path (on the caller thread):

1. `ThrowIfClosed()` (post-dispose guard) → precondition checks (null record/topic →
   `ArgumentNullException`; negative partition → `ArgumentOutOfRangeException`) **before** any
   pin / P/Invoke (§A5).
2. `Utf8Marshal.Pin(topic)` (call-scoped — the core copies the topic synchronously).
3. **`fixed`** over `Key`/`Value` spans (`ReadOnlyMemory<byte>?.Span`), with the
   absent (`-1`) / empty (non-null stack sentinel + `0`) / present (pointer + length) sentinel
   logic (§A4).
4. `Producer_send(...)` → future handle + sync `out_error`. If `out_error` non-null →
   unpin, `KafkaException.FromHandle(error)` **throws synchronously** (a sync validation /
   buffer failure). No future was created.
5. Unpin (end of `fixed` / topic pin) — the copy is done.
6. `new TaskCompletionSource<RecordMetadata>(RunContinuationsAsynchronously)`; enqueue
   `(future, tcs)` on the completion queue; signal the pump; return `tcs.Task`.
   *(Optional: if `FutureRecordMetadata_is_done`, complete synchronously without enqueuing —
   a `ValueTask`/short-circuit; decide during build.)*

The pump does **not** need the per-op `GCHandle` / span-the-op-ref machinery of the push
peripherals (there is no per-send native callback); the pump owns the future handles, and the
**teardown ordering** (§6.3) — not `DangerousAddRef` — enforces "parent outlives children"
(§A2). Wire cancellation by canceling the `tcs` on token fire (best-effort; the straggler
completion is a no-op).

### 6.3 The pump + MPSC queue

- Exactly **one** pump thread per `NativeProducer`, started lazily on first `Send` (or at
  construction — decide during build; lazy avoids a thread for send-less producers).
- Loop: wait on the signal → drain a batch of `(future, tcs)` from the `ConcurrentQueue` →
  build the `futures[]` / `outMetadata[]` / `outErrors[]` arrays → `get_all` (BLOCKS on the
  pump, never the caller — `block_on` parks only this thread; the Sender runs on the runtime
  worker pool, §A1) → per index: exactly one of `{metadata, error}` is non-null → read the
  metadata accessors into an owned `RecordMetadata` (topic copied out before destroy) **or**
  `KafkaException.FromHandle(error)` → `tcs.TrySetResult` / `TrySetException` → free that
  slot's handle → `destroy_all(futures)`. Every handle freed on every path; completion
  exactly-once (guard cancelled/done).
- **`RunContinuationsAsynchronously` is mandatory** (§A7) — otherwise a slow awaiter
  continuation runs on the pump thread and stalls every other completion.

### 6.4 Teardown integration (preserve the P2.1 single-layer shape)

Extend `NativeProducer.Dispose` / `DisposeAsync` / `CloseWithCallback` — do **not** add a
second teardown layer:

1. Take the one-shot `TryBeginClose()` latch (existing).
2. **Stop accepting sends** (the `_closed` gate already makes `Send` throw
   `ObjectDisposedException`).
3. **Drain / fault pending:** signal the pump to finish the in-flight `get_all`, then fault
   any still-queued `(future, tcs)` (their sends were enqueued but not yet drained) with a
   teardown `KafkaException` (or let the pump drain them first if graceful) — **join the
   pump** so no future handle is in use.
4. Existing graceful **flush/close** (sync `Producer_close` for `Dispose`,
   `Producer_close_async` for `DisposeAsync`/`CloseWithCallback`), swallow vs surface per the
   P2.1 flavor.
5. `_handle.Dispose()` → `Producer_destroy` (blocks/joins the Sender), exactly once.

Ordering rationale: the pump holds future handles derived from the producer, so
`Producer_destroy` must not run until the pump is joined (§A2 parent-outlives-children,
enforced by Dispose ordering — the ffi §A7 Option A teardown, verbatim). This mirrors
`NativeConsumer`'s "close before destroy" but with the added **pump-join** step that the
send path introduces.

---

## 7 · Tests required + DoD checklist

### 7.1 Tests (broker-free, `AsyncMockProducer`; DoD §3/§10 + ffi §A "tests required")

- **Resolves / faults:** `Send` `Task` resolves with the correct `RecordMetadata`
  (offset/partition/topic/timestamp); faults with `KafkaException` on `ErrorNext(code,
  message)` — assert **code + message content + retriable/fatal flags**.
- **Mutation-after-send** (§A4): mutate the caller's key/value buffer immediately after
  `Send` returns; the produced record (via history / completed metadata) is unchanged —
  proves the copy happened **during** the call (the call-scoped-pin correctness proof).
- **Allocation budget** (DoD §10 — the send path IS a hot path, so this applies **in full**):
  a large value adds **no value-sized managed allocation** on the send path
  (`GC.GetAllocatedBytesForCurrentThread()`, marginal large−small subtraction, per the
  hardened M7/P1 pattern).
- **Absent vs empty** key/value each produce the correct record (absent → `-1` sentinel;
  empty → non-null sentinel + `0`), asserted distinctly.
- **Concurrency:** concurrent `Send` from many threads is correct (the coarse mutex holds);
  high-concurrency produce does **not** starve the thread pool (no `Task.Run`/thread-per-send).
- **Dispose-with-sends-in-flight returns without hanging** (the pump-join regression) — the
  headline completion/deadlock guard, run under a test timeout.
- **Cancellation** is safe (cancels the wait, not the send) and frees handles; a canceled
  `Send` faults with `OperationCanceledException`.
- **Preconditions fire before any native call:** null topic / record →
  `ArgumentNullException`; negative partition → `ArgumentOutOfRangeException`; post-Dispose →
  `ObjectDisposedException` — each asserted to occur with no native side effect
  (message content asserted).
- **Non-ASCII topic** round-trips through send → `RecordMetadata.Topic` (UTF-8, §A3).
- **The four `MockProducer` helpers** behave per Java `MockProducer` / Python
  `_MockProducerMixin`: `CompleteNext`/`ErrorNext` drive a manual (`autoComplete: false`)
  send to success/failure; `HistoryCount` reflects sent records; `Clear` resets history +
  pending.
- **TFM smoke** (net462 via ns2.0 / net8.0 / net10.0): a `MockProducer` send round-trip loads
  and completes on each TFM.
- **Handles freed exactly once:** create/dispose many producers; send N; assert the native
  handle count returns to baseline (covers `destroy_all` + the metadata/error frees).

Assert **error-message content**, not just `is_err`/type (DoD §3).
**Byte-level wire-encoding tests: N/A** — Mode A adds no new wire types (the wire encoding
lives in the Rust core); state this explicitly in the self-review.

### 7.2 DoD checklist (definition-of-done.md)

- §1 CLAUDE.md + ffi §A consistency (esp. §A4 call-scoped pin, §A7 completion, §A5 errors).
- §2 all methods translated (Send + the four mock helpers + the value types).
- §3 all relevant tests translated + **error-message content asserted**; `@RepeatedTest`→loops
  where the Python/Java sibling has them.
- §5/§9 build + `dotnet test` (net8.0 + net10.0 run; net462 build-verified) +
  `dotnet format --verify-no-changes` clean; native-first
  `cargo build --features ffi` shows **no header delta** (Mode A proof).
- §6 no duplicated types; §7 no host-only type beyond the sanctioned scaffolding (the pump +
  queue are §A7-sanctioned plumbing, not Kafka logic); §8 no TODO/FIXME.
- **§10 hot-path allocation audit — applies in full** (send path). The allocation-budget test
  is the evidence.
- **§11 (consumer-trait-surface) — spirit applies to the producer:** `Send` stays a
  `Task`-returning method on `IAsyncProducer` (blocking-in-Java-via-`Future` → `Task`); **no
  `block_on`-wrapped sync façade**; **no per-send `Task.Run`/thread**; completion via
  `RunContinuationsAsynchronously`.

---

## 8 · Mode A confirmation + flagged Mode B gaps

**Mode A is sufficient for Option C.** Every symbol the plan needs is already in
`target/include/confluent_kafka.h` (verified while planning):

- `kafka_producer_Producer_send` (L2353) — sync send, returns future + `out_error`; Safety
  clause requires k/v valid **for the call only** (no "until callback" — the call-scoped-pin
  basis).
- `kafka_producer_FutureRecordMetadata_get_all` (L2561) — batched pull; futures **not**
  consumed.
- `kafka_producer_FutureRecordMetadata_destroy_all` (L2645).
- `kafka_producer_RecordMetadata_offset` / `_topic` / `_partition` / `_timestamp` / `_copy` /
  `_destroy` (L2664–2776).
- `kafka_producer_MockProducer_complete_next` / `_error_next` / `_history_count` / `_clear`
  (L2896–2948).

The Rust send path confirms the synchronous copy (`src/ffi/producer.rs` L262–281:
`rt.block_on(producer.send(record, None))`; the coarse `Mutex<ProducerKind>` at L965).

**No Mode B gap for Option C.** (For completeness: **Option B** would use
`Producer_send_async` (L2436), also present, whose header **borrows k/v until the callback**
— that borrow contract, not a missing symbol, is why B is dispreferred. **Option A** would
use `Producer_send_batch` (L2401) + the `ProducerRecord_t` mirror struct — also present.) So
whichever option is chosen, **no `src/**` / `src/ffi/**` / header / `cbindgen.toml` change is
required** — the hard Mode A invariant holds. If review surfaces a need not covered by these
symbols, STOP and flag it as a Rust-core dependency rather than inventing an ABI.

---

## 9 · Commit-hygiene reminders for the later loop (N=30)

- **Per-path `git add`** — stage only `bindings/dotnet/src/**` and
  `bindings/dotnet/tests/**` files this phase touches.
- **Never stage:** the repo-root `.claude/agents/dotnet-*.md` discovery copies (untracked
  workaround, CLAUDE.md §8.4); `COMMENTS.30.md` / `COMMENTS.DONE.30.md` (working files —
  `COMMENTS.30.md` is gitignored, `COMMENTS.DONE.30.md` is not, so never `git add` it);
  `.claude/agent-memory/**`; `target-linux*` build outputs; any staged `.so` / `.dylib`.
- **Commits:** `--no-gpg-sign`, and end each message with
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- **Native-first build** every cycle (`cargo build --features ffi` → `dotnet build`), and
  re-verify **no header delta** as the Mode A proof.
- On close-out, the Manager archives the approved plan + `COMMENTS.DONE.30.md` under
  `design/history/M11/P3-producer-send/`, updates `STATUS.md`, and resets `COMMENTS.30.md`.

---

*End of DRAFT PLAN — awaiting user review. No Actor/Critic spawned; nothing committed;
nothing archived.*
