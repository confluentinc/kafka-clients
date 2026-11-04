# M11/P1 — ".NET Producer FOUNDATION (interop + lifecycle scaffolding only)"

Status: **APPROVED (user 2026-08-12).** N=27. Executor: `dotnet-actor` (reviewed by `dotnet-critic`).
Branch: `prashah_dev_producer_foundation` (off `prashah_dev_dotnet_semaphore`). Do not disturb `prashah_dev_dotnet_semaphore` / PR #154 or `prashah_dev_dotnet_binding_consumer` / PR #150.

## 0 · Hard scope boundary (READ FIRST)

**M11/P1 = ONLY "Group A / the foundation" — internal scaffolding, NO public producer API.** No `IAsyncProducer`, no `KafkaProducer`/`MockProducer` **public** classes, no `Send`/`Flush`/`Close`/`PartitionsFor`, no `ProducerRecord`/`RecordMetadata`, no `ProducerRecord_t` mirror. This mirrors the consumer's early scaffolding (M1 interop, M2 SafeHandle/error) — the internal plumbing the public surface later sits on. **All Mode A** (the producer C ABI is fully present — **106** `kafka_producer_*` symbols, verified). **Hard line:** no `src/**` / `src/ffi/**` / `target/include/confluent_kafka.h` / Rust-core change — if the Actor thinks it needs one, STOP and flag the Manager. **P1 does NOT touch the §A7 pull-vs-push send-completion decision** (later phase) — it introduces no completion pump and no async bridge.

## 1 · Objective & context

Lay the producer's interop + lifecycle foundation so later phases (send/flush/close/partitions-for) are pure additive Mode-A ports. The producer path is the symmetric twin of the consumer scaffolding already shipped (`SafeConsumerHandle`/`SafeConsumerPropertiesHandle`/`NativeConsumer`, M1–M2), so P1 mirrors those files field-for-field on the producer side. Python's `bindings/python/producer.py` already restores the full producer *shape* on the same ABI — that is the roadmap target (§5 List 1), but **P1 builds none of it**.

## 2 · Verified findings (file:line)

- **Construct + lifecycle + properties ABI (all present in `target/include/confluent_kafka.h`):**
  - `kafka_producer_MockProducer_new(bool auto_complete) → Producer_t*` (`:2197`) — no props, no out-error, cannot fail.
  - `kafka_producer_ProducerProperties_new(void) → ProducerProperties_t*` (`:2208`); `_from_configs(const char*const*)` (`:2236`, a NULL-terminated flat key/value array — an alternative to new+put); `_put(props, key, value)` (`:2256`, void, no-op if null); `_destroy(props)` (`:2273`, void, null-safe).
  - `kafka_producer_KafkaProducer_new(const ProducerProperties_t* props, kafka_common_KafkaError_t** out_error) → Producer_t*` (`:2305`) — non-null handle on success; on failure returns null and sets `*out_error`. **Caller retains props ownership and frees it separately** after this call.
  - `kafka_producer_Producer_destroy(Producer_t*)` (`:2320`) — void, null-safe; per ffi §A2 it **blocks** (drops the runtime, waits for/joins the Sender).
  - `kafka_producer_Producer_close(Producer_t*, kafka_common_KafkaError_t** out_error)` (`:2827`) — the **sync graceful** close (error-surfacing); this backs the later app-facing `Close()`, not P1's Dispose.
  - `kafka_producer_Producer_close_async(Producer_t*, close_callback_t, user_data)` (`:2860`) + `Producer_close_callback_t` (`:348`) — the **push** async close; **deferred to phase B**.
- **ffi §A2 — the lifecycle contract (load-bearing for P1):** `Producer_t` is Category-1 client → `SafeProducerHandle : SafeHandle`, `ownsHandle: true`, `IsInvalid => handle == IntPtr.Zero`, `ReleaseHandle → Producer_destroy` (ffi-marshalling.md `:298`, `:306-308`). `ProducerProperties_t` is Category-1 config, **short-lived** — "freed by the binding, after `KafkaProducer_new`" (`:299`). "Prefer `Dispose` over the finalizer: `Producer_destroy` blocks … wrong on the finalizer thread … guard use-after-dispose with `ObjectDisposedException`" (`:317-320`); "the producer closes via `Dispose`, never the finalizer" (`:326-328`). The FULL Dispose (later) = stop sends → **join pump → flush/close** → release handle (`:315-316`); **P1 omits the pump-join/flush/graceful-close** (no pump, no send yet — §3.2).
- **Consumer scaffolding precedent to mirror (files exist):** `Internal/Interop/SafeConsumerHandle.cs`, `SafeConsumerPropertiesHandle.cs`, the shared base `SafeHandleZeroIsInvalid.cs`; `Internal/NativeConsumer.cs`; the M2/P2 hardening where the interop marshaller "invokes the private parameterless ctor and sets the handle atomically on return … no create→`SetHandle` allocation-gap window" (`SafeConsumerHandle.cs:20-24`). Phase precedents: `design/history/M1/P1-interop-scaffolding`, `M2/P1-error-model-safehandle`, `M2/P2-safehandle-return-hardening`.
- **Mode B confirmed:** `grep` for producer transaction/metrics ABI symbols → **0**. Those need a Rust-core + C-ABI change first (§5 List 2).

## 3 · P1 deliverables (exactly these two pieces + tests)

### (1) Producer interop scaffolding — the construct + lifecycle subset ONLY
- **`NativeMethods` `[DllImport]`s** for: `KafkaProducer_new` (returns `SafeProducerHandle`, `out IntPtr outError`), `MockProducer_new([MarshalAs(UnmanagedType.I1)] bool autoComplete)` (returns `SafeProducerHandle`), `ProducerProperties_new`/`_put`/`_destroy`, and `Producer_destroy` (used by `ReleaseHandle`). Return the construct functions **as `SafeProducerHandle`** (runtime invokes the private ctor + sets the handle atomically on return — the M2/P2 no-gap pattern), not raw `IntPtr`. Bool needs `[MarshalAs(I1)]`; strings are UTF-8 hand-marshalled (ffi §A3, reuse `Utf8Marshal`).
- **`SafeProducerHandle` + `SafeProducerPropertiesHandle`** — `: SafeHandleZeroIsInvalid` (reuse the existing shared base, no new base), private parameterless ctor, `ReleaseHandle → Producer_destroy` / `ProducerProperties_destroy` respectively. Mirror `SafeConsumerHandle`/`SafeConsumerPropertiesHandle`.
- **Config marshalling** — `IReadOnlyDictionary<string,string>` → `ProducerProperties_new` → per-entry `ProducerProperties_put` (UTF-8) → pass to `KafkaProducer_new` → **dispose the props handle after** (caller-frees-separately contract, §2). Keys are Java dotted names (`bootstrap.servers` required for a real producer); accept classic-only keys silently.
- **DEFER** (later phases, explicitly out of P1): the send DllImports (`Producer_send`, `FutureRecordMetadata_*`, `RecordMetadata_*`, the blittable `ProducerRecord_t` mirror struct) and the peripheral-op DllImports (`Producer_flush_async`, `Producer_close_async`, `Producer_partitions_for_async`).

### (2) `internal NativeProducer` lifecycle
- Two internal construction paths (mirroring `NativeConsumer`'s real/mock split): **from config** (`KafkaProducer_new` — read `out_error`; on failure throw a flat `KafkaException` and free the error handle, ffi §A5) and **mock** (`MockProducer_new(autoComplete)`).
- **`Dispose` / `DisposeAsync` — PINNED sequence (user decision 2026-08-12): `Producer_destroy` ONLY, routed through the SafeHandle.**

  ```csharp
  public void Dispose() {
      if (_disposed) return;   // idempotent
      _disposed = true;
      _handle.Dispose();       // SafeProducerHandle.ReleaseHandle → Producer_destroy
  }
  public ValueTask DisposeAsync() { Dispose(); return default; }  // P1: no async work yet
  ```

  Teardown routes through `SafeProducerHandle` (its `ReleaseHandle` calls `Producer_destroy`) — **no explicit graceful-close call in P1**. **Explicitly DEFERRED** (to the later send/flush phases, additive): the graceful `Producer_close`-first, the flush, and the pump-join. **Rationale to record:** P1 has no send path → no pending records to flush, no pump to join, so `Producer_destroy` (which blocks + joins the Sender per ffi §A2) is the minimal-correct subset. This mirrors what Python's full teardown (`_cancel` → `Producer_shutdown` → `Producer_close_async` → `Producer_destroy`) reduces to when send/pump don't exist yet. Guard use-after-dispose with `ObjectDisposedException`; `Dispose` idempotent (double-dispose safe); never rely on the finalizer (§A2).

### Tests (broker-free, internal via `InternalsVisibleTo`; no public API to test yet)
- Construct/dispose round-trip on the **mock** path (`MockProducer_new`).
- **Config marshalling** — construct via the config path with a `bootstrap.servers` dict; the `KafkaProducer` handle constructs without a broker (Java-faithful: the ctor doesn't connect). If the config path can't be exercised broker-free, assert the props `new→put→destroy` marshalling in isolation and note it.
- **Double-`Dispose`** safe (idempotent, per the pinned sequence).
- **Use-after-dispose** → `ObjectDisposedException`.
- **TFM-matrix smoke** — loads and round-trips on **net462** (via netstandard2.0), **net8.0**, **net10.0**.
- **N/A for P1:** the per-record send-path **allocation-budget** test — there is no send path yet; it lands with phase C.

## 4 · The reordered build sequence (context — P1 is only the first step)

**A foundation (P1, this phase)** → **B async peripherals** (`Flush`/`Close`, then `PartitionsFor`, over the **push** `_async`→`TaskCompletionSource` bridge, **reusing the consumer's §B7 pattern** — no pull-vs-push decision here) → **C send** (the pull-pump; the **send-completion Option A vs B** decision, ffi §A7, is **quarantined at this phase**) → **D sync producer** (the blocking `IProducer` mirror). The interface grows **additively** across B→C exactly as `IConsumer` grew (M5/P8a→P8b).

**Explicit note:** **P1 does NOT touch the pull-vs-push / §A7 send-completion decision at all.** That Option A vs B choice is made at **phase C (send)**, not now. P1 is interop + lifecycle only.

## 5 · Roadmap API lists (context only — NOT P1 deliverables)

### List 1 — In scope for the *Producer milestone* (Python-sibling surface; later phases P2+)
- `send` → `Task<RecordMetadata> Send(ProducerRecord)` · `flush` → `Task Flush()` · `partitions_for` → `Task<IReadOnlyList<PartitionInfo>> PartitionsFor(topic)` · `close` → `Close()` / `Close(TimeSpan)`.
- Value types `ProducerRecord`, `RecordMetadata`; `MockProducer` helpers `CompleteNext`/`ErrorNext`/`HistoryCount`/`Clear`.
- Both async (`AsyncProducer`/`IAsyncProducer` sibling) and later a sync mirror (`IProducer`/`Producer` sibling) — Python has both. Bytes-first (`ReadOnlyMemory<byte>` key/value); typed `<K,V>` deferred (like consumer M6/P1b).
- **P1 builds NONE of these — it is the internal foundation they sit on.**

### List 2 — In public Java `Producer<K,V>` but MISSING in BOTH Python AND the C ABI → Mode B, deferred (later milestones)
- **Transactions (5):** `initTransactions`, `beginTransaction`, `sendOffsetsToTransaction`, `commitTransaction`, `abortTransaction`.
- **Observability (4):** `metrics()`, `clientInstanceId(Duration)`, `registerMetricForSubscription`, `unregisterMetricFromSubscription`.
- These need a Rust-core + C-ABI change first (**0** producer-txn/metrics ABI symbols today, verified) → Mode B.
- **Design folds / nuances (note, not "missing"):** `send(record, Callback)` — the `Task` *replaces* the callback; **no** callback overload. `close(Duration)` — there is **no** `Producer_close_with_timeout` ABI (unlike the consumer), so a timed producer close is a **.NET-side deadline** or a deferred overload (later phase).

## 6 · Definition of Done

- `cargo build --features ffi [--release]` (native, unchanged) then `dotnet build` **0W/0E across the TFM matrix** (netstandard2.0/net8.0/net10.0 lib; net462/net8.0/net10.0 tests) under `Directory.Build.props` analyzers.
- `dotnet test` green on net8.0 + net10.0 (the P1 internal tests; TFM smoke); `dotnet format --verify-no-changes` clean.
- **Mode A hard line:** `git diff --stat` touches only `bindings/dotnet/src/**` + `bindings/dotnet/tests/**` — **zero** change to `target/include/confluent_kafka.h`, `src/ffi/**`, Rust core. (Confirm; STOP-and-flag if one seems needed.)
- **Shape/scope:** no public producer type ships (§0); the new types are all `internal` under `Internal/`/`Internal/Interop/`; `SafeHandle`/`Dispose`/marshalling satisfy the ffi §A2/§A3/§A5 anti-patterns. No `send`/`flush`/`partitions_for` DllImports (deferred). No `ProducerRecord_t` mirror.

## 7 · Governance / handoff (Manager, N=27)

Executor **`dotnet-actor N=27`** (interop + SafeHandle + lifecycle, header-down); reviewer **`dotnet-critic N=27`** (handle lifetime/`ReleaseHandle→Producer_destroy`, `[MarshalAs(I1)]` on `bool`, UTF-8 marshalling, props free-after-`new` ownership, the **pinned** `ObjectDisposedException` guard + idempotent double-dispose + `Producer_destroy`-only teardown, finalizer-avoidance, and the no-`src/ffi`/no-public-API/no-send-DllImport scope). Per-path `git add` with the guard (never the root `.claude/agents/dotnet-*.md` discovery copies, `COMMENTS.*27.md`, agent-memory, `target-linux*`, staged `.so`/`.dylib`); commits `--no-gpg-sign` + `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. On close: archive `COMMENTS.DONE.27.md` under `design/history/M11/P1-producer-foundation/`, update `design/current/STATUS.md` (M11/P1 DONE), reset `COMMENTS.27.md`.
