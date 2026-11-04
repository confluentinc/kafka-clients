# COMMENTS.DONE.27 — M11/P1 ".NET Producer FOUNDATION (interop + lifecycle scaffolding only)"

Local working record (NEVER committed — CLAUDE.md §8.4). Records decisions/deviations
made during execution of the M11/P1 producer foundation (Mode A). No Critic issues yet
(first pass).

## Deliverables implemented (per PLAN §3)

1. Producer interop scaffolding (construct + lifecycle subset only):
   - `src/Confluent.Kafka/Internal/Interop/SafeProducerHandle.cs` — `: SafeHandleZeroIsInvalid`,
     private ctor, `ReleaseHandle → Producer_destroy`.
   - `src/Confluent.Kafka/Internal/Interop/SafeProducerPropertiesHandle.cs` — `: SafeHandleZeroIsInvalid`,
     private ctor, static `Create()`, `ReleaseHandle → ProducerProperties_destroy`.
   - `NativeMethods.cs` — producer construct/lifecycle DllImports (`EntryPoint` = full ABI
     symbol, `Cdecl`): `ProducerPropertiesNew` (→ SafeProducerPropertiesHandle),
     `ProducerPropertiesPut(IntPtr,IntPtr,IntPtr)`, `ProducerPropertiesDestroy(IntPtr)`,
     `KafkaProducerNew(SafeProducerPropertiesHandle, out IntPtr)` (→ SafeProducerHandle),
     `MockProducerNew([MarshalAs(I1)] bool)` (→ SafeProducerHandle),
     `ProducerDestroy(IntPtr)`. Construct fns return the SafeHandle directly (M2/P2 no-gap).
2. `src/Confluent.Kafka/Internal/NativeProducer.cs` — `Create(config)` / `CreateMock(autoComplete)`
   + `Dispose`/`DisposeAsync` (pinned §3.2: `Producer_destroy` only via the SafeHandle;
   no graceful close / flush / pump-join) + `Handle` w/ `ObjectDisposedException` guard.
3. Tests (broker-free, `Internal/Interop/`): `SafeProducerHandleTests`,
   `ProducerConfigMarshalTests`, `ProducerFoundationTfmSmokeTests`.

## Deviations (recorded per §4 latitude)

- **D1 — atomic disposed latch, not a plain `bool`.** The PLAN §3.2 pseudocode shows
  `if (_disposed) return; _disposed = true;`. Implemented instead with
  `Interlocked.Exchange(ref _disposed, 1)` (int) + `Volatile.Read` in `ThrowIfDisposed`.
  Rationale: the task directive "mirror the consumer precedent field-for-field" — the
  sibling `NativeConsumer` uses an atomic `_closed` int (`TryBeginClose` via
  `Interlocked.CompareExchange`), and a torn read/write on a plain field is a race .NET has
  and Python's GIL hides (agent memory). **The teardown BEHAVIOR is exactly the pinned
  subset — `Producer_destroy` only, no graceful close / flush / pump-join** — so "EXACTLY
  the pinned sequence" (scoped to the teardown steps) holds; only the idempotency mechanism
  is hardened. Documented in `NativeProducer`'s remarks.

- **D2 — added two broker-free operational-failure tests.** PLAN §3's test list required the
  success config path + preconditions + double-Dispose + use-after-dispose + TFM smoke.
  Added `Create_UnparseableConfigValue_ThrowsKafkaException` (`batch.size=not-a-number` →
  `IllegalArgument("Invalid value for 'batch.size': …")`) and
  `Create_MissingBootstrapServers_ThrowsKafkaException` ("No resolvable bootstrap urls …")
  to satisfy ffi §A5 "a sync failure throws KafkaException with the right code/message/flags"
  + DoD §3 error-message assertion. Additive, still foundation (no public API). Also drives
  the M2/P2 SafeHandle-return IsInvalid failure path in a loop
  (`KafkaProducerNew_InvalidConfigValue_…`, producer analog of the consumer's
  classic-protocol regression). NOTE: an empty/`bootstrap`-less config does NOT construct a
  producer broker-free (the core requires resolvable bootstrap urls), so there is no
  "empty config succeeds" case — the full success path uses `bootstrap.servers=localhost:9092`.

## DoD results (local)

- `cargo build --features ffi` — green (native unchanged).
- `dotnet build` (lib + tests) — 0W/0E across netstandard2.0 / net8.0 / net10.0 / net462 (build).
- `dotnet test -f net10.0` — 438 passed / 0 failed (17 new producer tests). net8.0/net462
  build-only locally (no net8 runtime installed; CI runs them).
- `dotnet format --verify-no-changes` — clean (lib + tests).
- `git diff --stat` — only `bindings/dotnet/src/**` + `bindings/dotnet/tests/**`; zero churn
  to `confluent_kafka.h` / `src/ffi/**` / Rust core. Mode A held.

## Hard scope / Mode A confirmation

No public producer API; no send/flush/close/partitions-for DllImports; no
`ProducerRecord`/`RecordMetadata`; no `ProducerRecord_t` mirror; no completion pump / async
bridge (§A7 untouched). No `src/**` (Rust) / `src/ffi/**` / header change.
