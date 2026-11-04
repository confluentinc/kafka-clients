# COMMENTS.DONE.3 — M2/P1 "Error model + first SafeHandle (consumer client lifecycle)"

Archived record for the M2/P1 phase (Actor N=3). This file captures the
decisions and deviations made **during execution** (the forward-looking plan is
`design/history/M2/P1-error-model-safehandle/PLAN.md`). No Critic findings were
open at start; the working `COMMENTS.3.md` had no items.

## Decisions & deviations recorded during execution

### D3 — synchronous `Dispose()` only; `DisposeAsync()` deferred (deviation from CLAUDE.md §4)

CLAUDE.md §4's disposal default is **both** `IAsyncDisposable.DisposeAsync()`
(primary) and `IDisposable.Dispose()` (fallback). This phase ships **only** the
synchronous `IDisposable.Dispose()`. Justification: the only close primitive in
scope is the synchronous `Consumer_close_with_timeout` (`block_on` internally,
`src/ffi/consumer.rs`). `Consumer_close_async` needs the completion bridge (push
dispatcher / `TaskCompletionSource`), which is explicitly deferred this phase.
Wiring `DisposeAsync` now would be either sync-over-async (ffi §B7 anti-pattern)
or a dependency on the deferred bridge. `DisposeAsync` lands with poll/subscribe
and the bridge. This re-affirms PLAN D3.

### Dispose close-error handling — consume-not-rethrow (implementation decision)

`NativeConsumer.Dispose()` calls `Consumer_close_with_timeout`, then consumes the
returned error handle via `KafkaException.FromHandle` (which frees it exactly
once) but does **not** rethrow it. Rationale: `IDisposable.Dispose()` must not
throw (a throwing Dispose masks in-flight exceptions and breaks `using`), and a
best-effort teardown has no caller to hand a failure to. The channel for
surfacing a close failure to the caller is the future public
`CloseAsync(TimeSpan)` (deferred with the bridge, D3). The error handle is still
freed on every path — no leak. The mapped `KafkaException` is discarded (`_ =`).

### D5 — UTF-8 config-value round-trip: **CLOSED** (verified empirically)

PLAN D5 required the Actor to **empirically verify** whether a configured
`group.id` surfaces broker-free / pre-join before committing to the round-trip.

**Empirical result: it does — D5 is CLOSED, the round-trip is kept.**

- Test `Interop/Utf8RoundTripTests.ConfiguredGroupId_NonAscii_RoundTripsThroughGroupMetadata`
  creates a real (`group.protocol=consumer`) consumer with a non-ASCII
  `group.id = "café-Ω-日本語-😀"` and **no broker**, then reads it back via
  `Consumer_group_metadata` → `ConsumerGroupMetadata_group_id` →
  `Utf8Marshal.PtrToString`, asserting equality with the input. It **passes**
  (net10.0, ~166 ms — fast, confirming no network wait).
- Why it works: the core's `AsyncKafkaConsumer::group_metadata()` falls back to a
  stub built from the configured `group_id` when no membership metadata exists
  yet (`src/consumer/async_kafka_consumer.rs`, mirrored by the Rust unit test
  `group_metadata_after_creation_with_group_id`). So the configured value is
  observable before any join.
- The three conditional DllImports (`Consumer_group_metadata`,
  `ConsumerGroupMetadata_group_id`, `_destroy`) are therefore kept, and the test
  exercises an owned Category-3 handle (get → read → destroy, ffi §B2). This
  fulfils the M1/P1 forward-reference (`NativeLoadProbeTests` noted the true
  config-value corruption round-trip was "deferred to a phase … reading a config
  value back through `ConsumerGroupMetadata_group_id`").

The **non-ASCII error-message** round-trip is also covered independently
(`KafkaExceptionTests.NonAsciiInvalidConfigValue_ErrorMessageEchoesValue`, via
`group.protocol="café"` whose validation error echoes the value) — the
NUL-terminated output form of `Utf8Marshal.PtrToString`.

### Flat-now / typed-later `KafkaException` (ffi §A5) — re-affirmed

The flat `Code` / `IsRetriable` / `IsFatal` shape is intentional (the ABI exposes
only those). `FromHandle` is the seam for later typed subclasses under the same
base, non-breaking; documented in `<remarks>` with no timeline.

## Notes for the reviewer (non-blocking, for context)

- `KafkaException` carries the Java-style standard exception constructors
  (`()`, `(string?)`, `(string?, Exception?)`) plus the internal
  `(int, string?, bool, bool)` used by `FromHandle`. This mirrors Java's
  `KafkaException` shape **and** satisfies analyzer CA1032 with **no suppression**
  (build is 0 warnings / 0 errors under `TreatWarningsAsErrors`).
- `NativeConsumer` passes `props` to `KafkaConsumer_new` as the
  `SafeConsumerPropertiesHandle` (D6) so the marshaller does DangerousAddRef/
  Release; `ConsumerProperties_put` (an M1 IntPtr-typed decl, reused as-is) is
  fed `props.DangerousGetHandle()` inside a tight synchronous loop where `props`
  is stack-rooted and not disposed until the `finally` — safe without a manual
  AddRef.
- The `unsafe` boundary stays quarantined to `Internal/Interop/`; `NativeConsumer`
  under `Internal/` is `unsafe`-free (CLAUDE.md §2, PLAN D4).

## Verification (phase-scoped DoD gates — all green)

1. `cargo build --features ffi` — native cdylib + regenerated header present.
2. `dotnet build` — **0 warnings, 0 errors** across library TFMs
   (netstandard2.0, net8.0, net10.0) + test TFMs (net8.0, net10.0), with
   `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` + `GenerateDocumentationFile`
   active (CS1591 enforced on the public `KafkaException`).
3. `dotnet test -f net10.0` — **19 passed, 0 failed** (4 M1/P1 carried + 15 new:
   6 error/precondition, 5 lifecycle, 3 config-marshal, 1 D5 round-trip).
4. `dotnet format --verify-no-changes` — clean.
5. CI-only caveat (not blocking): only the .NET 10 runtime is installed locally,
   so the net8.0 test *run* and net462 (via netstandard2.0) are CI-only; both
   *build* legs pass.
