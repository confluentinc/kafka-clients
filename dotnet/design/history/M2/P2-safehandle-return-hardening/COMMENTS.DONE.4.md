# COMMENTS.DONE.4 — M2/P2 "SafeHandle marshaller-return hardening" (Actor N=4)

Archived record for the M2/P2 phase: execution decisions and deviations, plus the
Critic review outcome. The working `bindings/dotnet/COMMENTS.4.md` is gitignored
(repo-root `COMMENTS.[0-9]*.md`); this `COMMENTS.DONE.4.md` is committed (the `D`
breaks the ignore glob).

## Open review items at start of execution

None. The working `COMMENTS.4.md` read `_No open items yet._` before implementation
began — this is the first Actor pass of the phase, so there were no Critic findings
to resolve.

## What changed (matches the approved PLAN exactly)

1. **`Internal/Interop/NativeMethods.cs`** — three owned-handle constructors changed
   return type only (`IntPtr` → the `SafeHandle` subtype), nothing else touched:
   `ConsumerPropertiesNew() : SafeConsumerPropertiesHandle`,
   `KafkaConsumerNew(SafeConsumerPropertiesHandle, out IntPtr) : SafeConsumerHandle`,
   `MockConsumerNew(IntPtr) : SafeConsumerHandle`. The `KafkaError` five,
   `ConsumerProperties_put`/`_destroy`, `Consumer_close`/`_close_with_timeout`/
   `_destroy`, and the group-metadata trio are unchanged. Class-doc / per-method docs
   refreshed from "raw-IntPtr wrapping" to the marshaller-create-and-set idiom.
2. **`SafeConsumerPropertiesHandle.cs`** — `Create()` collapses to
   `=> NativeMethods.ConsumerPropertiesNew();` (marshaller invokes the private ctor).
   **`SafeConsumerHandle.cs`** — `FromRaw(IntPtr)` removed. Both keep their private
   parameterless ctor + unchanged `ReleaseHandle`.
3. **`Internal/NativeConsumer.cs`** — consumes the SafeHandle returns. On a non-null
   `out_error`, dispose the (IsInvalid) handle then throw `FromHandle`; else a
   defensive `IsInvalid` guard before storing. `props` still passed as the SafeHandle
   in-param (D6), disposed in `finally`; the `put` loop is unchanged;
   `Dispose` (graceful close→destroy) is unchanged.
4. **Tests** — `NativeLoadProbeTests` both flows moved to
   `using SafeConsumerPropertiesHandle props = NativeMethods.ConsumerPropertiesNew();`
   + `Assert.False(props.IsInvalid)` (Dispose frees; dropped the manual
   `ConsumerPropertiesDestroy`), non-ASCII no-crash assertion kept. **Key regression**
   added to `SafeConsumerHandleTests` (see below). `ConsumerConfigMarshalTests`,
   `KafkaExceptionTests`, `Utf8RoundTripTests` audited — they drive the public
   `NativeConsumer.Create`/`CreateMock` + `consumer.Handle`, never `FromRaw` or a
   raw-IntPtr return, so no change was needed.

## Key failure-path regression (the new test)

`SafeConsumerHandleTests.KafkaConsumerNew_ClassicProtocol_ReturnsInvalidHandleAndError_DisposeSkipsRelease`
drives `NativeMethods.KafkaConsumerNew` directly with `group.protocol=classic`
(unsupported → null native return) in a 50× loop and asserts:
`handle.IsInvalid` is true, `out_error != IntPtr.Zero` (fallible contract preserved),
disposing the IsInvalid handle skips `ReleaseHandle` (no spurious `Consumer_destroy`,
no crash / double-free under repetition), and the `out_error` still round-trips
through `KafkaException.FromHandle` (code 35 UNSUPPORTED_VERSION, `IsRetriable`/
`IsFatal` false, message contains "Classic group protocol").

## Decisions / deviations recorded during execution

- **`FromRaw` removed entirely (no thin helper kept).** The PLAN permitted keeping a
  thin non-marshalling helper only "if a call site truly needs one — it should not."
  No call site does (the handle arrives marshaller-wrapped from both constructors),
  so `FromRaw` was deleted rather than retained.
- **Defensive `IsInvalid`-without-error guard throws `KafkaException`.** After
  `FromHandle` returns null (success) the code re-checks `handle.IsInvalid`; a
  `(null handle, null error)` return violates the header's contract (a null
  `out_error` implies a non-null handle). Chosen surface: `KafkaException` (a core
  contract violation is an error originating in the core, per the type's own doc
  contract) with a descriptive message, not a precondition `.NET` exception (this is
  not a caller programmer error). Can't-happen per the header; the guard exists so an
  IsInvalid handle is never stored (a later `Handle` read would otherwise hand back a
  null pointer). It disposes the handle before throwing (no-op `ReleaseHandle`).
- **Removed now-unused `using System;`** from `SafeConsumerPropertiesHandle.cs`,
  `SafeConsumerHandle.cs`, and `NativeLoadProbeTests.cs` — after the edits `IntPtr` is
  no longer named by simple type name in those files, and IDE0005 fails the build
  under `EnforceCodeStyleInBuild` + `TreatWarningsAsErrors` (library generates docs).

## DoD (all green)

1. `cargo build --features ffi` — native + header (run FIRST).
2. `dotnet build` — 0 warnings / 0 errors across ns2.0 / net8.0 / net10.0 (library)
   + net8.0 / net10.0 (tests); CS1591 enforced on public `KafkaException`.
3. `dotnet test -f net10.0` — 20 passed, 0 failed (19 carried + 1 new regression).
4. `dotnet format --verify-no-changes` — clean.
- CI-only build legs (net8.0 test run, net462 via ns2.0) not run locally (only the
  .NET 10 runtime is installed); both *build* legs pass.

## Review outcome (M2/P2)

Critic (N=4) review pending — this record captures the Actor pass. Update on the
Critic verdict (findings resolved / clean) at review close, mirroring M2/P1.
