# .NET binding — Milestone 2 / Phase 2: "SafeHandle marshaller-return hardening"

**Status: APPROVED to implement (2026-07-22).** Mode A (no Rust authoring; the
header already has every symbol). Binding-local numbering; governance number
**N=4** (`dotnet-actor` / `dotnet-critic`). This is a **hardening** change, NOT a
bug fix — the current M2/P1 code is correct on net8/net10.

## Motivation

The M2/P1 SafeHandle factories use the `new SafeConsumerPropertiesHandle()` +
`SetHandle(raw)` two-step (`SafeConsumerPropertiesHandle.Create`,
`SafeConsumerHandle.FromRaw`). That leaves a small allocation-gap window: if an
exception (OOM, or an async `ThreadAbort` on net462) fires between obtaining the
native pointer and `SetHandle` running, the native handle leaks — `ReleaseHandle`
is never called because `IsInvalid` is still true. CKD (verified from source;
also targets net462) closes this by declaring the P/Invokes to **return the
SafeHandle directly**, so the interop marshaller creates-and-sets the handle
atomically inside a Constrained Execution Region. net462 — where the async-abort
window actually exists — is a supported + CI-tested TFM here, so we align with
CKD's robust idiom.

## Scope (what changes)

### 1 · `Internal/Interop/NativeMethods.cs` — three return types IntPtr → SafeHandle

- `ConsumerPropertiesNew()` : `IntPtr` → `SafeConsumerPropertiesHandle`
- `KafkaConsumerNew(SafeConsumerPropertiesHandle props, out IntPtr outError)` :
  `IntPtr` → `SafeConsumerHandle`
- `MockConsumerNew(IntPtr autoOffsetReset)` : `IntPtr` → `SafeConsumerHandle`

PRESERVE the fallible contract on `KafkaConsumerNew`: on failure the native
returns null → the marshaller hands back an **IsInvalid** `SafeConsumerHandle`
AND writes a non-null `*outError`. Do NOT change any other decl (the `KafkaError`
five, `ConsumerProperties_put`/`_destroy`, `Consumer_close`/`_close_with_timeout`/
`_destroy`, the group-metadata trio all stay as-is). Refresh the `NativeMethods`
class-doc / per-method doc that mentioned raw-`IntPtr` handle wrapping.

### 2 · `SafeConsumerPropertiesHandle.cs` / `SafeConsumerHandle.cs` — remove `new + SetHandle`

- `SafeConsumerPropertiesHandle.Create()` collapses to
  `return NativeMethods.ConsumerPropertiesNew();` (the marshaller invokes the
  **private** parameterless ctor — keep it; CKD precedent: a private ctor works
  with SafeHandle-return marshalling).
- `SafeConsumerHandle`: the handle now arrives already-wrapped from
  `KafkaConsumerNew` / `MockConsumerNew`. **Remove `FromRaw(IntPtr)`** (prefer
  removing; keep only a thin non-marshalling helper if a call site truly needs
  one — it should not). Keep the **private parameterless ctor** + `ReleaseHandle`
  unchanged.
- Both `ReleaseHandle` bodies (`ConsumerProperties_destroy` / `Consumer_destroy`)
  are **UNCHANGED**. The graceful close→destroy in `NativeConsumer.Dispose` is
  **UNCHANGED**.

### 3 · `Internal/NativeConsumer.cs` — consume the SafeHandle returns

- props via the property-handle factory (`Create()` now returns the SafeHandle);
  still passed to `KafkaConsumerNew` as the SafeHandle in-param (**D6 preserved**)
  and still disposed by the caller in a `finally`.
- `SafeConsumerHandle handle = NativeMethods.KafkaConsumerNew(props, out IntPtr
  outError);` → if `outError != IntPtr.Zero`: dispose `handle` (it is invalid;
  `ReleaseHandle` is a no-op) and `throw KafkaException.FromHandle(outError)`.
  Else defensively guard `handle.IsInvalid` and store it.
- Mock path: `handle = NativeMethods.MockConsumerNew(...)` (non-fallible).
- The `put` loop (`props.DangerousGetHandle()`) is **UNCHANGED**.

### 4 · Tests — sweep every IntPtr-return call site + add the failure-path regression

- **Audit ALL call sites** that assumed an `IntPtr` return. Known ripples:
  - `tests/.../Interop/NativeLoadProbeTests.cs` (M1/P1; the user has it open) —
    ~line 41 calls `ConsumerPropertiesNew()` then `ConsumerPropertiesDestroy` on a
    raw `IntPtr`. Move to `using var p = NativeMethods.ConsumerPropertiesNew();`,
    assert `!p.IsInvalid`, and let `Dispose` free it (drop the manual
    `ConsumerPropertiesDestroy`). Keep the non-ASCII no-crash assertion.
  - The M2/P1 Interop tests: `SafeConsumerHandleTests`, `ConsumerConfigMarshalTests`,
    `KafkaExceptionTests`, `Utf8RoundTripTests` — audit each for `FromRaw` /
    raw-`IntPtr`-return usage and update to the SafeHandle-return shape.
- **ADD a regression** (the key new test): `KafkaConsumerNew` with the
  classic-protocol config returns an **IsInvalid** `SafeConsumerHandle` AND a
  non-null `outError`; disposing that invalid handle causes **NO** spurious
  `Consumer_destroy`, no crash, no leak (`ReleaseHandle` is skipped when
  `IsInvalid`). Assert the `outError` still round-trips through
  `KafkaException.FromHandle` (classic-protocol message / flags) as before.
- Keep the existing lifecycle + error + UTF-8 tests green.

## Interop correctness notes (for actor/critic)

- SafeHandle-return marshalling is a **classic `[DllImport]`** feature, fully
  supported on the netstandard2.0 floor incl. net462 — no `[LibraryImport]`.
- The SafeHandle subtype MUST have a **marshaller-accessible parameterless ctor**;
  **private is fine** (CKD uses private ctors this way). Confirm both keep theirs.
- A SafeHandle **return** coexisting with a SafeHandle **in-param** on the same
  call (`KafkaConsumerNew`) is supported.
- Failure path: verify the null-return → IsInvalid-handle path does **NOT** invoke
  `ReleaseHandle` (no spurious destroy) — this is the key new test.

## Explicitly unchanged (do NOT touch)

- `ReleaseHandle` bodies; `NativeConsumer.Dispose` graceful close→destroy; the
  `put` loop; the `KafkaError` five decls; `ConsumerProperties_put`/`_destroy`;
  `Consumer_close`/`_close_with_timeout`/`_destroy`; the group-metadata trio;
  `KafkaException` and its `FromHandle`; D1–D6 semantics from M2/P1 (D6 in
  particular — props stays a SafeHandle in-param).

## DoD gates (.NET-specific; NOT cargo xtask / make verify)

1. `cargo build --features ffi` (from repo root) FIRST — native cdylib + header.
2. `dotnet build` — 0 warnings / 0 errors across library TFMs
   (netstandard2.0;net8.0;net10.0) + test TFMs (net8.0;net10.0); CS1591 still
   enforced on the public `KafkaException`.
3. `dotnet test -f net10.0` — all green (existing + the new failure-path regression).
4. `dotnet format --verify-no-changes` — clean.
5. CI-only: net8.0 test *run* + net462 (Windows) are CI-only; build legs must pass.

No TODO/FIXME; Apache-2.0 header preserved on touched/new `.cs` files.

## Governance (N=4)

- This PLAN: `design/history/M2/P2-safehandle-return-hardening/PLAN.md`.
- Working comments: `bindings/dotnet/COMMENTS.4.md` (gitignored by the repo-root
  `COMMENTS.[0-9]*.md` rule) → resolved to `COMMENTS.DONE.4.md` (committed; the
  `D` breaks the ignore glob) → archived to
  `design/history/M2/P2-safehandle-return-hardening/COMMENTS.DONE.4.md` at handoff.
  NEVER name a committed artifact `COMMENTS.<digit>*.md`.
- Personas: `dotnet-actor` (Actor N=4), `dotnet-critic` (Critic N=4). NEVER the
  Rust `actor-executor` / `kafka-critic`. Critic reviews via `git log`/`git show`.
- Persona/agent-memory tracking UNCHANGED from M2/P1: binding-local personas
  (`bindings/dotnet/.claude/agents/*.md`) stay TRACKED and untouched; the
  repo-root `.claude/agents/dotnet-*.md` copies are NEVER committed; agent-memory
  (except the tracked `.gitkeep`) is NEVER committed. Verify each commit's file
  list.
- Final handoff: update `design/current/STATUS.md`; archive `COMMENTS.DONE.4.md`;
  reset working `COMMENTS.4.md`.
