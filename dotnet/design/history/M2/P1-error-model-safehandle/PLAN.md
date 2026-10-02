# .NET binding — Milestone 2 / Phase 1: "Error model + first SafeHandle (consumer client lifecycle)"

**Status: APPROVED (2026-07-21).** First public-API phase. Mode A (consumer C ABI
already landed — no Rust authoring; the Actor adds `[DllImport]`s and reads the
generated header). Binding-local numbering, independent of the repo-root Rust
`design/`. Governance number **N=3** (`dotnet-actor` / `dotnet-critic`).

## Scope

The operational + precondition error model and Category-1 owned-handle lifecycle,
CONSUMER-FIRST. Restores the Java error + `AutoCloseable` shape in C# idiom; NO
Kafka logic (CLAUDE.md §1). Activates the five `kafka_common_KafkaError_*`
`[DllImport]`s that M1/P1 declared-but-never-called (runtime-verifying their
EntryPoints and the I1 bools). NO completion bridge, NO poll/subscribe/commit/
position/assignment, NO producer, NO receive-path (Category 3/4) handles, NO
public client type yet.

The only new PUBLIC type this phase is `KafkaException`; the consumer create →
close → destroy lifecycle stays INTERNAL (tested via `InternalsVisibleTo`).

## Deliverables

### 1 · Public error model (the FIRST public type)

- `public sealed class KafkaException : Exception` at the library project root —
  flat: `Code` (int), `IsRetriable` (bool), `IsFatal` (bool), `Message` (via
  base). Full XML docs (CS1591 is an error here — see Verification).
- Internal `static KafkaException? FromHandle(IntPtr error)` factory: a null
  handle = success (returns null / no throw). Otherwise reads
  `kafka_common_KafkaError_{code,message,is_retriable,is_fatal}` — **MESSAGE
  BEFORE FREE** — copies the values out, then `kafka_common_KafkaError_destroy`
  in a `finally` (freed exactly once even if construction throws). Holds copied
  values, never the handle.
- **Flat-now / typed-later** (ffi §A5): document via `<remarks>` that the type is
  currently flat/code-based and that typed subclasses MAY be added later UNDER
  this base WITHOUT breaking `catch (KafkaException)`. Framed as non-breaking; NO
  version or timeline promised.
- **Precondition surface** (ffi §A5): validate BEFORE any pin/marshal/P-Invoke →
  `ArgumentNullException` / `ArgumentException` / `ArgumentOutOfRangeException`,
  and `ObjectDisposedException` / `InvalidOperationException` — NEVER
  `KafkaException`. The ABI does not validate preconditions and panics on
  violation (UB across FFI), so this is mandatory.

### 2 · Native `[DllImport]` additions (`Internal/Interop/NativeMethods.cs`)

Add ONLY what the lifecycle + error proof needs; full ABI symbol as
`EntryPoint`; ffi §0.1 type map (opaque `*_t` / `const char*` → `IntPtr`,
`int32_t` → `int`, `int64_t` → `long`, `bool` → `[MarshalAs(I1)]`, out-param →
`out IntPtr`). Verified against `target/include/confluent_kafka.h`:

Required:
- `KafkaConsumerNew(SafeConsumerPropertiesHandle props, out IntPtr outError) -> IntPtr`
  — `kafka_consumer_KafkaConsumer_new` [header 532]. Fallible: non-null
  `outError` = failure. Props typed as the SafeHandle so the marshaller keeps it
  alive across the call (D6).
- `MockConsumerNew(IntPtr autoOffsetReset) -> IntPtr` —
  `kafka_consumer_MockConsumer_new` [554]. `IntPtr.Zero` => default `"latest"`.
- `ConsumerClose(IntPtr consumer) -> IntPtr` — `kafka_consumer_Consumer_close` [1865].
- `ConsumerCloseWithTimeout(IntPtr consumer, long timeoutMs) -> IntPtr` —
  `kafka_consumer_Consumer_close_with_timeout` [1875].
- `ConsumerDestroy(IntPtr consumer) -> void` — `kafka_consumer_Consumer_destroy` [569].

Optional (D5 UTF-8 config-value round-trip only):
- `ConsumerGroupMetadata(IntPtr consumer) -> IntPtr` —
  `kafka_consumer_Consumer_group_metadata` [2150].
- `ConsumerGroupMetadataGroupId(IntPtr meta) -> IntPtr` —
  `kafka_consumer_ConsumerGroupMetadata_group_id` [1073].
- `ConsumerGroupMetadataDestroy(IntPtr meta) -> void` —
  `kafka_consumer_ConsumerGroupMetadata_destroy` [1114].

Reused, no new decl: the five `kafka_common_KafkaError_*`
(`Code`/`Message`/`IsRetriable`/`IsFatal`/`ErrorDestroy`) and the
`ConsumerProperties_new/_put/_destroy` trio already exist from M1/P1. This phase
makes the five KafkaError decls **live callers** (first runtime EntryPoint + I1
validation). Refresh the `NativeMethods` class-doc XML (the M1/P1 "declared but
not called" note is now stale).

### 3 · First SafeHandles (`Internal/Interop/`, Category 1 owned ONLY — ffi §A2/§B2)

- `SafeHandleZeroIsInvalid : SafeHandle` — tiny shared base, `IsInvalid =>
  handle == IntPtr.Zero` (D2).
- `SafeConsumerPropertiesHandle : SafeHandleZeroIsInvalid` — short-lived config
  handle; `ReleaseHandle -> ConsumerProperties_destroy`; freed after
  `KafkaConsumer_new` consumes it.
- `SafeConsumerHandle : SafeHandleZeroIsInvalid` — the client handle;
  `ReleaseHandle -> Consumer_destroy` (bare, last resort). CRITICAL teardown
  (ffi §B2): `Consumer_destroy` is FIRE-AND-FORGET (cancels in-flight, does NOT
  join the background task); the graceful join is `Consumer_close` /
  `close_with_timeout`. So graceful teardown does close → destroy;
  `ReleaseHandle` is only the last-resort bare destroy.
- NO Category 3/4 handles (poll batch / borrow-roots / views) — deferred to the
  receive-path phase.

### 4 · Internal config marshalling + lifecycle wrapper (`Internal/`, no `unsafe`)

- An internal lifecycle wrapper orchestrates:
  `IReadOnlyDictionary<string,string>` → per-entry `ConsumerProperties_put`
  (key/value via `Utf8Marshal.Pin`; keys are Java dotted names, CLAUDE.md §4) →
  `KafkaConsumer_new` (check `outError` → `FromHandle`, throw on failure) →
  wrap `Consumer_t` in `SafeConsumerHandle`. Props SafeHandle disposed in a
  `finally` after `KafkaConsumer_new` (header: caller retains props ownership).
- Graceful teardown: `Dispose()` → `Consumer_close_with_timeout` (map error via
  `FromHandle`) → dispose `SafeConsumerHandle` (→ `Consumer_destroy`). Guards
  use-after-dispose with `ObjectDisposedException`.
- Lives under `Internal/` (uses only safe managed `Utf8Marshal.Pin` → no
  `unsafe`, keeping `unsafe` quarantined to `Internal/Interop/`, CLAUDE.md §2).
  Stays internal; the public `IConsumer`/`KafkaConsumer` lands with poll/subscribe.

## Decision log (D1–D6)

- **D1 — broker-free synchronous error source = `KafkaConsumer_new` with
  `group.protocol=classic` (or unset).** `src/ffi/consumer.rs:405` gates the
  classic protocol → `unsupported_version("Classic group protocol is not yet
  supported in this client; set group.protocol=consumer (KIP-848).")`,
  synchronously, before any network. Default `group.protocol` is `"classic"`
  (`consumer_config.rs:294`). Reuses the `KafkaConsumer_new` DllImport — no
  separate fallible-op DllImport. Flags: `unsupported_version` →
  `IsRetriable=false`, `IsFatal=false` (both-false = the I1-guard FALSE case;
  the marshalling-bug direction). A retriable=true source has no broker-free
  synchronous path without `poll` → the I1 *true* direction is deferred to the
  poll phase (only a FALSE case is mandated).
- **D2 — SafeHandle base = `SafeHandle` with `IsInvalid => handle == IntPtr.Zero`**
  (one shared `SafeHandleZeroIsInvalid` base). Matches ffi §A2/§B2 verbatim and
  the ckd precedent. Reject `SafeHandleZeroOrMinusOneIsInvalid` (also treats -1
  as invalid — not our ABI contract).
- **D3 — synchronous `IDisposable.Dispose()` only this phase; `IAsyncDisposable.
  DisposeAsync()` DEFERRED.** The only close primitive in scope is the
  synchronous `Consumer_close_with_timeout` (`block_on` internally,
  `consumer.rs:3108`). `Consumer_close_async` needs the completion bridge, which
  is explicitly deferred. Wiring `DisposeAsync` now would be sync-over-async
  (ffi §B7 anti-pattern) or depend on the deferred bridge. **This is a
  deliberate deviation from CLAUDE.md §4's "both" default**, justified by the
  deferred completion bridge; `DisposeAsync` lands with poll/subscribe.
- **D4 — internal lifecycle wrapper under `Internal/`** (not `Internal/Interop/`):
  no `unsafe` (safe `Utf8Marshal.Pin`), stays internal, holds the graceful
  close→destroy + `ObjectDisposedException` guard.
- **D5 — attempt the deferred UTF-8 config-value round-trip; documented fallback.**
  Try to close it now via non-ASCII `group.id` → `Consumer_group_metadata` →
  `ConsumerGroupMetadata_group_id` → `PtrToString` == input (adds the 3
  conditional DllImports + an owned Category-3 handle marshal-then-destroy in the
  TEST). **The Actor MUST verify empirically that the configured `group.id`
  surfaces broker-free / pre-join.** If it does NOT, keep the UTF-8 config-value
  round-trip DEFERRED and record why in `COMMENTS.DONE.3.md`. Either way, the
  non-ASCII ERROR-MESSAGE round-trip is covered now via an invalid config value
  containing non-ASCII (e.g. `group.protocol="café"` → validation error whose
  message echoes the value, `consumer_config.rs:648`).
- **D6 — pass props to `KafkaConsumer_new` as the SafeHandle type**, so the
  marshaller does DangerousAddRef/Release around the call (no manual
  `GC.KeepAlive`). Header: caller retains props ownership → dispose the props
  SafeHandle in a `finally`.

## Explicitly recorded decisions (per approval)

- **D3 DisposeAsync-deferred deviation from CLAUDE.md §4** — recorded above;
  re-affirm in `COMMENTS.DONE.3.md` at close.
- **Flat-now / typed-later `KafkaException` (ffi §A5)** — the flat `Code`/
  `IsRetriable`/`IsFatal` shape is intentional; `FromHandle` is the seam for
  later typed subclasses under the same base, non-breaking.
- **D5 attempt-with-documented-fallback** — recorded above; the Actor records
  the empirical outcome (closed or still-deferred) in `COMMENTS.DONE.3.md`.

## Explicitly deferred — do NOT build

- Poll / subscribe / commit / position / assignment and the entire completion
  bridge (push dispatcher / `TaskCompletionSource` / `RunContinuationsAsynchronously`).
- Public client surface: `IConsumer`, `KafkaConsumer`, `MockConsumer` (public),
  `ConsumerRecord(s)`, `ConsumerGroupMetadata` (public), rebalance listeners.
  The client lifecycle stays INTERNAL.
- Category 3/4 receive-path handles (poll-batch borrow-roots/views).
- All producer interop + the open §A7 producer completion decision.
- Typed `KafkaException` subclasses (kept flat; `FromHandle` is the later seam).

## File list

Add — library (public root): `KafkaException.cs`.
Add — library (`Internal/Interop/`): `SafeHandleZeroIsInvalid.cs`,
`SafeConsumerPropertiesHandle.cs`, `SafeConsumerHandle.cs`.
Add — library (`Internal/`): the internal lifecycle wrapper (`unsafe`-free).
Modify — library: `Internal/Interop/NativeMethods.cs` (DllImports §2 + doc refresh).
Add — tests: `KafkaExceptionTests.cs` (root, public-type), `Interop/
SafeConsumerHandleTests.cs`, `Interop/ConsumerConfigMarshalTests.cs`, and
(conditional D5) `Interop/Utf8RoundTripTests.cs`.
Do NOT touch M1/P1 `NativeLoadProbeTests.cs` / `Utf8MarshalTests.cs`.

## Test matrix (VALIDATION)

- **SafeHandle lifecycle** — `MockConsumer_new` (fast/deterministic) AND one
  `KafkaConsumer_new`(valid: `group.protocol=consumer`,
  `bootstrap.servers=localhost:9092`) case; create → assert not-invalid → Dispose
  (close_with_timeout(short) → destroy) → no crash; xUnit timeout guard fails
  fast if a broker-less close blocks. Double-Dispose safe; use-after-Dispose →
  `ObjectDisposedException`. Create/dispose many (MockConsumer loop) → baseline.
- **Error round-trip** — `KafkaConsumer_new`(`group.protocol=classic`) → non-null
  `outError` → `FromHandle` → assert `Code` == unsupported_version code,
  `IsRetriable==false` AND `IsFatal==false` (I1 guard), Message contains
  "Classic group protocol". Non-ASCII error message via `group.protocol="café"`.
  Handle freed exactly once (FromHandle `finally`; caller never double-frees).
- **Preconditions** — null dict → `ArgumentNullException` (+ bad-shape →
  `ArgumentException`) asserted BEFORE any native call; post-Dispose →
  `ObjectDisposedException`.
- **D5 (conditional)** — non-ASCII `group.id` → `group_metadata` → `group_id`
  readback == input; else documented-deferred.

## Verification (phase-scoped DoD gates — .NET-specific; NOT cargo xtask / make verify)

1. `cargo build --features ffi` — native cdylib + regenerated header (run FIRST).
2. `dotnet build` — 0 warnings / 0 errors across library TFMs
   `netstandard2.0;net8.0;net10.0` + test TFMs `net8.0;net10.0`, with
   `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` + `GenerateDocumentationFile`
   active (CS1591 enforced on the public `KafkaException`).
3. `dotnet test -f net10.0` — lifecycle + error + precondition (+ optional D5) pass.
4. `dotnet format --verify-no-changes` — clean.
5. CI-only caveat: only the .NET 10 runtime is installed locally → the net8.0
   test *run* and net462 (Windows) are CI-only; build legs must pass, do NOT
   block DoD on those runs.

New `.cs` files carry the Apache-2.0 header; no TODO/FIXME.

## Governance (binding-local, N=3)

- Personas: `dotnet-actor` (Actor N=3), `dotnet-critic` (Critic N=3). NEVER the
  Rust `actor-executor` / `kafka-critic`. Critic reviews commits via
  `git log`/`git show` (NOT `cargo xtask await-commit`).
- Review comments: `bindings/dotnet/COMMENTS.3.md` (working, gitignored by the
  repo-root `COMMENTS.[0-9]*.md` rule) → resolved to `COMMENTS.DONE.3.md`
  (committed; the `D` breaks the ignore glob). NEVER name a committed artifact
  `COMMENTS.<digit>*.md`.
- HARD CONSTRAINT (persona/memory tracking):
  - The BINDING-LOCAL personas `bindings/dotnet/.claude/agents/dotnet-{actor,critic}.md`
    are INTENTIONALLY tracked — KEEP as-is, do NOT untrack / `git rm --cached` /
    modify them.
  - The REPO-ROOT copies `.claude/agents/dotnet-{actor,critic}.md` (currently
    untracked) must NEVER be committed — never `git add` them.
  - agent-memory `bindings/dotnet/.claude/agent-memory/dotnet-{actor,critic}/*`
    must NEVER be committed (except the already-tracked `.gitkeep`). Agents MAY
    write learnings there locally, untracked.
  - Verify each commit's file list excludes the repo-root persona copies and any
    agent-memory `.md`.
- Final handoff: update `design/current/STATUS.md`; copy closed
  `COMMENTS.DONE.3.md` to `design/history/M2/P1-error-model-safehandle/`; reset
  the working `COMMENTS.3.md`.
