# .NET binding — Milestone 1 / Phase 1: "Interop scaffolding + native-load probe"

Status: APPROVED (plan pre-approved by user; execution began 2026-07-20)

The .NET binding keeps its own milestone/phase numbering, independent of the
root Rust `design/`. This is Milestone 1 / Phase 1 — the first *implementation*
phase after the M0/P0 skeleton.

## Scope

The **client-agnostic interop FOUNDATION** for the .NET binding, validated
through a featherweight **consumer-namespaced native-load probe**. This is a
**Mode A** phase (producer/consumer C ABI already landed — NO Rust authoring;
just `cargo build --features ffi` then port from the generated header down).

**SCOPE: structural / interop scaffolding only.** NO public managed API, NO
`SafeHandle`, NO completion bridge, NO Kafka logic
(`bindings/dotnet/CLAUDE.md §1` — shape, not logic). This phase proves the
P/Invoke plumbing loads and round-trips; the Java-shaped API lands in later
phases.

## Deliverables

### 1 · Build / config — LIBRARY csproj only

`src/Confluent.Kafka.ShareConsumer/Confluent.Kafka.ShareConsumer.csproj`:

- Add `<AllowUnsafeBlocks>true</AllowUnsafeBlocks>` to the **LIBRARY csproj
  ONLY**. NOT the shared `Directory.Build.props`, NOT the test project (the test
  project MUST stay unsafe-free).
- **Native-copy MSBuild target** in the library csproj (un-defers M0/P0
  decision D2): compute the per-OS native filename via MSBuild
  (`$([MSBuild]::IsOSPlatform('OSX'))` → `libconfluent_kafka.dylib`; Linux →
  `libconfluent_kafka.so`; Windows → `confluent_kafka.dll`) and the cargo
  profile dir from `$(Configuration)` (Debug → `target/debug`, Release →
  `target/release`), then add it as
  `<Content Include="<repo>/target/<cfg>/<file>" Link="<file>" CopyToOutputDirectory="PreserveNewest" />`.
  Use `<Content>` (NOT `<None>`) deliberately so it flows transitively to the
  referencing TEST project's output dir where the probe finds it at runtime.
  Repo root is 4 directory levels up from the library csproj. NEVER hardcode an
  absolute path or a `[DllImport]` filename (ffi §0.2). No NuGet, no
  `runtimes/{rid}/native/`. Preserve the `lib` prefix / OS suffix in the `Link`
  name so the loader resolves it.
- **Optional** (actor's discretion): emit a clear MSBuild `<Error>` if the
  native is absent, reinforcing §7.1 "never build .NET before Rust"; otherwise
  it surfaces as `DllNotFoundException` at runtime.
- **Analyzer suppressions (CONTINGENT — only if they actually fire):**
  `TreatWarningsAsErrors` is active. The P/Invoke class may trip CA5392
  (`DefaultDllImportSearchPaths`) and/or SYSLIB1054 (net7+ "use
  `[LibraryImport]`" — forbidden by the netstandard2.0 floor). **BUILD FIRST,
  observe which fire, then add NARROWLY SCOPED suppressions** (a file-scoped
  `.editorconfig` under `Internal/Interop/` OR `[SuppressMessage]` on the
  `Native` class) each with a one-line justification. NO global relaxations, no
  blanket disables.

### 2 · Interop scaffolding — all `internal`, under `Internal/Interop/`

Namespace `Confluent.Kafka.ShareConsumer.Internal.Interop`; `unsafe` lives ONLY
here.

- **`Native.cs`** — one `internal static class Native` of classic
  `[DllImport("confluent_kafka", CallingConvention = CallingConvention.Cdecl)]`
  declarations, uniform across ALL TFMs (no `#if`). `EntryPoint` = the full ABI
  symbol. Declare ONLY these 8 functions (spellings verified against
  `src/ffi/common.rs`, `src/ffi/consumer.rs`, and
  `target/include/confluent_kafka.h`):

  | C ABI symbol | C# |
  |---|---|
  | `int32_t kafka_common_KafkaError_code(const kafka_common_KafkaError_t*)` | `int Code(IntPtr)` |
  | `const char* kafka_common_KafkaError_message(const kafka_common_KafkaError_t*)` | `IntPtr Message(IntPtr)` |
  | `bool kafka_common_KafkaError_is_retriable(const kafka_common_KafkaError_t*)` | `[return: MarshalAs(I1)] bool IsRetriable(IntPtr)` |
  | `bool kafka_common_KafkaError_is_fatal(const kafka_common_KafkaError_t*)` | `[return: MarshalAs(I1)] bool IsFatal(IntPtr)` |
  | `void kafka_common_KafkaError_destroy(kafka_common_KafkaError_t*)` | `void ErrorDestroy(IntPtr)` |
  | `kafka_consumer_ConsumerProperties_t* kafka_consumer_ConsumerProperties_new(void)` | `IntPtr ConsumerPropertiesNew()` |
  | `void kafka_consumer_ConsumerProperties_put(props*, const char* key, const char* value)` | `void ConsumerPropertiesPut(IntPtr, IntPtr, IntPtr)` |
  | `void kafka_consumer_ConsumerProperties_destroy(props*)` | `void ConsumerPropertiesDestroy(IntPtr)` |

  Type map (ffi §0.1, verbatim): opaque `*_t` → `IntPtr`; `const char*` (in AND
  out) → `IntPtr`; `int32_t` → `int`; `bool` →
  `[MarshalAs(UnmanagedType.I1)] bool`. NO `[LibraryImport]`, NO
  `LPStr`/`LPUTF8Str`, NO `Marshal.PtrToStringUTF8` (netstandard2.0 floor). The
  C# short method names may drop the `kafka_<pkg>_` prefix — but then
  `EntryPoint` MUST carry the full ABI symbol (else `EntryPointNotFoundException`
  at runtime).

- **`Utf8.cs`** — `internal static class Utf8`:
  - `Pin(string)`: `Encoding.UTF8.GetBytes` → `new byte[len+1]` (trailing NUL) →
    pin (`GCHandle` Pinned) → expose `AddrOfPinnedObject` as `IntPtr` → **UNPIN
    IN `finally`**. Implement as a disposable pinned-buffer (`IDisposable`
    struct/handle) so callers use `using` and the pin is call-scoped (never held
    across a `Task`). `GCHandle` + `AddrOfPinnedObject` is safe managed API (no
    `unsafe` needed for `Pin`).
  - `PtrToString(IntPtr)`: NUL-scan then `Encoding.UTF8.GetString`; RETURNS
    `null` for `IntPtr.Zero`; `unsafe` (no `PtrToStringUTF8` on the floor).
    Implement ONLY the NUL-terminated form — the length-delimited (`out_len`)
    receive-path form is EXPLICITLY DEFERRED to a later phase; do NOT build it.

### 3 · Validation — native-load probe (TEST project)

`tests/Confluent.Kafka.ShareConsumer.UnitTests/`, internal-only, wired against
the internal interop — NOT any public API; access via the existing
`InternalsVisibleTo`.

- **`NativeLoadProbeTests.cs`** (xUnit, matches existing test style):
  - **Smoke:** `ConsumerPropertiesNew()` → `ConsumerPropertiesPut(props, key,
    value)` with `"bootstrap.servers"`/`"localhost:9092"` pinned via `Utf8.Pin`
    → `ConsumerPropertiesDestroy(props)`. Assert handle != `IntPtr.Zero` and no
    exception. Proves cdylib loads, native-copy target works, first `[DllImport]`
    round-trip, `Cdecl`, the type map, and `Utf8.Pin`.
  - **Non-ASCII variant:** same flow with a non-ASCII key/value (guards UTF-8
    marshalling INTO native; `put` returns `void` so the assertion is "no
    crash/corruption").
  - **`Utf8` round-trip unit test:** `Pin` a non-ASCII string then `PtrToString`
    it back and assert equality (real managed round-trip validating BOTH helpers
    + a multi-byte char at the buffer boundary), PLUS
    `PtrToString(IntPtr.Zero) == null` (ffi §A3 obligation; also exercises the
    otherwise-unused helper).
- Apache-2.0 license header on ALL new `.cs` files. No public types (internal
  only → no XML-doc obligation). No TODO/FIXME.

## Explicitly deferred — do NOT build

All public managed API (`IConsumer`/`IProducer`, `ConsumerRecord(s)`,
`KafkaConsumer`/`MockConsumer`, `KafkaProducer`/`MockProducer`, `PollAsync`/
`SendAsync`, public `KafkaException`, `ProducerRecord`/`RecordMetadata`);
`SafeHandle` subclasses and any client create/close/destroy lifecycle; the
completion bridge (pump/dispatcher) and async ops; consumer receive-path
length-delimited views/borrow-roots; ALL producer interop declarations.
Declaring the `KafkaError` functions without calling them is **intentional**
(shared foundation); their `EntryPoint`s are only runtime-validated when later
phases call them — noted so the Critic does not flag it.

## Approved decisions

- **D1 (un-defers M0/P0 D2)** — native-copy MSBuild target lands now, computed
  per-OS via MSBuild, `<Content>` (transitive to the test output), repo root 4
  levels up, never a hardcoded path/filename.
- **D2** — `<AllowUnsafeBlocks>` on the LIBRARY csproj only; test project stays
  unsafe-free.
- **D3** — classic `[DllImport]` uniform across all TFMs (netstandard2.0 floor
  forbids `[LibraryImport]`/`PtrToStringUTF8`/`LPUTF8Str`).
- **D4** — `Utf8.Pin` is a disposable call-scoped pin (`using`); `PtrToString`
  is NUL-terminated form only (length-delimited receive-path form deferred).
- **D5** — analyzer suppressions are contingent + narrowly scoped, added only
  after observing which warnings fire under `TreatWarningsAsErrors`.

## Verification (phase-scoped DoD gates — .NET-specific)

1. `cargo build --features ffi` — produces the cdylib in `target/debug/` AND
   the header at `target/include/confluent_kafka.h` (must run FIRST, CLAUDE.md
   §7.1).
2. `dotnet build` — 0 warnings / 0 errors across library TFMs
   (`netstandard2.0;net8.0;net10.0`) + test TFMs (`net8.0;net10.0`), with
   `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` active.
3. `dotnet test -f net10.0` — the probe passes.
4. `dotnet format --verify-no-changes` — clean.

Do NOT use `cargo xtask` / `make verify` (those are Rust-core gates).

- ⚠️ **CI-only (not blocking this phase):** only the .NET 10 runtime is
  installed on this machine. The net8.0 test *run* and net462 (Windows) are
  CI-only; their BUILD legs must pass, but do NOT block DoD on those *runs*.

## Governance (binding-local, N=2)

- Personas: `dotnet-actor` (Actor N=2), `dotnet-critic` (Critic N=2). NEVER the
  Rust `actor-executor` / `kafka-critic`.
- Review comments: `bindings/dotnet/COMMENTS.2.md` (working file, gitignored);
  resolved → `bindings/dotnet/COMMENTS.DONE.2.md` (the durable archived record,
  copied under `design/history/M1/P1-interop-scaffolding/` at close).
- Plans & design: `bindings/dotnet/design/` (NOT the repo-root `design/`).
- Agent memory: `bindings/dotnet/.claude/agent-memory/dotnet-{actor,critic}/`.
- Commit hygiene: incremental commits, `dotnet(M1/P1): …` prefix, Co-Authored-By
  trailer for Claude Opus 4.8 (1M context). Do NOT commit
  `.claude/agents/dotnet-{actor,critic}.md` (local-only) or `.DS_Store`.
- Stay on branch `prashah_dev_dotnet_interop_scaffolding_check`.
