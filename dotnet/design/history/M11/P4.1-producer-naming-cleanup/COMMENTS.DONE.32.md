# M11/P4.1 — Producer naming + shape cleanup — closed record (N=32)

Manager-archived closed record for the M11/P4.1 execution loop. Companion to
`PLAN.md`: `PLAN.md` is the forward-looking plan, this file records what
happened *during* execution.

## Loop outcome

**Closed CLEAN on the first pass — one Actor implementation, one Critic review,
no fix cycle.** No `COMMENTS.32.md` was ever created (the Critic found no real
issues), so there were no comments to resolve into a binding-root
`COMMENTS.DONE.32.md`. This archive exists for the phase record, not because
comments were moved.

- **Branch:** `prashah_dev_producer_sync` (stacks on `prashah_dev_producer_send`; base `ac824c3c`).
- **Commits (local only, not pushed):**
  - `68c80c91` — `dotnet(M11/P4.1): rename NativeProducer sync ops to bare names + async send to SendViaPump (Mode A)`
  - `36f802e6` — `dotnet(M11/P4.1): HistoryCount property -> method (Python + FDG parity); update CLAUDE.md §3 (Mode A)`

## What was delivered

### Change 1 — `NativeProducer` rename (internal-only, non-breaking)

`SendSync→Send`, `FlushSync→Flush`, `PartitionsForSync→PartitionsFor`,
`CloseSync→Close`, and the async pump send `Send(record, ct)→SendViaPump(record, ct)`.
Peripherals (`*WithCallback`), `Mock*` helpers, `Dispose`/`DisposeAsync` unchanged.
Call sites updated: sync forwarders → bare names; both async forwarders
(`AsyncKafkaProducer.cs:74`, `AsyncMockProducer.cs:75`) → `SendViaPump`;
xmldoc/prose refs updated in `NativeProducer.cs`, `NativeMethods.cs`, and three
sync producer test files (comment refs to the internal names).

Notable finding during implementation: there were **no internal callers** of the
renamed sync workers inside `NativeProducer.cs` — its teardown
(`Dispose`/`StopPump`/etc.) calls `NativeMethods.ProducerFlush`/`ProducerClose`
directly, not the `Flush`/`Close` workers, so nothing there needed changing.

### Change 2 — `HistoryCount` property → method (public mock API)

`MockProducer.HistoryCount` / `AsyncMockProducer.HistoryCount` → method form; all
test call sites (incl. the two `Assert.Throws<ObjectDisposedException>` cases) →
`HistoryCount()`; doc crefs fixed; `CLAUDE.md §3` sketch + note updated to record
it as a method per the FDG precedent (`Assignment()`/`Subscription()`/`Paused()`)
+ Python `history_count()` parity.

## Critic N=32 — round 1: CLEAN (0 issues, all severities)

Reviewed `git diff ac824c3c..HEAD` against the C ABI header and the Java
`Producer` public-API shape. Independently built and tested (did not trust the
Actor's report). Confirmed:

- **No overload-resolution flip.** `NativeProducer` has exactly two disjoint-named
  methods — `SendViaPump(ProducerRecord, CancellationToken) → Task<RecordMetadata>`
  (line 356) and sync `Send(ProducerRecord) → RecordMetadata` (line 484). The old
  2-arg `Send` overload is gone; a missed async-caller update would be a compile
  error (build is 0W/0E). Both async forwarders call `SendViaPump`, both sync
  forwarders call `Send`.
- **`SendViaPump` names the real mechanism.** Its body is genuinely the pull-pump
  path (`EnsurePump()` → inline `Producer_send` → enqueue `(future, TCS)` → return
  `Task`), not a `Producer_send_async` callback. Bodies were not swapped: sync
  `Send` is genuinely the blocking `FutureRecordMetadataGet` + singular
  `FutureRecordMetadataDestroy` path. The only surviving `SendWithCallback` token
  is deliberate prose ("Named `SendViaPump`, **not** `SendWithCallback`").
- **Consumer untouched.** `NativeConsumer.CloseSync`/`CloseSyncWithTimeout` and
  their call sites in `KafkaConsumer.cs`/`MockConsumer.cs` unchanged; the cross-type
  prose ref `<c>NativeConsumer.CloseSync</c>` in `NativeProducer.cs:953` correctly
  stays `CloseSync`.
- **Rename complete.** No producer-side `SendSync`/`FlushSync`/`PartitionsForSync`/
  `CloseSync` remains in source.
- **HistoryCount conversion sound.** Both mocks → method form; every test call site
  (incl. the two `Assert.Throws<ObjectDisposedException>(() => producer.HistoryCount())`
  cases — the throw is still observed, deferred inside the lambda in both property
  and method forms) updated; `NativeProducer.MockHistoryCount()` (line 680)
  unchanged.
- **Mode A hygiene.** Nothing changed outside `bindings/dotnet/`; no Rust `src/**`,
  `src/ffi/**`, `confluent_kafka.h`, or `cbindgen.toml`; no COMMENTS/agent-memory/
  agents/bin/obj staged.
- **CLAUDE.md §3 consistent.** Sketch `public int HistoryCount();`; note calls it "a
  method, not a property" per FDG + Python parity; no leftover "property because"
  wording — reads as an intentional decision record.

## Manager independent verification

- Commit SHAs / subjects match; `git show --stat` per commit shows only
  `bindings/dotnet/**` `.cs` + `bindings/dotnet/CLAUDE.md`.
- Mode-A invariant: `git diff --stat ac824c3c..HEAD -- src src/ffi
  target/include/confluent_kafka.h cbindgen.toml` is empty.
- Rename structure confirmed by grep: one `Send`, `SendViaPump` present, both async
  forwarders on `SendViaPump`, no lingering producer `*Sync` names, HistoryCount is
  a method in both mocks with no property-style test usages remaining.

## Verification (both Actor and Critic, independently)

- `cargo build --features ffi` — exit 0.
- `dotnet build` — **0 Warning(s) / 0 Error(s)** across the TFM matrix (lib
  ns2.0/net8.0/net10.0; tests net462/net8.0/net10.0). 0 warnings under
  `TreatWarningsAsErrors` confirms every `<see cref>` (incl. the new
  `HistoryCount()` method-crefs) resolves.
- `dotnet test -f net10.0` — **545 passed / 0 failed / 0 skipped** (net8.0/net462
  build-verified, run CI-only — only the .NET 10 runtime installed locally, the
  established gate).
- `dotnet format --verify-no-changes` — clean (exit 0).
