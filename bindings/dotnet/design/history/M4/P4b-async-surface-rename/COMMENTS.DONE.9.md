# COMMENTS.DONE.9 — M4/P4b async-surface rename (Actor N=9)

Closed record for the **M4/P4b — async-surface rename** phase. This phase is a
pure C# rename of M4/P4a's async consumer surface into its final shape: **Mode A,
no ABI change, no Rust authored, no new `DllImport`, no behavior change** — only
identifiers, file names, and one new small interface (`IConsumerCommon`).

Approved plan: `design/history/M4/P4b-async-surface-rename/PLAN.md` (status
APPROVED).

There were **no Critic comments to fix** this round — this is the initial Actor
implementation of an APPROVED plan, not a fixup cycle. This file records the
decisions and deviations made *during* execution (the split of duties per
CLAUDE.md §8.4).

## The rename applied

### Public (`src/Confluent.Kafka/`)
- `interface IConsumer` → `interface IAsyncConsumer : IConsumerCommon,
  IAsyncDisposable, IDisposable`.
- **new** `interface IConsumerCommon { void Wakeup(); ConsumerGroupMetadata
  GroupMetadata(); }` — the two non-blocking members moved off the async
  interface onto a shared base (reserves the split shape for a future sync
  `IConsumer` surface, M5).
- `class KafkaConsumer` → `AsyncKafkaConsumer`; `class MockConsumer` →
  `AsyncMockConsumer`.
- Dropped the `Async` suffix (methods still return `Task`): `PollAsync`→`Poll`,
  `SubscribeAsync`→`Subscribe`, `UnsubscribeAsync`→`Unsubscribe`,
  `SeekAsync`→`Seek`, `CloseAsync`→`Close`. `Dispose`/`DisposeAsync` unchanged.
- File renames (via `git mv`, history preserved): `IConsumer.cs`→
  `IAsyncConsumer.cs` (+ new `IConsumerCommon.cs`), `KafkaConsumer.cs`→
  `AsyncKafkaConsumer.cs`, `MockConsumer.cs`→`AsyncMockConsumer.cs`.
  `AsyncMockConsumer`'s inherent mock helpers (`Assign`/`AddRecord`/
  `SetPollError`) keep their names.
- Carried rationale docstrings moved onto the renamed members: `Seek`-is-async
  (blocking `addAndGet`; deliberate divergence from Python's sync `seek`),
  `byte[]` key/value/header, single-owner/not-thread-safe caveat, `Close()`
  surfaces the error (unlike `DisposeAsync`), `Wakeup()` cross-thread caveat.

### Internal (`Internal/NativeConsumer.cs`)
- Bridge methods `…Async` → `…WithCallback`: `PollWithCallback`,
  `SubscribeWithCallback`, `UnsubscribeWithCallback`, `SeekWithCallback`,
  `CloseWithCallback`, and `CloseAsyncInternal` → `CloseWithCallbackInternal`.
- `Dispose`/`DisposeAsync`/`SubmitOperation`/`SubmitVoidOperation`/`Wakeup`/
  `GroupMetadata`/`GroupId` unchanged.

### Prose / cref updates in non-renamed files (accuracy only, no behavior)
- `ConsumerGroupMetadata.cs`, `ConsumerRecord.cs`, `TopicPartition.cs`: `<c>`
  prose crefs updated (`IConsumer.GroupMetadata()`→`IConsumerCommon.GroupMetadata()`,
  `IConsumer.PollAsync(...)`→`IAsyncConsumer.Poll(...)`,
  `IConsumer.SeekAsync(...)`→`IAsyncConsumer.Seek(...)`,
  `KafkaConsumer`/`MockConsumer`→`AsyncKafkaConsumer`/`AsyncMockConsumer`).
- `NativeConsumer.cs` class-doc: the stale "the public `IConsumer` /
  `KafkaConsumer` / `MockConsumer` types land later" line updated to the new
  names ("`IAsyncConsumer` / `AsyncKafkaConsumer` / `AsyncMockConsumer` compose
  this wrapper"); two future-`CloseAsync(TimeSpan)` prose references updated to
  `Close(TimeSpan)` / the public `Close()`.
- Tests: all consumer references renamed (public → new public names; interop →
  `…WithCallback`), **every assertion kept** — 122 tests, same count. A stale
  `CloseAsyncInternal` doc reference in a public teardown test →
  `CloseWithCallbackInternal`; two "a AsyncMock…" → "an AsyncMock…" grammar
  fixes from the mechanical rename.

## Deviations / decisions during execution

- **D9.1 — PLAN sub-steps 1 (interface) and 2 (impl classes) landed as ONE green
  commit.** The plan lists them as separate commits, each "Build 0/0". They are
  in fact the minimal compiling unit for the public rename and cannot be green
  independently: the `IAsyncConsumer` doc `<see cref>`s the impl-class names
  (`AsyncKafkaConsumer`/`AsyncMockConsumer` — CS1574 if they don't exist yet),
  and the impls implement the renamed interface (won't compile against the old
  `IConsumer`). Committing only one half leaves the library red under
  `TreatWarningsAsErrors`. So they ship as commit 1 ("public IAsyncConsumer +
  IConsumerCommon; AsyncKafka/AsyncMockConsumer; drop Async suffix"). Sub-steps 3
  (internal `…WithCallback`), 4 (tests), 5 (docs) are separate commits as planned.
- **D9.2 — the library-only build gates the intermediate steps.** Because the
  test project references the public API, a full `dotnet build` (all six TFM
  legs) only goes green once the tests are renamed (step 4). So steps 1–3 gate on
  the **library** project building 0/0 (all three library TFMs); step 4 brings
  the tests + full solution + `dotnet test` green. This matches the PLAN's own
  gate wording (steps 1–3 "Build 0/0"; step 4 "all DoD gates green").
- **D9.3 — internal-test method NAMES renamed to track the internal rename.** The
  PLAN allows optional test file/class renames "but must keep their cases". The
  interop tests' method-name prefixes embedded the internal method names
  (`SubscribeAsync_...`, `PollAsync_...`, …); they were renamed to
  `SubscribeWithCallback_...` / `PollWithCallback_...` so the test names still
  name the method under test. Public-test method names dropped the suffix to
  `Poll_...` / `Subscribe_...` etc. No assertion changed.

## Guardrails — both respected (verified)

1. **`NativeMethods` P/Invoke declarations + `EntryPoint` strings untouched.**
   The extern names (`ConsumerPollAsync`, `ConsumerSubscribeAsync`,
   `ConsumerUnsubscribeAsync`, `ConsumerSeekAsync`, `ConsumerCloseAsync`) mirror
   the C ABI's own `_async` suffix (`kafka_consumer_Consumer_poll_async`, …) —
   that reflects the C ABI, not our public naming — and the `…WithCallback`
   methods still call them unchanged. `ConsumerCallbacks`, the marshallers, value
   types, `OperationCompletionSource`, the `SafeHandle`s, and `KafkaException`
   are all unchanged. Verified: no `NativeMethods.Consumer*Async` extern name or
   ABI `EntryPoint`/`_async"` string literal was edited (grep-confirmed in both
   `NativeMethods.cs` and the tests).
2. **Archived M4/P4a docs NOT retro-edited.**
   `design/history/M4/P4a-public-consumer/PLAN.md` and its `COMMENTS.DONE.8.md`
   are untouched — they remain the dated record referencing `IConsumer` /
   `PollAsync` as built at that time. Only the **current** `design/current/
   STATUS.md` moved to the new names (a new M4/P4b entry + a P4b verification
   section; the historical M4/P4a entry is left intact).

## DoD gate results (all green)

- `cargo build --features ffi` — native + header present; **no ABI change (Mode A)**.
- `dotnet build` — **0 warnings / 0 errors** across all six TFM legs (library
  netstandard2.0/net8.0/net10.0 + tests net462/net8.0/net10.0). CS1591 intact on
  every renamed public member; Apache-2.0 header on the new `IConsumerCommon.cs`;
  no TODO/FIXME. VSTHRD200 confirmed absent → suffix drop builds clean.
- `dotnet test -f net10.0` — **122 passed, 0 failed** (same count as M4/P4a);
  **20/20 serial full-suite runs green, 0 failures / 0 crashes**. Parallelism
  stays disabled (D8.8, not re-enabled).
- `dotnet format --verify-no-changes` — clean.

## Commits (on `prashah_dev_public_consumer_scaffolding`, `--no-gpg-sign`)

1. `dotnet(M4/P4b): public IAsyncConsumer + IConsumerCommon; AsyncKafka/AsyncMockConsumer; drop Async suffix`
   (interface layer + impl classes, one green unit — D9.1).
2. `dotnet(M4/P4b): rename internal NativeConsumer bridge methods …Async → …WithCallback`.
3. `dotnet(M4/P4b): rename test references to the renamed public + internal surface`.
4. `dotnet(M4/P4b): STATUS + closed record (M4/P4b async-surface rename); N=9` (docs).
