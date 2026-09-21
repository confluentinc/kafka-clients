# COMMENTS.DONE.15 — M5/P6 "Consumer commit" (Category D) (Actor N=15 → Critic N=15)

Closed record for the **M5/P6 — Consumer commit (Category D)** phase (the tracked archive
under the phase directory, per CLAUDE.md §8.4; the binding-root `COMMENTS.15.md` /
`COMMENTS.DONE.15.md` are local working files and stay untracked). Approved plan:
`design/history/M5/P6-consumer-commit/PLAN.md`. No pre-existing `COMMENTS.15.md` — fresh phase.

## Scope (as planned)

Three public members + one public ctor. **Mode A** (no Rust authored; `commit_sync_async`,
`commit_sync_offsets_async`, `commit_async` verified present). **Python-parity:** NO
`OffsetCommitCallback` variant, NO `TimeSpan` overload. Signatures: `Task Commit(ct)` +
`Task Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, ct)` on
`IAsyncConsumer`; `void CommitAsync()` on `IConsumerCommon` (user-resolved placement — the
fire-and-forget commit is flavor-independent). No new completion-bridge (both `_async` commits
reuse the void `op_callback_t` bridge; `commit_async` is the sync `EnforceRebalance` shape).

## Deviations from the plan (with rationale)

1. **`OffsetAndMetadata` collapsed to ONE public constructor** (planned: keep the internal ctor
   + add a public one). The planned public `(long, string?, int?)` and the shipped internal
   `(long, string, int?)` **collide under CS0111** — C# treats `string` and `string?` as the
   same parameter type for overload resolution. Resolved by making the single public ctor the
   one field-assignment site (the plan's stated goal); the receive-path marshaller
   (`OffsetMapMarshal.CopyOut`) now calls the public ctor directly. Safe: the marshaller always
   passes non-null (already normalized) metadata and a non-negative offset (the Rust core
   rejects negative offsets at construction), so the ctor's validate/coerce is a no-op there —
   receive-path behavior unchanged. Documented in the `OffsetAndMetadata` type remarks.

2. **Operational `KafkaException` commit fault is NOT reachable broker-free** (planned: assert
   if reachable, else document — the D-Q4 ceiling). The mock's `commit_async_impl` only fails
   via `ensure_not_closed()`; the binding intercepts a closed consumer as
   `ObjectDisposedException` before the native call, so no operational commit fault is
   observable end-to-end on the mock. Documented; the faulted-`Task` MECHANISM is identical to
   the E1 offset-map bridges and already proven there. Not a dropped assertion.

3. **Negative-partition precondition asserted via the `TopicPartition` ctor guard** — a negative
   partition cannot reach `SnapshotCommitOffsets` through a *constructed* `TopicPartition` (the
   struct's ctor rejects it first — the shipped offset-query precedent); the null-element-topic
   case uses `default(TopicPartition)`.

## Confirmed faithful (not a limit)

- **Leader epoch round-trips 7-in / 7-out.** FFI `read_offset_map` builds
  `OffsetAndMetadata::with_leader_epoch(offset, Some(7), meta)` from `leader_epochs[i] >= 0`; the
  mock stores it via `self.committed.extend(offsets)`; `committed()` returns `om.clone()` for an
  assigned TP; `OffsetMapMarshal` honors the presence flag. The marquee test asserts
  `LeaderEpoch == 7` strictly; the null-epoch variant asserts the `-1` sentinel → `null`.

## No-new-bridge audit (self-review)

- `ConsumerCallbacks.cs` byte-for-byte unchanged (diff-verified) — `Operation` reused for both
  `_async` commits. Shipped void/owned/scalar/E1/E2 bridge paths untouched;
  `WithPinnedCommitOffsets` / `SnapshotCommitOffsets` are NEW parallel helpers.
- Both commit-offsets string arrays (topics + metadata) pinned **call-scoped** and released in
  **one `finally`** — no leak. The sync `CommitAsync` frees the error handle exactly once via
  `KafkaException.FromHandle`. No per-element byte copy beyond the UTF-8 encode.

## Commits (branch `prashah_dev_public_consumer_remaining`)
- `d8314a1b` archive approved plan
- `9023d2b4` interop layer — `NativeMethods` (3 DllImports) + `NativeConsumer` commit methods + `WithPinnedCommitOffsets`/`SnapshotCommitOffsets` + `OffsetAndMetadata` public ctor
- `bd246021` public surface — `Commit` ×2 on `IAsyncConsumer`, `CommitAsync` on `IConsumerCommon`
- `3775649d` tests — `PublicConsumerCommitTests.cs` + the non-empty `Committed` round-trip
- `79b203be` doc-sync — CLAUDE.md §3/§4 commit mapping + STATUS.md M5/P6 entry (N=15)

## DoD gates (all green — Actor)
- `cargo build --features ffi` — no ABI change; `dotnet build` 0/0 across all 6 TFM legs,
  TreatWarningsAsErrors + CS1591 on the 2 `Commit` overloads + `CommitAsync` + the ctor.
- `dotnet test -f net10.0` — **241 → 261** (+20), green 3/3 full serial runs (D8.8 stable).
- `dotnet format --verify-no-changes` — clean.

---

## Critic N=15 — review outcome (closed)

**Review (`d8314a1b..79b203be`): CLEAN, 0 genuine findings, phase PASSES — Category D done.**
Independently verified against the C ABI header + the Kafka Java public-API shape + the plan:

- **No new completion-bridge:** `ConsumerCallbacks.cs` and `OperationCompletionSource.cs`
  byte-for-byte unchanged (empty diff); both `Commit` overloads reuse `SubmitVoidOperation` +
  `Operation`; `CommitAsync()` is the sync `EnforceRebalance`-shape path (error handle freed
  once via `FromHandle`). Zero deletions to `NativeConsumer.cs`/`NativeMethods.cs`.
- **`WithPinnedCommitOffsets` memory safety:** both string arrays (topics + metadata) pinned
  call-scoped, released in one `finally` with null-safe `?.Dispose()` — no pin leak / double-free.
  Call-scoped is valid (submit lambda runs synchronously; the ABI's `read_offset_map` copies all
  strings before the async op spawns). Sentinels (`-1` epoch, `""` metadata) correct.
- **Ctor collapse (deviation #1):** validates `offset < 0` → `ArgumentOutOfRangeException("Invalid
  negative offset")`, coerces null metadata → `""`. Routing the receive-path marshaller through
  it is provably safe — metadata always coalesced non-null, and offset can never be negative
  because the Rust core rejects negative offsets at construction, so no query result can
  spuriously throw. `LeaderEpoch` presence-flag mapping unchanged.
- **Shape / preconditions / doc-sync:** placement (`Commit` ×2 on `IAsyncConsumer`, `CommitAsync`
  on `IConsumerCommon`), exception types, no `TimeSpan`/no `IOffsetCommitCallback`, and the
  CLAUDE.md §3/§4 amendments all match the plan; root CLAUDE.md untouched; STATUS accurate.
- **Tests + reachability honesty:** the marquee round-trip genuinely exercises the new marshaller
  + E1 copy-out with real data (7-in/7-out epoch real; null-epoch `-1`→null; null metadata→`""`);
  the "operational commit-fault not reachable broker-free" claim is honest; no lost coverage.
- **DoD independently observed:** `cargo build --features ffi` success; `dotnet build` **0/0**
  across all six TFM legs; `dotnet test -f net10.0` **261 passed / 0 failed**, looped **20× clean**
  (commit family 10× in isolation); `dotnet format --verify-no-changes` clean. No `COMMENTS.15.md`
  findings written.

**Loop closed:** Actor N=15 → Critic N=15 (CLEAN, 0 findings). No fix cycle required; no
outstanding review comments. **Category D complete.**
