# COMMENTS.16 — dotnet-critic review (N=16)

## Scope
M5/P7 "Consumer sync seek + current-lag", 3 commits on `prashah_dev_public_consumer_remaining`:
`81435e4e` (interop + public surface), `3d51da79` (tests), `753398ed` (doc-sync). Base =
`5cbee583`. Reviewed against the C ABI header (`target/include/confluent_kafka.h`) + the
Kafka Java public-API shape, per the approved plan
`design/history/M5/P7-consumer-sync-seek-lag/PLAN.md`. Mock reachability claims verified
against `src/consumer/mock_consumer.rs` (read-only, to validate .NET-side claims — not a
Rust review).

## Verdict: CLEAN — 0 genuine findings.

Faithful, memory-safe, Java-shaped Mode A port. Every axis in the review brief checked out
against ground truth. Details below (recorded so the record is auditable, not because
anything needs fixing).

### P/Invoke correctness (primary) — verified against the header
- `ConsumerSeek(IntPtr consumer, IntPtr topic, int partition, long offset) → IntPtr`
  matches `kafka_consumer_Consumer_seek(consumer, const char* topic, int32_t partition,
  int64_t offset) → KafkaError*` (header l.1619). `int`/`long` per §0.1 (no `UIntPtr`);
  opaque handle + `const char*` in are `IntPtr`; `KafkaError*` return is `IntPtr`. ✓
- `ConsumerSeekWithMetadata(IntPtr consumer, IntPtr topic, int partition, long offset,
  int leaderEpoch, IntPtr metadata) → IntPtr` matches the header's exact param **order**
  `(consumer, topic, partition, offset, leader_epoch, metadata)` (l.1650). ✓ (the one
  param-order risk the brief flagged — correct.)
- `ConsumerCurrentLag(IntPtr consumer, IntPtr topic, int partition, out long outLag)` with
  `[return: MarshalAs(UnmanagedType.I1)] bool` matches `bool …_current_lag(consumer,
  const char* topic, int32_t partition, int64_t* out_lag)` (l.2173). `I1` present (§0.1
  1-byte-bool rule — a missing `I1` would read a 4-byte Win32 BOOL). ✓
- All three carry `EntryPoint = "kafka_consumer_Consumer_*"` + `CallingConvention.Cdecl`
  (§0.1 — short C# name without `EntryPoint` → runtime `EntryPointNotFoundException`). ✓

### Q2 removal (dead code, DoD §7) — verified complete
- `ConsumerSeekAsync` `[DllImport]` and `NativeConsumer.SeekWithCallback` both deleted;
  repo-wide grep over `src/` + `tests/` for `SeekWithCallback` / `ConsumerSeekAsync` →
  **zero** hits. The only surviving `Seek*` async members are `SeekToBeginning` /
  `SeekToEnd` (correctly unchanged — PLAN §9.5). ✓
- Cross-references cleaned: the `NativeConsumer` class-doc concurrent-op example was
  repointed `SeekWithCallback` → `PollWithCallback`; `NativeMethods` carries an explicit
  "intentionally NOT declared" note. No stale `<see cref>` left dangling.

### Marshalling / handle lifetime (ffi §B3/§B5) — verified
- Topic (both seeks + lag) and metadata (`SeekWithMetadata`) pinned **call-scoped** via
  `using Utf8Marshal.PinnedUtf8String` — released at method exit, never held past the
  P/Invoke. `SeekWithMetadata` pins two strings, both `using`. ✓
- `KafkaException.FromHandle(NativeMethods.ConsumerSeek/…WithMetadata(...))` consumes the
  returned `KafkaError*` and frees it exactly once (the shipped `EnforceRebalance` /
  `CommitAsync` sync-op discipline); `throw` iff non-null; null = success. No leak /
  double-free. `CurrentLag` has no error handle to manage. ✓
- Preconditions precede any pin/P-Invoke in all three (null topic → `ArgumentNullException`;
  negative partition → `ArgumentOutOfRangeException`; null `OffsetAndMetadata` →
  `ArgumentNullException`). ✓
- **Q1 ordering (the brief's key check):** in `Seek(string,int,long)` the `offset < 0`
  guard is placed **before** `ThrowIfClosed()`, so a negative offset throws
  `ArgumentOutOfRangeException` even on a closed consumer — exact message
  `"seek offset must not be a negative number"`, and asserted by tests at both the interop
  (`ConsumerUnsubscribeSeekGroupMetadataTests.Seek_NegativeOffset_*`) and public
  (`PublicConsumerApiTests` / `PublicConsumerSeekLagTests`) levels, incl. the
  `_EvenWhenClosed` variant. `SeekWithMetadata` correctly omits the offset guard (the
  `OffsetAndMetadata` ctor is the upstream gate — verified `OffsetAndMetadata.cs:80-84`,
  `(long offset, string? metadata = null, int? leaderEpoch = null)` rejecting `< 0` with
  `"Invalid negative offset"`). ✓

### `CurrentLag` semantics — verified
- `ConsumerCurrentLag(...) ? lag : (long?)null` — `false` → `null` for **both** unknown-lag
  and guard-not-acquired (Python parity; no `InvalidOperationException` split — which the
  ABI could not surface anyway, the return being a bare `bool`). `out long lag` is read only
  on the `true` branch. ✓ Matches Java `OptionalLong.empty → null`.

### Mock reachability honesty (critical) — verified against mock source
- `mock_consumer.rs:746-755`: `seek_with_metadata` uses only `offset_and_metadata.offset()`
  and **discards** metadata + leader epoch. The `Seek(tp, OffsetAndMetadata)` tests
  correctly assert **only** the offset round-trip (via `Position`) + that the call
  **succeeds** on an assigned partition as the metadata/leader-epoch *marshalling* proof —
  they do **not** over-claim a metadata/leader-epoch value read-back. The type/test remarks
  state the limit explicitly (M5/P4 `Committed` precedent). ✓
- `CurrentLag == 90` math verified against `mock_consumer.rs:438-457`: assigned +
  `end_offsets = Some(100)` + `position = Some(10)` → `Some(end - position) = 90`. The
  assigned-no-end `→ 0` (`None => Some(0)`, caught-up model) and unassigned `→ null`
  (`!is_assigned → None`) cases are also correctly asserted. ✓

### Test completeness (DoD §3) — verified
- Every async Seek caller migrated to sync (public: RoundTrip/Position/AllocationBudget/
  TfmSmoke/Api/Teardown; interop: the two `MockReadyToPoll` helpers via `Task.FromResult`,
  CompletionBridge, UnsubscribeSeekGroupMetadata). No leftover `await consumer.Seek` /
  `ThrowsAsync<…>(() => …Seek…)` (grep-confirmed). ✓
- New `PublicConsumerSeekLagTests.cs` covers: both overloads' offset round-trip; the
  leader-epoch present(7)/null(→-1)/non-ASCII/empty-`""` marshalling `[Theory]`;
  `CurrentLag` real(90)/assigned-no-end(0)/unassigned(null); all preconditions with exact
  messages; post-dispose (all three members); unassigned seek → synchronous `KafkaException`
  (both overloads); `IConsumerCommon`-reference reachability; a per-op allocation budget. ✓
- Removed `SeekWithCallback_PreCanceledToken_…` (sync Seek has no `CancellationToken`) —
  the pre-canceled → `OperationCanceledException` path stays covered by the surviving
  `SubscribeWithCallback_PreCanceledToken_…` (same `SubmitVoidOperation` gate). ✓
- Void-completion-bridge coverage **not** lost by re-expressing seek-unassigned as a sync
  throw: the void `OperationCallback` **success** path stays proven by subscribe/unsubscribe,
  and its **error** path (non-null error → faulted `Task`) stays proven by
  `Pause`/`Resume` unassigned-partition faults, which route the identical
  `SubmitPartitionOp` → `ConsumerPauseAsync`/`ConsumerResumeAsync` → `OperationCallback`
  bridge (`NativeConsumer.cs:615-633`). (Minor: the commit message cites "poll/position"
  for the error-path mechanism — those actually use the owned-handle/scalar callbacks, not
  the void one; the real void-error coverage is Pause/Resume. Message imprecision only, no
  code/coverage defect.) ✓

### Doc-sync (DoD §1) & decision hygiene (§4) — verified
- `CLAUDE.md` §1 (current_lag/seek_with_metadata now shipped-sync, only close_with_timeout
  a gap), §3 sketch (both `Seek` overloads + `CurrentLag` in the `IConsumerCommon` block,
  dropped from `IAsyncConsumer`, "Already wired" prose), §4 idiom-map row (seek/currentLag
  removed from the blocking-async trigger), §4 "Stays sync — exactly these" + the new §4
  divergence note (with the Q1 guard called out) — all present and internally consistent. ✓
- `STATUS.md` M5/P7 entry records Mode A no-header-delta, Q1 KEEP / Q2 REMOVE, the breaking
  async→sync + interface relocation, and the mock value-read-back limit. The §4 divergence
  (sync `Seek`/`CurrentLag` vs "blocks → async") is **recorded**, not silent. ✓

### §4 divergence / §12 hot-path — verified
- Seek/CurrentLag are per-op top-level members, not a hot path; the only per-call heap is
  the call-scoped topic/metadata UTF-8 encode + the `PinnedUtf8String` wrapper (no
  native-backed view, no per-call `Task`/`GCHandle`). The `SeekAndCurrentLag_PerOpAllocation`
  test's marginal-subtraction sanity bound (2048 B/pair, net8.0+) is consistent with that
  and with the shipped Position budget precedent. ✓

## Non-findings deliberately NOT filed (FP avoidance)
- The `partition < 0` / null-topic guards inside `SeekWithMetadata` and `CurrentLag` are
  only reachable via `default(TopicPartition)` (null topic) — negative partition is blocked
  upstream by the `TopicPartition` ctor. They are individually exercised only for
  `Seek(tp,long)`, not for the other two members. These are trivial, structurally-identical
  defensive guards; the risk of a real defect is nil, so this is **not** filed as a
  requirement gap (it would be a hypothetical/style nit under the FP-avoidance mandate).
- The `IConsumerCommon` XML docs for `Seek`/`CurrentLag` list only `ObjectDisposedException`
  (+ member-specific ones) and omit the `default(TopicPartition)` null-topic
  `ArgumentNullException`. Doc-completeness nit, consistent across members — not a defect.

## Notes
- I did not independently run `dotnet build`/`test` (static review; the Actor reports 282
  green on net10.0, all three build legs green, `dotnet format` clean, no header delta —
  and the local env lacks the net8 runtime per STATUS.md). The DoD verification is the
  Actor's; nothing in the diff contradicts the claimed results.
- No `COMMENTS.FP.md` / `COMMENTS.FN.md` present for this binding — no rule-update
  suggestions warranted this phase.
