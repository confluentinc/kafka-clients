# COMMENTS.DONE.10 — M5/P1 "Consumer sync read surface" (Actor N=10 → Critic N=10)

Closed record for the **M5/P1 — Consumer sync read surface** phase (the tracked
archive under the phase directory, per CLAUDE.md §8.4; the binding-root
`COMMENTS.10.md` / `COMMENTS.DONE.10.md` are local working files and stay
untracked). Approved plan:
`design/history/M5/P1-consumer-sync-read-surface/PLAN.md`.

**Mode A** (no Rust authored). Scope: the four sync consumer members
`Assignment()` / `Subscription()` / `Paused()` / `EnforceRebalance(string? reason
= null)` on `IConsumerCommon`. No pre-existing `COMMENTS.10.md` items to fix —
fresh phase (this is the initial Actor implementation of an APPROVED plan, not a
fixup cycle).

## Recorded deviations (PLAN §9)

- **(a) Placement — the four members on `IConsumerCommon`, not the literal §3-sketch
  `IAsyncConsumer`.** Consistent with M4/P4b moving `Wakeup`/`GroupMetadata` down onto
  the shared non-blocking base. The §3 sketch was updated in the doc-sync commit.

- **(b) The three getters are METHODS, not properties (reverses the §3 sketch's
  property form).** `Assignment()` / `Subscription()` / `Paused()` are plain `()` methods
  returning `IReadOnlyCollection<T>`. Grounds: the shipped `GroupMetadata()` method
  precedent + Java/Python parity (plain methods) + FDG (a method, not a property, when the
  accessor does a P/Invoke + marshalling, can throw, and returns a fresh owned snapshot per
  call). The generic idiom-map row "non-blocking getter → property" is intentionally kept;
  the override is scoped to these specific getters.

- **(c) `EnforceRebalance` as one method with `string? reason = null`** collapsing Java's
  two byte-identical overloads (confirmed against Python's `enforce_rebalance(self,
  reason=None)`).

- **(d) Stale ABI `enforce_rebalance` doc raised as a Rust-core doc-fix dependency.** The
  header (line ~1846) says `enforce_rebalance` "returns an unsupported-version error,
  matching Java." That is **wrong** — the core returns `Ok(())` (null error) and Java throws
  nothing (SOURCE-VERIFIED: `src/consumer/async_kafka_consumer.rs:3873-3876` `log::warn +
  Ok(())`; `src/consumer/mock_consumer.rs:964` `should_rebalance = true; Ok(())`;
  `src/ffi/consumer.rs:3072` `sync_void_op` → null on `Ok`). The .NET mapping follows the
  ACTUAL behavior (no-op success). The one-line Rust-core doc fix is **out of scope** for the
  C#-only `dotnet-actor` (Mode A) — raise it to the root `actor-executor` / `kafka-critic`.

- **(e) Reachability limits (D-Q4 / M3-P3 precedent), both recorded not silently skipped:**
  - **Non-empty `Paused()` unreachable broker-free** until a public `Pause` lands (later
    phase): the mock's `paused()` starts empty and nothing can add to it yet. Tested states
    are empty / assigned-but-not-paused; the non-empty path is a later-phase test.
  - **Deterministic forced-concurrency overlap unreachable broker-free:** mock ops resolve
    instantly; the one guard-holding op with a controllable duration is `poll` (out of
    scope). The concurrent-null → `InvalidOperationException` mapping is verified by the
    reachable free-guard seam + code inspection of the **shared `ThrowIfConcurrentNull`**
    (reused verbatim from the group-metadata read). Same non-deterministic ceiling as the
    shipped state reads.

## Execution decisions / discoveries

- **Shared `ThrowIfConcurrentNull(IntPtr) → IntPtr` helper (PLAN §4 "Actor's choice").**
  Refactored `GetGroupMetadataHandleOrThrow` to delegate to it, so all FIVE sync reads
  (`GroupId` / `GroupMetadata` / `Assignment` / `Subscription` / `Paused`) share **one**
  concurrency-null contract — did not introduce a new one.

- **No new `SafeHandle`** — the two list results are transient owned borrow-roots fully
  consumed on the caller's thread within one sync call (get → copy-out → `_destroy`); the
  `ConsumerGroupMetadataMarshal` read-and-free pattern (a raw `IntPtr` + `finally _destroy`),
  no `unsafe`.

- **DISCOVERY — the core rejects `Assign` + `Subscribe` on one consumer** ("Subscription to
  topics, partitions and pattern are mutually exclusive"). One draft test tried both; fixed
  to drive the subscribe path only (assignment via the assign path is covered separately).
  Getter results compare as **sets** (the core returns a `HashSet` — order not guaranteed).

## DoD (all green — Actor)

1. `cargo build --features ffi` — no ABI change (Mode A); header + native present.
2. `dotnet build` — 0 warnings / 0 errors across all library TFMs (netstandard2.0 / net8.0 /
   net10.0) + all test TFMs (net462 / net8.0 / net10.0). CS1591 on every new public member;
   Apache-2.0 header on the two new files; no TODO/FIXME.
3. `dotnet test` (net10.0, the local runtime) — **122 → 141**, green across ≥4 full runs;
   D8.8 serial execution unchanged. net8.0 *run* + net462 are CI/Windows-only (runtime not
   installed locally); all three test *build* legs pass locally.
4. `dotnet format --verify-no-changes` — clean.

## Commits (branch `prashah_dev_public_consumer_remaining`)

- `4e1ef0f` implementation (NativeMethods + 2 marshallers + NativeConsumer + IConsumerCommon
  + forwards)
- `aacf854` tests (`Interop/ConsumerSyncReadTests.cs`)
- `80c226e` doc-sync (CLAUDE.md §3/§4 + STATUS.md M5/P1)
- `6bb8170` archive approved plan

---

## Critic N=10 — review outcome (closed)

**Review (`4e1ef0f` / `aacf854` / `80c226e` / `6bb8170`): CLEAN, 0 genuine findings,
phase PASSES.** Independently verified against the C ABI header + the Kafka Java
public-API shape + the approved PLAN (CLAUDE.md §8.2 ground truth):

- **Memory safety (primary axis):** both marshallers (`TopicPartitionListMarshal`,
  `StringListMarshal`) free the owned Category-3 borrow-root **exactly once in a
  `finally`** (survives a mid-read throw), copy every element/string out **before**
  `_destroy`, and never free the borrowed Category-4 `_get` elements — `TopicPartition_destroy`
  is declared/called **nowhere** in `src/` (grep-confirmed). No double-free / UAF.
- **String form (§B3):** `StringList_get` + `TopicPartition_topic` are NUL-terminated in
  the header → both marshallers correctly use `Utf8Marshal.PtrToString(IntPtr)` (NUL-scan),
  not the length-delimited `out_len` form. No over-read.
- **P/Invoke (§0.1):** 11 new `[DllImport]`s, `EntryPoint` = full ABI symbol, `int`/`IntPtr`/
  `void` per the type map, `Cdecl`; transient reads read-and-freed (no new `SafeHandle`).
- **`EnforceRebalance`:** `reason` pinned call-scoped (`using Utf8Marshal.Pin`; `null →
  IntPtr.Zero`); `FromHandle` frees the error handle exactly once on both paths; no throw on
  the KIP-848 null path; the stale "unsupported-version" ABI doc is correctly **not** encoded.
- **Concurrency / error mapping (§B5):** null → `InvalidOperationException` (right message);
  post-dispose → `ObjectDisposedException`; never `KafkaException` for reads. The
  `ThrowIfConcurrentNull` refactor is **behavior-preserving** for the shipped
  `GroupMetadata`/`GroupId` (diffed `4e1ef0f~1` vs `4e1ef0f` — byte-identical message + throw
  condition).
- **Shape / decision hygiene:** four members on `IConsumerCommon`; three getters are `()`
  methods (not properties); `EnforceRebalance(string? reason = null)` one method; returns
  `IReadOnlyCollection<T>`; both client types forward; `AsyncMockConsumer.Assign(...)`
  untouched. Doc-sync (CLAUDE.md §3/§4 + STATUS) matches the code; plan archive byte-identical.
- **Tests (DoD §3):** non-ASCII round-trip **content** through both marshallers; post-dispose
  `ObjectDisposedException` deterministic; `EnforceRebalance` no-throw (not unsupported-version);
  allocation sanity on the caller thread; the two non-reachable slices (non-empty `Paused()`,
  forced concurrency) documented, not skipped; the Assign+Subscribe mutual-exclusion is a
  genuine `SubscriptionState` constraint.
- **DoD independently observed:** `cargo build --features ffi` current; `dotnet build`
  **0/0** across all library + test TFMs; `dotnet test -f net10.0` **141 passed, 0 failed**,
  looped **22×** all green (no host crash under D8.8 serial, no discovery under-count);
  `dotnet format --verify-no-changes` clean. No `COMMENTS.10.md` findings written.

**Loop closed:** Actor N=10 → Critic N=10 (CLEAN, 0 findings). No fix cycle required; no
outstanding review comments.
