# COMMENTS.DONE.12 — M5/P3 "Consumer partition ops" (Actor N=12 → Critic N=12)

Closed record for the **M5/P3 — Consumer partition ops** phase (the tracked archive under
the phase directory, per CLAUDE.md §8.4; the binding-root `COMMENTS.12.md` /
`COMMENTS.DONE.12.md` are local working files and stay untracked). Approved plan:
`design/history/M5/P3-consumer-partition-ops/PLAN.md`.

**Mode A** (no Rust authored). Scope: five void-async members on `IAsyncConsumer` —
`Assign` / `Pause` / `Resume` / `SeekToBeginning` / `SeekToEnd`
(`Task <Op>(IReadOnlyCollection<TopicPartition>, CancellationToken = default)`), reusing the
proven void bridge (no new bridge shape). No pre-existing `COMMENTS.12.md` items to fix —
fresh phase (initial Actor implementation of an APPROVED plan, not a fixup cycle).

## Deviations from the approved PLAN

### D1 — Failure-path test is `Pause` of an *unassigned* partition (deterministic operational failure), not the concurrent-op fallback
PLAN §6.8 asked to assert the `KafkaException` message on a failure path, and left the
concurrent-async-op path as a fallback "if no partition-op has a clean broker-free
operational failure on the mock". A cleaner, **deterministic** one exists: the mock's
`pause()` / `resume()` delegate to `SubscriptionState::pause(tp)?` / `resume(tp)?`, which
error on a partition **not in the assignment** (`assigned_state_mut` →
`KafkaError::illegal_state("No current assignment for partition <tp>")`). So `Pause`/`Resume`
of an unassigned partition faults the `Task` with a `KafkaException` whose message is asserted
(DoD §3), plus a `Pause_UnassignedPartition_FaultsThenConsumerReusable` companion. Preferred
over the concurrent path because the concurrent-op overlap is **non-deterministic broker-free**
(mock ops resolve instantly — the same D-Q4 ceiling recorded for M5/P1 sync reads and M5/P2
`Position`). (`assign` / `seekToBeginning` / `seekToEnd` are infallible on the mock, so
`pause`/`resume` is the one op family with a clean deterministic operational failure.)

### D2 — Negative-partition precondition asserted via the `TopicPartition` ctor guard; the binding's own check kept as defense-in-depth
PLAN §5/§6.9 requires a negative partition → `ArgumentOutOfRangeException`. A negative
partition **cannot reach any op through a constructed `TopicPartition`** — its ctor rejects it
(the shipped `Seek`/`Position` precedent). So the test asserts the ctor guard's type + message
("Partition must not be negative."), which is exactly what the op would throw were the value
smuggled in. `SubmitPartitionOp` still validates `tp.Partition < 0` itself (defense-in-depth).

### D3 — One shared `WithPinnedTopics(int, Func<int,string>, int[], Action<...>)` marshaller for BOTH the tuple-form sync `Assign` and the `TopicPartition`-form async ops
PLAN §3 asked to extract the sync `Assign`'s parallel-array pinning into one shared helper.
The sync `Assign` takes `IReadOnlyList<(string, int)>` while the async ops take
`IReadOnlyCollection<TopicPartition>`; rather than force one input type, the shared helper
takes a `count` + a `Func<int,string>` topic accessor + the pre-built blittable `int[]`
partitions, so each caller supplies its own already-validated pairs and there is **one**
pinning/unpinning code path with **no per-element copy beyond the UTF-8 encode** (§A4/§B4).

## No-new-bridge / free-exactly-once audit (DoD §4/§6, self-review)
- `ConsumerCallbacks.cs` and `OperationCompletionSource.cs` are **byte-for-byte untouched** —
  NO new callback / delegate / trampoline / `OperationCompletionSource` variant. The five ops
  route through `SubmitPartitionOp` → the **unchanged** `SubmitVoidOperation` →
  `ConsumerCallbacks.Operation` (the shipped void `op_callback_t` trampoline).
- The per-op `GCHandle` is allocated and freed exactly once by the unchanged void bridge
  (callback = sole owner via `OperationCompletionSource.FreeGcHandle`; `AbandonBeforeSubmit`
  covers the submit-threw path) — no new free site added this phase.
- The parallel-array pins are released **call-scoped** in `WithPinnedTopics`'s `finally`; the
  submit lambda runs synchronously inside `SubmitVoidOperation` before the `Task` is returned,
  so the pins never outlive the native submit call (verified the core copies during the call
  for both `Consumer_assign` and each `_async`'s `read_topic_partitions`).

## Reachability limits (recorded, not silently skipped)
- A **deterministic in-flight wakeup-vs-op overlap** and a **forced concurrent-op overlap** are
  not reproducible broker-free (mock ops resolve instantly — the D-Q4 ceiling). The reachable,
  deterministic properties are asserted instead: pre-canceled token →
  `OperationCanceledException` synchronously; `Wakeup()` leaves the consumer usable; the
  concurrent-async-op → faulted-`Task` mapping is delivered by the unchanged core guard + void
  bridge.

## Commits (branch `prashah_dev_public_consumer_remaining`)
- `2e19bac` archive approved plan
- `0b691ec` internal partition ops — `NativeMethods` DllImports + `NativeConsumer` async ops +
  shared marshaller
- `5763a7d` public partition ops on `IAsyncConsumer` + Assign reconciliation
- `8ae5cfc` public-surface partition-ops tests (22, broker-free)
- `fb642ec` doc-sync — STATUS M5/P3 entry (N=12) + CLAUDE.md sketch + `Paused()` rustdoc

## Assign reconciliation outcome
- Public inherent sync `AsyncMockConsumer.Assign(IReadOnlyList<TopicPartition>)` **removed** —
  one public `Assign` (async), no sync/async overload footgun.
- **10** public-root `consumer.Assign(...)` sites migrated to `await consumer.Assign(...)`
  across 5 files (sync `void` test methods became `async Task`); **every assertion kept**.
- Internal sync `NativeConsumer.Assign((string,int)[])` + `Consumer_assign` DllImport + the
  **4 `Interop/` tests** untouched (75 Interop tests green).

## DoD gate results (Actor, all green)
- `cargo build --features ffi`: native + header present, no ABI change (Mode A).
- `dotnet build`: 0 warnings / 0 errors across all 6 TFM legs (library ns2.0/net8.0/net10.0,
  tests net462/net8.0/net10.0); TreatWarningsAsErrors + CS1591 on every new public member.
- `dotnet test` (net10.0 local): **153 → 175** (+22), 0 fail/skip, stable across ≥3 full runs
  (D8.8 serial gate). net8.0 *run* + net462 CI/Windows-only; all three build legs pass locally.
- `dotnet format --verify-no-changes`: clean.

---

## Critic N=12 — review outcome (closed)

**Review (`2e19bac..fb642ec`): CLEAN, 0 genuine findings, phase PASSES.** Independently
verified against the C ABI header + the Kafka Java public-API shape + the approved PLAN:

- **No new bridge (phase core claim):** `ConsumerCallbacks.cs` + `OperationCompletionSource.cs`
  **byte-for-byte untouched**; the five ops route through the shipped void `op_callback_t`
  trampoline verbatim. The one new `NativePartitionOpSubmit` delegate is a *managed* submit-shape
  typedef (no `[UnmanagedFunctionPointer]`), not a new callback. `FreeGcHandle` caller set
  unchanged (3 trampoline `finally` + 1 `AbandonBeforeSubmit`; no Dispose-side free added).
- **Marshaller memory safety:** `WithPinnedTopics` pins call-scoped, unpins in `finally`; pin
  lifetime correct (the submit lambda runs synchronously inside `SubmitVoidOperation` and the
  ABI copies during the call — verified `read_topic_partitions` runs before `async_void_op`
  spawns, capturing an owned `Vec`); no per-element copy beyond the UTF-8 encode; `count == 0`
  safe.
- **Assign reconciliation:** public sync `Assign` removed (footgun gone); internal sync
  `Assign((string,int)[])` + `Consumer_assign` DllImport + 4 Interop tests untouched; 10
  public-root sites migrated with every assertion preserved (Assert/Fact counts identical
  before/after).
- **Deviations sound:** D1 failure-path (pause of unassigned partition) deterministic — the
  asserted message matches both the Rust `Display` and the C# `ToString()` (`"{topic}-{partition}"`).
- **DoD independently observed:** `cargo build --features ffi` success (no ABI change);
  `dotnet build` **0/0** across all 6 TFM legs; `dotnet test -f net10.0` **175 passed / 0
  failed / 0 skipped**, looped 12× all green (PartitionOps isolated 5× → 22/22);
  `dotnet format --verify-no-changes` clean. No `COMMENTS.12.md` findings written.
- **Below the finding bar (noted, not blocking):** the five interface members' XML doc lists a
  null *element* topic under both `ArgumentNullException` and `ArgumentException`; the code
  actually throws `ArgumentException` for that case (test-verified). Doc-only imprecision,
  runtime correct — candidate for a trivial doc tidy, not a defect.

**Loop closed:** Actor N=12 → Critic N=12 (CLEAN, 0 findings). No fix cycle required; no
outstanding review comments.
