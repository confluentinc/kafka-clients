# COMMENTS.DONE.8 — Execution decisions & deviations (M4/P4a — Public Consumer Client)

Actor N=8. Records decisions, deviations, and findings made **during** execution of
the APPROVED plan `design/history/M4/P4a-public-consumer/PLAN.md`. (Forward-looking
scope/decisions live in the PLAN; this file is the execution log.)

---

## D8.1 — `ConsumerRecord.Key`/`Value` unified on `byte[]?` (micro-decision A, locked)

**Deliberate deviation from the CLAUDE.md §3 `ReadOnlyMemory<byte>?` sketch.** Per the
plan's locked decision 2 (human, 2026-08-04), the public `ConsumerRecord.Key` /
`.Value` and `Header.Value` are all `byte[]?`, unifying the raw-byte surface:

- **Java fidelity + CKD parity:** the raw-bytes interim is `byte[]`; CKD's `Message`
  uses `byte[]`. A `ReadOnlyMemory`↔`byte[]` split between record bytes and header
  bytes would be an internal inconsistency.
- **No new cost:** the receive-path copy-out already allocates an owned `byte[]` and
  then *wraps* it in `ReadOnlyMemory` (the old internal type). Returning `byte[]`
  **removes** the wrap — a net simplification, not a new copy. The allocation budget
  is unchanged (§5.6).
- **Marshaller ripple:** `ConsumerRecordsMarshal.CopyBytes` now returns `byte[]?`
  (dropped the `ReadOnlyMemory` wrap); `unsafe` is no longer needed there
  (`Marshal.Copy` into an owned array is safe managed API), so the method and the
  file lost their `unsafe`. It stays a copy-out (managed owned array), no native
  reference retained (§B4).

Recorded per plan §6.A and the "Approved decisions" block.

---

## D8.2 — `CloseAsync()` wiring (no timeout; ABI-verified) — locked decision 6

VERIFIED via `target/include/confluent_kafka.h`: `Consumer_close_async` takes only a
callback (core default timeout); the only timeout-accepting close is the **sync**
`Consumer_close_with_timeout`. So `CloseAsync(TimeSpan)` is deferred (would need a
Rust-core `close_async_with_timeout` — Mode-B, out of scope); P4a ships
`CloseAsync(CancellationToken = default)` only.

**Wiring (distinct from `DisposeAsync`):** `NativeConsumer.CloseAsync(ct)` takes the
`TryBeginClose` one-shot latch, bridges `close_async` via the existing
`CloseAsyncInternal()`, then `_handle.Dispose()` (→ `Consumer_destroy`) in a
`finally` — destroy exactly once even on error. Unlike `DisposeAsync` (which swallows
the close `KafkaException`), `CloseAsync` **surfaces** it. A subsequent
`Dispose`/`DisposeAsync` loses the latch and no-ops (closed-flag idempotence). The
public `KafkaConsumer.CloseAsync` / `MockConsumer.CloseAsync` forward to it.

Recorded per plan §3 CloseAsync note + §10.

---

## D8.3 — `GroupMetadata` broker-free field reachability (M2/P1 D5 precedent)

VERIFIED the broker-free values the four fields carry (from the Rust core, read as a
contract — NOT authored):

- **Real `KafkaConsumer` (pre-join, no broker)** — `AsyncKafkaConsumer::group_metadata`
  returns a stub built from the configured `group.id` when no membership metadata
  exists yet (`src/consumer/async_kafka_consumer.rs:1629`; `ConsumerGroupMetadata::new`
  → `src/consumer/consumer_group_metadata.rs:58`):
  - `GroupId` = the configured `group.id` (reachable — the M2/P1 D5 round-trip).
  - `GenerationId` = `-1` (`UNKNOWN_GENERATION_ID`).
  - `MemberId` = `""` (empty; `UNKNOWN_MEMBER_ID`).
  - `GroupInstanceId` = **`null` even when `group.instance.id` is configured** —
    SOURCE-VERIFIED: the pre-join stub is `ConsumerGroupMetadata::new(group_id)`
    (`async_kafka_consumer.rs:1639`), which sets `group_instance_id = None` regardless
    of config; the configured static-member id only surfaces POST-JOIN via the state
    notifier (against a broker; `async_kafka_consumer.rs:1222`). So broker-free, only
    `GroupId` reflects config; the other three carry pre-join defaults. (The initial
    test asserting `GroupInstanceId == "instance-7"` broker-free failed and was
    corrected to assert `null` — the reachable truth, no flaky broker-dependent test.)
- **`MockConsumer`** — `MockConsumer::group_metadata` returns hard-coded Java-parity
  sentinels (`src/consumer/mock_consumer.rs:406`,
  `ConsumerGroupMetadata::with_details("dummy.group.id", 1, "1", None)`):
  - `GroupId` = `"dummy.group.id"` (NOT the configured value — the mock ignores config).
  - `GenerationId` = `1`, `MemberId` = `"1"`, `GroupInstanceId` = `null`.

**How tested (mirrors M2/P1 D5 — assert the reachable fields, document the rest, no
flaky test):** the full-field test uses a **real `KafkaConsumer`** (broker-free create;
no `bootstrap.servers` connection needed for `group_metadata`) and asserts all four
properties against these documented pre-join values, incl. a **non-ASCII** `group.id`
(the D5 round-trip carried to the public type) and a set `group.instance.id`
(→ non-null `GroupInstanceId`). A `MockConsumer` variant asserts the hard-coded
sentinels. Both are deterministic broker-free; `member_id`/`generation_id` are NOT
"post-join only unreachable" — they carry deterministic pre-join defaults, which is
what is asserted.

---

## D8.4 — `GroupMetadata` concurrent-overlap determinism (M3/P3 D-Q4 precedent)

The concurrent → `InvalidOperationException` mapping (core returns a null metadata
handle on its concurrent-access rejection → `GroupMetadata()` throws
`InvalidOperationException`, mirroring the internal `GroupId()` path) is VERIFIED by
inspection + carried unchanged from the internal `GroupId()`. As with M3/P3 D-Q4,
broker-free `MockConsumer`/pre-join `KafkaConsumer` ops resolve instantly, so a
deterministic forced submit→callback overlap is not reproducible without a
controllable-duration guard-holding op (out of scope). No flaky forced-overlap test is
shipped; the reachable seam (the full-field round-trip) is tested and the null-handle
→ `InvalidOperationException` mapping is verified by inspection. Documented here per
the D-Q4 precedent.

---

## D8.5 — `SeekAsync` negative-offset precondition (Java-fidelity fix, locked decision 11)

The internal `SeekAsync` validated `partition < 0` but not `offset < 0`. Java's
`AsyncKafkaConsumer.seek()` throws `IllegalArgumentException("seek offset must not be a
negative number")` on `offset < 0` before the blocking `addAndGet`. Added the
precondition (→ `ArgumentOutOfRangeException` with that exact message, thrown before
any native call) to `NativeConsumer.SeekAsync`. This is a Java-fidelity fix within
locked decision 11's scope, not a regression — the M3/P1 proof op only exercised the
unassigned-partition failure and never validated offset.

---

## D8.6 — Compose over absorb (micro-decision B); inherent mock helpers (consumer-threading §2)

`KafkaConsumer` / `MockConsumer` **hold** a `private readonly NativeConsumer _native`
and forward (per plan §2.5): the proven internal interop test surface stays intact, the
`unsafe`/`GCHandle` bookkeeping stays quarantined under `Internal/`, and teardown
composes (`Dispose`→`_native.Dispose()`, `DisposeAsync`→`_native.DisposeAsync()`,
`CloseAsync`→`_native.CloseAsync()`). `MockConsumer`'s mock-only helpers
(`AddRecord`, `SetPollError`, `Assign`) are inherent on the concrete type, **not** on
`IConsumer` (consumer-threading §2). `MockConsumer.AddRecord` uses the component-tuple
form `(string topic, int partition, long offset, byte[]? key, byte[]? value)` (locked
decision 4) — no public `ConsumerRecord` ctor this phase.

---

## D8.8 — FINDING: pre-existing intermittent host crash under xUnit parallel execution → disabled cross-collection parallelization

**Surfaced by the ~20× stability gate (plan §5.8).** Running the full suite repeatedly
exposed an **intermittent test-host crash** ("Test host process crashed"), ~1 in 3–10
runs, non-deterministic (a different partial pass-count each time, no named failing test).

**Root cause (source-verified, pre-existing since the M3 completion bridge):** each test
owns a Kafka consumer, which is **single-owner / not thread-safe** by contract (ffi §B1,
consumer-threading §§1/2). xUnit's default runs test **collections in parallel**, so
several tests create / drive / tear down their own native consumers concurrently across
threads. Native teardown is **fire-and-forget**: `Consumer_destroy`
(`src/ffi/consumer.rs:493`) shuts down the runtime and then **detaches — does NOT join —**
the callback dispatcher (`:506-510`, "do NOT join"). So a completion callback for one
consumer can still be firing on its foreign dispatcher thread while a *different* test's
teardown + GC runs in parallel — the **accepted single-owner residual** (an unawaited-op
straggler callback after destroy, M3/P2 residual #1) racing GC across tests. That
cross-test race intermittently crashes the host.

**Bisected as PRE-EXISTING, not introduced by P4a:** with all P4a Step-5 public tests
moved aside and the test project reverted to the tracked-HEAD (848c486, 81 tests) state,
the crash still reproduced (~1/10). P4a's additional consumers/teardown churn only raise
its frequency; it did not introduce it.

**Fix (standard harness setting, no assertion weakened):**
`[assembly: CollectionBehavior(DisableTestParallelization = true)]` in
`tests/.../AssemblyInfo.cs` — run the assembly's tests **serially**. This removes the
*inter-test* cross-thread race (which is itself the misuse the not-thread-safe contract
does not defend against); the ops *within* a test are already serialized by the
single-owner core guard, so nothing is weakened. It is the correct setting for a
not-thread-safe native-resource suite. Verified: 0 crashes across 12 (then 20) full-suite
runs after the change, vs ~1/3–1/10 before. A real *fix* of the residual (a Rust-core
dispatcher-join on destroy, or the deferred per-call `DangerousAddRef` managed hardening)
is out of P4a scope — candidate N=9 / a Rust-core dependency.

---

## D8.9 — TFM smoke could not `Subscribe` then `Assign` (mutually exclusive)

The plan §5.7 smoke sketch was "create → subscribe → add → poll → close", but
`MockConsumer.AddRecord` requires an **assigned** partition and Kafka rejects
assign-after-subscribe ("Subscription to topics, partitions and pattern are mutually
exclusive"). Split the smoke into two legs: the record-carrying round-trip uses
`Assign → seek → add → poll → close`, and a separate no-record leg exercises
`Subscribe → Unsubscribe → close`. Both cover the TFM matrix; neither combines the two
mutually-exclusive modes.

---

## D8.10 — net462 test TFM: two runtime-only tests guarded net8.0+

Adding `net462` to the test project TFMs (plan §5.7, via
`Microsoft.NETFramework.ReferenceAssemblies` so it BUILDS cross-platform) exposed two
pre-existing internal tests using net462-unavailable APIs:
`GC.GetTotalAllocatedBytes` (allocation budget) and `Task.IsCompletedSuccessfully`
(bridge). Fixes: the whole allocation-budget classes (internal + the new public one) are
`#if NET8_0_OR_GREATER` (runtime-behavior tests; net462 runs are Windows/CI-only anyway),
and the one `IsCompletedSuccessfully` assertion became
`Assert.Equal(TaskStatus.RanToCompletion, ...)` (net462-safe). The whole test project now
builds 0/0 on net462; its *run* stays Windows/CI-only.

---

## D8.11 — CloseAsync error-surfacing not reachable broker-free (verified by inspection)

`CloseAsync` surfaces a close `KafkaException` (unlike `DisposeAsync`, which swallows it).
Broker-free, a `MockConsumer`/pre-join `KafkaConsumer` close **succeeds**, so the
throw-path is not reachable without a broker. The mechanism is verified by inspection:
`NativeConsumer.CloseAsync` awaits `CloseAsyncInternal()` (bridges `close_async`) OUTSIDE
any `catch`, so a faulted close `Task` propagates its `KafkaException`; only
`DisposeAsync`'s `catch (KafkaException)` swallows it. The reachable broker-free assertion
(`CloseAsync` completes without faulting, then is idempotent with dispose) is tested; the
error-surfacing divergence from `DisposeAsync` is documented, not shipped as a flaky
broker-dependent test.

---

## D8.7 — Governance: `Wakeup()` now genuinely public/cross-thread (option (a), locked)

P4a is the first phase to expose `Wakeup()` on a public client, making the accepted
single-owner handle-TOCTOU residual reachable in principle. Per locked decision 5
(option (a)): the residual stays **accepted-by-design** and is documented on the public
`KafkaConsumer`/`IConsumer` as a not-thread-safe caveat (mirroring the Python sibling);
NO per-call `DangerousAddRef` hardening this phase (flagged as a candidate N=9
follow-up, not scheduled). The three M3/P2 accepted residuals remain accepted;
composition inherits them unchanged.

---

## OBS-1 (Critic N=8, LOW, test hardening) — RESOLVED

`bindings/dotnet/tests/Confluent.Kafka.UnitTests/PublicConsumerRoundTripTests.cs`

**Finding.** The `Poll(consumer)` helper returned `consumer.PollAsync(...)` directly,
bypassing the `TestTimeout` hang guard that PLAN §5 requires for "every awaited op and
every teardown", and many success-path tests `await Poll(consumer)`. The `SeekAsync` in
the `ReadyToPoll` setup helper was likewise unwrapped. Not reachable today (the mock poll
completes near-instantly), but a future stall in the owned-handle bridge or the mock would
hang the whole run instead of failing fast.

**Resolution.** Routed every awaited op in the file through `TestTimeout.Run` (matching
`PublicConsumerTfmSmokeTests.Poll` and the existing `TestTimeoutResult`):
- `Poll(IConsumer)` now awaits `PollAsync` inside `TestTimeout.Run(..., s_deadline)`
  (was: raw `PollAsync` return). This covers all success-path callsites unchanged.
- Added a `Poll(Task<ConsumerRecords>)` overload (same guard) and pointed
  `TestTimeoutResult` at it — no behavior change, just consolidation.
- `ReadyToPoll` now wraps its `SeekAsync` in `TestTimeout.Run`.
- The three FAILURE-path callsites (was `TestTimeout.Run(() => Poll(consumer), ...)`) now
  call the guarded `Poll(consumer)` directly, removing the redundant double-wrap while
  keeping the guard.
No assertion was weakened — this only adds/consolidates the timeout wrapper. Whole file
audited: the only awaited ops are the polls + the setup `SeekAsync`; teardown here is the
sync `using`/`Dispose()` (not awaited), so nothing else needed wrapping.

---

## OBS-2 (Critic N=8, LOW, doc-only) — RESOLVED

`bindings/dotnet/src/Confluent.Kafka/TopicPartition.cs`

**Finding.** `TopicPartition.ToString()` does not null-guard a `default(TopicPartition)`
(whose `Topic` is null), though `Equals`/`GetHashCode` do. This is inherent to the
`readonly struct` choice and matches confluent-kafka-dotnet; the XML doc did not mention
that a `default(TopicPartition)` has a null `Topic`.

**Resolution.** Doc-only, per the Critic's guidance — **no runtime guard added** (the
`readonly struct` value-type choice is deliberate; a runtime guard would diverge from CKD
and add no value since no P4a API produces a `default`). Added a `<remarks>` note on
`TopicPartition` documenting that a `default(TopicPartition)` bypasses the ctor validation
(null `Topic`, `Partition == 0`), that `Equals`/`GetHashCode` are null-safe for that state,
that `ToString()` reflects it (null `Topic` interpolates as empty, e.g. `"-0"`), and that
no public API in the binding produces a `default` value. Kept accurate and brief; behavior
unchanged.

---

## Critic N=8 — review outcome (closed)

**Initial review: NO BLOCKING ISSUES.** Independently verified (not trusting the Actor's
numbers): `cargo build --features ffi` clean; `dotnet build` 0/0 across all six TFM legs
(CS1591 satisfied); `dotnet format` clean; net10.0 test loop **30× serial → 30/30,
122 passed, 0 failed, 0 crashes**. Verified against the C ABI header + Java shape: copy-out
absent/tombstone/empty sentinels, length-delimited `out_len` strings (no NUL-scan),
`FreeGcHandle` sole-owner invariant (exactly the 3 expected sites), `CloseAsync`
latch→close(surfaces error)→destroy-once-in-finally, `SeekAsync` exact Java message before
native, `GroupMetadata` four-field + handle-destroy-in-finally + null→`InvalidOperationException`.
Two LOW non-blocking observations raised — **OBS-1** (test hang-guard) and **OBS-2**
(`TopicPartition` doc note) — both resolved above.

**`DisableTestParallelization` (D8.8) verdict: LEGITIMATE, not masking.** The Critic
independently bisected — re-enabling parallelism on P4a HEAD gave 3/20 host crashes; a
throwaway `git worktree` at the pre-P4a tip `8641d08` (M3/P3, 66 tests) reproduced the
identical 3/20 crash signature. The race is genuinely pre-existing (the accepted
single-owner unawaited-op vs fire-and-forget `Consumer_destroy` residual); serial execution
removes the inter-test race without weakening any assertion.

**Re-review of the OBS fixup (`bab3d0e`): CLEAN.** Verified the failure-path guard +
`KafkaException` type/message assertions survived the double-wrap removal, no assertion
weakened, scope = exactly the two named files, `fixup!` subject clean. Independently re-ran
net10.0 **22× serial → 22/22, 0 failures / 0 crashes**; `dotnet build` 0/0; `dotnet format`
clean. **Phase remains approved with the fixup folded in.**

**Loop closed:** Actor N=8 → Critic N=8 (approved) → Actor N=8 (OBS-1/OBS-2 fixup) →
Critic N=8 (re-review clean). No outstanding comments.
