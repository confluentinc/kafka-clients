# PLAN — M11/P6: Producer async in-flight cap (managed backpressure)

**Binding:** `bindings/dotnet` · **Milestone/Phase:** M11 / P6 · **Agent number:** N=39
**Branch:** `prashah_dev_dotnet_binding_producer_tuned` (base `732e259f` on `prashah_dev_dotnet_performance_new`) — do not switch.
**Mode:** **A** — .NET-managed-only. No C ABI / Rust-core / header / cbindgen change; no new `[DllImport]`. Manager verifies `git diff --stat <base>..HEAD` is empty over `src/**` (Rust), `src/ffi/**`, `target/include/confluent_kafka.h`, `cbindgen.toml`.
**Status:** APPROVED (human, all recommendations accepted). Archived here on approval; Actor(39)/Critic(39) loop to follow.

---

## 1. Goal (one line)

Bound the .NET **async** producer's outstanding-send depth to a managed cap of **N = 1000** records so pipelined produce stays low-latency, mirroring the Python sibling's `PRODUCER_MAX_ACCUMULATED_RECORDS = 1000` and Java `KafkaProducer.send()` blocking once `buffer.memory` is exhausted.

---

## 2. Background & confirmed root cause (verified against the code)

- The async send path is the **inline pull-pump** (ffi §A7 Option C): `AsyncKafkaProducer<K,V>.Send` → `NativeProducer.SendViaPump` → `ProducerSendMarshal.Send` calls `Producer_send` **inline on the caller thread** (core copies key/value synchronously, ffi §A4), returns a future, enqueues `(future, TCS)` on the one `SendCompletionPump` thread which drains a batched `FutureRecordMetadata_get_all` and completes each TCS. There is **no .NET-side accumulator** — records go straight to the Rust core's `RecordAccumulator`.
- **The only backpressure that exists today is the core's `buffer.memory`** (default 32 MB ≈ ~32k 1 KB records): an inline `Producer_send` blocks the caller up to `max.block.ms` only when the *core* buffer is full. A 32 MB buffer lets a deep queue form; latency ≈ depth ÷ drain-rate. Confirmed latency driver (setting `buffer.memory`=2 MiB dropped p50 ~53→6 ms at the same throughput; the reverted "pump batch cap" gave only 61→53 ms and is **not** re-proposed).
- Python stays low-latency on the same core with the same default `buffer.memory` purely because its C extension caps its **binding-side accumulator** at 1000 (`_confluentkafka.c:23`, `:686`, `:693`) and its `send()` waits on `Producer_on_space_available` (`:706`) before returning. **.NET has no binding-layer cap.** This phase adds one.

---

## 3. Scope

**In scope**
- A managed in-flight cap on the **async** send path only (`NativeProducer.SendViaPump` + `SendCompletionPump`).
- Acquire-before-`Producer_send`; release paired 1:1 with the pump's exactly-once future-destroy; teardown-safe wake of parked waiters.
- Unit tests: slot-leak regression, deadlock/Dispose regression, over-release guard, cancellation, backpressure-engages, plus a re-perf measurement.
- Doc-sync: `ffi-marshalling.md §A7` and the `SendCompletionPump.cs` "Backpressure" xmldoc (both currently say "no managed bound / hand-cap is added" — now stale), and `bindings/dotnet/CLAUDE.md §3` if it sketches the async send shape.

**Out of scope**
- The **sync** `NativeProducer.Send` path — verified to block per-message on `FutureRecordMetadataGet` (`NativeProducer.cs:506`), so it cannot pile up; it never touches the semaphore.
- Any C ABI / Rust-core change (Mode A). This is **not** the Option-A managed-accumulator rewrite — it is a thin semaphore in front of the existing Option-C path.
- Curing the existing "concurrent close can't wake a send stuck on a *full core buffer*" limitation. The cap **mitigates** it (§7.3) but does not remove it for pathological `buffer.memory` settings.
- A public config key for N (fixed const this phase) and headers/transactions/metrics on the producer.

---

## 4. Design

### 4.1 State added to `NativeProducer`
- `private const int MaxInflightSends = 1000;` — named const (Python `PRODUCER_MAX_ACCUMULATED_RECORDS` parity). The decided "bump to 2000 later" is a one-line change.
- `private readonly SemaphoreSlim _inflight = new SemaphoreSlim(MaxInflightSends, MaxInflightSends);` — **max count = N is deliberate**: any over-release throws `SemaphoreFullException`, a built-in "release ≤ acquire" guard (test §6.3). First `SemaphoreSlim` use in the binding; available on the netstandard2.0 floor (covers net462), net8.0, net10.0.
- `private readonly CancellationTokenSource _sendGate = new CancellationTokenSource();` — cancelled at the start of teardown to wake parked async waiters (§4.4). **Approved wake mechanism** (vs disposing the semaphore): keeps the semaphore alive so all releases are unconditionally safe; a parked-then-torn-down send faults with `OperationCanceledException`.

### 4.2 Acquire — fast path allocation-free, slow path is the only one that boxes
`SendViaPump` keeps its **synchronous** preconditions (so `ObjectDisposedException` / already-cancelled `OperationCanceledException` still surface as today, and `EnsurePump`'s under-lock close re-check is unchanged), then:

```
internal Task<RecordMetadata> SendViaPump(SerializedProducerRecord record, CancellationToken ct = default)
{
    ThrowIfClosed();                       // sync throw preserved
    ct.ThrowIfCancellationRequested();     // sync throw preserved
    SendCompletionPump pump = EnsurePump();// sync throw preserved (ThrowIfClosed under _pumpLock)

    // Fast path: uncontended try-acquire, no async state machine, no heap box.
    if (_inflight.Wait(0))
        return SendAcquired(pump, record, ct);   // slot held on entry

    // Slow path (cap engaged): only this path boxes a state machine + links a token.
    return SendAfterWaitAsync(pump, record, ct);
}
```

- `SemaphoreSlim.Wait(0)` is a non-blocking try-acquire (bool). The common uncontended path never suspends → allocates nothing beyond the existing TCS + topic pin (**DoD §10 preserved**).
- `SendAfterWaitAsync` is the only `async` method:
  ```
  private async Task<RecordMetadata> SendAfterWaitAsync(pump, record, ct)
  {
      using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _sendGate.Token);
      await _inflight.WaitAsync(linked.Token).ConfigureAwait(false);   // OCE if ct or teardown fires; no slot on throw
      if (Volatile.Read(ref _closed) != 0) { _inflight.Release(); throw new ObjectDisposedException(nameof(NativeProducer)); }
      return await SendAcquired(pump, record, ct).ConfigureAwait(false);
  }
  ```
  The linked-CTS alloc is on the contended (already-throttled) path only — acceptable. `SemaphoreSlim.WaitAsync` is FIFO for async waiters (ordering note §7.5).

### 4.3 Send + slot hand-off — `SendAcquired` (current `SendViaPump` body, now with a held slot)
Entered with **exactly one slot held**; releases it exactly once — either **transferred to the pump** (successful `Enqueue`) or **released here** (any pre-hand-off failure):

```
private Task<RecordMetadata> SendAcquired(pump, record, ct)
{
    IntPtr future;
    try {
        future = ProducerSendMarshal.Send(_handle, record.Topic, part, ts, record.Key, record.Value);
    } catch {
        _inflight.Release();          // sync out_error → KafkaException, NO future created (ProducerSendMarshal.Send:129)
        throw;
    }
    try {
        var completion = new TaskCompletionSource<RecordMetadata>(TaskCreationOptions.RunContinuationsAsynchronously);
        // existing cancellation registration (only for a cancelable token) ...
        pump.Enqueue(future, completion);   // OWNERSHIP of future + its slot transfers to the pump here
        return completion.Task;
    } catch {
        NativeMethods.FutureRecordMetadataDestroyAll(new[] { future }, 1);  // orphan (TCS/registration/Enqueue OOM)
        _inflight.Release();
        throw;
    }
}
```
`pump.Enqueue` **returns normally** on both outcomes — queues the future (pump owns) OR, on the `_stopped` race, destroys+faults it in place. Either way the pump handles the slot, so the outer `catch` runs only if `Enqueue` itself throws (e.g. OOM growing the queue) — no double-release. Mirrors the existing orphaned-future comment at `NativeProducer.cs:387-394`.

### 4.4 Release — tie to the pump's exactly-once future-destroy
The pump already guarantees **every future handle is destroyed exactly once on every path**. Piggyback release on that invariant: pass a release delegate at construction and release immediately after each destroy (same `finally`, atomically paired):

- `EnsurePump`: `_pump ??= new SendCompletionPump(freed => _inflight.Release(freed));`
- Pump release sites (each = count of enqueued futures destroyed; every enqueued future = exactly one acquired slot — §5):
  1. `ProcessBatch` `finally` — after `FutureRecordMetadataDestroyAll(futures, count)` (`:290`) → `releaseSlots(count)`. **`ProcessBatch` changes from `static` to instance** (or takes the delegate). `FaultBatchCompletions` stays **TCS-only** — must NOT release (ProcessBatch's `finally` already did).
  2. `DrainAndFaultRemaining` (teardown) — after `DestroyFutures(futures, count)` (`:336`) → `releaseSlots(count)`.
  3. `Enqueue` stopped-path — after `DestroyFutures(new[]{future},1)` (`:124`) → `releaseSlots(1)`.

**Do NOT** use a per-send `Task.ContinueWith(... Release())` — allocates per send (DoD §10 / CLAUDE §11). Release-on-destroy adds zero per-send allocation.

### 4.5 Teardown ordering (Dispose / DisposeAsync / Close / CloseWithCallback)
No new teardown *shape* — two additions, ordered against the existing `TryBeginClose` latch:
1. **First thing after winning the latch:** `_sendGate.Cancel();` — wakes every parked `WaitAsync` with `OperationCanceledException` (no slot acquired → nothing to release). New `SendViaPump` calls already throw `ObjectDisposedException` synchronously at `ThrowIfClosed`.
2. Existing `StopPump()` / `StopPumpAsync()` runs: flush resolves pending sends → pump's `get_all` returns → `ProcessBatch` destroys **and releases** those slots → `pump.Stop()` joins and `DrainAndFaultRemaining` destroys+releases the rest. **Semaphore alive throughout** → all releases safe.
3. `_sendGate.Dispose()` at the very end, after `pump.Stop()` joined — no release can arrive after. (`SemaphoreSlim` needs disposal only if `AvailableWaitHandle` was touched, which it never is; disposing `_inflight` optional/tidiness.)

---

## 5. SAFETY ANALYSIS — release exactly once per send on every path (the critical section)

**Invariant:** over the whole lifetime `#releases == #acquires`, and no `Release` ever runs on a slot that was not acquired (else `SemaphoreFullException` from the max-count ctor).

**Acquire sites (exactly two, each +1):**
- (A1) Fast path `_inflight.Wait(0) == true`.
- (A2) Slow path `await _inflight.WaitAsync(linked)` returning normally.

`WaitAsync`/`Wait(0)` that **throw** (OCE from `ct`/`_sendGate`, or `Wait(0)==false`) acquire nothing → no release owed. ✔

**Every acquired slot ends in exactly one release** (terminal states of a held slot):

| # | Path a send can end on | Where the future is destroyed | Slot released by | Once? |
|---|---|---|---|---|
| 1 | Normal success (broker/mock acks) | `ProcessBatch` finally `destroy_all` (`:290`) | `releaseSlots(count)` same finally | ✔ |
| 2 | Delivery error (non-null `error[i]`) | same `ProcessBatch` finally | same `releaseSlots(count)` | ✔ |
| 3 | `ProcessBatch` throws mid-loop (OOM / native get_all failure) | `ProcessBatch` finally still runs `destroy_all` | `releaseSlots(count)` in that finally; `RunLoop` catch → `FaultBatchCompletions` is TCS-only (no re-release) | ✔ |
| 4 | Teardown drains a still-queued send | `DrainAndFaultRemaining` `DestroyFutures` (`:336`) | `releaseSlots(count)` right after | ✔ |
| 5 | Send enqueued into an already-`_stopped` pump | `Enqueue` stopped-path `DestroyFutures` (`:124`) | `releaseSlots(1)` right after | ✔ |
| 6 | Synchronous `Producer_send` failure (`out_error`) | no future created (`ProducerSendMarshal.Send:129` throws) | `SendAcquired` pre-send `catch` `_inflight.Release()` | ✔ |
| 7 | Orphan: TCS/registration/`Enqueue` OOM before ownership transfer | `SendAcquired` orphan `catch` `destroy_all(1)` | same catch `_inflight.Release()` | ✔ |
| 8 | Slow-path waiter woken during teardown, won a freed slot | no future created (bails before `Producer_send`) | `SendAfterWaitAsync` `_closed` re-check → `_inflight.Release()` | ✔ |

**No path both transfers to the pump and releases locally:** `pump.Enqueue` returning normally (1–5) means the pump owns the slot; `SendAcquired`'s local releases (6, 7) run only when `Enqueue` was never reached or threw. Case 8 releases before any `Producer_send`. ✔

**No release can exceed the acquire count:** every pump `releaseSlots(count)` frees exactly the number of *enqueued* futures being destroyed, and **every enqueued future came from an A1/A2 acquire** (the pump's only feeder is `SendAcquired`→`Enqueue`; the sync `Send` path never enqueues and never acquires). Combined with the max-count ctor, any accounting bug surfaces as `SemaphoreFullException`, not a silent leak. ✔

**No deadlock:**
- New `Send` after teardown: `ThrowIfClosed` throws synchronously — never parks.
- Parked waiters at teardown: `_sendGate.Cancel()` wakes all with OCE (belt); any that won a freed slot in the cancel/free race hit the `_closed` re-check and bail (suspenders, case 8). Neither parks forever, neither over-releases.
- Pump-join no-hang property unchanged (flush-before-join, `StopPump`/`StopPumpAsync`); the cap adds no new blocking inside the pump.

**Residual (documented, matches existing "concurrent Send during Dispose is misuse"):** a slow-path waiter that wins a freed slot *and* passes the `_closed` re-check *before* the latch is observed (a check-then-act window that already exists between `EnsurePump` and `Producer_send` today) could issue one `Producer_send` during teardown; it is memory-safe (future is `Arc`-independent; `Producer_destroy` tolerates outstanding futures — M11/P3 round-2 finding) and its slot is still released via case 5/7. Same misuse envelope as the existing `ConcurrentSendAndDispose_DoesNotCrash` churn test, extended to the parked-waiter case (new test §6.2).

---

## 6. Test plan (mandatory — the rule is not "tested" without these)

New file(s) under `tests/Confluent.Kafka.UnitTests/` (e.g. `PublicProducerInflightCapTests.cs`), net10.0 execution gate (net8.0 build-only locally — §9). Exercise the cap deterministically against `MockProducer(autoComplete:false)`, holding completions with `MockCompleteNext`/`MockErrorNext` from a helper thread (the manual-mock cross-thread pattern the sync send tests already use).

- **6.1 Slot-leak regression:** push **M ≫ cap** (e.g. 5000) async sends through a manual mock; drive completions so all resolve; assert (a) all M tasks complete, and (b) the semaphore returns to **exactly baseline (N free)** — via a test-only `InternalsVisibleTo` accessor for `_inflight.CurrentCount`, or behaviorally (after draining, N further fast-path sends each acquire without blocking and the (N+1)-th blocks). No leaked slot.
- **6.2 Deadlock / Dispose regression:** engage the cap (fill N, park ≥1 waiter), then `Dispose()`/`DisposeAsync()` with sends in flight; assert teardown **returns without hanging** (bounded by `TestTimeout`) and every task **settles** (completed / faulted / `OperationCanceledException` / `ObjectDisposedException`) — never hangs, never crashes.
- **6.3 Over-release guard:** assert the max-count ctor is in force — a white-box test (via `InternalsVisibleTo`) that an extra `Release()` throws `SemaphoreFullException`; plus a behavioral assertion that after N sends complete, exactly N (not more) slots are available.
- **6.4 Cancellation while waiting for a slot:** fill the cap; issue one more `Send(record, cts.Token)` (parks at `WaitAsync`); cancel `cts`; assert the returned task faults with `OperationCanceledException`/`TaskCanceledException` and **no slot leaked or spuriously acquired**. Keep the existing already-cancelled-token → synchronous `OperationCanceledException` (fast path, unchanged).
- **6.5 Backpressure engages:** with `MockProducer(autoComplete:false)`, fire N+1 async sends without completing any; assert the (N+1)-th does **not** reach `Producer_send` until a completion frees a slot (mock history stays at N, or the (N+1)-th task doesn't transition until `MockCompleteNext`).
- **6.6 Re-perf measurement (not a hard unit gate):** run PerfV3 (`make producer-perf-test-dotnet CLIENT_VERSION=3`) paced 50k async, `VALUE_SIZE=1024`, `acks=all`, **default `buffer.memory`**; expect p50 toward ~6–12 ms (from ~53–61 ms) and record throughput. Not hard-gated in xUnit (local perf is env-dependent + the full perf leg is intermittently flaky); reported in the close-out. The functional cap tests (6.1–6.5) are the hard gates.

**Existing tests to re-confirm green (no regression):** `PublicProducerSendTests` (post-Dispose `ObjectDisposedException` `:286`, already-cancelled `:299`, concurrent churn `:426-489` — all use `Assert.ThrowsAsync`, so a fast-path synchronous throw still satisfies them), `PublicProducerSendAllocationBudgetTests` (uncontended fast path keeps its budget), `PublicProducerTeardownTests`.

---

## 7. Settled decisions (approved)

1. **Cap value = 1000 now**, named const `MaxInflightSends`; bump to 2000 later is a one-liner.
2. **Managed cap this phase** (not merely documenting `buffer.memory` tuning).
3. **Milestone M11 / Phase P6**, Mode A, N=39.
4. **On by default: YES** (Java-faithful; Python is on-by-default; fixes latency without user tuning).
5. **Fixed const, no config key this phase** (Python uses a const; a config key can be added later non-breakingly, still Mode A).
6. **Teardown wake mechanism: `_sendGate` CTS → `OperationCanceledException`** (not semaphore-dispose/ODE) — keeps every release unconditionally safe.

### 7.1 Python parity vs deliberate divergence (recorded)
Same intent, different mechanism. Python caps its *own binding-side accumulator* and its `send()` **accepts the record, then waits** on `on_space_available` before returning (`producer.py:258-270` sync, `:449-465` async). .NET has **no binding accumulator** — records go straight to the core via `Producer_send` — so .NET must **acquire a slot BEFORE `Producer_send`** (else the core buffer grows unbounded, defeating the purpose). Net effect identical: ≤ ~1000 records outstanding. Acquire-before-send vs accept-then-wait is a faithful-to-intent divergence forced by the architectures; recorded in the close-out and `ffi-marshalling.md §A7`.

### 7.3 Interaction with `buffer.memory` (confirmed, no conflict)
Both bound in-flight; **the tighter wins.** A 1000-record cap sits well under the default 32 MB core buffer (~32k records), so the cap is the effective bound and the core buffer effectively never fills — additionally **mitigating** the existing "concurrent close can't wake a send stuck on a full core buffer" limitation (which only bites when the core buffer fills). Caveat documented: if a user sets `buffer.memory` *below* the cap's footprint, the core buffer becomes the tighter bound and an inline `Producer_send` can still block on it (existing behavior).

### 7.4 Async caller-throttling nuance (documented, not a blocker)
Backpressure is fully effective for callers that **await** each `Send` (or use a bounded pipeline like PerfV3's channel). A pure fire-and-collect caller piles up parked state machines, but the **core buffer is still protected** (`Producer_send` is gated) — the latency fix — matching Java (`send()` blocks the calling thread; unbounded-thread callers also pile up). Documented; not addressed this phase.

### 7.5 Ordering under backpressure (documented)
`SemaphoreSlim.WaitAsync` is FIFO, so a single logical send flow preserves `Producer_send` order under backpressure; concurrent multi-flow callers have no ordering guarantee (same as Java). Documented; no action.

---

## 8. Definition of Done

Per `.claude/rules/definition-of-done.md` and `bindings/dotnet/CLAUDE.md`:
- All new methods/paths implemented; no TODO/FIXME; no duplicated types.
- Host-only scaffolding (`SemaphoreSlim`/`CancellationTokenSource`) adds **no Kafka behavior** (binding-layer flow control mirroring Python, not core logic) — allowed under `bindings/CLAUDE.md §1`.
- **DoD §10 (hot-path allocation):** the uncontended fast path adds **zero** per-send allocation (`Wait(0)` only); the contended path's state-machine + linked-CTS box is on the throttled path and acceptable. Guarded by §6.x + the existing allocation-budget test staying green.
- Build 0W/0E across the TFM matrix (lib netstandard2.0/net8.0/net10.0; tests net462/net8.0/net10.0); `dotnet format --verify-no-changes` clean; `cargo build --features ffi` shows **no header delta** (Mode A).
- `dotnet test -f net10.0` green (all new + existing). **CI-only gates (not loop blockers):** net8.0 **execution** (runtime absent locally — build-verified only); the Docker perf smoke / re-perf numbers (§6.6). State explicitly in STATUS + the reset COMMENTS note.
- Docs synced: `ffi-marshalling.md §A7`, `SendCompletionPump.cs` "Backpressure" xmldoc; `CLAUDE.md §3` if applicable.
- No Java classes translated → `marked_classes.txt` unchanged.

---

## 9. Deliverables / files expected to change (all under `bindings/dotnet/`)
- `src/Confluent.Kafka/Internal/NativeProducer.cs` — `_inflight`, `_sendGate`, `MaxInflightSends`; `SendViaPump` fast/slow split; `SendAcquired` + `SendAfterWaitAsync`; teardown `_sendGate.Cancel()`/dispose; `EnsurePump` passes the release delegate.
- `src/Confluent.Kafka/Internal/SendCompletionPump.cs` — ctor takes `Action<int> releaseSlots`; `ProcessBatch` `static`→instance; `releaseSlots(count)` after each destroy site (`ProcessBatch` finally, `DrainAndFaultRemaining`, `Enqueue` stopped-path); `FaultBatchCompletions` stays release-free.
- `tests/Confluent.Kafka.UnitTests/PublicProducerInflightCapTests.cs` (new) — §6.1–6.5; possibly a test-only `InternalsVisibleTo` for `_inflight.CurrentCount`.
- `.claude/rules/ffi-marshalling.md` (§A7 doc-sync), `CLAUDE.md §3` (if applicable).
- `design/current/STATUS.md` — M11/P6 entry (at handoff).

---

## 10. Risks / blockers
- **Slot leak → deadlock** — the headline risk; addressed by §5 (release paired to the exactly-once future-destroy + max-count guard + teardown wake). Critic N=39 must independently walk the §5 table against the code.
- **Teardown/parked-waiter race** — addressed by `_sendGate.Cancel()` + `_closed` re-check; residual is the documented existing misuse envelope.
- **Allocation regression on the fast path** — avoided by `Wait(0)`; guarded by the allocation-budget test.
- **No blockers** — Mode A, all ABI symbols already present, `SemaphoreSlim` on the floor.

---

## 11. Execution loop
Actor N=39 implements → Critic N=39 reviews commits (must verify §5 exactly-once table, Mode-A `git diff`, fast-path allocation budget, teardown no-hang) → Manager summarizes `COMMENTS.39.md`/`COMMENTS.DONE.39.md` → fix cycles until no approved issues → handoff (update `design/current/STATUS.md`, archive `COMMENTS.DONE.39.md` under `design/history/M11/P6-producer-inflight-cap/`, reset binding-root `COMMENTS.39.md`). Commit only — do NOT push (human manages pushes). Commits `--no-gpg-sign`, footer `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`; per-path `git add`.
