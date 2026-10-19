# COMMENTS.DONE.74 — M16/P1 (the .NET soak client), Critic cycle 1

Actor N=74. All five of `COMMENTS.74.md`'s findings are closed here: **three fixed**,
**two accepted as permanent residuals** on the maintainer's ruling.

Reviewed commits: `80aef49e`, `7de5db78`, `2ee3984e`, `2dc9f8fd` (base `29467d14`).
Fixups: `fixup! feat(M16/P1): the .NET soak client…` and
`fixup! test(M16/P1): the soak client's broker-free unit suite`.

⚠ This file is a **local working record** and is deliberately **never committed** at the
binding root (`bindings/dotnet/CLAUDE.md` §8.4) — unlike `COMMENTS.74.md` it is *not*
covered by the root `.gitignore`, so keeping it out of commits is a discipline, not a
mechanism. The single tracked copy is the Manager's archive under
`design/history/M16/P1-soak-client/`.

---

## FIXED

### 74.1 — Medium — the shutdown path's final metrics write was unguarded

**Accepted in full.** The Critic's control-flow reading was verified against the tree
before changing anything: `TerminateAsync`'s only `try` covered `_producer.Close()` and
closed before `SampleResources()` / `WriteFinal()` / `Close()` / `FinalReport()`, and
`Program.Main`'s `try` closed before `await soak.TerminateAsync()`. So a full disk — the
failure the *rollover* guard is explicitly justified by — took the SUMMARY verdict and
the exit-code contract with it.

**Fix, in two layers, because the two consequences have different blast radii:**

1. **Inner** — `SoakClient.FinalizeMetrics(metrics, sampleResources, logger)`: a total
   no-throw boundary around `SampleResources()` + `SetMeasurementEnd` + `StopCollecting`
   + `WriteFinal` + `Close`, so **`FinalReport()` always runs** and the SUMMARY is
   printed even when the metrics file cannot be written. `sampleResources` is *inside*
   the guard, not before it, because the .NET port added the two throw sources the
   Critic named (`Process.Refresh()`, `GC.GetTotalMemory`) that Python's
   `resource.getrusage()` does not have.
2. **Outer** — `Program.Main` wraps the whole teardown region (`TerminateAsync`, then
   `Dispose`) so the process **always** returns one of `SoakExitCodes`' five.
   `exited.Set()` moved into a `finally`: the soak's work is over even if teardown threw,
   and leaving it unset would let the watchdog hard-exit `ConsumerWedged` 60 s later and
   overwrite a verdict that was already decided.

The verdict itself is no longer computed by inline branches — `SoakExitCodes.ExitCodeFor`
is extracted and tested (message loss outranks a wedged loop, matching Python's `main()`).
A guard that protects an untested computation is worth little.

**Regression tests** (`ShutdownPathTests`): `FinalizeMetricsAbsorbsAFailingWriter`,
`FinalizeMetricsAbsorbsAFailingResourceSample`,
`FinalizeMetricsWritesTheFinalWindowWhenNothingFails`, `ExitCodeForRanksMessageLossFirst`.
The two absorb-tests use `Record.Exception` so the no-throw half is falsifiable rather
than being a test that merely throws.

**Mutation check** (the Critic's own model, applied to the new guard): removing the
`try`/`catch` from `FinalizeMetrics` → build **0 Warning(s) / 0 Error(s)**, then
`Failed: 2, Passed: 9` on `ShutdownPathTests`. The build status is reported with it
because a failed build plus `--no-build` prints a bogus `Passed!` off the stale binary.

### 74.3 — Low — a genuine poll error in the shutdown window was counted

**Accepted.** The Critic's own verification that the *cancellation* path is already safe
(`OperationCompletionSource.Complete` prefers `TrySetCanceled` when cancellation was
requested) was re-read and is correct — so this is specifically the residual: a real
broker error that happens to resolve the poll inside the shutdown window.

**Fix.** The whole poll-failure policy now goes through one decision function,
`SoakClient.ClassifyPollFailure(stopRequested, retriable, message, consecutive, max, out
fatalReason) → PollFailureAction {Suppress, Continue, Abort}`, with the shutdown check
**first**, mirroring Python's `if not self.run: break` placed *before* `_classify_error`
(`soakclient.py:1689-1691`). The consumer loop routes through it, so the tests drive the
code production runs (`definition-of-done.md` §12) rather than a parallel copy.

Ordering is load-bearing and is stated at the site: suppression outranks the terminal
bound, so an error that *would* abort is still suppressed once shutdown is requested —
recording a fatal reason there would turn a clean exit into `ConsumerWedged`. A
"check the token last" fix would pass a naive test and get exactly this case wrong.

**Regression tests** (`SoakErrorClassificationTests`): `APollFailureDuringShutdownIsSuppressed`,
`ShutdownSuppressesEvenAnOtherwiseTerminalFailure` (the discriminating one),
`OutsideShutdownThePolicyIsTheTwoTierBound` (6 cases — the pre-existing two-tier
behaviour is unchanged, i.e. the fix *added* a case rather than replacing one),
`AnAbortStillCarriesItsFatalReason`.

**Mutation check:** removing the `stopRequested` branch → build **0 Warning(s) /
0 Error(s)**, then `Failed: 2, Passed: 22`.

### 74.4 — Low — delivery continuations were not joined before `FinalReport()`

**Accepted.** `_producer.Close()` joining the pump guarantees every `Task` is *resolved*,
not that its thread-pool continuation has *run* — so `delivered=` could print short on a
clean shutdown (reading as message loss to a human despite `verdict=PASS`), and any
`IncrCounter` landing after `_metrics.Close()` is silently dropped. Python cannot have
this: `flush()` serves its delivery callbacks on the calling thread.

**Fix.** A `_pendingDeliveries` counter, incremented **before** the continuation is
attached and decremented in `OnDelivery`'s `finally` (last, after the counters are
written — releasing earlier would let the SUMMARY be read mid-update). `TerminateAsync`
then drains it **between `_producer.Close()` and the final window**, via
`SoakClient.DrainCounterAsync(read, bound)`.

**The hazard the Manager flagged is closed by construction, twice.** The wait is
deadline-bounded at `DeliveryDrainBoundMs` = 5000 ms and logs a warning rather than
hanging; and it cannot livelock, because `TerminateAsync` has already awaited
`Task.WhenAll(_producerTask, _consumerTask)`, so no *new* continuation can be attached —
the count only falls. The watchdog stays the backstop and is not leaned on as the normal
path.

**Regression tests** (`ShutdownPathTests`): `DrainCounterIsBoundedWhenTheCounterNeverReachesZero`
(asserted on the **clock**, since a drain that waited forever would hang the test run
rather than fail it), `DrainCounterReturnsImmediatelyWhenAlreadyZero`,
`DrainCounterWaitsForALateContinuation` (the property the fix is *for* — a counter
released on a background thread partway through the bound is waited for, not raced),
`TheDeliveryDrainBoundIsPositiveAndBounded`.

**Mutation check:** replacing the wait loop with an immediate `read()` → build
**0 Warning(s) / 0 Error(s)**, then `Failed: 1, Passed: 10` —
`DrainCounterWaitsForALateContinuation`.

---

## ACCEPTED RESIDUALS — permanently accepted, tracked, NOT fixed

Both stay exactly as they are, on the maintainer's ruling. **No code change was made for
either, and no comment was added asserting anything unmeasured about them.** They are
recorded here so they remain tracked rather than silently dropped — the
`ffi-marshalling.md` §B2 M9/P4 Q1/Q3 pattern.

### 74.2 — Low — `Program.Main` disposes the two `ManualResetEventSlim`s while the watchdog may still be returning from `exited.Wait(...)`

**ACCEPTED — do not file again, do not fix.**

The Critic's rationale, kept verbatim in substance: the watchdog thread blocks in
`exited.Wait(TimeSpan)` for the whole of `TerminateAsync`, so it is *always* a live
waiter when `exited.Set()` fires; `Main` then disposes both events on return, and
`ManualResetEventSlim.Dispose` is documented as unsafe while another thread is using the
object. A waiter that has not yet returned from the kernel wait could observe a disposed
handle and throw `ObjectDisposedException` on a background thread.

**Why it is accepted:** the process is already exiting at that point, and the watchdog is
a background thread. The Critic also recorded this as **not reproduced** — the race is
timing-dependent and could not be forced — and as **.NET-introduced**: Python's
`threading.Event` has no disposal and no equivalent hazard, so there is nothing in the
ported behaviour this diverges from.

Note the 74.1 fix *narrows* it incidentally (`exited.Set()` now runs in a `finally`, so
the watchdog is released on the throwing path too), but that is a side effect, not a fix
for this item.

### 74.5 — Low — `CreateAsync` leaks a constructed producer when consumer construction fails

**ACCEPTED — do not file again, do not fix.**

The Critic's rationale, kept: the producer is constructed before the consumer and the
`catch` only does `metrics.Close(); throw;`, so a throwing consumer constructor leaves the
producer's `SafeHandle`, its tokio runtime and its two background threads undisposed.

**Why it is accepted:** the Critic **measured** the only consequence that would matter —
whether it can wedge the exit-code path — and it cannot: both binding threads are created
with `IsBackground = true` (`Internal/SendCompletionPump.cs:211`,
`Internal/SendAccumulator.cs:220`), so the CLR does not wait for them and
`Program.Main`'s `return SoakExitCodes.TransientStartup` still takes effect. The impact is
confined to an undisposed native producer at a process that is on its way out. Python has
the same shape, so it is inherited rather than introduced here.

---

## Gate after the fixes

| Gate | Result |
|---|---|
| `cargo build --features ffi --release` | exit 0 |
| `dotnet build -c Release` (both projects, net8.0 + net10.0) | **0 Warning(s), 0 Error(s)** |
| `dotnet test -f net10.0` | **145 passed, 0 failed, 0 skipped** (no `Test Run Aborted`) |
| `dotnet test -f net8.0` (under `~/.dotnet/dotnet`) | **145 passed, 0 failed, 0 skipped** (no `Test Run Aborted`) |
| `dotnet format --verify-no-changes` × 2, by path | exit 0, both |
| Mode-A proof + control-positive | unchanged: empty over `src/` / header / `cbindgen.toml` / `Cargo*`; `internal static extern` 219 = 219 |
| End-to-end, real KRaft broker, 25 s | `produced=1858 delivered=1858 consumed=1857 duplicates=0 missed=0 errors=0 verdict=PASS`, exit 0, zero warnings, drain completed inside its bound |

Test count **125 → 145** (+20: 11 in the new `ShutdownPathTests`, 9 added to
`SoakErrorClassificationTests`). It did not go down.
