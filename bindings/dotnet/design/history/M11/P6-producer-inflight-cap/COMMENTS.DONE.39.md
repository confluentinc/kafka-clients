# COMMENTS.DONE.39 — resolved items, M11/P6 producer async in-flight cap

Reviewer: dotnet-critic (N=39). Actor fix cycle (N=39).

---

## [NON-BLOCKING / LOW] `_sendGate.Token` read after `_sendGate.Dispose()` — a 4th ODE path not covered by the disposal reasoning — RESOLVED (fix direction (a) applied)

- **File:** `src/Confluent.Kafka/Internal/NativeProducer.cs` — the
  `CreateLinkedTokenSource(cancellationToken, _sendGate.Token)` in `SendAfterWaitAsync`, against the
  four `_sendGate.Dispose()` teardown sites (`Dispose` / `DisposeAsync` / `Close` / `CloseWithCallback`).
- **Severity:** LOW, non-blocking. Memory-safe; no slot leak; misuse-only.
- **Problem (as filed):** `CancellationTokenSource.Token`'s getter calls `ThrowIfDisposed()`, so a
  concurrent async `Send` that enters `SendAfterWaitAsync` and is preempted until teardown runs
  `_sendGate.Dispose()` throws `ObjectDisposedException` with `ObjectName="CancellationTokenSource"`
  instead of `"NativeProducer"` — a cosmetic divergence from the intended row-8 ODE. The field/teardown
  comments justified disposal by enumerating only three hazards and did not cover this 4th `.Token`
  access.

### Resolution — fix direction (a): do NOT dispose `_sendGate`

Made `_sendGate` symmetric with `_inflight` (which is deliberately not disposed):

- **Removed all four `_sendGate.Dispose()` calls** — in `Dispose` (site 3), `DisposeAsync` (site 4),
  `Close` (site 2), and `CloseWithCallback` (site 1). This removes the ONLY after-dispose access to
  `_sendGate` entirely, so the `.Token` getter in `SendAfterWaitAsync` can never observe a disposed
  source — the ODE-message divergence is eliminated at the root.
- **`_sendGate.Cancel()` kept exactly as-is** at all four teardown sites (first action after winning the
  `TryBeginClose` latch — the parked-waiter wake mechanism). Not removed, not reordered.
- **Field comment rewritten** to state `_sendGate` is intentionally NOT disposed: a
  `CancellationTokenSource` that never touches `AvailableWaitHandle`/`WaitHandle` (we only `Cancel()` and
  read `Token`) allocates no wait handle and needs no disposal; not disposing keeps the
  `SendAfterWaitAsync` `.Token` read safe (symmetric with the deliberate decision to not dispose
  `_inflight`). Removed the now-obsolete text that justified disposing `_sendGate` (the three-hazard
  enumeration + "disposed at the very end" / CTR-race justification).
- **Teardown comments updated** — each former `_sendGate.Dispose()` site now carries a one-line note that
  `_sendGate` is intentionally NOT disposed (pointing at the field comment).
- **Per-send linked CTS confirmed unchanged** — `using CancellationTokenSource linked =
  CreateLinkedTokenSource(cancellationToken, _sendGate.Token)` in `SendAfterWaitAsync` is still
  `using`-disposed each send, so registrations on `_sendGate`/`ct` do not accumulate.

**Scope of change:** comments + removal of the four `_sendGate.Dispose()` calls only. The §5
exactly-once acquire/release logic, the fast/slow (`Wait(0)` / `WaitAsync`) split, `SendAcquired`, and
the release/destroy accounting are untouched. Mode A (no Rust/header/cbindgen delta); no new
`[DllImport]`.

**Verified:** `cargo build --features ffi` (no header delta) · `dotnet build` 0W/0E across the TFM
matrix (lib ns2.0/net8.0/net10.0; tests net462/net8.0/net10.0) · `dotnet format --verify-no-changes`
clean · `dotnet test -f net10.0` 600/600 (incl. the 8 cap tests).
