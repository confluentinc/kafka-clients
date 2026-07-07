# Critic 0 — Milestone 9 Phase 0a (`common.metrics` foundation)

Round-1 Issues 1, 2, and 3 are all RESOLVED and verified; see `COMMENTS.DONE.0.md`.
The rule-update suggestion below is left for the coordinator to action.

## Rule-update suggestions (per agent-roles.md)
`COMMENTS.FP.md` / `COMMENTS.FN.md` do not exist at repo root yet. If Issue 1 / Issue 3
recur across translated `toString`/message code, consider adding to `CLAUDE.md`
a note that **`double`→string parity requires a Java-`Double.toString`-style
formatter** — Rust `{}` on `f64` both drops the trailing `.0` for integral values
**and** never switches to scientific notation (Java does at `|x| >= 1e7` and non-integral
`|x| < 1e-3`) — whenever the rendered text is part of a behavioral/message contract.

---

# Critic 0 — Milestone 9 Phase 0b (Metrics registry + Sensor)

Phase 0b verdict was **READY**. Issues 5 and 6 are RESOLVED (see `COMMENTS.DONE.0.md`,
which also records the Critic's adjudications, verified-clean list, and verdict). One
LOW item remains open, intentionally carried forward:

## Issue 4 (Phase 0b): Reporter callbacks are not fault-isolated (Java wraps each in try/catch + log-and-continue)
- **DEFERRED to Phase 5 by coordinator decision.** Whether reporter callbacks become fallible (`-> Result`) or are wrapped in `catch_unwind` is a design decision for when the `MetricsReporter` trait surface is finalized alongside `ClientTelemetryReporter`; the only current reporters are no-ops, so nothing breaks today. Left here (not in DONE) so Phase 5 inherits it.
- **File**: `src/common/metrics/metrics.rs` (`register_metric` → `metric_change`, `remove_metric` → `metric_removal`, `close` → `close`)
- **Severity**: Behavior Mismatch (LOW; non-blocking, forward-looking)
- **Java Reference**: `Metrics.java:595-601` (`registerMetric`: `try { reporter.metricChange(metric); } catch (Exception e) { log.error(...); }`), `:552-558` (`removeMetric` / `metricRemoval`), `:686-693` (`close`)
- **Description**: Java isolates a faulting reporter: each `metricChange` / `metricRemoval` / `close` call is wrapped in `try/catch(Exception)` and logged, so one bad reporter neither aborts the register/remove/close operation nor prevents the *remaining* reporters from being notified. The Rust translation calls the reporter callbacks in a bare loop; because the trait methods return `()`, a reporter can only signal failure by panicking, and a panic propagates out of `register_metric`/`remove_metric`/`close`, aborting the operation and skipping later reporters. This is a robustness divergence for third-party reporters (a public plugin surface).
- **Expected**: a faulting reporter is contained; other reporters still fire and the operation completes.
- **Actual**: a panicking reporter unwinds the whole call and skips later reporters.
- **Mitigating context (why LOW / non-blocking)**: the only reporters are `FakeMetricsReporter` (no-op) and the test `LockingReporter`; neither faults, and no test exercises a throwing reporter. The reporter trait shape is deferred to Phase 5. **Recommendation**: when finalizing the reporter surface in Phase 5, restore fault isolation (fallible callbacks + log-and-continue, or per-call `catch_unwind`).
