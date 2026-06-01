# Phase 12 Review — Critic N=1

Reviewing commits (1/N) `a4378ce`, (2/N) `143f30a`, (3/N) `d3e9e15`.

| Batch | Issues | Status |
|---|---|---|
| 1 | — | All resolved — see `COMMENTS.DONE.1.md` |

---

## Notes on what was NOT filed

- **Single `tokio::spawn`** (`consumer-threading.md` §10): verified — exactly one `tokio::spawn` in the production ctor (line 1024). No per-event / per-RM spawns.
- **State-notifier registration happens before bg-task spawn**: verified — `register_state_listener` at line 914, `tokio::spawn` at line 1024.
- **Group-protocol gate**: verified — when `group_id.is_none()`, all four group-conditional managers are `None` (coordinator, commit, membership, heartbeat). The `.map(...)` chain at lines 752-826 handles both arms correctly.
- **Receive-path zero-copy** (`consumer-threading.md` §27): the new ctor doesn't touch the deserializer path — `Deserializers::new(key_deserializer, value_deserializer)` is wrapped in `Arc::new` once and threaded through. No new copies introduced.
- **Field-init order vs Java lines 390-508**: walked side-by-side. Order matches Java with the documented deferrals (telemetry, metrics, JMX). No silent re-ordering.
- **`Arc::clone(&metadata).metadata_arc()` extra clone** (line 700): performance nit only — `Arc::clone` then immediately calling `.metadata_arc()` is one extra atomic refcount op. Not worth filing.
- **Closure `signal_close_fn` lifetime**: verified — captures `signal_close_running: Arc<AtomicBool>` (clone of the bg-task's running flag) and `signal_close_wakeup: WakeupTrigger` (clone, which is itself an `Arc` internally). Closure outlives the spawn closure as required.
- **Two-reaper "unified into one" decision** (Phase 10 design pattern #2): the comment at line 956-958 acknowledges Java has two reapers; Rust uses one. Verified consistent with Phase 10 PLAN.

## Possible false-positive risk (not filed; flagging for confirmation)

- **`OffsetCommitCallbackInvoker` is given a FRESH `ConsumerInterceptors` instance instead of sharing with the `_interceptors` field** (line 729-731). Java line 446 passes the same `interceptors` reference. In current state both end up with empty Vecs, so behaviorally identical. If a future commit wires interceptor loading (Phase 12 PLAN.md "Out of scope"), this becomes a real divergence — the invoker would dispatch through its empty list while the consumer's interceptors held the loaded list. **Addressed via inline `TODO(milestone-N-interceptors)` marker at the callsite** — not filed as a Phase-12 issue.
