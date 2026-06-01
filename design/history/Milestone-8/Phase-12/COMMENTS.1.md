# Phase 12 — Critic round 2

Re-reviewed fixups `db3f793`, `8e2540c`, `6114cb0` against the Java
contract and the round-1 issue resolutions.

| Round-1 Issue | Verdict | Evidence |
|---|---|---|
| 1 — commit poll wire-up | **resolved correctly** | `run_once` (consumer_network_thread.rs:445-454) takes coord+commit handles atomically, calls `commit_arc.poll_with_coordinator(&mut coord_g, now)` between coordinator and heartbeat (Java `entries()` order). `cleanup()` adds explicit `drain_pending_offset_commit_requests()` (line 770-773). Regression test passes. |
| 2 — single notifier | **resolved correctly** | Production ctor builds `group_metadata`/`group_assignment_snapshot`/`state_notifier` once (lines 906-917), registers the SAME `state_notifier` Arc on membership manager (line 920), then threads the SAME Arcs into `AsyncKafkaConsumerComponents` (lines 1141-1143). `new_with_components` consumes the fields directly — no second notifier built (lines 1162-1164). Regression test cast-to-`Arc<dyn MemberStateListener>` and dispatches via the listener trait method; the consumer-side `group_metadata()` observes the write. |
| 3 — shared `Arc<AtomicI64>` | **resolved correctly** | Three-way identity: ctor builds `Arc::new(AtomicI64::new(MAX_POLL_TIMEOUT_MS))` (async_kafka_consumer.rs:1063), `Arc::clone` passed to `ConsumerNetworkThread::new` (line 1074), original Arc moved into `components.max_time_to_wait_ms` (line 1130) → struct field (line 1171) → read by `maximum_time_to_wait_ms()` (line 1376). Bg-task writes via `self.cached_max_time_to_wait_ms.store(...)` (consumer_network_thread.rs:544). All five Phase-10 test callsites pass `Arc::new(AtomicI64::new(MAX_POLL_TIMEOUT_MS))`. |
| 4 — auth-closure FIXME | **resolved correctly** | Inline comment at async_kafka_consumer.rs:900-913 explicitly characterizes the `maybe_throw_auth_failure` no-op as a correctness gap (not a perf cost). `FIXME(phase-9-sasl)` marker added at line 915 with Java reference (`AbstractFetch.java:452-457`). |
| 5 — orphan-drop | **resolved (subsumed by Issue 2)** | `let _ = (group_metadata, group_assignment_snapshot);` and the "intentionally dropped" rationale are gone — `grep -n "let _ = (group_metadata"` returns no hits. The Arcs are now properly threaded through components. |
| 6 — stale `max_time_to_wait_ms` comment | **resolved** | Comment block at `async_kafka_consumer.rs:1092-1100` rewritten to describe the actual wiring (Arc seeded with `MAX_POLL_TIMEOUT_MS`, bg-task writes every iteration, app-side accessor reads through shared Arc). Folded into Phase 12 commit (4/N). |

Cleared to proceed with Phase 12 commits (5)-(7). Round-1 + round-2 issues are
fully resolved; no open comments.
