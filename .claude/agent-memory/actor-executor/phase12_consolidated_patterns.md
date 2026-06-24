---
name: phase12_consolidated_patterns
description: Milestone-8 Phase 12 close-out — production ctor wiring, single-source-of-truth state notifier, shared Arc<AtomicI64>, response-routing audit, Phase 12.5 charter
metadata:
  type: project
---

Milestone-8 Phase 12 (AsyncKafkaConsumer production ctor + `new_consumer`
factory swap + integration tests) is CLOSED. The phase shipped the full
production constructor wired through `new_consumer<K, V>(...)`. Four
integration tests in `tests/integration/consumer_test.rs` are written
but `#[ignore]`-gated on a pre-existing Phase-10 response-routing gap
that Phase 12.5 will close.

## What shipped (commits 1-7/N)

- **Production ctor.** `AsyncKafkaConsumer::new(config, kd, vd)`
  translates Java's primary ctor (`AsyncKafkaConsumer.java:355-518`)
  line-for-line. Builds the full dependency closure (subscriptions,
  metadata, `NetworkClient`, `BackgroundEventHandler`, `FetchBuffer`,
  `Deserializers`, `ConsumerInterceptors`,
  `OffsetCommitCallbackInvoker`, `RequestManagers`,
  `ConsumerStateNotifier`, `ApplicationEventHandler`,
  `CompletableEventReaper`, `FetchCollector`), registers the
  state-notifier on the membership manager, and spawns the bg task.

- **Factory swap.** `new_consumer()::GroupProtocol::Consumer` arm
  flows through `AsyncKafkaConsumer::new`. Classic arm continues to
  return `unsupported_version`.

- **State-notifier single-source-of-truth (Issue 2).**
  `AsyncKafkaConsumerComponents` carries `group_metadata`,
  `group_assignment_snapshot`, and `state_notifier` as mandatory Arcs.
  Same Arcs registered on membership manager AND stored on the consumer
  struct. Java parity: single `AtomicReference<Optional<ConsumerGroupMetadata>>`
  and single `MemberStateListener` at `AsyncKafkaConsumer.java:289,
  343-353`.

- **Shared `Arc<AtomicI64>` for `max_time_to_wait_ms` (Issue 3).** Bg
  task writes via `cached_max_time_to_wait_ms.store(...)` every
  `run_once`; app-side reads via `maximum_time_to_wait_ms()` through
  the same Arc. Both ends seeded with `MAX_POLL_TIMEOUT_MS` (not
  Java's `Long.MAX_VALUE` — explicit "wake at least this often"
  bound).

- **Smoke test (Issue 9 moot).** `new_consumer_builds_and_closes_against_refused_broker`
  runs in ~100ms against `127.0.0.1:1`. The Critic predicted 30s; the
  measurement disagreed.

## What deferred to Phase 12.5

**Response-routing gap** — 4 of 6 `RequestManager`s do NOT call
`take_response_receiver()` when building their `UnsentRequest`s. The
oneshot receiver is dropped at request build time and broker responses
are silently lost. Audited in
`design/history/Milestone-8/Phase-12/RESPONSE-ROUTING-AUDIT.md`.

| RM | Verdict | Build line | Receiver-take site |
|---|---|---|---|
| `coordinator_request_manager` | **BROKEN** | 240 | NONE |
| `consumer_heartbeat_request_manager` | **BROKEN** | 278 | NONE |
| `commit_request_manager` | WIRED | 1413, 1479 | 1414, 1480 |
| `fetch_request_manager` | **BROKEN** | 266 | NONE |
| `offsets_request_manager` | WIRED | 326, 852, 924 | 327, 854, 925 |
| `topic_metadata_request_manager` | **BROKEN** | 344 | NONE (only in test at 896) |

Phase 12.5 charter at `design/history/Milestone-8/Phase-12.5/PLAN.md`.

## Canonical WIRED pattern

From `commit_request_manager.rs:1410-1429`:

```rust
let mut unsent = UnsentRequest::new(Box::new(builder), coord_node);
let response_rx = unsent.take_response_receiver().expect("receiver fresh");
let inner_for_handler = Arc::clone(inner);
tokio::spawn(async move {
    match response_rx.await {
        Ok(Ok(mut client_response)) => { /* handle success */ },
        Ok(Err(err))                => { /* handle failure */ },
        Err(_recv_err)              => { /* receiver dropped — translate to NetworkException */ },
    }
});
unsent
```

Two flavors of state access for the spawned forwarder:

1. **`Arc<...Inner>` interior mutability** — used by
   `commit_request_manager` (`CommitRequestManagerInner`). Forwarder
   clones the Arc and writes through interior mutability. Best when
   the RM's state is self-contained.
2. **mpsc channel-back** — used by `offsets_request_manager`. Forwarder
   posts a `PendingCompletion` enum variant onto a manager-owned
   `mpsc::UnboundedSender`; drained in `RequestManager::poll(now)`.
   Best when the forwarder needs access to state the manager also
   touches via `&mut self` (avoiding interior-mutability sprawl) or
   when the state-update has side effects beyond the RM (e.g.
   needs to call into `MembershipManager` which has its own locks).

## §16 risk on Phase 12.5 heartbeat

Phase 12.5 heartbeat fix needs `on_heartbeat_success` to call into
`membership_manager.on_heartbeat_success(response)` — but
`MembershipManager` holds `Arc<Mutex<...>>` itself. If the heartbeat
forwarder takes a `MutexGuard` and then `.await`s anything, it
deadlocks (§16). The mpsc channel-back pattern avoids this by
deferring the cross-RM call into the next `poll()` cycle.
**Recommendation in Phase 12.5 PLAN.md: channel-back for heartbeat.**

## Phase-11 deferral markers updated

The 5 markers listed at `Phase-12/PLAN.md:148-166` were re-examined
at close-out (commit 7/N). Each one depends on response routing
(Phase 12.5), not the production ctor. Updated the deferral comments
in-place from "Phase 12 integration tests" to "Phase 12.5 per
RESPONSE-ROUTING-AUDIT.md". No test was newly translatable from the
production ctor alone — the bg task cannot drive
FindCoordinator → Heartbeat → Fetch → OffsetCommit end-to-end until
4/6 RMs have their response receiver plumbed through.

## Pattern: stale-rustdoc deferral

When the rustdoc on a class lies about how the code is wired (Issue 8
in this phase), and the rewrite is bundled with the wire-up fix
(Phase 12.5), **leave the stale rustdoc in place as a "bug marker"
during the gap**. Editing the rustdoc to "the bg task will wire this
in Phase 12.5" creates churn at the same lines Phase 12.5 will
rewrite. The audit document (`RESPONSE-ROUTING-AUDIT.md`) committed
separately is the canonical "the rustdoc is wrong" reference for any
reader who lands on those files in the interim.

## What did NOT need fixing

- **Smoke test runtime (Issue 9).** Critic predicted 30s for
  `close().await` against refused broker. Measurement: 100ms. The
  `submit_and_drain` deadline math via `request_timeout_ms` is bounded
  by the bg-task's actual shutdown — the bg task's `run_once` returns
  `closing` well before the deadline fires.

## Open items at phase close

`COMMENTS.1.md` is empty. All 9 Critic issues resolved (6 closed in
Phase 12, 3 carried to Phase 12.5 with rationale in
`COMMENTS.DONE.1.md`).
