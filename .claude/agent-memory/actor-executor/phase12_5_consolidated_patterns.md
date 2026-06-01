---
name: phase12_5_consolidated_patterns
description: Phase 12.5 close-out — per-RM response routing patterns (Arc<Mutex<Inner>> vs Arc<Inner> + mpsc), state-listener registration, error-classifier mapping, integration test stabilization
metadata:
  type: project
---

Phase 12.5 (CLOSED, `ba37a51..9662a77`) wired response routing for
the 4 BROKEN RequestManagers (`coordinator_request_manager`,
`topic_metadata_request_manager`, `consumer_heartbeat_request_manager`,
`fetch_request_manager`) and un-ignored all 4 integration tests.

**Why:** Phase 12.5 closed a structural Phase-10 carry-over — Phase 10
did not enumerate per-RM `whenComplete` translation, so 4/6 RMs had
their `UnsentRequest` build sites returning `UnsentRequest::new(builder, None)`
without ever calling `take_response_receiver()`. Broker responses
fired the `FutureCompletionHandler` oneshot but the `Receiver` was
already dropped — responses dropped silently, manager state machines
stuck forever. Integration tests had to be `#[ignore]`-gated.

**How to apply:** Pick the right RM topology when wiring response
routing — see "RM Topology decision tree" below. The patterns landed
in Phase 12.5 are now the canonical reference for any future
Java→Rust `whenComplete` translation.

## RM Topology decision tree

1. **RM has only sync `poll(now)` handlers (no async transitions)** →
   refactor to `Arc<Mutex<Inner>>` interior mutability. Build path:

   ```rust
   pub struct Manager {
       inner: Arc<Mutex<Inner>>,
   }
   fn make_request(&self) -> UnsentRequest {
       let mut unsent = UnsentRequest::new(Box::new(builder), node);
       let response_rx = unsent.take_response_receiver().expect("receiver fresh");
       let inner = Arc::clone(&self.inner);
       tokio::spawn(async move {
           match response_rx.await {
               Ok(Ok(client_response)) => {
                   let mut g = inner.lock().unwrap();
                   g.on_response(client_response);
               },
               Ok(Err(err)) => {
                   let mut g = inner.lock().unwrap();
                   g.on_failure(err);
               },
               Err(_recv_err) => { /* sender dropped — manager owns no state to clean up */ },
           }
       });
       unsent
   }
   ```

   Examples: `coordinator_request_manager.rs` (`ba37a51`),
   `topic_metadata_request_manager.rs` (`a239b80`).

2. **RM has async transitions the sync `poll(now)` cannot drive
   inline** (e.g. `transition_to_fenced/_fatal` await §31
   `onPartitionsLost` listener) → keep state private inside an
   `Arc<Inner>` with interior mutability, BUT route async work via
   an mpsc side-channel that the bg task drains BEFORE
   `membership.reconcile(now).await`. Build path:

   ```rust
   pub struct Manager {
       inner: Arc<Inner>,
       pending_membership_transition_tx: mpsc::UnboundedSender<PendingMembershipTransition>,
       pending_membership_transition_rx: ...,
   }
   // In sync error handler:
   self.pending_membership_transition_tx.send(PendingMembershipTransition::Fenced).ok();
   ```

   Bg task in `run_once`:
   ```rust
   // Phase-2 manager poll
   manager.poll(now);
   // NEW Phase-2.5: drain pending async work BEFORE reconcile
   while let Some(t) = manager.try_take_pending_membership_transition() {
       match t {
           PendingMembershipTransition::Fenced => membership.transition_to_fenced().await?,
           PendingMembershipTransition::Fatal(err) => membership.transition_to_fatal(err).await?,
       }
   }
   // Phase-3 reconcile
   membership.reconcile(now).await;
   ```

   Examples: `consumer_heartbeat_request_manager.rs` (`1950caf`),
   `fetch_request_manager.rs` (`c2834cd`).

3. **RM already had it** (e.g. `commit_request_manager.rs`,
   `offsets_request_manager.rs`): already calls
   `take_response_receiver()` + spawns dispatch. The Phase-10 commit
   landed this pattern but Phase 10's PLAN didn't enumerate it as
   per-RM work, so the other 4 were missed.

## Listener-registration gotcha (Issue 7)

`CommitRequestManager` requires `member_info` (member_id + epoch)
populated before OffsetCommit requests carry the right identity. The
gotcha: the listener registration on the membership inner has to be
performed in `new_with_components`, NOT inside `RequestManagers::new`
or the commit RM ctor. Why: the membership inner is constructed in
`new_with_components` and registered listeners receive the
`update_member_epoch` fanout only AFTER they're added.

Symptom of missing registration: broker returns
`UNKNOWN_MEMBER_ID` on every OffsetCommit; consumer1.commit_sync
fails repeatedly. Regression test:
`commit_picks_up_member_id_epoch_after_registration` in
`async_kafka_consumer.rs` — instantiates membership inner, registers
the commit RM as a listener, calls `update_member_epoch(42)`,
asserts `commit.member_info_for_test().member_id` reflects the
fanout.

## HeartbeatErrorAction classifier — unknown error codes (Issue 5)

When `classify_response_error` returns `DelegateToSpecific` and the
specific handler returns `None` (no consumer-specific match), the
`unwrap_or_else` fallback MUST map to `Fatal`, not `Handled`. Java's
`AbstractHeartbeatRequestManager.java:435-441` `default:` arm calls
`handleFatalFailure(error.exception(errorMessage))` — silently
swallowing unknown error codes is a behavior regression.

The previous "we unwrapped to `Handled`" comment on the
`DelegateToSpecific` arm of the outer match is now stale; the
mapping fallback is `Fatal`. Updated in commit (6/N).

## Fenced vs Fatal — ErrorEvent emission matrix (Critic round-3)

| HeartbeatErrorAction | `BackgroundEvent::Error`? | `transition_to_*`? |
|----------------------|---------------------------|--------------------|
| `Handled`            | No                        | No                 |
| `Fenced`             | **NO** (internal state)   | `_fenced`          |
| `Fatal(err)`         | YES                       | `_fatal(err)`      |

Java: `AbstractHeartbeatRequestManager.java:411-427` (FENCED arm)
does NOT call `backgroundEventHandler.add(new ErrorEvent(...))`.
Translates to: Rust's `Fenced` arm of `handle_heartbeat_response`
must NOT emit `BackgroundEvent::Error`. Issue 6 fix.

## Integration test stabilization (Issue 8)

`test_commit_sync_then_resume_in_same_group` required three
test-side adjustments after Issue 7 fix landed:

1. `max.poll.records=5` on consumer1's config (production code is
   correct; default 500 lets one fetch return all 10 records).
2. 5s `tokio::time::sleep` between consumer1.close and consumer2
   construction (LeaveGroup processing race).
3. consumer2's polling deadline 30s → 60s (KIP-848 rebalance
   typically <10s but tolerates rare longer delays).

All three are test-side only. No production code changes.

## Test infra carry-overs

- Test files matching `tests/integration/*_test.rs` use the
  `--features integration-tests --test integration` invocation
  (not `--test integration_main` — that target doesn't exist).
- `cargo test --features integration-tests --test integration
  consumer_test` runs the 4 consumer integration tests; takes
  ~100-110s end to end.
- testcontainers + `ClusterConfig::with_properties(...)` is the
  way to enable KIP-848 broker-side
  (`KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS=classic,consumer`).
  Do NOT mutate `ClusterConfig::default()` — other integration
  suites depend on its hash key for cluster pooling.

## Related memories

- [[phase12_5_commits_1-2_notes]] — coordinator + topic_metadata
  Arc<Mutex<Inner>> migrations
- [[phase12_5_commit_4_notes]] — fetch_request_manager mpsc
  channel-back
- [[phase12_5_critic_round2_patterns]] — Java "reset state at top
  of error branch" parity, sync-to-async cross-RM dispatch
- [[phase12_5_critic_round3_patterns]] — error classifier
  unwrap_or default arm, ErrorEvent emission matrix
- [[phase12_consolidated_patterns]] — Phase 12 close-out, Phase
  12.5 charter (now closed)
