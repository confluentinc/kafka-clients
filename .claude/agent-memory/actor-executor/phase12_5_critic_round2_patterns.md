---
name: phase12_5_critic_round2_patterns
description: Two reusable patterns from Phase 12.5 round-2 fixes (Issues 3+4) — Java `resetHeartbeatState`-at-top-of-error-branch parity, and async-transition side-channel for sync-to-async cross-RM dispatch
metadata:
  type: project
---

# Phase 12.5 Critic round-2 patterns

Two patterns surfaced in fixing Issues 3 and 4 against commit
`1950caf Phase 12.5 (3/N): consumer_heartbeat_request_manager`.

## Pattern A — Java "reset state at the top of the error branch"

**Symptom**: Rust translation of Java's `onErrorResponse` branched on
the error code but did NOT call `resetHeartbeatState()` at the top of
the error branch. The transport-failure path correctly called the
equivalent, so it looked complete to surface review.

**Java line**: `AbstractHeartbeatRequestManager.java:356` —
`resetHeartbeatState();` runs at the TOP of `onErrorResponse`, before
`onFailedAttempt(currentTimeMs)` and before the per-error switch.

**Reusable rule**: when translating a Java handler whose **first
statement** is a state-reset call, the Rust translation MUST mirror
that first statement explicitly in BOTH the success-error and
transport-error branches (or wherever the dispatch lands). Do NOT
assume `classify_*` helpers will reset state by side effect — they
typically reset only the abstract layer's own per-attempt tracking,
NOT the per-request `SentFields`-style diff trackers that live on the
concrete (consumer-specific) subclass.

**How to apply during review**: when a Java handler has the shape
"reset X; then classify; then per-error dispatch", grep the Rust
translation for `reset` calls and verify they land at the same logical
position in both error branches. A missing reset in only the
response-error branch (and not the transport-error branch) is a
common asymmetric-translation bug.

## Pattern B — Sync-to-async cross-RM dispatch via side-channel

**Symptom**: Java's `whenComplete` lambda runs synchronously and can
call `membershipManager().transitionToFenced()` /
`transitionToFatal()` directly. Rust's equivalent transitions are
`async` (they await §31 onPartitionsLost listener callbacks), but
the heartbeat manager's `poll(now)` is sync (per
`consumer-threading.md` §10). A naïve translation either:

  - emits `BackgroundEvent::Error` to the user and pretends the bg-task
    will drive the transition — but no bg-task hook exists.
  - tries to `block_on(transition_to_*)` from inside sync `poll(now)`
    — deadlocks the runtime.

**Pattern**: side-channel via `mpsc::UnboundedChannel<TransitionEnum>`
on the heartbeat manager. The sync `poll(now)` pushes a transition
envelope; the bg-task `run_once` drains via a thin façade
(`RequestManagers::take_pending_membership_transitions()`) AFTER
`entries().poll(now)` (so the heartbeat's drain has classified) and
BEFORE `membership.reconcile(now).await` (so the membership state
machine observes the fence/fatal before reconciliation runs). The
bg-task drops the `request_managers` mutex guard before `.await`ing
the transition — §16 audit.

**Concrete signature shape**:

```rust
// heartbeat manager
pub(crate) enum PendingMembershipTransition {
    Fenced,
    Fatal(KafkaError),
}

impl ConsumerHeartbeatRequestManager {
    pub(crate) fn take_pending_membership_transitions(
        &mut self,
    ) -> Vec<PendingMembershipTransition> { ... }
}

// request_managers façade
impl RequestManagers {
    pub(crate) fn take_pending_membership_transitions(
        &mut self,
    ) -> Vec<PendingMembershipTransition> {
        match self.consumer_heartbeat.as_mut() {
            Some(h) => h.take_pending_membership_transitions(),
            None => Vec::new(),
        }
    }
}

// bg-task run_once, between entries() and reconcile()
if let Some(membership) = self.membership.clone() {
    let pending = {
        let mut rm_guard = self.request_managers.lock().unwrap_or_else(|p| p.into_inner());
        rm_guard.take_pending_membership_transitions()
    };
    for transition in pending {
        match transition {
            PendingMembershipTransition::Fenced => {
                let _ = membership.transition_to_fenced(now).await;
            }
            PendingMembershipTransition::Fatal(err) => {
                let _ = membership.transition_to_fatal(now).await;
            }
        }
    }
}
```

**Reusable when**:

- A Java `whenComplete` lambda OR a sync `poll(...)` method calls
  another RequestManager's method that is `async` in Rust.
- The async target manager is held as `Arc<...>` (so the bg-task can
  clone the handle and `.await` outside the lock).

**Test shape**: drive the source manager's classification, then call
the new `take_pending_*` drain accessor to assert the envelope was
pushed; then explicitly `.await` the target transition and assert the
post-transition state. This pattern tests both the side-channel
mechanism AND the downstream transition without needing the full
bg-task to be running.

**Cross-reference**: §31 (listener-callback handshake on caller task)
and §16 (no mutex held across `.await`) are both relevant — the
side-channel routes WORK from sync `poll(now)` to the bg-task's
`.await` boundary, which is the same boundary where §31 listener
handshakes resolve.

## Pattern C — `cfg(test)` field accessors for inspecting private state

For per-instance diff trackers like `SentFields` that are not visible
on the public request body, a `#[cfg(test)] pub(crate) fn
sent_fields_topics_populated(&self) -> bool` accessor is cleaner than
trying to downcast the `&dyn RequestBuilder` returned from
`UnsentRequest::request_builder()` (which has no downcast facility).
Test-only accessors should expose a **single boolean / minimal
observable**, not the raw inner field, so the production API surface
stays unchanged.
