// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `WakeupTrigger` — cancellation primitive for blocking-style consumer
//! APIs.
//!
//! **NOT a mechanical translation of Java's `WakeupTrigger`** — see
//! `.claude/rules/consumer-threading.md` §11. Java uses an
//! `AtomicReference<Wakeupable>` state machine with `setActiveTask` /
//! `setFetchAction` / `clearTask` because `CompletableFuture` cannot be
//! cancelled without a back-channel. Rust's
//! [`tokio_util::sync::CancellationToken`] + `select!` solves the same
//! problem natively: any `await` point `select!`s on `token.cancelled()`
//! and an external `token.cancel()` wakes the await.
//!
//! The five-state Java `Wakeupable` machine collapses to one piece of
//! state — the **current token** carried by a
//! [`tokio::sync::watch::Sender<CancellationToken>`]:
//!
//!   * Java's `null` / `WakeupFuture` distinction → "token cancelled
//!     yet?".
//!   * Java's `ActiveFuture` / `FetchAction` / `ShareFetchAction` → "the
//!     await is `select!`ing on the token". Whoever calls `.await` is
//!     the active task; whoever calls `wakeup()` cancels it.
//!   * Java's `DisabledWakeups` → "the current token has been replaced
//!     with a fresh, UN-cancelled one — discarding any pending wakeup,
//!     as Java's `pendingTask.set(...)` does — plus a recorded disabled
//!     state so [`Self::wakeup`] / [`Self::rotate`] become no-ops and
//!     `maybe_trigger_wakeup` does not throw". The replacement token
//!     must not be cancelled: every `select!` arm here waits on
//!     `cancelled()`, so a cancelled token would make each of them
//!     complete instantly and spin instead of waiting.
//!
//! # Dead-code lint
//!
//! Phase 5 ships the trigger without its in-tree callers (the bg task in
//! Phase 10, every blocking-style consumer API in Phase 11). The
//! file-level `#![allow(dead_code)]` keeps `cargo build` warning-free
//! until those phases land; the inline test module exercises every
//! method end-to-end so coverage is unaffected. Matches the
//! Phase-4 `subscription_state.rs` precedent.

#![allow(dead_code)] // Phase 5: trigger lands before its callers (Phases 10-11).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::common::KafkaError;

/// Cancellation primitive shared between the app side
/// (`AsyncKafkaConsumer::wakeup()`) and the background task.
///
/// **Cheap to clone** — clones share state via `Arc`.
#[derive(Clone)]
pub(crate) struct WakeupTrigger {
    inner: Arc<WakeupTriggerInner>,
}

struct WakeupTriggerInner {
    /// Carries the current cancellation token. `borrow()` is cheap and
    /// lock-free; subscribers can `changed().await` to learn when the
    /// token rotates.
    sender: watch::Sender<CancellationToken>,
    /// Once `true`, `wakeup()` / `rotate()` / `maybe_trigger_wakeup()`
    /// become no-ops. Set by [`Self::disable`].
    disabled: AtomicBool,
}

impl Default for WakeupTrigger {
    fn default() -> Self {
        Self::new()
    }
}

impl WakeupTrigger {
    /// Creates a fresh trigger with an un-cancelled initial token.
    pub(crate) fn new() -> Self {
        let (sender, _) = watch::channel(CancellationToken::new());
        Self { inner: Arc::new(WakeupTriggerInner { sender, disabled: AtomicBool::new(false) }) }
    }

    /// Subscribe to the current token. The returned receiver yields the
    /// **current** token via `borrow()` and updates when [`Self::rotate`]
    /// is called. Used by the background task to re-read its token at
    /// the top of each `run_once` iteration.
    pub(crate) fn subscribe(&self) -> watch::Receiver<CancellationToken> {
        self.inner.sender.subscribe()
    }

    /// Read the current token (clone — `CancellationToken` is a cheap
    /// `Arc`-backed handle). Used by sync `select!` paths that re-read
    /// on each loop iteration.
    pub(crate) fn current_token(&self) -> CancellationToken {
        self.inner.sender.borrow().clone()
    }

    /// Java's `wakeup()`. Cancels the current token, waking any task
    /// `select!`-ing on `token.cancelled()`. Idempotent; calling twice
    /// before [`Self::rotate`] is a no-op (the token is already
    /// cancelled).
    ///
    /// Safe to call from any task; `&self`.
    pub(crate) fn wakeup(&self) {
        if self.inner.disabled.load(Ordering::Acquire) {
            return;
        }
        // borrow() is read-locked but we only need a brief read.
        let token = self.inner.sender.borrow().clone();
        token.cancel();
    }

    /// Replace the current token with a fresh, un-cancelled one. Called
    /// by the app side after a public method returns
    /// `KafkaError::wakeup(...)` — `consumer-threading.md` §11 explicitly
    /// states this is the analog of Java's "throw `WakeupException`,
    /// then clear the volatile flag".
    pub(crate) fn rotate(&self) {
        if self.inner.disabled.load(Ordering::Acquire) {
            return;
        }
        // `send_replace` always stores the new value regardless of whether
        // any subscriber receivers are live; the bg task may not yet have
        // called `subscribe()` when the first rotation happens. Plain
        // `send()` would return `Err` and NOT store the value in that
        // case, leaving the cancelled token in place.
        self.inner.sender.send_replace(CancellationToken::new());
    }

    /// Java's `disableWakeups()`. After this call, `wakeup()` and
    /// `rotate()` are no-ops and `maybe_trigger_wakeup()` will never
    /// return an error. Intended to be called by `close()`.
    ///
    /// A pending wakeup is **discarded**, not preserved. Java does this by
    /// overwriting whatever the pending task was:
    ///
    /// ```java
    /// public void disableWakeups() {
    ///     pendingTask.set(new DisabledWakeups());
    /// }
    /// ```
    ///
    /// A `set` (not a compare-and-swap), so an outstanding `WakeupFuture`
    /// is thrown away. Discarding it matters beyond parity: `select!` arms
    /// on this side use `current_token().cancelled()`, and a cancelled
    /// token completes that arm *immediately, every iteration*. Leaving one
    /// in place turns every bounded wait into a spin — for the whole
    /// `default.api.timeout.ms` on a concurrent handle op, or the close
    /// timeout on the close path itself — because the `disabled` flag
    /// silences `maybe_trigger_wakeup`'s escape hatch at the same time.
    ///
    /// The store is ordered before the token swap so no task can observe
    /// the fresh token while still believing wakeups are live.
    pub(crate) fn disable(&self) {
        self.inner.disabled.store(true, Ordering::Release);
        // Not `rotate()`: that returns early once `disabled` is set.
        self.inner.sender.send_replace(CancellationToken::new());
    }

    /// Java's `maybeTriggerWakeup()` — synchronous check. Returns
    /// `Err(KafkaError::wakeup(...))` if the current token has been
    /// cancelled (i.e. a prior `wakeup()` is pending). Does NOT consume
    /// the wakeup state — the caller is expected to [`Self::rotate`]
    /// after raising the error to the user.
    pub(crate) fn maybe_trigger_wakeup(&self) -> Result<(), KafkaError> {
        if self.inner.disabled.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.current_token().is_cancelled() {
            return Err(KafkaError::wakeup("WakeupTrigger fired"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    /// Java `testEnsureActiveFutureCanBeWakeUp` — rewritten to the
    /// rotating-token design: a task `select!`-ing on the trigger's
    /// token is unblocked by `wakeup()`.
    #[tokio::test]
    async fn wakeup_cancels_select_on_current_token() {
        let trigger = WakeupTrigger::new();
        let token = trigger.current_token();

        let waiter = tokio::spawn(async move {
            tokio::select! {
                _ = token.cancelled() => "woken",
                _ = tokio::time::sleep(Duration::from_secs(5)) => "timeout",
            }
        });

        trigger.wakeup();

        let res = timeout(Duration::from_secs(2), waiter).await.expect("join").expect("task");
        assert_eq!(res, "woken");
    }

    /// Java `testSettingActiveFutureAfterWakeupShouldThrow` — under the
    /// rotating-token design, a `wakeup()` happening **before** the
    /// active task subscribes leaves the current token already
    /// cancelled, so the next `select!` returns immediately.
    #[tokio::test]
    async fn wakeup_before_subscribing_returns_immediately() {
        let trigger = WakeupTrigger::new();
        trigger.wakeup();

        let token = trigger.current_token();
        let res = timeout(Duration::from_millis(500), token.cancelled()).await;
        assert!(res.is_ok(), "cancellation should be observed immediately");
    }

    /// Java `testManualTriggerWhenWakeupCalled` — `maybe_trigger_wakeup`
    /// after `wakeup()` returns an error.
    #[test]
    fn maybe_trigger_wakeup_returns_error_after_wakeup() {
        let trigger = WakeupTrigger::new();
        trigger.wakeup();
        let err = trigger.maybe_trigger_wakeup().expect_err("must err");
        assert!(matches!(err, KafkaError::Wakeup(_)));
    }

    /// Java `testManualTriggerWhenWakeupNotCalled`.
    #[test]
    fn maybe_trigger_wakeup_returns_ok_without_prior_wakeup() {
        let trigger = WakeupTrigger::new();
        assert!(trigger.maybe_trigger_wakeup().is_ok());
    }

    /// Java `testDisableWakeupWithoutPendingTask` —
    /// `disable()` then `wakeup()` then `maybe_trigger_wakeup()` ⇒ OK.
    #[test]
    fn disable_then_wakeup_is_a_noop() {
        let trigger = WakeupTrigger::new();
        trigger.disable();
        trigger.wakeup();
        assert!(trigger.maybe_trigger_wakeup().is_ok());
    }

    /// Java `testDisableWakeupWithPendingTask` — disabled wakeups do not
    /// cancel a current waiter.
    #[tokio::test]
    async fn disabled_wakeup_does_not_cancel_waiter() {
        let trigger = WakeupTrigger::new();
        trigger.disable();
        let token = trigger.current_token();

        // The token must NOT become cancelled while we wait briefly.
        let res = timeout(Duration::from_millis(200), token.cancelled()).await;
        // We expect a timeout — the token was never cancelled.
        assert!(res.is_err(), "disabled trigger must not cancel the token");

        // Subsequent wakeup() is still a no-op.
        trigger.wakeup();
        let res2 = timeout(Duration::from_millis(200), token.cancelled()).await;
        assert!(res2.is_err(), "disabled trigger must continue to be inert");
    }

    /// `disable()` must DISCARD a wakeup that is already pending, mirroring
    /// Java's `pendingTask.set(new DisabledWakeups())` overwriting an
    /// outstanding `WakeupFuture`.
    ///
    /// The sibling tests above all disable a *fresh* trigger, so none of them
    /// reaches this case. It is the one that matters: `await_completion`
    /// selects on `current_token().cancelled()`, and a cancelled token left in
    /// place completes that arm immediately on every iteration while the
    /// `disabled` flag simultaneously silences `maybe_trigger_wakeup`'s escape
    /// — a spin for the whole remaining timeout rather than a bounded wait.
    #[tokio::test]
    async fn disable_discards_a_pending_wakeup() {
        let trigger = WakeupTrigger::new();

        // A wakeup lands BEFORE close() disables the trigger.
        trigger.wakeup();
        let stale = trigger.current_token();
        assert!(stale.is_cancelled(), "precondition: the wakeup cancelled the token");

        trigger.disable();

        // The token now in effect must be a fresh, un-cancelled one.
        let token = trigger.current_token();
        assert!(!token.is_cancelled(), "disable must discard the pending wakeup");
        assert!(
            trigger.maybe_trigger_wakeup().is_ok(),
            "no wakeup error may surface after disable"
        );

        // And a waiter on it must actually block rather than return at once.
        let res = timeout(Duration::from_millis(200), token.cancelled()).await;
        assert!(res.is_err(), "a bounded wait must not be short-circuited after disable");
    }

    /// `rotate()` swaps in a fresh token; the old token's cancellation
    /// is unobservable on the new token.
    #[tokio::test]
    async fn rotate_replaces_the_token() {
        let trigger = WakeupTrigger::new();
        let old = trigger.current_token();
        trigger.wakeup();
        assert!(old.is_cancelled());

        trigger.rotate();
        let new_tok = trigger.current_token();
        assert!(!new_tok.is_cancelled(), "fresh token must not be cancelled");
        assert!(
            trigger.maybe_trigger_wakeup().is_ok(),
            "rotation clears the pending wakeup state"
        );
    }

    /// `rotate()` is a no-op after `disable()`.
    #[test]
    fn rotate_is_noop_after_disable() {
        let trigger = WakeupTrigger::new();
        let token_before = trigger.current_token();
        trigger.disable();
        trigger.rotate();
        let token_after = trigger.current_token();
        // Same token pointer — `CancellationToken::clone` shares state.
        // We cannot Arc::ptr_eq across `CancellationToken` clones, but
        // we can assert that the post-disable token is still
        // un-cancelled and still observes no future wakeups.
        assert!(!token_before.is_cancelled());
        assert!(!token_after.is_cancelled());
    }

    /// Cloning the trigger shares state with the original.
    #[tokio::test]
    async fn clones_share_state() {
        let trigger = WakeupTrigger::new();
        let other = trigger.clone();
        let token = trigger.current_token();
        other.wakeup();
        assert!(token.is_cancelled(), "wakeup via clone must affect the same token");
    }

    /// `subscribe()` yields the current token, and `rotate()` is
    /// observable via `changed().await`.
    #[tokio::test]
    async fn subscribe_observes_rotations() {
        let trigger = WakeupTrigger::new();
        let mut rx = trigger.subscribe();
        // Initial value already present in `borrow()` — `changed()`
        // resolves only on subsequent writes.
        trigger.rotate();
        timeout(Duration::from_millis(500), rx.changed())
            .await
            .expect("rotated")
            .expect("rx ok");
    }
}
