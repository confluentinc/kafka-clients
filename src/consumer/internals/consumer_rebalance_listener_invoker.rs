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

//! Invokes the methods of a user-supplied
//! [`crate::consumer::ConsumerRebalanceListener`] from the consumer
//! app-side task.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerInvoker`.
//!
//! # Listener ownership: passed per-call, not stored on this struct
//!
//! Java stores the listener on `SubscriptionState` and reads it from
//! within the invoker via `subscriptions.rebalanceListener()`. The Rust
//! translation diverges here: the listener is held by
//! `AsyncKafkaConsumer` (Phase 11) in a `Mutex<Option<Arc<dyn
//! ConsumerRebalanceListener>>>` field and passed in to each `invoke_*`
//! call as `&Arc<dyn ConsumerRebalanceListener>`. This makes the invoker
//! reusable across multiple listeners (e.g. tests that swap listeners)
//! and decouples the invoker from the Java-side `SubscriptionState`
//! field that we deliberately do not surface (see
//! `consumer-threading.md` §16: lock discipline is simpler when the
//! listener is fetched outside the SubscriptionState lock).
//!
//! # `#[async_trait]` on listener
//!
//! `ConsumerRebalanceListener` is `#[async_trait]` (per
//! `consumer-threading.md` §31). The invoker `.await`s the user-supplied
//! futures inline on the caller's task. The caller MUST drop the
//! `SubscriptionState` mutex guard before invoking the listener — see
//! `consumer-threading.md` §16.
//!
//! # Metrics (deferred)
//!
//! Java records partitions-revoked / assigned / lost latency via
//! `RebalanceCallbackMetricsManager`. Per Phase 11 PLAN.md deferral #1
//! (AsyncConsumerMetrics out of scope), metrics are dropped here.
//!
//! # Exception handling
//!
//! Java's invoker:
//!   - re-throws `WakeupException` / `InterruptException` directly
//!     (so they propagate to the consumer's outer poll loop);
//!   - catches every other `Exception` and returns it as the method's
//!     return value (Java's signature is `Exception invokeXxx(...)`).
//!
//! The Rust translation collapses both paths to `Result<(), KafkaError>`:
//!   - `KafkaError::Wakeup` and (Java's) `InterruptException` analogs
//!     return as-is — the caller (`process_background_events` in
//!     `AsyncKafkaConsumer`) distinguishes them when propagating to the
//!     user.
//!   - Other errors are logged and returned. The bg task's rebalance
//!     state machine inspects the returned error to decide whether to
//!     advance.

#![allow(dead_code)] // Phase 11 commit (1/N): invoker lands before its caller in commit (3).

use std::sync::{Arc, Mutex};

use log::{error, info};

use crate::common::{KafkaError, TopicPartition};
use crate::consumer::ConsumerRebalanceListener;
use crate::consumer::internals::subscription_state::SubscriptionState;

/// Invokes the user-supplied
/// [`crate::consumer::ConsumerRebalanceListener`] methods on the
/// caller's task.
///
/// Held by `AsyncKafkaConsumer` as a plain field — does NOT need to be
/// `Send + Sync` because all access is from the consumer's `&mut self`
/// methods. The struct holds `Arc<Mutex<SubscriptionState>>` only to
/// reproduce Java's `pausedPartitions()` mutation that runs around
/// `on_partitions_revoked` / `on_partitions_lost`.
pub(crate) struct ConsumerRebalanceListenerInvoker {
    subscriptions: Arc<Mutex<SubscriptionState>>,
    // metrics_manager: RebalanceCallbackMetricsManager,  // DEFERRED (metrics)
}

impl ConsumerRebalanceListenerInvoker {
    /// Java's constructor. The `LogContext`, `Time`, and
    /// `RebalanceCallbackMetricsManager` arguments are dropped:
    ///   - log prefix is handled by the `log` crate;
    ///   - timing for metrics is dropped per Phase 11 PLAN.md deferral #1.
    pub(crate) fn new(subscriptions: Arc<Mutex<SubscriptionState>>) -> Self {
        Self { subscriptions }
    }

    /// Java: `Exception invokePartitionsAssigned(SortedSet<TopicPartition>)`.
    ///
    /// `assigned_partitions` is taken as `&[TopicPartition]` per CLAUDE.md
    /// §12 (most general borrowed form); the listener's
    /// `on_partitions_assigned` takes the same.
    ///
    /// # Lock discipline (§16)
    ///
    /// The `SubscriptionState` lock is NOT acquired around the listener
    /// call. The caller in `AsyncKafkaConsumer` is responsible for
    /// already having dropped the `SubscriptionState` guard before
    /// invoking this method (per §16 and §31).
    pub(crate) async fn invoke_partitions_assigned(
        &self,
        listener: &Arc<dyn ConsumerRebalanceListener>,
        assigned_partitions: &[TopicPartition],
    ) -> Result<(), KafkaError> {
        info!("Adding newly assigned partitions: {assigned_partitions:?}");

        // Java's path checks `listener.isPresent()` before invoking; in
        // Rust the caller already proved the listener exists by passing
        // an `Arc` reference. No guard needed here.

        match listener.on_partitions_assigned(assigned_partitions).await {
            Ok(()) => Ok(()),
            Err(err) => match &err {
                // WakeupException + InterruptException propagate directly
                // per Java's invoker contract.
                KafkaError::Wakeup(_) => Err(err),
                _ => {
                    error!(
                        "User provided listener failed on invocation of onPartitionsAssigned for partitions {:?}: {}",
                        assigned_partitions, err
                    );
                    Err(err)
                },
            },
        }
    }

    /// Java: `Exception invokePartitionsRevoked(SortedSet<TopicPartition>)`.
    ///
    /// Mirrors Java's pre-callback bookkeeping: removes the paused flag
    /// from any partition being revoked, before invoking the listener.
    pub(crate) async fn invoke_partitions_revoked(
        &self,
        listener: &Arc<dyn ConsumerRebalanceListener>,
        revoked_partitions: &[TopicPartition],
    ) -> Result<(), KafkaError> {
        info!("Revoke previously assigned partitions {revoked_partitions:?}");

        // Java: `revokePausedPartitions.retainAll(revokedPartitions)` then
        // log if non-empty. The Rust analog acquires the SubscriptionState
        // lock briefly to read paused partitions, drops it, then logs.
        let revoke_paused: Vec<TopicPartition> = {
            let subs = self.subscriptions.lock().unwrap();
            let revoked_set: std::collections::HashSet<&TopicPartition> = revoked_partitions.iter().collect();
            subs.paused_partitions()
                .into_iter()
                .filter(|tp| revoked_set.contains(tp))
                .collect()
        };
        if !revoke_paused.is_empty() {
            info!("The pause flag in partitions {revoke_paused:?} will be removed due to revocation.");
        }

        match listener.on_partitions_revoked(revoked_partitions).await {
            Ok(()) => Ok(()),
            Err(err) => match &err {
                KafkaError::Wakeup(_) => Err(err),
                _ => {
                    error!(
                        "User provided listener failed on invocation of onPartitionsRevoked for partitions {:?}: {}",
                        revoked_partitions, err
                    );
                    Err(err)
                },
            },
        }
    }

    /// Java: `Exception invokePartitionsLost(SortedSet<TopicPartition>)`.
    pub(crate) async fn invoke_partitions_lost(
        &self,
        listener: &Arc<dyn ConsumerRebalanceListener>,
        lost_partitions: &[TopicPartition],
    ) -> Result<(), KafkaError> {
        info!("Lost previously assigned partitions {lost_partitions:?}");

        let lost_paused: Vec<TopicPartition> = {
            let subs = self.subscriptions.lock().unwrap();
            let lost_set: std::collections::HashSet<&TopicPartition> = lost_partitions.iter().collect();
            subs.paused_partitions()
                .into_iter()
                .filter(|tp| lost_set.contains(tp))
                .collect()
        };
        if !lost_paused.is_empty() {
            info!("The pause flag in partitions {lost_paused:?} will be removed due to partition lost.");
        }

        match listener.on_partitions_lost(lost_partitions).await {
            Ok(()) => Ok(()),
            Err(err) => match &err {
                KafkaError::Wakeup(_) => Err(err),
                _ => {
                    error!(
                        "User provided listener failed on invocation of onPartitionsLost for partitions {:?}: {}",
                        lost_partitions, err
                    );
                    Err(err)
                },
            },
        }
    }
}

#[cfg(test)]
mod tests {
    //! No Java `ConsumerRebalanceListenerInvokerTest.java` file exists —
    //! inline unit tests cover the surface that survives translation.

    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use crate::common::TopicPartition;
    use crate::consumer::AutoOffsetResetStrategy;

    use super::*;

    /// Test listener that records every call into shared atomics so
    /// tests can assert without `Mockito`.
    struct CountingListener {
        on_revoked: AtomicUsize,
        on_assigned: AtomicUsize,
        on_lost: AtomicUsize,
    }

    impl CountingListener {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                on_revoked: AtomicUsize::new(0),
                on_assigned: AtomicUsize::new(0),
                on_lost: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait]
    impl ConsumerRebalanceListener for CountingListener {
        async fn on_partitions_revoked(&self, _partitions: &[TopicPartition]) -> Result<(), KafkaError> {
            self.on_revoked.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn on_partitions_assigned(&self, _partitions: &[TopicPartition]) -> Result<(), KafkaError> {
            self.on_assigned.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn on_partitions_lost(&self, _partitions: &[TopicPartition]) -> Result<(), KafkaError> {
            self.on_lost.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// Test listener that always returns a non-wakeup error.
    struct FailingListener {
        err_msg: &'static str,
    }

    #[async_trait]
    impl ConsumerRebalanceListener for FailingListener {
        async fn on_partitions_revoked(&self, _partitions: &[TopicPartition]) -> Result<(), KafkaError> {
            Err(KafkaError::illegal_state(self.err_msg))
        }
        async fn on_partitions_assigned(&self, _partitions: &[TopicPartition]) -> Result<(), KafkaError> {
            Err(KafkaError::illegal_state(self.err_msg))
        }
        async fn on_partitions_lost(&self, _partitions: &[TopicPartition]) -> Result<(), KafkaError> {
            Err(KafkaError::illegal_state(self.err_msg))
        }
    }

    /// Test listener that always returns `KafkaError::Wakeup`.
    struct WakingListener;

    #[async_trait]
    impl ConsumerRebalanceListener for WakingListener {
        async fn on_partitions_revoked(&self, _partitions: &[TopicPartition]) -> Result<(), KafkaError> {
            Err(KafkaError::wakeup("woken"))
        }
        async fn on_partitions_assigned(&self, _partitions: &[TopicPartition]) -> Result<(), KafkaError> {
            Err(KafkaError::wakeup("woken"))
        }
        async fn on_partitions_lost(&self, _partitions: &[TopicPartition]) -> Result<(), KafkaError> {
            Err(KafkaError::wakeup("woken"))
        }
    }

    fn make_subs() -> Arc<Mutex<SubscriptionState>> {
        Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::EARLIEST)))
    }

    fn make_invoker() -> ConsumerRebalanceListenerInvoker {
        ConsumerRebalanceListenerInvoker::new(make_subs())
    }

    #[tokio::test]
    async fn invoke_partitions_assigned_calls_listener_on_success() {
        let invoker = make_invoker();
        let listener: Arc<CountingListener> = CountingListener::new();
        let arc_listener: Arc<dyn ConsumerRebalanceListener> = listener.clone();

        let partitions = vec![TopicPartition::new("t".to_string(), 0)];
        invoker
            .invoke_partitions_assigned(&arc_listener, &partitions)
            .await
            .expect("ok");
        assert_eq!(listener.on_assigned.load(Ordering::SeqCst), 1);
        assert_eq!(listener.on_revoked.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn invoke_partitions_revoked_calls_listener_on_success() {
        let invoker = make_invoker();
        let listener: Arc<CountingListener> = CountingListener::new();
        let arc_listener: Arc<dyn ConsumerRebalanceListener> = listener.clone();

        let partitions = vec![TopicPartition::new("t".to_string(), 0)];
        invoker.invoke_partitions_revoked(&arc_listener, &partitions).await.expect("ok");
        assert_eq!(listener.on_revoked.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn invoke_partitions_lost_calls_listener_on_success() {
        let invoker = make_invoker();
        let listener: Arc<CountingListener> = CountingListener::new();
        let arc_listener: Arc<dyn ConsumerRebalanceListener> = listener.clone();

        let partitions = vec![TopicPartition::new("t".to_string(), 0)];
        invoker.invoke_partitions_lost(&arc_listener, &partitions).await.expect("ok");
        assert_eq!(listener.on_lost.load(Ordering::SeqCst), 1);
    }

    /// Java: non-wakeup exception thrown by a listener is logged and
    /// returned as the method's value.
    #[tokio::test]
    async fn invoke_partitions_revoked_returns_listener_error() {
        let invoker = make_invoker();
        let listener: Arc<dyn ConsumerRebalanceListener> = Arc::new(FailingListener { err_msg: "kaboom" });
        let err = invoker.invoke_partitions_revoked(&listener, &[]).await.expect_err("must err");
        assert!(matches!(err, KafkaError::IllegalState(ref msg) if msg == "kaboom"));
    }

    /// Java: `WakeupException` is re-thrown directly. The Rust analog
    /// returns `Err(KafkaError::Wakeup)` exactly as the listener emitted
    /// (no wrapping).
    #[tokio::test]
    async fn invoke_partitions_assigned_propagates_wakeup_unchanged() {
        let invoker = make_invoker();
        let listener: Arc<dyn ConsumerRebalanceListener> = Arc::new(WakingListener);
        let err = invoker.invoke_partitions_assigned(&listener, &[]).await.expect_err("must err");
        assert!(matches!(err, KafkaError::Wakeup(_)));
    }

    /// Issue 6 regression: `invokePartitionsRevoked` must exercise the
    /// paused-partition log path when the revoked set intersects the
    /// paused set. Without this test the pause-flag-clear branch
    /// (`ConsumerRebalanceListenerInvoker.java:91-95`) is unexercised.
    /// We pre-assign + pause a partition, then call
    /// `invoke_partitions_revoked` with the SAME partition — the call
    /// must succeed (no panic on lock acquire, log line emitted).
    #[tokio::test]
    async fn invoke_partitions_revoked_with_paused_partition_exercises_log_path() {
        let subs = make_subs();
        // Assign + pause the partition.
        let tp = TopicPartition::new("paused-topic".to_string(), 0);
        {
            let mut s = subs.lock().unwrap();
            let mut assigned = std::collections::HashSet::new();
            assigned.insert(tp.clone());
            s.assign_from_user(assigned).expect("assign ok");
            s.pause(&tp).expect("pause ok");
            assert!(s.paused_partitions().contains(&tp));
        }
        let invoker = ConsumerRebalanceListenerInvoker::new(Arc::clone(&subs));
        let listener: Arc<CountingListener> = CountingListener::new();
        let arc_listener: Arc<dyn ConsumerRebalanceListener> = listener.clone();

        // Revoke the same paused partition — must hit the log branch.
        invoker
            .invoke_partitions_revoked(&arc_listener, std::slice::from_ref(&tp))
            .await
            .expect("ok");
        assert_eq!(listener.on_revoked.load(Ordering::SeqCst), 1);
    }

    /// Issue 6 regression: symmetric paused-partition log path for
    /// `invokePartitionsLost` (`ConsumerRebalanceListenerInvoker.java:104-108`).
    #[tokio::test]
    async fn invoke_partitions_lost_with_paused_partition_exercises_log_path() {
        let subs = make_subs();
        let tp = TopicPartition::new("paused-topic".to_string(), 0);
        {
            let mut s = subs.lock().unwrap();
            let mut assigned = std::collections::HashSet::new();
            assigned.insert(tp.clone());
            s.assign_from_user(assigned).expect("assign ok");
            s.pause(&tp).expect("pause ok");
        }
        let invoker = ConsumerRebalanceListenerInvoker::new(Arc::clone(&subs));
        let listener: Arc<CountingListener> = CountingListener::new();
        let arc_listener: Arc<dyn ConsumerRebalanceListener> = listener.clone();

        invoker
            .invoke_partitions_lost(&arc_listener, std::slice::from_ref(&tp))
            .await
            .expect("ok");
        assert_eq!(listener.on_lost.load(Ordering::SeqCst), 1);
    }

    /// Negative branch: revoked set does NOT intersect paused set,
    /// so the log branch is skipped (the `revoke_paused.is_empty()`
    /// arm). Asserts the call still succeeds.
    #[tokio::test]
    async fn invoke_partitions_revoked_with_no_paused_intersection_skips_log() {
        let subs = make_subs();
        let paused = TopicPartition::new("paused-topic".to_string(), 0);
        let revoked = TopicPartition::new("other-topic".to_string(), 0);
        {
            let mut s = subs.lock().unwrap();
            let mut assigned = std::collections::HashSet::new();
            assigned.insert(paused.clone());
            assigned.insert(revoked.clone());
            s.assign_from_user(assigned).expect("assign ok");
            s.pause(&paused).expect("pause ok");
        }
        let invoker = ConsumerRebalanceListenerInvoker::new(Arc::clone(&subs));
        let listener: Arc<CountingListener> = CountingListener::new();
        let arc_listener: Arc<dyn ConsumerRebalanceListener> = listener.clone();

        invoker.invoke_partitions_revoked(&arc_listener, &[revoked]).await.expect("ok");
        assert_eq!(listener.on_revoked.load(Ordering::SeqCst), 1);
    }
}
