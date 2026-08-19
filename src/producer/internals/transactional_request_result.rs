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

// Staged ahead of its callers: `TransactionManager` (Phase 3/5) is the only
// consumer of this type, so under `#![deny(warnings)]` every method here is
// dead code until then. Same mechanism as `network_client.rs:15`. Remove this
// once `TransactionManager` lands.
#![allow(dead_code)]

//! Completion handle for an in-flight transactional request.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

use crate::common::KafkaError;

/// The result of a transactional operation (`init_transactions`,
/// `commit_transaction`, `abort_transaction`, `send_offsets_to_transaction`).
///
/// Translated from
/// `org.apache.kafka.clients.producer.internals.TransactionalRequestResult`.
///
/// # Why this is not a `oneshot`
///
/// Java uses a `CountDownLatch(1)` plus a **separate** `volatile boolean
/// isAcked` that is set only inside `await()`. That asymmetry is load-bearing:
/// `TransactionManager::handle_cached_transaction_request_result` keys off
/// `is_acked()`, **not** `is_completed()`, so an operation that completed but
/// was never awaited must hand back the *same* result object when the user
/// retries. A `tokio::sync::oneshot` is single-consumer and not re-awaitable,
/// so it cannot express this at all.
///
/// See `.claude/rules/producer-transactions.md` §5.
///
/// # Java behaviors preserved deliberately
///
/// - `is_acked` is set by the awaiting methods only — never by [`Self::done`]
///   or [`Self::fail`].
/// - Java sets `isAcked = true` *before* checking the error, so a **failed**
///   result is still marked acked. Preserved.
/// - Java's `InterruptException` path has no Rust analogue: there is no task
///   interruption to translate, so the `InterruptedException` catch block is
///   intentionally absent.
#[derive(Debug)]
pub(crate) struct TransactionalRequestResult {
    /// Replaces Java's `CountDownLatch(1)`. Note `notify_waiters()` does not
    /// store a permit, so awaiting must check `completed` first — see
    /// [`Self::await_result_timeout`].
    notify: Notify,
    /// Replaces Java's `volatile RuntimeException error`.
    error: Mutex<Option<KafkaError>>,
    /// Whether the operation has completed (successfully or not). Replaces
    /// `latch.getCount() == 0`.
    completed: AtomicBool,
    /// Whether the caller has observed completion via an await. Replaces
    /// Java's `volatile boolean isAcked`.
    acked: AtomicBool,
    /// The operation name, used in the timeout message.
    operation: String,
}

impl TransactionalRequestResult {
    /// Creates a pending result for the named operation.
    pub(crate) fn new(operation: impl Into<String>) -> Self {
        Self {
            notify: Notify::new(),
            error: Mutex::new(None),
            completed: AtomicBool::new(false),
            acked: AtomicBool::new(false),
            operation: operation.into(),
        }
    }

    /// Completes the operation with an error.
    ///
    /// Corresponds to Java's `fail(RuntimeException)`.
    pub(crate) fn fail(&self, error: KafkaError) {
        *self.error.lock().expect("transactional request result error mutex poisoned") = Some(error);
        self.completed.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Completes the operation successfully.
    ///
    /// Corresponds to Java's `done()`.
    pub(crate) fn done(&self) {
        self.completed.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Waits indefinitely for the operation to complete.
    ///
    /// Corresponds to Java's no-arg `await()`, which delegates to
    /// `await(Long.MAX_VALUE, MILLISECONDS)`.
    pub(crate) async fn await_result(&self) -> Result<(), KafkaError> {
        loop {
            // Create the future *before* checking `completed` so a `done()`
            // racing between the check and the await cannot be missed.
            let notified = self.notify.notified();
            if self.completed.load(Ordering::SeqCst) {
                return self.acknowledge();
            }
            notified.await;
        }
    }

    /// Waits up to `timeout` for the operation to complete.
    ///
    /// Corresponds to Java's `await(long, TimeUnit)`. Returns
    /// [`KafkaError::Timeout`] if the deadline expires before completion.
    pub(crate) async fn await_result_timeout(&self, timeout: Duration) -> Result<(), KafkaError> {
        match tokio::time::timeout(timeout, self.await_result()).await {
            Ok(result) => result,
            Err(_) => Err(KafkaError::timeout(format!(
                "Timeout expired after {}ms while awaiting {}",
                timeout.as_millis(),
                self.operation
            ))),
        }
    }

    /// Marks the result acknowledged and surfaces any error.
    ///
    /// Java sets `isAcked = true` and *then* throws, so a failed result is
    /// still acked. That ordering is preserved here.
    fn acknowledge(&self) -> Result<(), KafkaError> {
        self.acked.store(true, Ordering::SeqCst);
        match self.error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// The error this operation failed with, if any.
    ///
    /// Corresponds to Java's `error()`. Returns an owned clone because
    /// [`KafkaError`] is `Clone` and the result may be awaited more than once.
    pub(crate) fn error(&self) -> Option<KafkaError> {
        self.error
            .lock()
            .expect("transactional request result error mutex poisoned")
            .clone()
    }

    /// Whether the operation completed without error.
    ///
    /// Corresponds to Java's `isSuccessful()`.
    pub(crate) fn is_successful(&self) -> bool {
        self.is_completed() && self.error().is_none()
    }

    /// Whether the operation has completed, successfully or not.
    ///
    /// Corresponds to Java's `isCompleted()`. Non-blocking.
    pub(crate) fn is_completed(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }

    /// Whether a caller has observed completion by awaiting this result.
    ///
    /// Corresponds to Java's `isAcked()`. Distinct from [`Self::is_completed`]
    /// — see the type-level docs.
    pub(crate) fn is_acked(&self) -> bool {
        self.acked.load(Ordering::SeqCst)
    }

    /// The operation name this result was created for.
    pub(crate) fn operation(&self) -> &str {
        &self.operation
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::common::protocol::Errors;

    #[test]
    fn test_new_is_pending() {
        let result = TransactionalRequestResult::new("testOp");
        assert!(!result.is_completed());
        assert!(!result.is_acked());
        assert!(!result.is_successful());
        assert!(result.error().is_none());
        assert_eq!(result.operation(), "testOp");
    }

    #[tokio::test]
    async fn test_done_then_await_succeeds() {
        let result = TransactionalRequestResult::new("commitTransaction");
        result.done();
        assert!(result.is_completed());
        assert!(result.is_successful());
        // Not acked until awaited.
        assert!(!result.is_acked());

        result.await_result().await.expect("should complete successfully");
        assert!(result.is_acked());
    }

    /// `Notify::notify_waiters()` stores no permit, so a waiter arriving after
    /// `done()` would block forever if the implementation awaited before
    /// checking the flag. Guards that ordering.
    #[tokio::test]
    async fn test_await_after_done_returns_immediately() {
        let result = TransactionalRequestResult::new("initTransactions");
        result.done();
        // Would hang before returning if the completed-check came after the await.
        result.await_result().await.expect("should not block");
        assert!(result.is_acked());
    }

    #[tokio::test]
    async fn test_await_returns_error_on_failure() {
        let result = TransactionalRequestResult::new("abortTransaction");
        result.fail(KafkaError::with_message(Errors::InvalidTxnState, "bad state"));

        let error = result.await_result().await.expect_err("should surface the error");
        assert_eq!(error.message(), "bad state");
        assert!(!result.is_successful());
    }

    /// Java sets `isAcked = true` before checking `error`, so a failed result
    /// is still acked once awaited.
    #[tokio::test]
    async fn test_failed_result_is_still_acked() {
        let result = TransactionalRequestResult::new("commitTransaction");
        result.fail(KafkaError::with_message(Errors::InvalidTxnState, "bad state"));
        assert!(!result.is_acked());

        let _ = result.await_result().await;
        assert!(result.is_acked());
    }

    /// The distinction `handle_cached_transaction_request_result` depends on:
    /// completed-but-not-acked is a real state.
    #[test]
    fn test_completed_but_not_acked() {
        let result = TransactionalRequestResult::new("commitTransaction");
        result.done();
        assert!(result.is_completed());
        assert!(!result.is_acked(), "done() must not set acked; only awaiting does");
    }

    /// A `oneshot` could not do this. The result must stay awaitable.
    #[tokio::test]
    async fn test_result_is_re_awaitable() {
        let result = TransactionalRequestResult::new("commitTransaction");
        result.done();
        result.await_result().await.expect("first await");
        result.await_result().await.expect("second await");
        result.await_result().await.expect("third await");
        assert!(result.is_acked());
    }

    #[tokio::test]
    async fn test_re_await_of_failed_result_yields_same_error() {
        let result = TransactionalRequestResult::new("commitTransaction");
        result.fail(KafkaError::with_message(Errors::InvalidTxnState, "bad state"));

        let first = result.await_result().await.expect_err("first await");
        let second = result.await_result().await.expect_err("second await");
        assert_eq!(first.message(), second.message());
    }

    #[tokio::test]
    async fn test_timeout_message_content() {
        let result = TransactionalRequestResult::new("commitTransaction");
        let error = result
            .await_result_timeout(Duration::from_millis(10))
            .await
            .expect_err("should time out");
        // DoD §3: assert message content, not just that it errored.
        assert_eq!(error.message(), "Timeout expired after 10ms while awaiting commitTransaction");
        // A timeout does not complete or ack the result — it stays retryable.
        assert!(!result.is_completed());
        assert!(!result.is_acked());
    }

    #[tokio::test]
    async fn test_await_timeout_succeeds_when_completed_in_time() {
        let result = Arc::new(TransactionalRequestResult::new("initTransactions"));
        let completer = Arc::clone(&result);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            completer.done();
        });

        result
            .await_result_timeout(Duration::from_secs(5))
            .await
            .expect("should complete before the deadline");
        assert!(result.is_acked());
    }

    /// A waiter already parked when `done()` fires must be woken.
    #[tokio::test]
    async fn test_waiter_parked_before_completion_is_woken() {
        let result = Arc::new(TransactionalRequestResult::new("commitTransaction"));
        let waiter = Arc::clone(&result);
        let handle = tokio::spawn(async move { waiter.await_result().await });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!result.is_completed());
        result.done();

        handle
            .await
            .expect("task should not panic")
            .expect("should complete successfully");
        assert!(result.is_acked());
    }

    #[tokio::test]
    async fn test_multiple_concurrent_waiters_all_woken() {
        let result = Arc::new(TransactionalRequestResult::new("commitTransaction"));
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let waiter = Arc::clone(&result);
                tokio::spawn(async move { waiter.await_result().await })
            })
            .collect();

        tokio::time::sleep(Duration::from_millis(20)).await;
        result.done();

        for handle in handles {
            handle
                .await
                .expect("task should not panic")
                .expect("should complete successfully");
        }
    }

    #[test]
    fn test_fail_sets_error_and_completes() {
        let result = TransactionalRequestResult::new("abortTransaction");
        result.fail(KafkaError::with_message(Errors::InvalidTxnState, "bad state"));
        assert!(result.is_completed());
        assert!(!result.is_successful());
        assert_eq!(result.error().expect("error should be set").message(), "bad state");
    }
}
