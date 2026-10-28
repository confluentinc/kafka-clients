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

//! Callback interface notified when an async offset commit completes.
//!
//! Translated from `org.apache.kafka.clients.consumer.OffsetCommitCallback`.

use std::collections::HashMap;

use async_trait::async_trait;

use crate::common::{Error, TopicPartition};
use crate::consumer::OffsetAndMetadata;

/// A callback interface that the user can implement to trigger custom actions
/// when a commit request completes. The callback may be executed in any thread
/// calling poll().
///
/// # Translation notes
///
/// It runs on the task inside `poll()`, `commit_*()` or `close()`.
// Per `consumer-threading.md` §31, the callback executes on the caller's task
// that is currently inside `poll()` / `commit_*()` / `close()`, never on the
// background task.
//
// Bounds `Send + Sync + 'static`: the callback travels through events on the
// background task and is invoked on the app task after the background task has
// already moved on, so it is stored as `Arc<dyn>`.
#[async_trait]
#[doc(alias = "org.apache.kafka.clients.consumer.OffsetCommitCallback")]
pub trait OffsetCommitCallback: Send + Sync + 'static {
    /// A callback method the user can implement to provide asynchronous
    /// handling of commit request completion. This method will be called
    /// when the commit request sent to the server has been acknowledged.
    ///
    /// # Arguments
    ///
    /// - `offsets` - A map of the offsets and associated metadata that this
    ///   callback applies to
    /// - `error` - The error raised during processing of the request, or
    ///   `None` if the commit completed successfully
    ///
    /// # Errors
    ///
    /// - `Error::is_consumer_commit_failed_error` if the commit failed and cannot be retried. This
    ///   can only occur if you are using automatic group management with
    ///   `KafkaConsumer::subscribe`, or if there is an active group with the same groupId which is
    ///   using group management.
    /// - `Error::is_rebalance_in_progress_error` if the commit failed because it is in the middle
    ///   of a rebalance. In such cases commit could be retried after the rebalance is completed
    ///   with the `KafkaConsumer::poll` call.
    /// - `Error::is_wakeup_error` if `KafkaConsumer::wakeup` is called before or while this
    ///   function is called
    /// - `Error::is_interrupt_error` if the calling thread is interrupted before or while this
    ///   function is called
    /// - `Error::is_authorization_error` if not authorized to the topic or to the configured
    ///   groupId. See the error for more details
    /// - `Error::is_kafka_error` for any other unrecoverable errors (e.g. if offset metadata is too
    ///   large or if the committed offset is invalid).
    ///
    /// # Translation notes
    ///
    /// The errors above are those `error` may carry. An `Err` returned here
    /// propagates out of the call that ran the callback, as Java's thrown
    /// exception does.
    // Java's `onComplete` is `void` but may throw: an exception it throws is
    // not caught (`OffsetCommitCallbackInvoker.executeCallbacks`) and
    // propagates out of the call that ran the callback (`poll`, `commit_sync`,
    // `commit_async`, `close`; `poll` wraps it as
    // `ConsumerUtils.maybeWrapAsKafkaException` does). Callbacks queued behind
    // this one stay queued for the next call that runs them. The commit itself
    // has already completed.
    #[doc(alias = "org.apache.kafka.clients.consumer.OffsetCommitCallback#onComplete")]
    async fn on_complete(
        &self,
        offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
        error: Option<&Error>,
    ) -> Result<(), Error>;
}

/// Returns an [`OffsetCommitCallback`] that calls `function`, standing in for
/// a Java lambda implementing `OffsetCommitCallback`: `function` is
/// [`on_complete`](OffsetCommitCallback::on_complete), run synchronously as
/// Java's `onComplete` is. A method Java adds later as an overload of
/// `onComplete` gets its own factory, `with_<parameter>`.
///
/// ```
/// use std::sync::Arc;
///
/// use confluent_kafka::consumer::{offset_commit_callback, OffsetCommitCallback};
///
/// let callback: Arc<dyn OffsetCommitCallback> = Arc::new(offset_commit_callback::new(|offsets, error| {
///     match error {
///         Some(error) => eprintln!("commit of {} partitions failed: {error}", offsets.len()),
///         None => println!("committed {} partitions", offsets.len()),
///     }
///     Ok(())
/// }));
/// ```
///
/// # Arguments
///
/// - `function` - the body of [`on_complete`](OffsetCommitCallback::on_complete),
///   called with the committed offsets and the commit's error.
// stands in for a Java lambda implementing `OffsetCommitCallback` (CLAUDE.md §3)
#[doc(alias = "rust-only")]
pub fn new<F>(function: F) -> impl OffsetCommitCallback
where
    F: Fn(&HashMap<TopicPartition, OffsetAndMetadata>, Option<&Error>) -> Result<(), Error> + Send + Sync + 'static,
{
    struct FunctionOffsetCommitCallback<F>(F);

    #[async_trait]
    impl<F> OffsetCommitCallback for FunctionOffsetCommitCallback<F>
    where
        F: Fn(&HashMap<TopicPartition, OffsetAndMetadata>, Option<&Error>) -> Result<(), Error> + Send + Sync + 'static,
    {
        async fn on_complete(
            &self,
            offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
            error: Option<&Error>,
        ) -> Result<(), Error> {
            (self.0)(offsets, error)
        }
    }

    FunctionOffsetCommitCallback(function)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[tokio::test]
    async fn new_calls_the_function_with_the_offsets_and_error() {
        let seen = Arc::new(Mutex::new(None));
        let sink = Arc::clone(&seen);
        let callback: Arc<dyn OffsetCommitCallback> = Arc::new(new(move |offsets, error| {
            *sink.lock().unwrap() = Some((offsets.len(), error.is_some()));
            Ok(())
        }));
        let offsets = HashMap::from([(TopicPartition::new("t", 0), OffsetAndMetadata::new(5).unwrap())]);
        callback.on_complete(&offsets, None).await.unwrap();
        assert_eq!(*seen.lock().unwrap(), Some((1, false)));
    }

    #[tokio::test]
    async fn new_returns_the_function_error() {
        let callback = new(|_offsets, _error| Err(Error::local_illegal_state("from callback")));
        let result = callback
            .on_complete(&HashMap::new(), Some(&Error::local_illegal_state("commit failed")))
            .await;
        assert!(result.unwrap_err().to_string().contains("from callback"));
    }
}
