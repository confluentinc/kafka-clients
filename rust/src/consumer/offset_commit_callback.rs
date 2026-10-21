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
/// when a commit request completes.
///
/// Corresponds to Java's
/// `org.apache.kafka.clients.consumer.OffsetCommitCallback`.
///
/// # Invocation model
///
/// Per `consumer-threading.md` §31, the callback executes on the **caller's
/// task** that is currently inside `poll()` / `commit_*()` / `close()` —
/// never on the background task. Matches Java's contract: "the callback may
/// be executed in any thread calling poll()".
///
/// # Bounds: `Send + Sync + 'static`
///
/// The callback travels through events on the background task and is
/// invoked on the app task after the background task has already moved on.
/// `Arc<dyn>` storage is required for the symmetric "send through channel,
/// invoke later on app side" pipeline used by `process_background_events`.
/// `Box<dyn>` would forbid the clone and the design collapses.
#[async_trait]
#[doc(alias = "org.apache.kafka.clients.consumer.OffsetCommitCallback")]
pub trait OffsetCommitCallback: Send + Sync + 'static {
    /// A callback method the user can implement to provide asynchronous
    /// handling of commit request completion. This method will be called
    /// when the commit request sent to the server has been acknowledged.
    ///
    /// Corresponds to Java's
    /// `void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
    ///                  Exception exception)`.
    ///
    /// The `error` parameter matches Java's pattern of "exception == null
    /// means success".
    ///
    /// # Errors
    ///
    /// Java's `onComplete` is `void` but may throw: an exception it throws
    /// is not caught (`OffsetCommitCallbackInvoker.executeCallbacks`) and
    /// propagates out of the call that ran the callback (`poll`,
    /// `commit_sync`, `commit_async`, `close`; `poll` wraps it as
    /// `ConsumerUtils.maybeWrapAsKafkaException` does). An `Err` returned
    /// here is that exception: it propagates the same way, and callbacks
    /// queued behind this one stay queued for the next call that runs them.
    /// The commit itself has already completed.
    ///
    /// # Arguments
    ///
    /// * `offsets` - the offsets and associated metadata that this callback
    ///   applies to
    /// * `error` - `Some(&error)` if the commit failed, `None` if it
    ///   completed successfully
    #[doc(alias = "org.apache.kafka.clients.consumer.OffsetCommitCallback#onComplete")]
    async fn on_complete(
        &self,
        offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
        error: Option<&Error>,
    ) -> Result<(), Error>;
}

/// An [`OffsetCommitCallback`] that calls `function`, standing in for a Java
/// lambda implementing `OffsetCommitCallback`: `function` is
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
