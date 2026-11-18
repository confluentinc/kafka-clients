// Copyright 2026 Confluent Inc.
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

//! Callback interface notified when a share-group acknowledgement completes.
//!
//! Translated from `org.apache.kafka.clients.consumer.AcknowledgementCommitCallback`.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;

use crate::common::{Error, TopicIdPartition};

/// A callback interface that the user can implement to trigger custom actions
/// when an acknowledgement completes. The callback may be executed in any
/// thread calling `ShareConsumer::poll`.
///
/// # Translation notes
///
/// It runs on the caller's task, never on a background task.
// It runs inside `poll()`, `commit_sync()`, `commit_async()` or `close()` of
// the share consumer. Per `share-consumer.md` §3 (decision D8), the callback executes on the
// caller's task, inside `handle_completed_acknowledgements`, at the call
// sites of Java's `ShareConsumerImpl#handleCompletedAcknowledgements`.
//
// Bounds `Send + Sync + 'static`: the callback is registered on the consumer
// and invoked later on the app task, so it is stored as `Arc<dyn>`, as
// `OffsetCommitCallback` is.
#[async_trait]
#[doc(alias = "org.apache.kafka.clients.consumer.AcknowledgementCommitCallback")]
pub trait AcknowledgementCommitCallback: Send + Sync + 'static {
    /// A callback method the user can implement to provide asynchronous
    /// handling of acknowledgement completion. This method will be called
    /// when the acknowledgement request sent to the server has been
    /// completed.
    ///
    /// Note that even if the error is a retriable error, the acknowledgement
    /// could not be completed and the records need to be fetched again. The
    /// callback is called after any retries have been performed.
    ///
    /// # Arguments
    ///
    /// - `offsets` - A map of the offsets that this callback applies to.
    /// - `error` - The error raised during processing of the request, or
    ///   `None` if the acknowledgement completed successfully:
    ///   - `Error::is_authorization_error` if not authorized to the topic or
    ///     group
    ///   - `Error::InvalidRecordState` if the record state is invalid
    ///   - `Error::NotLeaderOrFollower` if the leader had changed by the time
    ///     the acknowledgements were sent
    ///   - `Error::Disconnect` if the broker disconnected before the request
    ///     could be completed
    ///   - `Error::Wakeup` if `KafkaShareConsumer::wakeup` is called before or
    ///     while this function is called
    ///   - `Error::is_kafka_error` for any other unrecoverable errors
    ///
    /// # Translation notes
    ///
    /// Java's `InterruptException` (the calling thread is interrupted) has no
    /// Rust counterpart: a task is not interrupted. The method returns
    /// nothing, as Java's `void onComplete`; a panic in it is not caught.
    // Java's `onComplete` is `void` but may throw; the caller,
    // `AcknowledgementCommitCallbackHandler#onComplete`, logs and swallows what
    // it throws. With no `Result` here there is nothing to swallow
    // (`share-consumer.md` §3).
    #[doc(alias = "org.apache.kafka.clients.consumer.AcknowledgementCommitCallback#onComplete")]
    async fn on_complete(&self, offsets: &HashMap<TopicIdPartition, HashSet<i64>>, error: Option<&Error>);
}

/// Returns an [`AcknowledgementCommitCallback`] that calls `function`,
/// standing in for a Java lambda implementing `AcknowledgementCommitCallback`:
/// `function` is [`on_complete`](AcknowledgementCommitCallback::on_complete),
/// run synchronously as Java's `onComplete` is. A method Java adds later as an
/// overload of `onComplete` gets its own factory, `with_<parameter>`.
///
/// ```
/// use std::sync::Arc;
///
/// use confluent_kafka::consumer::{acknowledgement_commit_callback, AcknowledgementCommitCallback};
///
/// let callback: Arc<dyn AcknowledgementCommitCallback> =
///     Arc::new(acknowledgement_commit_callback::new(|offsets, error| match error {
///         Some(error) => eprintln!("acknowledgement of {} partitions failed: {error}", offsets.len()),
///         None => println!("acknowledged {} partitions", offsets.len()),
///     }));
/// ```
///
/// # Arguments
///
/// - `function` - the body of
///   [`on_complete`](AcknowledgementCommitCallback::on_complete), called with
///   the acknowledged offsets and the acknowledgement's error.
// stands in for a Java lambda implementing `AcknowledgementCommitCallback`
// (CLAUDE.md §3), as `offset_commit_callback::new` does for
// `OffsetCommitCallback`
#[doc(alias = "rust-only")]
pub fn new<F>(function: F) -> impl AcknowledgementCommitCallback
where
    F: Fn(&HashMap<TopicIdPartition, HashSet<i64>>, Option<&Error>) + Send + Sync + 'static,
{
    struct FunctionAcknowledgementCommitCallback<F>(F);

    #[async_trait]
    impl<F> AcknowledgementCommitCallback for FunctionAcknowledgementCommitCallback<F>
    where
        F: Fn(&HashMap<TopicIdPartition, HashSet<i64>>, Option<&Error>) + Send + Sync + 'static,
    {
        async fn on_complete(&self, offsets: &HashMap<TopicIdPartition, HashSet<i64>>, error: Option<&Error>) {
            (self.0)(offsets, error);
        }
    }

    FunctionAcknowledgementCommitCallback(function)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::common::{TopicPartition, Uuid};

    #[tokio::test]
    async fn new_calls_the_function_with_the_offsets_and_error() {
        let seen = Arc::new(Mutex::new(None));
        let sink = Arc::clone(&seen);
        let callback: Arc<dyn AcknowledgementCommitCallback> = Arc::new(new(move |offsets, error| {
            *sink.lock().unwrap() = Some((offsets.clone(), error.map(Error::message).map(str::to_owned)));
        }));
        let partition = TopicIdPartition::new(Uuid::random_uuid(), TopicPartition::new("t", 0));
        let offsets = HashMap::from([(partition, HashSet::from([3, 1, 2]))]);

        callback.on_complete(&offsets, None).await;
        assert_eq!(*seen.lock().unwrap(), Some((offsets.clone(), None)));

        let error = Error::local_illegal_state("acknowledgement failed");
        callback.on_complete(&offsets, Some(&error)).await;
        assert_eq!(
            *seen.lock().unwrap(),
            Some((offsets, Some("acknowledgement failed".to_owned())))
        );
    }
}
