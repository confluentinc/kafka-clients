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

//! Executor that invokes the user-supplied [`AcknowledgementCommitCallback`]
//! on the caller's task when an acknowledgement commit completes.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.AcknowledgementCommitCallbackHandler`.

// Phase 2 (M9) lands this handler; the caller
// (`ShareConsumeRequestManager` / `ShareConsumerImpl`) that drives
// `on_complete` arrives in Phase 5/6.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures_util::FutureExt;
use log::error;

use crate::common::TopicIdPartition;
use crate::consumer::acknowledgement_commit_callback::AcknowledgementCommitCallback;

use super::acknowledgements::Acknowledgements;

/// Handles invocation of the user-supplied [`AcknowledgementCommitCallback`].
///
/// Translated from
/// `org.apache.kafka.clients.consumer.internals.AcknowledgementCommitCallbackHandler`.
///
/// Per `consumer-threading.md` §31, [`Self::on_complete`] runs on the caller's
/// task (inside `poll()`); the callback is never fired on the background task.
pub(crate) struct AcknowledgementCommitCallbackHandler {
    acknowledgement_commit_callback: Arc<dyn AcknowledgementCommitCallback>,
    entered_callback: bool,
}

impl AcknowledgementCommitCallbackHandler {
    /// Constructs a handler around the user-supplied callback.
    ///
    /// Mirrors Java's package-private constructor
    /// `AcknowledgementCommitCallbackHandler(AcknowledgementCommitCallback)`.
    pub(crate) fn new(acknowledgement_commit_callback: Arc<dyn AcknowledgementCommitCallback>) -> Self {
        Self { acknowledgement_commit_callback, entered_callback: false }
    }

    /// Whether the handler is currently inside a callback invocation.
    ///
    /// Mirrors Java's `public boolean hasEnteredCallback()`.
    pub(crate) fn has_entered_callback(&self) -> bool {
        self.entered_callback
    }

    /// Invokes the user callback once for each partition in each map of the
    /// supplied list, passing the acknowledged offsets and the completion
    /// error (if any).
    ///
    /// Mirrors Java's package-private
    /// `void onComplete(List<Map<TopicIdPartition, Acknowledgements>>)`.
    ///
    /// As in Java, a panic thrown by the user callback is caught, logged, and
    /// does not abort processing of the remaining partitions; `entered_callback`
    /// is always reset afterwards (Java's `finally`).
    pub(crate) async fn on_complete(
        &mut self,
        acknowledgements_map_list: Vec<HashMap<TopicIdPartition, Acknowledgements>>,
    ) {
        for acknowledgements_map in acknowledgements_map_list {
            for (partition, acknowledgements) in acknowledgements_map {
                let exception = acknowledgements.get_acknowledge_exception().cloned();
                // Java: `Set.copyOf(acknowledgements.getAcknowledgementsTypeMap().keySet())`.
                let offsets: HashSet<i64> = acknowledgements.get_acknowledgements_type_map().keys().copied().collect();

                let mut offsets_map: HashMap<TopicIdPartition, HashSet<i64>> = HashMap::with_capacity(1);
                offsets_map.insert(partition, offsets);

                self.entered_callback = true;
                // Java catches any Exception thrown by the callback, logs it,
                // and continues with the next partition. In Rust the callback
                // returns `()` and cannot return an error, so the only failure
                // mode is a panic — catch it to preserve the same behaviour.
                let result = AssertUnwindSafe(
                    self.acknowledgement_commit_callback
                        .on_complete(&offsets_map, exception.as_ref()),
                )
                .catch_unwind()
                .await;
                if result.is_err() {
                    error!("Exception thrown by acknowledgement commit callback");
                }
                self.entered_callback = false;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Translated from
    //! `org.apache.kafka.clients.consumer.internals.AcknowledgementCommitCallbackHandlerTest`.
    //!
    //! Java's test uses `TestUtils.retryOnExceptionWithTimeout` because the
    //! callback fires on the background thread; the Rust callback fires
    //! synchronously on the caller's task (§31), so no retry loop is needed —
    //! the recorded exceptions are observable immediately after `on_complete`
    //! returns.

    use super::*;
    use crate::common::protocol::errors::Errors;
    use crate::common::{KafkaError, TopicPartition, Uuid};
    use crate::consumer::AcknowledgeType;
    use async_trait::async_trait;
    use std::sync::Mutex;

    /// (`TopicIdPartition`, offset) key — the Java test's inner
    /// `TopicPartitionAndOffset`.
    type TopicPartitionAndOffset = (TopicIdPartition, i64);

    /// Test callback that records the error observed for each (partition,
    /// offset) pair. Mirrors Java's `TestableAcknowledgeCommitCallback`.
    struct RecordingCallback {
        exception_map: Mutex<HashMap<TopicPartitionAndOffset, Option<KafkaError>>>,
    }

    impl RecordingCallback {
        fn new() -> Self {
            Self { exception_map: Mutex::new(HashMap::new()) }
        }
    }

    #[async_trait]
    impl AcknowledgementCommitCallback for RecordingCallback {
        async fn on_complete(&self, offsets: &HashMap<TopicIdPartition, HashSet<i64>>, error: Option<&KafkaError>) {
            let mut map = self.exception_map.lock().unwrap();
            for (partition, offset_set) in offsets {
                for offset in offset_set {
                    map.insert((partition.clone(), *offset), error.cloned());
                }
            }
        }
    }

    fn tip(topic: &str, partition: i32) -> TopicIdPartition {
        TopicIdPartition::new(Uuid::random_uuid(), TopicPartition::new(topic.to_string(), partition))
    }

    /// Translated from `AcknowledgementCommitCallbackHandlerTest.testNoException`.
    #[tokio::test]
    async fn test_no_exception() {
        let tip0 = tip("test-topic", 0);
        let callback = Arc::new(RecordingCallback::new());
        let mut handler = AcknowledgementCommitCallbackHandler::new(callback.clone());

        let mut acknowledgements = Acknowledgements::empty();
        acknowledgements.add(0, AcknowledgeType::Accept);
        acknowledgements.add(1, AcknowledgeType::Reject);
        let mut acknowledgements_map = HashMap::new();
        acknowledgements_map.insert(tip0.clone(), acknowledgements);

        handler.on_complete(vec![acknowledgements_map]).await;

        let map = callback.exception_map.lock().unwrap();
        assert!(map.get(&(tip0.clone(), 0)).unwrap().is_none());
        assert!(map.get(&(tip0, 1)).unwrap().is_none());
    }

    /// Translated from `AcknowledgementCommitCallbackHandlerTest.testInvalidRecord`.
    #[tokio::test]
    async fn test_invalid_record() {
        let tip0 = tip("test-topic", 0);
        let callback = Arc::new(RecordingCallback::new());
        let mut handler = AcknowledgementCommitCallbackHandler::new(callback.clone());

        let mut acknowledgements = Acknowledgements::empty();
        acknowledgements.add(0, AcknowledgeType::Accept);
        acknowledgements.add(1, AcknowledgeType::Reject);
        acknowledgements.complete(Some(KafkaError::new(Errors::InvalidRecordState)));
        let mut acknowledgements_map = HashMap::new();
        acknowledgements_map.insert(tip0.clone(), acknowledgements);

        handler.on_complete(vec![acknowledgements_map]).await;

        let map = callback.exception_map.lock().unwrap();
        assert_eq!(
            map.get(&(tip0.clone(), 0)).unwrap().as_ref().unwrap().error(),
            Errors::InvalidRecordState
        );
        assert_eq!(
            map.get(&(tip0, 1)).unwrap().as_ref().unwrap().error(),
            Errors::InvalidRecordState
        );
    }

    /// Translated from `AcknowledgementCommitCallbackHandlerTest.testUnauthorizedTopic`.
    #[tokio::test]
    async fn test_unauthorized_topic() {
        let tip0 = tip("test-topic", 0);
        let callback = Arc::new(RecordingCallback::new());
        let mut handler = AcknowledgementCommitCallbackHandler::new(callback.clone());

        let mut acknowledgements = Acknowledgements::empty();
        acknowledgements.add(0, AcknowledgeType::Accept);
        acknowledgements.add(1, AcknowledgeType::Reject);
        acknowledgements.complete(Some(KafkaError::new(Errors::TopicAuthorizationFailed)));
        let mut acknowledgements_map = HashMap::new();
        acknowledgements_map.insert(tip0.clone(), acknowledgements);

        handler.on_complete(vec![acknowledgements_map]).await;

        let map = callback.exception_map.lock().unwrap();
        assert_eq!(
            map.get(&(tip0.clone(), 0)).unwrap().as_ref().unwrap().error(),
            Errors::TopicAuthorizationFailed
        );
        assert_eq!(
            map.get(&(tip0, 1)).unwrap().as_ref().unwrap().error(),
            Errors::TopicAuthorizationFailed
        );
    }

    /// Translated from `AcknowledgementCommitCallbackHandlerTest.testMultiplePartitions`.
    #[tokio::test]
    async fn test_multiple_partitions() {
        let tip0 = tip("test-topic", 0);
        let tip1 = tip("test-topic-2", 0);
        let tip2 = tip("test-topic-2", 1);
        let callback = Arc::new(RecordingCallback::new());
        let mut handler = AcknowledgementCommitCallbackHandler::new(callback.clone());

        let mut acknowledgements = Acknowledgements::empty();
        acknowledgements.add(0, AcknowledgeType::Accept);
        acknowledgements.add(1, AcknowledgeType::Reject);
        acknowledgements.complete(Some(KafkaError::new(Errors::TopicAuthorizationFailed)));
        let mut acknowledgements_map = HashMap::new();
        acknowledgements_map.insert(tip0.clone(), acknowledgements);

        let mut acknowledgements1 = Acknowledgements::empty();
        acknowledgements1.add(0, AcknowledgeType::Release);
        acknowledgements1.complete(Some(KafkaError::new(Errors::InvalidRecordState)));
        acknowledgements_map.insert(tip1.clone(), acknowledgements1);

        let mut acknowledgements_map2 = HashMap::new();
        let mut acknowledgements2 = Acknowledgements::empty();
        acknowledgements2.add(0, AcknowledgeType::Accept);
        acknowledgements_map2.insert(tip2.clone(), acknowledgements2);

        handler.on_complete(vec![acknowledgements_map, acknowledgements_map2]).await;

        let map = callback.exception_map.lock().unwrap();
        assert_eq!(
            map.get(&(tip0.clone(), 0)).unwrap().as_ref().unwrap().error(),
            Errors::TopicAuthorizationFailed
        );
        assert_eq!(
            map.get(&(tip0, 1)).unwrap().as_ref().unwrap().error(),
            Errors::TopicAuthorizationFailed
        );
        assert_eq!(
            map.get(&(tip1, 0)).unwrap().as_ref().unwrap().error(),
            Errors::InvalidRecordState
        );
        assert!(map.get(&(tip2, 0)).unwrap().is_none());
    }
}
