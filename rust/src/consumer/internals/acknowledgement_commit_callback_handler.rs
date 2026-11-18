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

//! Runs the user's [`AcknowledgementCommitCallback`] over completed
//! acknowledgements.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.AcknowledgementCommitCallbackHandler`.

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures_util::FutureExt;

use crate::common::TopicIdPartition;
use crate::consumer::AcknowledgementCommitCallback;
use crate::consumer::internals::{Acknowledgements, panic_payload_message};

/// Calls the registered [`AcknowledgementCommitCallback`] once per completed
/// `(partition, acknowledgements)` entry. It runs on the share consumer's
/// caller task (`share-consumer.md` §3).
#[doc(alias = "org.apache.kafka.clients.consumer.internals.AcknowledgementCommitCallbackHandler")]
pub(crate) struct AcknowledgementCommitCallbackHandler {
    acknowledgement_commit_callback: Arc<dyn AcknowledgementCommitCallback>,
    entered_callback: bool,
}

impl AcknowledgementCommitCallbackHandler {
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.AcknowledgementCommitCallbackHandler#AcknowledgementCommitCallbackHandler"
    )]
    pub(crate) fn new(acknowledgement_commit_callback: Arc<dyn AcknowledgementCommitCallback>) -> Self {
        Self { acknowledgement_commit_callback, entered_callback: false }
    }

    /// Whether the callback is running.
    // Java reads it in `ShareConsumerImpl#acquire` to reject consumer calls
    // from inside the callback; in Rust the consumer is mutably borrowed for
    // the whole call (`share-consumer.md` §3), so the flag is only observable
    // as Java's `false` between calls. That holds also when the `on_complete`
    // future is dropped mid-callback: `ResetOnDrop` clears it, as Java's
    // `finally` does.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.AcknowledgementCommitCallbackHandler#hasEnteredCallback"
    )]
    pub(crate) fn has_entered_callback(&self) -> bool {
        self.entered_callback
    }

    /// Calls the callback once for each `(partition, acknowledgements)` entry
    /// of each map, with the single-entry map `{partition: offsets}` and the
    /// entry's acknowledge error.
    ///
    /// The callback returns nothing (decision D8), so the only failure it can
    /// raise is a panic: Java catches what the callback throws, logs it and
    /// goes on with the remaining entries, and so does this method with a
    /// panic.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AcknowledgementCommitCallbackHandler#onComplete")]
    pub(crate) async fn on_complete(
        &mut self,
        acknowledgements_map_list: &[HashMap<TopicIdPartition, Acknowledgements>],
    ) {
        for acknowledgements_map in acknowledgements_map_list {
            for (partition, acknowledgements) in acknowledgements_map {
                let error = acknowledgements.acknowledge_error();
                let offsets_copy: HashSet<i64> = acknowledgements.acknowledgements_type_map().keys().copied().collect();
                let offsets = HashMap::from([(partition.clone(), offsets_copy)]);
                self.entered_callback = true;
                // Java's `finally { enteredCallback = false; }`: the guard also
                // runs when this future is dropped at the await (a cancelled
                // `poll` / `commit_*` / `close`).
                let entered_callback = ResetOnDrop(&mut self.entered_callback);
                let result = AssertUnwindSafe(self.acknowledgement_commit_callback.on_complete(&offsets, error))
                    .catch_unwind()
                    .await;
                drop(entered_callback);
                if let Err(payload) = result {
                    // Java: `LOG.error("Exception thrown by acknowledgement commit
                    // callback", e)`; "exception" is not used in Rust code
                    // (CLAUDE.md §2).
                    log::error!(
                        "Error thrown by acknowledgement commit callback: {}",
                        panic_payload_message(&*payload)
                    );
                }
            }
        }
    }
}

/// Clears the flag it borrows when dropped: Java's `finally` block, which an
/// `.await` that can be cancelled needs (translation-checklist R4).
// A private helper inside `onComplete` (DoD §7): Java's `try/finally` has no
// type.
struct ResetOnDrop<'a>(&'a mut bool);

impl Drop for ResetOnDrop<'_> {
    fn drop(&mut self) {
        *self.0 = false;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::common::protocol::Errors;
    use crate::common::{Error, TopicPartition, Uuid};
    use crate::consumer::AcknowledgeType;

    /// Java's `TopicPartitionAndOffset`, the key of `exceptionMap`.
    type TopicPartitionAndOffset = (TopicIdPartition, i64);

    /// Java's `exceptionMap`: the error each `(partition, offset)` was last
    /// reported with, `None` for a success.
    type ErrorMap = Arc<Mutex<HashMap<TopicPartitionAndOffset, Option<Error>>>>;

    /// The offsets map of each callback call, in call order.
    type Calls = Arc<Mutex<Vec<HashMap<TopicIdPartition, HashSet<i64>>>>>;

    /// Java's `TestableAcknowledgeCommitCallback`.
    struct TestableAcknowledgeCommitCallback {
        error_map: ErrorMap,
        calls: Calls,
    }

    #[async_trait]
    impl AcknowledgementCommitCallback for TestableAcknowledgeCommitCallback {
        async fn on_complete(&self, offsets_map: &HashMap<TopicIdPartition, HashSet<i64>>, error: Option<&Error>) {
            self.calls.lock().unwrap().push(offsets_map.clone());
            let mut error_map = self.error_map.lock().unwrap();
            for (partition, offsets) in offsets_map {
                for &offset in offsets {
                    error_map.insert((partition.clone(), offset), error.cloned());
                }
            }
        }
    }

    struct Fixture {
        handler: AcknowledgementCommitCallbackHandler,
        error_map: ErrorMap,
        calls: Calls,
        tip0: TopicIdPartition,
        tip1: TopicIdPartition,
        tip2: TopicIdPartition,
    }

    /// Java's `setup()` and the test-class fields.
    fn setup() -> Fixture {
        let error_map = ErrorMap::default();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let callback =
            TestableAcknowledgeCommitCallback { error_map: Arc::clone(&error_map), calls: Arc::clone(&calls) };
        Fixture {
            handler: AcknowledgementCommitCallbackHandler::new(Arc::new(callback)),
            error_map,
            calls,
            tip0: TopicIdPartition::new(Uuid::random_uuid(), TopicPartition::new("test-topic", 0)),
            tip1: TopicIdPartition::new(Uuid::random_uuid(), TopicPartition::new("test-topic-2", 0)),
            tip2: TopicIdPartition::new(Uuid::random_uuid(), TopicPartition::new("test-topic-2", 1)),
        }
    }

    impl Fixture {
        /// The error recorded for `(partition, offset)`. Java's
        /// `exceptionMap.get(tpo)`, except that a missing entry fails: every
        /// offset the tests read has been reported.
        fn error_for(&self, partition: &TopicIdPartition, offset: i64) -> Option<Error> {
            self.error_map
                .lock()
                .unwrap()
                .get(&(partition.clone(), offset))
                .unwrap_or_else(|| panic!("no callback for {partition}@{offset}"))
                .clone()
        }
    }

    fn accept_reject() -> Acknowledgements {
        let mut acknowledgements = Acknowledgements::empty();
        acknowledgements.add(0, Some(AcknowledgeType::Accept));
        acknowledgements.add(1, Some(AcknowledgeType::Reject));
        acknowledgements
    }

    // Java wraps the assertions in `TestUtils.retryOnExceptionWithTimeout`;
    // `on_complete` has run the callback by the time it returns, so they are
    // asserted once.

    // Java's `testNoException`; Rust code does not use "exception" (CLAUDE.md §2).
    #[tokio::test]
    async fn test_no_error() {
        let mut fixture = setup();
        let acknowledgements_map = HashMap::from([(fixture.tip0.clone(), accept_reject())]);

        fixture.handler.on_complete(&[acknowledgements_map]).await;

        assert!(fixture.error_for(&fixture.tip0, 0).is_none());
        assert!(fixture.error_for(&fixture.tip0, 1).is_none());
    }

    #[tokio::test]
    async fn test_invalid_record() {
        let mut fixture = setup();
        let mut acknowledgements = accept_reject();
        acknowledgements.complete(Errors::InvalidRecordState.error());
        let acknowledgements_map = HashMap::from([(fixture.tip0.clone(), acknowledgements)]);

        fixture.handler.on_complete(&[acknowledgements_map]).await;

        assert!(matches!(
            fixture.error_for(&fixture.tip0, 0),
            Some(Error::InvalidRecordState(_))
        ));
        assert!(matches!(
            fixture.error_for(&fixture.tip0, 1),
            Some(Error::InvalidRecordState(_))
        ));
    }

    #[tokio::test]
    async fn test_unauthorized_topic() {
        let mut fixture = setup();
        let mut acknowledgements = accept_reject();
        acknowledgements.complete(Errors::TopicAuthorizationFailed.error());
        let acknowledgements_map = HashMap::from([(fixture.tip0.clone(), acknowledgements)]);

        fixture.handler.on_complete(&[acknowledgements_map]).await;

        assert!(matches!(
            fixture.error_for(&fixture.tip0, 0),
            Some(Error::TopicAuthorization(_))
        ));
        assert!(matches!(
            fixture.error_for(&fixture.tip0, 1),
            Some(Error::TopicAuthorization(_))
        ));
    }

    #[tokio::test]
    async fn test_multiple_partitions() {
        let mut fixture = setup();
        let mut acknowledgements = accept_reject();
        acknowledgements.complete(Errors::TopicAuthorizationFailed.error());
        let mut acknowledgements1 = Acknowledgements::empty();
        acknowledgements1.add(0, Some(AcknowledgeType::Release));
        acknowledgements1.complete(Errors::InvalidRecordState.error());
        let acknowledgements_map = HashMap::from([
            (fixture.tip0.clone(), acknowledgements),
            (fixture.tip1.clone(), acknowledgements1),
        ]);

        let mut acknowledgements2 = Acknowledgements::empty();
        acknowledgements2.add(0, Some(AcknowledgeType::Accept));
        let acknowledgements_map2 = HashMap::from([(fixture.tip2.clone(), acknowledgements2)]);

        fixture
            .handler
            .on_complete(&[acknowledgements_map, acknowledgements_map2])
            .await;

        assert!(matches!(
            fixture.error_for(&fixture.tip0, 0),
            Some(Error::TopicAuthorization(_))
        ));
        assert!(matches!(
            fixture.error_for(&fixture.tip0, 1),
            Some(Error::TopicAuthorization(_))
        ));
        assert!(matches!(
            fixture.error_for(&fixture.tip1, 0),
            Some(Error::InvalidRecordState(_))
        ));
        assert!(fixture.error_for(&fixture.tip2, 0).is_none());
    }

    // The tests below are not in `AcknowledgementCommitCallbackHandlerTest`;
    // they pin the parts of `onComplete` that `share-consumer.md` §3 makes
    // contract.

    /// One call per `(partition, acknowledgements)` entry, each with a
    /// single-entry map holding all that entry's offsets.
    #[tokio::test]
    async fn test_one_call_per_partition_entry() {
        let mut fixture = setup();
        let mut gap_and_accept = Acknowledgements::empty();
        gap_and_accept.add_gap(3);
        gap_and_accept.add(4, Some(AcknowledgeType::Accept));
        let acknowledgements_map = HashMap::from([
            (fixture.tip0.clone(), accept_reject()),
            (fixture.tip1.clone(), gap_and_accept),
        ]);
        let acknowledgements_map2 = HashMap::from([(fixture.tip2.clone(), accept_reject())]);

        fixture
            .handler
            .on_complete(&[acknowledgements_map, acknowledgements_map2])
            .await;

        let mut calls = fixture.calls.lock().unwrap().clone();
        assert_eq!(3, calls.len());
        assert!(calls.iter().all(|call| call.len() == 1));
        calls.sort_by_key(|call| call.keys().next().unwrap().to_string());
        let mut expected = vec![
            HashMap::from([(fixture.tip0.clone(), HashSet::from([0, 1]))]),
            HashMap::from([(fixture.tip1.clone(), HashSet::from([3, 4]))]),
            HashMap::from([(fixture.tip2.clone(), HashSet::from([0, 1]))]),
        ];
        expected.sort_by_key(|call| call.keys().next().unwrap().to_string());
        assert_eq!(expected, calls);
        assert!(!fixture.handler.has_entered_callback());
    }

    /// The future is `Send`: the share consumer awaits it inside its own
    /// `Send` futures (`poll`, `commit_*`, `close`).
    #[test]
    fn test_on_complete_future_is_send() {
        fn assert_send<T: Send>(_: &T) {}
        let mut fixture = setup();
        let acknowledgements_map_list = [HashMap::from([(fixture.tip0.clone(), accept_reject())])];
        let future = fixture.handler.on_complete(&acknowledgements_map_list);
        assert_send(&future);
    }

    /// Java's `finally` resets `enteredCallback` on every exit; dropping the
    /// future while the callback is pending (a cancelled `poll`) must clear it
    /// too, or a later `ShareConsumerImpl#acquire` would reject every call.
    #[tokio::test]
    async fn test_dropped_future_resets_entered_callback() {
        struct PendingCallback;

        #[async_trait]
        impl AcknowledgementCommitCallback for PendingCallback {
            async fn on_complete(&self, _offsets: &HashMap<TopicIdPartition, HashSet<i64>>, _error: Option<&Error>) {
                std::future::pending::<()>().await;
            }
        }

        let fixture = setup();
        let mut handler = AcknowledgementCommitCallbackHandler::new(Arc::new(PendingCallback));
        let acknowledgements_map_list = [HashMap::from([(fixture.tip0.clone(), accept_reject())])];

        // Polled once, so the callback is entered and pending, then dropped.
        assert!(handler.on_complete(&acknowledgements_map_list).now_or_never().is_none());

        assert!(!handler.has_entered_callback());
    }

    /// A panicking callback is logged and swallowed, and the remaining entries
    /// still get their call.
    #[tokio::test]
    async fn test_panicking_callback_is_swallowed() {
        struct PanickingCallback {
            calls: Arc<Mutex<usize>>,
        }

        #[async_trait]
        impl AcknowledgementCommitCallback for PanickingCallback {
            async fn on_complete(&self, _offsets: &HashMap<TopicIdPartition, HashSet<i64>>, _error: Option<&Error>) {
                *self.calls.lock().unwrap() += 1;
                panic!("callback failed");
            }
        }

        let fixture = setup();
        let calls = Arc::new(Mutex::new(0));
        let mut handler =
            AcknowledgementCommitCallbackHandler::new(Arc::new(PanickingCallback { calls: Arc::clone(&calls) }));
        let acknowledgements_map = HashMap::from([
            (fixture.tip0.clone(), accept_reject()),
            (fixture.tip1.clone(), accept_reject()),
        ]);

        handler.on_complete(&[acknowledgements_map]).await;

        assert_eq!(2, *calls.lock().unwrap());
        assert!(!handler.has_entered_callback());
    }
}
