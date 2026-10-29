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

//! Translation of the Java test fixture
//! `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/ConsumerAssignmentPoller.java`.
//!
//! Java's poller is a `ShutdownableThread` that owns one consumer and calls
//! `poll(50ms)` in a loop, tracking the assignment through a wrapping
//! `ConsumerRebalanceListener`, counting received records, and recording the
//! first non-wakeup exception (after which the thread exits). Here the thread
//! becomes one spawned tokio task per poller (CLAUDE.md §9.7), and the
//! `volatile` / `synchronizedSet` fields become atomics and `std::sync::Mutex`es
//! shared with the test; no guard is ever held across an `.await`.
//!
//! The subscribe-mode (`ConsumerAssignmentPoller(Consumer, List<String>)`) and
//! assign-mode (`ConsumerAssignmentPoller(Consumer, Set<TopicPartition>)`)
//! constructors are translated; the user-listener chaining and
//! `subscribe(List)` have no caller among the translated tests yet.
//!
//! Java's poller only borrows its consumer: the test keeps using it after
//! `shutdown()` (e.g. to close it, or to start a second poller on it) and
//! closes it in `tearDown`. The task owns the consumer here, so
//! [`ConsumerAssignmentPoller::shutdown`] hands it back instead.
//!
//! A multilanguage scenario cannot pass a Rust listener to a gRPC consumer, so
//! [`ConsumerAssignmentPoller::start_with_log`] subscribes through a
//! [`ConsumerCallbackLog`]'s logging listener instead, which records every
//! listener call on any backend, and the task replays the new `assigned`,
//! `revoked` and `lost` entries into the same assignment view after each
//! `poll`, where Java's listener updates it inside that `poll`. A `lost` entry
//! removes, as Java's tracking listener, which does not override
//! `onPartitionsLost`, gets `onPartitionsRevoked` through the default method.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use confluent_kafka::common::Error;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::consumer::ConsumerRebalanceListener;
use confluent_kafka::consumer::DynConsumer;
use tokio::task::JoinHandle;

use crate::common::callback_log::ConsumerCallbackLog;
use crate::common::callback_log::KIND_ASSIGNED;
use crate::common::callback_log::KIND_LOST;
use crate::common::callback_log::KIND_REVOKED;

/// Byte-array consumer, Java's `Consumer<byte[], byte[]>`.
pub type BytesConsumer = Box<dyn DynConsumer<Vec<u8>, Vec<u8>>>;

/// Bound on joining the poller task in [`ConsumerAssignmentPoller::shutdown`].
/// Java's `ShutdownableThread.awaitShutdown` joins unbounded; a bound keeps a
/// wedged consumer from hanging the test run instead.
const SHUTDOWN_JOIN_TIMEOUT: Duration = Duration::from_secs(30);

/// The anonymous `ConsumerRebalanceListener` Java builds in the poller's
/// constructor: it mirrors every assignment change into `partitionAssignment`.
struct AssignmentTrackingListener {
    partition_assignment: Arc<Mutex<HashSet<TopicPartition>>>,
}

#[async_trait]
impl ConsumerRebalanceListener for AssignmentTrackingListener {
    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        let mut assignment = self.partition_assignment.lock().expect("assignment mutex poisoned");
        for tp in partitions {
            assignment.remove(tp);
        }
        Ok(())
    }

    async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        self.partition_assignment
            .lock()
            .expect("assignment mutex poisoned")
            .extend(partitions.iter().cloned());
        Ok(())
    }
}

/// The log-backed form of [`AssignmentTrackingListener`], for
/// [`ConsumerAssignmentPoller::start_with_log`]: the log the subscribed
/// logging listener writes to, and how many of its entries were already
/// applied (or predate the poller).
struct AssignmentTrackingLog {
    log: Arc<ConsumerCallbackLog>,
    applied: usize,
}

impl AssignmentTrackingLog {
    /// Applies the entries the listener logged since the last call to
    /// `partition_assignment`, as [`AssignmentTrackingListener`] applies its
    /// calls: `assigned` adds, `revoked` and `lost` remove.
    async fn apply_new_entries(&mut self, partition_assignment: &Mutex<HashSet<TopicPartition>>) -> Result<(), Error> {
        let entries = self.log.entries().await?;
        let mut assignment = partition_assignment.lock().expect("assignment mutex poisoned");
        for entry in entries.iter().skip(self.applied) {
            let partitions = entry
                .partitions
                .iter()
                .map(|(topic, partition)| TopicPartition::new(topic.clone(), *partition));
            match entry.kind.as_str() {
                KIND_ASSIGNED => assignment.extend(partitions),
                KIND_REVOKED | KIND_LOST => {
                    for tp in partitions {
                        assignment.remove(&tp);
                    }
                },
                _ => {},
            }
        }
        self.applied = entries.len();
        Ok(())
    }
}

/// See the module documentation.
pub struct ConsumerAssignmentPoller {
    partition_assignment: Arc<Mutex<HashSet<TopicPartition>>>,
    received_messages: Arc<AtomicUsize>,
    thrown_error: Arc<Mutex<Option<Error>>>,
    shutdown_requested: Arc<AtomicBool>,
    handle: Option<JoinHandle<BytesConsumer>>,
}

impl ConsumerAssignmentPoller {
    /// Java's `ConsumerAssignmentPoller(consumer, topicsToSubscribe)` followed
    /// by `start()`: subscribes with the tracking listener, then spawns the
    /// polling task, which owns the consumer until [`Self::shutdown`].
    pub async fn start(mut consumer: BytesConsumer, topics_to_subscribe: Vec<String>) -> Self {
        let partition_assignment = Arc::new(Mutex::new(HashSet::new()));
        let listener = Arc::new(AssignmentTrackingListener { partition_assignment: Arc::clone(&partition_assignment) });
        consumer
            .subscribe_with_topics_listener(topics_to_subscribe, listener)
            .await
            .expect("subscribe should succeed");
        Self::spawn(consumer, partition_assignment, None)
    }

    /// [`Self::start`] for a consumer of any backend: subscribes through `log`'s
    /// logging listener (the consumer's own log, from
    /// `create_with_callback_log`) instead of a Rust listener, which a gRPC
    /// consumer cannot take, and tracks the assignment from the `assigned`,
    /// `revoked` and `lost` entries the listener logs after the poller starts.
    /// The task applies them after each `poll`, so
    /// [`Self::consumer_assignment`] sees a `poll`'s callbacks once it returns.
    /// A failure to read the log ends the task like a `poll` error, recorded
    /// as [`Self::thrown_error`].
    pub async fn start_with_log(
        mut consumer: BytesConsumer,
        log: Arc<ConsumerCallbackLog>,
        topics_to_subscribe: Vec<String>,
    ) -> Self {
        let applied = log.entries().await.expect("read the consumer callback log").len();
        log.subscribe_with_logging_listener(&mut consumer, topics_to_subscribe)
            .await
            .expect("subscribe should succeed");
        Self::spawn(
            consumer,
            Arc::new(Mutex::new(HashSet::new())),
            Some(AssignmentTrackingLog { log, applied }),
        )
    }

    /// Java's `ConsumerAssignmentPoller(consumer, partitionsToAssign)` followed
    /// by `start()`: `assign`s the partitions (no listener fires for a manual
    /// assignment), then spawns the polling task.
    pub async fn start_with_partitions_to_assign(
        mut consumer: BytesConsumer,
        partitions_to_assign: Vec<TopicPartition>,
    ) -> Self {
        consumer.assign(partitions_to_assign).await.expect("assign should succeed");
        Self::spawn(consumer, Arc::new(Mutex::new(HashSet::new())), None)
    }

    /// Java's `start()`: the polling loop of `doWork()`. With `tracking_log`
    /// the assignment is replayed from the log after each `poll`.
    fn spawn(
        mut consumer: BytesConsumer,
        partition_assignment: Arc<Mutex<HashSet<TopicPartition>>>,
        mut tracking_log: Option<AssignmentTrackingLog>,
    ) -> Self {
        let received_messages = Arc::new(AtomicUsize::new(0));
        let thrown_error = Arc::new(Mutex::new(None));
        let shutdown_requested = Arc::new(AtomicBool::new(false));

        let handle = {
            let partition_assignment = Arc::clone(&partition_assignment);
            let received_messages = Arc::clone(&received_messages);
            let thrown_error = Arc::clone(&thrown_error);
            let shutdown_requested = Arc::clone(&shutdown_requested);
            tokio::spawn(async move {
                // Java's `ShutdownableThread.run`: `while (isRunning()) doWork();`,
                // where a throwing `doWork` ends the thread.
                while !shutdown_requested.load(Ordering::SeqCst) {
                    let polled = consumer.poll(Duration::from_millis(50)).await;
                    // The listener calls of this `poll`, as Java's listener
                    // applies them inside it, before its result is handled.
                    let polled = match &mut tracking_log {
                        Some(tracking) => match tracking.apply_new_entries(&partition_assignment).await {
                            Ok(()) => polled,
                            Err(e) => Err(e),
                        },
                        None => polled,
                    };
                    match polled {
                        Ok(records) => {
                            received_messages.fetch_add(records.count(), Ordering::SeqCst);
                        },
                        // Java: `catch (WakeupException e) { // ignore for shutdown }`.
                        Err(Error::Wakeup(_)) => {},
                        Err(e) => {
                            // Java: `thrownException = Optional.of(e); throw e;`,
                            // which `ShutdownableThread.run` catches and, unless
                            // a shutdown was requested, logs: `log.error("Error
                            // due to", e)`.
                            if !shutdown_requested.load(Ordering::SeqCst) {
                                log::error!("Error due to {e:?}");
                            }
                            *thrown_error.lock().expect("thrown error mutex poisoned") = Some(e);
                            break;
                        },
                    }
                }
                consumer
            })
        };

        Self {
            partition_assignment,
            received_messages,
            thrown_error,
            shutdown_requested,
            handle: Some(handle),
        }
    }

    /// Java's `consumerAssignment()`: a snapshot of the tracked assignment.
    pub fn consumer_assignment(&self) -> HashSet<TopicPartition> {
        self.partition_assignment.lock().expect("assignment mutex poisoned").clone()
    }

    /// Java's `getThrownException()`.
    pub fn thrown_error(&self) -> Option<Error> {
        self.thrown_error.lock().expect("thrown error mutex poisoned").clone()
    }

    /// Java's `receivedMessages()`.
    pub fn received_messages(&self) -> usize {
        self.received_messages.load(Ordering::SeqCst)
    }

    /// Java's `shutdown()` (`initiateShutdown()` + `awaitShutdown()`),
    /// returning the consumer the task owned; `None` if already shut down.
    ///
    /// Java's `initiateShutdown` also calls `consumer.wakeup()` to interrupt a
    /// blocked `poll`; the task owns the consumer here, and each `poll` is
    /// bounded at 50 ms, so the flag alone ends the loop within one poll.
    pub async fn shutdown(&mut self) -> Option<BytesConsumer> {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        let handle = self.handle.take()?;
        Some(
            tokio::time::timeout(SHUTDOWN_JOIN_TIMEOUT, handle)
                .await
                .expect("consumer assignment poller did not shut down in time")
                .expect("consumer assignment poller task panicked"),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use confluent_kafka::consumer::MockConsumer;

    use super::*;
    use crate::common::callback_log::CallbackLogEntry;

    /// Appends a rebalance entry to a native log, as its logging listener does.
    fn log_rebalance(log: &ConsumerCallbackLog, kind: &str, partitions: &[(&str, i32)]) {
        // Without `multilanguage-tests` the log has only the native variant.
        #[cfg_attr(not(feature = "multilanguage-tests"), expect(clippy::infallible_destructuring_match))]
        let entries = match log {
            ConsumerCallbackLog::Native(entries) => entries,
            #[cfg(feature = "multilanguage-tests")]
            ConsumerCallbackLog::Grpc(_) => panic!("a native log"),
        };
        entries.lock().expect("callback log poisoned").push(CallbackLogEntry {
            kind: kind.to_string(),
            partitions: partitions.iter().map(|(t, p)| (t.to_string(), *p)).collect(),
            ..Default::default()
        });
    }

    async fn wait_for_assignment(poller: &ConsumerAssignmentPoller, expected: &[(&str, i32)]) {
        let expected: HashSet<TopicPartition> =
            expected.iter().map(|(t, p)| TopicPartition::new(t.to_string(), *p)).collect();
        let deadline = Instant::now() + Duration::from_secs(10);
        while poller.consumer_assignment() != expected {
            assert!(
                Instant::now() < deadline,
                "assignment {:?}, expected {expected:?}",
                poller.consumer_assignment()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// `start_with_log` replays the entries logged after it started (an entry
    /// already in the log is not part of this poller's view): `assigned`
    /// adds, `revoked` and `lost` remove. No broker: a `MockConsumer`, whose
    /// entries the test logs as its listener would.
    #[tokio::test(flavor = "multi_thread")]
    async fn start_with_log_tracks_the_logged_assignment() {
        let log = Arc::new(ConsumerCallbackLog::native());
        log_rebalance(&log, KIND_ASSIGNED, &[("earlier", 0)]);
        let consumer: BytesConsumer =
            Box::new(MockConsumer::<Vec<u8>, Vec<u8>>::new("earliest").expect("mock consumer"));
        let mut poller = ConsumerAssignmentPoller::start_with_log(consumer, Arc::clone(&log), vec!["t".into()]).await;

        log_rebalance(&log, KIND_ASSIGNED, &[("t", 0), ("t", 1)]);
        wait_for_assignment(&poller, &[("t", 0), ("t", 1)]).await;
        log_rebalance(&log, KIND_REVOKED, &[("t", 0)]);
        wait_for_assignment(&poller, &[("t", 1)]).await;
        log_rebalance(&log, KIND_LOST, &[("t", 1)]);
        wait_for_assignment(&poller, &[]).await;

        assert!(poller.thrown_error().is_none(), "{:?}", poller.thrown_error());
        assert!(poller.shutdown().await.is_some());
    }
}
