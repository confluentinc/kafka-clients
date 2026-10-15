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
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerRebalanceListener;
use tokio::task::JoinHandle;

/// Byte-array consumer, Java's `Consumer<byte[], byte[]>`.
pub type BytesConsumer = Box<dyn Consumer<Vec<u8>, Vec<u8>>>;

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
        Self::spawn(consumer, partition_assignment)
    }

    /// Java's `ConsumerAssignmentPoller(consumer, partitionsToAssign)` followed
    /// by `start()`: `assign`s the partitions (no listener fires for a manual
    /// assignment), then spawns the polling task.
    pub async fn start_with_partitions_to_assign(
        mut consumer: BytesConsumer,
        partitions_to_assign: Vec<TopicPartition>,
    ) -> Self {
        consumer.assign(partitions_to_assign).await.expect("assign should succeed");
        Self::spawn(consumer, Arc::new(Mutex::new(HashSet::new())))
    }

    /// Java's `start()`: the polling loop of `doWork()`.
    fn spawn(mut consumer: BytesConsumer, partition_assignment: Arc<Mutex<HashSet<TopicPartition>>>) -> Self {
        let received_messages = Arc::new(AtomicUsize::new(0));
        let thrown_error = Arc::new(Mutex::new(None));
        let shutdown_requested = Arc::new(AtomicBool::new(false));

        let handle = {
            let received_messages = Arc::clone(&received_messages);
            let thrown_error = Arc::clone(&thrown_error);
            let shutdown_requested = Arc::clone(&shutdown_requested);
            tokio::spawn(async move {
                // Java's `ShutdownableThread.run`: `while (isRunning()) doWork();`,
                // where a throwing `doWork` ends the thread.
                while !shutdown_requested.load(Ordering::SeqCst) {
                    match consumer.poll(Duration::from_millis(50)).await {
                        Ok(records) => {
                            received_messages.fetch_add(records.count(), Ordering::SeqCst);
                        },
                        // Java: `catch (WakeupException e) { // ignore for shutdown }`.
                        Err(Error::Wakeup(_)) => {},
                        Err(e) => {
                            // Java: `thrownException = Optional.of(e); throw e;`.
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
