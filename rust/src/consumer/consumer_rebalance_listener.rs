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

//! Callback interface notified when the set of partitions assigned to the
//! consumer changes.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.ConsumerRebalanceListener`.

use async_trait::async_trait;

use crate::common::{Error, TopicPartition};
#[cfg(doc)]
use crate::consumer::GroupProtocol;

/// A callback interface that the user can implement to trigger custom actions
/// when the set of partitions assigned to the consumer changes.
///
/// This is applicable when the consumer is having Kafka auto-manage group
/// membership. If the consumer directly assigns partitions, those partitions
/// will never be reassigned and this callback is not applicable.
///
/// When Kafka is managing the group membership, a partition re-assignment will
/// be triggered any time the members of the group change or the subscription
/// of the members changes. This can occur when processes die, new process
/// instances are added or old instances come back to life after failure.
/// Partition re-assignments can also be triggered by changes affecting the
/// subscribed topics (e.g. when the number of partitions is administratively
/// adjusted).
///
/// There are many uses for this functionality. One common use is saving
/// offsets in a custom store. By saving offsets in the
/// [`on_partitions_revoked`](Self::on_partitions_revoked) call we can ensure
/// that any time partition assignment changes the offset gets saved.
///
/// Another use is flushing out any kind of cache of intermediate results the
/// consumer may be keeping. For example, consider a case where the consumer is
/// subscribed to a topic containing user page views, and the goal is to count
/// the number of page views per user for each five minute window. Let's say
/// the topic is partitioned by the user id so that all events for a particular
/// user go to a single consumer instance. The consumer can keep in memory a
/// running tally of actions per user and only flush these out to a remote data
/// store when its cache gets too big. However if a partition is reassigned it
/// may want to automatically trigger a flush of this cache, before the new
/// owner takes over consumption.
///
/// This callback will only execute in the user task as part of the
/// [`Consumer::poll`](crate::consumer::Consumer::poll) call whenever partition
/// assignment changes.
///
/// Under normal conditions, if a partition is reassigned from one consumer to
/// another, then the old consumer will always invoke `on_partitions_revoked`
/// for that partition prior to the new consumer invoking
/// `on_partitions_assigned` for the same partition. So if offsets or other
/// state is saved in the `on_partitions_revoked` call by one consumer member,
/// it will always be accessible by the time the other consumer member taking
/// over that partition and triggering its `on_partitions_assigned` callback to
/// load the state.
///
/// You can think of revocation as a graceful way to give up ownership of a
/// partition. In some cases, the consumer may not have an opportunity to do
/// so. For example, if the session times out, then the partitions may be
/// reassigned before we have a chance to revoke them gracefully. For this
/// case, we have a third callback
/// [`on_partitions_lost`](Self::on_partitions_lost). The difference between
/// this function and `on_partitions_revoked` is that upon invocation of
/// `on_partitions_lost`, the partitions may already be owned by some other
/// members in the group and therefore users would not be able to commit its
/// consumed offsets for example. Users could implement these two functions
/// differently (by default, `on_partitions_lost` will be calling
/// `on_partitions_revoked` directly); for example, in the
/// `on_partitions_lost` we should not need to store the offsets since we know
/// these partitions are no longer owned by the consumer at that time.
///
/// During a rebalance event, the `on_partitions_assigned` function will always
/// be triggered exactly once when the rebalance completes. That is, even if
/// there is no newly assigned partitions for a consumer member, its
/// `on_partitions_assigned` will still be triggered with an empty collection
/// of partitions. As a result this function can be used also to notify when a
/// rebalance event has happened. With eager rebalancing,
/// `on_partitions_revoked` will always be called at the start of a rebalance.
/// On the other hand, `on_partitions_lost` will only be called when there were
/// non-empty partitions that were lost. With cooperative rebalancing,
/// `on_partitions_revoked` and `on_partitions_lost` will only be triggered
/// when there are non-empty partitions revoked or lost from this consumer
/// member during a rebalance event.
///
/// It is possible for a [`Error::is_wakeup_error`] or
/// [`Error::is_interrupt_error`] error to be raised from one of these nested
/// invocations. In this case, the error will be propagated to the current
/// invocation of [`Consumer::poll`](crate::consumer::Consumer::poll) in which
/// this callback is being executed. This means it is not necessary to catch
/// these errors and re-attempt to wakeup or interrupt the consumer task. Also
/// if the callback function implementation itself returns an error, this
/// error will be propagated to the current invocation of `Consumer::poll` as
/// well.
///
/// Note that callbacks only serve as notification of an assignment change.
/// They cannot be used to express acceptance of the change. Hence returning
/// an error from a callback does not affect the assignment in any way, as it
/// will be propagated all the way up to the `Consumer::poll` call. If user
/// captures the error in the caller, the callback is still assumed successful
/// and no further retries will be attempted.
///
/// Here is pseudo-code for a callback implementation for saving offsets:
///
/// ```
/// use async_trait::async_trait;
/// use confluent_kafka::common::{Error, TopicPartition};
/// use confluent_kafka::consumer::{ConsumerHandle, ConsumerRebalanceListener};
/// # fn save_offset_in_external_store(_offset: i64) {}
/// # fn read_offset_from_external_store(_partition: &TopicPartition) -> i64 { 0 }
///
/// struct SaveOffsetsOnRebalance {
///     consumer: ConsumerHandle,
/// }
///
/// #[async_trait]
/// impl ConsumerRebalanceListener for SaveOffsetsOnRebalance {
///     async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
///         // save the offsets in an external store using some custom code not described here
///         for partition in partitions {
///             save_offset_in_external_store(self.consumer.position(partition).await?);
///         }
///         Ok(())
///     }
///
///     async fn on_partitions_lost(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
///         // do not need to save the offsets since these partitions are probably owned by other consumers already
///         Ok(())
///     }
///
///     async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
///         // read the offsets from an external store using some custom code not described here
///         for partition in partitions {
///             let offset = read_offset_from_external_store(partition);
///             self.consumer.seek_with_offset(partition.clone(), offset).await?;
///         }
///         Ok(())
///     }
/// }
/// ```
///
/// # Translation notes
///
/// A listener calls back into the consumer through a
/// [`ConsumerHandle`](crate::consumer::ConsumerHandle) taken with
/// [`Consumer::handle`](crate::consumer::Consumer::handle).
// Per `consumer-threading.md` §31, listener methods execute on the caller's
// task during `poll()` and `unsubscribe()`, and `close()` invokes
// `on_partitions_revoked` / `on_partitions_lost` for the partitions the group
// assigned — never on the background task, and never from other calls such
// as `commit_*()`; rebalance state does not advance until the callback future
// resolves.
//
// Bounds `Send + Sync + 'static`: the listener is stored as
// `Arc<dyn ConsumerRebalanceListener>` inside `SubscriptionState` (behind
// `Arc<Mutex<...>>`, `consumer-threading.md` §16) and cloned out of the lock
// before `.await`, so the trait must be `Send + Sync`.
//
// `#[async_trait]` is fine here: listener invocation is per-rebalance, not
// per-record, so one boxed future per callback is irrelevant.
#[async_trait]
#[doc(alias = "org.apache.kafka.clients.consumer.ConsumerRebalanceListener")]
pub trait ConsumerRebalanceListener: Send + Sync + 'static {
    /// A callback method the user can implement to provide handling of offset
    /// commits to a customized store. This method will be called during a
    /// rebalance operation when the consumer has to give up some partitions.
    /// The consumer may need to give up some partitions (thus this callback
    /// executed) under the following scenarios:
    ///
    /// - If the consumer assignment changes
    /// - If the consumer is being closed
    ///   ([`Consumer::close`](crate::consumer::Consumer::close))
    /// - If the consumer is unsubscribing
    ///   ([`Consumer::unsubscribe`](crate::consumer::Consumer::unsubscribe))
    ///
    /// It is recommended that offsets should be committed in this callback to
    /// either Kafka or a custom offset store to prevent duplicate data.
    ///
    /// This callback is always called before re-assigning the partitions. If
    /// the consumer is using the [`GroupProtocol::Classic`] rebalance protocol:
    ///
    /// - In eager rebalancing, `on_partitions_revoked` will be called with the
    ///   full set of assigned partitions as a parameter (all partitions are
    ///   revoked). It will be called even if there are no partitions to
    ///   revoke.
    /// - In cooperative rebalancing, `on_partitions_revoked` will be called
    ///   with the set of partitions to revoke, iff the set is non-empty.
    ///
    /// If the consumer is using the [`GroupProtocol::Consumer`] rebalance
    /// protocol, this callback will be called with the set of partitions to
    /// revoke iff the set is non-empty (same behavior as the
    /// `GroupProtocol::Classic` rebalance protocol with Cooperative mode).
    ///
    /// For examples on usage of this API, see the example of
    /// [`ConsumerRebalanceListener`].
    ///
    /// It is common for the revocation callback to use the consumer instance
    /// in order to commit offsets.
    /// It is possible for a [`Error::is_wakeup_error`] or
    /// [`Error::is_interrupt_error`] error to be raised from one of these
    /// nested invocations. In this case, the error will be propagated to the
    /// current invocation of [`Consumer::poll`](crate::consumer::Consumer::poll)
    /// in which this callback is being executed. This means it is not necessary
    /// to catch these errors and re-attempt to wakeup or interrupt the consumer
    /// task.
    ///
    /// # Arguments
    ///
    /// - `partitions` - The list of partitions that were assigned to the consumer and now need to
    ///   be revoked. This will include the full assignment under the Classic/Eager protocol, given
    ///   that it revokes all partitions. It will only include the subset to revoke under the
    ///   Classic/Cooperative and Consumer protocols.
    ///
    /// # Errors
    ///
    /// - `Error::is_wakeup_error` If raised from a nested call to `KafkaConsumer`
    /// - `Error::is_interrupt_error` If raised from a nested call to `KafkaConsumer`
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked")]
    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) -> Result<(), Error>;

    /// A callback method the user can implement to provide handling of
    /// customized offsets on completion of a successful partition
    /// re-assignment. This method will be called after the partition
    /// re-assignment completes (even if no new partitions were assigned to
    /// the consumer), and before the consumer starts fetching data, and only
    /// as the result of a [`Consumer::poll`](crate::consumer::Consumer::poll)
    /// call.
    ///
    /// It is guaranteed that under normal conditions all the processes in a
    /// consumer group will execute their
    /// [`on_partitions_revoked`](Self::on_partitions_revoked) callback before
    /// any instance executes this `on_partitions_assigned` callback. During
    /// exceptional scenarios, partitions may be migrated without the old owner
    /// being notified (i.e. their `on_partitions_revoked` callback not
    /// triggered), and later when the old owner consumer realized this event,
    /// the [`on_partitions_lost`](Self::on_partitions_lost) callback will be
    /// triggered by the consumer then.
    ///
    /// It is common for the assignment callback to use the consumer instance
    /// in order to query offsets.
    /// It is possible for a [`Error::is_wakeup_error`] or
    /// [`Error::is_interrupt_error`] error to be raised from one of these
    /// nested invocations. In this case, the error will be propagated to the
    /// current invocation of [`Consumer::poll`](crate::consumer::Consumer::poll)
    /// in which this callback is being executed. This means it is not necessary
    /// to catch these errors and re-attempt to wakeup or interrupt the consumer
    /// task.
    ///
    /// # Arguments
    ///
    /// - `partitions` - Partitions that have been added to the assignment as a result of the
    ///   rebalance. Note that partitions that were already owned by this consumer and remain
    ///   assigned are not included in this list under the Classic/Cooperative or Consumer
    ///   protocols. THe full assignment will be received under the Classic/Eager protocol.
    ///
    /// # Errors
    ///
    /// - `Error::is_wakeup_error` If raised from a nested call to `KafkaConsumer`
    /// - `Error::is_interrupt_error` If raised from a nested call to `KafkaConsumer`
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned")]
    async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) -> Result<(), Error>;

    /// A callback method you can implement to provide handling of cleaning up
    /// resources for partitions that have already been reassigned to other
    /// consumers. This method will not be called during normal execution as
    /// the owned partitions would first be revoked by calling the
    /// [`on_partitions_revoked`](Self::on_partitions_revoked), before being
    /// reassigned to other consumers during a rebalance event. However, during
    /// exceptional scenarios when the consumer realized that it does not own
    /// this partition any longer, i.e. not revoked via a normal rebalance
    /// event, then this method would be invoked.
    ///
    /// For example, this function is called if a consumer's session timeout
    /// has expired, or if a fatal error has been received indicating the
    /// consumer is no longer part of the group.
    ///
    /// By default it will just trigger `on_partitions_revoked`; for users who
    /// want to distinguish the handling logic of revoked partitions v.s. lost
    /// partitions, they can override the default implementation.
    ///
    /// It is possible for a [`Error::is_wakeup_error`] or
    /// [`Error::is_interrupt_error`] error to be raised from one of these
    /// nested invocations. In this case, the error will be propagated to the
    /// current invocation of [`Consumer::poll`](crate::consumer::Consumer::poll)
    /// in which this callback is being executed. This means it is not necessary
    /// to catch these errors and re-attempt to wakeup or interrupt the consumer
    /// task.
    ///
    /// # Arguments
    ///
    /// - `partitions` - The list of partitions that were assigned to the consumer and now have been
    ///   reassigned to other consumers. With both, the Classic and Consumer protocols, this will
    ///   always include all partitions that were previously assigned to the consumer.
    ///
    /// # Errors
    ///
    /// - `Error::is_wakeup_error` If raised from a nested call to `KafkaConsumer`
    /// - `Error::is_interrupt_error` If raised from a nested call to `KafkaConsumer`
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsLost")]
    async fn on_partitions_lost(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        self.on_partitions_revoked(partitions).await
    }
}
