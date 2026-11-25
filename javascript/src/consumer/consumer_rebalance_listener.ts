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

// Java's ConsumerRebalanceListener, an interface the user implements
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family): each
// method takes one object of Java's parameter names, and Java's default
// `onPartitionsLost` is an optional member, whose body the binding runs from
// src/internal/consumer_rebalance_listener_defaults.ts. This module holds only
// its type.

import type { TopicPartitionSet } from '../common/topic_partition_set';

/**
 * A callback interface that the user can implement to trigger custom actions
 * when the set of partitions assigned to the consumer changes.
 *
 * This is applicable when the consumer is having Kafka auto-manage group
 * membership. If the consumer directly assigns partitions, those partitions
 * will never be reassigned and this callback is not applicable.
 *
 * When Kafka is managing the group membership, a partition re-assignment will
 * be triggered any time the members of the group change or the subscription of
 * the members changes. This can occur when processes die, new process
 * instances are added or old instances come back to life after failure.
 * Partition re-assignments can also be triggered by changes affecting the
 * subscribed topics (e.g. when the number of partitions is administratively
 * adjusted).
 *
 * There are many uses for this functionality. One common use is saving
 * offsets in a custom store. By saving offsets in the `onPartitionsRevoked`
 * call we can ensure that any time partition assignment changes the offset
 * gets saved.
 *
 * This callback will only execute in the user thread as part of the
 * `Consumer.poll` call whenever partition assignment changes.
 *
 * Under normal conditions, if a partition is reassigned from one consumer to
 * another, then the old consumer will always invoke `onPartitionsRevoked` for
 * that partition prior to the new consumer invoking `onPartitionsAssigned` for
 * the same partition.
 *
 * You can think of revocation as a graceful way to give up ownership of a
 * partition. In some cases, the consumer may not have an opportunity to do so.
 * For example, if the session times out, then the partitions may be reassigned
 * before we have a chance to revoke them gracefully. For this case, we have a
 * third callback `onPartitionsLost`. The difference between this function and
 * `onPartitionsRevoked` is that upon invocation of `onPartitionsLost`, the
 * partitions may already be owned by some other members in the group and
 * therefore users would not be able to commit its consumed offsets for example
 * (by default, `onPartitionsLost` will be calling `onPartitionsRevoked`
 * directly).
 *
 * If the callback function implementation itself throws an error, this error
 * will be propagated to the current invocation of `KafkaConsumer.poll`. Note
 * that callbacks only serve as notification of an assignment change. They
 * cannot be used to express acceptance of the change.
 *
 * @remarks Each method takes one object, named as Java names its parameter,
 * and receives the partitions as a `TopicPartitionSet`. *(deviation)* A method
 * may return a Promise, which the consumer awaits before the rebalance goes on,
 * as JavaScript cannot block; a rejection is handled as a throw.
 */
export interface ConsumerRebalanceListener {
  /**
   * A callback method the user can implement to provide handling of offset
   * commits to a customized store. This method will be called during a
   * rebalance operation when the consumer has to give up some partitions: if
   * the consumer assignment changes, if the consumer is being closed, or if
   * the consumer is unsubscribing. It is recommended that offsets should be
   * committed in this callback to either Kafka or a custom offset store to
   * prevent duplicate data.
   *
   * This callback is always called before re-assigning the partitions. If the
   * consumer is using the `GroupProtocol.CONSUMER` rebalance protocol, this
   * callback will be called with the set of partitions to revoke iff the set
   * is non-empty.
   *
   * @param options - the partitions: the list of partitions that were
   *   assigned to the consumer and now need to be revoked
   * @returns anything: a Promise is awaited
   * @throws `WakeupError` If raised from a nested call to `KafkaConsumer`
   */
  onPartitionsRevoked(options: {
    /** The list of partitions that were assigned to the consumer and now need to be revoked. */
    partitions: TopicPartitionSet;
  }): unknown;

  /**
   * A callback method the user can implement to provide handling of
   * customized offsets on completion of a successful partition re-assignment.
   * This method will be called after the partition re-assignment completes
   * (even if no new partitions were assigned to the consumer), and before the
   * consumer starts fetching data, and only as the result of a `Consumer.poll`
   * call.
   *
   * It is guaranteed that under normal conditions all the processes in a
   * consumer group will execute their `onPartitionsRevoked` callback before
   * any instance executes this onPartitionsAssigned callback.
   *
   * @param options - the partitions: partitions that have been added to the
   *   assignment as a result of the rebalance
   * @returns anything: a Promise is awaited
   * @throws `WakeupError` If raised from a nested call to `KafkaConsumer`
   */
  onPartitionsAssigned(options: {
    /**
     * Partitions that have been added to the assignment as a result of the
     * rebalance. Partitions that were already owned by this consumer and
     * remain assigned are not included under the Consumer protocol.
     */
    partitions: TopicPartitionSet;
  }): unknown;

  /**
   * A callback method you can implement to provide handling of cleaning up
   * resources for partitions that have already been reassigned to other
   * consumers. This method will not be called during normal execution as the
   * owned partitions would first be revoked by calling the
   * `onPartitionsRevoked`, before being reassigned to other consumers during a
   * rebalance event. However, during exceptional scenarios when the consumer
   * realized that it does not own this partition any longer, i.e. not revoked
   * via a normal rebalance event, then this method would be invoked.
   *
   * For example, this function is called if a consumer's session timeout has
   * expired, or if a fatal error has been received indicating the consumer is
   * no longer part of the group.
   *
   * By default it will just trigger `onPartitionsRevoked`; for users who want
   * to distinguish the handling logic of revoked partitions v.s. lost
   * partitions, they can override the default implementation.
   *
   * @param options - the partitions: the list of partitions that were assigned
   *   to the consumer and now have been reassigned to other consumers
   * @returns anything: a Promise is awaited
   * @throws `WakeupError` If raised from a nested call to `KafkaConsumer`
   *
   * @remarks Optional, as Java's default method: when the listener lacks it,
   * the consumer calls the listener's `onPartitionsRevoked`.
   */
  onPartitionsLost?(options: {
    /**
     * The list of partitions that were assigned to the consumer and now have
     * been reassigned to other consumers.
     */
    partitions: TopicPartitionSet;
  }): unknown;
}
