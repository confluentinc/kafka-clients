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

// Java's Consumer interface, with the members KafkaConsumer translates
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family). Java's
// Consumer has no static member, so it has no runtime object. The members it
// leaves out are listed in the consumer package's documentation (index.ts).

import type { Metric } from '../common/metric';
import type { MetricNameMap } from '../common/metric_name_map';
import type { PartitionInfo } from '../common/partition_info';
import type { TopicPartition } from '../common/topic_partition';
import type { TopicPartitionMap } from '../common/topic_partition_map';
import type { TopicPartitionSet } from '../common/topic_partition_set';
import type { Duration } from '../duration';
import type { CloseOptions } from './close_options';
import type { ConsumerGroupMetadata } from './consumer_group_metadata';
import type { ConsumerRebalanceListener } from './consumer_rebalance_listener';
import type { ConsumerRecords } from './consumer_records';
import type { OffsetCommitCallback } from './offset_commit_callback';
import type { OffsetAndMetadata } from './offset_and_metadata';
import type { OffsetAndTimestamp } from './offset_and_timestamp';
import type { SubscriptionPattern } from './subscription_pattern';

/**
 * The consumer interface: the methods of `KafkaConsumer`.
 *
 * @typeParam K - the type of the keys the consumer returns
 * @typeParam V - the type of the values the consumer returns
 *
 * @see KafkaConsumer
 */
export interface Consumer<K = Buffer, V = Buffer> {
  /**
   * Get the set of partitions currently assigned to this consumer. If
   * subscription happened by directly assigning partitions using `assign`
   * then this will simply return the same partitions that were assigned. If
   * topic subscription was used, then this will give the set of topic
   * partitions currently assigned to the consumer (which may be none if the
   * assignment hasn't happened yet, or the partitions are in the process of
   * getting reassigned).
   *
   * The returned set is a snapshot of the current assignment at the time of
   * the call. It will not be updated if the assignment changes afterward.
   *
   * @returns A snapshot of the set of partitions currently assigned to this
   *   consumer
   * @throws `IllegalStateError` when the consumer is closed, the core's error
   * @throws `ConcurrentModificationError` while another call is in progress
   *
   * @remarks A new `TopicPartitionSet` the caller may change, where Java's set
   * is immutable.
   */
  assignment(): TopicPartitionSet;

  /**
   * Get the current subscription. Will return the same topics used in the
   * most recent call to `subscribe`, or an empty set if no such call has been
   * made.
   *
   * The returned set is a snapshot of the current subscription at the time of
   * the call. It will not be updated if the subscription changes afterward.
   *
   * @returns A snapshot of the set of topics currently subscribed to
   * @throws `IllegalStateError` when the consumer is closed, the core's error
   * @throws `ConcurrentModificationError` while another call is in progress
   */
  subscription(): Set<string>;

  /**
   * Subscribe to the given list of topics to get dynamically assigned
   * partitions. **Topic subscriptions are not incremental. This list will
   * replace the current assignment (if there is one).** It is not possible to
   * combine topic subscription with group management with manual partition
   * assignment through `assign`.
   *
   * If the given list of topics is empty, it is treated the same as
   * `unsubscribe()`.
   *
   * This is a short-hand for `subscribe({ topics, callback })`, which uses a
   * no-op listener. If you need the ability to seek to particular offsets,
   * you should prefer `subscribe({ topics, callback })`, since group
   * rebalances will cause partition offsets to be reset. You should also
   * provide your own listener if you are doing your own offset management
   * since the listener gives you an opportunity to commit offsets before a
   * rebalance finishes.
   *
   * @param options - the list of topics to subscribe to
   * @returns settling once the subscription is set
   * @throws `IllegalArgumentError` rejects with it if topics is null or
   *   contains null or empty elements
   * @throws `IllegalStateError` rejects with it if `subscribe()` is called
   *   previously with pattern, or assign is called previously (without a
   *   subsequent call to `unsubscribe()`)
   * @throws `TypeError` rejects with it if `topics` is not an iterable of
   *   strings (a string included)
   *
   * @remarks Java's blocking call returns a Promise here.
   * *(deviation: a null collection is refused before the closed, busy and group
   * checks, with its null error where Java reports those first. Why: the FFI
   * refuses the NULL at its boundary. Goes when: the FFI refuses it after the
   * core's checks.)*
   */
  subscribe(options: {
    /** The list of topics to subscribe to. */
    topics: Iterable<string>;
    pattern?: never;
    callback?: never;
  }): Promise<void>;
  /**
   * Subscribe to the given list of topics: the topics on their own, the same
   * as `subscribe({ topics })`.
   *
   * @param topics - The list of topics to subscribe to
   * @returns settling once the subscription is set
   */
  subscribe(topics: Iterable<string>): Promise<void>;
  /**
   * Subscribe to the given list of topics to get dynamically assigned
   * partitions. **Topic subscriptions are not incremental. This list will
   * replace the current assignment (if there is one).** Note that it is not
   * possible to combine topic subscription with group management with manual
   * partition assignment through `assign`.
   *
   * If the given list of topics is empty, it is treated the same as
   * `unsubscribe()`.
   *
   * As part of group management, the group coordinator will keep track of the
   * list of consumers that belong to a particular group and will trigger a
   * rebalance operation if any one of the following events are triggered:
   *
   * - Number of partitions change for any of the subscribed topics
   * - A subscribed topic is created or deleted
   * - An existing member of the consumer group is shutdown or fails
   * - A new member is added to the consumer group
   *
   * When any of these events are triggered, the provided listener will be
   * invoked in this way:
   *
   * - `ConsumerRebalanceListener.onPartitionsRevoked` will be invoked with the
   *   partitions to revoke, before re-assigning those partitions to another
   *   consumer.
   * - `ConsumerRebalanceListener.onPartitionsAssigned` will be invoked when the
   *   rebalance completes (even if no new partitions are assigned to the
   *   consumer)
   *
   * Note that rebalances will only occur during an active call to `poll`, so
   * callbacks will also only be invoked during that time.
   *
   * The provided listener will immediately override any listener set in a
   * previous call to subscribe. It is guaranteed, however, that the partitions
   * revoked/assigned through this interface are from topics subscribed in this
   * call. See `ConsumerRebalanceListener` for more details.
   *
   * @param options - the list of topics to subscribe to, and the listener
   * @returns settling once the subscription is set
   * @throws `IllegalArgumentError` rejects with it if topics is null or
   *   contains null or empty elements, or if listener is null
   * @throws `IllegalStateError` rejects with it if `subscribe()` is called
   *   previously with pattern, or assign is called previously (without a
   *   subsequent call to `unsubscribe()`)
   * @throws `TypeError` rejects with it if the listener lacks
   *   `onPartitionsRevoked` or `onPartitionsAssigned`, or is no object
   *
   * @remarks Java's blocking call returns a Promise here. The listener's
   * methods run inside the waiting call that delivers them, as in Java, and
   * may return a Promise, which that call awaits.
   */
  subscribe(options: {
    /** The list of topics to subscribe to. */
    topics: Iterable<string>;
    /**
     * Non-null listener instance to get notifications on partition
     * assignment/revocation for the subscribed topics.
     */
    callback: ConsumerRebalanceListener;
    pattern?: never;
  }): Promise<void>;
  /**
   * Subscribe to all topics matching the specified pattern, to get
   * dynamically assigned partitions. The pattern matching will be done
   * periodically against all topics. This is only supported under the
   * CONSUMER group protocol (see `ConsumerConfig.GROUP_PROTOCOL_CONFIG`).
   *
   * If the provided pattern is not compatible with Google RE2/J, an
   * `InvalidRegularExpression` will be eventually thrown on a call to `poll`
   * following this call to subscribe.
   *
   * See `subscribe({ topics, callback })` for details on the use of the
   * `ConsumerRebalanceListener`. Generally, rebalances are triggered when
   * there is a change to the topics matching the provided pattern and when
   * consumer group membership changes. Group rebalances only take place
   * during an active call to `poll`.
   *
   * @param options - the pattern to subscribe to, and the listener
   * @returns settling once the subscription is set
   * @throws `IllegalArgumentError` rejects with it if pattern is null or
   *   empty, or if the listener is null
   * @throws `IllegalStateError` rejects with it if `subscribe()` is called
   *   previously with topics, or assign is called previously (without a
   *   subsequent call to `unsubscribe()`)
   * @throws `TypeError` rejects with it if the listener lacks
   *   `onPartitionsRevoked` or `onPartitionsAssigned`, or is no object
   *
   * @remarks Java's blocking call returns a Promise here. A `RegExp` is no
   * pattern: Java's `Pattern` forms are left out.
   */
  subscribe(options: {
    /** Pattern to subscribe to, that must be compatible with Google RE2/J. */
    pattern: SubscriptionPattern;
    /**
     * Non-null listener instance to get notifications on partition
     * assignment/revocation for the subscribed topics.
     */
    callback: ConsumerRebalanceListener;
    topics?: never;
  }): Promise<void>;
  /**
   * Subscribe to all topics matching the specified pattern, to get
   * dynamically assigned partitions. The pattern matching will be done
   * periodically against topics. This is only supported under the CONSUMER
   * group protocol (see `ConsumerConfig.GROUP_PROTOCOL_CONFIG`).
   *
   * If the provided pattern is not compatible with Google RE2/J, an
   * `InvalidRegularExpression` will be eventually thrown on a call to `poll`
   * following this call to subscribe.
   *
   * This is a short-hand for `subscribe({ pattern, callback })`, which uses a
   * no-op listener. If you need the ability to seek to particular offsets,
   * you should prefer `subscribe({ pattern, callback })`, since group
   * rebalances will cause partition offsets to be reset. You should also
   * provide your own listener if you are doing your own offset management
   * since the listener gives you an opportunity to commit offsets before a
   * rebalance finishes.
   *
   * @param options - the pattern to subscribe to
   * @returns settling once the subscription is set
   * @throws `IllegalArgumentError` rejects with it if pattern is null or empty
   * @throws `IllegalStateError` rejects with it if `subscribe()` is called
   *   previously with topics, or assign is called previously (without a
   *   subsequent call to `unsubscribe()`)
   *
   * @remarks Java's blocking call returns a Promise here. A `RegExp` is no
   * pattern: Java's `Pattern` forms are left out.
   * *(deviation: a null pattern is refused before the closed, busy and group
   * checks, with its null error where Java reports those first. Why: the FFI
   * refuses the NULL at its boundary. Goes when: the FFI refuses it after the
   * core's checks.)*
   */
  subscribe(options: {
    /** Pattern to subscribe to, that must be compatible with Google RE2/J. */
    pattern: SubscriptionPattern;
    topics?: never;
    callback?: never;
  }): Promise<void>;
  /**
   * Subscribe to all topics matching the specified pattern: the pattern on
   * its own, the same as `subscribe({ pattern })`.
   *
   * @param pattern - Pattern to subscribe to, that must be compatible with
   *   Google RE2/J.
   * @returns settling once the subscription is set
   */
  subscribe(pattern: SubscriptionPattern): Promise<void>;

  /**
   * Manually assign a list of partitions to this consumer. This interface
   * does not allow for incremental assignment and will replace the previous
   * assignment (if there is one).
   *
   * If the given list of topic partitions is empty, it is treated the same as
   * `unsubscribe()`.
   *
   * Manual topic assignment through this method does not use the consumer's
   * group management functionality. As such, there will be no rebalance
   * operation triggered when group membership or cluster and topic metadata
   * change. Note that it is not possible to use both manual partition
   * assignment with `assign` and group assignment with
   * `subscribe({ topics, callback })`.
   *
   * If auto-commit is enabled, an async commit (based on the old assignment)
   * will be triggered before the new assignment replaces the old one.
   *
   * @param options - The list of partitions to assign this consumer
   * @returns settling once the assignment is set
   * @throws `IllegalArgumentError` rejects with it if partitions is null or
   *   contains null or empty topics
   * @throws `IllegalStateError` rejects with it if `subscribe()` is called
   *   previously with topics or pattern (without a subsequent call to
   *   `unsubscribe()`)
   *
   * @remarks Java's blocking call returns a Promise here.
   * *(deviation: a null collection is refused before the closed and busy
   * checks, with its null error where Java reports those first. Why: the FFI
   * refuses the NULL at its boundary. Goes when: the FFI refuses it after the
   * core's checks.)*
   */
  assign(options: {
    /** The list of partitions to assign this consumer. */
    partitions: Iterable<TopicPartition>;
  }): Promise<void>;
  /**
   * Manually assign a list of partitions: the partitions on their own, the
   * same as `assign({ partitions })`.
   *
   * @param partitions - The list of partitions to assign this consumer
   * @returns settling once the assignment is set
   */
  assign(partitions: Iterable<TopicPartition>): Promise<void>;

  /**
   * Unsubscribe from topics currently subscribed with `subscribe`. This also
   * clears any partitions directly assigned through `assign`.
   *
   * **Note:** Unlike `close()`, this method does not commit the pending
   * offsets before unsubscribing, even if `enable.auto.commit` is enabled. To
   * avoid duplicate processing upon re-joining, it is recommended to
   * explicitly call `commitSync()` before invoking this method.
   *
   * @returns settling once the consumer is unsubscribed
   * @throws `KafkaError` rejects with it for any other unrecoverable errors
   *   (e.g. rebalance callback errors)
   *
   * @remarks Java's blocking call returns a Promise here.
   */
  unsubscribe(): Promise<void>;

  /**
   * Fetch data for the topics or partitions specified using one of the
   * subscribe/assign APIs. It is an error to not have subscribed to any
   * topics or partitions before polling for data.
   *
   * On each poll, consumer will try to use the last consumed offset as the
   * starting offset and fetch sequentially. The last consumed offset can be
   * manually set through `seek` or automatically set as the last committed
   * offset for the subscribed list of partitions.
   *
   * This method returns immediately if there are records available or if the
   * position advances past control records or aborted transactions when
   * isolation.level=read_committed. Otherwise, it will await the passed
   * timeout. If the timeout expires, an empty record set will be returned.
   * Note that this method may block beyond the timeout in order to execute
   * custom `ConsumerRebalanceListener` callbacks.
   *
   * @param options - The maximum time to block
   * @returns resolving to the records since the last fetch for the
   *   subscribed list of topics and partitions
   * @throws `InvalidOffsetError` rejects with it if the offset for a
   *   partition or set of partitions is undefined or out of range and no
   *   offset reset policy has been configured
   * @throws `WakeupError` rejects with it if `wakeup()` is called before or
   *   while this function is called
   * @throws `AuthenticationError` rejects with it if authentication fails
   * @throws `AuthorizationError` rejects with it if caller lacks Read access
   *   to any of the subscribed topics or to the configured groupId
   * @throws `BootstrapResolutionError` rejects with it if DNS resolution of
   *   the bootstrap servers fails within `bootstrap.resolve.timeout.ms`
   * @throws `KafkaError` rejects with it for any other unrecoverable errors
   *   (e.g. invalid groupId or session timeout, errors deserializing key/value
   *   pairs, your rebalance callback thrown errors, or any new error cases in
   *   future versions)
   * @throws `IllegalArgumentError` rejects with it if the timeout value is
   *   negative
   * @throws `IllegalStateError` rejects with it if the consumer is not
   *   subscribed to any topics or manually assigned any partitions to consume
   *   from
   * @throws `InvalidTopicError` rejects with it if the current subscription
   *   contains any invalid topic
   * @throws `UnsupportedVersionError` rejects with it if the consumer
   *   attempts to fetch stable offsets when the broker doesn't support this
   *   feature. Also, if the consumer attempts to subscribe to a
   *   `SubscriptionPattern` and the broker doesn't support this feature
   * @throws `FencedInstanceIdError` rejects with it if this consumer instance
   *   gets fenced by broker
   * @throws `ConcurrentModificationError` rejects with it while another call
   *   is in progress
   * @throws `RangeError` rejects with it if the timeout is `NaN`,
   *   `-Infinity`, holds years, months or weeks, or is beyond
   *   `Number.MAX_SAFE_INTEGER` milliseconds
   * @throws `TypeError` rejects with it if the timeout is null
   *
   * @remarks Java's blocking call returns a Promise here. A key or value from
   * `poll()` points into the memory the client fetched, and a value you keep
   * holds that whole fetch in memory. To keep a value for long, copy it with
   * `Buffer.from(value)`. Electron doesn't allow external buffers, so on
   * Electron the client copies each consumed key and value.
   */
  poll(options: {
    /** The maximum time to block (must not be greater than `Long.MAX_VALUE` milliseconds). */
    timeout: Duration;
  }): Promise<ConsumerRecords<K, V>>;
  /**
   * Fetch data for the topics or partitions specified: the timeout on its
   * own, the same as `poll({ timeout })`.
   *
   * @param timeout - The maximum time to block
   * @returns resolving to the records since the last fetch
   */
  poll(timeout: Duration): Promise<ConsumerRecords<K, V>>;

  /**
   * Commit offsets returned on the last `poll()` for all the subscribed list
   * of topics and partitions.
   *
   * This commits offsets only to Kafka. The offsets committed using this API
   * will be used on the first fetch after every rebalance and also on
   * startup. As such, if you need to store offsets in anything other than
   * Kafka, this API should not be used.
   *
   * This is a synchronous commit and will block until either the commit
   * succeeds, an unrecoverable error is encountered (in which case it is
   * thrown to the caller), or the timeout specified by
   * `default.api.timeout.ms` expires (in which case a `TimeoutError` is
   * thrown to the caller).
   *
   * Note that asynchronous offset commits sent previously with `commitAsync`
   * (or similar) are guaranteed to have their callbacks invoked prior to
   * completion of this method, but only when the consumer is using the
   * consumer group protocol.
   *
   * @returns settling once the commit succeeded
   * @throws `CommitFailedError` rejects with it if the commit failed and
   *   cannot be retried. This fatal error can only occur if you are using
   *   automatic group management with `subscribe`, or if there is an active
   *   group with the same `group.id` which is using group management. In such
   *   cases, when you are trying to commit to partitions that are no longer
   *   assigned to this consumer because the consumer is for example no longer
   *   part of the group this error would be thrown.
   * @throws `RebalanceInProgressError` rejects with it if the consumer
   *   instance is in the middle of a rebalance so it is not yet determined
   *   which partitions would be assigned to the consumer. In such cases you
   *   can first complete the rebalance by calling `poll` and commit can be
   *   reconsidered afterwards. NOTE when you reconsider committing after the
   *   rebalance, the assigned partitions may have changed, and also for those
   *   partitions that are still assigned their fetch positions may have
   *   changed too if more records are returned from the `poll` call.
   * @throws `WakeupError` rejects with it if `wakeup()` is called before or
   *   while this function is called
   * @throws `AuthenticationError` rejects with it if authentication fails
   * @throws `AuthorizationError` rejects with it if not authorized to the
   *   topic or to the configured groupId
   * @throws `KafkaError` rejects with it for any other unrecoverable errors
   *   (e.g. if offset metadata is too large or if the topic does not exist)
   * @throws `TimeoutError` rejects with it if the timeout specified by
   *   `default.api.timeout.ms` expires before successful completion of the
   *   offset commit
   * @throws `FencedInstanceIdError` rejects with it if this consumer is using
   *   the classic group protocol and this instance gets fenced by broker
   * @throws `BootstrapResolutionError` rejects with it if DNS resolution of
   *   the bootstrap servers fails within `bootstrap.resolve.timeout.ms`
   * @throws `InvalidGroupIdError` rejects with it if the consumer has no
   *   `group.id`
   *
   * @remarks Java's blocking call returns a Promise here, as `commitAsync()`
   * does: only this one waits for the broker's answer.
   */
  commitSync(): Promise<void>;
  /**
   * Commit offsets returned on the last `poll()` for all the subscribed list
   * of topics and partitions.
   *
   * This commits offsets only to Kafka. The offsets committed using this API
   * will be used on the first fetch after every rebalance and also on
   * startup. As such, if you need to store offsets in anything other than
   * Kafka, this API should not be used.
   *
   * This is a synchronous commit and will block until either the commit
   * succeeds, an unrecoverable error is encountered (in which case it is
   * thrown to the caller), or the passed timeout expires.
   *
   * Note that asynchronous offset commits sent previously with `commitAsync`
   * (or similar) are guaranteed to have their callbacks invoked prior to
   * completion of this method, but only when the consumer is using the
   * consumer group protocol.
   *
   * @param options - the maximum amount of time to await completion of the
   *   offset commit
   * @returns settling once the commit succeeded
   * @throws `CommitFailedError` rejects with it if the commit failed and
   *   cannot be retried. This can only occur if you are using automatic group
   *   management with `subscribe`, or if there is an active group with the
   *   same `group.id` which is using group management. In such cases, when
   *   you are trying to commit to partitions that are no longer assigned to
   *   this consumer because the consumer is for example no longer part of the
   *   group this error would be thrown.
   * @throws `RebalanceInProgressError` rejects with it if the consumer
   *   instance is in the middle of a rebalance so it is not yet determined
   *   which partitions would be assigned to the consumer. In such cases you
   *   can first complete the rebalance by calling `poll` and commit can be
   *   reconsidered afterwards. NOTE when you reconsider committing after the
   *   rebalance, the assigned partitions may have changed, and also for those
   *   partitions that are still assigned their fetch positions may have
   *   changed too if more records are returned from the `poll` call.
   * @throws `WakeupError` rejects with it if `wakeup()` is called before or
   *   while this function is called
   * @throws `AuthenticationError` rejects with it if authentication fails
   * @throws `AuthorizationError` rejects with it if not authorized to the
   *   topic or to the configured groupId
   * @throws `KafkaError` rejects with it for any other unrecoverable errors
   *   (e.g. if offset metadata is too large or if the topic does not exist)
   * @throws `TimeoutError` rejects with it if the timeout expires before
   *   successful completion of the offset commit
   * @throws `FencedInstanceIdError` rejects with it if this consumer is using
   *   the classic group protocol and this instance gets fenced by broker
   * @throws `IllegalArgumentError` rejects with it if the timeout value is
   *   negative
   * @throws `RangeError` rejects with it if the timeout is `NaN`,
   *   `-Infinity`, holds years, months or weeks, or is beyond
   *   `Number.MAX_SAFE_INTEGER` milliseconds
   * @throws `TypeError` rejects with it if the timeout is null
   *
   * @remarks Java's blocking call returns a Promise here.
   * *(deviation: a negative timeout is refused before the closed and group
   * checks, which Java runs first. Why: the FFI checks it first. Goes when:
   * it checks it after them.)*
   */
  commitSync(options: {
    /** The maximum amount of time to await completion of the offset commit. */
    timeout: Duration;
    offsets?: never;
  }): Promise<void>;
  /**
   * Commit offsets returned on the last `poll()`, waiting up to the timeout:
   * the timeout on its own, the same as `commitSync({ timeout })`.
   *
   * @param timeout - The maximum amount of time to await completion of the
   *   offset commit
   * @returns settling once the commit succeeded
   */
  commitSync(timeout: Duration): Promise<void>;
  /**
   * Commit the specified offsets for the specified list of topics and
   * partitions.
   *
   * This commits offsets to Kafka. The offsets committed using this API will
   * be used on the first fetch after every rebalance and also on startup. As
   * such, if you need to store offsets in anything other than Kafka, this API
   * should not be used. The committed offset should be the next message your
   * application will consume, i.e. `nextRecordToBeProcessed.offset` (or
   * `ConsumerRecords.nextOffsets()`). You should also add the leader epoch as
   * commit metadata, which can be obtained from `ConsumerRecord.leaderEpoch`
   * or `ConsumerRecords.nextOffsets()`. If automatic group management with
   * `subscribe` is used, then the committed offsets must belong to the
   * currently auto-assigned partitions.
   *
   * This is a synchronous commit and will block until either the commit
   * succeeds or an unrecoverable error is encountered (in which case it is
   * thrown to the caller), or the timeout specified by
   * `default.api.timeout.ms` expires (in which case a `TimeoutError` is
   * thrown to the caller).
   *
   * Note that asynchronous offset commits sent previously with `commitAsync`
   * (or similar) are guaranteed to have their callbacks invoked prior to
   * completion of this method, but only when the consumer is using the
   * consumer group protocol.
   *
   * @param options - A map of offsets by partition with associated metadata.
   *   This map will be copied internally, so it is safe to mutate the map
   *   after returning.
   * @returns settling once the commit succeeded
   * @throws `CommitFailedError` rejects with it if the commit failed and
   *   cannot be retried. This can only occur if you are using automatic group
   *   management with `subscribe`, or if there is an active group with the
   *   same `group.id` which is using group management. In such cases, when
   *   you are trying to commit to partitions that are no longer assigned to
   *   this consumer because the consumer is for example no longer part of the
   *   group this error would be thrown.
   * @throws `RebalanceInProgressError` rejects with it if the consumer
   *   instance is in the middle of a rebalance so it is not yet determined
   *   which partitions would be assigned to the consumer. In such cases you
   *   can first complete the rebalance by calling `poll` and commit can be
   *   reconsidered afterwards. NOTE when you reconsider committing after the
   *   rebalance, the assigned partitions may have changed, and also for those
   *   partitions that are still assigned their fetch positions may have
   *   changed too if more records are returned from the `poll` call, so
   *   when you retry committing you should consider updating the passed in
   *   offsets
   * @throws `WakeupError` rejects with it if `wakeup()` is called before or
   *   while this function is called
   * @throws `AuthenticationError` rejects with it if authentication fails
   * @throws `AuthorizationError` rejects with it if not authorized to the
   *   topic or to the configured groupId
   * @throws `IllegalArgumentError` rejects with it if the committed offset is
   *   negative
   * @throws `KafkaError` rejects with it for any other unrecoverable errors
   *   (e.g. if offset metadata is too large or if the topic does not exist)
   * @throws `TimeoutError` rejects with it if the timeout expires before
   *   successful completion of the offset commit
   * @throws `FencedInstanceIdError` rejects with it if this consumer is using
   *   the classic group protocol and this instance gets fenced by broker
   * @throws `TypeError` rejects with it if `offsets` is null, holds a null
   *   key or value, or is not a `TopicPartitionMap` (a `Map` included)
   *
   * @remarks Java's blocking call returns a Promise here. Only a
   * `TopicPartitionMap` is taken: a `Map` keyed by `TopicPartition` compares
   * its keys by identity.
   */
  commitSync(options: {
    /** A map of offsets by partition with associated metadata, copied by the call. */
    offsets: TopicPartitionMap<OffsetAndMetadata>;
    timeout?: never;
  }): Promise<void>;
  /**
   * Commit the specified offsets: the offsets on their own, the same as
   * `commitSync({ offsets })`.
   *
   * @param offsets - A map of offsets by partition with associated metadata
   * @returns settling once the commit succeeded
   */
  commitSync(offsets: TopicPartitionMap<OffsetAndMetadata>): Promise<void>;
  /**
   * Commit the specified offsets for the specified list of topics and
   * partitions.
   *
   * This commits offsets to Kafka. The offsets committed using this API will
   * be used on the first fetch after every rebalance and also on startup. As
   * such, if you need to store offsets in anything other than Kafka, this API
   * should not be used. The committed offset should be the next message your
   * application will consume, i.e. `nextRecordToBeProcessed.offset` (or
   * `ConsumerRecords.nextOffsets()`). You should also add the leader epoch as
   * commit metadata, which can be obtained from `ConsumerRecord.leaderEpoch`
   * or `ConsumerRecords.nextOffsets()`. If automatic group management with
   * `subscribe` is used, then the committed offsets must belong to the
   * currently auto-assigned partitions.
   *
   * This is a synchronous commit and will block until either the commit
   * succeeds, an unrecoverable error is encountered (in which case it is
   * thrown to the caller), or the timeout expires.
   *
   * Note that asynchronous offset commits sent previously with `commitAsync`
   * (or similar) are guaranteed to have their callbacks invoked prior to
   * completion of this method, but only when the consumer is using the
   * consumer group protocol.
   *
   * @param options - A map of offsets by partition with associated metadata,
   *   copied internally, and the maximum amount of time to await completion
   *   of the offset commit
   * @returns settling once the commit succeeded
   * @throws `CommitFailedError` rejects with it if the commit failed and
   *   cannot be retried. This can only occur if you are using automatic group
   *   management with `subscribe`, or if there is an active group with the
   *   same `group.id` which is using group management. In such cases, when
   *   you are trying to commit to partitions that are no longer assigned to
   *   this consumer because the consumer is for example no longer part of the
   *   group this error would be thrown.
   * @throws `RebalanceInProgressError` rejects with it if the consumer
   *   instance is in the middle of a rebalance so it is not yet determined
   *   which partitions would be assigned to the consumer. In such cases you
   *   can first complete the rebalance by calling `poll` and commit can be
   *   reconsidered afterwards. NOTE when you reconsider committing after the
   *   rebalance, the assigned partitions may have changed, and also for those
   *   partitions that are still assigned their fetch positions may have
   *   changed too if more records are returned from the `poll` call, so
   *   when you retry committing you should consider updating the passed in
   *   offsets
   * @throws `WakeupError` rejects with it if `wakeup()` is called before or
   *   while this function is called
   * @throws `AuthenticationError` rejects with it if authentication fails
   * @throws `AuthorizationError` rejects with it if not authorized to the
   *   topic or to the configured groupId
   * @throws `IllegalArgumentError` rejects with it if the committed offset or
   *   the timeout is negative
   * @throws `KafkaError` rejects with it for any other unrecoverable errors
   *   (e.g. if offset metadata is too large or if the topic does not exist)
   * @throws `TimeoutError` rejects with it if the timeout expires before
   *   successful completion of the offset commit
   * @throws `FencedInstanceIdError` rejects with it if this consumer is using
   *   the classic group protocol and this instance gets fenced by broker
   * @throws `RangeError` rejects with it if the timeout is `NaN`,
   *   `-Infinity`, holds years, months or weeks, or is beyond
   *   `Number.MAX_SAFE_INTEGER` milliseconds
   * @throws `TypeError` rejects with it if the timeout or `offsets` is null,
   *   or `offsets` is not a `TopicPartitionMap`
   *
   * @remarks Java's blocking call returns a Promise here.
   * *(deviation: a negative timeout is refused before the closed and group
   * checks, which Java runs first. Why: the FFI checks it first. Goes when:
   * it checks it after them.)*
   */
  commitSync(options: {
    /** A map of offsets by partition with associated metadata, copied by the call. */
    offsets: TopicPartitionMap<OffsetAndMetadata>;
    /** The maximum amount of time to await completion of the offset commit. */
    timeout: Duration;
  }): Promise<void>;

  /**
   * Commit offsets returned on the last `poll()` for all the subscribed list
   * of topics and partition. Same as `commitAsync(null)`.
   *
   * @returns settling once the core took the commit, not when the broker
   *   answered
   * @throws `InvalidGroupIdError` rejects with it if the consumer has no
   *   `group.id`
   * @throws `FencedInstanceIdError` rejects with it if this consumer is using
   *   the classic group protocol and this instance gets fenced by broker
   *
   * @remarks Only `commitSync()` waits for the broker.
   */
  commitAsync(): Promise<void>;
  /**
   * Commit offsets returned on the last `poll()` for the subscribed list of
   * topics and partitions.
   *
   * This commits offsets only to Kafka. The offsets committed using this API
   * will be used on the first fetch after every rebalance and also on startup.
   * As such, if you need to store offsets in anything other than Kafka, this
   * API should not be used.
   *
   * This is an asynchronous call and will not block. Any errors encountered
   * are either passed to the callback (if provided) or discarded.
   *
   * Offsets committed through multiple calls to this API are guaranteed to be
   * sent in the same order as the invocations. Corresponding commit callbacks
   * are also invoked in the same order. Additionally note that offsets
   * committed through this API are guaranteed to complete before a subsequent
   * call to `commitSync()` (and variants) returns.
   *
   * @param options - the callback to invoke when the commit completes, or
   *   null for none, the same as `commitAsync()`
   * @returns settling once the core took the commit, not when the broker
   *   answered
   * @throws `InvalidGroupIdError` rejects with it if the consumer has no
   *   `group.id`
   * @throws `FencedInstanceIdError` rejects with it if this consumer is using
   *   the classic group protocol and this instance gets fenced by broker
   * @throws `TypeError` rejects with it if the callback is no function
   *
   * @remarks Java's `void` call returns a Promise here. The callback runs
   * inside a later waiting call, such as `poll()`, `commitSync()` or
   * `close()`, and may return a Promise, which that call awaits. On a
   * `KafkaConsumer` it gets `null` offsets for a failed commit, as Java's does.
   */
  commitAsync(options: {
    /** Callback to invoke when the commit completes. */
    callback: OffsetCommitCallback | null;
    offsets?: never;
  }): Promise<void>;
  /**
   * Commit offsets returned on the last `poll()`: the callback on its own, the
   * same as `commitAsync({ callback })`.
   *
   * @param callback - Callback to invoke when the commit completes
   * @returns settling once the core took the commit
   */
  commitAsync(callback: OffsetCommitCallback | null): Promise<void>;
  /**
   * Commit the specified offsets for the specified list of topics and
   * partitions to Kafka.
   *
   * This commits offsets to Kafka. The offsets committed using this API will
   * be used on the first fetch after every rebalance and also on startup. As
   * such, if you need to store offsets in anything other than Kafka, this API
   * should not be used. The committed offset should be the next message your
   * application will consume, i.e. `nextRecordToBeProcessed.offset` (or
   * `ConsumerRecords.nextOffsets()`). You should also add the leader epoch as
   * commit metadata, which can be obtained from `ConsumerRecord.leaderEpoch`
   * or `ConsumerRecords.nextOffsets()`. If automatic group management with
   * `subscribe` is used, then the committed offsets must belong to the
   * currently auto-assigned partitions.
   *
   * This is an asynchronous call and will not block. Any errors encountered
   * are either passed to the callback (if provided) or discarded.
   *
   * Offsets committed through multiple calls to this API are guaranteed to be
   * sent in the same order as the invocations. Corresponding commit callbacks
   * are also invoked in the same order. Additionally note that offsets
   * committed through this API are guaranteed to complete before a subsequent
   * call to `commitSync()` (and variants) returns.
   *
   * @param options - the offsets to commit, copied by the call, and the
   *   callback to invoke when the commit completes
   * @returns settling once the core took the commit, not when the broker
   *   answered
   * @throws `InvalidGroupIdError` rejects with it if the consumer has no
   *   `group.id`
   * @throws `FencedInstanceIdError` rejects with it if this consumer is using
   *   the classic group protocol and this instance gets fenced by broker
   * @throws `TypeError` rejects with it if `offsets` is null or not a
   *   `TopicPartitionMap`, or the callback is null or no function
   *
   * @remarks Java's `void` call returns a Promise here. The callback runs as
   * `commitAsync({ callback })`'s does; on a `KafkaConsumer`, an empty map
   * committed gives it `null` offsets, as Java's does.
   * *(deviation: a null callback is refused, where Java's commits without one.
   * Why: the FFI refuses a NULL callback. Goes when: it takes one.)*
   */
  commitAsync(options: {
    /** A map of offsets by partition with associated metadata, copied by the call. */
    offsets: TopicPartitionMap<OffsetAndMetadata>;
    /** Callback to invoke when the commit completes. */
    callback: OffsetCommitCallback;
  }): Promise<void>;

  /**
   * Overrides the fetch offsets that the consumer will use on the next
   * `poll(timeout)`. If this API is invoked for the same partition more than
   * once, the latest offset will be used on the next poll(). Note that you may
   * lose data if this API is arbitrarily used in the middle of consumption, to
   * reset the fetch offsets.
   *
   * The next Consumer Record which will be retrieved when poll() is invoked
   * will have the offset specified, given that a record with that offset
   * exists (i.e. it is a valid offset).
   *
   * `seekToBeginning` will go to the first offset in the topic. seek(0) is
   * equivalent to seekToBeginning for a TopicPartition with beginning offset
   * 0, assuming that there is a record at offset 0 still available.
   * `seekToEnd` is equivalent to seeking to the last offset of the partition,
   * but behavior depends on `isolation.level`, so see `seekToEnd`
   * documentation for more details.
   *
   * Seeking to the offset smaller than the log start offset or larger than
   * the log end offset means an invalid offset is reached. Invalid offset
   * behaviour is controlled by the `auto.offset.reset` property. If this is
   * set to "earliest", the next poll will return records from the starting
   * offset. If it is set to "latest", it will seek to the last offset
   * (similar to seekToEnd()). If it is set to "none", an
   * `OffsetOutOfRangeError` will be thrown.
   *
   * Note that, the seek offset won't change to the in-flight fetch request,
   * it will take effect in next fetch request. So, the consumer might wait
   * for `fetch.max.wait.ms` before starting to fetch the records from desired
   * offset.
   *
   * @param options - the TopicPartition on which the seek will be performed,
   *   and the next offset returned by poll()
   * @returns settling once the offset is set
   * @throws `IllegalArgumentError` rejects with it if the provided offset is
   *   negative
   * @throws `IllegalStateError` rejects with it if the provided
   *   TopicPartition is not assigned to this consumer
   *
   * @remarks Java's blocking call returns a Promise here.
   * *(deviation: the busy check comes before the negative offset's, and a
   * null partition gets the FFI's `TypeError`, where Java checks the offset
   * first. Why: the binding repeats no precondition, and the FFI checks the
   * busy consumer and the NULL first. Goes when: the FFI checks in Java's
   * order.)*
   */
  seek(options: {
    /** The TopicPartition on which the seek will be performed. */
    partition: TopicPartition;
    /** The next offset returned by poll(): a bigint, or a whole number up to 2^53 - 1. */
    offset: bigint | number;
    offsetAndMetadata?: never;
  }): Promise<void>;
  /**
   * Overrides the fetch offsets that the consumer will use on the next
   * `poll(timeout)`. If this API is invoked for the same partition more than
   * once, the latest offset will be used on the next poll(). Note that you may
   * lose data if this API is arbitrarily used in the middle of consumption, to
   * reset the fetch offsets. This method allows for setting the leaderEpoch
   * along with the desired offset.
   *
   * @param options - the TopicPartition on which the seek will be performed,
   *   and the offset with its leader epoch
   * @returns settling once the offset is set
   * @throws `IllegalArgumentError` rejects with it if the provided offset is
   *   negative
   * @throws `IllegalStateError` rejects with it if the provided
   *   TopicPartition is not assigned to this consumer
   *
   * @remarks Java's blocking call returns a Promise here. A null
   * `offsetAndMetadata` or partition gets the FFI's `TypeError`.
   */
  seek(options: {
    /** The TopicPartition on which the seek will be performed. */
    partition: TopicPartition;
    /** The next offset returned by poll(), with its leader epoch. */
    offsetAndMetadata: OffsetAndMetadata;
    offset?: never;
  }): Promise<void>;

  /**
   * Seek to the first offset for each of the given partitions. This function
   * evaluates lazily, seeking to the first offset in all partitions only when
   * `poll` or `position` are called. If no partitions are provided, seek to
   * the first offset for all of the currently assigned partitions.
   *
   * @param options - the partitions to seek
   * @returns settling once the seek is recorded
   * @throws `IllegalArgumentError` rejects with it if `partitions` is `null`
   * @throws `IllegalStateError` rejects with it if any of the provided
   *   partitions are not currently assigned to this consumer
   *
   * @remarks Java's blocking call returns a Promise here.
   */
  seekToBeginning(options: {
    /** The partitions to seek to their first offset. */
    partitions: Iterable<TopicPartition>;
  }): Promise<void>;
  /**
   * Seek to the first offset for each of the given partitions: the partitions
   * on their own, the same as `seekToBeginning({ partitions })`.
   *
   * @param partitions - the partitions to seek
   * @returns settling once the seek is recorded
   */
  seekToBeginning(partitions: Iterable<TopicPartition>): Promise<void>;

  /**
   * Seek to the last offset for each of the given partitions. This function
   * evaluates lazily, seeking to the final offset in all partitions only when
   * `poll` or `position` are called. If no partitions are provided, seek to
   * the final offset for all of the currently assigned partitions.
   *
   * If `isolation.level=read_committed`, the end offset will be the Last
   * Stable Offset, i.e., the offset of the first message with an open
   * transaction.
   *
   * @param options - the partitions to seek
   * @returns settling once the seek is recorded
   * @throws `IllegalArgumentError` rejects with it if `partitions` is `null`
   * @throws `IllegalStateError` rejects with it if any of the provided
   *   partitions are not currently assigned to this consumer
   *
   * @remarks Java's blocking call returns a Promise here.
   */
  seekToEnd(options: {
    /** The partitions to seek to their last offset. */
    partitions: Iterable<TopicPartition>;
  }): Promise<void>;
  /**
   * Seek to the last offset for each of the given partitions: the partitions
   * on their own, the same as `seekToEnd({ partitions })`.
   *
   * @param partitions - the partitions to seek
   * @returns settling once the seek is recorded
   */
  seekToEnd(partitions: Iterable<TopicPartition>): Promise<void>;

  /**
   * Get the offset of the *next record* that will be fetched (if a record
   * with that offset exists). This method may issue a remote call to the
   * server if there is no current position for the given partition.
   *
   * This call will block until either the position could be determined or an
   * unrecoverable error is encountered (in which case it is thrown to the
   * caller), or the timeout specified by `default.api.timeout.ms` expires (in
   * which case a `TimeoutError` is thrown to the caller).
   *
   * @param options - The partition to get the position for
   * @returns resolving to the current position of the consumer (that is, the
   *   offset of the next record to be fetched), a bigint
   * @throws `IllegalStateError` rejects with it if the provided
   *   TopicPartition is not assigned to this consumer
   * @throws `InvalidOffsetError` rejects with it if no offset is currently
   *   defined for the partition
   * @throws `WakeupError` rejects with it if `wakeup()` is called before or
   *   while this function is called
   * @throws `AuthenticationError` rejects with it if authentication fails
   * @throws `AuthorizationError` rejects with it if not authorized to the
   *   topic or to the configured groupId
   * @throws `UnsupportedVersionError` rejects with it if the consumer
   *   attempts to fetch stable offsets when the broker doesn't support this
   *   feature
   * @throws `KafkaError` rejects with it for any other unrecoverable errors
   * @throws `TimeoutError` rejects with it if the position cannot be
   *   determined before the timeout specified by `default.api.timeout.ms`
   *   expires
   *
   * @remarks Java's blocking call returns a Promise here.
   * *(deviation: a null partition, or one with a null topic, gets the FFI's
   * `TypeError` before the closed and busy checks, where Java reports those
   * and then "You can only check the position for partitions assigned to
   * this consumer.". Why: the FFI refuses the NULL at its boundary. Goes
   * when: the FFI refuses it after the core's checks.)*
   */
  position(options: {
    /** The partition to get the position for. */
    partition: TopicPartition;
    timeout?: never;
  }): Promise<bigint>;
  /**
   * Get the offset of the next record that will be fetched: the partition on
   * its own, the same as `position({ partition })`.
   *
   * @param partition - The partition to get the position for
   * @returns resolving to the current position of the consumer
   */
  position(partition: TopicPartition): Promise<bigint>;
  /**
   * Get the offset of the *next record* that will be fetched (if a record
   * with that offset exists). This method may issue a remote call to the
   * server if there is no current position for the given partition.
   *
   * This call will block until the position can be determined, an
   * unrecoverable error is encountered (in which case it is thrown to the
   * caller), or the timeout expires.
   *
   * @param options - The partition to get the position for, and the maximum
   *   amount of time to await determination of the current position
   * @returns resolving to the current position of the consumer (that is, the
   *   offset of the next record to be fetched), a bigint
   * @throws `IllegalStateError` rejects with it if the provided
   *   TopicPartition is not assigned to this consumer
   * @throws `InvalidOffsetError` rejects with it if no offset is currently
   *   defined for the partition
   * @throws `WakeupError` rejects with it if `wakeup()` is called before or
   *   while this function is called
   * @throws `TimeoutError` rejects with it if the position cannot be
   *   determined before the passed timeout expires
   * @throws `AuthenticationError` rejects with it if authentication fails
   * @throws `AuthorizationError` rejects with it if not authorized to the
   *   topic or to the configured groupId
   * @throws `KafkaError` rejects with it for any other unrecoverable errors
   * @throws `IllegalArgumentError` rejects with it if the timeout value is
   *   negative
   * @throws `RangeError` rejects with it if the timeout is `NaN`,
   *   `-Infinity`, holds years, months or weeks, or is beyond
   *   `Number.MAX_SAFE_INTEGER` milliseconds
   * @throws `TypeError` rejects with it if the timeout is null
   *
   * @remarks Java's blocking call returns a Promise here.
   * *(deviation: a negative timeout is refused before the closed check,
   * which Java runs first. Why: the FFI checks it first. Goes when: it
   * checks it after it.)*
   */
  position(options: {
    /** The partition to get the position for. */
    partition: TopicPartition;
    /** The maximum amount of time to await determination of the current position. */
    timeout: Duration;
  }): Promise<bigint>;

  /**
   * Get the last committed offsets for the given partitions (whether the
   * commit happened by this process or another). The returned offsets will be
   * used as the position for the consumer in the event of a failure.
   *
   * If any of the partitions requested do not exist, the result map will
   * contain null as the value for that partition.
   *
   * This call will do a remote call to get the latest committed offsets from
   * the server, and will block until the committed offsets are gotten
   * successfully, an unrecoverable error is encountered (in which case it is
   * thrown to the caller), or the timeout specified by
   * `default.api.timeout.ms` expires (in which case a `TimeoutError` is
   * thrown to the caller).
   *
   * @param options - The partitions to check
   * @returns resolving to the latest committed offsets for the given
   *   partitions; `null` will be returned for the partition if there is no
   *   such message.
   * @throws `WakeupError` rejects with it if `wakeup()` is called before or
   *   while this function is called
   * @throws `AuthenticationError` rejects with it if authentication fails
   * @throws `AuthorizationError` rejects with it if not authorized to the
   *   topic or to the configured groupId
   * @throws `UnsupportedVersionError` rejects with it if the consumer
   *   attempts to fetch stable offsets when the broker doesn't support this
   *   feature
   * @throws `KafkaError` rejects with it for any other unrecoverable errors
   * @throws `TimeoutError` rejects with it if the committed offset cannot be
   *   found before the timeout specified by `default.api.timeout.ms` expires
   * @throws `BootstrapResolutionError` rejects with it if DNS resolution of
   *   the bootstrap servers fails within `bootstrap.resolve.timeout.ms`
   * @throws `InvalidGroupIdError` rejects with it if the consumer has no
   *   `group.id`
   *
   * @remarks Java's blocking call returns a Promise here.
   * *(deviation: a null collection, or a partition with a null topic, gets
   * the FFI's `TypeError` before the closed and busy checks; without
   * `group.id`, `committed(null)` rejects with `InvalidGroupIdError` before
   * them. Why: the FFI refuses the NULL at its boundary, and the binding
   * checks the group first. Goes when: the FFI checks in Java's order.)*
   */
  committed(options: {
    /** The partitions to check. */
    partitions: Iterable<TopicPartition>;
    timeout?: never;
  }): Promise<TopicPartitionMap<OffsetAndMetadata | null>>;
  /**
   * Get the last committed offsets for the given partitions: the partitions
   * on their own, the same as `committed({ partitions })`.
   *
   * @param partitions - The partitions to check
   * @returns resolving to the latest committed offsets for the given
   *   partitions, `null` for a partition with none
   */
  committed(partitions: Iterable<TopicPartition>): Promise<TopicPartitionMap<OffsetAndMetadata | null>>;
  /**
   * Get the last committed offsets for the given partitions (whether the
   * commit happened by this process or another). The returned offsets will be
   * used as the position for the consumer in the event of a failure.
   *
   * If any of the partitions requested do not exist, the result map will
   * contain null as the value for that partition.
   *
   * This call will block to do a remote call to get the latest committed
   * offsets from the server.
   *
   * @param options - The partitions to check, and the maximum amount of time
   *   to await the latest committed offsets
   * @returns resolving to the latest committed offsets for the given
   *   partitions; `null` will be returned for the partition if there is no
   *   such message.
   * @throws `WakeupError` rejects with it if `wakeup()` is called before or
   *   while this function is called
   * @throws `AuthenticationError` rejects with it if authentication fails
   * @throws `AuthorizationError` rejects with it if not authorized to the
   *   topic or to the configured groupId
   * @throws `KafkaError` rejects with it for any other unrecoverable errors
   * @throws `TimeoutError` rejects with it if the committed offset cannot be
   *   found before expiration of the timeout
   * @throws `IllegalArgumentError` rejects with it if the timeout value is
   *   negative
   * @throws `RangeError` rejects with it if the timeout is `NaN`,
   *   `-Infinity`, holds years, months or weeks, or is beyond
   *   `Number.MAX_SAFE_INTEGER` milliseconds
   * @throws `TypeError` rejects with it if the timeout is null
   *
   * @remarks Java's blocking call returns a Promise here.
   * *(deviation: a negative timeout is refused, before the closed and group
   * checks too, where Java makes no check and waits without a time limit.
   * Why: the binding repeats no precondition, and the FFI refuses it. Goes
   * when: the FFI treats it as Java does.)*
   */
  committed(options: {
    /** The partitions to check. */
    partitions: Iterable<TopicPartition>;
    /** The maximum amount of time to await the latest committed offsets. */
    timeout: Duration;
  }): Promise<TopicPartitionMap<OffsetAndMetadata | null>>;

  /**
   * Get the metrics kept by the consumer.
   *
   * The returned map is an unmodifiable live view of the metrics. Changes to
   * the underlying metrics will be reflected in the returned map.
   *
   * @returns An unmodifiable live view of the map of metrics currently
   *   maintained by the consumer
   */
  metrics(): MetricNameMap<Metric>;

  /**
   * Get metadata about the partitions for a given topic. This method will
   * issue a remote call to the server if it does not already have any
   * metadata about the given topic.
   *
   * @param options - The topic to get partition metadata for
   * @returns resolving to the list of partitions, which will be empty when
   *   the given topic is not found. Note: when both the broker config
   *   `auto.create.topics.enable` and the consumer config
   *   `allow.auto.create.topics` are `true`, this method may return an empty
   *   list even though the topic is being auto-created in the background.
   *   Callers should not assume the topic does not exist based solely on an
   *   empty result.
   * @throws `WakeupError` rejects with it if `wakeup()` is called before or
   *   while this function is called
   * @throws `AuthenticationError` rejects with it if authentication fails
   * @throws `AuthorizationError` rejects with it if not authorized to the
   *   specified topic
   * @throws `KafkaError` rejects with it for any other unrecoverable errors
   * @throws `TimeoutError` rejects with it if the offset metadata could not be
   *   fetched before the amount of time allocated by `default.api.timeout.ms`
   *   expires
   * @throws `BootstrapResolutionError` rejects with it if DNS resolution of
   *   the bootstrap servers fails within `bootstrap.resolve.timeout.ms`
   *
   * @remarks Java's blocking call returns a Promise here. A null topic gets
   * the FFI's `TypeError`.
   */
  partitionsFor(options: {
    /** The topic to get partition metadata for. */
    topic: string;
    timeout?: never;
  }): Promise<PartitionInfo[]>;
  /**
   * Get metadata about the partitions for a given topic: the topic on its
   * own, the same as `partitionsFor({ topic })`.
   *
   * @param topic - The topic to get partition metadata for
   * @returns resolving to the list of partitions, empty when the topic is not
   *   found
   */
  partitionsFor(topic: string): Promise<PartitionInfo[]>;
  /**
   * Get metadata about the partitions for a given topic. This method will
   * issue a remote call to the server if it does not already have any
   * metadata about the given topic.
   *
   * @param options - The topic to get partition metadata for, and the maximum
   *   of time to await topic metadata
   * @returns resolving to the list of partitions, which will be empty when
   *   the given topic is not found. Note: when both the broker config
   *   `auto.create.topics.enable` and the consumer config
   *   `allow.auto.create.topics` are `true`, this method may return an empty
   *   list even though the topic is being auto-created in the background.
   *   Callers should not assume the topic does not exist based solely on an
   *   empty result.
   * @throws `WakeupError` rejects with it if `wakeup()` is called before or
   *   while this function is called
   * @throws `AuthenticationError` rejects with it if authentication fails
   * @throws `AuthorizationError` rejects with it if not authorized to the
   *   specified topic
   * @throws `TimeoutError` rejects with it if topic metadata cannot be fetched
   *   before expiration of the passed timeout
   * @throws `KafkaError` rejects with it for any other unrecoverable errors
   * @throws `IllegalArgumentError` rejects with it if the timeout value is
   *   negative
   * @throws `RangeError` rejects with it if the timeout is `NaN`,
   *   `-Infinity`, holds years, months or weeks, or is beyond
   *   `Number.MAX_SAFE_INTEGER` milliseconds
   * @throws `TypeError` rejects with it if the timeout or the topic is null
   *
   * @remarks Java's blocking call returns a Promise here.
   * *(deviation: a negative timeout is refused, before the closed check
   * too, where Java makes no check and waits without a time limit.
   * Why: the binding repeats no precondition, and the FFI refuses it. Goes
   * when: the FFI treats it as Java does.)*
   */
  partitionsFor(options: {
    /** The topic to get partition metadata for. */
    topic: string;
    /** The maximum of time to await topic metadata. */
    timeout: Duration;
  }): Promise<PartitionInfo[]>;

  /**
   * Get metadata about partitions for all topics that the user is authorized
   * to view. This method will issue a remote call to the server.
   *
   * @returns resolving to the map of topics and its partitions
   * @throws `WakeupError` rejects with it if `wakeup()` is called before or
   *   while this function is called
   * @throws `KafkaError` rejects with it for any other unrecoverable errors
   * @throws `TimeoutError` rejects with it if the offset metadata could not be
   *   fetched before the amount of time allocated by `default.api.timeout.ms`
   *   expires
   * @throws `BootstrapResolutionError` rejects with it if DNS resolution of
   *   the bootstrap servers fails within `bootstrap.resolve.timeout.ms`
   *
   * @remarks Java's blocking call returns a Promise here.
   */
  listTopics(): Promise<Map<string, PartitionInfo[]>>;
  /**
   * Get metadata about partitions for all topics that the user is authorized
   * to view. This method will issue a remote call to the server.
   *
   * @param options - The maximum time this operation will block to fetch
   *   topic metadata
   * @returns resolving to the map of topics and its partitions
   * @throws `WakeupError` rejects with it if `wakeup()` is called before or
   *   while this function is called
   * @throws `TimeoutError` rejects with it if the topic metadata could not be
   *   fetched before expiration of the passed timeout
   * @throws `KafkaError` rejects with it for any other unrecoverable errors
   * @throws `IllegalArgumentError` rejects with it if the timeout value is
   *   negative
   * @throws `RangeError` rejects with it if the timeout is `NaN`,
   *   `-Infinity`, holds years, months or weeks, or is beyond
   *   `Number.MAX_SAFE_INTEGER` milliseconds
   * @throws `TypeError` rejects with it if the timeout is null
   *
   * @remarks Java's blocking call returns a Promise here.
   * *(deviation: a negative timeout is refused, where Java makes no check
   * and waits without a time limit. Why: the binding repeats no
   * precondition, and the FFI refuses it. Goes when: the FFI treats it as
   * Java does.)*
   */
  listTopics(options: {
    /** The maximum time this operation will block to fetch topic metadata. */
    timeout: Duration;
  }): Promise<Map<string, PartitionInfo[]>>;
  /**
   * Get metadata about partitions for all topics: the timeout on its own, the
   * same as `listTopics({ timeout })`.
   *
   * @param timeout - The maximum time this operation will block to fetch
   *   topic metadata
   * @returns resolving to the map of topics and its partitions
   */
  listTopics(timeout: Duration): Promise<Map<string, PartitionInfo[]>>;

  /**
   * Get the set of partitions that were previously paused by a call to
   * `pause`.
   *
   * @returns The set of paused partitions
   * @throws `IllegalStateError` when the consumer is closed, the core's error
   * @throws `ConcurrentModificationError` while another call is in progress
   */
  paused(): TopicPartitionSet;

  /**
   * Suspend fetching from the requested partitions. Future calls to `poll`
   * will not return any records from these partitions until they have been
   * resumed using `resume`. Note that this method does not affect partition
   * subscription. In particular, it does not cause a group rebalance when
   * automatic assignment is used.
   *
   * The pause state is preserved across a rebalance for partitions that
   * remain assigned to this consumer, but it is lost for partitions that are
   * revoked. Which partitions are revoked depends on the group protocol in
   * use (see `ConsumerConfig.GROUP_PROTOCOL_CONFIG`):
   * - Classic group protocol: the behavior depends on the assignor configured
   *   in `ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG`: eager
   *   assignors revoke all partitions on every rebalance (pause state is not
   *   preserved); cooperative assignors only revoke the partitions that are
   *   reassigned to another consumer (pause state preserved for partitions
   *   that remain assigned)
   * - Consumer group protocol (KIP-848): only revokes partitions that are
   *   reassigned to another consumer (pause state preserved for partitions
   *   that remain assigned)
   *
   * @param options - The partitions which should be paused
   * @returns settling once the partitions are paused
   * @throws `IllegalStateError` rejects with it if any of the provided
   *   partitions are not currently assigned to this consumer
   *
   * @remarks Java's blocking call returns a Promise here.
   * *(deviation: a null collection is refused before the closed and busy
   * checks, and a partition with a null topic gets the FFI's `TypeError`
   * "topic must not be null", where Java reports those checks first and
   * "No current assignment for partition null-0". Why: the FFI refuses the
   * NULL at its boundary. Goes when: the FFI refuses it after the core's
   * checks, and takes a null topic.)*
   */
  pause(options: {
    /** The partitions which should be paused. */
    partitions: Iterable<TopicPartition>;
  }): Promise<void>;
  /**
   * Suspend fetching from the requested partitions: the partitions on their
   * own, the same as `pause({ partitions })`.
   *
   * @param partitions - The partitions which should be paused
   * @returns settling once the partitions are paused
   */
  pause(partitions: Iterable<TopicPartition>): Promise<void>;

  /**
   * Resume specified partitions which have been paused with `pause`. New
   * calls to `poll` will return records from these partitions if there are
   * any to be fetched. If the partitions were not previously paused, this
   * method is a no-op.
   *
   * @param options - The partitions which should be resumed
   * @returns settling once the partitions are resumed
   * @throws `IllegalStateError` rejects with it if any of the provided
   *   partitions are not currently assigned to this consumer
   *
   * @remarks Java's blocking call returns a Promise here.
   */
  resume(options: {
    /** The partitions which should be resumed. */
    partitions: Iterable<TopicPartition>;
  }): Promise<void>;
  /**
   * Resume specified partitions: the partitions on their own, the same as
   * `resume({ partitions })`.
   *
   * @param partitions - The partitions which should be resumed
   * @returns settling once the partitions are resumed
   */
  resume(partitions: Iterable<TopicPartition>): Promise<void>;

  /**
   * Look up the offsets for the given partitions by timestamp. The returned
   * offset for each partition is the earliest offset whose timestamp is
   * greater than or equal to the given timestamp in the corresponding
   * partition.
   *
   * This is a blocking call. The consumer does not have to be assigned the
   * partitions. If the message format version in a partition is before
   * 0.10.0, i.e. the messages do not have timestamps, null will be returned
   * for that partition.
   *
   * @param options - the mapping from partition to the timestamp to look up.
   * @returns resolving to a mapping from partition to the timestamp and
   *   offset of the first message with timestamp greater than or equal to the
   *   target timestamp. If the timestamp and offset for a specific partition
   *   cannot be found within the default timeout, and no corresponding
   *   message exists, the entry in the returned map will be `null`
   * @throws `AuthenticationError` rejects with it if authentication fails
   * @throws `AuthorizationError` rejects with it if not authorized to the
   *   topic(s)
   * @throws `IllegalArgumentError` rejects with it if the target timestamp is
   *   negative
   * @throws `TimeoutError` rejects with it if the offset metadata could not be
   *   fetched before the amount of time allocated by `default.api.timeout.ms`
   *   expires
   * @throws `UnsupportedVersionError` rejects with it if the broker does not
   *   support looking up the offsets by timestamp
   * @throws `BootstrapResolutionError` rejects with it if DNS resolution of
   *   the bootstrap servers fails within `bootstrap.resolve.timeout.ms`
   * @throws `TypeError` rejects with it if the map, a key or a timestamp is
   *   null, or the map is not a `TopicPartitionMap`
   *
   * @remarks Java's blocking call returns a Promise here.
   * *(deviation: a null map, key or timestamp is refused before the closed
   * and busy checks. Why: the FFI refuses the NULL at its boundary, and a
   * null timestamp has no value to cross as. Goes when: the FFI refuses them
   * after the core's checks.)*
   */
  offsetsForTimes(options: {
    /** The mapping from partition to the timestamp to look up: a bigint, or a whole number up to 2^53 - 1. */
    timestampsToSearch: TopicPartitionMap<bigint | number>;
    timeout?: never;
  }): Promise<TopicPartitionMap<OffsetAndTimestamp | null>>;
  /**
   * Look up the offsets for the given partitions by timestamp: the map on its
   * own, the same as `offsetsForTimes({ timestampsToSearch })`.
   *
   * @param timestampsToSearch - the mapping from partition to the timestamp
   *   to look up
   * @returns resolving to a mapping from partition to the offset and
   *   timestamp found, `null` for a partition with none
   */
  offsetsForTimes(timestampsToSearch: TopicPartitionMap<bigint | number>): Promise<TopicPartitionMap<OffsetAndTimestamp | null>>;
  /**
   * Look up the offsets for the given partitions by timestamp. The returned
   * offset for each partition is the earliest offset whose timestamp is
   * greater than or equal to the given timestamp in the corresponding
   * partition.
   *
   * This is a blocking call. The consumer does not have to be assigned the
   * partitions. If the message format version in a partition is before
   * 0.10.0, i.e. the messages do not have timestamps, null will be returned
   * for that partition.
   *
   * @param options - the mapping from partition to the timestamp to look up,
   *   and the maximum amount of time to await retrieval of the offsets
   * @returns resolving to a mapping from partition to the timestamp and
   *   offset of the first message with timestamp greater than or equal to the
   *   target timestamp. If the timestamp and offset for a specific partition
   *   cannot be found within timeout, and no corresponding message exists,
   *   the entry in the returned map will be `null`
   * @throws `AuthenticationError` rejects with it if authentication fails
   * @throws `AuthorizationError` rejects with it if not authorized to the
   *   topic(s)
   * @throws `IllegalArgumentError` rejects with it if the target timestamp or
   *   the timeout is negative
   * @throws `TimeoutError` rejects with it if the offset metadata could not be
   *   fetched before expiration of the passed timeout
   * @throws `UnsupportedVersionError` rejects with it if the broker does not
   *   support looking up the offsets by timestamp
   * @throws `RangeError` rejects with it if the timeout is `NaN`,
   *   `-Infinity`, holds years, months or weeks, or is beyond
   *   `Number.MAX_SAFE_INTEGER` milliseconds
   * @throws `TypeError` rejects with it if the timeout, the map, a key or a
   *   timestamp is null
   *
   * @remarks Java's blocking call returns a Promise here.
   * *(deviation: a negative timeout is refused, before the closed check
   * too, where Java makes no check and waits without a time limit.
   * Why: the binding repeats no precondition, and the FFI refuses it. Goes
   * when: the FFI treats it as Java does.)*
   */
  offsetsForTimes(options: {
    /** The mapping from partition to the timestamp to look up: a bigint, or a whole number up to 2^53 - 1. */
    timestampsToSearch: TopicPartitionMap<bigint | number>;
    /** The maximum amount of time to await retrieval of the offsets. */
    timeout: Duration;
  }): Promise<TopicPartitionMap<OffsetAndTimestamp | null>>;

  /**
   * Get the first offset for the given partitions.
   *
   * This method does not change the current consumer position of the
   * partitions.
   *
   * @param options - the partitions to get the earliest offsets.
   * @returns resolving to the earliest available offsets for the given
   *   partitions
   * @throws `AuthenticationError` rejects with it if authentication fails
   * @throws `AuthorizationError` rejects with it if not authorized to the
   *   topic(s)
   * @throws `TimeoutError` rejects with it if the offset metadata could not be
   *   fetched before expiration of the configured `default.api.timeout.ms`
   *
   * @remarks Java's blocking call returns a Promise here.
   * *(deviation: a null collection gets the FFI's `TypeError` before the
   * closed and busy checks. Why: the FFI refuses the NULL first. Goes when:
   * it refuses it after them.)*
   *
   * @see `seekToBeginning`
   */
  beginningOffsets(options: {
    /** The partitions to get the earliest offsets. */
    partitions: Iterable<TopicPartition>;
    timeout?: never;
  }): Promise<TopicPartitionMap<bigint>>;
  /**
   * Get the first offset for the given partitions: the partitions on their
   * own, the same as `beginningOffsets({ partitions })`.
   *
   * @param partitions - the partitions to get the earliest offsets
   * @returns resolving to the earliest available offsets for the given
   *   partitions
   */
  beginningOffsets(partitions: Iterable<TopicPartition>): Promise<TopicPartitionMap<bigint>>;
  /**
   * Get the first offset for the given partitions.
   *
   * This method does not change the current consumer position of the
   * partitions.
   *
   * @param options - the partitions to get the earliest offsets, and the
   *   maximum amount of time to await retrieval of the beginning offsets
   * @returns resolving to the earliest available offsets for the given
   *   partitions, and it will return empty map if zero timeout is provided
   * @throws `AuthenticationError` rejects with it if authentication fails
   * @throws `AuthorizationError` rejects with it if not authorized to the
   *   topic(s)
   * @throws `TimeoutError` rejects with it if the offset metadata could not be
   *   fetched before expiration of the passed timeout
   * @throws `IllegalArgumentError` rejects with it if the timeout value is
   *   negative
   * @throws `RangeError` rejects with it if the timeout is `NaN`,
   *   `-Infinity`, holds years, months or weeks, or is beyond
   *   `Number.MAX_SAFE_INTEGER` milliseconds
   * @throws `TypeError` rejects with it if the timeout is null
   *
   * @remarks Java's blocking call returns a Promise here.
   * *(deviation: a negative timeout is refused, where Java makes no check
   * and waits without a time limit. Why: the binding repeats no
   * precondition, and the FFI refuses it. Goes when: the FFI treats it as
   * Java does.)*
   *
   * @see `seekToBeginning`
   */
  beginningOffsets(options: {
    /** The partitions to get the earliest offsets. */
    partitions: Iterable<TopicPartition>;
    /** The maximum amount of time to await retrieval of the beginning offsets. */
    timeout: Duration;
  }): Promise<TopicPartitionMap<bigint>>;

  /**
   * Get the end offsets for the given partitions. In the default
   * `read_uncommitted` isolation level, the end offset is the high watermark
   * (that is, the offset of the last successfully replicated message plus
   * one). For `read_committed` consumers, the end offset is the last stable
   * offset (LSO), which is the minimum of the high watermark and the smallest
   * offset of any open transaction. Finally, if the partition has never been
   * written to, the end offset is 0.
   *
   * This method does not change the current consumer position of the
   * partitions.
   *
   * @param options - the partitions to get the end offsets.
   * @returns resolving to the end offsets for the given partitions, each a
   *   bigint
   * @throws `AuthenticationError` rejects with it if authentication fails
   * @throws `AuthorizationError` rejects with it if not authorized to the
   *   topic(s)
   * @throws `TimeoutError` rejects with it if the offset metadata could not be
   *   fetched before the amount of time allocated by `default.api.timeout.ms`
   *   expires
   *
   * @remarks Java's blocking call returns a Promise here.
   * *(deviation: a null collection gets the FFI's `TypeError` before the
   * closed and busy checks. Why: the FFI refuses the NULL first. Goes when:
   * it refuses it after them.)*
   *
   * @see `seekToEnd`
   */
  endOffsets(options: {
    /** The partitions to get the end offsets. */
    partitions: Iterable<TopicPartition>;
    timeout?: never;
  }): Promise<TopicPartitionMap<bigint>>;
  /**
   * Get the end offsets for the given partitions: the partitions on their
   * own, the same as `endOffsets({ partitions })`.
   *
   * @param partitions - the partitions to get the end offsets
   * @returns resolving to the end offsets for the given partitions
   */
  endOffsets(partitions: Iterable<TopicPartition>): Promise<TopicPartitionMap<bigint>>;
  /**
   * Get the end offsets for the given partitions. In the default
   * `read_uncommitted` isolation level, the end offset is the high watermark
   * (that is, the offset of the last successfully replicated message plus
   * one). For `read_committed` consumers, the end offset is the last stable
   * offset (LSO), which is the minimum of the high watermark and the smallest
   * offset of any open transaction. Finally, if the partition has never been
   * written to, the end offset is 0.
   *
   * This method does not change the current consumer position of the
   * partitions.
   *
   * @param options - the partitions to get the end offsets, and the maximum
   *   amount of time to await retrieval of the end offsets
   * @returns resolving to the end offsets for the given partitions, and it
   *   will return empty map if zero timeout is provided
   * @throws `AuthenticationError` rejects with it if authentication fails
   * @throws `AuthorizationError` rejects with it if not authorized to the
   *   topic(s)
   * @throws `TimeoutError` rejects with it if the offsets could not be
   *   fetched before expiration of the passed timeout
   * @throws `IllegalArgumentError` rejects with it if the timeout value is
   *   negative
   * @throws `RangeError` rejects with it if the timeout is `NaN`,
   *   `-Infinity`, holds years, months or weeks, or is beyond
   *   `Number.MAX_SAFE_INTEGER` milliseconds
   * @throws `TypeError` rejects with it if the timeout is null
   *
   * @remarks Java's blocking call returns a Promise here.
   * *(deviation: a negative timeout is refused, before the closed check
   * too, where Java makes no check and waits without a time limit.
   * Why: the binding repeats no precondition, and the FFI refuses it. Goes
   * when: the FFI treats it as Java does.)*
   *
   * @see `seekToEnd`
   */
  endOffsets(options: {
    /** The partitions to get the end offsets. */
    partitions: Iterable<TopicPartition>;
    /** The maximum amount of time to await retrieval of the end offsets. */
    timeout: Duration;
  }): Promise<TopicPartitionMap<bigint>>;

  /**
   * Get the consumer's current lag on the partition. Returns an "empty"
   * value, `null`, if the lag is not known, for example if there is no
   * position yet, or if the end offset is not known yet.
   *
   * This method uses locally cached metadata. If the log end offset is not
   * known yet, it triggers a request to fetch the log end offset, but returns
   * immediately.
   *
   * @param options - The partition to get the lag for.
   * @returns resolving to this `Consumer` instance's current lag for the given
   *   partition, a bigint, or `null` when it is not known
   * @throws `IllegalStateError` rejects with it if the `topicPartition` is
   *   not assigned
   *
   * @remarks *(deviation: a null partition gets the FFI's `TypeError` before
   * the closed and busy checks. Why: the FFI refuses the NULL first. Goes
   * when: it refuses it after them.)*
   */
  currentLag(options: {
    /** The partition to get the lag for. */
    topicPartition: TopicPartition;
  }): Promise<bigint | null>;
  /**
   * Get the consumer's current lag on the partition: the partition on its
   * own, the same as `currentLag({ topicPartition })`.
   *
   * @param topicPartition - The partition to get the lag for.
   * @returns resolving to the current lag, or `null` when it is not known
   */
  currentLag(topicPartition: TopicPartition): Promise<bigint | null>;

  /**
   * Return the current group metadata associated with this consumer.
   *
   * @returns consumer group metadata
   * @throws `InvalidGroupIdError` if consumer does not have a group
   * @throws `IllegalStateError` when the consumer is closed, the core's error
   * @throws `ConcurrentModificationError` while another call is in progress
   */
  groupMetadata(): ConsumerGroupMetadata;

  /**
   * Close the consumer with the default leave group behavior, waiting for up
   * to the default timeout of 30 seconds for any needed cleanup. If
   * auto-commit is enabled, this will commit the current offsets if possible
   * within the default timeout. See `close({ option })` for details. Note
   * that `wakeup()` cannot be used to interrupt close.
   *
   * This close operation will attempt all shutdown steps even if one of them
   * fails. It logs all encountered errors, continues to execute the next
   * steps, and finally throws the first error found.
   *
   * @returns settling once the consumer is closed
   * @throws `WakeupError` rejects with it if `wakeup()` is called before or
   *   while this function is called
   * @throws `KafkaError` rejects with it for any other error during close
   *   (e.g., errors thrown from rebalance callbacks or commit callbacks from
   *   previous asynchronous commits)
   * @throws `ConcurrentModificationError` rejects with it while another call,
   *   such as a pending `poll()`, is in progress
   *
   * @remarks Java's blocking call returns a Promise here. A consumer keeps
   * Node running until it is closed: stop the poll loop (`wakeup()`), then
   * await `close()`.
   */
  close(): Promise<void>;
  /**
   * Close the consumer cleanly. `CloseOptions` allows to specify a timeout
   * and a leave group behavior. If no timeout is specified, the default
   * timeout of 30 seconds is used. If no leave group behavior is specified,
   * the default leave group behavior is used.
   *
   * This method waits up to the timeout for the consumer to complete pending
   * commits and maybe leave the group, depending on the specified leave group
   * behavior. If auto-commit is enabled, this will commit the current offsets
   * if possible within the timeout. If the consumer is unable to complete
   * offset commits and to gracefully leave the group (if applicable) before
   * the timeout expires, the consumer is force closed. Note that `wakeup()`
   * cannot be used to interrupt close.
   *
   * The actual maximum wait time is bounded by the
   * `ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG` setting, which only applies to
   * operations performed with the broker (coordinator-related requests and
   * fetch sessions). Even if a larger timeout is specified, the consumer will
   * not wait longer than `ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG` for these
   * requests to complete during the close operation. Note that the execution
   * time of callbacks (such as `OffsetCommitCallback` and
   * `ConsumerRebalanceListener`) does not consume time from the close
   * timeout.
   *
   * This close operation will attempt all shutdown steps even if one of them
   * fails. It logs all encountered errors, continues to execute the next
   * steps, and finally throws the first error found.
   *
   * @param options - see `CloseOptions`; cannot be `null`
   * @returns settling once the consumer is closed
   * @throws `WakeupError` rejects with it if `wakeup()` is called before or
   *   while this function is called
   * @throws `KafkaError` rejects with it for any other error during close
   *   (e.g., errors thrown from rebalance callbacks or commit callbacks from
   *   previous asynchronous commits)
   * @throws `TypeError` rejects with it if `option` is null or not a
   *   `CloseOptions`
   *
   * @remarks *(deviation: a negative timeout is read as zero, where Java
   * throws IllegalArgumentException("The timeout cannot be negative."). Why:
   * the binding repeats no precondition. Goes when: the FFI returns Java's
   * error.)*
   */
  close(options: {
    /** See `CloseOptions`; cannot be `null`. */
    option: CloseOptions;
  }): Promise<void>;
  /**
   * Close the consumer with the given options: the options on their own, the
   * same as `close({ option })`.
   *
   * @param option - see `CloseOptions`
   * @returns settling once the consumer is closed
   */
  close(option: CloseOptions): Promise<void>;

  /**
   * Wakeup the consumer. This method is useful in particular to abort a long
   * poll. The call which is waiting in an operation will reject with
   * `WakeupError`. If no call is waiting in a method which can throw
   * `WakeupError`, the next call to such a method will raise it instead.
   *
   * @remarks It goes straight to the client and works at any time, from a
   * timer or a signal handler too.
   */
  wakeup(): void;

  /**
   * Closes the consumer as `close()` does, so that `await using` closes it.
   *
   * @returns settling once the consumer is closed, rejecting as `close()`
   *   rejects
   *
   * @remarks *(js-only)* JavaScript's async disposal protocol, the form
   * Java's `Closeable.close()` takes in a `try`-with-resources block.
   */
  [Symbol.asyncDispose](): Promise<void>;
}
