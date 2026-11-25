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
import type { TopicPartition } from '../common/topic_partition';
import type { TopicPartitionSet } from '../common/topic_partition_set';
import type { Duration } from '../duration';
import type { CloseOptions } from './close_options';
import type { ConsumerRecords } from './consumer_records';
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
   * @remarks Java's blocking call returns a Promise here. The form with a
   * rebalance listener is not in this release.
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
   * Subscribe to all topics matching the specified pattern, to get
   * dynamically assigned partitions. The pattern matching will be done
   * periodically against topics. This is only supported under the CONSUMER
   * group protocol (see `ConsumerConfig.GROUP_PROTOCOL_CONFIG`).
   *
   * If the provided pattern is not compatible with Google RE2/J, an
   * `InvalidRegularExpression` will be eventually thrown on a call to `poll`
   * following this call to subscribe.
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
   * change.
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
   * offsets before unsubscribing, even if `enable.auto.commit` is enabled.
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
   * starting offset and fetch sequentially.
   *
   * This method returns immediately if there are records available or if the
   * position advances past control records or aborted transactions when
   * isolation.level=read_committed. Otherwise, it will await the passed
   * timeout. If the timeout expires, an empty record set will be returned.
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
   * @throws `IllegalArgumentError` rejects with it if the timeout value is
   *   negative
   * @throws `IllegalStateError` rejects with it if the consumer is not
   *   subscribed to any topics or manually assigned any partitions to consume
   *   from
   * @throws `InvalidTopicError` rejects with it if the current subscription
   *   contains any invalid topic
   * @throws `UnsupportedVersionError` rejects with it if the broker doesn't
   *   support a feature the consumer needs
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
   * revoked.
   *
   * @param options - The partitions which should be paused
   * @returns settling once the partitions are paused
   * @throws `IllegalStateError` rejects with it if any of the provided
   *   partitions are not currently assigned to this consumer
   *
   * @remarks Java's blocking call returns a Promise here.
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
   * Close the consumer with the default leave group behavior, waiting for up
   * to the default timeout of 30 seconds for any needed cleanup. If
   * auto-commit is enabled, this will commit the current offsets if possible
   * within the default timeout. Note that `wakeup()` cannot be used to
   * interrupt close.
   *
   * This close operation will attempt all shutdown steps even if one of them
   * fails. It logs all encountered errors, continues to execute the next
   * steps, and finally throws the first error found.
   *
   * @returns settling once the consumer is closed
   * @throws `WakeupError` rejects with it if `wakeup()` is called before or
   *   while this function is called
   * @throws `KafkaError` rejects with it for any other error during close
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
   * behavior. If the consumer is unable to complete offset commits and to
   * gracefully leave the group (if applicable) before the timeout expires,
   * the consumer is force closed. Note that `wakeup()` cannot be used to
   * interrupt close.
   *
   * @param options - see `CloseOptions`; cannot be `null`
   * @returns settling once the consumer is closed
   * @throws `WakeupError` rejects with it if `wakeup()` is called before or
   *   while this function is called
   * @throws `KafkaError` rejects with it for any other error during close
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
