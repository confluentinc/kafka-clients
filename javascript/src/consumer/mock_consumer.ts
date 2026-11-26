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

// Java's MockConsumer over the C FFI's MockConsumer (javascript/CLAUDE.md,
// JavaScript Binding Conventions, Implementation over the FFI): the same
// native handle as a KafkaConsumer's, over the FFI's Consumer view, so its
// Consumer methods run src/internal/consumer_calls.ts's bodies, as
// KafkaConsumer's do. A record added crosses as the FFI's `void *` key and
// value, which the FFI's mock keeps without reading, and the addon holds the
// app's record until the mock is destroyed, so poll() returns the very
// records the test added (native/consumer.c, native/mock_values.c). An error
// given to setPollException or setOffsetsException comes back as the very
// same object.
//
// Left out for want of an FFI entry point: clientInstanceId,
// setClientInstanceId, disableTelemetry, injectTimeoutException,
// schedulePollTask, registerMetricForSubscription,
// unregisterMetricFromSubscription and addedMetrics. Left out, as in Python:
// enforceRebalance(), enforceRebalance(reason), shouldRebalance() and
// resetShouldRebalance(). Left out as on KafkaConsumer: the
// subscribe(Pattern...) forms, and until the client has rebalance listeners
// and commit callbacks, the forms taking one. Left out because Java
// deprecates them: MockConsumer(OffsetResetStrategy) and close(Duration).

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way (the addon is loaded there alone, javascript/CLAUDE.md "Files and layout")
import native = require('../internal/native');
import { JavaForms } from '../internal/args';
import * as calls from '../internal/consumer_calls';
import { UNBOUNDED_MILLIS } from '../internal/duration';
import { partitionInfoArgs } from '../internal/metadata';
import type { PartitionInfoArgs } from '../internal/metadata';
import { long, topicPartitionLongMap } from '../internal/payload';
import { KafkaError } from '../common/kafka_error';
import type { Metric } from '../common/metric';
import type { MetricNameMap } from '../common/metric_name_map';
import { PartitionInfo } from '../common/partition_info';
import type { TopicPartition } from '../common/topic_partition';
import { TopicPartitionMap } from '../common/topic_partition_map';
import type { TopicPartitionSet } from '../common/topic_partition_set';
import type { Duration } from '../duration';
import type { CloseOptions } from './close_options';
import type { Consumer } from './consumer';
import type { ConsumerGroupMetadata } from './consumer_group_metadata';
import { ConsumerRecord } from './consumer_record';
import type { ConsumerRecords } from './consumer_records';
import type { OffsetAndMetadata } from './offset_and_metadata';
import type { OffsetAndTimestamp } from './offset_and_timestamp';
import type { SubscriptionPattern } from './subscription_pattern';

// Java's forms of each mock method, in Java declaration order (src/internal/args.ts).
const CONSTRUCTOR_FORMS = new JavaForms('MockConsumer', [['offsetResetStrategy']], {
  bareTypes: { offsetResetStrategy: (value) => value === null || typeof value === 'string' },
});
const REBALANCE_FORMS = new JavaForms('rebalance', [['newAssignment']], {
  bareTypes: { newAssignment: (v) => v === null || calls.isIterable(v) },
});
const LOSE_PARTITIONS_FORMS = new JavaForms('losePartitions', [['partitionsLost']], {
  bareTypes: { partitionsLost: (v) => v === null || calls.isIterable(v) },
});
const ADD_RECORD_FORMS = new JavaForms('addRecord', [['record']], {
  bareTypes: { record: (v) => v === null || v instanceof ConsumerRecord },
});
const SET_MAX_POLL_RECORDS_FORMS = new JavaForms('setMaxPollRecords', [['maxPollRecords']], {
  bareTypes: { maxPollRecords: (v) => typeof v === 'bigint' || typeof v === 'number' },
});
// Any Error is taken bare, so one that is no KafkaError gets the TypeError
// that says so rather than the form error.
const isErrorOrNull = (value: unknown): boolean => value === null || value instanceof Error;
const SET_POLL_FORMS = new JavaForms('setPollException', [['exception']], { bareTypes: { exception: isErrorOrNull } });
const SET_OFFSETS_FORMS = new JavaForms('setOffsetsException', [['exception']], { bareTypes: { exception: isErrorOrNull } });
// A Map is taken bare too, so it gets payload.ts' hint rather than the form error.
const isMapOrNull = (value: unknown): boolean => value === null || value instanceof TopicPartitionMap || value instanceof Map;
const UPDATE_BEGINNING_FORMS = new JavaForms('updateBeginningOffsets', [['newOffsets']], { bareTypes: { newOffsets: isMapOrNull } });
const UPDATE_END_FORMS = new JavaForms('updateEndOffsets', [['newOffsets']], { bareTypes: { newOffsets: isMapOrNull } });
const UPDATE_DURATION_FORMS = new JavaForms('updateDurationOffsets', [['newOffsets']], { bareTypes: { newOffsets: isMapOrNull } });
const UPDATE_PARTITIONS_FORMS = new JavaForms('updatePartitions', [['topic', 'partitions']]);
const CLOSED_FORMS = new JavaForms('closed', [[]], { reader: true });
const SCHEDULE_NOP_FORMS = new JavaForms('scheduleNopPollTask', [[]]);
const LAST_POLL_TIMEOUT_FORMS = new JavaForms('lastPollTimeout', [[]], { reader: true });

/**
 * The error given to a mock's setter, checked: a KafkaError, or null.
 *
 * @param value - the value given
 * @returns the error, or null
 */
function checkKafkaError(value: unknown): KafkaError | null {
  if (value !== null && !(value instanceof KafkaError)) throw new TypeError('exception must be a KafkaError or null');
  return value;
}

/**
 * The offsets of a mock's update as the addon takes them: `[[topic,
 * partition], offset]` entries, a null map or key going to the FFI as NULL.
 *
 * @param value - the offsets given
 * @returns the entries
 */
function offsetEntries(value: unknown): Array<[[string | null, number] | null, bigint]> | null {
  const map = topicPartitionLongMap.take(value, 'newOffsets') as TopicPartitionMap<bigint | null> | null;
  if (map === null) return null;
  return [...map.entries()].map(([tp, offset]): [[string | null, number] | null, bigint] => {
    // The FFI reads a NULL offset as 0, where Java's mock keeps the null.
    if (offset === null) throw new TypeError('newOffsets must not contain a null offset');
    return [tp === null ? null : [tp.topic, tp.partition], offset];
  });
}

/**
 * The partitions of updatePartitions as the addon takes them: each
 * PartitionInfo's fields, a null collection or element going to the FFI as
 * NULL.
 *
 * @param value - the partitions given
 * @returns the fields
 */
function partitionInfoArgsOf(value: unknown): Array<PartitionInfoArgs | null> | null {
  if (value === null) return null;
  if (!calls.isIterable(value)) throw new TypeError('partitions must be an iterable of PartitionInfo');
  return [...(value as Iterable<unknown>)].map((info) => {
    if (info === null) return null;
    if (!(info instanceof PartitionInfo)) throw new TypeError('partitions must be an iterable of PartitionInfo');
    return partitionInfoArgs(info);
  });
}

/**
 * A mock of the `Consumer` interface you can use for testing code that uses
 * Kafka. This class is *not threadsafe*.
 *
 * @typeParam K - the type of the keys of the records the test adds
 * @typeParam V - the type of the values of the records the test adds
 *
 * @remarks It is the core's mock, reached through the addon as
 * `KafkaConsumer` is: every waiting method returns a Promise, and a call made
 * while another is pending rejects with `ConcurrentModificationError`. It
 * keeps Node running from its creation until `close()` closes it, as a
 * consumer does. `poll()` returns the records `addRecord` was given, the very
 * objects.
 */
export class MockConsumer<K, V> implements Consumer<K, V> {
  #handle: calls.ConsumerHandle;

  /**
   * A mock consumer is instantiated by providing
   * `ConsumerConfig.AUTO_OFFSET_RESET_CONFIG` value as the input.
   *
   * @param options - the offset reset strategy to use
   * @throws `IllegalArgumentError` when the strategy is not a valid one, the
   *   core's, or the given parameters are not `{offsetResetStrategy}`
   * @throws `TypeError` when `offsetResetStrategy` is not a string, or
   *   `undefined`
   */
  constructor(options: {
    /** The offset reset strategy to use: `earliest`, `latest`, `none` or `by_duration:<ISO-8601 duration>`. */
    offsetResetStrategy: string;
  });
  /**
   * A mock consumer is instantiated by providing
   * `ConsumerConfig.AUTO_OFFSET_RESET_CONFIG` value as the input: the strategy
   * on its own, the same as `{offsetResetStrategy}`.
   *
   * @param offsetResetStrategy - The offset reset strategy to use
   */
  constructor(offsetResetStrategy: string);
  constructor(options: unknown) {
    CONSTRUCTOR_FORMS.match(options, arguments.length);
    const strategy = CONSTRUCTOR_FORMS.value(options, 'offsetResetStrategy');
    if (strategy !== null && typeof strategy !== 'string') throw new TypeError('offsetResetStrategy must be a string');
    this.#handle = native.mockConsumerCreate(strategy);
  }

  assignment(): TopicPartitionSet {
    return calls.assignment(this.#handle, arguments.length);
  }

  subscription(): Set<string> {
    return calls.subscription(this.#handle, arguments.length);
  }

  subscribe(options: {
    /** The list of topics to subscribe to. */
    topics: Iterable<string>;
    pattern?: never;
    callback?: never;
  }): Promise<void>;
  subscribe(topics: Iterable<string>): Promise<void>;
  subscribe(options: {
    /** Pattern to subscribe to, that must be compatible with Google RE2/J. */
    pattern: SubscriptionPattern;
    topics?: never;
    callback?: never;
  }): Promise<void>;
  subscribe(pattern: SubscriptionPattern): Promise<void>;
  subscribe(options: unknown): Promise<void> {
    return calls.subscribe(this.#handle, options, arguments.length, false);
  }

  assign(options: {
    /** The list of partitions to assign this consumer. */
    partitions: Iterable<TopicPartition>;
  }): Promise<void>;
  assign(partitions: Iterable<TopicPartition>): Promise<void>;
  assign(options: unknown): Promise<void> {
    return calls.assign(this.#handle, options, arguments.length, false);
  }

  unsubscribe(): Promise<void> {
    return calls.unsubscribe(this.#handle, arguments.length);
  }

  poll(options: {
    /** The maximum time to block (must not be greater than `Long.MAX_VALUE` milliseconds). */
    timeout: Duration;
  }): Promise<ConsumerRecords<K, V>>;
  poll(timeout: Duration): Promise<ConsumerRecords<K, V>>;
  poll(options: unknown): Promise<ConsumerRecords<K, V>> {
    return calls.poll<K, V>(this.#handle, options, arguments.length);
  }

  commitSync(): Promise<void>;
  commitSync(options: {
    /** The maximum amount of time to await completion of the offset commit. */
    timeout: Duration;
    offsets?: never;
  }): Promise<void>;
  commitSync(timeout: Duration): Promise<void>;
  commitSync(options: {
    /** A map of offsets by partition with associated metadata, copied by the call. */
    offsets: TopicPartitionMap<OffsetAndMetadata>;
    timeout?: never;
  }): Promise<void>;
  commitSync(offsets: TopicPartitionMap<OffsetAndMetadata>): Promise<void>;
  commitSync(options: {
    /** A map of offsets by partition with associated metadata, copied by the call. */
    offsets: TopicPartitionMap<OffsetAndMetadata>;
    /** The maximum amount of time to await completion of the offset commit. */
    timeout: Duration;
  }): Promise<void>;
  commitSync(options?: unknown): Promise<void> {
    return calls.commitSync(this.#handle, options, arguments.length);
  }

  commitAsync(): Promise<void>;
  commitAsync(options?: unknown): Promise<void> {
    return calls.commitAsync(this.#handle, options, arguments.length);
  }

  seek(options: {
    /** The TopicPartition on which the seek will be performed. */
    partition: TopicPartition;
    /** The next offset returned by poll(): a bigint, or a whole number up to 2^53 - 1. */
    offset: bigint | number;
    offsetAndMetadata?: never;
  }): Promise<void>;
  seek(options: {
    /** The TopicPartition on which the seek will be performed. */
    partition: TopicPartition;
    /** The next offset returned by poll(), with its leader epoch. */
    offsetAndMetadata: OffsetAndMetadata;
    offset?: never;
  }): Promise<void>;
  seek(options: unknown): Promise<void> {
    return calls.seek(this.#handle, options, arguments.length);
  }

  seekToBeginning(options: {
    /** The partitions to seek to their first offset. */
    partitions: Iterable<TopicPartition>;
  }): Promise<void>;
  seekToBeginning(partitions: Iterable<TopicPartition>): Promise<void>;
  seekToBeginning(options: unknown): Promise<void> {
    return calls.seekToBeginning(this.#handle, options, arguments.length);
  }

  seekToEnd(options: {
    /** The partitions to seek to their last offset. */
    partitions: Iterable<TopicPartition>;
  }): Promise<void>;
  seekToEnd(partitions: Iterable<TopicPartition>): Promise<void>;
  seekToEnd(options: unknown): Promise<void> {
    return calls.seekToEnd(this.#handle, options, arguments.length);
  }

  position(options: {
    /** The partition to get the position for. */
    partition: TopicPartition;
    timeout?: never;
  }): Promise<bigint>;
  position(partition: TopicPartition): Promise<bigint>;
  position(options: {
    /** The partition to get the position for. */
    partition: TopicPartition;
    /** The maximum amount of time to await determination of the current position. */
    timeout: Duration;
  }): Promise<bigint>;
  position(options: unknown): Promise<bigint> {
    return calls.position(this.#handle, options, arguments.length);
  }

  committed(options: {
    /** The partitions to check. */
    partitions: Iterable<TopicPartition>;
    timeout?: never;
  }): Promise<TopicPartitionMap<OffsetAndMetadata | null>>;
  committed(partitions: Iterable<TopicPartition>): Promise<TopicPartitionMap<OffsetAndMetadata | null>>;
  committed(options: {
    /** The partitions to check. */
    partitions: Iterable<TopicPartition>;
    /** The maximum amount of time to await the latest committed offsets. */
    timeout: Duration;
  }): Promise<TopicPartitionMap<OffsetAndMetadata | null>>;
  committed(options: unknown): Promise<TopicPartitionMap<OffsetAndMetadata | null>> {
    // The mock needs no group.id: the binding's temporary check is KafkaConsumer's.
    return calls.committed(this.#handle, options, arguments.length, true);
  }

  /**
   * @remarks An empty map, as Java's mock's, which answers after `close()`
   * too, where Java's mock throws.
   */
  metrics(): MetricNameMap<Metric> {
    return calls.metrics(this.#handle, arguments.length);
  }

  partitionsFor(options: {
    /** The topic to get partition metadata for. */
    topic: string;
    timeout?: never;
  }): Promise<PartitionInfo[]>;
  partitionsFor(topic: string): Promise<PartitionInfo[]>;
  partitionsFor(options: {
    /** The topic to get partition metadata for. */
    topic: string;
    /** The maximum of time to await topic metadata. */
    timeout: Duration;
  }): Promise<PartitionInfo[]>;
  partitionsFor(options: unknown): Promise<PartitionInfo[]> {
    return calls.partitionsFor(this.#handle, options, arguments.length);
  }

  listTopics(): Promise<Map<string, PartitionInfo[]>>;
  listTopics(options: {
    /** The maximum time this operation will block to fetch topic metadata. */
    timeout: Duration;
  }): Promise<Map<string, PartitionInfo[]>>;
  listTopics(timeout: Duration): Promise<Map<string, PartitionInfo[]>>;
  listTopics(options?: unknown): Promise<Map<string, PartitionInfo[]>> {
    return calls.listTopics(this.#handle, options, arguments.length);
  }

  paused(): TopicPartitionSet {
    return calls.paused(this.#handle, arguments.length);
  }

  pause(options: {
    /** The partitions which should be paused. */
    partitions: Iterable<TopicPartition>;
  }): Promise<void>;
  pause(partitions: Iterable<TopicPartition>): Promise<void>;
  pause(options: unknown): Promise<void> {
    return calls.pause(this.#handle, options, arguments.length);
  }

  resume(options: {
    /** The partitions which should be resumed. */
    partitions: Iterable<TopicPartition>;
  }): Promise<void>;
  resume(partitions: Iterable<TopicPartition>): Promise<void>;
  resume(options: unknown): Promise<void> {
    return calls.resume(this.#handle, options, arguments.length);
  }

  /**
   * @remarks Not supported, as Java's mock: it rejects with
   * `UnsupportedOperationError` "Not implemented yet.".
   */
  offsetsForTimes(options: {
    /** The mapping from partition to the timestamp to look up: a bigint, or a whole number up to 2^53 - 1. */
    timestampsToSearch: TopicPartitionMap<bigint | number>;
    timeout?: never;
  }): Promise<TopicPartitionMap<OffsetAndTimestamp | null>>;
  offsetsForTimes(timestampsToSearch: TopicPartitionMap<bigint | number>): Promise<TopicPartitionMap<OffsetAndTimestamp | null>>;
  offsetsForTimes(options: {
    /** The mapping from partition to the timestamp to look up: a bigint, or a whole number up to 2^53 - 1. */
    timestampsToSearch: TopicPartitionMap<bigint | number>;
    /** The maximum amount of time to await retrieval of the offsets. */
    timeout: Duration;
  }): Promise<TopicPartitionMap<OffsetAndTimestamp | null>>;
  offsetsForTimes(options: unknown): Promise<TopicPartitionMap<OffsetAndTimestamp | null>> {
    return calls.offsetsForTimes(this.#handle, options, arguments.length);
  }

  beginningOffsets(options: {
    /** The partitions to get the earliest offsets. */
    partitions: Iterable<TopicPartition>;
    timeout?: never;
  }): Promise<TopicPartitionMap<bigint>>;
  beginningOffsets(partitions: Iterable<TopicPartition>): Promise<TopicPartitionMap<bigint>>;
  beginningOffsets(options: {
    /** The partitions to get the earliest offsets. */
    partitions: Iterable<TopicPartition>;
    /** The maximum amount of time to await retrieval of the beginning offsets. */
    timeout: Duration;
  }): Promise<TopicPartitionMap<bigint>>;
  beginningOffsets(options: unknown): Promise<TopicPartitionMap<bigint>> {
    return calls.beginningOffsets(this.#handle, options, arguments.length);
  }

  endOffsets(options: {
    /** The partitions to get the end offsets. */
    partitions: Iterable<TopicPartition>;
    timeout?: never;
  }): Promise<TopicPartitionMap<bigint>>;
  endOffsets(partitions: Iterable<TopicPartition>): Promise<TopicPartitionMap<bigint>>;
  endOffsets(options: {
    /** The partitions to get the end offsets. */
    partitions: Iterable<TopicPartition>;
    /** The maximum amount of time to await retrieval of the end offsets. */
    timeout: Duration;
  }): Promise<TopicPartitionMap<bigint>>;
  endOffsets(options: unknown): Promise<TopicPartitionMap<bigint>> {
    return calls.endOffsets(this.#handle, options, arguments.length);
  }

  currentLag(options: {
    /** The partition to get the lag for. */
    topicPartition: TopicPartition;
  }): Promise<bigint | null>;
  currentLag(topicPartition: TopicPartition): Promise<bigint | null>;
  currentLag(options: unknown): Promise<bigint | null> {
    return calls.currentLag(this.#handle, options, arguments.length);
  }

  /**
   * @remarks Java's mock's dummy metadata, `dummy.group.id`, generation 1,
   * member `1`: the mock needs no `group.id`.
   */
  groupMetadata(): ConsumerGroupMetadata {
    return calls.groupMetadata(this.#handle, arguments.length, true);
  }

  close(): Promise<void>;
  close(options: {
    /** See `CloseOptions`; cannot be `null`. */
    option: CloseOptions;
  }): Promise<void>;
  close(option: CloseOptions): Promise<void>;
  close(options?: unknown): Promise<void> {
    return calls.close(this.#handle, options, arguments.length, 'MockConsumer');
  }

  wakeup(): void {
    calls.wakeup(this.#handle, arguments.length);
  }

  /**
   * Simulate a rebalance event.
   *
   * @param options - the new assignment
   * @returns settling once the rebalance ran
   * @throws `IllegalArgumentError` rejects with it if the given parameters
   *   are not `{newAssignment}`
   * @throws `TypeError` rejects with it if `newAssignment` is not an iterable
   *   of `TopicPartition`, or `undefined`
   */
  rebalance(options: {
    /** The new assignment. */
    newAssignment: Iterable<TopicPartition>;
  }): Promise<void>;
  /**
   * Simulate a rebalance event.
   *
   * @param newAssignment - The new assignment
   * @returns settling once the rebalance ran
   */
  rebalance(newAssignment: Iterable<TopicPartition>): Promise<void>;
  rebalance(options: unknown): Promise<void> {
    const count = arguments.length;
    return calls.rejecting(() => {
      REBALANCE_FORMS.match(options, count);
      const pairs = calls.partitionPairs(REBALANCE_FORMS.value(options, 'newAssignment'), false);
      return calls.completion((done) => native.mockConsumerRebalance(this.#handle, pairs, done));
    });
  }

  /**
   * Simulates a partition loss event. Calls
   * `ConsumerRebalanceListener#onPartitionsLost` for the specified partitions
   * and removes them from the current assignment. Unlike `rebalance`, which
   * calls `ConsumerRebalanceListener#onPartitionsRevoked`, this method models
   * the case where the consumer loses partitions without a graceful revoke.
   *
   * Only records belonging to the lost partitions are cleared; records for
   * retained partitions are unaffected.
   *
   * @param options - the partitions to lose; all must be currently assigned
   * @returns settling once the partitions are lost
   * @throws `IllegalStateError` rejects with it if any partition is not
   *   currently assigned
   * @throws `IllegalArgumentError` rejects with it if the given parameters
   *   are not `{partitionsLost}`
   * @throws `TypeError` rejects with it if `partitionsLost` is not an
   *   iterable of `TopicPartition`, or `undefined`
   */
  losePartitions(options: {
    /** The partitions to lose; all must be currently assigned. */
    partitionsLost: Iterable<TopicPartition>;
  }): Promise<void>;
  /**
   * Simulates a partition loss event: the partitions on their own, the same
   * as `{partitionsLost}`.
   *
   * @param partitionsLost - The partitions to lose; all must be currently assigned
   * @returns settling once the partitions are lost
   */
  losePartitions(partitionsLost: Iterable<TopicPartition>): Promise<void>;
  losePartitions(options: unknown): Promise<void> {
    const count = arguments.length;
    return calls.rejecting(() => {
      LOSE_PARTITIONS_FORMS.match(options, count);
      const pairs = calls.partitionPairs(LOSE_PARTITIONS_FORMS.value(options, 'partitionsLost'), false);
      return calls.completion((done) => native.mockConsumerLosePartitions(this.#handle, pairs, done));
    });
  }

  /**
   * Adds a record to be returned when `poll` is called.
   *
   * @param options - the record to add
   * @throws `IllegalStateError` if the partition is not assigned to the
   *   consumer
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{record}`
   * @throws `TypeError` when `record` is not a `ConsumerRecord`, or
   *   `undefined`
   *
   * @remarks `poll` returns this very record.
   */
  addRecord(options: {
    /** The record to add. */
    record: ConsumerRecord<K, V>;
  }): void;
  /**
   * Adds a record to be returned when `poll` is called: the record on its
   * own, the same as `{record}`.
   *
   * @param record - The record to add
   */
  addRecord(record: ConsumerRecord<K, V>): void;
  addRecord(options: unknown): void {
    ADD_RECORD_FORMS.match(options, arguments.length);
    const record = ADD_RECORD_FORMS.value(options, 'record');
    if (record !== null && !(record instanceof ConsumerRecord)) throw new TypeError('record must be a ConsumerRecord');
    if (record === null) {
      // A null record goes to the FFI, which refuses it.
      native.mockConsumerAddRecord(this.#handle, null, null, 0, 0n, -1);
      return;
    }
    native.mockConsumerAddRecord(this.#handle, record, record.topic, record.partition, record.offset, record.leaderEpoch ?? -1);
  }

  /**
   * Sets the maximum number of records returned in a single call to `poll`.
   *
   * @param options - the max.poll.records
   * @throws `IllegalArgumentError` when `maxPollRecords` is less than 1, the
   *   core's, or the given parameters are not `{maxPollRecords}`
   * @throws `TypeError` when `maxPollRecords` is not a bigint or a whole
   *   number, or `undefined`
   */
  setMaxPollRecords(options: {
    /** The max.poll.records: a bigint, or a whole number up to 2^53 - 1. */
    maxPollRecords: bigint | number;
  }): void;
  /**
   * Sets the maximum number of records returned in a single call to `poll`:
   * the number on its own, the same as `{maxPollRecords}`.
   *
   * @param maxPollRecords - The max.poll.records
   */
  setMaxPollRecords(maxPollRecords: bigint | number): void;
  setMaxPollRecords(options: unknown): void {
    SET_MAX_POLL_RECORDS_FORMS.match(options, arguments.length);
    const maxPollRecords = long.take(SET_MAX_POLL_RECORDS_FORMS.value(options, 'maxPollRecords'), 'maxPollRecords') as bigint;
    native.mockConsumerSetMaxPollRecords(this.#handle, maxPollRecords);
  }

  /**
   * Sets an error to throw when `poll` is called.
   *
   * @param options - the error to throw, or null for none
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{exception}`
   * @throws `TypeError` when `exception` is neither a `KafkaError` nor null,
   *   or `undefined`
   */
  setPollException(options: {
    /** The error to throw, or null for none. */
    exception: KafkaError | null;
  }): void;
  /**
   * Sets an error to throw when `poll` is called.
   *
   * @param exception - The error to throw, or null for none
   */
  setPollException(exception: KafkaError | null): void;
  setPollException(options: unknown): void {
    SET_POLL_FORMS.match(options, arguments.length);
    native.mockConsumerSetError(this.#handle, 'poll', checkKafkaError(SET_POLL_FORMS.value(options, 'exception')));
  }

  /**
   * Sets an error to throw when offset-related methods are called.
   *
   * @param options - the error to throw, or null for none
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{exception}`
   * @throws `TypeError` when `exception` is neither a `KafkaError` nor null,
   *   or `undefined`
   */
  setOffsetsException(options: {
    /** The error to throw, or null for none. */
    exception: KafkaError | null;
  }): void;
  /**
   * Sets an error to throw when offset-related methods are called.
   *
   * @param exception - The error to throw, or null for none
   */
  setOffsetsException(exception: KafkaError | null): void;
  setOffsetsException(options: unknown): void {
    SET_OFFSETS_FORMS.match(options, arguments.length);
    native.mockConsumerSetError(this.#handle, 'offsets', checkKafkaError(SET_OFFSETS_FORMS.value(options, 'exception')));
  }

  /**
   * Updates the beginning offsets for the specified partitions.
   *
   * @param options - the beginning offsets to set
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{newOffsets}`
   * @throws `TypeError` when `newOffsets` is not a `TopicPartitionMap`, holds
   *   a null offset, or is `undefined`
   */
  updateBeginningOffsets(options: {
    /** The beginning offsets to set: bigints, or whole numbers up to 2^53 - 1. */
    newOffsets: TopicPartitionMap<bigint | number>;
  }): void;
  /**
   * Updates the beginning offsets for the specified partitions.
   *
   * @param newOffsets - The beginning offsets to set
   */
  updateBeginningOffsets(newOffsets: TopicPartitionMap<bigint | number>): void;
  updateBeginningOffsets(options: unknown): void {
    UPDATE_BEGINNING_FORMS.match(options, arguments.length);
    native.mockConsumerUpdateOffsets(this.#handle, 'beginning', offsetEntries(UPDATE_BEGINNING_FORMS.value(options, 'newOffsets')));
  }

  /**
   * Updates the end offsets for the specified partitions.
   *
   * @param options - the end offsets to set
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{newOffsets}`
   * @throws `TypeError` when `newOffsets` is not a `TopicPartitionMap`, holds
   *   a null offset, or is `undefined`
   */
  updateEndOffsets(options: {
    /** The end offsets to set: bigints, or whole numbers up to 2^53 - 1. */
    newOffsets: TopicPartitionMap<bigint | number>;
  }): void;
  /**
   * Updates the end offsets for the specified partitions.
   *
   * @param newOffsets - The end offsets to set
   */
  updateEndOffsets(newOffsets: TopicPartitionMap<bigint | number>): void;
  updateEndOffsets(options: unknown): void {
    UPDATE_END_FORMS.match(options, arguments.length);
    native.mockConsumerUpdateOffsets(this.#handle, 'end', offsetEntries(UPDATE_END_FORMS.value(options, 'newOffsets')));
  }

  /**
   * Updates the duration-based reset offsets for the specified partitions.
   *
   * @param options - the duration offsets to set
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{newOffsets}`
   * @throws `TypeError` when `newOffsets` is not a `TopicPartitionMap`, holds
   *   a null offset, or is `undefined`
   */
  updateDurationOffsets(options: {
    /** The duration offsets to set: bigints, or whole numbers up to 2^53 - 1. */
    newOffsets: TopicPartitionMap<bigint | number>;
  }): void;
  /**
   * Updates the duration-based reset offsets for the specified partitions.
   *
   * @param newOffsets - The duration offsets to set
   */
  updateDurationOffsets(newOffsets: TopicPartitionMap<bigint | number>): void;
  updateDurationOffsets(options: unknown): void {
    UPDATE_DURATION_FORMS.match(options, arguments.length);
    native.mockConsumerUpdateOffsets(this.#handle, 'duration', offsetEntries(UPDATE_DURATION_FORMS.value(options, 'newOffsets')));
  }

  /**
   * Updates the partition information for the specified topic.
   *
   * @param options - the topic to update, and the partition information
   * @throws `IllegalStateError` when the mock is closed, the core's
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{topic, partitions}`
   * @throws `TypeError` when `topic` or `partitions` is null, `partitions` is
   *   not an iterable of `PartitionInfo`, or either is `undefined`
   */
  updatePartitions(options: {
    /** The topic to update. */
    topic: string;
    /** The partition information. */
    partitions: Iterable<PartitionInfo>;
  }): void;
  updatePartitions(options: unknown): void {
    UPDATE_PARTITIONS_FORMS.match(options, arguments.length);
    const topic = UPDATE_PARTITIONS_FORMS.value(options, 'topic');
    if (topic !== null && typeof topic !== 'string') throw new TypeError('topic must be a string');
    native.mockConsumerUpdatePartitions(this.#handle, topic, partitionInfoArgsOf(UPDATE_PARTITIONS_FORMS.value(options, 'partitions')));
  }

  /**
   * Returns whether this consumer has been closed.
   *
   * @returns true if closed, false otherwise
   * @throws `TypeError` when it is given an argument
   */
  closed(): boolean {
    CLOSED_FORMS.match(undefined, arguments.length);
    return native.mockConsumerClosed(this.#handle);
  }

  /**
   * Schedules a no-op task to be executed during a poll invocation.
   *
   * @throws `TypeError` when it is given an argument
   */
  scheduleNopPollTask(): void {
    SCHEDULE_NOP_FORMS.match(undefined, arguments.length);
    native.mockConsumerScheduleNopPollTask(this.#handle);
  }

  /**
   * Returns the timeout used in the last `poll` call.
   *
   * @returns the last poll timeout, in milliseconds (`Infinity` for no
   *   limit), or null if poll has not been called
   * @throws `TypeError` when it is given an argument
   */
  lastPollTimeout(): number | null {
    LAST_POLL_TIMEOUT_FORMS.match(undefined, arguments.length);
    const timeout = native.mockConsumerLastPollTimeout(this.#handle);
    // Java's null (no poll yet) crosses as -1; Long.MAX_VALUE is Infinity.
    if (timeout < 0n) return null;
    return timeout === UNBOUNDED_MILLIS ? Infinity : Number(timeout);
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }
}
