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

// Java's KafkaConsumer over the C FFI's KafkaConsumer with NULL
// deserializers: keys and values cross as bytes and come back as Buffers over
// the fetched memory (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Serialization). The method bodies are src/internal/consumer_calls.ts's.

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way (the addon is loaded there alone, javascript/CLAUDE.md "Files and layout")
import native = require('../internal/native');
import { JavaForms } from '../internal/args';
import { checkConfigs, configEntries } from '../internal/config';
import * as calls from '../internal/consumer_calls';
import { debug } from '../internal/debug';
import { warnPreview } from '../internal/lifecycle';
import type { Metric } from '../common/metric';
import type { MetricNameMap } from '../common/metric_name_map';
import type { PartitionInfo } from '../common/partition_info';
import type { TopicPartition } from '../common/topic_partition';
import type { TopicPartitionMap } from '../common/topic_partition_map';
import type { TopicPartitionSet } from '../common/topic_partition_set';
import type { Duration } from '../duration';
import { UnsupportedOperationError } from '../unsupported_operation_error';
import type { CloseOptions } from './close_options';
import type { Consumer } from './consumer';
import type { ConsumerRebalanceListener } from './consumer_rebalance_listener';
import type { OffsetCommitCallback } from './offset_commit_callback';
import type { ConsumerConfigs } from './consumer_configs';
import type { ConsumerGroupMetadata } from './consumer_group_metadata';
import type { ConsumerRecords } from './consumer_records';
import type { OffsetAndMetadata } from './offset_and_metadata';
import type { OffsetAndTimestamp } from './offset_and_timestamp';
import type { SubscriptionPattern } from './subscription_pattern';

// The constructor's Map and Properties forms are the one `configs`, also taken
// bare (src/internal/args.ts).
const CONSTRUCTOR_FORMS = new JavaForms('KafkaConsumer', [['configs'], ['configs', 'keyDeserializer', 'valueDeserializer']], {
  bareIsPlainObject: true,
  bareTypes: { configs: (value) => typeof value === 'object' },
});

// Until the client has deserializers, a consumer takes none: keys and values
// are Buffers, and a deserializer is refused rather than ignored.
const NO_DESERIALIZERS = 'KafkaConsumer takes no deserializer yet: keys and values are Buffers';

/**
 * A Kafka client that consumes records from a Kafka cluster.
 *
 * @typeParam K - the type of the keys: `Buffer`
 * @typeParam V - the type of the values: `Buffer`
 *
 * @remarks Every waiting method returns a Promise, and the consumer takes one
 * call at a time, as Java's does: a call made while another is pending
 * rejects with `ConcurrentModificationError` ("KafkaConsumer is not safe for
 * multi-threaded access."), `wakeup()` and `metrics()` excepted. A consumer
 * keeps Node running from its creation until `close()` closes it, and no
 * garbage collection closes one: close every consumer (`close()`, or `await
 * using`). *(deviation: Java's client threads are daemon threads, so a
 * program can exit with a client still open. Why: Node exits in the middle of
 * an `await` when nothing holds it, so this way no pending poll is cut off.)*
 * Every consumer needs `group.protocol: 'consumer'`: the classic protocol is
 * not supported.
 */
export class KafkaConsumer<K = Buffer, V = Buffer> implements Consumer<K, V> {
  #handle: calls.ConsumerHandle;
  // Whether group.id was given, for the temporary group.id check
  // (javascript/CLAUDE.md, JavaScript Binding Conventions, Errors).
  readonly #groupIdDefined: boolean;

  /**
   * A consumer is instantiated by providing a set of key-value pairs as
   * configuration. Valid configuration strings are documented in Java's
   * ConsumerConfig.
   *
   * @param options - The consumer configs
   * @throws `KafkaError` when the configuration is invalid (`ConfigError`,
   *   the core's, with Java's message naming the key) or the consumer cannot
   *   be built
   * @throws `UnsupportedOperationError` when `group.protocol` is `classic`,
   *   the core's error, or deserializers are given, as arguments or through
   *   `key.deserializer` or `value.deserializer`
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{configs}`
   * @throws `TypeError` when `configs` is null, `undefined` or not a plain
   *   object (a `Map` included), or a config key or value holds a null byte
   *
   * @remarks Java's `Map` and `Properties` forms are `configs`. The forms
   * taking deserializers are not in this release: keys and values are
   * Buffers. Once per process it emits an `ExperimentalWarning`.
   */
  constructor(options: {
    /** The consumer configs: a plain object of Java's ConsumerConfig keys. */
    configs: ConsumerConfigs;
  });
  /**
   * A consumer is instantiated by providing a set of key-value pairs as
   * configuration: the configs on their own, the same as `{configs}`. A plain
   * object is read as `{configs}` when it has the key `configs`.
   *
   * @param configs - The consumer configs
   */
  constructor(configs: ConsumerConfigs);
  constructor(options: unknown) {
    const form = CONSTRUCTOR_FORMS.match(options, arguments.length);
    const configs = checkConfigs(CONSTRUCTOR_FORMS.value(options, 'configs'));
    const keyed = configs['key.deserializer'] !== undefined && configs['key.deserializer'] !== null;
    const valued = configs['value.deserializer'] !== undefined && configs['value.deserializer'] !== null;
    if (form === 1 || keyed || valued) throw new UnsupportedOperationError(NO_DESERIALIZERS);
    this.#groupIdDefined = configs['group.id'] !== undefined && configs['group.id'] !== null;
    this.#handle = native.consumerCreate(configEntries(configs));
    warnPreview();
    debug('KafkaConsumer created');
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
    /** The list of topics to subscribe to. */
    topics: Iterable<string>;
    /**
     * Non-null listener instance to get notifications on partition
     * assignment/revocation for the subscribed topics.
     */
    callback: ConsumerRebalanceListener;
    pattern?: never;
  }): Promise<void>;
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
  subscribe(options: {
    /** Pattern to subscribe to, that must be compatible with Google RE2/J. */
    pattern: SubscriptionPattern;
    topics?: never;
    callback?: never;
  }): Promise<void>;
  subscribe(pattern: SubscriptionPattern): Promise<void>;
  subscribe(options: unknown): Promise<void> {
    return calls.subscribe(this.#handle, options, arguments.length, true);
  }

  assign(options: {
    /** The list of partitions to assign this consumer. */
    partitions: Iterable<TopicPartition>;
  }): Promise<void>;
  assign(partitions: Iterable<TopicPartition>): Promise<void>;
  assign(options: unknown): Promise<void> {
    return calls.assign(this.#handle, options, arguments.length, true);
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
  commitAsync(options: {
    /** Callback to invoke when the commit completes. */
    callback: OffsetCommitCallback | null;
    offsets?: never;
  }): Promise<void>;
  commitAsync(callback: OffsetCommitCallback | null): Promise<void>;
  commitAsync(options: {
    /** A map of offsets by partition with associated metadata, copied by the call. */
    offsets: TopicPartitionMap<OffsetAndMetadata>;
    /** Callback to invoke when the commit completes. */
    callback: OffsetCommitCallback;
  }): Promise<void>;
  commitAsync(options?: unknown): Promise<void> {
    return calls.commitAsync(this.#handle, options, arguments.length, false);
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
    return calls.committed(this.#handle, options, arguments.length, this.#groupIdDefined);
  }

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

  groupMetadata(): ConsumerGroupMetadata {
    return calls.groupMetadata(this.#handle, arguments.length, this.#groupIdDefined);
  }

  close(): Promise<void>;
  close(options: {
    /** See `CloseOptions`; cannot be `null`. */
    option: CloseOptions;
  }): Promise<void>;
  close(option: CloseOptions): Promise<void>;
  close(options?: unknown): Promise<void> {
    return calls.close(this.#handle, options, arguments.length, 'KafkaConsumer');
  }

  wakeup(): void {
    calls.wakeup(this.#handle, arguments.length);
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }
}
