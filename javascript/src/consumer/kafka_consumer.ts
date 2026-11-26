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
// Serialization).

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way (the addon is loaded there alone, javascript/CLAUDE.md "Files and layout")
import native = require('../internal/native');
import { JavaForms } from '../internal/args';
import { checkConfigs, configEntries } from '../internal/config';
import { debug } from '../internal/debug';
import { durationToMillis, isDurationOrNull } from '../internal/duration';
import { warnPreview } from '../internal/lifecycle';
import { liveMetrics } from '../internal/live_metrics';
import { partitionInfoOf } from '../internal/metadata';
import {
  long,
  stringSet,
  topicPartition as topicPartitionKind,
  topicPartitionList,
  topicPartitionLongMap,
  topicPartitionOffsetMap,
} from '../internal/payload';
import type { Metric } from '../common/metric';
import type { MetricNameMap } from '../common/metric_name_map';
import type { PartitionInfo } from '../common/partition_info';
import { TopicPartition } from '../common/topic_partition';
import { TopicPartitionMap } from '../common/topic_partition_map';
import { TopicPartitionSet } from '../common/topic_partition_set';
import type { Duration } from '../duration';
import { InvalidGroupIdError } from '../common/errors/invalid_group_id_error';
import { UnsupportedOperationError } from '../unsupported_operation_error';
import { CloseOptions } from './close_options';
import type { Consumer } from './consumer';
import type { ConsumerConfigs } from './consumer_configs';
import { ConsumerGroupMetadata } from './consumer_group_metadata';
import { ConsumerRecord } from './consumer_record';
import { ConsumerRecords } from './consumer_records';
import { OffsetAndMetadata } from './offset_and_metadata';
import { OffsetAndTimestamp } from './offset_and_timestamp';
import { SubscriptionPattern } from './subscription_pattern';

/** The native consumer handle (`internal/native.ts`). */
type ConsumerHandle = ReturnType<typeof native.consumerCreate>;

/** A committed offset as the addon reads it: topic, partition, then offset, leader epoch and metadata, or null for none. */
type NativeOffsetEntry = [string, number, bigint | null, number?, string?];

/** A partition's fields, as the addon reads them (`internal/native.ts`). */
type NativePartitionInfo = Parameters<typeof partitionInfoOf>[0];

/** An offset found for a time, as the addon reads it: topic, partition, then offset, timestamp and leader epoch, or null for none. */
type NativeTimestampEntry = [string, number, bigint | null, bigint?, number?];

/** An offset as the addon reads it: topic, partition, offset. */
type NativeLongEntry = [string, number, bigint];

/**
 * Whether `value` is an iterable object, which a bare collection is.
 *
 * @param value - the value given
 * @returns whether it is one
 */
function isIterable(value: unknown): boolean {
  return typeof value === 'object' && value !== null && typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] === 'function';
}

// Java's forms of each method, in Java declaration order (src/internal/args.ts).
// The constructor's Map and Properties forms are the one `configs`, also taken
// bare. Java's subscribe(Pattern...) forms are left out, and so are, until the
// client has rebalance listeners and commit callbacks, the forms taking one.
const CONSTRUCTOR_FORMS = new JavaForms('KafkaConsumer', [['configs'], ['configs', 'keyDeserializer', 'valueDeserializer']], {
  bareIsPlainObject: true,
  bareTypes: { configs: (value) => typeof value === 'object' },
});
const SUBSCRIBE_FORMS = new JavaForms('subscribe', [['topics'], ['pattern']], {
  bareTypes: {
    topics: (value) => isIterable(value) && !(value instanceof SubscriptionPattern),
    pattern: (value) => value instanceof SubscriptionPattern,
  },
});
const partitionsForms = (name: string): JavaForms =>
  new JavaForms(name, [['partitions']], { bareTypes: { partitions: (value) => value === null || isIterable(value) } });
const ASSIGN_FORMS = partitionsForms('assign');
const PAUSE_FORMS = partitionsForms('pause');
const RESUME_FORMS = partitionsForms('resume');
const POLL_FORMS = new JavaForms('poll', [['timeout']], { bareTypes: { timeout: isDurationOrNull } });
const CLOSE_FORMS = new JavaForms('close', [[], ['option']], {
  bareTypes: { option: (value) => value === null || value instanceof CloseOptions },
});
const ASSIGNMENT_FORMS = new JavaForms('assignment', [[]], { reader: true });
const SUBSCRIPTION_FORMS = new JavaForms('subscription', [[]], { reader: true });
const PAUSED_FORMS = new JavaForms('paused', [[]], { reader: true });
const METRICS_FORMS = new JavaForms('metrics', [[]], { reader: true });
const UNSUBSCRIBE_FORMS = new JavaForms('unsubscribe', [[]]);
const WAKEUP_FORMS = new JavaForms('wakeup', [[]]);
// A bare null fits both one-parameter forms of commitSync, so neither takes it.
const COMMIT_SYNC_FORMS = new JavaForms('commitSync', [[], ['timeout'], ['offsets'], ['offsets', 'timeout']], {
  order: ['offsets', 'timeout'],
  bareTypes: {
    timeout: (value) => value !== null && isDurationOrNull(value),
    offsets: (value) => value instanceof TopicPartitionMap || value instanceof Map,
  },
});
const COMMIT_ASYNC_FORMS = new JavaForms('commitAsync', [[]]);
const SEEK_FORMS = new JavaForms('seek', [
  ['partition', 'offset'],
  ['partition', 'offsetAndMetadata'],
]);
const SEEK_TO_BEGINNING_FORMS = partitionsForms('seekToBeginning');
const SEEK_TO_END_FORMS = partitionsForms('seekToEnd');
const isPartitionOrNull = (value: unknown): boolean => value === null || value instanceof TopicPartition;
const POSITION_FORMS = new JavaForms('position', [['partition'], ['partition', 'timeout']], {
  bareTypes: { partition: isPartitionOrNull },
});
const COMMITTED_FORMS = new JavaForms('committed', [['partitions'], ['partitions', 'timeout']], {
  bareTypes: { partitions: (value) => value === null || isIterable(value) },
});
const CURRENT_LAG_FORMS = new JavaForms('currentLag', [['topicPartition']], { bareTypes: { topicPartition: isPartitionOrNull } });
const GROUP_METADATA_FORMS = new JavaForms('groupMetadata', [[]], { reader: true });
const PARTITIONS_FOR_FORMS = new JavaForms('partitionsFor', [['topic'], ['topic', 'timeout']], {
  bareTypes: { topic: (value) => value === null || typeof value === 'string' },
});
const LIST_TOPICS_FORMS = new JavaForms('listTopics', [[], ['timeout']], { bareTypes: { timeout: isDurationOrNull } });
const OFFSETS_FOR_TIMES_FORMS = new JavaForms('offsetsForTimes', [['timestampsToSearch'], ['timestampsToSearch', 'timeout']], {
  bareTypes: { timestampsToSearch: (value) => value === null || value instanceof TopicPartitionMap || value instanceof Map },
});
const BEGINNING_OFFSETS_FORMS = new JavaForms('beginningOffsets', [['partitions'], ['partitions', 'timeout']], {
  bareTypes: { partitions: (value) => value === null || isIterable(value) },
});
const END_OFFSETS_FORMS = new JavaForms('endOffsets', [['partitions'], ['partitions', 'timeout']], {
  bareTypes: { partitions: (value) => value === null || isIterable(value) },
});
const ENFORCE_REBALANCE_FORMS = new JavaForms('enforceRebalance', [[], ['reason']], {
  bareTypes: { reason: (value) => value === null || typeof value === 'string' },
});

// Until the client has deserializers, a consumer takes none: keys and values
// are Buffers, and a deserializer is refused rather than ignored.
const NO_DESERIALIZERS = 'KafkaConsumer takes no deserializer yet: keys and values are Buffers';

// The message of Java's AsyncKafkaConsumer#throwIfGroupIdNotDefined.
const NO_GROUP_ID = 'To use the group management or offset commit APIs, you must provide a valid group.id in the consumer configuration.';

// What the addon builds each record of a poll with.
const RECORD_FACTORY: typeof ConsumerRecord.internalCreate = (...fields) => ConsumerRecord.internalCreate(...fields);

/**
 * A Promise settled by a native completion that answers with an error or
 * nothing.
 *
 * @param start - starts the native call with its completion
 * @returns settling once the completion ran
 */
function completion(start: (done: (error: unknown) => void) => void): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with the error the core reported, a client Error src/internal/errors.ts built (typed unknown at the native boundary)
    start((error) => (error !== null ? reject(error) : resolve()));
  });
}

/**
 * A Promise of `run()`, rejecting with what it throws: a method returning a
 * Promise never throws.
 *
 * @param run - the method's body
 * @returns its Promise
 */
function rejecting<T>(run: () => Promise<T>): Promise<T> {
  try {
    return run();
  } catch (error) {
    // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what the checks threw: an Error of the client's or JavaScript's (typed unknown by catch)
    return Promise.reject(error);
  }
}

/**
 * The partitions as the addon takes them: `[topic, partition]` pairs, a null
 * element as null, or null for a null collection.
 *
 * @param value - the partitions given
 * @param blankNull - whether a null partition or topic is a blank topic, as
 *   Java's `assign` reads one
 * @returns the pairs
 */
function partitionPairs(value: unknown, blankNull: boolean): Array<[string | null, number] | null> | null {
  const items = topicPartitionList.take(value, 'partitions') as Array<TopicPartition | null> | null;
  if (items === null) return null;
  return items.map((tp) => {
    // Java's assign checks isBlank(tp != null ? tp.topic() : null): a blank
    // topic gets the core's message for it.
    if (blankNull && (tp === null || tp.topic === null)) return ['', tp === null ? 0 : tp.partition];
    return tp === null ? null : [tp.topic, tp.partition];
  });
}

/**
 * A partition as the addon takes it: a `[topic, partition]` pair, or null.
 *
 * @param value - the partition given
 * @param name - the parameter's name, for the error
 * @returns the pair
 */
function partitionPair(value: unknown, name: string): [string | null, number] | null {
  const tp = topicPartitionKind.take(value, name) as TopicPartition | null;
  return tp === null ? null : [tp.topic, tp.partition];
}

/**
 * An `OffsetAndMetadata` as the addon takes it: `[offset, leaderEpoch (-1 for
 * none), metadata]`.
 *
 * @param om - the offset and its metadata
 * @returns the fields
 */
function offsetFields(om: OffsetAndMetadata): [bigint, number, string] {
  return [om.offset, om.leaderEpoch ?? -1, om.metadata];
}

/**
 * The timeout of a timed form as the addon takes it, or null for the form
 * without one: a negative one goes to the FFI, which rejects it with the
 * core's message (javascript/CLAUDE.md, JavaScript Binding Conventions,
 * Signatures).
 *
 * @param forms - the method's forms
 * @param options - the argument the forms matched
 * @param timed - whether the matched form has a timeout
 * @returns the milliseconds, or null
 */
function timeoutOf(forms: JavaForms, options: unknown, timed: boolean): number | bigint | null {
  return timed ? durationToMillis(forms.value(options, 'timeout'), 'timeout') : null;
}

/**
 * A Promise settled by a native completion that answers with an error or a
 * value.
 *
 * @param start - starts the native call with its completion
 * @returns resolving to the value
 */
function answer<T>(start: (done: (error: unknown, value: T | null) => void) => void): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with the error the core reported, a client Error src/internal/errors.ts built (typed unknown at the native boundary)
    start((error, value) => (error !== null ? reject(error) : resolve(value as T)));
  });
}

/**
 * The addon's `[topic, partition, offset]` entries as a `TopicPartitionMap<bigint>`.
 *
 * @param entries - the entries
 * @returns the map
 */
function longMap(entries: NativeLongEntry[]): TopicPartitionMap<bigint> {
  const map = new TopicPartitionMap<bigint>();
  for (const [topic, partition, offset] of entries) map.set(new TopicPartition({ topic, partition }), offset);
  return map;
}

/**
 * A `TopicPartitionSet` of the addon's `[topic, partition]` pairs.
 *
 * @param pairs - the pairs
 * @returns the set
 */
function partitionSet(pairs: Array<[string, number]>): TopicPartitionSet {
  return new TopicPartitionSet(pairs.map(([topic, partition]) => new TopicPartition({ topic, partition })));
}

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
  #handle: ConsumerHandle;
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
    ASSIGNMENT_FORMS.match(undefined, arguments.length);
    return partitionSet(native.consumerAssignment(this.#handle));
  }

  subscription(): Set<string> {
    SUBSCRIPTION_FORMS.match(undefined, arguments.length);
    return new Set(native.consumerSubscription(this.#handle));
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
    const count = arguments.length;
    return rejecting(() => {
      // A RegExp is no pattern: only a SubscriptionPattern is (javascript/CLAUDE.md,
      // JavaScript Binding Conventions, Signatures).
      const given = options instanceof RegExp ? options : (options as { pattern?: unknown } | null)?.pattern;
      if (given instanceof RegExp) {
        throw new TypeError("pattern must be a SubscriptionPattern, not a RegExp: new SubscriptionPattern({ pattern: '...' })");
      }
      if (SUBSCRIBE_FORMS.match(options, count) === 0) {
        const topics = stringSet.take(SUBSCRIBE_FORMS.value(options, 'topics'), 'topics') as Set<string | null> | null;
        // A null topic is a blank one, which the core refuses with Java's
        // message for both.
        const list = topics === null ? null : [...topics].map((topic) => topic ?? '');
        return completion((done) => native.consumerSubscribeTopics(this.#handle, list, done));
      }
      const pattern = SUBSCRIBE_FORMS.value(options, 'pattern');
      if (pattern !== null && !(pattern instanceof SubscriptionPattern)) throw new TypeError('pattern must be a SubscriptionPattern');
      return completion((done) =>
        native.consumerSubscribePattern(this.#handle, pattern !== null, pattern === null ? null : pattern.pattern(), done),
      );
    });
  }

  assign(options: {
    /** The list of partitions to assign this consumer. */
    partitions: Iterable<TopicPartition>;
  }): Promise<void>;
  assign(partitions: Iterable<TopicPartition>): Promise<void>;
  assign(options: unknown): Promise<void> {
    const count = arguments.length;
    return rejecting(() => {
      ASSIGN_FORMS.match(options, count);
      const pairs = partitionPairs(ASSIGN_FORMS.value(options, 'partitions'), true);
      return completion((done) => native.consumerAssign(this.#handle, pairs, done));
    });
  }

  unsubscribe(): Promise<void> {
    const count = arguments.length;
    return rejecting(() => {
      UNSUBSCRIBE_FORMS.match(undefined, count);
      return completion((done) => native.consumerUnsubscribe(this.#handle, done));
    });
  }

  poll(options: {
    /** The maximum time to block (must not be greater than `Long.MAX_VALUE` milliseconds). */
    timeout: Duration;
  }): Promise<ConsumerRecords<K, V>>;
  poll(timeout: Duration): Promise<ConsumerRecords<K, V>>;
  poll(options: unknown): Promise<ConsumerRecords<K, V>> {
    const count = arguments.length;
    return rejecting(() => {
      POLL_FORMS.match(options, count);
      // A negative timeout goes to the FFI, which rejects it with the core's
      // message (javascript/CLAUDE.md, JavaScript Binding Conventions, Signatures).
      const timeoutMs = durationToMillis(POLL_FORMS.value(options, 'timeout'), 'timeout');
      return new Promise<ConsumerRecords<K, V>>((resolve, reject) => {
        native.consumerPoll(this.#handle, timeoutMs, RECORD_FACTORY, (error, partitions, nextOffsets) => {
          // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with the error the core reported, a client Error src/internal/errors.ts built (typed unknown at the native boundary)
          if (error !== null || partitions === null || nextOffsets === null) return reject(error);
          const records = new TopicPartitionMap<ConsumerRecord<K, V>[]>();
          for (const [topic, partition, list] of partitions) {
            records.set(new TopicPartition({ topic, partition }), list as ConsumerRecord<K, V>[]);
          }
          const next = new TopicPartitionMap<OffsetAndMetadata>();
          for (const [topic, partition, offset, leaderEpoch, metadata] of nextOffsets) {
            next.set(
              new TopicPartition({ topic, partition }),
              new OffsetAndMetadata({ offset, leaderEpoch: leaderEpoch < 0 ? null : leaderEpoch, metadata }),
            );
          }
          resolve(new ConsumerRecords<K, V>({ records, nextOffsets: next }));
        });
      });
    });
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
    const count = arguments.length;
    return rejecting(() => {
      const form = COMMIT_SYNC_FORMS.match(options, count);
      const withOffsets = form >= 2;
      let entries: Array<[[string | null, number] | null, [bigint, number, string] | null]> | null = null;
      if (withOffsets) {
        const offsets = topicPartitionOffsetMap.take(
          COMMIT_SYNC_FORMS.value(options, 'offsets'),
          'offsets',
        ) as TopicPartitionMap<OffsetAndMetadata | null> | null;
        // A null map, key or value goes to the FFI as NULL, which refuses it.
        entries =
          offsets === null
            ? null
            : [...offsets.entries()].map(([tp, om]) => [
                tp === null ? null : [tp.topic, tp.partition],
                om === null ? null : offsetFields(om),
              ]);
      }
      const timeoutMs = timeoutOf(COMMIT_SYNC_FORMS, options, form === 1 || form === 3);
      return completion((done) => native.consumerCommitSync(this.#handle, withOffsets, entries, timeoutMs, done));
    });
  }

  commitAsync(): Promise<void>;
  commitAsync(options?: unknown): Promise<void> {
    const count = arguments.length;
    return rejecting(() => {
      // The argument is read, so a callback, whose forms are not in this
      // release, gets the form error.
      COMMIT_ASYNC_FORMS.match(options, count);
      return completion((done) => native.consumerCommitAsync(this.#handle, done));
    });
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
    const count = arguments.length;
    return rejecting(() => {
      const withMetadata = SEEK_FORMS.match(options, count) === 1;
      const partition = partitionPair(SEEK_FORMS.value(options, 'partition'), 'partition');
      if (!withMetadata) {
        // A negative offset goes to the FFI, which refuses it with Java's message.
        const offset = long.take(SEEK_FORMS.value(options, 'offset'), 'offset') as bigint;
        return completion((done) => native.consumerSeek(this.#handle, partition, false, offset, done));
      }
      const om = SEEK_FORMS.value(options, 'offsetAndMetadata');
      if (om !== null && !(om instanceof OffsetAndMetadata)) throw new TypeError('offsetAndMetadata must be an OffsetAndMetadata');
      const fields = om === null ? null : offsetFields(om);
      return completion((done) => native.consumerSeek(this.#handle, partition, true, fields, done));
    });
  }

  seekToBeginning(options: {
    /** The partitions to seek to their first offset. */
    partitions: Iterable<TopicPartition>;
  }): Promise<void>;
  seekToBeginning(partitions: Iterable<TopicPartition>): Promise<void>;
  seekToBeginning(options: unknown): Promise<void> {
    const count = arguments.length;
    return rejecting(() => {
      SEEK_TO_BEGINNING_FORMS.match(options, count);
      const pairs = partitionPairs(SEEK_TO_BEGINNING_FORMS.value(options, 'partitions'), false);
      return completion((done) => native.consumerSeekToBeginning(this.#handle, pairs, done));
    });
  }

  seekToEnd(options: {
    /** The partitions to seek to their last offset. */
    partitions: Iterable<TopicPartition>;
  }): Promise<void>;
  seekToEnd(partitions: Iterable<TopicPartition>): Promise<void>;
  seekToEnd(options: unknown): Promise<void> {
    const count = arguments.length;
    return rejecting(() => {
      SEEK_TO_END_FORMS.match(options, count);
      const pairs = partitionPairs(SEEK_TO_END_FORMS.value(options, 'partitions'), false);
      return completion((done) => native.consumerSeekToEnd(this.#handle, pairs, done));
    });
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
    const count = arguments.length;
    return rejecting(() => {
      const form = POSITION_FORMS.match(options, count);
      const partition = partitionPair(POSITION_FORMS.value(options, 'partition'), 'partition');
      const timeoutMs = timeoutOf(POSITION_FORMS, options, form === 1);
      return answer<bigint>((done) => native.consumerPosition(this.#handle, partition, timeoutMs, done));
    });
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
    const count = arguments.length;
    return rejecting(async () => {
      const form = COMMITTED_FORMS.match(options, count);
      const given = COMMITTED_FORMS.value(options, 'partitions');
      // The temporary group.id check: Java's throwIfGroupIdNotDefined() comes
      // before its dereference of the null, which the FFI would report first.
      if (given === null && !this.#groupIdDefined) throw new InvalidGroupIdError(NO_GROUP_ID);
      const pairs = partitionPairs(given, false);
      const timeoutMs = timeoutOf(COMMITTED_FORMS, options, form === 1);
      const entries = await answer<NativeOffsetEntry[]>((done) => native.consumerCommitted(this.#handle, pairs, timeoutMs, done));
      const map = new TopicPartitionMap<OffsetAndMetadata | null>();
      for (const [topic, partition, offset, leaderEpoch = -1, metadata = ''] of entries) {
        map.set(
          new TopicPartition({ topic, partition }),
          offset === null ? null : new OffsetAndMetadata({ offset, leaderEpoch: leaderEpoch < 0 ? null : leaderEpoch, metadata }),
        );
      }
      return map;
    });
  }

  metrics(): MetricNameMap<Metric> {
    METRICS_FORMS.match(undefined, arguments.length);
    return liveMetrics(native.consumerMetrics(this.#handle));
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
    const count = arguments.length;
    return rejecting(async () => {
      const form = PARTITIONS_FOR_FORMS.match(options, count);
      const topic = PARTITIONS_FOR_FORMS.value(options, 'topic');
      if (topic !== null && typeof topic !== 'string') throw new TypeError('topic must be a string');
      const timeoutMs = timeoutOf(PARTITIONS_FOR_FORMS, options, form === 1);
      const partitions = await answer<NativePartitionInfo[]>((done) => native.consumerPartitionsFor(this.#handle, topic, timeoutMs, done));
      return partitions.map(partitionInfoOf);
    });
  }

  listTopics(): Promise<Map<string, PartitionInfo[]>>;
  listTopics(options: {
    /** The maximum time this operation will block to fetch topic metadata. */
    timeout: Duration;
  }): Promise<Map<string, PartitionInfo[]>>;
  listTopics(timeout: Duration): Promise<Map<string, PartitionInfo[]>>;
  listTopics(options?: unknown): Promise<Map<string, PartitionInfo[]>> {
    const count = arguments.length;
    return rejecting(async () => {
      const form = LIST_TOPICS_FORMS.match(options, count);
      const timeoutMs = timeoutOf(LIST_TOPICS_FORMS, options, form === 1);
      const topics = await answer<Array<[string, NativePartitionInfo[]]>>((done) =>
        native.consumerListTopics(this.#handle, timeoutMs, done),
      );
      return new Map(topics.map(([topic, partitions]) => [topic, partitions.map(partitionInfoOf)]));
    });
  }

  paused(): TopicPartitionSet {
    PAUSED_FORMS.match(undefined, arguments.length);
    return partitionSet(native.consumerPaused(this.#handle));
  }

  pause(options: {
    /** The partitions which should be paused. */
    partitions: Iterable<TopicPartition>;
  }): Promise<void>;
  pause(partitions: Iterable<TopicPartition>): Promise<void>;
  pause(options: unknown): Promise<void> {
    const count = arguments.length;
    return rejecting(() => {
      PAUSE_FORMS.match(options, count);
      const pairs = partitionPairs(PAUSE_FORMS.value(options, 'partitions'), false);
      return completion((done) => native.consumerPause(this.#handle, pairs, done));
    });
  }

  resume(options: {
    /** The partitions which should be resumed. */
    partitions: Iterable<TopicPartition>;
  }): Promise<void>;
  resume(partitions: Iterable<TopicPartition>): Promise<void>;
  resume(options: unknown): Promise<void> {
    const count = arguments.length;
    return rejecting(() => {
      RESUME_FORMS.match(options, count);
      const pairs = partitionPairs(RESUME_FORMS.value(options, 'partitions'), false);
      return completion((done) => native.consumerResume(this.#handle, pairs, done));
    });
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
    const count = arguments.length;
    return rejecting(async () => {
      const form = OFFSETS_FOR_TIMES_FORMS.match(options, count);
      const map = topicPartitionLongMap.take(
        OFFSETS_FOR_TIMES_FORMS.value(options, 'timestampsToSearch'),
        'timestampsToSearch',
      ) as TopicPartitionMap<bigint | null> | null;
      // A null map or key goes to the FFI as NULL, which refuses it. A null
      // timestamp, which Java's unboxing refuses with NullPointerException,
      // has no int64 to cross as: the binding's run-time type check.
      const entries =
        map === null
          ? null
          : [...map.entries()].map(([tp, timestamp]): [[string | null, number] | null, bigint] => {
              if (timestamp === null) throw new TypeError('timestampsToSearch must not contain a null timestamp');
              return [tp === null ? null : [tp.topic, tp.partition], timestamp];
            });
      const timeoutMs = timeoutOf(OFFSETS_FOR_TIMES_FORMS, options, form === 1);
      const found = await answer<NativeTimestampEntry[]>((done) => native.consumerOffsetsForTimes(this.#handle, entries, timeoutMs, done));
      const result = new TopicPartitionMap<OffsetAndTimestamp | null>();
      for (const [topic, partition, offset, timestamp = -1n, leaderEpoch = -1] of found) {
        result.set(
          new TopicPartition({ topic, partition }),
          offset === null ? null : new OffsetAndTimestamp({ offset, timestamp, leaderEpoch: leaderEpoch < 0 ? null : leaderEpoch }),
        );
      }
      return result;
    });
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
    const count = arguments.length;
    return rejecting(async () => {
      const form = BEGINNING_OFFSETS_FORMS.match(options, count);
      const pairs = partitionPairs(BEGINNING_OFFSETS_FORMS.value(options, 'partitions'), false);
      const timeoutMs = timeoutOf(BEGINNING_OFFSETS_FORMS, options, form === 1);
      return longMap(await answer<NativeLongEntry[]>((done) => native.consumerBeginningOffsets(this.#handle, pairs, timeoutMs, done)));
    });
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
    const count = arguments.length;
    return rejecting(async () => {
      const form = END_OFFSETS_FORMS.match(options, count);
      const pairs = partitionPairs(END_OFFSETS_FORMS.value(options, 'partitions'), false);
      const timeoutMs = timeoutOf(END_OFFSETS_FORMS, options, form === 1);
      return longMap(await answer<NativeLongEntry[]>((done) => native.consumerEndOffsets(this.#handle, pairs, timeoutMs, done)));
    });
  }

  currentLag(options: {
    /** The partition to get the lag for. */
    topicPartition: TopicPartition;
  }): Promise<bigint | null>;
  currentLag(topicPartition: TopicPartition): Promise<bigint | null>;
  currentLag(options: unknown): Promise<bigint | null> {
    const count = arguments.length;
    return rejecting(() => {
      CURRENT_LAG_FORMS.match(options, count);
      const partition = partitionPair(CURRENT_LAG_FORMS.value(options, 'topicPartition'), 'topicPartition');
      return answer<bigint | null>((done) => native.consumerCurrentLag(this.#handle, partition, done));
    });
  }

  groupMetadata(): ConsumerGroupMetadata {
    GROUP_METADATA_FORMS.match(undefined, arguments.length);
    // The temporary group.id check: the core makes it with a panic.
    if (!this.#groupIdDefined) throw new InvalidGroupIdError(NO_GROUP_ID);
    const [groupId, generationId, memberId, groupInstanceId] = native.consumerGroupMetadata(this.#handle);
    return ConsumerGroupMetadata.internalCreate(groupId, generationId, memberId, groupInstanceId);
  }

  enforceRebalance(): Promise<void>;
  enforceRebalance(options: {
    /** The reason why the new rebalance is needed. */
    reason: string;
  }): Promise<void>;
  enforceRebalance(reason: string): Promise<void>;
  enforceRebalance(options?: unknown): Promise<void> {
    const count = arguments.length;
    return rejecting(() => {
      const withReason = ENFORCE_REBALANCE_FORMS.match(options, count) === 1;
      const reason = withReason ? ENFORCE_REBALANCE_FORMS.value(options, 'reason') : null;
      if (reason !== null && typeof reason !== 'string') throw new TypeError('reason must be a string');
      return completion((done) => native.consumerEnforceRebalance(this.#handle, withReason, reason, done));
    });
  }

  close(): Promise<void>;
  close(options: {
    /** See `CloseOptions`; cannot be `null`. */
    option: CloseOptions;
  }): Promise<void>;
  close(option: CloseOptions): Promise<void>;
  close(options?: unknown): Promise<void> {
    const count = arguments.length;
    return rejecting(() => {
      // Java's close() is close(CloseOptions.timeout(DEFAULT_CLOSE_TIMEOUT));
      // the FFI's close_cb does that. close(option) builds the C options as
      // Java does (native/consumer.c, close_options_build): a null option
      // goes to the FFI, which refuses it.
      let timeoutMs: number | bigint | null = null;
      let operation: string | null = null;
      let withOptions = false;
      if (CLOSE_FORMS.match(options, count) === 1) {
        withOptions = true;
        const option = CLOSE_FORMS.value(options, 'option');
        if (option !== null) {
          if (!(option instanceof CloseOptions)) throw new TypeError('option must be a CloseOptions');
          const timeout = option.timeout();
          timeoutMs = timeout === null ? null : durationToMillis(timeout, 'timeout');
          operation = option.groupMembershipOperation().name();
        }
      }
      debug('KafkaConsumer closing');
      return new Promise<void>((resolve, reject) => {
        native.consumerClose(this.#handle, withOptions, timeoutMs, operation, (error, closedNow) => {
          if (closedNow) debug('KafkaConsumer closed');
          // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with the error the core reported, a client Error src/internal/errors.ts built (typed unknown at the native boundary)
          if (error !== null) reject(error);
          else resolve();
        });
      });
    });
  }

  wakeup(): void {
    WAKEUP_FORMS.match(undefined, arguments.length);
    native.consumerWakeup(this.#handle);
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }
}
