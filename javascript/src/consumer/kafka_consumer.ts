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
import { stringSet, topicPartitionList } from '../internal/payload';
import type { Metric } from '../common/metric';
import type { MetricNameMap } from '../common/metric_name_map';
import { TopicPartition } from '../common/topic_partition';
import { TopicPartitionMap } from '../common/topic_partition_map';
import { TopicPartitionSet } from '../common/topic_partition_set';
import type { Duration } from '../duration';
import { UnsupportedOperationError } from '../unsupported_operation_error';
import { CloseOptions } from './close_options';
import type { Consumer } from './consumer';
import type { ConsumerConfigs } from './consumer_configs';
import { ConsumerRecord } from './consumer_record';
import { ConsumerRecords } from './consumer_records';
import { OffsetAndMetadata } from './offset_and_metadata';
import { SubscriptionPattern } from './subscription_pattern';

/** The native consumer handle (`internal/native.ts`). */
type ConsumerHandle = ReturnType<typeof native.consumerCreate>;

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
// bare; its two forms taking deserializers are refused until the client has
// deserializers. Java's subscribe(Pattern...) forms are left out, and so are,
// until the client has rebalance listeners, the forms taking one.
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

// Until the client has deserializers (javascript/design/current/consumer, the
// plan's decision 13), a consumer takes none: keys and values are Buffers.
const NO_DESERIALIZERS = 'KafkaConsumer takes no deserializer yet: keys and values are Buffers';

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

  metrics(): MetricNameMap<Metric> {
    METRICS_FORMS.match(undefined, arguments.length);
    return liveMetrics(native.consumerMetrics(this.#handle));
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
