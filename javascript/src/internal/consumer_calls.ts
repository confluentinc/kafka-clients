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

// The bodies of Java's Consumer methods over a native consumer handle
// (private): each method of KafkaConsumer calls the function of the same name
// here, which checks the call's form and makes the call through the FFI's
// Consumer view (native/consumer.c), so a class over the same handle runs the
// same code.

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way (the addon is loaded there alone, javascript/CLAUDE.md "Files and layout")
import native = require('./native');
import { JavaForms } from './args';
import { debug } from './debug';
import { durationToMillis, isDurationOrNull } from './duration';
import { liveMetrics } from './live_metrics';
import { partitionInfoOf } from './metadata';
import {
  long,
  stringSet,
  topicPartition as topicPartitionKind,
  topicPartitionList,
  topicPartitionLongMap,
  topicPartitionOffsetMap,
} from './payload';
import type { Metric } from '../common/metric';
import type { MetricNameMap } from '../common/metric_name_map';
import type { PartitionInfo } from '../common/partition_info';
import { TopicPartition } from '../common/topic_partition';
import { TopicPartitionMap } from '../common/topic_partition_map';
import { TopicPartitionSet } from '../common/topic_partition_set';
import { InvalidGroupIdError } from '../common/errors/invalid_group_id_error';
import { CloseOptions } from '../consumer/close_options';
import { ConsumerGroupMetadata } from '../consumer/consumer_group_metadata';
import { ConsumerRecord } from '../consumer/consumer_record';
import { ConsumerRecords } from '../consumer/consumer_records';
import { OffsetAndMetadata } from '../consumer/offset_and_metadata';
import { OffsetAndTimestamp } from '../consumer/offset_and_timestamp';
import { SubscriptionPattern } from '../consumer/subscription_pattern';

/** The native consumer handle (`internal/native.ts`). */
export type ConsumerHandle = ReturnType<typeof native.consumerCreate>;

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
// Java's subscribe(Pattern...) forms are left out, and so are, until the
// client has rebalance listeners and commit callbacks, the forms taking one.
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
export function rejecting<T>(run: () => Promise<T>): Promise<T> {
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
 * A `TopicPartitionSet` of the addon's `[topic, partition]` pairs.
 *
 * @param pairs - the pairs
 * @returns the set
 */
function partitionSet(pairs: Array<[string, number]>): TopicPartitionSet {
  return new TopicPartitionSet(pairs.map(([topic, partition]) => new TopicPartition({ topic, partition })));
}
/**
 * The body of `Consumer#assignment`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function assignment(handle: ConsumerHandle, count: number): TopicPartitionSet {
  ASSIGNMENT_FORMS.match(undefined, count);
  return partitionSet(native.consumerAssignment(handle));
}

/**
 * The body of `Consumer#subscription`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function subscription(handle: ConsumerHandle, count: number): Set<string> {
  SUBSCRIPTION_FORMS.match(undefined, count);
  return new Set(native.consumerSubscription(handle));
}

/**
 * The body of `Consumer#subscribe`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param options - the argument given
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function subscribe(handle: ConsumerHandle, options: unknown, count: number): Promise<void> {
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
      return completion((done) => native.consumerSubscribeTopics(handle, list, done));
    }
    const pattern = SUBSCRIBE_FORMS.value(options, 'pattern');
    if (pattern !== null && !(pattern instanceof SubscriptionPattern)) throw new TypeError('pattern must be a SubscriptionPattern');
    return completion((done) =>
      native.consumerSubscribePattern(handle, pattern !== null, pattern === null ? null : pattern.pattern(), done),
    );
  });
}

/**
 * The body of `Consumer#assign`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param options - the argument given
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function assign(handle: ConsumerHandle, options: unknown, count: number): Promise<void> {
  return rejecting(() => {
    ASSIGN_FORMS.match(options, count);
    const pairs = partitionPairs(ASSIGN_FORMS.value(options, 'partitions'), true);
    return completion((done) => native.consumerAssign(handle, pairs, done));
  });
}

/**
 * The body of `Consumer#unsubscribe`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function unsubscribe(handle: ConsumerHandle, count: number): Promise<void> {
  return rejecting(() => {
    UNSUBSCRIBE_FORMS.match(undefined, count);
    return completion((done) => native.consumerUnsubscribe(handle, done));
  });
}

/**
 * The body of `Consumer#poll`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param options - the argument given
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function poll<K, V>(handle: ConsumerHandle, options: unknown, count: number): Promise<ConsumerRecords<K, V>> {
  return rejecting(() => {
    POLL_FORMS.match(options, count);
    // A negative timeout goes to the FFI, which rejects it with the core's
    // message (javascript/CLAUDE.md, JavaScript Binding Conventions, Signatures).
    const timeoutMs = durationToMillis(POLL_FORMS.value(options, 'timeout'), 'timeout');
    return new Promise<ConsumerRecords<K, V>>((resolve, reject) => {
      native.consumerPoll(handle, timeoutMs, RECORD_FACTORY, (error, partitions, nextOffsets) => {
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

/**
 * The body of `Consumer#commitSync`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param options - the argument given
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function commitSync(handle: ConsumerHandle, options: unknown, count: number): Promise<void> {
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
    return completion((done) => native.consumerCommitSync(handle, withOffsets, entries, timeoutMs, done));
  });
}

/**
 * The body of `Consumer#commitAsync`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param options - the argument given
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function commitAsync(handle: ConsumerHandle, options: unknown, count: number): Promise<void> {
  return rejecting(() => {
    // The argument is read, so a callback, whose forms are not in this
    // release, gets the form error.
    COMMIT_ASYNC_FORMS.match(options, count);
    return completion((done) => native.consumerCommitAsync(handle, done));
  });
}

/**
 * The body of `Consumer#seek`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param options - the argument given
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function seek(handle: ConsumerHandle, options: unknown, count: number): Promise<void> {
  return rejecting(() => {
    const withMetadata = SEEK_FORMS.match(options, count) === 1;
    const partition = partitionPair(SEEK_FORMS.value(options, 'partition'), 'partition');
    if (!withMetadata) {
      // A negative offset goes to the FFI, which refuses it with Java's message.
      const offset = long.take(SEEK_FORMS.value(options, 'offset'), 'offset') as bigint;
      return completion((done) => native.consumerSeek(handle, partition, false, offset, done));
    }
    const om = SEEK_FORMS.value(options, 'offsetAndMetadata');
    if (om !== null && !(om instanceof OffsetAndMetadata)) throw new TypeError('offsetAndMetadata must be an OffsetAndMetadata');
    const fields = om === null ? null : offsetFields(om);
    return completion((done) => native.consumerSeek(handle, partition, true, fields, done));
  });
}

/**
 * The body of `Consumer#seekToBeginning`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param options - the argument given
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function seekToBeginning(handle: ConsumerHandle, options: unknown, count: number): Promise<void> {
  return rejecting(() => {
    SEEK_TO_BEGINNING_FORMS.match(options, count);
    const pairs = partitionPairs(SEEK_TO_BEGINNING_FORMS.value(options, 'partitions'), false);
    return completion((done) => native.consumerSeekToBeginning(handle, pairs, done));
  });
}

/**
 * The body of `Consumer#seekToEnd`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param options - the argument given
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function seekToEnd(handle: ConsumerHandle, options: unknown, count: number): Promise<void> {
  return rejecting(() => {
    SEEK_TO_END_FORMS.match(options, count);
    const pairs = partitionPairs(SEEK_TO_END_FORMS.value(options, 'partitions'), false);
    return completion((done) => native.consumerSeekToEnd(handle, pairs, done));
  });
}

/**
 * The body of `Consumer#position`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param options - the argument given
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function position(handle: ConsumerHandle, options: unknown, count: number): Promise<bigint> {
  return rejecting(() => {
    const form = POSITION_FORMS.match(options, count);
    const partition = partitionPair(POSITION_FORMS.value(options, 'partition'), 'partition');
    const timeoutMs = timeoutOf(POSITION_FORMS, options, form === 1);
    return answer<bigint>((done) => native.consumerPosition(handle, partition, timeoutMs, done));
  });
}

/**
 * The body of `Consumer#committed`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param options - the argument given
 * @param count - the number of arguments given
 * @param groupIdDefined - whether group.id was given, for the temporary group.id check
 * @returns what the method returns
 */
export function committed(
  handle: ConsumerHandle,
  options: unknown,
  count: number,
  groupIdDefined: boolean,
): Promise<TopicPartitionMap<OffsetAndMetadata | null>> {
  return rejecting(async () => {
    const form = COMMITTED_FORMS.match(options, count);
    const given = COMMITTED_FORMS.value(options, 'partitions');
    // The temporary group.id check: Java's throwIfGroupIdNotDefined() comes
    // before its dereference of the null, which the FFI would report first.
    if (given === null && !groupIdDefined) throw new InvalidGroupIdError(NO_GROUP_ID);
    const pairs = partitionPairs(given, false);
    const timeoutMs = timeoutOf(COMMITTED_FORMS, options, form === 1);
    const entries = await answer<NativeOffsetEntry[]>((done) => native.consumerCommitted(handle, pairs, timeoutMs, done));
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

/**
 * The body of `Consumer#metrics`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function metrics(handle: ConsumerHandle, count: number): MetricNameMap<Metric> {
  METRICS_FORMS.match(undefined, count);
  return liveMetrics(native.consumerMetrics(handle));
}

/**
 * The body of `Consumer#partitionsFor`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param options - the argument given
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function partitionsFor(handle: ConsumerHandle, options: unknown, count: number): Promise<PartitionInfo[]> {
  return rejecting(async () => {
    const form = PARTITIONS_FOR_FORMS.match(options, count);
    const topic = PARTITIONS_FOR_FORMS.value(options, 'topic');
    if (topic !== null && typeof topic !== 'string') throw new TypeError('topic must be a string');
    const timeoutMs = timeoutOf(PARTITIONS_FOR_FORMS, options, form === 1);
    const partitions = await answer<NativePartitionInfo[]>((done) => native.consumerPartitionsFor(handle, topic, timeoutMs, done));
    return partitions.map(partitionInfoOf);
  });
}

/**
 * The body of `Consumer#listTopics`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param options - the argument given
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function listTopics(handle: ConsumerHandle, options: unknown, count: number): Promise<Map<string, PartitionInfo[]>> {
  return rejecting(async () => {
    const form = LIST_TOPICS_FORMS.match(options, count);
    const timeoutMs = timeoutOf(LIST_TOPICS_FORMS, options, form === 1);
    const topics = await answer<Array<[string, NativePartitionInfo[]]>>((done) => native.consumerListTopics(handle, timeoutMs, done));
    return new Map(topics.map(([topic, partitions]) => [topic, partitions.map(partitionInfoOf)]));
  });
}

/**
 * The body of `Consumer#paused`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function paused(handle: ConsumerHandle, count: number): TopicPartitionSet {
  PAUSED_FORMS.match(undefined, count);
  return partitionSet(native.consumerPaused(handle));
}

/**
 * The body of `Consumer#pause`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param options - the argument given
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function pause(handle: ConsumerHandle, options: unknown, count: number): Promise<void> {
  return rejecting(() => {
    PAUSE_FORMS.match(options, count);
    const pairs = partitionPairs(PAUSE_FORMS.value(options, 'partitions'), false);
    return completion((done) => native.consumerPause(handle, pairs, done));
  });
}

/**
 * The body of `Consumer#resume`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param options - the argument given
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function resume(handle: ConsumerHandle, options: unknown, count: number): Promise<void> {
  return rejecting(() => {
    RESUME_FORMS.match(options, count);
    const pairs = partitionPairs(RESUME_FORMS.value(options, 'partitions'), false);
    return completion((done) => native.consumerResume(handle, pairs, done));
  });
}

/**
 * The body of `Consumer#offsetsForTimes`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param options - the argument given
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function offsetsForTimes(
  handle: ConsumerHandle,
  options: unknown,
  count: number,
): Promise<TopicPartitionMap<OffsetAndTimestamp | null>> {
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
    const found = await answer<NativeTimestampEntry[]>((done) => native.consumerOffsetsForTimes(handle, entries, timeoutMs, done));
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

/**
 * The body of `Consumer#beginningOffsets`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param options - the argument given
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function beginningOffsets(handle: ConsumerHandle, options: unknown, count: number): Promise<TopicPartitionMap<bigint>> {
  return rejecting(async () => {
    const form = BEGINNING_OFFSETS_FORMS.match(options, count);
    const pairs = partitionPairs(BEGINNING_OFFSETS_FORMS.value(options, 'partitions'), false);
    const timeoutMs = timeoutOf(BEGINNING_OFFSETS_FORMS, options, form === 1);
    const entries = await answer<NativeLongEntry[]>((done) => native.consumerBeginningOffsets(handle, pairs, timeoutMs, done));
    // The entries an error's payload holds too: the same reader builds the map.
    return topicPartitionLongMap.core(entries, {}) as TopicPartitionMap<bigint>;
  });
}

/**
 * The body of `Consumer#endOffsets`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param options - the argument given
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function endOffsets(handle: ConsumerHandle, options: unknown, count: number): Promise<TopicPartitionMap<bigint>> {
  return rejecting(async () => {
    const form = END_OFFSETS_FORMS.match(options, count);
    const pairs = partitionPairs(END_OFFSETS_FORMS.value(options, 'partitions'), false);
    const timeoutMs = timeoutOf(END_OFFSETS_FORMS, options, form === 1);
    const entries = await answer<NativeLongEntry[]>((done) => native.consumerEndOffsets(handle, pairs, timeoutMs, done));
    // The entries an error's payload holds too: the same reader builds the map.
    return topicPartitionLongMap.core(entries, {}) as TopicPartitionMap<bigint>;
  });
}

/**
 * The body of `Consumer#currentLag`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param options - the argument given
 * @param count - the number of arguments given
 * @returns what the method returns
 */
export function currentLag(handle: ConsumerHandle, options: unknown, count: number): Promise<bigint | null> {
  return rejecting(() => {
    CURRENT_LAG_FORMS.match(options, count);
    const partition = partitionPair(CURRENT_LAG_FORMS.value(options, 'topicPartition'), 'topicPartition');
    return answer<bigint | null>((done) => native.consumerCurrentLag(handle, partition, done));
  });
}

/**
 * The body of `Consumer#groupMetadata`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param count - the number of arguments given
 * @param groupIdDefined - whether group.id was given, for the temporary group.id check
 * @returns what the method returns
 */
export function groupMetadata(handle: ConsumerHandle, count: number, groupIdDefined: boolean): ConsumerGroupMetadata {
  GROUP_METADATA_FORMS.match(undefined, count);
  // The temporary group.id check: the core makes it with a panic.
  if (!groupIdDefined) throw new InvalidGroupIdError(NO_GROUP_ID);
  const [groupId, generationId, memberId, groupInstanceId] = native.consumerGroupMetadata(handle);
  return ConsumerGroupMetadata.internalCreate(groupId, generationId, memberId, groupInstanceId);
}

/**
 * The body of `Consumer#close`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param options - the argument given
 * @param count - the number of arguments given
 * @param client - the client's class name, for the debug lines
 * @returns what the method returns
 */
export function close(handle: ConsumerHandle, options: unknown, count: number, client: string): Promise<void> {
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
    debug('%s closing', client);
    return new Promise<void>((resolve, reject) => {
      native.consumerClose(handle, withOptions, timeoutMs, operation, (error, closedNow) => {
        if (closedNow) debug('%s closed', client);
        // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with the error the core reported, a client Error src/internal/errors.ts built (typed unknown at the native boundary)
        if (error !== null) reject(error);
        else resolve();
      });
    });
  });
}

/**
 * The body of `Consumer#wakeup`, over a native consumer handle.
 *
 * @param handle - the native consumer handle
 * @param count - the number of arguments given
 */
export function wakeup(handle: ConsumerHandle, count: number): void {
  WAKEUP_FORMS.match(undefined, count);
  native.consumerWakeup(handle);
}
