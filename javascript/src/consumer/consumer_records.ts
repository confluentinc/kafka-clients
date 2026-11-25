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

// Java's ConsumerRecords<K, V>, a class with methods. Java keeps the records'
// map as it is given and copies the next offsets (`Map.copyOf`); every
// collection a method returns is a new object the caller may change
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family), where
// Java returns unmodifiable views.

import { TopicPartition } from '../common/topic_partition';
import { TopicPartitionMap } from '../common/topic_partition_map';
import { TopicPartitionSet } from '../common/topic_partition_set';
import { JavaForms } from '../internal/args';
import { illegalArgument } from '../internal/errors';
import type { ConsumerRecord } from './consumer_record';
import { OffsetAndMetadata } from './offset_and_metadata';

// Java's constructors, in Java declaration order (src/internal/args.ts); the
// deprecated ConsumerRecords(Map) is not translated.
const FORMS = new JavaForms('ConsumerRecords', [['records', 'nextOffsets']]);
const RECORDS_FORMS = new JavaForms('records', [['partition'], ['topic']], {
  bareTypes: { partition: (value) => value instanceof TopicPartition, topic: (value) => typeof value === 'string' },
});
const NEXT_OFFSETS_FORMS = new JavaForms('nextOffsets', [[]], { reader: true });
const PARTITIONS_FORMS = new JavaForms('partitions', [[]], { reader: true });
const COUNT_FORMS = new JavaForms('count', [[]], { reader: true });
const IS_EMPTY_FORMS = new JavaForms('isEmpty', [[]], { reader: true });
const EMPTY_FORMS = new JavaForms('empty', [[]], { reader: true });

// A map argument keyed by TopicPartition: only a TopicPartitionMap
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family).
function topicPartitionMap(value: unknown, name: string): TopicPartitionMap<unknown> {
  if (!(value instanceof TopicPartitionMap)) {
    throw new TypeError(
      `${name} must be a TopicPartitionMap, not ${value instanceof Map ? 'a Map' : typeof value}: a Map keyed by TopicPartition ` +
        'compares its keys by identity, so build one with new TopicPartitionMap(entries)',
    );
  }
  return value as TopicPartitionMap<unknown>;
}

// Java's `Map.copyOf(nextOffsets)`: a copy, refusing a null map, key or value.
function copyNextOffsets(value: unknown): TopicPartitionMap<OffsetAndMetadata> {
  if (value === null) throw new TypeError('nextOffsets must not be null');
  const copy = new TopicPartitionMap<OffsetAndMetadata>();
  for (const [tp, offset] of topicPartitionMap(value, 'nextOffsets')) {
    if (tp === null || offset === null) throw new TypeError('nextOffsets must not contain null');
    if (!(offset instanceof OffsetAndMetadata)) throw new TypeError('nextOffsets must hold only OffsetAndMetadata values');
    copy.set(tp, offset);
  }
  return copy;
}

/**
 * A container that holds the list `ConsumerRecord` per partition for a
 * particular topic. There is one `ConsumerRecord` list for every topic
 * partition returned by a `Consumer.poll` operation.
 */
export class ConsumerRecords<K, V> implements Iterable<ConsumerRecord<K, V>> {
  /** Java's `EMPTY`: a record set with no records and no next offsets, frozen. */
  static readonly EMPTY: ConsumerRecords<never, never> = new ConsumerRecords<never, never>({
    records: new TopicPartitionMap(),
    nextOffsets: new TopicPartitionMap(),
  });

  static {
    Object.freeze(ConsumerRecords.EMPTY);
  }

  readonly #records: TopicPartitionMap<ConsumerRecord<K, V>[]> | null;

  readonly #nextOffsets: TopicPartitionMap<OffsetAndMetadata>;

  /**
   * Constructs a new ConsumerRecords with the given records and next offsets.
   *
   * @param options - the records and the next offsets
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{records, nextOffsets}`
   * @throws `TypeError` when `nextOffsets` is null or holds a null, either is
   *   not a `TopicPartitionMap`, or a next offset is not an `OffsetAndMetadata`
   *
   * @remarks The records are kept as given, as Java keeps its map: a method
   * that reads them throws `TypeError` when they are null. The next offsets are
   * copied, as Java's `Map.copyOf` copies them. Java's one-argument
   * constructor is deprecated, so `{ records }` alone is refused.
   */
  constructor(options: {
    /** The records for each partition. */
    records: TopicPartitionMap<ConsumerRecord<K, V>[]>;
    /**
     * The next offset and metadata for each partition whose position was
     * advanced during the poll call. These represent the offsets that the
     * consumer will start reading from on the next poll.
     */
    nextOffsets: TopicPartitionMap<OffsetAndMetadata>;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const records = FORMS.value(options, 'records');
    if (records !== null) topicPartitionMap(records, 'records');
    this.#nextOffsets = copyNextOffsets(FORMS.value(options, 'nextOffsets'));
    this.#records = records as TopicPartitionMap<ConsumerRecord<K, V>[]> | null;
  }

  /**
   * Get just the records for the given partition.
   *
   * @param partition - The partition to get records for
   * @returns a new array of the partition's records, empty when it has none
   * @throws `TypeError` when the records are null
   */
  records(partition: TopicPartition): ConsumerRecord<K, V>[];
  /**
   * Get just the records for the given partition, the same as
   * `records(partition)`.
   *
   * @param options - the partition to get records for
   * @returns a new array of the partition's records
   */
  records(options: {
    /** The partition to get records for. */
    partition: TopicPartition | null;
    topic?: never;
  }): ConsumerRecord<K, V>[];
  /**
   * Get just the records for the given topic.
   *
   * @param topic - the topic to get records for
   * @returns a new array of the topic's records, partition by partition
   * @throws `IllegalArgumentError` when the topic is null ("Topic must be
   *   non-null.")
   * @throws `TypeError` when the records are null
   */
  records(topic: string): ConsumerRecord<K, V>[];
  /**
   * Get just the records for the given topic.
   *
   * @param options - the topic to get records for
   * @returns a new array of the topic's records, partition by partition, where
   *   Java returns an `Iterable`, as Python returns a list
   * @throws `IllegalArgumentError` when the topic is null
   */
  records(options: {
    /** The topic to get records for. */
    topic: string;
    partition?: never;
  }): ConsumerRecord<K, V>[];
  records(options: unknown): ConsumerRecord<K, V>[] {
    const form = RECORDS_FORMS.match(options, arguments.length);
    if (form === 0) {
      const partition = RECORDS_FORMS.value(options, 'partition') as TopicPartition | null;
      const list = this.#map().get(partition);
      return list === null ? [] : [...list];
    }
    const topic = RECORDS_FORMS.value(options, 'topic');
    if (topic === null) throw illegalArgument('Topic must be non-null.');
    if (typeof topic !== 'string') throw new TypeError('topic must be a string');
    const out: ConsumerRecord<K, V>[] = [];
    for (const [tp, list] of this.#map()) if (tp.topic === topic) out.push(...list);
    return out;
  }

  /**
   * Get the next offsets and metadata corresponding to all topic partitions
   * for which the position have been advanced in this poll call.
   *
   * @returns The next offsets that the consumer will consume, a new map
   * @throws `TypeError` when it is given an argument
   */
  nextOffsets(): TopicPartitionMap<OffsetAndMetadata> {
    NEXT_OFFSETS_FORMS.match(undefined, arguments.length);
    return new TopicPartitionMap(this.#nextOffsets);
  }

  /**
   * Get the partitions which have records contained in this record set.
   *
   * @returns The set of partitions with data in this record set (may be empty
   *   if no data was returned), a new set
   * @throws `TypeError` when it is given an argument, or the records are null
   */
  partitions(): TopicPartitionSet {
    PARTITIONS_FORMS.match(undefined, arguments.length);
    return new TopicPartitionSet(this.#map().keys());
  }

  /**
   * The records of every partition, partition by partition, Java's
   * `iterator()`.
   *
   * @returns an iterator over the records
   * @throws `TypeError` when the records are null
   */
  *[Symbol.iterator](): Iterator<ConsumerRecord<K, V>> {
    for (const list of this.#map().values()) yield* list;
  }

  /**
   * The number of records for all topics
   *
   * @returns the number of records
   * @throws `TypeError` when it is given an argument, or the records are null
   */
  count(): number {
    COUNT_FORMS.match(undefined, arguments.length);
    let count = 0;
    for (const list of this.#map().values()) count += list.length;
    return count;
  }

  /**
   * Returns whether this container has any records.
   *
   * @returns True if there are no records, false otherwise
   * @throws `TypeError` when it is given an argument, or the records are null
   */
  isEmpty(): boolean {
    IS_EMPTY_FORMS.match(undefined, arguments.length);
    return this.#map().size() === 0;
  }

  /**
   * Returns an empty ConsumerRecords instance.
   *
   * @returns An empty ConsumerRecords, `EMPTY`
   * @throws `TypeError` when it is given an argument
   */
  static empty<K, V>(): ConsumerRecords<K, V> {
    EMPTY_FORMS.match(undefined, arguments.length);
    return ConsumerRecords.EMPTY;
  }

  // The records, which Java dereferences.
  #map(): TopicPartitionMap<ConsumerRecord<K, V>[]> {
    if (this.#records === null) throw new TypeError('records must not be null');
    return this.#records;
  }
}
