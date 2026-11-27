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

// A poll's records through the consumer's deserializers (private;
// javascript/CLAUDE.md, JavaScript Binding Conventions, Serialization), as
// Java's CompletedFetch.parseRecord runs them: for each record the key's
// deserializer, then the value's, each only for a key or value that is not
// null, given the record's own Headers and a Buffer over the fetched bytes. A
// plain result adds no microtask; a thenable is awaited, record by record.
//
// A failing deserializer ends the batch where Java's FetchCollector ends it:
// the records deserialized before it are returned, and RecordDeserializationError
// is the poll's failure only when there are none (CompletedFetch.fetchRecords
// rethrows only for an empty fetch). The core has already moved the positions
// past the whole batch, so each partition is sought back to its first record
// not returned before poll() settles, which leaves the failing record where
// Java leaves it: seek past it to skip it, and otherwise the next poll fails
// on it again *(deviation, as in Python: Java never moves past the record. Why:
// the deserializers run in JavaScript after the core's poll)*.

import { isThenable } from './serde';
import { RecordDeserializationError } from '../common/errors/record_deserialization_error';
import type { Deserializer } from '../common/serialization/deserializer';
import { TopicPartition } from '../common/topic_partition';
import { TopicPartitionMap } from '../common/topic_partition_map';
import { ConsumerRecord } from '../consumer/consumer_record';
import { ConsumerRecords } from '../consumer/consumer_records';
import { OffsetAndMetadata } from '../consumer/offset_and_metadata';

/** A partition's records as a poll returns them: its topic, its partition and its records, with Buffers. */
export type PartitionRecords = readonly [string, number, ReadonlyArray<ConsumerRecord<Buffer, Buffer>>];

/** A partition's next offset as a poll returns it: topic, partition, offset, leader epoch (-1 for none) and metadata. */
export type NextOffset = readonly [string, number, bigint, number, string];

/** What a failing batch asks of the consumer: the records to return, the failure, and the positions to seek back to. */
export interface Deserialized<K, V> {
  /** The records deserialized before the failure, or all of them. */
  readonly records: ConsumerRecords<K, V>;
  /** The failure that ended the batch, or null. */
  readonly error: RecordDeserializationError | null;
  /** Each partition's first record not returned, `[partition, offset]`, to seek back to. */
  readonly rewind: ReadonlyArray<readonly [TopicPartition, bigint]>;
}

/** The batch being deserialized. */
interface Batch {
  readonly partitions: readonly PartitionRecords[];
  readonly nextOffsets: readonly NextOffset[];
  readonly key: Deserializer<unknown> | null;
  readonly value: Deserializer<unknown> | null;
  readonly out: TopicPartitionMap<ConsumerRecord<unknown, unknown>[]>;
  // The TopicPartition of each partition, made once.
  readonly tps: TopicPartition[];
}

/**
 * The records of a poll as a `ConsumerRecords`, the next offsets the core
 * reported, built as the consumer builds a poll without deserializers.
 *
 * @param partitions - the partitions' records
 * @param nextOffsets - the partitions' next offsets
 * @returns the records
 */
export function recordsOf<K, V>(partitions: readonly PartitionRecords[], nextOffsets: readonly NextOffset[]): ConsumerRecords<K, V> {
  const records = new TopicPartitionMap<ConsumerRecord<K, V>[]>();
  for (const [topic, partition, list] of partitions) {
    records.set(new TopicPartition({ topic, partition }), list as unknown as ConsumerRecord<K, V>[]);
  }
  const next = new TopicPartitionMap<OffsetAndMetadata>();
  for (const [topic, partition, offset, leaderEpoch, metadata] of nextOffsets) {
    next.set(
      new TopicPartition({ topic, partition }),
      new OffsetAndMetadata({ offset, leaderEpoch: leaderEpoch < 0 ? null : leaderEpoch, metadata }),
    );
  }
  return new ConsumerRecords<K, V>({ records, nextOffsets: next });
}

/**
 * Deserializes a poll's records with the consumer's deserializers, a null one
 * leaving its Buffers as they are.
 *
 * @param partitions - the partitions' records, with Buffers
 * @param nextOffsets - the partitions' next offsets
 * @param key - the key's deserializer, or null
 * @param value - the value's deserializer, or null
 * @returns the records and the failure that ended the batch, or a Promise of
 *   them once a deserializer returned a thenable
 */
export function deserializeBatch<K, V>(
  partitions: readonly PartitionRecords[],
  nextOffsets: readonly NextOffset[],
  key: Deserializer<K> | null,
  value: Deserializer<V> | null,
): Deserialized<K, V> | Promise<Deserialized<K, V>> {
  const batch: Batch = {
    partitions,
    nextOffsets,
    key,
    value,
    out: new TopicPartitionMap(),
    tps: partitions.map(([topic, partition]) => new TopicPartition({ topic, partition })),
  };
  return run(batch, 0, 0, null, false) as Deserialized<K, V> | Promise<Deserialized<K, V>>;
}

/** Java's origins, read once. */
const KEY = RecordDeserializationError.DeserializationExceptionOrigin.KEY;
const VALUE = RecordDeserializationError.DeserializationExceptionOrigin.VALUE;

/**
 * Deserializes the batch from record `ri` of partition `pi`, as Java's
 * `parseRecord` does each record: a throw, or a thenable whose `then` cannot
 * be read, is the deserializer's failure. The key is already deserialized as
 * `keyResult` when `keyDone`.
 *
 * @param b - the batch
 * @param pi - the partition's index
 * @param ri - the record's index
 * @param keyResult - the record's key, deserialized, when `keyDone`
 * @param keyDone - whether the record's key is deserialized
 * @returns the outcome, or a Promise of it
 */
function run(
  b: Batch,
  pi: number,
  ri: number,
  keyResult: unknown,
  keyDone: boolean,
): Deserialized<unknown, unknown> | Promise<Deserialized<unknown, unknown>> {
  for (; pi < b.partitions.length; pi++, ri = 0) {
    const list = b.partitions[pi][2];
    for (; ri < list.length; ri++) {
      const record = list[ri];
      let k: unknown = record.key;
      if (keyDone) {
        k = keyResult;
        keyDone = false;
      } else if (record.key !== null && b.key !== null) {
        let thenable: boolean;
        try {
          k = b.key.deserialize({ topic: record.topic, headers: record.headers, data: record.key });
          thenable = isThenable(k);
        } catch (thrown) {
          return failed(b, pi, ri, KEY, thrown);
        }
        if (thenable) {
          const p = pi;
          const r = ri;
          return Promise.resolve(k).then(
            (made: unknown) => run(b, p, r, made, true),
            (thrown: unknown) => failed(b, p, r, KEY, thrown),
          );
        }
      }
      let v: unknown = record.value;
      if (record.value !== null && b.value !== null) {
        let thenable: boolean;
        try {
          v = b.value.deserialize({ topic: record.topic, headers: record.headers, data: record.value });
          thenable = isThenable(v);
        } catch (thrown) {
          return failed(b, pi, ri, VALUE, thrown);
        }
        if (thenable) {
          const p = pi;
          const r = ri;
          const madeKey = k;
          return Promise.resolve(v).then(
            (made: unknown) => {
              take(b, p, record, madeKey, made);
              return run(b, p, r + 1, null, false);
            },
            (thrown: unknown) => failed(b, p, r, VALUE, thrown),
          );
        }
      }
      take(b, pi, record, k, v);
    }
  }
  return { records: built(b, null), error: null, rewind: [] };
}

/**
 * Adds a deserialized record to the records returned.
 *
 * @param b - the batch
 * @param pi - its partition's index
 * @param record - the record as fetched
 * @param key - its key, deserialized
 * @param value - its value, deserialized
 */
function take(b: Batch, pi: number, record: ConsumerRecord<Buffer, Buffer>, key: unknown, value: unknown): void {
  const tp = b.tps[pi];
  let list = b.out.get(tp);
  if (list === null) {
    list = [];
    b.out.set(tp, list);
  }
  list.push(ConsumerRecord.internalWithKeyValue(record, key, value));
}

/**
 * The `ConsumerRecords` of the records returned, with the core's next offsets
 * for the partitions returned whole. A partition cut short at index `cut`
 * (the failing one, or one after it) gets, as Java's
 * `CompletedFetch.fetchRecords` leaves `nextFetchOffset`, the last returned
 * record's offset + 1 with its leader epoch, or no next offset when none of
 * its records is returned.
 *
 * @param b - the batch
 * @param cut - the index of the first partition not returned whole, or null
 * @returns the records
 */
function built(b: Batch, cut: number | null): ConsumerRecords<unknown, unknown> {
  const next = new TopicPartitionMap<OffsetAndMetadata>();
  const whole = new Set(cut === null ? b.tps.map((_, i) => i) : b.tps.map((_, i) => i).filter((i) => i < cut));
  for (const [topic, partition, offset, leaderEpoch, metadata] of b.nextOffsets) {
    const index = b.partitions.findIndex(([t, p]) => t === topic && p === partition);
    if (index !== -1 && !whole.has(index)) continue;
    next.set(
      new TopicPartition({ topic, partition }),
      new OffsetAndMetadata({ offset, leaderEpoch: leaderEpoch < 0 ? null : leaderEpoch, metadata }),
    );
  }
  if (cut !== null) {
    const returned = b.out.get(b.tps[cut]);
    if (returned !== null && returned.length > 0) {
      const last = returned[returned.length - 1];
      next.set(b.tps[cut], new OffsetAndMetadata({ offset: last.offset + 1n, leaderEpoch: last.leaderEpoch, metadata: '' }));
    }
  }
  return new ConsumerRecords({ records: b.out, nextOffsets: next });
}

/**
 * The outcome of a batch a deserializer failed at record `ri` of partition
 * `pi`: Java's `CompletedFetch.newRecordDeserializationException`, the
 * records returned before it, and each partition's first record not returned.
 *
 * @param b - the batch
 * @param pi - the partition's index
 * @param ri - the record's index
 * @param origin - the key's or the value's
 * @param thrown - what the deserializer threw, or rejected with
 * @returns the outcome
 */
function failed(
  b: Batch,
  pi: number,
  ri: number,
  origin: RecordDeserializationError.DeserializationExceptionOrigin,
  thrown: unknown,
): Deserialized<unknown, unknown> {
  const record = b.partitions[pi][2][ri];
  const tp = b.tps[pi];
  const error = new RecordDeserializationError({
    origin,
    partition: tp,
    offset: record.offset,
    timestamp: record.timestamp,
    timestampType: record.timestampType,
    keyBuffer: record.key,
    valueBuffer: record.value,
    headers: record.headers,
    message:
      `Error deserializing ${origin.name()} for partition ${String(tp)} at offset ${record.offset}. ` +
      'If needed, please seek past the record to continue consumption.',
    cause: null,
  });
  // The thrown value itself, whatever it is (`null` and `undefined` included).
  Object.defineProperty(error, 'cause', { value: thrown, writable: true, enumerable: false, configurable: true });
  const rewind: Array<readonly [TopicPartition, bigint]> = [[tp, record.offset]];
  for (let i = pi + 1; i < b.partitions.length; i++) {
    const first = b.partitions[i][2][0];
    if (first !== undefined) rewind.push([b.tps[i], first.offset]);
  }
  return { records: built(b, pi), error, rewind };
}
