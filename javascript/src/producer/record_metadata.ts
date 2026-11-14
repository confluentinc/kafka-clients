// Copyright 2025 Confluent Inc.
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

// Java's RecordMetadata, a data class (root CLAUDE.md, JavaScript Binding
// Conventions, Class family): Java's getters are readonly fields of a frozen
// object, set in the constructor.

import { JavaForms } from '../internal/args';
import { checkInt, checkLong } from '../internal/java_number';
import { TopicPartition } from '../common/topic_partition';

// Java's public constructor (src/internal/args.ts).
const FORMS = new JavaForms('RecordMetadata', [
  ['topicPartition', 'baseOffset', 'batchIndex', 'timestamp', 'serializedKeySize', 'serializedValueSize'],
]);

// The mark of the library's own construction (internalCreate), which no
// caller can give: the fields come from the native layer, checked there.
const INTERNAL: unique symbol = Symbol('RecordMetadata.internalCreate');

// Java's ProduceResponse.INVALID_OFFSET and RecordBatch.NO_TIMESTAMP.
const INVALID_OFFSET = -1n;
const NO_TIMESTAMP = -1n;

/** The fields the native layer reads a delivered record's metadata into (native/producer.c). */
interface RecordMetadataFields {
  topic: string;
  partition: number;
  hasOffset: boolean;
  offset: bigint;
  hasTimestamp: boolean;
  timestamp: bigint;
  serializedKeySize: number;
  serializedValueSize: number;
}

/**
 * The metadata for a record that has been acknowledged by the server.
 *
 * Built by the client; a delivery callback for a failed record receives one
 * whose fields are -1.
 */
export class RecordMetadata {
  /** The topic the record was appended to */
  readonly topic: string;

  /** The partition the record was sent to */
  readonly partition: number;

  /** The offset of the record in the topic/partition, or -1 if hasOffset is false. */
  readonly offset: bigint;

  /** true if the offset is included in the metadata, false otherwise. */
  readonly hasOffset: boolean;

  /** The timestamp of the record in the topic/partition, or -1 if hasTimestamp is false. */
  readonly timestamp: bigint;

  /** true if a valid timestamp exists, false otherwise. */
  readonly hasTimestamp: boolean;

  /**
   * The size of the serialized, uncompressed key in bytes. If key is null, the
   * returned size is -1.
   */
  readonly serializedKeySize: number;

  /**
   * The size of the serialized, uncompressed value in bytes. If value is null,
   * the returned size is -1.
   */
  readonly serializedValueSize: number;

  /**
   * Partition value for record without partition assigned
   */
  static readonly UNKNOWN_PARTITION: -1 = -1;

  /**
   * Makes `RecordMetadata` nominal: TypeScript checks it by name, not by
   * shape, so an object literal with the same fields is no `RecordMetadata`.
   * It holds no state, and a spread or a JSON copy leaves it out.
   */
  readonly #brand = true;

  /**
   * Creates the metadata of a record, Java's `RecordMetadata(topicPartition,
   * baseOffset, batchIndex, timestamp, serializedKeySize,
   * serializedValueSize)`: the offset is `baseOffset + batchIndex`, or -1
   * when the base offset is -1 (the offset is unknown).
   *
   * @param options - the topic partition, the base offset, the index of the
   *   record in its batch, the timestamp and the serialized key and value
   *   sizes
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{topicPartition, baseOffset, batchIndex, timestamp,
   *   serializedKeySize, serializedValueSize}`
   * @throws `TypeError` when an argument has the wrong type, is null or is
   *   `undefined`
   * @throws `RangeError` when an `int` is beyond Java's `int`, a bigint
   *   beyond Java's `long`, or a number beyond 2^53 - 1
   *
   * @remarks A `long` takes a bigint, or a whole number up to 2^53 - 1. The
   * object is frozen. *(deviation: Java's constructor takes a null
   * `topicPartition`, and its `topic()` then throws. Why: the fields of a
   * data class are set when it is built.)*
   */
  constructor(options: {
    /** The topic and partition the record was sent to. */
    topicPartition: TopicPartition;
    /** The offset of the record's batch: a bigint, or a whole number. */
    baseOffset: bigint | number;
    /** The index of the record in its batch. */
    batchIndex: number;
    /** The timestamp of the record, -1 for none: a bigint, or a whole number. */
    timestamp: bigint | number;
    /** The size of the serialized key in bytes, -1 for a null key. */
    serializedKeySize: number;
    /** The size of the serialized value in bytes, -1 for a null value. */
    serializedValueSize: number;
  });
  constructor(options: unknown, internal?: typeof INTERNAL) {
    if (internal === INTERNAL) {
      const fields = options as RecordMetadataFields;
      this.topic = fields.topic;
      this.partition = fields.partition;
      this.offset = fields.hasOffset ? fields.offset : -1n;
      this.hasOffset = fields.hasOffset;
      this.timestamp = fields.hasTimestamp ? fields.timestamp : -1n;
      this.hasTimestamp = fields.hasTimestamp;
      this.serializedKeySize = fields.serializedKeySize;
      this.serializedValueSize = fields.serializedValueSize;
      Object.freeze(this);
      return;
    }
    FORMS.match(options, arguments.length);
    const topicPartition = FORMS.value(options, 'topicPartition');
    if (topicPartition === null) throw new TypeError('topicPartition must not be null');
    if (!(topicPartition instanceof TopicPartition)) throw new TypeError('topicPartition must be a TopicPartition');
    const baseOffset = checkLong(FORMS.value(options, 'baseOffset'), 'baseOffset');
    const batchIndex = checkInt(FORMS.value(options, 'batchIndex'), 'batchIndex');
    const timestamp = checkLong(FORMS.value(options, 'timestamp'), 'timestamp');
    const serializedKeySize = checkInt(FORMS.value(options, 'serializedKeySize'), 'serializedKeySize');
    const serializedValueSize = checkInt(FORMS.value(options, 'serializedValueSize'), 'serializedValueSize');
    // Java ignores the batchIndex if the base offset is -1, since this
    // indicates the offset is unknown; the sum wraps as Java's long does.
    const offset = baseOffset === -1n ? baseOffset : BigInt.asIntN(64, baseOffset + BigInt(batchIndex));
    // Java's topic() reads topicPartition.topic(), which a null topic makes
    // null too; the field type is Java's String.
    this.topic = topicPartition.topic as string;
    this.partition = topicPartition.partition;
    this.offset = offset;
    this.hasOffset = offset !== INVALID_OFFSET;
    this.timestamp = timestamp;
    this.hasTimestamp = timestamp !== NO_TIMESTAMP;
    this.serializedKeySize = serializedKeySize;
    this.serializedValueSize = serializedValueSize;
    Object.freeze(this);
  }

  /**
   * The metadata of a delivered record, from the fields the native layer read.
   * Called by `src/internal/native.ts`.
   *
   * @internal
   */
  static internalCreate(fields: RecordMetadataFields): RecordMetadata {
    return new (RecordMetadata as unknown as new (fields: RecordMetadataFields, internal: typeof INTERNAL) => RecordMetadata)(fields, INTERNAL);
  }

  toString(): string {
    return `${this.topic}-${this.partition}@${this.offset}`;
  }

  /**
   * The metadata as `JSON.stringify` writes it: its fields in their order,
   * the offset and the timestamp decimal strings, which `BigInt()` reads
   * back exactly. It ignores the key `JSON.stringify` passes.
   *
   * @returns the metadata's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol: `JSON.stringify` throws
   * on a `bigint`, and a `number` loses digits above 2^53.
   */
  toJSON(): {
    topic: string;
    partition: number;
    offset: string;
    hasOffset: boolean;
    timestamp: string;
    hasTimestamp: boolean;
    serializedKeySize: number;
    serializedValueSize: number;
  } {
    return {
      topic: this.topic,
      partition: this.partition,
      offset: this.offset.toString(),
      hasOffset: this.hasOffset,
      timestamp: this.timestamp.toString(),
      hasTimestamp: this.hasTimestamp,
      serializedKeySize: this.serializedKeySize,
      serializedValueSize: this.serializedValueSize,
    };
  }
}
