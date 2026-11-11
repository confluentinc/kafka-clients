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

/** The fields the native layer reads a delivered record's metadata into (native/producer.c). */
interface RecordMetadataFields {
  topic: string;
  partition: number;
  hasOffset: boolean;
  offset: number;
  hasTimestamp: boolean;
  timestamp: number;
  serializedKeySize: number;
  serializedValueSize: number;
}

/**
 * The metadata for a record that has been acknowledged by the server.
 *
 * Built by the client; a delivery callback for a failed record receives one
 * whose fields are -1. Java's public constructor takes a `TopicPartition`,
 * which is not translated yet, so the constructor is private.
 */
export class RecordMetadata {
  /** The topic the record was appended to */
  readonly topic: string;

  /** The partition the record was sent to */
  readonly partition: number;

  /** The offset of the record in the topic/partition, or -1 if hasOffset is false. */
  readonly offset: number;

  /** true if the offset is included in the metadata, false otherwise. */
  readonly hasOffset: boolean;

  /** The timestamp of the record in the topic/partition, or -1 if hasTimestamp is false. */
  readonly timestamp: number;

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

  private constructor(fields: RecordMetadataFields) {
    this.topic = fields.topic;
    this.partition = fields.partition;
    this.offset = fields.hasOffset ? fields.offset : -1;
    this.hasOffset = fields.hasOffset;
    this.timestamp = fields.hasTimestamp ? fields.timestamp : -1;
    this.hasTimestamp = fields.hasTimestamp;
    this.serializedKeySize = fields.serializedKeySize;
    this.serializedValueSize = fields.serializedValueSize;
  }

  /**
   * The metadata of a delivered record, from the fields the native layer read.
   * Called by `src/internal/native.ts`.
   *
   * @internal
   */
  static internalCreate(fields: RecordMetadataFields): RecordMetadata {
    return new RecordMetadata(fields);
  }

  toString(): string {
    return `${this.topic}-${this.partition}@${this.offset}`;
  }
}
