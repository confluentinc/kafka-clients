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

/**
 * The metadata for a record that has been acknowledged by the server.
 *
 * Built by the client; a delivery callback for a failed record receives one
 * whose fields are -1.
 */
export class RecordMetadata {
  #topic: string;
  #partition: number;
  #offset: number;
  #hasOffset: boolean;
  #timestamp: number;
  #hasTimestamp: boolean;
  #serializedKeySize: number;
  #serializedValueSize: number;

  /**
   * Partition value for record without partition assigned
   */
  static readonly UNKNOWN_PARTITION: -1 = -1;

  /**
   * Built by the native layer, through the factory lib/_native.ts registers
   * (private).
   */
  private constructor(fields: {
    topic: string;
    partition: number;
    hasOffset: boolean;
    offset: number;
    hasTimestamp: boolean;
    timestamp: number;
    serializedKeySize: number;
    serializedValueSize: number;
  }) {
    this.#topic = fields.topic;
    this.#partition = fields.partition;
    this.#hasOffset = fields.hasOffset;
    this.#offset = fields.hasOffset ? fields.offset : -1;
    this.#hasTimestamp = fields.hasTimestamp;
    this.#timestamp = fields.hasTimestamp ? fields.timestamp : -1;
    this.#serializedKeySize = fields.serializedKeySize;
    this.#serializedValueSize = fields.serializedValueSize;
  }

  /** true if the offset is included in the metadata, false otherwise. */
  get hasOffset(): boolean {
    return this.#hasOffset;
  }

  /** The offset of the record in the topic/partition, or -1 if hasOffset is false. */
  get offset(): number {
    return this.#offset;
  }

  /** true if a valid timestamp exists, false otherwise. */
  get hasTimestamp(): boolean {
    return this.#hasTimestamp;
  }

  /** The timestamp of the record in the topic/partition, or -1 if hasTimestamp is false. */
  get timestamp(): number {
    return this.#timestamp;
  }

  /**
   * The size of the serialized, uncompressed key in bytes. If key is null, the
   * returned size is -1.
   */
  get serializedKeySize(): number {
    return this.#serializedKeySize;
  }

  /**
   * The size of the serialized, uncompressed value in bytes. If value is null,
   * the returned size is -1.
   */
  get serializedValueSize(): number {
    return this.#serializedValueSize;
  }

  /** The topic the record was appended to */
  get topic(): string {
    return this.#topic;
  }

  /** The partition the record was sent to */
  get partition(): number {
    return this.#partition;
  }

  toString(): string {
    return `${this.#topic}-${this.#partition}@${this.#offset}`;
  }
}
