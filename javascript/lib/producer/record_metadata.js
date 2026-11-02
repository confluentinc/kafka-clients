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


'use strict';

/**
 * The metadata for a record that has been acknowledged by the server.
 *
 * Built by the client; a delivery callback for a failed record receives one
 * whose fields are -1.
 */
class RecordMetadata {
  #topic;
  #partition;
  #offset;
  #hasOffset;
  #timestamp;
  #hasTimestamp;
  #serializedKeySize;
  #serializedValueSize;

  /**
   * Partition value for record without partition assigned
   * @type {number}
   */
  static UNKNOWN_PARTITION = -1;

  /** @internal Built by the native layer through lib/index.js's factory. */
  constructor(fields) {
    this.#topic = fields.topic;
    this.#partition = fields.partition;
    this.#hasOffset = fields.hasOffset;
    this.#offset = fields.hasOffset ? fields.offset : -1;
    this.#hasTimestamp = fields.hasTimestamp;
    this.#timestamp = fields.hasTimestamp ? fields.timestamp : -1;
    this.#serializedKeySize = fields.serializedKeySize;
    this.#serializedValueSize = fields.serializedValueSize;
  }

  /** @returns {boolean} true if the offset is included in the metadata, false otherwise. */
  get hasOffset() {
    return this.#hasOffset;
  }

  /** @returns {number} The offset of the record in the topic/partition, or -1 if hasOffset is false. */
  get offset() {
    return this.#offset;
  }

  /** @returns {boolean} true if a valid timestamp exists, false otherwise. */
  get hasTimestamp() {
    return this.#hasTimestamp;
  }

  /** @returns {number} The timestamp of the record in the topic/partition, or -1 if hasTimestamp is false. */
  get timestamp() {
    return this.#timestamp;
  }

  /**
   * @returns {number} The size of the serialized, uncompressed key in bytes. If key is null, the
   *   returned size is -1.
   */
  get serializedKeySize() {
    return this.#serializedKeySize;
  }

  /**
   * @returns {number} The size of the serialized, uncompressed value in bytes. If value is null,
   *   the returned size is -1.
   */
  get serializedValueSize() {
    return this.#serializedValueSize;
  }

  /** @returns {string} The topic the record was appended to */
  get topic() {
    return this.#topic;
  }

  /** @returns {number} The partition the record was sent to */
  get partition() {
    return this.#partition;
  }

  toString() {
    return `${this.#topic}-${this.#partition}@${this.#offset}`;
  }
}

module.exports = { RecordMetadata };
