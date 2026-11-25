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

'use strict';

// The library's own members the tests reach: the `@internal` members named
// `internal...` and an error class's private `_ffiId`. They are not in the
// published types (`stripInternal` leaves them out of dist/'s declarations,
// and test/typing checks it), so a test reaches them only through these
// views, each a cast through `unknown` to the member's shape in src/. A test
// that calls a published member never goes through here.

/**
 * A KafkaFuture with the library's own completion (src/common/kafka_future.ts).
 *
 * @template T
 * @typedef {import('@confluentinc/kafka-javascript/common').KafkaFuture<T> & {
 *   internalComplete(value: T, error: unknown): boolean;
 * }} InternalFuture
 */

/**
 * Headers with the library's own read-only state, which send() sets, and
 * their copy for the native send (src/common/header/headers.ts).
 *
 * @typedef {import('@confluentinc/kafka-javascript/common/header').Headers & {
 *   setReadOnly(): void;
 *   isReadOnly(): boolean;
 *   internalToNative(): (Uint8Array | null)[] | null;
 * }} InternalHeaders
 */

/**
 * The record as the addon takes it: topic, partition, timestamp, key,
 * value and the headers, each key's bytes then its value
 * (src/producer/producer_record.ts).
 *
 * @typedef {[string, number, bigint, Uint8Array | null, Uint8Array | null, (Uint8Array | null)[] | null]} NativeRecord
 */

/**
 * A ProducerRecord with the library's own conversion for the addon.
 *
 * @template K, V
 * @typedef {import('@confluentinc/kafka-javascript/producer').ProducerRecord<K, V> & {
 *   internalToNative(key?: Uint8Array | null, value?: Uint8Array | null): NativeRecord;
 * }} InternalProducerRecord
 */

/**
 * The fields of the library's own RecordMetadata factory, as the addon gives them
 * (src/producer/record_metadata.ts).
 *
 * @typedef {{ topic: string, partition: number, hasOffset: boolean, offset: bigint, hasTimestamp: boolean, timestamp: bigint, serializedKeySize: number, serializedValueSize: number }} RecordMetadataFields
 */

/**
 * A new KafkaFuture from the library's own factory, `KafkaFuture.internalCreate`:
 * Java's `new KafkaFutureImpl<>()`, or a record's future when `record`.
 *
 * @template [T=unknown]
 * @param {boolean} record whether it is a record's future (Java's FutureRecordMetadata)
 * @returns {InternalFuture<T>} the future, not completed
 */
function createFuture(record) {
  const { KafkaFuture } = require('@confluentinc/kafka-javascript/common');
  const factory = /** @type {{ internalCreate(record: boolean): InternalFuture<T> }} */ (/** @type {unknown} */ (KafkaFuture));
  return factory.internalCreate(record);
}

/**
 * `future` with the library's own completion.
 *
 * @template T
 * @param {import('@confluentinc/kafka-javascript/common').KafkaFuture<T>} future a future the library made
 * @returns {InternalFuture<T>} the same future
 */
function internalFuture(future) {
  return /** @type {InternalFuture<T>} */ (/** @type {unknown} */ (future));
}

/**
 * `headers` with the library's own read-only state and native copy.
 *
 * @param {import('@confluentinc/kafka-javascript/common/header').Headers} headers the headers
 * @returns {InternalHeaders} the same headers
 */
function internalHeaders(headers) {
  return /** @type {InternalHeaders} */ (/** @type {unknown} */ (headers));
}

/**
 * `record` with the library's own conversion for the addon.
 *
 * @template K, V
 * @param {import('@confluentinc/kafka-javascript/producer').ProducerRecord<K, V>} record the record
 * @returns {InternalProducerRecord<K, V>} the same record
 */
function internalRecord(record) {
  return /** @type {InternalProducerRecord<K, V>} */ (/** @type {unknown} */ (record));
}

/**
 * A RecordMetadata from the library's own factory, `RecordMetadata.internalCreate`.
 *
 * @param {RecordMetadataFields} fields the fields, as the addon gives them
 * @returns {import('@confluentinc/kafka-javascript/producer').RecordMetadata} the metadata
 */
function createRecordMetadata(fields) {
  const { RecordMetadata } = require('@confluentinc/kafka-javascript/producer');
  const factory =
    /** @type {{ internalCreate(fields: RecordMetadataFields): import('@confluentinc/kafka-javascript/producer').RecordMetadata }} */ (
      /** @type {unknown} */ (RecordMetadata)
    );
  return factory.internalCreate(fields);
}

/**
 * A ConsumerGroupMetadata from the library's own factory,
 * `ConsumerGroupMetadata.internalCreate`, which the addon calls with the
 * group metadata the core gives (src/consumer/consumer_group_metadata.ts).
 *
 * @param {string} groupId the group ID
 * @param {number} generationId the generation ID
 * @param {string} memberId the member ID
 * @param {string | null} groupInstanceId the group instance ID, or null
 * @returns {import('@confluentinc/kafka-javascript/consumer').ConsumerGroupMetadata} the metadata
 */
function createConsumerGroupMetadata(groupId, generationId, memberId, groupInstanceId) {
  const { ConsumerGroupMetadata } = require('@confluentinc/kafka-javascript/consumer');
  const factory =
    /** @type {{ internalCreate(groupId: string, generationId: number, memberId: string, groupInstanceId: string | null): import('@confluentinc/kafka-javascript/consumer').ConsumerGroupMetadata }} */ (
      /** @type {unknown} */ (ConsumerGroupMetadata)
    );
  return factory.internalCreate(groupId, generationId, memberId, groupInstanceId);
}

/**
 * The private FFI id of an error class, its `_ffiId`.
 *
 * @param {object} cls an error class
 * @returns {unknown} the id: a number on every class the registry knows
 */
function ffiId(cls) {
  return /** @type {{ _ffiId?: unknown }} */ (/** @type {unknown} */ (cls))._ffiId;
}

module.exports = {
  createFuture,
  internalFuture,
  internalHeaders,
  internalRecord,
  createRecordMetadata,
  createConsumerGroupMetadata,
  ffiId,
};
