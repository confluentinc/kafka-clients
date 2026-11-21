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
 * The private FFI id of an error class, its `_ffiId`.
 *
 * @param {object} cls an error class
 * @returns {unknown} the id: a number on every class the registry knows
 */
function ffiId(cls) {
  return /** @type {{ _ffiId?: unknown }} */ (/** @type {unknown} */ (cls))._ffiId;
}

/**
 * A new topic as the addon takes it (src/admin/new_topic.ts).
 *
 * @typedef {{ name: string | null, numPartitions: number, replicationFactor: number, assignments: Array<[number, number[]]> | null, configs: Array<[string, string | null]> | null }} NativeNewTopic
 */

/**
 * `topic` with the library's own conversion for the addon.
 *
 * @param {import('@confluentinc/kafka-javascript/admin').NewTopic} topic the new topic
 * @returns {{ internalToNative(): NativeNewTopic }} the same topic
 */
function internalNewTopic(topic) {
  return /** @type {{ internalToNative(): NativeNewTopic }} */ (/** @type {unknown} */ (topic));
}

/**
 * Java's package-private factories of a Result keyed by topic id or by name,
 * `ofTopicIds` / `ofTopicNames`, as the library's `internalOfTopicIds` /
 * `internalOfTopicNames` (src/admin/delete_topics_result.ts,
 * src/admin/describe_topics_result.ts).
 *
 * @template V, R
 * @param {{ prototype: R }} cls the Result class
 * @returns {{ internalOfTopicIds(futures: import('@confluentinc/kafka-javascript/common').UuidMap<V>): R, internalOfTopicNames(futures: Map<string, V>): R }} its factories
 */
function topicResultFactories(cls) {
  return /** @type {{ internalOfTopicIds(futures: import('@confluentinc/kafka-javascript/common').UuidMap<V>): R, internalOfTopicNames(futures: Map<string, V>): R }} */ (
    /** @type {unknown} */ (cls)
  );
}

/**
 * Java's package-private `DeleteConsumerGroupOffsetsResult(future,
 * partitions)`, the library's `internalCreate`, the errors null for Java's
 * `Errors.NONE` (src/admin/delete_consumer_group_offsets_result.ts).
 *
 * @param {import('@confluentinc/kafka-javascript/common').KafkaFuture<import('@confluentinc/kafka-javascript/common').TopicPartitionMap<Error | null>>} future the future of each partition's error
 * @param {import('@confluentinc/kafka-javascript/common').TopicPartitionSet} partitions the partitions of the request
 * @returns {import('@confluentinc/kafka-javascript/admin').DeleteConsumerGroupOffsetsResult} the result
 */
function deleteConsumerGroupOffsetsResult(future, partitions) {
  const { DeleteConsumerGroupOffsetsResult } = require('@confluentinc/kafka-javascript/admin');
  const factory =
    /** @type {{ internalCreate(f: typeof future, p: typeof partitions): import('@confluentinc/kafka-javascript/admin').DeleteConsumerGroupOffsetsResult }} */ (
      /** @type {unknown} */ (DeleteConsumerGroupOffsetsResult)
    );
  return factory.internalCreate(future, partitions);
}

/**
 * Java's package-private `RemoveMembersFromConsumerGroupResult(future,
 * memberInfos)`, the library's `internalCreate`, the errors keyed by group
 * instance id and null for Java's `Errors.NONE`
 * (src/admin/remove_members_from_consumer_group_result.ts).
 *
 * @param {import('@confluentinc/kafka-javascript/common').KafkaFuture<Map<string | null, Error | null>>} future the future of each member's error
 * @param {import('@confluentinc/kafka-javascript/admin').MemberToRemoveSet} memberInfos the members of the request
 * @returns {import('@confluentinc/kafka-javascript/admin').RemoveMembersFromConsumerGroupResult} the result
 */
function removeMembersResult(future, memberInfos) {
  const { RemoveMembersFromConsumerGroupResult } = require('@confluentinc/kafka-javascript/admin');
  const factory =
    /** @type {{ internalCreate(f: typeof future, m: typeof memberInfos): import('@confluentinc/kafka-javascript/admin').RemoveMembersFromConsumerGroupResult }} */ (
      /** @type {unknown} */ (RemoveMembersFromConsumerGroupResult)
    );
  return factory.internalCreate(future, memberInfos);
}

/**
 * Java's package-private `ListGroupsResult(future)`, the library's
 * `internalCreate` (src/admin/list_groups_result.ts).
 *
 * @param {import('@confluentinc/kafka-javascript/common').KafkaFuture<Iterable<unknown>>} future the listings and errors
 * @returns {import('@confluentinc/kafka-javascript/admin').ListGroupsResult} the result
 */
function listGroupsResult(future) {
  const { ListGroupsResult } = require('@confluentinc/kafka-javascript/admin');
  const factory = /** @type {{ internalCreate(f: typeof future): import('@confluentinc/kafka-javascript/admin').ListGroupsResult }} */ (
    /** @type {unknown} */ (ListGroupsResult)
  );
  return factory.internalCreate(future);
}

/**
 * Java's package-private `ElectLeadersResult(electionFuture)`, the library's
 * `internalCreate` (src/admin/elect_leaders_result.ts).
 *
 * @param {import('@confluentinc/kafka-javascript/common').KafkaFuture<import('@confluentinc/kafka-javascript/common').TopicPartitionMap<unknown>>} future the election's outcome by partition
 * @returns {import('@confluentinc/kafka-javascript/admin').ElectLeadersResult} the result
 */
function electLeadersResult(future) {
  const { ElectLeadersResult } = require('@confluentinc/kafka-javascript/admin');
  const factory = /** @type {{ internalCreate(f: typeof future): import('@confluentinc/kafka-javascript/admin').ElectLeadersResult }} */ (
    /** @type {unknown} */ (ElectLeadersResult)
  );
  return factory.internalCreate(future);
}

module.exports = {
  electLeadersResult,
  createFuture,
  internalFuture,
  internalHeaders,
  internalRecord,
  createRecordMetadata,
  ffiId,
  internalNewTopic,
  topicResultFactories,
  deleteConsumerGroupOffsetsResult,
  removeMembersResult,
  listGroupsResult,
};
