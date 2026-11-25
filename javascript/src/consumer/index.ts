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

/**
 * The public classes of Java's `org.apache.kafka.clients.consumer` this
 * client has: the `Consumer` interface and `KafkaConsumer`, the consumer's
 * value types (`ConsumerRecord`, `ConsumerRecords`, `OffsetAndMetadata`,
 * `OffsetAndTimestamp`, `SubscriptionPattern`, `ConsumerGroupMetadata`,
 * `CloseOptions` and the `GroupProtocol` enum), the interfaces its user
 * implements (`ConsumerRebalanceListener` and `OffsetCommitCallback`),
 * `ConsumerConfig` and the type of a consumer's configuration, and the
 * consumer's error classes, generated from the Java sources.
 *
 * `KafkaConsumer` returns keys and values as Buffers over the memory it
 * fetched: a key or value you keep holds that whole fetch in memory, so copy
 * one you keep for long with `Buffer.from(value)`. Electron doesn't allow
 * external buffers, so on Electron the client copies each consumed key and
 * value. Every consumer needs `group.protocol: 'consumer'`: the classic
 * protocol is not supported.
 *
 * Not in this release yet: `Consumer` and `KafkaConsumer`'s offset, commit and
 * metadata calls (`commitSync`, `commitAsync`, `seek`, `seekToBeginning`,
 * `seekToEnd`, `position`, `committed`, `partitionsFor`, `listTopics`,
 * `beginningOffsets`, `endOffsets`, `offsetsForTimes`, `currentLag`,
 * `groupMetadata` and `enforceRebalance`), `subscribe`'s forms with a
 * `ConsumerRebalanceListener`, and `KafkaConsumer`'s constructor forms with
 * deserializers, which throw `UnsupportedOperationError`.
 *
 * Left out for want of an FFI entry point: `clientInstanceId`,
 * `registerMetricForSubscription` and `unregisterMetricFromSubscription`.
 * Left out because the JS conventions take only a `SubscriptionPattern` as a
 * pattern: the `subscribe(Pattern)` and `subscribe(Pattern,
 * ConsumerRebalanceListener)` forms; a `RegExp` throws a `TypeError` that
 * points to `SubscriptionPattern`.
 *
 * `ConsumerConfig` holds Java's constants only, as its `ConfigDef` machinery
 * is not translated: `ConsumerConfig(Properties)`, `ConsumerConfig(Map)`,
 * `appendDeserializerToConfig`, `configNames()`, `configDef()` and
 * `main(String[])` are left out, and constructing it throws `TypeError`. Its
 * constants include those Java's class inherits from `AbstractConfig`, such
 * as `CONFIG_PROVIDERS_CONFIG`.
 *
 * Left out because Java deprecates them: `close(Duration)` (use
 * `close({ option: CloseOptions.timeout({ timeout }) })`),
 * `ConsumerRecords(Map)` and `ConsumerGroupMetadata`'s public constructors;
 * a `ConsumerGroupMetadata` comes from the consumer.
 *
 * @packageDocumentation
 */

export { CloseOptions } from './close_options';
export type { Consumer } from './consumer';
export type { ConsumerConfigs } from './consumer_configs';
export { ConsumerGroupMetadata } from './consumer_group_metadata';
export type { ConsumerRebalanceListener } from './consumer_rebalance_listener';
export { ConsumerRecord } from './consumer_record';
export { ConsumerRecords } from './consumer_records';
export { GroupProtocol } from './group_protocol';
export { KafkaConsumer } from './kafka_consumer';
export { OffsetAndMetadata } from './offset_and_metadata';
export { OffsetAndTimestamp } from './offset_and_timestamp';
export type { OffsetCommitCallback } from './offset_commit_callback';
export { SubscriptionPattern } from './subscription_pattern';
// BEGIN GENERATED EXPORTS (cargo xtask generate-error-codes; do not edit)
export { CommitFailedError } from './commit_failed_error';
export { ConsumerConfig } from './consumer_config';
export { InvalidOffsetError } from './invalid_offset_error';
export { LogTruncationError } from './log_truncation_error';
export { NoOffsetForPartitionError } from './no_offset_for_partition_error';
export { OffsetOutOfRangeError } from './offset_out_of_range_error';
export { RetriableCommitFailedError } from './retriable_commit_failed_error';
// END GENERATED EXPORTS

// tsc's CommonJS output sets `exports.__esModule`: declared, a default import
// under `esModuleInterop` is a compile error rather than `undefined` (import by
// name or `import * as` instead; Node's own ESM default import still works).
export declare const __esModule: true;
