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
 * client has: the `Consumer` interface, `KafkaConsumer` and `MockConsumer`,
 * the consumer's value types (`ConsumerRecord`, `ConsumerRecords`, `OffsetAndMetadata`,
 * `OffsetAndTimestamp`, `SubscriptionPattern`, `ConsumerGroupMetadata`,
 * `CloseOptions` and the `GroupProtocol` enum), the interfaces its user
 * implements (`ConsumerRebalanceListener` and `OffsetCommitCallback`),
 * `ConsumerConfig` and the type of a consumer's configuration, and the
 * consumer's error classes, generated from the Java sources.
 *
 * `KafkaConsumer` returns keys and values as Buffers. A key or value from
 * `poll()` points into the memory the client fetched, and a value you keep
 * holds that whole fetch in memory. To keep a value for long, copy it with
 * `Buffer.from(value)`. Electron doesn't allow external buffers, so on
 * Electron the client copies each consumed key and value. Every consumer needs
 * `group.protocol: 'consumer'`: the classic
 * protocol is not supported.
 *
 * `MockConsumer` is the core's mock, reached as `KafkaConsumer` is: its
 * waiting methods return Promises, it takes one call at a time, it keeps Node
 * running until `close()`, so a test closes every mock, and it emits no
 * preview warning. Its type arguments are written, as Java's are, since it
 * takes no deserializers: `new MockConsumer<string, string>('earliest')`.
 * `poll()` returns the very records `addRecord()` was given, each held until
 * the mock is garbage collected after its `close()`, and an error given to
 * `setPollException()` or `setOffsetsException()` comes back as the very same
 * object, once. Where the core's mock differs from Java's, the mock does as
 * the core's does:
 * - while another call on the mock is pending, `addRecord()`,
 *   `setMaxPollRecords()`, `updatePartitions()` and the waiting methods throw
 *   or reject with `ConcurrentModificationError`, and
 *   `updateBeginningOffsets()`, `updateEndOffsets()`,
 *   `updateDurationOffsets()`, `setPollException()`, `setOffsetsException()`
 *   and `scheduleNopPollTask()` are ignored, where Java's mock waits and
 *   applies them;
 * - a `null` offset given to an `update*Offsets()` method is a `TypeError`,
 *   where Java's mock keeps the null;
 * - a `null` topic or partitions given to `updatePartitions()`, or a
 *   `PartitionInfo` there with a null topic, replica list, replica or host, is
 *   a `TypeError`, where Java's mock stores it, and so is
 *   `close({ option: null })`, which Java's mock ignores;
 * - `subscribe({ pattern: null })` rejects with "Topic pattern to subscribe to
 *   cannot be null", where Java's mock says "Topic pattern cannot be null";
 * - `metrics()` answers an empty map after `close()` too, where Java's mock
 *   throws "This consumer has already been closed.";
 * - `addRecord(null)` is a `TypeError` even on a closed mock, where Java's
 *   mock reports the closed mock first.
 *
 * Not in this release yet: `commitAsync`'s forms with an
 * `OffsetCommitCallback`, `subscribe`'s forms with a
 * `ConsumerRebalanceListener`, and `KafkaConsumer`'s constructor forms with
 * deserializers, which throw `UnsupportedOperationError`.
 *
 * Left out for want of an FFI entry point: `clientInstanceId`,
 * `registerMetricForSubscription` and `unregisterMetricFromSubscription`, and
 * `MockConsumer`'s `setClientInstanceId`, `disableTelemetry`,
 * `injectTimeoutException`, `schedulePollTask` and `addedMetrics`.
 * Left out, as in Python: `enforceRebalance()` and `enforceRebalance(reason)`,
 * which Java's consumer only logs as unsupported under the consumer group
 * protocol, the only one this client has, and `MockConsumer`'s
 * `shouldRebalance()` and `resetShouldRebalance()`, which only track it.
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
 * `MockConsumer(OffsetResetStrategy)` (pass the strategy's name),
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
export { MockConsumer } from './mock_consumer';
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
