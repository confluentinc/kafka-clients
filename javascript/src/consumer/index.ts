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
 * A `ConsumerRebalanceListener` given to `subscribe()` and an
 * `OffsetCommitCallback` given to `commitAsync()` run inside the waiting call
 * that delivers them (`poll()`, `unsubscribe()`, `commitSync()`, `close()`,
 * ...), which settles only after they return; each may return a Promise,
 * which that call awaits. A callback that never settles has no timeout, and
 * `wakeup()` doesn't end it: the call that ran it waits too. While one runs,
 * from any code, a timer's included, the calls a listener needs go through
 * the consumer's callback handle and work: `commitSync()`, `commitAsync()`,
 * `seek()`, `seekToBeginning()`, `seekToEnd()`, `pause()`, `resume()`,
 * `position()`, `committed()`, `partitionsFor()`, `listTopics()`,
 * `offsetsForTimes()`, `beginningOffsets()`, `endOffsets()`, `currentLag()`,
 * `assignment()`, `paused()`, `groupMetadata()` and `wakeup()`, and
 * `metrics()` answers; `poll()`, `subscribe()`, `assign()`, `unsubscribe()`,
 * `close()` and `subscription()` reject or throw with
 * `ConcurrentModificationError`, where Java's listener may call them. A call a
 * callback makes without awaiting it may run after the callback returned,
 * and then gets the consumer's longer busy message.
 *
 * What a listener or commit callback throws, or rejects with, comes back as
 * the very same value, where Java's consumer has it: a listener's from
 * `poll()` and `unsubscribe()`, a `KafkaError` as itself and anything else as
 * the `cause` of `KafkaError('User rebalance callback throws an error')`, and
 * from `close()` inside `KafkaError('Failed to close kafka consumer')`; a
 * commit callback's from `commitSync()` and `commitAsync()` as itself, from
 * `poll()` a `KafkaError` as itself and anything else inside a `KafkaError`
 * built from it, and from `close()` as the `cause` of
 * `KafkaError('Failed to close kafka consumer')`. A commit callback's
 * `offsets` is `null` for a failed commit and for an empty map committed, as
 * Java's is. `commitAsync({ callback: null })` commits without a callback, as
 * Java's `commitAsync()` does, while `commitAsync({ offsets, callback: null })`
 * rejects with a `TypeError`, where Java's commits without one.
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
 *   `setMaxPollRecords()`, `updatePartitions()`, `assignment()`,
 *   `subscription()`, `paused()`, `groupMetadata()` and the waiting methods
 *   throw or reject with `ConcurrentModificationError`, and
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
 * - `partitionsFor(null)` and `currentLag(null)` reject with a `TypeError`,
 *   where Java's mock answers `[]` and `0`, and a `null` collection given to
 *   `subscribe()`, `assign()`, `seekToBeginning()` or `seekToEnd()` rejects
 *   with `KafkaConsumer`'s `IllegalArgumentError`, where Java's mock throws
 *   a `NullPointerException`;
 * - a null partition or topic given to `assign()` or `subscribe()` rejects
 *   with a `TypeError`, where Java's mock throws a `NullPointerException`
 *   for a null partition and stores a null topic;
 * - a negative timeout rejects with `IllegalArgumentError` "Timeout must not
 *   be negative", where Java's mock ignores it;
 * - `metrics()` answers an empty map after `close()` too, where Java's mock
 *   throws "This consumer has already been closed.";
 * - `addRecord(null)` is a `TypeError` even on a closed mock, where Java's
 *   mock reports the closed mock first;
 * - inside a listener or commit callback the mock runs, `commitSync()`,
 *   `commitAsync()`, `currentLag()`, `assignment()`, `paused()` and
 *   `groupMetadata()` work, its other waiting `Consumer` methods but `close()`
 *   reject with `UnsupportedOperationError`, and `poll()`, `subscribe()`,
 *   `assign()`, `unsubscribe()`, `close()`, `subscription()`, `addRecord()`,
 *   `setMaxPollRecords()`, `updatePartitions()`, `rebalance()` and
 *   `losePartitions()` reject or throw with `ConcurrentModificationError`,
 *   while `updateBeginningOffsets()`, `updateEndOffsets()`,
 *   `updateDurationOffsets()`, `setPollException()`, `setOffsetsException()`
 *   and `scheduleNopPollTask()` are ignored, and `closed()`,
 *   `lastPollTimeout()`, `metrics()` and `wakeup()` answer, where Java's mock
 *   takes them all. A
 *   listener's or commit callback's error comes back from `rebalance()`,
 *   `losePartitions()` and `commitAsync()` as itself, and a commit callback
 *   gets the map committed, as Java's mock passes it. The mock keeps its
 *   listener after `close()`, as Java's does.
 *
 * Not in this release yet: `KafkaConsumer`'s constructor forms with
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
