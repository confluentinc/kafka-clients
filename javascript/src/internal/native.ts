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

// Loads the Node-API addon (private) and does its one-time setup: the native
// layer builds its errors and metadata through the JS classes registered
// here, so every module that needs the addon requires this one, and no
// index.ts does any setup.
//
// The addon is `confluent-kafka-javascript.node`, from this machine's
// platform package or else the development build (`native_loader.ts`).

import { join } from 'node:path';

import { debug } from './debug';
import { fromNative } from './errors';
import { loadAddon, platformKey } from './native_loader';
import { warn } from './warn';
import { RecordMetadata } from '../producer/record_metadata';

/** A metric name's fields, as the addon reads them (native/metrics.c, metric_name_to_js). */
interface NativeMetricName {
  name: string;
  group: string;
  description: string;
  // The tags sorted by key.
  tags: Array<[string, string]>;
}

/** A live metrics view the addon owns (native/metrics.c), opaque to JavaScript. */
type MetricsViewHandle = object;

/** A metric taken from a view, which the addon owns (native/metrics.c), opaque to JavaScript. */
type MetricHandle = object;

/** A metric as the view gives it: its name's fields and its handle. */
type NativeMetricEntry = [NativeMetricName, MetricHandle];

/** The counts of native/metrics.c, for the memory tests: the view and metric handles made, and destroyed. */
interface MetricsStats {
  viewsMade: number;
  viewsFreed: number;
  metricsMade: number;
  metricsFreed: number;
}

/** A node's fields, as the addon reads them (native/producer.c, node_to_js). */
interface NativeNode {
  id: number;
  host: string | null;
  port: number;
  rack: string | null;
  isFenced: boolean;
}

/** A partition's fields, as the addon reads them (native/producer.c, partition_info_to_js). */
interface NativePartitionInfo {
  topic: string;
  partition: number;
  leader: NativeNode | null;
  replicas: NativeNode[] | null;
  inSyncReplicas: NativeNode[] | null;
  offlineReplicas: NativeNode[] | null;
}

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

/** A native producer handle (native/producer.c), opaque to JavaScript. */
type ProducerHandle = object;

/** The counts of native/headers.c, for the memory tests: the native RecordHeaders the sends made, and destroyed. */
interface HeaderStats {
  made: number;
  freed: number;
}

/** The counts of native/producer.c, for the memory tests: each object or unit made, and freed or given back. */
interface ProducerStats {
  producersMade: number;
  producersFreed: number;
  // The units of a producer struct's `owners`: the handle object, the threadsafe function, each wait and deferral.
  ownerUnitsTaken: number;
  ownerUnitsReleased: number;
  contextsMade: number;
  contextsFreed: number;
  slotsTaken: number;
  slotsReturned: number;
  futuresHeld: number;
  futuresDestroyed: number;
  waitsStarted: number;
  waitsCompleted: number;
  // A callback's Promise awaited: deferred, reported, and its function finalized.
  deferralsMade: number;
  deferralsReported: number;
  deferralsReleased: number;
  bufferRefsTaken: number;
  bufferRefsReleased: number;
  mockRecordsMade: number;
  mockRecordsFreed: number;
  injectedMade: number;
  injectedFreed: number;
}

/** The counts of native/error.c, for the memory tests: owned errors received, made, freed and given to the core. */
interface ErrorStats {
  owned: number;
  made: number;
  freed: number;
  given: number;
}

/** What a send built, read back from the native record (native/record_spy.c `record_spy_capture`), for the tests. */
interface SpiedRecord {
  topic: string;
  partition: number;
  timestamp: bigint;
  key: Buffer | null;
  value: Buffer | null;
  // The key and value src/ passed the addon, and whether the record's key and value are their memory.
  keyArg: Uint8Array | null;
  valueArg: Uint8Array | null;
  keyInPlace: boolean;
  valueInPlace: boolean;
  headers: [string, Buffer | null][];
}

/** A native consumer handle (native/consumer.c), opaque to JavaScript. */
type ConsumerHandle = object;

/** A partition's records of a poll, as the addon builds them: its topic and partition, then its records. */
type NativePartitionRecords = [string, number, unknown[]];

/** A next offset of a poll, as the addon reads it: topic, partition, offset, leader epoch (-1 for none), metadata. */
type NativeNextOffset = [string, number, bigint, number, string];

/** What the addon builds each record of a poll with (src/consumer/consumer_record.ts `internalCreate`). */
type RecordFactory = (
  topic: string,
  partition: number,
  offset: bigint,
  timestamp: bigint,
  timestampTypeId: number,
  serializedKeySize: number,
  serializedValueSize: number,
  // The record's headers, until its Headers copies them out, or null for none.
  headers: object | null,
  // A Buffer over the batch's memory, or a copy where external buffers are refused.
  key: Buffer | null,
  value: Buffer | null,
  leaderEpoch: number,
  deliveryCount: number,
) => unknown;

/** The counts of native/consumer.c, for the memory tests: each object made, and freed. */
interface ConsumerStats {
  consumersMade: number;
  consumersFreed: number;
  batchesMade: number;
  batchesDestroyed: number;
  // The external buffers over a batch's memory, made and collected.
  buffersMade: number;
  buffersFreed: number;
  // The handles of a record's headers, made and collected.
  sourcesMade: number;
  sourcesFreed: number;
  // The keys and values copied, where external buffers are refused.
  copies: number;
}

/** The functions the addon registers (native/addon.c). */
interface Native {
  probe(): boolean;
  setFactories(
    error: (fields: unknown) => unknown,
    metadata: (fields: RecordMetadataFields) => RecordMetadata,
    // An error, or a thrown value, the addon has nowhere else to report.
    warn: (error: unknown) => void,
  ): void;
  // True the first time any environment of the process asks.
  previewNoticeDue(): boolean;
  // The configuration's [key, value] pairs, encodeLikeJava's bytes of each.
  producerCreate(entries: Array<[Uint8Array, Uint8Array]>): ProducerHandle;
  producerSend(
    handle: ProducerHandle,
    topic: string,
    partition: number,
    // -1n for Java's null.
    timestamp: bigint,
    key: Uint8Array | null,
    // Borrowed, not copied: the addon holds a reference on each until the
    // hand-over (javascript/CLAUDE.md, rule 8).
    value: Uint8Array | null,
    // The record's headers, each key's encodeLikeJava bytes then its value,
    // which the addon copies into a native RecordHeaders for the call, or
    // null for none.
    headers: (Uint8Array | null)[] | null,
    // The slot of the record's native future in the producer's table, which
    // a wait started before the delivery takes, null when the hand-over
    // failed (native/producer.c, the record's future).
    onHandover: (error: unknown, future: number | null) => void,
    // Returns undefined (the callback returned), the text of what it threw,
    // or a Promise of either, settling once the callback's own Promise did:
    // the report waits for it (native/producer.c, send_defer). The metadata
    // is null when the addon could not build it; the error is then the
    // failure, as it is when the delivery's error could not be built, and
    // `partition` the partition of the core's metadata, when it was read.
    onComplete: (
      metadata: RecordMetadata | null,
      error: Error | null,
      partition?: number,
    ) => string | undefined | Promise<string | undefined>,
  ): void;
  producerFlush(handle: ProducerHandle, onComplete: (error: unknown) => void): void;
  // Java's metrics() through kafka_producer_Producer_metrics (no _cb twin):
  // the live view, valid after the producer is destroyed.
  producerMetrics(handle: ProducerHandle): MetricsViewHandle;
  // The view's number of metrics, read now.
  metricsViewSize(view: MetricsViewHandle): number;
  // Whether a metric is registered now under the name (name, group, tags),
  // the tags as [key, value] pairs; Java's equality ignores the description.
  metricsViewHas(view: MetricsViewHandle, name: string, group: string, tags: Array<[string, string]>): boolean;
  // The metric registered now under that name, or null.
  metricsViewGet(view: MetricsViewHandle, name: string, group: string, tags: Array<[string, string]>): NativeMetricEntry | null;
  // The metrics registered now, each taken from the view.
  metricsViewEntries(view: MetricsViewHandle): NativeMetricEntry[];
  // The metric's value, measured now: a double or an int a number, a long a
  // bigint, a string gauge a string.
  metricValue(metric: MetricHandle): number | bigint | string | null;
  // Whether the metric's provider is a Measurable.
  metricIsMeasurable(metric: MetricHandle): boolean;
  // Java's partitionsFor through partitions_for_cb; the partitions' fields
  // as src/common/partition_info.ts's constructor takes them, each node as
  // src/common/node.ts's does. A null topic goes to the core as NULL.
  producerPartitionsFor(
    handle: ProducerHandle,
    topic: string | null,
    onComplete: (error: unknown, partitions: NativePartitionInfo[] | null) => void,
  ): void;
  producerClose(
    handle: ProducerHandle,
    // null when the timeout was left out (Java's close()), else the
    // timeout as it was given, a negative one included.
    timeoutMs: number | bigint | null,
    // closedNow: this close closed the producer (a rejected timeout, or a
    // later close, did not).
    onComplete: (error: unknown, closedNow: boolean) => void,
  ): void;
  producerPump(handle: ProducerHandle): void;
  // A native producer handle over the FFI's MockProducer, Java's
  // MockProducer(autoComplete, null, null, null): every producer* function
  // takes it, through its __as_Producer view.
  mockProducerCreate(autoComplete: boolean): ProducerHandle;
  // A mock's send of the app's own record, which the mock keeps: its key and
  // value cross as the FFI's `void *`, never read (native/producer.c,
  // MockRecord). The completions are producerSend's.
  mockProducerSend(
    handle: ProducerHandle,
    topic: string,
    partition: number,
    // -1n for Java's null.
    timestamp: bigint,
    record: object,
    onHandover: (error: unknown, future: number | null) => void,
    onComplete: (
      metadata: RecordMetadata | null,
      error: Error | null,
      partition?: number,
    ) => string | undefined | Promise<string | undefined>,
  ): void;
  // The records the mock holds, the app's own, in send order.
  mockProducerHistory(handle: ProducerHandle): object[];
  mockProducerClear(handle: ProducerHandle): void;
  // Java's public error field of the mock, set to `error` or cleared by null.
  mockProducerSetError(handle: ProducerHandle, field: 'send' | 'flush' | 'partitionsFor' | 'close', error: Error | null): void;
  // Java's errorNext(error), completeNext() for null: whether a send waited.
  mockProducerCompleteNext(handle: ProducerHandle, error: Error | null): boolean;
  // Java's closed() or flushed().
  mockProducerState(handle: ProducerHandle, which: 'closed' | 'flushed'): boolean;
  // A wait on a record's future through get_cb (timeoutMs null) or
  // get_with_timeout_cb; `done` runs once it completed, from the producer's
  // drain, or inside the call once the producer is destroyed. timedOut: the
  // wait timed out, the error the core's TimeoutError. failed: the answer
  // could not be built, the error the failure, any thrown value.
  recordFutureGet(
    handle: ProducerHandle,
    future: number,
    timeoutMs: number | null,
    done: (metadata: RecordMetadata | null, error: unknown, timedOut: boolean, failed: boolean) => void,
  ): void;
  // The configuration's [key, value] pairs, encodeLikeJava's bytes of each:
  // KafkaConsumer(configs) with NULL deserializers.
  consumerCreate(entries: Array<[Uint8Array, Uint8Array]>): ConsumerHandle;
  // Java's poll(Duration) through poll_cb; each record built by `factory`.
  // timeoutMs as it was given, a negative one included.
  consumerPoll(
    handle: ConsumerHandle,
    timeoutMs: number | bigint,
    factory: RecordFactory,
    done: (error: unknown, partitions: NativePartitionRecords[] | null, nextOffsets: NativeNextOffset[] | null) => void,
  ): void;
  // Java's subscribe(Collection) through subscribe_with_topics_cb; null goes to the FFI as NULL.
  consumerSubscribeTopics(handle: ConsumerHandle, topics: string[] | null, done: (error: unknown) => void): void;
  // Java's subscribe(SubscriptionPattern) through subscribe_with_pattern_cb:
  // `given` false for a null pattern, else the pattern's string.
  consumerSubscribePattern(handle: ConsumerHandle, given: boolean, pattern: string | null, done: (error: unknown) => void): void;
  // Java's assign, pause and resume of a Collection<TopicPartition>, as
  // [topic, partition] pairs; a null collection or element goes to the FFI as NULL.
  consumerAssign(handle: ConsumerHandle, partitions: Array<[string | null, number] | null> | null, done: (error: unknown) => void): void;
  consumerPause(handle: ConsumerHandle, partitions: Array<[string | null, number] | null> | null, done: (error: unknown) => void): void;
  consumerResume(handle: ConsumerHandle, partitions: Array<[string | null, number] | null> | null, done: (error: unknown) => void): void;
  consumerUnsubscribe(handle: ConsumerHandle, done: (error: unknown) => void): void;
  // Java's commitSync forms through their `_cb` twins: `offsets` as
  // [[topic, partition], [offset, leaderEpoch (-1 for none), metadata]]
  // entries when withOffsets, a null map, key or value going to the FFI as
  // NULL; timeoutMs null for the forms without a timeout.
  consumerCommitSync(
    handle: ConsumerHandle,
    withOffsets: boolean,
    offsets: Array<[[string | null, number] | null, [bigint, number, string] | null]> | null,
    timeoutMs: number | bigint | null,
    done: (error: unknown) => void,
  ): void;
  // Java's commitAsync(), settling once the core took the commit.
  consumerCommitAsync(handle: ConsumerHandle, done: (error: unknown) => void): void;
  // Java's seek(TopicPartition, long), or with withMetadata
  // seek(TopicPartition, OffsetAndMetadata), `offset` then the
  // OffsetAndMetadata's [offset, leaderEpoch (-1 for none), metadata]; a null
  // partition or OffsetAndMetadata goes to the FFI as NULL.
  consumerSeek(
    handle: ConsumerHandle,
    partition: [string | null, number] | null,
    withMetadata: boolean,
    offset: bigint | [bigint, number, string] | null,
    done: (error: unknown) => void,
  ): void;
  consumerSeekToBeginning(
    handle: ConsumerHandle,
    partitions: Array<[string | null, number] | null> | null,
    done: (error: unknown) => void,
  ): void;
  consumerSeekToEnd(handle: ConsumerHandle, partitions: Array<[string | null, number] | null> | null, done: (error: unknown) => void): void;
  // Java's position, timeoutMs null for the form without a timeout.
  consumerPosition(
    handle: ConsumerHandle,
    partition: [string | null, number] | null,
    timeoutMs: number | bigint | null,
    done: (error: unknown, position: bigint | null) => void,
  ): void;
  // Java's committed: each partition's [topic, partition, offset, leaderEpoch
  // (-1 for none), metadata], or [topic, partition, null] for none.
  consumerCommitted(
    handle: ConsumerHandle,
    partitions: Array<[string | null, number] | null> | null,
    timeoutMs: number | bigint | null,
    done: (error: unknown, entries: Array<[string, number, bigint | null, number?, string?]> | null) => void,
  ): void;
  // Java's currentLag: null for an empty OptionalLong.
  consumerCurrentLag(
    handle: ConsumerHandle,
    partition: [string | null, number] | null,
    done: (error: unknown, lag: bigint | null) => void,
  ): void;
  // Java's groupMetadata() through its fallible form: groupId, generationId,
  // memberId and groupInstanceId (null for none).
  consumerGroupMetadata(handle: ConsumerHandle): [string, number, string, string | null];
  // Java's partitionsFor, timeoutMs null for the form without a timeout; a
  // null topic goes to the FFI as NULL.
  consumerPartitionsFor(
    handle: ConsumerHandle,
    topic: string | null,
    timeoutMs: number | bigint | null,
    done: (error: unknown, partitions: NativePartitionInfo[] | null) => void,
  ): void;
  // Java's listTopics: each topic with its partitions.
  consumerListTopics(
    handle: ConsumerHandle,
    timeoutMs: number | bigint | null,
    done: (error: unknown, topics: Array<[string, NativePartitionInfo[]]> | null) => void,
  ): void;
  // Java's offsetsForTimes: [[topic, partition], timestamp] entries, a null
  // map or key going to the FFI as NULL; each answer [topic, partition,
  // offset, timestamp, leaderEpoch (-1 for none)], or [topic, partition, null].
  consumerOffsetsForTimes(
    handle: ConsumerHandle,
    timestampsToSearch: Array<[[string | null, number] | null, bigint]> | null,
    timeoutMs: number | bigint | null,
    done: (error: unknown, entries: Array<[string, number, bigint | null, bigint?, number?]> | null) => void,
  ): void;
  // Java's beginningOffsets and endOffsets: each answer [topic, partition, offset].
  consumerBeginningOffsets(
    handle: ConsumerHandle,
    partitions: Array<[string | null, number] | null> | null,
    timeoutMs: number | bigint | null,
    done: (error: unknown, entries: Array<[string, number, bigint]> | null) => void,
  ): void;
  consumerEndOffsets(
    handle: ConsumerHandle,
    partitions: Array<[string | null, number] | null> | null,
    timeoutMs: number | bigint | null,
    done: (error: unknown, entries: Array<[string, number, bigint]> | null) => void,
  ): void;
  // Java's assignment(), subscription() and paused(), through their fallible forms.
  consumerAssignment(handle: ConsumerHandle): Array<[string, number]>;
  consumerSubscription(handle: ConsumerHandle): string[];
  consumerPaused(handle: ConsumerHandle): Array<[string, number]>;
  consumerWakeup(handle: ConsumerHandle): void;
  // Java's metrics(): the live view, valid after the consumer is destroyed.
  consumerMetrics(handle: ConsumerHandle): MetricsViewHandle;
  // Java's close() (withOptions false), or close(CloseOptions) from its
  // timeout (null: not set) and its operation's name, a null operation for
  // a null option. closedNow: this close closed the consumer.
  consumerClose(
    handle: ConsumerHandle,
    withOptions: boolean,
    timeoutMs: number | bigint | null,
    operation: string | null,
    done: (error: unknown, closedNow: boolean) => void,
  ): void;
  // A consumed record's headers, copied out of the native record: each key a
  // string, each value a Buffer of its own or null.
  consumerRecordHeaders(source: object): Array<[string, Buffer | null]>;
  // The counts of native/consumer.c. Test-only.
  consumerStats(): ConsumerStats;
  // Makes the addon act as if Node-API refused external buffers, as Electron
  // does, for the polls completed from now on. Test-only.
  consumerRefuseExternalBuffers(on: boolean): void;
  // The counts of native/metrics.c. Test-only.
  metricsStats(): MetricsStats;
  // The counts of native/headers.c. Test-only.
  headerStats(): HeaderStats;
  // The counts of native/producer.c. Test-only.
  producerStats(): ProducerStats;
  // The counts of native/error.c. Test-only.
  errorStats(): ErrorStats;
  // Arms `times` (1 when left out) more failures of `step` of the addon, each
  // failing the next run of its site as its Node-API call failing would.
  // Test-only.
  faultNext(step: 'notify' | 'metadata' | 'error' | 'slot' | 'partitions' | 'close' | 'enqueue' | 'failure', times?: number): undefined;
  // Disarms every step, and returns the failures still armed, a name each. Test-only.
  faultNext(step: null): string[];
  // The records the sends built since the last call; arms the capture again for `arm`. Test-only.
  recordSpy(arm: boolean): SpiedRecord[];
}

// Loads an addon instance of this copy of the package's own: process.dlopen
// with a module object of its own, not require(), whose cache hands every copy
// that resolves the same .node file (two versions in node_modules sharing one
// platform package) the one instance. The factories registered below are an
// instance's, so this copy's producers build this copy's errors and
// RecordMetadata, as each copy of Java's kafka-clients, loaded by a class
// loader of its own, throws its own classes. The file is resolved as require()
// resolves it.
function loadOwnInstance(id: string): unknown {
  const addonModule = { exports: {} };
  process.dlopen(addonModule, require.resolve(id));
  return addonModule.exports;
}

// This module is dist/internal/native.js, and dist/ is beside native/ in the
// checkout, so the development build is <dist>/../native/build/Release/.
const loaded = loadAddon(loadOwnInstance, platformKey(process), join(__dirname, '..', '..'));
const native = loaded.addon as Native;
debug('addon loaded from %s', loaded.path);

// An error the addon has nowhere else to report (a failed report to the core,
// an error freeing a handle, a throw from the binding's own JS while a
// callback queue drains) goes to process.emitWarning (`warn`). The native
// layer builds RecordMetadata through its library-only factory.
native.setFactories(
  (fields) => fromNative(fields),
  (fields) => RecordMetadata.internalCreate(fields),
  warn,
);

export = native;
