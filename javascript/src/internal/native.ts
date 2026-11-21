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

/** A native admin client handle (native/admin.c), opaque to JavaScript. */
type AdminHandle = object;

/** The box over an admin RPC's native result (native/admin.c), opaque to JavaScript. */
type AdminResultBox = object;

/** A native MockAdminClient.Builder handle (native/admin.c), opaque to JavaScript. */
type MockAdminBuilderHandle = object;

/** A node's fields as the addon takes them (native/admin.c, admin_node_from_js). */
interface NodeFields {
  id: number;
  host: string | null;
  port: number;
  rack: string | null;
  isFenced: boolean;
}

/** The counts of native/admin.c, for the lifetime tests. */
interface AdminStats {
  adminsMade: number;
  adminsFreed: number;
  resultsMade: number;
  resultsFreed: number;
  futuresMade: number;
  futuresFreed: number;
  waitsStarted: number;
  waitsCompleted: number;
}

/**
 * The setters an admin options object ran, in the order they last ran, as
 * `[setter, value]` pairs, or null for Java's null options (the FFI reports
 * it).
 */
type AdminOptionsSetters = Array<[string, unknown]> | null;

// An ACL binding or filter as the addon passes it: [resourceTypeCode, name,
// patternTypeCode, principal, host, operationCode, permissionTypeCode], each
// enum as Java's byte code; a filter's name, principal and host may be null.
type AclFields = [number, string | null, number, string | null, string | null, number, number];

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
  // The counts of native/metrics.c. Test-only.
  metricsStats(): MetricsStats;
  // The counts of native/headers.c. Test-only.
  headerStats(): HeaderStats;
  // Admin.create(configs): the configuration's [key, value] pairs,
  // encodeLikeJava's bytes of each.
  adminCreate(entries: Array<[Uint8Array, Uint8Array]>): AdminHandle;
  // Admin.close() for a timeoutMs of null, else close(Duration), through the
  // `_cb` twins; closedNow: this close closed the client.
  adminClose(handle: AdminHandle, timeoutMs: number | bigint | null, onComplete: (closedNow: boolean) => void): void;
  // The future accessor `name` of a result hands out (its key null, or a
  // string), as its slot in the client's table.
  adminResultFuture(handle: AdminHandle, box: AdminResultBox, name: string, key: unknown): number;
  // The futures of a map accessor, as [key, slot] pairs, each key as the
  // addon reads it.
  adminResultFutures(handle: AdminHandle, box: AdminResultBox, name: string): Array<[unknown, number]> | null;
  // The untimed wait on a future, through get_cb, once: `done` gets its
  // value, converted to plain values, or its error.
  adminFutureWait(handle: AdminHandle, slot: number, done: (value: unknown, error: Error | null) => void): void;
  // A timed wait through get_with_timeout_cb: timedOut when the wait timed
  // out, the error the core's TimeoutError; the outcome comes from the
  // untimed wait.
  adminFutureGet(handle: AdminHandle, slot: number, timeoutMs: number, done: (error: Error | null, timedOut: boolean) => void): void;
  // The RPCs: each returns its result's box at once; a null options is
  // Java's null, which the FFI reports.
  adminDescribeCluster(handle: AdminHandle, options: AdminOptionsSetters): AdminResultBox;
  adminListTopics(handle: AdminHandle, options: AdminOptionsSetters): AdminResultBox;
  // Admin.createTopics: each NewTopic as its fields (NewTopic.internalToNative).
  adminCreateTopics(handle: AdminHandle, newTopics: unknown[], options: AdminOptionsSetters): AdminResultBox;
  // Admin.deleteTopics and describeTopics: form 0 for names (Java's
  // Collection<String> form), 1 for a TopicNameCollection, 2 for a
  // TopicIdCollection, the ids as [mostSigBits, leastSigBits].
  adminDeleteTopics(handle: AdminHandle, form: number, items: unknown[], options: AdminOptionsSetters): AdminResultBox;
  adminDescribeTopics(handle: AdminHandle, form: number, items: unknown[], options: AdminOptionsSetters): AdminResultBox;
  // Admin.createPartitions: [topic, NewPartitions fields] pairs.
  adminCreatePartitions(handle: AdminHandle, newPartitions: Array<[string, unknown]>, options: AdminOptionsSetters): AdminResultBox;
  // Admin.deleteRecords: [[topic, partition], beforeOffset] pairs.
  adminDeleteRecords(
    handle: AdminHandle,
    recordsToDelete: Array<[[string | null, number], bigint]>,
    options: AdminOptionsSetters,
  ): AdminResultBox;
  // Admin.describeConfigs: each resource as [typeId, name] (Java's byte id).
  adminDescribeConfigs(handle: AdminHandle, resources: Array<[number, string]>, options: AdminOptionsSetters): AdminResultBox;
  // Admin.incrementalAlterConfigs: [[typeId, name], [[opId, name, value]]] pairs.
  adminIncrementalAlterConfigs(
    handle: AdminHandle,
    configs: Array<[[number, string], Array<[number, string, string | null]>]>,
    options: AdminOptionsSetters,
  ): AdminResultBox;
  // Admin.listConfigResources: the types as Java's byte ids (none: every type).
  adminListConfigResources(handle: AdminHandle, typeIds: number[], options: AdminOptionsSetters): AdminResultBox;
  // Admin.describeLogDirs: the broker ids.
  adminDescribeLogDirs(handle: AdminHandle, brokers: number[], options: AdminOptionsSetters): AdminResultBox;
  // Admin.alterReplicaLogDirs: [[topic, partition, brokerId], logDir] pairs.
  adminAlterReplicaLogDirs(
    handle: AdminHandle,
    replicaAssignment: Array<[[string, number, number], string | null]>,
    options: AdminOptionsSetters,
  ): AdminResultBox;
  // Admin.describeReplicaLogDirs: each replica as [topic, partition, brokerId].
  adminDescribeReplicaLogDirs(handle: AdminHandle, replicas: Array<[string, number, number]>, options: AdminOptionsSetters): AdminResultBox;
  // Admin.electLeaders: the election type's ordinal, the partitions as
  // [topic, partition] pairs or null for every partition.
  adminElectLeaders(
    handle: AdminHandle,
    electionType: number,
    partitions: Array<[string, number]> | null,
    options: AdminOptionsSetters,
  ): AdminResultBox;
  // Admin.alterPartitionReassignments: [[topic, partition], targetReplicas or
  // null] pairs, null cancelling.
  adminAlterPartitionReassignments(
    handle: AdminHandle,
    reassignments: Array<[[string, number], number[] | null]>,
    options: AdminOptionsSetters,
  ): AdminResultBox;
  // Admin.listPartitionReassignments: the partitions, or null for every one.
  adminListPartitionReassignments(
    handle: AdminHandle,
    partitions: Array<[string, number]> | null,
    options: AdminOptionsSetters,
  ): AdminResultBox;
  // Admin.listOffsets: [[topic, partition], spec, timestamp] rows (the spec in
  // the C enum's order), the options from Java's READ_COMMITTED constructor
  // when readCommitted.
  adminListOffsets(
    handle: AdminHandle,
    topicPartitionOffsets: Array<[[string, number], number, bigint]>,
    readCommitted: boolean,
    options: AdminOptionsSetters,
  ): AdminResultBox;
  // Admin.listGroups: the futures `all`, `valid` and `errors`.
  adminListGroups(handle: AdminHandle, options: AdminOptionsSetters): AdminResultBox;
  // Admin.describeConsumerGroups, describeClassicGroups and deleteConsumerGroups:
  // the group ids.
  adminDescribeConsumerGroups(handle: AdminHandle, groupIds: string[], options: AdminOptionsSetters): AdminResultBox;
  adminDescribeClassicGroups(handle: AdminHandle, groupIds: string[], options: AdminOptionsSetters): AdminResultBox;
  adminDeleteConsumerGroups(handle: AdminHandle, groupIds: string[], options: AdminOptionsSetters): AdminResultBox;
  // Admin.listConsumerGroupOffsets: the group id (Java's (groupId) form) or,
  // for a null one, the specs as [groupId, [topic, partition] pairs or null].
  adminListConsumerGroupOffsets(
    handle: AdminHandle,
    groupId: string | null,
    groupSpecs: Array<[string, Array<[string, number]> | null]> | null,
    options: AdminOptionsSetters,
  ): AdminResultBox;
  // Admin.alterConsumerGroupOffsets: [[topic, partition], offset, leaderEpoch
  // (-1: none), metadata] rows.
  adminAlterConsumerGroupOffsets(
    handle: AdminHandle,
    groupId: string,
    offsets: Array<[[string, number], bigint, number, string]>,
    options: AdminOptionsSetters,
  ): AdminResultBox;
  // Admin.deleteConsumerGroupOffsets: [topic, partition] pairs.
  adminDeleteConsumerGroupOffsets(
    handle: AdminHandle,
    groupId: string,
    partitions: Array<[string, number]>,
    options: AdminOptionsSetters,
  ): AdminResultBox;
  // Admin.removeMembersFromConsumerGroup: the members' group instance ids, or
  // null in removeAll mode.
  adminRemoveMembersFromConsumerGroup(
    handle: AdminHandle,
    groupId: string,
    members: string[] | null,
    options: AdminOptionsSetters,
  ): AdminResultBox;
  // Admin.createAcls: each binding as [resourceTypeCode, name,
  // patternTypeCode, principal, host, operationCode, permissionTypeCode].
  adminCreateAcls(handle: AdminHandle, acls: AclFields[], options: AdminOptionsSetters): AdminResultBox;
  // Admin.describeAcls: the filter as its seven fields, a name, principal or
  // host null for any.
  adminDescribeAcls(handle: AdminHandle, filter: AclFields, options: AdminOptionsSetters): AdminResultBox;
  // Admin.deleteAcls: each filter as its seven fields.
  adminDeleteAcls(handle: AdminHandle, filters: AclFields[], options: AdminOptionsSetters): AdminResultBox;
  // Admin.describeClientQuotas: the filter as [strict, [[entityType,
  // matchKind (0 exact, 1 default, 2 any), name | null], ...]].
  adminDescribeClientQuotas(
    handle: AdminHandle,
    filter: [boolean, Array<[string, number, string | null]>],
    options: AdminOptionsSetters,
  ): AdminResultBox;
  // Admin.alterClientQuotas: each alteration as [entityEntries, ops], an
  // entity's entries [[entityType, name | null], ...], an op [key, value |
  // null].
  adminAlterClientQuotas(
    handle: AdminHandle,
    entries: Array<[Array<[string, string | null]>, Array<[string, number | null]>]>,
    options: AdminOptionsSetters,
  ): AdminResultBox;
  // Admin.describeUserScramCredentials: the users, or null for every user.
  adminDescribeUserScramCredentials(handle: AdminHandle, users: string[] | null, options: AdminOptionsSetters): AdminResultBox;
  // Admin.alterUserScramCredentials: an upsertion as [0, user,
  // mechanismType, iterations, password, salt], a deletion as [1, user,
  // mechanismType].
  adminAlterUserScramCredentials(
    handle: AdminHandle,
    alterations: Array<[0, string, number, number, Uint8Array, Uint8Array] | [1, string, number]>,
    options: AdminOptionsSetters,
  ): AdminResultBox;
  // Admin.createDelegationToken.
  adminCreateDelegationToken(handle: AdminHandle, options: AdminOptionsSetters): AdminResultBox;
  // Admin.renewDelegationToken: the HMAC, copied during the call.
  adminRenewDelegationToken(handle: AdminHandle, hmac: Uint8Array, options: AdminOptionsSetters): AdminResultBox;
  // Admin.expireDelegationToken: the HMAC, copied during the call.
  adminExpireDelegationToken(handle: AdminHandle, hmac: Uint8Array, options: AdminOptionsSetters): AdminResultBox;
  // Admin.describeDelegationToken.
  adminDescribeDelegationToken(handle: AdminHandle, options: AdminOptionsSetters): AdminResultBox;
  // Admin.describeProducers: the partitions as [topic, partition].
  adminDescribeProducers(handle: AdminHandle, partitions: Array<[string, number]>, options: AdminOptionsSetters): AdminResultBox;
  // Admin.describeTransactions: the transactional ids.
  adminDescribeTransactions(handle: AdminHandle, transactionalIds: string[], options: AdminOptionsSetters): AdminResultBox;
  // Admin.abortTransaction: the spec as [[topic, partition], producerId,
  // producerEpoch, coordinatorEpoch].
  adminAbortTransaction(
    handle: AdminHandle,
    spec: [[string, number], bigint, number, number],
    options: AdminOptionsSetters,
  ): AdminResultBox;
  // Admin.forceTerminateTransaction: the transactional id.
  adminForceTerminateTransaction(handle: AdminHandle, transactionalId: string, options: AdminOptionsSetters): AdminResultBox;
  // Admin.listTransactions.
  adminListTransactions(handle: AdminHandle, options: AdminOptionsSetters): AdminResultBox;
  // Admin.fenceProducers: the transactional ids.
  adminFenceProducers(handle: AdminHandle, transactionalIds: string[], options: AdminOptionsSetters): AdminResultBox;
  // Admin.describeFeatures.
  adminDescribeFeatures(handle: AdminHandle, options: AdminOptionsSetters): AdminResultBox;
  // Admin.updateFeatures: [feature, [maxVersionLevel, upgradeTypeCode]]
  // pairs; an empty map or a blank feature name throws the core's error.
  adminUpdateFeatures(handle: AdminHandle, featureUpdates: Array<[string, [number, number]]>, options: AdminOptionsSetters): AdminResultBox;
  // Admin.unregisterController: the controller id.
  adminUnregisterController(handle: AdminHandle, controllerId: number, options: AdminOptionsSetters): AdminResultBox;
  // Java's MockAdminClient() (no argument) or MockAdminClient(brokers,
  // controller).
  mockAdminCreate(...args: [] | [brokers: NodeFields[] | null, controller: NodeFields | null]): AdminHandle;
  // Java's MockAdminClient.controller(Node) and timeoutNextRequest(int).
  mockAdminController(handle: AdminHandle, node: NodeFields | null): void;
  mockAdminTimeoutNextRequest(handle: AdminHandle, numberOfRequests: number): void;
  // Java's MockAdminClient.addTopic(internal, name, partitions, configs),
  // markTopicForDeletion(name) and setFetchesRemainingUntilVisible(topicName, n).
  mockAdminAddTopic(
    handle: AdminHandle,
    internal: boolean,
    name: string | null,
    partitions: unknown[],
    configs: Array<[string, string | null]> | null,
  ): void;
  mockAdminMarkTopicForDeletion(handle: AdminHandle, name: string | null): void;
  mockAdminSetFetchesRemainingUntilVisible(handle: AdminHandle, topicName: string | null, fetchesRemainingUntilVisible: number): void;
  // Java's MockAdminClient.updateEndOffsets (end) or updateBeginningOffsets:
  // [[topic, partition], offset] pairs.
  mockAdminUpdateOffsets(handle: AdminHandle, end: boolean, newOffsets: Array<[[string, number], bigint]>): void;
  // Java's MockAdminClient.updateConsumerGroupOffsets: [[topic, partition], offset] pairs.
  mockAdminUpdateConsumerGroupOffsets(handle: AdminHandle, newOffsets: Array<[[string, number], bigint]>): void;
  // Java's new MockAdminClient.Builder(), its setters, run at once, and build().
  mockAdminBuilderNew(): MockAdminBuilderHandle;
  mockAdminBuilderSet(builder: MockAdminBuilderHandle, setter: string, value: unknown): void;
  mockAdminBuilderBuild(builder: MockAdminBuilderHandle): AdminHandle;
  // The counts of native/admin.c. Test-only.
  adminStats(): AdminStats;
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
