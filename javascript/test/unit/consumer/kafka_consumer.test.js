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

// KafkaConsumerTest (Apache Kafka 4.4.0) for KafkaConsumer without a broker,
// with Java's messages, and the binding's own contracts that need none. Every
// consumer sets group.protocol=consumer: the core implements KIP-848 only, and
// Java's tests run for both protocols, so each port runs the consumer
// protocol's arm. Where Java drives a MockClient only to make a call wait, an
// unreachable bootstrap address stands for it.
//
// Of Java's 135 tests, 36 are ported here:
//   testConstructorClose, testOsDefaultSocketBufferSizes,
//   shouldIgnoreGroupInstanceIdForEmptyGroupId, testEmptyGroupId,
//   testGroupIdWithWhitespace,
//   testConsumerConstructorFailsWithConfigExceptionOnUnresolvableBootstrapWhenTimeoutZero,
//   testOperationsBySubscribingConsumerWithDefaultGroupId,
//   testOperationsByAssigningConsumerWithDefaultGroupId, testSubscription,
//   testSubscriptionOnNullTopicCollection, testSubscriptionOnNullTopic,
//   testSubscriptionOnEmptyTopic, testSubscriptionOnEmptyPattern (its
//   SubscriptionPattern form), testAssignOnNullTopicPartition,
//   testAssignOnEmptyTopicPartition, testAssignOnNullTopicInPartition,
//   testAssignOnEmptyTopicInPartition, testPause,
//   testPauseFlagPreservedForRetainedPartitionOnManualAssignmentChange,
//   verifyPollTimesOutDuringMetadataUpdate, testPollWithNoSubscription,
//   testPollWithEmptySubscription, testPollWithEmptyUserAssignment,
//   testPreventMultiThread, testConsumerBootstrapResolutionExceptionPropagatedToPoll,
//   testAssignedPartitionsMetrics (its assign half: the group assignment needs
//   a broker), testMetricsRemovedOnClose, testClosingConsumerUnregistersConsumerMetrics,
//   testPollIdleRatioZero (its default value: the zero-length poll is recorded
//   through the package-private kafkaConsumerMetrics() on a MockTime),
//   testCloseShouldBeIdempotent, testSeekNegative, testInvalidGroupMetadata,
//   testCommittedThrowsTimeoutExceptionForNoResponse, testOffsetsForTimesTimeout,
//   testBeginningOffsetsTimeout and testEndOffsetsTimeout.
//
// Not here yet, for want of deserializers:
// configurableObjectsShouldSeeGeneratedClientId. Against a broker, where Java
// scripts a MockClient, in test/integration/kafka_consumer_broker.test.js: 14
// cases, listed there.
//
// The other 84 are not ported, each for its reason:
// - the classic protocol only: Java runs these with group.protocol=classic,
//   and the core implements KIP-848 only (consumer-threading.md §20):
//   testPollReturnsRecords,
//   testSecondPollWithDeserializationErrorThrowsRecordDeserializationException,
//   testSubscriptionWithEmptyPartitionAssignment, verifyHeartbeatSent,
//   verifyHeartbeatSentWhenFetchedDataReady,
//   verifyNoCoordinatorLookupForManualAssignmentWithOffsetCommit,
//   testAutoCommitSentBeforePositionUpdate, testRegexSubscription,
//   testChangingRegexSubscription, testWakeupWithFetchDataAvailable,
//   testPollThrowsInterruptExceptionIfInterrupted,
//   testSubscriptionChangesWithAutoCommitEnabled,
//   testSubscriptionChangesWithAutoCommitDisabled,
//   testUnsubscribeShouldTriggerPartitionsRevokedWithValidGeneration,
//   testUnsubscribeShouldTriggerPartitionsLostWithNoGeneration, testGracefulClose,
//   testCloseTimeoutDueToNoResponseForCloseFetchRequest, testCloseTimeout,
//   testLeaveGroupTimeout, testCloseNoWait, testCloseInterrupt,
//   testShouldAttemptToRejoinGroupAfterSyncGroupFailed,
//   testPartitionsForNonExistingTopic, testPartitionsForAuthenticationFailure,
//   testBeginningOffsetsAuthenticationFailure, testEndOffsetsAuthenticationFailure,
//   testOffsetsForTimesAuthenticationFailure, testCommitSyncAuthenticationFailure,
//   testCommittedAuthenticationFailure, testRebalanceException,
//   testReturnRecordsDuringRebalance, testGetGroupMetadata,
//   testCurrentLagPreventsMultipleInFlightRequests,
//   testCurrentLagClearsFlagOnFatalPartitionError,
//   testCurrentLagClearsFlagOnRetriablePartitionError,
//   testEnforceRebalanceWithManualAssignment,
//   testEnforceRebalanceTriggersRebalanceOnNextPoll, testEnforceRebalanceReason,
//   testAssignorNameConflict, testSubscribeToRe2jPatternNotSupportedForClassicConsumer,
//   testPauseFlagPreservedForRetainedPartitionAcrossRebalance,
//   testUnsubscribeDoesNotCommitOffsetsEvenWithAutoCommitEnabled and
//   testClassicConsumerCloseRunsRevocationCallbackAndAttemptsLeaveGroupWhenInterrupted;
// - the classic consumer's KIP-848 recommendation log, which only the classic
//   consumer writes: testClassicProtocolLogsRecommendationToTryConsumerProtocol,
//   testDefaultProtocolLogsRecommendationToTryConsumerProtocol,
//   testNoGroupIdDoesNotLogGroupProtocolMessage and
//   testConsumerProtocolDoesNotLogRecommendation;
// - a MockClient answer a broker does not give, or a mocked collaborator,
//   which the binding cannot inject: testFetchProgressWithMissingPartitionPosition,
//   testFetchResponseWithUnexpectedPartitionIsIgnored,
//   testFetchStableOffsetThrowInCommitted, testFetchStableOffsetThrowInPoll,
//   testFetchStableOffsetThrowInPosition, testPollAuthenticationFailure,
//   testConstructorFailsOnNetworkClientConstructorFailure and testCurrentLag
//   (the core's translation is test_current_lag in
//   rust/src/consumer/kafka_consumer.rs);
// - metrics the client does not have: the KIP-1076 metric subscription, whose
//   FFI entry points are missing
//   (testSubscribingCustomMetricsDoesntAffectConsumerMetrics,
//   testSubscribingCustomMetricsWithSameNameDoesntAffectConsumerMetrics,
//   testUnsubscribingCustomMetricsWithSameNameDoesntAffectConsumerMetrics,
//   testUnsubscribingNonExistingMetricsDoesntCauseError); a metric reporter,
//   JMX or a telemetry reporter, which the core does not load
//   (testShouldOnlyCallMetricReporterMetricChangeOnceWithExistingConsumerMetric,
//   testShouldNotCallMetricReporterMetricRemovalWithExistingConsumerMetric,
//   testMetricsReporterAutoGeneratedClientId, testDisableJmxAndClientTelemetryReporter,
//   testExplicitlyOnlyEnableJmxReporter, testExplicitlyOnlyEnableClientTelemetryReporter,
//   testConstructorInvalidMetricReporters, testConsumerJmxPrefix);
//   testMonitorablePlugins (plugin metrics of a deserializer and an
//   interceptor, which the client does not register);
//   testMetricConfigRecordingLevelInfo (the private metricsRegistry());
//   testPollTimeMetrics, testPollIdleRatio, testMeasureCommitSyncDurationOnFailure,
//   testMeasureCommitSyncDuration, testMeasureCommittedDurationOnFailure and
//   testMeasureCommittedDuration (values a MockTime makes exact: the core's
//   clock cannot be injected);
// - not in the client: testClientInstanceId, testClientInstanceIdInvalidTimeout
//   and testClientInstanceIdNoTelemetryReporterRegistered (clientInstanceId
//   has no FFI entry point), testSubscriptionOnNullPattern (Java's Pattern
//   forms are left out: a pattern is only a SubscriptionPattern);
// - configuration the core does not check, or does not let a test read:
//   testInvalidSocketSendBufferSize and testInvalidSocketReceiveBufferSize
//   (the core does not validate the send.buffer.bytes and
//   receive.buffer.bytes ranges), testUnusedConfigs (only the core reports
//   the keys it does not use, in its own log), testInterceptorConstructorClose
//   and testInterceptorConstructorConfigurationWithExceptionShouldCloseRemainingInstances
//   (the client runs no interceptors).

const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

const { Temporal } = require('@js-temporal/polyfill');

const {
  ConcurrentModificationError,
  IllegalArgumentError,
  IllegalStateError,
  UnsupportedOperationError,
} = require('@confluentinc/kafka-javascript');
const { KafkaError, TopicPartition, TopicPartitionMap, TopicPartitionSet } = require('@confluentinc/kafka-javascript/common');
const { ConfigError } = require('@confluentinc/kafka-javascript/common/config');
const {
  BootstrapResolutionError,
  InvalidConfigurationError,
  InvalidGroupIdError,
  TimeoutError: KafkaTimeoutError,
  WakeupError,
} = require('@confluentinc/kafka-javascript/common/errors');
const {
  CloseOptions,
  ConsumerGroupMetadata,
  ConsumerRecords,
  KafkaConsumer,
  OffsetAndMetadata,
  SubscriptionPattern,
} = require('@confluentinc/kafka-javascript/consumer');

/** @typedef {import('@confluentinc/kafka-javascript/consumer').ConsumerConfigs} ConsumerConfigs */

const TOPIC = 'test';
const GROUP_ID = 'mock-group';
const TP0 = new TopicPartition({ topic: TOPIC, partition: 0 });
// Java's getConsumerRebalanceListener: a listener that does nothing.
const LISTENER = { onPartitionsRevoked() {}, onPartitionsAssigned() {} };
const TP1 = new TopicPartition({ topic: TOPIC, partition: 1 });

// The message of Java's AsyncKafkaConsumer.throwIfGroupIdNotDefined().
const NO_GROUP_ID = 'To use the group management or offset commit APIs, you must provide a valid group.id in the consumer configuration.';
const CLOSED = 'This consumer has already been closed.';
const CONCURRENT = 'KafkaConsumer is not safe for multi-threaded access.';
const CLASSIC = 'Classic group protocol is not yet supported in this client; set group.protocol=consumer (KIP-848).';
const NO_DESERIALIZERS = 'KafkaConsumer takes no deserializer yet: keys and values are Buffers';
const NOT_SUBSCRIBED = 'Consumer is not subscribed to any topics or assigned any partitions';
/** @param {string} fn */
const CLOSED_GETTER = (fn) => `Rust panic caught at the FFI boundary in kafka_consumer_Consumer_${fn}: ${CLOSED}`;

/**
 * A consumer configuration against an unreachable broker.
 *
 * @param {string | null} [groupId] the group, or null for none
 * @param {ConsumerConfigs} [extra] more keys
 * @returns {ConsumerConfigs} the configuration
 */
function configs(groupId = GROUP_ID, extra = {}) {
  return {
    'bootstrap.servers': '127.0.0.1:1',
    'group.protocol': 'consumer',
    ...(groupId === null ? {} : { 'group.id': groupId }),
    ...extra,
  };
}

/** @type {KafkaConsumer[]} */
const opened = [];

/**
 * A consumer closed after the test.
 *
 * @param {string | null} [groupId] the group, or null for none
 * @param {ConsumerConfigs} [extra] more keys
 * @returns {KafkaConsumer} the consumer
 */
function newConsumer(groupId = GROUP_ID, extra = {}) {
  const consumer = new KafkaConsumer({ configs: configs(groupId, extra) });
  opened.push(consumer);
  return consumer;
}

afterEach(async () => {
  for (const consumer of opened.splice(0)) {
    await consumer.close({ option: CloseOptions.timeout({ timeout: 0 }) });
  }
});

/**
 * The value of the consumer's metric `name` in `group`, or undefined.
 *
 * @param {KafkaConsumer} consumer
 * @param {string} name
 * @param {string} group
 * @returns {unknown} the metric's value
 */
function metricValue(consumer, name, group) {
  for (const [key, metric] of consumer.metrics()) if (key.name() === name && key.group() === group) return metric.metricValue();
  return undefined;
}

/**
 * The consumer's metric names, `<group>:<name>`.
 *
 * @param {KafkaConsumer} consumer
 * @returns {string[]} the names, sorted
 */
function metricNames(consumer) {
  return [...consumer.metrics().keys()].map((n) => `${n.group()}:${n.name()}`).sort();
}

/**
 * The partitions of a set, as `topic-partition` strings.
 *
 * @param {TopicPartitionSet} set
 * @returns {string[]} the strings
 */
const names = (set) => [...set].map(String).sort();

/**
 * The check of Java's construction failure: a bare KafkaError "Failed to
 * construct kafka consumer" whose cause is a `cause` with `message`.
 *
 * @param {Function} cause the cause's class
 * @param {string} message the cause's message
 * @returns {(e: unknown) => boolean} the check
 */
function constructionFailure(cause, message) {
  return (e) => {
    assert.ok(e instanceof KafkaError);
    assert.equal(e.constructor, KafkaError);
    assert.equal(e.message, 'Failed to construct kafka consumer');
    assert.ok(e.cause instanceof cause);
    assert.equal(/** @type {Error} */ (e.cause).message, message);
    return true;
  };
}

// ---- KafkaConsumerTest -------------------------------------------------------

test('testConstructorClose', () => {
  assert.throws(
    () =>
      new KafkaConsumer({
        configs: { 'bootstrap.servers': 'invalid-23-8409-adsfsdj', 'group.protocol': 'consumer', 'client.id': 'testConstructorClose' },
      }),
    constructionFailure(ConfigError, 'Invalid url in bootstrap.servers: invalid-23-8409-adsfsdj'),
  );
});

test('testOsDefaultSocketBufferSizes', () => {
  // Selectable.USE_DEFAULT_BUFFER_SIZE is -1.
  newConsumer(null, { 'send.buffer.bytes': -1, 'receive.buffer.bytes': -1 });
});

test('shouldIgnoreGroupInstanceIdForEmptyGroupId', () => {
  newConsumer(null, { 'group.instance.id': 'instance_id' });
});

for (const [name, groupId] of [
  ['testEmptyGroupId', ''],
  ['testGroupIdWithWhitespace', ' '],
]) {
  test(name, () => {
    assert.throws(
      () => new KafkaConsumer({ configs: configs(groupId) }),
      constructionFailure(InvalidGroupIdError, 'The configured group.id should not be an empty string or whitespace.'),
    );
  });
}

test('testConsumerConstructorFailsWithConfigExceptionOnUnresolvableBootstrapWhenTimeoutZero', () => {
  // The default bootstrap.resolve.timeout.ms of 0 resolves the bootstrap
  // servers in the constructor.
  assert.throws(
    () => new KafkaConsumer({ configs: configs('test-group', { 'bootstrap.servers': 'unresolvable.invalid:9092' }) }),
    constructionFailure(ConfigError, 'No resolvable bootstrap urls given in bootstrap.servers'),
  );
});

test('testOperationsBySubscribingConsumerWithDefaultGroupId', async () => {
  // enable.auto.commit=true needs a group.id: the core's error, unwrapped.
  assert.throws(() => new KafkaConsumer({ configs: configs(null, { 'enable.auto.commit': true }) }), {
    constructor: InvalidConfigurationError,
    message: 'enable.auto.commit cannot be set to true when default group id (null) is used.',
  });
  await assert.rejects(newConsumer(null).subscribe({ topics: [TOPIC] }), { constructor: InvalidGroupIdError, message: NO_GROUP_ID });
  // Java's subscribe() checks the group before the topics.
  // @ts-expect-error a null topic the types forbid: the run time sends it as a blank one
  await assert.rejects(newConsumer(null).subscribe({ topics: [null] }), { constructor: InvalidGroupIdError, message: NO_GROUP_ID });
  await assert.rejects(newConsumer(null).committed({ partitions: [TP0] }), { constructor: InvalidGroupIdError, message: NO_GROUP_ID });
  await assert.rejects(newConsumer(null).commitAsync(), { constructor: InvalidGroupIdError, message: NO_GROUP_ID });
  await assert.rejects(newConsumer(null).commitSync(), { constructor: InvalidGroupIdError, message: NO_GROUP_ID });
});

test('testOperationsByAssigningConsumerWithDefaultGroupId', async () => {
  const consumer = newConsumer(null);
  await consumer.assign({ partitions: [TP0] });
  assert.deepEqual(names(consumer.assignment()), ['test-0']);
  await assert.rejects(consumer.committed({ partitions: [TP0] }), { constructor: InvalidGroupIdError, message: NO_GROUP_ID });
  await assert.rejects(consumer.commitAsync(), { constructor: InvalidGroupIdError, message: NO_GROUP_ID });
  await assert.rejects(consumer.commitSync(), { constructor: InvalidGroupIdError, message: NO_GROUP_ID });
});

test('testSubscription', async () => {
  const consumer = newConsumer();
  await consumer.subscribe({ topics: [TOPIC] });
  assert.deepEqual(consumer.subscription(), new Set([TOPIC]));
  assert.equal(consumer.assignment().size(), 0);

  await consumer.subscribe({ topics: [] });
  assert.equal(consumer.subscription().size, 0);
  assert.equal(consumer.assignment().size(), 0);

  await consumer.assign({ partitions: [TP0] });
  assert.equal(consumer.subscription().size, 0);
  assert.deepEqual(names(consumer.assignment()), ['test-0']);

  await consumer.unsubscribe();
  assert.equal(consumer.subscription().size, 0);
  assert.equal(consumer.assignment().size(), 0);
});

test('testSubscriptionOnNullTopicCollection', async () => {
  // @ts-expect-error a null the types forbid: the FFI refuses it with Java's error
  await assert.rejects(newConsumer().subscribe({ topics: null }), {
    constructor: IllegalArgumentError,
    message: 'Topic collection to subscribe to cannot be null',
  });
});

test('testSubscriptionOnNullTopic', async () => {
  // @ts-expect-error a null topic the types forbid: the core refuses it with Java's error
  await assert.rejects(newConsumer().subscribe({ topics: [null] }), {
    constructor: IllegalArgumentError,
    message: 'Topic collection to subscribe to cannot contain null or empty topic',
  });
});

test('testSubscriptionOnEmptyTopic', async () => {
  await assert.rejects(newConsumer().subscribe({ topics: ['  '] }), {
    constructor: IllegalArgumentError,
    message: 'Topic collection to subscribe to cannot contain null or empty topic',
  });
});

test('testSubscriptionOnEmptyPattern', async () => {
  // Java's subscribe(Pattern.compile("")) is left out; its SubscriptionPattern form.
  await assert.rejects(newConsumer().subscribe({ pattern: new SubscriptionPattern({ pattern: '' }) }), {
    constructor: IllegalArgumentError,
    message: 'Topic pattern to subscribe to cannot be empty',
  });
});

test('testAssignOnNullTopicPartition', async () => {
  // @ts-expect-error a null the types forbid: the FFI refuses it with Java's error
  await assert.rejects(newConsumer(null).assign({ partitions: null }), {
    constructor: IllegalArgumentError,
    message: 'Topic partitions collection to assign to cannot be null',
  });
});

test('testAssignOnEmptyTopicPartition', async () => {
  const consumer = newConsumer(null);
  await consumer.assign({ partitions: [] });
  assert.equal(consumer.subscription().size, 0);
  assert.equal(consumer.assignment().size(), 0);
});

test('testAssignOnNullTopicInPartition', async () => {
  const consumer = newConsumer(null);
  const message = 'Topic partitions to assign to cannot have null or empty topic';
  // TopicPartition stores a null topic, as Java's does.
  await assert.rejects(consumer.assign({ partitions: [new TopicPartition({ topic: null, partition: 0 })] }), {
    constructor: IllegalArgumentError,
    message,
  });
  // Java: isBlank(tp != null ? tp.topic() : null).
  // @ts-expect-error a null element the types forbid
  await assert.rejects(consumer.assign({ partitions: [null] }), { constructor: IllegalArgumentError, message });
});

test('testAssignOnEmptyTopicInPartition', async () => {
  await assert.rejects(newConsumer(null).assign({ partitions: [new TopicPartition({ topic: '  ', partition: 0 })] }), {
    constructor: IllegalArgumentError,
    message: 'Topic partitions to assign to cannot have null or empty topic',
  });
});

test('testPause', async () => {
  const consumer = newConsumer();
  await consumer.assign({ partitions: [TP0] });
  assert.deepEqual(names(consumer.assignment()), ['test-0']);
  assert.equal(consumer.paused().size(), 0);

  await consumer.pause({ partitions: [TP0] });
  assert.deepEqual(names(consumer.paused()), ['test-0']);

  await consumer.resume({ partitions: [TP0] });
  assert.equal(consumer.paused().size(), 0);

  await consumer.unsubscribe();
  assert.equal(consumer.paused().size(), 0);
});

test('testPauseFlagPreservedForRetainedPartitionOnManualAssignmentChange', async () => {
  const consumer = newConsumer();
  // Manually assign two partitions and pause one of them.
  await consumer.assign({ partitions: [TP0, TP1] });
  await consumer.pause({ partitions: [TP0] });
  assert.deepEqual(names(consumer.paused()), ['test-0']);

  // Change the assignment while keeping tp0 assigned.
  await consumer.assign({ partitions: [TP0] });
  assert.deepEqual(names(consumer.assignment()), ['test-0']);
  // The pause state of the partition that remains assigned must be preserved.
  assert.deepEqual(names(consumer.paused()), ['test-0']);

  // A partition (re)added to the assignment starts unpaused.
  await consumer.assign({ partitions: [TP0, TP1] });
  assert.deepEqual(names(consumer.paused()), ['test-0']);
});

test('verifyPollTimesOutDuringMetadataUpdate', async () => {
  // Java asserts no FETCH is sent while the metadata is not updated; with an
  // unreachable broker there is none, and poll(0) returns an empty record set.
  const consumer = newConsumer();
  await consumer.subscribe({ topics: [TOPIC], callback: LISTENER });
  const records = await consumer.poll({ timeout: 0 });
  assert.ok(records instanceof ConsumerRecords);
  assert.equal(records.isEmpty(), true);
});

for (const [name, setup] of /** @type {const} */ ([
  ['testPollWithNoSubscription', 'none'],
  ['testPollWithEmptySubscription', 'subscribe'],
  ['testPollWithEmptyUserAssignment', 'assign'],
])) {
  test(name, async () => {
    const consumer = newConsumer(setup === 'none' ? null : GROUP_ID);
    if (setup === 'subscribe') await consumer.subscribe({ topics: [] });
    if (setup === 'assign') await consumer.assign({ partitions: [] });
    await assert.rejects(consumer.poll({ timeout: 0 }), { constructor: IllegalStateError, message: NOT_SUBSCRIBED });
  });
}

test('testPreventMultiThread', async () => {
  // Java's second thread is a second call while the first poll waits.
  const consumer = newConsumer();
  await consumer.subscribe({ topics: [TOPIC] });
  const polling = consumer.poll({ timeout: 5000 });
  await assert.rejects(consumer.poll({ timeout: 0 }), { constructor: ConcurrentModificationError, message: CONCURRENT });
  consumer.wakeup();
  await assert.rejects(polling, WakeupError);
});

test('testConsumerBootstrapResolutionExceptionPropagatedToPoll', async () => {
  const consumer = new KafkaConsumer({
    configs: {
      'bootstrap.servers': 'unresolvable.invalid:9092',
      // A short bootstrap timeout, so the test doesn't take too long.
      'bootstrap.resolve.timeout.ms': 3000,
      'group.protocol': 'consumer',
      'group.id': 'test-group',
    },
  });
  opened.push(consumer);
  await consumer.subscribe({ topics: ['test-topic'] });
  const message = 'Failed to resolve bootstrap servers after 3000ms. Please check your bootstrap.servers configuration and DNS settings.';
  const deadline = Date.now() + 15000;
  /** @type {unknown} */
  let failure = null;
  while (failure === null && Date.now() < deadline) {
    await consumer.poll({ timeout: 100 }).catch((/** @type {unknown} */ e) => {
      failure = e;
    });
  }
  assert.ok(failure instanceof BootstrapResolutionError, String(failure));
  assert.equal(failure.message, message);
  // Any further call fails the same way: the error is not cleared.
  await assert.rejects(consumer.poll({ timeout: 100 }), { constructor: BootstrapResolutionError, message });
}, 30000);

test('testAssignedPartitionsMetrics', async () => {
  // Its assign half: the group assignment needs a broker.
  const consumer = newConsumer();
  const deadline = Date.now() + 10000;
  // The metric is added by the background task: wait for it.
  while (metricValue(consumer, 'assigned-partitions', 'consumer-coordinator-metrics') === undefined) {
    assert.ok(Date.now() < deadline, 'Consumer should register the assigned-partitions metric');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.equal(metricValue(consumer, 'assigned-partitions', 'consumer-coordinator-metrics'), 0);
  await consumer.assign({ partitions: [TP0] });
  assert.equal(metricValue(consumer, 'assigned-partitions', 'consumer-coordinator-metrics'), 1);
  await consumer.assign({ partitions: [TP0, TP1] });
  assert.equal(metricValue(consumer, 'assigned-partitions', 'consumer-coordinator-metrics'), 2);
});

test('testMetricsRemovedOnClose', async () => {
  const consumer = new KafkaConsumer({ configs: { 'group.protocol': 'consumer', 'bootstrap.servers': 'localhost:9999' } });
  const before = metricNames(consumer);
  // There's a meta-metric named "count" that is automatically added to the metrics map.
  assert.ok(before.includes('kafka-metrics-count:count'), 'The "count" meta-metric was unexpectedly missing from the Consumer metrics');
  assert.ok(before.length > 1, 'The consumer should have created metrics, but they are unexpectedly empty');
  assert.ok(before.some((n) => n.startsWith('consumer-metrics:')));
  await consumer.close({ option: CloseOptions.timeout({ timeout: 0 }) });
  const after = metricNames(consumer);
  assert.ok(after.includes('kafka-metrics-count:count'), 'The "count" meta-metric was unexpectedly missing from the Consumer metrics');
  assert.deepEqual(
    after.filter((n) => n !== 'kafka-metrics-count:count'),
    [],
    'The consumer should have removed its metrics on close(), but there are metrics remaining',
  );
});

test('testClosingConsumerUnregistersConsumerMetrics', async () => {
  const consumer = new KafkaConsumer({ configs: configs() });
  await consumer.subscribe({ topics: [TOPIC] });
  const present = (/** @type {string} */ name) => metricValue(consumer, name, 'consumer-metrics') !== undefined;
  assert.ok(present('last-poll-seconds-ago'));
  assert.ok(present('time-between-poll-avg'));
  assert.ok(present('time-between-poll-max'));
  await consumer.close();
  assert.ok(!present('last-poll-seconds-ago'));
  assert.ok(!present('time-between-poll-avg'));
  assert.ok(!present('time-between-poll-max'));
});

test('testPollIdleRatioZero', () => {
  // Test default value.
  assert.ok(Number.isNaN(metricValue(newConsumer(), 'poll-idle-ratio-avg', 'consumer-metrics')));
});

test('testCloseShouldBeIdempotent', async () => {
  const consumer = new KafkaConsumer({ configs: configs() });
  await consumer.close({ option: CloseOptions.timeout({ timeout: 0 }) });
  await consumer.close({ option: CloseOptions.timeout({ timeout: 0 }) });
  assert.throws(() => consumer.assignment(), { constructor: IllegalStateError, message: CLOSED_GETTER('assignment') });
});

test('testSeekNegative', async () => {
  const consumer = newConsumer(null);
  const partition = new TopicPartition({ topic: 'nonExistTopic', partition: 0 });
  await consumer.assign({ partitions: [partition] });
  await assert.rejects(consumer.seek({ partition, offset: -1 }), {
    constructor: IllegalArgumentError,
    message: 'seek offset must not be a negative number',
  });
});

test('testInvalidGroupMetadata', async () => {
  // Java's second thread is a call while a poll waits.
  const consumer = new KafkaConsumer({ configs: configs() });
  await consumer.subscribe({ topics: [TOPIC] });
  const polling = consumer.poll({ timeout: 5000 });
  assert.throws(() => consumer.groupMetadata(), { constructor: ConcurrentModificationError, message: CONCURRENT });
  consumer.wakeup();
  await assert.rejects(polling, WakeupError);
  await consumer.close({ option: CloseOptions.timeout({ timeout: 0 }) });
  // The closed getter panics in the core, whose message the FFI prefixes.
  assert.throws(() => consumer.groupMetadata(), { constructor: IllegalStateError, message: CLOSED_GETTER('group_metadata') });
});

test('testCommittedThrowsTimeoutExceptionForNoResponse', async () => {
  // Java's coordinator never answers; an unreachable broker never does either.
  const consumer = newConsumer();
  await consumer.assign({ partitions: [TP0] });
  await assert.rejects(consumer.committed({ partitions: [TP0], timeout: 1000 }), {
    constructor: KafkaTimeoutError,
    message:
      'Timeout of 1000ms expired before the last committed offset for partitions [test-0] could be determined. ' +
      'Try tuning default.api.timeout.ms larger to relax the threshold.',
  });
});

// Java's MockClient never answers the ListOffsets; an unreachable broker never
// does either, so default.api.timeout.ms (Java's 60000) runs out sooner.
for (const [name, call] of /** @type {const} */ ([
  [
    'testOffsetsForTimesTimeout',
    (/** @type {KafkaConsumer} */ c) => c.offsetsForTimes({ timestampsToSearch: new TopicPartitionMap([[TP0, 0n]]) }),
  ],
  ['testBeginningOffsetsTimeout', (/** @type {KafkaConsumer} */ c) => c.beginningOffsets({ partitions: [TP0] })],
  ['testEndOffsetsTimeout', (/** @type {KafkaConsumer} */ c) => c.endOffsets({ partitions: [TP0] })],
])) {
  test(name, async () => {
    const consumer = newConsumer(GROUP_ID, { 'default.api.timeout.ms': 500 });
    await assert.rejects(call(consumer), { constructor: KafkaTimeoutError, message: 'Failed to get offsets by times in 500ms' });
  });
}

// ---- the binding's own contracts --------------------------------------------

test('the classic protocol, the default, gets the core UnsupportedOperationError', () => {
  assert.throws(() => new KafkaConsumer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'group.id': GROUP_ID } }), {
    constructor: UnsupportedOperationError,
    message: CLASSIC,
  });
  assert.throws(() => new KafkaConsumer({ configs: configs(GROUP_ID, { 'group.protocol': 'classic' }) }), {
    constructor: UnsupportedOperationError,
    message: CLASSIC,
  });
});

test('deserializers are refused until the client has them, as arguments and as config keys', () => {
  const error = { constructor: UnsupportedOperationError, message: NO_DESERIALIZERS };
  // @ts-expect-error the deserializer forms are not in this release's types
  assert.throws(() => new KafkaConsumer({ configs: configs(), keyDeserializer: null, valueDeserializer: null }), error);
  for (const key of ['key.deserializer', 'value.deserializer']) {
    assert.throws(() => new KafkaConsumer({ configs: configs(GROUP_ID, { [key]: 'org.example.Deserializer' }) }), error);
  }
  // A null or undefined key is no deserializer.
  newConsumer(GROUP_ID, { 'key.deserializer': null, 'value.deserializer': undefined });
});

test('the constructor takes {configs} or the configs bare, and nothing else', () => {
  const forms = 'KafkaConsumer() takes one of (configs), (configs, keyDeserializer, valueDeserializer)';
  // The bare form, checked once.
  opened.push(new KafkaConsumer(configs()));
  // @ts-expect-error no argument
  assert.throws(() => new KafkaConsumer(), { constructor: IllegalArgumentError, message: `${forms}; got ()` });
  // @ts-expect-error a second argument
  assert.throws(() => new KafkaConsumer(configs(), 1), {
    constructor: IllegalArgumentError,
    message: `${forms}; got 2 arguments, not one object of named parameters`,
  });
  // @ts-expect-error a key of a form left out
  assert.throws(() => new KafkaConsumer({ configs: configs(), keyDeserializer: null }), {
    constructor: IllegalArgumentError,
    message: `${forms}; got (configs, keyDeserializer)`,
  });
  // An undefined value, which TypeScript lets through for a required key.
  assert.throws(() => new KafkaConsumer({ configs: undefined }), {
    constructor: TypeError,
    message: "configs must not be undefined. Leave the key out, or pass null for Java's null",
  });
  // @ts-expect-error a Map is no configuration
  assert.throws(() => new KafkaConsumer(new Map()), { constructor: TypeError, message: 'configs must be a plain object, not a Map' });
});

test('subscribe: its forms, bare and by name, and their errors', async () => {
  const consumer = newConsumer();
  const forms = 'subscribe() takes one of (topics), (topics, callback), (pattern, callback), (pattern)';
  await consumer.subscribe([TOPIC]);
  assert.deepEqual(consumer.subscription(), new Set([TOPIC]));
  await consumer.unsubscribe();
  await consumer.subscribe(new SubscriptionPattern({ pattern: 'te.*' }));
  assert.equal(consumer.subscription().size, 0);
  await consumer.unsubscribe();
  // @ts-expect-error no argument
  await assert.rejects(consumer.subscribe(), { constructor: IllegalArgumentError, message: `${forms}; got ()` });
  // A bare string, which TypeScript takes as an Iterable<string>.
  await assert.rejects(consumer.subscribe('t'), { constructor: IllegalArgumentError, message: `${forms}; got a string` });
  // A string under topics, which TypeScript takes as an Iterable<string>.
  await assert.rejects(consumer.subscribe({ topics: 't' }), {
    constructor: TypeError,
    message: 'topics must be an iterable of strings, not string',
  });
  // @ts-expect-error a bare null: Java's subscribe(null) is ambiguous
  await assert.rejects(consumer.subscribe(null), { constructor: IllegalArgumentError, message: `${forms}; got null` });
  // @ts-expect-error both forms' keys
  await assert.rejects(consumer.subscribe({ topics: [TOPIC], pattern: new SubscriptionPattern({ pattern: 'x' }) }), {
    constructor: IllegalArgumentError,
    message: `${forms}; got (topics, pattern)`,
  });
  // A callback on its own is no form.
  // @ts-expect-error a callback without topics or a pattern
  await assert.rejects(consumer.subscribe({ callback: LISTENER }), {
    constructor: IllegalArgumentError,
    message: `${forms}; got (callback)`,
  });
  // @ts-expect-error a second argument
  await assert.rejects(consumer.subscribe([TOPIC], 1), {
    constructor: IllegalArgumentError,
    message: `${forms}; got 2 arguments, not one object of named parameters`,
  });
  // @ts-expect-error undefined topics
  await assert.rejects(consumer.subscribe({ topics: undefined }), {
    constructor: TypeError,
    message: "topics must not be undefined. Leave the key out, or pass null for Java's null",
  });
  // @ts-expect-error a topic that is no string
  await assert.rejects(consumer.subscribe({ topics: [1] }), {
    constructor: TypeError,
    message: 'topics must hold only strings or null, not a number',
  });
  const regExp = "pattern must be a SubscriptionPattern, not a RegExp: new SubscriptionPattern({ pattern: '...' })";
  // @ts-expect-error a RegExp is no pattern
  await assert.rejects(consumer.subscribe(/te.*/), { constructor: TypeError, message: regExp });
  // @ts-expect-error a RegExp is no pattern
  await assert.rejects(consumer.subscribe({ pattern: /te.*/ }), { constructor: TypeError, message: regExp });
  // @ts-expect-error a string is no pattern
  await assert.rejects(consumer.subscribe({ pattern: 'te.*' }), {
    constructor: TypeError,
    message: 'pattern must be a SubscriptionPattern',
  });
  // @ts-expect-error a null the types forbid: the FFI refuses it with Java's error
  await assert.rejects(consumer.subscribe({ pattern: null }), {
    constructor: IllegalArgumentError,
    message: 'Topic pattern to subscribe to cannot be null',
  });
  // A subscription to topics, then to a pattern.
  await consumer.subscribe({ topics: [TOPIC] });
  await assert.rejects(consumer.subscribe({ pattern: new SubscriptionPattern({ pattern: 'x' }) }), {
    constructor: IllegalStateError,
    message: 'Subscription to topics, partitions and pattern are mutually exclusive',
  });
});

test('subscribe with a listener: its two forms, a null listener with Java message, and the listener checks', async () => {
  const consumer = newConsumer();
  const pattern = new SubscriptionPattern({ pattern: 'te.*' });
  await consumer.subscribe({ topics: [TOPIC], callback: LISTENER });
  assert.deepEqual(consumer.subscription(), new Set([TOPIC]));
  await consumer.unsubscribe();
  await consumer.subscribe({ pattern, callback: LISTENER });
  await consumer.unsubscribe();
  // A listener without onPartitionsLost: Java's default runs.
  await consumer.subscribe({ topics: [TOPIC], callback: { onPartitionsRevoked() {}, onPartitionsAssigned() {} } });
  await consumer.unsubscribe();
  // Java's "RebalanceListener cannot be null", before its check of the topics
  // or the pattern (testRe2JPatternSubscription's assertion).
  for (const call of [
    // @ts-expect-error a null the types forbid
    () => consumer.subscribe({ topics: [TOPIC], callback: null }),
    // @ts-expect-error a null the types forbid
    () => consumer.subscribe({ topics: null, callback: null }),
    // @ts-expect-error a null the types forbid
    () => consumer.subscribe({ pattern, callback: null }),
    // @ts-expect-error a null the types forbid
    () => consumer.subscribe({ pattern: null, callback: null }),
  ]) {
    await assert.rejects(call(), { constructor: IllegalArgumentError, message: 'RebalanceListener cannot be null' });
  }
  // A missing required method goes to the FFI as NULL, which refuses it: a
  // TypeError with the FFI's text.
  // @ts-expect-error no onPartitionsRevoked
  await assert.rejects(consumer.subscribe({ topics: [TOPIC], callback: { onPartitionsAssigned() {} } }), {
    constructor: TypeError,
    message: 'on_partitions_revoked must not be null',
  });
  // @ts-expect-error no onPartitionsAssigned
  await assert.rejects(consumer.subscribe({ pattern, callback: { onPartitionsRevoked() {} } }), {
    constructor: TypeError,
    message: 'on_partitions_assigned must not be null',
  });
  // A listener that is no object.
  for (const callback of [() => {}, 'x', 1]) {
    // @ts-expect-error no listener
    await assert.rejects(consumer.subscribe({ topics: [TOPIC], callback }), {
      constructor: TypeError,
      message: 'callback must be a ConsumerRebalanceListener',
    });
  }
  // An undefined listener, which TypeScript lets through as a key left out.
  await assert.rejects(consumer.subscribe({ topics: [TOPIC], callback: undefined }), {
    constructor: TypeError,
    message: "callback must not be undefined. Leave the key out, or pass null for Java's null",
  });
  // @ts-expect-error a second argument, Java's subscribe(topics, listener) written positionally
  await assert.rejects(consumer.subscribe([TOPIC], LISTENER), {
    constructor: IllegalArgumentError,
    message:
      'subscribe() takes one of (topics), (topics, callback), (pattern, callback), (pattern); got 2 arguments, not one object of named parameters',
  });
  // Nothing was subscribed.
  assert.equal(consumer.subscription().size, 0);
});

test('assign, pause and resume: their forms and errors, the FFI refusing a null', async () => {
  const consumer = newConsumer();
  await consumer.assign([TP0]);
  await consumer.pause([TP0]);
  assert.deepEqual(names(consumer.paused()), ['test-0']);
  await consumer.resume(new Set([TP0]));
  assert.equal(consumer.paused().size(), 0);
  for (const method of /** @type {const} */ (['assign', 'pause', 'resume'])) {
    // @ts-expect-error no argument
    await assert.rejects(consumer[method](), {
      constructor: IllegalArgumentError,
      message: `${method}() takes one of (partitions); got ()`,
    });
    // @ts-expect-error a string
    await assert.rejects(consumer[method]('x'), {
      constructor: IllegalArgumentError,
      message: `${method}() takes one of (partitions); got a string`,
    });
    // @ts-expect-error a plain object is no TopicPartition
    await assert.rejects(consumer[method]({ partitions: [{ topic: 't', partition: 0 }] }), {
      constructor: TypeError,
      message: 'partitions must hold only TopicPartition or null, not an object',
    });
  }
  // @ts-expect-error a null the types forbid: the FFI refuses it
  await assert.rejects(consumer.pause({ partitions: null }), {
    constructor: TypeError,
    message: 'The partitions to pause must be nonnull',
  });
  // @ts-expect-error a null the types forbid: the FFI refuses it
  await assert.rejects(consumer.resume({ partitions: null }), {
    constructor: TypeError,
    message: 'The partitions to resume must be nonnull',
  });
  // @ts-expect-error a null element the types forbid: the FFI refuses it
  await assert.rejects(consumer.pause({ partitions: [null] }), { constructor: TypeError, message: 'partitions must not contain null' });
  await assert.rejects(consumer.pause({ partitions: [new TopicPartition({ topic: 'x', partition: 0 })] }), {
    constructor: IllegalStateError,
    message: 'No current assignment for partition x-0',
  });
  // A bare null is Java's assign(null).
  // @ts-expect-error a null the types forbid
  await assert.rejects(consumer.assign(null), {
    constructor: IllegalArgumentError,
    message: 'Topic partitions collection to assign to cannot be null',
  });
});

test('the null checks come before the closed, busy and group checks: the FFI refuses the NULL at its boundary', async () => {
  const TOPICS_NULL = { constructor: IllegalArgumentError, message: 'Topic collection to subscribe to cannot be null' };
  const PATTERN_NULL = { constructor: IllegalArgumentError, message: 'Topic pattern to subscribe to cannot be null' };
  const ASSIGN_NULL = { constructor: IllegalArgumentError, message: 'Topic partitions collection to assign to cannot be null' };
  // Without a group.id: Java throws InvalidGroupIdException first.
  const groupless = newConsumer(null);
  // @ts-expect-error a null the types forbid
  await assert.rejects(groupless.subscribe({ topics: null }), TOPICS_NULL);
  // @ts-expect-error a null the types forbid
  await assert.rejects(groupless.subscribe({ pattern: null }), PATTERN_NULL);
  // While a poll is pending: Java throws ConcurrentModificationException first.
  const busy = newConsumer();
  await busy.subscribe({ topics: [TOPIC] });
  const polling = busy.poll({ timeout: 10000 });
  // @ts-expect-error a null the types forbid
  await assert.rejects(busy.subscribe({ topics: null }), TOPICS_NULL);
  // @ts-expect-error a null the types forbid
  await assert.rejects(busy.assign({ partitions: null }), ASSIGN_NULL);
  busy.wakeup();
  await assert.rejects(polling, WakeupError);
  // Closed: Java throws IllegalStateException("This consumer has already been closed.") first.
  const closed = new KafkaConsumer({ configs: configs() });
  await closed.close({ option: CloseOptions.timeout({ timeout: 0 }) });
  // @ts-expect-error a null the types forbid
  await assert.rejects(closed.subscribe({ topics: null }), TOPICS_NULL);
  // @ts-expect-error a null the types forbid
  await assert.rejects(closed.subscribe({ pattern: null }), PATTERN_NULL);
  // @ts-expect-error a null the types forbid
  await assert.rejects(closed.assign({ partitions: null }), ASSIGN_NULL);
  // @ts-expect-error a null the types forbid
  await assert.rejects(closed.pause({ partitions: null }), { constructor: TypeError, message: 'The partitions to pause must be nonnull' });
  // @ts-expect-error a null the types forbid
  await assert.rejects(closed.resume({ partitions: null }), {
    constructor: TypeError,
    message: 'The partitions to resume must be nonnull',
  });
  // A null topic in pause or resume: Java throws "No current assignment for partition null-0".
  const assigned = newConsumer();
  await assigned.assign({ partitions: [TP0] });
  const nullTopic = new TopicPartition({ topic: null, partition: 0 });
  await assert.rejects(assigned.pause({ partitions: [nullTopic] }), { constructor: TypeError, message: 'topic must not be null' });
  await assert.rejects(assigned.resume({ partitions: [nullTopic] }), { constructor: TypeError, message: 'topic must not be null' });
});

test('the null checks of the offset calls come before the closed and busy checks, and the group.id check before all', async () => {
  const nullTopic = new TopicPartition({ topic: null, partition: 0 });
  // Closed: Java throws IllegalStateException("This consumer has already been closed.") first.
  const closed = new KafkaConsumer({ configs: configs() });
  await closed.close({ option: CloseOptions.timeout({ timeout: 0 }) });
  // @ts-expect-error a null the types forbid
  await assert.rejects(closed.position(null), { constructor: TypeError, message: 'partition must not be null' });
  await assert.rejects(closed.position(nullTopic), { constructor: TypeError, message: 'topic must not be null' });
  // @ts-expect-error a null the types forbid
  await assert.rejects(closed.committed(null), { constructor: TypeError, message: 'partitions must not be null' });
  // @ts-expect-error a null the types forbid
  await assert.rejects(closed.currentLag(null), { constructor: TypeError, message: 'topic_partition must not be null' });
  // @ts-expect-error a null the types forbid
  await assert.rejects(closed.seek({ partition: null, offset: 0 }), { constructor: TypeError, message: 'partition must not be null' });
  // @ts-expect-error a null the types forbid
  await assert.rejects(closed.offsetsForTimes(null), { constructor: TypeError, message: 'Timestamps to search cannot be null' });
  // @ts-expect-error a null the types forbid
  await assert.rejects(closed.offsetsForTimes(new TopicPartitionMap([[TP0, null]])), {
    constructor: TypeError,
    message: 'timestampsToSearch must not contain a null timestamp',
  });
  // @ts-expect-error a null the types forbid
  await assert.rejects(closed.beginningOffsets(null), { constructor: TypeError, message: 'Partitions cannot be null' });
  // @ts-expect-error a null the types forbid
  await assert.rejects(closed.endOffsets(null), { constructor: TypeError, message: 'Partitions cannot be null' });
  // @ts-expect-error a null the types forbid
  await assert.rejects(closed.partitionsFor(null), { constructor: TypeError, message: 'topic must not be null' });
  // Java checks these first too: no deviation.
  // @ts-expect-error a null the types forbid
  await assert.rejects(closed.commitSync({ offsets: null }), { constructor: TypeError, message: 'offsets must not be null' });
  // @ts-expect-error a null the types forbid
  await assert.rejects(closed.seekToBeginning(null), {
    constructor: IllegalArgumentError,
    message: 'Partitions collection cannot be null',
  });
  // Without group.id, the binding's check comes before the closed check.
  const closedGroupless = new KafkaConsumer({ configs: configs(null) });
  await closedGroupless.close({ option: CloseOptions.timeout({ timeout: 0 }) });
  assert.throws(() => closedGroupless.groupMetadata(), { constructor: InvalidGroupIdError, message: NO_GROUP_ID });
  // @ts-expect-error a null the types forbid
  await assert.rejects(closedGroupless.committed(null), { constructor: InvalidGroupIdError, message: NO_GROUP_ID });
  // A negative timeout: the FFI checks it before the core's closed and group
  // checks, which Java runs first (on an open consumer, Java's commitSync and
  // position say "Invalid negative timeout -1", the others wait).
  const NEGATIVE = { constructor: IllegalArgumentError, message: 'Timeout must not be negative' };
  const offsets = new TopicPartitionMap([[TP0, new OffsetAndMetadata(1)]]);
  for (const c of [closed, closedGroupless]) {
    for (const call of [
      () => c.commitSync({ timeout: -1 }),
      () => c.commitSync({ offsets, timeout: -1 }),
      () => c.position({ partition: TP0, timeout: -1 }),
      () => c.committed({ partitions: [TP0], timeout: -1 }),
      () => c.partitionsFor({ topic: TOPIC, timeout: -1 }),
      () => c.listTopics({ timeout: -1 }),
      () => c.offsetsForTimes({ timestampsToSearch: new TopicPartitionMap([[TP0, 0n]]), timeout: -1 }),
      () => c.beginningOffsets({ partitions: [TP0], timeout: -1 }),
      () => c.endOffsets({ partitions: [TP0], timeout: -1 }),
    ]) {
      await assert.rejects(call(), NEGATIVE);
    }
  }
  // Open, without group.id: the FFI's timeout check before the core's group check.
  const groupless = newConsumer(null);
  for (const call of [
    () => groupless.commitSync({ timeout: -1 }),
    () => groupless.commitSync({ offsets, timeout: -1 }),
    () => groupless.committed({ partitions: [TP0], timeout: -1 }),
  ]) {
    await assert.rejects(call(), NEGATIVE);
  }
  // poll keeps Java's order: Java checks the timeout before the closed check.
  await assert.rejects(closed.poll({ timeout: -1 }), NEGATIVE);
  // With a valid timeout, the closed and group checks answer.
  await assert.rejects(closed.commitSync({ timeout: 0 }), { constructor: IllegalStateError, message: CLOSED });
  await assert.rejects(groupless.commitSync({ timeout: 0 }), { constructor: InvalidGroupIdError, message: NO_GROUP_ID });
  // While a poll is pending: Java throws ConcurrentModificationException first.
  const busy = newConsumer();
  await busy.assign({ partitions: [TP0] });
  const polling = busy.poll({ timeout: 10000 });
  // @ts-expect-error a null the types forbid
  await assert.rejects(busy.position(null), { constructor: TypeError, message: 'partition must not be null' });
  // The busy check comes before seek's negative offset, which Java checks first.
  await assert.rejects(busy.seek({ partition: TP0, offset: -1 }), { constructor: ConcurrentModificationError, message: CONCURRENT });
  busy.wakeup();
  await assert.rejects(polling, WakeupError);
  const busyGroupless = newConsumer(null);
  await busyGroupless.assign({ partitions: [TP0] });
  const pollingGroupless = busyGroupless.poll({ timeout: 10000 });
  assert.throws(() => busyGroupless.groupMetadata(), { constructor: InvalidGroupIdError, message: NO_GROUP_ID });
  busyGroupless.wakeup();
  await assert.rejects(pollingGroupless, WakeupError);
});

test('poll: its forms, a negative timeout with the core message, a Temporal.Duration', async () => {
  const consumer = newConsumer();
  await consumer.assign({ partitions: [TP0] });
  // The bare form, checked once.
  assert.ok((await consumer.poll(0)) instanceof ConsumerRecords);
  assert.equal((await consumer.poll({ timeout: Temporal.Duration.from({ milliseconds: 1 }) })).count(), 0);
  await assert.rejects(consumer.poll({ timeout: -1 }), { constructor: IllegalArgumentError, message: 'Timeout must not be negative' });
  // @ts-expect-error no argument
  await assert.rejects(consumer.poll(), { constructor: IllegalArgumentError, message: 'poll() takes one of (timeout); got ()' });
  // @ts-expect-error a null the types forbid
  await assert.rejects(consumer.poll(null), { constructor: TypeError, message: 'timeout must not be null' });
  // @ts-expect-error a string
  await assert.rejects(consumer.poll('1'), { constructor: IllegalArgumentError, message: 'poll() takes one of (timeout); got a string' });
  await assert.rejects(consumer.poll(NaN), { constructor: RangeError, message: 'timeout must be a number of milliseconds, not NaN' });
});

test('commitSync: its four forms, bare and by name, and their errors', async () => {
  const consumer = newConsumer();
  const forms = 'commitSync() takes one of (), (timeout), (offsets), (offsets, timeout)';
  const offsets = new TopicPartitionMap([[TP0, new OffsetAndMetadata({ offset: 5n, leaderEpoch: 1, metadata: 'm' })]]);
  // Nothing consumed: each form commits nothing and resolves.
  await consumer.commitSync();
  await consumer.commitSync({ timeout: 100 });
  // The bare forms, checked once each.
  await consumer.commitSync(100);
  await consumer.commitSync(new TopicPartitionMap());
  await consumer.commitSync({ offsets: new TopicPartitionMap(), timeout: Temporal.Duration.from({ milliseconds: 100 }) });
  // Offsets to an unassigned partition: the broker would take them; the
  // unreachable one never answers. The text is the core's: Java's commitSync
  // attaches no message of its own. Under load the event reaper may expire the
  // commit first, with its "<event> was <n> ms past its expiration of <deadline>",
  // the event named as the core names it, as Java's reaper races the commit too.
  await assert.rejects(consumer.commitSync({ offsets, timeout: 300 }), {
    constructor: KafkaTimeoutError,
    message: /^(Timeout of 300ms expired before successfully committing offsets|.+ was \d+ ms past its expiration of \d+)$/,
  });
  // A bare null fits both one-parameter forms, so neither takes it.
  // @ts-expect-error a null the types forbid
  await assert.rejects(consumer.commitSync(null), { constructor: IllegalArgumentError, message: `${forms}; got null` });
  // @ts-expect-error a string
  await assert.rejects(consumer.commitSync('1'), { constructor: IllegalArgumentError, message: `${forms}; got a string` });
  // @ts-expect-error a second argument
  await assert.rejects(consumer.commitSync(offsets, 100), {
    constructor: IllegalArgumentError,
    message: `${forms}; got 2 arguments, not one object of named parameters`,
  });
  // @ts-expect-error a name no form has
  await assert.rejects(consumer.commitSync({ offset: offsets }), { constructor: IllegalArgumentError, message: `${forms}; got (offset)` });
  // @ts-expect-error undefined is no value
  await assert.rejects(consumer.commitSync(undefined), {
    constructor: TypeError,
    message: "timeout must not be undefined. Leave the argument out, or pass null for Java's null",
  });
  // A Map keyed by TopicPartition is refused with the hint, by name and bare.
  const hint =
    'offsets must be a TopicPartitionMap, not a Map: a Map keyed by TopicPartition compares its keys by identity, so build one with new TopicPartitionMap(entries)';
  // @ts-expect-error a Map is no TopicPartitionMap
  await assert.rejects(consumer.commitSync({ offsets: new Map([[TP0, new OffsetAndMetadata(1)]]) }), {
    constructor: TypeError,
    message: hint,
  });
  // @ts-expect-error a Map is no TopicPartitionMap
  await assert.rejects(consumer.commitSync(new Map()), { constructor: TypeError, message: hint });
  // Java's new HashMap<>(null) throws; the FFI refuses a null map or value.
  // @ts-expect-error a null the types forbid
  await assert.rejects(consumer.commitSync({ offsets: null }), { constructor: TypeError, message: 'offsets must not be null' });
  // @ts-expect-error a null value the types forbid
  await assert.rejects(consumer.commitSync({ offsets: new TopicPartitionMap([[TP0, null]]) }), {
    constructor: TypeError,
    message: 'offsets must not contain null',
  });
  // A timeout: the core's message for a negative one, null a TypeError.
  for (const call of [() => consumer.commitSync({ timeout: -1 }), () => consumer.commitSync({ offsets, timeout: -1 })]) {
    await assert.rejects(call(), { constructor: IllegalArgumentError, message: 'Timeout must not be negative' });
  }
  // @ts-expect-error a null the types forbid
  await assert.rejects(consumer.commitSync({ timeout: null }), { constructor: TypeError, message: 'timeout must not be null' });
  // @ts-expect-error a null the types forbid
  await assert.rejects(consumer.commitSync({ offsets, timeout: null }), { constructor: TypeError, message: 'timeout must not be null' });
});

test('commitAsync() settles once the core took the commit, and takes no argument', async () => {
  const consumer = newConsumer();
  await consumer.assign({ partitions: [TP0] });
  await consumer.commitAsync();
  // The forms with a callback are not in this release.
  await assert.rejects(
    // @ts-expect-error a callback
    consumer.commitAsync(() => {}),
    { constructor: IllegalArgumentError, message: 'commitAsync() takes one of (); got a function' },
  );
  await assert.rejects(
    // @ts-expect-error an argument
    consumer.commitAsync(undefined),
    { constructor: TypeError, message: 'commitAsync() takes no argument: leave it out rather than pass undefined' },
  );
});

test('seek: its two forms and their errors', async () => {
  const consumer = newConsumer();
  const forms = 'seek() takes one of (partition, offset), (partition, offsetAndMetadata)';
  await consumer.assign({ partitions: [TP0] });
  await consumer.seek({ partition: TP0, offset: 5 });
  assert.equal(await consumer.position({ partition: TP0 }), 5n);
  await consumer.seek({ partition: TP0, offsetAndMetadata: new OffsetAndMetadata({ offset: 7n, leaderEpoch: 2, metadata: '' }) });
  assert.equal(await consumer.position({ partition: TP0 }), 7n);
  // Each form takes its parameters by name: no bare form.
  // @ts-expect-error a bare partition
  await assert.rejects(consumer.seek(TP0), { constructor: IllegalArgumentError, message: `${forms}; got a TopicPartition` });
  // @ts-expect-error two forms at once
  await assert.rejects(consumer.seek({ partition: TP0, offset: 1, offsetAndMetadata: new OffsetAndMetadata(1) }), {
    constructor: IllegalArgumentError,
    message: `${forms}; got (partition, offset, offsetAndMetadata)`,
  });
  await assert.rejects(consumer.seek({ partition: TP0, offset: 2 ** 60 }), {
    constructor: RangeError,
    message: 'offset must be a bigint beyond 2^53 - 1: a number holds no larger integer exactly',
  });
  // @ts-expect-error a string
  await assert.rejects(consumer.seek({ partition: TP0, offset: '1' }), {
    constructor: TypeError,
    message: 'offset must be a bigint or an integer',
  });
  // @ts-expect-error a number is no OffsetAndMetadata
  await assert.rejects(consumer.seek({ partition: TP0, offsetAndMetadata: 1 }), {
    constructor: TypeError,
    message: 'offsetAndMetadata must be an OffsetAndMetadata',
  });
  // The FFI refuses a null partition or OffsetAndMetadata.
  // @ts-expect-error a null the types forbid
  await assert.rejects(consumer.seek({ partition: null, offset: 1 }), { constructor: TypeError, message: 'partition must not be null' });
  // @ts-expect-error a null the types forbid
  await assert.rejects(consumer.seek({ partition: TP0, offsetAndMetadata: null }), {
    constructor: TypeError,
    message: 'offset_and_metadata must not be null',
  });
  await assert.rejects(consumer.seek({ partition: TP1, offset: 1 }), {
    constructor: IllegalStateError,
    message: 'No current assignment for partition test-1',
  });
});

test('seekToBeginning and seekToEnd: bare and by name, Java null check first', async () => {
  const consumer = newConsumer();
  await consumer.assign({ partitions: [TP0] });
  await consumer.seekToBeginning({ partitions: [TP0] });
  await consumer.seekToEnd({ partitions: [TP0] });
  // The bare forms, checked once each.
  await consumer.seekToBeginning([TP0]);
  await consumer.seekToEnd(new Set([TP0]));
  for (const method of /** @type {const} */ (['seekToBeginning', 'seekToEnd'])) {
    // Java's own null check, before the closed and busy checks.
    // @ts-expect-error a null the types forbid
    await assert.rejects(consumer[method]({ partitions: null }), {
      constructor: IllegalArgumentError,
      message: 'Partitions collection cannot be null',
    });
    // @ts-expect-error a null the types forbid
    await assert.rejects(consumer[method](null), { constructor: IllegalArgumentError, message: 'Partitions collection cannot be null' });
    await assert.rejects(consumer[method]({ partitions: [TP1] }), {
      constructor: IllegalStateError,
      message: 'No current assignment for partition test-1',
    });
    // @ts-expect-error a string
    await assert.rejects(consumer[method]('test-0'), {
      constructor: IllegalArgumentError,
      message: `${method}() takes one of (partitions); got a string`,
    });
  }
});

test('position: its forms, bare and by name, and their errors', async () => {
  const consumer = newConsumer();
  await consumer.assign({ partitions: [TP0] });
  await consumer.seek({ partition: TP0, offset: 3n });
  // The bare form, checked once.
  const position = await consumer.position(TP0);
  assert.equal(typeof position, 'bigint');
  assert.equal(position, 3n);
  assert.equal(await consumer.position({ partition: TP0, timeout: 100 }), 3n);
  await assert.rejects(consumer.position({ partition: TP1 }), {
    constructor: IllegalStateError,
    message: 'You can only check the position for partitions assigned to this consumer.',
  });
  await assert.rejects(consumer.position({ partition: TP0, timeout: -1 }), {
    constructor: IllegalArgumentError,
    message: 'Timeout must not be negative',
  });
  // @ts-expect-error a null the types forbid
  await assert.rejects(consumer.position({ partition: TP0, timeout: null }), {
    constructor: TypeError,
    message: 'timeout must not be null',
  });
  // @ts-expect-error a string
  await assert.rejects(consumer.position('test-0'), {
    constructor: IllegalArgumentError,
    message: 'position() takes one of (partition), (partition, timeout); got a string',
  });
});

test('committed: its forms, the group.id check of committed(null), and their errors', async () => {
  const consumer = newConsumer();
  const forms = 'committed() takes one of (partitions), (partitions, timeout)';
  // Nothing asked: an empty map, the core's answer without a request.
  const none = await consumer.committed({ partitions: [] });
  assert.ok(none instanceof TopicPartitionMap);
  assert.equal(none.size(), 0);
  // The bare form, checked once.
  assert.equal((await consumer.committed(new Set())).size(), 0);
  assert.equal((await consumer.committed({ partitions: [], timeout: 100 })).size(), 0);
  // With a group.id, the FFI refuses the null.
  // @ts-expect-error a null the types forbid
  await assert.rejects(consumer.committed({ partitions: null }), { constructor: TypeError, message: 'partitions must not be null' });
  // Without one, the binding throws Java's InvalidGroupIdException first, as
  // Java's throwIfGroupIdNotDefined() comes before the null's dereference.
  const groupless = newConsumer(null);
  // @ts-expect-error a null the types forbid
  await assert.rejects(groupless.committed(null), { constructor: InvalidGroupIdError, message: NO_GROUP_ID });
  // @ts-expect-error a null the types forbid
  await assert.rejects(groupless.committed({ partitions: null, timeout: 100 }), { constructor: InvalidGroupIdError, message: NO_GROUP_ID });
  await assert.rejects(consumer.committed({ partitions: [TP0], timeout: -1 }), {
    constructor: IllegalArgumentError,
    message: 'Timeout must not be negative',
  });
  // @ts-expect-error a string
  await assert.rejects(consumer.committed('test-0'), { constructor: IllegalArgumentError, message: `${forms}; got a string` });
});

test('currentLag: null while the lag is unknown, a bigint argument, and its errors', async () => {
  const consumer = newConsumer();
  await consumer.assign({ partitions: [TP0] });
  // Assigned, with no position or end offset yet: Java's empty OptionalLong.
  assert.equal(await consumer.currentLag({ topicPartition: TP0 }), null);
  // The bare form, checked once.
  assert.equal(await consumer.currentLag(TP0), null);
  await assert.rejects(consumer.currentLag({ topicPartition: TP1 }), {
    constructor: IllegalStateError,
    message: 'No current assignment for partition test-1',
  });
  // @ts-expect-error a null the types forbid
  await assert.rejects(consumer.currentLag(null), { constructor: TypeError, message: 'topic_partition must not be null' });
  // @ts-expect-error a string
  await assert.rejects(consumer.currentLag('test-0'), {
    constructor: IllegalArgumentError,
    message: 'currentLag() takes one of (topicPartition); got a string',
  });
});

test('groupMetadata(): the group, the temporary group.id check, no argument', () => {
  const metadata = newConsumer().groupMetadata();
  assert.ok(metadata instanceof ConsumerGroupMetadata);
  assert.equal(metadata.groupId(), GROUP_ID);
  // Before the member joins.
  assert.equal(metadata.generationId(), -1);
  assert.equal(metadata.memberId(), '');
  assert.equal(metadata.groupInstanceId(), null);
  assert.equal(newConsumer(GROUP_ID, { 'group.instance.id': 'i' }).groupMetadata().groupInstanceId(), 'i');
  assert.throws(() => newConsumer(null).groupMetadata(), { constructor: InvalidGroupIdError, message: NO_GROUP_ID });
  // A null group.id is no group.id.
  assert.throws(() => newConsumer(GROUP_ID, { 'group.id': null }).groupMetadata(), {
    constructor: InvalidGroupIdError,
    message: NO_GROUP_ID,
  });
  // @ts-expect-error an argument
  assert.throws(() => newConsumer().groupMetadata(1), {
    constructor: TypeError,
    message: 'groupMetadata() only reads: it takes no argument',
  });
});

test('partitionsFor and listTopics: their forms and errors', async () => {
  const consumer = newConsumer(GROUP_ID, { 'default.api.timeout.ms': 300 });
  // No broker: the core's timeouts. The usual text is the one the core's
  // application side gives when it stops waiting; Java's says "Timeout expired
  // while fetching topic metadata" for both. At the deadline the core's
  // topic-metadata manager, which gives Java's text, or its event reaper, "<event>
  // was <n> ms past its expiration of <deadline>", may complete the request
  // first, so either is taken; in Java, whose application thread waits with no
  // deadline of its own, the two race each other.
  /** @param {string} usual */
  const metadataTimeout = (usual) =>
    new RegExp(`^(${usual}|Timeout expired while fetching topic metadata|.+ was \\d+ ms past its expiration of \\d+)$`);
  await assert.rejects(consumer.partitionsFor({ topic: TOPIC }), {
    constructor: KafkaTimeoutError,
    message: metadataTimeout('Timeout expired while waiting for TopicMetadata'),
  });
  // The bare forms, checked once each.
  await assert.rejects(consumer.partitionsFor(TOPIC), KafkaTimeoutError);
  await assert.rejects(consumer.listTopics(), {
    constructor: KafkaTimeoutError,
    message: metadataTimeout('Timeout expired while waiting for AllTopicsMetadata'),
  });
  await assert.rejects(consumer.listTopics(100), KafkaTimeoutError);
  // The core's text too: Java's listTopics throws a TimeoutException with no message.
  await assert.rejects(consumer.listTopics({ timeout: 0 }), {
    constructor: KafkaTimeoutError,
    message: "Timeout of 0ms expired before all topics' metadata could be listed",
  });
  await assert.rejects(consumer.partitionsFor({ topic: TOPIC, timeout: 100 }), KafkaTimeoutError);
  // The FFI refuses a null topic.
  // @ts-expect-error a null the types forbid
  await assert.rejects(consumer.partitionsFor(null), { constructor: TypeError, message: 'topic must not be null' });
  for (const call of [() => consumer.partitionsFor({ topic: TOPIC, timeout: -1 }), () => consumer.listTopics({ timeout: -1 })]) {
    await assert.rejects(call(), { constructor: IllegalArgumentError, message: 'Timeout must not be negative' });
  }
  // @ts-expect-error a null the types forbid
  await assert.rejects(consumer.listTopics(null), { constructor: TypeError, message: 'timeout must not be null' });
  // @ts-expect-error a number is no topic
  await assert.rejects(consumer.partitionsFor(1), {
    constructor: IllegalArgumentError,
    message: 'partitionsFor() takes one of (topic), (topic, timeout); got a number',
  });
  // @ts-expect-error a string is no timeout
  await assert.rejects(consumer.listTopics('1'), {
    constructor: IllegalArgumentError,
    message: 'listTopics() takes one of (), (timeout); got a string',
  });
});

test('offsetsForTimes: its forms, a null timestamp, a Map refused, and the core checks', async () => {
  const consumer = newConsumer(GROUP_ID, { 'default.api.timeout.ms': 300 });
  // Nothing asked: an empty map, the core's answer without a request.
  const none = await consumer.offsetsForTimes({ timestampsToSearch: new TopicPartitionMap() });
  assert.ok(none instanceof TopicPartitionMap);
  assert.equal(none.size(), 0);
  // The bare form, checked once; a number is a long too.
  assert.equal((await consumer.offsetsForTimes(new TopicPartitionMap())).size(), 0);
  await assert.rejects(consumer.offsetsForTimes({ timestampsToSearch: new TopicPartitionMap([[TP0, 5]]), timeout: 100 }), {
    constructor: KafkaTimeoutError,
    message: 'Failed to get offsets by times in 100ms',
  });
  // Java's check of each timestamp, the core's.
  await assert.rejects(consumer.offsetsForTimes({ timestampsToSearch: new TopicPartitionMap([[TP0, -5n]]) }), {
    constructor: IllegalArgumentError,
    message: 'The target time for partition test-0 is -5. The target time cannot be negative.',
  });
  // A null timestamp has no int64 to cross as: Java's unboxing throws NullPointerException.
  // @ts-expect-error a null the types forbid
  await assert.rejects(consumer.offsetsForTimes({ timestampsToSearch: new TopicPartitionMap([[TP0, null]]) }), {
    constructor: TypeError,
    message: 'timestampsToSearch must not contain a null timestamp',
  });
  // Java's requireNonNull, the FFI's.
  // @ts-expect-error a null the types forbid
  await assert.rejects(consumer.offsetsForTimes(null), { constructor: TypeError, message: 'Timestamps to search cannot be null' });
  const hint =
    'timestampsToSearch must be a TopicPartitionMap, not a Map: a Map keyed by TopicPartition compares its keys by identity, so build one with new TopicPartitionMap(entries)';
  // @ts-expect-error a Map is no TopicPartitionMap
  await assert.rejects(consumer.offsetsForTimes(new Map([[TP0, 0n]])), { constructor: TypeError, message: hint });
  // @ts-expect-error a Map is no TopicPartitionMap
  await assert.rejects(consumer.offsetsForTimes({ timestampsToSearch: new Map() }), { constructor: TypeError, message: hint });
  await assert.rejects(consumer.offsetsForTimes({ timestampsToSearch: new TopicPartitionMap([[TP0, 2 ** 60]]) }), RangeError);
  await assert.rejects(consumer.offsetsForTimes({ timestampsToSearch: new TopicPartitionMap(), timeout: -1 }), {
    constructor: IllegalArgumentError,
    message: 'Timeout must not be negative',
  });
});

test('beginningOffsets and endOffsets: their forms and errors', async () => {
  const consumer = newConsumer(GROUP_ID, { 'default.api.timeout.ms': 300 });
  for (const method of /** @type {const} */ (['beginningOffsets', 'endOffsets'])) {
    const none = await consumer[method]({ partitions: [] });
    assert.ok(none instanceof TopicPartitionMap);
    assert.equal(none.size(), 0);
    // The bare form, checked once each.
    assert.equal((await consumer[method](new Set())).size(), 0);
    // A zero timeout answers an empty map at once, as Java's.
    assert.equal((await consumer[method]({ partitions: [TP0], timeout: 0 })).size(), 0);
    await assert.rejects(consumer[method]({ partitions: [TP0], timeout: 100 }), {
      constructor: KafkaTimeoutError,
      message: 'Failed to get offsets by times in 100ms',
    });
    // Java's requireNonNull, the FFI's.
    // @ts-expect-error a null the types forbid
    await assert.rejects(consumer[method](null), { constructor: TypeError, message: 'Partitions cannot be null' });
    await assert.rejects(consumer[method]({ partitions: [TP0], timeout: -1 }), {
      constructor: IllegalArgumentError,
      message: 'Timeout must not be negative',
    });
    // @ts-expect-error a string
    await assert.rejects(consumer[method]('test-0'), {
      constructor: IllegalArgumentError,
      message: `${method}() takes one of (partitions), (partitions, timeout); got a string`,
    });
  }
});

test('unsubscribe, wakeup and the getters take no argument', async () => {
  const consumer = newConsumer();
  // @ts-expect-error an argument
  await assert.rejects(consumer.unsubscribe(1), TypeError);
  // @ts-expect-error an argument
  assert.throws(() => consumer.wakeup(1), TypeError);
  for (const method of /** @type {const} */ (['assignment', 'subscription', 'paused', 'metrics'])) {
    // @ts-expect-error an argument
    assert.throws(() => consumer[method](1), { constructor: TypeError, message: `${method}() only reads: it takes no argument` });
  }
});

test('one call at a time: the core busy error, wakeup() ending the waiting poll, close() refused meanwhile', async () => {
  const consumer = newConsumer();
  await consumer.subscribe({ topics: [TOPIC] });
  const polling = consumer.poll({ timeout: 10000 });
  for (const call of [
    consumer.subscribe({ topics: [TOPIC] }),
    consumer.assign({ partitions: [TP0] }),
    consumer.unsubscribe(),
    consumer.commitSync(),
    consumer.commitAsync(),
    consumer.seek({ partition: TP0, offset: 0 }),
    consumer.seekToBeginning({ partitions: [TP0] }),
    consumer.seekToEnd({ partitions: [TP0] }),
    consumer.position({ partition: TP0 }),
    consumer.committed({ partitions: [TP0] }),
    consumer.currentLag({ topicPartition: TP0 }),
    consumer.partitionsFor({ topic: TOPIC }),
    consumer.listTopics(),
    consumer.offsetsForTimes({ timestampsToSearch: new TopicPartitionMap([[TP0, 0n]]) }),
    consumer.beginningOffsets({ partitions: [TP0] }),
    consumer.endOffsets({ partitions: [TP0] }),
    consumer.close(),
  ]) {
    await assert.rejects(call, { constructor: ConcurrentModificationError, message: CONCURRENT });
  }
  assert.throws(() => consumer.assignment(), { constructor: ConcurrentModificationError, message: CONCURRENT });
  assert.throws(() => consumer.groupMetadata(), { constructor: ConcurrentModificationError, message: CONCURRENT });
  // metrics() is exempt.
  assert.ok(consumer.metrics().size() > 0);
  consumer.wakeup();
  await assert.rejects(polling, { constructor: WakeupError, message: '' });
  // The wakeup was used once: the next poll waits as asked.
  assert.equal((await consumer.poll({ timeout: 0 })).isEmpty(), true);
});

test('a wakeup with no waiting call wakes the next one', async () => {
  const consumer = newConsumer();
  await consumer.subscribe({ topics: [TOPIC] });
  consumer.wakeup();
  await assert.rejects(consumer.poll({ timeout: 10000 }), WakeupError);
});

test('close(option) with and without a timeout, a negative one read as zero; the closed getters', async () => {
  // With no timeout set, CloseOptions closes as Java builds them: the
  // default timeout, here unspent, as nothing is pending.
  const leave = newConsumer();
  opened.pop();
  await leave.close({
    option: CloseOptions.groupMembershipOperation({ operation: CloseOptions.GroupMembershipOperation.REMAIN_IN_GROUP }),
  });
  const timed = newConsumer();
  opened.pop();
  await timed.close(
    CloseOptions.timeout({ timeout: 0 }).withGroupMembershipOperation({ operation: CloseOptions.GroupMembershipOperation.LEAVE_GROUP }),
  );
  // A negative timeout is read as zero by the FFI, where Java throws.
  const negative = newConsumer();
  opened.pop();
  const started = Date.now();
  await negative.close({ option: CloseOptions.timeout({ timeout: -5 }) });
  assert.ok(Date.now() - started < 5000);
  // Java's close(null) dereferences the null: the FFI refuses it.
  const nulled = newConsumer();
  // @ts-expect-error a null the types forbid
  await assert.rejects(nulled.close({ option: null }), { constructor: TypeError, message: 'options must not be null' });
  // @ts-expect-error a number is no CloseOptions
  await assert.rejects(nulled.close(1), { constructor: IllegalArgumentError, message: 'close() takes one of (), (option); got a number' });
  // @ts-expect-error a plain object is no CloseOptions
  await assert.rejects(nulled.close({ option: 5 }), { constructor: TypeError, message: 'option must be a CloseOptions' });
  // The closed getters panic in the core, whose message the FFI prefixes.
  for (const fn of /** @type {const} */ (['assignment', 'subscription', 'paused'])) {
    assert.throws(() => leave[fn](), { constructor: IllegalStateError, message: CLOSED_GETTER(fn) });
  }
  assert.throws(() => leave.groupMetadata(), { constructor: IllegalStateError, message: CLOSED_GETTER('group_metadata') });
  // The waiting calls get the core closed error, as a rejection.
  for (const call of [
    () => leave.poll({ timeout: 0 }),
    () => leave.subscribe({ topics: [TOPIC] }),
    () => leave.commitSync(),
    () => leave.commitSync({ timeout: 0 }),
    () => leave.commitSync({ offsets: new TopicPartitionMap() }),
    () => leave.commitSync({ offsets: new TopicPartitionMap(), timeout: 0 }),
    () => leave.commitAsync(),
    () => leave.seek({ partition: TP0, offset: 0 }),
    () => leave.seek({ partition: TP0, offsetAndMetadata: new OffsetAndMetadata(0) }),
    () => leave.seekToBeginning({ partitions: [TP0] }),
    () => leave.seekToEnd({ partitions: [TP0] }),
    () => leave.position({ partition: TP0 }),
    () => leave.position({ partition: TP0, timeout: 0 }),
    () => leave.committed({ partitions: [TP0] }),
    () => leave.committed({ partitions: [TP0], timeout: 0 }),
    () => leave.currentLag({ topicPartition: TP0 }),
    () => leave.partitionsFor({ topic: TOPIC }),
    () => leave.partitionsFor({ topic: TOPIC, timeout: 0 }),
    () => leave.listTopics(),
    () => leave.listTopics({ timeout: 0 }),
    () => leave.offsetsForTimes({ timestampsToSearch: new TopicPartitionMap() }),
    () => leave.offsetsForTimes({ timestampsToSearch: new TopicPartitionMap(), timeout: 0 }),
    () => leave.beginningOffsets({ partitions: [TP0] }),
    () => leave.beginningOffsets({ partitions: [TP0], timeout: 0 }),
    () => leave.endOffsets({ partitions: [TP0] }),
    () => leave.endOffsets({ partitions: [TP0], timeout: 0 }),
  ]) {
    await assert.rejects(call(), { constructor: IllegalStateError, message: CLOSED });
  }
  // A second close is harmless.
  await leave.close();
  await leave[Symbol.asyncDispose]();
});

test('metrics() is a live view, and answers after close()', async () => {
  const consumer = new KafkaConsumer({ configs: configs() });
  const view = consumer.metrics();
  const lastPoll = () => [...view.keys()].some((n) => n.name() === 'last-poll-seconds-ago' && n.group() === 'consumer-metrics');
  await consumer.subscribe({ topics: [TOPIC] });
  assert.equal(lastPoll(), true);
  // Removed by the close: the view taken before sees it go.
  await consumer.close();
  assert.equal(lastPoll(), false);
  assert.ok(consumer.metrics().size() > 0);
});

test('a Promise method never throws: it rejects', () => {
  const consumer = newConsumer();
  for (const call of [
    // @ts-expect-error a wrong argument
    () => consumer.subscribe(1),
    // @ts-expect-error a wrong argument
    () => consumer.assign(1),
    // @ts-expect-error a wrong argument
    () => consumer.poll('x'),
    // @ts-expect-error a wrong argument
    () => consumer.close(1),
    // @ts-expect-error a wrong argument
    () => consumer.commitSync('x'),
    // @ts-expect-error a wrong argument
    () => consumer.commitAsync(1),
    // @ts-expect-error a wrong argument
    () => consumer.seek(1),
    // @ts-expect-error a wrong argument
    () => consumer.seekToBeginning(1),
    // @ts-expect-error a wrong argument
    () => consumer.seekToEnd(1),
    // @ts-expect-error a wrong argument
    () => consumer.position(1),
    // @ts-expect-error a wrong argument
    () => consumer.committed(1),
    // @ts-expect-error a wrong argument
    () => consumer.currentLag(1),
    // @ts-expect-error a wrong argument
    () => consumer.partitionsFor(1),
    // @ts-expect-error a wrong argument
    () => consumer.listTopics('x'),
    // @ts-expect-error a wrong argument
    () => consumer.offsetsForTimes(1),
    // @ts-expect-error a wrong argument
    () => consumer.beginningOffsets(1),
    // @ts-expect-error a wrong argument
    () => consumer.endOffsets(1),
  ]) {
    const result = call();
    assert.ok(result instanceof Promise);
    result.catch(() => {});
  }
});

/**
 * Runs `script` in a child Node with the package root as its directory.
 *
 * @param {string} script
 * @param {number} timeout
 * @returns {import('node:child_process').SpawnSyncReturns<string>} the outcome
 */
function child(script, timeout) {
  return spawnSync(process.execPath, ['-e', script], { cwd: path.join(__dirname, '..', '..', '..'), encoding: 'utf8', timeout });
}

const CHILD_CONSUMER = `
  const { KafkaConsumer, CloseOptions } = require('@confluentinc/kafka-javascript/consumer');
  const c = new KafkaConsumer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'group.protocol': 'consumer', 'group.id': 'g' } });
`;

test('a consumer keeps Node running until close(), and every call made on it gets its answer', () => {
  // Never closed: Node keeps running until the child is killed.
  const open = child(`${CHILD_CONSUMER} console.log('open');`, 3000);
  assert.equal(open.signal, 'SIGTERM');
  assert.equal(open.stdout, 'open\n');
  // A close the FFI refuses (null options) closes nothing: the consumer stays
  // open and keeps Node running.
  const refused = child(
    `${CHILD_CONSUMER}
    c.close({ option: null }).catch((e) => console.log('refused: ' + e.message + '; ' + c.assignment().size()));`,
    3000,
  );
  assert.equal(refused.signal, 'SIGTERM', refused.stderr);
  assert.equal(refused.stdout, 'refused: options must not be null; 0\n');
  // Closed: Node exits, after the answers of the calls made on it, the closed
  // consumer included.
  const closed = child(
    `${CHILD_CONSUMER}
    c.close({ option: CloseOptions.timeout({ timeout: 0 }) }).then(() => {
      console.log('closed');
      return c.poll({ timeout: 0 }).catch((e) => console.log('after: ' + e.message));
    }).then(() => c.close()).then(() => console.log('closed again'));`,
    30000,
  );
  assert.equal(closed.signal, null, closed.stderr);
  assert.equal(closed.status, 0, closed.stderr);
  assert.equal(closed.stdout, `closed\nafter: ${CLOSED}\nclosed again\n`);
});

test('the preview warning is emitted once per process', () => {
  const r = child(
    `${CHILD_CONSUMER}
    const d = new KafkaConsumer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'group.protocol': 'consumer' } });
    Promise.all([c.close(), d.close()]);`,
    30000,
  );
  assert.equal(r.status, 0, r.stderr);
  const warnings = r.stderr.split('\n').filter((line) => line.includes('ExperimentalWarning'));
  assert.equal(warnings.length, 1, r.stderr);
  assert.match(warnings[0], /The JavaScript client is in preview and not yet recommended for production use\./);
});
