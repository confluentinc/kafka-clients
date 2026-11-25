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
// Of Java's 135 tests, 30 are ported here:
//   testConstructorClose, testOsDefaultSocketBufferSizes,
//   shouldIgnoreGroupInstanceIdForEmptyGroupId, testEmptyGroupId,
//   testGroupIdWithWhitespace,
//   testConsumerConstructorFailsWithConfigExceptionOnUnresolvableBootstrapWhenTimeoutZero,
//   testOperationsBySubscribingConsumerWithDefaultGroupId and
//   testOperationsByAssigningConsumerWithDefaultGroupId (their constructor,
//   subscribe and assign halves: the commit halves need commitSync,
//   commitAsync and committed, which this release does not have), testSubscription,
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
//   through the package-private kafkaConsumerMetrics() on a MockTime) and
//   testCloseShouldBeIdempotent.
//
// Not here yet, for want of the methods they test (the offsets, commits and
// group metadata): testSeekNegative, testInvalidGroupMetadata,
// testOffsetsForTimesTimeout, testBeginningOffsetsTimeout,
// testEndOffsetsTimeout and testCommittedThrowsTimeoutExceptionForNoResponse;
// for want of deserializers: configurableObjectsShouldSeeGeneratedClientId.
// Against a broker, where Java scripts a MockClient: testMissingOffsetNoResetPolicy,
// testResetToCommittedOffset, testResetUsingAutoResetPolicy,
// testResetUsingDurationBasedAutoResetPolicy, testOffsetIsValidAfterSeek,
// testCommitsFetchedDuringAssign, testNoCommittedOffsets,
// testManualAssignmentChangeWithAutoCommitEnabled,
// testManualAssignmentChangeWithAutoCommitDisabled, testOffsetOfPausedPartitions,
// testListOffsetShouldUpdateSubscriptions, testSubscriptionOnInvalidTopic,
// verifyNoCoordinatorLookupForManualAssignmentWithSeek and testPollSendsRequestToJoin.
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
const { KafkaError, TopicPartition, TopicPartitionSet } = require('@confluentinc/kafka-javascript/common');
const { ConfigError } = require('@confluentinc/kafka-javascript/common/config');
const {
  BootstrapResolutionError,
  InvalidConfigurationError,
  InvalidGroupIdError,
  WakeupError,
} = require('@confluentinc/kafka-javascript/common/errors');
const { CloseOptions, ConsumerRecords, KafkaConsumer, SubscriptionPattern } = require('@confluentinc/kafka-javascript/consumer');

/** @typedef {import('@confluentinc/kafka-javascript/consumer').ConsumerConfigs} ConsumerConfigs */

const TOPIC = 'test';
const GROUP_ID = 'mock-group';
const TP0 = new TopicPartition({ topic: TOPIC, partition: 0 });
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
  // Its constructor and subscribe halves: the commit half needs the commit
  // calls. enable.auto.commit=true needs a group.id: the core's error, unwrapped.
  assert.throws(() => new KafkaConsumer({ configs: configs(null, { 'enable.auto.commit': true }) }), {
    constructor: InvalidConfigurationError,
    message: 'enable.auto.commit cannot be set to true when default group id (null) is used.',
  });
  await assert.rejects(newConsumer(null).subscribe({ topics: [TOPIC] }), { constructor: InvalidGroupIdError, message: NO_GROUP_ID });
  // Java's subscribe() checks the group before the topics.
  // @ts-expect-error a null topic the types forbid: the run time sends it as a blank one
  await assert.rejects(newConsumer(null).subscribe({ topics: [null] }), { constructor: InvalidGroupIdError, message: NO_GROUP_ID });
});

test('testOperationsByAssigningConsumerWithDefaultGroupId', async () => {
  // Its assign half: the commit half needs the commit calls.
  const consumer = newConsumer(null);
  await consumer.assign({ partitions: [TP0] });
  assert.deepEqual(names(consumer.assignment()), ['test-0']);
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
  await consumer.subscribe({ topics: [TOPIC] });
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
  const forms = 'subscribe() takes one of (topics), (pattern)';
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
  // The listener forms come with the listeners.
  // @ts-expect-error a callback key
  await assert.rejects(consumer.subscribe({ topics: [TOPIC], callback: {} }), {
    constructor: IllegalArgumentError,
    message: `${forms}; got (topics, callback)`,
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
    consumer.close(),
  ]) {
    await assert.rejects(call, { constructor: ConcurrentModificationError, message: CONCURRENT });
  }
  assert.throws(() => consumer.assignment(), { constructor: ConcurrentModificationError, message: CONCURRENT });
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
  // The waiting calls get the core closed error, as a rejection.
  await assert.rejects(leave.poll({ timeout: 0 }), { constructor: IllegalStateError, message: CLOSED });
  await assert.rejects(leave.subscribe({ topics: [TOPIC] }), { constructor: IllegalStateError, message: CLOSED });
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
