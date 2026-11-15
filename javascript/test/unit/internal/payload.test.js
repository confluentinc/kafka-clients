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

// The payloads of the generated error classes (root CLAUDE.md, JavaScript
// Binding Conventions, Errors, Q39; Types, Q23 and Q25): Java's constructors
// that take a payload, in the { message, cause, ...Java's parameters } shape,
// and the payload getters over private fields. Java has no test of its own
// for these exception classes; the messages asserted are the ones Java's
// constructors build (their source in kafka/), and an error the core reports
// carries the payload the FFI's view of its class gives.

const test = require('node:test');
const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { MetricName, TopicPartition, TopicPartitionMap, TopicPartitionSet } = require('@confluentinc/kafka-javascript/common');
const {
  DuplicateResourceError,
  GroupAuthorizationError,
  InvalidTopicError,
  RecordTooLargeError,
  ResourceNotFoundError,
  ThrottlingQuotaExceededError,
  TopicAuthorizationError,
} = require('@confluentinc/kafka-javascript/common/errors');
const { ConfigError } = require('@confluentinc/kafka-javascript/common/config');
const { KafkaMetric, QuotaViolationError } = require('@confluentinc/kafka-javascript/common/metrics');
const { CorrelationIdMismatchError } = require('@confluentinc/kafka-javascript/common/requests');
const { InvalidOffsetError, LogTruncationError, NoOffsetForPartitionError, OffsetOutOfRangeError } =
  require('@confluentinc/kafka-javascript/consumer');
const { fromNative } = require('../../../dist/internal/errors');

const tp = (topic, partition) => new TopicPartition({ topic, partition });

test("TopicAuthorizationError: Java's three constructors and unauthorizedTopics()", () => {
  const built = new TopicAuthorizationError({ unauthorizedTopics: ['a', 'b'] });
  assert.equal(built.message, 'Not authorized to access topics: [a, b]');
  assert.deepEqual(built.unauthorizedTopics(), new Set(['a', 'b']));
  // A returned collection is a new object the caller may change.
  built.unauthorizedTopics().add('c');
  assert.deepEqual(built.unauthorizedTopics(), new Set(['a', 'b']));
  assert.notEqual(built.unauthorizedTopics(), built.unauthorizedTopics());
  const named = new TopicAuthorizationError({ message: 'denied', unauthorizedTopics: new Set(['t']) });
  assert.equal(named.message, 'denied');
  assert.deepEqual(named.unauthorizedTopics(), new Set(['t']));
  // TopicAuthorizationException(String message): this(message, Set.of()).
  assert.deepEqual(new TopicAuthorizationError('only').unauthorizedTopics(), new Set());
  // An iterable is read once: a generator is kept.
  function* topics() {
    yield 'x';
    yield 'y';
  }
  const fromGenerator = new TopicAuthorizationError({ unauthorizedTopics: topics() });
  assert.equal(fromGenerator.message, 'Not authorized to access topics: [x, y]');
  assert.deepEqual(fromGenerator.unauthorizedTopics(), new Set(['x', 'y']));
  // A string is no Collection<String>, as Python's guard says.
  assert.throws(() => new TopicAuthorizationError({ unauthorizedTopics: 'abc' }), {
    constructor: TypeError,
    message: 'unauthorizedTopics must be an iterable of strings, not string',
  });
  assert.throws(() => new TopicAuthorizationError({ unauthorizedTopics: 5 }), {
    constructor: TypeError,
    message: 'unauthorizedTopics must be an iterable of strings',
  });
  // A payload is given by name only; a form error lists the forms by count, ties in Java's order.
  assert.throws(() => new TopicAuthorizationError(['a']), {
    constructor: IllegalArgumentError,
    message: 'TopicAuthorizationError() takes one of ({ unauthorizedTopics }), (message), ({ message, unauthorizedTopics }); got an array',
  });
  assert.throws(() => new TopicAuthorizationError({ message: 'm', unauthorizedTopics: [] }, 2), {
    constructor: IllegalArgumentError,
    message: 'TopicAuthorizationError() takes one of ({ unauthorizedTopics }), (message), ({ message, unauthorizedTopics }); got 2 arguments',
  });
  // A getter only reads.
  assert.throws(() => built.unauthorizedTopics(1), { constructor: TypeError, message: 'unauthorizedTopics() only reads: it takes no argument' });
  // The payload is private: no public field.
  assert.deepEqual(Object.keys(built), []);
});

test('InvalidTopicError: the message and cause forms set an empty set; the payload forms keep it', () => {
  const cause = new Error('inner');
  for (const e of [new InvalidTopicError(), new InvalidTopicError('m'), new InvalidTopicError(cause), new InvalidTopicError({ message: 'm', cause })]) {
    assert.deepEqual(e.invalidTopics(), new Set());
  }
  assert.equal(new InvalidTopicError({ invalidTopics: ['bad topic'] }).message, 'Invalid topics: [bad topic]');
  const both = new InvalidTopicError({ message: 'm', invalidTopics: ['x'] });
  assert.equal(both.message, 'm');
  assert.deepEqual(both.invalidTopics(), new Set(['x']));
  assert.throws(() => new InvalidTopicError({ cause, invalidTopics: [] }), {
    constructor: IllegalArgumentError,
    message: 'InvalidTopicError() takes one of (), (message), (cause), ({ invalidTopics }), ({ message, cause }), ({ message, invalidTopics }); got (cause, invalidTopics)',
  });
});

test('RecordTooLargeError: recordTooLargePartitions is a TopicPartitionMap<bigint>, null by default', () => {
  assert.equal(new RecordTooLargeError('m').recordTooLargePartitions(), null);
  const given = new TopicPartitionMap([[tp('t', 0), 5]]);
  const e = new RecordTooLargeError({ message: 'too large', recordTooLargePartitions: given });
  const got = e.recordTooLargePartitions();
  assert.ok(got instanceof TopicPartitionMap);
  // A long taken as a number is a bigint; the map is a copy.
  assert.equal(got.get(tp('t', 0)), 5n);
  given.set(tp('t', 1), 6n);
  assert.equal(e.recordTooLargePartitions().size(), 1);
  // Only a TopicPartitionMap is taken, with a hint for a Map.
  assert.throws(() => new RecordTooLargeError({ message: 'm', recordTooLargePartitions: new Map([[tp('t', 0), 1n]]) }), {
    constructor: TypeError,
    message:
      'recordTooLargePartitions must be a TopicPartitionMap, not a Map: a Map keyed by TopicPartition compares its keys by identity, ' +
      'so build one with new TopicPartitionMap(entries)',
  });
  assert.throws(() => new RecordTooLargeError({ message: 'm', recordTooLargePartitions: new TopicPartitionMap([[tp('t', 0), 1.5]]) }), {
    constructor: TypeError,
    message: 'a value of recordTooLargePartitions must be a bigint or an integer',
  });
  assert.equal(new RecordTooLargeError({ message: 'm', recordTooLargePartitions: null }).recordTooLargePartitions(), null);
});

test('OffsetOutOfRangeError: offsetOutOfRangePartitions and partitions(), its key set', () => {
  const offsets = new TopicPartitionMap([[tp('t', 0), 7n], [tp('t', 1), 9n]]);
  const e = new OffsetOutOfRangeError({ offsetOutOfRangePartitions: offsets });
  assert.equal(e.message, 'Offsets out of range with no configured reset policy for partitions: {t-0=7, t-1=9}');
  assert.equal(e.offsetOutOfRangePartitions().toString(), '{t-0=7, t-1=9}');
  const partitions = e.partitions();
  assert.ok(partitions instanceof TopicPartitionSet);
  assert.equal(partitions.toString(), '[t-0, t-1]');
  assert.ok(e instanceof InvalidOffsetError);
  assert.equal(new OffsetOutOfRangeError({ message: 'm', offsetOutOfRangePartitions: offsets }).message, 'm');
  // Java's keySet() dereferences the field.
  const empty = new OffsetOutOfRangeError({ message: 'm', offsetOutOfRangePartitions: null });
  assert.throws(() => empty.partitions(), { constructor: TypeError, message: 'offsetOutOfRangePartitions must not be null' });
  assert.throws(() => new OffsetOutOfRangeError('m'), {
    constructor: IllegalArgumentError,
    message: 'OffsetOutOfRangeError() takes one of ({ offsetOutOfRangePartitions }), ({ message, offsetOutOfRangePartitions }); got a string',
  });
});

test('NoOffsetForPartitionError: one partition or a collection, kept as a TopicPartitionSet', () => {
  const one = new NoOffsetForPartitionError({ partition: tp('t', 2) });
  assert.equal(one.message, 'Undefined offset with no reset policy for partition: t-2');
  assert.equal(one.partitions().toString(), '[t-2]');
  const many = new NoOffsetForPartitionError({ partitions: [tp('t', 0), tp('t', 1), tp('t', 0)] });
  assert.equal(many.message, 'Undefined offset with no reset policy for partitions: [t-0, t-1]');
  assert.equal(many.partitions().size(), 2);
  // Set.copyOf dereferences the collection and refuses a null element.
  assert.throws(() => new NoOffsetForPartitionError({ partitions: null }), { constructor: TypeError, message: 'partitions must not be null' });
  assert.throws(() => new NoOffsetForPartitionError({ partitions: [null] }), { constructor: TypeError, message: 'partitions must not contain null' });
  assert.throws(() => new NoOffsetForPartitionError({ partition: 't-0' }), { constructor: TypeError, message: 'partition must be a TopicPartition' });
});

test("LogTruncationError: no constructor (its divergent offsets are the consumer's OffsetAndMetadata), the core's payload read", () => {
  assert.throws(() => new LogTruncationError({ message: 'm' }), {
    constructor: TypeError,
    message: 'LogTruncationError cannot be constructed: its Java constructors take a type the client does not translate yet',
  });
  const e = fromNative({ code: -20, message: 'truncated', payload: { offsetOutOfRangePartitions: [['t', 0, 3n]] } });
  assert.ok(e instanceof LogTruncationError && e instanceof OffsetOutOfRangeError);
  assert.equal(e.message, 'truncated');
  assert.equal(e.offsetOutOfRangePartitions().get(tp('t', 0)), 3n);
  assert.equal(e.partitions().toString(), '[t-0]');
  assert.equal(typeof e.divergentOffsets, 'undefined');
});

test('the String payloads: groupId, resource; GroupAuthorizationError.forGroupId', () => {
  assert.equal(new GroupAuthorizationError('m').groupId(), null);
  assert.equal(new GroupAuthorizationError({ message: 'm', groupId: 'g' }).groupId(), 'g');
  for (const e of [GroupAuthorizationError.forGroupId('g'), GroupAuthorizationError.forGroupId({ groupId: 'g' })]) {
    assert.ok(e instanceof GroupAuthorizationError);
    assert.equal(e.message, 'Not authorized to access group: g');
    assert.equal(e.groupId(), 'g');
  }
  assert.equal(GroupAuthorizationError.forGroupId(null).message, 'Not authorized to access group: null');
  assert.throws(() => GroupAuthorizationError.forGroupId(5), {
    constructor: IllegalArgumentError,
    message: 'forGroupId() takes one of (groupId); got a number',
  });
  assert.throws(() => new GroupAuthorizationError({ message: 'm', groupId: 5 }), { constructor: TypeError, message: 'groupId must be a string or null' });
  for (const cls of [ResourceNotFoundError, DuplicateResourceError]) {
    const cause = new Error('c');
    assert.equal(new cls('m').resource(), null, cls.name);
    assert.equal(new cls({ message: 'm', cause }).resource(), null, cls.name);
    assert.equal(new cls({ resource: 'r', message: 'm' }).resource(), 'r', cls.name);
    const withCause = new cls({ resource: 'r', message: 'm', cause });
    assert.equal(withCause.resource(), 'r');
    assert.equal(withCause.cause, cause);
  }
});

test('the number payloads: throttleTimeMs, the correlation ids, QuotaViolationError', () => {
  assert.equal(new ThrottlingQuotaExceededError('m').throttleTimeMs(), 0);
  assert.equal(new ThrottlingQuotaExceededError({ throttleTimeMs: 250, message: 'm' }).throttleTimeMs(), 250);
  assert.throws(() => new ThrottlingQuotaExceededError({ throttleTimeMs: 2 ** 31, message: 'm' }), { constructor: RangeError });
  const mismatch = new CorrelationIdMismatchError({ message: 'm', requestCorrelationId: 1, responseCorrelationId: 2 });
  assert.deepEqual([mismatch.requestCorrelationId(), mismatch.responseCorrelationId()], [1, 2]);
  // The FFI has no accessor for the correlation ids: an error the core
  // reports reads -1, as the Python client's does.
  const core = fromNative({ code: -24, message: 'mismatch' });
  assert.deepEqual([core.requestCorrelationId(), core.responseCorrelationId()], [-1, -1]);
  const metricName = new MetricName({ name: 'n', group: 'g', description: 'd', tags: new Map() });
  const metric = new KafkaMetric({ lock: {}, metricName, valueProvider: { measure: () => 1 }, config: null, time: { milliseconds: () => 0 } });
  const quota = new QuotaViolationError({ metric, value: 2, bound: 1.5 });
  assert.equal(quota.metric(), metric);
  assert.deepEqual([quota.value(), quota.bound()], [2, 1.5]);
  assert.equal(quota.message, '');
  // Java's toString() override, its doubles as Java's Double.toString writes them.
  assert.equal(String(quota), "QuotaViolationError: 'MetricName [name=n, group=g, description=d, tags={}]' violated quota. Actual: 2.0, Threshold: 1.5");
  // The core gives the value and the bound, not the KafkaMetric.
  const reported = fromNative({ code: -26, message: '', payload: { value: 3, bound: 1 } });
  assert.deepEqual([reported.metric(), reported.value(), reported.bound()], [null, 3, 1]);
  assert.equal(String(reported), 'QuotaViolationError');
});

test("ConfigError: Java's (name, value) and (name, value, message) build the message", () => {
  assert.equal(new ConfigError({ name: 'linger.ms', value: true }).message, 'Invalid value true for configuration linger.ms');
  assert.equal(new ConfigError({ name: 'a', value: null, message: 'bad' }).message, 'Invalid value null for configuration a: bad');
  assert.equal(new ConfigError({ name: 'a', value: 1, message: null }).message, 'Invalid value 1 for configuration a');
  assert.equal(new ConfigError('plain').message, 'plain');
  assert.throws(() => new ConfigError({ name: 'a' }), {
    constructor: IllegalArgumentError,
    message: 'ConfigError() takes one of (message), ({ name, value }), ({ name, value, message }); got (name)',
  });
});

test('an error the core reports carries the payload the FFI view gives', () => {
  const topics = fromNative({ code: 29, message: 'denied', apiError: true, payload: { unauthorizedTopics: ['a'] } });
  assert.ok(topics instanceof TopicAuthorizationError);
  assert.deepEqual(topics.unauthorizedTopics(), new Set(['a']));
  const large = fromNative({ code: 10, message: 'large', apiError: true, payload: { recordTooLargePartitions: [['t', 1, 2n]] } });
  assert.equal(large.recordTooLargePartitions().get(tp('t', 1)), 2n);
  const none = fromNative({ code: 10, message: 'large', apiError: true, payload: { recordTooLargePartitions: null } });
  assert.equal(none.recordTooLargePartitions(), null);
  const partitions = fromNative({ code: -21, message: 'no offset', payload: { partitions: [['t', 0], [null, 1]] } });
  assert.equal(partitions.partitions().toString(), '[t-0, null-1]');
  assert.equal(fromNative({ code: 30, message: 'g', apiError: true, payload: { groupId: null } }).groupId(), null);
  assert.equal(fromNative({ code: 89, message: 'q', apiError: true, payload: { throttleTimeMs: 7 } }).throttleTimeMs(), 7);
  // Without a payload, each field keeps Java's default.
  assert.deepEqual(fromNative({ code: 17, message: 'i', apiError: true }).invalidTopics(), null);
});
