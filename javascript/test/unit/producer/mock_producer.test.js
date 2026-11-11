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

// Java's MockProducerTest, its non-transactional cases. Not translated:
// testPartitioner, which builds the mock over a Cluster with partitions (the
// constructor taking a Cluster is not translated: its only extra parameter
// is a type the client does not translate); the transaction tests
// (shouldInitTransactions, ..., shouldThrowOnAbortTransactionIfProducerIsClosed,
// shouldNotThrowOnFlushProducerIfProducerIsFenced), as the mock's transaction
// members are not translated yet. A send is awaited where Java's returns its
// future: the mock's send() returns a Promise of it, as KafkaProducer's does.

const test = require('node:test');
const assert = require('node:assert/strict');

const { ExecutionError, IllegalArgumentError, IllegalStateError } = require('@confluentinc/kafka-javascript');
const { KafkaFuture } = require('@confluentinc/kafka-javascript/common');
const { MockProducer, ProducerRecord, RecordMetadata } = require('@confluentinc/kafka-javascript/producer');

const topic = 'topic';
const record1 = new ProducerRecord({ topic, key: 'key1', value: 'value1' });
const record2 = new ProducerRecord({ topic, key: 'key2', value: 'value2' });
const CLOSED = { constructor: IllegalStateError, message: 'MockProducer is already closed.' };

/** Java's test MockSerializer: a string as its bytes. */
function mockSerializer() {
  return { serialize: ({ data }) => (data === null ? null : Buffer.from(data)) };
}

function buildMockProducer(autoComplete) {
  return new MockProducer({ autoComplete, partitioner: null, keySerializer: mockSerializer(), valueSerializer: mockSerializer() });
}

/** Java's isError(future): whether its get() fails. */
async function isError(future) {
  try {
    await future.get();
    return false;
  } catch {
    return true;
  }
}

test('testAutoCompleteMock', async () => {
  const producer = buildMockProducer(true);
  const metadata = await producer.send(record1);
  assert.ok(metadata instanceof KafkaFuture);
  assert.equal(metadata.isDone(), true, 'Send should be immediately complete');
  assert.equal(await isError(metadata), false, 'Send should be successful');
  assert.equal((await metadata.get()).offset, 0, 'Offset should be 0');
  assert.equal((await metadata.get()).topic, topic);
  assert.deepEqual(producer.history(), [record1], 'We should have the record in our history');
  assert.equal(producer.history()[0], record1);
  producer.clear();
  assert.equal(producer.history().length, 0, 'Clear should erase our history');
  await producer.close();
});

test('testManualCompletion', async () => {
  const producer = buildMockProducer(false);
  const md1 = await producer.send(record1);
  assert.equal(md1.isDone(), false, "Send shouldn't have completed");
  const md2 = await producer.send(record2);
  assert.equal(md2.isDone(), false, "Send shouldn't have completed");
  assert.equal(producer.completeNext(), true, 'Complete the first request');
  assert.equal(await isError(md1), false, 'Request should be successful');
  assert.equal(md2.isDone(), false, 'Second request still incomplete');
  const e = new IllegalArgumentError('blah');
  assert.equal(producer.errorNext(e), true, 'Complete the second request with an error');
  await assert.rejects(md2.get(), (err) => err instanceof ExecutionError && err.cause === e);
  assert.equal(producer.completeNext(), false, 'No more requests to complete');

  const md3 = await producer.send(record1);
  const md4 = await producer.send(record2);
  assert.ok(!md3.isDone() && !md4.isDone(), 'Requests should not be completed.');
  await producer.flush();
  assert.ok(md3.isDone() && md4.isDone(), 'Requests should be completed.');
  await producer.close();
});

test('shouldThrowOnSendIfProducerIsClosed', async () => {
  const producer = buildMockProducer(true);
  await producer.close();
  await assert.rejects(producer.send(null), CLOSED);
});

test('shouldThrowOnFlushProducerIfProducerIsClosed', async () => {
  const producer = buildMockProducer(true);
  await producer.close();
  await assert.rejects(producer.flush(), CLOSED);
});

test('shouldThrowClassCastException', async () => {
  // Java's IntegerSerializer given a String throws ClassCastException; a
  // serializer's own error rejects send() as it is.
  const integerSerializer = {
    serialize: ({ data }) => {
      if (data !== null && typeof data !== 'number') throw new TypeError(`${typeof data} is not a number`);
      return data === null ? null : Buffer.alloc(4);
    },
  };
  const customProducer = new MockProducer({ autoComplete: true, partitioner: null, keySerializer: integerSerializer, valueSerializer: mockSerializer() });
  try {
    await assert.rejects(customProducer.send(new ProducerRecord({ topic, key: 'key1', value: 'value1' })), {
      constructor: TypeError,
      message: 'string is not a number',
    });
  } finally {
    await customProducer.close();
  }
});

test('shouldBeFlushedIfNoBufferedRecords', () => {
  const producer = buildMockProducer(true);
  assert.equal(producer.flushed(), true);
});

test('shouldBeFlushedWithAutoCompleteIfBufferedRecords', async () => {
  const producer = buildMockProducer(true);
  await producer.send(record1);
  assert.equal(producer.flushed(), true);
});

test('shouldNotBeFlushedWithNoAutoCompleteIfBufferedRecords', async () => {
  const producer = buildMockProducer(false);
  await producer.send(record1);
  assert.equal(producer.flushed(), false);
});

test('shouldNotBeFlushedAfterFlush', async () => {
  const producer = buildMockProducer(false);
  await producer.send(record1);
  await producer.flush();
  assert.equal(producer.flushed(), true);
});

test('testMetadataOnException', async () => {
  const producer = buildMockProducer(false);
  let seen = null;
  const metadata = await producer.send({
    record: record2,
    callback: ({ metadata: md, exception }) => {
      assert.notEqual(md, null);
      assert.ok(md instanceof RecordMetadata);
      assert.equal(md.offset, -1, 'Invalid offset');
      assert.equal(md.timestamp, -1, 'Invalid timestamp');
      assert.equal(md.serializedKeySize, -1, 'Invalid Serialized Key size');
      assert.equal(md.serializedValueSize, -1, 'Invalid Serialized value size');
      seen = exception;
    },
  });
  const e = new IllegalArgumentError('dummy exception');
  assert.equal(producer.errorNext(e), true, 'Complete the second request with an error');
  assert.equal(seen, e);
  await assert.rejects(metadata.get(), (err) => err instanceof ExecutionError && err.cause === e);
});

// The translation's own cases: Java's mock's behaviour the tests above do not
// reach, and the JavaScript forms.

test("a completed send's metadata and callback are Java's mock's: partition 0, offsets in order, no timestamp, sizes 0", async () => {
  const producer = buildMockProducer(true);
  const calls = [];
  const callback = ({ metadata, exception }) => calls.push([metadata, exception]);
  const first = await (await producer.send({ record: record1, callback })).get();
  const second = await (await producer.send({ record: new ProducerRecord({ topic, partition: 5, key: null, value: 'v' }), callback })).get();
  const other = await (await producer.send(new ProducerRecord({ topic: 'other', value: 'v' }))).get();
  assert.deepEqual(
    [first, second, other].map((m) => [m.topic, m.partition, m.offset, m.hasOffset, m.timestamp, m.hasTimestamp, m.serializedKeySize, m.serializedValueSize]),
    [
      [topic, 0, 0, true, -1, false, 0, 0],
      [topic, 0, 1, true, -1, false, 0, 0],
      ['other', 0, 0, true, -1, false, 0, 0],
    ],
  );
  assert.deepEqual(calls.map(([m, e]) => [m.offset, e]), [[0, null], [1, null]]);
});

test('the injected errors are thrown again as the same instance', async () => {
  const producer = buildMockProducer(true);
  const sendError = new Error('send');
  producer.sendException = sendError;
  await assert.rejects(producer.send(record1), (e) => e === sendError);
  // Checked before the record, as Java's send() checks it first.
  await assert.rejects(producer.send(null), (e) => e === sendError);
  producer.sendException = null;
  await assert.rejects(producer.send(null), { constructor: TypeError, message: 'record must not be null' });
  const flushError = new Error('flush');
  producer.flushException = flushError;
  await assert.rejects(producer.flush(), (e) => e === flushError);
  const closeError = new Error('close');
  producer.closeException = closeError;
  await assert.rejects(producer.close(), (e) => e === closeError);
  // A close that threw leaves the mock open.
  assert.equal(producer.closed(), false);
  producer.closeException = null;
  await producer.close();
  assert.equal(producer.closed(), true);
});

test('a callback that throws: the error propagates out of the completing call, and the future never completes', async () => {
  const boom = new Error('boom');
  const auto = buildMockProducer(true);
  await assert.rejects(auto.send({ record: record1, callback: () => { throw boom; } }), (e) => e === boom);
  assert.equal(auto.history().length, 1);
  const manual = buildMockProducer(false);
  const future = await manual.send({ record: record1, callback: () => { throw boom; } });
  assert.throws(() => manual.completeNext(), (e) => e === boom);
  assert.equal(future.isDone(), false);
});

test("a callback's Promise is awaited before the record's future completes, flush() included", async () => {
  const producer = buildMockProducer(false);
  let release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  const future = await producer.send({ record: record1, callback: () => gate });
  let flushed = false;
  const flushing = producer.flush().then(() => {
    flushed = true;
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(future.isDone(), false);
  assert.equal(flushed, false);
  release();
  await flushing;
  assert.equal(future.isDone(), true);
});

test('a serializer returning a Promise is awaited; a null serializer is not called', async () => {
  const order = [];
  const producer = new MockProducer({
    autoComplete: true,
    partitioner: null,
    keySerializer: null,
    valueSerializer: { serialize: ({ data, headers }) => (order.push([data, headers.toArray().length]), Promise.resolve(Buffer.from(data))) },
  });
  const sent = producer.send(new ProducerRecord({ topic, key: 3, value: 'v' }));
  order.push('returned');
  const future = await sent;
  assert.equal(future.isDone(), true);
  // The serializer gets new, empty headers, as Java's mock passes it.
  assert.deepEqual(order, [['v', 0], 'returned']);
  await assert.rejects(
    new MockProducer({ autoComplete: true, partitioner: null, keySerializer: { serialize: () => 'x' }, valueSerializer: null }).send(record1),
    { constructor: TypeError, message: 'keySerializer.serialize() must return a Uint8Array or null, not a string' },
  );
});

test("MockProducer() is Java's no-argument form: no auto-completion, no serializer", async () => {
  const producer = new MockProducer();
  const future = await producer.send(new ProducerRecord({ topic, key: 'k', value: { any: 'value' } }));
  assert.equal(future.isDone(), false);
  assert.equal(producer.flushed(), false);
  assert.equal(producer.completeNext(), true);
  assert.equal((await future.get()).offset, 0);
  await producer[Symbol.asyncDispose]();
  assert.equal(producer.closed(), true);
});

test("the constructor and the methods take Java's forms", async () => {
  const forms = 'MockProducer() takes one of (autoComplete, partitioner, keySerializer, valueSerializer), ()';
  assert.throws(() => new MockProducer({ autoComplete: true }), { constructor: IllegalArgumentError, message: `${forms}; got (autoComplete)` });
  assert.throws(() => new MockProducer({ cluster: null, autoComplete: true, partitioner: null, keySerializer: null, valueSerializer: null }), {
    constructor: IllegalArgumentError,
    message: `${forms}; got (autoComplete, partitioner, keySerializer, valueSerializer, cluster)`,
  });
  assert.throws(() => new MockProducer({ autoComplete: 'yes', partitioner: null, keySerializer: null, valueSerializer: null }), {
    constructor: TypeError,
    message: 'autoComplete must be a boolean',
  });
  assert.throws(() => new MockProducer({ autoComplete: true, partitioner: null, keySerializer: mockSerializer, valueSerializer: null }), {
    constructor: IllegalArgumentError,
    message: "keySerializer: pass a serializer, not a function that makes one — did you forget '()'?",
  });
  const producer = buildMockProducer(false);
  await assert.rejects(producer.send(record1, () => {}), {
    constructor: IllegalArgumentError,
    message: 'send() takes one of (record), (record, callback); got 2 arguments, not one object of named parameters',
  });
  await assert.rejects(producer.send({ record: record1, callback: 1 }), {
    constructor: TypeError,
    message: 'callback must be a function ({ metadata, exception }) => void',
  });
  await assert.rejects(producer.flush(1), { constructor: IllegalArgumentError, message: 'flush() takes one of (); got a number' });
  assert.equal(producer.errorNext({ e: null }), false);
  assert.throws(() => producer.errorNext(), { constructor: IllegalArgumentError, message: 'errorNext() takes one of (e); got ()' });
  assert.throws(() => producer.closed(1), { constructor: TypeError, message: 'closed() only reads: it takes no argument' });
  assert.throws(() => producer.history(1), { constructor: TypeError, message: 'history() only reads: it takes no argument' });
  // Java's mock does not read close's timeout.
  await producer.close({ timeout: 5000 });
  await producer.close(0);
  await assert.rejects(producer.close('soon'), { constructor: IllegalArgumentError, message: 'close() takes one of (), (timeout); got a string' });
});
