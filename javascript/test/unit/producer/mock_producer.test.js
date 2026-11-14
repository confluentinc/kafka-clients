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

// Java's MockProducerTest, its non-transactional tests, over the FFI's mock
// (root CLAUDE.md, JavaScript Binding Conventions, Implementation over the
// FFI, Q62), then the binding's own: the call shapes, the error setters and
// the records the mock keeps.
//
// Not translated:
// - testPartitioner: it builds the mock with a Cluster, Java's form the
//   binding leaves out (Scope, Q30), so the mock's cluster is always empty.
// - The transaction tests (shouldInitTransactions, shouldBeginTransactions,
//   shouldCommitEmptyTransaction, the should...IfProducerGotFenced and
//   should...IfTransactionsAreEnabled tests, the offsets tests,
//   shouldThrowOnInitTransactionIfProducerIsClosed and the other
//   transaction calls' ...IfProducerIsClosed tests, and
//   shouldNotThrowOnFlushProducerIfProducerIsFenced): the binding leaves the
//   mock's transactions out, by request.

const test = require('node:test');
const assert = require('node:assert/strict');

const { ExecutionError, IllegalArgumentError, IllegalStateError } = require('@confluentinc/kafka-javascript');
const { KafkaError } = require('@confluentinc/kafka-javascript/common');
const { stringSerializer } = require('@confluentinc/kafka-javascript/common/serialization');
const { MockProducer, ProducerRecord, RecordMetadata } = require('@confluentinc/kafka-javascript/producer');

const topic = 'topic';
const record1 = new ProducerRecord({ topic, key: 'key1', value: 'value1' });
const record2 = new ProducerRecord({ topic, key: 'key2', value: 'value2' });

// Java's org.apache.kafka.test.MockSerializer: the data's bytes.
function mockSerializer() {
  return { serialize: ({ data }) => (data === null ? null : Buffer.from(String(data))) };
}

function buildMockProducer(autoComplete) {
  return new MockProducer({ autoComplete, partitioner: null, keySerializer: mockSerializer(), valueSerializer: mockSerializer() });
}

// A call made once the mock is closed completes through a closed mock, which
// no longer keeps the event loop alive: something must, until it settles.
async function holdingTheLoop(body) {
  const hold = setInterval(() => {}, 1000);
  try {
    return await body();
  } finally {
    clearInterval(hold);
  }
}

async function cleanup(producer) {
  if (!producer.closed()) await producer.close();
}

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
  assert.ok(metadata.isDone(), 'Send should be immediately complete');
  assert.equal(await isError(metadata), false, 'Send should be successful');
  assert.equal((await metadata.get()).offset, 0n, 'Offset should be 0');
  assert.equal((await metadata.get()).topic, topic);
  assert.deepEqual(producer.history(), [record1], 'We should have the record in our history');
  producer.clear();
  assert.equal(producer.history().length, 0, 'Clear should erase our history');
  await cleanup(producer);
});

test('testManualCompletion', async () => {
  const producer = buildMockProducer(false);
  const md1 = await producer.send(record1);
  assert.equal(md1.isDone(), false, "Send shouldn't have completed");
  const md2 = await producer.send(record2);
  assert.equal(md2.isDone(), false, "Send shouldn't have completed");
  assert.ok(producer.completeNext(), 'Complete the first request');
  assert.equal(await isError(md1), false, 'Request should be successful');
  assert.equal(md2.isDone(), false, 'Second request still incomplete');
  const e = new IllegalArgumentError('blah');
  assert.ok(producer.errorNext(e), 'Complete the second request with an error');
  await assert.rejects(md2.get(), (err) => {
    assert.ok(err instanceof ExecutionError);
    assert.equal(err.cause, e);
    return true;
  });
  assert.equal(producer.completeNext(), false, 'No more requests to complete');

  const md3 = await producer.send(record1);
  const md4 = await producer.send(record2);
  assert.ok(!md3.isDone() && !md4.isDone(), 'Requests should not be completed.');
  await producer.flush();
  assert.ok(md3.isDone() && md4.isDone(), 'Requests should be completed.');
  await cleanup(producer);
});

test('shouldThrowOnSendIfProducerIsClosed', async () => {
  const producer = buildMockProducer(true);
  await producer.close();
  // Java sends a null record, which its closed check reports first; a null
  // record is the binding's TypeError, checked before the call reaches the
  // core, so a record is sent.
  await holdingTheLoop(() =>
    assert.rejects(producer.send(record1), { constructor: IllegalStateError, message: 'MockProducer is already closed.' }),
  );
});

test('shouldThrowOnFlushProducerIfProducerIsClosed', async () => {
  const producer = buildMockProducer(true);
  await producer.close();
  await holdingTheLoop(() => assert.rejects(producer.flush(), { constructor: IllegalStateError, message: 'MockProducer is already closed.' }));
});

test('shouldThrowClassCastException', async () => {
  // Java's IntegerSerializer given a string throws ClassCastException; the
  // JavaScript serializer of a type throws its TypeError, which rejects the
  // send as it is.
  const customProducer = new MockProducer({
    autoComplete: true,
    partitioner: null,
    keySerializer: { serialize: ({ data }) => (typeof data === 'number' ? Buffer.alloc(4) : assert.fail('not reached')) },
    valueSerializer: stringSerializer(),
  });
  await assert.rejects(customProducer.send(new ProducerRecord({ topic, key: 1, value: 2 })), {
    constructor: TypeError,
    message: 'stringSerializer: data must be a string or null, not a number',
  });
  await customProducer.close();
});

test('shouldBeFlushedIfNoBufferedRecords', async () => {
  const producer = buildMockProducer(true);
  assert.ok(producer.flushed());
  await cleanup(producer);
});

test('shouldBeFlushedWithAutoCompleteIfBufferedRecords', async () => {
  const producer = buildMockProducer(true);
  await producer.send(record1);
  assert.ok(producer.flushed());
  await cleanup(producer);
});

test('shouldNotBeFlushedWithNoAutoCompleteIfBufferedRecords', async () => {
  const producer = buildMockProducer(false);
  await producer.send(record1);
  assert.equal(producer.flushed(), false);
  await cleanup(producer);
});

test('shouldNotBeFlushedAfterFlush', async () => {
  const producer = buildMockProducer(false);
  await producer.send(record1);
  await producer.flush();
  assert.ok(producer.flushed());
  await cleanup(producer);
});

test('testMetadataOnException', async () => {
  const producer = buildMockProducer(false);
  let seen = null;
  const metadata = await producer.send({
    record: record2,
    callback: ({ metadata: md, exception }) => {
      seen = { md, exception };
    },
  });
  const e = new IllegalArgumentError('dummy exception');
  assert.ok(producer.errorNext(e), 'Complete the second request with an error');
  assert.notEqual(seen, null);
  assert.ok(seen.md instanceof RecordMetadata);
  assert.equal(seen.md.offset, -1n, 'Invalid offset');
  assert.equal(seen.md.timestamp, -1n, 'Invalid timestamp');
  assert.equal(seen.md.serializedKeySize, -1, 'Invalid Serialized Key size');
  assert.equal(seen.md.serializedValueSize, -1, 'Invalid Serialized value size');
  assert.equal(seen.exception, e);
  await assert.rejects(metadata.get(), (err) => err instanceof ExecutionError && err.cause === e);
  await cleanup(producer);
});

// ---- the binding's own ---------------------------------------------------------

test("the constructor takes Java's () and (autoComplete, partitioner, keySerializer, valueSerializer)", async () => {
  const forms = 'MockProducer() takes one of (), (autoComplete, partitioner, keySerializer, valueSerializer)';
  assert.throws(() => new MockProducer({ autoComplete: true }), { constructor: IllegalArgumentError, message: `${forms}; got (autoComplete)` });
  assert.throws(() => new MockProducer({ autoComplete: 1, partitioner: null, keySerializer: null, valueSerializer: null }), {
    constructor: TypeError,
    message: 'autoComplete must be a boolean',
  });
  assert.throws(() => new MockProducer({ autoComplete: true, partitioner: null, keySerializer: stringSerializer, valueSerializer: null }), {
    constructor: IllegalArgumentError,
    message: "keySerializer: pass a serializer, not a function that makes one — did you forget '()'?",
  });
  // Java's MockProducer() does not complete sends on its own.
  const producer = new MockProducer();
  const future = await producer.send(record1);
  assert.equal(future.isDone(), false);
  assert.ok(producer.completeNext());
  assert.equal((await future.get()).offset, 0n);
  // The partitioner is never called: the mock's cluster is empty.
  const partitioned = new MockProducer({ autoComplete: true, partitioner: { partition: () => assert.fail('called') }, keySerializer: null, valueSerializer: null });
  assert.equal((await (await partitioned.send(record1)).get()).partition, 0);
  await partitioned.close();
  await producer.close();
});

test("history() returns the app's own records, which clear() drops, and the mock keeps any key and value", async () => {
  const producer = new MockProducer({ autoComplete: true, partitioner: null, keySerializer: null, valueSerializer: null });
  const object = { id: 1 };
  const records = [new ProducerRecord({ topic, key: object, value: [1, 2] }), new ProducerRecord({ topic, value: null }), record1];
  for (const record of records) await producer.send(record);
  const history = producer.history();
  assert.equal(history.length, 3);
  for (let i = 0; i < 3; i++) assert.equal(history[i], records[i]);
  assert.equal(history[0].key, object);
  // A new array each call, as Java copies its list.
  assert.notEqual(producer.history(), history);
  producer.clear();
  assert.deepEqual(producer.history(), []);
  await producer.send(record2);
  assert.deepEqual(producer.history(), [record2]);
  // A refused send is not kept.
  const refusal = new Error('no');
  producer.setSendException(refusal);
  await assert.rejects(producer.send(record1), (e) => e === refusal);
  assert.deepEqual(producer.history(), [record2]);
  await producer.close();
});

test('the serializers run on every send, with empty headers, and the record keeps its own headers writable', async () => {
  const seen = [];
  const serializer = (side) => ({
    serialize: async ({ topic: t, headers, data }) => {
      seen.push([side, t, data, headers.toArray().length]);
      headers.add({ key: 'by-serializer', value: null });
      return null;
    },
  });
  const producer = new MockProducer({ autoComplete: true, partitioner: null, keySerializer: serializer('key'), valueSerializer: serializer('value') });
  const record = new ProducerRecord({ topic, partition: null, key: 'k', value: 'v', headers: [] });
  await producer.send(record);
  assert.deepEqual(seen, [
    ['key', topic, 'k', 0],
    ['value', topic, 'v', 1],
  ]);
  // Java's mock serializes with fresh headers and does not seal the record's.
  assert.equal(record.headers.toArray().length, 0);
  record.headers.add({ key: 'later', value: null });
  await producer.close();
});

test('each error setter makes its call throw the very same value; null clears it', async () => {
  const producer = new MockProducer({ autoComplete: false, partitioner: null, keySerializer: null, valueSerializer: null });
  const sendError = new KafkaError('send');
  producer.setSendException(sendError);
  await assert.rejects(producer.send(record1), (e) => e === sendError);
  producer.setSendException({ sendException: null });
  await producer.send(record1);

  const flushError = new RangeError('flush');
  producer.setFlushException(flushError);
  await assert.rejects(producer.flush(), (e) => e === flushError);
  producer.setFlushException(null);
  await producer.flush();

  const partitionsForError = new IllegalStateError('partitionsFor');
  producer.setPartitionsForException({ partitionsForException: partitionsForError });
  await assert.rejects(producer.partitionsFor(topic), (e) => e === partitionsForError);
  producer.setPartitionsForException(null);
  assert.deepEqual(await producer.partitionsFor({ topic }), []);

  const closeError = new Error('close');
  producer.setCloseException(closeError);
  await assert.rejects(producer.close(), (e) => e === closeError);
  // A close that threw leaves the mock open, as in Java.
  assert.equal(producer.closed(), false);
  producer.setCloseException(null);
  await producer.close({ timeout: 0 });
  assert.ok(producer.closed());

  assert.throws(() => producer.setSendException({ sendException: 'boom' }), { constructor: TypeError, message: 'sendException must be an Error or null' });
  assert.throws(() => producer.setSendException('boom'), {
    constructor: IllegalArgumentError,
    message: 'setSendException() takes one of (sendException); got a string',
  });
  assert.throws(() => producer.setSendException({}), {
    constructor: IllegalArgumentError,
    message: 'setSendException() takes one of (sendException); got ()',
  });
});

test("errorNext takes Java's e, on its own or by name, and null completes the call", async () => {
  const producer = new MockProducer({ autoComplete: false, partitioner: null, keySerializer: null, valueSerializer: null });
  const first = await producer.send(record1);
  const second = await producer.send(record2);
  assert.ok(producer.errorNext(null));
  assert.equal((await first.get()).offset, 0n);
  const e = new Error('by name');
  assert.ok(producer.errorNext({ e }));
  await assert.rejects(second.get(), (err) => err.cause === e);
  assert.equal(producer.errorNext(e), false);
  assert.throws(() => producer.errorNext('x'), { constructor: IllegalArgumentError });
  assert.throws(() => producer.errorNext({ e: 'x' }), { constructor: TypeError, message: 'e must be an Error or null' });
  assert.throws(() => producer.completeNext(1), { constructor: IllegalArgumentError });
  assert.throws(() => producer.history(1), { constructor: TypeError });
  await producer.close();
});

test('a callback runs inside the call that completes its record, and metrics() is empty', async () => {
  const producer = new MockProducer({ autoComplete: true, partitioner: null, keySerializer: null, valueSerializer: null });
  let ran = false;
  const sending = producer.send({ record: record1, callback: () => (ran = true) });
  assert.ok(ran, 'the callback of an auto-completed send runs inside send()');
  await sending;
  assert.equal(producer.metrics().size(), 0);
  // A callback that throws is logged by the core; the future completes all the same.
  const future = await producer.send({
    record: record2,
    callback: () => {
      throw new Error('callback');
    },
  });
  assert.equal((await future.get()).offset, 1n);
  await producer[Symbol.asyncDispose]();
  assert.ok(producer.closed());
});
