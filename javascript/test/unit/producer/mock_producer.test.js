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

const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

const { ExecutionError, IllegalArgumentError, IllegalStateError } = require('@confluentinc/kafka-javascript');
const { KafkaError } = require('@confluentinc/kafka-javascript/common');
const { RecordTooLargeError } = require('@confluentinc/kafka-javascript/common/errors');
const { stringSerializer } = require('@confluentinc/kafka-javascript/common/serialization');
const { MockProducer, ProducerRecord, RecordMetadata } = require('@confluentinc/kafka-javascript/producer');

/** @typedef {import('@confluentinc/kafka-javascript/common').KafkaFuture<unknown>} AnyFuture */
/** @template T @typedef {import('@confluentinc/kafka-javascript/common/serialization').Serializer<T>} Serializer */

const topic = 'topic';
const record1 = new ProducerRecord({ topic, key: 'key1', value: 'value1' });
const record2 = new ProducerRecord({ topic, key: 'key2', value: 'value2' });

// Java's org.apache.kafka.test.MockSerializer: the data's bytes.
/** @returns {Serializer<unknown>} */
function mockSerializer() {
  return { serialize: ({ data }) => (data === null ? null : Buffer.from(String(data))) };
}

/** @param {boolean} autoComplete */
function buildMockProducer(autoComplete) {
  return new MockProducer({ autoComplete, partitioner: null, keySerializer: mockSerializer(), valueSerializer: mockSerializer() });
}

// A call made once the mock is closed completes through a closed mock, which
// no longer keeps the event loop alive: something must, until it settles.
/**
 * @template T
 * @param {() => Promise<T>} body
 * @returns {Promise<T>}
 */
async function holdingTheLoop(body) {
  const hold = setInterval(() => {}, 1000);
  try {
    return await body();
  } finally {
    clearInterval(hold);
  }
}

/** @param {MockProducer<unknown, unknown>} producer */
async function cleanup(producer) {
  if (!producer.closed()) await producer.close();
}

/** @param {AnyFuture} future */
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

// Java's form of shouldThrowOnSendIfProducerIsClosed: send(null) on a closed
// mock. Java's mock checks closed first (IllegalStateException "MockProducer
// is already closed."); the JS mock refuses the null record first, a recorded
// deviation (MockProducer.send's remarks, javascript/CLAUDE.md: the binding
// keeps no closed state, and its own TypeScript check of the record runs
// before the call reaches the core's mock, which holds the closed state).
// The test above covers the closed error with a record.
test("shouldThrowOnSendIfProducerIsClosed, Java's null record: the null check comes first (deviation)", async () => {
  const producer = buildMockProducer(true);
  await producer.close();
  await holdingTheLoop(() =>
    // @ts-expect-error a null record: Java's test sends one, the binding's null check throws TypeError
    assert.rejects(producer.send(null), { constructor: TypeError, message: 'record must not be null' }),
  );
});

test('shouldThrowOnFlushProducerIfProducerIsClosed', async () => {
  const producer = buildMockProducer(true);
  await producer.close();
  await holdingTheLoop(() =>
    assert.rejects(producer.flush(), { constructor: IllegalStateError, message: 'MockProducer is already closed.' }),
  );
});

test('shouldThrowClassCastException', async () => {
  // Java's IntegerSerializer (the key serializer) given the string "key1"
  // throws ClassCastException; the JavaScript serializer of a type throws its
  // TypeError, which rejects the send as it is. The key serializer runs first,
  // so the value serializer is never reached.
  /** @type {unknown[]} */
  const valueCalls = [];
  const valueSerializer = stringSerializer();
  const customProducer = new MockProducer({
    autoComplete: true,
    partitioner: null,
    keySerializer: {
      serialize: ({ data }) => {
        if (typeof data !== 'number') throw new TypeError(`integerSerializer: data must be a number or null, not a ${typeof data}`);
        return Buffer.alloc(4);
      },
    },
    valueSerializer: {
      serialize: (args) => {
        valueCalls.push(args.data);
        return valueSerializer.serialize(args);
      },
    },
  });
  await assert.rejects(customProducer.send(new ProducerRecord({ topic, key: 'key1', value: 'value1' })), {
    constructor: TypeError,
    message: 'integerSerializer: data must be a number or null, not a string',
  });
  assert.deepEqual(valueCalls, []);
  // The value serializer's own type check, as before: a number for the string serializer.
  const stringValued = new MockProducer({ autoComplete: true, partitioner: null, keySerializer: null, valueSerializer });
  // @ts-expect-error a number for the string serializer: its type check throws TypeError
  await assert.rejects(stringValued.send(new ProducerRecord({ topic, key: null, value: 2 })), {
    constructor: TypeError,
    message: 'stringSerializer: data must be a string or null, not a number',
  });
  await customProducer.close();
  await stringValued.close();
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
  let seen = /** @type {{ md: RecordMetadata, exception: Error | null } | null} */ (null);
  const metadata = await producer.send({
    record: record2,
    callback: ({ metadata: md, exception }) => {
      seen = { md, exception };
    },
  });
  const e = new IllegalArgumentError('dummy exception');
  assert.ok(producer.errorNext(e), 'Complete the second request with an error');
  assert.notEqual(seen, null);
  assert.ok(seen);
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
  // @ts-expect-error a form missing keys: the form check throws IllegalArgumentError
  assert.throws(() => new MockProducer({ autoComplete: true }), {
    constructor: IllegalArgumentError,
    message: `${forms}; got (autoComplete)`,
  });
  // @ts-expect-error a number for autoComplete: the type check throws TypeError
  assert.throws(() => new MockProducer({ autoComplete: 1, partitioner: null, keySerializer: null, valueSerializer: null }), {
    constructor: TypeError,
    message: 'autoComplete must be a boolean',
  });
  // @ts-expect-error the factory for a serializer: the serde check throws IllegalArgumentError
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
  const partitioned = new MockProducer({
    autoComplete: true,
    partitioner: { partition: () => assert.fail('called') },
    keySerializer: null,
    valueSerializer: null,
  });
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

test('the serializers run on every send, each with fresh empty headers, and the record keeps its own headers writable', async () => {
  /** @type {unknown[][]} */
  const seen = [];
  /**
   * @param {string} side
   * @returns {Serializer<string>}
   */
  const serializer = (side) => ({
    serialize: async ({ topic: t, headers, data }) => {
      seen.push([side, t, data, headers.toArray().length]);
      headers.add({ key: 'by-serializer', value: null });
      return null;
    },
  });
  const producer = new MockProducer({
    autoComplete: true,
    partitioner: null,
    keySerializer: serializer('key'),
    valueSerializer: serializer('value'),
  });
  const record = new ProducerRecord({ topic, partition: null, key: 'k', value: 'v', headers: [] });
  await producer.send(record);
  // Java's mock gives each serializer a new RecordHeaders: the value
  // serializer does not see the header the key serializer added.
  assert.deepEqual(seen, [
    ['key', topic, 'k', 0],
    ['value', topic, 'v', 0],
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

  // @ts-expect-error a string for an Error: the type check throws TypeError
  assert.throws(() => producer.setSendException({ sendException: 'boom' }), {
    constructor: TypeError,
    message: 'sendException must be an Error or null',
  });
  // @ts-expect-error a string on its own: the form check throws IllegalArgumentError
  assert.throws(() => producer.setSendException('boom'), {
    constructor: IllegalArgumentError,
    message: 'setSendException() takes one of (sendException); got a string',
  });
  // @ts-expect-error a missing key: the form check throws IllegalArgumentError
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
  await assert.rejects(second.get(), (err) => /** @type {Error} */ (err).cause === e);
  assert.equal(producer.errorNext(e), false);
  // @ts-expect-error a string on its own: the form check throws IllegalArgumentError
  assert.throws(() => producer.errorNext('x'), { constructor: IllegalArgumentError });
  // @ts-expect-error a string for an Error: the type check throws TypeError
  assert.throws(() => producer.errorNext({ e: 'x' }), { constructor: TypeError, message: 'e must be an Error or null' });
  // @ts-expect-error an argument to a method that takes none: the form check throws IllegalArgumentError
  assert.throws(() => producer.completeNext(1), { constructor: IllegalArgumentError });
  // @ts-expect-error an argument to a method that takes none: the reader throws TypeError
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
  await producer[Symbol.asyncDispose]();
  assert.ok(producer.closed());
});

test("a callback that throws makes the completing call throw it, as Java's mock lets it propagate", async () => {
  const thrown = new Error('callback');
  const throwing = () => {
    throw thrown;
  };
  // send() with autoComplete rejects with it.
  const auto = new MockProducer({ autoComplete: true, partitioner: null, keySerializer: null, valueSerializer: null });
  await assert.rejects(auto.send({ record: record1, callback: throwing }), (e) => e === thrown);
  // The record was sent: Java's history has it.
  assert.equal(auto.history().length, 1);
  await auto.send(record2);
  await auto.close();
  // completeNext() and errorNext() throw it; flush() rejects with it.
  const manual = new MockProducer();
  const futures = [];
  for (let i = 0; i < 3; i++) futures.push(await manual.send({ record: record1, callback: throwing }));
  assert.throws(
    () => manual.completeNext(),
    (e) => e === thrown,
  );
  assert.throws(
    () => manual.errorNext(new KafkaError('next')),
    (e) => e === thrown,
  );
  await assert.rejects(manual.flush(), (e) => e === thrown);
  // The calls after it are not affected.
  await manual.send(record2);
  assert.equal(manual.completeNext(), true);
  // The core's mock completes the future all the same (a deviation, the mock
  // being the core's: Java's leaves it incomplete).
  assert.equal((await futures[0].get()).offset, 0n);
  await manual.close();
});

test("a serializer's error rejects the mock's send() as it is, an ApiError included: Java's mock serializes with no try", async () => {
  for (const isKey of [true, false]) {
    for (const async of [false, true]) {
      for (const error of [new RecordTooLargeError('too large'), new TypeError('wrong type'), new KafkaError('other')]) {
        const failing = {
          serialize: () => {
            if (async) return Promise.reject(error);
            throw error;
          },
        };
        const producer = new MockProducer({
          autoComplete: true,
          partitioner: null,
          keySerializer: isKey ? failing : mockSerializer(),
          valueSerializer: isKey ? mockSerializer() : failing,
        });
        let ran = false;
        await assert.rejects(producer.send({ record: record1, callback: () => (ran = true) }), (e) => e === error);
        assert.equal(ran, false);
        assert.deepEqual(producer.history(), []);
        await producer.close();
      }
    }
  }
});

// A callback's Promise that rejects is the callback's error, as a throw is
// (root conventions, Threads and callbacks). send() with autoComplete and
// flush() reject with it; completeNext() and errorNext(), being synchronous,
// keep it, oldest first, for the next send(), flush() or close(), once
// (Python's AsyncMockProducer). flush() and close() wait for every callback
// Promise already started. The next call is made at once, with no
// event-loop turn in between.

test("send() with autoComplete rejects with its callback Promise's rejection, and nothing is logged", async () => {
  const boom = new Error('boom');
  const producer = buildMockProducer(true);
  /** @type {string[]} */
  const warnings = [];
  const onWarning = (/** @type {Error} */ w) => warnings.push(w.message);
  process.on('warning', onWarning);
  try {
    await assert.rejects(producer.send({ record: record1, callback: () => Promise.reject(boom) }), (e) => e === boom);
    // Raised once: the next calls are not affected.
    await producer.send(record2);
    await producer.flush();
    await producer.close();
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(warnings, []);
  } finally {
    process.off('warning', onWarning);
  }
});

test("send() with autoComplete waits for its callback's Promise", async () => {
  const producer = buildMockProducer(true);
  let done = false;
  const future = await producer.send({
    record: record1,
    callback: () =>
      new Promise((resolve) => setTimeout(resolve, 20)).then(() => {
        done = true;
      }),
  });
  assert.equal(done, true);
  assert.equal(future.isDone(), true);
  await producer.close();
});

test("flush() rejects with a callback Promise's rejection, once it settled", async () => {
  const boom = new Error('boom');
  const producer = buildMockProducer(false);
  const future = await producer.send({ record: record1, callback: () => Promise.reject(boom) });
  await assert.rejects(producer.flush(), (e) => e === boom);
  // The core's mock completes the record's future all the same, as for a
  // throw (the recorded deviation: Java's leaves it incomplete).
  assert.equal(future.isDone(), true);
  // Raised once.
  await producer.flush();
  await producer.close();
});

test("completeNext() and errorNext() keep a callback Promise's rejection for the next send()", async () => {
  for (const complete of [
    (/** @type {MockProducer<unknown, unknown>} */ p) => p.completeNext(),
    (/** @type {MockProducer<unknown, unknown>} */ p) => p.errorNext(new KafkaError('next')),
  ]) {
    const boom = new Error('boom');
    const producer = buildMockProducer(false);
    await producer.send({ record: record1, callback: () => Promise.reject(boom) });
    assert.equal(complete(producer), true);
    await new Promise((resolve) => setImmediate(resolve));
    await assert.rejects(producer.send(record2), (e) => e === boom);
    // Raised once.
    await producer.send(record2);
    await producer.close();
  }
});

test('completeNext(); await close() waits for the callback and rejects with its error', async () => {
  const boom = new Error('boom');
  const producer = buildMockProducer(false);
  await producer.send({ record: record1, callback: () => Promise.reject(boom) });
  producer.completeNext();
  await assert.rejects(producer.close(), (e) => e === boom);
  assert.equal(producer.closed(), true);
  // [Symbol.asyncDispose], the end of an `await using` scope, waits the same way.
  const disposed = buildMockProducer(false);
  await disposed.send({ record: record1, callback: () => Promise.reject(boom) });
  disposed.completeNext();
  await assert.rejects(disposed[Symbol.asyncDispose](), (e) => e === boom);
});

test('completeNext(); await flush() waits for the callback and rejects with its error', async () => {
  const boom = new Error('boom');
  const producer = buildMockProducer(false);
  const future = await producer.send({ record: record1, callback: () => Promise.reject(boom) });
  producer.completeNext();
  await assert.rejects(producer.flush(), (e) => e === boom);
  assert.equal(future.isDone(), true);
  // Raised once.
  await producer.flush();
  await producer.close();
});

test('two failing callbacks: both errors are raised, oldest first, one per later call', async () => {
  const first = new Error('first');
  const second = new Error('second');
  const producer = buildMockProducer(false);
  await producer.send({ record: record1, callback: () => Promise.reject(first) });
  await producer.send({ record: record2, callback: () => Promise.reject(second) });
  producer.completeNext();
  producer.completeNext();
  await assert.rejects(producer.flush(), (e) => e === first);
  await assert.rejects(producer.send(record1), (e) => e === second);
  await producer.flush();
  await producer.close();
});

test('two rejections inside one flush(): it waits for every callback, rejects with the oldest (the first to reject), and keeps the other', async () => {
  const first = new Error('first');
  const second = new Error('second');
  const producer = buildMockProducer(false);
  /** @type {() => void} */
  let release = () => {};
  const gate = new Promise((resolve) => {
    release = () => resolve(undefined);
  });
  // The first callback rejects late, the second at once, a third resolves late.
  await producer.send({ record: record1, callback: () => gate.then(() => Promise.reject(first)) });
  await producer.send({ record: record2, callback: () => Promise.reject(second) });
  let slowDone = false;
  const slow = await producer.send({
    record: record1,
    callback: () =>
      new Promise((resolve) => setTimeout(resolve, 30)).then(() => {
        slowDone = true;
      }),
  });
  let settled = false;
  const flushing = producer.flush().finally(() => {
    settled = true;
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(settled, false, 'flush() waits for the callbacks it started');
  release();
  await assert.rejects(flushing, (e) => e === second);
  assert.equal(slowDone, true);
  assert.equal(slow.isDone(), true);
  await assert.rejects(producer.close(), (e) => e === first);
});

test('flush() over a fast rejection and a slow resolution settles only after both', async () => {
  const boom = new Error('boom');
  const producer = buildMockProducer(false);
  await producer.send({ record: record1, callback: () => Promise.reject(boom) });
  let slowDone = false;
  await producer.send({
    record: record2,
    callback: () =>
      new Promise((resolve) => setTimeout(resolve, 30)).then(() => {
        slowDone = true;
      }),
  });
  const started = Date.now();
  await assert.rejects(producer.flush(), (e) => e === boom);
  assert.equal(slowDone, true);
  assert.ok(Date.now() - started >= 25, 'flush() waited for the slow callback');
  await producer.close();
});

// Blocks the JS thread, with no event-loop turn, until the core's mock has
// completed every waiting record on its own task (its flush queues the
// deliveries for the next drain), or 5 s passed.
function blockUntilFlushed(/** @type {MockProducer<unknown, unknown>} */ producer) {
  const cell = new Int32Array(new SharedArrayBuffer(4));
  for (let waited = 0; !producer.flushed() && waited < 5000; waited += 5) Atomics.wait(cell, 0, 0, 5);
  assert.equal(producer.flushed(), true, "the core's mock flushed within 5 s");
}

test('a callback that awaits a nested send() before it fails rejects its own send(), not a later call', async () => {
  for (const nestedFirst of [true, false]) {
    const outerError = new Error('outer');
    const producer = buildMockProducer(true);
    /** @type {Promise<unknown> | null} */
    let nested = null;
    const outer = producer.send({
      record: record1,
      callback: nestedFirst
        ? async () => {
            await producer.send(record2);
            throw outerError;
          }
        : () => {
            // A synchronous throw after a nested send.
            nested = producer.send(record2);
            nested.catch(() => {});
            throw outerError;
          },
    });
    outer.catch(() => {});
    await assert.rejects(outer, (e) => e === outerError);
    if (nested !== null) await nested;
    // Nothing was kept for an unrelated call.
    await producer.send(record2);
    await producer.flush();
    await producer.close();
    assert.equal(producer.history().length, 3);
  }
});

test("a send() from a callback's synchronous part rejects with its own callback's throw or rejection, as Java's nested send() throws it", async () => {
  // Java's MockProducer#send with autoComplete runs Completion#complete, which
  // calls onCompletion with no try, inside the nested send(). The callback
  // queue does not nest, so the nested record's callback runs once the outer
  // callback returned; the nested send() waits for it.
  for (const shape of ['throws', 'rejects']) {
    for (const outerAwaits of [true, false]) {
      const nestedError = new Error(`nested callback ${shape}`);
      const nestedCallback =
        shape === 'throws'
          ? () => {
              throw nestedError;
            }
          : () => Promise.reject(nestedError);
      const producer = buildMockProducer(true);
      /** @type {Promise<unknown> | null} */
      let nested = null;
      /** @type {unknown} */
      let caught = null;
      await producer.send({
        record: record1,
        callback: outerAwaits
          ? async () => {
              try {
                await producer.send({ record: record2, callback: nestedCallback });
              } catch (e) {
                caught = e;
              }
            }
          : () => {
              nested = producer.send({ record: record2, callback: nestedCallback });
              nested.catch(() => {});
            },
      });
      if (nested !== null) await assert.rejects(nested, (e) => e === nestedError);
      else assert.equal(caught, nestedError, `${shape}: the awaited nested send() rejects with its own error`);
      // No other call sees it.
      await producer.send(record2);
      await producer.flush();
      await producer.close();
      assert.equal(producer.history().length, 3);
    }
  }
});

test("a send() whose drain runs a pending flush()'s callbacks leaves their errors to the flush() and later calls", async () => {
  for (const throws of [false, true]) {
    const first = new Error('first');
    const second = new Error('second');
    const producer = buildMockProducer(false);
    const fail = (/** @type {Error} */ e) =>
      throws
        ? () => {
            throw e;
          }
        : () => Promise.reject(e);
    await producer.send({ record: record1, callback: fail(first) });
    await producer.send({ record: record2, callback: fail(second) });
    const flushing = producer.flush();
    flushing.catch(() => {});
    // The core's mock completes the records on its own task; the send()
    // below then drains their deliveries in its own hand-over.
    blockUntilFlushed(producer);
    const sending = producer.send(record1);
    sending.catch(() => {});
    await assert.rejects(flushing, (e) => e === first);
    await sending;
    if (throws) {
      // A throw is the completing call's only (the recorded deviation of
      // flush()): nothing is kept.
      await producer.close();
    } else {
      // The other rejection is kept for the next call.
      await assert.rejects(producer.send(record2), (e) => e === second);
      await producer.close();
    }
  }
});

test('Promise.allSettled([flush(), send()]): each error reaches its own caller', async () => {
  // One mock for every round (each mock holds its runtime's threads until it
  // is collected): a round leaves nothing kept, as the last assertions show.
  // The send() raced the flush's drain in about one round in ten before the
  // fix.
  const producer = buildMockProducer(false);
  for (let round = 0; round < 500; round++) {
    const boom = new Error(`boom ${round}`);
    await producer.send({ record: record1, callback: () => Promise.reject(boom) });
    const [flushed, sent] = await Promise.allSettled([producer.flush(), producer.send(record2)]);
    assert.equal(flushed.status, 'rejected', `round ${round}`);
    assert.equal(/** @type {PromiseRejectedResult} */ (flushed).reason, boom);
    assert.equal(sent.status, 'fulfilled', `round ${round}`);
    // record2 waits: flush it, nothing is kept.
    await producer.flush();
  }
  await producer.close();
});

test('await close() inside an async callback, before its first await, settles, and so does the send() waiting for that callback', async () => {
  for (const dispose of [false, true]) {
    const close = (/** @type {MockProducer<unknown, unknown>} */ p) => (dispose ? p[Symbol.asyncDispose]() : p.close());
    // An auto-completed send() waits for its callback, which awaits close().
    const auto = buildMockProducer(true);
    let closedInside = false;
    await auto.send({
      record: record1,
      callback: async () => {
        await close(auto);
        closedInside = auto.closed();
      },
    });
    assert.equal(closedInside, true);
    // completeNext() runs the callback; the close() it awaits settles.
    const manual = buildMockProducer(false);
    /** @type {Promise<void>[]} */
    const inner = [];
    await manual.send({
      record: record1,
      callback: async () => {
        const closing = close(manual);
        inner.push(closing);
        await closing;
      },
    });
    manual.completeNext();
    assert.equal(inner.length, 1);
    await inner[0];
    assert.equal(manual.closed(), true);
  }
});

test('flush() with a kept error first flushes: it completes the waiting records and waits for their callbacks, then rejects with the oldest', async () => {
  const boom = new Error('boom');
  const producer = buildMockProducer(false);
  await producer.send({ record: record1, callback: () => Promise.reject(boom) });
  let slowDone = false;
  await producer.send({
    record: record2,
    callback: () =>
      new Promise((resolve) => setTimeout(resolve, 30)).then(() => {
        slowDone = true;
      }),
  });
  let lastDone = false;
  const waiting = await producer.send({
    record: record1,
    callback: () => {
      lastDone = true;
    },
  });
  // The first callback rejects at once and its error is kept; the second
  // runs 30 ms.
  producer.completeNext();
  await new Promise((resolve) => setImmediate(resolve));
  producer.completeNext();
  await assert.rejects(producer.flush(), (e) => e === boom);
  assert.equal(slowDone, true);
  assert.equal(lastDone, true);
  assert.equal(waiting.isDone(), true);
  assert.equal(producer.flushed(), true);
  await producer.close();
});

test('the kept callback errors raise no unhandled rejection under --unhandled-rejections=strict', () => {
  const r = spawnSync(
    process.execPath,
    [
      '--unhandled-rejections=strict',
      '--no-warnings',
      '-e',
      `
    const { MockProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    (async () => {
      const p = new MockProducer();
      const record = new ProducerRecord({ topic: 't', value: Buffer.from('v') });
      for (let i = 0; i < 3; i++) await p.send({ record, callback: () => Promise.reject(new Error('cb' + i)) });
      p.completeNext();
      p.errorNext(new Error('delivery'));
      await p.flush().catch(() => {});
      await new Promise((resolve) => setTimeout(resolve, 20));
      // One error kept, never raised: still no unhandled rejection.
      await p.close().catch(() => {});
      console.log('done');
    })();
  `,
    ],
    { cwd: path.join(__dirname, '..', '..', '..'), encoding: 'utf8', timeout: 30000 },
  );
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout.trim(), 'done');
});

test('flush() completes every waiting record before it rejects with the first throw (a recorded deviation)', async () => {
  const mock = new MockProducer();
  /** @type {string[]} */
  const ran = [];
  const futures = [];
  for (const name of ['cb0', 'cb1', 'cb2']) {
    futures.push(
      await mock.send({
        record: record1,
        callback: () => {
          ran.push(name);
          throw new Error(name);
        },
      }),
    );
  }
  await assert.rejects(mock.flush(), { message: 'cb0' });
  // Java's flush() stops at cb0, leaving two records waiting; the core's mock completes them all.
  assert.deepEqual(ran, ['cb0', 'cb1', 'cb2']);
  assert.deepEqual(
    futures.map((f) => f.isDone()),
    [true, true, true],
  );
  assert.equal(mock.completeNext(), false);
  await mock.close();
});

test("Java's empty-cluster mock sends to partition 0 whatever the record says, with its offsets", async () => {
  const producer = new MockProducer({ autoComplete: false, partitioner: null, keySerializer: null, valueSerializer: null });
  const explicit = new ProducerRecord({ topic, partition: 3, key: null, value: Buffer.from('a') });
  const first = await producer.send(explicit);
  const second = await producer.send(new ProducerRecord({ topic, value: Buffer.from('b') }));
  let errorPartition = null;
  await producer.send({
    record: new ProducerRecord({ topic, partition: 5, key: null, value: Buffer.from('c') }),
    callback: ({ metadata }) => (errorPartition = metadata.partition),
  });
  assert.equal(producer.completeNext(), true);
  assert.equal(producer.completeNext(), true);
  assert.equal(producer.errorNext(new KafkaError('e')), true);
  const [m1, m2] = [await first.get(), await second.get()];
  assert.deepEqual([m1.partition, m1.offset, m2.partition, m2.offset], [0, 0n, 0, 1n]);
  assert.equal(errorPartition, 0);
  // history() is the app's own record, its partition as given.
  assert.equal(producer.history()[0], explicit);
  assert.equal(producer.history()[0].partition, 3);
  await producer.close();
});

test('a refused send is not in history() and keeps nothing: its callback never runs', async () => {
  const producer = new MockProducer();
  const error = new KafkaError('refused');
  producer.setSendException(error);
  let ran = false;
  await assert.rejects(producer.send({ record: record1, callback: () => (ran = true) }), (e) => e === error);
  assert.equal(ran, false);
  assert.deepEqual(producer.history(), []);
  await producer.close();
});

/**
 * @param {string} script
 * @param {number} [timeout]
 */
function runNode(script, timeout = 60000) {
  return spawnSync(process.execPath, ['--expose-gc', '--no-warnings', '-e', script], {
    cwd: path.join(__dirname, '..', '..', '..'),
    encoding: 'utf8',
    timeout,
  });
}

// A closed mock nothing references is collected and its native mock
// destroyed (its threads gone), whatever sends were still waiting at its
// close or dropped by clear(): the addon holds a waiting send's delivery
// function weakly, the mock strongly (native/producer.c, SendCtx.mock_pending).
(!['linux', 'darwin'].includes(process.platform) ? test.skip : test)(
  'a closed mock is destroyed when collected, a send pending at close() or dropped by clear() included',
  () => {
    const r = runNode(
      `
    const { execSync } = require('node:child_process');
    const fs = require('node:fs');
    const { MockProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    const threads = () =>
      process.platform === 'linux'
        ? fs.readdirSync('/proc/self/task').length
        : execSync('ps -M -p ' + process.pid).toString().trim().split('\\n').length - 1;
    const collected = [];
    const registry = new FinalizationRegistry((name) => collected.push(name));
    const settle = async () => {
      for (let i = 0; i < 10; i++) {
        global.gc();
        await new Promise((r) => setTimeout(r, 100));
      }
    };
    const record = () => new ProducerRecord({ topic: 't', value: Buffer.from('v') });
    const CASES = {
      'pending at close': async (m) => {
        await m.send({ record: record(), callback: () => {} });
        await m.close();
      },
      'dropped by clear': async (m) => {
        await m.send({ record: record(), callback: () => {} });
        await m.send(record());
        m.clear();
        await m.close();
      },
    };
    async function one(name, run) {
      const m = new MockProducer();
      registry.register(m, name);
      await run(m);
    }
    (async () => {
      const hold = setInterval(() => {}, 1000);
      await one('warm-up', async (m) => m.close());
      await settle();
      for (const [name, run] of Object.entries(CASES)) {
        const before = threads();
        for (let i = 0; i < 5; i++) await one(name, run);
        await settle();
        console.log(name, collected.filter((c) => c === name).length, threads() - before);
      }
      // The constraint: a completeNext() after close() still runs the callback, as Java's does.
      const m = new MockProducer();
      let ran = false;
      await m.send({ record: record(), callback: () => (ran = true) });
      await m.close();
      global.gc();
      console.log('completeNext after close', m.completeNext(), ran);
      clearInterval(hold);
    })();
  `,
      120000,
    );
    assert.equal(r.status, 0, r.stderr);
    const lines = r.stdout.trim().split('\n');
    assert.equal(lines[2], 'completeNext after close true true', r.stdout);
    const counts = lines.slice(0, 2).map((l) => /^(.*) (\d+) (-?\d+)$/.exec(l));
    assert.deepEqual(
      counts.map((m) => m && [m[1], m[2]]),
      [
        ['pending at close', '5'],
        ['dropped by clear', '5'],
      ],
      r.stdout,
    );
    // About 18 threads per live mock: five leaked mocks would leave about 90.
    // The deepEqual above has checked that both lines matched.
    for (const m of /** @type {RegExpExecArray[]} */ (counts)) assert.ok(Number(m[3]) < 10, `${m[1]}: ${m[3]} threads left over`);
  },
);

// An open mock keeps neither the records it refused nor the errors it was
// given once the core is done with them (native/producer.c,
// send_on_handover and injected_errors_sweep).
test('an open mock releases a refused record at once, and an injected error once the core dropped it', () => {
  const r = runNode(`
    const { MockProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    const { KafkaError } = require('@confluentinc/kafka-javascript/common');
    const collected = { record: 0, error: 0 };
    const registry = new FinalizationRegistry((what) => collected[what]++);
    const settle = async () => {
      for (let i = 0; i < 10; i++) {
        global.gc();
        await new Promise((r) => setTimeout(r, 50));
      }
    };
    // In functions of their own, so no frame keeps the last value.
    async function refuse(m) {
      const record = new ProducerRecord({ topic: 't', value: Buffer.alloc(16) });
      registry.register(record, 'record');
      await m.send(record).catch(() => {});
    }
    async function inject(m, i) {
      await m.send(new ProducerRecord({ topic: 't', value: Buffer.alloc(16) }));
      const error = new KafkaError('next ' + i);
      registry.register(error, 'error');
      m.errorNext(error);
    }
    (async () => {
      const m = new MockProducer();
      m.setSendException(new KafkaError('refused'));
      for (let i = 0; i < 20; i++) await refuse(m);
      m.setSendException(null);
      for (let i = 0; i < 20; i++) await inject(m, i);
      // A mock call sweeps the errors the core no longer holds.
      m.history();
      m.clear();
      await settle();
      console.log(collected.record, collected.error);
      await m.close();
    })();
  `);
  assert.equal(r.status, 0, r.stderr);
  const [records, errors] = r.stdout.trim().split(' ').map(Number);
  assert.equal(records, 20, r.stdout);
  assert.equal(errors, 20, r.stdout);
});
