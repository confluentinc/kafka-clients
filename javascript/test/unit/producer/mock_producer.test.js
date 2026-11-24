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
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Implementation over the
// FFI), then the binding's own: the call shapes, the error setters and
// the records the mock keeps.
//
// Not translated:
// - testPartitioner: it builds the mock with a Cluster, Java's form the
//   binding leaves out (Scope), so the mock's cluster is always empty.
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
const { RecordHeader } = require('@confluentinc/kafka-javascript/common/header');
const { stringSerializer } = require('@confluentinc/kafka-javascript/common/serialization');
const { MockProducer, ProducerRecord, RecordMetadata } = require('@confluentinc/kafka-javascript/producer');
// Every probe child runs with the allocator's poisoning (freed memory filled,
// guard pages around large blocks), so a use after free or a double free in
// the native layer, at a collection or a destroy, fails the probe.
const { POISON } = require('../../memory/_probe');

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
  await assert.rejects(producer.send(record1), { constructor: IllegalStateError, message: 'MockProducer is already closed.' });
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
  // @ts-expect-error a null record: Java's test sends one, the binding's null check throws TypeError
  await assert.rejects(producer.send(null), { constructor: TypeError, message: 'record must not be null' });
});

test('shouldThrowOnFlushProducerIfProducerIsClosed', async () => {
  const producer = buildMockProducer(true);
  await producer.close();
  await assert.rejects(producer.flush(), { constructor: IllegalStateError, message: 'MockProducer is already closed.' });
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

test("history() keeps the app's record with its own Headers, holding the Header objects given and writable, as Java's mock leaves them", async () => {
  const producer = new MockProducer({ autoComplete: true, partitioner: null, keySerializer: null, valueSerializer: null });
  const h = new RecordHeader({ key: 'h', value: Buffer.from('v') });
  const record = new ProducerRecord({ topic, partition: null, key: null, value: Buffer.from('v'), headers: [h, ['p', null]] });
  await producer.send(record);
  const [kept] = producer.history();
  assert.equal(kept, record);
  assert.equal(kept.headers, record.headers);
  const [first, pair] = kept.headers.toArray();
  assert.equal(first, h);
  assert.ok(pair instanceof RecordHeader);
  assert.deepEqual([pair.key(), pair.value()], ['p', null]);
  // Java's MockProducer does not seal the headers: a change after send() is seen in history().
  record.headers.add({ key: 'later', value: null });
  assert.equal(producer.history()[0].headers.lastHeader('later')?.key(), 'later');
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
// (JS conventions, Threads and callbacks). send() with autoComplete and
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

test("two rejections inside one flush(): the callbacks run one at a time, it waits for each, rejects with the oldest (the first callback's), and keeps the other", async () => {
  const first = new Error('first');
  const second = new Error('second');
  const producer = buildMockProducer(false);
  /** @type {() => void} */
  let release = () => {};
  const gate = new Promise((resolve) => {
    release = () => resolve(undefined);
  });
  /** @type {string[]} */
  const ran = [];
  // The first callback rejects late, the second at once, a third resolves late:
  // each starts only once the one before it settled.
  await producer.send({
    record: record1,
    callback: () => {
      ran.push('first');
      return gate.then(() => Promise.reject(first));
    },
  });
  await producer.send({
    record: record2,
    callback: () => {
      ran.push('second');
      return Promise.reject(second);
    },
  });
  let slowDone = false;
  const slow = await producer.send({
    record: record1,
    callback: () => {
      ran.push('slow');
      return new Promise((resolve) => setTimeout(resolve, 30)).then(() => {
        slowDone = true;
      });
    },
  });
  let settled = false;
  const flushing = producer.flush().finally(() => {
    settled = true;
  });
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(settled, false, 'flush() waits for the callbacks it completed');
  assert.deepEqual(ran, ['first'], "the later callbacks wait for the first one's Promise");
  assert.equal(slow.isDone(), false);
  release();
  await assert.rejects(flushing, (e) => e === first);
  assert.deepEqual(ran, ['first', 'second', 'slow']);
  assert.equal(slowDone, true);
  assert.equal(slow.isDone(), true);
  await assert.rejects(producer.close(), (e) => e === second);
});

// The callbacks run one at a time, in completion order, as KafkaProducer's do
// (javascript/CLAUDE.md, rule 6; DECISIONS.md, entry 34): the mock's go
// through the same native run queue. Java's run one after another on the
// thread that completes their records.
test("callbacks run one at a time in completion order, as on KafkaProducer: a later record's callback waits for an earlier one's Promise", async () => {
  for (const autoComplete of [false, true]) {
    const producer = buildMockProducer(autoComplete);
    /** @type {() => void} */
    let release = () => {};
    const released = new Promise((resolve) => {
      release = () => resolve(undefined);
    });
    /** @type {string[]} */
    const events = [];
    /** @type {Array<Promise<AnyFuture>>} */
    const sends = [];
    for (let i = 0; i < 3; i++) {
      const sending = producer.send({
        record: record1,
        callback: async () => {
          events.push(`start ${i}`);
          // The first holds back until released; the later ones take less
          // time the later they come, so run together they would end in
          // reverse order.
          if (i === 0) await released;
          else await new Promise((resolve) => setTimeout(resolve, (3 - i) * 20));
          events.push(`end ${i}`);
        },
      });
      sending.catch(() => {});
      sends.push(sending);
    }
    if (!autoComplete) {
      for (const sending of sends) await sending;
      // Each completeNext() returns before a later record's callback runs.
      for (let i = 0; i < 3; i++) assert.equal(producer.completeNext(), true);
    }
    let flushed = false;
    const flushing = producer.flush().then(() => {
      flushed = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.deepEqual(events, ['start 0'], `autoComplete ${autoComplete}: the later callbacks wait for the first one's Promise`);
    assert.equal(flushed, false, `autoComplete ${autoComplete}: flush() waits for the callbacks waiting for their turn`);
    release();
    await flushing;
    assert.deepEqual(events, ['start 0', 'end 0', 'start 1', 'end 1', 'start 2', 'end 2']);
    for (const sending of sends) assert.equal((await sending).isDone(), true);
    await producer.close();
  }
});

test('with autoComplete, a send() whose callback waits for its turn settles once that callback ran, rejecting with its error', async () => {
  const boom = new Error('second');
  const producer = buildMockProducer(true);
  /** @type {() => void} */
  let release = () => {};
  const released = new Promise((resolve) => {
    release = () => resolve(undefined);
  });
  const first = producer.send({ record: record1, callback: () => released });
  let secondSettled = false;
  const second = producer.send({
    record: record2,
    callback: () => {
      throw boom;
    },
  });
  second.then(
    () => (secondSettled = true),
    () => (secondSettled = true),
  );
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(secondSettled, false, "the second send() waits for its callback, which waits for the first one's Promise");
  release();
  await first;
  await assert.rejects(second, (e) => e === boom);
  // Raised once, by its own send().
  await producer.send(record1);
  await producer.close();
});

test('a callback that waited for its turn runs after the call that completed its record returned: what it throws is kept for the next send(), once', async () => {
  // Java's completeNext() runs the callback inside it and throws what it
  // throws. Here the callback waits for an earlier callback's Promise, so it
  // runs once that settled, after completeNext() or errorNext() returned: its
  // throw is kept, as a rejection is (Python's AsyncMockProducer keeps a
  // callback task's error the same way).
  for (const completing of ['completeNext', 'errorNext']) {
    const late = new Error(`late ${completing}`);
    const producer = buildMockProducer(false);
    /** @type {() => void} */
    let release = () => {};
    const released = new Promise((resolve) => {
      release = () => resolve(undefined);
    });
    await producer.send({ record: record1, callback: () => released });
    const second = await producer.send({
      record: record2,
      callback: () => {
        throw late;
      },
    });
    assert.equal(producer.completeNext(), true);
    // The call completing the second record returns before its callback runs.
    if (completing === 'completeNext') assert.equal(producer.completeNext(), true);
    else assert.equal(producer.errorNext(new KafkaError('delivery')), true);
    assert.equal(second.isDone(), false);
    release();
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(second.isDone(), true);
    // A completing call in between does not drop it.
    assert.equal(producer.completeNext(), false);
    await assert.rejects(producer.send(record1), (e) => e === late, completing);
    // Raised once.
    await producer.send(record1);
    await producer.flush();
    await producer.close();
  }
});

test('clear() keeps the callback of a completed record that waits for its turn: it runs in its turn, once, and its future completes', () => {
  // Java's clear() drops the completions of the sends still waiting; a record
  // completeNext() completed has had its callback run. Here the callback
  // waits for an earlier callback's Promise when clear() lets the mock's hold
  // on it go: the addon holds it until it runs, through collections.
  const r = spawnSync(
    process.execPath,
    [
      '--expose-gc',
      '-e',
      `
    const { MockProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    (async () => {
      const p = new MockProducer();
      const record = new ProducerRecord({ topic: 't', value: Buffer.from('v') });
      let release;
      const released = new Promise((resolve) => (release = resolve));
      await p.send({ record, callback: () => released });
      let ran = 0;
      const second = await p.send({ record, callback: () => { ran++; } });
      p.completeNext();
      p.completeNext();
      p.clear();
      for (let i = 0; i < 5; i++) {
        global.gc();
        await new Promise((resolve) => setImmediate(resolve));
      }
      release();
      await new Promise((resolve) => setTimeout(resolve, 50));
      console.log(JSON.stringify({ ran, done: second.isDone() }));
      await p.close();
    })();
  `,
    ],
    { cwd: path.join(__dirname, '..', '..', '..'), encoding: 'utf8', timeout: 30000, env: { ...process.env, ...POISON } },
  );
  assert.equal(r.status, 0, r.stderr);
  assert.deepEqual(JSON.parse(r.stdout.trim().split('\n').pop() ?? ''), { ran: 1, done: true });
});

test('close() and flush() made from a callback that waited for its turn wait for that callback, as after its first await: awaited there, they never settle', async () => {
  // Before its first await, a callback the core runs in its drain skips the
  // wait of a close(); one that waited for its turn behind an earlier
  // callback's Promise runs outside that drain, so the core waits for it.
  for (const call of ['close', 'flush']) {
    const producer = buildMockProducer(false);
    /** @type {() => void} */
    let release = () => {};
    const released = new Promise((resolve) => {
      release = () => resolve(undefined);
    });
    /** @type {string | null} */
    let seen = null;
    /** @type {Promise<void> | null} */
    let made = null;
    await producer.send({ record: record1, callback: () => released });
    await producer.send({
      record: record2,
      callback: async () => {
        const calling = call === 'close' ? producer.close() : producer.flush();
        made = calling;
        seen = await Promise.race([
          calling.then(
            () => 'settled',
            () => 'settled',
          ),
          new Promise((resolve) => setTimeout(() => resolve('pending'), 100)),
        ]);
      },
    });
    producer.completeNext();
    producer.completeNext();
    release();
    await new Promise((resolve) => setTimeout(resolve, 300));
    assert.equal(seen, 'pending', `${call}: it waits for the callback that awaits it`);
    // Once the callback settled, it settles too.
    assert.ok(made !== null);
    await made;
    await cleanup(producer);
  }
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

test('a callback that fails after a nested send() rejects its own send(), not a later call', async () => {
  for (const nestedFirst of [true, false]) {
    const outerError = new Error('outer');
    const producer = buildMockProducer(true);
    /** @type {Promise<unknown> | null} */
    let nested = null;
    /** @type {string | null} */
    let nestedWhileOuterRan = null;
    const outer = producer.send({
      record: record1,
      callback: nestedFirst
        ? async () => {
            // The nested record's callback waits for its turn behind this
            // callback's Promise, and its send() for that callback: awaited
            // here it would never settle. It is still pending when this
            // callback fails.
            nested = producer.send(record2);
            nestedWhileOuterRan = await Promise.race([
              nested.then(
                () => 'settled',
                () => 'settled',
              ),
              new Promise((resolve) => setTimeout(() => resolve('pending'), 50)),
            ]);
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
    if (nestedFirst) assert.equal(nestedWhileOuterRan, 'pending');
    // Nothing was kept for an unrelated call.
    await producer.send(record2);
    await producer.flush();
    await producer.close();
    assert.equal(producer.history().length, 3);
  }
});

test("a completing call made from a callback keeps an earlier callback's throw for the outer flush(), as Java's flush() throws it", async () => {
  // Java's MockProducer#flush completes the records in order; the first
  // onCompletion that throws ends it with that error. Here the core's mock
  // completes both, and B's callback makes a completing call of its own,
  // which must not drop A's throw.
  for (const nestedCall of ['completeNext', 'errorNext', 'flush']) {
    const producer = buildMockProducer(false);
    const errorA = new Error('A');
    /** @type {Promise<void> | null} */
    let nested = null;
    /** @type {unknown} */
    let nestedThrew = null;
    await producer.send({
      record: record1,
      callback: () => {
        throw errorA;
      },
    });
    await producer.send({
      record: record2,
      callback: () => {
        try {
          if (nestedCall === 'completeNext') producer.completeNext();
          else if (nestedCall === 'errorNext') producer.errorNext(null);
          else {
            nested = producer.flush();
            nested.catch(() => {});
          }
        } catch (e) {
          nestedThrew = e;
        }
      },
    });
    await assert.rejects(producer.flush(), (e) => e === errorA, `${nestedCall}: the outer flush() rejects with A's throw`);
    assert.equal(nestedThrew, null, `${nestedCall}: the nested call throws nothing of another record's`);
    if (nested !== null) await nested;
    // Raised once: later calls resolve.
    await producer.flush();
    await producer.close();
  }
});

test("a send() from a callback's synchronous part rejects with its own callback's throw or rejection, as Java's nested send() throws it", async () => {
  // Java's MockProducer#send with autoComplete runs Completion#complete, which
  // calls onCompletion with no try, inside the nested send(). The callback
  // queue does not nest, and the callbacks run one at a time, so the nested
  // record's callback runs once the outer callback, and the Promise it
  // returned, settled; the nested send() waits for it.
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
      /** @type {string | null} */
      let seenInside = null;
      await producer.send({
        record: record1,
        callback: outerAwaits
          ? async () => {
              // Its callback runs in its turn, once this callback's Promise
              // settled: awaited here, the nested send() would never settle.
              const sending = producer.send({ record: record2, callback: nestedCallback });
              nested = sending;
              sending.catch(() => {});
              seenInside = await Promise.race([
                sending.then(
                  () => 'settled',
                  () => 'settled',
                ),
                new Promise((resolve) => setTimeout(() => resolve('pending'), 50)),
              ]);
            }
          : () => {
              nested = producer.send({ record: record2, callback: nestedCallback });
              nested.catch(() => {});
            },
      });
      assert.ok(nested !== null);
      await assert.rejects(nested, (e) => e === nestedError);
      if (outerAwaits) assert.equal(seenInside, 'pending', `${shape}: the nested send() waits for the outer callback`);
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
    { cwd: path.join(__dirname, '..', '..', '..'), encoding: 'utf8', timeout: 30000, env: { ...process.env, ...POISON } },
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
    env: { ...process.env, ...POISON },
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

// A call made on a closed mock holds Node until it settles, as Java's call
// returns, and so does a second close() made while the first runs
// (javascript/CLAUDE.md, "Threads and liveness"): the mock's calls are the
// producer's counted _cb calls, through the same view. Each child below has
// nothing else holding Node, prints a line once the call settled, and exits
// with 0. Java 4.4.0's MockProducer answers are the expected ones.

// One child per call: a call that never settled would end the child there.
const MOCK_AFTER_CLOSE = [
  // Java's close(), close(Duration) and try-with-resources' close() return.
  { call: 'm.close()', outcome: 'resolved' },
  { call: 'm.close(5)', outcome: 'resolved' },
  { call: 'm[Symbol.asyncDispose]()', outcome: 'resolved' },
  // Java's flush() and send() check that the mock is open.
  { call: 'm.flush()', outcome: 'IllegalStateError: MockProducer is already closed.' },
  // Answered inside the call.
  {
    call: "m.send(new ProducerRecord({ topic: 't', value: Buffer.from('v') }))",
    outcome: 'IllegalStateError: MockProducer is already closed.',
  },
  // Java's partitionsFor() reads the mock's cluster, Cluster.empty().
  { call: "m.partitionsFor('t')", outcome: 'resolved with []' },
];

for (const { call, outcome } of MOCK_AFTER_CLOSE) {
  test(`${call} on a closed mock settles with Java's answer while nothing else holds Node`, () => {
    const r = runNode(`
      const { IllegalStateError } = require('@confluentinc/kafka-javascript');
      const { MockProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
      const m = new MockProducer();
      (async () => {
        await m.close();
        const outcome = await ${call}.then(
          (value) => (value === undefined ? 'resolved' : 'resolved with ' + JSON.stringify(value)),
          (e) => (e.constructor === IllegalStateError ? 'IllegalStateError' : 'another class, ' + e.constructor.name) + ': ' + e.message,
        );
        console.log('after the call:', outcome);
      })();
    `);
    assert.equal(r.status, 0, r.stderr);
    assert.equal(r.stdout, `after the call: ${outcome}\n`);
  });
}

// Two closes at once, awaited together: the second is answered after the
// first's completion, which only the hold of a waiting call covers. Each
// round's mock stays reachable until its calls settled (`kept`): one
// collected with a call pending is the JS conventions' Future improvements
// entry, not this test's subject.
test('two close() calls at once on an open mock both settle, in each of 25 rounds', () => {
  const r = runNode(`
    const { MockProducer } = require('@confluentinc/kafka-javascript/producer');
    let kept;
    (async () => {
      const outcomes = new Set();
      for (let round = 0; round < 25; round++) {
        const m = new MockProducer();
        for (const o of await Promise.allSettled([m.close(), m.close()])) outcomes.add(o.status);
        kept = m;
      }
      console.log('all settled:', [...outcomes].join(', '));
    })();
  `);
  assert.equal(r.status, 0, r.stderr);
  // Java's close() returns, both times.
  assert.equal(r.stdout, 'all settled: fulfilled\n');
});

// A call whose answer the addon can't build still settles (native/producer.c,
// js_take_failure in native/error.c): flush(), partitionsFor() and close()
// reject with the failure, send() rejects for a refused record and resolves
// for an accepted one, and a delivery's callback runs once with the failure.
// A wait on a record's future whose answer can't be built fails the future
// only when its delivery left it pending, and warns otherwise; a timed one
// that timed out rejects its get() with the failure. The test-only
// faultNext() (native/addon.c) makes steps of the addon fail, as their
// Node-API calls failing would.
const native = require('../../../dist/internal/native');
// What a failed conversion of a core error throws (native/error.c,
// error_to_js), and what js_take_failure makes, naming the step, when no
// exception is pending.
const ERROR_FAILED = /^Node-API call failed: converting a kafka_common_Error_t \(/;
const PARTITIONS_FAILED = /^Node-API call failed: converting a partitionsFor answer \(/;
const CLOSE_FAILED = /^Node-API call failed: converting a close's answer \(/;

/**
 * The producer's record-future slots and waits still held (native/producer.c, producerStats()).
 *
 * @returns {{ slots: number, waits: number }} the slots taken and not given back, and the waits not completed
 */
function nativeHeld() {
  const s = native.producerStats();
  return { slots: s.slotsTaken - s.slotsReturned, waits: s.waitsStarted - s.waitsCompleted };
}

/**
 * Turns the event loop until `holds()` is true.
 *
 * @param {() => boolean} holds the condition
 */
async function untilHolds(holds) {
  for (let i = 0; i < 1000 && !holds(); i++) await new Promise((resolve) => setImmediate(resolve));
  assert.ok(holds(), 'the condition never held');
}

describe("a call whose answer the addon can't build", () => {
  // A step left armed by a test that failed before it ran would fail the next test's.
  afterEach(() => {
    native.faultNext(null);
  }, 10000);

  test("a construction whose notify registration fails throws the FFI's error", () => {
    native.faultNext('notify');
    assert.throws(() => new MockProducer(), { name: 'TypeError', message: 'self_ must not be null' });
    assert.deepEqual(native.faultNext(null), []);
  }, 10000);

  test("flush() rejects with the failure when its error can't be built", async () => {
    const m = new MockProducer();
    await m.close();
    native.faultNext('error');
    await assert.rejects(m.flush(), { name: 'Error', message: ERROR_FAILED });
    assert.deepEqual(native.faultNext(null), []);
    // The mock's own state is whole: the next flush has the core's answer.
    await assert.rejects(m.flush(), { name: 'IllegalStateError', message: 'MockProducer is already closed.' });
  }, 10000);

  test('partitionsFor() rejects with the failure, its error or its partitions not built', async () => {
    const m = new MockProducer();
    m.setPartitionsForException(new Error('injected'));
    native.faultNext('error');
    await assert.rejects(m.partitionsFor('t'), { name: 'Error', message: ERROR_FAILED });
    m.setPartitionsForException(null);
    native.faultNext('partitions');
    await assert.rejects(m.partitionsFor('t'), { name: 'Error', message: PARTITIONS_FAILED });
    assert.deepEqual(native.faultNext(null), []);
    assert.deepEqual(await m.partitionsFor('t'), []);
    await m.close();
  }, 10000);

  test('close() rejects with the failure, and the mock is closed', async () => {
    const m = new MockProducer();
    native.faultNext('close');
    await assert.rejects(m.close(), { name: 'Error', message: CLOSE_FAILED });
    assert.deepEqual(native.faultNext(null), []);
    assert.equal(m.closed(), true);
    await m.close();
  }, 10000);

  test("send() rejects for a refused record whose error can't be built", async () => {
    const m = new MockProducer();
    m.setSendException(new Error('refused'));
    native.faultNext('error');
    await assert.rejects(m.send(new ProducerRecord({ topic: 't', value: Buffer.from('v') })), {
      name: 'Error',
      message: ERROR_FAILED,
    });
    assert.deepEqual(native.faultNext(null), []);
    assert.deepEqual(m.history(), []);
    m.setSendException(null);
    await m.send(new ProducerRecord({ topic: 't', value: Buffer.from('v') }));
    assert.equal(m.history().length, 1);
    await m.close();
  }, 10000);

  test("send() resolves for an accepted record whose slot can't be made, its future completed by the delivery", async () => {
    const m = new MockProducer();
    const base = nativeHeld();
    /** @type {string[]} */
    const calls = [];
    native.faultNext('slot');
    const future = await m.send({
      record: new ProducerRecord({ topic: 't', value: Buffer.from('v') }),
      callback: ({ metadata, exception }) => {
        calls.push(`${String(metadata.offset)} ${String(exception)}`);
      },
    });
    assert.deepEqual(native.faultNext(null), []);
    assert.equal(future.isDone(), false);
    // No native wait: a timed get runs on the JS timer.
    await assert.rejects(future.get({ timeout: 20 }), { name: 'TimeoutError', message: 'Timeout after waiting for 20 ms.' });
    assert.equal(m.completeNext(), true);
    const metadata = await future.get();
    assert.ok(metadata instanceof RecordMetadata);
    assert.equal(metadata.offset, 0n);
    assert.deepEqual(calls, ['0 null']);
    // The slot taken for the record went back once its future was destroyed.
    assert.deepEqual(nativeHeld(), base);
    await m.close();
  }, 10000);

  test("the callback runs once with the failure, and an auto-completed send() settles, when the delivery's metadata can't be built", async () => {
    const m = new MockProducer({ autoComplete: true, partitioner: null, keySerializer: null, valueSerializer: null });
    /** @type {Array<{ metadata: unknown, exception: Error | null }>} */
    const calls = [];
    native.faultNext('metadata');
    const future = await m.send({
      record: new ProducerRecord({ topic: 't', partition: 2, key: null, value: Buffer.from('v') }),
      callback: ({ metadata, exception }) => void calls.push({ metadata: metadata.toJSON(), exception }),
    });
    assert.deepEqual(native.faultNext(null), []);
    assert.equal(calls.length, 1);
    const [{ metadata, exception }] = calls;
    // Java's empty metadata of a failed record, as the core's mock gives it
    // (the next test): partition 0 of the mock's empty cluster, -1 elsewhere.
    assert.deepEqual(metadata, {
      topic: 't',
      partition: 0,
      offset: '-1',
      hasOffset: false,
      timestamp: '-1',
      hasTimestamp: false,
      serializedKeySize: -1,
      serializedValueSize: -1,
    });
    assert.ok(exception instanceof Error);
    assert.match(exception.message, /^Node-API call failed: converting a record's delivery \(/);
    assert.equal(future.isDone(), true);
    await assert.rejects(future.get(), (/** @type {unknown} */ e) => e instanceof ExecutionError && e.cause === exception);
    await m.close();
  }, 10000);

  test("the callback runs once with the failure and the core's metadata when the delivery's error can't be built", async () => {
    const m = new MockProducer();
    /** @type {Array<{ metadata: unknown, exception: Error | null }>} */
    const calls = [];
    const future = await m.send({
      record: new ProducerRecord({ topic: 't', value: Buffer.from('v') }),
      callback: ({ metadata, exception }) => void calls.push({ metadata: metadata.toJSON(), exception }),
    });
    native.faultNext('error');
    assert.equal(m.errorNext(new Error('injected')), true);
    assert.deepEqual(native.faultNext(null), []);
    assert.equal(calls.length, 1);
    const [{ metadata, exception }] = calls;
    assert.deepEqual(metadata, {
      topic: 't',
      partition: 0,
      offset: '-1',
      hasOffset: false,
      timestamp: '-1',
      hasTimestamp: false,
      serializedKeySize: -1,
      serializedValueSize: -1,
    });
    assert.ok(exception instanceof Error);
    assert.match(exception.message, ERROR_FAILED);
    await assert.rejects(future.get(), (/** @type {unknown} */ e) => e instanceof ExecutionError && e.cause === exception);
    await m.close();
  }, 10000);

  test("a delivery whose metadata can't be built still waits for its turn, then runs once with the failure", async () => {
    const m = new MockProducer();
    const base = nativeHeld();
    /** @type {string[]} */
    const calls = [];
    /** @type {() => void} */
    let release = () => {};
    const first = await m.send({
      record: new ProducerRecord({ topic: 't', value: Buffer.from('v') }),
      callback: () =>
        new Promise((resolve) => {
          calls.push('first');
          release = () => resolve(undefined);
        }),
    });
    const second = await m.send({
      record: new ProducerRecord({ topic: 't', value: Buffer.from('v') }),
      callback: ({ metadata, exception }) => void calls.push(`second ${String(metadata)} ${String(exception?.message)}`),
    });
    assert.equal(m.completeNext(), true);
    native.faultNext('metadata');
    // The first callback's Promise is pending: the second delivery waits behind it.
    assert.equal(m.completeNext(), true);
    assert.deepEqual(native.faultNext(null), []);
    assert.deepEqual(calls, ['first']);
    release();
    assert.equal((await first.get()).offset, 0n);
    await assert.rejects(second.get(), ExecutionError);
    assert.equal(calls.length, 2);
    assert.match(calls[1], /^second t-0@-1 Node-API call failed: converting a record's delivery \(/);
    // The waits the get() calls started end in a later turn: none is left to the next test.
    await untilHolds(() => nativeHeld().waits === base.waits);
    await m.close();
  }, 10000);

  test("a delivery the run queue can't take runs at once, out of turn, and the callback whose turn it is keeps it", async () => {
    const m = new MockProducer({ autoComplete: true, partitioner: null, keySerializer: null, valueSerializer: null });
    /** @type {string[]} */
    const calls = [];
    /** @type {() => void} */
    let release = () => {};
    // With autoComplete, send() waits for its callback's Promise.
    const first = m.send({
      record: new ProducerRecord({ topic: 't', value: Buffer.from('v') }),
      callback: () =>
        new Promise((resolve) => {
          calls.push('first');
          release = () => resolve(undefined);
        }),
    });
    await untilHolds(() => calls.length === 1);
    native.faultNext('enqueue');
    const second = m.send({
      record: new ProducerRecord({ topic: 't', value: Buffer.from('v') }),
      callback: ({ metadata }) => void calls.push(`second ${String(metadata.offset)}`),
    });
    await untilHolds(() => calls.length === 2);
    assert.deepEqual(native.faultNext(null), []);
    assert.equal((await (await second).get()).offset, 1n);
    // The first callback's Promise still holds the turn: the next delivery waits behind it.
    const third = m.send({
      record: new ProducerRecord({ topic: 't', value: Buffer.from('v') }),
      callback: ({ metadata }) => void calls.push(`third ${String(metadata.offset)}`),
    });
    for (let i = 0; i < 20; i++) await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(calls, ['first', 'second 1']);
    release();
    assert.equal((await (await first).get()).offset, 0n);
    assert.equal((await (await third).get()).offset, 2n);
    assert.deepEqual(calls, ['first', 'second 1', 'third 2']);
    await m.close();
  }, 10000);

  /** The failures of waits a test hands to process.emitWarning, while it runs `body`. */
  async function warnedDuring(/** @type {() => Promise<void>} */ body) {
    /** @type {Error[]} */
    const warned = [];
    /** @type {(warning: Error) => void} */
    const listener = (warning) => void warned.push(warning);
    process.on('warning', listener);
    try {
      await body();
      // process.emitWarning emits on the next tick.
      await new Promise((resolve) => setImmediate(resolve));
    } finally {
      process.off('warning', listener);
    }
    return warned.filter((w) => w.name !== 'ExperimentalWarning');
  }

  for (const timeout of [null, 60000]) {
    const form = timeout === null ? 'an untimed' : 'a timed';
    test(`${form} wait that does not time out warns of a failure it can't build, and leaves the future's outcome`, async () => {
      const m = new MockProducer();
      const base = nativeHeld();
      const future = await m.send({
        record: new ProducerRecord({ topic: 't', value: Buffer.from('v') }),
        // The delivery's own metadata is built: the wait's, next, is not.
        callback: () => void native.faultNext('metadata'),
      });
      /** @type {RecordMetadata | null} */
      let metadata = null;
      const warned = await warnedDuring(async () => {
        const got = timeout === null ? future.get() : future.get({ timeout });
        assert.equal(m.completeNext(), true);
        metadata = await got;
        await untilHolds(() => nativeHeld().waits === base.waits);
      });
      assert.deepEqual(native.faultNext(null), []);
      assert.ok(metadata !== null && /** @type {RecordMetadata} */ (metadata).offset === 0n);
      assert.equal(warned.length, 1);
      assert.match(warned[0].message, /^Node-API call failed: converting a record future's answer \(/);
      assert.equal((await future.get()).offset, 0n);
      assert.deepEqual(nativeHeld(), base);
      await m.close();
    }, 10000);

    test(`${form} wait that can't build its answer fails a future its delivery left pending, instead of hanging`, async () => {
      const m = new MockProducer();
      /** @type {string[]} */
      const calls = [];
      const future = await m.send({
        record: new ProducerRecord({ topic: 't', value: Buffer.from('v') }),
        callback: () => void calls.push('callback'),
      });
      const got = timeout === null ? future.get() : future.get({ timeout });
      // The delivery reaches no JavaScript (its metadata, nor even its failure, can be built), and the wait's metadata fails too.
      native.faultNext('metadata', 2);
      native.faultNext('failure');
      assert.equal(m.completeNext(), true);
      await assert.rejects(got, (/** @type {unknown} */ e) => {
        assert.ok(e instanceof ExecutionError);
        assert.ok(e.cause instanceof Error);
        assert.match(e.cause.message, /^Node-API call failed: converting a record future's answer \(/);
        return true;
      });
      assert.deepEqual(native.faultNext(null), []);
      assert.deepEqual(calls, []);
      assert.equal(future.isCompletedExceptionally(), true);
      await m.close();
    }, 10000);
  }

  test('a timed get() that times out rejects with the failure at its deadline, and leaves the future to the delivery', async () => {
    const m = new MockProducer();
    const base = nativeHeld();
    /** @type {string[]} */
    const calls = [];
    const future = await m.send({
      record: new ProducerRecord({ topic: 't', value: Buffer.from('v') }),
      callback: ({ metadata }) => {
        calls.push(String(metadata.offset));
      },
    });
    native.faultNext('error');
    const started = performance.now();
    await assert.rejects(future.get({ timeout: 50 }), { name: 'Error', message: ERROR_FAILED });
    const waited = performance.now() - started;
    assert.ok(waited >= 45, `rejected after ${waited} ms`);
    assert.deepEqual(native.faultNext(null), []);
    assert.equal(future.isDone(), false);
    assert.equal(m.completeNext(), true);
    assert.equal((await future.get()).offset, 0n);
    assert.deepEqual(calls, ['0']);
    assert.deepEqual(nativeHeld(), base);
    await m.close();
  }, 10000);

  test("a timed get() that timed out warns of a failure it can't build once the delivery completed the future first", async () => {
    const m = new MockProducer();
    const base = nativeHeld();
    /** @type {Array<Promise<RecordMetadata>>} */
    const gets = [];
    const future = await m.send({
      record: new ProducerRecord({ topic: 't', value: Buffer.from('v') }),
      callback: () => {
        // Started before the delivery is recorded, the wait times out before
        // the record's native future completes, after this callback, and its
        // TimeoutError is not built.
        native.faultNext('error');
        gets.push(future.get({ timeout: 0 }));
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100);
      },
    });
    const warned = await warnedDuring(async () => {
      assert.equal(m.completeNext(), true);
      assert.equal(gets.length, 1);
      // The get() has the delivery's answer.
      assert.equal((await gets[0]).offset, 0n);
      await untilHolds(() => nativeHeld().waits === base.waits);
    });
    assert.deepEqual(native.faultNext(null), []);
    assert.equal(warned.length, 1);
    assert.match(warned[0].message, ERROR_FAILED);
    assert.deepEqual(nativeHeld(), base);
    await m.close();
  }, 10000);
});

// A call made on a closed mock holds Node until it settles, and so does the
// close that closes it, when its answer can't be built too: each child has
// nothing else holding Node and prints a line once the call settled. Without
// the settle it would exit with 0 in the middle of the await.
const FAULT_AFTER_CLOSE = [
  { closed: true, step: 'error', call: 'm.flush()' },
  { closed: true, step: 'close', call: 'm.close()' },
  { closed: false, step: 'close', call: 'm.close()' },
];

for (const { closed, step, call } of FAULT_AFTER_CLOSE) {
  test(`${call} ${closed ? 'after close()' : 'closing the mock'} settles while nothing else holds Node when its answer can't be built`, () => {
    const r = runNode(
      `
      const native = require('./dist/internal/native');
      const { MockProducer } = require('@confluentinc/kafka-javascript/producer');
      const m = new MockProducer();
      (async () => {
        if (${String(closed)}) await m.close();
        native.faultNext('${step}');
        const outcome = await ${call}.then(
          () => 'resolved',
          (e) => e.constructor.name + ': ' + e.message,
        );
        console.log('after the call: ' + outcome);
      })();
    `,
      30000,
    );
    assert.equal(r.status, 0, r.stderr);
    assert.match(r.stdout, /^after the call: Error: Node-API call failed: converting (a kafka_common_Error_t|a close's answer) \(/);
  }, 40000);
}

test("a wait that can't build its answer settles on a closed mock while nothing else holds Node, failing a future its delivery left pending", () => {
  const r = runNode(
    `
    const native = require('./dist/internal/native');
    const { MockProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    const m = new MockProducer();
    (async () => {
      const future = await m.send(new ProducerRecord({ topic: 't', value: Buffer.from('v') }));
      const got = future.get().then(
        () => 'resolved',
        (e) => e.constructor.name + ': ' + e.cause.message,
      );
      await m.close();
      // The delivery reaches no JavaScript (its metadata, nor even its
      // failure, can be built), and the wait's metadata fails too.
      native.faultNext('metadata', 2);
      native.faultNext('failure');
      m.completeNext();
      console.log('after the call: ' + (await got));
    })();
  `,
    30000,
  );
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.stdout, /^after the call: ExecutionError: Node-API call failed: converting a record future's answer \(/);
}, 40000);
