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

test('the serializers run on every send, each with fresh empty headers, and the record keeps its own headers writable', async () => {
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
  assert.throws(() => manual.completeNext(), (e) => e === thrown);
  assert.throws(() => manual.errorNext(new KafkaError('next')), (e) => e === thrown);
  await assert.rejects(manual.flush(), (e) => e === thrown);
  // The calls after it are not affected.
  await manual.send(record2);
  assert.equal(manual.completeNext(), true);
  // The core's mock completes the future all the same (a deviation, the mock
  // being the core's: Java's leaves it incomplete).
  assert.equal((await futures[0].get()).offset, 0n);
  await manual.close();
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

test("a refused send is not in history() and keeps nothing: its callback never runs", async () => {
  const producer = new MockProducer();
  const error = new KafkaError('refused');
  producer.setSendException(error);
  let ran = false;
  await assert.rejects(producer.send({ record: record1, callback: () => (ran = true) }), (e) => e === error);
  assert.equal(ran, false);
  assert.deepEqual(producer.history(), []);
  await producer.close();
});

function runNode(script, timeout = 60000) {
  const { spawnSync } = require('node:child_process');
  const path = require('node:path');
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
test('a closed mock is destroyed when collected, a send pending at close() or dropped by clear() included', { skip: !['linux', 'darwin'].includes(process.platform) }, () => {
  const r = runNode(`
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
  `, 120000);
  assert.equal(r.status, 0, r.stderr);
  const lines = r.stdout.trim().split('\n');
  assert.equal(lines[2], 'completeNext after close true true', r.stdout);
  const counts = lines.slice(0, 2).map((l) => /^(.*) (\d+) (-?\d+)$/.exec(l));
  assert.deepEqual(counts.map((m) => m && [m[1], m[2]]), [['pending at close', '5'], ['dropped by clear', '5']], r.stdout);
  // About 18 threads per live mock: five leaked mocks would leave about 90.
  for (const m of counts) assert.ok(Number(m[3]) < 10, `${m[1]}: ${m[3]} threads left over`);
});

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
