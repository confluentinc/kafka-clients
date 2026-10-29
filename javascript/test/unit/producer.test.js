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


'use strict';

// The producer against an unreachable broker: no broker is needed, every
// record fails in the core with its own error.

const test = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

const { KafkaError, KafkaProducer, ProducerRecord, RecordMetadata } = require('../..');

const MAX_BLOCK_MS = 300;
const UNREACHABLE = {
  'bootstrap.servers': '127.0.0.1:1',
  'max.block.ms': MAX_BLOCK_MS,
  'delivery.timeout.ms': 1000,
  'request.timeout.ms': 500,
  'linger.ms': 0,
};
const TIMEOUT_MESSAGE = (topic) => `Topic ${topic} not present in metadata after ${MAX_BLOCK_MS} ms.`;
const CLOSED_MESSAGE = 'Cannot perform operation after producer has been closed';
const FLUSH_IN_CALLBACK =
  'KafkaProducer.flush() invocation inside a callback is not permitted because it may lead to deadlock.';

test('send rejects with the core timeout after the callback ran once', async () => {
  const producer = new KafkaProducer(UNREACHABLE);
  try {
    const calls = [];
    const promise = producer.send(new ProducerRecord({ topic: 'unit-t', key: 'k', value: 'v' }), (metadata, error) =>
      calls.push({ metadata, error }),
    );
    assert.ok(promise instanceof Promise);
    await assert.rejects(promise, (e) => {
      // The callback ran exactly once, before the rejection.
      assert.equal(calls.length, 1);
      assert.ok(e instanceof KafkaError);
      assert.equal(e.name, 'REQUEST_TIMED_OUT');
      assert.equal(e.code, 7);
      assert.equal(e.message, TIMEOUT_MESSAGE('unit-t'));
      assert.equal(e.retriable, true);
      assert.equal(e.apiError, true);
      return true;
    });
    const [{ metadata, error }] = calls;
    assert.ok(metadata instanceof RecordMetadata);
    assert.equal(metadata.topic, 'unit-t');
    assert.equal(metadata.partition, RecordMetadata.UNKNOWN_PARTITION);
    assert.equal(metadata.hasOffset, false);
    assert.equal(metadata.offset, -1);
    assert.equal(metadata.toString(), 'unit-t--1@-1');
    assert.equal(error.message, TIMEOUT_MESSAGE('unit-t'));
    assert.equal(calls.length, 1);
  } finally {
    await producer.close();
  }
});

test('a send without a callback rejects the same way', async () => {
  const producer = new KafkaProducer(UNREACHABLE);
  try {
    await assert.rejects(producer.send(new ProducerRecord({ topic: 'unit-u', value: Buffer.from([0]) })), {
      name: 'REQUEST_TIMED_OUT',
      message: TIMEOUT_MESSAGE('unit-u'),
    });
  } finally {
    await producer.close();
  }
});

test('a throwing callback does not change the outcome', async () => {
  const producer = new KafkaProducer(UNREACHABLE);
  try {
    let ran = 0;
    const promise = producer.send(new ProducerRecord({ topic: 'unit-x', value: 'v' }), () => {
      ran++;
      throw new Error('boom');
    });
    await assert.rejects(promise, { name: 'REQUEST_TIMED_OUT', message: TIMEOUT_MESSAGE('unit-x') });
    assert.equal(ran, 1);
  } finally {
    await producer.close();
  }
});

test('flush inside a callback rejects with the core error', async () => {
  const producer = new KafkaProducer(UNREACHABLE);
  try {
    let flushed;
    const sent = producer.send(new ProducerRecord({ topic: 'unit-f', value: 'v' }), () => {
      flushed = producer.flush();
    });
    await assert.rejects(sent, { name: 'REQUEST_TIMED_OUT' });
    assert.ok(flushed instanceof Promise);
    await assert.rejects(flushed, (e) => {
      assert.ok(e instanceof KafkaError);
      assert.equal(e.message, FLUSH_IN_CALLBACK);
      return true;
    });
    // Outside a callback it completes.
    await producer.flush();
  } finally {
    await producer.close();
  }
});

test('after close: send throws, flush resolves, close is idempotent', async () => {
  const producer = new KafkaProducer(UNREACHABLE);
  const first = producer.close();
  // While closing too.
  assert.equal(producer.close(), first);
  assert.throws(() => producer.send(new ProducerRecord({ topic: 't', value: 'v' })), (e) => {
    assert.ok(e instanceof KafkaError);
    assert.equal(e.name, 'LOCAL_ILLEGAL_STATE');
    assert.equal(e.code, -4);
    assert.equal(e.message, CLOSED_MESSAGE);
    return true;
  });
  await first;
  await producer.close();
  await producer.close({ timeoutMs: 0 });
  assert.throws(() => producer.send(new ProducerRecord({ topic: 't', value: 'v' })), { message: CLOSED_MESSAGE });
  // Java's flush() after close returns at once: nothing is left to send.
  await producer.flush();
});

test('a negative close timeout is rejected by the core and closes nothing', async () => {
  const producer = new KafkaProducer(UNREACHABLE);
  await assert.rejects(producer.close({ timeoutMs: -1 }), (e) => {
    assert.ok(e instanceof KafkaError);
    assert.equal(e.name, 'LOCAL_ILLEGAL_ARGUMENT');
    assert.equal(e.message, 'The timeout cannot be negative.');
    return true;
  });
  // Still open.
  const sent = producer.send(new ProducerRecord({ topic: 'unit-n', value: 'v' }), () => {});
  await producer.close({ timeoutMs: 0 });
  await assert.rejects(sent, KafkaError);
});

test('argument errors throw synchronously', async () => {
  const producer = new KafkaProducer(UNREACHABLE);
  try {
    assert.throws(() => producer.send({ topic: 't' }), { name: 'TypeError', message: 'record must be a ProducerRecord' });
    assert.throws(() => producer.send(new ProducerRecord({ topic: 't' }), 'cb'), {
      name: 'TypeError',
      message: 'callback must be a function (metadata, error) => void',
    });
    assert.throws(() => producer.close({ timeoutMs: 1.5 }), { name: 'TypeError', message: 'timeoutMs must be an integer' });
    assert.throws(() => new KafkaProducer('bootstrap.servers=x'), { name: 'TypeError' });
  } finally {
    await producer.close();
  }
});

// Runs `script` in a fresh Node process, returning its exit status, output and
// wall time.
function runNode(script) {
  const started = process.hrtime.bigint();
  const r = spawnSync(process.execPath, ['-e', script], {
    cwd: path.join(__dirname, '..', '..'),
    encoding: 'utf8',
    timeout: 30000,
  });
  return { ...r, ms: Number((process.hrtime.bigint() - started) / 1000000n) };
}

test('an idle producer lets the process exit', () => {
  const r = runNode(`
    const { KafkaProducer } = require('.');
    new KafkaProducer({ 'bootstrap.servers': '127.0.0.1:1' });
    console.log('created');
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'created\n');
  assert.ok(r.ms < 10000, `took ${r.ms} ms`);
});

test('a pending send keeps the process alive until it completes', () => {
  const r = runNode(`
    const { KafkaProducer, ProducerRecord } = require('.');
    const p = new KafkaProducer({ 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 1500 });
    p.send(new ProducerRecord({ topic: 'alive', value: 'v' }), (m, e) => console.log('callback:', e.message));
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'callback: Topic alive not present in metadata after 1500 ms.\n');
  assert.ok(r.ms >= 1400, `exited after ${r.ms} ms`);
});

test('terminating a worker with records in flight tears its producer down cleanly', () => {
  // Environment teardown finalizes the threadsafe function while the producer
  // still runs and its tasks still fire the notify hook (COMMENTS.100 item 5).
  const r = runNode(`
    const { Worker } = require('node:worker_threads');
    const body = \`
      const { parentPort } = require('node:worker_threads');
      const { KafkaProducer, ProducerRecord } = require(\${JSON.stringify(process.cwd())});
      const p = new KafkaProducer({ 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 50, 'linger.ms': 0 });
      for (let i = 0; i < 200; i++) {
        p.send(new ProducerRecord({ topic: 'teardown', value: String(i) }), () => {}).catch(() => {});
      }
      parentPort.postMessage('sent');
    \`;
    (async () => {
      for (let round = 0; round < 20; round++) {
        const w = new Worker(body, { eval: true });
        await new Promise((res, rej) => { w.once('message', res); w.once('error', rej); });
        await new Promise((res) => setTimeout(res, round % 4 * 20));
        await w.terminate();
      }
      console.log('done');
    })().catch((e) => { console.error(e); process.exit(1); });
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'done\n');
});
