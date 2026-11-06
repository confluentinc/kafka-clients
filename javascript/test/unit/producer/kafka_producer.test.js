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

const { IllegalArgumentError, IllegalStateError } = require('@confluentinc/kafka-javascript');
const { KafkaError, KafkaFuture } = require('@confluentinc/kafka-javascript/common');
const { KafkaProducer, ProducerRecord, RecordMetadata } = require('@confluentinc/kafka-javascript/producer');

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
const BOOTSTRAP_MESSAGE =
  'Failed to resolve bootstrap servers after 1000ms. Please check your bootstrap.servers configuration and DNS settings.';
const FLUSH_IN_CALLBACK =
  'KafkaProducer.flush() invocation inside a callback is not permitted because it may lead to deadlock.';

test('send resolves at hand-over; the future fails after the callback ran once', async () => {
  const producer = new KafkaProducer(UNREACHABLE);
  try {
    const calls = [];
    const sent = producer.send(new ProducerRecord({ topic: 'unit-t', key: 'k', value: 'v' }), (metadata, error) =>
      calls.push({ metadata, error }),
    );
    assert.ok(sent instanceof Promise);
    // Java's send() returns after max.block.ms with a failed future (its
    // TimeoutException is an ApiException): the callback has run by then.
    const future = await sent;
    assert.ok(future instanceof KafkaFuture);
    assert.equal(calls.length, 1);
    assert.equal(future.isDone(), true);
    assert.equal(future.isCompletedExceptionally(), true);
    assert.equal(future.isCancelled(), false);
    assert.equal(future.cancel(true), false);
    assert.equal(future.isCancelled(), false);
    await assert.rejects(future.get(), (e) => {
      assert.ok(e instanceof KafkaError);
      assert.equal(e.name, 'TimeoutError');
      assert.equal(e.errorName, 'REQUEST_TIMED_OUT');
      assert.equal(e.code, 7);
      assert.equal(e.message, TIMEOUT_MESSAGE('unit-t'));
      assert.equal(e.retriable, true);
      assert.equal(e.apiError, true);
      return true;
    });
    // get() again rejects the same way.
    await assert.rejects(future.get(), { name: 'TimeoutError', errorName: 'REQUEST_TIMED_OUT', message: TIMEOUT_MESSAGE('unit-t') });
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

test('a send without a callback fails its future the same way', async () => {
  const producer = new KafkaProducer(UNREACHABLE);
  try {
    const future = await producer.send(new ProducerRecord({ topic: 'unit-u', value: Buffer.from([0]) }));
    await assert.rejects(future.get(), {
      name: 'TimeoutError',
      errorName: 'REQUEST_TIMED_OUT',
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
    const future = await producer.send(new ProducerRecord({ topic: 'unit-x', value: 'v' }), () => {
      ran++;
      throw new Error('boom');
    });
    await assert.rejects(future.get(), { name: 'TimeoutError', errorName: 'REQUEST_TIMED_OUT', message: TIMEOUT_MESSAGE('unit-x') });
    assert.equal(ran, 1);
  } finally {
    await producer.close();
  }
});

test('flush inside a callback rejects with the core error', async () => {
  const producer = new KafkaProducer(UNREACHABLE);
  try {
    let flushed;
    const future = await producer.send(new ProducerRecord({ topic: 'unit-f', value: 'v' }), () => {
      flushed = producer.flush();
    });
    await assert.rejects(future.get(), { name: 'TimeoutError', errorName: 'REQUEST_TIMED_OUT', message: TIMEOUT_MESSAGE('unit-f') });
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

test('a hand-over failure rejects send() and never reaches the callback', async () => {
  const producer = new KafkaProducer({
    'bootstrap.servers': 'does-not-exist.invalid:9092',
    'bootstrap.resolve.timeout.ms': 1000,
  });
  try {
    let calls = 0;
    await assert.rejects(
      producer.send(new ProducerRecord({ topic: 'unit-h', value: 'v' }), () => calls++),
      (e) => {
        assert.ok(e instanceof KafkaError);
        assert.equal(e.name, 'BootstrapResolutionError');
        assert.equal(e.errorName, 'BOOTSTRAP_RESOLUTION');
        assert.equal(e.message, BOOTSTRAP_MESSAGE);
        return true;
      },
    );
    assert.equal(calls, 0);
  } finally {
    await producer.close();
  }
});

test('KafkaFuture is made only by the client', () => {
  assert.throws(() => new KafkaFuture(), { name: 'TypeError', message: 'KafkaFuture is not constructible' });
});

test('after close: send throws, flush resolves, close is idempotent', async () => {
  const producer = new KafkaProducer(UNREACHABLE);
  const first = producer.close();
  // While closing too.
  assert.equal(producer.close(), first);
  assert.throws(() => producer.send(new ProducerRecord({ topic: 't', value: 'v' })), (e) => {
    // Java's IllegalStateException is a java.lang exception, not a KafkaException.
    assert.ok(e instanceof IllegalStateError);
    assert.ok(!(e instanceof KafkaError));
    assert.equal(e.name, 'IllegalStateError');
    assert.equal(e.errorName, 'LOCAL_ILLEGAL_STATE');
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

test("close checks its timeout as Java's close(Duration) does, whatever the state", async () => {
  const negative = (e) => {
    // Java's IllegalArgumentException is a java.lang exception, not a KafkaException.
    assert.ok(e instanceof IllegalArgumentError);
    assert.ok(!(e instanceof KafkaError));
    assert.equal(e.name, 'IllegalArgumentError');
    assert.equal(e.errorName, 'LOCAL_ILLEGAL_ARGUMENT');
    assert.equal(e.code, -3);
    assert.equal(e.message, 'The timeout cannot be negative.');
    return true;
  };
  const producer = new KafkaProducer(UNREACHABLE);
  const first = producer.close();
  // While a close is in progress.
  await assert.rejects(producer.close({ timeoutMs: -1 }), negative);
  assert.throws(() => producer.close({ timeoutMs: 1.5 }), { name: 'TypeError', message: 'timeoutMs must be an integer' });
  await first;
  // And once closed.
  await assert.rejects(producer.close({ timeoutMs: -1 }), negative);
  assert.throws(() => producer.close({ timeoutMs: 1.5 }), { name: 'TypeError', message: 'timeoutMs must be an integer' });
  assert.throws(() => producer.close(5), { name: 'TypeError', message: 'close takes an options object {timeoutMs}' });
  await producer.close({ timeoutMs: 0 });
});

test('a negative close timeout on an open producer is rejected first and changes nothing', async () => {
  const negative = (e) => {
    // Java's IllegalArgumentException is a java.lang exception, not a KafkaException.
    assert.ok(e instanceof IllegalArgumentError);
    assert.ok(!(e instanceof KafkaError));
    assert.equal(e.name, 'IllegalArgumentError');
    assert.equal(e.errorName, 'LOCAL_ILLEGAL_ARGUMENT');
    assert.equal(e.code, -3);
    assert.equal(e.message, 'The timeout cannot be negative.');
    return true;
  };
  // Java's close(Duration) throws before touching the producer: a send() in
  // the same turn is still accepted.
  const producer = new KafkaProducer(UNREACHABLE);
  const rejected = producer.close({ timeoutMs: -1 });
  const sent = producer.send(new ProducerRecord({ topic: 'unit-n', value: 'v' }));
  await assert.rejects(rejected, negative);
  // The record is handed over (it fails only for the missing metadata, as any
  // record to this broker does).
  const future = await sent;
  await producer.close();
  await assert.rejects(future.get(), (e) => {
    assert.ok(e instanceof KafkaError);
    assert.equal(e.name, 'TimeoutError');
    assert.equal(e.errorName, 'REQUEST_TIMED_OUT');
    assert.equal(e.message, TIMEOUT_MESSAGE('unit-n'));
    return true;
  });
  assert.throws(() => producer.send(new ProducerRecord({ topic: 'unit-n', value: 'v' })), { message: CLOSED_MESSAGE });

  // A close() in the same turn is a close of its own, not the rejected one.
  const other = new KafkaProducer(UNREACHABLE);
  const rejectedToo = other.close({ timeoutMs: -1 });
  const closed = other.close();
  await assert.rejects(rejectedToo, negative);
  await closed;
  assert.throws(() => other.send(new ProducerRecord({ topic: 'unit-n', value: 'v' })), { message: CLOSED_MESSAGE });
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

test('a string holding a NUL is refused where the C ABI would truncate it', async () => {
  // A C reader stops at the first NUL: 'acks\0x' would read as 'acks', and
  // 'good\0bad' as the topic 'good'.
  assert.throws(() => new KafkaProducer({ ...UNREACHABLE, 'acks\0ignored': 'all' }), {
    name: 'TypeError',
    message: 'config keys must be strings without null bytes',
  });
  assert.throws(() => new KafkaProducer({ ...UNREACHABLE, acks: '0\0bogus' }), {
    name: 'TypeError',
    message: 'config values must be strings without null bytes',
  });
  const producer = new KafkaProducer(UNREACHABLE);
  const calls = [];
  const callback = () => calls.push('callback');
  assert.throws(() => producer.send(new ProducerRecord({ topic: 'good\0bad', value: 'v' }), callback), {
    name: 'TypeError',
    message: 'topic must be a string without null bytes',
  });
  assert.throws(
    () => producer.send(new ProducerRecord({ topic: 'unit-nul', value: 'v', headers: [{ key: 'k\0x', value: 'h' }] }), callback),
    { name: 'TypeError', message: 'a header key must be a string without null bytes' },
  );
  // Neither send left anything pending: the close has nothing to wait for and
  // no callback ran.
  const started = Date.now();
  await producer.close();
  assert.ok(Date.now() - started < MAX_BLOCK_MS * 10, `close took ${Date.now() - started} ms`);
  assert.deepEqual(calls, []);
});

test('a refused NUL-holding send leaves an unclosed producer idle', () => {
  const r = runNode(`
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    const p = new KafkaProducer({ 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 60000 });
    for (const record of [
      new ProducerRecord({ topic: 'a\\0b', value: 'v' }),
      new ProducerRecord({ topic: 'idle', value: 'v', headers: [{ key: 'a\\0b', value: 'v' }] }),
    ]) {
      try { p.send(record, () => console.log('callback ran')); } catch (e) { console.log(e.name, e.message); }
    }
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(
    r.stdout,
    'TypeError topic must be a string without null bytes\n' +
      'TypeError a header key must be a string without null bytes\n',
  );
  assert.ok(r.ms < 30000, `took ${r.ms} ms`);
});

// Runs `script` in a fresh Node process (killed after `timeout` ms), returning
// its exit status, output and wall time.
function runNode(script, timeout = 30000) {
  const started = process.hrtime.bigint();
  const r = spawnSync(process.execPath, ['-e', script], {
    cwd: path.join(__dirname, '..', '..', '..'),
    encoding: 'utf8',
    timeout,
  });
  return { ...r, ms: Number((process.hrtime.bigint() - started) / 1000000n) };
}

test('an idle producer lets the process exit', () => {
  const r = runNode(`
    const { KafkaProducer } = require('@confluentinc/kafka-javascript/producer');
    new KafkaProducer({ 'bootstrap.servers': '127.0.0.1:1' });
    console.log('created');
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'created\n');
  assert.ok(r.ms < 10000, `took ${r.ms} ms`);
});

test('a pending send keeps the process alive until it completes', () => {
  const r = runNode(`
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    const p = new KafkaProducer({ 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 1500 });
    p.send(new ProducerRecord({ topic: 'alive', value: 'v' }), (m, e) => console.log('callback:', e.message));
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'callback: Topic alive not present in metadata after 1500 ms.\n');
  assert.ok(r.ms >= 1400, `exited after ${r.ms} ms`);
});

test('terminating a worker with records in flight tears its producer down cleanly', () => {
  // Environment teardown finalizes the threadsafe function while the producer
  // still runs and its tasks still fire the notify hook: a hook call racing
  // the finalizer must not reach the released threadsafe function.
  const r = runNode(`
    const { Worker } = require('node:worker_threads');
    const body = \`
      const { parentPort } = require('node:worker_threads');
      const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
      const p = new KafkaProducer({ 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 50, 'linger.ms': 0 });
      for (let i = 0; i < 200; i++) {
        p.send(new ProducerRecord({ topic: 'teardown', value: String(i) }), () => {});
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

test('terminating a worker whose unclosed producer holds an unsent record is prompt', () => {
  // D11: environment teardown finalizes a producer close() was never called
  // on. Its destroy would close it gracefully first, waiting up to
  // max.block.ms + delivery.timeout.ms (3 min by default) for a record no
  // broker will take; the finalizer force-closes it (timeout 0) instead, so
  // the record fails promptly (the connection to 127.0.0.1:1 is refused, so
  // the I/O task is in no long wait).
  const r = runNode(
    `
    const { Worker } = require('node:worker_threads');
    const body = \`
      const { parentPort } = require('node:worker_threads');
      const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
      const p = new KafkaProducer({ 'bootstrap.servers': '127.0.0.1:1' });
      p.send(new ProducerRecord({ topic: 'unclosed', value: 'v' }), () => {}).catch(() => {});
      parentPort.postMessage('sent');
    \`;
    (async () => {
      const w = new Worker(body, { eval: true });
      await new Promise((res, rej) => { w.once('message', res); w.once('error', rej); });
      const started = Date.now();
      await w.terminate();
      const ms = Date.now() - started;
      console.log(ms < 2000 ? 'terminated promptly' : \`terminated after \${ms} ms\`);
    })().catch((e) => { console.error(e); process.exit(1); });
  `,
    15000,
  );
  assert.equal(r.error, undefined, `the process did not exit: ${r.error}`);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'terminated promptly\n');
});

test('an un-awaited send whose hand-over fails surfaces as an unhandled rejection', () => {
  const r = runNode(`
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    process.on('unhandledRejection', (e) => {
      console.log('unhandled:', e.name, e.message);
      p.close();
    });
    const p = new KafkaProducer({ 'bootstrap.servers': 'does-not-exist.invalid:9092', 'bootstrap.resolve.timeout.ms': 1000 });
    p.send(new ProducerRecord({ topic: 'h', value: 'v' }), () => console.log('callback ran'));
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, `unhandled: BootstrapResolutionError ${BOOTSTRAP_MESSAGE}\n`);
});

test('a failed future whose get() is never called raises no unhandled rejection', () => {
  const r = runNode(`
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    process.on('unhandledRejection', (e) => console.log('unhandled:', e.message));
    (async () => {
      const p = new KafkaProducer({ 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 200 });
      const future = await p.send(new ProducerRecord({ topic: 'unread', value: 'v' }));
      await p.close();
      console.log('done', future.isCompletedExceptionally());
    })();
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'done true\n');
});
