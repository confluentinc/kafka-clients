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

const { Temporal } = require('@js-temporal/polyfill');

const { ExecutionError, IllegalArgumentError, IllegalStateError } = require('@confluentinc/kafka-javascript');
const { KafkaError, KafkaFuture } = require('@confluentinc/kafka-javascript/common');
const errors = require('@confluentinc/kafka-javascript/common/errors');
const { Header } = require('@confluentinc/kafka-javascript/common/header');
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
/**
 * A failed record's delivery error: its future's get() rejects with Java's
 * ExecutionError, whose message is the cause's String(), and whose cause is
 * the error the delivery failed with, which this rejects with.
 */
function delivery(future) {
  return future.get().catch((e) => {
    assert.equal(e.constructor, ExecutionError);
    assert.equal(e.message, String(e.cause));
    throw e.cause;
  });
}

/**
 * Runs `body` with the event loop held by a timer: a closed producer no longer
 * keeps Node running (javascript/CLAUDE.md, "Threads and liveness"), so a test
 * awaiting a call made after close() holds the loop itself, as an app's own
 * work would.
 */
async function holdingTheLoop(body) {
  const hold = setInterval(() => {}, 1000);
  try {
    return await body();
  } finally {
    clearInterval(hold);
  }
}

const CTOR_FORMS = 'KafkaProducer() takes one of (configs), (configs, keySerializer, valueSerializer)';

const FLUSH_IN_CALLBACK =
  'KafkaProducer.flush() invocation inside a callback is not permitted because it may lead to deadlock.';

test('send resolves at hand-over; the future fails after the callback ran once', async () => {
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  try {
    const calls = [];
    const sent = producer.send({
      record: new ProducerRecord({ topic: 'unit-t', key: 'k', value: 'v' }),
      // A block body: a callback that returns a value is awaited (rule 6).
      callback: ({ metadata, exception }) => {
        calls.push({ metadata, error: exception });
      },
    });
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
    await assert.rejects(delivery(future), (e) => {
      assert.ok(e instanceof KafkaError);
      // Classified with instanceof: Kafka's TimeoutException is a RetriableException, an ApiException.
      assert.ok(e instanceof errors.TimeoutError);
      assert.ok(e instanceof errors.RetriableError && e instanceof errors.ApiError);
      assert.equal(e.name, 'TimeoutError');
      assert.equal(e.message, TIMEOUT_MESSAGE('unit-t'));
      assert.ok(!('code' in e) && !('retriable' in e));
      return true;
    });
    // get() again rejects the same way.
    await assert.rejects(delivery(future), { name: 'TimeoutError', message: TIMEOUT_MESSAGE('unit-t') });
    const [{ metadata, error }] = calls;
    // The callback gets the raw error; get() and getNow() wrap that very error.
    await assert.rejects(future.get(), (e) => e instanceof ExecutionError && e.cause === error);
    assert.throws(() => future.getNow(null), (e) => e instanceof ExecutionError && e.cause === error);
    assert.ok(metadata instanceof RecordMetadata);
    assert.equal(metadata.topic, 'unit-t');
    assert.equal(metadata.partition, RecordMetadata.UNKNOWN_PARTITION);
    assert.equal(metadata.hasOffset, false);
    // Java's longs: bigints.
    assert.equal(metadata.offset, -1n);
    assert.equal(metadata.timestamp, -1n);
    assert.equal(metadata.toString(), 'unit-t--1@-1');
    // The fields are data: a spread or JSON copy keeps every one, the longs
    // as decimal strings in JSON.
    assert.deepEqual(JSON.parse(JSON.stringify(metadata)), { ...metadata, offset: '-1', timestamp: '-1' });
    assert.equal(Object.keys(metadata).length, 8);
    assert.equal(error.message, TIMEOUT_MESSAGE('unit-t'));
    assert.equal(calls.length, 1);
  } finally {
    await producer.close();
  }
});

test('a send without a callback fails its future the same way', async () => {
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  try {
    const future = await producer.send({ record: new ProducerRecord({ topic: 'unit-u', value: Buffer.from([0]) }) });
    await assert.rejects(delivery(future), {
      name: 'TimeoutError',
      message: TIMEOUT_MESSAGE('unit-u'),
    });
  } finally {
    await producer.close();
  }
});

test('a throwing callback does not change the outcome', async () => {
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  try {
    let ran = 0;
    const future = await producer.send({
      record: new ProducerRecord({ topic: 'unit-x', value: 'v' }),
      callback: () => {
        ran++;
        throw new Error('boom');
      },
    });
    await assert.rejects(delivery(future), { name: 'TimeoutError', message: TIMEOUT_MESSAGE('unit-x') });
    assert.equal(ran, 1);
  } finally {
    await producer.close();
  }
});

test('a callback that returns a Promise is awaited: the future and flush() wait for it', async () => {
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  try {
    let release;
    const released = new Promise((res) => (release = res));
    let seen = null;
    const future = await producer.send({
      record: new ProducerRecord({ topic: 'unit-async', value: 'v' }),
      callback: async ({ metadata, exception }) => {
        seen = { metadata, exception };
        await released;
      },
    });
    // The callback ran (Java's send() returned after max.block.ms), and is
    // still pending: neither the future nor a flush covering it is done.
    assert.ok(seen !== null);
    assert.equal(seen.exception.message, TIMEOUT_MESSAGE('unit-async'));
    assert.equal(future.isDone(), false);
    let flushed = false;
    const flush = producer.flush().then(() => (flushed = true));
    await new Promise((res) => setTimeout(res, 200));
    assert.equal(future.isDone(), false);
    assert.equal(flushed, false);
    release();
    await flush;
    assert.equal(future.isDone(), true);
    await assert.rejects(delivery(future), (e) => e === seen.exception);
  } finally {
    await producer.close();
  }
});

test("a callback's rejection is handled as a throw: the outcome is unchanged and nothing surfaces", () => {
  const r = runNode(`
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    process.on('unhandledRejection', (e) => console.log('unhandled:', e && e.message));
    (async () => {
      const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 200 } });
      const future = await p.send({
        record: new ProducerRecord({ topic: 'rejects', value: 'v' }),
        callback: () => Promise.reject(new Error('boom')),
      });
      await p.flush();
      const outcome = await future.get().then(() => 'delivered', (e) => e.cause.name);
      // A thenable that is no Promise, and a then getter that throws, too.
      const thenable = await p.send({ record: new ProducerRecord({ topic: 'rejects', value: 'v' }), callback: () => ({ then: (ok, fail) => fail(1) }) });
      const getter = await p.send({ record: new ProducerRecord({ topic: 'rejects', value: 'v' }), callback: () => ({ get then() { throw new Error('getter'); } }) });
      await p.close();
      console.log(outcome, thenable.isDone(), getter.isDone());
    })();
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'TimeoutError true true\n');
});

test('a callback that throws or rejects with a value that cannot be read still completes its future', () => {
  const r = runNode(`
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    process.on('unhandledRejection', (e) => console.log('unhandled'));
    process.on('uncaughtException', (e) => console.log('uncaught'));
    (async () => {
      const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 200 } });
      const revoked = () => { const { proxy, revoke } = Proxy.revocable({}, {}); revoke(); return proxy; };
      const badMessage = () => { const e = new Error('x'); Object.defineProperty(e, 'message', { get() { throw new Error('getter'); } }); return e; };
      const symbolName = () => { const e = new Error('x'); e.name = Symbol('n'); return e; };
      const values = [revoked, badMessage, symbolName];
      const futures = [];
      for (const make of values) {
        futures.push(await p.send({ record: new ProducerRecord({ topic: 'exotic', value: 'v' }), callback: () => Promise.reject(make()) }));
        futures.push(await p.send({ record: new ProducerRecord({ topic: 'exotic', value: 'v' }), callback: () => { throw make(); } }));
      }
      await p.flush();
      await p.close();
      console.log(futures.map((f) => f.isDone()).join(' '));
    })();
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'true true true true true true\n');
});

test('a never-settling callback Promise nothing references is not reported at a garbage collection', () => {
  // Held strongly until it reports: a collection neither completes the
  // record's future nor lets flush() return.
  const r = runNode(
    `
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    (async () => {
      const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 100 } });
      const future = await p.send({
        record: new ProducerRecord({ topic: 'never', value: 'v' }),
        callback: async () => { await new Promise(() => {}); },
      });
      let flushed = false;
      p.flush().then(() => (flushed = true));
      for (let i = 0; i < 4; i++) {
        global.gc();
        await new Promise((res) => setTimeout(res, 50));
      }
      console.log('flushed', flushed, 'done', future.isDone());
      process.exit(0);
    })();
  `,
    30000,
    ['--expose-gc'],
  );
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'flushed false done false\n');
});

test('close() waits for a pending callback Promise, and [Symbol.asyncDispose] closes as close() does', async () => {
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  let release;
  const released = new Promise((res) => (release = res));
  await producer.send({ record: new ProducerRecord({ topic: 'unit-dispose', value: 'v' }), callback: () => released });
  let closed = false;
  const closing = producer[Symbol.asyncDispose]().then(() => (closed = true));
  assert.ok(closing instanceof Promise);
  await new Promise((res) => setTimeout(res, 200));
  assert.equal(closed, false);
  release();
  await closing;
  await assert.rejects(producer.send(new ProducerRecord({ topic: 't', value: 'v' })), { name: 'IllegalStateError', message: CLOSED_MESSAGE });
  // Disposing again resolves, as close() again does: the core's second
  // close, whose completion a closed producer no longer holds Node for.
  await holdingTheLoop(() => producer[Symbol.asyncDispose]());
});

test('flush inside a callback rejects with the core error', async () => {
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  try {
    let flushed;
    const future = await producer.send({
      record: new ProducerRecord({ topic: 'unit-f', value: 'v' }),
      callback: () => {
        flushed = producer.flush();
      },
    });
    await assert.rejects(delivery(future), { name: 'TimeoutError', message: TIMEOUT_MESSAGE('unit-f') });
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
    configs: { 'bootstrap.servers': 'does-not-exist.invalid:9092', 'bootstrap.resolve.timeout.ms': 1000 },
  });
  try {
    let calls = 0;
    await assert.rejects(
      producer.send({ record: new ProducerRecord({ topic: 'unit-h', value: 'v' }), callback: () => calls++ }),
      (e) => {
        assert.ok(e instanceof KafkaError);
        assert.ok(e instanceof errors.BootstrapResolutionError);
        assert.equal(e.name, 'BootstrapResolutionError');
        assert.equal(e.message, BOOTSTRAP_MESSAGE);
        return true;
      },
    );
    assert.equal(calls, 0);
  } finally {
    await producer.close();
  }
});

// The constructor is TypeScript-private and the factory @internal (left out
// of the declarations, test/typing); there is no run-time guard.
test('KafkaFuture is made by the client, through its library-only factory', () => {
  const future = KafkaFuture.internalCreate(true);
  assert.ok(future instanceof KafkaFuture);
  assert.equal(future.isDone(), false);
  future.internalComplete('v', null);
  future.internalComplete('w', null);
  assert.equal(future.isDone(), true);
  return future.get().then((v) => assert.equal(v, 'v'));
});

// A ProducerRecord field reassigned from plain JavaScript after the
// constructor checked it: send() rejects with the constructor's error, and
// nothing reaches the core.
test("after close every call goes to the core: send rejects with its closed error, flush and close resolve", async () => {
  const closedError = (e) => {
    // Java's IllegalStateException is a java.lang exception, not a KafkaException.
    assert.ok(e instanceof IllegalStateError);
    assert.ok(!(e instanceof KafkaError));
    assert.equal(e.name, 'IllegalStateError');
    assert.equal(e.message, CLOSED_MESSAGE);
    return true;
  };
  await holdingTheLoop(async () => {
    const producer = new KafkaProducer({ configs: UNREACHABLE });
    const first = producer.close();
    // A second close while the first runs is a close of its own, in the core.
    const second = producer.close();
    assert.notEqual(second, first);
    await first;
    // Sent once the close was awaited (callers await, javascript/CLAUDE.md):
    // a send racing the close may get either of Java's outcomes.
    const afterClose = producer.send({ record: new ProducerRecord({ topic: 't', value: 'v' }) });
    assert.ok(afterClose instanceof Promise);
    await assert.rejects(afterClose, closedError);
    // Java's flush() has no closed check: the core's flush resolves.
    await producer.flush();
    await second;
    await producer.close();
    await producer.close({ timeout: 0 });
    await assert.rejects(producer.send({ record: new ProducerRecord({ topic: 't', value: 'v' }) }), closedError);
    await producer.flush();
  });
});

test('close checks its timeout, open or closed: the closed state is the core\'s', async () => {
  const negative = (e) => {
    // Java's IllegalArgumentException is a java.lang exception, not a KafkaException.
    assert.ok(e instanceof IllegalArgumentError);
    assert.ok(!(e instanceof KafkaError));
    assert.equal(e.name, 'IllegalArgumentError');
    assert.ok(e instanceof RangeError);
    assert.equal(e.message, 'The timeout cannot be negative.');
    return true;
  };
  const notADuration = { name: 'TypeError', message: 'timeout must be a number of milliseconds or a Temporal.Duration' };
  // Each refused timeout changes nothing.
  const checks = async (producer) => {
    await assert.rejects(producer.close({ timeout: -1 }), negative);
    await assert.rejects(producer.close(-1), negative);
    await assert.rejects(producer.close(-0.5), negative);
    await assert.rejects(producer.close({ timeout: Temporal.Duration.from({ milliseconds: -1 }) }), negative);
    await assert.rejects(producer.close(Temporal.Duration.from({ milliseconds: -1 })), negative);
    await assert.rejects(producer.close({ timeout: '5' }), notADuration);
    // A bare value that is neither a number nor a Temporal.Duration fits no form.
    await assert.rejects(producer.close('5'), {
      name: 'IllegalArgumentError',
      message: 'close() takes one of (), (timeout); got a string',
    });
    await assert.rejects(producer.close({ timeout: null }), { name: 'TypeError', message: 'timeout must not be null' });
    await assert.rejects(producer.close(null), { name: 'TypeError', message: 'timeout must not be null' });
    await assert.rejects(producer.close({ timeout: undefined }), {
      name: 'TypeError',
      message: "timeout must not be undefined. Leave the key out, or pass null for Java's null",
    });
    await assert.rejects(producer.close(undefined), {
      name: 'TypeError',
      message: "timeout must not be undefined. Leave the argument out, or pass null for Java's null",
    });
    for (const value of [NaN, -Infinity]) {
      await assert.rejects(producer.close(value), { name: 'RangeError', message: `timeout must be a number of milliseconds, not ${value}` });
    }
    await assert.rejects(producer.close({ timeout: 2 ** 53 }), {
      name: 'RangeError',
      message: 'timeout of 9007199254740992 ms is above Number.MAX_SAFE_INTEGER ms; pass Infinity for no limit',
    });
    await assert.rejects(producer.close({ timeout: Temporal.Duration.from({ months: 1 }) }), (e) => {
      assert.equal(e.constructor, RangeError);
      assert.equal(e.message, 'timeout must not hold years, months or weeks, which have no fixed length');
      return true;
    });
  };
  await holdingTheLoop(async () => {
    const producer = new KafkaProducer({ configs: UNREACHABLE });
    await checks(producer);
    // The producer is still open.
    await producer.flush();
    await producer.close();
    // Closed: the same checks, the same errors.
    await checks(producer);
  });
  // Accepted: a whole or fractional number, Infinity, a Temporal.Duration, bare or named.
  for (const timeout of [{ timeout: 0 }, 1.5, Infinity, { timeout: Temporal.Duration.from({ seconds: 1 }) }, Temporal.Duration.from({ seconds: 1 })]) {
    await new KafkaProducer({ configs: UNREACHABLE }).close(timeout);
  }
});

test("close(Infinity) is no limit: Java's Long.MAX_VALUE milliseconds, as close() is", () => {
  // Java's close() is close(Duration.ofMillis(Long.MAX_VALUE)). The core logs
  // the timeout it closes with (less the time its submission drain took, for
  // a timed close), at the RUST_LOG level read when its first client is built,
  // so a fresh process closes the producers.
  const r = spawnSync(
    process.execPath,
    [
      '-e',
      `
      const { KafkaProducer } = require('@confluentinc/kafka-javascript/producer');
      (async () => {
        for (const timeout of [undefined, Infinity, { timeout: Infinity }]) {
          const p = new KafkaProducer({ 'bootstrap.servers': '127.0.0.1:1' });
          await (timeout === undefined ? p.close() : p.close(timeout));
        }
        console.log('closed');
      })();
    `,
    ],
    { cwd: path.join(__dirname, '..', '..', '..'), encoding: 'utf8', timeout: 60000, env: { ...process.env, RUST_LOG: 'info' } },
  );
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'closed\n');
  const logged = [...r.stderr.matchAll(/Closing the Kafka producer with timeoutMillis = (\d+) ms\./g)].map((m) => BigInt(m[1]));
  assert.equal(logged.length, 3, r.stderr);
  const LONG_MAX = 9223372036854775807n;
  assert.equal(logged[0], LONG_MAX);
  // A timed close: Long.MAX_VALUE less the drain's few milliseconds.
  for (const ms of logged.slice(1)) assert.ok(ms <= LONG_MAX && LONG_MAX - ms < 60000n, `closed with ${ms} ms`);
});

test("close and send take Java's forms, the one-parameter ones bare too, and reject the others", async () => {
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  try {
    const record = new ProducerRecord({ topic: 'unit-forms', value: 'v' });
    for (const [args, given] of [
      [{ timeoutMs: 0 }, 'timeoutMs'],
      [{ timeout: 0, timeoutMs: 0 }, 'timeout, timeoutMs'],
    ]) {
      const closed = producer.close(args);
      assert.ok(closed instanceof Promise);
      await assert.rejects(closed, {
        name: 'IllegalArgumentError',
        message: `close() takes one of (), (timeout); got (${given})`,
      });
    }
    // send() with no argument at all; send(undefined) names the record.
    await assert.rejects(producer.send(), {
      name: 'IllegalArgumentError',
      message: 'send() takes one of (record), (record, callback); got ()',
    });
    await assert.rejects(producer.send(undefined), {
      name: 'TypeError',
      message: "record must not be undefined. Leave the argument out, or pass null for Java's null",
    });
    // A bare value that is no ProducerRecord fits no form.
    await assert.rejects(producer.send(5), {
      name: 'IllegalArgumentError',
      message: 'send() takes one of (record), (record, callback); got a number',
    });
    // Java's send(record, callback) is send({record, callback}): a second
    // argument is refused, never dropped with its callback.
    let ran = 0;
    const twoArgs = producer.send(record, () => ran++);
    assert.ok(twoArgs instanceof Promise);
    await assert.rejects(twoArgs, {
      name: 'IllegalArgumentError',
      message: 'send() takes one of (record), (record, callback); got 2 arguments, not one object of named parameters',
    });
    assert.equal(ran, 0);
    // Extra arguments are refused.
    await assert.rejects(producer.close(0, 1), {
      name: 'IllegalArgumentError',
      message: 'close() takes one of (), (timeout); got 2 arguments, not one object of named parameters',
    });
    for (const [args, given] of [
      [{}, ''],
      [{ callback: () => {} }, 'callback'],
      [{ record, callback: null, extra: 1 }, 'record, callback, extra'],
    ]) {
      await assert.rejects(producer.send(args), {
        name: 'IllegalArgumentError',
        message: `send() takes one of (record), (record, callback); got (${given})`,
      });
    }
    await assert.rejects(producer.send({ record, callback: undefined }), {
      name: 'TypeError',
      message: "callback must not be undefined. Leave the key out, or pass null for Java's null",
    });
    // The record on its own, named, and with Java's null callback.
    for (const args of [record, { record }, { record, callback: null }]) {
      const future = await producer.send(args);
      await assert.rejects(delivery(future), { name: 'TimeoutError', message: TIMEOUT_MESSAGE('unit-forms') });
    }
    // The producer is still open after the rejected calls.
    await producer.flush();
  } finally {
    await producer.close();
  }
});

test("flush() takes no argument: Java's flush(), never 1.x's flush(timeout, callback)", async () => {
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  try {
    let ran = 0;
    const withCallback = producer.flush(100, () => ran++);
    assert.ok(withCallback instanceof Promise);
    await assert.rejects(withCallback, {
      name: 'IllegalArgumentError',
      message: 'flush() takes one of (); got 2 arguments, not one object of named parameters',
    });
    await assert.rejects(producer.flush({ timeout: 100 }), { name: 'IllegalArgumentError', message: 'flush() takes one of (); got (timeout)' });
    await assert.rejects(producer.flush(100), { name: 'IllegalArgumentError', message: 'flush() takes one of (); got a number' });
    assert.equal(ran, 0);
    // undefined is never "not given": flush(undefined) is refused; flush({}) is flush().
    await assert.rejects(producer.flush(undefined), {
      name: 'TypeError',
      message: 'flush() takes no argument: leave it out rather than pass undefined',
    });
    await producer.flush({});
  } finally {
    await producer.close();
  }
  // The form is checked before the closed producer.
  await assert.rejects(producer.flush(100), { name: 'IllegalArgumentError' });
});

test('send reads only the parameters the form check saw: own enumerable keys', async () => {
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  try {
    const record = new ProducerRecord({ topic: 'unit-own', value: 'v' });
    let ran = 0;
    // An object with an inherited callback is no options object: it is read
    // as a bare value, which fits no form.
    const inherited = Object.assign(Object.create({ callback: () => ran++ }), { record });
    await assert.rejects(producer.send(inherited), {
      name: 'IllegalArgumentError',
      message: 'send() takes one of (record), (record, callback); got an object',
    });
    // A non-enumerable callback is not given.
    const hidden = { record };
    Object.defineProperty(hidden, 'callback', { value: () => ran++, enumerable: false });
    const future = await producer.send(hidden);
    await assert.rejects(delivery(future), { name: 'TimeoutError' });
    // Nor is one on a polluted Object.prototype.
    Object.prototype.callback = () => ran++;
    let polluted;
    try {
      polluted = await producer.send({ record });
    } finally {
      delete Object.prototype.callback;
    }
    await assert.rejects(delivery(polluted), { name: 'TimeoutError' });
    assert.equal(ran, 0);
  } finally {
    await producer.close();
  }
});

test('a closed producer checks a send\'s record and callback before the core reports it closed', async () => {
  // The binding keeps no closed state: the record is checked and converted
  // in JavaScript, then the core refuses it, where Java's doSend checks the
  // closed producer first (a deviation, documented on send()).
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  await producer.close();
  await assert.rejects(producer.send({ record: null }), { name: 'TypeError', message: 'record must not be null' });
  await assert.rejects(producer.send(null), { name: 'TypeError', message: 'record must not be null' });
  await assert.rejects(producer.send({ record: {} }), { name: 'TypeError', message: 'record must be a ProducerRecord' });
  await assert.rejects(producer.send({ record: new ProducerRecord({ topic: 't', value: 'v' }), callback: 'cb' }), {
    name: 'TypeError',
    message: 'callback must be a function ({ metadata, exception }) => void',
  });
  await assert.rejects(producer.send({ record: new ProducerRecord({ topic: 't', value: 'v' }) }), { name: 'IllegalStateError', message: CLOSED_MESSAGE });
  // The form is checked first: a bare value that fits no form included.
  await assert.rejects(producer.send({}), { name: 'IllegalArgumentError', message: 'send() takes one of (record), (record, callback); got ()' });
  await assert.rejects(producer.send(42), { name: 'IllegalArgumentError', message: 'send() takes one of (record), (record, callback); got a number' });
});

test("the constructor takes Java's {configs} or the configs on their own, a plain object", async () => {
  // The configs on their own and named build the same producer.
  for (const args of [UNREACHABLE, { configs: UNREACHABLE }]) {
    const producer = new KafkaProducer(args);
    const future = await producer.send(new ProducerRecord({ topic: 'unit-ctor', value: 'v' }));
    await assert.rejects(delivery(future), { name: 'TimeoutError', message: TIMEOUT_MESSAGE('unit-ctor') });
    await producer.close();
  }
  // A plain object is the options object when it has the key configs: a
  // stray key is then a form error, not a config key.
  assert.throws(() => new KafkaProducer(), { name: 'IllegalArgumentError', message: `${CTOR_FORMS}; got ()` });
  assert.throws(() => new KafkaProducer(undefined), {
    name: 'TypeError',
    message: "configs must not be undefined. Leave the argument out, or pass null for Java's null",
  });
  assert.throws(() => new KafkaProducer(UNREACHABLE, {}), {
    name: 'IllegalArgumentError',
    message: `${CTOR_FORMS}; got 2 arguments, not one object of named parameters`,
  });
  for (const [args, given] of [
    [{ configs: UNREACHABLE, keySerializer: null }, 'configs, keySerializer'],
    [{ 'bootstrap.servers': '127.0.0.1:1', configs: UNREACHABLE }, 'configs, bootstrap.servers'],
  ]) {
    assert.throws(() => new KafkaProducer(args), {
      name: 'IllegalArgumentError',
      message: `${CTOR_FORMS}; got (${given})`,
    });
  }
  // An empty object is an empty configuration.
  assert.throws(() => new KafkaProducer({}), {
    name: 'ConfigError',
    message: 'Missing required configuration "bootstrap.servers" which has no default value.',
  });
  // Java's Map and Properties forms are one plain object: a Map is refused, bare or named.
  for (const args of [new Map([['bootstrap.servers', '127.0.0.1:1']]), { configs: new Map() }]) {
    assert.throws(() => new KafkaProducer(args), { name: 'TypeError', message: 'configs must be a plain object, not a Map' });
  }
  assert.throws(() => new KafkaProducer({ configs: null }), { name: 'TypeError', message: 'configs must not be null' });
  assert.throws(() => new KafkaProducer(null), { name: 'TypeError', message: 'configs must not be null' });
  assert.throws(() => new KafkaProducer({ configs: undefined }), {
    name: 'TypeError',
    message: "configs must not be undefined. Leave the key out, or pass null for Java's null",
  });
  assert.throws(() => new KafkaProducer('bootstrap.servers=x'), {
    name: 'IllegalArgumentError',
    message: `${CTOR_FORMS}; got a string`,
  });
  // A config value set to undefined is still left out.
  const producer = new KafkaProducer({ ...UNREACHABLE, 'client.id': undefined });
  await producer.close();
});

test('a negative close timeout on an open producer is rejected first and changes nothing', async () => {
  const negative = (e) => {
    // Java's IllegalArgumentException is a java.lang exception, not a KafkaException.
    assert.ok(e instanceof IllegalArgumentError);
    assert.ok(!(e instanceof KafkaError));
    assert.equal(e.name, 'IllegalArgumentError');
    assert.ok(e instanceof RangeError);
    assert.equal(e.message, 'The timeout cannot be negative.');
    return true;
  };
  // Java's close(Duration) throws before touching the producer: a send() in
  // the same turn is still accepted.
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  const rejected = producer.close({ timeout: -1 });
  const sent = producer.send({ record: new ProducerRecord({ topic: 'unit-n', value: 'v' }) });
  await assert.rejects(rejected, negative);
  // The record is handed over (it fails only for the missing metadata, as any
  // record to this broker does).
  const future = await sent;
  await producer.close();
  await assert.rejects(delivery(future), (e) => {
    assert.ok(e instanceof KafkaError);
    assert.ok(e instanceof errors.TimeoutError);
    assert.equal(e.name, 'TimeoutError');
    assert.equal(e.message, TIMEOUT_MESSAGE('unit-n'));
    return true;
  });
  await assert.rejects(producer.send({ record: new ProducerRecord({ topic: 'unit-n', value: 'v' }) }), { message: CLOSED_MESSAGE });

  // A close() in the same turn is a close of its own, not the rejected one.
  const other = new KafkaProducer({ configs: UNREACHABLE });
  const rejectedToo = other.close({ timeout: -1 });
  const closed = other.close();
  await assert.rejects(rejectedToo, negative);
  await closed;
  await assert.rejects(other.send({ record: new ProducerRecord({ topic: 'unit-n', value: 'v' }) }), { message: CLOSED_MESSAGE });
});

test('close() while a send waits for metadata rejects that send with a bare KafkaError', async () => {
  const producer = new KafkaProducer({ configs: { ...UNREACHABLE, 'max.block.ms': 60000 } });
  const calls = [];
  const sent = producer.send({ record: new ProducerRecord({ topic: 'unit-close-waiting', value: 'v' }), callback: () => {
      calls.push('callback');
    } });
  let settled = false;
  sent.then(
    () => (settled = true),
    () => (settled = true),
  );
  // Let the record reach the core, where it waits for the topic's metadata.
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(settled, false);
  const started = Date.now();
  await producer.close();
  await assert.rejects(sent, (e) => {
    assert.equal(e.constructor, KafkaError);
    assert.equal(e.message, 'Producer closed while send in progress');
    return true;
  });
  // Well below max.block.ms (60 s), however loaded the machine.
  assert.ok(Date.now() - started < 30000, `close took ${Date.now() - started} ms`);
  assert.deepEqual(calls, []);
});

test('flush() does not wait for a send still waiting for metadata', async () => {
  const blockMs = 1500;
  const producer = new KafkaProducer({ configs: { ...UNREACHABLE, 'max.block.ms': blockMs } });
  try {
    const calls = [];
    const sent = producer.send({
      record: new ProducerRecord({ topic: 'unit-flush-waiting', value: 'v' }),
      callback: ({ exception }) => {
        calls.push(exception);
      },
    });
    let handedOver = false;
    sent.then(
      () => (handedOver = true),
      () => (handedOver = true),
    );
    await producer.flush();
    // flush() covers only the sends that had resolved: this one still waits.
    assert.equal(handedOver, false);
    assert.deepEqual(calls, []);
    // It then fails on its own max.block.ms: a resolved send() and a failed
    // future, its callback run once.
    const future = await sent;
    const message = `Topic unit-flush-waiting not present in metadata after ${blockMs} ms.`;
    assert.equal(future.isCompletedExceptionally(), true);
    await assert.rejects(delivery(future), { name: 'TimeoutError', message });
    assert.equal(calls.length, 1);
    assert.equal(calls[0].name, 'TimeoutError');
    assert.equal(calls[0].message, message);
  } finally {
    await producer.close();
  }
});

test('argument errors reject: send() and close() never throw', async () => {
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  try {
    const sent = producer.send({ record: { topic: 't' } });
    assert.ok(sent instanceof Promise);
    await assert.rejects(sent, { name: 'TypeError', message: 'record must be a ProducerRecord' });
    // Java's doSend dereferences a null record.
    await assert.rejects(producer.send({ record: null }), { name: 'TypeError', message: 'record must not be null' });
    await assert.rejects(producer.send(null), { name: 'TypeError', message: 'record must not be null' });
    await assert.rejects(producer.send({ record: new ProducerRecord({ topic: 't', value: 'v' }), callback: 'cb' }), {
      name: 'TypeError',
      message: 'callback must be a function ({ metadata, exception }) => void',
    });
    const closed = producer.close({ timeout: 'soon' });
    assert.ok(closed instanceof Promise);
    await assert.rejects(closed, { name: 'TypeError', message: 'timeout must be a number of milliseconds or a Temporal.Duration' });
  } finally {
    await producer.close();
  }
});

test('a string holding a NUL is refused where the C ABI would truncate it', async () => {
  // A C reader stops at the first NUL: 'acks\0x' would read as 'acks', and
  // 'good\0bad' as the topic 'good'.
  assert.throws(() => new KafkaProducer({ configs: { ...UNREACHABLE, 'acks\0ignored': 'all' } }), {
    name: 'TypeError',
    message: 'config keys must be strings without null bytes',
  });
  assert.throws(() => new KafkaProducer({ configs: { ...UNREACHABLE, acks: '0\0bogus' } }), {
    name: 'TypeError',
    message: 'config values must be strings without null bytes',
  });
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  const calls = [];
  const callback = () => calls.push('callback');
  await assert.rejects(producer.send({ record: new ProducerRecord({ topic: 'good\0bad', value: 'v' }), callback }), {
    name: 'TypeError',
    message: 'topic must be a string without null bytes',
  });
  await assert.rejects(
    producer.send({
      record: new ProducerRecord({ topic: 'unit-nul', partition: null, key: null, value: 'v', headers: [new Header({ key: 'k\0x', value: Buffer.from('h') })] }),
      callback,
    }),
    { name: 'TypeError', message: 'a header key must be a string without null bytes' },
  );
  // Neither send left anything pending: the close has nothing to wait for and
  // no callback ran.
  const started = Date.now();
  await producer.close();
  assert.ok(Date.now() - started < MAX_BLOCK_MS * 10, `close took ${Date.now() - started} ms`);
  assert.deepEqual(calls, []);
});

test('a refused NUL-holding send leaves nothing pending: the producer then closes at once', () => {
  const r = runNode(`
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    const { Header } = require('@confluentinc/kafka-javascript/common/header');
    const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 60000 } });
    (async () => {
      for (const record of [
        new ProducerRecord({ topic: 'a\\0b', value: 'v' }),
        new ProducerRecord({ topic: 'idle', partition: null, key: null, value: 'v', headers: [new Header({ key: 'a\\0b', value: Buffer.from('v') })] }),
      ]) {
        await p.send({ record, callback: () => console.log('callback ran') }).catch((e) => console.log(e.name, e.message));
      }
      await p.close();
    })();
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(
    r.stdout,
    'TypeError topic must be a string without null bytes\n' +
      'TypeError a header key must be a string without null bytes\n',
  );
  assert.ok(r.ms < 30000, `took ${r.ms} ms`);
});

// The addon borrows the caller's key and value Buffers: it holds a reference
// on each until the send's hand-over, when the core no longer reads them, and
// drops it then, whatever the hand-over's outcome (javascript/CLAUDE.md, rule 8).
test('a key or value Buffer is held until the hand-over and released at it', () => {
  const r = runNode(
    `
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    // Unreachable: the record waits for metadata in the core, which reads the
    // caller's own memory until its hand-over.
    const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 60000 } });
    const refs = [];
    const sends = [];
    for (let i = 0; i < 20; i++) {
      const key = new Uint8Array(1024).fill(1);
      const value = Buffer.alloc(65536, 2);
      refs.push(new WeakRef(key), new WeakRef(value));
      sends.push(p.send(new ProducerRecord({ topic: 'borrowed', key, value })).then(() => 'sent', (e) => e.name));
    }
    const alive = () => refs.filter((r) => r.deref() !== undefined).length;
    setTimeout(async () => {
      global.gc();
      console.log('before the hand-over', alive());
      // A zero close fails the waiting records at their hand-over.
      await p.close({ timeout: 0 });
      console.log('outcomes', [...new Set(await Promise.all(sends))].join(','));
      setImmediate(() => {
        global.gc();
        console.log('after the hand-over', alive());
      });
    }, 200);
  `,
    30000,
    ['--expose-gc'],
  );
  assert.equal(r.status, 0, r.stderr);
  const lines = r.stdout.trim().split('\n');
  assert.equal(lines[0], 'before the hand-over 40');
  assert.match(lines[1], /^outcomes /);
  assert.equal(lines[1].includes('sent'), false, lines[1]);
  assert.equal(lines[2], 'after the hand-over 0');
});

test('the references of many sends are released: the process does not grow with them', () => {
  // 20000 sends of a 64 KiB value hold 1.3 GB; a Buffer kept per record would
  // show far above the bound.
  const r = runNode(
    `
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 0 } });
    const run = async (n) => {
      for (let i = 0; i < n; i++) await p.send(new ProducerRecord({ topic: 'leak', value: Buffer.alloc(65536, 7) })).catch(() => {});
    };
    (async () => {
      await run(2000);
      global.gc();
      const before = process.memoryUsage().rss;
      await run(20000);
      global.gc();
      console.log(Math.round((process.memoryUsage().rss - before) / 1e6));
      await p.close({ timeout: 0 });
    })();
  `,
    60000,
    ['--expose-gc'],
  );
  assert.equal(r.status, 0, r.stderr);
  const grownMb = Number(r.stdout.trim());
  assert.ok(grownMb < 256, `the process grew by ${grownMb} MB over 20000 sends of 64 KiB`);
});

// Runs `script` in a fresh Node process (killed after `timeout` ms), returning
// its exit status, output and wall time.
function runNode(script, timeout = 30000, flags = [], env = undefined) {
  const started = process.hrtime.bigint();
  const r = spawnSync(process.execPath, [...flags, '-e', script], {
    cwd: path.join(__dirname, '..', '..', '..'),
    encoding: 'utf8',
    timeout,
    env: env === undefined ? process.env : { ...process.env, ...env },
  });
  return { ...r, ms: Number((process.hrtime.bigint() - started) / 1000000n) };
}

test('an open producer keeps the process running until close() closes it', () => {
  const r = runNode(`
    const { KafkaProducer } = require('@confluentinc/kafka-javascript/producer');
    const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1' } });
    console.log('created');
    // Nothing else holds the event loop: the producer does, until closed.
    setTimeout(() => p.close().then(() => console.log('closed')), 1000).unref();
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'created\nclosed\n');
  assert.ok(r.ms >= 900, `exited after ${r.ms} ms`);
});

test('a producer never closed keeps the process running', () => {
  // As KafkaJS and 1.x from connect() to disconnect(): a script that forgets
  // close() never exits on its own (javascript/CLAUDE.md, "Threads and liveness").
  const r = runNode(
    `
    const { KafkaProducer } = require('@confluentinc/kafka-javascript/producer');
    new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1' } });
    console.log('created');
  `,
    3000,
  );
  assert.equal(r.stdout, 'created\n');
  assert.equal(r.signal, 'SIGTERM', `the process exited on its own: status ${r.status}`);
});

test('a pending send keeps the process alive until it completes and the producer is closed', () => {
  const r = runNode(`
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 1500 } });
    p.send({
      record: new ProducerRecord({ topic: 'alive', value: 'v' }),
      callback: ({ exception }) => console.log('callback:', exception.message),
    }).then(() => p.close());
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
      const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 50, 'linger.ms': 0 } });
      for (let i = 0; i < 200; i++) {
        p.send({ record: new ProducerRecord({ topic: 'teardown', value: String(i) }), callback: () => {} });
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

test('a producer dropped while open keeps running: no garbage collection closes it', () => {
  // The core logs each close at INFO. A producer dropped while its record is
  // pending, and collected, still delivers the record's callback, is never
  // closed, and keeps the process running.
  const r = runNode(
    `
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    let p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 300 } });
    p.send({ record: new ProducerRecord({ topic: 'dropped', value: 'v' }), callback: ({ exception }) => console.log('callback:', exception.name) });
    p = null;
    global.gc();
    setTimeout(() => {
      global.gc();
      setTimeout(() => console.error('MARK dropped producer collected'), 100);
    }, 1000);
  `,
    4000,
    ['--expose-gc', '--no-warnings'],
    { RUST_LOG: 'info' },
  );
  assert.equal(r.stdout, 'callback: TimeoutError\n');
  assert.equal(r.signal, 'SIGTERM', `the process exited on its own: status ${r.status}`);
  assert.ok(r.stderr.includes('MARK dropped producer collected'), r.stderr);
  assert.equal(r.stderr.split('\n').filter((l) => l.includes('Closing the Kafka producer')).length, 0, r.stderr);
});

test('a closed producer lets the process exit, and is destroyed when garbage collected', () => {
  // Closed once, by the app's close(): the garbage collection that follows
  // destroys the native producer and closes nothing more.
  const r = runNode(
    `
    const { KafkaProducer } = require('@confluentinc/kafka-javascript/producer');
    let p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1' } });
    p.close().then(() => {
      p = null;
      global.gc();
      setTimeout(() => {
        global.gc();
        console.log('collected');
      }, 100);
    });
  `,
    30000,
    ['--expose-gc', '--no-warnings'],
    { RUST_LOG: 'info' },
  );
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'collected\n');
  assert.equal(r.stderr.split('\n').filter((l) => l.includes('Closing the Kafka producer')).length, 1, r.stderr);
});

// The destroy itself, observed: the producer is collected (a
// FinalizationRegistry), and the native producer's threads are gone once
// the finalizer destroyed it. A send refused once the core reported the
// close, or failed by the close, leaves no JS function of its own alive
// that would hold the producer (javascript/CLAUDE.md, rule 10).
test('a closed producer is destroyed when collected, a send refused after close() or failed by it included', { skip: !['linux', 'darwin'].includes(process.platform) }, () => {
  const r = runNode(
    `
    const { execSync } = require('node:child_process');
    const fs = require('node:fs');
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
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
    const CASES = {
      'close': async (p) => {
        await p.close();
      },
      'send after close': async (p) => {
        await p.close();
        await p.send(new ProducerRecord({ topic: 't', value: 'v' })).catch(() => {});
      },
      'send failed by close': async (p) => {
        // Waits for metadata in the core until the zero close fails it.
        const sent = p.send(new ProducerRecord({ topic: 't', value: 'v' })).catch(() => {});
        await new Promise((r) => setImmediate(r));
        await p.close({ timeout: 0 });
        await sent;
      },
    };
    // In a function of its own, so no frame keeps the producer.
    async function one(name, run) {
      const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 60000 } });
      registry.register(p, name);
      await run(p);
    }
    (async () => {
      // A closed producer no longer holds the loop: the test does.
      const hold = setInterval(() => {}, 1000);
      // The first producer starts what the process keeps afterwards.
      await one('warm-up', CASES['close']);
      await settle();
      for (const [name, run] of Object.entries(CASES)) {
        const before = threads();
        for (let i = 0; i < 5; i++) await one(name, run);
        await settle();
        console.log(name, collected.filter((c) => c === name).length, threads() - before);
      }
      clearInterval(hold);
    })();
  `,
    120000,
    ['--expose-gc', '--no-warnings'],
  );
  assert.equal(r.status, 0, r.stderr);
  // Five producers collected per case, and fewer threads left over than one
  // live producer runs (about 18): each destroyed producer took its threads
  // with it. A leak shows as 5 live producers, about 90 threads.
  const lines = r.stdout.trim().split('\n').map((l) => /^(.*) (\d+) (-?\d+)$/.exec(l));
  assert.deepEqual(lines.map((m) => m && [m[1], m[2]]), [['close', '5'], ['send after close', '5'], ['send failed by close', '5']], r.stdout);
  for (const m of lines) assert.ok(Number(m[3]) < 10, `${m[1]}: ${m[3]} threads left over`);
});

test('the preview notice is one ExperimentalWarning per process, which --no-warnings silences', () => {
  const script = `
    const { KafkaProducer } = require('@confluentinc/kafka-javascript/producer');
    (async () => {
      for (let i = 0; i < 3; i++) await new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1' } }).close();
    })();
  `;
  let r = runNode(script);
  assert.equal(r.status, 0, r.stderr);
  const notices = r.stderr.split('\n').filter((l) => l.includes('ExperimentalWarning'));
  assert.deepEqual(notices.length, 1, r.stderr);
  assert.match(
    notices[0],
    /\) ExperimentalWarning: The JavaScript client is in preview and not yet recommended for production use\. The public API is not stable before the GA release and may change\. We welcome your feedback while the design is still open$/,
  );
  assert.ok(!r.stderr.includes('never closed'), r.stderr);
  r = runNode(script, 30000, ['--no-warnings']);
  assert.equal(r.status, 0, r.stderr);
  assert.ok(!r.stderr.includes('ExperimentalWarning'), r.stderr);
});

test('an un-awaited send whose hand-over fails surfaces as an unhandled rejection', () => {
  const r = runNode(`
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    process.on('unhandledRejection', (e) => {
      console.log('unhandled:', e.name, e.message);
      p.close();
    });
    const p = new KafkaProducer({ configs: { 'bootstrap.servers': 'does-not-exist.invalid:9092', 'bootstrap.resolve.timeout.ms': 1000 } });
    p.send({ record: new ProducerRecord({ topic: 'h', value: 'v' }), callback: () => console.log('callback ran') });
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, `unhandled: BootstrapResolutionError ${BOOTSTRAP_MESSAGE}\n`);
});

test('a failed future whose get() is never called raises no unhandled rejection', () => {
  const r = runNode(`
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    process.on('unhandledRejection', (e) => console.log('unhandled:', e.message));
    (async () => {
      const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 200 } });
      const future = await p.send({ record: new ProducerRecord({ topic: 'unread', value: 'v' }) });
      await p.close();
      console.log('done', future.isCompletedExceptionally());
    })();
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'done true\n');
});

// Serializers (root CLAUDE.md, JavaScript Binding Conventions,
// Serialization): an object with Java's serialize, taking one object
// { topic, headers, data }, run in JavaScript inside send(), awaited when it
// returns a Promise, closed when the producer closes.

/** A serializer recording its calls, returning `out(data)`. */
function recording(out, calls) {
  return {
    serialize({ topic, headers, data }) {
      calls.push({ topic, headers, data });
      return out(data);
    },
  };
}

/** Collects the process warnings emitted while `body` runs and one turn after. */
async function warningsOf(body) {
  const seen = [];
  const listener = (w) => seen.push(w);
  process.on('warning', listener);
  try {
    await body();
    await new Promise((resolve) => setImmediate(resolve));
    await new Promise((resolve) => setImmediate(resolve));
  } finally {
    process.off('warning', listener);
  }
  return seen;
}

test('the serializers run in send(), key then value, with the topic, the headers and null data too', async () => {
  const calls = [];
  const keySerializer = recording((data) => (data === null ? null : Buffer.from(`k:${data}`)), calls);
  const valueSerializer = recording((data) => Buffer.from(JSON.stringify(data)), calls);
  const producer = new KafkaProducer({ configs: UNREACHABLE, keySerializer, valueSerializer });
  try {
    const record = new ProducerRecord({ topic: 'unit-serde', key: 7, value: { a: 1 } });
    const future = await producer.send(record);
    assert.deepEqual(calls.map((c) => [c.topic, c.data]), [['unit-serde', 7], ['unit-serde', { a: 1 }]]);
    // The record's own headers, which the serializer may change before the
    // hand-over seals them.
    assert.equal(calls[0].headers, record.headers);
    assert.throws(() => record.headers.add({ key: 'late', value: null }), { name: 'IllegalStateError', message: 'RecordHeaders has been closed.' });
    await assert.rejects(delivery(future), { name: 'TimeoutError', message: TIMEOUT_MESSAGE('unit-serde') });
    // A null key is passed to the serializer, which decides what it maps to.
    calls.length = 0;
    await producer.send(new ProducerRecord({ topic: 'unit-serde', value: null }));
    assert.deepEqual(calls.map((c) => c.data), [null, null]);
  } finally {
    await producer.close();
  }
});

test('a serializer may add headers before the hand-over', async () => {
  const valueSerializer = {
    serialize({ headers, data }) {
      headers.add({ key: 'type', value: Buffer.from('json') });
      return Buffer.from(JSON.stringify(data));
    },
  };
  const producer = new KafkaProducer({ configs: UNREACHABLE, keySerializer: null, valueSerializer });
  try {
    const record = new ProducerRecord({ topic: 'unit-serde-h', value: [1] });
    await producer.send(record);
    assert.equal(record.headers.lastHeader('type').value().toString(), 'json');
  } finally {
    await producer.close();
  }
});

test('a null serializer sends the record bytes as they are, as no serializer does', async () => {
  const calls = [];
  const producer = new KafkaProducer({ configs: UNREACHABLE, keySerializer: null, valueSerializer: recording(() => null, calls) });
  try {
    await producer.send(new ProducerRecord({ topic: 'unit-serde-n', key: Buffer.from('k'), value: 'anything' }));
    assert.deepEqual(calls.map((c) => c.data), ['anything']);
    // Without a key serializer the key must be bytes or a string.
    await assert.rejects(producer.send(new ProducerRecord({ topic: 'unit-serde-n', key: 3, value: 'v' })), {
      name: 'TypeError',
      message: 'key must be a Buffer, a Uint8Array, a string or null when the producer has no key serializer',
    });
  } finally {
    await producer.close();
  }
});

test('a serializer returning a Promise is awaited; one returning a plain value adds no microtask', async () => {
  const order = [];
  const valueSerializer = {
    serialize({ data }) {
      order.push(`serialize ${data}`);
      return data === 'async' ? Promise.resolve(Buffer.from(data)).then((b) => (order.push('resolved'), b)) : Buffer.from(data);
    },
  };
  const producer = new KafkaProducer({ configs: UNREACHABLE, keySerializer: null, valueSerializer });
  try {
    const sync = producer.send(new ProducerRecord({ topic: 'unit-serde-a', value: 'sync' }));
    const pending = producer.send(new ProducerRecord({ topic: 'unit-serde-a', value: 'async' }));
    order.push('returned');
    assert.deepEqual(order, ['serialize sync', 'serialize async', 'returned']);
    await sync;
    const future = await pending;
    assert.ok(future instanceof KafkaFuture);
    assert.deepEqual(order, ['serialize sync', 'serialize async', 'returned', 'resolved']);
    await assert.rejects(delivery(future), { name: 'TimeoutError', message: TIMEOUT_MESSAGE('unit-serde-a') });
  } finally {
    await producer.close();
  }
});

test('the value serializer runs after the key serializer Promise settled', async () => {
  const order = [];
  let release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  const keySerializer = {
    serialize() {
      order.push('key');
      return gate.then(() => (order.push('key settled'), Buffer.from('k')));
    },
  };
  const valueSerializer = {
    serialize() {
      order.push('value');
      return Buffer.from('v');
    },
  };
  const producer = new KafkaProducer({ configs: UNREACHABLE, keySerializer, valueSerializer });
  try {
    const sent = producer.send(new ProducerRecord({ topic: 'unit-serde-o', key: 1, value: 2 }));
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(order, ['key']);
    release();
    await sent;
    assert.deepEqual(order, ['key', 'key settled', 'value']);
  } finally {
    await producer.close();
  }
});

test('what a serializer throws, rejects with or returns that is no bytes rejects send()', async () => {
  const boom = new Error('boom');
  for (const [serializer, expected] of [
    [{ serialize: () => { throw boom; } }, (e) => e === boom],
    [{ serialize: () => Promise.reject(boom) }, (e) => e === boom],
    [{ serialize: () => 'text' }, { name: 'TypeError', message: 'valueSerializer.serialize() must return a Uint8Array or null, not a string' }],
    [{ serialize: () => undefined }, { name: 'TypeError', message: 'valueSerializer.serialize() must return a Uint8Array or null, not an undefined' }],
    [{ serialize: async () => 5 }, { name: 'TypeError', message: 'valueSerializer.serialize() must return a Uint8Array or null, not a number' }],
  ]) {
    const producer = new KafkaProducer({ configs: UNREACHABLE, keySerializer: null, valueSerializer: serializer });
    try {
      const calls = [];
      const record = new ProducerRecord({ topic: 'unit-serde-e', value: 'v' });
      await assert.rejects(producer.send({ record, callback: (r) => calls.push(r) }), expected);
      // A send refused before the hand-over leaves the headers writable and
      // never reaches the callback.
      record.headers.add({ key: 'still', value: null });
      assert.equal(calls.length, 0);
    } finally {
      await producer.close();
    }
  }
});

test('a producer closed while a serializer Promise is pending rejects that send as closed', async () => {
  let release;
  const valueSerializer = {
    serialize: () =>
      new Promise((resolve) => {
        release = () => resolve(Buffer.from('v'));
      }),
  };
  const producer = new KafkaProducer({ configs: UNREACHABLE, keySerializer: null, valueSerializer });
  const sent = producer.send(new ProducerRecord({ topic: 'unit-serde-c', value: 'v' }));
  await producer.close();
  release();
  await assert.rejects(sent, { name: 'IllegalStateError', message: CLOSED_MESSAGE });
  // The binding keeps no closed state: on a closed producer the serializers
  // still run, and the core reports the closed producer at the hand-over.
  const calls = [];
  const closed = new KafkaProducer({ configs: UNREACHABLE, keySerializer: null, valueSerializer: recording(() => Buffer.from('v'), calls) });
  await closed.close();
  await holdingTheLoop(() =>
    assert.rejects(closed.send(new ProducerRecord({ topic: 'unit-serde-c', value: 'v' })), { name: 'IllegalStateError', message: CLOSED_MESSAGE }),
  );
  assert.deepEqual(calls.map((c) => c.data), ['v']);
});

test("a serializer argument is a serializer object or null; a function gets Python's hint", () => {
  const forms = (given) => ({ name: 'IllegalArgumentError', message: `${CTOR_FORMS}; got (${given})` });
  const value = { serialize: () => null };
  assert.throws(() => new KafkaProducer({ configs: UNREACHABLE, keySerializer: value }), forms('configs, keySerializer'));
  assert.throws(() => new KafkaProducer({ configs: UNREACHABLE, valueSerializer: value }), forms('configs, valueSerializer'));
  function stringSerializer() {
    return value;
  }
  assert.throws(() => new KafkaProducer({ configs: UNREACHABLE, keySerializer: stringSerializer, valueSerializer: null }), {
    name: 'IllegalArgumentError',
    message: "keySerializer: pass a serializer, not a function that makes one — did you forget '()'?",
  });
  for (const bad of [{}, 'string', 1, { serialize: 'x' }]) {
    assert.throws(() => new KafkaProducer({ configs: UNREACHABLE, keySerializer: null, valueSerializer: bad }), {
      name: 'TypeError',
      message: 'valueSerializer must be a Serializer: an object with a serialize({ topic, headers, data }) method, or null',
    });
  }
  assert.throws(() => new KafkaProducer({ configs: UNREACHABLE, keySerializer: { serialize() {}, close: 1 }, valueSerializer: null }), {
    name: 'TypeError',
    message: 'keySerializer.close must be a function when given',
  });
  assert.throws(() => new KafkaProducer({ configs: UNREACHABLE, keySerializer: undefined, valueSerializer: null }), {
    name: 'TypeError',
    message: "keySerializer must not be undefined. Leave the key out, or pass null for Java's null",
  });
});

test('close() closes both serializers, a failure being a process warning, never a rejection', async () => {
  const closed = [];
  const keySerializer = { serialize: () => null, close: () => closed.push('key') };
  const valueSerializer = {
    serialize: () => null,
    close() {
      closed.push('value');
      throw new Error('cannot close');
    },
  };
  const warnings = await warningsOf(async () => {
    const producer = new KafkaProducer({ configs: UNREACHABLE, keySerializer, valueSerializer });
    await producer.close();
    // A second close closes nothing again (the core's close, whose completion
    // a closed producer no longer holds Node for).
    await holdingTheLoop(() => producer.close());
  });
  assert.deepEqual(closed, ['key', 'value']);
  assert.deepEqual(
    warnings.map((w) => w.message),
    ['Failed to close producer valueSerializer: Error: cannot close'],
  );
  // A serializer without close() is closed by doing nothing, Java's default;
  // a close() returning a rejected Promise is a warning too.
  const rejected = await warningsOf(async () => {
    const producer = new KafkaProducer({
      configs: UNREACHABLE,
      keySerializer: { serialize: () => null },
      valueSerializer: { serialize: () => null, close: () => Promise.reject(new Error('later')) },
    });
    await producer.close();
  });
  assert.deepEqual(rejected.map((w) => w.message), ['Failed to close producer valueSerializer: Error: later']);
});

test("a producer whose construction fails closes the serializers; a ConfigError of the configuration's check, which Java throws before that catch, does not", async () => {
  const closed = [];
  const serializer = (name) => ({ serialize: () => null, close: () => closed.push(name) });
  // Inside Java's constructor catch: relabelled "Failed to construct kafka producer", serializers closed.
  assert.throws(
    () =>
      new KafkaProducer({
        configs: { 'bootstrap.servers': '127.0.0.1:1', 'delivery.timeout.ms': 1 },
        keySerializer: serializer('key'),
        valueSerializer: serializer('value'),
      }),
    (e) => e.constructor === KafkaError && e.message === 'Failed to construct kafka producer' && e.cause.name === 'ConfigError',
  );
  assert.deepEqual(closed, ['key', 'value']);
  // Java's new ProducerConfig(...) validates before that catch: the serializers stay open.
  closed.length = 0;
  assert.throws(
    () => new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', acks: 'sometimes' }, keySerializer: serializer('key'), valueSerializer: serializer('value') }),
    { name: 'ConfigError', message: 'Invalid value sometimes for configuration acks: String must be one of: all, -1, 0, 1' },
  );
  assert.deepEqual(closed, []);
});

// partitionsFor (Java's partitionsFor(String topic)): a Promise through the
// _cb twin, resolving to an array of PartitionInfo.
test("partitionsFor rejects with the core's errors: a metadata timeout, Java's null topic, and its forms", async () => {
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  try {
    const pending = producer.partitionsFor('unit-pf');
    assert.ok(pending instanceof Promise);
    await assert.rejects(pending, { name: 'TimeoutError', message: TIMEOUT_MESSAGE('unit-pf') });
    await assert.rejects(producer.partitionsFor({ topic: 'unit-pf' }), { name: 'TimeoutError', message: TIMEOUT_MESSAGE('unit-pf') });
    // Java's Objects.requireNonNull(topic, "topic cannot be null"), the core's.
    await assert.rejects(producer.partitionsFor(null), { constructor: TypeError, message: 'topic cannot be null' });
    await assert.rejects(producer.partitionsFor({ topic: 5 }), { constructor: TypeError, message: 'topic must be a string' });
    await assert.rejects(producer.partitionsFor(5), {
      name: 'IllegalArgumentError',
      message: 'partitionsFor() takes one of (topic); got a number',
    });
    await assert.rejects(producer.partitionsFor(), { name: 'IllegalArgumentError', message: 'partitionsFor() takes one of (topic); got ()' });
    await assert.rejects(producer.partitionsFor('a', 'b'), {
      name: 'IllegalArgumentError',
      message: 'partitionsFor() takes one of (topic); got 2 arguments, not one object of named parameters',
    });
  } finally {
    await producer.close();
  }
});
