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
//
// PlaintextProducerSendTest.testWrongSerializer is ported here: a Buffer
// given to the configured stringSerializer rejects send() with
// SerializationError, as Java's KafkaProducer#doSend wraps the serializer's
// ClassCastException (a TypeError here).

const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

const { Temporal } = require('@js-temporal/polyfill');

const { ExecutionError, IllegalArgumentError, IllegalStateError } = require('@confluentinc/kafka-javascript');
const { KafkaError, KafkaFuture } = require('@confluentinc/kafka-javascript/common');
const errors = require('@confluentinc/kafka-javascript/common/errors');
const { Header } = require('@confluentinc/kafka-javascript/common/header');
const { stringSerializer } = require('@confluentinc/kafka-javascript/common/serialization');
const { KafkaProducer, ProducerRecord, RecordMetadata } = require('@confluentinc/kafka-javascript/producer');
const { createFuture } = require('../../_internal');
// Every probe child runs with the allocator's poisoning (freed memory filled,
// guard pages around large blocks), so a use after free or a double free in
// the native layer, at a collection or a destroy, fails the probe.
const { POISON } = require('../../memory/_probe');

/** @template T @typedef {import('@confluentinc/kafka-javascript/common/serialization').Serializer<T>} Serializer */
/** @typedef {import('@confluentinc/kafka-javascript/common/header').Headers} Headers */
/** @typedef {import('node:assert').AssertPredicate} AssertPredicate */
/** @typedef {{ topic: string, headers: Headers, data: unknown }} SerializeCall */

const MAX_BLOCK_MS = 300;
const UNREACHABLE = {
  'bootstrap.servers': '127.0.0.1:1',
  'max.block.ms': MAX_BLOCK_MS,
  'delivery.timeout.ms': 1000,
  'request.timeout.ms': 500,
  'linger.ms': 0,
};
/** @param {string} topic */
const TIMEOUT_MESSAGE = (topic) => `Topic ${topic} not present in metadata after ${MAX_BLOCK_MS} ms.`;
const CLOSED_MESSAGE = 'Cannot perform operation after producer has been closed';
const BOOTSTRAP_MESSAGE =
  'Failed to resolve bootstrap servers after 1000ms. Please check your bootstrap.servers configuration and DNS settings.';
/**
 * A failed record's delivery error: its future's get() rejects with Java's
 * ExecutionError, whose message is the cause's String(), and whose cause is
 * the error the delivery failed with, which this rejects with.
 *
 * @param {KafkaFuture<unknown>} future
 */
function delivery(future) {
  return future.get().catch((/** @type {ExecutionError} */ e) => {
    assert.equal(e.constructor, ExecutionError);
    assert.equal(e.message, String(e.cause));
    throw e.cause;
  });
}

const CTOR_FORMS = 'KafkaProducer() takes one of (configs), (configs, keySerializer, valueSerializer)';

const FLUSH_IN_CALLBACK = 'KafkaProducer.flush() invocation inside a callback is not permitted because it may lead to deadlock.';

test('send resolves at hand-over; the future fails after the callback ran once', async () => {
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  try {
    /** @type {Array<{ metadata: RecordMetadata, error: Error | null }>} */
    const calls = [];
    const sent = producer.send({
      record: new ProducerRecord({ topic: 'unit-t', key: Buffer.from('k'), value: Buffer.from('v') }),
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
    assert.throws(
      () => future.getNow(null),
      (e) => e instanceof ExecutionError && e.cause === error,
    );
    assert.ok(metadata instanceof RecordMetadata);
    assert.equal(metadata.topic, 'unit-t');
    assert.equal(metadata.partition, RecordMetadata.UNKNOWN_PARTITION);
    assert.equal(metadata.hasOffset, false);
    // Java's longs: bigints.
    assert.equal(metadata.offset, -1n);
    // Java's nullMetadata (KafkaProducer#doSend's catch of an ApiException):
    // no timestamp, and -1 for both serialized sizes.
    assert.equal(metadata.hasTimestamp, false);
    assert.equal(metadata.timestamp, -1n);
    assert.equal(metadata.serializedKeySize, -1);
    assert.equal(metadata.serializedValueSize, -1);
    assert.equal(metadata.toString(), 'unit-t--1@-1');
    // The fields are data: a spread or JSON copy keeps every one, the longs
    // as decimal strings in JSON.
    assert.deepEqual(JSON.parse(JSON.stringify(metadata)), { ...metadata, offset: '-1', timestamp: '-1' });
    assert.equal(Object.keys(metadata).length, 8);
    assert.ok(error);
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
      record: new ProducerRecord({ topic: 'unit-x', value: Buffer.from('v') }),
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
    let release = /** @type {((value?: unknown) => void) | undefined} */ (undefined);
    const released = new Promise((res) => (release = res));
    let seen = /** @type {{ metadata: RecordMetadata, exception: Error | null } | null} */ (null);
    const future = await producer.send({
      record: new ProducerRecord({ topic: 'unit-async', value: Buffer.from('v') }),
      callback: async ({ metadata, exception }) => {
        seen = { metadata, exception };
        await released;
      },
    });
    // The callback ran (Java's send() returned after max.block.ms), and is
    // still pending: neither the future nor a flush covering it is done.
    assert.ok(seen !== null);
    assert.ok(seen.exception);
    assert.equal(seen.exception.message, TIMEOUT_MESSAGE('unit-async'));
    assert.equal(future.isDone(), false);
    let flushed = false;
    const flush = producer.flush().then(() => (flushed = true));
    await new Promise((res) => setTimeout(res, 200));
    assert.equal(future.isDone(), false);
    assert.equal(flushed, false);
    assert.ok(release);
    release();
    await flush;
    assert.equal(future.isDone(), true);
    // seen was checked not null above.
    await assert.rejects(delivery(future), (e) => e === /** @type {{ exception: Error | null }} */ (seen).exception);
  } finally {
    await producer.close();
  }
});

// Records sent this far apart complete this far apart: each fails its
// metadata wait max.block.ms after its own send, so they complete, and their
// callbacks are due, in send order.
const COMPLETION_GAP_MS = 60;
const gap = () => new Promise((res) => setTimeout(res, COMPLETION_GAP_MS));

test("callbacks run one at a time in completion order: a later record's callback waits for an earlier one's Promise", async () => {
  // Java runs its callbacks one after another on the producer's I/O thread, so
  // a slow callback holds back the next: here a later record's callback starts
  // only once an earlier callback's Promise settled, and flush() and close()
  // still settle, after every callback they cover.
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  let release = /** @type {((value?: unknown) => void) | undefined} */ (undefined);
  const released = new Promise((res) => (release = res));
  try {
    /** @type {string[]} */
    const events = [];
    /** @type {Array<Promise<KafkaFuture<RecordMetadata>>>} */
    const sends = [];
    for (let i = 0; i < 4; i++) {
      sends.push(
        producer.send({
          record: new ProducerRecord({ topic: `unit-serial-${i}`, value: Buffer.from('v') }),
          callback: async () => {
            events.push(`start ${i}`);
            // The first holds back until released; the later ones take less
            // time the later they come, so run together they would end in
            // reverse order.
            if (i === 0) await released;
            else await new Promise((res) => setTimeout(res, (4 - i) * 40));
            events.push(`end ${i}`);
          },
        }),
      );
      await gap();
    }
    // Every record has completed, and the first callback's Promise holds the
    // others back: none of them started, and neither flush() nor a future is done.
    const futures = await Promise.all(sends);
    let flushed = false;
    const flush = producer.flush().then(() => (flushed = true));
    await new Promise((res) => setTimeout(res, 200));
    assert.deepEqual(events, ['start 0']);
    assert.equal(flushed, false);
    assert.deepEqual(
      futures.map((f) => f.isDone()),
      [false, false, false, false],
    );
    release?.();
    await flush;
    assert.deepEqual(events, ['start 0', 'end 0', 'start 1', 'end 1', 'start 2', 'end 2', 'start 3', 'end 3']);
    // Each record's future completed after its own callback, with its own outcome.
    for (let i = 0; i < 4; i++) {
      assert.equal(futures[i].isDone(), true);
      await assert.rejects(delivery(futures[i]), { name: 'TimeoutError', message: TIMEOUT_MESSAGE(`unit-serial-${i}`) });
    }
  } finally {
    // A failed check must not leave close() waiting for the first callback.
    release?.();
    await producer.close();
  }
}, 30000);

test("a callback's timed get() on a later record's future times out: that record's callback waits for this one", async () => {
  // As Java's get(timeout), called on the I/O thread from a callback, times out
  // on a record whose callback that thread runs next: the later record's future
  // completes only after its callback ran, which waits for this callback.
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  try {
    const { TimeoutError } = require('@confluentinc/kafka-javascript');
    /** @type {string[]} */
    const events = [];
    /** @type {Promise<KafkaFuture<RecordMetadata>> | undefined} */
    let second;
    /** @type {unknown} */
    let outcome;
    const first = producer.send({
      record: new ProducerRecord({ topic: 'unit-timed-1', value: Buffer.from('v') }),
      callback: async () => {
        events.push('first callback');
        const future = /** @type {KafkaFuture<RecordMetadata>} */ (await second);
        outcome = await future.get({ timeout: 200 }).then(
          () => 'resolved',
          (/** @type {unknown} */ e) => e,
        );
        events.push('first callback ends');
      },
    });
    await gap();
    second = producer.send({
      record: new ProducerRecord({ topic: 'unit-timed-2', value: Buffer.from('v') }),
      callback: () => {
        events.push('second callback');
      },
    });
    const [firstFuture, secondFuture] = await Promise.all([first, second]);
    await producer.flush();
    assert.ok(outcome instanceof TimeoutError, `the timed get() settled with ${String(outcome)}`);
    assert.equal(outcome.constructor, TimeoutError);
    assert.deepEqual(events, ['first callback', 'first callback ends', 'second callback']);
    // Then each record completed with its own delivery error.
    await assert.rejects(delivery(firstFuture), { name: 'TimeoutError', message: TIMEOUT_MESSAGE('unit-timed-1') });
    await assert.rejects(delivery(secondFuture), { name: 'TimeoutError', message: TIMEOUT_MESSAGE('unit-timed-2') });
  } finally {
    await producer.close();
  }
}, 30000);

test('a callback queued behind a pending Promise runs once its turn comes, before close() settles', async () => {
  // close() waits for every callback of the records it covers, the ones still
  // waiting for their turn included, as Java's close() joins the I/O thread.
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  /** @type {string[]} */
  const events = [];
  let release = /** @type {((value?: unknown) => void) | undefined} */ (undefined);
  const released = new Promise((res) => (release = res));
  /** @type {Promise<unknown> | null} */
  let closing = null;
  try {
    const first = producer.send({
      record: new ProducerRecord({ topic: 'unit-close-1', value: Buffer.from('v') }),
      callback: async () => {
        events.push('first');
        await released;
      },
    });
    await gap();
    const second = producer.send({
      record: new ProducerRecord({ topic: 'unit-close-2', value: Buffer.from('v') }),
      callback: () => {
        events.push('second');
      },
    });
    const futures = await Promise.all([first, second]);
    let closed = false;
    closing = producer.close().then(() => (closed = true));
    await new Promise((res) => setTimeout(res, 200));
    assert.deepEqual(events, ['first']);
    assert.equal(closed, false);
    release?.();
    await closing;
    assert.deepEqual(events, ['first', 'second']);
    assert.deepEqual(
      futures.map((f) => f.isDone()),
      [true, true],
    );
  } finally {
    // A failed check must not leave the close waiting for the first callback.
    release?.();
    await (closing ?? producer.close());
  }
}, 30000);

test("a callback's rejection is handled as a throw: the outcome is unchanged and nothing surfaces", () => {
  const r = runNode(`
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    process.on('unhandledRejection', (e) => console.log('unhandled:', e && e.message));
    (async () => {
      const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 200 } });
      const future = await p.send({
        record: new ProducerRecord({ topic: 'rejects', value: Buffer.from('v') }),
        callback: () => Promise.reject(new Error('boom')),
      });
      await p.flush();
      const outcome = await future.get().then(() => 'delivered', (e) => e.cause.name);
      // A thenable that is no Promise, and a then getter that throws, too.
      const thenable = await p.send({ record: new ProducerRecord({ topic: 'rejects', value: Buffer.from('v') }), callback: () => ({ then: (ok, fail) => fail(1) }) });
      const getter = await p.send({ record: new ProducerRecord({ topic: 'rejects', value: Buffer.from('v') }), callback: () => ({ get then() { throw new Error('getter'); } }) });
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
        futures.push(await p.send({ record: new ProducerRecord({ topic: 'exotic', value: Buffer.from('v') }), callback: () => Promise.reject(make()) }));
        futures.push(await p.send({ record: new ProducerRecord({ topic: 'exotic', value: Buffer.from('v') }), callback: () => { throw make(); } }));
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
        record: new ProducerRecord({ topic: 'never', value: Buffer.from('v') }),
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
  let release = /** @type {((value?: unknown) => void) | undefined} */ (undefined);
  const released = new Promise((res) => (release = res));
  await producer.send({ record: new ProducerRecord({ topic: 'unit-dispose', value: Buffer.from('v') }), callback: () => released });
  let closed = false;
  const closing = producer[Symbol.asyncDispose]().then(() => (closed = true));
  assert.ok(closing instanceof Promise);
  await new Promise((res) => setTimeout(res, 200));
  assert.equal(closed, false);
  assert.ok(release);
  release();
  await closing;
  await assert.rejects(producer.send(new ProducerRecord({ topic: 't', value: Buffer.from('v') })), {
    name: 'IllegalStateError',
    message: CLOSED_MESSAGE,
  });
  // Disposing again resolves, as close() again does: the core's second
  // close, which holds Node until its answer, as any call made on a closed
  // producer does (javascript/CLAUDE.md, "Threads and liveness").
  await producer[Symbol.asyncDispose]();
});

test('flush inside a callback rejects with the core error', async () => {
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  try {
    let flushed = /** @type {Promise<void> | undefined} */ (undefined);
    const future = await producer.send({
      record: new ProducerRecord({ topic: 'unit-f', value: Buffer.from('v') }),
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
      producer.send({ record: new ProducerRecord({ topic: 'unit-h', value: Buffer.from('v') }), callback: () => calls++ }),
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

/**
 * A future with the library's own completion (test/_internal.js).
 *
 * @template T
 * @typedef {import('../../_internal').InternalFuture<T>} InternalFuture
 */

// The constructor is TypeScript-private and the factory @internal (left out
// of the declarations, test/typing); there is no run-time guard.
test('KafkaFuture is made by the client, through its library-only factory', () => {
  /** @type {InternalFuture<string>} */
  const future = createFuture(true);
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
test('after close every call goes to the core: send rejects with its closed error, flush and close resolve', async () => {
  /** @param {unknown} e */
  const closedError = (e) => {
    // Java's IllegalStateException is a java.lang exception, not a KafkaException.
    assert.ok(e instanceof IllegalStateError);
    assert.ok(!(e instanceof KafkaError));
    assert.equal(e.name, 'IllegalStateError');
    assert.equal(e.message, CLOSED_MESSAGE);
    return true;
  };
  // Nothing else holds the event loop: each call made on the closed producer
  // holds it until its answer (javascript/CLAUDE.md, "Threads and liveness").
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  const first = producer.close();
  // A second close while the first runs is a close of its own, in the core.
  const second = producer.close();
  assert.notEqual(second, first);
  await first;
  // Sent once the close was awaited (callers await, javascript/CLAUDE.md):
  // a send racing the close may get either of Java's outcomes.
  const afterClose = producer.send({ record: new ProducerRecord({ topic: 't', value: Buffer.from('v') }) });
  assert.ok(afterClose instanceof Promise);
  await assert.rejects(afterClose, closedError);
  // Java's flush() has no closed check: the core's flush resolves.
  await producer.flush();
  await second;
  await producer.close();
  await producer.close({ timeout: 0 });
  await assert.rejects(producer.send({ record: new ProducerRecord({ topic: 't', value: Buffer.from('v') }) }), closedError);
  await producer.flush();
});

test("close checks its timeout, open or closed: the closed state is the core's", async () => {
  /** @param {unknown} e */
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
  /** @param {KafkaProducer} producer */
  const checks = async (producer) => {
    await assert.rejects(producer.close({ timeout: -1 }), negative);
    await assert.rejects(producer.close(-1), negative);
    await assert.rejects(producer.close(-0.5), negative);
    await assert.rejects(producer.close({ timeout: Temporal.Duration.from({ milliseconds: -1 }) }), negative);
    await assert.rejects(producer.close(Temporal.Duration.from({ milliseconds: -1 })), negative);
    // @ts-expect-error a string timeout: the duration check rejects with TypeError
    await assert.rejects(producer.close({ timeout: '5' }), notADuration);
    // A bare value that is neither a number nor a Temporal.Duration fits no form.
    // @ts-expect-error a bare string: the form check rejects with IllegalArgumentError
    await assert.rejects(producer.close('5'), {
      name: 'IllegalArgumentError',
      message: 'close() takes one of (), (timeout); got a string',
    });
    // @ts-expect-error a null timeout: the null check rejects with TypeError
    await assert.rejects(producer.close({ timeout: null }), { name: 'TypeError', message: 'timeout must not be null' });
    // @ts-expect-error a null timeout: the null check rejects with TypeError
    await assert.rejects(producer.close(null), { name: 'TypeError', message: 'timeout must not be null' });
    // @ts-expect-error an undefined timeout: the undefined check rejects with TypeError
    await assert.rejects(producer.close({ timeout: undefined }), {
      name: 'TypeError',
      message: "timeout must not be undefined. Leave the key out, or pass null for Java's null",
    });
    // @ts-expect-error an undefined timeout: the undefined check rejects with TypeError
    await assert.rejects(producer.close(undefined), {
      name: 'TypeError',
      message: "timeout must not be undefined. Leave the argument out, or pass null for Java's null",
    });
    for (const value of [NaN, -Infinity]) {
      await assert.rejects(producer.close(value), {
        name: 'RangeError',
        message: `timeout must be a number of milliseconds, not ${value}`,
      });
    }
    await assert.rejects(producer.close({ timeout: 2 ** 53 }), {
      name: 'RangeError',
      message: 'timeout of 9007199254740992 ms is above Number.MAX_SAFE_INTEGER ms; pass Infinity for no limit',
    });
    await assert.rejects(producer.close({ timeout: Temporal.Duration.from({ months: 1 }) }), (/** @type {RangeError} */ e) => {
      assert.equal(e.constructor, RangeError);
      assert.equal(e.message, 'timeout must not hold years, months or weeks, which have no fixed length');
      return true;
    });
  };
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  await checks(producer);
  // The producer is still open.
  await producer.flush();
  await producer.close();
  // Closed: the same checks, the same errors.
  await checks(producer);
  // Accepted: a whole or fractional number, Infinity, a Temporal.Duration, bare or named.
  for (const timeout of [
    { timeout: 0 },
    1.5,
    Infinity,
    { timeout: Temporal.Duration.from({ seconds: 1 }) },
    Temporal.Duration.from({ seconds: 1 }),
  ]) {
    // A row of the union of close's forms, valid each, for which tsc picks no
    // single overload: the call goes through the implementation's parameter.
    const producer = /** @type {{ close(timeout: unknown): Promise<void> }} */ (
      /** @type {unknown} */ (new KafkaProducer({ configs: UNREACHABLE }))
    );
    await producer.close(timeout);
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
    { cwd: path.join(__dirname, '..', '..', '..'), encoding: 'utf8', timeout: 60000, env: { ...process.env, ...POISON, RUST_LOG: 'info' } },
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
    const record = new ProducerRecord({ topic: 'unit-forms', value: Buffer.from('v') });
    for (const [args, given] of [
      [{ timeoutMs: 0 }, 'timeoutMs'],
      [{ timeout: 0, timeoutMs: 0 }, 'timeout, timeoutMs'],
    ]) {
      // @ts-expect-error keys no form has: the form check rejects with IllegalArgumentError
      const closed = producer.close(args);
      assert.ok(closed instanceof Promise);
      await assert.rejects(closed, {
        name: 'IllegalArgumentError',
        message: `close() takes one of (), (timeout); got (${given})`,
      });
    }
    // send() with no argument at all; send(undefined) names the record.
    // @ts-expect-error no argument: the form check rejects with IllegalArgumentError
    await assert.rejects(producer.send(), {
      name: 'IllegalArgumentError',
      message: 'send() takes one of (record), (record, callback); got ()',
    });
    // @ts-expect-error an undefined record: the undefined check rejects with TypeError
    await assert.rejects(producer.send(undefined), {
      name: 'TypeError',
      message: "record must not be undefined. Leave the argument out, or pass null for Java's null",
    });
    // A bare value that is no ProducerRecord fits no form.
    // @ts-expect-error a bare number: the form check rejects with IllegalArgumentError
    await assert.rejects(producer.send(5), {
      name: 'IllegalArgumentError',
      message: 'send() takes one of (record), (record, callback); got a number',
    });
    // Java's send(record, callback) is send({record, callback}): a second
    // argument is refused, never dropped with its callback.
    let ran = 0;
    // @ts-expect-error a second argument: the form check rejects with IllegalArgumentError
    const twoArgs = producer.send(record, () => ran++);
    assert.ok(twoArgs instanceof Promise);
    await assert.rejects(twoArgs, {
      name: 'IllegalArgumentError',
      message: 'send() takes one of (record), (record, callback); got 2 arguments, not one object of named parameters',
    });
    assert.equal(ran, 0);
    // Extra arguments are refused.
    // @ts-expect-error a second argument: the form check rejects with IllegalArgumentError
    await assert.rejects(producer.close(0, 1), {
      name: 'IllegalArgumentError',
      message: 'close() takes one of (), (timeout); got 2 arguments, not one object of named parameters',
    });
    for (const [args, given] of [
      [{}, ''],
      [{ callback: () => {} }, 'callback'],
      [{ record, callback: null, extra: 1 }, 'record, callback, extra'],
    ]) {
      // @ts-expect-error keys no form has: the form check rejects with IllegalArgumentError
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
      // A row of the union of send's forms, valid each, for which tsc picks no
      // single overload: the call goes through the implementation's parameter.
      const sender = /** @type {{ send(args: unknown): ReturnType<typeof producer.send> }} */ (/** @type {unknown} */ (producer));
      const future = await sender.send(args);
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
    // @ts-expect-error arguments to flush(): the form check rejects with IllegalArgumentError
    const withCallback = producer.flush(100, () => ran++);
    assert.ok(withCallback instanceof Promise);
    await assert.rejects(withCallback, {
      name: 'IllegalArgumentError',
      message: 'flush() takes one of (); got 2 arguments, not one object of named parameters',
    });
    // @ts-expect-error an argument to flush(): the form check rejects with IllegalArgumentError
    await assert.rejects(producer.flush({ timeout: 100 }), {
      name: 'IllegalArgumentError',
      message: 'flush() takes one of (); got (timeout)',
    });
    // @ts-expect-error an argument to flush(): the form check rejects with IllegalArgumentError
    await assert.rejects(producer.flush(100), { name: 'IllegalArgumentError', message: 'flush() takes one of (); got a number' });
    assert.equal(ran, 0);
    // undefined is never "not given": flush(undefined) is refused; flush({}) is flush().
    // @ts-expect-error an undefined argument: the undefined check rejects with TypeError
    await assert.rejects(producer.flush(undefined), {
      name: 'TypeError',
      message: 'flush() takes no argument: leave it out rather than pass undefined',
    });
    // @ts-expect-error an argument to flush(), whose one form takes none: the run time reads an empty options object as flush()
    await producer.flush({});
  } finally {
    await producer.close();
  }
  // The form is checked before the closed producer.
  // @ts-expect-error an argument to flush(): the form check rejects with IllegalArgumentError
  await assert.rejects(producer.flush(100), { name: 'IllegalArgumentError' });
});

test('send reads only the parameters the form check saw: own enumerable keys', async () => {
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  try {
    const record = new ProducerRecord({ topic: 'unit-own', value: Buffer.from('v') });
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
    /** @type {Record<string, unknown>} */ (Object.prototype).callback = () => ran++;
    let polluted;
    try {
      polluted = await producer.send({ record });
    } finally {
      delete (/** @type {Record<string, unknown>} */ (Object.prototype).callback);
    }
    await assert.rejects(delivery(polluted), { name: 'TimeoutError' });
    assert.equal(ran, 0);
  } finally {
    await producer.close();
  }
});

test("a closed producer checks a send's record and callback before the core reports it closed", async () => {
  // The binding keeps no closed state: the record is checked and converted
  // in JavaScript, then the core refuses it, where Java's doSend checks the
  // closed producer first (a deviation, documented on send()).
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  await producer.close();
  // @ts-expect-error a null record: the null check rejects with TypeError
  await assert.rejects(producer.send({ record: null }), { name: 'TypeError', message: 'record must not be null' });
  // @ts-expect-error a null record: the null check rejects with TypeError
  await assert.rejects(producer.send(null), { name: 'TypeError', message: 'record must not be null' });
  // @ts-expect-error a plain object for a ProducerRecord: the class check rejects with TypeError
  await assert.rejects(producer.send({ record: {} }), { name: 'TypeError', message: 'record must be a ProducerRecord' });
  // @ts-expect-error a string callback: the type check rejects with TypeError
  await assert.rejects(producer.send({ record: new ProducerRecord({ topic: 't', value: Buffer.from('v') }), callback: 'cb' }), {
    name: 'TypeError',
    message: 'callback must be a function ({ metadata, exception }) => void',
  });
  await assert.rejects(producer.send({ record: new ProducerRecord({ topic: 't', value: Buffer.from('v') }) }), {
    name: 'IllegalStateError',
    message: CLOSED_MESSAGE,
  });
  // The form is checked first: a bare value that fits no form included.
  // @ts-expect-error a missing record: the form check rejects with IllegalArgumentError
  await assert.rejects(producer.send({}), {
    name: 'IllegalArgumentError',
    message: 'send() takes one of (record), (record, callback); got ()',
  });
  // @ts-expect-error a bare number: the form check rejects with IllegalArgumentError
  await assert.rejects(producer.send(42), {
    name: 'IllegalArgumentError',
    message: 'send() takes one of (record), (record, callback); got a number',
  });
});

test("the constructor takes Java's {configs} or the configs on their own, a plain object", async () => {
  // The configs on their own and named build the same producer.
  for (const args of [UNREACHABLE, { configs: UNREACHABLE }]) {
    // A row of the union of the constructor's forms, valid each, for which tsc
    // picks no single overload: the call goes through the implementation's parameter.
    const Producer = /** @type {new (args: unknown) => InstanceType<typeof KafkaProducer>} */ (/** @type {unknown} */ (KafkaProducer));
    const producer = new Producer(args);
    const future = await producer.send(new ProducerRecord({ topic: 'unit-ctor', value: Buffer.from('v') }));
    await assert.rejects(delivery(future), { name: 'TimeoutError', message: TIMEOUT_MESSAGE('unit-ctor') });
    await producer.close();
  }
  // A plain object is the options object when it has the key configs: a
  // stray key is then a form error, not a config key.
  // @ts-expect-error no argument: the form check throws IllegalArgumentError
  assert.throws(() => new KafkaProducer(), { name: 'IllegalArgumentError', message: `${CTOR_FORMS}; got ()` });
  // @ts-expect-error undefined configs: the undefined check throws TypeError
  assert.throws(() => new KafkaProducer(undefined), {
    name: 'TypeError',
    message: "configs must not be undefined. Leave the argument out, or pass null for Java's null",
  });
  // @ts-expect-error a second argument: the form check throws IllegalArgumentError
  assert.throws(() => new KafkaProducer(UNREACHABLE, {}), {
    name: 'IllegalArgumentError',
    message: `${CTOR_FORMS}; got 2 arguments, not one object of named parameters`,
  });
  for (const [args, given] of [
    [{ configs: UNREACHABLE, keySerializer: null }, 'configs, keySerializer'],
    [{ 'bootstrap.servers': '127.0.0.1:1', configs: UNREACHABLE }, 'configs, bootstrap.servers'],
  ]) {
    // @ts-expect-error keys no form has: the form check throws IllegalArgumentError
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
    // @ts-expect-error a Map for the configs: the plain-object check throws TypeError
    assert.throws(() => new KafkaProducer(args), { name: 'TypeError', message: 'configs must be a plain object, not a Map' });
  }
  assert.throws(() => new KafkaProducer({ configs: null }), { name: 'TypeError', message: 'configs must not be null' });
  // @ts-expect-error null configs: the null check throws TypeError
  assert.throws(() => new KafkaProducer(null), { name: 'TypeError', message: 'configs must not be null' });
  assert.throws(() => new KafkaProducer({ configs: undefined }), {
    name: 'TypeError',
    message: "configs must not be undefined. Leave the key out, or pass null for Java's null",
  });
  // @ts-expect-error a string for the configs: the form check throws IllegalArgumentError
  assert.throws(() => new KafkaProducer('bootstrap.servers=x'), {
    name: 'IllegalArgumentError',
    message: `${CTOR_FORMS}; got a string`,
  });
  // A config value set to undefined is still left out.
  const producer = new KafkaProducer({ ...UNREACHABLE, 'client.id': undefined });
  await producer.close();
});

test('a negative close timeout on an open producer is rejected first and changes nothing', async () => {
  /** @param {unknown} e */
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
  const sent = producer.send({ record: new ProducerRecord({ topic: 'unit-n', value: Buffer.from('v') }) });
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
  await assert.rejects(producer.send({ record: new ProducerRecord({ topic: 'unit-n', value: Buffer.from('v') }) }), {
    message: CLOSED_MESSAGE,
  });

  // A close() in the same turn is a close of its own, not the rejected one.
  const other = new KafkaProducer({ configs: UNREACHABLE });
  const rejectedToo = other.close({ timeout: -1 });
  const closed = other.close();
  await assert.rejects(rejectedToo, negative);
  await closed;
  await assert.rejects(other.send({ record: new ProducerRecord({ topic: 'unit-n', value: Buffer.from('v') }) }), {
    message: CLOSED_MESSAGE,
  });
});

test('close() while a send waits for metadata rejects that send with a bare KafkaError', async () => {
  const producer = new KafkaProducer({ configs: { ...UNREACHABLE, 'max.block.ms': 60000 } });
  /** @type {string[]} */
  const calls = [];
  const sent = producer.send({
    record: new ProducerRecord({ topic: 'unit-close-waiting', value: Buffer.from('v') }),
    callback: () => {
      calls.push('callback');
    },
  });
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
  // ProducerMetadataTest.testMetadataAwaitAfterClose: Java's
  // ProducerMetadata#awaitUpdate throws a bare KafkaException("Requested
  // metadata update after close"), which KafkaProducer#doSend wraps. The cause
  // is what shows the close ended the wait, not a timeout.
  await assert.rejects(sent, (/** @type {Error} */ e) => {
    assert.equal(e.constructor, KafkaError);
    assert.equal(e.message, 'Producer closed while send in progress');
    assert.ok(e.cause instanceof KafkaError);
    assert.equal(e.cause.constructor, KafkaError);
    assert.equal(e.cause.message, 'Requested metadata update after close');
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
    /** @type {Array<Error | null>} */
    const calls = [];
    const sent = producer.send({
      record: new ProducerRecord({ topic: 'unit-flush-waiting', value: Buffer.from('v') }),
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
    // The deepEqual above narrowed calls to an empty array: read it as it is now.
    const [first] = /** @type {Array<Error | null>} */ (calls);
    assert.ok(first);
    assert.equal(first.name, 'TimeoutError');
    assert.equal(first.message, message);
  } finally {
    await producer.close();
  }
});

test('argument errors reject: send() and close() never throw', async () => {
  const producer = new KafkaProducer({ configs: UNREACHABLE });
  try {
    // @ts-expect-error a plain object for a ProducerRecord: the class check rejects with TypeError
    const sent = producer.send({ record: { topic: 't' } });
    assert.ok(sent instanceof Promise);
    await assert.rejects(sent, { name: 'TypeError', message: 'record must be a ProducerRecord' });
    // Java's doSend dereferences a null record.
    // @ts-expect-error a null record: the null check rejects with TypeError
    await assert.rejects(producer.send({ record: null }), { name: 'TypeError', message: 'record must not be null' });
    // @ts-expect-error a null record: the null check rejects with TypeError
    await assert.rejects(producer.send(null), { name: 'TypeError', message: 'record must not be null' });
    // @ts-expect-error a string callback: the type check rejects with TypeError
    await assert.rejects(producer.send({ record: new ProducerRecord({ topic: 't', value: Buffer.from('v') }), callback: 'cb' }), {
      name: 'TypeError',
      message: 'callback must be a function ({ metadata, exception }) => void',
    });
    // @ts-expect-error a string timeout: the duration check rejects with TypeError
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
  /** @type {string[]} */
  const calls = [];
  const callback = () => calls.push('callback');
  await assert.rejects(producer.send({ record: new ProducerRecord({ topic: 'good\0bad', value: Buffer.from('v') }), callback }), {
    name: 'TypeError',
    message: 'topic must be a string without null bytes',
  });
  // A header key crosses to the core when the header is made (the native
  // RecordHeader), so a NUL in it is refused there, before any send.
  assert.throws(() => new Header({ key: 'k\0x', value: Buffer.from('h') }), {
    name: 'TypeError',
    message: 'a header key must be a string without null bytes',
  });
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
      await p
        .send({ record: new ProducerRecord({ topic: 'a\\0b', value: Buffer.from('v') }), callback: () => console.log('callback ran') })
        .catch((e) => console.log(e.name, e.message));
      try {
        new Header({ key: 'a\\0b', value: Buffer.from('v') });
      } catch (e) {
        console.log(e.name, e.message);
      }
      await p.close();
    })();
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(
    r.stdout,
    'TypeError topic must be a string without null bytes\n' + 'TypeError a header key must be a string without null bytes\n',
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
/**
 * @param {string} script
 * @param {number} [timeout]
 * @param {string[]} [flags]
 * @param {Record<string, string>} [env]
 */
function runNode(script, timeout = 30000, flags = [], env = undefined) {
  const started = process.hrtime.bigint();
  const r = spawnSync(process.execPath, [...flags, '-e', script], {
    cwd: path.join(__dirname, '..', '..', '..'),
    encoding: 'utf8',
    timeout,
    env: { ...process.env, ...POISON, ...env },
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
      record: new ProducerRecord({ topic: 'alive', value: Buffer.from('v') }),
      callback: ({ exception }) => console.log('callback:', exception.message),
    }).then(() => p.close());
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'callback: Topic alive not present in metadata after 1500 ms.\n');
  assert.ok(r.ms >= 1400, `exited after ${r.ms} ms`);
});

// A call made on a closed producer holds Node until it settles, as Java's
// call returns, and so does a second close() whose answer comes after the
// first close's (javascript/CLAUDE.md, "Threads and liveness"): each child
// below has nothing else holding Node, prints a line once the call settled,
// and exits with 0. Java 4.4.0's answers are the expected ones.

// One child per call: a call that never settled would end the child there.
const AFTER_CLOSE = [
  // Java's close(), close(Duration.ZERO) and try-with-resources' close() return.
  { call: 'p.close()', outcome: 'resolved' },
  { call: 'p.close(0)', outcome: 'resolved' },
  { call: 'p[Symbol.asyncDispose]()', outcome: 'resolved' },
  // Java's flush() has no closed check: it returns.
  { call: 'p.flush()', outcome: 'resolved' },
  // A topic the closed producer has no metadata for: Java's partitionsFor()
  // asks for a metadata update, and ProducerMetadata#awaitUpdate throws
  // KafkaException("Requested metadata update after close").
  { call: "p.partitionsFor('t')", outcome: 'KafkaError: Requested metadata update after close' },
  // Answered inside the call, with Java's errors.
  { call: 'p.close(-1)', outcome: 'IllegalArgumentError: The timeout cannot be negative.' },
  { call: "p.send(new ProducerRecord({ topic: 't', value: Buffer.from('v') }))", outcome: `IllegalStateError: ${CLOSED_MESSAGE}` },
];

for (const { call, outcome } of AFTER_CLOSE) {
  test(`${call} after close() settles with Java's answer while nothing else holds Node`, () => {
    const r = runNode(`
      const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
      const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1' } });
      (async () => {
        await p.close();
        const outcome = await ${call}.then(
          (value) => (value === undefined ? 'resolved' : 'resolved with ' + String(value)),
          (e) => e.constructor.name + ': ' + e.message,
        );
        console.log('after the call:', outcome);
      })();
    `);
    assert.equal(r.status, 0, r.stderr);
    assert.equal(r.stdout, `after the call: ${outcome}\n`);
  });
}

test("a late close() and flush() settle while nothing else holds Node, after a record's callback ran", () => {
  // The record's callback fires with the metadata timeout (Java's send()
  // returns a failed future then: its TimeoutException is an ApiException),
  // then the producer is closed, and the late calls must still hold Node
  // until their answers: Java's close() and flush() return. A count ended by
  // the record's callback as well as by its hand-over would let Node go
  // before them. The lines are printed at the end: a write to stdout holds
  // the event loop for a moment of its own.
  const r = runNode(`
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': ${MAX_BLOCK_MS} } });
    const outcome = (call) => call.then(() => 'resolved', (e) => e.constructor.name + ': ' + e.message);
    const lines = [];
    (async () => {
      await p.send({
        record: new ProducerRecord({ topic: 't', value: Buffer.from('v') }),
        // A block body: a callback that returns a value is awaited (rule 6).
        callback: ({ exception }) => {
          lines.push('callback: ' + exception.constructor.name + ': ' + exception.message);
        },
      });
      await p.close();
      lines.push('closed');
      lines.push('late close: ' + (await outcome(p.close())));
      lines.push('late flush: ' + (await outcome(p.flush())));
      console.log(lines.join('\\n'));
    })();
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, `callback: TimeoutError: ${TIMEOUT_MESSAGE('t')}\nclosed\nlate close: resolved\nlate flush: resolved\n`);
});

// Two calls at once, awaited together (javascript/CLAUDE.md, "How the client
// is used"). The second close() is answered after the first close's
// completion, which only the hold of a waiting call covers. In the flush
// pairs flush has been answered first in every round run, so those two check
// Java's answer and the exit. Each round's producer stays reachable until
// its calls settled (`kept`): one collected with a call pending is the JS
// conventions' Future improvements entry, not these tests' subject.
for (const pair of ['p.close(), p.close()', 'p.close(), p.close(0)', 'p.close(), p.flush()', 'p.flush(), p.close()']) {
  test(`${pair} at once on an open producer both settle, in each of 25 rounds`, () => {
    const r = runNode(
      `
      const { KafkaProducer } = require('@confluentinc/kafka-javascript/producer');
      let kept;
      (async () => {
        const outcomes = new Set();
        for (let round = 0; round < 25; round++) {
          const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1' } });
          for (const o of await Promise.allSettled([${pair}])) outcomes.add(o.status);
          kept = p;
        }
        console.log('all settled:', [...outcomes].join(', '));
      })();
    `,
      60000,
    );
    assert.equal(r.status, 0, r.stderr);
    // Java's close() and flush() both return.
    assert.equal(r.stdout, 'all settled: fulfilled\n');
  });
}

// The hold still ends: a closed producer with no call waiting lets the child
// exit at once, the time from the last line to the process's exit event.
for (const { what, body } of [
  { what: 'a closed producer with no call waiting', body: 'await p.close();' },
  {
    what: 'a closed producer whose late calls settled',
    body: "await p.close();\n        await p.flush();\n        await p.close();\n        await p.send(new ProducerRecord({ topic: 't', value: Buffer.from('v') })).catch(() => {});",
  },
]) {
  test(`${what} lets the process exit right after its last answer`, () => {
    const r = runNode(`
      const { performance } = require('node:perf_hooks');
      const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
      let mark;
      process.on('exit', () => console.log('exited after', performance.now() - mark < 1000 ? 'less than 1s' : 'more'));
      const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1' } });
      (async () => {
        ${body}
        mark = performance.now();
        console.log('answered');
      })();
    `);
    assert.equal(r.status, 0, r.stderr);
    assert.equal(r.stdout, 'answered\nexited after less than 1s\n');
  });
}

test('an open producer keeps the process running after a call made on it settled', () => {
  // The count of calls waiting ends; the open producer's hold does not.
  const r = runNode(
    `
    const { KafkaProducer } = require('@confluentinc/kafka-javascript/producer');
    const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1' } });
    p.flush().then(() => console.log('flushed'));
  `,
    3000,
  );
  assert.equal(r.stdout, 'flushed\n');
  assert.equal(r.signal, 'SIGTERM', `the process exited on its own: status ${r.status}`);
});

// A guard of Java's answer and of the exit: this call is answered before the
// close's completion, while the open producer still holds Node, so it passes
// without the count of waiting calls too.
test("a send waiting for metadata across close(0) rejects with Java's error and the process exits, in each of 25 rounds", () => {
  // The record waits for metadata in the core when close(0) comes. Java's
  // doSend throws KafkaException("Producer closed while send in progress"),
  // caused by "Requested metadata update after close". Each producer stays
  // reachable until its calls settled, as above.
  const r = runNode(
    `
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    const describe = (e) => e.constructor.name + ': ' + e.message + ', caused by ' + e.cause.constructor.name + ': ' + e.cause.message;
    let kept;
    (async () => {
      const outcomes = new Set();
      for (let round = 0; round < 25; round++) {
        const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 60000 } });
        const sent = p.send(new ProducerRecord({ topic: 't', value: Buffer.from('v') })).then(() => 'resolved', describe);
        // Long enough for the record to be waiting for metadata.
        await new Promise((res) => setTimeout(res, 100));
        await p.close(0);
        outcomes.add(await sent);
        kept = p;
      }
      console.log('all settled:', [...outcomes].join(', '));
    })();
  `,
    60000,
  );
  assert.equal(r.status, 0, r.stderr);
  assert.equal(
    r.stdout,
    'all settled: KafkaError: Producer closed while send in progress, caused by KafkaError: Requested metadata update after close\n',
  );
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
        // A Buffer value, so every record is handed over and waits in the
        // core; a refusal must not crash the worker before 'sent' is posted.
        p.send({ record: new ProducerRecord({ topic: 'teardown', value: Buffer.from(String(i)) }), callback: () => {} }).catch(() => {});
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
    p.send({ record: new ProducerRecord({ topic: 'dropped', value: Buffer.from('v') }), callback: ({ exception }) => console.log('callback:', exception.name) });
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
(!['linux', 'darwin'].includes(process.platform) ? test.skip : test)(
  'a closed producer is destroyed when collected, a send refused after close() or failed by it included',
  () => {
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
        await p.send(new ProducerRecord({ topic: 't', value: Buffer.from('v') })).catch(() => {});
      },
      'send failed by close': async (p) => {
        // Waits for metadata in the core until the zero close fails it.
        const sent = p.send(new ProducerRecord({ topic: 't', value: Buffer.from('v') })).catch(() => {});
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
    const lines = r.stdout
      .trim()
      .split('\n')
      .map((l) => /^(.*) (\d+) (-?\d+)$/.exec(l));
    assert.deepEqual(
      lines.map((m) => m && [m[1], m[2]]),
      [
        ['close', '5'],
        ['send after close', '5'],
        ['send failed by close', '5'],
      ],
      r.stdout,
    );
    // The deepEqual above has checked that every line matched.
    for (const m of /** @type {RegExpExecArray[]} */ (lines)) assert.ok(Number(m[3]) < 10, `${m[1]}: ${m[3]} threads left over`);
  },
);

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
    p.send({ record: new ProducerRecord({ topic: 'h', value: Buffer.from('v') }), callback: () => console.log('callback ran') });
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
      const future = await p.send({ record: new ProducerRecord({ topic: 'unread', value: Buffer.from('v') }) });
      await p.close();
      console.log('done', future.isCompletedExceptionally());
    })();
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'done true\n');
});

// Serializers (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Serialization): an object with Java's serialize, taking one object
// { topic, headers, data }, run in JavaScript inside send(), awaited when it
// returns a Promise, closed when the producer closes.

/**
 * A serializer recording its calls, returning `out(data)`.
 *
 * @template T
 * @param {(data: T | null) => Uint8Array | null} out
 * @param {SerializeCall[]} calls
 * @returns {Serializer<T>}
 */
function recording(out, calls) {
  return {
    serialize({ topic, headers, data }) {
      calls.push({ topic, headers, data });
      return out(data);
    },
  };
}

/**
 * Collects the process warnings emitted while `body` runs and one turn after.
 *
 * @param {() => Promise<void>} body
 */
async function warningsOf(body) {
  /** @type {Error[]} */
  const seen = [];
  /** @param {Error} w */
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
  /** @type {SerializeCall[]} */
  const calls = [];
  const keySerializer = recording((data) => (data === null ? null : Buffer.from(`k:${data}`)), calls);
  const valueSerializer = recording((data) => Buffer.from(JSON.stringify(data)), calls);
  const producer = new KafkaProducer({ configs: UNREACHABLE, keySerializer, valueSerializer });
  try {
    const record = new ProducerRecord({ topic: 'unit-serde', key: 7, value: { a: 1 } });
    const future = await producer.send(record);
    assert.deepEqual(
      calls.map((c) => [c.topic, c.data]),
      [
        ['unit-serde', 7],
        ['unit-serde', { a: 1 }],
      ],
    );
    // The record's own headers, which the serializer may change before the
    // hand-over seals them.
    assert.equal(calls[0].headers, record.headers);
    // KafkaProducerTest.testHeadersSuccess: both serializers get the record's
    // headers.
    assert.equal(calls[1].headers, record.headers);
    assert.throws(() => record.headers.add({ key: 'late', value: null }), {
      name: 'IllegalStateError',
      message: 'RecordHeaders has been closed.',
    });
    await assert.rejects(delivery(future), { name: 'TimeoutError', message: TIMEOUT_MESSAGE('unit-serde') });
    // A null key is passed to the serializer, which decides what it maps to.
    calls.length = 0;
    await producer.send(new ProducerRecord({ topic: 'unit-serde', value: null }));
    assert.deepEqual(
      calls.map((c) => c.data),
      [null, null],
    );
  } finally {
    await producer.close();
  }
});

test('a serializer may add headers before the hand-over', async () => {
  /** @type {Serializer<number[]>} */
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
    const type = record.headers.lastHeader('type');
    assert.ok(type);
    const value = type.value();
    assert.ok(value);
    assert.equal(value.toString(), 'json');
  } finally {
    await producer.close();
  }
});

test('a null serializer sends the record bytes as they are, as no serializer does', async () => {
  /** @type {SerializeCall[]} */
  const calls = [];
  const producer = new KafkaProducer({ configs: UNREACHABLE, keySerializer: null, valueSerializer: recording(() => null, calls) });
  try {
    await producer.send(new ProducerRecord({ topic: 'unit-serde-n', key: Buffer.from('k'), value: 'anything' }));
    assert.deepEqual(
      calls.map((c) => c.data),
      ['anything'],
    );
    // Without a key serializer the key must be bytes; a string points to stringSerializer() (JS conventions, Types and Class family).
    // @ts-expect-error a number key without a key serializer: the bytes check rejects with TypeError
    await assert.rejects(producer.send(new ProducerRecord({ topic: 'unit-serde-n', key: 3, value: 'v' })), {
      name: 'TypeError',
      message: 'key must be a Buffer, a Uint8Array or null when the producer has no key serializer',
    });
    // @ts-expect-error a string key without a key serializer: the bytes check rejects with TypeError
    await assert.rejects(producer.send(new ProducerRecord({ topic: 'unit-serde-n', key: 'k', value: 'v' })), {
      name: 'TypeError',
      message:
        "key is a string, and the producer has no key serializer: give the producer keySerializer: stringSerializer() (or 'key.serializer': " +
        'stringSerializer in its configuration), from @confluentinc/kafka-javascript/common/serialization, or send a Buffer or a Uint8Array',
    });
    const bytesOnly = new KafkaProducer({ configs: UNREACHABLE });
    // @ts-expect-error a string value without a value serializer: the bytes check rejects with TypeError
    await assert.rejects(bytesOnly.send(new ProducerRecord({ topic: 'unit-serde-n', value: 'v' })), {
      name: 'TypeError',
      message:
        "value is a string, and the producer has no value serializer: give the producer valueSerializer: stringSerializer() (or 'value.serializer': " +
        'stringSerializer in its configuration), from @confluentinc/kafka-javascript/common/serialization, or send a Buffer or a Uint8Array',
    });
    await bytesOnly.close();
  } finally {
    await producer.close();
  }
});

test('a serializer returning a Promise is awaited; one returning a plain value adds no microtask', async () => {
  /** @type {string[]} */
  const order = [];
  /** @type {Serializer<string>} */
  const valueSerializer = {
    serialize({ data }) {
      order.push(`serialize ${data}`);
      assert.ok(data !== null);
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
  /** @type {string[]} */
  const order = [];
  let release = /** @type {((value?: unknown) => void) | undefined} */ (undefined);
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  /** @type {Serializer<number>} */
  const keySerializer = {
    serialize() {
      order.push('key');
      return gate.then(() => (order.push('key settled'), Buffer.from('k')));
    },
  };
  /** @type {Serializer<number>} */
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
    assert.ok(release);
    release();
    await sent;
    assert.deepEqual(order, ['key', 'key settled', 'value']);
  } finally {
    await producer.close();
  }
});

test('what a serializer throws, rejects with or returns that is no bytes rejects send()', async () => {
  const boom = new Error('boom');
  /** @type {Array<[Serializer<string>, AssertPredicate]>} */
  const cases = [
    [
      {
        serialize: () => {
          throw boom;
        },
      },
      (e) => e === boom,
    ],
    [{ serialize: () => Promise.reject(boom) }, (e) => e === boom],
    [
      // @ts-expect-error a serializer returning a string: the return check rejects send() with TypeError
      { serialize: () => 'text' },
      { name: 'TypeError', message: 'valueSerializer.serialize() must return a Uint8Array or null, not a string' },
    ],
    [
      // @ts-expect-error a serializer returning undefined: the return check rejects send() with TypeError
      { serialize: () => undefined },
      { name: 'TypeError', message: 'valueSerializer.serialize() must return a Uint8Array or null, not an undefined' },
    ],
    [
      // @ts-expect-error a serializer resolving to a number: the return check rejects send() with TypeError
      { serialize: async () => 5 },
      { name: 'TypeError', message: 'valueSerializer.serialize() must return a Uint8Array or null, not a number' },
    ],
  ];
  for (const [serializer, expected] of cases) {
    const producer = new KafkaProducer({ configs: UNREACHABLE, keySerializer: null, valueSerializer: serializer });
    try {
      /** @type {unknown[]} */
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

// Java's KafkaProducer#doSend answers a serializer's throw: an ApiException
// goes to the callback and a FutureFailure, a ClassCastException becomes a
// SerializationException, anything else is rethrown.

test('PlaintextProducerSendTest.testWrongSerializer: a Buffer given to the configured stringSerializer rejects send() with SerializationError', async () => {
  const producer = new KafkaProducer({ ...UNREACHABLE, 'key.serializer': stringSerializer, 'value.serializer': stringSerializer });
  try {
    const record = new ProducerRecord({ topic: 'topic', partition: 0, key: Buffer.from('key'), value: Buffer.from('value') });
    await assert.rejects(
      // @ts-expect-error Buffers given to a producer of strings, as Java's test gives byte[] to StringSerializer
      producer.send(record),
      (e) =>
        e instanceof errors.SerializationError &&
        e.message === "Can't convert key of class Buffer to class stringSerializer specified in key.serializer" &&
        e.cause instanceof TypeError &&
        e.cause.message === 'stringSerializer: data must be a string or null, not a Buffer',
    );
  } finally {
    await producer.close();
  }
});

/**
 * A serializer that throws `error`, or rejects with it when `async`.
 *
 * @param {unknown} error
 * @param {boolean} async
 * @returns {Serializer<unknown>}
 */
function failingSerializer(error, async) {
  return {
    serialize: () => {
      if (async) return Promise.reject(error);
      throw error;
    },
  };
}

/** A serializer of the data's bytes. @type {Serializer<unknown>} */
const bytesOf = { serialize: ({ data }) => (data === null ? null : Buffer.from(String(data))) };

/**
 * A producer whose key or value serializer is `failing`, the other one
 * `bytesOf`.
 *
 * @param {Serializer<unknown>} failing
 * @param {boolean} isKey
 */
function producerFailing(failing, isKey) {
  return new KafkaProducer({
    configs: UNREACHABLE,
    keySerializer: isKey ? failing : bytesOf,
    valueSerializer: isKey ? bytesOf : failing,
  });
}

const SERIALIZER_CASES = [
  { isKey: true, async: false },
  { isKey: true, async: true },
  { isKey: false, async: false },
  { isKey: false, async: true },
];

test("a serializer's ApiError runs the callback once with Java's failure metadata, and send() resolves to a failed future", async () => {
  for (const { isKey, async } of SERIALIZER_CASES) {
    for (const partition of [3, null]) {
      const label = `${isKey ? 'key' : 'value'} ${async ? 'async' : 'sync'} partition ${partition}`;
      const error = new errors.RecordTooLargeError('too large');
      const producer = producerFailing(failingSerializer(error, async), isKey);
      try {
        /** @type {Array<{ metadata: RecordMetadata, exception: Error | null }>} */
        const calls = [];
        const record = new ProducerRecord({ topic: 'unit-serde-api', partition, key: 'k', value: 'v' });
        const future = await producer.send({ record, callback: (r) => void calls.push(r) });
        assert.equal(calls.length, 1, label);
        assert.equal(calls[0].exception, error, label);
        const { metadata } = calls[0];
        assert.deepEqual(
          [metadata.topic, metadata.partition, metadata.offset, metadata.hasOffset, metadata.timestamp],
          ['unit-serde-api', partition ?? -1, -1n, false, -1n],
          label,
        );
        assert.deepEqual([metadata.serializedKeySize, metadata.serializedValueSize], [-1, -1], label);
        // Java's FutureFailure: done, failed, not cancellable.
        assert.equal(future.isDone(), true, label);
        assert.equal(future.isCompletedExceptionally(), true, label);
        assert.equal(future.cancel({ mayInterruptIfRunning: true }), false, label);
        await assert.rejects(future.get(), (e) => e instanceof ExecutionError && e.cause === error, label);
        assert.equal(calls.length, 1, label);
        // The record never reached the producer: its headers stay writable.
        record.headers.add({ key: 'still', value: null });
      } finally {
        await producer.close();
      }
    }
  }
});

test("a serializer's ApiError without a callback resolves send() to the failed future", async () => {
  for (const { isKey, async } of SERIALIZER_CASES) {
    const error = new errors.InvalidTopicError('invalid');
    const producer = producerFailing(failingSerializer(error, async), isKey);
    try {
      const future = await producer.send(new ProducerRecord({ topic: 'unit-serde-api', value: 'v' }));
      await assert.rejects(future.get(), (e) => e instanceof ExecutionError && e.cause === error);
    } finally {
      await producer.close();
    }
  }
});

test("a key serializer's ApiError leaves the value serializer unrun, as Java's doSend", async () => {
  const error = new errors.RecordTooLargeError('too large');
  let valueRan = false;
  /** @type {Serializer<unknown>} */
  const valueSerializer = { serialize: () => ((valueRan = true), null) };
  const producer = new KafkaProducer({ configs: UNREACHABLE, keySerializer: failingSerializer(error, false), valueSerializer });
  try {
    await producer.send(new ProducerRecord({ topic: 'unit-serde-api', key: 'k', value: 'v' }));
    assert.equal(valueRan, false);
  } finally {
    await producer.close();
  }
});

test("the callback of a serializer's ApiError: a Promise it returns is awaited, and what it throws or rejects with rejects send()", async () => {
  const error = new errors.RecordTooLargeError('too large');
  const producer = producerFailing(failingSerializer(error, false), false);
  try {
    const record = new ProducerRecord({ topic: 'unit-serde-api', value: 'v' });
    // Awaited: send() resolves once the callback's Promise did.
    let done = false;
    const future = await producer.send({
      record,
      callback: () =>
        new Promise((resolve) => setTimeout(resolve, 20)).then(() => {
          done = true;
        }),
    });
    assert.equal(done, true);
    assert.equal(future.isDone(), true);
    // A throw, as Java's onCompletion throws out of send().
    const thrown = new Error('callback');
    await assert.rejects(
      producer.send({
        record,
        callback: () => {
          throw thrown;
        },
      }),
      (e) => e === thrown,
    );
    // A rejection, as a throw.
    await assert.rejects(producer.send({ record, callback: () => Promise.reject(thrown) }), (e) => e === thrown);
  } finally {
    await producer.close();
  }
});

test("a serializer's TypeError rejects send() with SerializationError, Java's message and the TypeError as its cause", async () => {
  /** A serializer that throws a TypeError, Java's ClassCastException. */
  class WrongSerializer {
    /** @returns {Uint8Array} */
    serialize() {
      throw new TypeError('wrong type');
    }
  }
  /** @type {Array<[unknown, string]>} */
  const data = [
    ['text', 'String'],
    [7, 'Number'],
    [7n, 'BigInt'],
    [Buffer.from('b'), 'Buffer'],
    [new Uint8Array(1), 'Uint8Array'],
    [{}, 'Object'],
    [Object.create(null), 'Object'],
    [new Map(), 'Map'],
    [null, 'null'],
  ];
  for (const { isKey, async } of SERIALIZER_CASES) {
    for (const [given, className] of data) {
      const field = isKey ? 'key' : 'value';
      const cause = new TypeError('wrong type');
      // A class instance names its class; a plain object Object.
      for (const [serializer, serializerName] of /** @type {Array<[Serializer<unknown>, string]>} */ ([
        [async ? { serialize: () => Promise.reject(cause) } : failingSerializer(cause, false), 'Object'],
        [new WrongSerializer(), 'WrongSerializer'],
      ])) {
        if (async && serializerName === 'WrongSerializer') continue;
        const producer = producerFailing(serializer, isKey);
        try {
          let ran = false;
          const record = new ProducerRecord({ topic: 'unit-serde-cast', key: isKey ? given : 'k', value: isKey ? 'v' : given });
          await assert.rejects(
            producer.send({ record, callback: () => (ran = true) }),
            (e) =>
              e instanceof errors.SerializationError &&
              e.message === `Can't convert ${field} of class ${className} to class ${serializerName} specified in ${field}.serializer` &&
              e.cause instanceof TypeError &&
              (serializerName === 'WrongSerializer' || e.cause === cause),
          );
          assert.equal(ran, false);
        } finally {
          await producer.close();
        }
      }
    }
  }
});

test("a serializer's other errors reject send() as they are: a KafkaError that is no ApiError, SerializationError included, and a non-Kafka error", async () => {
  for (const { isKey, async } of SERIALIZER_CASES) {
    for (const error of [
      new errors.SerializationError('bad data'),
      new KafkaError('not an api error'),
      new RangeError('range'),
      'a string',
    ]) {
      const producer = producerFailing(failingSerializer(error, async), isKey);
      try {
        let ran = false;
        await assert.rejects(
          producer.send({ record: new ProducerRecord({ topic: 'unit-serde-other', key: 'k', value: 'v' }), callback: () => (ran = true) }),
          (e) => e === error,
        );
        assert.equal(ran, false);
      } finally {
        await producer.close();
      }
    }
  }
});

test('a producer closed while a serializer Promise is pending rejects that send as closed', async () => {
  let release = /** @type {((value?: unknown) => void) | undefined} */ (undefined);
  /** @type {Serializer<string>} */
  const valueSerializer = {
    serialize: () =>
      new Promise((resolve) => {
        release = () => resolve(Buffer.from('v'));
      }),
  };
  const producer = new KafkaProducer({ configs: UNREACHABLE, keySerializer: null, valueSerializer });
  const sent = producer.send(new ProducerRecord({ topic: 'unit-serde-c', value: 'v' }));
  await producer.close();
  assert.ok(release);
  release();
  await assert.rejects(sent, { name: 'IllegalStateError', message: CLOSED_MESSAGE });
  // The binding keeps no closed state: on a closed producer the serializers
  // still run, and the core reports the closed producer at the hand-over.
  /** @type {SerializeCall[]} */
  const calls = [];
  const closed = new KafkaProducer({
    configs: UNREACHABLE,
    keySerializer: null,
    valueSerializer: recording(() => Buffer.from('v'), calls),
  });
  await closed.close();
  await assert.rejects(closed.send(new ProducerRecord({ topic: 'unit-serde-c', value: 'v' })), {
    name: 'IllegalStateError',
    message: CLOSED_MESSAGE,
  });
  assert.deepEqual(
    calls.map((c) => c.data),
    ['v'],
  );
});

test("a serializer argument is a serializer object or null; a function gets Python's hint", () => {
  /** @param {string} given */
  const forms = (given) => ({ name: 'IllegalArgumentError', message: `${CTOR_FORMS}; got (${given})` });
  const value = { serialize: () => null };
  // @ts-expect-error a key serializer without a value serializer: the form check throws IllegalArgumentError
  assert.throws(() => new KafkaProducer({ configs: UNREACHABLE, keySerializer: value }), forms('configs, keySerializer'));
  // @ts-expect-error a value serializer without a key serializer: the form check throws IllegalArgumentError
  assert.throws(() => new KafkaProducer({ configs: UNREACHABLE, valueSerializer: value }), forms('configs, valueSerializer'));
  // The built-in factory, given where its result goes.
  // @ts-expect-error the factory for a serializer: the serde check throws IllegalArgumentError
  assert.throws(() => new KafkaProducer({ configs: UNREACHABLE, keySerializer: stringSerializer, valueSerializer: null }), {
    name: 'IllegalArgumentError',
    message: "keySerializer: pass a serializer, not a function that makes one — did you forget '()'?",
  });
  for (const bad of [{}, 'string', 1, { serialize: 'x' }]) {
    // @ts-expect-error values that are no serializer: the serde check throws TypeError
    assert.throws(() => new KafkaProducer({ configs: UNREACHABLE, keySerializer: null, valueSerializer: bad }), {
      name: 'TypeError',
      message: 'valueSerializer must be a Serializer: an object with a serialize({ topic, headers, data }) method, or null',
    });
  }
  // @ts-expect-error a close that is no function: the serde check throws TypeError
  assert.throws(() => new KafkaProducer({ configs: UNREACHABLE, keySerializer: { serialize() {}, close: 1 }, valueSerializer: null }), {
    name: 'TypeError',
    message: 'keySerializer.close must be a function when given',
  });
  // @ts-expect-error an undefined key serializer: the undefined check throws TypeError
  assert.throws(() => new KafkaProducer({ configs: UNREACHABLE, keySerializer: undefined, valueSerializer: null }), {
    name: 'TypeError',
    message: "keySerializer must not be undefined. Leave the key out, or pass null for Java's null",
  });
});

test('close() closes both serializers, a failure being a process warning, never a rejection', async () => {
  /** @type {string[]} */
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
    // KafkaProducerTest.testSerializerClose: the serializers stay open while
    // the producer is (Java's CLOSE_COUNT unchanged inside the try).
    assert.deepEqual(closed, []);
    await producer.close();
    // A second close closes nothing again: the core's close, which holds Node
    // until its answer, as any call made on a closed producer does
    // (javascript/CLAUDE.md, "Threads and liveness").
    await producer.close();
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
  assert.deepEqual(
    rejected.map((w) => w.message),
    ['Failed to close producer valueSerializer: Error: later'],
  );
});

test("a producer whose construction fails closes the serializers; a ConfigError of the configuration's check, which Java throws before that catch, does not", async () => {
  /** @type {string[]} */
  const closed = [];
  /** @param {string} name */
  const serializer = (name) => ({ serialize: () => null, close: () => closed.push(name) });
  // Inside Java's constructor catch: relabelled "Failed to construct kafka producer", serializers closed.
  assert.throws(
    () =>
      new KafkaProducer({
        configs: { 'bootstrap.servers': '127.0.0.1:1', 'delivery.timeout.ms': 1 },
        keySerializer: serializer('key'),
        valueSerializer: serializer('value'),
      }),
    (/** @type {KafkaError} */ e) =>
      e.constructor === KafkaError &&
      e.message === 'Failed to construct kafka producer' &&
      /** @type {Error} */ (e.cause).name === 'ConfigError',
  );
  assert.deepEqual(closed, ['key', 'value']);
  // Java's new ProducerConfig(...) validates before that catch: the serializers stay open.
  closed.length = 0;
  assert.throws(
    () =>
      new KafkaProducer({
        configs: { 'bootstrap.servers': '127.0.0.1:1', acks: 'sometimes' },
        keySerializer: serializer('key'),
        valueSerializer: serializer('value'),
      }),
    { name: 'ConfigError', message: 'Invalid value sometimes for configuration acks: String must be one of: all, -1, 0, 1' },
  );
  assert.deepEqual(closed, []);
});

// The serde config keys (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Serialization): a synchronous function ({ configs, isKey }) =>
// serializer, called once with the whole configuration, in place of Java's
// class and its configure(configs, isKey).
test('key.serializer and value.serializer take a function the producer calls once with the configuration', async () => {
  /** @type {Array<{ isKey: boolean, configs: Readonly<Record<string, unknown>> }>} */
  const made = [];
  /** @type {SerializeCall[]} */
  const calls = [];
  /** @param {string} prefix */
  const factory = (prefix) => (/** @type {{ configs: Readonly<Record<string, unknown>>, isKey: boolean }} */ options) => {
    made.push({ isKey: options.isKey, configs: options.configs });
    return recording((data) => (data === null ? null : Buffer.from(`${prefix}${data}`)), calls);
  };
  const configs = { ...UNREACHABLE, 'key.serializer': factory('k:'), 'value.serializer': factory('v:'), 'some.key': 5 };
  const producer = new KafkaProducer(configs);
  try {
    assert.deepEqual(
      made.map((m) => m.isKey),
      [true, false],
    );
    // The whole configuration, each value as it was given, read-only.
    assert.equal(made[0].configs['some.key'], 5);
    assert.equal(made[0].configs['key.serializer'], configs['key.serializer']);
    assert.deepEqual(Object.keys(made[0].configs), Object.keys(configs));
    assert.ok(Object.isFrozen(made[0].configs));
    await producer.send(new ProducerRecord({ topic: 'unit-serde-c', key: 1, value: 2 }));
    assert.deepEqual(
      calls.map((c) => c.data),
      [1, 2],
    );
  } finally {
    await producer.close();
  }
  // Called once: not again at send() or close().
  assert.equal(made.length, 2);
});

test("a serializer argument wins over its config key, which is then ignored; a null one is the config key's", async () => {
  let called = 0;
  const make = () => {
    called++;
    return { serialize: () => null };
  };
  const p1 = new KafkaProducer({
    // @ts-expect-error a string for a serde config key, which its type refuses: the serializer argument wins and the run time ignores the key
    configs: { ...UNREACHABLE, 'key.serializer': make, 'value.serializer': 'not even a function' },
    keySerializer: { serialize: () => null },
    valueSerializer: { serialize: () => null },
  });
  await p1.close();
  assert.equal(called, 0);
  const p2 = new KafkaProducer({ configs: { ...UNREACHABLE, 'key.serializer': make }, keySerializer: null, valueSerializer: null });
  await p2.close();
  assert.equal(called, 1);
});

test('a serde config key that is no synchronous function throws ConfigError, saying what to pass', async () => {
  /** @param {string} argument */
  const pass = (argument) =>
    `pass a synchronous function ({ configs, isKey }) => serializer, such as stringSerializer, or the serializer itself as the ${argument} argument of the constructor`;
  /** A serializer class, which a serde config key refuses. */
  class StringSerializer {
    serialize() {
      return null;
    }
  }
  for (const [key, argument] of [
    ['key.serializer', 'keySerializer'],
    ['value.serializer', 'valueSerializer'],
  ]) {
    for (const [value, message] of [
      [
        'org.apache.kafka.common.serialization.StringSerializer',
        `Invalid value org.apache.kafka.common.serialization.StringSerializer for configuration ${key}: a class name cannot be loaded in JavaScript; ${pass(argument)}`,
      ],
      [
        { serialize: () => null },
        `Invalid value an object for configuration ${key}: a serializer object is not a configuration value; ${pass(argument)}`,
      ],
      [5, `Invalid value a number for configuration ${key}: ${pass(argument)}`],
      [StringSerializer, `Invalid value a function for configuration ${key}: a class is not called; ${pass(argument)}`],
      [
        async () => ({ serialize: () => null }),
        `Invalid value a function for configuration ${key}: the function returned a Promise; it must return the serializer itself, synchronously`,
      ],
      [
        () => 'org.example.Serializer',
        `Invalid value a function for configuration ${key}: the function returned a string, not a serializer: an object with a serialize({ topic, headers, data }) method`,
      ],
    ]) {
      assert.throws(
        () => new KafkaProducer({ ...UNREACHABLE, [key]: value }),
        (/** @type {Error} */ e) => {
          assert.equal(e.constructor.name, 'ConfigError', key);
          assert.equal(e.message, message);
          return true;
        },
      );
    }
    // null and undefined leave the key unset: no serializer.
    for (const value of [null, undefined]) {
      const producer = new KafkaProducer({ ...UNREACHABLE, [key]: value });
      await producer.close();
    }
  }
});

test('a serde config function that throws is a construction failure, and the serializers made so far are closed', async () => {
  /** @type {string[]} */
  const closed = [];
  const cause = new Error('cannot build');
  assert.throws(
    () =>
      new KafkaProducer({
        ...UNREACHABLE,
        'key.serializer': () => ({ serialize: () => null, close: () => closed.push('key') }),
        'value.serializer': () => {
          throw cause;
        },
      }),
    (/** @type {KafkaError} */ e) =>
      e.constructor === KafkaError && e.message === 'Failed to construct kafka producer' && e.cause === cause,
  );
  assert.deepEqual(closed, ['key']);
  // A key function that throws closes nothing: the value serializer given is
  // not taken yet, as Java's constructor takes the key serializer first.
  closed.length = 0;
  assert.throws(
    () =>
      new KafkaProducer({
        configs: {
          ...UNREACHABLE,
          'key.serializer': () => {
            throw cause;
          },
        },
        keySerializer: null,
        valueSerializer: { serialize: () => null, close: () => closed.push('value argument') },
      }),
    (/** @type {KafkaError} */ e) =>
      e.constructor === KafkaError && e.message === 'Failed to construct kafka producer' && e.cause === cause,
  );
  assert.deepEqual(closed, []);
  // A value function that throws closes the key serializer given, which was taken.
  closed.length = 0;
  assert.throws(
    () =>
      new KafkaProducer({
        configs: {
          ...UNREACHABLE,
          'value.serializer': () => {
            throw cause;
          },
        },
        keySerializer: { serialize: () => null, close: () => /** @type {string[]} */ (closed).push('key argument') },
        valueSerializer: null,
      }),
    (/** @type {KafkaError} */ e) =>
      e.constructor === KafkaError && e.message === 'Failed to construct kafka producer' && e.cause === cause,
  );
  assert.deepEqual(closed, ['key argument']);
  // One the core's configuration check refuses afterwards is closed too: only
  // the producer holds it.
  closed.length = 0;
  assert.throws(
    () =>
      new KafkaProducer({
        configs: {
          'bootstrap.servers': '127.0.0.1:1',
          acks: 'sometimes',
          'key.serializer': () => ({ serialize: () => null, close: () => /** @type {string[]} */ (closed).push('key') }),
        },
        keySerializer: null,
        valueSerializer: { serialize: () => null, close: () => /** @type {string[]} */ (closed).push('value argument') },
      }),
    { name: 'ConfigError', message: 'Invalid value sometimes for configuration acks: String must be one of: all, -1, 0, 1' },
  );
  assert.deepEqual(closed, ['key']);
  // close() closes a configured serializer as it closes a given one.
  closed.length = 0;
  const producer = new KafkaProducer({
    ...UNREACHABLE,
    'value.serializer': () => ({ serialize: () => null, close: () => /** @type {string[]} */ (closed).push('value') }),
  });
  await producer.close();
  assert.deepEqual(closed, ['value']);
});

test('stringSerializer goes in a serde config key without (), configured by its encoding keys', async () => {
  assert.throws(
    () => new KafkaProducer({ ...UNREACHABLE, 'key.serializer': stringSerializer, 'key.serializer.encoding': 'UTF-32' }),
    (/** @type {KafkaError & { cause: Error }} */ e) => {
      assert.equal(e.constructor, KafkaError);
      assert.equal(e.message, 'Failed to construct kafka producer');
      assert.equal(e.cause.name, 'SerializationError');
      assert.equal(e.cause.message, 'Unsupported encoding UTF-32');
      return true;
    },
  );
  // The key serializer reads key.serializer.encoding, the value one falls back to serializer.encoding.
  assert.throws(
    () =>
      new KafkaProducer({
        ...UNREACHABLE,
        'key.serializer': stringSerializer,
        'value.serializer': stringSerializer,
        'key.serializer.encoding': 'UTF-8',
        'serializer.encoding': 'windows-1252',
      }),
    (/** @type {Error & { cause: Error }} */ e) => e.cause.message === 'Unsupported encoding windows-1252',
  );
  const producer = new KafkaProducer({ ...UNREACHABLE, 'key.serializer': stringSerializer, 'value.serializer': stringSerializer });
  await producer.close();
});

test('a serde config function that reads the whole configuration leaves out only the keys the core does not know', () => {
  // The function spreads the configuration, reading every key: the producer
  // keys still reach the core (the producer has its bootstrap servers and
  // client id), and only schema.registry.url, which no producer key is, is
  // kept from the core, so the core lists nothing as unused.
  const script = `
    const { KafkaProducer } = require('@confluentinc/kafka-javascript/producer');
    let seen;
    const p = new KafkaProducer({
      'bootstrap.servers': 'localhost:1',
      'client.id': 'spread-client',
      'max.block.ms': 200,
      'schema.registry.url': 'http://sr',
      'value.serializer': ({ configs }) => {
        seen = { ...configs };
        return { serialize: () => null };
      },
    });
    console.log(Object.keys(seen).sort().join(','));
    console.log(p.metrics().keys().next().value.tags().get('client-id'));
    p.close().then(() => console.log('closed'));
  `;
  const r = runNode(script, 120000, [], { RUST_LOG: 'info' });
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'bootstrap.servers,client.id,max.block.ms,schema.registry.url,value.serializer\nspread-client\nclosed\n');
  assert.ok(!r.stderr.includes('were supplied but are not used'), r.stderr);
});

test("the keys a serde config function reads don't reach the core, nor do the serde keys; the consumer's serde keys do", () => {
  // The core lists the keys it does not use in one INFO line, at the RUST_LOG
  // level read when its first client is built, so a fresh process builds it.
  const script = `
    const { KafkaProducer } = require('@confluentinc/kafka-javascript/producer');
    const { stringSerializer } = require('@confluentinc/kafka-javascript/common/serialization');
    new KafkaProducer({
      'bootstrap.servers': 'localhost:1',
      'schema.registry.url': 'http://sr',
      'key.deserializer': 'org.example.Deserializer',
      'key.serializer': ({ configs }) => {
        if (configs['schema.registry.url'] !== 'http://sr') throw new Error('not read');
        return { serialize: () => null };
      },
      // The built-in factory without (), which reads its encoding keys.
      'value.serializer': stringSerializer,
      'value.serializer.encoding': 'UTF-8',
      'serializer.encoding': 'utf8',
    })
      .close()
      .then(() => console.log('closed'));
  `;
  const r = runNode(script, 120000, [], { RUST_LOG: 'info' });
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'closed\n');
  // serializer.encoding is not read: value.serializer.encoding is set, so
  // Java's StringSerializer does not fall back to it.
  assert.ok(
    r.stderr.includes("These configurations '[key.deserializer, serializer.encoding]' were supplied but are not used yet."),
    r.stderr,
  );
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
    // @ts-expect-error a null topic: the core's null check rejects with TypeError
    await assert.rejects(producer.partitionsFor(null), { constructor: TypeError, message: 'topic cannot be null' });
    // @ts-expect-error a number topic: the type check rejects with TypeError
    await assert.rejects(producer.partitionsFor({ topic: 5 }), { constructor: TypeError, message: 'topic must be a string' });
    // @ts-expect-error a bare number: the form check rejects with IllegalArgumentError
    await assert.rejects(producer.partitionsFor(5), {
      name: 'IllegalArgumentError',
      message: 'partitionsFor() takes one of (topic); got a number',
    });
    // @ts-expect-error no argument: the form check rejects with IllegalArgumentError
    await assert.rejects(producer.partitionsFor(), {
      name: 'IllegalArgumentError',
      message: 'partitionsFor() takes one of (topic); got ()',
    });
    // @ts-expect-error a second argument: the form check rejects with IllegalArgumentError
    await assert.rejects(producer.partitionsFor('a', 'b'), {
      name: 'IllegalArgumentError',
      message: 'partitionsFor() takes one of (topic); got 2 arguments, not one object of named parameters',
    });
  } finally {
    await producer.close();
  }
});

// metrics (Java's metrics()): synchronous, a snapshot of the core's metrics.
test("metrics() is a MetricNameMap of KafkaMetric, read when it runs, keyed by Java's MetricName", async () => {
  const { MetricName, MetricNameMap } = require('@confluentinc/kafka-javascript/common');
  const { KafkaMetric } = require('@confluentinc/kafka-javascript/common/metrics');
  const producer = new KafkaProducer({ configs: { ...UNREACHABLE, 'client.id': 'unit-metrics' } });
  try {
    const metrics = producer.metrics();
    assert.ok(metrics instanceof MetricNameMap);
    assert.ok(metrics.size() > 0);
    for (const [name, metric] of metrics) {
      assert.ok(name instanceof MetricName);
      assert.ok(metric instanceof KafkaMetric);
      assert.equal(metric.metricName(), name);
      const value = metric.metricValue();
      assert.ok(['number', 'bigint', 'string'].includes(typeof value), `${name}: ${typeof value}`);
    }
    // Looked up by an equal MetricName, Java's equals: group, name and tags.
    const key = new MetricName({
      name: 'record-send-total',
      group: 'producer-metrics',
      description: '',
      tags: new Map([['client-id', 'unit-metrics']]),
    });
    assert.equal(metrics.has(key), true);
    const total = metrics.get(key);
    assert.ok(total instanceof KafkaMetric);
    assert.equal(total.isMeasurable(), true);
    assert.equal(total.metricValue(), 0);
    // A snapshot: a second call reads the values again.
    await producer.send(new ProducerRecord({ topic: 'unit-metrics', value: Buffer.from('v') }));
    assert.notEqual(producer.metrics(), metrics);
    // @ts-expect-error an argument to a reader: the form check throws TypeError
    assert.throws(() => producer.metrics(1), { constructor: TypeError, message: 'metrics() only reads: it takes no argument' });
  } finally {
    await producer.close();
  }
});

// KafkaProducerTest's configuration and construction tests (Apache Kafka
// 4.4.0). Java's baseProperties() name StringSerializer for both serdes; the
// configuration checks run in the core's ProducerConfig, reached through the
// constructor (Java's new ProducerConfig(...) before its constructor's try),
// so they throw a bare ConfigError, synchronously.

const { ConfigError } = require('@confluentinc/kafka-javascript/common/config');

/**
 * A producer of Java's baseProperties() plus `configs`.
 *
 * @param {Record<string, unknown>} configs
 */
function baseProducer(configs) {
  return new KafkaProducer({
    configs: { 'bootstrap.servers': '127.0.0.1:1', ...configs },
    keySerializer: stringSerializer(),
    valueSerializer: stringSerializer(),
  });
}

/**
 * Asserts that the configuration builds a producer, and closes it. Java's
 * getBoolean / getString read-backs of the parsed values are not translated
 * (ProducerConfig is translated for its constants only).
 *
 * @param {Record<string, unknown>} configs
 */
async function assertBuilds(configs) {
  const producer = baseProducer(configs);
  await producer.close({ timeout: 0 });
}

/**
 * Asserts that the configuration is refused with a ConfigError of `message`.
 *
 * @param {Record<string, unknown>} configs
 * @param {string} message
 */
function assertRefused(configs, message) {
  assert.throws(
    () => baseProducer(configs),
    (e) => {
      assert.ok(e instanceof ConfigError && e instanceof KafkaError, `${e}`);
      assert.equal(e.message, message);
      return true;
    },
  );
}

// KafkaProducerTest.testAcksAndIdempotenceForIdempotentProducers. Java checks
// only the class; the message is ProducerConfig#postProcessAndValidateIdempotenceConfigs'.
// The transactional.id forms are left out with transactions (the preview has
// no transaction surface).
test('the idempotent producer refuses acks other than all when enable.idempotence is set; without it acks builds', async () => {
  await assertBuilds({ acks: '0', 'enable.idempotence': false });
  await assertBuilds({ acks: 'all', 'enable.idempotence': false });
  await assertBuilds({ acks: '0' });
  await assertBuilds({ acks: '1' });
  assertRefused(
    { acks: '1', 'enable.idempotence': true },
    'Must set acks to all in order to use the idempotent producer. Otherwise we cannot guarantee idempotence.',
  );
});

// KafkaProducerTest.testRetriesAndIdempotenceForIdempotentProducers (the
// transactional.id forms left out, as above).
test('the idempotent producer refuses retries 0 when enable.idempotence is set; without it retries 0 builds', async () => {
  await assertBuilds({ retries: 0, 'enable.idempotence': false });
  await assertBuilds({ retries: 0 });
  assertRefused({ retries: 0, 'enable.idempotence': true }, 'Must set retries to non-zero when using the idempotent producer.');
});

// KafkaProducerTest.testInflightRequestsAndIdempotenceForIdempotentProducers
// (Java pins this message with assertEquals; the transactional.id forms left
// out, as above).
test('the idempotent producer refuses more than 5 in-flight requests per connection', async () => {
  const message = 'To use the idempotent producer, max.in.flight.requests.per.connection must be set to at most 5. Current value is 6.';
  await assertBuilds({ 'max.in.flight.requests.per.connection': 6, 'enable.idempotence': false });
  assertRefused({ 'max.in.flight.requests.per.connection': 6 }, message);
  assertRefused({ 'max.in.flight.requests.per.connection': 6, 'enable.idempotence': true }, message);
});

// KafkaProducerTest.testOsDefaultSocketBufferSizes,
// testInvalidSocketSendBufferSize and testInvalidSocketReceiveBufferSize: -1
// is Java's Selectable.USE_DEFAULT_BUFFER_SIZE; below it, ConfigDef.Range's
// "Value must be at least -1" (Java asserts a KafkaException, which a
// ConfigException is).
test('send.buffer.bytes and receive.buffer.bytes take -1, the OS default, and refuse anything below', async () => {
  const producer = new KafkaProducer({
    configs: { 'bootstrap.servers': '127.0.0.1:1', 'send.buffer.bytes': -1, 'receive.buffer.bytes': -1 },
  });
  await producer.close({ timeout: 0 });
  for (const key of ['send.buffer.bytes', 'receive.buffer.bytes']) {
    assert.throws(
      () => new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', [key]: -2 } }),
      (e) => {
        assert.ok(e instanceof ConfigError && e instanceof KafkaError, `${e}`);
        assert.equal(e.message, `Invalid value -2 for configuration ${key}: Value must be at least -1`);
        return true;
      },
    );
  }
});

// KafkaProducerTest.testDeliveryTimeoutAndLingerMsConfig: inside Java's
// constructor try (KafkaProducer#configureDeliveryTimeout), so relabelled
// "Failed to construct kafka producer" with the ConfigError as its cause; the
// boundary, 1000 = 999 + 1, builds.
test('delivery.timeout.ms below linger.ms + request.timeout.ms fails the construction; equal to it builds', async () => {
  const configs = {
    'client.id': 'testDeliveryTimeoutAndLingerMsConfig',
    'delivery.timeout.ms': 1000,
    'linger.ms': 1000,
    'request.timeout.ms': 1,
  };
  assert.throws(
    () => baseProducer(configs),
    (/** @type {KafkaError} */ e) => {
      assert.equal(e.constructor, KafkaError);
      assert.equal(e.message, 'Failed to construct kafka producer');
      assert.ok(e.cause instanceof ConfigError);
      assert.equal(e.cause.message, 'delivery.timeout.ms should be equal to or larger than linger.ms + request.timeout.ms');
      return true;
    },
  );
  await assertBuilds({ ...configs, 'linger.ms': 999 });
});

// KafkaProducerTest.testProducerBootstrapResolutionExceptionPropagated: Java
// loops on partitionsFor for up to 15 s; the core ends the metadata wait when
// the resolution budget runs out, so the first call fails, and so does every
// later one ("This guards against accidentally clearing the bootstrap error
// from the metadata layer.").
test('an unresolvable bootstrap fails partitionsFor with BootstrapResolutionError, and every later call too', async () => {
  const producer = new KafkaProducer({
    configs: { 'bootstrap.servers': 'unresolvable.invalid:9092', 'bootstrap.resolve.timeout.ms': 3000 },
    keySerializer: stringSerializer(),
    valueSerializer: stringSerializer(),
  });
  const message = 'Failed to resolve bootstrap servers after 3000ms. Please check your bootstrap.servers configuration and DNS settings.';
  /** @param {unknown} e */
  const isBootstrapError = (e) => {
    assert.ok(e instanceof errors.BootstrapResolutionError && e instanceof KafkaError, `${e}`);
    assert.equal(e.message, message);
    return true;
  };
  try {
    await assert.rejects(producer.partitionsFor('test-topic'), isBootstrapError);
    await assert.rejects(producer.partitionsFor('test-topic'), isBootstrapError);
    await assert.rejects(producer.send(new ProducerRecord({ topic: 'test-topic', value: 'v' })), isBootstrapError);
  } finally {
    await producer.close();
  }
}, 30000);

// KafkaProducerTest.testProducerConstructorFailsWithConfigExceptionOnUnresolvableBootstrapWhenTimeoutZero:
// the default bootstrap.resolve.timeout.ms, 0, resolves in the constructor,
// where ClientUtils#parseAndValidateAddresses throws inside Java's try.
test('with the default bootstrap.resolve.timeout.ms an unresolvable bootstrap fails the construction with a ConfigError cause', () => {
  /** @type {string[]} */
  const closed = [];
  /** @param {string} name */
  const serializer = (name) => ({ serialize: () => null, close: () => closed.push(name) });
  assert.throws(
    () =>
      new KafkaProducer({
        configs: { 'bootstrap.servers': 'unresolvable.invalid:9092' },
        keySerializer: serializer('key'),
        valueSerializer: serializer('value'),
      }),
    (/** @type {KafkaError} */ e) => {
      assert.equal(e.constructor, KafkaError);
      assert.equal(e.message, 'Failed to construct kafka producer');
      assert.ok(e.cause instanceof ConfigError);
      assert.equal(e.cause.message, 'No resolvable bootstrap urls given in bootstrap.servers');
      return true;
    },
  );
  // Inside Java's catch: the serializers given are closed.
  assert.deepEqual(closed, ['key', 'value']);
});

// KafkaProducerTest.testCallbackAndInterceptorHandleError: Java's MockClient
// only keeps the metadata from arriving, so the send fails on the metadata
// wait, as here. The callback gets Java's nullMetadata
// (RecordMetadata(tp, -1, -1, RecordBatch.NO_TIMESTAMP, -1, -1)). Java's
// interceptor count is not translated (no interceptors).
test("a failed send's callback gets Java's null metadata: the topic, no offset or timestamp, sizes and partition -1", async () => {
  const producer = new KafkaProducer({
    configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 1000 },
    keySerializer: stringSerializer(),
    valueSerializer: stringSerializer(),
  });
  try {
    /** @type {Array<{ metadata: RecordMetadata, exception: Error | null }>} */
    const calls = [];
    // Java's "Invalid topic name due to space": like Java, nothing checks the
    // name before the metadata wait.
    await producer.send({
      record: new ProducerRecord({ topic: 'topic abc', value: 'HelloKafka' }),
      callback: ({ metadata, exception }) => {
        calls.push({ metadata, exception });
      },
    });
    assert.equal(calls.length, 1);
    const [{ metadata, exception }] = calls;
    assert.ok(exception instanceof errors.TimeoutError, `${exception}`);
    assert.equal(exception.message, 'Topic topic abc not present in metadata after 1000 ms.');
    assert.ok(metadata);
    assert.equal(metadata.topic, 'topic abc', 'Topic name should be valid even on send failure');
    assert.equal(metadata.hasOffset, false);
    assert.equal(metadata.offset, -1n);
    assert.equal(metadata.hasTimestamp, false);
    assert.equal(metadata.timestamp, -1n);
    assert.equal(metadata.serializedKeySize, -1);
    assert.equal(metadata.serializedValueSize, -1);
    assert.equal(metadata.partition, -1);
  } finally {
    await producer.close();
  }
});

/** @param {string} name @param {string} clientId */
function producerMetric(name, clientId) {
  const { MetricName } = require('@confluentinc/kafka-javascript/common');
  return new MetricName({ name, group: 'producer-metrics', description: '', tags: new Map([['client-id', clientId]]) });
}

// KafkaProducerTest.testFlushMeasureLatency is ported in the broker suite
// (test/integration/producer.test.js): Java's MockTime(1) moves its clock on
// every read, while here a flush with nothing pending can take less than one
// tick of the real clock, so each flush there waits for a record of its own.

// KafkaProducerMetricsTest.shouldRemoveMetricsOnClose: Java's
// KafkaProducerMetrics#close removes its seven *-time-ns-total metrics (and
// txn-prepare-time-ns-total, which the Java test does not list); the other
// metrics stay, as Java's Metrics.close() keeps the registry. metrics() has no
// closed check in Java, and none here.
test("close() removes the producer's *-time-ns-total metrics, as Java's KafkaProducerMetrics.close() does", async () => {
  const names = [
    'flush-time-ns-total',
    'txn-init-time-ns-total',
    'txn-begin-time-ns-total',
    'txn-commit-time-ns-total',
    'txn-abort-time-ns-total',
    'txn-send-offsets-time-ns-total',
    'metadata-wait-time-ns-total',
    'txn-prepare-time-ns-total',
  ];
  const producer = new KafkaProducer({ configs: { ...UNREACHABLE, 'client.id': 'unit-metrics-close' } });
  const before = producer.metrics();
  for (const name of names) {
    assert.equal(before.has(producerMetric(name, 'unit-metrics-close')), true, name);
    assert.equal(before.get(producerMetric(name, 'unit-metrics-close'))?.metricValue(), 0, name);
  }
  await producer.close();
  const after = producer.metrics();
  for (const name of names) assert.equal(after.has(producerMetric(name, 'unit-metrics-close')), false, name);
  assert.equal(after.has(producerMetric('record-send-total', 'unit-metrics-close')), true);
});

// KafkaProducerMetricsTest.shouldRecordMetadataWaitTime, its failure half
// (the success half needs a broker: test/integration/producer.test.js): a
// send whose metadata wait times out records nothing, as Java's
// KafkaProducer#waitOnMetadata throws inside its loop, before
// recordMetadataWait.
test('a send whose metadata wait times out leaves metadata-wait-time-ns-total at 0', async () => {
  const producer = new KafkaProducer({ configs: { ...UNREACHABLE, 'client.id': 'unit-metadata-wait' } });
  const key = producerMetric('metadata-wait-time-ns-total', 'unit-metadata-wait');
  try {
    const future = await producer.send(new ProducerRecord({ topic: 'unit-mw', value: Buffer.from('v') }));
    await assert.rejects(delivery(future), { name: 'TimeoutError', message: TIMEOUT_MESSAGE('unit-mw') });
    assert.equal(producer.metrics().get(key)?.metricValue(), 0);
  } finally {
    await producer.close();
  }
});

// kafka_Bytes_t's length is an int32_t: a key, a value or a header value of
// 2^31 bytes or more is refused before it crosses, never wrapped to a short
// or negative length that would send other bytes. The 2 GiB array is mapped
// lazily (calloc-backed, never written); a machine that cannot map it skips.
test('a key, a value and a header value of 2^31 bytes are refused with RangeError, with no callback and nothing pending', () => {
  const r = runNode(`
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    const { Header, Headers } = require('@confluentinc/kafka-javascript/common/header');
    let huge;
    try {
      huge = new Uint8Array(2 ** 31);
    } catch (e) {
      console.log('skip', e.message);
      process.exit(0);
    }
    const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 60000 } });
    let callbacks = 0;
    const callback = () => callbacks++;
    (async () => {
      for (const [what, record] of [
        ['key', () => new ProducerRecord({ topic: 't', partition: 0, key: huge, value: null })],
        ['value', () => new ProducerRecord({ topic: 't', partition: 0, key: null, value: huge })],
      ]) {
        await p.send({ record: record(), callback }).then(
          () => console.log(what, 'accepted'),
          (e) => console.log(what, e.constructor.name, e.message),
        );
      }
      try {
        new Header({ key: 'h', value: huge });
        console.log('Header accepted');
      } catch (e) {
        console.log('Header', e.constructor.name, e.message);
      }
      try {
        new Headers().add({ key: 'h', value: huge });
        console.log('Headers.add accepted');
      } catch (e) {
        console.log('Headers.add', e.constructor.name, e.message);
      }
      const started = Date.now();
      await p.close();
      console.log('callbacks', callbacks, 'closed at once', Date.now() - started < 5000);
    })();
  `);
  assert.equal(r.status, 0, r.stderr);
  if (r.stdout.startsWith('skip')) return;
  const refused = 'RangeError a byte array crossing the C FFI is limited to 2^31 - 1 bytes';
  assert.equal(r.stdout, `key ${refused}\nvalue ${refused}\nHeader ${refused}\nHeaders.add ${refused}\ncallbacks 0 closed at once true\n`);
}, 60000);

// One table over the completion paths a producer without a broker reaches:
// how many times the callback runs, whether send() resolves or rejects, the
// future's state and the error's class, message and cause. Java's contract
// (KafkaProducer#doSend): an ApiException reaches the callback once and fails
// the future while send() returns; any other throw is rethrown, with no
// callback. A record taken by the core runs its callback exactly once.
test('the completion matrix: each path runs the callback at most once and fails the future or send() as Java does', async () => {
  const { SerializationError, TimeoutError } = errors;
  /** @type {Serializer<string>} */
  const plain = { serialize: ({ data }) => (data === null ? null : Buffer.from(data)) };
  /**
   * @typedef {{
   *   name: string,
   *   configs?: Record<string, unknown>,
   *   serializer?: Serializer<string>,
   *   before?: (p: KafkaProducer<string, string>) => Promise<void>,
   *   callbacks: number,
   *   send: 'resolves' | 'rejects',
   *   error: Function,
   *   message: string | RegExp,
   *   cause?: Function,
   * }} Path
   */
  /** @type {Path[]} */
  const paths = [
    { name: 'metadata timeout', callbacks: 1, send: 'resolves', error: TimeoutError, message: TIMEOUT_MESSAGE('matrix-metadata timeout') },
    {
      name: 'refused at hand-over, closed producer',
      before: (p) => p.close(),
      callbacks: 0,
      send: 'rejects',
      error: IllegalStateError,
      message: CLOSED_MESSAGE,
    },
    {
      name: 'serializer TypeError',
      serializer: {
        serialize: () => {
          throw new TypeError('not a string');
        },
      },
      callbacks: 0,
      send: 'rejects',
      error: SerializationError,
      message: /^Can't convert value of class /,
      cause: TypeError,
    },
    {
      name: 'serializer rejection',
      serializer: { serialize: () => Promise.reject(new RangeError('rejected')) },
      callbacks: 0,
      send: 'rejects',
      error: RangeError,
      message: 'rejected',
    },
    {
      name: 'serializer ApiError',
      serializer: {
        serialize: () => {
          throw new errors.InvalidTopicError('invalid');
        },
      },
      callbacks: 1,
      send: 'resolves',
      error: errors.InvalidTopicError,
      message: 'invalid',
    },
  ];
  for (const path of paths) {
    const producer = new KafkaProducer({
      configs: { ...UNREACHABLE, ...path.configs },
      keySerializer: plain,
      valueSerializer: path.serializer ?? plain,
    });
    try {
      if (path.before) await path.before(producer);
      /** @type {Array<Error | null>} */
      const calls = [];
      const sending = producer.send({
        record: new ProducerRecord({ topic: `matrix-${path.name}`, partition: 0, key: 'k', value: 'v' }),
        callback: ({ exception }) => void calls.push(exception),
      });
      /** @type {Error | undefined} */
      let failure;
      if (path.send === 'rejects') {
        await assert.rejects(sending, (/** @type {Error} */ e) => {
          failure = e;
          return true;
        });
      } else {
        const future = await sending;
        await assert.rejects(future.get(), (/** @type {ExecutionError} */ e) => {
          assert.equal(e.constructor, ExecutionError, path.name);
          failure = /** @type {Error} */ (e.cause);
          return true;
        });
        assert.equal(future.isDone(), true, path.name);
        assert.equal(future.isCompletedExceptionally(), true, path.name);
        assert.equal(calls[0], failure, path.name);
      }
      // A late second callback would show here.
      await new Promise((resolve) => setTimeout(resolve, 50));
      assert.equal(calls.length, path.callbacks, `${path.name}: the callback ran ${calls.length} times`);
      assert.ok(failure, path.name);
      assert.equal(failure.constructor, path.error, `${path.name}: ${failure}`);
      if (typeof path.message === 'string') assert.equal(failure.message, path.message, path.name);
      else assert.match(failure.message, path.message, path.name);
      if (path.cause) assert.equal(/** @type {Error} */ (failure.cause).constructor, path.cause, path.name);
    } finally {
      await producer.close();
    }
  }
});

// The core logs a callback's throw or rejection once per record, with its own
// line (the producer logs a send callback's error, as Java's
// ProducerBatch#completeFutureAndFireCallbacks logs "Error executing
// user-provided callback on message for topic-partition"), and the record's
// outcome does not change.
test("a throwing and a rejecting callback are each logged once by the core, with Java's text, and change no outcome", () => {
  const r = runNode(
    `
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 300 } });
    (async () => {
      const thrown = await p.send({ record: new ProducerRecord({ topic: 'logged-throw', partition: 0, key: null, value: Buffer.from('v') }), callback: () => { throw new Error('boom-sync'); } });
      const rejected = await p.send({ record: new ProducerRecord({ topic: 'logged-reject', partition: 0, key: null, value: Buffer.from('v') }), callback: async () => { throw new Error('boom-async'); } });
      for (const f of [thrown, rejected]) await f.get().catch((e) => console.log(e.cause.constructor.name, e.cause.message));
      await p.close();
    })();
  `,
    60000,
    ['--no-warnings'],
    { RUST_LOG: 'warn' },
  );
  assert.equal(r.status, 0, r.stderr);
  assert.equal(
    r.stdout,
    'TimeoutError Topic logged-throw not present in metadata after 300 ms.\nTimeoutError Topic logged-reject not present in metadata after 300 ms.\n',
  );
  /** @param {string} tp @param {string} text */
  const lines = (tp, text) =>
    r.stderr
      .split('\n')
      .filter((l) => l.includes(`Error executing user-provided callback on message for topic-partition '${tp}'`) && l.includes(text));
  assert.equal(lines('logged-throw-0', 'boom-sync').length, 1, r.stderr);
  assert.equal(lines('logged-reject-0', 'boom-async').length, 1, r.stderr);
});

test('partitioner.type takes the three built-in partitioners; any other value is a ConfigError with the core text', async () => {
  const { ConfigError } = require('@confluentinc/kafka-javascript/common/config');
  for (const type of ['ConsistentRandomPartitioner', 'Murmur2RandomPartitioner', 'RoundRobinPartitioner']) {
    await new KafkaProducer({ configs: { ...UNREACHABLE, 'partitioner.type': type } }).close();
  }
  for (const type of ['Bogus', 'murmur2', 'consistent_random', '']) {
    assert.throws(() => new KafkaProducer({ configs: { ...UNREACHABLE, 'partitioner.type': type } }), {
      constructor: ConfigError,
      message: `Invalid value ${type} for configuration partitioner.type: Partitioner type ${type} is not supported; the supported types are ConsistentRandomPartitioner, Murmur2RandomPartitioner and RoundRobinPartitioner.`,
    });
  }
});

// MIGRATION.md: librdkafka's keys and a 1.x kafkaJS block reach the core as
// unknown keys, listed in its one INFO line, and change nothing.
test("librdkafka and 1.x keys are inert: listed as unused, and linger.ms keeps Java's default", () => {
  const r = runNode(
    `
    const { KafkaProducer } = require('@confluentinc/kafka-javascript/producer');
    new KafkaProducer({ configs: { 'bootstrap.servers': 'localhost:1', 'queue.buffering.max.ms': 1000, 'message.timeout.ms': 5000, dr_cb: true, kafkaJS: { acks: 1 } } })
      .close()
      .then(() => console.log('closed'));
  `,
    60000,
    ['--no-warnings'],
    { RUST_LOG: 'info' },
  );
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'closed\n');
  const unused = r.stderr.split('\n').filter((l) => l.includes('were supplied but are not used yet.'));
  assert.equal(unused.length, 1, r.stderr);
  for (const key of ['queue.buffering.max.ms', 'message.timeout.ms', 'dr_cb', 'kafkaJS']) assert.ok(unused[0].includes(key), unused[0]);
  assert.match(r.stderr, /^\s*linger\.ms = 5$/m);
});

// `await using` (Node 24 and later): the scope's end closes the producer, and
// a close that throws while the block throws gives a SuppressedError, as the
// README says.
(Number(process.versions.node.split('.')[0]) >= 24 ? test : test.skip)(
  'await using closes the producer at the end of its scope; a failing close beside a throwing block is a SuppressedError',
  () => {
    const r = runNode(`
    const { MockProducer } = require('@confluentinc/kafka-javascript/producer');
    (async () => {
      let kept;
      {
        await using p = new MockProducer();
        kept = p;
      }
      console.log('closed', kept.closed());
      const e1 = new Error('close failed');
      const e2 = new Error('block failed');
      let second;
      try {
        await using p = new MockProducer();
        second = p;
        p.setCloseException(e1);
        throw e2;
      } catch (e) {
        console.log(e.constructor.name, e.error === e1, e.suppressed === e2);
      }
      // The failed close left the mock open, as Java's MockProducer#close
      // throws before it closes, and an open mock holds Node: a close without
      // the error closes it, so the process ends.
      console.log('open after the failed close', !second.closed());
      second.setCloseException(null);
      await second.close();
    })();
  `);
    assert.equal(r.status, 0, r.stderr);
    assert.equal(r.stdout, 'closed true\nSuppressedError true true\nopen after the failed close true\n');
  },
);

// The security keys without a broker: the SSL store errors the core reports
// when the producer is built, and the secrets that never reach a message, a
// warning or a log line. The binding checks no value (javascript/CLAUDE.md,
// JavaScript Binding Conventions, Configuration): every text here is the
// core's.
{
  const crypto = require('node:crypto');
  const fs = require('node:fs');
  const os = require('node:os');

  /** @typedef {import('@confluentinc/kafka-javascript/producer').ProducerConfigs} ProducerConfigs */

  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'js-ssl-config-'));
  afterAll(() => fs.rmSync(dir, { recursive: true, force: true }));

  /**
   * Asserts that building an SSL producer with `configs` throws Java's
   * KafkaProducer#KafkaProducer wrapper, "Failed to construct kafka producer",
   * whose cause is the core's ConfigError with `message`.
   *
   * @param {ProducerConfigs} configs
   * @param {string} message
   */
  function assertConstructionFails(configs, message) {
    assert.throws(
      () => new KafkaProducer({ configs: { 'bootstrap.servers': 'localhost:1', 'security.protocol': 'SSL', ...configs } }),
      (e) => {
        assert.equal(/** @type {Error} */ (e).constructor, KafkaError);
        assert.equal(/** @type {Error} */ (e).message, 'Failed to construct kafka producer');
        const cause = /** @type {Error} */ (e).cause;
        assert.ok(cause instanceof ConfigError, String(cause));
        assert.equal(cause.message, message);
        return true;
      },
    );
  }

  // Java's SslChannelBuilder#configure builds the SslFactory, which loads the
  // stores, while KafkaProducer#KafkaProducer builds its channel builder, so a
  // store that cannot be loaded fails the construction, never the first
  // connection; KafkaProducer#KafkaProducer wraps the failure as
  // KafkaException("Failed to construct kafka producer", t).
  test('a missing truststore file fails the construction with the core ConfigError', () => {
    const missing = path.join(dir, 'missing-truststore.pem');
    assertConstructionFails(
      { 'ssl.truststore.location': missing },
      `Failed to create channel builder: Failed to open truststore file '${missing}': No such file or directory (os error 2)`,
    );
  });

  test('a truststore file that is not PEM fails the construction with the core ConfigError', () => {
    const notPem = path.join(dir, 'not-pem-truststore.txt');
    fs.writeFileSync(notPem, 'this is not a certificate\n');
    assertConstructionFails(
      { 'ssl.truststore.location': notPem },
      `Failed to create channel builder: No certificates found in truststore file '${notPem}'`,
    );
  });

  test('an encrypted private key in the keystore file fails the construction with the core ConfigError', () => {
    // A real encrypted PKCS#8 key, which Java's
    // DefaultSslEngineFactory.PemStore#privateKey decrypts with ssl.key.password;
    // the core does not decrypt it and reports it instead.
    const { privateKey } = crypto.generateKeyPairSync('ec', {
      namedCurve: 'prime256v1',
      privateKeyEncoding: { type: 'pkcs8', format: 'pem', cipher: 'aes-128-cbc', passphrase: 'key-secret' },
      publicKeyEncoding: { type: 'spki', format: 'pem' },
    });
    assert.ok(privateKey.startsWith('-----BEGIN ENCRYPTED PRIVATE KEY-----'));
    const keystore = path.join(dir, 'encrypted-keystore.pem');
    fs.writeFileSync(keystore, privateKey);
    assertConstructionFails(
      { 'ssl.keystore.location': keystore, 'ssl.keystore.type': 'PEM', 'ssl.key.password': 'key-secret' },
      'Failed to create channel builder: Encrypted PEM private keys are not supported by this client; provide an unencrypted PKCS#8 key',
    );
  });

  test('a JKS truststore type fails the construction with the core ConfigError', () => {
    assertConstructionFails(
      { 'ssl.truststore.type': 'JKS' },
      'Failed to create channel builder: JKS format is not supported for truststore in Rust. ' +
        'Convert to PEM format using: keytool -exportcert -alias <alias> -keystore <keystore.jks> -rfc',
    );
  });

  // The two secrets: a store password given as a number, which the binding
  // writes as its decimal text, and the password in a SASL/PLAIN JAAS line.
  const NUMBER_PASSWORD = 987654321987;
  const JAAS_PASSWORD = 'jaas-secret-7Q9xK';

  test('a password never appears in an error message, a warning, a NODE_DEBUG line or a RUST_LOG=debug line', () => {
    // Java's AbstractConfig#logAll prints a PASSWORD as [hidden], and
    // ConfigDef.convertToString keeps a Password's value out of every text
    // (org.apache.kafka.common.config.types.Password#toString). The child builds
    // producers whose construction succeeds and producers whose construction
    // fails, sends a record that times out on its metadata, and prints every
    // error it catches, with its causes, and every warning.
    const script = `
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    process.on('warning', (w) => console.log('warning', w.name, w.message));
    const module = 'org.apache.kafka.common.security.plain.PlainLoginModule';
    const jaas = [
      module + ' required username="user" password="${JAAS_PASSWORD}";',
      module + ' required username="user" password="${JAAS_PASSWORD};',
      'password="${JAAS_PASSWORD}";',
    ];
    const print = (where, e) => {
      for (let x = e; x; x = x.cause) console.log(where, x.name, x.message, String(x), x.stack);
    };
    (async () => {
      for (const line of jaas) {
        for (const protocol of ['SASL_PLAINTEXT', 'SASL_SSL']) {
          let producer;
          try {
            producer = new KafkaProducer({
              configs: {
                'bootstrap.servers': 'localhost:1',
                'security.protocol': protocol,
                'sasl.mechanism': 'PLAIN',
                'sasl.jaas.config': line,
                'ssl.keystore.password': ${NUMBER_PASSWORD},
                'ssl.truststore.password': ${NUMBER_PASSWORD},
                'max.block.ms': 500,
              },
            });
          } catch (e) {
            print('construction', e);
            continue;
          }
          try {
            await (await producer.send(new ProducerRecord({ topic: 't', value: Buffer.from('v') }))).get();
          } catch (e) {
            print('send', e);
          }
          await producer.close();
        }
      }
      console.log('done');
    })();
  `;
    const r = spawnSync(process.execPath, ['-e', script], {
      cwd: path.join(__dirname, '..', '..', '..'),
      encoding: 'utf8',
      timeout: 120000,
      env: { ...process.env, ...POISON, NODE_DEBUG: 'confluent_kafka', RUST_LOG: 'debug' },
    });
    assert.equal(r.status, 0, r.stderr);
    const output = r.stdout + r.stderr;
    // Each channel was reached: the errors and the logs the secrets could be in.
    assert.ok(r.stdout.endsWith('done\n'), r.stdout);
    assert.ok(
      r.stdout.includes('construction ConfigError Failed to create channel builder: SASL PLAIN authentication requires a password'),
      r.stdout,
    );
    assert.ok(r.stdout.includes('send TimeoutError Topic t not present in metadata after 500 ms.'), r.stdout);
    assert.ok(r.stderr.includes('CONFLUENT_KAFKA'), 'no NODE_DEBUG=confluent_kafka line');
    assert.ok(/ DEBUG /.test(r.stderr), 'no RUST_LOG=debug line');
    assert.ok(r.stderr.includes('sasl.jaas.config = [hidden]'), 'no ProducerConfig values dump');
    assert.ok(!output.includes(JAAS_PASSWORD), 'the JAAS password reached the output');
    assert.ok(!output.includes(String(NUMBER_PASSWORD)), 'the store password reached the output');
  });
}
