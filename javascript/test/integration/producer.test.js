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

// Broker-backed producer test: runs only when KAFKA_BOOTSTRAP_SERVERS is set
// (`make -C javascript test-integration` starts a scratch broker for it).

const test = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

const { ExecutionError, TimeoutError: RootTimeoutError } = require('@confluentinc/kafka-javascript');
const { KafkaFuture } = require('@confluentinc/kafka-javascript/common');
const { TimeoutError } = require('@confluentinc/kafka-javascript/common/errors');
const { Header, Headers } = require('@confluentinc/kafka-javascript/common/header');
const { KafkaProducer, ProducerRecord, RecordMetadata } = require('@confluentinc/kafka-javascript/producer');
const { fetchRecords } = require('./_fetch_records');

const BOOTSTRAP = process.env.KAFKA_BOOTSTRAP_SERVERS;
const SKIP = BOOTSTRAP ? false : 'KAFKA_BOOTSTRAP_SERVERS is not set: no broker to produce to';
if (SKIP) console.log(`skipping the integration tests: ${SKIP}`);

// A topic no earlier test produced to: its metadata is fetched on the first
// send, so the records queue for it in the core.
let topicCount = 0;
function freshTopic(what) {
  return `js-it-${what}-${process.pid}-${Date.now()}-${topicCount++}`;
}

const N = 300;

test('produces one record with a key, a value and headers', { skip: SKIP, timeout: 120000 }, async () => {
  const topic = freshTopic('one');
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, acks: 'all' } });
  try {
    const before = Date.now();
    let callbacks = 0;
    let callbackMetadata;
    const future = await producer.send({
      record: new ProducerRecord({
        topic,
        partition: null,
        key: 'k1',
        value: Buffer.from('hello-from-node'),
        headers: [new Header({ key: 'h1', value: Buffer.from('v1') }), new Header({ key: 'h2', value: null })],
      }),
      callback: ({ metadata: m, exception: error }) => {
        callbacks++;
        callbackMetadata = m;
        assert.equal(error, null);
      },
    });
    assert.ok(future instanceof KafkaFuture);
    const metadata = await future.get();
    assert.equal(callbacks, 1);
    assert.equal(future.isDone(), true);
    assert.equal(future.isCompletedExceptionally(), false);
    assert.ok(metadata instanceof RecordMetadata);
    assert.equal(callbackMetadata.toString(), metadata.toString());
    assert.equal(metadata.topic, topic);
    assert.equal(metadata.partition >= 0, true);
    assert.equal(metadata.hasOffset, true);
    assert.ok(metadata.offset >= 0, `offset ${metadata.offset}`);
    assert.equal(metadata.hasTimestamp, true);
    assert.ok(metadata.timestamp >= before - 60000, `timestamp ${metadata.timestamp}`);
    assert.equal(metadata.serializedKeySize, 2);
    assert.equal(metadata.serializedValueSize, 15);
    // A data object: a spread and a JSON copy keep every field, Java's longs
    // as bigints, and as decimal strings in JSON.
    const fields = {
      topic,
      partition: metadata.partition,
      offset: metadata.offset,
      hasOffset: true,
      timestamp: metadata.timestamp,
      hasTimestamp: true,
      serializedKeySize: 2,
      serializedValueSize: 15,
    };
    assert.equal(typeof metadata.offset, 'bigint');
    assert.equal(typeof metadata.timestamp, 'bigint');
    assert.deepEqual(JSON.parse(JSON.stringify(metadata)), { ...fields, offset: String(metadata.offset), timestamp: String(metadata.timestamp) });
    assert.deepEqual({ ...metadata }, fields);
    // get() again returns the same outcome, timed or not, and so do the
    // future's other readers and dependants.
    assert.equal((await future.get()).toString(), metadata.toString());
    assert.equal(await future.get(1000), metadata);
    assert.equal(future.getNow(null), metadata);
    assert.equal(await future.toCompletionStage(), metadata);
    assert.equal(future.thenApply((m) => m.offset).getNow(-1), metadata.offset);
    assert.equal(await KafkaFuture.allOf([future]).get(), null);
    // A record's future is never cancelled.
    assert.equal(future.cancel(true), false);
    assert.equal(future.isCancelled(), false);

    // A second record lands after the first on the same partition.
    const next = await (
      await producer.send({ record: new ProducerRecord({ topic, partition: metadata.partition, key: null, value: 'second' }) })
    ).get();
    assert.equal(next.partition, metadata.partition);
    assert.equal(next.offset, metadata.offset + 1n);
    assert.equal(next.serializedKeySize, -1);
    await producer.flush();

    const { records } = await fetchRecords(BOOTSTRAP, topic, metadata.partition);
    assert.deepEqual(
      records.map((r) => [r.offset, r.key && r.key.toString(), r.value.toString(), r.headers.map((h) => [h.key, h.value && h.value.toString()])]),
      [
        [Number(metadata.offset), 'k1', 'hello-from-node', [['h1', 'v1'], ['h2', null]]],
        [Number(next.offset), null, 'second', []],
      ],
    );
  } finally {
    await producer.close();
  }
});

test("a record's dependants complete inside the delivery, whatever their functions throw or cancel", { skip: SKIP, timeout: 120000 }, async () => {
  const topic = freshTopic('dependants');
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, acks: 'all' } });
  try {
    const future = await producer.send(new ProducerRecord({ topic, value: 'v' }));
    // A thrown value whose class cannot be read fails its dependant, and
    // stops neither the record's other dependants nor its get().
    const revoked = Proxy.revocable({}, {});
    revoked.revoke();
    const failing = future.thenApply(() => {
      throw revoked.proxy;
    });
    const failed = failing.get(10000);
    const sibling = future.thenApply((m) => m.topic);
    // A cancel() inside a dependant's action finishes the cancelled future's
    // own dependants, and an allOf over it, before it returns.
    const other = KafkaFuture.internalCreate(false);
    const otherDep = other.whenComplete(() => {});
    const otherAll = KafkaFuture.allOf([other]);
    const seen = [];
    future.whenComplete(() => {
      seen.push(other.cancel(true), otherDep.isDone(), otherDep.isCancelled(), otherAll.isDone(), otherAll.isCancelled());
    });
    const metadata = await future.get(10000);
    assert.equal(metadata.topic, topic);
    assert.equal(sibling.getNow(null), topic);
    await assert.rejects(failed, (e) => e.constructor === ExecutionError && e.cause === revoked.proxy);
    assert.equal(failing.isCompletedExceptionally(), true);
    assert.deepEqual(seen, [true, true, true, true, true]);
  } finally {
    await producer.close();
  }
});

test("a record's future waits through the FFI's get_cb and get_with_timeout_cb, completing after its callback", { skip: SKIP, timeout: 120000 }, async () => {
  const topic = freshTopic('native-future');
  // A long linger keeps the record undelivered while its future is read.
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 3000 } });
  try {
    // The topic's metadata first, so the next record is taken at once.
    await (await producer.send(new ProducerRecord({ topic, value: 'warm-up' }))).get();
    const order = [];
    let future = null;
    future = await producer.send({
      record: new ProducerRecord({ topic, value: 'v' }),
      callback: () => {
        order.push(`callback done=${future.isDone()}`);
      },
    });
    future.whenComplete(() => order.push('dependant'));
    assert.equal(future.isDone(), false);
    assert.equal(future.getNow('absent'), 'absent');
    // The FFI keeps the time of a timed wait: no JavaScript timer runs.
    const setTimeoutBefore = globalThis.setTimeout;
    let timers = 0;
    globalThis.setTimeout = (...args) => {
      timers += 1;
      return setTimeoutBefore(...args);
    };
    try {
      // The root TimeoutError (Java's java.util.concurrent.TimeoutException),
      // with the core's message; a negative timeout does not wait.
      await assert.rejects(future.get({ timeout: 100 }), (e) => e.constructor === RootTimeoutError && e.message === 'Timed out waiting for KafkaFuture after 100 ms');
      await assert.rejects(future.get(-1), (e) => e.constructor === RootTimeoutError && e.message === 'Timed out waiting for KafkaFuture after 0 ms');
    } finally {
      globalThis.setTimeout = setTimeoutBefore;
    }
    assert.equal(timers, 0);
    assert.equal(future.isDone(), false);
    const metadata = await future.get();
    assert.equal(metadata.topic, topic);
    // The callback ran before the future completed, then the dependant.
    assert.deepEqual(order, ['callback done=false', 'dependant']);
    assert.equal(future.isDone(), true);
    assert.equal(future.getNow(null), metadata);
    // A done future answers a timed get at once, with the same metadata.
    assert.equal(await future.get({ timeout: 0 }), metadata);
  } finally {
    await producer.close();
  }
});

// A record's native future across close, collection and destroy
// (javascript/CLAUDE.md, rule 5): read each way (never; get() once and twice;
// a timed get that times out, then get(); a dependant; toCompletionStage()),
// each producer closed and collected. What it shows: every producer and JS
// future is collected (no wait keeps its JS future, nor a future its
// producer) and the producers' threads are gone (each closed producer was
// destroyed), and a future kept past its producer's destroy still answers
// from its delivery. Then 200 records sent one after another on one producer
// use at most 2 slots of its table of record futures: each slot is given back
// once JS recorded the delivery, which is when the context destroys the
// native future (a context that never gives its slot back would take a new
// one per record).
//
// Not covered: a native future never destroyed, a wait that never releases
// its unit of the producer struct, or its reference on the send's context.
// None has an effect JavaScript can see (a future and a context hold no
// thread, and a unit only keeps a small struct), and the RSS they add, a few
// hundred bytes per record, is within the noise of a bound that would not
// flake.
test("a record's native future is released across close, collection and destroy, whatever read it", { skip: SKIP || !['linux', 'darwin'].includes(process.platform), timeout: 180000 }, () => {
  const topic = freshTopic('destroy-futures');
  const script = `
    const { execSync } = require('node:child_process');
    const fs = require('node:fs');
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    const { KafkaFuture } = require('@confluentinc/kafka-javascript/common');
    const threads = () =>
      process.platform === 'linux'
        ? fs.readdirSync('/proc/self/task').length
        : execSync('ps -M -p ' + process.pid).toString().trim().split('\\n').length - 1;
    const collected = [];
    const registry = new FinalizationRegistry((name) => collected.push(name));
    const count = (name) => collected.filter((c) => c === name).length;
    // The slots the hand-overs give the records' futures.
    const futureClass = Object.getPrototypeOf(KafkaFuture.completedFuture(null));
    const setNative = futureClass.internalSetNative;
    const slots = new Set();
    futureClass.internalSetNative = function (producer, slot) {
      slots.add(slot);
      return setNative.call(this, producer, slot);
    };
    const settle = async () => {
      for (let i = 0; i < 10; i++) {
        global.gc();
        await new Promise((r) => setTimeout(r, 100));
      }
    };
    const kept = [];
    // In a function of its own, so no frame keeps the producer.
    async function one(round) {
      const p = new KafkaProducer({ configs: { 'bootstrap.servers': process.env.BOOTSTRAP, 'linger.ms': 500 } });
      registry.register(p, 'producer');
      await (await p.send(new ProducerRecord({ topic: process.env.TOPIC, value: 'w' }))).get();
      const futures = [];
      for (let i = 0; i < 6; i++) futures.push(await p.send(new ProducerRecord({ topic: process.env.TOPIC, value: 'v' + i })));
      for (const f of futures) registry.register(f, 'future');
      const waits = [futures[1].get(), futures[2].get(), futures[2].get()];
      const timedOut = await futures[3].get({ timeout: 20 }).then(() => 'resolved', (e) => e.constructor.name + ': ' + e.message);
      waits.push(futures[3].get(), futures[4].whenComplete(() => {}).get(), futures[5].toCompletionStage());
      const results = await Promise.all(waits);
      await p.close();
      if (round === 0) kept.push(futures[0]);
      return timedOut + ' ' + results.length;
    }
    (async () => {
      // A closed producer no longer holds the loop: the test does.
      const hold = setInterval(() => {}, 1000);
      console.log(await one(0));
      await settle();
      const before = threads();
      for (let r = 1; r <= 5; r++) await one(r);
      await settle();
      console.log('producers', count('producer'), 'futures', count('future'), 'threads', threads() - before < 10);
      // In a function of its own, so no frame keeps the future after.
      await (async () => {
        const f = kept.pop();
        console.log('kept', f.isDone(), f.getNow(null).topic === process.env.TOPIC, (await f.get()).topic === process.env.TOPIC, (await f.get({ timeout: 0 })).topic === process.env.TOPIC);
      })();
      await settle();
      console.log('futures', count('future'));
      // Sequential records, each awaited, on one producer: a slot given back
      // at each delivery is taken again by a later record.
      const p = new KafkaProducer({ configs: { 'bootstrap.servers': process.env.BOOTSTRAP } });
      await (await p.send(new ProducerRecord({ topic: process.env.TOPIC, value: 'w' }))).get();
      slots.clear();
      for (let i = 0; i < 200; i++) await (await p.send(new ProducerRecord({ topic: process.env.TOPIC, value: 's' + i }))).get();
      console.log('slots', slots.size <= 2 ? 'at most 2' : slots.size);
      await p.close();
      clearInterval(hold);
    })();
  `;
  const r = spawnSync(process.execPath, ['--expose-gc', '--no-warnings', '-e', script], {
    cwd: path.join(__dirname, '..', '..'),
    encoding: 'utf8',
    timeout: 170000,
    env: { ...process.env, BOOTSTRAP, TOPIC: topic },
  });
  assert.equal(r.status, 0, r.stderr);
  assert.equal(
    r.stdout,
    'TimeoutError: Timed out waiting for KafkaFuture after 20 ms 6\n' +
      // Six producers, 35 futures: all but the one kept.
      'producers 6 futures 35 threads true\n' +
      'kept true true true true\n' +
      'futures 36\n' +
      'slots at most 2\n',
  );
});

test('NUL bytes in a key, a value and a header value cross byte-exact', { skip: SKIP, timeout: 120000 }, async () => {
  // Only the strings the C ABI reads as NUL-terminated (topic, header key,
  // config) refuse a NUL; bytes cross with their length.
  const topic = freshTopic('nul');
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, acks: 'all' } });
  try {
    const sent = [
      { key: 'k\0ey', value: 'v\0al\0', header: Buffer.from('h\0v') },
      { key: Buffer.from([0, 1, 0]), value: new Uint8Array([0, 0, 7, 0]), header: Buffer.from([0]) },
    ];
    let partition;
    for (const { key, value, header } of sent) {
      const metadata = await (
        await producer.send({
          record: new ProducerRecord({ topic, partition: 0, key, value, headers: [new Header({ key: 'h', value: header })] }),
        })
      ).get();
      partition = metadata.partition;
    }
    const { records } = await fetchRecords(BOOTSTRAP, topic, partition);
    const bytes = (d) => (typeof d === 'string' ? Buffer.from(d, 'utf8') : Buffer.from(d));
    assert.deepEqual(
      records.map((r) => [r.key, r.value, r.headers]),
      sent.map(({ key, value, header }) => [bytes(key), bytes(value), [{ key: 'h', value: bytes(header), keyBytes: Buffer.from('h') }]]),
    );
  } finally {
    await producer.close();
  }
});

test('the serializers bytes, a header one adds and the sizes reach the broker', { skip: SKIP, timeout: 120000 }, async () => {
  const topic = freshTopic('serde');
  const keySerializer = { serialize: ({ data }) => (data === null ? null : Buffer.from(`key-${data}`)) };
  const valueSerializer = {
    serialize: async ({ headers, data }) => {
      headers.add({ key: 'type', value: Buffer.from('json') });
      return Buffer.from(JSON.stringify(data));
    },
  };
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, acks: 'all' }, keySerializer, valueSerializer });
  try {
    const metadata = await (await producer.send(new ProducerRecord({ topic, partition: 0, key: 7, value: { a: 1 } }))).get();
    assert.equal(metadata.serializedKeySize, 'key-7'.length);
    assert.equal(metadata.serializedValueSize, '{"a":1}'.length);
    const { records } = await fetchRecords(BOOTSTRAP, topic, metadata.partition);
    assert.deepEqual(
      records.map((r) => [r.key, r.value, r.headers]),
      [[Buffer.from('key-7'), Buffer.from('{"a":1}'), [{ key: 'type', value: Buffer.from('json'), keyBytes: Buffer.from('type') }]]],
    );
  } finally {
    await producer.close();
  }
});

test('CreateTime: the timestamp given, or the send time, reaches the broker', { skip: SKIP, timeout: 120000 }, async () => {
  const topic = freshTopic('create-time');
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, acks: 'all' } });
  try {
    // The two C record paths: without headers, and through the options
    // builder with them.
    const explicit = 1700000000123;
    const withoutHeaders = await (
      await producer.send({ record: new ProducerRecord({ topic, partition: 0, timestamp: explicit, key: null, value: 'a' }) })
    ).get();
    const withHeaders = await (
      await producer.send({
        record: new ProducerRecord({
          topic,
          partition: 0,
          // A long given as a bigint.
          timestamp: BigInt(explicit) + 1n,
          key: null,
          value: 'b',
          headers: [new Header({ key: 'h', value: Buffer.from('v') })],
        }),
      })
    ).get();
    assert.equal(withoutHeaders.timestamp, BigInt(explicit));
    assert.equal(withHeaders.timestamp, BigInt(explicit) + 1n);
    // No timestamp: the producer stamps its current time.
    const before = Date.now();
    const future = await producer.send({ record: new ProducerRecord({ topic, partition: 0, timestamp: null, key: null, value: 'c' }) });
    const after = Date.now();
    const stamped = await future.get();
    assert.ok(stamped.timestamp >= before && stamped.timestamp <= after, `${before} <= ${stamped.timestamp} <= ${after}`);

    const { records } = await fetchRecords(BOOTSTRAP, topic, 0);
    assert.deepEqual(
      records.map((r) => [r.value.toString(), r.timestamp]),
      [
        ['a', explicit],
        ['b', explicit + 1],
        ['c', Number(stamped.timestamp)],
      ],
    );
  } finally {
    await producer.close();
  }
});

// Sends N records to partition 0 of `topic`, awaiting each send() (the
// hand-over) before the next, and returns their futures in send order.
async function sendSequentially(producer, topic, n, value) {
  const futures = [];
  for (let i = 0; i < n; i++) {
    futures.push(await producer.send({ record: new ProducerRecord({ topic, partition: 0, key: null, value: value(i) }) }));
  }
  return futures;
}

async function assertSendOrder(futures) {
  const offsets = [];
  for (const future of futures) offsets.push((await future.get()).offset);
  const outOfOrder = offsets.filter((offset, i) => i > 0 && offset <= offsets[i - 1]).length;
  assert.equal(outOfOrder, 0, `offsets in send order: ${offsets.slice(0, 20).join(',')}...`);
  return offsets;
}

test('awaited sends to a fresh topic land in send order', { skip: SKIP, timeout: 120000 }, async () => {
  const topic = freshTopic('order');
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP } });
  try {
    // The first records queue in the core on the missing metadata.
    const futures = await sendSequentially(producer, topic, N, (i) => String(i));
    const offsets = await assertSendOrder(futures);
    assert.deepEqual(offsets, Array.from({ length: N }, (_, i) => BigInt(i)));
    const { records } = await fetchRecords(BOOTSTRAP, topic, 0);
    assert.deepEqual(
      records.map((r) => r.value.toString()),
      Array.from({ length: N }, (_, i) => String(i)),
    );
  } finally {
    await producer.close();
  }
});

test('awaited sends that wait for buffer memory land in send order', { skip: SKIP, timeout: 120000 }, async () => {
  const topic = freshTopic('memory');
  // Batches of one record each, with memory for only a few: a send waits for
  // memory (the would-block path) on cached metadata.
  const producer = new KafkaProducer({
    configs: {
      'bootstrap.servers': BOOTSTRAP,
      'buffer.memory': 16384,
      'batch.size': 4096,
      'linger.ms': 0,
    },
  });
  try {
    // Metadata cached first.
    await (await producer.send({ record: new ProducerRecord({ topic, partition: 0, key: null, value: 'warm-up' }) })).get();
    const payload = 'x'.repeat(3000);
    const futures = await sendSequentially(producer, topic, N, (i) => `${i}:${payload}`);
    const offsets = await assertSendOrder(futures);
    assert.deepEqual(offsets, Array.from({ length: N }, (_, i) => BigInt(i + 1)));
  } finally {
    await producer.close();
  }
});

test('flush after awaited sends completes every future', { skip: SKIP, timeout: 120000 }, async () => {
  const topic = freshTopic('flush');
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 1000 } });
  try {
    const futures = await sendSequentially(producer, topic, 50, (i) => String(i));
    await producer.flush();
    assert.deepEqual(
      futures.map((f) => f.isDone()),
      futures.map(() => true),
    );
    await assertSendOrder(futures);
  } finally {
    await producer.close();
  }
});

test('close after awaited sends delivers every record', { skip: SKIP, timeout: 120000 }, async () => {
  const topic = freshTopic('close');
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 1000 } });
  let callbacks = 0;
  const futures = [];
  for (let i = 0; i < 50; i++) {
    futures.push(
      await producer.send({
        record: new ProducerRecord({ topic, partition: 0, key: null, value: String(i) }),
        callback: ({ metadata: m, exception: error }) => {
          assert.equal(error, null);
          callbacks++;
        },
      }),
    );
  }
  await producer.close();
  assert.equal(callbacks, 50);
  assert.deepEqual(
    futures.map((f) => f.isDone() && !f.isCompletedExceptionally()),
    futures.map(() => true),
  );
  await assertSendOrder(futures);
  const { highWatermark, records } = await fetchRecords(BOOTSTRAP, topic, 0);
  assert.equal(highWatermark, 50);
  assert.equal(records.length, 50);
});

test('a Buffer reused after each awaited send keeps every record value', { skip: SKIP, timeout: 120000 }, async () => {
  const topic = freshTopic('reuse');
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 100 } });
  try {
    const buffer = Buffer.alloc(8);
    const futures = [];
    for (let i = 0; i < 20; i++) {
      // Rewritten once send() resolved: the client no longer reads it.
      buffer.write(`value-${String(i).padStart(2, '0')}`);
      futures.push(await producer.send({ record: new ProducerRecord({ topic, partition: 0, key: null, value: buffer }) }));
    }
    buffer.fill(0x7a);
    await producer.flush();
    await assertSendOrder(futures);
    const { records } = await fetchRecords(BOOTSTRAP, topic, 0);
    assert.deepEqual(
      records.map((r) => r.value.toString()),
      Array.from({ length: 20 }, (_, i) => `value-${String(i).padStart(2, '0')}`),
    );
  } finally {
    await producer.close();
  }
});

test('a Buffer changed once send() settled: the broker gets the bytes given', { skip: SKIP, timeout: 120000 }, async () => {
  // A fresh topic: the first send waits for its metadata, so its record is
  // still in the core while send() is pending, reading the caller's own
  // memory. The key and value are left alone until send() settled; the
  // header value is copied during the call, so it may change at once.
  const topic = freshTopic('borrowed');
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, acks: 'all' } });
  try {
    const key = Buffer.from('key-0');
    const value = new Uint8Array(Buffer.from('value-0'));
    const header = Buffer.from('header-0');
    const record = new ProducerRecord({ topic, partition: 0, key, value, headers: [new Header({ key: 'h', value: header })] });
    const sent = producer.send(record);
    header.fill(0x7a);
    const future = await sent;
    key.fill(0x7a);
    value.fill(0x7a);
    await future.get();
    const { records } = await fetchRecords(BOOTSTRAP, topic, 0);
    assert.deepEqual(
      records.map((r) => [r.key && r.key.toString(), r.value.toString(), r.headers.map((h) => h.value.toString())]),
      [['key-0', 'value-0', ['header-0']]],
    );
  } finally {
    await producer.close();
  }
});

// An accepted record's Buffers are released at its hand-over, when send()
// resolves, and not held until delivery (javascript/CLAUDE.md, rule 8). A
// record the producer takes needs a broker, hence an integration test; the
// unit tests cover a record refused at its hand-over. linger.ms holds the
// batch in the producer while the Buffers are collected; a child process
// runs with --expose-gc.
test("an accepted record's key and value Buffers are released when send() resolves, before delivery", { skip: SKIP, timeout: 120000 }, () => {
  const topic = freshTopic('released');
  const script = `
    const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': process.env.BOOTSTRAP, 'linger.ms': 5000 } });
    const settle = async () => {
      for (let i = 0; i < 5; i++) {
        global.gc();
        await new Promise((r) => setTimeout(r, 50));
      }
    };
    (async () => {
      // The topic's metadata first, so the next record is taken at once.
      await (await producer.send(new ProducerRecord({ topic: process.env.TOPIC, partition: 0, key: null, value: 'warm-up' }))).get();
      const refs = [];
      const future = await (async () => {
        const key = Buffer.from('released-key');
        const value = Buffer.alloc(65536, 0x61);
        refs.push(new WeakRef(key), new WeakRef(value));
        return producer.send(new ProducerRecord({ topic: process.env.TOPIC, partition: 0, key, value }));
      })();
      await settle();
      console.log('done before delivery', future.isDone());
      console.log('alive', refs.filter((r) => r.deref() !== undefined).length);
      const metadata = await future.get();
      console.log('delivered', metadata.serializedValueSize);
      await producer.close();
    })();
  `;
  const r = spawnSync(process.execPath, ['--expose-gc', '--no-warnings', '-e', script], {
    cwd: path.join(__dirname, '..', '..'),
    encoding: 'utf8',
    timeout: 90000,
    env: { ...process.env, BOOTSTRAP, TOPIC: topic },
  });
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'done before delivery false\nalive 0\ndelivered 65536\n');
});

test("strings are encoded as Java's StringSerializer encodes them: a lone surrogate as ?", { skip: SKIP, timeout: 120000 }, async () => {
  const topic = freshTopic('surrogates');
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'client.id': 'js-\uD800' } });
  // Java's StringSerializer bytes for each string.
  const cases = [
    ['a\uD800b', '613f62'],
    ['a\uDC00b', '613f62'],
    ['\uDC00\uD800', '3f3f'],
    ['\uD83D\uDE00', 'f09f9880'],
  ];
  try {
    for (const [s] of cases) {
      await (await producer.send(new ProducerRecord({ topic, partition: 0, key: s, value: s, headers: [new Header({ key: s, value: null })] }))).get();
    }
    const { records } = await fetchRecords(BOOTSTRAP, topic, 0);
    assert.deepEqual(
      records.map((r) => [r.key.toString('hex'), r.value.toString('hex'), r.headers[0].keyBytes.toString('hex')]),
      cases.map(([, hex]) => [hex, hex, hex]),
    );
  } finally {
    await producer.close();
  }
});

test('a record\'s headers are read-only once send() has taken it', { skip: SKIP, timeout: 120000 }, async () => {
  const topic = freshTopic('sealed');
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP } });
  try {
    const headers = new Headers([new Header({ key: 'a', value: null })]);
    const record = new ProducerRecord({ topic, partition: 0, key: null, value: 'v', headers });
    record.headers.add({ key: 'b', value: Buffer.from('1') });
    const sent = producer.send(record);
    assert.throws(() => record.headers.add({ key: 'c', value: null }), { name: 'IllegalStateError', message: 'RecordHeaders has been closed.' });
    // The Headers given was copied into the record: it stays writable.
    headers.add({ key: 'c', value: null });
    await (await sent).get();
    const { records } = await fetchRecords(BOOTSTRAP, topic, 0);
    assert.deepEqual(records[0].headers.map((h) => h.key), ['a', 'b']);
  } finally {
    await producer.close();
  }
});

// Watches the event loop during a send loop: `turn()` counts the sends
// between two of its turns (a setImmediate that reschedules itself), which is
// bounded whatever the load, unlike a timer's gap. One turn is due at least
// after FAST_PATH_YIELD_EVERY + 1 sends taken at once, so two consecutive
// turns are at most about 2 x 1001 sends apart.
function eventLoopWatch() {
  const watch = { turns: 0, sends: 0, maxSendsPerTurn: 0, running: true };
  const turn = () => {
    if (!watch.running) return;
    watch.turns++;
    watch.maxSendsPerTurn = Math.max(watch.maxSendsPerTurn, watch.sends);
    watch.sends = 0;
    setImmediate(turn);
  };
  setImmediate(turn);
  watch.stop = () => {
    watch.running = false;
    watch.maxSendsPerTurn = Math.max(watch.maxSendsPerTurn, watch.sends);
  };
  return watch;
}
const MAX_SENDS_PER_TURN = 2 * 1001 + 64;

async function warmedProducer(what) {
  const topic = freshTopic(what);
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 5 } });
  // Warmed: the topic's metadata is cached, so the producer takes every
  // record at once and no send() suspends on its own.
  await (await producer.send({ record: new ProducerRecord({ topic, partition: 0, key: null, value: 'warm' }) })).get();
  return { topic, producer };
}

test('a loop awaiting sends taken at once lets the event loop and the callbacks run', { skip: SKIP, timeout: 300000 }, async () => {
  const { topic, producer } = await warmedProducer('yield');
  try {
    const SENDS = 200000;
    const value = Buffer.alloc(100, 0x78);
    const watch = eventLoopWatch();
    let callbacks = 0;
    let nested = null;
    const rssBefore = process.memoryUsage().rss;
    let rssPeak = rssBefore;
    for (let i = 0; i < SENDS; i++) {
      watch.sends++;
      await producer.send({
        record: new ProducerRecord({ topic, partition: 0, key: null, value }),
        callback: ({ metadata: m, exception: error }) => {
          assert.equal(error, null);
          callbacks++;
          // A send from inside a delivery callback (inside a drain, maybe the
          // one send() runs on entry): its own pump does not nest.
          if (nested === null) nested = producer.send({ record: new ProducerRecord({ topic, partition: 0, key: null, value: 'nested' }) });
        },
      });
      if (i % 10000 === 0) rssPeak = Math.max(rssPeak, process.memoryUsage().rss);
    }
    const callbacksDuringLoop = callbacks;
    watch.stop();
    // The event loop kept turning and the delivery callbacks ran while the
    // loop sent: their backlog stays bounded instead of growing with the run.
    assert.ok(watch.turns > 0, 'the event loop never turned during the loop');
    assert.ok(
      watch.maxSendsPerTurn <= MAX_SENDS_PER_TURN,
      `${watch.maxSendsPerTurn} sends ran between two event-loop turns`,
    );
    assert.ok(callbacksDuringLoop > SENDS / 2, `only ${callbacksDuringLoop} of ${SENDS} callbacks ran during the loop`);
    assert.ok(rssPeak - rssBefore < 300e6, `RSS grew by ${Math.round((rssPeak - rssBefore) / 1e6)} MB`);
    assert.ok(nested !== null);
    const nestedMetadata = await (await nested).get();
    assert.equal(nestedMetadata.topic, topic);
    await producer.flush();
    assert.equal(callbacks, SENDS);
  } finally {
    await producer.close();
  }
});

test('a loop awaiting one send in seven lets the event loop run', { skip: SKIP, timeout: 300000 }, async () => {
  // 7 divides 1001 (the yield period when only one send per period waits for
  // the turn), so a turn given to a fire-and-forget send would never reach
  // the awaited one.
  const { topic, producer } = await warmedProducer('yield-seven');
  try {
    const ITERATIONS = 30000;
    const L = 7;
    const value = Buffer.alloc(100, 0x78);
    let ticks = 0;
    const timer = setInterval(() => ticks++, 1);
    const watch = eventLoopWatch();
    let callbacks = 0;
    const callback = ({ metadata: m, exception: error }) => {
      assert.equal(error, null);
      callbacks++;
    };
    const fired = [];
    for (let i = 0; i < ITERATIONS; i++) {
      for (let j = 0; j < L; j++) {
        watch.sends++;
        const sent = producer.send({ record: new ProducerRecord({ topic, partition: 0, key: null, value }), callback });
        if (j === 0) await sent;
        else fired.push(sent);
      }
    }
    const ticksDuringLoop = ticks;
    watch.stop();
    clearInterval(timer);
    assert.ok(ticksDuringLoop > 0, 'the timer never fired during the loop');
    assert.ok(watch.turns > 0, 'the event loop never turned during the loop');
    assert.ok(
      watch.maxSendsPerTurn <= MAX_SENDS_PER_TURN,
      `${watch.maxSendsPerTurn} sends ran between two event-loop turns`,
    );
    await Promise.all(fired);
    await producer.flush();
    assert.equal(callbacks, ITERATIONS * L);
  } finally {
    await producer.close();
  }
});

test('queued hand-overs of a second caller do not keep the first from yielding', { skip: SKIP, timeout: 300000 }, async () => {
  // Caller B's hand-overs are queued and complete inside caller A's pumps,
  // with no event-loop turn between them: they must not restart the count A's
  // sends taken at once reach the deferral with.
  const topic = freshTopic('yield-two-callers');
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 5, 'max.block.ms': 1 } });
  try {
    // With max.block.ms 1 the first sends may time out before the metadata
    // arrives: retry until partition 0 is known.
    for (let i = 0; ; i++) {
      try {
        await (await producer.send({ record: new ProducerRecord({ topic, partition: 0, key: null, value: 'warm' }) })).get();
        break;
      } catch (e) {
        if (i > 200) throw e;
        await new Promise((r) => setTimeout(r, 50));
      }
    }
    const SENDS = 300000;
    const value = Buffer.alloc(100, 0x78);
    let aDone = false;
    let bSends = 0;
    let bFirst = null;
    const watch = eventLoopWatch();
    // Partition 1000 does not exist: each hand-over is queued and resolves
    // about 1 ms later with a future failed with Kafka's TimeoutError.
    const b = (async () => {
      while (!aDone) {
        const future = await producer.send({ record: new ProducerRecord({ topic, partition: 1000, key: null, value: 'b' }) });
        // A failed future's get() rejects with ExecutionError, the delivery error its cause.
        const outcome = future
          .get()
          .then(() => 'ok', (e) => (e instanceof ExecutionError && e.cause instanceof TimeoutError ? 'TimeoutError' : String(e)));
        if (bFirst === null) bFirst = await outcome;
        bSends++;
      }
    })();
    for (let i = 0; i < SENDS; i++) {
      watch.sends++;
      await producer.send({ record: new ProducerRecord({ topic, partition: 0, key: null, value }) });
    }
    aDone = true;
    watch.stop();
    await b;
    assert.ok(bSends > 0, 'caller B never handed a record over');
    assert.equal(bFirst, 'TimeoutError');
    assert.ok(watch.turns > 0, 'the event loop never turned during the loop');
    assert.ok(
      watch.maxSendsPerTurn <= MAX_SENDS_PER_TURN,
      `${watch.maxSendsPerTurn} sends ran between two event-loop turns`,
    );
    await producer.flush();
  } finally {
    await producer.close();
  }
});

test('send() runs the delivery callbacks already due before handing its record over', { skip: SKIP, timeout: 300000 }, async () => {
  const { topic, producer } = await warmedProducer('pump');
  try {
    const SENDS = 50000;
    let inSend = false;
    let callbacks = 0;
    let callbacksInsideSend = 0;
    const callback = ({ metadata: m, exception: error }) => {
      assert.equal(error, null);
      callbacks++;
      if (inSend) callbacksInsideSend++;
    };
    for (let i = 0; i < SENDS; i++) {
      inSend = true;
      let sent;
      try {
        sent = producer.send({ record: new ProducerRecord({ topic, partition: 0, key: null, value: `v${i}` }), callback });
      } finally {
        inSend = false;
      }
      await sent;
    }
    // Without the pump, callbacks run only from the threadsafe function's
    // call_js, outside every send() call.
    assert.ok(callbacksInsideSend > 0, 'no delivery callback ran inside a send() call');
    await producer.flush();
    assert.equal(callbacks, SENDS);
  } finally {
    await producer.close();
  }
});

test("partitionsFor resolves to the topic's partitions, PartitionInfo with Node values", { skip: SKIP, timeout: 120000 }, async () => {
  const { Node, PartitionInfo } = require('@confluentinc/kafka-javascript/common');
  const topic = freshTopic('pf');
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP } });
  try {
    // The first send creates the topic (auto-creation), with one partition.
    await (await producer.send(new ProducerRecord({ topic, value: Buffer.from('v') }))).get();
    const partitions = await producer.partitionsFor(topic);
    assert.ok(Array.isArray(partitions));
    assert.equal(partitions.length, 1);
    const [info] = partitions;
    assert.ok(info instanceof PartitionInfo);
    assert.equal(info.topic(), topic);
    assert.equal(info.partition(), 0);
    assert.ok(info.leader() instanceof Node);
    assert.equal(info.leader().isEmpty, false);
    assert.deepEqual(info.replicas().map((n) => n.id), [info.leader().id]);
    assert.ok(info.replicas()[0].equals(info.leader()));
    assert.deepEqual(info.inSyncReplicas().map((n) => n.id), [info.leader().id]);
    assert.deepEqual(info.offlineReplicas(), []);
    assert.equal(info.toString(), `Partition(topic = ${topic}, partition = 0, leader = ${info.leader().id}, replicas = [${info.leader().id}], isr = [${info.leader().id}], offlineReplicas = [])`);
  } finally {
    await producer.close();
  }
});

test('metrics() counts the records sent, read again on each call', { skip: SKIP, timeout: 120000 }, async () => {
  const { MetricName } = require('@confluentinc/kafka-javascript/common');
  const topic = freshTopic('metrics');
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'client.id': 'js-it-metrics' } });
  try {
    const key = new MetricName({ name: 'record-send-total', group: 'producer-metrics', description: '', tags: new Map([['client-id', 'js-it-metrics']]) });
    assert.equal(producer.metrics().get(key).metricValue(), 0);
    for (let i = 0; i < 3; i++) await (await producer.send(new ProducerRecord({ topic, value: Buffer.from(`v${i}`) }))).get();
    assert.equal(producer.metrics().get(key).metricValue(), 3);
  } finally {
    await producer.close();
  }
});
