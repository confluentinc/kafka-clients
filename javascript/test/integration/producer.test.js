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

const { ExecutionError } = require('@confluentinc/kafka-javascript');
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
    // A data object: a spread and a JSON copy keep every field.
    assert.deepEqual(JSON.parse(JSON.stringify(metadata)), {
      topic,
      partition: metadata.partition,
      offset: metadata.offset,
      hasOffset: true,
      timestamp: metadata.timestamp,
      hasTimestamp: true,
      serializedKeySize: 2,
      serializedValueSize: 15,
    });
    assert.deepEqual({ ...metadata }, JSON.parse(JSON.stringify(metadata)));
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
    assert.equal(next.offset, metadata.offset + 1);
    assert.equal(next.serializedKeySize, -1);
    await producer.flush();

    const { records } = await fetchRecords(BOOTSTRAP, topic, metadata.partition);
    assert.deepEqual(
      records.map((r) => [r.offset, r.key && r.key.toString(), r.value.toString(), r.headers.map((h) => [h.key, h.value && h.value.toString()])]),
      [
        [metadata.offset, 'k1', 'hello-from-node', [['h1', 'v1'], ['h2', null]]],
        [next.offset, null, 'second', []],
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
          timestamp: explicit + 1,
          key: null,
          value: 'b',
          headers: [new Header({ key: 'h', value: Buffer.from('v') })],
        }),
      })
    ).get();
    assert.equal(withoutHeaders.timestamp, explicit);
    assert.equal(withHeaders.timestamp, explicit + 1);
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
        ['c', stamped.timestamp],
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
    assert.deepEqual(offsets, Array.from({ length: N }, (_, i) => i));
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
    assert.deepEqual(offsets, Array.from({ length: N }, (_, i) => i + 1));
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
