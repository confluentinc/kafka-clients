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

const { KafkaFuture, KafkaProducer, ProducerRecord, RecordMetadata } = require('../..');
const { fetchRecords } = require('./fetch_records');

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
  const producer = new KafkaProducer({ 'bootstrap.servers': BOOTSTRAP, acks: 'all' });
  try {
    const before = Date.now();
    let callbacks = 0;
    let callbackMetadata;
    const future = await producer.send(
      new ProducerRecord({
        topic,
        key: 'k1',
        value: Buffer.from('hello-from-node'),
        headers: [
          { key: 'h1', value: 'v1' },
          { key: 'h2', value: null },
        ],
      }),
      (m, error) => {
        callbacks++;
        callbackMetadata = m;
        assert.equal(error, null);
      },
    );
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
    // get() again returns the same outcome.
    assert.equal((await future.get()).toString(), metadata.toString());

    // A second record lands after the first on the same partition.
    const next = await (
      await producer.send(new ProducerRecord({ topic, partition: metadata.partition, value: 'second' }))
    ).get();
    assert.equal(next.partition, metadata.partition);
    assert.equal(next.offset, metadata.offset + 1);
    assert.equal(next.serializedKeySize, -1);
    await producer.flush();

    const { records } = await fetchRecords(BOOTSTRAP, topic, metadata.partition);
    assert.deepEqual(
      records.map((r) => [r.offset, r.key && r.key.toString(), r.value.toString()]),
      [
        [metadata.offset, 'k1', 'hello-from-node'],
        [next.offset, null, 'second'],
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
    futures.push(await producer.send(new ProducerRecord({ topic, partition: 0, value: value(i) })));
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
  const producer = new KafkaProducer({ 'bootstrap.servers': BOOTSTRAP });
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
    'bootstrap.servers': BOOTSTRAP,
    'buffer.memory': 16384,
    'batch.size': 4096,
    'linger.ms': 0,
  });
  try {
    // Metadata cached first.
    await (await producer.send(new ProducerRecord({ topic, partition: 0, value: 'warm-up' }))).get();
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
  const producer = new KafkaProducer({ 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 1000 });
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
  const producer = new KafkaProducer({ 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 1000 });
  let callbacks = 0;
  const futures = [];
  for (let i = 0; i < 50; i++) {
    futures.push(
      await producer.send(new ProducerRecord({ topic, partition: 0, value: String(i) }), (m, error) => {
        assert.equal(error, null);
        callbacks++;
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
  const producer = new KafkaProducer({ 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 100 });
  try {
    const buffer = Buffer.alloc(8);
    const futures = [];
    for (let i = 0; i < 20; i++) {
      // Rewritten once send() resolved: the client no longer reads it.
      buffer.write(`value-${String(i).padStart(2, '0')}`);
      futures.push(await producer.send(new ProducerRecord({ topic, partition: 0, value: buffer })));
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
  const producer = new KafkaProducer({ 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 5 });
  // Warmed: the topic's metadata is cached, so the producer takes every
  // record at once and no send() suspends on its own.
  await (await producer.send(new ProducerRecord({ topic, partition: 0, value: 'warm' }))).get();
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
      await producer.send(new ProducerRecord({ topic, partition: 0, value }), (m, error) => {
        assert.equal(error, null);
        callbacks++;
        // A send from inside a delivery callback (inside a drain, maybe the
        // one send() runs on entry): its own pump does not nest.
        if (nested === null) nested = producer.send(new ProducerRecord({ topic, partition: 0, value: 'nested' }));
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
    const callback = (m, error) => {
      assert.equal(error, null);
      callbacks++;
    };
    const fired = [];
    for (let i = 0; i < ITERATIONS; i++) {
      for (let j = 0; j < L; j++) {
        watch.sends++;
        const sent = producer.send(new ProducerRecord({ topic, partition: 0, value }), callback);
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
  const producer = new KafkaProducer({ 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 5, 'max.block.ms': 1 });
  try {
    // With max.block.ms 1 the first sends may time out before the metadata
    // arrives: retry until partition 0 is known.
    for (let i = 0; ; i++) {
      try {
        await (await producer.send(new ProducerRecord({ topic, partition: 0, value: 'warm' }))).get();
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
    // about 1 ms later with a future failed with REQUEST_TIMED_OUT.
    const b = (async () => {
      while (!aDone) {
        const future = await producer.send(new ProducerRecord({ topic, partition: 1000, value: 'b' }));
        const outcome = future.get().then(() => 'ok', (e) => e.name);
        if (bFirst === null) bFirst = await outcome;
        bSends++;
      }
    })();
    for (let i = 0; i < SENDS; i++) {
      watch.sends++;
      await producer.send(new ProducerRecord({ topic, partition: 0, value }));
    }
    aDone = true;
    watch.stop();
    await b;
    assert.ok(bSends > 0, 'caller B never handed a record over');
    assert.equal(bFirst, 'REQUEST_TIMED_OUT');
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
    const callback = (m, error) => {
      assert.equal(error, null);
      callbacks++;
      if (inSend) callbacksInsideSend++;
    };
    for (let i = 0; i < SENDS; i++) {
      inSend = true;
      let sent;
      try {
        sent = producer.send(new ProducerRecord({ topic, partition: 0, value: `v${i}` }), callback);
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
