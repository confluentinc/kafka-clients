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

// The memory probes that need a broker (test/memory/_probe.js: a child with
// --expose-gc, the allocator's poisoning and the addon's test-only counts).
// A KafkaProducer's send() resolves at the record's hand-over, which needs
// the topic's metadata, so only a broker leaves a time between the hand-over
// and the delivery: linger.ms holds the batch in the producer for that time.
// Against an unreachable broker the hand-over fails with the delivery. A
// consumer's batch needs fetched records, so only a broker gives one.

const assert = require('node:assert/strict');
const path = require('node:path');

const { NO_LEAKS, probeResult, droppedFuturesProbe, droppedFutureCalls } = require('../memory/_probe');

const BOOTSTRAP = /** @type {string} */ (process.env.KAFKA_BOOTSTRAP_SERVERS);
const SKIP = BOOTSTRAP ? false : 'KAFKA_BOOTSTRAP_SERVERS is not set: no broker to produce to';
if (SKIP) console.log(`skipping the memory probes that need a broker: ${SKIP}`);

// The record's context owns its callback, not its future (javascript/CLAUDE.md,
// rule 5), and a future need not be awaited: a dropped one still runs its
// callback once, with the delivery. The record's key and value Buffers are the
// app's, borrowed only until the hand-over (rule 8): they are collected before
// the delivery, while the record waits in the producer.
(SKIP ? test.skip : test)(
  "a future dropped before its delivery still runs its callback once, with the delivery, and its record's Buffers go at the hand-over: a producer",
  () => {
    const topic = `js-it-dropped-futures-${process.pid}-${Date.now()}`;
    const configs = { 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 10000 };
    const out = probeResult(droppedFuturesProbe(`new KafkaProducer({ configs: ${JSON.stringify(configs)} })`, 'await p.flush();', topic), {
      timeout: 120000,
    });
    assert.deepEqual(out.calls, droppedFutureCalls(topic));
    assert.deepEqual(out.beforeDelivery, ['value 0', 'value 1', 'value 2']);
    assert.deepEqual(out.collected, ['future 0', 'future 1', 'future 2', 'value 0', 'value 1', 'value 2']);
    assert.deepEqual(out.leaks, NO_LEAKS);
  },
  180000,
);

// A consumer's poll hands out its keys and values as Buffers over the batch
// the core fetched (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Serialization), and each record's Headers keeps the batch until it
// copied its headers out: the batch is destroyed once nothing needs it,
// and outlives close() and the consumer. Where external buffers are refused
// (the test-only consumerRefuseExternalBuffers, as Electron does), the keys
// and values are copies, which hold nothing of the batch.

/**
 * A probe that writes `count` records to a fresh topic (with headers when
 * `headers`), reads them back with a consumer, and then runs `body`, which
 * sees `records` (the records polled), `consumer` (closed by then when
 * `closeFirst`), `batches()` (the batches made and not destroyed) and
 * `stats()`.
 *
 * @param {{ count: number, headers: boolean, refuse: boolean, body: string }} options
 * @returns {string} the probe's body
 */
function batchProbe({ count, headers, refuse, body }) {
  const topic = `js-it-batch-${process.pid}-${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
  return `
    const { KafkaProducer, ProducerRecord } = require(D + '/producer');
    const { KafkaConsumer } = require(D + '/consumer');
    const { TopicPartition } = require(D + '/common');
    const { RecordHeader } = require(D + '/common/header');
    const { createTopic } = require(${JSON.stringify(path.join(__dirname, '_fetch_records'))});
    const BOOTSTRAP = ${JSON.stringify(BOOTSTRAP)};
    const TOPIC = ${JSON.stringify(topic)};
    const stats = () => native.consumerStats();
    const batches = () => stats().batchesMade - stats().batchesDestroyed;
    (async () => {
      const hold = setInterval(() => {}, 1000);
      await createTopic(BOOTSTRAP, TOPIC);
      await (async () => {
        const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 100 } });
        const futures = [];
        for (let i = 0; i < ${count}; i++) {
          futures.push(await producer.send(new ProducerRecord({
            topic: TOPIC, partition: 0, timestamp: null, key: Buffer.from('key-' + i), value: Buffer.from('value-' + i),
            headers: ${headers} ? [new RecordHeader({ key: 'h', value: Buffer.from('header-' + i) })] : [],
          })));
        }
        for (const f of futures) await f.get();
        await producer.close();
      })();
      native.consumerRefuseExternalBuffers(${refuse});
      let consumer = new KafkaConsumer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'group.protocol': 'consumer', 'auto.offset.reset': 'earliest' } });
      await consumer.assign({ partitions: [new TopicPartition({ topic: TOPIC, partition: 0 })] });
      let records = [];
      const deadline = Date.now() + 30000;
      while (records.length < ${count} && Date.now() < deadline) records.push(...(await consumer.poll({ timeout: 500 })));
      native.consumerRefuseExternalBuffers(false);
      const out = { polled: records.length };
      ${body}
      out.leaks = leaks();
      console.log(JSON.stringify(out));
      clearInterval(hold);
    })();
  `;
}

(SKIP ? test.skip : test)(
  "a poll's keys and values are Buffers over its batch, which they keep, past close() and the consumer, until they are collected",
  () => {
    const out = probeResult(
      batchProbe({
        count: 5,
        headers: false,
        refuse: false,
        body: `
          const keys = records.map((r) => r.key);
          const values = records.map((r) => r.value);
          out.external = stats().buffersMade;
          out.copies = stats().copies;
          records = null;
          await settle();
          // The Buffers keep the batch, the records gone.
          out.heldByBuffers = batches();
          await consumer.close();
          consumer = null;
          await settle();
          out.heldAfterClose = batches();
          out.consumersLeft = stats().consumersMade - stats().consumersFreed;
          // Valid after close() and after the consumer is gone: freed memory
          // would read as the allocator's poison.
          out.read = keys.map((k, i) => k.toString() + '=' + values[i].toString());
          keys.length = 0;
          values.length = 0;
          await settle();
          out.heldAtEnd = batches();
        `,
      }),
      { timeout: 120000 },
    );
    assert.equal(out.polled, 5);
    // Each key and value is external: zero copy.
    assert.equal(out.external, 10);
    assert.equal(out.copies, 0);
    assert.ok(out.heldByBuffers >= 1);
    assert.equal(out.heldAfterClose, out.heldByBuffers);
    assert.equal(out.consumersLeft, 0);
    assert.deepEqual(
      out.read,
      [0, 1, 2, 3, 4].map((i) => `key-${i}=value-${i}`),
    );
    assert.equal(out.heldAtEnd, 0);
    assert.deepEqual(out.leaks, NO_LEAKS);
  },
  180000,
);

(SKIP ? test.skip : test)(
  "a record's Headers keeps its batch until it copied its headers out, and then holds nothing of it",
  () => {
    const out = probeResult(
      batchProbe({
        count: 3,
        headers: true,
        refuse: false,
        body: `
          const headers = records.map((r) => r.headers);
          records = null;
          await settle();
          // Only the Headers are left, none read yet: they keep the batch.
          out.heldByHeaders = batches();
          await consumer.close();
          consumer = null;
          // Read after close(): each copies its list out of the batch.
          out.read = headers.map((h) => h.lastHeader('h').value().toString());
          await settle();
          out.heldAfterRead = batches();
          // The copies stay valid.
          out.again = headers.map((h) => h.toArray()[0].value().toString());
        `,
      }),
      { timeout: 120000 },
    );
    assert.equal(out.polled, 3);
    assert.ok(out.heldByHeaders >= 1);
    assert.deepEqual(out.read, ['header-0', 'header-1', 'header-2']);
    assert.equal(out.heldAfterRead, 0);
    assert.deepEqual(out.again, out.read);
    assert.deepEqual(out.leaks, NO_LEAKS);
  },
  180000,
);

(SKIP ? test.skip : test)(
  'where external buffers are refused, keys and values are copies, which hold nothing of the batch: at once without headers',
  () => {
    const out = probeResult(
      batchProbe({
        count: 4,
        headers: false,
        refuse: true,
        body: `
          // Every key and value is held, and the batch is gone at once.
          await settle();
          out.external = stats().buffersMade;
          out.copies = stats().copies;
          out.sources = stats().sourcesMade;
          out.held = batches();
          await consumer.close();
          consumer = null;
          await settle();
          out.read = records.map((r) => r.key.toString() + '=' + r.value.toString() + ':' + r.headers.toArray().length);
        `,
      }),
      { timeout: 120000 },
    );
    assert.equal(out.polled, 4);
    assert.equal(out.external, 0);
    assert.equal(out.copies, 8);
    // A record without headers gets an empty Headers, which holds nothing.
    assert.equal(out.sources, 0);
    assert.equal(out.held, 0);
    assert.deepEqual(
      out.read,
      [0, 1, 2, 3].map((i) => `key-${i}=value-${i}:0`),
    );
    assert.deepEqual(out.leaks, NO_LEAKS);
  },
  180000,
);

(SKIP ? test.skip : test)(
  "where external buffers are refused and records have headers, the batch goes once the last record's Headers is read",
  () => {
    const out = probeResult(
      batchProbe({
        count: 3,
        headers: true,
        refuse: true,
        body: `
          await settle();
          out.copies = stats().copies;
          out.heldBefore = batches();
          // Read every Headers but the last: the last one keeps the batch.
          out.read = records.slice(0, -1).map((r) => r.headers.lastHeader('h').value().toString());
          await settle();
          out.heldBeforeLast = batches();
          out.read.push(records[records.length - 1].headers.lastHeader('h').value().toString());
          await settle();
          out.heldAfterLast = batches();
          await consumer.close();
          consumer = null;
          await settle();
          // Every key, value and header is still valid.
          out.values = records.map((r) => r.value.toString());
        `,
      }),
      { timeout: 120000 },
    );
    assert.equal(out.polled, 3);
    assert.equal(out.copies, 6);
    assert.ok(out.heldBefore >= 1);
    assert.ok(out.heldBeforeLast >= 1);
    assert.equal(out.heldAfterLast, 0);
    assert.deepEqual(out.read, ['header-0', 'header-1', 'header-2']);
    assert.deepEqual(out.values, ['value-0', 'value-1', 'value-2']);
    assert.deepEqual(out.leaks, NO_LEAKS);
  },
  180000,
);
