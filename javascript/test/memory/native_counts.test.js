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

// The native layer's handles, counted: every producer, unit of a producer's
// ownership, send context, future slot, record future, wait, callback
// deferral, Buffer reference, mock record, error JS gave a mock and owned
// error the addon made is freed once all is closed and collected
// (native/producer.c producerStats(), native/error.c errorStats()). Each
// probe runs in a child with --expose-gc and the allocator's poisoning, so a
// handle freed twice, or used after its free, crashes it.

const assert = require('node:assert/strict');

const { NO_LEAKS, probeResult: probe, droppedFuturesProbe, droppedFutureCalls } = require('./_probe');

test('every native handle a producer makes is freed once, after close, collection and destroy', () => {
  const out = probe(`
    const { KafkaProducer, MockProducer, ProducerRecord } = require(D + '/producer');
    const { Header } = require(D + '/common/header');
    const record = (topic) =>
      new ProducerRecord({
        topic,
        partition: null,
        key: Buffer.from('k'),
        value: Buffer.from('v'),
        headers: [new Header({ key: 'h', value: null })],
      });
    const swallow = () => {};
    (async () => {
      const hold = setInterval(() => {}, 1000);
      await (async () => {
        // A producer against an unreachable broker: every record fails in the
        // core with its own error.
        const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 150 } });
        const futures = [];
        futures.push(await p.send({ record: record('plain'), callback: () => {} }));
        futures.push(await p.send({ record: record('throws'), callback: () => { throw new Error('boom'); } }));
        futures.push(await p.send({ record: record('resolves'), callback: () => Promise.resolve() }));
        futures.push(await p.send({ record: record('rejects'), callback: () => Promise.reject(new Error('later')) }));
        futures.push(await p.send(record('no-callback')));
        // A wait started before the delivery, a timed one, and one after it.
        const early = futures.map((f) => f.get().catch(swallow));
        await futures[0].get({ timeout: 0 }).catch(swallow);
        await p.flush();
        await Promise.all(early);
        await Promise.all(futures.map((f) => f.get().catch(swallow)));
        await p.close();
        // Refused after close: the core's error, owned and freed.
        await p.send(record('closed')).catch(swallow);
        await p.flush().catch(swallow);

        // A mock: completed, failed with an error JS gave it, refused by an
        // injected send error, dropped by clear(), and pending at its close.
        const m = new MockProducer({ autoComplete: false, partitioner: null, keySerializer: null, valueSerializer: null });
        const done = await m.send(record('mock'));
        const failed = await m.send(record('mock'));
        // Waits on the native futures before the delivery: one untimed, one
        // timed out, one timed that the delivery answers.
        const waiting = [done.get(), failed.get().catch(swallow), done.get({ timeout: 60000 })];
        await failed.get({ timeout: 0 }).catch(swallow);
        m.completeNext();
        m.errorNext(new Error('injected'));
        await Promise.all(waiting);
        m.setSendException(new Error('refused'));
        await m.send(record('mock')).catch(swallow);
        m.setSendException(null);
        await m.send(record('mock'));
        m.clear();
        await m.send(record('mock'));
        await m.close();
      })();
      await settle();
      console.log(JSON.stringify({ leaks: leaks(), stats: native.producerStats(), errors: native.errorStats() }));
      clearInterval(hold);
    })();
  `);
  assert.deepEqual(out.leaks, NO_LEAKS);
  // The workload reached every kind of handle it counts.
  const s = out.stats;
  for (const made of [
    'producersMade',
    'contextsMade',
    'slotsTaken',
    'futuresHeld',
    'waitsStarted',
    'deferralsMade',
    'bufferRefsTaken',
    'mockRecordsMade',
    'injectedMade',
  ]) {
    assert.ok(s[made] > 0, `${made} is ${s[made]}`);
  }
  assert.equal(s.producersMade, 2);
  assert.equal(s.deferralsMade, 2);
  assert.ok(out.errors.owned > 0 && out.errors.made > 0 && out.errors.given > 0, JSON.stringify(out.errors));
});

test('a hundred thousand timed gets on a pending future leave no wait behind, and a later get resolves', () => {
  const out = probe(
    `
    const { MockProducer, ProducerRecord } = require(D + '/producer');
    (async () => {
      const hold = setInterval(() => {}, 1000);
      const result = {};
      await (async () => {
        const m = new MockProducer({ autoComplete: false, partitioner: null, keySerializer: null, valueSerializer: null });
        const future = await m.send(new ProducerRecord({ topic: 't', value: Buffer.from('v') }));
        const base = native.producerStats();
        const names = new Map();
        for (let i = 0; i < 100000; i++) {
          try {
            await future.get({ timeout: 0 });
            names.set('resolved', (names.get('resolved') ?? 0) + 1);
          } catch (e) {
            const key = e.name + ': ' + e.message;
            names.set(key, (names.get(key) ?? 0) + 1);
          }
        }
        const s = native.producerStats();
        result.outcomes = Object.fromEntries(names);
        result.pendingWaits = s.waitsStarted - s.waitsCompleted;
        result.waitsStarted = s.waitsStarted - base.waitsStarted;
        result.heldUnits = (s.ownerUnitsTaken - base.ownerUnitsTaken) - (s.ownerUnitsReleased - base.ownerUnitsReleased);
        result.doneBefore = future.isDone();
        m.completeNext();
        result.offset = String((await future.get()).offset);
        await m.close();
      })();
      await settle();
      result.leaks = leaks();
      console.log(JSON.stringify(result));
      clearInterval(hold);
    })();
  `,
    { timeout: 300000 },
  );
  // The core's text for a record's future (javascript/CLAUDE.md, the
  // record's future's deviation from Java's "Timeout after waiting for <ms> ms.").
  assert.deepEqual(out.outcomes, { 'TimeoutError: Timed out waiting for KafkaFuture after 0 ms': 100000 });
  assert.equal(out.waitsStarted, 100000);
  assert.equal(out.pendingWaits, 0);
  assert.equal(out.heldUnits, 0);
  assert.equal(out.doneBefore, false);
  assert.equal(out.offset, '0');
  assert.deepEqual(out.leaks, NO_LEAKS);
});

test('a callback Promise still pending when the closed producer is collected is reported once, before the destroy, and runs no JS after', () => {
  // close() from inside a callback closes as close({ timeout: 0 }) does, as
  // Java's org.apache.kafka.clients.producer.KafkaProducer#close does on its
  // I/O thread, so it completes with the callback's Promise still pending.
  // Once the closed producer is collected, no JS can read the record's
  // future (a KafkaFuture holding a slot holds the producer's handle), so the
  // destroy frees its native future, slot and context, each once
  // (javascript/CLAUDE.md, rule 5: the context destroys the native future
  // exactly once), as Java's garbage collector frees a closed producer's.
  const out = probe(`
    const { KafkaProducer, ProducerRecord } = require(D + '/producer');
    const collected = [];
    const registry = new FinalizationRegistry((name) => collected.push(name));
    const events = [];
    (async () => {
      const hold = setInterval(() => {}, 1000);
      const result = {};
      await (async () => {
        const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 100 } });
        registry.register(p, 'producer');
        let closing;
        // Nothing waits on the record's future: a wait would hold the producer.
        await p.send({
          record: new ProducerRecord({ topic: 'never', value: Buffer.from('v') }),
          callback: () => {
            events.push('callback');
            closing = p.close();
            return new Promise(() => {});
          },
        });
        await new Promise((resolve) => setTimeout(resolve, 400));
        events.push(await closing.then(() => 'closed', (e) => 'close failed: ' + e.message));
        const s = native.producerStats();
        result.beforeCollection = { made: s.deferralsMade, reported: s.deferralsReported };
      })();
      await settle(20);
      const s = native.producerStats();
      result.atDestroy = { made: s.deferralsMade, reported: s.deferralsReported, released: s.deferralsReleased };
      // Nothing more reaches JS once the producer is destroyed.
      events.push('collected');
      await settle(5);
      result.events = events;
      result.collected = collected;
      result.leaks = leaks();
      console.log(JSON.stringify(result));
      clearInterval(hold);
    })();
  `);
  assert.deepEqual(out.events, ['callback', 'closed', 'collected']);
  assert.deepEqual(out.collected, ['producer']);
  assert.deepEqual(out.beforeCollection, { made: 1, reported: 0 });
  assert.deepEqual(out.atDestroy, { made: 1, reported: 1, released: 1 });
  assert.deepEqual(out.leaks, NO_LEAKS);
});

// The same probe on a KafkaProducer needs a broker, so that its send()
// resolves at the hand-over well before the delivery:
// test/integration/memory_probes.test.js.
test('a future dropped before its delivery still runs its callback once, with the delivery: a mock', () => {
  const out = probe(
    droppedFuturesProbe(
      `new MockProducer({ autoComplete: false, partitioner: null, keySerializer: null, valueSerializer: null })`,
      `while (p.completeNext());`,
      'dropped-futures',
    ),
  );
  assert.deepEqual(out.calls, droppedFutureCalls('dropped-futures'));
  // The mock keeps what it sent, as Java's history() does, and holds each
  // pending send's callback until completeNext(): nothing is collected
  // before the delivery, everything once the closed mock is.
  assert.deepEqual(out.beforeDelivery, []);
  assert.deepEqual(out.collected, ['future 0', 'future 1', 'future 2', 'value 0', 'value 1', 'value 2']);
  assert.deepEqual(out.leaks, NO_LEAKS);
});
