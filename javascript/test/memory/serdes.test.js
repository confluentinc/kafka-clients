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

// The serdes and serde config functions of both clients, and the parsed
// configuration a client is built from (native/config.c, configStats()): the
// configuration is freed once the client is built or its construction failed,
// without waiting for a collection, and create() holds it across its awaits
// only; a client whose serdes and config functions reference it is collected
// once it is closed and dropped, as is everything they hold. Each probe runs
// in a child with --expose-gc and the allocator's poisoning.

const assert = require('node:assert/strict');

const { NO_LEAKS, probeResult: probe } = require('./_probe');

test("a client's parsed configuration is freed once it is built or its construction failed, and create() holds it only across its awaits", () => {
  const out = probe(`
    const { KafkaProducer } = require(D + '/producer');
    const { KafkaConsumer, CloseOptions } = require(D + '/consumer');
    const { StringSerializer, StringDeserializer } = require(D + '/common/serialization');
    const live = () => { const s = native.configStats(); return s.configsMade - s.configsFreed; };
    const P = { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 200 };
    const C = { 'bootstrap.servers': '127.0.0.1:1', 'group.id': 'g', 'group.protocol': 'consumer' };
    const quiet = (f) => { try { f(); } catch {} };
    (async () => {
      const hold = setInterval(() => {}, 1000);
      const counts = {};
      await (async () => {
        const p = new KafkaProducer({ ...P, 'key.serializer': () => new StringSerializer() });
        const c = new KafkaConsumer({ ...C, 'key.deserializer': () => new StringDeserializer() });
        counts.built = live();
        // A function that throws, the core's refusal after the functions ran,
        // and a function that returns no serde.
        quiet(() => new KafkaProducer({ ...P, 'key.serializer': () => { throw null; } }));
        quiet(() => new KafkaConsumer({ ...C, 'group.protocol': 'classic', 'key.deserializer': () => new StringDeserializer() }));
        quiet(() => new KafkaConsumer({ ...C, 'value.deserializer': () => 5 }));
        counts.failed = live();
        // create(): held while a function waits, freed once it rejects or the client is built.
        let release;
        const waiting = KafkaConsumer.create({ ...C, 'key.deserializer': () => new Promise((r) => { release = r; }) });
        await new Promise((r) => setImmediate(r));
        counts.awaiting = live();
        release(new StringDeserializer());
        const created = await waiting;
        counts.created = live();
        await KafkaProducer.create({ ...P, 'value.serializer': async () => { throw new Error('no schema'); } }).catch(() => {});
        await KafkaConsumer.create({ ...C, 'fetch.min.bytes': 'many' }).catch(() => {});
        counts.createFailed = live();
        await Promise.all([p.close(), c.close(CloseOptions.timeout(0)), created.close(CloseOptions.timeout(0))]);
      })();
      await settle();
      console.log(JSON.stringify({ counts, leaks: leaks() }));
      clearInterval(hold);
    })();
  `);
  assert.deepEqual(out.counts, { built: 0, failed: 0, awaiting: 1, created: 0, createFailed: 0 });
  assert.deepEqual(out.leaks, NO_LEAKS);
});

test('serdes and serde config functions that reference their own client are let go with it, once it is closed and dropped', () => {
  const out = probe(`
    const { KafkaProducer } = require(D + '/producer');
    const { KafkaConsumer, CloseOptions } = require(D + '/consumer');
    const P = { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 200 };
    const C = { 'bootstrap.servers': '127.0.0.1:1', 'group.id': 'g', 'group.protocol': 'consumer' };
    const collected = [];
    const registry = new FinalizationRegistry((name) => collected.push(name));
    // A serde whose methods reach its client through 'owner', set once the client exists.
    const serde = (box) => ({
      serialize: () => (box.owner === null ? null : null),
      deserialize: () => box.owner,
      close: () => { box.closed = box.owner !== null; },
    });
    (async () => {
      const hold = setInterval(() => {}, 1000);
      const closed = {};
      await (async () => {
        const clients = {};
        const boxes = {};
        const make = (name, build) => {
          const box = { owner: null, closed: false };
          boxes[name] = box;
          const client = build(box);
          box.owner = client;
          clients[name] = client;
          registry.register(client, name);
        };
        make('producer args', (box) => new KafkaProducer({ configs: P, keySerializer: serde(box), valueSerializer: serde(box) }));
        make('producer keys', (box) => new KafkaProducer({ ...P, 'key.serializer': () => serde(box), 'value.serializer': () => serde(box) }));
        make('consumer args', (box) => new KafkaConsumer({ configs: C, keyDeserializer: serde(box), valueDeserializer: serde(box) }));
        make('consumer keys', (box) => new KafkaConsumer({ ...C, 'key.deserializer': () => serde(box), 'value.deserializer': () => serde(box) }));
        for (const [name, create] of [
          ['producer create', () => KafkaProducer.create({ ...P, 'value.serializer': async () => serde(boxes['producer create']) })],
          ['consumer create', () => KafkaConsumer.create({ ...C, 'key.deserializer': async () => serde(boxes['consumer create']) })],
        ]) {
          boxes[name] = { owner: null, closed: false };
          const client = await create();
          boxes[name].owner = client;
          clients[name] = client;
          registry.register(client, name);
        }
        for (const [name, client] of Object.entries(clients)) {
          await (name.startsWith('consumer') ? client.close(CloseOptions.timeout(0)) : client.close());
          closed[name] = boxes[name].closed;
        }
      })();
      await settle();
      console.log(JSON.stringify({ closed, collected: collected.sort(), leaks: leaks() }));
      clearInterval(hold);
    })();
  `);
  const names = ['consumer args', 'consumer create', 'consumer keys', 'producer args', 'producer create', 'producer keys'];
  assert.deepEqual(out.closed, Object.fromEntries(names.map((n) => [n, true])));
  assert.deepEqual(out.collected, names);
  assert.deepEqual(out.leaks, NO_LEAKS);
});
