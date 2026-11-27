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

// The record a send built, read back from the native record through the
// test-only recordSpy() (native/record_spy.c): no copy of a key or a value,
// proved by address rather than by reading the source as the linter's
// check-zero-copy does (javascript/CLAUDE.md, rule 8), and every field the
// record holds, the headers in order.

const assert = require('node:assert/strict');

const { NO_LEAKS, CRASH_TEXT, runProbe, lastJson } = require('./_probe');

test('a send hands the core the caller’s own key and value memory, and the record holds every field as given', () => {
  const r = runProbe(`
    const { RecordHeader } = require(D + '/common/header');
    const { StringSerializer } = require(D + '/common/serialization');
    const { KafkaProducer, ProducerRecord } = require(D + '/producer');
    const hex = (b) => (b === null ? null : Buffer.from(b.buffer, b.byteOffset, b.byteLength).toString('hex'));
    const spied = (s, key, value) => ({
      topic: s.topic,
      partition: s.partition,
      timestamp: String(s.timestamp),
      key: hex(s.key),
      value: hex(s.value),
      keyIsTheCallers: s.keyArg === key,
      valueIsTheCallers: s.valueArg === value,
      keyInPlace: s.keyInPlace,
      valueInPlace: s.valueInPlace,
      headers: s.headers.map(([k, v]) => [k, hex(v)]),
    });
    (async () => {
      const hold = setInterval(() => {}, 1000);
      const out = {};
      await (async () => {
        const configs = { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 200 };
        const p = new KafkaProducer({ configs });
        // A Buffer key and value, every field given, headers with a null and
        // an empty value: RecordHeader objects, [key, value] pairs and a
        // Header the app implements, mixed.
        const key = Buffer.from('key-1');
        const value = Buffer.from('value-1');
        native.recordSpy(true);
        await p.send({
          record: new ProducerRecord({
            topic: 'read-back',
            partition: 2,
            timestamp: 5n,
            key,
            value,
            headers: [
              new RecordHeader({ key: 'trace', value: null }),
              ['empty', Buffer.alloc(0)],
              { key: () => 'trace', value: () => Buffer.from('t2') },
              ['z', new Uint8Array([0, 255])],
            ],
          }),
          callback: () => {},
        });
        out.buffers = native.recordSpy(true).map((s) => spied(s, key, value));
        // A Uint8Array view into the middle of a larger ArrayBuffer, partition
        // and timestamp left out.
        const backing = new ArrayBuffer(64);
        new Uint8Array(backing).fill(7);
        const view = new Uint8Array(backing, 13, 5);
        await p.send({ record: new ProducerRecord({ topic: 'read-back', key: view, value: view }), callback: () => {} });
        out.view = native.recordSpy(true).map((s) => spied(s, view, view));
        // A null key, and an empty value: zero bytes have no address to
        // compare, only Java's null and empty told apart.
        const empty = new Uint8Array(0);
        await p.send({ record: new ProducerRecord({ topic: 'read-back', key: null, value: empty }), callback: () => {} });
        out.empty = native.recordSpy(false).map((s) => {
          const { keyInPlace, valueInPlace, ...rest } = spied(s, null, empty);
          return rest;
        });
        await p.close();

        // A serializer's output: the very Buffers it returned.
        const made = [];
        const serializer = {
          serialize: ({ data }) => {
            const b = Buffer.from('ser:' + data);
            made.push(b);
            return b;
          },
        };
        const s = new KafkaProducer({ configs, keySerializer: serializer, valueSerializer: serializer });
        native.recordSpy(true);
        await s.send({ record: new ProducerRecord({ topic: 'read-back', key: 'k', value: 'v' }), callback: () => {} });
        out.serialized = native.recordSpy(false).map((x) => spied(x, made[0], made[1]));
        await s.close();

        // A string the client encodes itself: its own bytes, still not copied again.
        const t = new KafkaProducer({ configs, keySerializer: new StringSerializer(), valueSerializer: new StringSerializer() });
        native.recordSpy(true);
        await t.send({ record: new ProducerRecord({ topic: 'read-back', key: 'é', value: 'ü' }), callback: () => {} });
        const [own] = native.recordSpy(false);
        out.encoded = { key: hex(own.key), value: hex(own.value), keyInPlace: own.keyInPlace, valueInPlace: own.valueInPlace };
        await t.close();
        // Disarmed: nothing is kept.
        out.disarmed = native.recordSpy(false).length;
      })();
      await settle();
      out.leaks = leaks();
      console.log(JSON.stringify(out));
      clearInterval(hold);
    })();
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.doesNotMatch(r.stderr, CRASH_TEXT);
  const out = /** @type {Record<string, unknown>} */ (lastJson(r));
  const hex = (/** @type {string} */ s) => Buffer.from(s).toString('hex');
  const inPlace = { keyIsTheCallers: true, valueIsTheCallers: true, keyInPlace: true, valueInPlace: true };
  assert.deepEqual(out.buffers, [
    {
      topic: 'read-back',
      partition: 2,
      timestamp: '5',
      key: hex('key-1'),
      value: hex('value-1'),
      ...inPlace,
      headers: [
        ['trace', null],
        ['empty', ''],
        ['trace', hex('t2')],
        ['z', '00ff'],
      ],
    },
  ]);
  assert.deepEqual(out.view, [
    { topic: 'read-back', partition: -1, timestamp: '-1', key: '0707070707', value: '0707070707', ...inPlace, headers: [] },
  ]);
  assert.deepEqual(out.empty, [
    {
      topic: 'read-back',
      partition: -1,
      timestamp: '-1',
      key: null,
      value: '',
      keyIsTheCallers: true,
      valueIsTheCallers: true,
      headers: [],
    },
  ]);
  assert.deepEqual(out.serialized, [
    { topic: 'read-back', partition: -1, timestamp: '-1', key: hex('ser:k'), value: hex('ser:v'), ...inPlace, headers: [] },
  ]);
  assert.deepEqual(out.encoded, { key: 'c3a9', value: 'c3bc', keyInPlace: true, valueInPlace: true });
  assert.equal(out.disarmed, 0);
  assert.deepEqual(out.leaks, NO_LEAKS);
});
