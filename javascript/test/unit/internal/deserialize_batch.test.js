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

// src/internal/deserialize_batch.ts: a poll's records through the consumer's
// deserializers, over records built as the addon builds them
// (ConsumerRecord.internalCreate). The broker tests
// (test/integration/consumer.test.js) run it through poll().

const assert = require('node:assert/strict');

const { deserializeBatch } = require('../../../dist/internal/deserialize_batch');
const { RecordDeserializationError } = require('@confluentinc/kafka-javascript/common/errors');
const { ConsumerRecord } = require('@confluentinc/kafka-javascript/consumer');

/**
 * A fetched record of partition `partition` of topic `t`, as the addon builds it.
 *
 * @param {number} partition
 * @param {number} offset
 * @param {string | null} key
 * @param {string | null} value
 * @returns {ConsumerRecord<Buffer, Buffer>}
 */
function fetched(partition, offset, key, value) {
  return /** @type {any} */ (ConsumerRecord).internalCreate(
    't',
    partition,
    BigInt(offset),
    1000n + BigInt(offset),
    0,
    key === null ? -1 : key.length,
    value === null ? -1 : value.length,
    null,
    key === null ? null : Buffer.from(key),
    value === null ? null : Buffer.from(value),
    5,
    -1,
  );
}

/** Two partitions of three records each, and the core's next offsets. */
const batch = () =>
  /** @type {any} */ ({
    partitions: [
      ['t', 0, [fetched(0, 0, 'a', 'x'), fetched(0, 1, null, 'y'), fetched(0, 2, 'c', null)]],
      ['t', 1, [fetched(1, 7, 'd', 'z'), fetched(1, 8, 'e', 'w'), fetched(1, 9, 'f', 'v')]],
    ],
    nextOffsets: [
      ['t', 0, 3n, 5, ''],
      ['t', 1, 10n, 5, ''],
    ],
  });

const text = { deserialize: (/** @type {{ data: Buffer }} */ { data }) => data.toString() };

test('plain results settle the batch at once, with no Promise and no microtask; null keys and values are not deserialized', () => {
  /** @type {string[]} */
  const calls = [];
  const recording = (/** @type {string} */ role) => ({
    deserialize: (/** @type {{ data: Buffer }} */ { data }) => {
      assert.ok(Buffer.isBuffer(data));
      calls.push(`${role} ${data.toString()}`);
      return data.toString().toUpperCase();
    },
  });
  const { partitions, nextOffsets } = batch();
  const out = deserializeBatch(partitions, nextOffsets, recording('key'), recording('value'));
  assert.ok(!(out instanceof Promise));
  assert.equal(out.error, null);
  assert.deepEqual(
    out.records.records({ topic: 't' }).map((/** @type {ConsumerRecord<string, string>} */ r) => [r.offset, r.key, r.value]),
    [
      [0n, 'A', 'X'],
      [1n, null, 'Y'],
      [2n, 'C', null],
      [7n, 'D', 'Z'],
      [8n, 'E', 'W'],
      [9n, 'F', 'V'],
    ],
  );
  assert.deepEqual(calls, ['key a', 'value x', 'value y', 'key c', 'key d', 'value z', 'key e', 'value w', 'key f', 'value v']);
  // The records keep the fetched ones' other fields and their very Headers.
  const first = out.records.records({ topic: 't' })[0];
  assert.equal(first.headers, partitions[0][2][0].headers);
  assert.equal(first.leaderEpoch, 5);
  assert.ok(Object.isFrozen(first));
  assert.deepEqual(
    [...out.records.nextOffsets().entries()].map(([tp, om]) => [String(tp), om.offset]),
    [
      ['t-0', 3n],
      ['t-1', 10n],
    ],
  );
});

test('a thenable is awaited, record by record, and a null deserializer leaves its Buffers', async () => {
  const { partitions, nextOffsets } = batch();
  const out = deserializeBatch(partitions, nextOffsets, null, {
    deserialize: async (/** @type {{ data: Buffer }} */ { data }) => data.toString(),
  });
  assert.ok(out instanceof Promise);
  const done = await out;
  const records = done.records.records({ topic: 't' });
  assert.equal(records.length, 6);
  assert.ok(Buffer.isBuffer(records[0].key));
  assert.equal(records[0].value, 'x');
});

test("a failure ends the batch: the records before it, Java's error, and each partition's first record not returned", async () => {
  for (const [origin, keyFails] of /** @type {const} */ ([
    ['KEY', true],
    ['VALUE', false],
  ])) {
    for (const thrown of [new Error('bad'), null, undefined]) {
      for (const async of [false, true]) {
        const failing = {
          deserialize: (/** @type {{ data: Buffer }} */ { data }) => {
            const fails = data.toString() === (keyFails ? 'e' : 'w');
            if (!fails) return async ? Promise.resolve(data.toString()) : data.toString();
            if (async) return Promise.reject(thrown);
            throw thrown;
          },
        };
        const { partitions, nextOffsets } = batch();
        const out = await deserializeBatch(partitions, nextOffsets, failing, failing);
        // Partition 0 whole, partition 1 up to offset 7.
        assert.deepEqual(
          out.records.records({ topic: 't' }).map((/** @type {ConsumerRecord<string, string>} */ r) => r.offset),
          [0n, 1n, 2n, 7n],
        );
        // The partition cut short: the last returned record's offset + 1.
        assert.deepEqual(
          [...out.records.nextOffsets().entries()].map(([tp, om]) => [String(tp), om.offset, om.leaderEpoch]),
          [
            ['t-0', 3n, 5],
            ['t-1', 8n, 5],
          ],
        );
        assert.deepEqual(
          out.rewind.map((/** @type {[unknown, bigint]} */ [tp, offset]) => [String(tp), offset]),
          [['t-1', 8n]],
        );
        const error = out.error;
        assert.ok(error instanceof RecordDeserializationError);
        assert.equal(error.origin().name(), origin);
        assert.equal(
          error.message,
          `Error deserializing ${origin} for partition t-1 at offset 8. If needed, please seek past the record to continue consumption.`,
        );
        assert.equal(error.offset(), 8n);
        assert.equal(error.timestamp(), 1008n);
        assert.deepEqual([error.keyBuffer(), error.valueBuffer()], [Buffer.from('e'), Buffer.from('w')]);
        assert.equal(error.headers(), partitions[1][2][1].headers);
        assert.ok(Object.hasOwn(error, 'cause'));
        assert.equal(error.cause, thrown);
      }
    }
  }
});

test('a failure at the first record of a batch returns none, and seeks every partition back', () => {
  const { partitions, nextOffsets } = batch();
  const out = deserializeBatch(
    partitions,
    nextOffsets,
    {
      deserialize: () => {
        throw new Error('first');
      },
    },
    text,
  );
  assert.ok(!(out instanceof Promise));
  assert.equal(out.records.isEmpty(), true);
  assert.equal(out.records.nextOffsets().size(), 0);
  assert.deepEqual(
    out.rewind.map((/** @type {[unknown, bigint]} */ [tp, offset]) => [String(tp), offset]),
    [
      ['t-0', 0n],
      ['t-1', 7n],
    ],
  );
});
