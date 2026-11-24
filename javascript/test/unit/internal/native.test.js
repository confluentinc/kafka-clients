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

const assert = require('node:assert/strict');

const native = require('../../../dist/internal/native');

test('the addon loads and links libconfluent_kafka', () => {
  assert.equal(native.probe(), true);
});

test('producerClose takes a bigint timeout exactly, and refuses one beyond 64 bits', async () => {
  // src/internal/duration.ts hands Infinity over as Long.MAX_VALUE, a bigint, which a
  // number cannot hold.
  // The configuration crosses as encodeLikeJava's bytes.
  const handle = native.producerCreate([[Buffer.from('bootstrap.servers'), Buffer.from('127.0.0.1:1')]]);
  assert.throws(() => native.producerClose(handle, 2n ** 64n, () => {}), {
    name: 'RangeError',
    message: 'close timeoutMs must fit a signed 64-bit integer',
  });
  const result = await new Promise((resolve) => {
    native.producerClose(handle, 9223372036854775807n, (/** @type {unknown} */ error, /** @type {unknown} */ destroyed) =>
      resolve({ error, destroyed }),
    );
  });
  assert.deepEqual(result, { error: null, destroyed: true });
});

test('producerCreate takes the configuration as bytes, and refuses a string', () => {
  assert.throws(() => native.producerCreate([['bootstrap.servers', '127.0.0.1:1']]), {
    name: 'TypeError',
    message: 'config keys must be strings',
  });
});

test("faultNext arms failures of the addon's steps, and answers and disarms the ones still armed", () => {
  // Test-only: each failure armed fails the next run of its step's site (native/addon.c).
  assert.equal(native.faultNext('close', 2), undefined);
  assert.equal(native.faultNext('slot'), undefined);
  assert.deepEqual(native.faultNext(null), ['slot', 'close', 'close']);
  assert.deepEqual(native.faultNext(null), []);
  const refused = {
    name: 'TypeError',
    message: 'faultNext(step, times): step null, or notify, metadata, error, slot, partitions, close or failure; times a positive count',
  };
  assert.throws(() => native.faultNext('nothing'), refused);
  assert.throws(() => native.faultNext('slot', 0), refused);
  assert.deepEqual(native.faultNext(null), []);
});
