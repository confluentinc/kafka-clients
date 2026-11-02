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

const test = require('node:test');
const assert = require('node:assert/strict');

const { configEntries } = require('../../lib/_config');

test('config values are stringified', () => {
  assert.deepEqual(
    configEntries({
      'bootstrap.servers': 'localhost:9092',
      'enable.idempotence': true,
      'linger.ms': 5,
      'buffer.memory': 33554432n,
      'compression.type': 'lz4',
      'interceptor.classes': ['a', 'b'],
      'batch.size': [16384],
    }),
    [
      ['bootstrap.servers', 'localhost:9092'],
      ['enable.idempotence', 'true'],
      ['linger.ms', '5'],
      ['buffer.memory', '33554432'],
      ['compression.type', 'lz4'],
      ['interceptor.classes', 'a,b'],
      ['batch.size', '16384'],
    ],
  );
  assert.deepEqual(configEntries({ acks: false }), [['acks', 'false']]);
});

test('null and undefined values are dropped', () => {
  assert.deepEqual(configEntries({ 'client.id': null, acks: undefined, 'linger.ms': 0 }), [['linger.ms', '0']]);
});

test('a Map is accepted, and its non-string keys rejected', () => {
  assert.deepEqual(configEntries(new Map([['acks', 'all']])), [['acks', 'all']]);
  assert.throws(() => configEntries(new Map([[1, 'x']])), {
    name: 'TypeError',
    message: 'config keys must be strings, not number',
  });
});

test('values of other types are rejected', () => {
  assert.throws(() => configEntries({ acks: {} }), {
    name: 'TypeError',
    message: 'config acks must be a string, a number, a boolean or an array of them, not object',
  });
  assert.throws(() => configEntries({ acks: () => 1 }), { name: 'TypeError' });
  assert.throws(() => configEntries(null), { name: 'TypeError', message: 'configs must be an object or a Map, not null' });
  assert.throws(() => configEntries([]), { name: 'TypeError', message: 'configs must be an object or a Map, not array' });
});
