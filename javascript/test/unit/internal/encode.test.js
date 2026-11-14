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

// src/internal/encode.ts: the one place a string becomes bytes. The expected
// bytes are what Java's StringSerializer (String.getBytes(UTF_8)) writes for
// each string, taken from a run of Kafka's own StringSerializer, and what the
// Python client's encoding writes ("replace": `?` for a lone surrogate); Node's
// own Buffer.from writes U+FFFD (ef bf bd) instead.

const test = require('node:test');
const assert = require('node:assert/strict');

const { encodeLikeJava } = require('../../../dist/internal/encode');
const { configEntries } = require('../../../dist/internal/config');
const { Header } = require('@confluentinc/kafka-javascript/common/header');
const { ProducerRecord } = require('@confluentinc/kafka-javascript/producer');

// [what, string, Java's StringSerializer bytes, as hex]
const JAVA = [
  ['a lone high surrogate', 'a\uD800b', '613f62'],
  ['a lone low surrogate', 'a\uDC00b', '613f62'],
  ['a reversed pair', '\uDC00\uD800', '3f3f'],
  ['a normal emoji', '😀', 'f09f9880'],
  ['a high surrogate at the end', 'x\uD83D', '783f'],
  ['only a high surrogate', '\uD800', '3f'],
  ['ASCII', 'plain', Buffer.from('plain').toString('hex')],
  ['the empty string', '', ''],
];

test("strings are encoded as Java's StringSerializer encodes them, a lone surrogate as ?", () => {
  for (const [what, s, hex] of JAVA) {
    const bytes = encodeLikeJava(s);
    assert.ok(Buffer.isBuffer(bytes), what);
    assert.equal(bytes.toString('hex'), hex, what);
  }
  // Node's own conversion differs, which is why the strings go through here.
  assert.equal(Buffer.from('a\uD800b', 'utf8').toString('hex'), '61efbfbd62');
});

test('a key, a value and a header key are encoded the Java way at send', () => {
  for (const [what, s, hex] of JAVA) {
    const record = new ProducerRecord({
      topic: 't',
      partition: null,
      key: s,
      value: s,
      headers: [new Header({ key: s, value: null })],
    });
    const [, , , key, value, headers] = record.internalToNative();
    assert.equal(Buffer.from(key).toString('hex'), hex, `${what}: key`);
    assert.equal(Buffer.from(value).toString('hex'), hex, `${what}: value`);
    // The header key crosses as its bytes, encodeLikeJava's.
    assert.equal(Buffer.from(headers[0][0]).toString('hex'), hex, `${what}: header key`);
  }
});

test('config keys and values cross as encodeLikeJava bytes', () => {
  for (const [what, s, hex] of JAVA) {
    if (s === '') continue;
    const [[key, value]] = configEntries({ [`client.id${s}`]: s });
    assert.equal(value.toString('hex'), hex, `${what}: value`);
    assert.equal(key.toString('hex'), Buffer.from('client.id').toString('hex') + hex, `${what}: key`);
  }
  // A list's items go through it too.
  const [[key, value]] = configEntries({ 'bootstrap.servers': ['a\uD800', 'b'] });
  assert.deepEqual([key.toString(), value.toString()], ['bootstrap.servers', 'a?,b']);
});
