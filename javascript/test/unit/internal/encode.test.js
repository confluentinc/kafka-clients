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
// own Buffer.from writes U+FFFD (ef bf bd) instead. decodeLikeJava reads bytes
// back as Java's new String(bytes, UTF_8) does (test/java_vectors.json, the
// StringDeserializer rows, checks it against Java).

const assert = require('node:assert/strict');

const { decodeLikeJava, encodeLikeJava } = require('../../../dist/internal/encode');
const { configEntries: untypedEntries } = require('../../../dist/internal/config');
const { RecordHeader } = require('@confluentinc/kafka-javascript/common/header');
const { StringSerializer } = require('@confluentinc/kafka-javascript/common/serialization');
const { ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
const { internalRecord } = require('../../_internal');

// The compiled JavaScript carries no signature: configEntries is typed by its source's.
const configEntries = /** @type {typeof import('../../../src/internal/config').configEntries} */ (untypedEntries);

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

test('a key and a value through StringSerializer, and a header key, are encoded the Java way', () => {
  const serializer = new StringSerializer();
  for (const [what, s, hex] of JAVA) {
    const record = new ProducerRecord({
      topic: 't',
      partition: null,
      key: s,
      value: s,
      headers: [new RecordHeader({ key: s, value: null }), [s, null]],
    });
    const key = serializer.serialize({ topic: 't', headers: record.headers, data: record.key });
    const value = serializer.serialize({ topic: 't', headers: record.headers, data: record.value });
    assert.ok(key instanceof Uint8Array && value instanceof Uint8Array, what);
    const [, , , , , headers] = internalRecord(record).internalToNative(key, value);
    assert.equal(Buffer.from(key).toString('hex'), hex, `${what}: key`);
    assert.equal(Buffer.from(value).toString('hex'), hex, `${what}: value`);
    // The header keys cross as their bytes, encodeLikeJava's, at send(): the
    // headers keep the key as given, as Java's RecordHeader keeps the String.
    assert.ok(headers);
    for (const i of [0, 2]) assert.equal(Buffer.from(/** @type {Uint8Array} */ (headers[i])).toString('hex'), hex, `${what}: header key`);
    assert.equal(record.headers.toArray()[0].key(), s, what);
  }
});

test('config keys and values cross as encodeLikeJava bytes', () => {
  for (const [what, s, hex] of JAVA) {
    if (s === '') continue;
    const [[key, value]] = configEntries({ [`client.id${s}`]: s }, ['key.serializer', 'value.serializer']);
    assert.equal(value.toString('hex'), hex, `${what}: value`);
    assert.equal(key.toString('hex'), Buffer.from('client.id').toString('hex') + hex, `${what}: key`);
  }
  // A list's items go through it too.
  const [[key, value]] = configEntries({ 'bootstrap.servers': ['a\uD800', 'b'] }, ['key.serializer', 'value.serializer']);
  assert.deepEqual([key.toString(), value.toString()], ['bootstrap.servers', 'a?,b']);
});

test("bytes are read as Java reads them: an encoded surrogate is one U+FFFD, where Node's decoder writes one per byte", () => {
  for (const [hex, expected] of [
    ['', ''],
    ['616263', 'abc'],
    ['eda080', '\ufffd'],
    ['61eda08062', 'a\ufffdb'],
    ['eda000', '\ufffd\u0000'],
    ['eda0ff', '\ufffd\ufffd'],
    ['ed9fbf', '\ud7ff'],
    ['ff', '\ufffd'],
  ]) {
    assert.equal(decodeLikeJava(Buffer.from(hex, 'hex')), expected, hex);
  }
  // Node's own decoder differs there, which is why the bytes go through here.
  assert.equal(Buffer.from('eda080', 'hex').toString('utf8'), '\ufffd\ufffd\ufffd');
});
