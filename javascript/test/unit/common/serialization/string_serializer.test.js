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

// stringSerializer(), Java's StringSerializer. The expected bytes are what
// Java's StringSerializer (kafka-clients 4.3.1, JDK 25) writes for each
// string, unconfigured and configured with each of Java's names of UTF-8.
// Java's SerializationTest cases of the string serde are translated for the
// serializer: the deserializer waits for the consumer, so a round trip is
// checked against Java's bytes, and of stringSerdeShouldSupportDifferentEncodings
// only UTF-8, the one encoding supported (UTF-16 is refused, below).

const test = require('node:test');
const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { SerializationError } = require('@confluentinc/kafka-javascript/common/errors');
const { Headers } = require('@confluentinc/kafka-javascript/common/header');
const { stringSerializer } = require('@confluentinc/kafka-javascript/common/serialization');

// [string, Java's bytes as hex]
const JAVA = [
  ['', ''],
  ['abc', '616263'],
  ['é', 'c3a9'],
  ['日本語', 'e697a5e69cace8aa9e'],
  ['😀', 'f09f9880'],
  ['a\uD800b', '613f62'],
  ['\uDC00', '3f'],
  ['\uD800\uD800', '3f3f'],
  ['\uDBFF', '3f'],
  ['x\uDFFFy\uD83D', '783f793f'],
];

function serialize(serializer, data) {
  const out = serializer.serialize({ topic: 't', headers: new Headers(), data });
  return out === null ? null : Buffer.from(out).toString('hex');
}

test('stringSerdeShouldSupportDifferentEncodings', () => {
  const str = 'my string';
  for (const encoding of ['UTF-8']) {
    const serializer = stringSerializer({ configs: { 'key.serializer.encoding': encoding }, isKey: true });
    assert.equal(serialize(serializer, str), Buffer.from(str, 'latin1').toString('hex'), encoding);
  }
});

test('stringSerdeConfigureThrowsOnUnknownEncoding', () => {
  const encoding = 'encoding-does-not-exist';
  const serializerConfigs = { 'key.serializer.encoding': encoding };
  assert.throws(() => stringSerializer({ configs: serializerConfigs, isKey: true }), SerializationError);
});

test("stringSerializer() writes Java's StringSerializer bytes, a lone surrogate as ?, null as null", () => {
  for (const serializer of [
    stringSerializer(),
    stringSerializer({ configs: {}, isKey: true }),
    ...['UTF-8', 'utf8', 'UTF8', 'unicode-1-1-utf-8', 'Utf-8'].map((encoding) =>
      stringSerializer({ configs: { 'serializer.encoding': encoding }, isKey: false }),
    ),
  ]) {
    for (const [s, hex] of JAVA) assert.equal(serialize(serializer, s), hex, JSON.stringify(s));
    assert.equal(serialize(serializer, null), null);
  }
  // A Buffer, the client's own.
  assert.ok(Buffer.isBuffer(stringSerializer().serialize({ topic: 't', headers: new Headers(), data: 'x' })));
});

test('key.serializer.encoding or value.serializer.encoding takes precedence over serializer.encoding', () => {
  const configs = { 'key.serializer.encoding': 'UTF-8', 'serializer.encoding': 'nope-charset' };
  // Java: the key serializer reads key.serializer.encoding, the value one
  // falls back to serializer.encoding, an unknown charset.
  assert.equal(serialize(stringSerializer({ configs, isKey: true }), 'a'), '61');
  assert.throws(() => stringSerializer({ configs, isKey: false }), (e) => {
    assert.ok(e instanceof SerializationError);
    assert.equal(e.message, 'Unsupported encoding nope-charset');
    return true;
  });
  assert.equal(serialize(stringSerializer({ configs: { 'value.serializer.encoding': 'utf8', 'serializer.encoding': 'x' }, isKey: false }), 'a'), '61');
  // A value that is not a string is ignored, as Java ignores one that is no String.
  assert.equal(serialize(stringSerializer({ configs: { 'serializer.encoding': 5 }, isKey: true }), 'a'), '61');
  // Only UTF-8 matches Java byte for byte: another charset Java has is refused, Java's message.
  for (const encoding of ['ISO-8859-1', 'US-ASCII', 'UTF-16', 'UTF-16LE', 'UTF-16BE', 'bad name!', ' UTF-8']) {
    assert.throws(() => stringSerializer({ configs: { 'key.serializer.encoding': encoding }, isKey: true }), {
      name: 'SerializationError',
      message: `Unsupported encoding ${encoding}`,
    });
  }
});

test("stringSerializer takes Java's configure(configs, isKey) or nothing, and its serializer only strings", () => {
  assert.throws(() => stringSerializer({ configs: {} }), {
    constructor: IllegalArgumentError,
    message: 'stringSerializer() takes one of (), (configs, isKey); got (configs)',
  });
  assert.throws(() => stringSerializer({ configs: null, isKey: true }), { constructor: TypeError, message: 'configs must be an object, not null' });
  assert.throws(() => stringSerializer({ configs: {}, isKey: 1 }), { constructor: TypeError, message: 'isKey must be a boolean, not a number' });
  assert.throws(() => stringSerializer().serialize({ topic: 't', headers: new Headers(), data: 5 }), {
    constructor: TypeError,
    message: 'stringSerializer: data must be a string or null, not a number',
  });
  // Java's StringSerializer has no close(): the producer runs Java's default.
  assert.equal(stringSerializer().close, undefined);
});
