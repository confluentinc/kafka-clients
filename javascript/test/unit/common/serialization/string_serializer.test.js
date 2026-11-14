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
// string, unconfigured and configured with each of Java's six standard
// charsets, and the aliases are the JDK's (Charset.aliases()). Java's
// SerializationTest cases of the string serde are translated for the
// serializer: the deserializer waits for the consumer, so a round trip is
// checked against Java's bytes.

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
  // Java: "my string".getBytes(UTF_8) and getBytes(UTF_16).
  for (const [encoding, hex] of [
    ['UTF-8', '6d7920737472696e67'],
    ['UTF-16', 'feff006d007900200073007400720069006e0067'],
  ]) {
    const serializer = stringSerializer({ configs: { 'key.serializer.encoding': encoding }, isKey: true });
    assert.equal(serialize(serializer, str), hex, encoding);
  }
});

// The strings, then each charset's bytes from Java's StringSerializer ('' for
// an empty array): ASCII, DEL, the first byte beyond ASCII, Latin-1 letters,
// the first character beyond Latin-1, BMP characters, a surrogate pair, lone
// and reversed surrogates, U+FFFF and a BOM character.
const STRINGS = ['', 'abc', '\u007f', '\u0080', '\u00e9', '\u00ff', '\u0100', '\u20ac', '\u65e5\u672c', '\uD83D\uDE00', 'a\uD800b', '\uDC00', '\uD800\uD800', '\uDBFF', 'x\uDFFFy\uD83D', '\uDC00\uD800', '\uFFFF', '\uFEFF'];
const CHARSETS = {
  'US-ASCII': '- 616263 7f 3f 3f 3f 3f 3f 3f3f 3f 613f62 3f 3f3f 3f 783f793f 3f3f 3f 3f',
  'ISO-8859-1': '- 616263 7f 80 e9 ff 3f 3f 3f3f 3f 613f62 3f 3f3f 3f 783f793f 3f3f 3f 3f',
  'UTF-8': '- 616263 7f c280 c3a9 c3bf c480 e282ac e697a5e69cac f09f9880 613f62 3f 3f3f 3f 783f793f 3f3f efbfbf efbbbf',
  'UTF-16BE': '- 006100620063 007f 0080 00e9 00ff 0100 20ac 65e5672c d83dde00 0061fffd0062 fffd fffdfffd fffd 0078fffd0079fffd fffdfffd ffff feff',
  'UTF-16LE': '- 610062006300 7f00 8000 e900 ff00 0001 ac20 e5652c67 3dd800de 6100fdff6200 fdff fdfffdff fdff 7800fdff7900fdff fdfffdff ffff fffe',
  'UTF-16':
    '- feff006100620063 feff007f feff0080 feff00e9 feff00ff feff0100 feff20ac feff65e5672c feffd83dde00 feff0061fffd0062 ' +
    'fefffffd fefffffdfffd fefffffd feff0078fffd0079fffd fefffffdfffd feffffff fefffeff',
};
// The JDK's aliases of each (Charset.aliases()).
const ALIASES = {
  'US-ASCII': ['646', 'ANSI_X3.4-1968', 'ANSI_X3.4-1986', 'ASCII', 'IBM367', 'ISO646-US', 'ISO_646.irv:1991', 'ascii7', 'cp367', 'csASCII', 'iso-ir-6', 'iso_646.irv:1983', 'us'],
  'ISO-8859-1': ['819', '8859_1', 'IBM-819', 'IBM819', 'ISO8859-1', 'ISO8859_1', 'ISO_8859-1', 'ISO_8859-1:1987', 'ISO_8859_1', 'cp819', 'csISOLatin1', 'iso-ir-100', 'l1', 'latin1'],
  'UTF-8': ['UTF8', 'unicode-1-1-utf-8'],
  'UTF-16BE': ['ISO-10646-UCS-2', 'UTF_16BE', 'UnicodeBigUnmarked', 'X-UTF-16BE'],
  'UTF-16LE': ['UTF_16LE', 'UnicodeLittleUnmarked', 'X-UTF-16LE'],
  'UTF-16': ['UTF_16', 'UnicodeBig', 'unicode', 'utf16'],
};

test("each of Java's standard charsets writes Java's bytes, under its name and aliases in any case", () => {
  for (const [charset, vector] of Object.entries(CHARSETS)) {
    const expected = vector.split(' ').map((h) => (h === '-' ? '' : h));
    assert.equal(expected.length, STRINGS.length, charset);
    for (const name of [charset, charset.toLowerCase(), ...ALIASES[charset], ...ALIASES[charset].map((a) => a.toUpperCase())]) {
      const serializer = stringSerializer({ configs: { 'value.serializer.encoding': name }, isKey: false });
      STRINGS.forEach((str, i) => assert.equal(serialize(serializer, str), expected[i], `${name}: ${JSON.stringify(str)}`));
      assert.equal(serialize(serializer, null), null, name);
    }
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
  // Java's key/value precedence, with two charsets that differ.
  const both = { 'key.serializer.encoding': 'UTF-16BE', 'serializer.encoding': 'UTF-8' };
  assert.equal(serialize(stringSerializer({ configs: both, isKey: true }), 'a'), '0061');
  assert.equal(serialize(stringSerializer({ configs: both, isKey: false }), 'a'), '61');
  // A value that is not a string is ignored, as Java ignores one that is no String.
  assert.equal(serialize(stringSerializer({ configs: { 'serializer.encoding': 5 }, isKey: true }), 'a'), '61');
  // A charset outside Java's standard six, an unknown name or an illegal one
  // is refused with Java's message (a deviation for a charset Java's runtime
  // has, such as windows-1252).
  for (const encoding of ['windows-1252', 'UTF-32', 'nope-charset', 'bad name!', ' UTF-8']) {
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
  // A Map's entries are no properties: refused rather than silently ignored.
  assert.throws(() => stringSerializer({ configs: new Map([['serializer.encoding', 'UTF-16']]), isKey: false }), {
    constructor: TypeError,
    message: 'configs must be a plain object, not a Map',
  });
  assert.throws(() => stringSerializer({ configs: {}, isKey: 1 }), { constructor: TypeError, message: 'isKey must be a boolean, not a number' });
  assert.throws(() => stringSerializer().serialize({ topic: 't', headers: new Headers(), data: 5 }), {
    constructor: TypeError,
    message: 'stringSerializer: data must be a string or null, not a number',
  });
  // Java's StringSerializer has no close(): the producer runs Java's default.
  assert.equal(stringSerializer().close, undefined);
});
