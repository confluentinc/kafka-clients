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

// The package's built-in serde classes (javascript/CLAUDE.md, JavaScript
// Binding Conventions, Serialization). First Java's SerializationTest
// (org.apache.kafka.common.serialization): Java's Serdes is not translated,
// so each case builds the serializer and the deserializer of its type with
// `new`. Then each of the twenty classes against Java's own bytes and
// messages: test/java_vectors.json holds what kafka-clients' serializers
// write for each value and what its deserializers read from each byte string,
// as the consumer calls them, deserialize(topic, headers, ByteBuffer)
// (tools/java-vectors/JavaVectors.java); and the checks Java's types make and
// JavaScript's do not: a value of another type, and a number beyond the Java
// type.
//
// Left out, with their reasons:
// - the 19 listSerde* cases (listSerdeShouldReturnEmptyCollection to
//   listSerdeShouldReturnStack): the List serdes are left out, since they read
//   class names in configure(), which JavaScript cannot load;
// - testSerdeFromUnknown and testSerdeFromNotNull: Serdes is not translated;
// - stringSerdeShouldSupportDifferentEncodings and
//   stringSerdeConfigureThrowsOnUnknownEncoding: the String serdes are UTF-8
//   only, with no configure();
// - stringDeserializerSupportByteBuffer: the ByteBuffer serdes are left out,
//   and a deserializer always gets a Buffer.
// The ByteBuffer and Bytes rows of Java's testData go for the same reasons.

const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { SerializationError } = require('@confluentinc/kafka-javascript/common/errors');
const { Headers } = require('@confluentinc/kafka-javascript/common/header');
const S = require('@confluentinc/kafka-javascript/common/serialization');

const topic = 'testTopic';

/**
 * Java's testData, by type: each serde pair and the values it round-trips.
 * A Long is a bigint; a Float is a number a float holds exactly, as Java's
 * float literal is; a UUID its canonical text; a byte[] a Buffer.
 *
 * @type {Array<[string, { serialize(o: object): unknown }, { deserialize(o: object): unknown }, unknown[]]>}
 */
const testData = [
  ['String', new S.StringSerializer(), new S.StringDeserializer(), [null, 'my string']],
  ['Short', new S.ShortSerializer(), new S.ShortDeserializer(), [null, 32767, -32768]],
  ['Integer', new S.IntegerSerializer(), new S.IntegerDeserializer(), [null, 423412424, -41243432]],
  ['Long', new S.LongSerializer(), new S.LongDeserializer(), [null, 922337203685477580n, -922337203685477581n]],
  ['Float', new S.FloatSerializer(), new S.FloatDeserializer(), [null, Math.fround(5678567.12312), Math.fround(-5678567.12341)]],
  ['Double', new S.DoubleSerializer(), new S.DoubleDeserializer(), [null, 5678567.12312, -5678567.12341]],
  ['byte[]', new S.ByteArraySerializer(), new S.ByteArrayDeserializer(), [null, Buffer.from('my string')]],
  ['UUID', new S.UUIDSerializer(), new S.UUIDDeserializer(), [null, crypto.randomUUID()]],
];

/**
 * @param {{ serialize(o: object): unknown }} serializer
 * @param {unknown} data
 * @returns {Buffer | null}
 */
function serialize(serializer, data) {
  const out = serializer.serialize({ topic, headers: new Headers(), data });
  assert.ok(out === null || out instanceof Uint8Array, 'a built-in serializer answers at once');
  return out === null ? null : Buffer.from(/** @type {Uint8Array} */ (out));
}

/**
 * @param {{ deserialize(o: object): unknown }} deserializer
 * @param {Buffer | null} data
 * @returns {unknown}
 */
function deserialize(deserializer, data) {
  return deserializer.deserialize({ topic, headers: new Headers(), data });
}

test('allSerdesShouldRoundtripInput', () => {
  for (const [type, serializer, deserializer, values] of testData) {
    for (const value of values) {
      const serialized = serialize(serializer, value);
      // Java's deserialize(topic, data) and deserialize(topic, headers,
      // ByteBuffer) are the one deserialize({ topic, headers, data }).
      assert.deepEqual(
        deserialize(deserializer, serialized),
        value,
        `Should get the original ${type} after serialization and deserialization`,
      );
    }
  }
});

test('allSerdesShouldSupportNull', () => {
  for (const [type, serializer, deserializer] of testData) {
    assert.equal(serialize(serializer, null), null, `Should support null in ${type} serialization`);
    assert.equal(deserialize(deserializer, null), null, `Should support null in ${type} deserialization`);
  }
});

test('floatDeserializerShouldThrowSerializationExceptionOnZeroBytes', () => {
  assert.throws(() => deserialize(new S.FloatDeserializer(), Buffer.alloc(0)), SerializationError);
});

test('floatDeserializerShouldThrowSerializationExceptionOnTooFewBytes', () => {
  assert.throws(() => deserialize(new S.FloatDeserializer(), Buffer.alloc(3)), SerializationError);
});

test('floatDeserializerShouldThrowSerializationExceptionOnTooManyBytes', () => {
  assert.throws(() => deserialize(new S.FloatDeserializer(), Buffer.alloc(5)), SerializationError);
});

test('floatSerdeShouldPreserveNaNValues', () => {
  // Java's NaNs 0x7f800001 and 0x7f800002 are signaling ones, which reading
  // them into a JavaScript number turns quiet (0x7fc00001, 0x7fc00002): a
  // number cannot hold a float's signaling NaN *(deviation, recorded in
  // NOTES.md)*. The payload a number holds is kept, so the quiet NaNs with
  // the same payloads round-trip bit for bit, and every NaN stays a NaN.
  const serializer = new S.FloatSerializer();
  const deserializer = new S.FloatDeserializer();
  for (const [given, kept] of [
    ['7f800001', '7fc00001'],
    ['7f800002', '7fc00002'],
    ['7fc00001', '7fc00001'],
    ['7fc00002', '7fc00002'],
  ]) {
    const someNaN = Buffer.from(given, 'hex').readFloatBE(0);
    const roundtrip = /** @type {number} */ (deserialize(deserializer, serialize(serializer, someNaN)));
    assert.ok(Number.isNaN(roundtrip), given);
    const bits = Buffer.alloc(4);
    bits.writeFloatBE(roundtrip);
    assert.equal(bits.toString('hex'), kept, given);
  }
});

test('testSerializeVoid', () => {
  assert.equal(serialize(new S.VoidSerializer(), null), null);
});

test('testDeserializeVoid', () => {
  assert.equal(deserialize(new S.VoidDeserializer(), null), null);
});

test('voidDeserializerShouldThrowOnNotNullValues', () => {
  assert.throws(() => deserialize(new S.VoidDeserializer(), Buffer.alloc(5)), IllegalArgumentError);
});

test('testBooleanSerializer', () => {
  // Java's @ParameterizedTest over true and false.
  for (const dataToSerialize of [true, false]) {
    assert.deepEqual(serialize(new S.BooleanSerializer(), dataToSerialize), Buffer.of(dataToSerialize ? 1 : 0));
  }
});

test('testBooleanDeserializer', () => {
  // Java's @ParameterizedTest over true and false.
  for (const dataToDeserialize of [true, false]) {
    assert.equal(deserialize(new S.BooleanDeserializer(), Buffer.of(dataToDeserialize ? 1 : 0)), dataToDeserialize);
  }
});

test('booleanDeserializerShouldThrowOnEmptyInput', () => {
  assert.throws(() => deserialize(new S.BooleanDeserializer(), Buffer.alloc(0)), SerializationError);
});

// ---- each class against Java's vectors, and the checks of its types --------

const vectors = require('../../../java_vectors.json');

const NAMES = ['String', 'ByteArray', 'Short', 'Integer', 'Long', 'Float', 'Double', 'Boolean', 'UUID', 'Void'];

/** @type {Record<string, new () => any>} */
const CLASSES = /** @type {any} */ (S);

/**
 * The value a vector's `data` or `value` stands for, by class: a Long's
 * decimal string as a bigint, a Float's and a Double's raw bits as the
 * number, a byte[]'s hex as a Buffer.
 *
 * @param {string} cls
 * @param {unknown} json
 * @returns {unknown}
 */
function valueOf(cls, json) {
  if (json === null) return null;
  if (cls.startsWith('Long')) return BigInt(/** @type {string} */ (json));
  if (cls.startsWith('Float')) return Buffer.from(/** @type {string} */ (json), 'hex').readFloatBE(0);
  if (cls.startsWith('Double')) return Buffer.from(/** @type {string} */ (json), 'hex').readDoubleBE(0);
  if (cls.startsWith('ByteArray')) return Buffer.from(/** @type {string} */ (json), 'hex');
  return json;
}

/**
 * Whether a Float vector stands for a signaling NaN, which a JavaScript number
 * cannot hold: reading it turns it quiet (floatSerdeShouldPreserveNaNValues).
 *
 * @param {string} cls
 * @param {unknown} json
 * @returns {boolean}
 */
function signalingNaN(cls, json) {
  if (!cls.startsWith('Float') || typeof json !== 'string') return false;
  const bits = Number.parseInt(json, 16);
  return (bits & 0x7f800000) === 0x7f800000 && (bits & 0x7fffff) !== 0 && (bits & 0x400000) === 0;
}

test('the twenty classes are exported, built with new, and take no argument', () => {
  for (const name of NAMES) {
    for (const role of ['Serializer', 'Deserializer']) {
      const cls = CLASSES[`${name}${role}`];
      assert.equal(typeof cls, 'function', `${name}${role}`);
      const serde = new cls();
      assert.equal(serde.constructor.name, `${name}${role}`);
      // No configure(): UTF-8 only, and Java does not call it for a serde given ready.
      assert.equal(/** @type {any} */ (serde).configure, undefined, `${name}${role}`);
      // Java's default close(), which the client runs for a serde without one.
      assert.equal(/** @type {any} */ (serde).close, undefined, `${name}${role}`);
    }
  }
});

test("each serializer writes Java's bytes for each value", () => {
  for (const { class: cls, data, bytes } of vectors.serializers) {
    const serializer = new CLASSES[cls]();
    const value = valueOf(cls, data);
    const out = serializer.serialize({ topic: 't', headers: new Headers(), data: value });
    if (signalingNaN(cls, data)) {
      // A quiet NaN with the same payload: the number read turned it quiet.
      assert.ok(Number.isNaN(Buffer.from(out).readFloatBE(0)), `${cls} ${String(data)}`);
      continue;
    }
    assert.equal(out === null ? null : Buffer.from(out).toString('hex'), bytes, `${cls} ${String(data)}`);
  }
});

test("each deserializer reads Java's value, or throws Java's error, for each byte string", () => {
  for (const row of vectors.deserializers) {
    const { class: cls, bytes } = row;
    const deserializer = new CLASSES[cls]();
    const call = () => deserializer.deserialize({ topic: 't', headers: new Headers(), data: Buffer.from(bytes, 'hex') });
    if ('error' in row) {
      const error = /** @type {{ class: string, message: string }} */ (row.error);
      const expected = error.class === 'IllegalArgumentException' ? IllegalArgumentError : SerializationError;
      assert.equal(error.class === 'IllegalArgumentException' || error.class === 'SerializationException', true, error.class);
      assert.throws(call, (/** @type {Error} */ e) => {
        assert.ok(e instanceof expected, `${cls} ${bytes}`);
        assert.equal(e.message, error.message, `${cls} ${bytes}`);
        return true;
      });
      continue;
    }
    const got = call();
    if (cls === 'FloatDeserializer' && signalingNaN(cls, row.value)) {
      assert.ok(Number.isNaN(got), `${cls} ${bytes}`);
    } else if (cls === 'FloatDeserializer' || cls === 'DoubleDeserializer') {
      const out = Buffer.alloc(cls === 'FloatDeserializer' ? 4 : 8);
      if (cls === 'FloatDeserializer') out.writeFloatBE(/** @type {number} */ (got));
      else out.writeDoubleBE(/** @type {number} */ (got));
      assert.equal(out.toString('hex'), row.value, `${cls} ${bytes}`);
    } else {
      assert.deepEqual(got, valueOf(cls, row.value), `${cls} ${bytes}`);
    }
  }
});

test('a deserializer answers at once, its result no Promise, and ByteArrayDeserializer returns the very Buffer given', () => {
  const data = Buffer.from('my string');
  const out = new S.ByteArrayDeserializer().deserialize({ topic: 't', headers: new Headers(), data });
  assert.equal(out, data);
  for (const name of NAMES) {
    const out = new CLASSES[`${name}Deserializer`]().deserialize({ topic: 't', headers: new Headers(), data: null });
    assert.equal(out, null, name);
  }
});

test('ByteArraySerializer returns the very bytes given, not copied', () => {
  const data = new Uint8Array([1, 2, 3]);
  assert.equal(new S.ByteArraySerializer().serialize({ topic: 't', headers: new Headers(), data }), data);
});

test('a serializer given a value of another type throws SerializationError, naming the JavaScript types', () => {
  for (const [name, wrong, takes] of [
    ['String', 5, 'a string or null, not a number'],
    ['ByteArray', 'abc', 'a Uint8Array or null, not a string'],
    ['Short', 5n, 'a number or null, not a bigint'],
    ['Integer', '5', 'a number or null, not a string'],
    ['Long', '5', 'a bigint, a number or null, not a string'],
    ['Float', 5n, 'a number or null, not a bigint'],
    ['Double', true, 'a number or null, not a boolean'],
    ['Boolean', 1, 'a boolean or null, not a number'],
    ['UUID', Buffer.alloc(16), 'a string or null, not a Buffer'],
    ['Void', 0, 'only null, not a number'],
  ]) {
    const serializer = new CLASSES[`${name}Serializer`]();
    assert.throws(
      () => serializer.serialize({ topic: 't', headers: new Headers(), data: wrong }),
      (/** @type {Error} */ e) => {
        assert.ok(e instanceof SerializationError, String(name));
        assert.equal(e.message, `${String(name)}Serializer takes ${String(takes)}`);
        return true;
      },
    );
  }
});

test('a number beyond the Java type throws RangeError, a long beyond 2^53 included', () => {
  const at = (/** @type {string} */ name, /** @type {unknown} */ data) =>
    new CLASSES[`${name}Serializer`]().serialize({ topic: 't', headers: new Headers(), data });
  for (const [name, data, message] of [
    ['Short', 32768, 'ShortSerializer takes a whole number from -32768 to 32767, not 32768'],
    ['Short', -32769, 'ShortSerializer takes a whole number from -32768 to 32767, not -32769'],
    ['Short', 1.5, 'ShortSerializer takes a whole number from -32768 to 32767, not 1.5'],
    ['Integer', 2 ** 31, 'IntegerSerializer takes a whole number from -2147483648 to 2147483647, not 2147483648'],
    ['Integer', Number.NaN, 'IntegerSerializer takes a whole number from -2147483648 to 2147483647, not NaN'],
    ['Long', 2 ** 53, 'LongSerializer takes a whole number up to 2^53 - 1, or a bigint, not 9007199254740992'],
    ['Long', 0.5, 'LongSerializer takes a whole number up to 2^53 - 1, or a bigint, not 0.5'],
    ['Long', 2n ** 63n, 'LongSerializer takes a bigint from -9223372036854775808 to 9223372036854775807, not 9223372036854775808'],
    ['Long', -(2n ** 63n) - 1n, 'LongSerializer takes a bigint from -9223372036854775808 to 9223372036854775807, not -9223372036854775809'],
  ]) {
    assert.throws(() => at(/** @type {string} */ (name), data), { constructor: RangeError, message: /** @type {string} */ (message) });
  }
  // The edges are taken: a whole number up to 2^53 - 1, and a bigint up to Long.MAX_VALUE.
  assert.equal(Buffer.from(at('Long', 2 ** 53 - 1)).toString('hex'), '001fffffffffffff');
  assert.equal(Buffer.from(at('Long', 2n ** 63n - 1n)).toString('hex'), '7fffffffffffffff');
  assert.equal(Buffer.from(at('Short', -32768)).toString('hex'), '8000');
});

test("a UUID is its canonical text, in either case, written in lowercase; another text is Java's parse error", () => {
  const serializer = new S.UUIDSerializer();
  for (const text of ['not-a-uuid', '1-2-3-4-5', '', '123e4567e89b12d3a456426614174000', '{123e4567-e89b-12d3-a456-426614174000}']) {
    assert.throws(() => serializer.serialize({ topic: 't', headers: new Headers(), data: text }), {
      constructor: SerializationError,
      message: 'Error parsing data into UUID',
    });
  }
  const uuid = crypto.randomUUID();
  const out = serializer.serialize({ topic: 't', headers: new Headers(), data: uuid.toUpperCase() });
  assert.equal(Buffer.from(/** @type {Uint8Array} */ (out)).toString(), uuid);
});

test('a deserializer given no Buffer throws TypeError', () => {
  for (const name of NAMES.filter((n) => n !== 'Void')) {
    assert.throws(() => new CLASSES[`${name}Deserializer`]().deserialize({ topic: 't', headers: new Headers(), data: 'abc' }), {
      constructor: TypeError,
      message: `${name}Deserializer takes a Buffer or null, not a string`,
    });
  }
  // A Uint8Array is read over the same memory.
  const bytes = new Uint8Array([0, 0, 0, 7]);
  assert.equal(new S.IntegerDeserializer().deserialize({ topic: 't', headers: new Headers(), data: /** @type {Buffer} */ (bytes) }), 7);
});
