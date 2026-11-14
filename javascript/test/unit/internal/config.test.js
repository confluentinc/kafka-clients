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

// src/internal/config.ts: the binding has no ConfigDef table and checks no
// value (root CLAUDE.md, JavaScript Binding Conventions, Configuration). It
// writes every value as the string the FFI takes, drops null and undefined,
// and passes every key to the core, which checks them with Java's messages.

const test = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

const { configEntries: entriesAsBytes, decimal } = require('../../../dist/internal/config');
const { ConfigError } = require('@confluentinc/kafka-javascript/common/config');
const { KafkaError } = require('@confluentinc/kafka-javascript/common');
const { KafkaProducer } = require('@confluentinc/kafka-javascript/producer');

/** The `[key, value]` pairs as strings: configEntries hands the addon encodeLikeJava's bytes. */
function configEntries(configs) {
  return entriesAsBytes(configs).map(([key, value]) => {
    assert.ok(Buffer.isBuffer(key) && Buffer.isBuffer(value));
    return [key.toString(), value.toString()];
  });
}

test('every value is written as a string: booleans, numbers, bigints, arrays and anything else', () => {
  assert.deepEqual(
    configEntries({
      'bootstrap.servers': 'localhost:9092',
      'enable.idempotence': true,
      'linger.ms': 5,
      'buffer.memory': 33554432n,
      // A long above 2^53 goes as its exact digits, for the core to check.
      'metadata.max.age.ms': 2n ** 62n,
      'compression.type': 'lz4',
      'interceptor.classes': ['a', 'b'],
      'batch.size': '16384',
      'not.a.producer.key': false,
      'another.unknown': 7,
      'a.bigint': 5n,
      'an.array': [1, 2n, 'x'],
      'an.object': {},
      'a.function': () => 1,
      kafkaJS: { brokers: ['localhost:9092'] },
    }),
    [
      ['bootstrap.servers', 'localhost:9092'],
      ['enable.idempotence', 'true'],
      ['linger.ms', '5'],
      ['buffer.memory', '33554432'],
      ['metadata.max.age.ms', '4611686018427387904'],
      ['compression.type', 'lz4'],
      ['interceptor.classes', 'a,b'],
      ['batch.size', '16384'],
      ['not.a.producer.key', 'false'],
      ['another.unknown', '7'],
      ['a.bigint', '5'],
      ['an.array', '1,2,x'],
      ['an.object', '[object Object]'],
      ['a.function', '() => 1'],
      ['kafkaJS', '[object Object]'],
    ],
  );
});

test('null and undefined leave the key unset, whatever its validator', () => {
  assert.deepEqual(
    configEntries({ 'client.id': null, acks: undefined, 'bootstrap.servers': null, 'compression.type': null, 'linger.ms': 0 }),
    [['linger.ms', '0']],
  );
});

test("configs is a plain object: a Map is refused, and a symbol key rejected with Java's ConfigError", () => {
  // Java's Map and Properties forms are one plain object: a Map's entries
  // aren't object keys, so it would otherwise be read as an object with none.
  for (const configs of [new Map([['acks', 'all']]), new Map()]) {
    assert.throws(() => configEntries(configs), { name: 'TypeError', message: 'configs must be a plain object, not a Map' });
  }
  assert.throws(() => configEntries(new (class Configs {})()), {
    name: 'TypeError',
    message: 'configs must be a plain object, not a class instance',
  });
  assert.throws(() => new KafkaProducer({ configs: new Map([['bootstrap.servers', 'localhost:9092']]) }), {
    name: 'TypeError',
    message: 'configs must be a plain object, not a Map',
  });
  // An object without a prototype is a plain one.
  const bare = Object.create(null);
  bare.acks = 'all';
  assert.deepEqual(configEntries(bare), [['acks', 'all']]);
  // Java's Utils.castToStringObjectMap: a key that is not a string (a symbol)
  // is a ConfigError, checked before any value.
  assert.throws(() => configEntries({ [Symbol('k')]: 'x' }), {
    name: 'ConfigError',
    message: 'Invalid value x for configuration Symbol(k): Key must be a string.',
  });
  assert.throws(() => configEntries({ 'linger.ms': true, [Symbol('k')]: 'x' }), (e) => {
    assert.ok(e instanceof ConfigError);
    assert.equal(e.message, 'Invalid value x for configuration Symbol(k): Key must be a string.');
    return true;
  });
  assert.throws(() => configEntries(null), { name: 'TypeError', message: 'configs must not be null' });
  assert.throws(() => configEntries([]), { name: 'TypeError', message: 'configs must be a plain object, not array' });
});

test('numbers are written in decimal, never in exponent form', () => {
  assert.deepEqual(configEntries({ 'metadata.max.age.ms': 1e21, 'not.a.key': 1e-7, 'other.key': -2.5e-8 }), [
    ['metadata.max.age.ms', '1000000000000000000000'],
    ['not.a.key', '0.0000001'],
    ['other.key', '-0.000000025'],
  ]);
  for (const [n, text] of [
    [0, '0'],
    [123.5, '123.5'],
    [1.25e22, '12500000000000000000000'],
    [1.5e-10, '0.00000000015'],
    [-1e21, '-1000000000000000000000'],
    [NaN, 'NaN'],
  ]) {
    assert.equal(decimal(n), text, String(n));
  }
});

test('key.serializer and value.serializer are refused, whatever their value, pointing to the constructor arguments', () => {
  for (const [key, argument] of [
    ['key.serializer', 'keySerializer'],
    ['value.serializer', 'valueSerializer'],
  ]) {
    for (const value of ['org.apache.kafka.common.serialization.StringSerializer', null, 5]) {
      assert.throws(() => configEntries({ [key]: value }), (e) => {
        assert.ok(e instanceof ConfigError, key);
        assert.equal(
          e.message,
          `${key} is not a configuration of this client: a serializer is the ${argument} argument of the constructor, never a configuration value`,
        );
        return true;
      });
    }
    // undefined leaves the key out, as for every key.
    assert.deepEqual(configEntries({ [key]: undefined }), []);
  }
});

test("the core checks the values, with Java's messages naming the key", () => {
  for (const [configs, message] of [
    [{ 'linger.ms': true }, 'Invalid value true for configuration linger.ms: Not a number of type LONG'],
    [{ 'batch.size': 42.5 }, 'Invalid value 42.5 for configuration batch.size: Not a number of type INT'],
    [{ 'linger.ms': {} }, 'Invalid value [object Object] for configuration linger.ms: Not a number of type LONG'],
    [{ 'linger.ms': 2n ** 63n }, 'Invalid value 9223372036854775808 for configuration linger.ms: Not a number of type LONG'],
    [{ 'enable.idempotence': 1 }, 'Invalid value 1 for configuration enable.idempotence: Expected value to be either true or false'],
    [{ acks: 5 }, 'Invalid value 5 for configuration acks: String must be one of: all, -1, 0, 1'],
  ]) {
    assert.throws(
      () => new KafkaProducer({ configs: { 'bootstrap.servers': 'localhost:1', ...configs } }),
      (e) => {
        assert.ok(e instanceof ConfigError);
        assert.ok(e instanceof KafkaError);
        assert.equal(e.message, message);
        return true;
      },
      message,
    );
  }
});

test('an unknown key, a kafkaJS block included, reaches the core, which builds the producer', async () => {
  const producer = new KafkaProducer({
    configs: { 'bootstrap.servers': 'localhost:1', 'not.a.key': 'x', kafkaJS: { brokers: ['localhost:9092'] } },
  });
  await producer.close();
});

// One key of every type, SSL and SASL keys included; the values are in Java's
// validators' ranges.
const TYPED_KEYS = {
  'request.timeout.ms': 30000, // INT
  'metadata.max.age.ms': 300000n, // LONG
  'sasl.login.refresh.min.period.seconds': 60, // SHORT
  'sasl.login.refresh.window.factor': 0.8, // DOUBLE
  'sasl.mechanism': 'GSSAPI', // STRING
  'ssl.keystore.password': 'keystore-secret', // PASSWORD
  'ssl.enabled.protocols': ['TLSv1.2', 'TLSv1.3'], // LIST
  'client.id': 'typed-keys',
};

test('the producer takes a typed key of every type', async () => {
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': 'localhost:1', 'enable.idempotence': true, ...TYPED_KEYS } });
  await producer.close();
});

test('an unsupported SASL mechanism is rejected by the core', () => {
  // PLAIN is the only SASL mechanism the core implements: under a SASL
  // security.protocol any other one, Java's GSSAPI default included, fails
  // the producer's construction with the core's ConfigError.
  assert.throws(
    () =>
      new KafkaProducer({
        configs: {
          'bootstrap.servers': 'localhost:1',
          'security.protocol': 'SASL_PLAINTEXT',
          'sasl.mechanism': 'SCRAM-SHA-256',
        },
      }),
    (e) => {
      assert.ok(e instanceof ConfigError);
      assert.equal(e.name, 'ConfigError');
      assert.equal(e.message, 'Invalid value SCRAM-SHA-256 for configuration sasl.mechanism: only PLAIN is supported by this client');
      return true;
    },
  );
  assert.throws(() => new KafkaProducer({ configs: { 'bootstrap.servers': 'localhost:1', 'security.protocol': 'SASL_SSL' } }), {
    name: 'ConfigError',
    message: 'Invalid value GSSAPI for configuration sasl.mechanism: only PLAIN is supported by this client',
  });
});

test('an undefined SSL password is reported as unused', () => {
  // The core leaves ssl.keystore.password undefined (PEM is its only store
  // format, and Java rejects a store password with PEM), so a value set for
  // it is listed in the client's unused-configurations INFO line. The core
  // logs to stderr at the RUST_LOG level read when its first client is built,
  // so a fresh process builds the producer.
  const script = `
    const { KafkaProducer } = require('@confluentinc/kafka-javascript/producer');
    new KafkaProducer({ configs: { 'bootstrap.servers': 'localhost:1', 'security.protocol': 'PLAINTEXT', 'ssl.keystore.password': 'p' } })
      .close()
      .then(() => console.log('closed'));
  `;
  const r = spawnSync(process.execPath, ['-e', script], {
    cwd: path.join(__dirname, '..', '..', '..'),
    encoding: 'utf8',
    timeout: 120000,
    env: { ...process.env, RUST_LOG: 'info' },
  });
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'closed\n');
  assert.ok(
    r.stderr.includes("These configurations '[ssl.keystore.password]' were supplied but are not used yet."),
    r.stderr,
  );
});
