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

// lib/_config.ts, as python/test/unit/test_config.py, test_config_matrix.py
// and test_config_null.py check python/confluent_kafka/_config.py: every key of
// the generated producer table with a value of its ConfigDef type and of a
// wrong one, null against the key's validator, and the security checks the
// core makes.

const test = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

const { configEntries } = require('../../dist/_config');
const { PRODUCER, PRODUCER_NULL } = require('../../dist/_config_types');
const { ConfigError } = require('@confluentinc/kafka-javascript/common/config');
const { KafkaError } = require('@confluentinc/kafka-javascript/common');
const { KafkaProducer } = require('@confluentinc/kafka-javascript/producer');

test('config values are stringified', () => {
  assert.deepEqual(
    configEntries({
      'bootstrap.servers': 'localhost:9092',
      'enable.idempotence': true,
      'linger.ms': 5,
      'buffer.memory': 33554432n,
      'compression.type': 'lz4',
      'interceptor.classes': ['a', 'b'],
      'batch.size': '16384',
      'not.a.producer.key': false,
      'another.unknown': [1, 2],
    }),
    [
      ['bootstrap.servers', 'localhost:9092'],
      ['enable.idempotence', 'true'],
      ['linger.ms', '5'],
      ['buffer.memory', '33554432'],
      ['compression.type', 'lz4'],
      ['interceptor.classes', 'a,b'],
      ['batch.size', '16384'],
      ['not.a.producer.key', 'false'],
      ['another.unknown', '1,2'],
    ],
  );
});

test('undefined values, and null ones the validator accepts, are dropped', () => {
  assert.deepEqual(configEntries({ 'client.id': null, acks: undefined, 'linger.ms': 0 }), [['linger.ms', '0']]);
});

test("a Map is accepted, and its non-string keys rejected with Java's ConfigError", () => {
  assert.deepEqual(configEntries(new Map([['acks', 'all']])), [['acks', 'all']]);
  // Java's Utils.castToStringObjectMap, Python's prepare.
  assert.throws(() => configEntries(new Map([[1, 'x']])), (e) => {
    assert.ok(e instanceof ConfigError);
    assert.equal(e.message, 'Invalid value x for configuration 1: Key must be a string.');
    return true;
  });
  assert.throws(() => configEntries(new Map([[Symbol('k'), 'x']])), {
    name: 'ConfigError',
    message: 'Invalid value x for configuration Symbol(k): Key must be a string.',
  });
  assert.throws(() => new KafkaProducer(new Map([['bootstrap.servers', 'localhost:9092'], [1, 'x']])), {
    name: 'ConfigError',
    message: 'Invalid value x for configuration 1: Key must be a string.',
  });
  // Every key is checked before any value, as Java and Python do: the bad
  // key is reported, not the earlier key's bad value.
  assert.throws(() => configEntries(new Map([['linger.ms', true], [1, 'x']])), {
    name: 'ConfigError',
    message: 'Invalid value x for configuration 1: Key must be a string.',
  });
});

test('values of an unknown key of other types are rejected', () => {
  assert.throws(() => configEntries({ 'not.a.key': {} }), {
    name: 'TypeError',
    message: 'config not.a.key must be a string, a number, a boolean or an array of them, not object',
  });
  assert.throws(() => configEntries({ 'not.a.key': () => 1 }), { name: 'TypeError' });
  assert.throws(() => configEntries(null), { name: 'TypeError', message: 'configs must be an object or a Map, not null' });
  assert.throws(() => configEntries([]), { name: 'TypeError', message: 'configs must be an object or a Map, not array' });
});

// A value of each ConfigDef type, and the text the core parses for it.
const GOOD = {
  INT: [7, '7'],
  LONG: [7n, '7'],
  SHORT: [7, '7'],
  DOUBLE: [1.5, '1.5'],
  STRING: ['x', 'x'],
  PASSWORD: ['secret', 'secret'],
  BOOLEAN: [true, 'true'],
  LIST: [['a', 'b'], 'a,b'],
  CLASS: ['org.example.Cls', 'org.example.Cls'],
};

// A value of the wrong JavaScript type for each type, as Java's message shows
// it, and Java's ConfigDef.parseType message. A password is never shown.
const BAD = {
  INT: [42.5, '42.5', 'Expected value to be a 32-bit integer, but it was a number'],
  LONG: [{}, '[object Object]', 'Expected value to be a 64-bit integer (long), but it was a object'],
  SHORT: [1.5, '1.5', 'Expected value to be a 16-bit integer (short), but it was a number'],
  DOUBLE: [true, 'true', 'Expected value to be a double, but it was a boolean'],
  STRING: [5, '5', 'Expected value to be a string, but it was a number'],
  PASSWORD: [5, '[hidden]', 'Expected value to be a string, but it was a number'],
  BOOLEAN: [1, '1', 'Expected value to be either true or false'],
  LIST: [53, '53', 'Expected a comma separated list.'],
  CLASS: [5, '5', 'Expected a Class instance or class name.'],
};

test('the producer table covers every type here, SSL and SASL keys included', () => {
  const types = new Set(PRODUCER.values());
  assert.deepEqual([...types].sort(), Object.keys(GOOD).sort());
  assert.ok(PRODUCER.size > 100);
  for (const key of ['ssl.keystore.password', 'ssl.key.password', 'ssl.truststore.password', 'ssl.keystore.key', 'sasl.jaas.config']) {
    assert.equal(PRODUCER.get(key), 'PASSWORD', key);
  }
  assert.equal(PRODUCER.get('security.protocol'), 'STRING');
  assert.equal(PRODUCER.get('ssl.enabled.protocols'), 'LIST');
  assert.ok(!PRODUCER.has('max.poll.records'));
});

test('a value of the key type reaches the core, for every producer key', () => {
  for (const [key, type] of PRODUCER) {
    const [value, text] = GOOD[type];
    assert.deepEqual(configEntries({ [key]: value }), [[key, text]], key);
    // A string is the core's to parse, for every type.
    assert.deepEqual(configEntries({ [key]: 'text' }), [[key, 'text']], key);
  }
});

test('a value of the wrong type is a ConfigError with Java\'s message, for every producer key', () => {
  for (const [key, type] of PRODUCER) {
    const [value, shown, message] = BAD[type];
    assert.throws(
      () => configEntries({ [key]: value }),
      (e) => {
        assert.ok(e instanceof ConfigError, key);
        assert.ok(e instanceof KafkaError, key);
        assert.equal(e.message, `Invalid value ${shown} for configuration ${key}: ${message}`, key);
        assert.equal(e.cause, undefined);
        return true;
      },
      key,
    );
  }
});

test('a password of the wrong type is not echoed', () => {
  assert.throws(() => configEntries({ 'sasl.jaas.config': ['user', 'pass-word'] }), (e) => {
    assert.equal(
      e.message,
      'Invalid value [hidden] for configuration sasl.jaas.config: Expected value to be a string, but it was a array',
    );
    assert.ok(!e.message.includes('pass-word'));
    return true;
  });
});

test('null follows the key validator, for every producer key', () => {
  for (const key of PRODUCER.keys()) {
    const refused = PRODUCER_NULL.get(key);
    if (refused === undefined) {
      assert.deepEqual(configEntries({ [key]: null }), [], key);
      continue;
    }
    const [form, message] = refused;
    assert.throws(
      () => configEntries({ [key]: null }),
      { name: 'ConfigError', message: form === 'named' ? `Invalid value null for configuration ${key}: ${message}` : message },
      key,
    );
  }
});

const refused = (key) => {
  try {
    configEntries({ [key]: null });
  } catch (e) {
    assert.ok(e instanceof ConfigError);
    return e.message;
  }
  assert.fail(`${key}: null was accepted`);
};

test('null: a Range validator, the security keys included', () => {
  for (const key of ['linger.ms', 'max.in.flight.requests.per.connection', 'sasl.login.refresh.window.factor',
    'compression.gzip.level', 'compression.zstd.level']) {
    assert.equal(refused(key), `Invalid value null for configuration ${key}: Value must be non-null`);
  }
});

test('null: a ValidString validator', () => {
  assert.equal(refused('acks'), 'Invalid value null for configuration acks: String must be one of: all, -1, 0, 1');
  assert.equal(
    refused('compression.type'),
    'Invalid value null for configuration compression.type: String must be one of: none, gzip, snappy, lz4, zstd',
  );
});

test('null: a CaseInsensitiveValidString validator, security.protocol included', () => {
  // Java joins the upper-cased values of a HashSet, in its iteration order.
  for (const [key, valid] of [
    ['security.protocol', 'SASL_SSL, PLAINTEXT, SSL, SASL_PLAINTEXT'],
    ['metadata.recovery.strategy', 'REBOOTSTRAP, NONE'],
    ['buffer.memory.allocation.strategy', 'FULL, INCREMENTAL'],
  ]) {
    assert.equal(
      refused(key),
      `Invalid value null for configuration ${key}: String must be one of (case insensitive): ${valid}`,
    );
  }
});

test('null: a ValidList refusing null', () => {
  assert.equal(refused('bootstrap.servers'), "Configuration 'bootstrap.servers' values must not be null.");
});

test('null allowed: the key is not set', () => {
  for (const key of ['transactional.id', 'client.id', 'sasl.jaas.config', 'ssl.keystore.password', 'key.serializer']) {
    assert.deepEqual(configEntries({ [key]: null }), [], key);
  }
});

test('the producer refuses a null its validator rejects before the core sees the configs', () => {
  assert.throws(() => new KafkaProducer({ 'bootstrap.servers': 'localhost:1', acks: null }), {
    name: 'ConfigError',
    message: 'Invalid value null for configuration acks: String must be one of: all, -1, 0, 1',
  });
});

// One key of every type, SSL and SASL keys included; the values are in Java's
// validators' ranges.
const TYPED_KEYS = {
  'request.timeout.ms': 30000, // INT
  'metadata.max.age.ms': 300000, // LONG
  'sasl.login.refresh.min.period.seconds': 60, // SHORT
  'sasl.login.refresh.window.factor': 0.8, // DOUBLE
  'sasl.mechanism': 'GSSAPI', // STRING
  'ssl.keystore.password': 'keystore-secret', // PASSWORD
  'ssl.enabled.protocols': ['TLSv1.2', 'TLSv1.3'], // LIST
  'client.id': 'typed-keys',
};

test('the producer takes a typed key of every type', async () => {
  const producer = new KafkaProducer({ 'bootstrap.servers': 'localhost:1', 'enable.idempotence': true, ...TYPED_KEYS });
  await producer.close();
});

test('an unsupported SASL mechanism is rejected by the core', () => {
  // PLAIN is the only SASL mechanism the core implements: under a SASL
  // security.protocol any other one, Java's GSSAPI default included, fails
  // the producer's construction with the core's ConfigError.
  assert.throws(
    () =>
      new KafkaProducer({
        'bootstrap.servers': 'localhost:1',
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'SCRAM-SHA-256',
      }),
    (e) => {
      assert.ok(e instanceof ConfigError);
      assert.equal(e.errorName, 'CONFIG');
      assert.equal(e.message, 'Invalid value SCRAM-SHA-256 for configuration sasl.mechanism: only PLAIN is supported by this client');
      return true;
    },
  );
  assert.throws(() => new KafkaProducer({ 'bootstrap.servers': 'localhost:1', 'security.protocol': 'SASL_SSL' }), {
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
    new KafkaProducer({ 'bootstrap.servers': 'localhost:1', 'security.protocol': 'PLAINTEXT', 'ssl.keystore.password': 'p' })
      .close()
      .then(() => console.log('closed'));
  `;
  const r = spawnSync(process.execPath, ['-e', script], {
    cwd: path.join(__dirname, '..', '..'),
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
