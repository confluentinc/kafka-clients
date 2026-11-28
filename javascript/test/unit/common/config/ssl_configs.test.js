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

// SslConfigs, SaslConfigs and SecurityConfig, as
// python/test/unit/test_config_classes.py checks the Python ones.
//
// Java's SaslConfigsTest (Apache Kafka 4.4.0), 11 tests. Each parses a
// ConfigDef built by SaslConfigs.addClientSaslSupport(ConfigDef), which is not
// translated (JS conventions, Scope: the ConfigDef machinery), but the core's
// ProducerConfig defines the same four keys with the same ranges
// (with_client_sasl_support), so they are ported through the KafkaProducer
// constructor, as producer_config.test.js ports ProducerConfigTest
// (javascript/design/current/producer/DECISIONS.md, entry 28):
//   testSaslLoginRefreshMinValuesAreValid, testSaslLoginRefreshMaxValuesAreValid:
//     ported here (the producer builds; the parsed values are not readable).
//   the eight testSaslLoginRefresh*IsReallyMinimum / *IsReallyMaximum: ported
//     here, with Java's ConfigDef.Range message in full (Java checks the class).
//   testSaslLoginRefreshDefaults: skipped: it reads the parsed defaults, and
//     no getter of a parsed configuration is translated; core:
//     config_def.rs test_client_sasl_support.
// SslConfigs and SecurityConfig have no Java test class.

const assert = require('node:assert/strict');

const { javaStringConstants } = require('../../../_java_source');
const { SslConfigs, SaslConfigs, SecurityConfig } = require('@confluentinc/kafka-javascript/common/config');

/** @type {Array<[typeof SslConfigs | typeof SaslConfigs | typeof SecurityConfig, string]>} */
const classes = [
  [SslConfigs, 'org.apache.kafka.common.config.SslConfigs'],
  [SaslConfigs, 'org.apache.kafka.common.config.SaslConfigs'],
  [SecurityConfig, 'org.apache.kafka.common.config.SecurityConfig'],
];
for (const [cls, fqn] of classes) {
  test(`every literal constant of ${cls.name} matches Java`, () => {
    const { literals, deprecated } = javaStringConstants(fqn);
    assert.ok(literals.length > 0);
    for (const [name, value] of literals) {
      if (deprecated.has(name)) assert.ok(!(name in cls), name);
      else assert.equal(/** @type {Record<string, unknown>} */ (/** @type {unknown} */ (cls))[name], value, name);
    }
  });

  test(`${cls.name} keeps Java's default constructor`, () => {
    assert.ok(new cls() instanceof cls);
  });
}

test('evaluated constants are evaluated as Java does', () => {
  assert.equal(SaslConfigs.DEFAULT_SASL_MECHANISM, 'GSSAPI');
  assert.equal(SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR, 0.8);
  assert.equal(SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS, 60);
  assert.equal(SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM, 'SunX509');
  assert.equal(SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM, 'PKIX');
  // A Set constant is a read-only set, in Java's order: add, delete and clear
  // throw, as Java's unmodifiable set's do.
  for (const set of [SslConfigs.RECONFIGURABLE_CONFIGS, SslConfigs.NON_RECONFIGURABLE_CONFIGS]) {
    assert.ok(set instanceof Set);
    for (const change of [() => set.add('x'), () => set.delete('ssl.client.auth'), () => set.clear()]) {
      assert.throws(change, { name: 'UnsupportedOperationError', message: '' });
    }
  }
  assert.equal([...SslConfigs.RECONFIGURABLE_CONFIGS][0], 'ssl.keystore.type');
  assert.ok(SslConfigs.RECONFIGURABLE_CONFIGS.has('ssl.keystore.type'));
  assert.ok(SslConfigs.NON_RECONFIGURABLE_CONFIGS.has('ssl.client.auth'));
  assert.equal(SslConfigs.NON_RECONFIGURABLE_CONFIGS.size, 10);
});

test("the store type defaults are Java's, not the core's", () => {
  // Java's JKS; the core reads only PEM, which the constants' docs say.
  assert.equal(SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE, 'JKS');
  assert.equal(SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE, 'JKS');
});

// SaslConfigsTest, through the producer's configuration check (header above).
// The values are the strings Java's tests put in their props; Java's
// ConfigException(name, value, message) prints the parsed value, so
// -0.000001 is Java's Double.toString, -1.0E-6.
const { KafkaProducer } = require('@confluentinc/kafka-javascript/producer');
const { ConfigError } = require('@confluentinc/kafka-javascript/common/config');
const { KafkaError } = require('@confluentinc/kafka-javascript/common');

const SASL_BOOTSTRAP = { 'bootstrap.servers': '127.0.0.1:1' };

/** @type {Array<[string, Record<string, string>]>} */
const SASL_VALID = [
  [
    'testSaslLoginRefreshMinValuesAreValid',
    {
      [SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR]: '0.5',
      [SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER]: '0.0',
      [SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS]: '0',
      [SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS]: '0',
    },
  ],
  [
    'testSaslLoginRefreshMaxValuesAreValid',
    {
      [SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR]: '1.0',
      [SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER]: '0.25',
      [SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS]: '900',
      [SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS]: '3600',
    },
  ],
];
for (const [label, configs] of SASL_VALID) {
  test(`SaslConfigsTest.${label}: the producer takes the values`, async () => {
    // Java's assertEquals on the parsed values is not translatable: no getter
    // of a parsed configuration.
    const producer = new KafkaProducer({ ...SASL_BOOTSTRAP, ...configs });
    await producer.close({ timeout: 0 });
  });
}

/** @type {Array<[string, string, string, string, string]>} */
const SASL_REFUSED = [
  [
    'testSaslLoginRefreshWindowFactorMinValueIsReallyMinimum',
    SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR,
    '0.499999',
    '0.499999',
    'Value must be at least 0.5',
  ],
  [
    'testSaslLoginRefreshWindowFactorMaxValueIsReallyMaximum',
    SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR,
    '1.0001',
    '1.0001',
    'Value must be no more than 1.0',
  ],
  [
    'testSaslLoginRefreshWindowJitterMinValueIsReallyMinimum',
    SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER,
    '-0.000001',
    '-1.0E-6',
    'Value must be at least 0.0',
  ],
  [
    'testSaslLoginRefreshWindowJitterMaxValueIsReallyMaximum',
    SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER,
    '0.251',
    '0.251',
    'Value must be no more than 0.25',
  ],
  [
    'testSaslLoginRefreshMinPeriodSecondsMinValueIsReallyMinimum',
    SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS,
    '-1',
    '-1',
    'Value must be at least 0',
  ],
  [
    'testSaslLoginRefreshMinPeriodSecondsMaxValueIsReallyMaximum',
    SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS,
    '901',
    '901',
    'Value must be no more than 900',
  ],
  [
    'testSaslLoginRefreshBufferSecondsMinValueIsReallyMinimum',
    SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS,
    '-1',
    '-1',
    'Value must be at least 0',
  ],
  [
    'testSaslLoginRefreshBufferSecondsMaxValueIsReallyMaximum',
    SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS,
    '3601',
    '3601',
    'Value must be no more than 3600',
  ],
];
for (const [label, key, value, printed, message] of SASL_REFUSED) {
  test(`SaslConfigsTest.${label}: the producer refuses ${key}=${value}`, () => {
    assert.throws(
      () => new KafkaProducer({ ...SASL_BOOTSTRAP, [key]: value }),
      (e) => {
        assert.ok(e instanceof ConfigError && e instanceof KafkaError, `${e}`);
        assert.equal(e.message, `Invalid value ${printed} for configuration ${key}: ${message}`);
        return true;
      },
    );
  });
}
