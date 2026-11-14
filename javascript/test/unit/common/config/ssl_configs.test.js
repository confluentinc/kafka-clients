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
// Java's SaslConfigsTest (Apache Kafka 4.4.0), 11 tests
// (testSaslLoginRefreshDefaults, testSaslLoginRefreshMinValuesAreValid,
// testSaslLoginRefreshMaxValuesAreValid and the eight
// testSaslLoginRefresh*IsReallyMinimum / *IsReallyMaximum): skipped, as in
// python/test/unit/test_config_class_ports.py: each parses a ConfigDef built by
// SaslConfigs.addClientSaslSupport(ConfigDef), which is not translated (root
// conventions, Scope: the ConfigDef machinery); core: none. SslConfigs and
// SecurityConfig have no Java test class.

const test = require('node:test');
const assert = require('node:assert/strict');

const { javaStringConstants } = require('../../../_java_source');
const { SslConfigs, SaslConfigs, SecurityConfig } = require('@confluentinc/kafka-javascript/common/config');

for (const [cls, fqn] of [
  [SslConfigs, 'org.apache.kafka.common.config.SslConfigs'],
  [SaslConfigs, 'org.apache.kafka.common.config.SaslConfigs'],
  [SecurityConfig, 'org.apache.kafka.common.config.SecurityConfig'],
]) {
  test(`every literal constant of ${cls.name} matches Java`, () => {
    const { literals, deprecated } = javaStringConstants(fqn);
    assert.ok(literals.length > 0);
    for (const [name, value] of literals) {
      if (deprecated.has(name)) assert.ok(!(name in cls), name);
      else assert.equal(cls[name], value, name);
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
