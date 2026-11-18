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

// ProducerConfig, as python/test/unit/test_config_classes.py checks the
// Python one, and Java's ProducerConfigTest (Apache Kafka 4.4.0), as
// python/test/unit/test_config_class_ports.py ports it. ProducerConfig is
// translated for its constants only (root conventions, Scope): its ConfigDef
// machinery is not, so a test of a configuration the client rejects is ported
// through the KafkaProducer constructor, which runs the same ConfigDef check in
// the core (the message asserted in full, where Java asserts it names the
// key). ProducerConfigTest, 13 tests:
//   testInvalidCompressionType, testInvalidSecurityProtocol,
//   testInvalidMetadataRecoveryStrategy,
//   testInvalidBufferMemoryAllocationStrategy: ported here.
//   testValidBufferMemoryAllocationStrategy: ported here (accepted; the parsed
//     value is not readable, as getString is not translated).
//   testAppendSerializerToConfig, testAppendSerializerToConfigWithException:
//     skipped: appendSerializerToConfig is not translated (serdes are
//     constructor arguments, root conventions, Serialization); core: none.
//   testDefaultMetadataRecoveryStrategy,
//   testDefaultBufferMemoryAllocationStrategy: skipped: getString is not
//     translated; core: producer_config.rs test_default_metadata_recovery_strategy,
//     test_default_buffer_memory_allocation_strategy.
//   testCaseInsensitiveSecurityProtocol: ported here (the producer builds;
//     the originals() read is not translated).
//   testUpperboundCheckOfEnableIdempotence: ported here: the constructor
//     reaches the core's postProcessParsedConfig through
//     kafka_producer_ProducerConfig_new, as for the rejected values above.
//   testTwoPhaseCommitIncompatibleWithTransactionTimeout: skipped:
//     transactions are left out of the preview, by request; core:
//     producer_config.rs, the test of the same name.
//   testValidateConfigPropertiesFile: skipped: it reads the Java docs'
//     properties file; core: none.
// SaslConfigsTest, 11 tests: 10 ported through the constructor, one skipped
// (test/unit/common/config/ssl_configs.test.js says which).

const assert = require('node:assert/strict');

const { javaStringConstants } = require('../../_java_source');
const { KafkaProducer, ProducerConfig } = require('@confluentinc/kafka-javascript/producer');
const { ConfigError } = require('@confluentinc/kafka-javascript/common/config');

const BOOTSTRAP = { [ProducerConfig.BOOTSTRAP_SERVERS_CONFIG]: '127.0.0.1:1' };

test('every literal constant of ProducerConfig matches Java; a deprecated one is absent', () => {
  const { literals, deprecated } = javaStringConstants('org.apache.kafka.clients.producer.ProducerConfig');
  assert.ok(deprecated.has('PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG'));
  for (const [name, value] of literals) {
    if (deprecated.has(name)) assert.ok(!(name in ProducerConfig), name);
    else assert.equal(/** @type {Record<string, unknown>} */ (/** @type {unknown} */ (ProducerConfig))[name], value, name);
  }
});

test('the constants Java takes from CommonClientConfigs and AbstractConfig', () => {
  assert.equal(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 'bootstrap.servers');
  assert.equal(ProducerConfig.SECURITY_PROVIDERS_CONFIG, 'security.providers');
  assert.equal(ProducerConfig.CONFIG_PROVIDERS_CONFIG, 'config.providers');
  assert.equal(ProducerConfig.PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG, 'partitioner.adaptive.partitioning.enable');
});

test('ProducerConfig is not instantiable', () => {
  // @ts-expect-error a private constructor: the constructor throws TypeError
  assert.throws(() => new ProducerConfig({ 'bootstrap.servers': 'x' }), {
    name: 'TypeError',
    message: 'ProducerConfig is not instantiable: its ConfigDef machinery is not translated; a configuration is a plain object of its keys',
  });
});

const REJECTED = [
  [
    'ProducerConfigTest.testInvalidCompressionType',
    ProducerConfig.COMPRESSION_TYPE_CONFIG,
    'abc',
    'String must be one of: none, gzip, snappy, lz4, zstd',
  ],
  [
    'ProducerConfigTest.testInvalidSecurityProtocol',
    'security.protocol',
    'abc',
    'String must be one of (case insensitive): SASL_SSL, PLAINTEXT, SSL, SASL_PLAINTEXT',
  ],
  [
    'ProducerConfigTest.testInvalidMetadataRecoveryStrategy',
    'metadata.recovery.strategy',
    'abc',
    'String must be one of (case insensitive): REBOOTSTRAP, NONE',
  ],
  [
    'ProducerConfigTest.testInvalidBufferMemoryAllocationStrategy',
    ProducerConfig.BUFFER_MEMORY_ALLOCATION_STRATEGY_CONFIG,
    'abc',
    'String must be one of (case insensitive): FULL, INCREMENTAL',
  ],
];

for (const [label, key, value, message] of REJECTED) {
  test(`${label}: the producer refuses ${key}=${value}`, () => {
    assert.throws(
      () => new KafkaProducer({ ...BOOTSTRAP, [key]: value }),
      (e) => {
        assert.ok(e instanceof ConfigError, `${e}`);
        assert.equal(e.message, `Invalid value ${value} for configuration ${key}: ${message}`);
        return true;
      },
    );
  });
}

test('ProducerConfigTest.testValidBufferMemoryAllocationStrategy: the producer takes it', async () => {
  const producer = new KafkaProducer({
    ...BOOTSTRAP,
    [ProducerConfig.BUFFER_MEMORY_ALLOCATION_STRATEGY_CONFIG]: ProducerConfig.BUFFER_MEMORY_ALLOCATION_STRATEGY_INCREMENTAL,
  });
  await producer.close({ timeout: 0 });
});

test('ProducerConfigTest.testUpperboundCheckOfEnableIdempotence: the idempotent producer takes at most 5 in-flight requests', async () => {
  // enable.idempotence keeps its default, true, as in Java. Java's message,
  // asserted with assertEquals, from
  // ProducerConfig#postProcessAndValidateIdempotenceConfigs; a check of the
  // configuration, so a bare ConfigError, not "Failed to construct kafka
  // producer".
  assert.throws(
    () => new KafkaProducer({ ...BOOTSTRAP, [ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION]: '6' }),
    (e) => {
      assert.ok(e instanceof ConfigError, `${e}`);
      assert.equal(
        e.message,
        'To use the idempotent producer, max.in.flight.requests.per.connection must be set to at most 5. Current value is 6.',
      );
      return true;
    },
  );
  const producer = new KafkaProducer({ ...BOOTSTRAP, [ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION]: '5' });
  await producer.close({ timeout: 0 });
});

test('ProducerConfigTest.testCaseInsensitiveSecurityProtocol: the producer takes a lowercase security.protocol', async () => {
  const { SecurityProtocol } = require('@confluentinc/kafka-javascript/common/security/auth');
  const { SaslConfigs } = require('@confluentinc/kafka-javascript/common/config');
  // Java: SecurityProtocol.SASL_SSL.name.toLowerCase(Locale.ROOT).
  const saslSslLowerCase = SecurityProtocol.SASL_SSL.name().toLowerCase();
  // Java builds only a ProducerConfig; this builds a producer, whose SASL_SSL
  // channel needs the two SASL keys Java's test leaves out: the core supports
  // PLAIN only (Java's GSSAPI default is refused) and PLAIN needs a username
  // and a password. The core's translation adds sasl.mechanism for the same
  // reason. Java's originals() read-back is not translated.
  const producer = new KafkaProducer({
    ...BOOTSTRAP,
    'security.protocol': saslSslLowerCase,
    [SaslConfigs.SASL_MECHANISM]: 'PLAIN',
    [SaslConfigs.SASL_JAAS_CONFIG]: 'org.apache.kafka.common.security.plain.PlainLoginModule required username="u" password="p";',
  });
  await producer.close({ timeout: 0 });
});
