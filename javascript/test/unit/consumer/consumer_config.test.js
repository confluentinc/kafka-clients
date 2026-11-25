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

// ConsumerConfig, as test/unit/producer/producer_config.test.js checks
// ProducerConfig: translated for its constants only (JS conventions, Scope),
// its ConfigDef machinery not. The ports of Java's ConsumerConfigTest come
// with the consumer, which runs its configuration through the core.

const assert = require('node:assert/strict');

const { javaStringConstants } = require('../../_java_source');
const { ConsumerConfig } = require('@confluentinc/kafka-javascript/consumer');

test('every literal constant of ConsumerConfig matches Java; a deprecated one is absent', () => {
  const { literals, deprecated } = javaStringConstants('org.apache.kafka.clients.consumer.ConsumerConfig');
  for (const [name, value] of literals) {
    if (deprecated.has(name)) assert.ok(!(name in ConsumerConfig), name);
    else assert.equal(/** @type {Record<string, unknown>} */ (/** @type {unknown} */ (ConsumerConfig))[name], value, name);
  }
});

test('the constants Java takes from CommonClientConfigs and AbstractConfig', () => {
  assert.equal(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 'bootstrap.servers');
  assert.equal(ConsumerConfig.GROUP_ID_CONFIG, 'group.id');
  assert.equal(ConsumerConfig.GROUP_PROTOCOL_CONFIG, 'group.protocol');
  assert.equal(ConsumerConfig.CONFIG_PROVIDERS_CONFIG, 'config.providers');
});

test('ConsumerConfig is not instantiable', () => {
  // @ts-expect-error a private constructor: the constructor throws TypeError
  assert.throws(() => new ConsumerConfig({ 'bootstrap.servers': 'x' }), {
    name: 'TypeError',
    message: 'ConsumerConfig is not instantiable: its ConfigDef machinery is not translated; a configuration is a plain object of its keys',
  });
});

// Java's ConsumerConfigTest (Apache Kafka 4.4.0), 17 tests, ported through the
// KafkaConsumer constructor, which runs the same ConfigDef checks in the core
// (the message asserted in full, where Java asserts it names the key), as
// test/unit/producer/producer_config.test.js ports ProducerConfigTest:
//   testOverrideEnableAutoCommit, testInvalidGroupInstanceId,
//   testInvalidSecurityProtocol, testCaseInsensitiveSecurityProtocol,
//   testRemoteAssignorConfig, testRemoteAssignorWithClassicGroupProtocol,
//   testInvalidMetadataRecoveryStrategy, testProtocolConfigValidation and
//   testUnsupportedConfigsWithConsumerGroupProtocol: ported here. Where Java
//   reads a parsed value back, the port checks the consumer builds: the
//   parsed configuration has no getter here.
//   testOverrideClientId, ensureDefaultThrowOnUnsupportedStableFlagToFalse,
//   testDefaultConsumerGroupConfig, testDefaultMetadataRecoveryStrategy:
//     skipped: each only reads a parsed value (getString, getBoolean), which
//     no getter gives, ConfigDef not being translated.
//   testAppendDeserializerToConfig, testAppendDeserializerToConfigWithException:
//     skipped: appendDeserializerToConfig is not translated (deserializers are
//     constructor arguments, JS conventions, Serialization).
//   testDefaultPartitionAssignor: skipped: the assignors are not translated
//     (JS conventions, Scope).
//   testValidateConfigPropertiesFile: skipped: it reads the Java docs'
//     properties file.

const { KafkaConsumer } = require('@confluentinc/kafka-javascript/consumer');
const { UnsupportedOperationError } = require('@confluentinc/kafka-javascript');
const { ConfigError } = require('@confluentinc/kafka-javascript/common/config');
const { InvalidConfigurationError } = require('@confluentinc/kafka-javascript/common/errors');

/** @typedef {import('@confluentinc/kafka-javascript/consumer').ConsumerConfigs} ConsumerConfigs */

const BASE = { [ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG]: '127.0.0.1:1', [ConsumerConfig.GROUP_PROTOCOL_CONFIG]: 'consumer' };

/**
 * Builds a consumer and closes it: the configuration is accepted.
 *
 * @param {ConsumerConfigs} configs
 */
async function builds(configs) {
  await new KafkaConsumer({ configs }).close();
}

test('testOverrideEnableAutoCommit', async () => {
  // Our default properties (no 'enable.auto.commit' or 'group.id') are valid.
  await builds(BASE);
  // Explicitly disabling 'enable.auto.commit' still works.
  await builds({ ...BASE, [ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG]: false });
  // Enabling 'enable.auto.commit' but without 'group.id' fails.
  assert.throws(() => new KafkaConsumer({ configs: { ...BASE, [ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG]: true } }), {
    constructor: InvalidConfigurationError,
    message: 'enable.auto.commit cannot be set to true when default group id (null) is used.',
  });
  // Then adding 'group.id' to the mix allows it to pass OK.
  await builds({ ...BASE, [ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG]: true, [ConsumerConfig.GROUP_ID_CONFIG]: 'test-group' });
  // Without the 'enable.auto.commit' flag, and its default of true.
  await builds({ ...BASE, [ConsumerConfig.GROUP_ID_CONFIG]: 'test-group' });
});

test('testInvalidGroupInstanceId', () => {
  assert.throws(() => new KafkaConsumer({ configs: { ...BASE, [ConsumerConfig.GROUP_INSTANCE_ID_CONFIG]: '' } }), {
    constructor: ConfigError,
    message: 'Invalid value  for configuration group.instance.id: String must be non-empty',
  });
});

test('testInvalidSecurityProtocol', () => {
  assert.throws(() => new KafkaConsumer({ configs: { ...BASE, 'security.protocol': 'abc' } }), {
    constructor: ConfigError,
    message:
      'Invalid value abc for configuration security.protocol: String must be one of (case insensitive): SASL_SSL, PLAINTEXT, SSL, SASL_PLAINTEXT',
  });
});

test('testCaseInsensitiveSecurityProtocol', async () => {
  // Java reads the original value back; the lower-case name is accepted.
  await builds({ ...BASE, 'security.protocol': 'plaintext' });
});

test('testRemoteAssignorConfig', async () => {
  await builds({ ...BASE, [ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG]: 'SomeAssignor' });
});

test('testRemoteAssignorWithClassicGroupProtocol', () => {
  const configs = {
    [ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG]: '127.0.0.1:1',
    [ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG]: 'SomeAssignor',
  };
  assert.throws(() => new KafkaConsumer({ configs }), {
    constructor: ConfigError,
    message: 'group.remote.assignor cannot be set when group.protocol=CLASSIC',
  });
});

test('testInvalidMetadataRecoveryStrategy', () => {
  assert.throws(() => new KafkaConsumer({ configs: { ...BASE, 'metadata.recovery.strategy': 'abc' } }), {
    constructor: ConfigError,
    message: 'Invalid value abc for configuration metadata.recovery.strategy: String must be one of (case insensitive): REBOOTSTRAP, NONE',
  });
});

test('testProtocolConfigValidation', async () => {
  // Java's ConsumerConfig accepts both protocols, in any case; the classic
  // one then gets the core's UnsupportedOperationError (consumer-threading.md
  // §20), the configuration having passed.
  const CLASSIC = 'Classic group protocol is not yet supported in this client; set group.protocol=consumer (KIP-848).';
  for (const [protocol, outcome] of /** @type {const} */ ([
    ['consumer', 'built'],
    ['classic', 'classic'],
    ['Consumer', 'built'],
    ['Classic', 'classic'],
    ['invalid', 'invalid'],
  ])) {
    const configs = { ...BASE, [ConsumerConfig.GROUP_PROTOCOL_CONFIG]: protocol };
    if (outcome === 'built') await builds(configs);
    else if (outcome === 'classic')
      assert.throws(() => new KafkaConsumer({ configs }), { constructor: UnsupportedOperationError, message: CLASSIC });
    else
      assert.throws(() => new KafkaConsumer({ configs }), {
        constructor: ConfigError,
        message: 'Invalid value invalid for configuration group.protocol: String must be one of (case insensitive): CONSUMER, CLASSIC',
      });
  }
});

test('testUnsupportedConfigsWithConsumerGroupProtocol', () => {
  for (const [name, value] of /** @type {Array<[string, string | number]>} */ ([
    [ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, 'RoundRobinAssignor'],
    [ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000],
    [ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000],
  ])) {
    assert.throws(() => new KafkaConsumer({ configs: { ...BASE, [name]: value } }), {
      constructor: ConfigError,
      message: `${name} cannot be set when group.protocol=CONSUMER`,
    });
  }
});
