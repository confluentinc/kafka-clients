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
