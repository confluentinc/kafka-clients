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
// Python one.

const test = require('node:test');
const assert = require('node:assert/strict');

const { javaStringConstants } = require('../../_java_source');
const { ProducerConfig } = require('@confluentinc/kafka-javascript/producer');

test('every literal constant of ProducerConfig matches Java; a deprecated one is absent', () => {
  const { literals, deprecated } = javaStringConstants('org.apache.kafka.clients.producer.ProducerConfig');
  assert.ok(deprecated.has('PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG'));
  for (const [name, value] of literals) {
    if (deprecated.has(name)) assert.ok(!(name in ProducerConfig), name);
    else assert.equal(ProducerConfig[name], value, name);
  }
});

test("the constants Java takes from CommonClientConfigs and AbstractConfig", () => {
  assert.equal(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 'bootstrap.servers');
  assert.equal(ProducerConfig.SECURITY_PROVIDERS_CONFIG, 'security.providers');
  assert.equal(ProducerConfig.CONFIG_PROVIDERS_CONFIG, 'config.providers');
  assert.equal(ProducerConfig.PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG, 'partitioner.adaptive.partitioning.enable');
});

test('ProducerConfig is not instantiable', () => {
  assert.throws(() => new ProducerConfig({ 'bootstrap.servers': 'x' }), {
    name: 'TypeError',
    message: 'ProducerConfig is not instantiable: its ConfigDef machinery is not translated; a configuration is a plain object of its keys',
  });
});
