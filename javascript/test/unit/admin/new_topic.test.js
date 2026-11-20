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

// Java's NewTopicTest. convertToCreatableTopic is package-private in Java:
// its translation is the addon's view of the new topic, internalToNative.

const assert = require('node:assert/strict');

const { NewTopic } = require('@confluentinc/kafka-javascript/admin');
const { internalNewTopic } = require('../../_internal');

const TEST_TOPIC = 'testtopic';
const NUM_PARTITIONS = 3;
const REPLICATION_FACTOR = 1;
const CLEANUP_POLICY_CONFIG_KEY = 'cleanup.policy';
const CLEANUP_POLICY_CONFIG_VALUE = 'compact';
const BROKER_IDS = [1, 2];
const NO_NUM_PARTITIONS = -1;
const NO_REPLICATION_FACTOR = -1;

test('testConstructorWithPartitionsAndReplicationFactor', () => {
  const topic = new NewTopic({ name: TEST_TOPIC, numPartitions: NUM_PARTITIONS, replicationFactor: REPLICATION_FACTOR });
  assert.equal(topic.name(), TEST_TOPIC);
  assert.equal(topic.numPartitions(), NUM_PARTITIONS);
  assert.equal(topic.replicationFactor(), REPLICATION_FACTOR);
  assert.equal(topic.replicasAssignments(), null);
});

test('testConstructorWithOptionalValues', () => {
  const topic = new NewTopic({ name: TEST_TOPIC, numPartitions: null, replicationFactor: null });
  assert.equal(topic.name(), TEST_TOPIC);
  assert.equal(topic.numPartitions(), NO_NUM_PARTITIONS);
  assert.equal(topic.replicationFactor(), NO_REPLICATION_FACTOR);
  assert.equal(topic.replicasAssignments(), null);
});

test('testConstructorWithReplicasAssignments', () => {
  const replicasAssignments = new Map([[0, BROKER_IDS]]);
  const newTopic = new NewTopic({ name: TEST_TOPIC, replicasAssignments });
  assert.equal(newTopic.name(), TEST_TOPIC);
  assert.equal(newTopic.numPartitions(), NO_NUM_PARTITIONS);
  assert.equal(newTopic.replicationFactor(), NO_REPLICATION_FACTOR);
  assert.deepEqual(new Map(newTopic.replicasAssignments()), replicasAssignments);
});

test('testConfigsNotNull', () => {
  const newTopic = new NewTopic({ name: TEST_TOPIC, numPartitions: NUM_PARTITIONS, replicationFactor: REPLICATION_FACTOR });
  const configs = new Map([[CLEANUP_POLICY_CONFIG_KEY, CLEANUP_POLICY_CONFIG_VALUE]]);
  newTopic.configs(configs);
  assert.deepEqual(newTopic.configs(), configs);
  // A plain object of the keys is a configuration map too.
  newTopic.configs({ [CLEANUP_POLICY_CONFIG_KEY]: CLEANUP_POLICY_CONFIG_VALUE });
  assert.deepEqual(newTopic.configs(), configs);
});

test('testConfigsNull', () => {
  const newTopic = new NewTopic({ name: TEST_TOPIC, numPartitions: NUM_PARTITIONS, replicationFactor: REPLICATION_FACTOR });
  assert.equal(newTopic.configs(), null);
});

test('testUnmodifiableReplicasAssignments', () => {
  const newTopic = new NewTopic({ name: TEST_TOPIC, replicasAssignments: new Map([[0, BROKER_IDS]]) });
  const returnedAssignments = /** @type {Map<number, number[]>} */ (newTopic.replicasAssignments());
  assert.throws(() => returnedAssignments.set(1, [3, 4]), { name: 'UnsupportedOperationError' });
});

test('testConvertToCreatableTopicWithPartitionsAndReplicationFactor', () => {
  const newTopic = new NewTopic({ name: TEST_TOPIC, numPartitions: NUM_PARTITIONS, replicationFactor: REPLICATION_FACTOR });
  const creatableTopic = internalNewTopic(newTopic).internalToNative();
  assert.equal(creatableTopic.name, TEST_TOPIC);
  assert.ok(creatableTopic.numPartitions > 0);
  assert.equal(creatableTopic.numPartitions, NUM_PARTITIONS);
  assert.ok(creatableTopic.replicationFactor > 0);
  assert.equal(creatableTopic.replicationFactor, REPLICATION_FACTOR);
});

test('testConvertToCreatableTopicWithReplicasAssignments', () => {
  const partitionIndex = 0;
  const topic = new NewTopic({ name: TEST_TOPIC, replicasAssignments: new Map([[partitionIndex, BROKER_IDS]]) });
  topic.configs(new Map([[CLEANUP_POLICY_CONFIG_KEY, CLEANUP_POLICY_CONFIG_VALUE]]));
  const creatableTopic = internalNewTopic(topic).internalToNative();
  assert.equal(creatableTopic.name, TEST_TOPIC);
  assert.equal(creatableTopic.numPartitions, NO_NUM_PARTITIONS);
  assert.equal(creatableTopic.replicationFactor, NO_REPLICATION_FACTOR);
  assert.notEqual(creatableTopic.assignments, null);
  assert.deepEqual(creatableTopic.assignments, [[partitionIndex, BROKER_IDS]]);
  assert.deepEqual(creatableTopic.configs, [[CLEANUP_POLICY_CONFIG_KEY, CLEANUP_POLICY_CONFIG_VALUE]]);
});

test('testToString', () => {
  const topic1 = new NewTopic({ name: TEST_TOPIC, numPartitions: NUM_PARTITIONS, replicationFactor: REPLICATION_FACTOR });
  assert.equal(
    topic1.toString(),
    `(name=${TEST_TOPIC}, numPartitions=${NUM_PARTITIONS}, replicationFactor=${REPLICATION_FACTOR}, replicasAssignments=null, configs=null)`,
  );
  topic1.configs(new Map([[CLEANUP_POLICY_CONFIG_KEY, CLEANUP_POLICY_CONFIG_VALUE]]));
  assert.equal(
    topic1.toString(),
    `(name=${TEST_TOPIC}, numPartitions=${NUM_PARTITIONS}, replicationFactor=${REPLICATION_FACTOR}, replicasAssignments=null, configs={${CLEANUP_POLICY_CONFIG_KEY}=${CLEANUP_POLICY_CONFIG_VALUE}})`,
  );
  const topic2 = new NewTopic({ name: TEST_TOPIC, replicasAssignments: new Map([[0, BROKER_IDS]]) });
  assert.equal(
    topic2.toString(),
    `(name=${TEST_TOPIC}, numPartitions=default, replicationFactor=default, replicasAssignments={0=[1, 2]}, configs=null)`,
  );
});

test('testEqualsAndHashCode', () => {
  const topic1 = new NewTopic({ name: TEST_TOPIC, numPartitions: NUM_PARTITIONS, replicationFactor: REPLICATION_FACTOR });
  const topic2 = new NewTopic({ name: TEST_TOPIC, numPartitions: NUM_PARTITIONS, replicationFactor: REPLICATION_FACTOR });
  const topic3 = new NewTopic({ name: 'another-topic', numPartitions: NUM_PARTITIONS, replicationFactor: REPLICATION_FACTOR });
  assert.ok(topic1.equals(topic2));
  assert.ok(!topic1.equals(topic3));
  assert.equal(topic1.hashCode(), topic2.hashCode());
  assert.notEqual(topic1.hashCode(), topic3.hashCode());
});

// ---- the binding's own ----------------------------------------------------------

test("a form Java has not, and a value Java's types refuse", () => {
  // @ts-expect-error the two forms' keys mixed
  assert.throws(() => new NewTopic({ name: 't', numPartitions: 1, replicationFactor: 1, replicasAssignments: new Map() }), {
    name: 'IllegalArgumentError',
  });
  assert.throws(() => new NewTopic({ name: 't', numPartitions: 1, replicationFactor: 2 ** 15 }), { name: 'RangeError' });
  // @ts-expect-error Java's Map: a plain object is no Map of partitions
  assert.throws(() => new NewTopic({ name: 't', replicasAssignments: { 0: [1] } }), {
    name: 'TypeError',
    message: 'replicasAssignments must be a Map, not an object',
  });
});

test("the JSON form: the fields, Java's sentinels for a default", () => {
  const topic = new NewTopic({ name: 't', numPartitions: null, replicationFactor: 2 }).configs({ 'cleanup.policy': 'compact' });
  assert.deepEqual(JSON.parse(JSON.stringify(topic)), {
    name: 't',
    numPartitions: -1,
    replicationFactor: 2,
    replicasAssignments: null,
    configs: { 'cleanup.policy': 'compact' },
  });
  const assigned = new NewTopic({ name: 't', replicasAssignments: new Map([[0, [1, 2]]]) });
  assert.deepEqual(JSON.parse(JSON.stringify(assigned)).replicasAssignments, { 0: [1, 2] });
});
