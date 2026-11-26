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

// The option classes (javascript/CLAUDE.md, JavaScript Binding Conventions, Class
// family): AbstractOptions, abstract; the keyed constructors; a
// getter and setter of one name as one method, and a pair of two names kept;
// the setters recorded in the order they last ran, which the client runs on
// the native options; Java's timeoutMs as an Integer. Java ships no
// test of these classes.

const assert = require('node:assert/strict');

const { AbstractOptions, DescribeClusterOptions, ListTopicsOptions } = require('@confluentinc/kafka-javascript/admin');

/**
 * The setters an options object ran, the library's own view (src/admin/abstract_options.ts).
 *
 * @param {AbstractOptions} options
 * @returns {Array<[string, unknown]>}
 */
const setters = (options) =>
  /** @type {{ internalSetters(): Array<[string, unknown]> }} */ (/** @type {unknown} */ (options)).internalSetters();

test('AbstractOptions is abstract', () => {
  // @ts-expect-error an abstract class: tsc refuses new, and the run time throws TypeError
  assert.throws(() => new AbstractOptions(), {
    name: 'TypeError',
    message: 'AbstractOptions cannot be constructed directly; use one of its subclasses',
  });
  assert.ok(new DescribeClusterOptions() instanceof AbstractOptions);
});

test("timeoutMs: no argument reads, one sets and returns the options; Java's Integer", () => {
  const o = new DescribeClusterOptions();
  assert.equal(o.timeoutMs(), null);
  assert.equal(o.timeoutMs(1000), o);
  assert.equal(o.timeoutMs(), 1000);
  assert.equal(o.timeoutMs({ timeoutMs: null }), o);
  assert.equal(o.timeoutMs(), null);
  o.timeoutMs(-5);
  assert.equal(o.timeoutMs(), -5);
  assert.throws(() => o.timeoutMs(2 ** 31), { name: 'RangeError', message: 'timeoutMs must be from -2147483648 to 2147483647' });
  assert.throws(() => o.timeoutMs(Infinity), { name: 'RangeError', message: 'timeoutMs must be finite, not Infinity' });
  assert.throws(() => o.timeoutMs(1.5), { name: 'TypeError', message: 'timeoutMs must be an integer' });
  // @ts-expect-error an empty options object neither reads nor sets
  assert.throws(() => o.timeoutMs({}), {
    name: 'TypeError',
    message: 'timeoutMs() takes the value to set: leave the argument out to read it',
  });
  // @ts-expect-error undefined is never a value
  assert.throws(() => o.timeoutMs(undefined), TypeError);
});

test('DescribeClusterOptions: the keyed constructor and the getter / setter pairs', () => {
  const o = new DescribeClusterOptions({ includeFencedBrokers: true, timeoutMs: 10, includeAuthorizedOperations: undefined });
  assert.equal(o.includeFencedBrokers(), true);
  assert.equal(o.includeAuthorizedOperations(), false);
  assert.equal(o.timeoutMs(), 10);
  // In the fixed order (the parent's field first), whatever the key order.
  assert.deepEqual(setters(o), [
    ['timeoutMs', 10],
    ['includeFencedBrokers', true],
  ]);
  assert.equal(o.includeAuthorizedOperations(true), o);
  assert.equal(o.includeAuthorizedOperations(), true);
  o.timeoutMs(20);
  // The order the setters last ran.
  assert.deepEqual(setters(o), [
    ['includeFencedBrokers', true],
    ['includeAuthorizedOperations', true],
    ['timeoutMs', 20],
  ]);
  // @ts-expect-error a null boolean: Java's primitive cannot hold it
  assert.throws(() => o.includeFencedBrokers({ includeFencedBrokers: null }), {
    name: 'TypeError',
    message: 'includeFencedBrokers must be a boolean, not null',
  });
  // @ts-expect-error an unknown key
  assert.throws(() => new DescribeClusterOptions({ validateOnly: true }), {
    name: 'IllegalArgumentError',
    message:
      'DescribeClusterOptions() takes an object of the keys (timeoutMs, includeAuthorizedOperations, includeFencedBrokers); got (validateOnly)',
  });
  assert.deepEqual(setters(new DescribeClusterOptions()), []);
});

test("ListTopicsOptions: Java's listInternal / shouldListInternal, toString, equals and hashCode", () => {
  const o = new ListTopicsOptions();
  assert.equal(o.shouldListInternal(), false);
  assert.equal(o.listInternal(true), o);
  assert.equal(o.shouldListInternal(), true);
  // @ts-expect-error the setter with no value: it does not read
  assert.throws(() => o.listInternal(), {
    name: 'TypeError',
    message: 'listInternal() takes the value to set: read it with shouldListInternal()',
  });
  // @ts-expect-error the reader takes no argument
  assert.throws(() => o.shouldListInternal(true), { name: 'TypeError', message: 'shouldListInternal() only reads: it takes no argument' });
  assert.equal(o.toString(), 'ListTopicsOptions(listInternal=true)');
  assert.ok(o.equals(new ListTopicsOptions({ listInternal: true })));
  assert.ok(!o.equals(new ListTopicsOptions()));
  assert.ok(!o.equals(null));
  assert.equal(o.hashCode(), new ListTopicsOptions({ listInternal: true }).hashCode());
  // Objects.hash(listInternal): 31 + Boolean.hashCode.
  assert.equal(new ListTopicsOptions().hashCode(), 31 + 1237);
});

// Every setter of every *Options class reaches the native options: the addon
// runs each recorded setter through its per-class table in native/admin.c,
// which throws "no such setter of the admin options" for a name it lacks.
// Each setter runs twice, as Python's options table runs each once
// (python/test/unit/test_admin_client_topics.py), and the call is made on an
// unreachable address, where it must return its Result without a throw.
test('every setter of every Options class, run once and twice, reaches its call without a throw', async () => {
  const admin = require('@confluentinc/kafka-javascript/admin');
  const {
    ElectionType,
    GroupState,
    GroupType,
    IsolationLevel,
    TopicCollection,
    TopicPartition,
    TopicPartitionMap,
    TopicPartitionReplica,
    TopicPartitionReplicaMap,
  } = require('@confluentinc/kafka-javascript/common');
  const acl = require('@confluentinc/kafka-javascript/common/acl');
  const { ConfigResource, ConfigResourceMap } = require('@confluentinc/kafka-javascript/common/config');
  const quota = require('@confluentinc/kafka-javascript/common/quota');
  const { PatternType, ResourcePattern, ResourceType } = require('@confluentinc/kafka-javascript/common/resource');
  const { KafkaPrincipal } = require('@confluentinc/kafka-javascript/common/security/auth');
  const { OffsetAndMetadata } = require('@confluentinc/kafka-javascript/consumer');

  const tp = new TopicPartition({ topic: 't', partition: 0 });
  const replica = new TopicPartitionReplica({ topic: 't', partition: 0, brokerId: 1 });
  const resource = new ConfigResource({ type: ConfigResource.Type.TOPIC, name: 't' });
  const principal = new KafkaPrincipal({ principalType: KafkaPrincipal.USER_TYPE, name: 'alice' });
  const binding = new acl.AclBinding({
    pattern: new ResourcePattern({ resourceType: ResourceType.TOPIC, name: 't', patternType: PatternType.LITERAL }),
    entry: new acl.AccessControlEntry({
      principal: 'User:x',
      host: '*',
      operation: acl.AclOperation.READ,
      permissionType: acl.AclPermissionType.ALLOW,
    }),
  });
  const info = new admin.ScramCredentialInfo({ mechanism: admin.ScramMechanism.SCRAM_SHA_256, iterations: 4096 });

  /**
   * One options class: a fresh instance, the setters to run on it as
   * [method, value], and the call that takes it.
   *
   * @typedef {{ make: () => AbstractOptions, set: Array<[string, unknown]>, run: (client: import('@confluentinc/kafka-javascript/admin').Admin, options: any) => unknown }} Case
   */
  /** @type {Record<string, Case>} */
  const cases = {
    AbortTransactionOptions: {
      make: () => new admin.AbortTransactionOptions(),
      set: [],
      run: (c, options) =>
        c.abortTransaction({
          spec: new admin.AbortTransactionSpec({ topicPartition: tp, producerId: 1n, producerEpoch: 0, coordinatorEpoch: 0 }),
          options,
        }),
    },
    AlterClientQuotasOptions: {
      make: () => new admin.AlterClientQuotasOptions(),
      set: [['validateOnly', true]],
      run: (c, options) =>
        c.alterClientQuotas({
          entries: [new quota.ClientQuotaAlteration({ entity: new quota.ClientQuotaEntity(new Map([['user', 'u']])), ops: [] })],
          options,
        }),
    },
    AlterConfigsOptions: {
      make: () => new admin.AlterConfigsOptions(),
      set: [['validateOnly', true]],
      run: (c, options) =>
        c.incrementalAlterConfigs({
          configs: new ConfigResourceMap([
            [
              resource,
              [
                new admin.AlterConfigOp({
                  configEntry: new admin.ConfigEntry({ name: 'retention.ms', value: '1' }),
                  operationType: admin.AlterConfigOp.OpType.SET,
                }),
              ],
            ],
          ]),
          options,
        }),
    },
    AlterConsumerGroupOffsetsOptions: {
      make: () => new admin.AlterConsumerGroupOffsetsOptions(),
      set: [],
      run: (c, options) =>
        c.alterConsumerGroupOffsets({ groupId: 'g', offsets: new TopicPartitionMap([[tp, new OffsetAndMetadata(1n)]]), options }),
    },
    AlterPartitionReassignmentsOptions: {
      make: () => new admin.AlterPartitionReassignmentsOptions(),
      set: [['allowReplicationFactorChange', false]],
      run: (c, options) => c.alterPartitionReassignments({ reassignments: new TopicPartitionMap([[tp, null]]), options }),
    },
    AlterReplicaLogDirsOptions: {
      make: () => new admin.AlterReplicaLogDirsOptions(),
      set: [],
      run: (c, options) => c.alterReplicaLogDirs({ replicaAssignment: new TopicPartitionReplicaMap([[replica, '/d']]), options }),
    },
    AlterUserScramCredentialsOptions: {
      make: () => new admin.AlterUserScramCredentialsOptions(),
      set: [],
      run: (c, options) =>
        c.alterUserScramCredentials({
          alterations: [new admin.UserScramCredentialUpsertion({ user: 'u', credentialInfo: info, password: 'p' })],
          options,
        }),
    },
    CreateAclsOptions: {
      make: () => new admin.CreateAclsOptions(),
      set: [],
      run: (c, options) => c.createAcls({ acls: [binding], options }),
    },
    CreateDelegationTokenOptions: {
      make: () => new admin.CreateDelegationTokenOptions(),
      set: [
        ['maxLifetimeMs', 60000n],
        ['renewers', [principal]],
        ['owner', principal],
      ],
      run: (c, options) => c.createDelegationToken({ options }),
    },
    CreatePartitionsOptions: {
      make: () => new admin.CreatePartitionsOptions(),
      set: [
        ['validateOnly', true],
        ['retryOnQuotaViolation', false],
      ],
      run: (c, options) => c.createPartitions({ newPartitions: { t: admin.NewPartitions.increaseTo(3) }, options }),
    },
    CreateTopicsOptions: {
      make: () => new admin.CreateTopicsOptions(),
      set: [
        ['validateOnly', true],
        ['retryOnQuotaViolation', false],
      ],
      run: (c, options) =>
        c.createTopics({ newTopics: [new admin.NewTopic({ name: 't', numPartitions: 1, replicationFactor: 1 })], options }),
    },
    DeleteAclsOptions: {
      make: () => new admin.DeleteAclsOptions(),
      set: [],
      run: (c, options) => c.deleteAcls({ filters: [binding.toFilter()], options }),
    },
    DeleteConsumerGroupOffsetsOptions: {
      make: () => new admin.DeleteConsumerGroupOffsetsOptions(),
      set: [],
      run: (c, options) => c.deleteConsumerGroupOffsets({ groupId: 'g', partitions: [tp], options }),
    },
    DeleteConsumerGroupsOptions: {
      make: () => new admin.DeleteConsumerGroupsOptions(),
      set: [],
      run: (c, options) => c.deleteConsumerGroups({ groupIds: ['g'], options }),
    },
    DeleteRecordsOptions: {
      make: () => new admin.DeleteRecordsOptions(),
      set: [],
      run: (c, options) =>
        c.deleteRecords({ recordsToDelete: new TopicPartitionMap([[tp, admin.RecordsToDelete.beforeOffset(0n)]]), options }),
    },
    DeleteTopicsOptions: {
      make: () => new admin.DeleteTopicsOptions(),
      set: [['retryOnQuotaViolation', false]],
      run: (c, options) => c.deleteTopics({ topics: TopicCollection.ofTopicNames(['t']), options }),
    },
    DescribeAclsOptions: {
      make: () => new admin.DescribeAclsOptions(),
      set: [],
      run: (c, options) => c.describeAcls({ filter: binding.toFilter(), options }),
    },
    DescribeClassicGroupsOptions: {
      make: () => new admin.DescribeClassicGroupsOptions(),
      set: [['includeAuthorizedOperations', true]],
      run: (c, options) => c.describeClassicGroups({ groupIds: ['g'], options }),
    },
    DescribeClientQuotasOptions: {
      make: () => new admin.DescribeClientQuotasOptions(),
      set: [],
      run: (c, options) => c.describeClientQuotas({ filter: quota.ClientQuotaFilter.all(), options }),
    },
    DescribeClusterOptions: {
      make: () => new admin.DescribeClusterOptions(),
      set: [
        ['includeAuthorizedOperations', true],
        ['includeFencedBrokers', true],
      ],
      run: (c, options) => c.describeCluster({ options }),
    },
    DescribeConfigsOptions: {
      make: () => new admin.DescribeConfigsOptions(),
      set: [
        ['includeSynonyms', true],
        ['includeDocumentation', true],
      ],
      run: (c, options) => c.describeConfigs({ resources: [resource], options }),
    },
    DescribeConsumerGroupsOptions: {
      make: () => new admin.DescribeConsumerGroupsOptions(),
      set: [['includeAuthorizedOperations', true]],
      run: (c, options) => c.describeConsumerGroups({ groupIds: ['g'], options }),
    },
    DescribeDelegationTokenOptions: {
      make: () => new admin.DescribeDelegationTokenOptions(),
      set: [['owners', [principal]]],
      run: (c, options) => c.describeDelegationToken({ options }),
    },
    DescribeFeaturesOptions: {
      make: () => new admin.DescribeFeaturesOptions(),
      set: [['nodeId', 1]],
      run: (c, options) => c.describeFeatures({ options }),
    },
    DescribeLogDirsOptions: {
      make: () => new admin.DescribeLogDirsOptions(),
      set: [],
      run: (c, options) => c.describeLogDirs({ brokers: [1], options }),
    },
    DescribeProducersOptions: {
      make: () => new admin.DescribeProducersOptions(),
      set: [['brokerId', 1]],
      run: (c, options) => c.describeProducers({ partitions: [tp], options }),
    },
    DescribeReplicaLogDirsOptions: {
      make: () => new admin.DescribeReplicaLogDirsOptions(),
      set: [],
      run: (c, options) => c.describeReplicaLogDirs({ replicas: [replica], options }),
    },
    DescribeTopicsOptions: {
      make: () => new admin.DescribeTopicsOptions(),
      set: [
        ['includeAuthorizedOperations', true],
        ['partitionSizeLimitPerResponse', 5],
      ],
      run: (c, options) => c.describeTopics({ topicNames: ['t'], options }),
    },
    DescribeTransactionsOptions: {
      make: () => new admin.DescribeTransactionsOptions(),
      set: [],
      run: (c, options) => c.describeTransactions({ transactionalIds: ['x'], options }),
    },
    DescribeUserScramCredentialsOptions: {
      make: () => new admin.DescribeUserScramCredentialsOptions(),
      set: [],
      run: (c, options) => c.describeUserScramCredentials({ users: ['u'], options }),
    },
    ElectLeadersOptions: {
      make: () => new admin.ElectLeadersOptions(),
      set: [],
      run: (c, options) => c.electLeaders({ electionType: ElectionType.PREFERRED, partitions: [tp], options }),
    },
    ExpireDelegationTokenOptions: {
      make: () => new admin.ExpireDelegationTokenOptions(),
      set: [['expiryTimePeriodMs', -1n]],
      run: (c, options) => c.expireDelegationToken({ hmac: new Uint8Array([1]), options }),
    },
    FenceProducersOptions: {
      make: () => new admin.FenceProducersOptions(),
      set: [],
      run: (c, options) => c.fenceProducers({ transactionalIds: ['x'], options }),
    },
    ListConfigResourcesOptions: {
      make: () => new admin.ListConfigResourcesOptions(),
      set: [],
      run: (c, options) => c.listConfigResources({ configResourceTypes: [ConfigResource.Type.TOPIC], options }),
    },
    ListConsumerGroupOffsetsOptions: {
      make: () => new admin.ListConsumerGroupOffsetsOptions(),
      set: [['requireStable', true]],
      run: (c, options) => c.listConsumerGroupOffsets({ groupId: 'g', options }),
    },
    ListGroupsOptions: {
      make: () => new admin.ListGroupsOptions(),
      set: [
        ['inGroupStates', [GroupState.STABLE]],
        ['withTypes', [GroupType.CONSUMER]],
        ['withProtocolTypes', ['consumer']],
      ],
      run: (c, options) => c.listGroups({ options }),
    },
    ListOffsetsOptions: {
      make: () => new admin.ListOffsetsOptions(IsolationLevel.READ_COMMITTED),
      set: [],
      run: (c, options) => c.listOffsets({ topicPartitionOffsets: new TopicPartitionMap([[tp, admin.OffsetSpec.latest()]]), options }),
    },
    ListPartitionReassignmentsOptions: {
      make: () => new admin.ListPartitionReassignmentsOptions(),
      set: [],
      run: (c, options) => c.listPartitionReassignments({ partitions: [tp], options }),
    },
    ListTopicsOptions: {
      make: () => new admin.ListTopicsOptions(),
      set: [['listInternal', true]],
      run: (c, options) => c.listTopics({ options }),
    },
    ListTransactionsOptions: {
      make: () => new admin.ListTransactionsOptions(),
      set: [
        ['filterStates', [admin.TransactionState.ONGOING]],
        ['filterProducerIds', [1n]],
        ['filterOnDuration', 1000n],
        ['filterOnTransactionalIdPattern', 'x.*'],
      ],
      run: (c, options) => c.listTransactions({ options }),
    },
    RemoveMembersFromConsumerGroupOptions: {
      make: () => new admin.RemoveMembersFromConsumerGroupOptions([new admin.MemberToRemove('i')]),
      set: [['reason', 'r']],
      run: (c, options) => c.removeMembersFromConsumerGroup({ groupId: 'g', options }),
    },
    RenewDelegationTokenOptions: {
      make: () => new admin.RenewDelegationTokenOptions(),
      set: [['renewTimePeriodMs', 60000n]],
      run: (c, options) => c.renewDelegationToken({ hmac: new Uint8Array([1]), options }),
    },
    TerminateTransactionOptions: {
      make: () => new admin.TerminateTransactionOptions(),
      set: [],
      run: (c, options) => c.forceTerminateTransaction({ transactionalId: 'x', options }),
    },
    UnregisterControllerOptions: {
      make: () => new admin.UnregisterControllerOptions(),
      set: [],
      run: (c, options) => c.unregisterController({ controllerId: 1, options }),
    },
    UpdateFeaturesOptions: {
      make: () => new admin.UpdateFeaturesOptions(),
      set: [['validateOnly', true]],
      run: (c, options) =>
        c.updateFeatures({
          featureUpdates: {
            'metadata.version': new admin.FeatureUpdate({ maxVersionLevel: 1, upgradeType: admin.FeatureUpdate.UpgradeType.UPGRADE }),
          },
          options,
        }),
    },
  };

  // Every exported *Options class has its row.
  const exported = Object.keys(admin)
    .filter((name) => name.endsWith('Options') && name !== 'AbstractOptions')
    .sort();
  assert.deepEqual(Object.keys(cases).sort(), exported);

  const client = admin.Admin.create({ 'bootstrap.servers': '127.0.0.1:1' });
  try {
    for (const [name, { make, set, run }] of Object.entries(cases)) {
      for (const times of [1, 2]) {
        const options = make();
        options.timeoutMs(1);
        for (let i = 0; i < times; i++) {
          for (const [setter, value] of set) {
            const result = /** @type {Record<string, (value: unknown) => unknown>} */ (/** @type {unknown} */ (options))[setter].call(
              options,
              value,
            );
            // reason() returns void, as Java's does; the others the options.
            if (setter !== 'reason') assert.equal(result, options, `${name}.${setter}`);
          }
        }
        // The setters that ran, each once, in the order they last ran.
        assert.deepEqual(
          setters(options)
            .map(([s]) => s)
            .sort(),
          ['timeoutMs', ...set.map(([s]) => s)].sort(),
          name,
        );
        assert.doesNotThrow(() => run(client, options), `${name}, setters run ${times} time(s)`);
      }
    }
  } finally {
    await client.close({ timeout: 0 });
  }
}, 60000);
