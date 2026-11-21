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

// Broker-backed admin client test: runs only when KAFKA_BOOTSTRAP_SERVERS is
// set (`make -C javascript test-integration` starts a scratch broker for it).

const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

const {
  Admin,
  ClassicGroupDescription,
  ConsumerGroupDescription,
  GroupListing,
  ListConsumerGroupOffsetsSpec,
  ListGroupsOptions,
  MemberToRemove,
  RemoveMembersFromConsumerGroupOptions,
  AlterConfigOp,
  AlterConfigsOptions,
  ConfigEntry,
  CreateTopicsOptions,
  DescribeConfigsOptions,
  ListConfigResourcesOptions,
  ListOffsetsOptions,
  ListOffsetsResult,
  NewPartitionReassignment,
  OffsetSpec,
  LogDirDescription,
  DeletedRecords,
  DescribeClusterOptions,
  DescribeTopicsOptions,
  ListTopicsOptions,
  NewPartitions,
  NewTopic,
  RecordsToDelete,
  TopicDescription,
  TopicListing,
} = require('@confluentinc/kafka-javascript/admin');
const { AclOperation } = require('@confluentinc/kafka-javascript/common/acl');
const {
  ClassicGroupState,
  GroupState,
  GroupType,
  ElectionType,
  IsolationLevel,
  Node,
  TopicCollection,
  TopicPartition,
  TopicPartitionInfo,
  TopicPartitionMap,
  TopicPartitionReplica,
  TopicPartitionReplicaMap,
  Uuid,
} = require('@confluentinc/kafka-javascript/common');
const { ConfigResource, ConfigResourceMap } = require('@confluentinc/kafka-javascript/common/config');
const {
  ElectionNotNeededError,
  GroupIdNotFoundError,
  UnknownMemberIdError,
  InvalidReplicaAssignmentError,
  LogDirNotFoundError,
  NoReassignmentInProgressError,
  TopicExistsError,
  UnknownTopicOrPartitionError,
} = require('@confluentinc/kafka-javascript/common/errors');
const { OffsetAndMetadata } = require('@confluentinc/kafka-javascript/consumer');
const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');

// A string in every test that runs: the tests are skipped when it is unset.
const BOOTSTRAP = /** @type {string} */ (process.env.KAFKA_BOOTSTRAP_SERVERS);
const SKIP = BOOTSTRAP ? false : 'KAFKA_BOOTSTRAP_SERVERS is not set: no broker to administer';
if (SKIP) console.log(`skipping the integration tests: ${SKIP}`);

(SKIP ? test.skip : test)(
  'describeCluster: the brokers, the controller, the cluster id and the authorized operations',
  async () => {
    const admin = Admin.create({ 'bootstrap.servers': BOOTSTRAP });
    try {
      const result = admin.describeCluster();
      const nodes = await result.nodes().get();
      assert.ok(nodes.length >= 1);
      for (const n of nodes) assert.ok(n instanceof Node && n.id >= 0 && n.port > 0, String(n));
      const controller = await result.controller().get();
      assert.ok(controller instanceof Node);
      assert.ok(nodes.some((n) => n.id === controller.id));
      const clusterId = await result.clusterId().get();
      assert.equal(typeof clusterId, 'string');
      assert.ok(clusterId.length > 0);
      // Not requested: Java's null.
      assert.equal(await result.authorizedOperations().get(), null);
      const withOperations = admin.describeCluster(new DescribeClusterOptions({ includeAuthorizedOperations: true }));
      const operations = await withOperations.authorizedOperations().get();
      assert.ok(operations instanceof Set);
      for (const op of operations) assert.ok(op instanceof AclOperation);
      assert.equal(await withOperations.clusterId().get(), clusterId);
    } finally {
      await admin.close();
    }
  },
  60000,
);

(SKIP ? test.skip : test)(
  'listTopics: the internal topics only when asked for',
  async () => {
    const admin = Admin.create({ configs: { 'bootstrap.servers': BOOTSTRAP } });
    try {
      // __consumer_offsets exists once a group has committed; whatever the
      // broker has, the listing without internal topics holds none.
      const listings = await admin.listTopics().namesToListings().get();
      assert.ok(listings instanceof Map);
      for (const [name, listing] of listings) {
        assert.ok(listing instanceof TopicListing);
        assert.equal(listing.name(), name);
        assert.equal(listing.isInternal(), false);
        assert.ok(listing.topicId() !== null);
      }
      const all = admin.listTopics(new ListTopicsOptions({ listInternal: true }));
      const names = await all.names().get();
      for (const name of listings.keys()) assert.ok(names.has(name));
      assert.equal((await all.listings().get()).length, names.size);
    } finally {
      await admin.close();
    }
  },
  60000,
);

/**
 * `value`, asserted present: a nullable Result accessor narrowed.
 *
 * @template T
 * @param {T} value
 * @returns {NonNullable<T>}
 */
function present(value) {
  assert.ok(value !== null && value !== undefined);
  return /** @type {NonNullable<T>} */ (value);
}

/**
 * A future's value, retried while the broker answers that the topic is not
 * known yet: a topic just created reaches the other brokers' metadata later.
 *
 * @template T
 * @param {() => import('@confluentinc/kafka-javascript/common').KafkaFuture<T>} call
 * @returns {Promise<T>}
 */
async function retryUnknown(call) {
  for (let attempt = 0; ; attempt++) {
    try {
      return await call().get();
    } catch (error) {
      const cause = /** @type {{ cause?: unknown }} */ (error).cause;
      if (attempt >= 20 || !(cause instanceof UnknownTopicOrPartitionError)) throw error;
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
  }
}

(SKIP ? test.skip : test)(
  'createTopics, describeTopics, createPartitions, deleteRecords and deleteTopics',
  async () => {
    const admin = Admin.create({ 'bootstrap.servers': BOOTSTRAP });
    const topic = `admin-topics-${process.pid}-${Date.now()}`;
    try {
      const validated = admin.createTopics({
        newTopics: [new NewTopic({ name: topic, numPartitions: 2, replicationFactor: 1 })],
        options: new CreateTopicsOptions({ validateOnly: true }),
      });
      await validated.all().get();
      const created = admin.createTopics([
        new NewTopic({ name: topic, numPartitions: 2, replicationFactor: 1 }).configs({ 'cleanup.policy': 'delete' }),
      ]);
      await created.all().get();
      assert.equal(await created.numPartitions(topic).get(), 2);
      assert.equal(await created.replicationFactor(topic).get(), 1);
      const id = await created.topicId(topic).get();
      assert.ok(id instanceof Uuid);
      const config = await created.config(topic).get();
      assert.equal(config?.get('cleanup.policy')?.value(), 'delete');
      // The same topic again: the broker's TopicExistsError.
      const again = admin.createTopics([new NewTopic({ name: topic, numPartitions: 1, replicationFactor: 1 })]);
      await assert.rejects(again.all().get(), (error) => /** @type {Error} */ (error).cause instanceof TopicExistsError);

      const byName = await retryUnknown(() => present(admin.describeTopics([topic]).allTopicNames()));
      const description = byName.get(topic);
      assert.ok(description instanceof TopicDescription);
      assert.equal(description.name, topic);
      assert.equal(description.isInternal, false);
      assert.equal(description.partitions.length, 2);
      assert.ok(description.partitions[0] instanceof TopicPartitionInfo);
      assert.ok(description.topicId?.equals(id));
      assert.equal(description.authorizedOperations, null);
      const byId = await retryUnknown(() =>
        present(admin.describeTopics({ topics: TopicCollection.ofTopicIds([id]), options: new DescribeTopicsOptions() }).allTopicIds()),
      );
      assert.equal(byId.get(id)?.name, topic);

      await admin
        .createPartitions({ newPartitions: { [topic]: NewPartitions.increaseTo(3) } })
        .all()
        .get();
      const grown = await retryUnknown(() => present(admin.describeTopics([topic]).allTopicNames()));
      assert.equal(grown.get(topic)?.partitions.length, 3);

      const records = new TopicPartitionMap([[new TopicPartition({ topic, partition: 0 }), RecordsToDelete.beforeOffset(0n)]]);
      const deleted = admin.deleteRecords(records);
      const low = await present(present(deleted.lowWatermarks()).get(new TopicPartition({ topic, partition: 0 }))).get();
      assert.ok(low instanceof DeletedRecords);
      assert.equal(low.lowWatermark(), 0n);

      await admin
        .deleteTopics(TopicCollection.ofTopicIds([id]))
        .all()
        .get();
      const gone = admin.deleteTopics([topic]);
      await assert.rejects(
        present(present(gone.topicNameValues()).get(topic)).get(),
        (error) => /** @type {Error} */ (error).cause instanceof UnknownTopicOrPartitionError,
      );
    } finally {
      await admin.close();
    }
  },
  120000,
);

/**
 * A future's value, retried while `accept` refuses it: a config change
 * reaches describeConfigs a little later.
 *
 * @template T
 * @param {() => import('@confluentinc/kafka-javascript/common').KafkaFuture<T>} call
 * @param {(value: T) => boolean} accept
 * @returns {Promise<T>}
 */
async function retryUntil(call, accept) {
  for (let attempt = 0; ; attempt++) {
    const value = await retryUnknown(call);
    if (accept(value) || attempt >= 40) return value;
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
}

(SKIP ? test.skip : test)(
  'describeConfigs, incrementalAlterConfigs and listConfigResources',
  async () => {
    const admin = Admin.create({ 'bootstrap.servers': BOOTSTRAP });
    const topic = `admin-configs-${process.pid}-${Date.now()}`;
    try {
      await admin
        .createTopics([new NewTopic({ name: topic, numPartitions: 1, replicationFactor: 1 })])
        .all()
        .get();
      const res = new ConfigResource({ type: ConfigResource.Type.TOPIC, name: topic });
      const controller = present(await admin.describeCluster().controller().get());
      const broker = new ConfigResource({ type: ConfigResource.Type.BROKER, name: String(controller.id) });

      const described = await retryUnknown(() =>
        admin.describeConfigs({ resources: [res, broker], options: new DescribeConfigsOptions({ includeSynonyms: true }) }).all(),
      );
      assert.ok(described instanceof ConfigResourceMap);
      const policy = described.get(res)?.get('cleanup.policy');
      assert.ok(policy instanceof ConfigEntry);
      assert.equal(policy.value(), 'delete');
      assert.ok(present(policy.synonyms()).length >= 1);
      assert.ok(present(described.get(broker)).get('log.dirs') instanceof ConfigEntry);

      const SET = AlterConfigOp.OpType.SET;
      const set = new AlterConfigOp({ configEntry: new ConfigEntry({ name: 'retention.ms', value: '123456' }), operationType: SET });
      // validateOnly changes nothing.
      await admin
        .incrementalAlterConfigs({
          configs: new ConfigResourceMap([[res, [set]]]),
          options: new AlterConfigsOptions({ validateOnly: true }),
        })
        .all()
        .get();
      const altered = admin.incrementalAlterConfigs(new ConfigResourceMap([[res, [set]]]));
      await present(altered.values().get(res)).get();
      const after = await retryUntil(
        () => admin.describeConfigs([res]).all(),
        (configs) => configs.get(res)?.get('retention.ms')?.value() === '123456',
      );
      assert.equal(after.get(res)?.get('retention.ms')?.value(), '123456');
      const unset = new AlterConfigOp({
        configEntry: new ConfigEntry({ name: 'retention.ms', value: null }),
        operationType: AlterConfigOp.OpType.DELETE,
      });
      await admin
        .incrementalAlterConfigs(new ConfigResourceMap([[res, [unset]]]))
        .all()
        .get();

      // An unknown topic: the broker's UnknownTopicOrPartitionError.
      const missing = new ConfigResource({ type: ConfigResource.Type.TOPIC, name: `${topic}-missing` });
      await assert.rejects(
        present(admin.describeConfigs([missing]).values().get(missing)).get(),
        (error) => /** @type {Error} */ (error).cause instanceof UnknownTopicOrPartitionError,
      );

      const listed = await retryUntil(
        () =>
          admin.listConfigResources({ configResourceTypes: [ConfigResource.Type.TOPIC], options: new ListConfigResourcesOptions() }).all(),
        (resources) => resources.some((r) => r.equals(res)),
      );
      assert.ok(listed.some((r) => r.equals(res)));
      for (const r of listed) assert.equal(r.type(), ConfigResource.Type.TOPIC);
      const everything = await admin.listConfigResources().all().get();
      assert.ok(everything.some((r) => r.equals(res)));
    } finally {
      await admin
        .deleteTopics([topic])
        .all()
        .get()
        .catch(() => {});
      await admin.close();
    }
  },
  60000,
);

(SKIP ? test.skip : test)(
  'describeLogDirs, describeReplicaLogDirs and alterReplicaLogDirs',
  async () => {
    const admin = Admin.create({ 'bootstrap.servers': BOOTSTRAP });
    const topic = `admin-log-dirs-${process.pid}-${Date.now()}`;
    try {
      await admin
        .createTopics([new NewTopic({ name: topic, numPartitions: 1, replicationFactor: 1 })])
        .all()
        .get();
      const broker = present(await admin.describeCluster().controller().get()).id;
      const tp = new TopicPartition({ topic, partition: 0 });
      const dirs = await retryUntil(
        () => present(admin.describeLogDirs([broker]).descriptions().get(broker)),
        (byPath) => [...byPath.values()].some((d) => d.replicaInfos.has(tp)),
      );
      // The scratch broker has one log directory.
      assert.equal(dirs.size, 1);
      const [[path, description]] = [...dirs];
      assert.ok(description instanceof LogDirDescription);
      assert.equal(description.error, null);
      assert.equal(description.isCordoned, false);
      assert.ok(description.totalBytes !== null && description.totalBytes > 0n);
      assert.ok(description.replicaInfos.get(tp)?.isFuture() === false);
      const all = await admin.describeLogDirs([broker]).allDescriptions().get();
      assert.ok(all.get(broker)?.has(path));

      const replica = new TopicPartitionReplica({ topic, partition: 0, brokerId: broker });
      const info = await present(admin.describeReplicaLogDirs([replica]).values().get(replica)).get();
      assert.equal(info.getCurrentReplicaLogDir(), path);
      assert.equal(info.getFutureReplicaLogDir(), null);
      assert.equal(info.getFutureReplicaOffsetLag(), -1n);

      // The scratch broker's only directory: a move there is no move; a
      // directory it has not is the broker's LogDirNotFoundError.
      await admin
        .alterReplicaLogDirs(new TopicPartitionReplicaMap([[replica, path]]))
        .all()
        .get();
      const elsewhere = admin.alterReplicaLogDirs(new TopicPartitionReplicaMap([[replica, '/no-such-log-dir']]));
      await assert.rejects(
        present(elsewhere.values().get(replica)).get(),
        (error) => /** @type {Error} */ (error).cause instanceof LogDirNotFoundError,
      );
    } finally {
      await admin
        .deleteTopics([topic])
        .all()
        .get()
        .catch(() => {});
      await admin.close();
    }
  },
  60000,
);

(SKIP ? test.skip : test)(
  'listOffsets, electLeaders, alterPartitionReassignments and listPartitionReassignments',
  async () => {
    const admin = Admin.create({ 'bootstrap.servers': BOOTSTRAP });
    const topic = `admin-offsets-${process.pid}-${Date.now()}`;
    const tp = new TopicPartition({ topic, partition: 0 });
    try {
      await admin
        .createTopics([new NewTopic({ name: topic, numPartitions: 1, replicationFactor: 1 })])
        .all()
        .get();
      const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, acks: 'all' } });
      try {
        for (const timestamp of [1000n, 2000n, 3000n]) {
          await (await producer.send(new ProducerRecord({ topic, partition: 0, timestamp, key: null, value: Buffer.from('v') }))).get();
        }
      } finally {
        await producer.close();
      }

      /**
       * The listed offset of the topic's partition for `spec`.
       *
       * @param {import('@confluentinc/kafka-javascript/admin').OffsetSpec} spec
       * @param {import('@confluentinc/kafka-javascript/admin').ListOffsetsOptions} [options]
       */
      const listed = (spec, options = new ListOffsetsOptions()) =>
        retryUnknown(() => admin.listOffsets({ topicPartitionOffsets: new TopicPartitionMap([[tp, spec]]), options }).partitionResult(tp));
      const earliest = await listed(OffsetSpec.earliest());
      assert.ok(earliest instanceof ListOffsetsResult.ListOffsetsResultInfo);
      assert.equal(earliest.offset(), 0n);
      assert.equal((await listed(OffsetSpec.latest())).offset(), 3n);
      assert.equal((await listed(OffsetSpec.latest(), new ListOffsetsOptions(IsolationLevel.READ_COMMITTED))).offset(), 3n);
      const byTime = await listed(OffsetSpec.forTimestamp(2000));
      assert.equal(byTime.offset(), 1n);
      assert.equal(byTime.timestamp(), 2000n);
      assert.equal(typeof byTime.leaderEpoch(), 'number');
      const max = await listed(OffsetSpec.maxTimestamp());
      assert.equal(max.offset(), 2n);
      assert.equal(max.timestamp(), 3000n);
      const all = await admin
        .listOffsets(new TopicPartitionMap([[tp, OffsetSpec.latest()]]))
        .all()
        .get();
      assert.equal(all.get(tp)?.offset(), 3n);

      // One broker: its only replica already leads, so a preferred election
      // is not needed.
      const elections = await admin
        .electLeaders({ electionType: ElectionType.PREFERRED, partitions: [tp] })
        .partitions()
        .get();
      assert.ok(elections.get(tp) instanceof ElectionNotNeededError, String(elections.get(tp)));
      await assert.rejects(
        admin
          .electLeaders({ electionType: ElectionType.PREFERRED, partitions: [tp] })
          .all()
          .get(),
        (error) => /** @type {Error} */ (error).cause instanceof ElectionNotNeededError,
      );

      // No reassignment in progress: none listed, and cancelling one is the
      // broker's NoReassignmentInProgressError.
      assert.equal((await admin.listPartitionReassignments().reassignments().get()).size(), 0);
      assert.equal((await admin.listPartitionReassignments([tp]).reassignments().get()).size(), 0);
      const cancelled = admin.alterPartitionReassignments(new TopicPartitionMap([[tp, null]]));
      await assert.rejects(
        present(cancelled.values().get(tp)).get(),
        (error) => /** @type {Error} */ (error).cause instanceof NoReassignmentInProgressError,
      );
      // A broker the cluster has not is an invalid assignment.
      const invalid = admin.alterPartitionReassignments(new TopicPartitionMap([[tp, new NewPartitionReassignment([999])]]));
      await assert.rejects(invalid.all().get(), (error) => /** @type {Error} */ (error).cause instanceof InvalidReplicaAssignmentError);
    } finally {
      await admin
        .deleteTopics([topic])
        .all()
        .get()
        .catch(() => {});
      await admin.close();
    }
  },
  60000,
);

(SKIP ? test.skip : test)(
  'the group calls: offsets committed to an empty group, listed, described, deleted; the group listed, its members removed, the group deleted',
  async () => {
    const admin = Admin.create({ 'bootstrap.servers': BOOTSTRAP });
    const topic = `js-admin-groups-${process.pid}-${Date.now()}`;
    const groupId = `js-admin-group-${process.pid}-${Date.now()}`;
    try {
      await admin
        .createTopics([new NewTopic({ name: topic, numPartitions: 2, replicationFactor: 1 })])
        .all()
        .get();
      const tp0 = new TopicPartition({ topic, partition: 0 });
      const tp1 = new TopicPartition({ topic, partition: 1 });

      // An empty group: its offsets can be altered, which creates it.
      const altered = admin.alterConsumerGroupOffsets({
        groupId,
        offsets: new TopicPartitionMap([
          [tp0, new OffsetAndMetadata({ offset: 5n, metadata: 'm' })],
          [tp1, new OffsetAndMetadata(7n)],
        ]),
      });
      assert.equal(await altered.all().get(), null);
      assert.equal(await altered.partitionResult(tp0).get(), null);

      const offsets = await admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
      assert.equal(offsets.get(tp0)?.offset, 5n);
      assert.equal(offsets.get(tp0)?.metadata, 'm');
      assert.equal(offsets.get(tp1)?.offset, 7n);
      const selected = await admin
        .listConsumerGroupOffsets({ groupSpecs: { [groupId]: new ListConsumerGroupOffsetsSpec({ topicPartitions: [tp1] }) } })
        .all()
        .get();
      assert.deepEqual([...selected.keys()], [groupId]);
      assert.equal(selected.get(groupId)?.get(tp1)?.offset, 7n);

      // The group is listed, as an empty classic group with no protocol.
      const listings = await admin.listGroups().all().get();
      const listing = listings.find((l) => l.groupId() === groupId);
      assert.ok(listing instanceof GroupListing, JSON.stringify(listings));
      assert.equal(listing.groupState(), GroupState.EMPTY);
      assert.equal(listing.type(), GroupType.CLASSIC);
      assert.ok(listing.isSimpleConsumerGroup());
      const stable = await admin
        .listGroups(new ListGroupsOptions({ groupStates: [GroupState.STABLE] }))
        .all()
        .get();
      assert.ok(!stable.some((l) => l.groupId() === groupId));

      const described = await admin.describeConsumerGroups([groupId]).all().get();
      const description = described.get(groupId);
      assert.ok(description instanceof ConsumerGroupDescription);
      assert.equal(description.groupId, groupId);
      assert.equal(description.groupState, GroupState.EMPTY);
      assert.deepEqual(description.members, []);
      assert.ok(description.coordinator instanceof Node);

      const classic = await admin.describeClassicGroups([groupId]).all().get();
      const classicDescription = classic.get(groupId);
      assert.ok(classicDescription instanceof ClassicGroupDescription);
      assert.equal(classicDescription.state, ClassicGroupState.EMPTY);
      assert.deepEqual(classicDescription.members, []);

      // A static member the group has not: the broker's UnknownMemberIdError.
      const member = new MemberToRemove('absent-instance');
      const removed = admin.removeMembersFromConsumerGroup({ groupId, options: new RemoveMembersFromConsumerGroupOptions([member]) });
      await assert.rejects(
        removed.memberResult(member).get(),
        (error) => /** @type {Error} */ (error).cause instanceof UnknownMemberIdError,
      );

      // An empty group's offsets can be deleted, and then the group.
      const offsetsDeleted = admin.deleteConsumerGroupOffsets({ groupId, partitions: [tp0] });
      assert.equal(await offsetsDeleted.all().get(), null);
      assert.equal(await offsetsDeleted.partitionResult(tp0).get(), null);
      const left = await admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
      assert.equal(left.has(tp0), false);
      assert.equal(await admin.deleteConsumerGroups([groupId]).all().get(), null);
      await assert.rejects(
        admin.deleteConsumerGroups([groupId]).all().get(),
        (error) => /** @type {Error} */ (error).cause instanceof GroupIdNotFoundError,
      );
    } finally {
      await admin
        .deleteTopics([topic])
        .all()
        .get()
        .catch(() => {});
      await admin.close();
    }
  },
  60000,
);

// ---- the ACL calls ---------------------------------------------------------------------------
//
// ACLs need an authorizer, which the scratch broker of `make test-integration`
// has not: there every ACL call fails with the broker's
// SecurityDisabledException, which this test checks. With an authorizer (P8's
// harness clusters), it creates, describes and deletes a binding instead.

const acl = require('@confluentinc/kafka-javascript/common/acl');
const res = require('@confluentinc/kafka-javascript/common/resource');
const { SecurityDisabledError } = require('@confluentinc/kafka-javascript/common/errors');

(SKIP ? test.skip : test)(
  'createAcls, describeAcls and deleteAcls: the binding round trip, or the broker refusing them without an authorizer',
  async () => {
    const admin = Admin.create({ 'bootstrap.servers': BOOTSTRAP });
    const name = `js-acl-${process.pid}-${Date.now()}`;
    const binding = new acl.AclBinding({
      pattern: new res.ResourcePattern({ resourceType: res.ResourceType.TOPIC, name, patternType: res.PatternType.LITERAL }),
      entry: new acl.AccessControlEntry({
        principal: 'User:js-test',
        host: '*',
        operation: acl.AclOperation.READ,
        permissionType: acl.AclPermissionType.ALLOW,
      }),
    });
    const filter = binding.toFilter();
    try {
      let described;
      try {
        described = await admin.describeAcls(filter).values().get();
      } catch (error) {
        const cause = /** @type {Error} */ (error).cause;
        assert.ok(cause instanceof SecurityDisabledError, String(cause));
        assert.equal(cause.message, 'No Authorizer is configured on the broker');
        const created = admin.createAcls([binding]);
        await assert.rejects(created.all().get(), (e) => {
          const c = /** @type {Error} */ (e).cause;
          return c instanceof SecurityDisabledError && c.message === 'No Authorizer is configured.';
        });
        const deleted = admin.deleteAcls([filter]);
        await assert.rejects(deleted.all().get(), (e) => {
          const c = /** @type {Error} */ (e).cause;
          return c instanceof SecurityDisabledError && c.message === 'No Authorizer is configured.';
        });
        console.log('the broker has no authorizer: the ACL round trip is skipped, its SecurityDisabledError checked');
        return;
      }
      assert.deepEqual(described, []);
      const created = admin.createAcls([binding]);
      assert.equal(await created.all().get(), null);
      const createdFuture = created.values().get(binding);
      assert.ok(createdFuture);
      assert.equal(await createdFuture.get(), null);
      let found = /** @type {Array<import('@confluentinc/kafka-javascript/common/acl').AclBinding>} */ ([]);
      for (let i = 0; i < 20 && found.length === 0; i++) {
        found = await admin.describeAcls(filter).values().get();
        if (found.length === 0) await new Promise((r) => setTimeout(r, 250));
      }
      assert.equal(found.length, 1);
      assert.ok(found[0].equals(binding), String(found[0]));
      const deleted = admin.deleteAcls([filter]);
      const results = deleted.values().get(filter);
      assert.ok(results);
      const values = (await results.get()).values();
      assert.equal(values.length, 1);
      assert.equal(values[0].exception(), null);
      assert.ok(values[0].binding()?.equals(binding));
      assert.deepEqual((await deleted.all().get()).map(String), [String(binding)]);
    } finally {
      await admin.close();
    }
  },
  60000,
);

// ---- the quota, SCRAM and delegation-token calls ---------------------------------------------
//
// Client quotas and SCRAM credentials work on a plain KRaft broker; the
// round trips alter, describe and remove them. Delegation tokens need a
// token secret and a SASL listener, which the scratch broker has not: there
// the broker refuses the calls, which the test checks with the broker's
// error, and with them (P8's harness clusters) it creates, describes, renews
// and expires a token.

const quotaPkg = require('@confluentinc/kafka-javascript/common/quota');
const { KafkaPrincipal: Principal } = require('@confluentinc/kafka-javascript/common/security/auth');
const securityAdmin = require('@confluentinc/kafka-javascript/admin');

(SKIP ? test.skip : test)(
  'alterClientQuotas and describeClientQuotas: set, read back and remove a quota',
  async () => {
    const admin = Admin.create({ 'bootstrap.servers': BOOTSTRAP });
    const user = `js-quota-${process.pid}-${Date.now()}`;
    const entity = new quotaPkg.ClientQuotaEntity(new Map([[quotaPkg.ClientQuotaEntity.USER, user]]));
    const filter = quotaPkg.ClientQuotaFilter.containsOnly([
      quotaPkg.ClientQuotaFilterComponent.ofEntity({ entityType: 'user', entityName: user }),
    ]);
    try {
      const op = (/** @type {number | null} */ value) => new quotaPkg.ClientQuotaAlteration.Op({ key: 'producer_byte_rate', value });
      const altered = admin.alterClientQuotas([new quotaPkg.ClientQuotaAlteration({ entity, ops: [op(1024)] })]);
      assert.equal(await altered.all().get(), null);
      /** @type {Map<string, number> | null} */
      let quotas = null;
      for (let i = 0; i < 20 && quotas === null; i++) {
        const entities = admin.describeClientQuotas(filter).entities();
        assert.ok(entities);
        quotas = (await entities.get()).get(entity);
        if (quotas === null) await new Promise((r) => setTimeout(r, 250));
      }
      assert.ok(quotas, 'the quota was not described');
      assert.deepEqual([...quotas], [['producer_byte_rate', 1024]]);
      // validateOnly alters nothing; a null value removes the quota.
      const validated = admin.alterClientQuotas({
        entries: [new quotaPkg.ClientQuotaAlteration({ entity, ops: [op(1)] })],
        options: new securityAdmin.AlterClientQuotasOptions({ validateOnly: true }),
      });
      assert.equal(await validated.all().get(), null);
      assert.equal(
        await admin
          .alterClientQuotas([new quotaPkg.ClientQuotaAlteration({ entity, ops: [op(null)] })])
          .all()
          .get(),
        null,
      );
      let removed = false;
      for (let i = 0; i < 20 && !removed; i++) {
        const entities = admin.describeClientQuotas(filter).entities();
        assert.ok(entities);
        removed = (await entities.get()).size() === 0;
        if (!removed) await new Promise((r) => setTimeout(r, 250));
      }
      assert.ok(removed, 'the quota was not removed');
    } finally {
      await admin.close();
    }
  },
  60000,
);

(SKIP ? test.skip : test)(
  'alterUserScramCredentials and describeUserScramCredentials: upsert, read back and delete a credential',
  async () => {
    const admin = Admin.create({ 'bootstrap.servers': BOOTSTRAP });
    const user = `js-scram-${process.pid}-${Date.now()}`;
    const info = new securityAdmin.ScramCredentialInfo({ mechanism: securityAdmin.ScramMechanism.SCRAM_SHA_256, iterations: 8192 });
    try {
      const upserted = admin.alterUserScramCredentials([
        new securityAdmin.UserScramCredentialUpsertion({ user, credentialInfo: info, password: 'secret' }),
      ]);
      assert.deepEqual([...upserted.values().keys()], [user]);
      assert.equal(await upserted.all().get(), null);
      /** @type {import('@confluentinc/kafka-javascript/admin').UserScramCredentialsDescription | null} */
      let description = null;
      for (let i = 0; i < 20 && description === null; i++) {
        try {
          description = await admin.describeUserScramCredentials([user]).description(user).get();
        } catch {
          await new Promise((r) => setTimeout(r, 250));
        }
      }
      assert.ok(description, 'the credential was not described');
      assert.equal(description.name, user);
      assert.equal(description.credentialInfos.length, 1);
      assert.ok(description.credentialInfos[0].equals(info), String(description.credentialInfos[0]));
      const result = admin.describeUserScramCredentials([user]);
      assert.deepEqual(await result.users().get(), [user]);
      assert.ok((await result.all().get()).get(user)?.equals(description));
      // A user that is not part of the request: Java's ResourceNotFoundException.
      await assert.rejects(result.description('nobody').get(), (e) => {
        const c = /** @type {Error} */ (e).cause;
        return c instanceof Error && c.name === 'ResourceNotFoundError' && c.message === 'No such user: nobody';
      });
      const deleted = admin.alterUserScramCredentials([
        new securityAdmin.UserScramCredentialDeletion({ user, mechanism: securityAdmin.ScramMechanism.SCRAM_SHA_256 }),
      ]);
      assert.equal(await deleted.all().get(), null);
    } finally {
      await admin.close();
    }
  },
  60000,
);

(SKIP ? test.skip : test)(
  'the delegation-token calls: the round trip, or the broker refusing them without a token secret or SASL',
  async () => {
    const admin = Admin.create({ 'bootstrap.servers': BOOTSTRAP });
    const owner = new Principal({ principalType: Principal.USER_TYPE, name: 'js-token' });
    try {
      let token;
      try {
        token = await admin
          .createDelegationToken(new securityAdmin.CreateDelegationTokenOptions({ renewers: [owner] }))
          .delegationToken()
          .get();
      } catch (error) {
        const cause = /** @type {Error} */ (error).cause;
        // Over PLAINTEXT the broker refuses the call before it checks for a
        // token secret: Java's UnsupportedByAuthenticationException.
        // With SASL but no token secret: Java's DelegationTokenDisabledException.
        // Both carry the broker's Errors text.
        const refusals = new Map([
          [
            'UnsupportedByAuthenticationError',
            'Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.',
          ],
          ['DelegationTokenDisabledError', 'Delegation Token feature is not enabled.'],
        ]);
        assert.ok(cause instanceof Error, String(cause));
        assert.ok(refusals.has(cause.name), String(cause));
        assert.equal(cause.message, refusals.get(cause.name));
        await assert.rejects(admin.describeDelegationToken().delegationTokens().get(), (e) => {
          const c = /** @type {Error} */ (e).cause;
          return c instanceof Error && c.name === cause.name && c.message === cause.message;
        });
        console.log(`the broker refuses delegation tokens (${cause.name}: ${cause.message}): the token round trip is skipped`);
        return;
      }
      const listed = await admin.describeDelegationToken().delegationTokens().get();
      assert.ok(listed.some((t) => t.equals(token)));
      const renewed = await admin.renewDelegationToken(token.hmac()).expiryTimestamp().get();
      assert.equal(typeof renewed, 'bigint');
      const expired = await admin.expireDelegationToken(token.hmac()).expiryTimestamp().get();
      assert.equal(typeof expired, 'bigint');
    } finally {
      await admin.close();
    }
  },
  60000,
);

// ---- the transaction and producer calls ------------------------------------------------------
//
// The JavaScript producer has no transactions in the preview, so no
// transaction is left open: fenceProducers initializes a transactional id
// (an InitProducerId with no transaction), which listTransactions and
// describeTransactions then see as Empty; forceTerminateTransaction fences
// it again; an idempotent producer gives describeProducers a producer to
// describe; abortTransaction, with no hanging transaction to abort, writes
// its abort marker all the same. The scratch broker's transaction log has one
// replica (the Makefile's KAFKA_TRANSACTION_STATE_LOG_* settings), as the Rust
// harness's clusters have.

const txnAdmin = require('@confluentinc/kafka-javascript/admin');

(SKIP ? test.skip : test)(
  'fenceProducers, listTransactions, describeTransactions, forceTerminateTransaction, describeProducers and abortTransaction',
  async () => {
    const admin = Admin.create({ 'bootstrap.servers': BOOTSTRAP });
    const topic = `admin-txn-${process.pid}-${Date.now()}`;
    const id = `js-txn-${process.pid}-${Date.now()}`;
    try {
      const fenced = admin.fenceProducers([id]);
      assert.equal(await fenced.all().get(), null);
      const producerId = await fenced.producerId(id).get();
      assert.equal(typeof producerId, 'bigint');
      assert.ok(producerId >= 0n);
      assert.equal(typeof (await fenced.epochId(id).get()), 'number');
      assert.equal(await present(fenced.fencedProducers().get(id)).get(), null);

      // The per-broker futures the outer future's value holds.
      const byBroker = await admin.listTransactions().byBrokerId().get();
      assert.ok(byBroker.size >= 1);
      for (const [broker, future] of byBroker) {
        assert.equal(typeof broker, 'number');
        assert.ok(Array.isArray(await future.get()));
      }
      const listed = await retryUntil(
        () => admin.listTransactions(new txnAdmin.ListTransactionsOptions({ filteredProducerIds: [producerId] })).all(),
        (listings) => listings.some((l) => l.transactionalId() === id),
      );
      const listing = present(listed.find((l) => l.transactionalId() === id));
      assert.ok(listing instanceof txnAdmin.TransactionListing);
      assert.equal(listing.producerId(), producerId);
      assert.equal(listing.state(), txnAdmin.TransactionState.EMPTY);
      const byState = await admin
        .listTransactions(new txnAdmin.ListTransactionsOptions({ filteredStates: [txnAdmin.TransactionState.ONGOING] }))
        .all()
        .get();
      assert.ok(!byState.some((l) => l.transactionalId() === id));
      assert.ok((await admin.listTransactions().allByBrokerId().get()).size >= 1);

      const description = await admin.describeTransactions([id]).description(id).get();
      assert.ok(description instanceof txnAdmin.TransactionDescription);
      assert.equal(description.state, txnAdmin.TransactionState.EMPTY);
      assert.equal(description.producerId, producerId);
      assert.equal(description.transactionStartTimeMs, null);
      assert.equal(description.topicPartitions?.size(), 0);
      assert.ok(description.transactionTimeoutMs > 0n);
      const unknown = await failureOf(admin.describeTransactions([`${id}-none`]).all());
      assert.equal(unknown.name, 'TransactionalIdNotFoundError', String(unknown));

      assert.equal(await admin.forceTerminateTransaction(id).result().get(), null);
      const after = await admin.describeTransactions([id]).description(id).get();
      assert.ok(after.producerEpoch > description.producerEpoch || after.producerId !== description.producerId, String(after));

      await admin
        .createTopics([new NewTopic({ name: topic, numPartitions: 1, replicationFactor: 1 })])
        .all()
        .get();
      const tp = new TopicPartition({ topic, partition: 0 });
      const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, acks: 'all', 'enable.idempotence': true } });
      try {
        await (await producer.send(new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from('v') }))).get();
      } finally {
        await producer.close();
      }
      const states = await retryUnknown(() => admin.describeProducers([tp]).partitionResult(tp));
      const active = present(states.activeProducers());
      assert.equal(active.length, 1);
      assert.ok(active[0] instanceof txnAdmin.ProducerState);
      assert.equal(active[0].lastSequence(), 0);
      assert.equal(active[0].currentTransactionStartOffset(), null);
      assert.equal(active[0].coordinatorEpoch(), null);
      assert.ok((await admin.describeProducers([tp]).all().get()).get(tp));

      const spec = new txnAdmin.AbortTransactionSpec({
        topicPartition: tp,
        producerId: active[0].producerId(),
        producerEpoch: active[0].producerEpoch(),
        coordinatorEpoch: 0,
      });
      assert.equal(await admin.abortTransaction(spec).all().get(), null);
    } finally {
      await admin.close();
    }
  },
  120000,
);

/**
 * The cause a future fails with.
 *
 * @param {import('@confluentinc/kafka-javascript/common').KafkaFuture<unknown>} future
 * @returns {Promise<Error>}
 */
async function failureOf(future) {
  try {
    await future.get();
  } catch (error) {
    return /** @type {Error} */ (/** @type {{ cause: unknown }} */ (error).cause);
  }
  assert.fail('the future succeeded');
}

const featureAdmin = require('@confluentinc/kafka-javascript/admin');
const {
  ControllerIdNotRegisteredError,
  InvalidUpdateVersionError,
  UnsupportedVersionError,
} = require('@confluentinc/kafka-javascript/common/errors');

(SKIP ? test.skip : test)(
  'describeFeatures, updateFeatures and unregisterController',
  async () => {
    const admin = Admin.create({ 'bootstrap.servers': BOOTSTRAP });
    try {
      const metadata = await admin.describeFeatures().featureMetadata().get();
      assert.ok(metadata instanceof featureAdmin.FeatureMetadata);
      const epoch = metadata.finalizedFeaturesEpoch();
      assert.equal(typeof epoch, 'bigint');
      const version = present(metadata.finalizedFeatures().get('metadata.version'));
      assert.ok(version instanceof featureAdmin.FinalizedVersionRange);
      assert.equal(version.minVersionLevel(), version.maxVersionLevel());
      const supported = present(metadata.supportedFeatures().get('metadata.version'));
      assert.ok(supported instanceof featureAdmin.SupportedVersionRange);
      assert.ok(supported.minVersion() <= version.maxVersionLevel() && version.maxVersionLevel() <= supported.maxVersion());
      // The node the request goes to, given: the single broker's own view.
      const fromNode = await admin
        .describeFeatures(new featureAdmin.DescribeFeaturesOptions({ nodeId: 1 }))
        .featureMetadata()
        .get();
      assert.ok(present(fromNode.finalizedFeatures().get('metadata.version')).equals(version));

      // validateOnly: an upgrade to the current level is valid and changes nothing.
      const same = new featureAdmin.FeatureUpdate({
        maxVersionLevel: version.maxVersionLevel(),
        upgradeType: featureAdmin.FeatureUpdate.UpgradeType.UPGRADE,
      });
      const validated = admin.updateFeatures({
        featureUpdates: { 'metadata.version': same },
        options: new featureAdmin.UpdateFeaturesOptions({ validateOnly: true }),
      });
      assert.deepEqual([...validated.values().keys()], ['metadata.version']);
      assert.equal(await validated.all().get(), null);

      // A feature the controller does not support: the broker's error.
      const unknown = admin.updateFeatures(
        new Map([
          [
            'no.such.feature',
            new featureAdmin.FeatureUpdate({ maxVersionLevel: 1, upgradeType: featureAdmin.FeatureUpdate.UpgradeType.UPGRADE }),
          ],
        ]),
      );
      const refused = await failureOf(unknown.all());
      assert.ok(refused instanceof InvalidUpdateVersionError, String(refused));
      assert.equal(
        refused.message,
        'The update failed for all features since the following feature had an error: Invalid update version 1 for feature ' +
          'no.such.feature. Local controller 1 does not support this feature.',
      );

      // No controller has this id. metadata.version 33 is the first to support
      // controller unregistration (KAFKA-20395): a broker that cannot reach it
      // (the pinned Kafka 4.3 image) does not offer the RPC, which the core
      // reports itself; one finalized below it refuses the request; otherwise
      // the controller refuses the unknown id.
      const UNREGISTER_CONTROLLER_LEVEL = 33;
      const [expectedClass, expectedMessage] =
        supported.maxVersion() < UNREGISTER_CONTROLLER_LEVEL
          ? [UnsupportedVersionError, 'The node does not support UNREGISTER_CONTROLLER']
          : version.maxVersionLevel() < UNREGISTER_CONTROLLER_LEVEL
            ? [UnsupportedVersionError, 'The current MetadataVersion is too old to support controller unregistration.']
            : [ControllerIdNotRegisteredError, 'Controller ID 9999 is not currently registered.'];
      const unregistered = await failureOf(admin.unregisterController(9999).all());
      assert.ok(unregistered instanceof expectedClass, String(unregistered));
      assert.equal(unregistered.message, expectedMessage);
    } finally {
      await admin.close();
    }
  },
  60000,
);

// listTransactions().byBrokerId()'s per-broker futures are borrowed from the
// outer future's value, so each holds its parent until it is freed. Only a
// broker answers listTransactions (the mock does not implement it), so the
// native counts are checked here, in a child process that can collect its
// garbage, as the unit test's lifetime test checks the other calls'.
(SKIP ? test.skip : test)(
  'listTransactions().byBrokerId(): the per-broker futures free their parent, read, unread and kept',
  () => {
    const script = `
    const native = require('./dist/internal/native.js');
    const { Admin } = require('@confluentinc/kafka-javascript/admin');
    (async () => {
      const before = native.adminStats();
      const kept = [];
      async function round() {
        const a = Admin.create({ 'bootstrap.servers': ${JSON.stringify(String(BOOTSTRAP))} });
        const byBroker = await a.listTransactions().byBrokerId().get();
        for (const future of byBroker.values()) await future.get();
        kept.push([...byBroker.values()][0]);
        await a.listTransactions().all().get();
        a.listTransactions();
        await a.close();
      }
      for (let i = 0; i < 2; i++) await round();
      for (let i = 0; i < 20; i++) { global.gc(); await new Promise((r) => setTimeout(r, 20)); }
      // A kept per-broker future, its Result and outer future gone, still reads.
      const read = await Promise.all(kept.map((f) => f.get()));
      kept.length = 0;
      let after = native.adminStats();
      for (let i = 0; i < 100 && after.adminsFreed - before.adminsFreed < 2; i++) {
        global.gc();
        await new Promise((r) => setTimeout(r, 20));
        after = native.adminStats();
      }
      const d = (k) => after[k] - before[k];
      console.log(JSON.stringify([read.every(Array.isArray), d('adminsMade'), d('adminsFreed'), d('resultsMade') === d('resultsFreed'), d('futuresMade') === d('futuresFreed'), d('waitsStarted') === d('waitsCompleted')]));
    })();
  `;
    const r = spawnSync(process.execPath, ['--no-warnings', '--expose-gc', '-e', script], {
      cwd: path.join(__dirname, '..', '..'),
      encoding: 'utf8',
      timeout: 60000,
    });
    assert.equal(r.status, 0, r.stderr);
    assert.equal(r.stdout, '[true,2,2,true,true,true]\n');
  },
  90000,
);
