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

const {
  Admin,
  AlterConfigOp,
  AlterConfigsOptions,
  ConfigEntry,
  CreateTopicsOptions,
  DescribeConfigsOptions,
  ListConfigResourcesOptions,
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
const { LogDirNotFoundError, TopicExistsError, UnknownTopicOrPartitionError } = require('@confluentinc/kafka-javascript/common/errors');

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
