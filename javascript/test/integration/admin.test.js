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
  CreateTopicsOptions,
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
  Uuid,
} = require('@confluentinc/kafka-javascript/common');
const { TopicExistsError, UnknownTopicOrPartitionError } = require('@confluentinc/kafka-javascript/common/errors');

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
