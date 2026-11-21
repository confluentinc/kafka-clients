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

// MockAdminClient over the FFI's mock. Java's MockAdminClientTest tests only
// describeStreamsGroups, which the C FFI does not have, so, as the root
// conventions ask of a class Java ships without tests of its own, these are
// the Rust core's tests of its translation (rust/src/admin/mock_admin_client.rs),
// for the cluster, the builder and the topic calls, read through the
// public API; then the binding's own: the constructors' and setters' forms.

const assert = require('node:assert/strict');

const { ExecutionError, IllegalArgumentError, UnsupportedOperationError } = require('@confluentinc/kafka-javascript');
const {
  AlterConfigOp,
  AlterConfigsOptions,
  ConfigEntry,
  CreateTopicsOptions,
  DeleteTopicsOptions,
  DescribeClusterOptions,
  DescribeLogDirsOptions,
  DescribeReplicaLogDirsOptions,
  ListConfigResourcesOptions,
  DescribeTopicsOptions,
  ListTopicsOptions,
  MockAdminClient,
  NewPartitions,
  NewTopic,
  RecordsToDelete,
} = require('@confluentinc/kafka-javascript/admin');
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
const {
  InvalidRequestError,
  InvalidReplicationFactorError,
  KafkaStorageError,
  ReplicaNotAvailableError,
  TimeoutError,
  TopicExistsError,
  UnknownTopicIdError,
  UnknownTopicOrPartitionError,
} = require('@confluentinc/kafka-javascript/common/errors');

/**
 * @template T
 * @typedef {import('@confluentinc/kafka-javascript/common').KafkaFuture<T>} KafkaFuture
 */
/** @typedef {import('@confluentinc/kafka-javascript/admin').TopicDescription} TopicDescription */

/** @param {number} id */
const node = (id) => new Node({ id, host: `h${id}`, port: 9092 });
const fourNodes = () => [0, 1, 2, 3].map(node);

/**
 * The broker ids describeCluster reports, and its controller's.
 *
 * @param {MockAdminClient} mock
 */
async function cluster(mock) {
  const result = mock.describeCluster();
  const nodes = await result.nodes().get();
  const controller = await result.controller().get();
  return { nodes, controller };
}

/**
 * The future's failure, the cause of its ExecutionError.
 *
 * @param {import('@confluentinc/kafka-javascript/common').KafkaFuture<unknown>} future
 * @returns {Promise<unknown>}
 */
async function failure(future) {
  try {
    await future.get();
  } catch (error) {
    assert.ok(error instanceof ExecutionError, String(error));
    return error.cause;
  }
  assert.fail('the future succeeded');
}

/** @type {MockAdminClient[]} */
const opened = [];

/**
 * A mock closed after the test, as `await using` would (Node 22 has no
 * `await using`).
 *
 * @param {MockAdminClient} mock
 * @returns {MockAdminClient}
 */
function track(mock) {
  opened.push(mock);
  return mock;
}

afterEach(async () => {
  for (const mock of opened.splice(0)) await mock.close();
});

test('MockAdminClient(): one broker, Node.noNode(), also the controller', async () => {
  const mock = track(new MockAdminClient());
  const { nodes, controller } = await cluster(mock);
  assert.deepEqual(nodes.map(String), [Node.noNode().toString()]);
  assert.ok(controller && controller.equals(Node.noNode()));
});

test('with_brokers_controller_rejects_a_controller_outside_the_brokers', () => {
  assert.throws(() => new MockAdminClient({ brokers: fourNodes(), controller: node(7) }), {
    name: 'IllegalArgumentError',
    message: 'The controller node must be in the list of brokers',
  });
});

test('set_controller_requires_a_broker', async () => {
  const nodes = fourNodes();
  const mock = track(new MockAdminClient({ brokers: nodes, controller: nodes[2] }));
  assert.equal((await cluster(mock)).controller?.id, 2);
  mock.controller(nodes[3]);
  assert.equal((await cluster(mock)).controller?.id, 3);
  assert.throws(() => mock.controller(node(7)), {
    name: 'IllegalArgumentError',
    message: 'The controller node must be in the list of brokers',
  });
  assert.equal((await cluster(mock)).controller?.id, 3);
});

test('set_num_brokers_grows_the_broker_and_log_dir_lists', async () => {
  const mock = track(MockAdminClient.create().numBrokers(5).build());
  const { nodes } = await cluster(mock);
  assert.deepEqual(
    nodes.map((n) => [n.id, n.host, n.port]),
    [0, 1, 2, 3, 4].map((id) => [id, 'localhost', 1000 + id]),
  );
});

test('set_num_brokers_shrinks_both_lists_to_their_prefix', async () => {
  const mock = track(
    MockAdminClient.create()
      .numBrokers(3)
      .brokerLogDirs([['a'], ['b'], ['c']])
      .numBrokers(2)
      .build(),
  );
  const { nodes } = await cluster(mock);
  assert.deepEqual(
    nodes.map((n) => [n.id, n.port]),
    [
      [0, 1000],
      [1, 1001],
    ],
  );
});

test('shrinking_past_a_short_log_dir_list_fails_like_sub_list', () => {
  const short = () =>
    MockAdminClient.create()
      .numBrokers(3)
      .brokerLogDirs([['a']]);
  assert.throws(() => short().numBrokers(2), { name: 'IllegalArgumentError', message: 'toIndex = 2' });
  // brokers(list) resizes through numBrokers(list.size()) first.
  assert.throws(() => short().brokers([node(7), node(8)]), { name: 'IllegalArgumentError', message: 'toIndex = 2' });
});

test('a_negative_broker_count_fails_like_sub_list_even_when_empty', () => {
  assert.throws(() => MockAdminClient.create().numBrokers(0).numBrokers(-3), {
    name: 'IllegalArgumentError',
    message: 'fromIndex(0) > toIndex(-3)',
  });
});

test('set_brokers_resizes_the_log_dirs_before_replacing_the_brokers', async () => {
  const nodes = [5, 6, 7].map(node);
  const mock = track(
    MockAdminClient.create()
      .brokerLogDirs([['custom']])
      .brokers(nodes)
      .build(),
  );
  const { nodes: described, controller } = await cluster(mock);
  assert.deepEqual(
    described.map((n) => n.id),
    [5, 6, 7],
  );
  // brokers.get(0) of the new list.
  assert.equal(controller?.id, 5);
});

test('set_controller_picks_the_broker_at_index_or_fails_at_the_call', async () => {
  const three = () => MockAdminClient.create().numBrokers(3);
  const mock = track(three().controller(1).build());
  const { controller } = await cluster(mock);
  assert.deepEqual([controller?.id, controller?.host, controller?.port], [1, 'localhost', 1001]);
  assert.throws(() => three().controller(3), { name: 'IllegalArgumentError', message: 'Index 3 out of bounds for length 3' });
  assert.throws(() => three().controller(-1), { name: 'IllegalArgumentError', message: 'Index -1 out of bounds for length 3' });
});

test('build_rejects_a_controller_no_longer_in_the_broker_list', () => {
  assert.throws(() => MockAdminClient.create().numBrokers(3).controller(2).numBrokers(1).build(), {
    name: 'IllegalArgumentError',
    message: 'The controller node must be in the list of brokers',
  });
  assert.throws(
    () =>
      MockAdminClient.create()
        .controller(0)
        .brokers([node(9)])
        .build(),
    {
      name: 'IllegalArgumentError',
      message: 'The controller node must be in the list of brokers',
    },
  );
});

test('builder_setters_reach_the_built_mock: the cluster id', async () => {
  const mock = track(
    MockAdminClient.create()
      .clusterId('my-cluster')
      .defaultPartitions(7)
      .defaultReplicationFactor(2)
      .usingRaftController(true)
      .featureLevels(new Map([['f', 3]]))
      .minSupportedFeatureLevels({ minSupportedFeatureLevels: { f: 1 } })
      .maxSupportedFeatureLevels(new Map([['f', 5]]))
      .defaultGroupConfigs(new Map([['group.session.timeout.ms', '45000']]))
      .build(),
  );
  assert.equal(await mock.describeCluster().clusterId().get(), 'my-cluster');
});

test('controller_is_always_one_of_the_seeded_nodes', async () => {
  for (let numBrokers = 1; numBrokers <= 3; numBrokers++) {
    const mock = track(MockAdminClient.create().numBrokers(numBrokers).build());
    const { nodes, controller } = await cluster(mock);
    assert.ok(controller);
    assert.ok(nodes.some((n) => n.equals(controller)));
    assert.equal(controller.id, 0);
    assert.equal(nodes.length, numBrokers);
  }
});

test('describe_cluster_returns_brokers_and_controller', async () => {
  const mock = track(MockAdminClient.create().numBrokers(3).build());
  const result = mock.describeCluster(new DescribeClusterOptions());
  assert.equal((await result.nodes().get()).length, 3);
  assert.equal((await result.controller().get())?.id, 0);
  assert.equal(await result.clusterId().get(), MockAdminClient.DEFAULT_CLUSTER_ID);
  const operations = await result.authorizedOperations().get();
  assert.ok(operations);
  assert.equal(operations.size, 0);
});

test('describe_cluster_timeout_recovers_on_next_call', async () => {
  const mock = track(MockAdminClient.create().numBrokers(3).build());
  mock.timeoutNextRequest(1);
  const timedOut = mock.describeCluster();
  for (const future of [timedOut.nodes(), timedOut.controller(), timedOut.clusterId()]) {
    const cause = await failure(future);
    assert.ok(cause instanceof TimeoutError, String(cause));
  }
  assert.equal((await mock.describeCluster().nodes().get()).length, 3);
});

test('listTopics on an empty mock, and its timeout', async () => {
  const mock = track(MockAdminClient.create().build());
  const result = mock.listTopics({ options: new ListTopicsOptions({ listInternal: true }) });
  assert.equal((await result.namesToListings().get()).size, 0);
  assert.deepEqual(await result.listings().get(), []);
  assert.deepEqual([...(await result.names().get())], []);
  mock.timeoutNextRequest(1);
  const cause = await failure(mock.listTopics().namesToListings());
  assert.ok(cause instanceof TimeoutError);
  assert.equal(/** @type {Error} */ (cause).message, 'The mock timed out the request.');
});

test('the futures are done once the mock answered, and getNow reads them', async () => {
  const mock = track(new MockAdminClient());
  const result = mock.describeCluster();
  await result.clusterId().get();
  assert.equal(result.clusterId().isDone(), true);
  assert.equal(result.clusterId().getNow(null), MockAdminClient.DEFAULT_CLUSTER_ID);
});

test("(deviation) a mock call's futures show their outcome on a later event-loop turn; await get() is unaffected", async () => {
  // Java's mock completes them before returning; the core answers them from
  // a task of its own (MockAdminClient's documentation), whose answer the
  // binding runs only on a later turn, so a future is never done when the call
  // returns. Only the turns after are checked.
  const mock = track(new MockAdminClient());
  const result = mock.describeCluster();
  const clusterId = result.clusterId();
  assert.equal(await clusterId.get(), MockAdminClient.DEFAULT_CLUSTER_ID);
  const nodes = result.nodes();
  const timedOut = (mock.timeoutNextRequest(1), mock.listTopics().namesToListings());
  // The core's tasks answer within a few turns of the event loop.
  for (let i = 0; i < 100 && !(nodes.isDone() && timedOut.isDone()); i++) await new Promise((resolve) => setImmediate(resolve));
  assert.equal(nodes.isDone(), true);
  assert.equal(nodes.getNow({ valueIfAbsent: null })?.length, 1);
  assert.equal(timedOut.isCompletedExceptionally(), true);
});

test("the Builder's brokerLogDirs takes any iterable of iterables, never a string", async () => {
  const mock = track(
    new MockAdminClient.Builder()
      .numBrokers(2)
      .brokerLogDirs(new Set([new Set(['/a']), ['/b']]))
      .build(),
  );
  assert.equal((await mock.describeCluster().nodes().get()).length, 2);
  // A string is an iterable of strings to tsc; Java's List<String> is no
  // string, which the run-time check refuses.
  assert.throws(() => new MockAdminClient.Builder().brokerLogDirs(['/a']), {
    name: 'TypeError',
    message: 'brokerLogDirs must be an iterable of iterables of strings, not one holding a string',
  });
});

test("defaultPartitions names its own parameter in Java's short check", () => {
  assert.throws(() => new MockAdminClient.Builder().defaultPartitions(1000000), {
    name: 'RangeError',
    message: 'numPartitions must be from -32768 to 32767',
  });
  assert.throws(() => new MockAdminClient.Builder().defaultPartitions(1.5), {
    name: 'TypeError',
    message: 'numPartitions must be an integer',
  });
});

test("the Results' readers only read: an argument is a TypeError", () => {
  const mock = track(new MockAdminClient());
  // The readers called with an argument tsc refuses: as a plain JavaScript caller calls them.
  const loose = (/** @type {object} */ o) => /** @type {Record<string, (x: unknown) => unknown>} */ (/** @type {unknown} */ (o));
  const cluster = loose(mock.describeCluster());
  const topics = loose(mock.listTopics());
  for (const name of ['nodes', 'controller', 'clusterId', 'authorizedOperations']) {
    assert.throws(() => cluster[name](1), { name: 'TypeError', message: `${name}() only reads: it takes no argument` });
  }
  for (const name of ['namesToListings', 'listings', 'names']) {
    assert.throws(() => topics[name](1), { name: 'TypeError', message: `${name}() only reads: it takes no argument` });
  }
});

test("the Builder's keyed constructor runs the setters in Java's field order", async () => {
  const mock = track(new MockAdminClient.Builder({ controller: 2, brokers: [node(4), node(5), node(6)], clusterId: 'k' }).build());
  const { nodes, controller } = await cluster(mock);
  assert.deepEqual(
    nodes.map((n) => n.id),
    [4, 5, 6],
  );
  assert.equal(controller?.id, 6);
  assert.equal(await mock.describeCluster().clusterId().get(), 'k');
  // @ts-expect-error numBrokers is a setter with no field of its own: no key
  assert.throws(() => new MockAdminClient.Builder({ numBrokers: 1 }), {
    name: 'IllegalArgumentError',
    message:
      'Builder() takes an object of the keys (clusterId, brokers, controller, brokerLogDirs, defaultPartitions, usingRaftController, defaultReplicationFactor, featureLevels, minSupportedFeatureLevels, maxSupportedFeatureLevels, defaultGroupConfigs); got (numBrokers)',
  });
});

// ---- the topics (the Rust core's tests of the mock's topic calls) ------------------

/** The Rust core's `admin()`: a Builder's mock of three brokers. */
const admin = () => track(new MockAdminClient.Builder().numBrokers(3).build());
/** @param {number} id the node a Builder seeds */
const seededBroker = (id) => new Node({ id, host: 'localhost', port: 1000 + id });
const unknownBroker = () => new Node({ id: 99, host: 'elsewhere', port: 9999 });
/**
 * @param {number} partition
 * @param {Node | null} leader
 * @param {Node[]} replicas
 * @param {Node[]} isr
 */
const partitionInfo = (partition, leader, replicas, isr) =>
  new TopicPartitionInfo({ partition, leader, replicas, isr, elr: [], lastKnownElr: [] });
/**
 * @param {string} name
 * @param {number} numPartitions
 * @param {number} replicationFactor
 */
const newTopic = (name, numPartitions, replicationFactor) => new NewTopic({ name, numPartitions, replicationFactor });

test('create_topics_on_new_fails_for_the_no_node_broker_as_java_does', async () => {
  const mock = track(new MockAdminClient());
  const result = mock.createTopics({ newTopics: [newTopic('t', 1, 1)], options: new CreateTopicsOptions() });
  const error = await failure(result.all());
  assert.ok(error instanceof IllegalArgumentError, String(error));
  assert.equal(/** @type {Error} */ (error).message, 'Index -1 out of bounds for length 1');
});

test('create_then_list_and_describe', async () => {
  const client = admin();
  const result = client.createTopics({ newTopics: [newTopic('t', 2, 2)], options: new CreateTopicsOptions() });
  await result.all().get();
  assert.equal(await result.numPartitions('t').get(), 2);
  assert.equal(await result.replicationFactor('t').get(), 2);
  const names = await client.listTopics(new ListTopicsOptions()).names().get();
  assert.ok(names.has('t'));
  const desc = /** @type {KafkaFuture<Map<string, TopicDescription>>} */ (
    client.describeTopics({ topics: TopicCollection.ofTopicNames(['t']), options: new DescribeTopicsOptions() }).allTopicNames()
  );
  assert.equal((await desc.get()).get('t')?.partitions.length, 2);
});

test('create_existing_topic_fails_with_topic_exists', async () => {
  const client = admin();
  await client
    .createTopics([newTopic('t', 1, 1)])
    .all()
    .get();
  const result = client.createTopics([newTopic('t', 1, 1)]);
  const error = await failure(/** @type {KafkaFuture<void>} */ (result.values().get('t')));
  assert.ok(error instanceof TopicExistsError, String(error));
  assert.equal(error.message, 'Topic t exists already.');
});

test('create_with_replication_factor_too_large_fails', async () => {
  const client = track(new MockAdminClient.Builder().build());
  const result = client.createTopics([newTopic('t', 1, 5)]);
  const error = await failure(/** @type {KafkaFuture<void>} */ (result.values().get('t')));
  assert.ok(error instanceof InvalidReplicationFactorError, String(error));
});

test('mock_create_topics_rejects_a_leader_with_no_log_directories', async () => {
  const client = track(new MockAdminClient.Builder().brokerLogDirs([[]]).build());
  const result = client.createTopics([newTopic('t', 1, 1)]);
  const error = await failure(/** @type {KafkaFuture<void>} */ (result.values().get('t')));
  assert.equal(/** @type {Error} */ (error).message, 'Broker 0 has no log directories.');
});

test('describe_nonexistent_topic_is_unknown_topic', async () => {
  const client = admin();
  const result = client.describeTopics(TopicCollection.ofTopicNames(['missing']));
  const error = await failure(/** @type {KafkaFuture<TopicDescription>} */ (result.topicNameValues()?.get('missing')));
  assert.ok(error instanceof UnknownTopicOrPartitionError, String(error));
  assert.equal(error.message, 'Topic missing not found.');
});

test('delete_then_gone', async () => {
  const client = admin();
  await client
    .createTopics([newTopic('t', 1, 1)])
    .all()
    .get();
  await client
    .deleteTopics({ topics: TopicCollection.ofTopicNames(['t']), options: new DeleteTopicsOptions() })
    .all()
    .get();
  const names = await client.listTopics().names().get();
  assert.ok(!names.has('t'));
});

test('delete_missing_topic_fails', async () => {
  const client = admin();
  const result = client.deleteTopics(TopicCollection.ofTopicNames(['nope']));
  const error = await failure(/** @type {KafkaFuture<void>} */ (result.topicNameValues()?.get('nope')));
  assert.ok(error instanceof UnknownTopicOrPartitionError, String(error));
});

test('timeout_next_request_times_out_create', async () => {
  const client = admin();
  client.timeoutNextRequest(1);
  const result = client.createTopics([newTopic('t', 1, 1)]);
  assert.ok((await failure(/** @type {KafkaFuture<void>} */ (result.values().get('t')))) instanceof TimeoutError);
  await client
    .createTopics([newTopic('t2', 1, 1)])
    .all()
    .get();
});

test('mock_add_topic_rejects_a_duplicate', async () => {
  const mock = admin();
  const partitions = [partitionInfo(0, seededBroker(0), [seededBroker(0)], [])];
  mock.addTopic({ internal: false, name: 'topic', partitions, configs: null });
  assert.throws(() => mock.addTopic({ internal: false, name: 'topic', partitions, configs: null }), {
    name: 'IllegalArgumentError',
    message: 'Topic topic was already added.',
  });
});

test('mock_add_topic_rejects_an_unknown_leader', async () => {
  const mock = admin();
  assert.throws(
    () =>
      mock.addTopic({
        internal: false,
        name: 'topic',
        partitions: [partitionInfo(0, unknownBroker(), [seededBroker(0)], [])],
        configs: null,
      }),
    { message: 'Leader broker unknown' },
  );
  assert.throws(
    () => mock.addTopic({ internal: false, name: 'other', partitions: [partitionInfo(0, null, [seededBroker(0)], [])], configs: null }),
    { message: 'Leader broker unknown' },
  );
  assert.equal((await mock.listTopics().names().get()).size, 0);
});

test('mock_add_topic_rejects_unknown_brokers_in_the_replica_and_isr_lists', () => {
  const mock = admin();
  assert.throws(
    () =>
      mock.addTopic({
        internal: false,
        name: 'topic',
        partitions: [partitionInfo(0, seededBroker(0), [seededBroker(0), unknownBroker()], [])],
        configs: null,
      }),
    { message: 'Unknown brokers in replica list' },
  );
  assert.throws(
    () =>
      mock.addTopic({
        internal: false,
        name: 'topic',
        partitions: [partitionInfo(0, seededBroker(0), [seededBroker(0)], [unknownBroker()])],
        configs: null,
      }),
    { message: 'Unknown brokers in isr list' },
  );
});

test('mock_add_topic_keeps_its_configs, and markTopicForDeletion hides it from describeTopics', async () => {
  const mock = admin();
  const partitions = [partitionInfo(0, seededBroker(0), [seededBroker(0)], [seededBroker(0)])];
  mock.addTopic({ internal: false, name: 't', partitions, configs: { 'cleanup.policy': 'compact' } });
  const described = await /** @type {KafkaFuture<Map<string, TopicDescription>>} */ (mock.describeTopics(['t']).allTopicNames()).get();
  assert.equal(described.get('t')?.name, 't');
  mock.markTopicForDeletion('t');
  const error = await failure(/** @type {KafkaFuture<TopicDescription>} */ (mock.describeTopics(['t']).topicNameValues()?.get('t')));
  assert.ok(error instanceof UnknownTopicOrPartitionError, String(error));
  assert.throws(() => mock.markTopicForDeletion('nope'), { name: 'IllegalArgumentError', message: 'Topic nope did not exist.' });
});

test('describe_topics_by_name_and_by_id_hide_the_topic_until_visible', async () => {
  const mock = admin();
  mock.addTopic({
    internal: false,
    name: 't',
    partitions: [partitionInfo(0, seededBroker(0), [seededBroker(0)], [seededBroker(0)])],
    configs: null,
  });
  const topicId = /** @type {TopicDescription} */ (
    (await /** @type {KafkaFuture<Map<string, TopicDescription>>} */ (mock.describeTopics(['t']).allTopicNames()).get()).get('t')
  ).topicId;
  mock.setFetchesRemainingUntilVisible({ topicName: 't', fetchesRemainingUntilVisible: 2 });
  for (let i = 0; i < 2; i++) {
    const error = await failure(/** @type {KafkaFuture<TopicDescription>} */ (mock.describeTopics(['t']).topicNameValues()?.get('t')));
    assert.ok(error instanceof UnknownTopicOrPartitionError, String(error));
    assert.equal(error.message, 'Topic t not found.');
  }
  const visible = await /** @type {KafkaFuture<TopicDescription>} */ (mock.describeTopics(['t']).topicNameValues()?.get('t')).get();
  assert.equal(visible.name, 't');
  mock.setFetchesRemainingUntilVisible({ topicName: 't', fetchesRemainingUntilVisible: 2 });
  const byId = () => mock.describeTopics(TopicCollection.ofTopicIds([/** @type {Uuid} */ (topicId)]));
  for (let i = 0; i < 2; i++) {
    const error = await failure(/** @type {KafkaFuture<TopicDescription>} */ (byId().topicIdValues()?.get(topicId)));
    assert.ok(error instanceof UnknownTopicIdError, String(error));
    assert.equal(error.message, `Topic id${topicId} not found.`);
  }
  assert.equal((await /** @type {KafkaFuture<TopicDescription>} */ (byId().topicIdValues()?.get(topicId)).get()).name, 't');
});

test('set_fetches_remaining_until_visible_rejects_an_unknown_topic', () => {
  assert.throws(() => admin().setFetchesRemainingUntilVisible({ topicName: 'nope', fetchesRemainingUntilVisible: 1 }), {
    name: 'IllegalArgumentError',
    message: 'No such topic as nope',
  });
});

test("createPartitions and deleteRecords fail with the mock's UnsupportedOperationError, as Java's mock throws", async () => {
  const client = admin();
  await client
    .createTopics([newTopic('t', 1, 1)])
    .all()
    .get();
  const cp = client.createPartitions({ newPartitions: { t: NewPartitions.increaseTo(2) } });
  const error = await failure(cp.all());
  assert.ok(error instanceof UnsupportedOperationError, String(error));
  assert.equal(error.message, 'Not implemented yet');
  const records = new TopicPartitionMap([[new TopicPartition({ topic: 't', partition: 0 }), RecordsToDelete.beforeOffset(1n)]]);
  const dr = client.deleteRecords(records);
  assert.deepEqual([...(dr.lowWatermarks()?.keys() ?? [])].map(String), ['t-0']);
  assert.ok((await failure(dr.all())) instanceof UnsupportedOperationError);
});

test("createTopics' refinements: the config, the id, and a topic not in the request", async () => {
  const client = admin();
  const result = client.createTopics([newTopic('t', 1, 1).configs({ 'cleanup.policy': 'compact' })]);
  assert.deepEqual([...result.values().keys()], ['t']);
  assert.notEqual(result.values(), result.values());
  const config = await result.config('t').get();
  assert.equal(config?.get('cleanup.policy')?.value(), 'compact');
  assert.ok((await result.topicId({ topic: 't' }).get()) instanceof Uuid);
  const error = await failure(result.config('nope'));
  assert.ok(error instanceof IllegalArgumentError, String(error));
  assert.equal(error.message, 'Topic nope was not part of the createTopics request');
  // @ts-expect-error null: Java's map has no future for it
  assert.throws(() => result.config(null), { name: 'TypeError', message: 'topic must not be null' });
});

test("a null argument or element: Java's NullPointerException as a TypeError naming the RPC's parameter", () => {
  const client = admin();
  // @ts-expect-error null: Java's describeTopics dereferences it
  assert.throws(() => client.describeTopics({ topicNames: null }), { name: 'TypeError', message: 'topicNames must not be null' });
  assert.throws(() => client.describeTopics({ topicNames: 'abc' }), {
    name: 'TypeError',
    message: 'topicNames must be an iterable of strings, not string',
  });
  // @ts-expect-error a null element
  assert.throws(() => client.createTopics([null]), { name: 'TypeError', message: 'newTopics must not contain null' });
  // @ts-expect-error a null value
  assert.throws(() => client.createPartitions({ newPartitions: { t: null } }), {
    name: 'TypeError',
    message: 'newPartitions must not contain null',
  });
  const nullValue = /** @type {TopicPartitionMap<RecordsToDelete>} */ (
    /** @type {unknown} */ (new TopicPartitionMap([[new TopicPartition({ topic: 't', partition: 0 }), null]]))
  );
  assert.throws(() => client.deleteRecords(nullValue), { name: 'TypeError', message: 'recordsToDelete must not contain null' });
  const nullKey = new TopicPartitionMap([[null, RecordsToDelete.beforeOffset(1n)]]);
  assert.throws(() => client.deleteRecords(nullKey), { name: 'TypeError', message: 'recordsToDelete must not contain a null key' });
  const nullId = TopicCollection.ofTopicIds(/** @type {Uuid[]} */ (/** @type {unknown} */ ([null])));
  assert.throws(() => client.deleteTopics(nullId), { name: 'TypeError', message: 'topics must not contain null' });
  assert.throws(
    () =>
      client.addTopic({
        internal: false,
        name: 't',
        partitions: /** @type {TopicPartitionInfo[]} */ (/** @type {unknown} */ ([null])),
        configs: null,
      }),
    {
      name: 'TypeError',
      message: 'partitions must not contain null',
    },
  );
});

test('deleteTopics by id, and the forms of the topic RPCs', async () => {
  const client = admin();
  await client
    .createTopics([newTopic('t', 1, 1)])
    .all()
    .get();
  const id = /** @type {TopicDescription} */ (
    (await /** @type {KafkaFuture<Map<string, TopicDescription>>} */ (client.describeTopics(['t']).allTopicNames()).get()).get('t')
  ).topicId;
  const result = client.deleteTopics(TopicCollection.ofTopicIds([/** @type {Uuid} */ (id)]));
  assert.equal(result.topicNameValues(), null);
  await result.all().get();
  // A string is an iterable of strings to tsc; Java's Collection<String> is
  // no string, which the run-time check refuses.
  assert.throws(() => client.deleteTopics('t'), { name: 'TypeError', message: 'topics must be an iterable of strings, not string' });
  // @ts-expect-error Java's Collection<NewTopic>
  assert.throws(() => client.createTopics([{ name: 't' }]), {
    name: 'TypeError',
    message: 'newTopics must contain only NewTopic, not an object',
  });
  // @ts-expect-error a Map of numbers: Java's Map<String, NewPartitions>
  assert.throws(() => client.createPartitions(new Map([['t', 1]])), {
    name: 'TypeError',
    message: 'newPartitions must hold only NewPartitions, not a number',
  });
  // @ts-expect-error a plain Map: only a TopicPartitionMap is taken
  assert.throws(() => client.deleteRecords(new Map()), { name: 'TypeError' });
  // @ts-expect-error the options of another RPC
  assert.throws(() => client.describeTopics({ topicNames: ['t'], options: new CreateTopicsOptions() }), {
    name: 'TypeError',
    message: 'options must be a DescribeTopicsOptions, not a CreateTopicsOptions: build one with new DescribeTopicsOptions()',
  });
});

test('the forms of the constructors and the methods', () => {
  // @ts-expect-error a controller without brokers: the form check throws IllegalArgumentError
  assert.throws(() => new MockAdminClient({ controller: node(0) }), {
    name: 'IllegalArgumentError',
    message: 'MockAdminClient() takes one of (), (brokers, controller); got (controller)',
  });
  // @ts-expect-error a string for the brokers: an iterable of Node is taken
  assert.throws(() => new MockAdminClient({ brokers: 'b', controller: node(0) }), {
    name: 'TypeError',
    message: 'brokers must be an iterable of Node, not a string',
  });
  // @ts-expect-error a null controller: Java dereferences it, the FFI's TypeError
  assert.throws(() => new MockAdminClient({ brokers: [node(0)], controller: null }), TypeError);
  const mock = new MockAdminClient();
  // @ts-expect-error a string for the count: the form check throws IllegalArgumentError
  assert.throws(() => mock.timeoutNextRequest('1'), IllegalArgumentError);
  // @ts-expect-error a setter with no value: TypeError
  assert.throws(() => MockAdminClient.create().numBrokers(), { name: 'TypeError', message: 'numBrokers() takes the value to set' });
  return mock.close();
});

// ---- configs and log dirs ------------------------------------------------------------
//
// The Rust core's tests of its mock's configs (rust/src/admin/mock_admin_client.rs),
// and new tests of its log-dir calls, which no Java or Rust test covers: they
// follow Java's MockAdminClient#describeLogDirs, #alterReplicaLogDirs and
// #describeReplicaLogDirs as the core translates them.

const { Type } = ConfigResource;
/**
 * @param {ConfigResource.Type} type
 * @param {string} name
 */
const resource = (type, name) => new ConfigResource({ type, name });
/**
 * @param {string} name
 * @param {string | null} value
 * @param {AlterConfigOp.OpType} opType
 */
const op = (name, value, opType) => new AlterConfigOp({ configEntry: new ConfigEntry({ name, value }), operationType: opType });

/**
 * One resource's configs, from `client`.
 *
 * @param {MockAdminClient} client
 * @param {ConfigResource} res
 */
async function described(client, res) {
  const future = client.describeConfigs([res]).values().get(res);
  assert.ok(future);
  return future.get();
}

/**
 * The future of `res` in an incrementalAlterConfigs of `ops` on it.
 *
 * @param {MockAdminClient} client
 * @param {ConfigResource} res
 * @param {AlterConfigOp[]} ops
 */
function altered(client, res, ops) {
  const future = client
    .incrementalAlterConfigs(new ConfigResourceMap([[res, ops]]))
    .values()
    .get(res);
  assert.ok(future);
  return future;
}

test('describe_configs_returns_a_null_value_set_by_incremental_alter_configs', async () => {
  const client = track(new MockAdminClient.Builder().defaultGroupConfigs(new Map([['consumer.session.timeout.ms', '45000']])).build());
  await client
    .createTopics([newTopic('t', 1, 1)])
    .all()
    .get();
  const topic = resource(Type.TOPIC, 't');
  const broker = resource(Type.BROKER, '0');
  const metrics = resource(Type.CLIENT_METRICS, 'metrics');
  const group = resource(Type.GROUP, 'group');
  const SET = AlterConfigOp.OpType.SET;
  const configs = new ConfigResourceMap([
    [topic, [op('cleanup.policy', null, SET)]],
    [broker, [op('log.retention.ms', null, SET)]],
    [metrics, [op('interval.ms', null, SET)]],
    [group, [op('consumer.heartbeat.interval.ms', null, SET), op('consumer.session.timeout.ms', null, SET)]],
  ]);
  await client.incrementalAlterConfigs({ configs, options: new AlterConfigsOptions() }).all().get();
  for (const [res, name] of /** @type {Array<[ConfigResource, string]>} */ ([
    [topic, 'cleanup.policy'],
    [broker, 'log.retention.ms'],
    [metrics, 'interval.ms'],
    [group, 'consumer.heartbeat.interval.ms'],
  ])) {
    const entry = (await described(client, res)).get(name);
    assert.ok(entry, `${name} is stored for ${res}`);
    assert.equal(entry.value(), null, res.toString());
  }
  assert.equal((await described(client, group)).get('consumer.session.timeout.ms')?.value(), '45000');
});

test('describe_configs_topic_returns_stored_configs', async () => {
  const client = admin();
  await client
    .createTopics([newTopic('t', 1, 1).configs({ 'retention.ms': '1000' })])
    .all()
    .get();
  assert.equal((await described(client, resource(Type.TOPIC, 't'))).get('retention.ms')?.value(), '1000');
});

test('describe_configs_broker_returns_default_replication_factor', async () => {
  const client = admin();
  assert.equal((await described(client, resource(Type.BROKER, '0'))).get('default.replication.factor')?.value(), '3');
});

test('describe_configs_unknown_topic_is_unknown_topic_error', async () => {
  const client = admin();
  const res = resource(Type.TOPIC, 'missing');
  const future = client.describeConfigs([res]).values().get(res);
  assert.ok(future);
  const error = await failure(future);
  assert.ok(error instanceof UnknownTopicOrPartitionError, String(error));
  assert.equal(error.message, "Resource ConfigResource(type=TOPIC, name='missing') not found.");
});

test('describe_configs_unknown_broker_is_invalid_request', async () => {
  const client = admin();
  const res = resource(Type.BROKER, '99');
  const future = client.describeConfigs([res]).values().get(res);
  assert.ok(future);
  const error = await failure(future);
  assert.ok(error instanceof InvalidRequestError, String(error));
  assert.equal(error.message, 'Broker 99 not found.');
});

test('describe_configs_timeout_recovers_on_next_call', async () => {
  const client = admin();
  client.timeoutNextRequest(1);
  const res = resource(Type.BROKER, '0');
  const timedOut = client.describeConfigs([res]).values().get(res);
  assert.ok(timedOut);
  assert.ok((await failure(timedOut)) instanceof TimeoutError);
  await described(client, res);
});

test('incremental_alter_configs_topic_set_and_delete', async () => {
  const client = admin();
  await client
    .createTopics([newTopic('t', 1, 1)])
    .all()
    .get();
  const res = resource(Type.TOPIC, 't');
  await altered(client, res, [op('retention.ms', '42', AlterConfigOp.OpType.SET)]).get();
  assert.equal((await described(client, res)).get('retention.ms')?.value(), '42');
  await altered(client, res, [op('retention.ms', null, AlterConfigOp.OpType.DELETE)]).get();
  assert.equal((await described(client, res)).get('retention.ms'), null);
});

test('incremental_alter_configs_unknown_topic_is_unknown_topic_error', async () => {
  const client = admin();
  const error = await failure(altered(client, resource(Type.TOPIC, 'missing'), [op('k', 'v', AlterConfigOp.OpType.SET)]));
  assert.ok(error instanceof UnknownTopicOrPartitionError, String(error));
  assert.equal(error.message, 'No such topic as missing');
});

test('incremental_alter_configs_broker_logger_is_unsupported_without_message', async () => {
  const client = admin();
  const error = await failure(altered(client, resource(Type.BROKER_LOGGER, '0'), [op('k', 'v', AlterConfigOp.OpType.SET)]));
  assert.ok(error instanceof UnsupportedOperationError, String(error));
  assert.equal(error.message, '');
});

test('incremental_alter_configs_client_metrics_creates_resource', async () => {
  const client = admin();
  const res = resource(Type.CLIENT_METRICS, 'cm');
  await altered(client, res, [op('interval.ms', '5000', AlterConfigOp.OpType.SET)]).get();
  const listed = await client
    .listConfigResources({ configResourceTypes: [Type.CLIENT_METRICS], options: new ListConfigResourcesOptions() })
    .all()
    .get();
  assert.ok(listed.some((r) => r.equals(res)));
});

test('incremental_alter_configs_empty_client_metrics_name_is_invalid_request', async () => {
  const client = admin();
  const error = await failure(altered(client, resource(Type.CLIENT_METRICS, ''), [op('k', 'v', AlterConfigOp.OpType.SET)]));
  assert.ok(error instanceof InvalidRequestError, String(error));
  assert.equal(error.message, 'Empty resource name');
});

test('list_config_resources_all_types_when_empty', async () => {
  const client = admin();
  await client
    .createTopics([newTopic('t', 1, 1)])
    .all()
    .get();
  for (const listed of [
    await client.listConfigResources().all().get(),
    await client.listConfigResources({ configResourceTypes: [], options: new ListConfigResourcesOptions() }).all().get(),
  ]) {
    const set = new ConfigResourceMap(listed.map((r) => /** @type {[ConfigResource, boolean]} */ ([r, true])));
    assert.ok(set.has(resource(Type.TOPIC, 't')));
    // 3 brokers -> broker 0..2 and broker-logger 0..2.
    for (const i of ['0', '1', '2']) {
      assert.ok(set.has(resource(Type.BROKER, i)));
      assert.ok(set.has(resource(Type.BROKER_LOGGER, i)));
    }
  }
});

test('list_config_resources_filters_by_type', async () => {
  const client = admin();
  await client
    .createTopics([newTopic('t', 1, 1)])
    .all()
    .get();
  const listed = await client
    .listConfigResources({ configResourceTypes: new Set([Type.TOPIC]), options: new ListConfigResourcesOptions() })
    .all()
    .get();
  assert.deepEqual(listed.map(String), ["ConfigResource(type=TOPIC, name='t')"]);
});

test("the config calls' forms and null arguments: Java's NullPointerException as a TypeError naming the parameter", () => {
  const client = admin();
  const res = resource(Type.TOPIC, 't');
  // @ts-expect-error a null collection, which the types forbid: Java dereferences it
  assert.throws(() => client.describeConfigs(null), { name: 'TypeError', message: 'resources must not be null' });
  // @ts-expect-error a null element, which the types forbid: Java dereferences it
  assert.throws(() => client.describeConfigs([null]), { name: 'TypeError', message: 'resources must not contain null' });
  // @ts-expect-error a bare string where Java takes a collection
  assert.throws(() => client.describeConfigs('t'), {
    name: 'TypeError',
    message: 'resources must be an iterable of ConfigResource, not a string',
  });
  // @ts-expect-error a Map: Java's map keyed by ConfigResource is a ConfigResourceMap
  assert.throws(() => client.incrementalAlterConfigs(new Map()), {
    name: 'TypeError',
    message: 'configs must be a ConfigResourceMap, not a Map: build one with new ConfigResourceMap()',
  });
  assert.throws(() => client.incrementalAlterConfigs(new ConfigResourceMap([[null, []]])), {
    name: 'TypeError',
    message: 'configs must not contain a null key',
  });
  // @ts-expect-error a null element, which the types forbid: Java dereferences each operation
  assert.throws(() => client.incrementalAlterConfigs(new ConfigResourceMap([[res, [null]]])), {
    name: 'TypeError',
    message: 'configs must not contain null',
  });
  assert.throws(
    () =>
      client.incrementalAlterConfigs(
        new ConfigResourceMap([[res, [new AlterConfigOp({ configEntry: null, operationType: AlterConfigOp.OpType.SET })]]]),
      ),
    { name: 'TypeError', message: 'configs must not contain an AlterConfigOp with a null configEntry' },
  );
  // @ts-expect-error a form Java has not: listConfigResources(configResourceTypes) without the options
  assert.throws(() => client.listConfigResources({ configResourceTypes: [] }), {
    name: 'IllegalArgumentError',
    message: 'listConfigResources() takes one of (), (configResourceTypes, options); got (configResourceTypes)',
  });
  // @ts-expect-error a null element, which the types forbid: Java unboxes each broker id
  assert.throws(() => client.describeLogDirs([0, null]), { name: 'TypeError', message: 'brokers must not contain null' });
  // @ts-expect-error a bare string where Java takes a collection
  assert.throws(() => client.describeLogDirs('0'), { name: 'TypeError', message: 'brokers must be an iterable of numbers, not a string' });
  assert.throws(() => client.alterReplicaLogDirs(new TopicPartitionReplicaMap([[null, '/a']])), {
    name: 'TypeError',
    message: 'replicaAssignment must not contain a null key',
  });
  // @ts-expect-error a null element, which the types forbid: Java dereferences it
  assert.throws(() => client.describeReplicaLogDirs([null]), { name: 'TypeError', message: 'replicas must not contain null' });
});

/**
 * @param {string} topic
 * @param {number} partition
 * @param {number} brokerId
 */
const replica = (topic, partition, brokerId) => new TopicPartitionReplica({ topic, partition, brokerId });

test("describeLogDirs: each broker asked for, its topics' replicas in their first log directory", async () => {
  const client = track(
    new MockAdminClient.Builder()
      .numBrokers(1)
      .brokerLogDirs([['/a', '/b']])
      .build(),
  );
  await client
    .createTopics([newTopic('t', 2, 1)])
    .all()
    .get();
  const result = client.describeLogDirs({ brokers: [0, 7], options: new DescribeLogDirsOptions() });
  assert.deepEqual([...result.descriptions().keys()], [0, 7]);
  const all = await result.allDescriptions().get();
  assert.equal(all.get(7)?.size, 0);
  const dirs = all.get(0);
  assert.ok(dirs);
  assert.deepEqual([...dirs.keys()], ['/a']);
  const description = dirs.get('/a');
  assert.ok(description);
  assert.equal(description.error, null);
  assert.equal(description.totalBytes, null);
  assert.equal(description.usableBytes, null);
  assert.equal(description.isCordoned, false);
  assert.deepEqual([...description.replicaInfos.keys()].map(String).sort(), ['t-0', 't-1']);
  assert.equal(
    description.replicaInfos.get(new TopicPartition({ topic: 't', partition: 0 }))?.toString(),
    'ReplicaInfo(size=0, offsetLag=0, isFuture=false)',
  );
});

test("alterReplicaLogDirs: a known directory moves the replica, others fail as Java's mock fails them", async () => {
  const client = track(
    new MockAdminClient.Builder()
      .numBrokers(1)
      .brokerLogDirs([['/a', '/b']])
      .build(),
  );
  await client
    .createTopics([newTopic('t', 1, 1)])
    .all()
    .get();
  const moved = replica('t', 0, 0);
  const result = client.alterReplicaLogDirs(
    new TopicPartitionReplicaMap([
      [moved, '/b'],
      [replica('t', 0, 5), '/a'],
      [replica('u', 0, 0), '/a'],
    ]),
  );
  assert.equal(result.values().size(), 3);
  await result.values().get(moved)?.get();
  const offline = client
    .alterReplicaLogDirs(new TopicPartitionReplicaMap([[moved, '/c']]))
    .values()
    .get(moved);
  assert.ok(offline);
  const offlineError = await failure(offline);
  assert.ok(offlineError instanceof KafkaStorageError, String(offlineError));
  assert.equal(offlineError.message, 'Log directory /c is offline');
  const noBroker = result.values().get(replica('t', 0, 5));
  assert.ok(noBroker);
  const noBrokerError = await failure(noBroker);
  assert.ok(noBrokerError instanceof ReplicaNotAvailableError, String(noBrokerError));
  assert.equal(noBrokerError.message, "Can't find t-0-5");
  const noTopic = result.values().get(replica('u', 0, 0));
  assert.ok(noTopic);
  assert.ok((await failure(noTopic)) instanceof ReplicaNotAvailableError);
  assert.ok((await failure(result.all())) instanceof ReplicaNotAvailableError);

  const described = await client
    .describeReplicaLogDirs([moved, replica('t', 5, 0), replica('u', 0, 0)])
    .all()
    .get();
  // An unknown topic's replica is left out, as Java's mock leaves it.
  assert.equal(described.size(), 2);
  const info = described.get(moved);
  assert.ok(info);
  assert.equal(info.getCurrentReplicaLogDir(), '/a');
  assert.equal(info.getCurrentReplicaOffsetLag(), 0n);
  assert.equal(info.getFutureReplicaLogDir(), '/b');
  assert.equal(info.getFutureReplicaOffsetLag(), 0n);
  assert.equal(info.toString(), '(currentReplicaLogDir=/a, futureReplicaLogDir=/b, futureReplicaOffsetLag=0)');
  const none = described.get(replica('t', 5, 0));
  assert.ok(none);
  assert.equal(none.getCurrentReplicaLogDir(), null);
  assert.equal(none.getFutureReplicaOffsetLag(), -1n);
  assert.equal(none.toString(), 'ReplicaLogDirInfo(currentReplicaLogDir=null)');
  assert.equal(
    JSON.stringify(none),
    '{"currentReplicaLogDir":null,"currentReplicaOffsetLag":"-1","futureReplicaLogDir":null,"futureReplicaOffsetLag":"-1"}',
  );
});

test('describeReplicaLogDirs: an unmoved replica reads its current log directory only', async () => {
  const client = track(
    new MockAdminClient.Builder()
      .numBrokers(1)
      .brokerLogDirs([['/a', '/b']])
      .build(),
  );
  await client
    .createTopics([newTopic('t', 1, 1)])
    .all()
    .get();
  const values = client.describeReplicaLogDirs({ replicas: [replica('t', 0, 0)], options: new DescribeReplicaLogDirsOptions() }).values();
  const info = await values.get(replica('t', 0, 0))?.get();
  assert.ok(info);
  assert.equal(info.toString(), 'ReplicaLogDirInfo(currentReplicaLogDir=/a)');
  assert.equal(info.getFutureReplicaLogDir(), null);
});

test("the config and log-dir Results' readers only read", () => {
  const client = admin();
  for (const [name, call] of /** @type {Array<[string, () => void]>} */ ([
    // @ts-expect-error an argument given to a reader: it only reads
    ['values', () => client.describeConfigs([]).values(1)],
    // @ts-expect-error an argument given to a reader: it only reads
    ['all', () => client.incrementalAlterConfigs(new ConfigResourceMap()).all(1)],
    // @ts-expect-error an argument given to a reader: it only reads
    ['all', () => client.listConfigResources().all(1)],
    // @ts-expect-error an argument given to a reader: it only reads
    ['descriptions', () => client.describeLogDirs([]).descriptions(1)],
    // @ts-expect-error an argument given to a reader: it only reads
    ['allDescriptions', () => client.describeLogDirs([]).allDescriptions(1)],
    // @ts-expect-error an argument given to a reader: it only reads
    ['values', () => client.alterReplicaLogDirs(new TopicPartitionReplicaMap()).values(1)],
    // @ts-expect-error an argument given to a reader: it only reads
    ['all', () => client.describeReplicaLogDirs([]).all(1)],
  ])) {
    assert.throws(call, { name: 'TypeError', message: `${name}() only reads: it takes no argument` });
  }
});
