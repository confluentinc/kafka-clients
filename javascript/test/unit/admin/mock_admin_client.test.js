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
  AlterPartitionReassignmentsOptions,
  ElectLeadersOptions,
  ListOffsetsOptions,
  ListPartitionReassignmentsOptions,
  NewPartitionReassignment,
  OffsetSpec,
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

test('builder: a string-keyed map given by name is a Map or a plain object, never another object', () => {
  // A class instance is no map keyed by strings (Q25): it is refused, not read as an empty map.
  // @ts-expect-error a value the types forbid
  assert.throws(() => MockAdminClient.create().featureLevels({ featureLevels: new Set([1]) }), {
    name: 'TypeError',
    message: 'featureLevels must be a Map or a plain object, not a Set',
  });
  // @ts-expect-error a value the types forbid
  assert.throws(() => MockAdminClient.create().defaultGroupConfigs({ defaultGroupConfigs: new Date(0) }), {
    name: 'TypeError',
    message: 'defaultGroupConfigs must be a Map or a plain object, not a Date',
  });
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
  // Neither a Map nor a plain object: a class instance is not a map.
  // @ts-expect-error a NewPartitions where Java's Map<String, NewPartitions> goes
  assert.throws(() => client.createPartitions({ newPartitions: NewPartitions.increaseTo(2) }), {
    name: 'TypeError',
    message: 'newPartitions must be a Map or a plain object, not a NewPartitions',
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
  // A method of no parameter refuses an argument with the form error.
  // @ts-expect-error a wrong argument: create() takes none
  assert.throws(() => MockAdminClient.create(1), { name: 'IllegalArgumentError', message: 'create() takes one of (); got a number' });
  // @ts-expect-error a wrong argument: build() takes none
  assert.throws(() => MockAdminClient.create().build(1), {
    name: 'IllegalArgumentError',
    message: 'build() takes one of (); got a number',
  });
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

// The core's mock answers each replica on its own; for broker 5 Java's
// MockAdminClient#alterReplicaLogDirs calls brokerLogDirs.get(5) and throws
// IndexOutOfBoundsException from the call, where the core's fails that
// replica's future with ReplicaNotAvailableError.
test("alterReplicaLogDirs: a known directory moves the replica, the others fail with the core's per-replica errors", async () => {
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

// ---- the leader, reassignment and offset calls ---------------------------------------
// The Rust core's tests of its mock's reassignments
// (alter_then_list_partition_reassignments_reports_adding_and_removing and the
// two list_partition_reassignments regressions), then the calls Java's mock
// implements without a core test: listOffsets over updateBeginningOffsets and
// updateEndOffsets, a TimestampSpec and electLeaders, which Java's mock throws
// UnsupportedOperationException for.

/**
 * @param {string} topic
 * @param {number} partition
 */
const tp = (topic, partition) => new TopicPartition({ topic, partition });

/**
 * One topic `rt` and its only partition reassigned to brokers 1 and 2: the
 * Rust core's admin_with_reassignment.
 */
async function adminWithReassignment() {
  const client = admin();
  await client
    .createTopics([new NewTopic({ name: 'rt', numPartitions: 1, replicationFactor: 3 })])
    .all()
    .get();
  await client
    .alterPartitionReassignments(new TopicPartitionMap([[tp('rt', 0), new NewPartitionReassignment([1, 2])]]))
    .all()
    .get();
  return client;
}

test('alter_then_list_partition_reassignments_reports_adding_and_removing', async () => {
  const client = await adminWithReassignment();
  let listed = await client
    .listPartitionReassignments({ partitions: null, options: new ListPartitionReassignmentsOptions() })
    .reassignments()
    .get();
  const reassignment = listed.get(tp('rt', 0));
  assert.ok(reassignment);
  // The mock seeds every partition with all three brokers as replicas, so
  // targeting {1, 2} removes broker 0 and adds nothing.
  assert.deepEqual(reassignment.replicas(), [0, 1, 2]);
  assert.deepEqual(reassignment.addingReplicas(), []);
  assert.deepEqual(reassignment.removingReplicas(), [0]);
  assert.equal(reassignment.toString(), 'PartitionReassignment(replicas=[0, 1, 2], addingReplicas=[], removingReplicas=[0])');
  // A null value (Java's empty Optional) cancels.
  await client
    .alterPartitionReassignments({
      reassignments: new TopicPartitionMap([[tp('rt', 0), null]]),
      options: new AlterPartitionReassignmentsOptions(),
    })
    .all()
    .get();
  listed = await client.listPartitionReassignments().reassignments().get();
  assert.equal(listed.size(), 0);
});

test('list_partition_reassignments_after_topic_deletion_fails_the_future', async () => {
  const client = await adminWithReassignment();
  await client.deleteTopics(['rt']).all().get();
  const cause = await failure(client.listPartitionReassignments().reassignments());
  assert.ok(cause instanceof Error);
  assert.equal(cause.message, 'Internal MockAdminClient logic error: found reassignment for rt-0, but no TopicMetadata');
});

test('list_partition_reassignments_after_topic_shrink_fails_the_future', async () => {
  const client = admin();
  await client
    .createTopics([new NewTopic({ name: 'rt2', numPartitions: 2, replicationFactor: 3 })])
    .all()
    .get();
  // Reassign the second partition, so the index survives the topic's removal
  // but not its recreation.
  await client
    .alterPartitionReassignments(new TopicPartitionMap([[tp('rt2', 1), new NewPartitionReassignment([1, 2])]]))
    .all()
    .get();
  await client.deleteTopics(['rt2']).all().get();
  await client
    .createTopics([new NewTopic({ name: 'rt2', numPartitions: 1, replicationFactor: 3 })])
    .all()
    .get();
  const cause = await failure(client.listPartitionReassignments().reassignments());
  assert.ok(cause instanceof Error);
  assert.equal(cause.message, 'Internal MockAdminClient logic error: found reassignment for rt2-1, but no TopicPartitionInfo');
});

test('listPartitionReassignments for given partitions answers only those reassigned', async () => {
  const client = await adminWithReassignment();
  const only = await client
    .listPartitionReassignments([tp('rt', 0)])
    .reassignments()
    .get();
  assert.deepEqual([...only.keys()].map(String), ['rt-0']);
  const none = await client
    .listPartitionReassignments({ partitions: new Set([tp('other', 0)]) })
    .reassignments()
    .get();
  assert.equal(none.size(), 0);
});

test("alterPartitionReassignments fails a partition the mock has not with the core's UnknownTopicOrPartitionError", async () => {
  const client = admin();
  await client
    .createTopics([new NewTopic({ name: 't', numPartitions: 1, replicationFactor: 1 })])
    .all()
    .get();
  const values = client
    .alterPartitionReassignments(
      new TopicPartitionMap([
        [tp('t', 0), new NewPartitionReassignment([0])],
        [tp('t', 5), new NewPartitionReassignment([0])],
        [tp('absent', 0), null],
      ]),
    )
    .values();
  assert.equal(await values.get(tp('t', 0))?.get(), null);
  const out = values.get(tp('t', 5));
  assert.ok(out);
  // The core's message (Java's mock throws new UnknownTopicOrPartitionException(), whose message is null).
  for (const future of [out, values.get(tp('absent', 0))]) {
    assert.ok(future);
    const cause = await failure(future);
    assert.ok(cause instanceof UnknownTopicOrPartitionError);
    assert.equal(cause.message, 'This server does not host this topic-partition.');
  }
});

test('listOffsets answers the seeded beginning offset for earliest and the end offset for the other specs', async () => {
  const client = admin();
  client.updateBeginningOffsets(new TopicPartitionMap([[tp('t', 0), 5n]]));
  client.updateEndOffsets({
    newOffsets: new TopicPartitionMap(
      /** @type {Array<[TopicPartition, bigint | number]>} */ ([
        [tp('t', 0), 50],
        [tp('t', 1), 7n],
      ]),
    ),
  });
  const result = client.listOffsets(
    new TopicPartitionMap([
      [tp('t', 0), OffsetSpec.earliest()],
      [tp('t', 1), OffsetSpec.latest()],
    ]),
  );
  const earliest = await result.partitionResult(tp('t', 0)).get();
  assert.equal(earliest.offset(), 5n);
  assert.equal(earliest.timestamp(), -1n);
  assert.equal(earliest.leaderEpoch(), null);
  assert.equal(earliest.toString(), 'ListOffsetsResultInfo(offset=5, timestamp=-1, leaderEpoch=Optional.empty)');
  const all = await result.all().get();
  assert.equal(
    String(all),
    '{t-0=ListOffsetsResultInfo(offset=5, timestamp=-1, leaderEpoch=Optional.empty), t-1=ListOffsetsResultInfo(offset=7, timestamp=-1, leaderEpoch=Optional.empty)}',
  );
  // Every spec but earliest and a timestamp reads the end offset.
  for (const spec of [
    OffsetSpec.maxTimestamp(),
    OffsetSpec.earliestLocal(),
    OffsetSpec.latestTiered(),
    OffsetSpec.earliestPendingUpload(),
    new OffsetSpec(),
  ]) {
    const info = await client
      .listOffsets(new TopicPartitionMap([[tp('t', 0), spec]]))
      .partitionResult(tp('t', 0))
      .get();
    assert.equal(info.offset(), 50n);
  }
  // A later seeding replaces the offset.
  client.updateEndOffsets(new TopicPartitionMap([[tp('t', 0), 60n]]));
  assert.equal(
    (
      await client
        .listOffsets(new TopicPartitionMap([[tp('t', 0), OffsetSpec.latest()]]))
        .partitionResult(tp('t', 0))
        .get()
    ).offset(),
    60n,
  );
});

test("listOffsets reads a null spec as Java's getOffsetFromSpec does: the latest offset", async () => {
  const client = admin();
  client.updateEndOffsets(new TopicPartitionMap([[tp('t', 0), 70n]]));
  const info = await client
    // @ts-expect-error a null the types forbid: Java's instanceof chain falls through to the latest offset
    .listOffsets(new TopicPartitionMap([[tp('t', 0), null]]))
    .partitionResult(tp('t', 0))
    .get();
  assert.equal(info.offset(), 70n);
  await client.close();
});

test("listOffsets fails a TimestampSpec with Java's mock's UnsupportedOperationError, its isolation level aside", async () => {
  const client = admin();
  const result = client.listOffsets({
    topicPartitionOffsets: new TopicPartitionMap([[tp('t', 0), OffsetSpec.forTimestamp(1000)]]),
    options: new ListOffsetsOptions(IsolationLevel.READ_COMMITTED),
  });
  const cause = await failure(result.partitionResult(tp('t', 0)));
  assert.ok(cause instanceof UnsupportedOperationError);
  // Java's message, as its mock spells it.
  assert.equal(cause.message, 'Not implement yet');
  assert.ok((await failure(result.all())) instanceof UnsupportedOperationError);
  assert.throws(() => result.partitionResult(tp('t', 1)), {
    name: 'IllegalArgumentError',
    message: 'List Offsets for partition "t-1" was not attempted',
  });
});

test("electLeaders fails with Java's mock's UnsupportedOperationError", async () => {
  const client = admin();
  for (const result of [
    client.electLeaders({ electionType: ElectionType.PREFERRED, partitions: null }),
    client.electLeaders({ electionType: ElectionType.UNCLEAN, partitions: [tp('t', 0)], options: new ElectLeadersOptions() }),
  ]) {
    const cause = await failure(result.partitions());
    assert.ok(cause instanceof UnsupportedOperationError);
    assert.equal(cause.message, 'Not implemented yet');
    assert.ok((await failure(result.all())) instanceof UnsupportedOperationError);
  }
});

test("the leader, reassignment and offset calls' arguments: Java's nulls as TypeErrors naming the RPC's parameter", () => {
  const client = admin();
  assert.throws(
    // @ts-expect-error a null the types forbid: Java's request builder dereferences it
    () => client.electLeaders({ electionType: null, partitions: null }),
    { name: 'TypeError', message: 'electionType must not be null' },
  );
  assert.throws(
    // @ts-expect-error a wrong argument: a string is no iterable of TopicPartition
    () => client.electLeaders({ electionType: ElectionType.PREFERRED, partitions: 'abc' }),
    { name: 'TypeError', message: 'partitions must be an iterable of TopicPartition, not a string' },
  );
  assert.throws(
    // @ts-expect-error a null element the types forbid: Java dereferences it
    () => client.listPartitionReassignments([null]),
    { name: 'TypeError', message: 'partitions must not contain null' },
  );
  assert.throws(
    // @ts-expect-error a null the types forbid: Java's map is dereferenced
    () => client.alterPartitionReassignments(null),
    { name: 'TypeError', message: 'reassignments must not be null' },
  );
  assert.throws(
    // @ts-expect-error a wrong argument: a Map where Java takes a Map<TopicPartition, ...>
    () => client.alterPartitionReassignments(new Map()),
    { name: 'TypeError', message: 'reassignments must be a TopicPartitionMap, not a Map: build one with new TopicPartitionMap()' },
  );
  assert.throws(() => client.alterPartitionReassignments(new TopicPartitionMap([[null, null]])), {
    name: 'TypeError',
    message: 'reassignments must not contain a null key',
  });
  assert.throws(
    // @ts-expect-error a null the types forbid: Java's (Set) form calls Optional.of(partitions)
    () => client.listPartitionReassignments({ partitions: null }),
    { name: 'TypeError', message: 'partitions must not be null' },
  );
  assert.throws(
    () => client.listOffsets({ topicPartitionOffsets: new TopicPartitionMap(), options: new ListOffsetsOptions({ isolationLevel: null }) }),
    { name: 'TypeError', message: 'ListOffsetsOptions.isolationLevel must not be null' },
  );
  assert.throws(
    // @ts-expect-error a null the types forbid: Java's putAll dereferences it
    () => client.updateEndOffsets(null),
    { name: 'TypeError', message: 'newOffsets must not be null' },
  );
  for (const update of [
    // @ts-expect-error a wrong argument: Java's Map<TopicPartition, Long> is a TopicPartitionMap
    () => client.updateEndOffsets(new Map([[tp('t', 0), 5n]])),
    // @ts-expect-error a wrong argument: Java's Map<TopicPartition, Long> is a TopicPartitionMap
    () => client.updateBeginningOffsets(new Map()),
    // @ts-expect-error a wrong argument: Java's Map<TopicPartition, Long> is a TopicPartitionMap
    () => client.updateConsumerGroupOffsets(new Map()),
  ]) {
    assert.throws(update, {
      name: 'TypeError',
      message: 'newOffsets must be a TopicPartitionMap, not a Map: build one with new TopicPartitionMap()',
    });
  }
  assert.throws(
    // @ts-expect-error a null value the types forbid: the C API takes each offset as a long
    () => client.updateBeginningOffsets(new TopicPartitionMap([[tp('t', 0), null]])),
    { name: 'TypeError', message: 'newOffsets must not contain null' },
  );
});

test("the leader, reassignment and offset calls' forms", () => {
  const client = admin();
  assert.throws(
    // @ts-expect-error a wrong argument: a form Java has not
    () => client.electLeaders({ electionType: ElectionType.PREFERRED }),
    {
      name: 'IllegalArgumentError',
      message: 'electLeaders() takes one of (electionType, partitions), (electionType, partitions, options); got (electionType)',
    },
  );
  assert.throws(
    // @ts-expect-error a wrong argument: a form Java has not
    () => client.listPartitionReassignments({ partitions: [], options: new ListPartitionReassignmentsOptions(), extra: 1 }),
    {
      name: 'IllegalArgumentError',
      message:
        'listPartitionReassignments() takes one of (), (partitions), (partitions, options), (options); got (partitions, options, extra)',
    },
  );
  // The options on their own, the partitions on their own, and nothing.
  client.listPartitionReassignments(new ListPartitionReassignmentsOptions());
  client.listPartitionReassignments(new Set([tp('t', 0)]));
  client.listPartitionReassignments();
});

test("the leader, reassignment and offset Results' readers only read", () => {
  const client = admin();
  const offsets = new TopicPartitionMap([[tp('t', 0), OffsetSpec.latest()]]);
  for (const [name, call] of /** @type {Array<[string, () => void]>} */ ([
    // @ts-expect-error an argument given to a reader: it only reads
    ['partitions', () => client.electLeaders({ electionType: ElectionType.PREFERRED, partitions: null }).partitions(1)],
    // @ts-expect-error an argument given to a reader: it only reads
    ['all', () => client.electLeaders({ electionType: ElectionType.PREFERRED, partitions: null }).all(1)],
    // @ts-expect-error an argument given to a reader: it only reads
    ['values', () => client.alterPartitionReassignments(new TopicPartitionMap()).values(1)],
    // @ts-expect-error an argument given to a reader: it only reads
    ['all', () => client.alterPartitionReassignments(new TopicPartitionMap()).all(1)],
    // @ts-expect-error an argument given to a reader: it only reads
    ['reassignments', () => client.listPartitionReassignments().reassignments(1)],
    // @ts-expect-error an argument given to a reader: it only reads
    ['all', () => client.listOffsets(offsets).all(1)],
  ])) {
    assert.throws(call, { name: 'TypeError', message: `${name}() only reads: it takes no argument` });
  }
});

// ---- the group calls ----------------------------------------------------------------------
//
// The Rust core's mock tests of listConsumerGroupOffsets
// (mock_list_consumer_group_offsets_rejects_a_negative_seeded_offset,
// mock_list_consumer_group_offsets_negative_offset_outside_the_selection_is_ignored),
// then what Java's mock does for the other group calls: listGroups lists one
// group per seeded group config (none here), and describeConsumerGroups,
// describeClassicGroups, deleteConsumerGroups, deleteConsumerGroupOffsets,
// removeMembersFromConsumerGroup and alterConsumerGroupOffsets throw Java's
// UnsupportedOperationException ("Not implemented yet"; "Not implement yet",
// sic, for alterConsumerGroupOffsets), which the core's mock reports on the
// futures.

const {
  MemberToRemove,
  ListConsumerGroupOffsetsSpec,
  RemoveMembersFromConsumerGroupOptions,
} = require('@confluentinc/kafka-javascript/admin');
const { OffsetAndMetadata } = require('@confluentinc/kafka-javascript/consumer');

const groupTp = new TopicPartition({ topic: 'topic', partition: 0 });

test('mock_list_consumer_group_offsets_rejects_a_negative_seeded_offset', async () => {
  const mock = new MockAdminClient();
  mock.updateConsumerGroupOffsets(new TopicPartitionMap([[groupTp, -1]]));
  const specs = new Map([['group', new ListConsumerGroupOffsetsSpec()]]);
  const cause = await failure(mock.listConsumerGroupOffsets(specs).partitionsToOffsetAndMetadata());
  assert.ok(cause instanceof IllegalArgumentError);
  assert.equal(cause.message, 'Invalid negative offset');
  // The mock is still usable afterwards.
  mock.updateConsumerGroupOffsets({ newOffsets: new TopicPartitionMap([[groupTp, 7n]]) });
  const offsets = await mock.listConsumerGroupOffsets(specs).partitionsToOffsetAndMetadata().get();
  assert.equal(offsets.get(groupTp)?.offset, 7n);
  await mock.close();
});

test('mock_list_consumer_group_offsets_negative_offset_outside_the_selection_is_ignored', async () => {
  const mock = new MockAdminClient();
  const other = new TopicPartition({ topic: 'topic', partition: 1 });
  mock.updateConsumerGroupOffsets(
    new TopicPartitionMap([
      [groupTp, 5n],
      [other, -1n],
    ]),
  );
  const specs = new Map([['group', new ListConsumerGroupOffsetsSpec({ topicPartitions: [groupTp] })]]);
  const offsets = await mock.listConsumerGroupOffsets(specs).partitionsToOffsetAndMetadata().get();
  assert.equal(offsets.size(), 1);
  assert.equal(offsets.get(groupTp)?.offset, 5n);
  await mock.close();
});

// The core's mock answers every seeded offset for one group; Java 4.4.0's
// default spec has null topicPartitions, which Java's mock dereferences
// (NullPointerException).
test("listConsumerGroupOffsets: the core's mock answers every seeded offset for one group; several groups not implemented", async () => {
  const mock = new MockAdminClient();
  mock.updateConsumerGroupOffsets(new TopicPartitionMap([[groupTp, 3n]]));
  const result = mock.listConsumerGroupOffsets('any');
  const all = await result.all().get();
  assert.deepEqual([...all.keys()], ['any']);
  const offsets = all.get('any');
  assert.ok(offsets);
  const committed = offsets.get(groupTp);
  assert.ok(committed instanceof OffsetAndMetadata);
  assert.equal(String(committed), "OffsetAndMetadata{offset=3, leaderEpoch=null, metadata=''}");
  const two = mock.listConsumerGroupOffsets({
    groupSpecs: { a: new ListConsumerGroupOffsetsSpec(), b: new ListConsumerGroupOffsetsSpec() },
  });
  const cause = await failure(two.partitionsToOffsetAndMetadata('a'));
  assert.ok(cause instanceof UnsupportedOperationError);
  assert.equal(cause.message, 'Not implemented yet');
  await mock.close();
});

test("listGroups lists one CONSUMER / consumer / STABLE group per seeded group config, as Java's mock does", async () => {
  const { GroupListing } = require('@confluentinc/kafka-javascript/admin');
  const { ConfigResource, ConfigResourceMap } = require('@confluentinc/kafka-javascript/common/config');
  const { GroupState, GroupType } = require('@confluentinc/kafka-javascript/common');
  const mock = new MockAdminClient();
  const resource = new ConfigResource({ type: ConfigResource.Type.GROUP, name: 'g1' });
  const op = new AlterConfigOp({
    configEntry: new ConfigEntry({ name: 'consumer.session.timeout.ms', value: '45000' }),
    operationType: AlterConfigOp.OpType.SET,
  });
  await mock
    .incrementalAlterConfigs(new ConfigResourceMap([[resource, [op]]]))
    .all()
    .get();
  const [listing, ...rest] = await mock.listGroups().all().get();
  assert.deepEqual(rest, []);
  assert.ok(listing instanceof GroupListing);
  assert.equal(listing.groupId(), 'g1');
  assert.equal(listing.type(), GroupType.CONSUMER);
  assert.equal(listing.protocol(), 'consumer');
  assert.equal(listing.groupState(), GroupState.STABLE);
  assert.equal(listing.isSimpleConsumerGroup(), false);
  assert.equal(String(listing), "(groupId='g1', type=Consumer, protocol='consumer', groupState=Stable)");
  assert.deepEqual(await mock.listGroups().valid().get(), [listing]);
  assert.deepEqual(await mock.listGroups().errors().get(), []);
  await mock.close();
});

test("listGroups: one listing per seeded group config, none here; Java's three futures", async () => {
  const mock = new MockAdminClient();
  const result = mock.listGroups();
  assert.deepEqual(await result.all().get(), []);
  assert.deepEqual(await result.valid().get(), []);
  assert.deepEqual(await result.errors().get(), []);
  await mock.close();
});

test("a client's RemoveMembersFromConsumerGroupResult runs Java's two checks for a null member, or one with a null group instance id", async () => {
  const mock = new MockAdminClient();
  const some = mock.removeMembersFromConsumerGroup({
    groupId: 'g',
    options: new RemoveMembersFromConsumerGroupOptions([new MemberToRemove('i')]),
  });
  // @ts-expect-error a null the types forbid: Java's HashSet holds none
  assert.throws(() => some.memberResult(null), {
    name: 'IllegalArgumentError',
    message: 'Member null was not included in the original request',
  });
  const all = mock.removeMembersFromConsumerGroup({ groupId: 'g', options: new RemoveMembersFromConsumerGroupOptions() });
  // @ts-expect-error a null the types forbid: the removeAll check comes first
  assert.throws(() => all.memberResult(null), {
    name: 'IllegalArgumentError',
    message: "The method: memberResult is not applicable in 'removeAll' mode",
  });
  // A member whose group instance id is null is in no request either: Java's
  // Object.toString() of it, Objects.hash(null) being 31 (0x1f).
  const noId = new MemberToRemove(null);
  assert.throws(() => some.memberResult(noId), {
    name: 'IllegalArgumentError',
    message: 'Member org.apache.kafka.clients.admin.MemberToRemove@1f was not included in the original request',
  });
  assert.throws(() => all.memberResult(noId), {
    name: 'IllegalArgumentError',
    message: "The method: memberResult is not applicable in 'removeAll' mode",
  });
  await mock.close();
});

test("the group calls Java's mock does not implement fail their futures with UnsupportedOperationError", async () => {
  const mock = new MockAdminClient();
  const member = new MemberToRemove('i');
  /** @type {Array<[import('@confluentinc/kafka-javascript/common').KafkaFuture<unknown>, string]>} */
  const futures = [
    [mock.describeConsumerGroups(['g']).all(), 'Not implemented yet'],
    [mock.describeClassicGroups(['g']).all(), 'Not implemented yet'],
    [mock.deleteConsumerGroups(['g']).all(), 'Not implemented yet'],
    [mock.deleteConsumerGroupOffsets({ groupId: 'g', partitions: [groupTp] }).partitionResult(groupTp), 'Not implemented yet'],
    [mock.deleteConsumerGroupOffsets({ groupId: 'g', partitions: [groupTp] }).all(), 'Not implemented yet'],
    [
      mock
        .removeMembersFromConsumerGroup({ groupId: 'g', options: new RemoveMembersFromConsumerGroupOptions([member]) })
        .memberResult(member),
      'Not implemented yet',
    ],
    [
      mock.removeMembersFromConsumerGroup({ groupId: 'g', options: new RemoveMembersFromConsumerGroupOptions() }).all(),
      'Not implemented yet',
    ],
    [
      mock
        .alterConsumerGroupOffsets({ groupId: 'g', offsets: new TopicPartitionMap([[groupTp, new OffsetAndMetadata(1n)]]) })
        .partitionResult(groupTp),
      'Not implement yet',
    ],
  ];
  for (const [future, message] of futures) {
    const cause = await failure(future);
    assert.ok(cause instanceof UnsupportedOperationError, String(cause));
    assert.equal(cause.message, message);
  }
  await mock.close();
});

test("updateConsumerGroupOffsets' forms: a TopicPartitionMap of offsets only", async () => {
  const mock = new MockAdminClient();
  // @ts-expect-error a plain Map where Java takes Map<TopicPartition, Long>
  assert.throws(() => mock.updateConsumerGroupOffsets(new Map()), {
    name: 'TypeError',
    message: 'newOffsets must be a TopicPartitionMap, not a Map: build one with new TopicPartitionMap()',
  });
  assert.throws(
    // @ts-expect-error a null offset
    () => mock.updateConsumerGroupOffsets(new TopicPartitionMap([[groupTp, null]])),
    { name: 'TypeError' },
  );
  await mock.close();
});

test('removeMembersFromConsumerGroup in several worker threads: each call carries its own members', () => {
  // The addon is loaded once per process and shared by every worker, so the
  // members the native options are built from must stay the calling thread's.
  const { spawnSync } = require('node:child_process');
  const path = require('node:path');
  const script = `
    const { Worker } = require('node:worker_threads');
    const body = \`
      const { parentPort, workerData } = require('node:worker_threads');
      const { MemberToRemove, MockAdminClient, RemoveMembersFromConsumerGroupOptions } = require('@confluentinc/kafka-javascript/admin');
      (async () => {
        const mock = new MockAdminClient();
        const mine = new MemberToRemove(workerData);
        let wrong = 0;
        for (let i = 0; i < 5000; i++) {
          const r = mock.removeMembersFromConsumerGroup({ groupId: 'g', options: new RemoveMembersFromConsumerGroupOptions([mine]) });
          try { r.memberResult(mine); } catch (e) { wrong += 1; }
        }
        await mock.close();
        parentPort.postMessage(wrong);
      })();
    \`;
    Promise.all(['alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta'].map((name) => new Promise((res, rej) => {
      const w = new Worker(body, { eval: true, workerData: name });
      w.once('message', res);
      w.once('error', rej);
    }))).then((wrong) => console.log(JSON.stringify(wrong)), (e) => { console.error(e); process.exit(1); });
  `;
  const r = spawnSync(process.execPath, ['--no-warnings', '-e', script], {
    cwd: path.join(__dirname, '..', '..', '..'),
    encoding: 'utf8',
    timeout: 120000,
  });
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, '[0,0,0,0,0,0,0,0]\n');
}, 150000);

test("the checks run in Java's order: options first, where Java's body reads them before the arguments it only stores", async () => {
  const mock = new MockAdminClient();
  for (const call of [
    // @ts-expect-error nulls the types forbid: Java's first statement is options.reason()
    () => mock.removeMembersFromConsumerGroup({ groupId: null, options: null }),
    // @ts-expect-error nulls the types forbid: Java's handler stores the partitions, then reads options.timeoutMs()
    () => mock.deleteConsumerGroupOffsets({ groupId: 'g', partitions: null, options: null }),
    // @ts-expect-error nulls the types forbid: Java's handler stores the offsets, then reads options.timeoutMs()
    () => mock.alterConsumerGroupOffsets({ groupId: null, offsets: null, options: null }),
    // @ts-expect-error nulls the types forbid: Java reads options.requireStable() first
    () => mock.listConsumerGroupOffsets({ groupId: null, options: null }),
    // @ts-expect-error nulls the types forbid: Java reads options.timeoutMs() before the election type
    () => mock.electLeaders({ electionType: null, partitions: null, options: null }),
    // @ts-expect-error nulls the types forbid: Java reads options.timeoutMs() before the types
    () => mock.listConfigResources({ configResourceTypes: null, options: null }),
  ]) {
    assert.throws(call, { name: 'TypeError', message: 'options must not be null' });
  }
  const tpNullTopic = new TopicPartition({ topic: null, partition: 0 });
  for (const call of [
    // @ts-expect-error nulls the types forbid: Java iterates the ids, storing a null one, before options.includeAuthorizedOperations()
    () => mock.describeConsumerGroups({ groupIds: [null], options: null }),
    // @ts-expect-error nulls the types forbid: likewise for the classic groups
    () => mock.describeClassicGroups({ groupIds: [null], options: null }),
    // @ts-expect-error nulls the types forbid: Java reads options.timeoutMs() before it uses the ids
    () => mock.deleteConsumerGroups({ groupIds: [null], options: null }),
    // @ts-expect-error nulls the types forbid: Java reads options.timeoutMs() for each broker's call before it unboxes the id
    () => mock.describeLogDirs({ brokers: [null], options: null }),
    // @ts-expect-error nulls the types forbid: Java reads the operations only when it builds the request
    () => mock.incrementalAlterConfigs({ configs: new ConfigResourceMap([[resource(Type.TOPIC, 't'), null]]), options: null }),
    // @ts-expect-error nulls the types forbid: Java reads a key's topic only when it builds the request
    () => mock.listOffsets({ topicPartitionOffsets: new TopicPartitionMap([[tpNullTopic, OffsetSpec.latest()]]), options: null }),
  ]) {
    assert.throws(call, { name: 'TypeError', message: 'options must not be null' });
  }
  // A null collection or key Java dereferences first.
  assert.throws(
    // @ts-expect-error nulls the types forbid: Java iterates the ids first
    () => mock.describeConsumerGroups({ groupIds: null, options: null }),
    { name: 'TypeError', message: 'groupIds must not be null' },
  );
  assert.throws(
    // @ts-expect-error nulls the types forbid: Java's unmodifiable map refuses a null key first
    () => mock.listOffsets({ topicPartitionOffsets: new TopicPartitionMap([[null, OffsetSpec.latest()]]), options: null }),
    { name: 'TypeError', message: 'topicPartitionOffsets must not contain a null key' },
  );
  assert.throws(
    // @ts-expect-error nulls the types forbid: Java's nodeFor dereferences each resource first
    () => mock.incrementalAlterConfigs({ configs: new ConfigResourceMap([[null, []]]), options: null }),
    { name: 'TypeError', message: 'configs must not contain a null key' },
  );
  // The (Map, options) form reads groupSpecs.keySet() before the options.
  // @ts-expect-error nulls the types forbid: Java's first dereference is groupSpecs
  assert.throws(() => mock.listConsumerGroupOffsets({ groupSpecs: null, options: null }), {
    name: 'TypeError',
    message: 'groupSpecs must not be null',
  });
  // A null spec is read only when the request is built: the options first.
  // @ts-expect-error nulls the types forbid: Java reads options.requireStable() before the specs
  assert.throws(() => mock.listConsumerGroupOffsets({ groupSpecs: { g: null }, options: null }), {
    name: 'TypeError',
    message: 'options must not be null',
  });
  await mock.close();
});

test("the configs, offsets and groups RPCs' one-parameter forms by name work as their bare forms", async () => {
  const A = require('@confluentinc/kafka-javascript/admin');
  const C = require('@confluentinc/kafka-javascript/common');
  const { ConfigResource } = require('@confluentinc/kafka-javascript/common/config');
  const mock = new MockAdminClient();
  const p = new C.TopicPartition({ topic: 't', partition: 0 });
  const replica = new C.TopicPartitionReplica({ topic: 't', partition: 0, brokerId: 0 });
  /** @type {Array<[unknown, Function]>} */
  const calls = [
    [mock.describeConfigs({ resources: [new ConfigResource({ type: ConfigResource.Type.TOPIC, name: 't' })] }), A.DescribeConfigsResult],
    [mock.alterReplicaLogDirs({ replicaAssignment: new C.TopicPartitionReplicaMap([[replica, '/tmp']]) }), A.AlterReplicaLogDirsResult],
    [mock.describeLogDirs({ brokers: [0] }), A.DescribeLogDirsResult],
    [mock.describeReplicaLogDirs({ replicas: [replica] }), A.DescribeReplicaLogDirsResult],
    [
      mock.alterPartitionReassignments({ reassignments: new C.TopicPartitionMap([[p, new A.NewPartitionReassignment([0])]]) }),
      A.AlterPartitionReassignmentsResult,
    ],
    [mock.listOffsets({ topicPartitionOffsets: new C.TopicPartitionMap([[p, A.OffsetSpec.latest()]]) }), A.ListOffsetsResult],
    [mock.describeConsumerGroups({ groupIds: ['g'] }), A.DescribeConsumerGroupsResult],
    [mock.describeClassicGroups({ groupIds: ['g'] }), A.DescribeClassicGroupsResult],
    [mock.deleteConsumerGroups({ groupIds: ['g'] }), A.DeleteConsumerGroupsResult],
    [mock.listPartitionReassignments({ options: new A.ListPartitionReassignmentsOptions() }), A.ListPartitionReassignmentsResult],
    [mock.listGroups({ options: new A.ListGroupsOptions() }), A.ListGroupsResult],
    [mock.listConsumerGroupOffsets({ groupId: 'g' }), A.ListConsumerGroupOffsetsResult],
  ];
  for (const [result, cls] of calls) assert.ok(result instanceof cls, cls.name);
  mock.updateBeginningOffsets({ newOffsets: new C.TopicPartitionMap([[p, 0n]]) });
  await mock.close();
});

test('list_topics_and_describe_configs_count_down_the_same_counter', async () => {
  const { ConfigResource } = require('@confluentinc/kafka-javascript/common/config');
  const mock = new MockAdminClient.Builder().numBrokers(3).build();
  await mock
    .createTopics([new NewTopic({ name: 't', numPartitions: 1, replicationFactor: 1 })])
    .all()
    .get();
  mock.setFetchesRemainingUntilVisible({ topicName: 't', fetchesRemainingUntilVisible: 2 });
  assert.deepEqual([...(await mock.listTopics().names().get())], [], 'the first fetch hides t');
  const resource = new ConfigResource({ type: ConfigResource.Type.TOPIC, name: 't' });
  const future = mock.describeConfigs([resource]).values().get(resource);
  assert.ok(future);
  const cause = await failure(future);
  assert.ok(cause instanceof UnknownTopicOrPartitionError, 'the second fetch hides t');
  assert.equal(cause.message, "Resource ConfigResource(type=TOPIC, name='t') not found.");
  assert.deepEqual([...(await mock.listTopics().names().get())], ['t'], 'the counter reached zero');
  await mock.close();
});

// ---- the ACL calls ------------------------------------------------------------------------
//
// Java's MockAdminClient throws UnsupportedOperationException ("Not implemented
// yet") from createAcls, describeAcls and deleteAcls; the core's mock fails
// each future with it instead (the Rust core's mock_admin_client.rs), one per
// distinct binding or filter, so the mock's Result keeps Java's keys.

const acl = require('@confluentinc/kafka-javascript/common/acl');
const res = require('@confluentinc/kafka-javascript/common/resource');

test("the ACL calls Java's mock does not implement fail their futures with UnsupportedOperationError", async () => {
  const mock = new MockAdminClient();
  const binding = new acl.AclBinding({
    pattern: new res.ResourcePattern({
      resourceType: res.ResourceType.TOPIC,
      name: 't',
      patternType: res.PatternType.LITERAL,
    }),
    entry: new acl.AccessControlEntry({
      principal: 'User:a',
      host: '*',
      operation: acl.AclOperation.READ,
      permissionType: acl.AclPermissionType.ALLOW,
    }),
  });
  const created = mock.createAcls([binding, binding]);
  assert.equal(created.values().size(), 1);
  const deleted = mock.deleteAcls([acl.AclBindingFilter.ANY, binding.toFilter()]);
  assert.equal(deleted.values().size(), 2);
  const futures = [
    created.values().get(binding),
    created.all(),
    mock.describeAcls(acl.AclBindingFilter.ANY).values(),
    deleted.values().get(acl.AclBindingFilter.ANY),
    deleted.all(),
  ];
  for (const future of futures) {
    assert.ok(future);
    const cause = await failure(future);
    assert.ok(cause instanceof UnsupportedOperationError, String(cause));
    assert.equal(cause.message, 'Not implemented yet');
  }
  await mock.close();
});

// ---- the quota, SCRAM and delegation-token calls ---------------------------------------------
//
// Java's MockAdminClient throws UnsupportedOperationException from
// describeClientQuotas and alterClientQuotas ("Not implement yet", Java's
// text) and from describeUserScramCredentials and alterUserScramCredentials
// ("Not implemented yet"); the core's mock fails each future with it instead,
// as for the ACL calls. Java's mock implements the delegation-token calls,
// which Java's MockAdminClientTest does not test: these translate the Rust
// core's tests of its mock's (rust/src/admin/mock_admin_client.rs).

const quota = require('@confluentinc/kafka-javascript/common/quota');
const { KafkaPrincipal } = require('@confluentinc/kafka-javascript/common/security/auth');
const { DelegationTokenNotFoundError, InvalidPrincipalTypeError } = require('@confluentinc/kafka-javascript/common/errors');

test("the quota and SCRAM calls Java's mock does not implement fail their futures with UnsupportedOperationError", async () => {
  const mock = new MockAdminClient();
  const A = require('@confluentinc/kafka-javascript/admin');
  const entity = new quota.ClientQuotaEntity(new Map([['user', 'a']]));
  const quotas = mock.alterClientQuotas([
    new quota.ClientQuotaAlteration({ entity, ops: [new quota.ClientQuotaAlteration.Op({ key: 'k', value: 1 })] }),
  ]);
  const quotaFutures = quotas.values();
  assert.ok(quotaFutures);
  assert.equal(quotaFutures.size(), 1);
  const scram = mock.describeUserScramCredentials(['u']);
  const altered = mock.alterUserScramCredentials([
    new A.UserScramCredentialDeletion({ user: 'u', mechanism: A.ScramMechanism.SCRAM_SHA_256 }),
  ]);
  /** @type {Array<[import('@confluentinc/kafka-javascript/common').KafkaFuture<unknown> | null, string]>} */
  const futures = [
    [mock.describeClientQuotas(quota.ClientQuotaFilter.all()).entities(), 'Not implement yet'],
    [quotaFutures.get(entity), 'Not implement yet'],
    [quotas.all(), 'Not implement yet'],
    [scram.all(), 'Not implemented yet'],
    [scram.users(), 'Not implemented yet'],
    [scram.description('u'), 'Not implemented yet'],
    [altered.values().get('u') ?? null, 'Not implemented yet'],
    [altered.all(), 'Not implemented yet'],
  ];
  for (const [future, message] of futures) {
    assert.ok(future);
    const cause = await failure(future);
    assert.ok(cause instanceof UnsupportedOperationError, String(cause));
    assert.equal(cause.message, message);
  }
  await mock.close();
});

/** @param {string} name */
const user = (name) => new KafkaPrincipal({ principalType: KafkaPrincipal.USER_TYPE, name });

test('create_delegation_token_rejects_non_user_renewer', async () => {
  const mock = track(new MockAdminClient());
  const options = new (require('@confluentinc/kafka-javascript/admin').CreateDelegationTokenOptions)().renewers([
    new KafkaPrincipal({ principalType: 'Group', name: 'admins' }),
  ]);
  const cause = await failure(mock.createDelegationToken(options).delegationToken());
  assert.ok(cause instanceof InvalidPrincipalTypeError, String(cause));
  assert.equal(cause.message, '');
});

test('create_delegation_token_without_a_renewer_reports_an_error', async () => {
  // Java's options.renewers().get(0) throws IndexOutOfBoundsException; the
  // core's mock fails the future instead, with its own message.
  const mock = track(new MockAdminClient());
  const cause = await failure(mock.createDelegationToken().delegationToken());
  assert.ok(cause instanceof IllegalArgumentError, String(cause));
  assert.equal(cause.message, 'createDelegationToken requires at least one renewer: MockAdminClient makes the first renewer the owner');
  assert.deepEqual(await mock.describeDelegationToken().delegationTokens().get(), []);
});

test('create_then_describe_lists_token', async () => {
  const { CreateDelegationTokenOptions } = require('@confluentinc/kafka-javascript/admin');
  const mock = track(new MockAdminClient());
  const token = await mock
    .createDelegationToken(new CreateDelegationTokenOptions({ renewers: [user('alice')], maxLifetimeMs: 1000 }))
    .delegationToken()
    .get();
  assert.ok(token.tokenInfo().owner().equals(user('alice')));
  assert.equal(token.tokenInfo().maxTimestamp(), 1000n);
  assert.equal(token.tokenInfo().expiryTimestamp(), -1n);
  assert.deepEqual([...token.hmac()], [...Buffer.from(token.tokenInfo().tokenId(), 'utf8')]);
  const listed = await mock.describeDelegationToken().delegationTokens().get();
  assert.equal(listed.length, 1);
  assert.ok(listed[0].equals(token));
});

test('renew_delegation_token_found_and_not_found', async () => {
  const { CreateDelegationTokenOptions, RenewDelegationTokenOptions } = require('@confluentinc/kafka-javascript/admin');
  const mock = track(new MockAdminClient());
  const unknown = await failure(
    mock
      .renewDelegationToken({ hmac: Buffer.from('nope'), options: new RenewDelegationTokenOptions({ renewTimePeriodMs: 10 }) })
      .expiryTimestamp(),
  );
  assert.ok(unknown instanceof DelegationTokenNotFoundError, String(unknown));
  assert.equal(unknown.message, '');
  const token = await mock
    .createDelegationToken(new CreateDelegationTokenOptions({ renewers: [user('alice')] }))
    .delegationToken()
    .get();
  const expiry = await mock
    .renewDelegationToken({ hmac: token.hmac(), options: new RenewDelegationTokenOptions({ renewTimePeriodMs: 4242n }) })
    .expiryTimestamp()
    .get();
  assert.equal(expiry, 4242n);
});

test('expire_delegation_token_not_found', async () => {
  const mock = track(new MockAdminClient());
  const cause = await failure(mock.expireDelegationToken(Buffer.from('nope')).expiryTimestamp());
  assert.ok(cause instanceof DelegationTokenNotFoundError, String(cause));
  assert.equal(cause.message, '');
});

test('expire_delegation_token_negative_one_removes_token', async () => {
  const { CreateDelegationTokenOptions, ExpireDelegationTokenOptions } = require('@confluentinc/kafka-javascript/admin');
  const mock = track(new MockAdminClient());
  const token = await mock
    .createDelegationToken(new CreateDelegationTokenOptions({ renewers: [user('alice')] }))
    .delegationToken()
    .get();
  const expiry = await mock
    .expireDelegationToken({ hmac: token.hmac(), options: new ExpireDelegationTokenOptions({ expiryTimePeriodMs: -1 }) })
    .expiryTimestamp()
    .get();
  assert.equal(expiry, -1n);
  assert.deepEqual(await mock.describeDelegationToken().delegationTokens().get(), []);
});

test('describe_delegation_token_owners_filter', async () => {
  const { CreateDelegationTokenOptions, DescribeDelegationTokenOptions } = require('@confluentinc/kafka-javascript/admin');
  const mock = track(new MockAdminClient());
  const alice = await mock
    .createDelegationToken(new CreateDelegationTokenOptions({ renewers: [user('alice')] }))
    .delegationToken()
    .get();
  await mock
    .createDelegationToken(new CreateDelegationTokenOptions({ renewers: [user('bob')] }))
    .delegationToken()
    .get();
  const listed = await mock
    .describeDelegationToken(new DescribeDelegationTokenOptions({ owners: [user('alice')] }))
    .delegationTokens()
    .get();
  assert.equal(listed.length, 1);
  assert.ok(listed[0].equals(alice));
});

// ---- the transaction and producer calls --------------------------------------------------------
//
// Java's MockAdminClient throws UnsupportedOperationException ("Not implemented
// yet") from describeProducers, describeTransactions, abortTransaction,
// forceTerminateTransaction, listTransactions and fenceProducers; the core's
// mock fails each future with it instead (the Rust core's
// mock_admin_client.rs), one per distinct partition or id, so the mock's
// Result keeps Java's keys.

test("the transaction and producer calls Java's mock does not implement fail their futures with UnsupportedOperationError", async () => {
  const { AbortTransactionSpec: Spec, ListTransactionsOptions: ListOptions } = require('@confluentinc/kafka-javascript/admin');
  const { TopicPartition: Tp } = require('@confluentinc/kafka-javascript/common');
  const mock = new MockAdminClient();
  const tp = new Tp({ topic: 't', partition: 0 });
  const producers = mock.describeProducers([tp, new Tp({ topic: 't', partition: 0 })]);
  const described = mock.describeTransactions(['a', 'a']);
  const listed = mock.listTransactions(new ListOptions());
  const fenced = mock.fenceProducers(['a']);
  const futures = [
    producers.partitionResult(tp),
    producers.all(),
    described.description('a'),
    described.all(),
    mock.abortTransaction(new Spec({ topicPartition: tp, producerId: 1n, producerEpoch: 0, coordinatorEpoch: 0 })).all(),
    mock.forceTerminateTransaction('a').result(),
    listed.all(),
    listed.byBrokerId(),
    listed.allByBrokerId(),
    fenced.producerId('a'),
    fenced.epochId('a'),
    fenced.all(),
    fenced.fencedProducers().get('a'),
  ];
  for (const future of futures) {
    assert.ok(future);
    const cause = await failure(future);
    assert.ok(cause instanceof UnsupportedOperationError, String(cause));
    assert.equal(cause.message, 'Not implemented yet');
  }
  assert.throws(() => described.description('b'), {
    name: 'IllegalArgumentError',
    message: 'TransactionalId `b` was not included in the request',
  });
  await mock.close();
});

// ---- the feature and controller calls ----------------------------------------------------------
//
// Java's MockAdminClientTest has no feature or controller test, so these
// translate the Rust core's tests of its mock (mock_admin_client.rs,
// mock_describe_features_* , mock_update_features_* and
// mock_unregister_controller_*), whose mock does what Java's
// MockAdminClient.describeFeatures, updateFeatures and unregisterController
// do.

const {
  DescribeFeaturesOptions: FeaturesOptions,
  FeatureUpdate,
  FinalizedVersionRange,
  SupportedVersionRange,
  UnregisterControllerOptions: ControllerOptions,
  UpdateFeaturesOptions: UpdateOptions,
} = require('@confluentinc/kafka-javascript/admin');
const { UnsupportedVersionError } = require('@confluentinc/kafka-javascript/common/errors');

/**
 * A mock seeded with one feature `feature` at level 3, supported over [1, 5].
 *
 * @returns {MockAdminClient} the mock
 */
function adminWithFeatures() {
  return MockAdminClient.create()
    .featureLevels(new Map([['feature', 3]]))
    .minSupportedFeatureLevels(new Map([['feature', 1]]))
    .maxSupportedFeatureLevels(new Map([['feature', 5]]))
    .build();
}

/**
 * Updates `feature` to `next` and reads its future's failure, or null.
 *
 * @param {MockAdminClient} mock - the mock
 * @param {number} next - the new level
 * @param {import('@confluentinc/kafka-javascript/admin').FeatureUpdate.UpgradeType} upgradeType - the kind of update
 * @returns {Promise<unknown>} the failure's cause, or null
 */
async function updateOne(mock, next, upgradeType) {
  const result = mock.updateFeatures(new Map([['feature', new FeatureUpdate({ maxVersionLevel: next, upgradeType })]]));
  const future = result.values().get('feature');
  assert.ok(future);
  try {
    await future.get();
    return null;
  } catch (e) {
    assert.ok(e instanceof ExecutionError);
    return e.cause;
  }
}

/**
 * The finalized range of `feature`.
 *
 * @param {MockAdminClient} mock - the mock
 * @returns {Promise<unknown>} the range
 */
async function finalized(mock) {
  return (await mock.describeFeatures().featureMetadata().get()).finalizedFeatures().get('feature');
}

test('mock_describe_features_returns_seeded_ranges: the epoch is 123, the ranges the seeded levels', async () => {
  const mock = adminWithFeatures();
  const metadata = await mock.describeFeatures(new FeaturesOptions()).featureMetadata().get();
  assert.equal(metadata.finalizedFeaturesEpoch(), 123n);
  assert.ok(new FinalizedVersionRange({ minVersionLevel: 3, maxVersionLevel: 3 }).equals(metadata.finalizedFeatures().get('feature')));
  assert.ok(new SupportedVersionRange({ minVersion: 1, maxVersion: 5 }).equals(metadata.supportedFeatures().get('feature')));
  await mock.close();
});

test('mock_update_features_upgrade_succeeds_and_mutates_level', async () => {
  const mock = adminWithFeatures();
  assert.equal(await updateOne(mock, 4, FeatureUpdate.UpgradeType.UPGRADE), null);
  assert.ok(new FinalizedVersionRange({ minVersionLevel: 4, maxVersionLevel: 4 }).equals(await finalized(mock)));
  await mock.close();
});

test('mock_update_features_validate_only_does_not_mutate', async () => {
  const mock = adminWithFeatures();
  const result = mock.updateFeatures({
    featureUpdates: { feature: new FeatureUpdate({ maxVersionLevel: 4, upgradeType: FeatureUpdate.UpgradeType.UPGRADE }) },
    options: new UpdateOptions({ validateOnly: true }),
  });
  await result.all().get();
  assert.ok(new FinalizedVersionRange({ minVersionLevel: 3, maxVersionLevel: 3 }).equals(await finalized(mock)));
  await mock.close();
});

test("mock_update_features' refusals: the whole batch fails with Java's InvalidRequestException message", async () => {
  const { UPGRADE, SAFE_DOWNGRADE, UNKNOWN } = FeatureUpdate.UpgradeType;
  const cases = /** @type {Array<[number, import('@confluentinc/kafka-javascript/admin').FeatureUpdate.UpgradeType, string]>} */ ([
    [2, UPGRADE, "Invalid update version 2 for feature feature. Can't upgrade to lower version."],
    [4, SAFE_DOWNGRADE, "Invalid update version 4 for feature feature. Can't downgrade to newer version."],
    [6, UPGRADE, "Invalid update version 6 for feature feature. Can't upgrade above 5"],
    // A deletion (level 0) with SAFE_DOWNGRADE; the minimum is 1.
    [0, SAFE_DOWNGRADE, "Invalid update version 0 for feature feature. Can't downgrade below 1"],
    [2, UNKNOWN, 'Invalid update version 2 for feature feature. Invalid upgrade type.'],
  ]);
  for (const [next, type, message] of cases) {
    const mock = adminWithFeatures();
    const cause = await updateOne(mock, next, type);
    assert.ok(cause instanceof InvalidRequestError, String(cause));
    assert.equal(cause.message, message);
    // The level is left as it was.
    assert.ok(new FinalizedVersionRange({ minVersionLevel: 3, maxVersionLevel: 3 }).equals(await finalized(mock)));
    await mock.close();
  }
});

test('mock_update_features_unsafe_downgrade_succeeds: the even-level guard is dead code, as in Java', async () => {
  const mock = adminWithFeatures();
  assert.equal(await updateOne(mock, 2, FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE), null);
  assert.ok(new FinalizedVersionRange({ minVersionLevel: 2, maxVersionLevel: 2 }).equals(await finalized(mock)));
  await mock.close();
});

test("updateFeatures on the mock: an empty map is no error, as Java's mock checks none", async () => {
  const mock = adminWithFeatures();
  const result = mock.updateFeatures(new Map());
  assert.equal(result.values().size, 0);
  await result.all().get();
  await mock.close();
});

test("mock_unregister_controller_without_raft_controller_is_unsupported: Java's UnsupportedVersionException('')", async () => {
  const mock = new MockAdminClient();
  const cause = await failure(mock.unregisterController(1).all());
  assert.ok(cause instanceof UnsupportedVersionError, String(cause));
  assert.equal(cause.message, '');
  await mock.close();
});

// Java's future is complete at return; the mock's shows its outcome on a later
// turn of the event loop (a deviation MockAdminClient's documentation lists), so the test
// awaits it.
test('mock_unregister_controller_with_raft_controller_succeeds', async () => {
  const mock = MockAdminClient.create().usingRaftController(true).build();
  const result = mock.unregisterController({ controllerId: 1, options: new ControllerOptions() });
  await result.all().get();
  assert.equal(result.all().isDone(), true);
  await mock.close();
});
