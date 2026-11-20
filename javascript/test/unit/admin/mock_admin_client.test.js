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
// describeStreamsGroups, which the C FFI does not have, so, as the JS
// conventions ask of a class Java ships without tests of its own, these are
// the Rust core's tests of its translation (rust/src/admin/mock_admin_client.rs),
// for the cluster, the builder and the listing of topics, read through the
// public API; then the binding's own: the constructors' and setters' forms.

const assert = require('node:assert/strict');

const { ExecutionError, IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { DescribeClusterOptions, ListTopicsOptions, MockAdminClient } = require('@confluentinc/kafka-javascript/admin');
const { Node } = require('@confluentinc/kafka-javascript/common');
const { TimeoutError } = require('@confluentinc/kafka-javascript/common/errors');

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
