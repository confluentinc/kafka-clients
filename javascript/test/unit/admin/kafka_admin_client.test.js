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

// KafkaAdminClient over the FFI. Java's KafkaAdminClientTest and its siblings
// (KafkaAdminClientConfigTest, KafkaAdminClientTopicTest and the rest on
// KafkaAdminClientTestBase) drive Java's MockClient, which the binding cannot
// inject: they are skipped, their translations being the Rust core's
// (rust/src/admin/kafka_admin_client.rs). Where Java uses the double only to
// make a call fail, the port runs against an unreachable bootstrap address,
// as Python's test_admin_java_ports.py does; those of this file cover
// describeCluster, listTopics, the client's lifecycle and configuration, and
// the topic calls: KafkaAdminClientTest#testAdminClientFailureWhenClosed and
// #testTimeoutWithoutMetadata on createTopics, as Java writes them, and
// KafkaAdminClientTopicTest#testInvalidTopicNames and
// #testDescribeTopicsTimeoutWhenNoBrokerResponds.
// AdminClientConfigTest is ported through Admin.create, as SaslConfigsTest is
// through the producer's constructor: testInvalidMetadataRecoveryStrategy
// here; testDefaultMetadataRecoveryStrategy is skipped, as AdminClientConfig
// is translated for its constants only, so the parsed value cannot be read
// (core: admin_client_config.rs test_default_metadata_recovery_strategy).
// For the config and log-dir calls, KafkaAdminClientTopicTest's isCordoned
// assertions (testDescribeLogDirsWithCordonedDir and assertDescriptionContains)
// are ported as Python ports them, over the conversion of the addon's rows
// (test/unit/internal/admin_config_calls.test.js); the rest of those calls'
// Java tests need MockClient. KafkaAdminClientTopicTest#testElectLeaders,
// #testAlterPartitionReassignments, #testListPartitionReassignments and the
// #testListOffsets* tests drive MockClient too (core: test_elect_leaders,
// test_list_partition_reassignments, test_list_offsets); their calls are
// ported as Python ports them, timing out against the unreachable address
// with the core's message.
// The group calls' Java tests (KafkaAdminClientConsumerGroupTest, on
// KafkaAdminClientTestBase) drive MockClient too (core: the group tests of
// kafka_admin_client.rs); testListGroupsMetadataFailure and its consumer,
// share and streams forms are ported as Python ports them (an unreachable
// bootstrap finds no broker), and each group call times out with the core's
// message, as python/test/unit/test_admin_client_groups.py checks.
// Then the binding's own: the forms, liveness and the native objects'
// lifetimes, each in a child process.

const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

const { ExecutionError, IllegalArgumentError, IllegalStateError } = require('@confluentinc/kafka-javascript');
const {
  Admin,
  AlterConsumerGroupOffsetsOptions,
  AlterPartitionReassignmentsOptions,
  CreateTopicsOptions,
  DeleteConsumerGroupOffsetsOptions,
  DeleteConsumerGroupsOptions,
  DescribeClassicGroupsOptions,
  DescribeClusterOptions,
  DescribeConsumerGroupsOptions,
  DescribeTopicsOptions,
  ElectLeadersOptions,
  KafkaAdminClient,
  ListConsumerGroupOffsetsOptions,
  ListConsumerGroupOffsetsSpec,
  ListGroupsOptions,
  ListOffsetsOptions,
  ListPartitionReassignmentsOptions,
  ListTopicsOptions,
  NewPartitionReassignment,
  MemberToRemove,
  NewTopic,
  OffsetSpec,
  RemoveMembersFromConsumerGroupOptions,
} = require('@confluentinc/kafka-javascript/admin');
const { OffsetAndMetadata } = require('@confluentinc/kafka-javascript/consumer');
const { ElectionType, KafkaError, TopicPartition, TopicPartitionMap } = require('@confluentinc/kafka-javascript/common');
const { ConfigError } = require('@confluentinc/kafka-javascript/common/config');
const { BootstrapResolutionError, InvalidTopicError, TimeoutError } = require('@confluentinc/kafka-javascript/common/errors');

/**
 * @template T
 * @typedef {import('@confluentinc/kafka-javascript/common').KafkaFuture<T>} KafkaFuture
 */
const { POISON } = require('../../memory/_probe');

const UNREACHABLE = '127.0.0.1:1';

/**
 * An admin client over an unreachable bootstrap address.
 *
 * @param {Record<string, unknown>} [configs]
 */
function admin(configs = {}) {
  return Admin.create({ 'bootstrap.servers': UNREACHABLE, ...configs });
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

/**
 * Runs `script` in a child Node process, from the package root.
 *
 * @param {string} script
 * @param {number} [timeout]
 * @param {Array<string>} [flags]
 */
function runNode(script, timeout = 30000, flags = []) {
  const started = process.hrtime.bigint();
  const r = spawnSync(process.execPath, ['--no-warnings', ...flags, '-e', script], {
    cwd: path.join(__dirname, '..', '..', '..'),
    encoding: 'utf8',
    timeout,
    env: { ...process.env, ...POISON },
  });
  return { ...r, ms: Number((process.hrtime.bigint() - started) / 1000000n) };
}

// ---- AdminClientConfigTest --------------------------------------------------

test('testInvalidMetadataRecoveryStrategy: a ConfigError naming the key', () => {
  assert.throws(
    () => admin({ 'metadata.recovery.strategy': 'abc' }),
    (error) => {
      assert.ok(error instanceof ConfigError);
      assert.equal(
        error.message,
        'Invalid value abc for configuration metadata.recovery.strategy: String must be one of (case insensitive): REBOOTSTRAP, NONE',
      );
      return true;
    },
  );
});

test("testDefaultMetadataRecoveryStrategy's accepted side: the default and the two names build a client", async () => {
  await admin().close();
  for (const strategy of ['rebootstrap', 'none']) await admin({ 'metadata.recovery.strategy': strategy }).close();
});

// ---- KafkaAdminClientTest -----------------------------------------------------

test("testDefaultApiTimeoutAndRequestTimeoutConflicts: Java's createInternal wraps the ConfigError", () => {
  assert.throws(
    () => admin({ 'default.api.timeout.ms': 500 }),
    (error) => {
      assert.ok(error instanceof KafkaError && error.constructor === KafkaError);
      assert.equal(error.message, 'Failed to create new KafkaAdminClient');
      assert.ok(error.cause instanceof ConfigError);
      assert.equal(
        /** @type {Error} */ (error.cause).message,
        'The specified value of default.api.timeout.ms must be no smaller than the value of request.timeout.ms.',
      );
      return true;
    },
  );
});

test('testAdminConstructorFailsWithConfigExceptionOnUnresolvableBootstrapWhenTimeoutZero', () => {
  assert.throws(
    () => Admin.create({ configs: { 'bootstrap.servers': 'unresolvable.invalid:9092' } }),
    (error) => {
      assert.ok(error instanceof KafkaError);
      assert.equal(error.message, 'Failed to create new KafkaAdminClient');
      assert.equal(/** @type {Error} */ (error.cause).constructor, ConfigError);
      assert.equal(/** @type {Error} */ (error.cause).message, 'No resolvable bootstrap urls given in bootstrap.servers');
      return true;
    },
  );
});

test('testAdminBootstrapResolutionExceptionPropagated: every call fails once the budget ran out', async () => {
  const client = Admin.create({ 'bootstrap.servers': 'unresolvable.invalid:9092', 'bootstrap.resolve.timeout.ms': 3000 });
  try {
    const started = Date.now();
    /** @type {unknown} */
    let cause = null;
    while (!(cause instanceof BootstrapResolutionError)) {
      assert.ok(Date.now() - started < 15000, 'Expected BootstrapResolutionError to be thrown within 15000ms');
      cause = await failure(client.listTopics().names());
    }
    assert.equal(
      /** @type {Error} */ (cause).message,
      'Failed to resolve bootstrap servers after 3000ms. Please check your bootstrap.servers configuration and DNS settings.',
    );
    assert.ok((await failure(client.listTopics().names())) instanceof BootstrapResolutionError);
  } finally {
    await client.close();
  }
}, 30000);

test("testCloseAdminClient: close() resolves, and a call after it fails its futures with the core's error", async () => {
  const client = admin();
  assert.ok(client instanceof KafkaAdminClient);
  await client.close();
  // Java's testAdminClientFailureWhenClosed: the call returns its Result,
  // whose futures fail with IllegalStateException.
  const cause = await failure(client.listTopics().namesToListings());
  assert.equal(/** @type {Error} */ (cause).name, 'IllegalStateError');
  assert.equal(/** @type {Error} */ (cause).message, 'Cannot accept new calls when AdminClient is closing.');
  // A second close resolves too.
  await client.close();
});

test('testCloseAdminClientInCallback: close() from an action of a future returns', async () => {
  const client = admin();
  const result = client.listTopics(new ListTopicsOptions({ timeoutMs: 1000 }));
  /** @type {Promise<void> | null} */
  let closing = null;
  result.listings().whenComplete(() => {
    closing = client.close();
  });
  await failure(result.listings());
  assert.ok(closing);
  await closing;
});

test("a call that finds no node times out with the core's TimeoutError", async () => {
  const client = admin({ 'request.timeout.ms': 10 });
  try {
    const cause = await failure(client.describeCluster(new DescribeClusterOptions({ timeoutMs: 500 })).nodes());
    assert.ok(cause instanceof TimeoutError);
    assert.match(/** @type {Error} */ (cause).message, /^Timed out waiting for a node assignment\. Call: /);
  } finally {
    await client.close({ timeout: 0 });
  }
});

test("the leader, reassignment and offset calls return at once and time out with the core's message naming Java's call", async () => {
  const client = admin();
  const tp = new TopicPartition({ topic: 't', partition: 0 });
  /** @type {Array<[string, () => KafkaFuture<unknown>]>} */
  const calls = [
    [
      'electLeaders',
      () =>
        client
          .electLeaders({ electionType: ElectionType.PREFERRED, partitions: [tp], options: new ElectLeadersOptions({ timeoutMs: 300 }) })
          .partitions(),
    ],
    [
      'alterPartitionReassignments',
      () =>
        client
          .alterPartitionReassignments({
            reassignments: new TopicPartitionMap([[tp, new NewPartitionReassignment([1])]]),
            options: new AlterPartitionReassignmentsOptions({ timeoutMs: 300 }),
          })
          .all(),
    ],
    [
      'listPartitionReassignments',
      () => client.listPartitionReassignments(new ListPartitionReassignmentsOptions({ timeoutMs: 300 })).reassignments(),
    ],
    [
      'listOffsets(api=METADATA)',
      () =>
        client
          .listOffsets({
            topicPartitionOffsets: new TopicPartitionMap([[tp, OffsetSpec.latest()]]),
            options: new ListOffsetsOptions().timeoutMs(300),
          })
          .all(),
    ],
  ];
  try {
    for (const [call, run] of calls) {
      const started = Date.now();
      const future = run();
      assert.ok(Date.now() - started < 1000, call);
      const cause = await failure(future);
      assert.ok(cause instanceof TimeoutError, call);
      assert.equal(cause.message, `Timed out waiting for a node assignment. Call: ${call}`);
    }
  } finally {
    await client.close({ timeout: 0 });
  }
});

test('testListGroupsMetadataFailure and its consumer, share and streams forms: no broker found, the error an element', async () => {
  const client = admin({ retries: 0 });
  try {
    for (const options of [
      new ListGroupsOptions({ timeoutMs: 300 }),
      ListGroupsOptions.forConsumerGroups().timeoutMs(300),
      ListGroupsOptions.forStreamsGroups().timeoutMs(300),
      ListGroupsOptions.forShareGroups().timeoutMs(300),
    ]) {
      const result = client.listGroups(options);
      const cause = await failure(result.all());
      assert.ok(cause instanceof KafkaError && cause.constructor === KafkaError, String(cause));
      assert.equal(cause.message, 'Failed to find brokers to send ListGroups');
      assert.deepEqual(await result.valid().get(), []);
      const errors = await result.errors().get();
      assert.equal(errors.length, 1);
      const [error] = errors;
      assert.ok(error instanceof KafkaError);
      assert.equal(error.message, 'Failed to find brokers to send ListGroups');
      assert.ok(error.cause instanceof TimeoutError);
      assert.equal(error.cause.message, 'Timed out waiting for a node assignment. Call: findAllBrokers');
      // Java's fields: the same future on every call.
      assert.equal(result.all(), result.all());
    }
  } finally {
    await client.close({ timeout: 0 });
  }
});

test("the group calls return at once and time out with the core's message naming Java's call", async () => {
  const client = admin({ 'default.api.timeout.ms': 300, 'request.timeout.ms': 100 });
  const tp = new TopicPartition({ topic: 't', partition: 0 });
  const member = new MemberToRemove('i1');
  const timedOut = (/** @type {string} */ call) => `Timed out waiting for a node assignment. Call: ${call}(api=FIND_COORDINATOR)`;
  try {
    const described = client.describeConsumerGroups({
      groupIds: ['g1', 'g2'],
      options: new DescribeConsumerGroupsOptions({ timeoutMs: 300 }),
    });
    for (const future of described.describedGroups().values())
      assert.equal(/** @type {Error} */ (await failure(future)).message, timedOut('describeConsumerGroups'));
    assert.equal(/** @type {Error} */ (await failure(described.all())).message, timedOut('describeConsumerGroups'));
    assert.equal((await client.describeConsumerGroups([]).all().get()).size, 0);
    const classic = client.describeClassicGroups({ groupIds: ['g'], options: new DescribeClassicGroupsOptions({ timeoutMs: 300 }) });
    const classicFuture = classic.describedGroups().get('g');
    assert.ok(classicFuture);
    assert.equal(/** @type {Error} */ (await failure(classicFuture)).message, timedOut('describeClassicGroups'));
    let offsets = client.listConsumerGroupOffsets({ groupId: 'g', options: new ListConsumerGroupOffsetsOptions({ timeoutMs: 300 }) });
    assert.equal(/** @type {Error} */ (await failure(offsets.partitionsToOffsetAndMetadata())).message, timedOut('offsetFetch'));
    offsets = client.listConsumerGroupOffsets({
      groupSpecs: { a: new ListConsumerGroupOffsetsSpec({ topicPartitions: [tp] }), b: new ListConsumerGroupOffsetsSpec() },
      options: new ListConsumerGroupOffsetsOptions({ timeoutMs: 300, requireStable: true }),
    });
    for (const groupId of ['a', 'b']) {
      assert.equal(/** @type {Error} */ (await failure(offsets.partitionsToOffsetAndMetadata(groupId))).message, timedOut('offsetFetch'));
    }
    assert.throws(() => offsets.partitionsToOffsetAndMetadata('c'), {
      name: 'IllegalArgumentError',
      message: "Offsets for consumer group 'c' were not requested.",
    });
    assert.throws(() => offsets.partitionsToOffsetAndMetadata(), {
      name: 'IllegalStateError',
      message:
        'Offsets from multiple consumer groups were requested. Use partitionsToOffsetAndMetadata(groupId) instead to get future for a specific group.',
    });
    assert.equal((await client.listConsumerGroupOffsets({ groupSpecs: {} }).all().get()).size, 0);
    const deleted = client.deleteConsumerGroups({ groupIds: ['g'], options: new DeleteConsumerGroupsOptions({ timeoutMs: 300 }) });
    assert.equal(/** @type {Error} */ (await failure(deleted.all())).message, timedOut('deleteConsumerGroups'));
    const offsetDelete = client.deleteConsumerGroupOffsets({
      groupId: 'g',
      partitions: [tp],
      options: new DeleteConsumerGroupOffsetsOptions({ timeoutMs: 300 }),
    });
    assert.equal(/** @type {Error} */ (await failure(offsetDelete.partitionResult(tp))).message, timedOut('offsetDelete'));
    assert.equal(/** @type {Error} */ (await failure(offsetDelete.all())).message, timedOut('offsetDelete'));
    // No partition: the call's own future still times out (the core's all()).
    const none = client.deleteConsumerGroupOffsets({
      groupId: 'g',
      partitions: [],
      options: new DeleteConsumerGroupOffsetsOptions({ timeoutMs: 300 }),
    });
    assert.equal(/** @type {Error} */ (await failure(none.all())).message, timedOut('offsetDelete'));
    const altered = client.alterConsumerGroupOffsets({
      groupId: 'g',
      offsets: new TopicPartitionMap([[tp, new OffsetAndMetadata(5n)]]),
      options: new AlterConsumerGroupOffsetsOptions({ timeoutMs: 300 }),
    });
    assert.equal(/** @type {Error} */ (await failure(altered.partitionResult(tp))).message, timedOut('offsetCommit'));
    assert.equal(/** @type {Error} */ (await failure(altered.all())).message, timedOut('offsetCommit'));
    const removed = client.removeMembersFromConsumerGroup({ groupId: 'g', options: new RemoveMembersFromConsumerGroupOptions([member]) });
    assert.equal(/** @type {Error} */ (await failure(removed.memberResult(member))).message, timedOut('leaveGroup'));
    assert.equal(/** @type {Error} */ (await failure(removed.all())).message, timedOut('leaveGroup'));
    assert.throws(() => removed.memberResult(new MemberToRemove('other')), {
      name: 'IllegalArgumentError',
      message: 'Member other was not included in the original request',
    });
    // removeAll mode first describes the group with the default options; the
    // core writes "error" where Java's message says "exception".
    const all = await failure(
      client.removeMembersFromConsumerGroup({ groupId: 'g', options: new RemoveMembersFromConsumerGroupOptions() }).all(),
    );
    assert.ok(all instanceof KafkaError);
    assert.equal(all.message, 'Encounter error when trying to get members from group: g');
    assert.ok(all.cause instanceof TimeoutError);
    assert.equal(all.cause.message, timedOut('describeConsumerGroups'));
  } finally {
    await client.close({ timeout: 0 });
  }
});

test("the group calls' forms and null arguments: TypeError naming the RPC's own parameter", async () => {
  const client = admin();
  try {
    // A string is an Iterable<string> to tsc: only the run time refuses it.
    assert.throws(() => client.describeConsumerGroups('g'), {
      name: 'TypeError',
      message: 'groupIds must be an iterable of strings, not string',
    });
    // @ts-expect-error a null id, which the C API cannot carry
    assert.throws(() => client.deleteConsumerGroups([null]), { name: 'TypeError', message: 'groupIds must not contain null' });
    // @ts-expect-error a null group id
    assert.throws(() => client.listConsumerGroupOffsets(null), { name: 'TypeError', message: 'groupId must not be null' });
    // @ts-expect-error the names of two forms at once
    assert.throws(() => client.listConsumerGroupOffsets({ groupId: 'g', groupSpecs: new Map() }), {
      name: 'IllegalArgumentError',
      message:
        'listConsumerGroupOffsets() takes one of (groupId), (groupId, options), (groupSpecs), (groupSpecs, options); got (groupId, groupSpecs)',
    });
    assert.throws(
      // @ts-expect-error a null spec
      () => client.listConsumerGroupOffsets({ groupSpecs: { g: null } }),
      { name: 'TypeError', message: 'groupSpecs must not contain null' },
    );
    assert.throws(
      // @ts-expect-error a plain Map where Java takes a Map<TopicPartition, OffsetAndMetadata>
      () => client.alterConsumerGroupOffsets({ groupId: 'g', offsets: new Map() }),
      { name: 'TypeError', message: 'offsets must be a TopicPartitionMap, not a Map: build one with new TopicPartitionMap()' },
    );
    const nullOffset = new TopicPartitionMap([[new TopicPartition({ topic: 't', partition: 0 }), null]]);
    // @ts-expect-error a null offset
    assert.throws(() => client.alterConsumerGroupOffsets({ groupId: 'g', offsets: nullOffset }), {
      name: 'TypeError',
      message: 'offsets must not contain null',
    });
    // @ts-expect-error a null element, which Java's request builder dereferences
    assert.throws(() => client.deleteConsumerGroupOffsets({ groupId: 'g', partitions: [null] }), {
      name: 'TypeError',
      message: 'partitions must not contain null',
    });
    assert.throws(
      // @ts-expect-error the options left out: Java has only the (groupId, options) form
      () => client.removeMembersFromConsumerGroup({ groupId: 'g' }),
      { name: 'IllegalArgumentError', message: 'removeMembersFromConsumerGroup() takes one of (groupId, options); got (groupId)' },
    );
    const nullMember = new RemoveMembersFromConsumerGroupOptions([new MemberToRemove(null)]);
    assert.throws(() => client.removeMembersFromConsumerGroup({ groupId: 'g', options: nullMember }), {
      name: 'TypeError',
      message: 'RemoveMembersFromConsumerGroupOptions.members must not contain a MemberToRemove with a null groupInstanceId',
    });
  } finally {
    await client.close({ timeout: 0 });
  }
});

test('a timed get waits through the core, which keeps the time', async () => {
  const client = admin();
  try {
    const future = client.listTopics(new ListTopicsOptions({ timeoutMs: 5000 })).namesToListings();
    await assert.rejects(future.get({ timeout: 100 }), { name: 'TimeoutError', message: 'Timed out waiting for KafkaFuture after 100 ms' });
    assert.equal(future.isDone(), false);
    assert.equal(future.getNow({ valueIfAbsent: null }), null);
  } finally {
    await client.close({ timeout: 0 });
  }
});

test('cancel() cancels the JavaScript future, not the request', async () => {
  const client = admin();
  try {
    const result = client.listTopics(new ListTopicsOptions({ timeoutMs: 300 }));
    assert.equal(result.namesToListings().cancel(true), true);
    assert.equal(result.namesToListings().isCancelled(), true);
    await assert.rejects(result.namesToListings().get(), { name: 'CancellationError' });
  } finally {
    await client.close({ timeout: 0 });
  }
});

test('testAdminClientFailureWhenClosed: createTopics on a closed client fails its futures with IllegalStateError', async () => {
  const client = admin();
  await client.close();
  const topic = new NewTopic({ name: 'myTopic', replicasAssignments: new Map([[0, [0, 1, 2]]]) });
  const cause = await failure(client.createTopics({ newTopics: [topic], options: new CreateTopicsOptions({ timeoutMs: 10000 }) }).all());
  assert.ok(cause instanceof IllegalStateError, `Expected an IllegalStateException error, but got ${String(cause)}`);
});

test('testTimeoutWithoutMetadata: createTopics times out when no metadata arrives', async () => {
  const client = admin({ 'request.timeout.ms': 10 });
  try {
    const topic = new NewTopic({ name: 'myTopic', replicasAssignments: new Map([[0, [0, 1, 2]]]) });
    const cause = await failure(client.createTopics({ newTopics: [topic], options: new CreateTopicsOptions({ timeoutMs: 1000 }) }).all());
    assert.ok(cause instanceof TimeoutError, String(cause));
  } finally {
    await client.close();
  }
});

// ---- KafkaAdminClientTopicTest ------------------------------------------------

test("testInvalidTopicNames: an empty name fails its deleteTopics, describeTopics and createTopics futures, Java's null name aside", async () => {
  // Java's null name: the C FFI refuses a null element, where Java fails its
  // future (asList("", null)); the empty name stands for both.
  const client = admin();
  try {
    const deleted = client.deleteTopics(['']).topicNameValues();
    assert.ok(deleted);
    assert.ok((await failure(/** @type {KafkaFuture<void>} */ (deleted.get('')))) instanceof InvalidTopicError);
    const described = client.describeTopics(['']).topicNameValues();
    assert.ok(described);
    assert.ok((await failure(/** @type {KafkaFuture<unknown>} */ (described.get('')))) instanceof InvalidTopicError);
    const created = client.createTopics([new NewTopic({ name: '', numPartitions: 1, replicationFactor: 1 })]).values();
    const cause = await failure(/** @type {KafkaFuture<void>} */ (created.get('')));
    assert.ok(cause instanceof InvalidTopicError, String(cause));
    assert.equal(/** @type {Error} */ (cause).message, "The given topic name '' cannot be represented in a request.");
  } finally {
    await client.close();
  }
});

test('testDescribeTopicsTimeoutWhenNoBrokerResponds: the call timeout, well before the request timeout', async () => {
  const client = admin({ retries: 0, 'request.timeout.ms': 30000 });
  try {
    const start = Date.now();
    const result = client.describeTopics({ topicNames: ['test-topic'], options: new DescribeTopicsOptions({ timeoutMs: 200 }) });
    const values = result.topicNameValues();
    assert.ok(values);
    const cause = await failure(/** @type {KafkaFuture<unknown>} */ (values.get('test-topic')));
    const duration = Date.now() - start;
    assert.ok(cause instanceof TimeoutError, String(cause));
    assert.ok(duration >= 150 && duration < 30000, `took ${duration} ms`);
  } finally {
    await client.close();
  }
}, 40000);

// ---- the binding's own ----------------------------------------------------------

test("Admin.create takes the configs bare or by name, and Java's form errors", async () => {
  await Admin.create({ configs: { 'bootstrap.servers': UNREACHABLE } }).close();
  // @ts-expect-error a Map: a plain object of keys is taken
  assert.throws(() => Admin.create(new Map([['bootstrap.servers', UNREACHABLE]])), {
    name: 'TypeError',
    message: 'configs must be a plain object, not a Map',
  });
  // @ts-expect-error two arguments
  assert.throws(() => Admin.create({ configs: {} }, {}), IllegalArgumentError);
  // A null configuration: Java copies the map. (tsc reads the object as a
  // configuration map holding a key `configs`, a limit of tsc.)
  assert.throws(() => Admin.create({ configs: null }), { name: 'TypeError', message: 'configs must not be null' });
  assert.throws(() => Admin.create({ configs: { 'bootstrap.servers': UNREACHABLE, [Symbol('k')]: 1 } }), {
    name: 'ConfigError',
    message: 'Invalid value 1 for configuration Symbol(k): Key must be a string.',
  });
});

test("the RPCs' forms: nothing, the options on their own or by name; Java's null is the FFI's TypeError", async () => {
  const client = admin();
  try {
    client.describeCluster();
    client.describeCluster(new DescribeClusterOptions());
    client.describeCluster({ options: new DescribeClusterOptions() });
    client.listTopics(new ListTopicsOptions());
    // @ts-expect-error a ListTopicsOptions for describeCluster: the form check throws IllegalArgumentError
    assert.throws(() => client.describeCluster(new ListTopicsOptions()), {
      name: 'IllegalArgumentError',
      message: 'describeCluster() takes one of (), (options); got a ListTopicsOptions',
    });
    // @ts-expect-error a plain object of the options' keys: an instance is taken
    assert.throws(() => client.listTopics({ options: { listInternal: true } }), {
      name: 'TypeError',
      message: 'options must be a ListTopicsOptions, not an object: build one with new ListTopicsOptions()',
    });
    // @ts-expect-error an unknown key: the form check throws IllegalArgumentError
    assert.throws(() => client.listTopics({ timeoutMs: 5 }), {
      name: 'IllegalArgumentError',
      message: 'listTopics() takes one of (), (options); got (timeoutMs)',
    });
    // @ts-expect-error null options: Java dereferences them, the FFI's TypeError
    assert.throws(() => client.describeCluster(null), { name: 'TypeError', message: 'options must not be null' });
    // @ts-expect-error undefined is never a value
    assert.throws(() => client.listTopics(undefined), TypeError);
  } finally {
    await client.close({ timeout: 0 });
  }
});

test("close()'s forms: a negative timeout is read as zero, a form Java has not is refused", async () => {
  const client = admin();
  // @ts-expect-error a string: the form check rejects it
  await assert.rejects(client.close('1'), { name: 'IllegalArgumentError', message: 'close() takes one of (), (timeout); got a string' });
  await client.close(-1);
  await client.close({ timeout: Infinity });
});

test('an open admin client keeps the process running until close() closes it, and a closed one lets it exit', () => {
  let r = runNode(
    `
    const { Admin } = require('@confluentinc/kafka-javascript/admin');
    const a = Admin.create({ 'bootstrap.servers': '${UNREACHABLE}' });
    setTimeout(() => a.close().then(() => console.log('closed')), 1000).unref();
  `,
  );
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'closed\n');
  assert.ok(r.ms >= 900, `exited after ${r.ms} ms`);
  r = runNode(
    `
    const { Admin } = require('@confluentinc/kafka-javascript/admin');
    Admin.create({ 'bootstrap.servers': '${UNREACHABLE}' });
    console.log('created');
  `,
    3000,
  );
  assert.equal(r.stdout, 'created\n');
  assert.equal(r.signal, 'SIGTERM', `the process exited on its own: status ${r.status}`);
});

test('a failed admin future nobody reads raises no unhandled rejection', () => {
  const r = runNode(`
    const { Admin, MockAdminClient } = require('@confluentinc/kafka-javascript/admin');
    process.on('unhandledRejection', (e) => console.log('unhandled:', e && e.message));
    (async () => {
      const a = Admin.create({ 'bootstrap.servers': '${UNREACHABLE}' });
      await a.close();
      // A closed client's futures fail, unread.
      a.describeCluster();
      a.listTopics();
      const mock = new MockAdminClient();
      mock.timeoutNextRequest(2);
      // A mock's timed-out futures, unread.
      mock.describeCluster();
      mock.listTopics();
      await new Promise((resolve) => setTimeout(resolve, 500));
      await mock.close();
      console.log('done');
    })();
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'done\n');
});

test('a call made on a closed admin client holds Node until its answer', () => {
  const r = runNode(`
    const { Admin } = require('@confluentinc/kafka-javascript/admin');
    const a = Admin.create({ 'bootstrap.servers': '${UNREACHABLE}' });
    a.close().then(async () => {
      try { await a.describeCluster().clusterId().get(); } catch (e) { console.log(e.cause.message); }
    });
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'Cannot accept new calls when AdminClient is closing.\n');
});

test('the native objects are freed: the client once closed and collected, each result and future once read', () => {
  const r = runNode(
    `
    const native = require('./dist/internal/native.js');
    const { Admin, AlterConfigOp, ConfigEntry, MemberToRemove, MockAdminClient, NewPartitionReassignment, NewTopic, OffsetSpec, RemoveMembersFromConsumerGroupOptions } = require('@confluentinc/kafka-javascript/admin');
    const { OffsetAndMetadata } = require('@confluentinc/kafka-javascript/consumer');
    const { ElectionType, TopicCollection, TopicPartition, TopicPartitionMap, TopicPartitionReplica, TopicPartitionReplicaMap } = require('@confluentinc/kafka-javascript/common');
    const { ConfigResource, ConfigResourceMap } = require('@confluentinc/kafka-javascript/common/config');
    const { CreateDelegationTokenOptions } = require('@confluentinc/kafka-javascript/admin');
    const { AclBindingFilter } = require('@confluentinc/kafka-javascript/common/acl');
    const { ClientQuotaFilter } = require('@confluentinc/kafka-javascript/common/quota');
    const { KafkaPrincipal } = require('@confluentinc/kafka-javascript/common/security/auth');
    (async () => {
      const before = native.adminStats();
      // One function per round, so no client stays in a frame after it.
      async function round() {
        const a = Admin.create({ 'bootstrap.servers': '${UNREACHABLE}' });
        a.listTopics();
        await a.close({ timeout: 0 });
        const m = new MockAdminClient();
        await m.describeCluster().clusterId().get();
        await m.close();
        // The map accessors (each future holding the map's box) and the
        // refinements, read and unread.
        const t = new MockAdminClient.Builder().numBrokers(1).build();
        const created = t.createTopics([new NewTopic({ name: 't', numPartitions: 1, replicationFactor: 1 })]);
        await created.all().get();
        await created.numPartitions('t').get();
        created.config('t');
        await created.topicId('absent').get().catch(() => {});
        const byName = t.describeTopics(['t', 'u']);
        await byName.topicNameValues().get('t').get();
        const id = (await t.describeTopics(['t']).allTopicNames().get()).get('t').topicId;
        t.describeTopics(TopicCollection.ofTopicIds([id]));
        // The config and log-dir calls: their map futures (keyed by
        // ConfigResource, broker id and TopicPartitionReplica) and one future.
        const res = new ConfigResource({ type: ConfigResource.Type.TOPIC, name: 't' });
        const configs = t.describeConfigs([res]);
        await configs.all().get();
        t.describeConfigs([new ConfigResource({ type: ConfigResource.Type.BROKER, name: '9' })]);
        const set = new AlterConfigOp({ configEntry: new ConfigEntry({ name: 'retention.ms', value: '1' }), operationType: AlterConfigOp.OpType.SET });
        await t.incrementalAlterConfigs(new ConfigResourceMap([[res, [set]]])).all().get();
        await t.listConfigResources().all().get();
        await t.describeLogDirs([0, 1]).allDescriptions().get();
        const replica = new TopicPartitionReplica({ topic: 't', partition: 0, brokerId: 0 });
        await t.alterReplicaLogDirs(new TopicPartitionReplicaMap([[replica, '/tmp/kafka-logs']])).all().get();
        await t.describeReplicaLogDirs([replica]).all().get();
        // The leader, reassignment and offset calls: a map of futures keyed by
        // TopicPartition, the partition_result futures, and one future each.
        const tp = new TopicPartition({ topic: 't', partition: 0 });
        t.updateEndOffsets(new TopicPartitionMap([[tp, 3n]]));
        await t.listOffsets(new TopicPartitionMap([[tp, OffsetSpec.latest()], [new TopicPartition({ topic: 't', partition: 9 }), OffsetSpec.forTimestamp(1)]])).partitionResult(tp).get();
        await t.alterPartitionReassignments(new TopicPartitionMap([[tp, new NewPartitionReassignment([0])]])).all().get();
        await t.listPartitionReassignments().reassignments().get();
        t.listPartitionReassignments([tp]);
        await t.electLeaders({ electionType: ElectionType.PREFERRED, partitions: null }).all().get().catch(() => {});
        // The group calls: the map futures keyed by group id, the futures read
        // by key (by group id, partition and member), and one future each.
        const g = new MockAdminClient();
        g.updateConsumerGroupOffsets(new TopicPartitionMap([[tp, 4n]]));
        await g.listConsumerGroupOffsets('grp').all().get();
        await g.listGroups().all().get();
        await g.describeConsumerGroups(['a', 'b']).all().get().catch(() => {});
        await g.describeClassicGroups(['a']).all().get().catch(() => {});
        await g.deleteConsumerGroups(['a']).all().get().catch(() => {});
        await g.deleteConsumerGroupOffsets({ groupId: 'grp', partitions: [tp] }).all().get().catch(() => {});
        await g.alterConsumerGroupOffsets({ groupId: 'grp', offsets: new TopicPartitionMap([[tp, new OffsetAndMetadata(1n)]]) }).partitionResult(tp).get().catch(() => {});
        const member = new MemberToRemove('m');
        const removed = g.removeMembersFromConsumerGroup({ groupId: 'grp', options: new RemoveMembersFromConsumerGroupOptions([member]) });
        await removed.memberResult(member).get().catch(() => {});
        removed.all();
        await g.close();
        // The security calls' futures the result lends (its box held by each):
        // the ACLs, the quota entities and the four token calls, read and
        // unread.
        const s = new MockAdminClient();
        await s.describeAcls(AclBindingFilter.ANY).values().get().catch(() => {});
        s.describeAcls(AclBindingFilter.ANY);
        await s.describeClientQuotas(ClientQuotaFilter.all()).entities().get().catch(() => {});
        const renewer = new KafkaPrincipal({ principalType: KafkaPrincipal.USER_TYPE, name: 'r' });
        const token = await s.createDelegationToken(new CreateDelegationTokenOptions({ renewers: [renewer] })).delegationToken().get();
        await s.renewDelegationToken(token.hmac()).expiryTimestamp().get();
        await s.describeDelegationToken().delegationTokens().get();
        s.describeDelegationToken();
        await s.expireDelegationToken(token.hmac()).expiryTimestamp().get();
        s.renewDelegationToken(token.hmac());
        await s.close();
        await t.deleteTopics(['t']).all().get();
        await t.close();
      }
      for (let i = 0; i < 3; i++) await round();
      await new Promise((r) => setTimeout(r, 50));
      let after = native.adminStats();
      for (let i = 0; i < 100 && after.adminsFreed - before.adminsFreed < 15; i++) {
        global.gc();
        await new Promise((r) => setTimeout(r, 20));
        after = native.adminStats();
      }
      const d = (k) => after[k] - before[k];
      console.log(JSON.stringify([d('adminsMade'), d('adminsFreed'), d('resultsMade') === d('resultsFreed'), d('futuresMade') === d('futuresFreed'), d('waitsStarted') === d('waitsCompleted')]));
    })();
  `,
    60000,
    ['--expose-gc'],
  );
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, '[15,15,true,true,true]\n');
});

test('the admin client emits the preview notice once; the mock emits none', () => {
  const r = spawnSync(
    process.execPath,
    [
      '-e',
      `
    const { Admin, MockAdminClient } = require('@confluentinc/kafka-javascript/admin');
    (async () => { await new MockAdminClient().close(); await Admin.create({ 'bootstrap.servers': '${UNREACHABLE}' }).close(); await Admin.create({ 'bootstrap.servers': '${UNREACHABLE}' }).close(); })();
  `,
    ],
    { cwd: path.join(__dirname, '..', '..', '..'), encoding: 'utf8', timeout: 30000 },
  );
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stderr.split('\n').filter((l) => l.includes('ExperimentalWarning')).length, 1, r.stderr);
});

// ---- the security calls ------------------------------------------------------------------
//
// The admin client's security calls over an unreachable bootstrap address:
// what Python's test_admin_client_security.py checks of them. Each call
// returns its Result at once, as Java's does, and every future of it fails
// with Java's message for a call that found no node; the forms and the null
// arguments are refused as Java refuses them. Java's KafkaAdminClientConfigTest
// cases of these RPCs (testDescribeAcls, testCreateAcls, testDeleteAcls,
// testCreateAclsToController and testDeleteAclsToController) script broker
// responses through Java's MockClient, which the binding cannot inject
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Tests); the Rust core translates
// them.

const { CreateAclsOptions, DeleteAclsOptions, DescribeAclsOptions } = require('@confluentinc/kafka-javascript/admin');
const {
  AccessControlEntry,
  AccessControlEntryFilter,
  AclBinding,
  AclBindingFilter,
  AclBindingFilterMap,
  AclBindingMap,
  AclOperation,
  AclPermissionType,
} = require('@confluentinc/kafka-javascript/common/acl');
const { InvalidRequestError } = require('@confluentinc/kafka-javascript/common/errors');
const { KafkaFuture: KafkaFutureClass } = require('@confluentinc/kafka-javascript/common');
const { PatternType, ResourcePattern, ResourcePatternFilter, ResourceType } = require('@confluentinc/kafka-javascript/common/resource');

/** @param {string} name */
const binding = (name = 't') =>
  new AclBinding({
    pattern: new ResourcePattern({ resourceType: ResourceType.TOPIC, name, patternType: PatternType.LITERAL }),
    entry: new AccessControlEntry({
      principal: 'User:a',
      host: '*',
      operation: AclOperation.READ,
      permissionType: AclPermissionType.ALLOW,
    }),
  });

test("the ACL calls return at once and time out with the core's message naming Java's call", async () => {
  const client = admin();
  const timedOut = (/** @type {string} */ call) => `Timed out waiting for a node assignment. Call: ${call}`;
  try {
    let started = Date.now();
    const described = client.describeAcls({ filter: AclBindingFilter.ANY, options: new DescribeAclsOptions({ timeoutMs: 300 }) });
    assert.ok(Date.now() - started < 1000);
    let cause = await failure(described.values());
    assert.ok(cause instanceof TimeoutError, String(cause));
    assert.equal(cause.message, timedOut('describeAcls'));

    started = Date.now();
    const created = client.createAcls({
      acls: [binding('a'), binding('b'), binding('a')],
      options: new CreateAclsOptions({ timeoutMs: 300 }),
    });
    assert.ok(Date.now() - started < 1000);
    const values = created.values();
    assert.ok(values instanceof AclBindingMap);
    // Java's futures map holds one future per distinct binding.
    assert.deepEqual([...values.keys()].map((b) => b.pattern().name()).sort(), ['a', 'b']);
    const a = values.get(binding('a'));
    assert.ok(a instanceof KafkaFutureClass);
    for (const future of [...values.values(), created.all()]) {
      cause = await failure(future);
      assert.ok(cause instanceof TimeoutError, String(cause));
      assert.equal(cause.message, timedOut('createAcls'));
    }

    const deleted = client.deleteAcls({
      filters: [AclBindingFilter.ANY, binding('c').toFilter()],
      options: new DeleteAclsOptions({ timeoutMs: 300 }),
    });
    const byFilter = deleted.values();
    assert.ok(byFilter instanceof AclBindingFilterMap);
    assert.equal(byFilter.size(), 2);
    assert.ok(byFilter.has(binding('c').toFilter()));
    for (const future of [...byFilter.values(), deleted.all()]) {
      cause = await failure(future);
      assert.ok(cause instanceof TimeoutError, String(cause));
      assert.equal(cause.message, timedOut('deleteAcls'));
    }
    // values() hands out a new map each call; the futures are the same.
    assert.notEqual(deleted.values(), byFilter);
    assert.equal(deleted.values().get(AclBindingFilter.ANY), byFilter.get(AclBindingFilter.ANY));
  } finally {
    await client.close({ timeout: 0 });
  }
});

test("testDescribeAcls' unknown filter: the core fails the future at once with Java's InvalidRequestException message", async () => {
  const client = admin();
  try {
    const unknown = new AclBindingFilter({
      patternFilter: new ResourcePatternFilter({ resourceType: ResourceType.UNKNOWN, name: null, patternType: PatternType.ANY }),
      entryFilter: AccessControlEntryFilter.ANY,
    });
    const cause = await failure(client.describeAcls(unknown).values());
    assert.ok(cause instanceof InvalidRequestError, String(cause));
    assert.equal(cause.message, 'The AclBindingFilter must not contain UNKNOWN elements.');
  } finally {
    await client.close({ timeout: 0 });
  }
});

test("createAcls' binding with an UNKNOWN element: its own future fails with Java's message, the others go on", async () => {
  const client = admin();
  try {
    const unknown = new AclBinding({
      pattern: new ResourcePattern({ resourceType: ResourceType.UNKNOWN, name: 't', patternType: PatternType.LITERAL }),
      entry: binding().entry(),
    });
    const result = client.createAcls({ acls: [unknown, binding()], options: new CreateAclsOptions({ timeoutMs: 300 }) });
    const future = result.values().get(unknown);
    assert.ok(future);
    const cause = await failure(future);
    assert.ok(cause instanceof InvalidRequestError, String(cause));
    assert.equal(cause.message, 'Invalid ACL creation: Resource type is UNKNOWN.');
    const other = result.values().get(binding());
    assert.ok(other);
    assert.ok((await failure(other)) instanceof TimeoutError);
  } finally {
    await client.close({ timeout: 0 });
  }
});

test("the ACL calls' forms and null arguments: TypeError naming the RPC's own parameter", async () => {
  const client = admin();
  try {
    // A string is an Iterable<string> to tsc only as a wrong element type: the run time refuses it.
    // @ts-expect-error a string where Java takes a Collection<AclBinding>
    assert.throws(() => client.createAcls('acl'), { name: 'TypeError', message: 'acls must be an iterable of AclBinding, not a string' });
    // @ts-expect-error a null collection
    assert.throws(() => client.createAcls(null), { name: 'TypeError', message: 'acls must not be null' });
    // @ts-expect-error a null element, which Java's loop dereferences
    assert.throws(() => client.createAcls([binding(), null]), { name: 'TypeError', message: 'acls must not contain null' });
    // @ts-expect-error a filter where Java takes an AclBinding
    assert.throws(() => client.createAcls([AclBindingFilter.ANY]), {
      name: 'TypeError',
      message: 'acls must contain only AclBinding, not an AclBindingFilter',
    });
    // Java dereferences the bindings before the options.
    // @ts-expect-error a null element and null options
    assert.throws(() => client.createAcls({ acls: [null], options: null }), { name: 'TypeError', message: 'acls must not contain null' });
    // @ts-expect-error null options
    assert.throws(() => client.createAcls({ acls: [], options: null }), { name: 'TypeError', message: 'options must not be null' });
    // @ts-expect-error a null filter, which Java dereferences first
    assert.throws(() => client.describeAcls({ filter: null, options: null }), { name: 'TypeError', message: 'filter must not be null' });
    // @ts-expect-error a binding where Java takes a filter
    assert.throws(() => client.describeAcls(binding()), {
      name: 'TypeError',
      message: 'filter must be an AclBindingFilter, not an AclBinding',
    });
    // @ts-expect-error null options
    assert.throws(() => client.describeAcls({ filter: AclBindingFilter.ANY, options: null }), {
      name: 'TypeError',
      message: 'options must not be null',
    });
    // @ts-expect-error a null element, which Java's loop dereferences
    assert.throws(() => client.deleteAcls([null]), { name: 'TypeError', message: 'filters must not contain null' });
    // @ts-expect-error a string where Java takes a Collection<AclBindingFilter>
    assert.throws(() => client.deleteAcls('f'), {
      name: 'TypeError',
      message: 'filters must be an iterable of AclBindingFilter, not a string',
    });
    assert.throws(
      // @ts-expect-error a key Java's forms have not
      () => client.deleteAcls({ filters: [], timeoutMs: 1 }),
      { name: 'IllegalArgumentError', message: 'deleteAcls() takes one of (filters), (filters, options); got (filters, timeoutMs)' },
    );
    // Options of another RPC: the same shape to tsc, which only the run time refuses.
    assert.throws(() => client.deleteAcls({ filters: [], options: new CreateAclsOptions() }), {
      name: 'TypeError',
      message: 'options must be a DeleteAclsOptions, not a CreateAclsOptions: build one with new DeleteAclsOptions()',
    });
    // An empty collection: Java's call still runs, its futures none.
    const none = client.deleteAcls([]);
    assert.equal(none.values().size(), 0);
    assert.deepEqual(await none.all().get(), []);
    assert.equal(client.createAcls([]).values().size(), 0);
  } finally {
    await client.close({ timeout: 0 });
  }
});

// The quota, SCRAM and delegation-token calls, as Python's
// test_admin_client_security.py checks them over an unreachable bootstrap
// address. Java's KafkaAdminClientConfigTest cases of these RPCs
// (testDescribeClientQuotas, testAlterClientQuotas,
// testDescribeUserScramCredentials and testAlterUserScramCredentials*) script
// broker responses through Java's MockClient, which the binding cannot inject
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Tests); the Rust core
// translates them. Java's client tests have none of the delegation-token
// calls.

const {
  AlterClientQuotasOptions,
  AlterUserScramCredentialsOptions,
  AlterUserScramCredentialsResult,
  CreateDelegationTokenOptions,
  DescribeClientQuotasOptions,
  DescribeDelegationTokenOptions,
  DescribeUserScramCredentialsOptions,
  ExpireDelegationTokenOptions,
  RenewDelegationTokenOptions,
  ScramCredentialInfo,
  ScramMechanism,
  UserScramCredentialAlteration,
  UserScramCredentialDeletion,
  UserScramCredentialUpsertion,
} = require('@confluentinc/kafka-javascript/admin');
const {
  ClientQuotaAlteration,
  ClientQuotaEntity,
  ClientQuotaEntityMap,
  ClientQuotaFilter,
  ClientQuotaFilterComponent,
} = require('@confluentinc/kafka-javascript/common/quota');
const { KafkaPrincipal } = require('@confluentinc/kafka-javascript/common/security/auth');

const QUOTA_ENTITY = new ClientQuotaEntity(new Map([[ClientQuotaEntity.USER, 'alice']]));
/** @param {string} key @param {number | null} value */
const quotaOp = (key, value) => new ClientQuotaAlteration.Op({ key, value });

test("the quota, SCRAM and delegation-token calls return at once and time out with the core's message naming Java's call", async () => {
  const client = admin();
  const timedOut = (/** @type {string} */ call) => `Timed out waiting for a node assignment. Call: ${call}`;
  const user = new KafkaPrincipal({ principalType: KafkaPrincipal.USER_TYPE, name: 'alice' });
  /** @type {Array<[string, () => import('@confluentinc/kafka-javascript/common').KafkaFuture<unknown>[]]>} */
  const calls = [
    [
      'createDelegationToken',
      () => [client.createDelegationToken(new CreateDelegationTokenOptions({ timeoutMs: 300, renewers: [user] })).delegationToken()],
    ],
    [
      'renewDelegationToken',
      () => [
        client
          .renewDelegationToken({ hmac: Buffer.from('mac'), options: new RenewDelegationTokenOptions({ timeoutMs: 300 }) })
          .expiryTimestamp(),
      ],
    ],
    [
      'expireDelegationToken',
      () => [
        client
          .expireDelegationToken({ hmac: Buffer.from('mac'), options: new ExpireDelegationTokenOptions({ timeoutMs: 300 }) })
          .expiryTimestamp(),
      ],
    ],
    [
      'describeDelegationToken',
      () => [client.describeDelegationToken(new DescribeDelegationTokenOptions({ timeoutMs: 300 })).delegationTokens()],
    ],
    [
      'describeClientQuotas',
      () => {
        const entities = client
          .describeClientQuotas({ filter: ClientQuotaFilter.all(), options: new DescribeClientQuotasOptions({ timeoutMs: 300 }) })
          .entities();
        assert.ok(entities);
        return [entities];
      },
    ],
    [
      'alterClientQuotas',
      () => {
        const r = client.alterClientQuotas({
          entries: [new ClientQuotaAlteration({ entity: QUOTA_ENTITY, ops: [quotaOp('producer_byte_rate', 1024)] })],
          options: new AlterClientQuotasOptions({ timeoutMs: 300 }),
        });
        const values = r.values();
        assert.ok(values instanceof ClientQuotaEntityMap);
        assert.ok(values.has(new ClientQuotaEntity(new Map([['user', 'alice']]))));
        return [...values.values(), r.all()];
      },
    ],
    [
      'describeUserScramCredentials',
      () => {
        const r = client.describeUserScramCredentials({
          users: ['u'],
          options: new DescribeUserScramCredentialsOptions({ timeoutMs: 300 }),
        });
        return [r.all(), r.users(), r.description('u')];
      },
    ],
    [
      'alterUserScramCredentials',
      () => {
        const r = client.alterUserScramCredentials({
          alterations: [new UserScramCredentialDeletion({ user: 'u', mechanism: ScramMechanism.SCRAM_SHA_256 })],
          options: new AlterUserScramCredentialsOptions({ timeoutMs: 300 }),
        });
        return [...r.values().values(), r.all()];
      },
    ],
  ];
  try {
    for (const [name, call] of calls) {
      const started = Date.now();
      const futures = call();
      assert.ok(Date.now() - started < 1000, name);
      for (const future of futures) {
        const cause = await failure(future);
        assert.ok(cause instanceof TimeoutError, `${name}: ${String(cause)}`);
        assert.equal(cause.message, timedOut(name));
      }
    }
  } finally {
    await client.close({ timeout: 0 });
  }
});

test("describeUserScramCredentials' forms: (), (users), (users, options); a null user is skipped, as Java's builder skips it", async () => {
  const client = admin({ 'default.api.timeout.ms': '300', 'request.timeout.ms': '100' });
  try {
    const options = new DescribeUserScramCredentialsOptions({ timeoutMs: 300 });
    // @ts-expect-error Java has no (options) form
    assert.throws(() => client.describeUserScramCredentials({ options }), {
      name: 'IllegalArgumentError',
      message: 'describeUserScramCredentials() takes one of (), (users), (users, options); got (options)',
    });
    // A string is an Iterable<string> to tsc: the run time refuses it.
    assert.throws(() => client.describeUserScramCredentials('u'), {
      name: 'TypeError',
      message: 'users must be an iterable of strings, not string',
    });
    // @ts-expect-error null options
    assert.throws(() => client.describeUserScramCredentials({ users: null, options: null }), {
      name: 'TypeError',
      message: 'options must not be null',
    });
    for (const result of [
      client.describeUserScramCredentials(),
      client.describeUserScramCredentials(null),
      client.describeUserScramCredentials(['u', /** @type {string} */ (/** @type {unknown} */ (null))]),
      client.describeUserScramCredentials({ users: null, options }),
      client.describeUserScramCredentials({ users: [], options }),
    ]) {
      const cause = await failure(result.users());
      assert.ok(cause instanceof TimeoutError, String(cause));
      assert.equal(cause.message, 'Timed out waiting for a node assignment. Call: describeUserScramCredentials');
    }
    // Each reader hands out a new future of the core's; description(null)
    // names no user, as Java's does.
    const result = client.describeUserScramCredentials();
    assert.notEqual(result.users(), result.users());
    assert.ok((await failure(result.description(null))) instanceof TimeoutError);
    // @ts-expect-error a number where Java takes a String
    assert.throws(() => result.description(1), {
      name: 'IllegalArgumentError',
      message: 'description() takes one of (userName); got a number',
    });
    // @ts-expect-error a reader takes no argument
    assert.throws(() => result.all(1), { name: 'TypeError', message: 'all() only reads: it takes no argument' });
  } finally {
    await client.close({ timeout: 0 });
  }
});

test('testAlterUserScramCredentialsUnknownMechanism without its scripted response: a failed call fails every user', async () => {
  // Java fails the users of an illegal alteration only in handleResponse, so
  // a call that fails (handleFailure: completeAllExceptionally) fails every
  // user, the UNKNOWN-mechanism ones included, with the call's error.
  const client = admin();
  try {
    const result = client.alterUserScramCredentials({
      alterations: [
        new UserScramCredentialDeletion({ user: 'user0', mechanism: ScramMechanism.UNKNOWN }),
        new UserScramCredentialUpsertion({
          user: 'user1',
          credentialInfo: new ScramCredentialInfo({ mechanism: ScramMechanism.UNKNOWN, iterations: 8192 }),
          password: 'password',
        }),
        new UserScramCredentialUpsertion({
          user: 'user2',
          credentialInfo: new ScramCredentialInfo({ mechanism: ScramMechanism.SCRAM_SHA_256, iterations: 4096 }),
          password: 'password',
        }),
      ],
      options: new AlterUserScramCredentialsOptions({ timeoutMs: 300 }),
    });
    const values = result.values();
    assert.deepEqual([...values.keys()].sort(), ['user0', 'user1', 'user2']);
    assert.ok(result instanceof AlterUserScramCredentialsResult);
    for (const future of [...values.values(), result.all()]) {
      const cause = await failure(future);
      assert.ok(cause instanceof TimeoutError, String(cause));
      assert.equal(cause.message, 'Timed out waiting for a node assignment. Call: alterUserScramCredentials');
    }
  } finally {
    await client.close({ timeout: 0 });
  }
});

test("the quota, SCRAM and token calls' forms and null arguments: TypeError naming the RPC's own parameter, in Java's order", async () => {
  const client = admin();
  try {
    // describeClientQuotas: Java reads the options first, the filter later
    // (the deviation the admin package lists).
    // @ts-expect-error null options
    assert.throws(() => client.describeClientQuotas({ filter: null, options: null }), {
      name: 'TypeError',
      message: 'options must not be null',
    });
    // @ts-expect-error a null filter
    assert.throws(() => client.describeClientQuotas(null), { name: 'TypeError', message: 'filter must not be null' });
    // @ts-expect-error a null the types forbid: Java stores it
    assert.throws(() => client.describeClientQuotas(ClientQuotaFilter.contains(null)), {
      name: 'TypeError',
      message: 'filter must not be a ClientQuotaFilter with null components',
    });
    assert.throws(
      // @ts-expect-error a null component
      () => client.describeClientQuotas(ClientQuotaFilter.containsOnly([ClientQuotaFilterComponent.ofEntityType('user'), null])),
      { name: 'TypeError', message: 'filter must not contain a null ClientQuotaFilterComponent' },
    );
    // alterClientQuotas: Java dereferences the entries and each entry before
    // the options, the rest when it builds the request.
    // @ts-expect-error null entries
    assert.throws(() => client.alterClientQuotas({ entries: null, options: null }), {
      name: 'TypeError',
      message: 'entries must not be null',
    });
    // @ts-expect-error a null entry
    assert.throws(() => client.alterClientQuotas({ entries: [null], options: null }), {
      name: 'TypeError',
      message: 'entries must not contain null',
    });
    // @ts-expect-error null options
    assert.throws(() => client.alterClientQuotas({ entries: [], options: null }), {
      name: 'TypeError',
      message: 'options must not be null',
    });
    // @ts-expect-error a null the types forbid: Java stores it
    assert.throws(() => client.alterClientQuotas([new ClientQuotaAlteration({ entity: null, ops: [] })]), {
      name: 'TypeError',
      message: 'entries must not contain a ClientQuotaAlteration with a null entity',
    });
    // @ts-expect-error a null the types forbid: Java stores it
    assert.throws(() => client.alterClientQuotas([new ClientQuotaAlteration({ entity: QUOTA_ENTITY, ops: null })]), {
      name: 'TypeError',
      message: 'entries must not contain a ClientQuotaAlteration with null ops',
    });
    // @ts-expect-error a null the types forbid: Java stores it
    assert.throws(() => client.alterClientQuotas([new ClientQuotaAlteration({ entity: new ClientQuotaEntity(null), ops: [] })]), {
      name: 'TypeError',
      message: 'entries must not be a ClientQuotaEntity with null entries',
    });
    assert.throws(
      () =>
        client.alterClientQuotas([
          new ClientQuotaAlteration({ entity: QUOTA_ENTITY, ops: [quotaOp(/** @type {string} */ (/** @type {unknown} */ (null)), 1)] }),
        ]),
      {
        name: 'TypeError',
        message: 'entries must not contain a ClientQuotaAlteration.Op with a null key',
      },
    );
    // @ts-expect-error a string where Java takes a Collection
    assert.throws(() => client.alterClientQuotas('q'), {
      name: 'TypeError',
      message: 'entries must be an iterable of ClientQuotaAlteration, not a string',
    });
    // A null op value is a removal: the call is sent.
    const removal = client
      .alterClientQuotas([new ClientQuotaAlteration({ entity: QUOTA_ENTITY, ops: [quotaOp('producer_byte_rate', null)] })])
      .values();
    assert.equal(removal?.size(), 1);

    // alterUserScramCredentials: Java dereferences each alteration first.
    // @ts-expect-error a null alteration
    assert.throws(() => client.alterUserScramCredentials({ alterations: [null], options: null }), {
      name: 'TypeError',
      message: 'alterations must not contain null',
    });
    // @ts-expect-error null options
    assert.throws(() => client.alterUserScramCredentials({ alterations: [], options: null }), {
      name: 'TypeError',
      message: 'options must not be null',
    });
    class Other extends UserScramCredentialAlteration {
      constructor() {
        super({ user: 'u' });
      }
    }
    assert.throws(() => client.alterUserScramCredentials([new Other()]), {
      name: 'TypeError',
      message: 'alterations must contain only UserScramCredentialUpsertion or UserScramCredentialDeletion, not an Other',
    });

    // createDelegationToken: Java's first read is the renewers, each
    // dereferenced, at the call.
    // @ts-expect-error a null the types forbid: Java stores it
    assert.throws(() => client.createDelegationToken(new CreateDelegationTokenOptions({ renewers: null })), {
      name: 'TypeError',
      message: 'CreateDelegationTokenOptions.renewers must not be null',
    });
    assert.throws(
      // @ts-expect-error a null renewer
      () => client.createDelegationToken(new CreateDelegationTokenOptions({ renewers: [null] })),
      { name: 'TypeError', message: 'CreateDelegationTokenOptions.renewers must not contain null' },
    );
    // @ts-expect-error null options
    assert.throws(() => client.createDelegationToken(null), { name: 'TypeError', message: 'options must not be null' });
    assert.throws(
      // @ts-expect-error an argument where Java takes none
      () => client.createDelegationToken({ timeoutMs: 1 }),
      { name: 'IllegalArgumentError', message: 'createDelegationToken() takes one of (), (options); got (timeoutMs)' },
    );
    // renew and expire: Java reads the options, then the HMAC when it builds
    // the request (the deviation the admin package lists).
    // @ts-expect-error null options and a null HMAC
    assert.throws(() => client.renewDelegationToken({ hmac: null, options: null }), {
      name: 'TypeError',
      message: 'options must not be null',
    });
    // @ts-expect-error a null HMAC
    assert.throws(() => client.renewDelegationToken(null), { name: 'TypeError', message: 'hmac must not be null' });
    // @ts-expect-error a string where Java takes byte[]
    assert.throws(() => client.expireDelegationToken('mac'), { name: 'TypeError', message: 'hmac must be a Uint8Array, not a string' });
    assert.throws(
      // @ts-expect-error a null owner
      () => client.describeDelegationToken(new DescribeDelegationTokenOptions({ owners: [null] })),
      { name: 'TypeError', message: 'DescribeDelegationTokenOptions.owners must not contain null' },
    );
  } finally {
    await client.close({ timeout: 0 });
  }
});

// The transaction and producer calls, as Python's test_admin_client_security.py
// checks them over an unreachable bootstrap address. Java's KafkaAdminClientTransactionTest (testDescribeProducers*,
// testDescribeTransactions*, testAbortTransaction*,
// testForceTerminateTransaction*, testListTransactions*, testFenceProducers*)
// scripts broker responses through Java's MockClient, which the binding
// cannot inject (javascript/CLAUDE.md, JavaScript Binding Conventions, Tests); the
// Rust core translates them (kafka_admin_client.rs, test_describe_producers,
// test_force_terminate_transaction and the rest).

const {
  AbortTransactionOptions,
  AbortTransactionSpec,
  DescribeProducersOptions,
  DescribeTransactionsOptions,
  FenceProducersOptions,
  ListTransactionsOptions,
  TerminateTransactionOptions,
  TransactionState,
} = require('@confluentinc/kafka-javascript/admin');

test("the transaction and producer calls return at once and time out with the core's message naming Java's call", async () => {
  const client = admin({ 'default.api.timeout.ms': 300, 'request.timeout.ms': 100 });
  const tp = new TopicPartition({ topic: 't', partition: 0 });
  const timedOut = (/** @type {string} */ call, /** @type {string} */ api) =>
    `Timed out waiting for a node assignment. Call: ${call}(api=${api})`;
  /** @param {KafkaFuture<unknown> | undefined} future */
  const message = async (future) => {
    assert.ok(future);
    const cause = await failure(future);
    assert.ok(cause instanceof TimeoutError, String(cause));
    return cause.message;
  };
  try {
    const started = Date.now();
    const producers = client.describeProducers({ partitions: [tp, tp], options: new DescribeProducersOptions({ timeoutMs: 300 }) });
    const toBroker = client.describeProducers({ partitions: [tp], options: new DescribeProducersOptions({ timeoutMs: 300, brokerId: 1 }) });
    const described = client.describeTransactions({
      transactionalIds: ['a', 'a'],
      options: new DescribeTransactionsOptions({ timeoutMs: 300 }),
    });
    const spec = new AbortTransactionSpec({ topicPartition: tp, producerId: 1n, producerEpoch: 0, coordinatorEpoch: 0 });
    const aborted = client.abortTransaction({ spec, options: new AbortTransactionOptions({ timeoutMs: 300 }) });
    const terminated = client.forceTerminateTransaction({
      transactionalId: 'a',
      options: new TerminateTransactionOptions({ timeoutMs: 300 }),
    });
    const listed = client.listTransactions(
      new ListTransactionsOptions({
        timeoutMs: 300,
        filteredStates: [TransactionState.ONGOING],
        filteredProducerIds: [1n],
        filteredDuration: 5n,
        filteredTransactionalIdPattern: 'a.*',
      }),
    );
    const fenced = client.fenceProducers({ transactionalIds: ['a'], options: new FenceProducersOptions({ timeoutMs: 300 }) });
    assert.ok(Date.now() - started < 1000);
    assert.equal(await message(producers.partitionResult(tp)), timedOut('describeProducers', 'METADATA'));
    assert.equal(await message(producers.all()), timedOut('describeProducers', 'METADATA'));
    // With a broker id, Java's StaticBrokerStrategy skips the leader lookup.
    assert.equal(await message(toBroker.all()), timedOut('describeProducers', 'DESCRIBE_PRODUCERS'));
    assert.equal(await message(described.description('a')), timedOut('describeTransactions', 'FIND_COORDINATOR'));
    assert.equal(await message(described.all()), timedOut('describeTransactions', 'FIND_COORDINATOR'));
    assert.equal(await message(aborted.all()), timedOut('abortTransaction', 'METADATA'));
    // Java's forceTerminateTransaction runs fenceProducers, whose handler names the call.
    assert.equal(await message(terminated.result()), timedOut('fenceProducer', 'FIND_COORDINATOR'));
    for (const future of [listed.all(), listed.byBrokerId(), listed.allByBrokerId()])
      assert.equal(await message(future), timedOut('listTransactions', 'METADATA'));
    for (const future of [fenced.producerId('a'), fenced.epochId('a'), fenced.all(), fenced.fencedProducers().get('a')])
      assert.equal(await message(future), timedOut('fenceProducer', 'FIND_COORDINATOR'));
    assert.throws(() => producers.partitionResult(new TopicPartition({ topic: 't', partition: 1 })), {
      name: 'IllegalArgumentError',
      message: 'Topic partition t-1 was not included in the request',
    });
    assert.throws(() => fenced.producerId('b'), {
      name: 'IllegalArgumentError',
      message: 'TransactionalId `b` was not included in the request',
    });
    assert.equal((await client.describeTransactions([]).all().get()).size, 0);
  } finally {
    await client.close({ timeout: 0 });
  }
});

test("the transaction and producer calls' forms and null arguments: TypeError naming the RPC's own parameter, in Java's order", async () => {
  const client = admin();
  const tp = new TopicPartition({ topic: 't', partition: 0 });
  const spec = new AbortTransactionSpec({ topicPartition: tp, producerId: 1n, producerEpoch: 0, coordinatorEpoch: 0 });
  try {
    // A string is an Iterable<string> to tsc: only the run time refuses it.
    assert.throws(() => client.describeTransactions('a'), {
      name: 'TypeError',
      message: 'transactionalIds must be an iterable of strings, not string',
    });
    assert.throws(() => client.fenceProducers('a'), {
      name: 'TypeError',
      message: 'transactionalIds must be an iterable of strings, not string',
    });
    // @ts-expect-error Java's null collection: its first dereference
    assert.throws(() => client.describeProducers({ partitions: null, options: null }), {
      name: 'TypeError',
      message: 'partitions must not be null',
    });
    // @ts-expect-error a null partition: Java's unmodifiable map refuses it at the call
    assert.throws(() => client.describeProducers([null]), { name: 'TypeError', message: 'partitions must not contain null' });
    // @ts-expect-error the options next, before a null topic Java reads only when it sends
    assert.throws(() => client.describeProducers({ partitions: [new TopicPartition({ topic: null, partition: 0 })], options: null }), {
      name: 'TypeError',
      message: 'options must not be null',
    });
    assert.throws(() => client.describeProducers([new TopicPartition({ topic: null, partition: 0 })]), {
      name: 'TypeError',
      message: 'partitions must not contain a TopicPartition with a null topic',
    });
    // @ts-expect-error the options before a null id, which Java stores
    assert.throws(() => client.describeTransactions({ transactionalIds: [null], options: null }), {
      name: 'TypeError',
      message: 'options must not be null',
    });
    // @ts-expect-error a null id, which the C API cannot carry
    assert.throws(() => client.describeTransactions([null]), { name: 'TypeError', message: 'transactionalIds must not contain null' });
    // @ts-expect-error a null id, which the C API cannot carry
    assert.throws(() => client.fenceProducers([null]), { name: 'TypeError', message: 'transactionalIds must not contain null' });
    // @ts-expect-error a null spec
    assert.throws(() => client.abortTransaction(null), { name: 'TypeError', message: 'spec must not be null' });
    const nullPartition = new AbortTransactionSpec({ topicPartition: null, producerId: 1n, producerEpoch: 0, coordinatorEpoch: 0 });
    // @ts-expect-error a spec with a null partition, before the options
    assert.throws(() => client.abortTransaction({ spec: nullPartition, options: null }), {
      name: 'TypeError',
      message: 'spec must not be an AbortTransactionSpec with a null topicPartition',
    });
    // @ts-expect-error the options before the id, which Java stores
    assert.throws(() => client.forceTerminateTransaction({ transactionalId: null, options: null }), {
      name: 'TypeError',
      message: 'options must not be null',
    });
    // @ts-expect-error a null id, which the C API cannot carry
    assert.throws(() => client.forceTerminateTransaction(null), { name: 'TypeError', message: 'transactionalId must not be null' });
    // @ts-expect-error a state Java's request builder dereferences
    const nullState = new ListTransactionsOptions({ filteredStates: [null] });
    assert.throws(() => client.listTransactions(nullState), {
      name: 'TypeError',
      message: 'ListTransactionsOptions.filteredStates must not contain null',
    });
    // @ts-expect-error a producer id Java's request builder dereferences
    const nullId = new ListTransactionsOptions({ filteredProducerIds: [null] });
    assert.throws(() => client.listTransactions(nullId), {
      name: 'TypeError',
      message: 'ListTransactionsOptions.filteredProducerIds must not contain null',
    });
    // @ts-expect-error Java's null options
    assert.throws(() => client.listTransactions({ options: null }), { name: 'TypeError', message: 'options must not be null' });
    assert.throws(
      // @ts-expect-error an argument Java's listTransactions() has not
      () => client.listTransactions({ options: new ListTransactionsOptions(), spec }),
      { name: 'IllegalArgumentError', message: 'listTransactions() takes one of (), (options); got (options, spec)' },
    );
  } finally {
    await client.close({ timeout: 0 });
  }
});

// The feature and controller calls, as Python's test_admin_client_security.py
// and test_admin_java_ports.py check them over an unreachable bootstrap
// address. Java's KafkaAdminClientTest
// testUpdateFeaturesShouldFailRequestForEmptyUpdates,
// testUpdateFeaturesShouldFailRequestForInvalidFeatureName and
// testUpdateFeaturesShouldFailRequestInClientWhenDowngradeFlagIsNotSetDuringDeletion
// need no scripted broker and are translated here, and so is
// testUnregisterControllerTimeoutMaxWait, the unreachable bootstrap address
// standing for the MockClient that never answers, as in Python; its other
// testUpdateFeatures*, testDescribeFeatures* and testUnregisterController*
// script broker responses through Java's MockClient, which the binding cannot
// inject (javascript/CLAUDE.md, JavaScript Binding Conventions, Tests); the Rust
// core translates them (kafka_admin_client.rs).

const {
  DescribeFeaturesOptions,
  FeatureUpdate,
  UnregisterControllerOptions,
  UpdateFeaturesOptions,
} = require('@confluentinc/kafka-javascript/admin');

const upgradeTo = (/** @type {number} */ level) =>
  new FeatureUpdate({ maxVersionLevel: level, upgradeType: FeatureUpdate.UpgradeType.UPGRADE });

test('testUnregisterControllerTimeoutMaxWait', async () => {
  const client = admin({});
  try {
    const result = client.unregisterController({ controllerId: 1, options: new UnregisterControllerOptions({ timeoutMs: 10 }) });
    const cause = await failure(result.all());
    assert.ok(cause instanceof TimeoutError, String(cause));
    assert.equal(cause.message, 'Timed out waiting for a node assignment. Call: unregisterController');
  } finally {
    await client.close({ timeout: 0 });
  }
});

test("the feature and controller calls return at once and time out with the core's message naming Java's call", async () => {
  const client = admin({ 'default.api.timeout.ms': 300, 'request.timeout.ms': 100 });
  // Each is a plain Java Call, whose name the message carries without an api.
  const timedOut = (/** @type {string} */ call) => `Timed out waiting for a node assignment. Call: ${call}`;
  /** @param {KafkaFuture<unknown> | undefined} future */
  const message = async (future) => {
    assert.ok(future);
    const cause = await failure(future);
    assert.ok(cause instanceof TimeoutError, String(cause));
    return cause.message;
  };
  try {
    const started = Date.now();
    const described = client.describeFeatures(new DescribeFeaturesOptions({ timeoutMs: 300 }));
    const toNode = client.describeFeatures({ options: new DescribeFeaturesOptions({ timeoutMs: 300, nodeId: 1 }) });
    const updated = client.updateFeatures({
      featureUpdates: { 'metadata.version': upgradeTo(2), other: upgradeTo(1) },
      options: new UpdateFeaturesOptions({ timeoutMs: 300, validateOnly: true }),
    });
    const unregistered = client.unregisterController({ controllerId: 1, options: new UnregisterControllerOptions({ timeoutMs: 300 }) });
    assert.ok(Date.now() - started < 1000);
    assert.equal(await message(described.featureMetadata()), timedOut('describeFeatures'));
    assert.equal(await message(toNode.featureMetadata()), timedOut('describeFeatures'));
    assert.deepEqual([...updated.values().keys()], ['metadata.version', 'other']);
    for (const future of [...updated.values().values(), updated.all()]) assert.equal(await message(future), timedOut('updateFeatures'));
    assert.equal(await message(unregistered.all()), timedOut('unregisterController'));
  } finally {
    await client.close({ timeout: 0 });
  }
});

test('testUpdateFeaturesShouldFailRequestForEmptyUpdates and ForInvalidFeatureName: the core throws at the call', async () => {
  const client = admin();
  try {
    assert.throws(() => client.updateFeatures(new Map()), {
      name: 'IllegalArgumentError',
      message: 'Feature updates can not be null or empty.',
    });
    assert.throws(() => client.updateFeatures({ featureUpdates: {} }), {
      name: 'IllegalArgumentError',
      message: 'Feature updates can not be null or empty.',
    });
    // Neither a Map nor a plain object: Java's Map<String, FeatureUpdate>.
    // @ts-expect-error a FeatureUpdate where the map goes
    assert.throws(() => client.updateFeatures({ featureUpdates: upgradeTo(2) }), {
      name: 'TypeError',
      message: 'featureUpdates must be a Map or a plain object, not a FeatureUpdate',
    });
    // @ts-expect-error a Set where the map goes
    assert.throws(() => client.updateFeatures({ featureUpdates: new Set() }), {
      name: 'TypeError',
      message: 'featureUpdates must be a Map or a plain object, not a Set',
    });
    assert.throws(
      () =>
        client.updateFeatures(
          new Map([
            ['feature', upgradeTo(2)],
            ['', upgradeTo(2)],
          ]),
        ),
      {
        name: 'IllegalArgumentError',
        message: 'Provided feature can not be empty.',
      },
    );
    // Java's Utils.isBlank(null) is true: a null name is a blank one.
    // @ts-expect-error a null feature name, which a Map can hold
    assert.throws(() => client.updateFeatures(new Map([[null, upgradeTo(2)]])), {
      name: 'IllegalArgumentError',
      message: 'Provided feature can not be empty.',
    });
  } finally {
    await client.close({ timeout: 0 });
  }
});

test('testUpdateFeaturesShouldFailRequestInClientWhenDowngradeFlagIsNotSetDuringDeletion', () => {
  assert.throws(() => upgradeTo(0), {
    name: 'IllegalArgumentError',
    message: 'The upgradeType flag should be set to SAFE_DOWNGRADE or UNSAFE_DOWNGRADE when the provided maxVersionLevel:0 is < 1.',
  });
});

test("the feature and controller calls' forms and null arguments: TypeError naming the RPC's own parameter, in Java's order", async () => {
  const client = admin();
  try {
    // @ts-expect-error Java's null options, its first dereference
    assert.throws(() => client.describeFeatures({ options: null }), { name: 'TypeError', message: 'options must not be null' });
    assert.throws(
      // @ts-expect-error an argument Java's describeFeatures() has not
      () => client.describeFeatures({ options: new DescribeFeaturesOptions(), nodeId: 1 }),
      { name: 'IllegalArgumentError', message: 'describeFeatures() takes one of (), (options); got (options, nodeId)' },
    );
    // @ts-expect-error Java's null map, its first dereference
    assert.throws(() => client.updateFeatures({ featureUpdates: null }), { name: 'TypeError', message: 'featureUpdates must not be null' });
    // A plain object given alone is the options object (javascript/CLAUDE.md,
    // JavaScript Binding Conventions, Signatures, Q15): the map goes by name.
    // @ts-expect-error a plain object of updates given alone
    assert.throws(() => client.updateFeatures({ 'metadata.version': upgradeTo(2) }), {
      name: 'IllegalArgumentError',
      message: 'updateFeatures() takes one of (featureUpdates), (featureUpdates, options); got (metadata.version)',
    });
    // @ts-expect-error a null update, which Java's request builder reads and the C API cannot carry
    assert.throws(() => client.updateFeatures(new Map([['f', null]])), {
      name: 'TypeError',
      message: 'featureUpdates must not contain null',
    });
    // @ts-expect-error a value of another class
    assert.throws(() => client.updateFeatures(new Map([['f', 2]])), {
      name: 'TypeError',
      message: 'featureUpdates must hold only FeatureUpdates, not a number',
    });
    // Plain JavaScript can give the constructor a null type, which Java stores.
    // @ts-expect-error a null upgrade type
    const nullType = new FeatureUpdate({ maxVersionLevel: 2, upgradeType: null });
    assert.throws(() => client.updateFeatures(new Map([['f', nullType]])), {
      name: 'TypeError',
      message: 'featureUpdates must not contain a FeatureUpdate with a null upgradeType',
    });
    // @ts-expect-error Java's null options: the FFI's error
    assert.throws(() => client.updateFeatures({ featureUpdates: new Map([['f', upgradeTo(2)]]), options: null }), {
      name: 'TypeError',
      message: 'options must not be null',
    });
    // @ts-expect-error Java's int cannot be null
    assert.throws(() => client.unregisterController(null), { name: 'TypeError', message: 'controllerId must be an integer' });
    // @ts-expect-error Java's int cannot be null
    assert.throws(() => client.unregisterController({ controllerId: null }), {
      name: 'TypeError',
      message: 'controllerId must be an integer',
    });
    assert.throws(() => client.unregisterController(2 ** 31), {
      name: 'RangeError',
      message: 'controllerId must be from -2147483648 to 2147483647',
    });
    // @ts-expect-error Java's null options, its first dereference
    assert.throws(() => client.unregisterController({ controllerId: 1, options: null }), {
      name: 'TypeError',
      message: 'options must not be null',
    });
  } finally {
    await client.close({ timeout: 0 });
  }
});
