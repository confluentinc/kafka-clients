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
// Java tests need MockClient.
// Then the binding's own: the forms, liveness and the native objects'
// lifetimes, each in a child process.

const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

const { ExecutionError, IllegalArgumentError, IllegalStateError } = require('@confluentinc/kafka-javascript');
const {
  Admin,
  CreateTopicsOptions,
  DescribeClusterOptions,
  DescribeTopicsOptions,
  KafkaAdminClient,
  ListTopicsOptions,
  NewTopic,
} = require('@confluentinc/kafka-javascript/admin');
const { KafkaError } = require('@confluentinc/kafka-javascript/common');
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
    const { Admin, AlterConfigOp, ConfigEntry, MockAdminClient, NewTopic } = require('@confluentinc/kafka-javascript/admin');
    const { TopicCollection, TopicPartitionReplica, TopicPartitionReplicaMap } = require('@confluentinc/kafka-javascript/common');
    const { ConfigResource, ConfigResourceMap } = require('@confluentinc/kafka-javascript/common/config');
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
        await t.deleteTopics(['t']).all().get();
        await t.close();
      }
      for (let i = 0; i < 3; i++) await round();
      await new Promise((r) => setTimeout(r, 50));
      let after = native.adminStats();
      for (let i = 0; i < 100 && after.adminsFreed - before.adminsFreed < 9; i++) {
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
  assert.equal(r.stdout, '[9,9,true,true,true]\n');
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
