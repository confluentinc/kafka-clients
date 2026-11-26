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

// The admin client against one PLAINTEXT broker, ported from Java's
// integration tests (PlaintextAdminIntegrationTest, BaseAdminIntegrationTest,
// AddPartitionsTest, ListOffsetsIntegrationTest, DynamicGroupConfigIntegrationTest)
// where one broker with no authorizer is enough. Runs only when
// KAFKA_BOOTSTRAP_SERVERS is set (`make -C javascript test-integration`
// starts a scratch broker, Kafka 4.3.1, node id 1, one log directory). The
// broker's texts are asserted as that image gives them; where Java 4.4's test
// expects another text, the comment says so.

const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

const { ExecutionError, IllegalArgumentError, IllegalStateError } = require('@confluentinc/kafka-javascript');
const {
  Admin,
  AlterConfigOp,
  AlterConfigsOptions,
  AlterConsumerGroupOffsetsOptions,
  ConfigEntry,
  CreatePartitionsOptions,
  CreateTopicsOptions,
  DescribeConfigsOptions,
  NewPartitions,
  NewTopic,
  OffsetSpec,
  RecordsToDelete,
  ScramCredentialInfo,
  ScramMechanism,
  TransactionState,
  UserScramCredentialDeletion,
  UserScramCredentialUpsertion,
} = require('@confluentinc/kafka-javascript/admin');
const {
  ClassicGroupState,
  ElectionType,
  GroupState,
  GroupType,
  KafkaFuture,
  TopicPartition,
  TopicPartitionMap,
  TopicPartitionReplica,
} = require('@confluentinc/kafka-javascript/common');
const { ConfigResource, ConfigResourceMap } = require('@confluentinc/kafka-javascript/common/config');
const {
  DuplicateResourceError,
  ElectionNotNeededError,
  GroupIdNotFoundError,
  InvalidConfigurationError,
  InvalidPartitionsError,
  InvalidReplicaAssignmentError,
  InvalidReplicationFactorError,
  InvalidRequestError,
  InvalidTopicError,
  TimeoutError,
  TopicExistsError,
  UnacceptableCredentialError,
  UnknownTopicOrPartitionError,
  UnsupportedSaslMechanismError,
} = require('@confluentinc/kafka-javascript/common/errors');
const quota = require('@confluentinc/kafka-javascript/common/quota');
const { OffsetAndMetadata } = require('@confluentinc/kafka-javascript/consumer');
const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
const { docker, waitForBroker } = require('./_fetch_records');

/** @typedef {import('@confluentinc/kafka-javascript/admin').Admin} AdminClient */

// A string in every test that runs: the tests are skipped when it is unset.
const BOOTSTRAP = /** @type {string} */ (process.env.KAFKA_BOOTSTRAP_SERVERS);
const SKIP = BOOTSTRAP ? false : 'KAFKA_BOOTSTRAP_SERVERS is not set: no broker to administer';
if (SKIP) console.log(`skipping the integration tests: ${SKIP}`);
const CONTAINER = process.env.KAFKA_CONTAINER;
const SKIP_CONTAINER = SKIP || (CONTAINER ? false : 'KAFKA_CONTAINER is not set: no broker container to run a console consumer in');
if (SKIP_CONTAINER && !SKIP) console.log(`skipping the classic-group test: ${SKIP_CONTAINER}`);

beforeAll(async () => {
  if (!SKIP) await waitForBroker(BOOTSTRAP);
}, 90_000);

let count = 0;
/**
 * A name no other run uses.
 *
 * @param {string} what
 */
const fresh = (what) => `js-ports-${what}-${process.pid}-${Date.now()}-${count++}`;

/** An admin client of the broker. */
const newAdmin = () => Admin.create({ 'bootstrap.servers': BOOTSTRAP });

/**
 * `value`, asserted present.
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
 * The error a future fails with: the cause of its ExecutionError.
 *
 * @param {KafkaFuture<unknown> | null | undefined} future
 * @returns {Promise<Error>}
 */
async function failureOf(future) {
  assert.ok(future, 'no future for the key');
  try {
    await future.get();
  } catch (error) {
    assert.ok(error instanceof ExecutionError, String(error));
    assert.ok(error.cause instanceof Error, String(error.cause));
    return error.cause;
  }
  assert.fail('the future succeeded');
}

/**
 * Asserts the future fails with exactly `cls` and `message`.
 *
 * @param {KafkaFuture<unknown> | null | undefined} future
 * @param {Function} cls
 * @param {string} message
 */
async function assertFails(future, cls, message) {
  const cause = await failureOf(future);
  assert.equal(cause.constructor, cls, String(cause));
  assert.equal(cause.message, message);
}

/** @param {number} ms */
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * A future's value, retried while `accept` refuses it or the broker does not
 * know the topic yet: what one call changed reaches the next a little later.
 *
 * @template T
 * @param {() => KafkaFuture<T>} call
 * @param {(value: T) => boolean} [accept]
 * @returns {Promise<T>}
 */
async function retryUntil(call, accept = () => true) {
  for (let attempt = 0; ; attempt++) {
    try {
      const value = await call().get();
      if (accept(value) || attempt >= 40) return value;
    } catch (error) {
      const cause = /** @type {{ cause?: unknown }} */ (error).cause;
      if (attempt >= 40 || !(cause instanceof UnknownTopicOrPartitionError)) throw error;
    }
    await sleep(250);
  }
}

/**
 * Creates the topics, one partition each unless given, and waits until the
 * broker describes them.
 *
 * @param {AdminClient} admin
 * @param {string[]} names
 * @param {number} [partitions]
 */
async function createTopics(admin, names, partitions = 1) {
  await admin
    .createTopics(names.map((name) => new NewTopic({ name, numPartitions: partitions, replicationFactor: 1 })))
    .all()
    .get();
  await retryUntil(() => present(admin.describeTopics(names).allTopicNames()));
}

/**
 * Deletes the topics, ignoring what fails: a test's clean-up.
 *
 * @param {AdminClient} admin
 * @param {string[]} names
 */
async function deleteTopics(admin, names) {
  await admin
    .deleteTopics(names)
    .all()
    .get()
    .catch(() => {});
}

/**
 * One alteration op.
 *
 * @param {string} name
 * @param {string | null} value
 * @param {'SET' | 'DELETE' | 'APPEND' | 'SUBTRACT'} type
 */
const op = (name, value, type) =>
  new AlterConfigOp({ configEntry: new ConfigEntry({ name, value }), operationType: AlterConfigOp.OpType[type] });

/**
 * incrementalAlterConfigs of one resource, its future.
 *
 * @param {AdminClient} admin
 * @param {ConfigResource} resource
 * @param {AlterConfigOp[]} ops
 * @param {boolean} [validateOnly]
 */
const alter = (admin, resource, ops, validateOnly = false) =>
  present(
    admin
      .incrementalAlterConfigs({ configs: new ConfigResourceMap([[resource, ops]]), options: new AlterConfigsOptions({ validateOnly }) })
      .values()
      .get(resource),
  );

/**
 * The config of one resource, read until `accept` takes it.
 *
 * @param {AdminClient} admin
 * @param {ConfigResource} resource
 * @param {(config: import('@confluentinc/kafka-javascript/admin').Config) => boolean} [accept]
 * @param {DescribeConfigsOptions} [options]
 */
const configOf = (admin, resource, accept = () => true, options = new DescribeConfigsOptions()) =>
  retryUntil(
    () =>
      present(
        admin
          .describeConfigs({ resources: [resource], options })
          .values()
          .get(resource),
      ),
    accept,
  );

// ---- close -------------------------------------------------------------------------

// PlaintextAdminIntegrationTest#testDelayedClose: close with a long timeout
// lets a call in flight finish, refuses new calls, and a second close
// returns; with a zero timeout, as the control, the call is aborted.
(SKIP ? test.skip : test)(
  'testDelayedClose: close({ timeout: 2h }) lets the call in flight finish and refuses new calls; close({ timeout: 0 }) aborts it',
  async () => {
    const topic = fresh('delayed-close');
    const validate = (/** @type {AdminClient} */ admin) =>
      admin
        .createTopics({
          newTopics: [new NewTopic({ name: topic, numPartitions: 1, replicationFactor: 1 })],
          options: new CreateTopicsOptions({ validateOnly: true }),
        })
        .all();

    const admin = newAdmin();
    const future = validate(admin);
    assert.equal(future.isDone(), false);
    await admin.close({ timeout: 2 * 60 * 60 * 1000 });
    assert.equal(await future.get(), null);
    const late = admin.createTopics([new NewTopic({ name: topic, numPartitions: 1, replicationFactor: 1 })]);
    await assertFails(late.all(), IllegalStateError, 'Cannot accept new calls when AdminClient is closing.');
    await assertFails(late.values().get(topic), IllegalStateError, 'Cannot accept new calls when AdminClient is closing.');
    await admin.close({ timeout: 2 * 60 * 60 * 1000 });

    const control = newAdmin();
    const aborted = validate(control);
    assert.equal(aborted.isDone(), false);
    await control.close({ timeout: 0 });
    await assertFails(aborted, TimeoutError, 'The AdminClient thread has exited. Call: createTopics');
  },
  60_000,
);

// ---- createTopics ------------------------------------------------------------------

// PlaintextAdminIntegrationTest#testCreateExistingTopicsThrowTopicExistsException.
(SKIP ? test.skip : test)(
  'testCreateExistingTopicsThrowTopicExistsException: the key and its refinements fail, validateOnly and a large replication factor too',
  async () => {
    const admin = newAdmin();
    const topic = fresh('exists');
    try {
      await createTopics(admin, [topic]);
      const message = `Topic '${topic}' already exists.`;
      const again = admin.createTopics([new NewTopic({ name: topic, numPartitions: 2, replicationFactor: 1 })]);
      await assertFails(again.values().get(topic), TopicExistsError, message);
      await assertFails(again.all(), TopicExistsError, message);
      await assertFails(again.numPartitions(topic), TopicExistsError, message);
      await assertFails(again.replicationFactor(topic), TopicExistsError, message);
      await assertFails(again.config(topic), TopicExistsError, message);
      await assertFails(again.topicId(topic), TopicExistsError, message);
      const validated = admin.createTopics({
        newTopics: [new NewTopic({ name: topic, numPartitions: 2, replicationFactor: 5 })],
        options: new CreateTopicsOptions({ validateOnly: true }),
      });
      await assertFails(validated.all(), TopicExistsError, message);
    } finally {
      await deleteTopics(admin, [topic]);
      await admin.close();
    }
  },
  60_000,
);

// PlaintextAdminIntegrationTest#testLongTopicNames and
// BaseAdminIntegrationTest#testCreateDeleteTopics: each key of one call gets its
// own outcome. A name given twice is one key, as Java's map keeps the first
// NewTopic (KafkaAdminClient#createTopics), so the topic has the first count.
(SKIP ? test.skip : test)(
  'testLongTopicNames and testCreateDeleteTopics: one createTopics, each key its own outcome; the valid topics exist with the first count',
  async () => {
    const admin = newAdmin();
    const existing = fresh('mixed-existing');
    const created = fresh('mixed-new');
    const badRf = fresh('mixed-rf');
    const badConfig = fresh('mixed-config');
    const longest = `${fresh('long')}-`.padEnd(249, 'x');
    const tooLong = `${fresh('long')}-`.padEnd(250, 'y');
    try {
      await createTopics(admin, [existing]);
      const result = admin.createTopics([
        new NewTopic({ name: created, numPartitions: 1, replicationFactor: 1 }),
        new NewTopic({ name: created, numPartitions: 3, replicationFactor: 1 }),
        new NewTopic({ name: existing, numPartitions: 1, replicationFactor: 1 }),
        new NewTopic({ name: badRf, numPartitions: 1, replicationFactor: 3 }),
        new NewTopic({ name: badConfig, numPartitions: 1, replicationFactor: 1 }).configs({ 'retention.ms': 'abc' }),
        new NewTopic({ name: longest, numPartitions: 1, replicationFactor: 1 }),
        new NewTopic({ name: tooLong, numPartitions: 1, replicationFactor: 1 }),
      ]);
      const values = result.values();
      assert.equal(values.size, 6);
      assert.equal(await present(values.get(created)).get(), null);
      assert.equal(await present(values.get(longest)).get(), null);
      await assertFails(values.get(existing), TopicExistsError, `Topic '${existing}' already exists.`);
      await assertFails(
        values.get(badRf),
        InvalidReplicationFactorError,
        'Unable to replicate the partition 3 time(s): The target replication factor of 3 cannot be reached because only 1 broker(s) ' +
          'are registered or some brokers have all their log directories cordoned.',
      );
      await assertFails(
        values.get(badConfig),
        InvalidConfigurationError,
        'Invalid value abc for configuration retention.ms: Not a number of type LONG',
      );
      await assertFails(
        values.get(tooLong),
        InvalidTopicError,
        `Topic name is invalid: the length of '${tooLong}' is longer than the max allowed length 249`,
      );
      const all = await failureOf(result.all());
      assert.ok(
        all instanceof TopicExistsError ||
          all instanceof InvalidReplicationFactorError ||
          all instanceof InvalidConfigurationError ||
          all instanceof InvalidTopicError,
        String(all),
      );
      const described = await retryUntil(() => present(admin.describeTopics([created, longest]).allTopicNames()));
      assert.equal(present(described.get(created)).partitions.length, 1);
      assert.equal(present(described.get(longest)).partitions.length, 1);
    } finally {
      await deleteTopics(admin, [existing, created, longest]);
      await admin.close();
    }
  },
  60_000,
);

// PlaintextAdminIntegrationTest#testCreateTopicsReturnsConfigs and Python's
// test_admin_broker: a NewTopic with no count and no replication factor gets
// the broker's defaults, and the config the call returns has each entry's
// source and flags, with no synonyms and no documentation.
(SKIP ? test.skip : test)(
  "testCreateTopicsReturnsConfigs: the broker's default count and replication factor, and the returned config entries",
  async () => {
    const admin = newAdmin();
    const topic = fresh('returns-configs');
    try {
      const result = admin.createTopics([new NewTopic({ name: topic, numPartitions: null, replicationFactor: null })]);
      assert.equal(await result.numPartitions(topic).get(), 1);
      assert.equal(await result.replicationFactor(topic).get(), 1);
      const config = present(await result.config(topic).get());
      const policy = present(config.get('cleanup.policy'));
      assert.equal(policy.value(), 'delete');
      assert.equal(policy.source(), ConfigEntry.ConfigSource.DEFAULT_CONFIG);
      assert.equal(policy.isSensitive(), false);
      assert.equal(policy.isReadOnly(), false);
      const sources = new Set(config.entries().map((e) => e.source()));
      assert.ok(sources.has(ConfigEntry.ConfigSource.DEFAULT_CONFIG));
      for (const entry of config.entries()) {
        assert.deepEqual(entry.synonyms(), [], entry.name());
        assert.equal(entry.documentation(), null, entry.name());
        assert.equal(entry.isSensitive(), false, entry.name());
        assert.equal(entry.isReadOnly(), false, entry.name());
      }
    } finally {
      await deleteTopics(admin, [topic]);
      await admin.close();
    }
  },
  60_000,
);

// ---- createPartitions --------------------------------------------------------------

// PlaintextAdminIntegrationTest#testCreatePartitions and AddPartitionsTest:
// several topics in one call, each refused for its own reason; validateOnly
// changes nothing.
(SKIP ? test.skip : test)(
  'testCreatePartitions: the same count, fewer, an unknown topic, an assignment count mismatch and an unknown broker, in one call',
  async () => {
    const admin = newAdmin();
    const [same, fewer, mismatch, unknownBroker] = ['same', 'fewer', 'mismatch', 'broker'].map(fresh);
    const unknown = fresh('unknown');
    const all = [same, fewer, mismatch, unknownBroker];
    try {
      await createTopics(admin, all, 3);
      const result = admin.createPartitions({
        newPartitions: {
          [same]: NewPartitions.increaseTo(3),
          [fewer]: NewPartitions.increaseTo(2),
          [unknown]: NewPartitions.increaseTo(4),
          [mismatch]: NewPartitions.increaseTo({ totalCount: 6, newAssignments: [[1]] }),
          [unknownBroker]: NewPartitions.increaseTo({ totalCount: 4, newAssignments: [[7]] }),
        },
      });
      const values = result.values();
      await assertFails(values.get(same), InvalidPartitionsError, 'Topic already has 3 partition(s).');
      await assertFails(
        values.get(fewer),
        InvalidPartitionsError,
        `The topic ${fewer} currently has 3 partition(s); 2 would not be an increase.`,
      );
      await assertFails(values.get(unknown), UnknownTopicOrPartitionError, 'This server does not host this topic-partition.');
      await assertFails(
        values.get(mismatch),
        InvalidReplicaAssignmentError,
        'Attempted to add 3 additional partition(s), but only 1 assignment(s) were specified.',
      );
      await assertFails(
        values.get(unknownBroker),
        InvalidReplicaAssignmentError,
        'The manual partition assignment includes broker 7, but no such broker is registered.',
      );

      const validated = admin.createPartitions({
        newPartitions: { [same]: NewPartitions.increaseTo(5) },
        options: new CreatePartitionsOptions({ validateOnly: true }),
      });
      assert.equal(await validated.all().get(), null);
      const described = await present(admin.describeTopics(all).allTopicNames()).get();
      for (const name of all) assert.equal(present(described.get(name)).partitions.length, 3, name);
    } finally {
      await deleteTopics(admin, all);
      await admin.close();
    }
  },
  60_000,
);

// ---- configs -----------------------------------------------------------------------

// PlaintextAdminIntegrationTest#testIncrementalAlterAndDescribeGroupConfigs,
// #testDescribeConfigsNonexistentForKraft and DynamicGroupConfigIntegrationTest.
(SKIP ? test.skip : test)(
  'the GROUP config resource: the defaults of an unknown group, SET and DELETE, validateOnly, an out-of-range value',
  async () => {
    const admin = newAdmin();
    const group = new ConfigResource({ type: ConfigResource.Type.GROUP, name: fresh('group-config') });
    const KEY = 'consumer.session.timeout.ms';
    try {
      // A group that does not exist: its defaults.
      const defaults = await configOf(admin, group);
      const timeout = present(defaults.get(KEY));
      assert.equal(timeout.value(), '45000');
      assert.equal(timeout.source(), ConfigEntry.ConfigSource.DEFAULT_CONFIG);
      assert.ok(defaults.get('consumer.heartbeat.interval.ms'));

      assert.equal(await alter(admin, group, [op(KEY, '50000', 'SET')]).get(), null);
      const set = await configOf(admin, group, (c) => c.get(KEY)?.value() === '50000');
      assert.equal(present(set.get(KEY)).value(), '50000');
      assert.equal(present(set.get(KEY)).source(), ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG);

      assert.equal(await alter(admin, group, [op(KEY, '55000', 'SET')], true).get(), null);
      await sleep(500);
      assert.equal(present((await configOf(admin, group)).get(KEY)).value(), '50000');

      // Java 4.4's DynamicGroupConfigIntegrationTest expects "consumer.session.timeout.ms
      // must be in the range 45000 to 60000 inclusive."; the 4.3.1 broker names the bound it broke.
      await assertFails(
        alter(admin, group, [op(KEY, '1000', 'SET')]),
        InvalidConfigurationError,
        'consumer.session.timeout.ms must be greater than or equal to group.consumer.min.session.timeout.ms',
      );
      await assertFails(
        alter(admin, group, [op(KEY, '70000', 'SET')]),
        InvalidConfigurationError,
        'consumer.session.timeout.ms must be less than or equal to group.consumer.max.session.timeout.ms',
      );

      assert.equal(await alter(admin, group, [op(KEY, null, 'DELETE')]).get(), null);
      const reset = await configOf(admin, group, (c) => c.get(KEY)?.value() === '45000');
      assert.equal(present(reset.get(KEY)).source(), ConfigEntry.ConfigSource.DEFAULT_CONFIG);
    } finally {
      await admin.close();
    }
  },
  60_000,
);

// PlaintextAdminIntegrationTest#testValidIncrementalAlterConfigs,
// #testAppendAlreadyExistsConfigsAndSubtractNotExistsConfigs,
// #testAppendConfigToEmptyDefaultValue and #testInvalidIncrementalAlterConfigs.
(SKIP ? test.skip : test)(
  'APPEND and SUBTRACT on cleanup.policy, and the misuses the broker refuses',
  async () => {
    const admin = newAdmin();
    const topic = fresh('append');
    const resource = new ConfigResource({ type: ConfigResource.Type.TOPIC, name: topic });
    const policy = (/** @type {string} */ value) => configOf(admin, resource, (c) => c.get('cleanup.policy')?.value() === value);
    try {
      await createTopics(admin, [topic]);
      assert.equal(await alter(admin, resource, [op('cleanup.policy', 'compact', 'APPEND')]).get(), null);
      assert.equal(present((await policy('delete,compact')).get('cleanup.policy')).value(), 'delete,compact');
      // A value already in the list is not added twice.
      assert.equal(await alter(admin, resource, [op('cleanup.policy', 'compact', 'APPEND')]).get(), null);
      await sleep(500);
      assert.equal(present((await policy('delete,compact')).get('cleanup.policy')).value(), 'delete,compact');
      assert.equal(await alter(admin, resource, [op('cleanup.policy', 'delete', 'SUBTRACT')]).get(), null);
      assert.equal(present((await policy('compact')).get('cleanup.policy')).value(), 'compact');
      // A value not in the list: nothing to remove.
      assert.equal(await alter(admin, resource, [op('cleanup.policy', 'delete', 'SUBTRACT')]).get(), null);
      await sleep(500);
      assert.equal(present((await policy('compact')).get('cleanup.policy')).value(), 'compact');

      await assertFails(
        alter(admin, resource, [op('compression.type', 'gzip', 'APPEND')]),
        InvalidConfigurationError,
        "Can't APPEND to key compression.type because its type is not LIST.",
      );
      await assertFails(
        alter(admin, resource, [op('compression.type', 'zip', 'SET')]),
        InvalidConfigurationError,
        'Invalid value zip for configuration compression.type: String must be one of: uncompressed, zstd, lz4, snappy, gzip, producer',
      );
      await assertFails(
        alter(admin, resource, [op('min.cleanable.dirty.ratio', '1.1', 'SET')]),
        InvalidConfigurationError,
        'Invalid value 1.1 for configuration min.cleanable.dirty.ratio: Value must be no more than 1',
      );
      await assertFails(
        alter(admin, resource, [op('retention.ms', '1000', 'SET'), op('retention.ms', '2000', 'SET')]),
        InvalidRequestError,
        'Error due to duplicate config keys',
      );
      // PlaintextAdminIntegrationTest#testNullConfigs, its SET op: the broker
      // refuses a null value. (Its NewTopic.configs part is left out: the C API
      // cannot carry a null config value.)
      await assertFails(
        alter(admin, resource, [op('retention.bytes', null, 'SET')]),
        InvalidRequestError,
        'Null value not supported for : retention.bytes',
      );
    } finally {
      await deleteTopics(admin, [topic]);
      await admin.close();
    }
  },
  60_000,
);

// PlaintextAdminIntegrationTest's Log4j tests
// (#testDescribeLogLevels, #testIncrementalAlterConfigsForLog4jLogLevels,
// #testIncrementalAlterConfigsForLog4jLogLevelsCanResetLoggerToCurrentRoot,
// #testIncrementalAlterConfigsForLog4jLogLevelsCannotResetRootLogger,
// #testIncrementalAlterConfigsForLog4jLogLevelsDoesNotWorkWithInvalidConfigs,
// #testAlterConfigsForLog4jLogLevelsDoesNotWork).
(SKIP ? test.skip : test)(
  "BROKER_LOGGER: describe broker 1's loggers, SET a level and read it back, reset it to the root's; the misuses",
  async () => {
    const admin = newAdmin();
    const loggers = new ConfigResource({ type: ConfigResource.Type.BROKER_LOGGER, name: '1' });
    const LOGGER = 'kafka.server.ReplicaManager';
    try {
      const described = await configOf(admin, loggers);
      const root = present(described.get('root'));
      const rootLevel = present(root.value());
      assert.ok(['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL'].includes(rootLevel), rootLevel);
      assert.ok(described.entries().length > 1);
      for (const entry of described.entries())
        assert.equal(entry.source(), ConfigEntry.ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG, entry.name());

      const level = rootLevel === 'DEBUG' ? 'TRACE' : 'DEBUG';
      assert.equal(await alter(admin, loggers, [op(LOGGER, level, 'SET')]).get(), null);
      assert.equal(present((await configOf(admin, loggers, (c) => c.get(LOGGER)?.value() === level)).get(LOGGER)).value(), level);
      assert.equal(await alter(admin, loggers, [op(LOGGER, null, 'DELETE')]).get(), null);
      assert.equal(present((await configOf(admin, loggers, (c) => c.get(LOGGER)?.value() === rootLevel)).get(LOGGER)).value(), rootLevel);

      await assertFails(
        alter(admin, loggers, [op('root', null, 'DELETE')]),
        InvalidRequestError,
        'Removing the log level of the root logger is not allowed',
      );
      await assertFails(
        alter(admin, loggers, [op(LOGGER, 'DEBUG', 'APPEND')]),
        InvalidRequestError,
        'APPEND operation is not allowed for the BROKER_LOGGER resource',
      );
      await assertFails(
        alter(admin, loggers, [op(LOGGER, 'DEBUG', 'SUBTRACT')]),
        InvalidRequestError,
        'SUBTRACT operation is not allowed for the BROKER_LOGGER resource',
      );
      await assertFails(
        alter(admin, loggers, [op(LOGGER, 'NOTALEVEL', 'SET')]),
        InvalidConfigurationError,
        `Cannot set the log level of ${LOGGER} to NOTALEVEL as it is not a supported log level. Valid log levels are DEBUG, ERROR, FATAL, INFO, TRACE, WARN`,
      );
    } finally {
      await admin.close();
    }
  },
  60_000,
);

// PlaintextAdminIntegrationTest#testIncludeDocumentation and
// #testDescribeConfigsNonexistent, and the sources Java's describeConfigs
// tests read: DEFAULT, STATIC_BROKER, DYNAMIC_DEFAULT_BROKER and DYNAMIC_BROKER,
// with the synonyms in the broker's precedence order.
(SKIP ? test.skip : test)(
  'describeConfigs: documentation only when asked, each source and the synonym order, two resources named 1, and the unknown resources',
  async () => {
    const admin = newAdmin();
    const broker = new ConfigResource({ type: ConfigResource.Type.BROKER, name: '1' });
    const brokerDefault = new ConfigResource({ type: ConfigResource.Type.BROKER, name: '' });
    const KEY = 'log.retention.bytes';
    try {
      const plain = await configOf(admin, broker);
      assert.equal(present(plain.get('log.retention.hours')).documentation(), null);
      const documented = await configOf(admin, broker, () => true, new DescribeConfigsOptions({ includeDocumentation: true }));
      assert.match(present(present(documented.get('log.retention.hours')).documentation()), /^The number of hours to keep a log file/);

      assert.equal(await alter(admin, brokerDefault, [op(KEY, '1073741824000', 'SET')]).get(), null);
      assert.equal(await alter(admin, broker, [op(KEY, '1073741824001', 'SET')]).get(), null);
      const synonyms = await configOf(
        admin,
        broker,
        (c) => c.get(KEY)?.synonyms()?.length === 3,
        new DescribeConfigsOptions({ includeSynonyms: true }),
      );
      const entry = present(synonyms.get(KEY));
      assert.equal(entry.value(), '1073741824001');
      assert.equal(entry.source(), ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG);
      assert.deepEqual(
        present(entry.synonyms()).map((s) => [s.source(), s.value()]),
        [
          [ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG, '1073741824001'],
          [ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG, '1073741824000'],
          [ConfigEntry.ConfigSource.DEFAULT_CONFIG, '-1'],
        ],
      );
      assert.equal(present(synonyms.get('listeners')).source(), ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG);
      assert.equal(present(synonyms.get('log.retention.hours')).source(), ConfigEntry.ConfigSource.DEFAULT_CONFIG);
      const atDefault = present((await configOf(admin, brokerDefault)).get(KEY));
      assert.equal(atDefault.source(), ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG);

      // A TOPIC and a BROKER both named 1 are two keys.
      const topicOne = new ConfigResource({ type: ConfigResource.Type.TOPIC, name: '1' });
      const both = admin.describeConfigs([topicOne, broker]);
      assert.equal(both.values().size(), 2);
      assert.ok(present(await present(both.values().get(broker)).get()).get('log.dirs'));
      assert.equal((await failureOf(both.values().get(topicOne))).constructor, UnknownTopicOrPartitionError);

      // Java's testDescribeConfigsNonexistent asserts the classes only.
      const none = new ConfigResource({ type: ConfigResource.Type.TOPIC, name: 'none_topic' });
      assert.equal((await failureOf(admin.describeConfigs([none]).all())).constructor, UnknownTopicOrPartitionError);
      for (const type of [ConfigResource.Type.BROKER, ConfigResource.Type.BROKER_LOGGER]) {
        const missing = new ConfigResource({ type, name: '-1' });
        const cause = await failureOf(
          admin.describeConfigs({ resources: [missing], options: new DescribeConfigsOptions({ timeoutMs: 2000 }) }).all(),
        );
        assert.equal(cause.constructor, TimeoutError, String(cause));
        assert.equal(cause.message, 'Timed out waiting for a node assignment. Call: describeConfigs');
      }
    } finally {
      await alter(admin, broker, [op(KEY, null, 'DELETE')])
        .get()
        .catch(() => {});
      await alter(admin, brokerDefault, [op(KEY, null, 'DELETE')])
        .get()
        .catch(() => {});
      await admin.close();
    }
  },
  60_000,
);

// ---- offsets, records and elections ------------------------------------------------

/**
 * Produces `count` records to partition 0 and waits for each.
 *
 * @param {string} topic
 * @param {number} count
 */
async function produce(topic, count) {
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, acks: 'all' } });
  try {
    for (let i = 0; i < count; i++)
      await (await producer.send(new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from(`v${i}`) }))).get();
  } finally {
    await producer.close();
  }
}

// ListOffsetsIntegrationTest#testListMaxTimestampWithEmptyLog, and
// DeleteRecordsRequest.HIGH_WATERMARK (-1): deleteRecords up to the high
// watermark. DeleteRecordsResult.all() and DescribeReplicaLogDirsResult.all(),
// which no other broker test reads, resolve.
(SKIP ? test.skip : test)(
  'the sentinels: maxTimestamp() of an empty partition, beforeOffset(-1n); DeleteRecordsResult.all() and DescribeReplicaLogDirsResult.all()',
  async () => {
    const admin = newAdmin();
    const [empty, filled] = [fresh('empty'), fresh('filled')];
    try {
      await createTopics(admin, [empty, filled]);
      const emptyTp = new TopicPartition({ topic: empty, partition: 0 });
      const max = await retryUntil(() =>
        admin.listOffsets(new TopicPartitionMap([[emptyTp, OffsetSpec.maxTimestamp()]])).partitionResult(emptyTp),
      );
      assert.equal(max.offset(), -1n);
      assert.equal(max.timestamp(), -1n);

      await produce(filled, 3);
      const tp = new TopicPartition({ topic: filled, partition: 0 });
      const deleted = admin.deleteRecords(new TopicPartitionMap([[tp, RecordsToDelete.beforeOffset(-1n)]]));
      assert.equal(await deleted.all().get(), null);
      assert.equal((await present(present(deleted.lowWatermarks()).get(tp)).get()).lowWatermark(), 3n);
      const earliest = await admin
        .listOffsets(new TopicPartitionMap([[tp, OffsetSpec.earliest()]]))
        .partitionResult(tp)
        .get();
      assert.equal(earliest.offset(), 3n);

      const dirs = await present(admin.describeLogDirs([1]).descriptions().get(1)).get();
      const [dir] = [...dirs.keys()];
      const replicaDirs = await admin
        .describeReplicaLogDirs([new TopicPartitionReplica({ topic: filled, partition: 0, brokerId: 1 })])
        .all()
        .get();
      assert.equal(replicaDirs.size(), 1);
      const info = present(replicaDirs.get(new TopicPartitionReplica({ topic: filled, partition: 0, brokerId: 1 })));
      assert.equal(info.getCurrentReplicaLogDir(), dir);
      assert.equal(info.getFutureReplicaLogDir(), null);
    } finally {
      await deleteTopics(admin, [empty, filled]);
      await admin.close();
    }
  },
  60_000,
);

// PlaintextAdminIntegrationTest#testElectPreferredLeaders: an unknown topic's
// partition fails its own key; one broker, so the known one needs no election.
(SKIP ? test.skip : test)(
  "testElectPreferredLeaders: an unknown topic's partition beside a known one",
  async () => {
    const admin = newAdmin();
    const topic = fresh('elect');
    const missing = fresh('elect-missing');
    try {
      await createTopics(admin, [topic]);
      const known = new TopicPartition({ topic, partition: 0 });
      const unknown = new TopicPartition({ topic: missing, partition: 0 });
      const result = await admin
        .electLeaders({ electionType: ElectionType.PREFERRED, partitions: [unknown, known] })
        .partitions()
        .get();
      assert.equal(result.size(), 2);
      const unknownError = /** @type {Error} */ (present(result.get(unknown)));
      assert.equal(unknownError.constructor, UnknownTopicOrPartitionError);
      assert.equal(unknownError.message, `No such topic as ${missing}`);
      const knownError = /** @type {Error} */ (present(result.get(known)));
      assert.equal(knownError.constructor, ElectionNotNeededError);
      assert.equal(knownError.message, 'Leader election not needed for topic partition.');
    } finally {
      await deleteTopics(admin, [topic]);
      await admin.close();
    }
  },
  60_000,
);

// ---- groups ------------------------------------------------------------------------

// KafkaAdminClientConsumerGroupTest#testAlterConsumerGroupOffsets: a partition
// not in the request is Java's IllegalArgumentException once the call
// succeeded, and the call's own error once it failed.
(SKIP ? test.skip : test)(
  'AlterConsumerGroupOffsetsResult.partitionResult for a partition not in the request',
  async () => {
    const admin = newAdmin();
    const topic = fresh('partition-result');
    const group = fresh('partition-result-group');
    const other = new TopicPartition({ topic: 'other', partition: 9 });
    try {
      await createTopics(admin, [topic]);
      const tp = new TopicPartition({ topic, partition: 0 });
      const offsets = new TopicPartitionMap([[tp, new OffsetAndMetadata(1n)]]);
      const done = admin.alterConsumerGroupOffsets({ groupId: group, offsets });
      assert.equal(await done.all().get(), null);
      await assertFails(done.partitionResult(other), IllegalArgumentError, `Alter offset for partition "${other}" was not attempted`);

      // A call that fails as a whole (its deadline already passed).
      const failed = admin.alterConsumerGroupOffsets({
        groupId: group,
        offsets,
        options: new AlterConsumerGroupOffsetsOptions({ timeoutMs: 0 }),
      });
      const callError = await failureOf(failed.all());
      assert.ok(callError instanceof TimeoutError, String(callError));
      const partitionError = await failureOf(failed.partitionResult(other));
      assert.equal(partitionError.constructor, callError.constructor);
      assert.equal(partitionError.message, callError.message);
    } finally {
      await admin
        .deleteConsumerGroups([group])
        .all()
        .get()
        .catch(() => {});
      await deleteTopics(admin, [topic]);
      await admin.close();
    }
  },
  60_000,
);

// PlaintextAdminIntegrationTest#testDescribeClassicGroups and #testListGroups:
// a classic group with a member, run by the broker image's console consumer
// with group.protocol=classic in the broker's container. The broker
// advertises localhost:<KAFKA_PORT>, which inside the container nothing
// listens on, so a busybox nc forwards that port to the broker's own 9092.
(SKIP_CONTAINER ? test.skip : test)(
  'testDescribeClassicGroups: a classic group with a member, described and listed as a STABLE classic group',
  async () => {
    const container = /** @type {string} */ (CONTAINER);
    const port = BOOTSTRAP.split(':').pop();
    const admin = newAdmin();
    const topic = fresh('classic');
    const group = fresh('classic-group');
    const clientId = fresh('classic-client');
    const run = `${process.pid}-${count++}`;
    try {
      await createTopics(admin, [topic], 2);
      docker(['exec', '-d', container, 'sh', '-c', `echo $$ > /tmp/fwd-${run}.pid; exec nc -lk -p ${port} -e nc localhost 9092`]);
      docker([
        'exec',
        '-d',
        container,
        'sh',
        '-c',
        `echo $$ > /tmp/cc-${run}.pid; exec /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 ` +
          `--topic ${topic} --group ${group} --consumer-property group.protocol=classic --consumer-property client.id=${clientId} ` +
          `--timeout-ms 60000 > /tmp/cc-${run}.out 2>&1`,
      ]);
      // The group exists once the consumer has joined it.
      /** @type {Map<string, import('@confluentinc/kafka-javascript/admin').ClassicGroupDescription> | null} */
      let groups = null;
      for (const deadline = Date.now() + 60_000; groups === null;) {
        assert.ok(
          Date.now() < deadline,
          `the console consumer did not join ${group}: ${docker(['exec', container, 'cat', `/tmp/cc-${run}.out`])}`,
        );
        try {
          const described = await admin.describeClassicGroups([group]).all().get();
          const g = described.get(group);
          if (g?.state === ClassicGroupState.STABLE && g.members.length === 1) groups = described;
        } catch (error) {
          assert.ok(/** @type {{ cause?: unknown }} */ (error).cause instanceof GroupIdNotFoundError, String(error));
        }
        if (groups === null) await sleep(500);
      }
      const description = present(groups.get(group));
      assert.equal(description.groupId, group);
      assert.equal(description.state, ClassicGroupState.STABLE);
      assert.equal(description.protocol, 'consumer');
      assert.equal(description.protocolData, 'range');
      assert.equal(description.isSimpleConsumerGroup, false);
      assert.equal(description.members.length, 1);
      const [member] = description.members;
      assert.equal(member.clientId, clientId);
      assert.ok(member.consumerId.startsWith(`${clientId}-`), member.consumerId);
      assert.equal(member.groupInstanceId, null);
      assert.deepEqual([...member.assignment.topicPartitions()].map(String).sort(), [`${topic}-0`, `${topic}-1`]);

      const consumer = present((await admin.describeConsumerGroups([group]).all().get()).get(group));
      assert.equal(consumer.type, GroupType.CLASSIC);
      assert.equal(consumer.groupState, GroupState.STABLE);
      assert.equal(consumer.partitionAssignor, 'range');
      assert.equal(consumer.members.length, 1);

      const listing = present((await admin.listGroups().all().get()).find((l) => l.groupId() === group));
      assert.equal(listing.type(), GroupType.CLASSIC);
      assert.equal(listing.groupState(), GroupState.STABLE);
      assert.equal(listing.protocol(), 'consumer');
      assert.equal(listing.isSimpleConsumerGroup(), false);
    } finally {
      try {
        docker(['exec', container, 'sh', '-c', `kill $(cat /tmp/cc-${run}.pid) $(cat /tmp/fwd-${run}.pid) 2>/dev/null; true`]);
      } catch {
        // the container is gone
      }
      await deleteTopics(admin, [topic]);
      await admin.close();
    }
  },
  120_000,
);

// ---- quotas and SCRAM --------------------------------------------------------------

/**
 * The quotas of the entities `filter` matches, read until `accept` takes them.
 *
 * @param {AdminClient} admin
 * @param {import('@confluentinc/kafka-javascript/common/quota').ClientQuotaFilter} filter
 * @param {(entities: import('@confluentinc/kafka-javascript/common/quota').ClientQuotaEntityMap<Map<string, number>>) => boolean} accept
 */
const quotasOf = (admin, filter, accept) => retryUntil(() => present(admin.describeClientQuotas(filter).entities()), accept);

/**
 * An alteration op.
 *
 * @param {string} key
 * @param {number | null} value
 */
const quotaOp = (key, value) => new quota.ClientQuotaAlteration.Op({ key, value });

// PlaintextAdminIntegrationTest#testDefaultNameQuotaIsNotEqualToDefaultQuota,
// and the .NET quota tests.
(SKIP ? test.skip : test)(
  'quotas: the default user and a user named <default> are two entities; two ops; one entity twice; 12.5 and 1e15 read back',
  async () => {
    const admin = newAdmin();
    const defaultUser = new quota.ClientQuotaEntity(new Map([[quota.ClientQuotaEntity.USER, null]]));
    const namedDefault = new quota.ClientQuotaEntity(new Map([[quota.ClientQuotaEntity.USER, '<default>']]));
    const user = fresh('quota');
    try {
      assert.equal(defaultUser.equals(namedDefault), false);
      const altered = admin.alterClientQuotas([
        new quota.ClientQuotaAlteration({
          entity: defaultUser,
          ops: [quotaOp('producer_byte_rate', 100), quotaOp('request_percentage', 12.5)],
        }),
        new quota.ClientQuotaAlteration({ entity: namedDefault, ops: [quotaOp('producer_byte_rate', 1e15)] }),
      ]);
      assert.equal(altered.values().size(), 2);
      assert.equal(await altered.all().get(), null);

      const defaults = await quotasOf(
        admin,
        quota.ClientQuotaFilter.containsOnly([quota.ClientQuotaFilterComponent.ofDefaultEntity('user')]),
        (entities) => entities.size() === 1,
      );
      assert.equal(defaults.size(), 1);
      assert.deepEqual([...present(defaults.get(defaultUser))].sort(), [
        ['producer_byte_rate', 100],
        ['request_percentage', 12.5],
      ]);
      const everything = await quotasOf(
        admin,
        quota.ClientQuotaFilter.all(),
        (entities) => entities.has(namedDefault) && entities.has(defaultUser),
      );
      assert.deepEqual([...present(everything.get(namedDefault))], [['producer_byte_rate', 1e15]]);
      assert.ok(everything.has(defaultUser));

      // One entity twice, its entries in the other order: one key. Java's
      // KafkaAdminClient#alterClientQuotas sends both alterations, and the
      // broker applies the first and refuses the duplicate entity.
      /** @param {string[][]} entries */
      const entity = (entries) => new quota.ClientQuotaEntity(new Map(/** @type {Array<[string, string]>} */ (entries)));
      const first = entity([
        ['user', user],
        ['client-id', 'c'],
      ]);
      const second = entity([
        ['client-id', 'c'],
        ['user', user],
      ]);
      const twice = admin.alterClientQuotas([
        new quota.ClientQuotaAlteration({ entity: first, ops: [quotaOp('producer_byte_rate', 1)] }),
        new quota.ClientQuotaAlteration({ entity: second, ops: [quotaOp('consumer_byte_rate', 2)] }),
      ]);
      assert.equal(twice.values().size(), 1);
      const duplicate = await failureOf(twice.values().get(second));
      assert.equal(duplicate.constructor, InvalidRequestError);
      // The broker writes the entity's entries in its own map's order.
      assert.ok(
        [`Ignoring duplicate entity ${first}`, `Ignoring duplicate entity ${second}`].includes(duplicate.message),
        duplicate.message,
      );
      const applied = await quotasOf(
        admin,
        quota.ClientQuotaFilter.contains([quota.ClientQuotaFilterComponent.ofEntity({ entityType: 'user', entityName: user })]),
        (entities) => entities.size() === 1,
      );
      assert.deepEqual([...present(applied.get(second))], [['producer_byte_rate', 1]]);
    } finally {
      await admin
        .alterClientQuotas([
          new quota.ClientQuotaAlteration({
            entity: defaultUser,
            ops: [quotaOp('producer_byte_rate', null), quotaOp('request_percentage', null)],
          }),
          new quota.ClientQuotaAlteration({ entity: namedDefault, ops: [quotaOp('producer_byte_rate', null)] }),
          new quota.ClientQuotaAlteration({
            entity: new quota.ClientQuotaEntity(
              new Map([
                ['user', user],
                ['client-id', 'c'],
              ]),
            ),
            ops: [quotaOp('producer_byte_rate', null), quotaOp('consumer_byte_rate', null)],
          }),
        ])
        .all()
        .get()
        .catch(() => {});
      await admin.close();
    }
  },
  60_000,
);

const sha256 = new ScramCredentialInfo({ mechanism: ScramMechanism.SCRAM_SHA_256, iterations: 8192 });
const sha512 = new ScramCredentialInfo({ mechanism: ScramMechanism.SCRAM_SHA_512, iterations: 4096 });

// KafkaAdminClient#alterUserScramCredentials and #describeUserScramCredentials,
// whose misuses Java fails only once the broker answers (untested in Java).
(SKIP ? test.skip : test)(
  'SCRAM misuses: an empty user, an empty password, an UNKNOWN mechanism, one user twice in a describe or an alteration',
  async () => {
    const admin = newAdmin();
    const user = fresh('scram-misuse');
    try {
      const emptyUser = admin.alterUserScramCredentials([
        new UserScramCredentialUpsertion({ user: '', credentialInfo: sha256, password: 'p' }),
      ]);
      await assertFails(emptyUser.values().get(''), UnacceptableCredentialError, 'Username must not be empty');
      const emptyPassword = admin.alterUserScramCredentials([
        new UserScramCredentialUpsertion({ user, credentialInfo: sha256, password: '' }),
      ]);
      await assertFails(emptyPassword.values().get(user), UnacceptableCredentialError, 'Password must not be empty');
      const unknown = admin.alterUserScramCredentials([new UserScramCredentialDeletion({ user, mechanism: ScramMechanism.UNKNOWN })]);
      await assertFails(unknown.values().get(user), UnsupportedSaslMechanismError, 'Unknown SCRAM mechanism');
      await assertFails(
        admin.describeUserScramCredentials([user, user]).all(),
        DuplicateResourceError,
        `Cannot describe SCRAM credentials for the same user twice in a single request: ${user}`,
      );
      const twice = admin.alterUserScramCredentials([
        new UserScramCredentialUpsertion({ user, credentialInfo: sha256, password: 'a' }),
        new UserScramCredentialUpsertion({ user, credentialInfo: sha512, password: 'b' }),
      ]);
      await assertFails(twice.values().get(user), DuplicateResourceError, 'A user credential cannot be altered twice in the same request');
      await assertFails(twice.all(), DuplicateResourceError, 'A user credential cannot be altered twice in the same request');
    } finally {
      await admin.close();
    }
  },
  60_000,
);

// PlaintextAdminIntegrationTest#testDescribeUserScramCredentials.
(SKIP ? test.skip : test)(
  'testDescribeUserScramCredentials: all users, two in one call, two mechanisms of one user and one of them deleted',
  async () => {
    const admin = newAdmin();
    const [alice, bob] = [fresh('scram-a'), fresh('scram-b')];
    try {
      assert.equal(
        await admin
          .alterUserScramCredentials([
            new UserScramCredentialUpsertion({ user: alice, credentialInfo: sha256, password: 'pa' }),
            new UserScramCredentialUpsertion({ user: bob, credentialInfo: sha256, password: 'pb' }),
          ])
          .all()
          .get(),
        null,
      );
      // A second mechanism for alice, in its own call.
      assert.equal(
        await admin
          .alterUserScramCredentials([new UserScramCredentialUpsertion({ user: alice, credentialInfo: sha512, password: 'pa' })])
          .all()
          .get(),
        null,
      );
      const everyone = await retryUntil(
        () => admin.describeUserScramCredentials().all(),
        (all) => all.get(alice)?.credentialInfos.length === 2 && all.has(bob),
      );
      assert.ok(everyone.has(alice) && everyone.has(bob));
      const pair = await admin.describeUserScramCredentials([alice, bob]).all().get();
      assert.deepEqual([...pair.keys()].sort(), [alice, bob].sort());
      const mechanisms = (/** @type {string} */ user) =>
        present(pair.get(user))
          .credentialInfos.map((i) => i.mechanism().mechanismName())
          .sort();
      assert.deepEqual(mechanisms(alice), ['SCRAM-SHA-256', 'SCRAM-SHA-512']);
      assert.deepEqual(mechanisms(bob), ['SCRAM-SHA-256']);

      assert.equal(
        await admin
          .alterUserScramCredentials([new UserScramCredentialDeletion({ user: alice, mechanism: ScramMechanism.SCRAM_SHA_256 })])
          .all()
          .get(),
        null,
      );
      const left = await retryUntil(
        () => admin.describeUserScramCredentials([alice]).description(alice),
        (d) => d.credentialInfos.length === 1,
      );
      assert.equal(left.credentialInfos.length, 1);
      assert.ok(left.credentialInfos[0].equals(sha512), String(left.credentialInfos[0]));
    } finally {
      await admin
        .alterUserScramCredentials([
          new UserScramCredentialDeletion({ user: alice, mechanism: ScramMechanism.SCRAM_SHA_512 }),
          new UserScramCredentialDeletion({ user: bob, mechanism: ScramMechanism.SCRAM_SHA_256 }),
        ])
        .all()
        .get()
        .catch(() => {});
      await admin.close();
    }
  },
  60_000,
);

// ---- calls in flight, wide values and non-ASCII names ------------------------------

// The JS conventions' calls in flight (Threads and callbacks), as the Python and
// .NET suites run them: many calls on one client at once, their dependants
// run once each where their source completes, nobody awaiting the sources.
(SKIP ? test.skip : test)(
  'about 50 calls in flight on one client: Promise.all, thenApply and whenComplete once each in completion order, allOf across two Results',
  async () => {
    const admin = newAdmin();
    const topic = fresh('in-flight');
    try {
      await createTopics(admin, [topic]);
      /** @type {KafkaFuture<unknown>[]} */
      const sources = [];
      for (let i = 0; i < 50; i++) {
        const kind = i % 3;
        sources.push(
          kind === 0
            ? present(admin.describeTopics([topic]).allTopicNames())
            : kind === 1
              ? admin.listTopics().names()
              : admin.describeCluster().nodes(),
        );
      }
      /** @type {number[]} */
      const completed = [];
      const applied = new Array(50).fill(0);
      const dependants = sources.map((source, i) => {
        source.whenComplete((_value, error) => {
          assert.equal(error, null);
          assert.equal(source.isDone(), true);
          completed.push(i);
        });
        return source.thenApply(() => {
          applied[i]++;
          return i;
        });
      });
      const results = await Promise.all(dependants.map((d) => d.get()));
      assert.deepEqual(results, [...Array(50).keys()]);
      assert.deepEqual(applied, new Array(50).fill(1));
      assert.equal(completed.length, 50);
      assert.deepEqual(
        [...completed].sort((a, b) => a - b),
        [...Array(50).keys()],
      );
      for (const source of sources) assert.equal(source.isDone(), true);

      const first = admin.describeCluster();
      const second = admin.listTopics();
      const both = KafkaFuture.allOf([first.clusterId(), second.names()]);
      // Before any network round trip ends: a String future's getNow answers
      // the value given, and neither future is done.
      const ABSENT = 'absent';
      assert.equal(first.clusterId().isDone(), false);
      assert.equal(first.clusterId().getNow(ABSENT), ABSENT);
      assert.equal(both.isDone(), false);
      assert.equal(await both.get(), null);
      assert.equal(both.isDone(), true);
      assert.equal(typeof present(first.clusterId().getNow(null)), 'string');
      assert.ok(present(second.names().getNow(null)).has(topic));
    } finally {
      await deleteTopics(admin, [topic]);
      await admin.close();
    }

    // The native objects of 50 calls in flight are all freed once the client
    // is closed and collected, in a child process that can collect its garbage.
    const script = `
    const native = require('./dist/internal/native.js');
    const { Admin } = require('@confluentinc/kafka-javascript/admin');
    (async () => {
      const before = native.adminStats();
      async function round() {
        const a = Admin.create({ 'bootstrap.servers': ${JSON.stringify(BOOTSTRAP)} });
        const futures = [];
        for (let i = 0; i < 50; i++) futures.push(i % 2 ? a.listTopics().names() : a.describeCluster().nodes());
        await Promise.all(futures.map((f) => f.get()));
        await a.close();
      }
      await round();
      let after = native.adminStats();
      for (let i = 0; i < 100 && after.adminsFreed - before.adminsFreed < 1; i++) {
        global.gc();
        await new Promise((r) => setTimeout(r, 20));
        after = native.adminStats();
      }
      for (let i = 0; i < 20; i++) { global.gc(); await new Promise((r) => setTimeout(r, 20)); }
      after = native.adminStats();
      const d = (k) => after[k] - before[k];
      console.log(JSON.stringify([d('adminsMade'), d('adminsFreed'), d('resultsMade') === d('resultsFreed'), d('futuresMade') === d('futuresFreed'), d('waitsStarted') === d('waitsCompleted')]));
    })();
  `;
    const r = spawnSync(process.execPath, ['--no-warnings', '--expose-gc', '-e', script], {
      cwd: path.join(__dirname, '..', '..'),
      encoding: 'utf8',
      timeout: 60000,
    });
    assert.equal(r.status, 0, r.stderr);
    assert.equal(r.stdout, '[1,1,true,true,true]\n');
  },
  120_000,
);

// Python's test_wide_ints and the .NET marshal tests: a committed offset of
// 2^53 + 1, which a JavaScript number cannot hold, reads back exactly.
(SKIP ? test.skip : test)(
  'a committed offset of 2^53 + 1 reads back exactly',
  async () => {
    const admin = newAdmin();
    const topic = fresh('wide');
    const group = fresh('wide-group');
    try {
      await createTopics(admin, [topic]);
      const tp = new TopicPartition({ topic, partition: 0 });
      const offset = 2n ** 53n + 1n;
      assert.equal(
        await admin
          .alterConsumerGroupOffsets({ groupId: group, offsets: new TopicPartitionMap([[tp, new OffsetAndMetadata(offset)]]) })
          .all()
          .get(),
        null,
      );
      const read = await admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
      assert.equal(present(read.get(tp)).offset, offset);
    } finally {
      await admin
        .deleteConsumerGroups([group])
        .all()
        .get()
        .catch(() => {});
      await deleteTopics(admin, [topic]);
      await admin.close();
    }
  },
  60_000,
);

// The .NET string tests: names outside ASCII cross both ways, and the Results
// find the caller's own strings.
(SKIP ? test.skip : test)(
  'non-ASCII group id, transactional id, SCRAM user, quota user and client id read back exactly',
  async () => {
    const admin = newAdmin();
    const topic = fresh('unicode');
    const name = `grüße-日本-😀-${process.pid}-${Date.now()}`;
    const clientId = `client-é-${Date.now()}`;
    const entity = new quota.ClientQuotaEntity(
      new Map([
        ['user', name],
        ['client-id', clientId],
      ]),
    );
    try {
      await createTopics(admin, [topic]);
      const tp = new TopicPartition({ topic, partition: 0 });
      // A group id, with metadata outside ASCII.
      const altered = admin.alterConsumerGroupOffsets({
        groupId: name,
        offsets: new TopicPartitionMap([[tp, new OffsetAndMetadata({ offset: 3n, metadata: 'mé' })]]),
      });
      assert.equal(await altered.all().get(), null);
      const offsets = await admin.listConsumerGroupOffsets(name).partitionsToOffsetAndMetadata().get();
      assert.equal(present(offsets.get(tp)).metadata, 'mé');
      assert.ok((await admin.listGroups().all().get()).some((l) => l.groupId() === name));
      const described = admin.describeConsumerGroups([name]).describedGroups();
      assert.deepEqual([...described.keys()], [name]);
      assert.equal((await present(described.get(name)).get()).groupId, name);

      // A transactional id.
      const fenced = admin.fenceProducers([name]);
      assert.equal(await present(fenced.fencedProducers().get(name)).get(), null);
      const transaction = await admin.describeTransactions([name]).description(name).get();
      assert.equal(transaction.state, TransactionState.EMPTY);
      assert.ok((await admin.listTransactions().all().get()).some((l) => l.transactionalId() === name));

      // A SCRAM user.
      const upserted = admin.alterUserScramCredentials([
        new UserScramCredentialUpsertion({ user: name, credentialInfo: sha512, password: 'pä' }),
      ]);
      assert.deepEqual([...upserted.values().keys()], [name]);
      assert.equal(await upserted.all().get(), null);
      const scram = await retryUntil(() => admin.describeUserScramCredentials([name]).users());
      assert.deepEqual(scram, [name]);
      assert.equal((await admin.describeUserScramCredentials([name]).description(name).get()).name, name);

      // A quota user and client id.
      assert.equal(
        await admin
          .alterClientQuotas([new quota.ClientQuotaAlteration({ entity, ops: [quotaOp('producer_byte_rate', 5)] })])
          .all()
          .get(),
        null,
      );
      const quotas = await quotasOf(
        admin,
        quota.ClientQuotaFilter.contains([quota.ClientQuotaFilterComponent.ofEntity({ entityType: 'user', entityName: name })]),
        (entities) => entities.size() === 1,
      );
      const [found] = [...quotas.keys()];
      assert.deepEqual([...found.entries()].sort(), [
        ['client-id', clientId],
        ['user', name],
      ]);
      assert.deepEqual([...present(quotas.get(entity))], [['producer_byte_rate', 5]]);
    } finally {
      await admin
        .alterClientQuotas([new quota.ClientQuotaAlteration({ entity, ops: [quotaOp('producer_byte_rate', null)] })])
        .all()
        .get()
        .catch(() => {});
      await admin
        .alterUserScramCredentials([new UserScramCredentialDeletion({ user: name, mechanism: ScramMechanism.SCRAM_SHA_512 })])
        .all()
        .get()
        .catch(() => {});
      await admin
        .deleteConsumerGroups([name])
        .all()
        .get()
        .catch(() => {});
      await deleteTopics(admin, [topic]);
      await admin.close();
    }
  },
  60_000,
);
