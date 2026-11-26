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

// The Admin interface's calls with no broker: an unreachable bootstrap
// address, or the MockAdminClient. Java's tests of these paths
// (KafkaAdminClientTest, KafkaAdminClientTopicTest) script MockClient, which
// the binding cannot inject; where the double only makes a call fail or leaves
// it pending, the port runs against 127.0.0.1:1, as kafka_admin_client.test.js
// does. The rest are the binding's own: what crosses the C API (64-bit values,
// null and empty kept apart) and the value-keyed Results.

const assert = require('node:assert/strict');

const { ExecutionError } = require('@confluentinc/kafka-javascript');
const {
  AbortTransactionSpec,
  Admin,
  AlterConfigOp,
  AlterConsumerGroupOffsetsOptions,
  AlterPartitionReassignmentsOptions,
  ConfigEntry,
  CreateAclsOptions,
  CreateDelegationTokenOptions,
  CreateTopicsOptions,
  DeleteConsumerGroupOffsetsOptions,
  DeleteTopicsOptions,
  DescribeConfigsOptions,
  DescribeDelegationTokenOptions,
  DescribeTopicsOptions,
  DescribeUserScramCredentialsOptions,
  ElectLeadersOptions,
  FeatureUpdate,
  ListConfigResourcesOptions,
  ListGroupsOptions,
  ListPartitionReassignmentsOptions,
  ListTopicsOptions,
  ListTransactionsOptions,
  MemberToRemove,
  MockAdminClient,
  NewTopic,
  OffsetSpec,
  RemoveMembersFromConsumerGroupOptions,
  ScramCredentialInfo,
  ScramMechanism,
  UserScramCredentialUpsertion,
} = require('@confluentinc/kafka-javascript/admin');
const {
  ElectionType,
  KafkaError,
  KafkaFuture,
  TopicCollection,
  TopicPartition,
  TopicPartitionMap,
  TopicPartitionReplica,
  TopicPartitionReplicaMap,
  Uuid,
} = require('@confluentinc/kafka-javascript/common');
const acl = require('@confluentinc/kafka-javascript/common/acl');
const { ConfigError, ConfigResource, ConfigResourceMap } = require('@confluentinc/kafka-javascript/common/config');
const {
  DelegationTokenNotFoundError,
  InvalidRequestError,
  InvalidTopicError,
  TimeoutError,
} = require('@confluentinc/kafka-javascript/common/errors');
const quota = require('@confluentinc/kafka-javascript/common/quota');
const res = require('@confluentinc/kafka-javascript/common/resource');
const { KafkaPrincipal } = require('@confluentinc/kafka-javascript/common/security/auth');
const { DelegationToken, TokenInformation } = require('@confluentinc/kafka-javascript/common/security/token/delegation');
const { OffsetAndMetadata } = require('@confluentinc/kafka-javascript/consumer');

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
 * @param {KafkaFuture<unknown> | null | undefined} future
 * @returns {Promise<Error>}
 */
async function failure(future) {
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

/** A createTopics call that stays pending on the unreachable address for 15 minutes. */
const pendingCreate = (/** @type {import('@confluentinc/kafka-javascript/admin').Admin} */ client, /** @type {string} */ name) =>
  client
    .createTopics({
      newTopics: [new NewTopic({ name, numPartitions: 1, replicationFactor: 1 })],
      options: new CreateTopicsOptions({ timeoutMs: 900000 }),
    })
    .all();

const EXITED = 'The AdminClient thread has exited. Call: createTopics';

// ---- close() with calls pending -------------------------------------------------

// PlaintextAdminIntegrationTest#testForceClose: close(Duration.ZERO) with a
// call pending fails it with TimeoutException; Java's AdminClientRunnable
// fails what is left with "The AdminClient thread has exited." followed by
// " Call: " and the call's name (TimeoutProcessor#handleTimeouts). The same with a timeout the call outlives, and
// from a future's action, as the Python and .NET close tests check.
for (const timeout of [0, 1500]) {
  test(`testForceClose: close({ timeout: ${timeout} }) fails each pending call once, wakes a waiting get(), and returns on time`, async () => {
    const client = admin();
    const futures = [1, 2, 3].map((i) => pendingCreate(client, `t${i}`));
    let completions = 0;
    futures[0].whenComplete(() => {
      completions++;
    });
    /** @type {Promise<unknown>} */
    const waiting = futures[1].get().then(
      () => assert.fail('the pending call succeeded'),
      (error) => error,
    );
    await new Promise((resolve) => setTimeout(resolve, 200));
    for (const future of futures) assert.equal(future.isDone(), false);
    const started = Date.now();
    await client.close({ timeout });
    const took = Date.now() - started;
    assert.ok(took >= timeout - 50 && took < timeout + 3000, `close took ${took} ms`);
    for (const future of futures) {
      const cause = await failure(future);
      assert.ok(cause instanceof TimeoutError, String(cause));
      assert.equal(cause.message, EXITED);
    }
    const woken = await waiting;
    assert.ok(woken instanceof ExecutionError, String(woken));
    assert.ok(woken.cause instanceof TimeoutError);
    assert.equal(woken.cause.message, EXITED);
    assert.equal(completions, 1);
  }, 30000);
}

test("close({ timeout: 0 }) from a future's action aborts another pending call", async () => {
  const client = admin();
  const first = client.listTopics(new ListTopicsOptions({ timeoutMs: 300 })).names();
  const second = pendingCreate(client, 't');
  /** @type {Promise<void> | null} */
  let closing = null;
  first.whenComplete(() => {
    closing = client.close({ timeout: 0 });
  });
  const started = Date.now();
  const firstCause = await failure(first);
  assert.ok(firstCause instanceof TimeoutError);
  assert.equal(firstCause.message, 'Timed out waiting for a node assignment. Call: listTopics');
  const secondCause = await failure(second);
  assert.ok(secondCause instanceof TimeoutError);
  assert.equal(secondCause.message, EXITED);
  assert.ok(Date.now() - started < 10000);
  assert.ok(closing);
  await closing;
}, 30000);

// ---- timeoutMs ------------------------------------------------------------------

// PlaintextAdminIntegrationTest#testDescribeConfigWithOptionTimeoutMs,
// #testDeleteTopicsWithOptionTimeoutMs, #testListConfigResourcesTimeoutMs and
// AdminMetadataTest#testListTopicsWithOptionTimeoutMs: a timeout of 0 (and a
// negative one, read as already passed) fails the call at once with
// TimeoutException, not after default.api.timeout.ms.
test('timeoutMs 0 and -5 fail describeConfigs, deleteTopics, listConfigResources and listTopics at once', async () => {
  const client = admin({ 'default.api.timeout.ms': 120000 });
  const resource = new ConfigResource({ type: ConfigResource.Type.TOPIC, name: 't' });
  try {
    for (const timeoutMs of [0, -5]) {
      /** @type {Array<[string, () => KafkaFuture<unknown>]>} */
      const calls = [
        [
          'describeConfigs',
          () => client.describeConfigs({ resources: [resource], options: new DescribeConfigsOptions({ timeoutMs }) }).all(),
        ],
        ['deleteTopics', () => client.deleteTopics({ topics: ['t'], options: new DeleteTopicsOptions({ timeoutMs }) }).all()],
        [
          'listConfigResources',
          () => client.listConfigResources({ configResourceTypes: [], options: new ListConfigResourcesOptions({ timeoutMs }) }).all(),
        ],
        ['listTopics', () => client.listTopics(new ListTopicsOptions({ timeoutMs })).names()],
      ];
      for (const [call, run] of calls) {
        const started = Date.now();
        const cause = await failure(run());
        const took = Date.now() - started;
        assert.ok(cause instanceof TimeoutError, `${call} ${timeoutMs}: ${String(cause)}`);
        assert.equal(cause.message, `Timed out waiting for a node assignment. Call: ${call}`);
        assert.ok(took < 5000, `${call} with timeoutMs ${timeoutMs} took ${took} ms`);
      }
    }
  } finally {
    await client.close({ timeout: 0 });
  }
}, 60000);

test('timeoutMs(null) after a set goes back to the default', async () => {
  const options = new ListTopicsOptions({ timeoutMs: 5 });
  assert.equal(options.timeoutMs(), 5);
  assert.equal(options.timeoutMs(null), options);
  assert.equal(options.timeoutMs(), null);
  // Back to default.api.timeout.ms: the call outlives the 5 ms it had.
  const client = admin({ 'default.api.timeout.ms': 1500, 'request.timeout.ms': 1000 });
  try {
    const started = Date.now();
    const cause = await failure(client.listTopics(options).names());
    const took = Date.now() - started;
    assert.ok(cause instanceof TimeoutError, String(cause));
    assert.ok(took >= 1000, `the call failed after ${took} ms, before default.api.timeout.ms`);
  } finally {
    await client.close({ timeout: 0 });
  }
}, 30000);

// ---- the requests the core refuses before it sends them -------------------------

// KafkaAdminClientTopicTest#testDescribeTopicsByIds and KafkaAdminClient
// #deleteTopics: Uuid.ZERO_UUID cannot be represented in a request, so only its
// key fails, at once; the valid id is still pending.
test('testDescribeTopicsByIds: Uuid.ZERO_UUID beside a valid id fails only its own key, in deleteTopics and describeTopics', async () => {
  const client = admin();
  const id = Uuid.randomUuid();
  try {
    const deleted = present(client.deleteTopics(TopicCollection.ofTopicIds([Uuid.ZERO_UUID, id])).topicIdValues());
    const deleteCause = await failure(deleted.get(Uuid.ZERO_UUID));
    assert.equal(deleteCause.constructor, InvalidTopicError);
    assert.equal(deleteCause.message, "The given topic ID 'AAAAAAAAAAAAAAAAAAAAAA' cannot be represented in a request.");
    assert.equal(present(deleted.get(id)).isDone(), false);

    const described = present(
      client
        .describeTopics({
          topics: TopicCollection.ofTopicIds([Uuid.ZERO_UUID, id]),
          options: new DescribeTopicsOptions({ timeoutMs: 300 }),
        })
        .topicIdValues(),
    );
    const describeCause = await failure(described.get(Uuid.ZERO_UUID));
    assert.equal(describeCause.constructor, InvalidTopicError);
    assert.equal(describeCause.message, "The given topic id 'AAAAAAAAAAAAAAAAAAAAAA' cannot be represented in a request.");
    const other = await failure(described.get(id));
    assert.ok(other instanceof TimeoutError, String(other));
  } finally {
    await client.close({ timeout: 0 });
  }
});

// KafkaAdminClientTopicTest#testAlterPartitionReassignments and
// #testListPartitionReassignments: a blank topic or a negative partition fails
// its own alter key, and the whole list call.
test('testAlterPartitionReassignments: a blank topic and partition -1 fail only their alter keys, and the whole list future', async () => {
  const client = admin();
  const blank = new TopicPartition({ topic: '', partition: 0 });
  const negative = new TopicPartition({ topic: 't', partition: -1 });
  const valid = new TopicPartition({ topic: 't', partition: 0 });
  try {
    const altered = client.alterPartitionReassignments({
      reassignments: new TopicPartitionMap([
        [blank, null],
        [negative, null],
        [valid, null],
      ]),
      options: new AlterPartitionReassignmentsOptions({ timeoutMs: 300 }),
    });
    const blankCause = await failure(altered.values().get(blank));
    assert.equal(blankCause.constructor, InvalidTopicError);
    assert.equal(blankCause.message, "The given topic name '' cannot be represented in a request.");
    const negativeCause = await failure(altered.values().get(negative));
    assert.equal(negativeCause.constructor, InvalidTopicError);
    assert.equal(negativeCause.message, 'The given partition index -1 is not valid.');
    const validCause = await failure(altered.values().get(valid));
    assert.ok(validCause instanceof TimeoutError, String(validCause));

    const listedBlank = await failure(client.listPartitionReassignments([blank, valid]).reassignments());
    assert.equal(listedBlank.constructor, InvalidTopicError);
    assert.equal(listedBlank.message, "The given topic name '' cannot be represented in a request.");
    const listedNegative = await failure(client.listPartitionReassignments([negative, valid]).reassignments());
    assert.equal(listedNegative.constructor, InvalidTopicError);
    assert.equal(listedNegative.message, 'The given partition index -1 is not valid.');
  } finally {
    await client.close({ timeout: 0 });
  }
});

// KafkaAdminClient#createAcls validates each binding before it sends any: an
// UNKNOWN element fails that binding's future with InvalidRequestException.
test('createAcls with an UNKNOWN pattern type, operation or permission type: InvalidRequestError with Java text', async () => {
  const client = admin();
  /**
   * @param {import('@confluentinc/kafka-javascript/common/resource').PatternType} patternType
   * @param {import('@confluentinc/kafka-javascript/common/acl').AclOperation} operation
   * @param {import('@confluentinc/kafka-javascript/common/acl').AclPermissionType} permissionType
   */
  const binding = (patternType, operation, permissionType) =>
    new acl.AclBinding({
      pattern: new res.ResourcePattern({ resourceType: res.ResourceType.TOPIC, name: 't', patternType }),
      entry: new acl.AccessControlEntry({ principal: 'User:x', host: '*', operation, permissionType }),
    });
  const cases = [
    [
      binding(res.PatternType.UNKNOWN, acl.AclOperation.READ, acl.AclPermissionType.ALLOW),
      'Invalid ACL creation: Resource pattern type is UNKNOWN.',
    ],
    [binding(res.PatternType.LITERAL, acl.AclOperation.UNKNOWN, acl.AclPermissionType.ALLOW), 'Invalid ACL creation: Operation is UNKNOWN'],
    [
      binding(res.PatternType.LITERAL, acl.AclOperation.READ, acl.AclPermissionType.UNKNOWN),
      'Invalid ACL creation: Permission type is UNKNOWN',
    ],
  ];
  try {
    const result = client.createAcls({
      acls: cases.map(([b]) => /** @type {import('@confluentinc/kafka-javascript/common/acl').AclBinding} */ (b)),
      options: new CreateAclsOptions({ timeoutMs: 300 }),
    });
    for (const [b, message] of cases) {
      const cause = await failure(result.values().get(/** @type {import('@confluentinc/kafka-javascript/common/acl').AclBinding} */ (b)));
      assert.equal(cause.constructor, InvalidRequestError);
      assert.equal(cause.message, message);
    }
  } finally {
    await client.close({ timeout: 0 });
  }
});

// KafkaAdminClient#createInternal: AdminClientConfig's checks and
// ClientUtils#parseAndValidateAddresses throw ConfigException, which Java wraps
// in KafkaException("Failed to create new KafkaAdminClient").
test('Admin.create with neither bootstrap key, with both, or with a URL without a port', () => {
  /** @type {Array<[import('@confluentinc/kafka-javascript/admin').AdminClientConfigs, string]>} */
  const cases = [
    [{}, 'You must set either bootstrap.servers or bootstrap.controllers'],
    [
      { 'bootstrap.servers': 'localhost:1', 'bootstrap.controllers': 'localhost:2' },
      'You cannot set both bootstrap.servers and bootstrap.controllers',
    ],
    [{ 'bootstrap.servers': 'localhost' }, 'Invalid url in bootstrap.servers: localhost'],
  ];
  for (const [configs, message] of cases) {
    assert.throws(
      () => Admin.create(configs),
      (error) => {
        assert.ok(error instanceof KafkaError && error.constructor === KafkaError, String(error));
        assert.equal(error.message, 'Failed to create new KafkaAdminClient');
        assert.equal(/** @type {Error} */ (error.cause).constructor, ConfigError);
        assert.equal(/** @type {Error} */ (error.cause).message, message);
        return true;
      },
    );
  }
});

// ---- null and empty kept apart -------------------------------------------------

test("an ACL filter's null name matches every name and '' only the empty one; deleteAcls keeps them as two keys", async () => {
  /** @param {string | null} name */
  const filter = (name) =>
    new acl.AclBindingFilter({
      patternFilter: new res.ResourcePatternFilter({ resourceType: res.ResourceType.TOPIC, name, patternType: res.PatternType.LITERAL }),
      entryFilter: acl.AccessControlEntryFilter.ANY,
    });
  const binding = new acl.AclBinding({
    pattern: new res.ResourcePattern({ resourceType: res.ResourceType.TOPIC, name: 't', patternType: res.PatternType.LITERAL }),
    entry: new acl.AccessControlEntry({
      principal: 'User:x',
      host: '*',
      operation: acl.AclOperation.READ,
      permissionType: acl.AclPermissionType.ALLOW,
    }),
  });
  assert.equal(filter(null).equals(filter('')), false);
  assert.equal(filter(null).matches(binding), true);
  assert.equal(filter('').matches(binding), false);
  assert.equal(filter(null).patternFilter().name(), null);
  assert.equal(filter('').patternFilter().name(), '');
  const client = admin();
  try {
    const deleted = client.deleteAcls([filter(null), filter('')]);
    assert.equal(deleted.values().size(), 2);
    assert.ok(deleted.values().get(filter(null)));
    assert.ok(deleted.values().get(filter('')));
  } finally {
    await client.close({ timeout: 0 });
  }
});

test("quota entities { user: null }, { user: '' } and { user: 'alice' } are three keys, each found with a fresh instance", async () => {
  /** @param {string | null} user */
  const entity = (user) => new quota.ClientQuotaEntity(new Map([[quota.ClientQuotaEntity.USER, user]]));
  const op = new quota.ClientQuotaAlteration.Op({ key: 'producer_byte_rate', value: 1 });
  const client = admin();
  try {
    const result = client.alterClientQuotas(
      [null, '', 'alice'].map((user) => new quota.ClientQuotaAlteration({ entity: entity(user), ops: [op] })),
    );
    assert.equal(result.values().size(), 3);
    for (const user of [null, '', 'alice']) {
      assert.ok(result.values().get(entity(user)), String(user));
      assert.equal(present(result.values().get(entity(user))).isDone(), false);
    }
    assert.equal(entity(null).equals(entity('')), false);
  } finally {
    await client.close({ timeout: 0 });
  }
});

// Java's MockAdminClient#describeDelegationToken lists every token when the
// owners are empty and filters by owner otherwise; the options keep null
// (Java's default, every owner) and [] apart.
test('token owners null and [] kept apart; a zero-length HMAC, password or salt crosses as empty', async () => {
  assert.equal(new DescribeDelegationTokenOptions().owners(), null);
  assert.equal(new DescribeDelegationTokenOptions({ owners: null }).owners(), null);
  assert.deepEqual(new DescribeDelegationTokenOptions({ owners: [] }).owners(), []);
  const alice = new KafkaPrincipal({ principalType: KafkaPrincipal.USER_TYPE, name: 'alice' });
  const bob = new KafkaPrincipal({ principalType: KafkaPrincipal.USER_TYPE, name: 'bob' });
  const mock = MockAdminClient.create().numBrokers(1).build();
  try {
    const token = await mock
      .createDelegationToken(new CreateDelegationTokenOptions({ renewers: [alice] }))
      .delegationToken()
      .get();
    /** @param {DescribeDelegationTokenOptions} options */
    const listed = async (options) =>
      (await mock.describeDelegationToken(options).delegationTokens().get()).map((t) => t.tokenInfo().tokenId());
    const id = token.tokenInfo().tokenId();
    assert.deepEqual(await listed(new DescribeDelegationTokenOptions({ owners: null })), [id]);
    assert.deepEqual(await listed(new DescribeDelegationTokenOptions({ owners: [] })), [id]);
    assert.deepEqual(await listed(new DescribeDelegationTokenOptions({ owners: [alice] })), [id]);
    assert.deepEqual(await listed(new DescribeDelegationTokenOptions({ owners: [bob] })), []);
    // An empty HMAC is no token's: Java's mock compares the bytes.
    const renewed = await failure(mock.renewDelegationToken(new Uint8Array(0)).expiryTimestamp());
    assert.equal(renewed.constructor, DelegationTokenNotFoundError);
    assert.equal(renewed.message, '');
    const expired = await failure(mock.expireDelegationToken(new Uint8Array(0)).expiryTimestamp());
    assert.equal(expired.constructor, DelegationTokenNotFoundError);
    assert.equal((await listed(new DescribeDelegationTokenOptions())).length, 1);
  } finally {
    await mock.close();
  }
  const empty = new DelegationToken({ tokenInformation: tokenInformation(), hmac: new Uint8Array(0) });
  assert.equal(empty.hmac().length, 0);
  assert.equal(empty.hmacAsBase64String(), '');
  const upsertion = new UserScramCredentialUpsertion({
    user: 'u',
    credentialInfo: new ScramCredentialInfo({ mechanism: ScramMechanism.SCRAM_SHA_256, iterations: 4096 }),
    password: new Uint8Array(0),
    salt: new Uint8Array(0),
  });
  assert.equal(upsertion.password().length, 0);
  assert.equal(upsertion.salt().length, 0);
  const client = admin();
  try {
    const altered = client.alterUserScramCredentials([upsertion]);
    assert.deepEqual([...altered.values().keys()], ['u']);
  } finally {
    await client.close({ timeout: 0 });
  }
});

/** A TokenInformation for a token built by hand. */
function tokenInformation() {
  const owner = new KafkaPrincipal({ principalType: KafkaPrincipal.USER_TYPE, name: 'alice' });
  return new TokenInformation({ tokenId: 'id', owner, renewers: [], issueTimestamp: 0, maxTimestamp: 0, expiryTimestamp: 0 });
}

// ---- value-keyed Results -------------------------------------------------------

test('every value-keyed Result is found with an equal key built separately, and a repeated key is one entry', async () => {
  const client = admin();
  try {
    // A Uuid with negative halves, rebuilt from its string.
    const id = new Uuid({ mostSigBits: -5n, leastSigBits: -(2n ** 63n) });
    const text = id.toString();
    const deleted = present(
      client.deleteTopics(TopicCollection.ofTopicIds([Uuid.fromString(text), Uuid.fromString(text)])).topicIdValues(),
    );
    assert.equal(deleted.size(), 1);
    assert.ok(deleted.get(Uuid.fromString(text)));
    assert.equal(Uuid.fromString(text).getMostSignificantBits(), -5n);
    assert.equal(Uuid.fromString(text).getLeastSignificantBits(), -(2n ** 63n));

    const resource = () => new ConfigResource({ type: ConfigResource.Type.TOPIC, name: 't' });
    const described = client.describeConfigs([resource(), resource()]);
    assert.equal(described.values().size(), 1);
    assert.ok(described.values().get(resource()));
    const altered = client.incrementalAlterConfigs(
      new ConfigResourceMap([
        [
          resource(),
          [
            new AlterConfigOp({
              configEntry: new ConfigEntry({ name: 'retention.ms', value: '1' }),
              operationType: AlterConfigOp.OpType.SET,
            }),
          ],
        ],
      ]),
    );
    assert.ok(altered.values().get(resource()));

    const binding = () =>
      new acl.AclBinding({
        pattern: new res.ResourcePattern({ resourceType: res.ResourceType.GROUP, name: 'g', patternType: res.PatternType.PREFIXED }),
        entry: new acl.AccessControlEntry({
          principal: 'User:x',
          host: '*',
          operation: acl.AclOperation.READ,
          permissionType: acl.AclPermissionType.ALLOW,
        }),
      });
    const created = client.createAcls([binding(), binding()]);
    assert.equal(created.values().size(), 1);
    assert.ok(created.values().get(binding()));
    const deletedAcls = client.deleteAcls([binding().toFilter(), binding().toFilter()]);
    assert.equal(deletedAcls.values().size(), 1);
    assert.ok(deletedAcls.values().get(binding().toFilter()));

    const quotas = client.alterClientQuotas([
      new quota.ClientQuotaAlteration({
        entity: new quota.ClientQuotaEntity(
          new Map([
            ['user', 'u'],
            ['client-id', 'c'],
          ]),
        ),
        ops: [],
      }),
    ]);
    assert.ok(
      quotas.values().get(
        new quota.ClientQuotaEntity(
          new Map([
            ['client-id', 'c'],
            ['user', 'u'],
          ]),
        ),
      ),
    );

    const replica = () => new TopicPartitionReplica({ topic: 't', partition: 0, brokerId: 1 });
    const replicaDirs = client.describeReplicaLogDirs([replica(), replica()]);
    assert.equal(replicaDirs.values().size(), 1);
    assert.ok(replicaDirs.values().get(replica()));
    const moved = client.alterReplicaLogDirs(new TopicPartitionReplicaMap([[replica(), '/d']]));
    assert.ok(moved.values().get(replica()));

    const removed = client.removeMembersFromConsumerGroup({
      groupId: 'g',
      options: new RemoveMembersFromConsumerGroupOptions([new MemberToRemove('i'), new MemberToRemove('i')]),
    });
    assert.ok(removed.memberResult(new MemberToRemove('i')) instanceof KafkaFuture);
  } finally {
    await client.close({ timeout: 0 });
  }
});

// ---- empty requests --------------------------------------------------------------

// Where Java sends nothing for an empty request (KafkaAdminClient#createTopics,
// #deleteTopics, the AdminApiDriver with no keys), the future settles at once;
// where it still sends one (listPartitionReassignments, electLeaders,
// describeUserScramCredentials, the group-offset calls through the
// coordinator), it waits for a node and times out.
test('an empty request settles at once where Java sends nothing, and waits for a node where Java sends one', async () => {
  const client = admin();
  try {
    assert.equal(await client.createTopics([]).all().get(), null);
    assert.equal(await client.deleteTopics([]).all().get(), null);
    assert.equal((await client.describeConsumerGroups([]).all().get()).size, 0);

    /** @type {Array<[string, () => KafkaFuture<unknown>]>} */
    const waits = [
      [
        'listPartitionReassignments',
        () =>
          client
            .listPartitionReassignments({ partitions: [], options: new ListPartitionReassignmentsOptions({ timeoutMs: 1000 }) })
            .reassignments(),
      ],
      [
        'electLeaders',
        () =>
          client
            .electLeaders({ electionType: ElectionType.PREFERRED, partitions: [], options: new ElectLeadersOptions({ timeoutMs: 1000 }) })
            .partitions(),
      ],
      [
        'offsetCommit(api=FIND_COORDINATOR)',
        () =>
          client
            .alterConsumerGroupOffsets({
              groupId: 'g',
              offsets: new TopicPartitionMap(),
              options: new AlterConsumerGroupOffsetsOptions({ timeoutMs: 1000 }),
            })
            .all(),
      ],
      [
        'offsetDelete(api=FIND_COORDINATOR)',
        () =>
          client
            .deleteConsumerGroupOffsets({
              groupId: 'g',
              partitions: [],
              options: new DeleteConsumerGroupOffsetsOptions({ timeoutMs: 1000 }),
            })
            .all(),
      ],
      [
        'describeUserScramCredentials',
        () =>
          client.describeUserScramCredentials({ users: [], options: new DescribeUserScramCredentialsOptions({ timeoutMs: 1000 }) }).all(),
      ],
    ];
    for (const [call, run] of waits) {
      const started = Date.now();
      const cause = await failure(run());
      const took = Date.now() - started;
      assert.ok(cause instanceof TimeoutError, `${call}: ${String(cause)}`);
      assert.equal(cause.message, `Timed out waiting for a node assignment. Call: ${call}`);
      assert.ok(took >= 900, `${call} settled after ${took} ms, without waiting for a node`);
    }
  } finally {
    await client.close({ timeout: 0 });
  }
}, 30000);

// ---- 64-bit values ---------------------------------------------------------------

test('64-bit values cross both ways: Long.MAX and MIN offsets on the mock, and the ends of the int and short ranges', async () => {
  const LONG_MAX = 2n ** 63n - 1n;
  const LONG_MIN = -(2n ** 63n);
  const mock = MockAdminClient.create().numBrokers(1).build();
  const tp = new TopicPartition({ topic: 't', partition: 0 });
  try {
    await mock
      .createTopics([new NewTopic({ name: 't', numPartitions: 1, replicationFactor: 1 })])
      .all()
      .get();
    mock.updateEndOffsets(new TopicPartitionMap([[tp, LONG_MAX]]));
    mock.updateBeginningOffsets(new TopicPartitionMap([[tp, LONG_MIN]]));
    const latest = await mock
      .listOffsets(new TopicPartitionMap([[tp, OffsetSpec.latest()]]))
      .partitionResult(tp)
      .get();
    assert.equal(latest.offset(), LONG_MAX);
    const earliest = await mock
      .listOffsets(new TopicPartitionMap([[tp, OffsetSpec.earliest()]]))
      .partitionResult(tp)
      .get();
    assert.equal(earliest.offset(), LONG_MIN);
    mock.updateConsumerGroupOffsets(new TopicPartitionMap([[tp, LONG_MAX]]));
    const committed = await mock.listConsumerGroupOffsets('g').partitionsToOffsetAndMetadata().get();
    assert.equal(present(committed.get(tp)).offset, LONG_MAX);
    assert.throws(() => mock.updateEndOffsets(new TopicPartitionMap([[tp, 2n ** 63n]])), {
      name: 'RangeError',
      message: 'an offset must be from -9223372036854775808 to 9223372036854775807',
    });

    const token = await mock
      .createDelegationToken(
        new CreateDelegationTokenOptions({
          renewers: [new KafkaPrincipal({ principalType: KafkaPrincipal.USER_TYPE, name: 'alice' })],
          maxLifetimeMs: LONG_MAX,
        }),
      )
      .delegationToken()
      .get();
    assert.equal(token.tokenInfo().maxTimestamp(), LONG_MAX);
  } finally {
    await mock.close();
  }

  const spec = new AbortTransactionSpec({ topicPartition: tp, producerId: LONG_MAX, producerEpoch: 32767, coordinatorEpoch: 2147483647 });
  assert.equal(spec.producerId(), LONG_MAX);
  assert.equal(spec.producerEpoch(), 32767);
  assert.equal(spec.coordinatorEpoch(), 2147483647);
  assert.equal(
    new AbortTransactionSpec({
      topicPartition: tp,
      producerId: LONG_MIN,
      producerEpoch: -32768,
      coordinatorEpoch: -(2 ** 31),
    }).producerId(),
    LONG_MIN,
  );
  const listing = new ListTransactionsOptions({ filteredProducerIds: [LONG_MAX, LONG_MIN], filteredDuration: LONG_MAX });
  assert.deepEqual([...listing.filteredProducerIds()].sort(), [LONG_MIN, LONG_MAX].sort());
  assert.equal(listing.filteredDuration(), LONG_MAX);
  assert.equal(new CreateDelegationTokenOptions({ maxLifetimeMs: LONG_MAX }).maxLifetimeMs(), LONG_MAX);

  /** @type {Array<[() => unknown, string]>} */
  const refused = [
    [
      () => new AbortTransactionSpec({ topicPartition: tp, producerId: 2n ** 63n, producerEpoch: 0, coordinatorEpoch: 0 }),
      'producerId must be from -9223372036854775808 to 9223372036854775807',
    ],
    [
      () => new AbortTransactionSpec({ topicPartition: tp, producerId: 1n, producerEpoch: 32768, coordinatorEpoch: 0 }),
      'producerEpoch must be from -32768 to 32767',
    ],
    [
      () => new ListTransactionsOptions({ filteredProducerIds: [2n ** 63n] }),
      'producerIdFilters must be from -9223372036854775808 to 9223372036854775807',
    ],
    [
      () => new CreateDelegationTokenOptions({ maxLifetimeMs: 2n ** 63n }),
      'maxLifetimeMs must be from -9223372036854775808 to 9223372036854775807',
    ],
    [() => new OffsetAndMetadata(2n ** 63n), 'offset must be from -9223372036854775808 to 9223372036854775807'],
    [() => new NewTopic({ name: 'x', numPartitions: 1, replicationFactor: 32768 }), 'replicationFactor must be from -32768 to 32767'],
    [
      () => new FeatureUpdate({ maxVersionLevel: 32768, upgradeType: FeatureUpdate.UpgradeType.UPGRADE }),
      'maxVersionLevel must be from -32768 to 32767',
    ],
  ];
  for (const [build, message] of refused) assert.throws(build, { name: 'RangeError', message });

  // The ends of the int and short ranges are accepted.
  const topic = new NewTopic({ name: 'x', numPartitions: 2147483647, replicationFactor: 32767 });
  assert.equal(topic.numPartitions(), 2147483647);
  assert.equal(topic.replicationFactor(), 32767);
  assert.equal(new FeatureUpdate({ maxVersionLevel: 32767, upgradeType: FeatureUpdate.UpgradeType.UPGRADE }).maxVersionLevel(), 32767);
  const client = admin();
  try {
    const dirs = client.describeLogDirs([2147483647, -2147483648]);
    assert.deepEqual(
      [...dirs.descriptions().keys()].sort((a, b) => a - b),
      [-2147483648, 2147483647],
    );
    assert.throws(() => client.describeLogDirs([2147483648]), {
      name: 'RangeError',
      message: 'a broker id must be from -2147483648 to 2147483647',
    });
    assert.ok(client.alterConsumerGroupOffsets({ groupId: 'g', offsets: new TopicPartitionMap([[tp, new OffsetAndMetadata(LONG_MAX)]]) }));
  } finally {
    await client.close({ timeout: 0 });
  }
});
