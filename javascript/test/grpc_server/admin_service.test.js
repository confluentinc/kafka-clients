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

// The AdminService of the gRPC conformance server (grpc-server/), called
// in-process against the client's MockAdminClient, as producer_service.test.js
// calls the ProducerService: the requests are the plain objects
// @grpc/proto-loader hands the handlers (snake_case fields, int64s as decimal
// strings, an unset field absent), and no broker is needed.

const assert = require('node:assert/strict');
const path = require('node:path');

const { AdminService } = require('../../grpc-server/grpc_admin');
const { loadProtos } = require('../../grpc-server/grpc_server');
const { LOCAL_ILLEGAL_ARGUMENT } = require('../../grpc-server/grpc_admin_translate');
const { LOCAL_ILLEGAL_STATE, LOCAL_UNSUPPORTED_OPERATION } = require('../../grpc-server/grpc_translate');
const { errorFfiId } = require('../../dist/internal/errors');
const { InvalidRequestError } = require('@confluentinc/kafka-javascript/common/errors');
const { DelegationToken } = require('@confluentinc/kafka-javascript/common/security/token/delegation');

/**
 * @typedef {{ code: number, message: string }} ProtoError
 * @typedef {{ key: Record<string, unknown>, error?: ProtoError, value?: unknown }} Entry
 * @typedef {{ entries?: Entry[], error?: ProtoError }} Keyed
 */

/** @type {AdminService[]} */
const services = [];

/**
 * A new service, its clients closed after the test.
 *
 * @returns {AdminService} the service
 */
function newService() {
  const service = new AdminService();
  services.push(service);
  return service;
}

afterEach(async () => {
  await Promise.all(services.splice(0).map((s) => s.closeAll(0)));
});

/**
 * A mock admin client of the service (an empty config).
 *
 * @param {AdminService} service the service
 * @returns {Promise<string>} its admin_id, as the wire carries a uint64
 */
async function mock(service) {
  const response = /** @type {{ admin_id: number, error?: ProtoError }} */ (await service.createAdmin({ config: {}, num_brokers: 1 }));
  assert.equal(response.error, undefined, JSON.stringify(response.error));
  assert.ok(response.admin_id > 0);
  return String(response.admin_id);
}

/** The core mock's error for a call Java's MockAdminClient leaves unimplemented. */
const NOT_IMPLEMENTED = { code: LOCAL_UNSUPPORTED_OPERATION, message: 'Not implemented yet' };

test('every RPC of admin_service.proto has a handler', () => {
  // The service as grpc_server.js loads it from the shared proto, so an RPC
  // added there, or a misspelled RPC name here, fails this test.
  const pkg = /** @type {{ AdminService: { service: Record<string, unknown> } }} */ (
    loadProtos(path.join(__dirname, '..', '..', '..', 'rust', 'multilanguage-test-server', 'proto'))
  );
  const rpcs = Object.keys(pkg.AdminService.service).sort();
  assert.equal(rpcs.length, 47);
  assert.deepEqual(Object.keys(AdminService.RPCS).sort(), rpcs);
  const service = newService();
  for (const [rpc, method] of Object.entries(AdminService.RPCS)) {
    assert.equal(typeof (/** @type {Record<string, unknown>} */ (/** @type {unknown} */ (service))[method]), 'function', rpc);
  }
  assert.deepEqual(Object.keys(service.handlers()).sort(), rpcs);
});

test('CreateAdmin with an empty config takes the mock path, and CreateTopics then ListTopics read it back', async () => {
  const service = newService();
  const id = await mock(service);
  const created = /** @type {Keyed} */ (
    await service.createTopics({ admin_id: id, topics: [{ name: 't', num_partitions: 2, replication_factor: 1 }] })
  );
  assert.ok(created.entries);
  assert.equal(created.entries.length, 1);
  assert.deepEqual(created.entries[0].key, { name: 't' });
  const listed = /** @type {{ listings: Array<{ name: string }> }} */ (await service.listTopics({ admin_id: id }));
  assert.deepEqual(
    listed.listings.map((l) => l.name),
    ['t'],
  );
});

test('a call naming an admin client the server does not hold answers its error', async () => {
  const service = newService();
  assert.deepEqual(await service.listGroups({ admin_id: '42' }), { error: { code: LOCAL_ILLEGAL_STATE, message: 'unknown admin_id 42' } });
  assert.deepEqual(await service.deleteConsumerGroups({ admin_id: '42', group_ids: ['g'] }), {
    error: { code: LOCAL_ILLEGAL_STATE, message: 'unknown admin_id 42' },
  });
});

test('Close is idempotent: a second Close, and one of an unknown id, are silent successes', async () => {
  const service = newService();
  const id = await mock(service);
  assert.deepEqual(await service.close({ admin_id: id }), {});
  assert.deepEqual(await service.close({ admin_id: id }), {});
  assert.deepEqual(await service.listGroups({ admin_id: id }), { error: { code: LOCAL_ILLEGAL_STATE, message: `unknown admin_id ${id}` } });
});

test('a keyed call puts each key failure in its own entry, read through the ExecutionError cause', async () => {
  const service = newService();
  const id = await mock(service);
  assert.deepEqual(await service.deleteConsumerGroups({ admin_id: id, group_ids: ['g', 'h'] }), {
    entries: [
      { key: { name: 'g' }, error: NOT_IMPLEMENTED },
      { key: { name: 'h' }, error: NOT_IMPLEMENTED },
    ],
  });
  // A repeated partition is one entry.
  assert.deepEqual(
    await service.deleteConsumerGroupOffsets({
      admin_id: id,
      group_id: 'g',
      partitions: [
        { topic: 't', partition: 0 },
        { topic: 't', partition: 0 },
      ],
    }),
    { entries: [{ key: { partition: { topic: 't', partition: 0 } }, error: NOT_IMPLEMENTED }] },
  );
  const update = /** @type {Keyed} */ (
    await service.updateFeatures({ admin_id: id, feature_updates: { 'metadata.version': { max_version_level: 1, upgrade_type: 1 } } })
  );
  assert.deepEqual(update, {
    entries: [
      {
        key: { name: 'metadata.version' },
        error: {
          code: errorFfiId(new InvalidRequestError('x')),
          message: "Invalid update version 1 for feature metadata.version. Can't upgrade above 0",
        },
      },
    ],
  });
});

test('removeMembersFromConsumerGroup without members is removeAll, whose failure is the whole call', async () => {
  const service = newService();
  const id = await mock(service);
  assert.deepEqual(await service.removeMembersFromConsumerGroup({ admin_id: id, group_id: 'g' }), { error: NOT_IMPLEMENTED });
  assert.deepEqual(
    await service.removeMembersFromConsumerGroup({
      admin_id: id,
      group_id: 'g',
      members: { members: [{ group_instance_id: 'i' }] },
      reason: 'r',
    }),
    { entries: [{ key: { name: 'i' }, error: NOT_IMPLEMENTED }] },
  );
});

test('a whole-value call answers its value, or its failure as the response error', async () => {
  const service = newService();
  const id = await mock(service);
  assert.deepEqual(await service.listGroups({ admin_id: id }), { valid: [], listing_errors: [] });
  assert.deepEqual(await service.describeFeatures({ admin_id: id }), {
    metadata: { finalized_features: {}, supported_features: {}, finalized_features_epoch: '123' },
  });
  assert.deepEqual(await service.describeDelegationToken({ admin_id: id }), { tokens: [] });
  assert.deepEqual(
    await service.describeAcls({ admin_id: id, filter: { resource_type: 1, pattern_type: 1, operation: 1, permission_type: 1 } }),
    {
      error: NOT_IMPLEMENTED,
    },
  );
  assert.deepEqual(await service.listTransactions({ admin_id: id, duration_ms: '-1' }), { error: NOT_IMPLEMENTED });
  assert.deepEqual(await service.forceTerminateTransaction({ admin_id: id, transactional_id: 'tx' }), { error: NOT_IMPLEMENTED });
});

test('updateFeatures with no update: the mock answers an empty result', async () => {
  const service = newService();
  const id = await mock(service);
  // A real client refuses an empty map (the response error); the core mock answers it with no entry.
  assert.deepEqual(await service.updateFeatures({ admin_id: id }), { entries: [] });
});

test('a request the server cannot translate is LOCAL_ILLEGAL_ARGUMENT, with the server prefix', async () => {
  const service = newService();
  const id = await mock(service);
  assert.deepEqual(await service.listConsumerGroupOffsets({ admin_id: id, group_specs: [{ group_id: 'g' }, { group_id: 'g' }] }), {
    error: { code: LOCAL_ILLEGAL_ARGUMENT, message: 'js server: group id `g` appears more than once in listConsumerGroupOffsets' },
  });
  assert.deepEqual(await service.abortTransaction({ admin_id: id }), {
    error: { code: LOCAL_ILLEGAL_ARGUMENT, message: 'js server: abort_transaction requires a topic_partition' },
  });
  assert.deepEqual(await service.describeClientQuotas({ admin_id: id, components: [{ entity_type: 'user' }] }), {
    error: { code: LOCAL_ILLEGAL_ARGUMENT, message: 'js server: ClientQuotaFilterComponent has no match_kind (got 0)' },
  });
  assert.deepEqual(await service.describeClientQuotas({ admin_id: id, components: [{ entity_type: 'user', match_kind: 1 }] }), {
    error: { code: LOCAL_ILLEGAL_ARGUMENT, message: 'js server: ClientQuotaFilterComponent with MATCH_KIND_EXACT carries no match_name' },
  });
});

test('the ACL, quota and SCRAM keys cross as the proto keys, absent nullable fields kept absent', async () => {
  const service = newService();
  const id = await mock(service);
  const binding = {
    resource_type: 2,
    resource_name: 't',
    pattern_type: 3,
    principal: 'User:a',
    host: '*',
    operation: 3,
    permission_type: 3,
  };
  assert.deepEqual(await service.createAcls({ admin_id: id, acls: [binding] }), {
    entries: [{ key: { acl_binding: binding }, error: NOT_IMPLEMENTED }],
  });
  const filter = { resource_type: 1, pattern_type: 1, operation: 1, permission_type: 1 };
  assert.deepEqual(await service.deleteAcls({ admin_id: id, filters: [filter] }), {
    entries: [{ key: { acl_binding_filter: filter }, error: NOT_IMPLEMENTED }],
  });
  const entity = { entries: [{ entity_type: 'user' }] };
  const altered = /** @type {Keyed} */ (
    await service.alterClientQuotas({ admin_id: id, entries: [{ entity, ops: [{ key: 'producer_byte_rate' }] }] })
  );
  assert.ok(altered.entries);
  assert.deepEqual(altered.entries[0].key, { client_quota_entity: entity });
  assert.deepEqual(
    await service.alterUserScramCredentials({ admin_id: id, alterations: [{ user: 'u', is_deletion: true, mechanism: 1 }] }),
    { entries: [{ key: { name: 'u' }, error: NOT_IMPLEMENTED }] },
  );
});

test('an op_type outside Java byte range is a malformed request, as an unknown one is', async () => {
  const service = newService();
  const id = await mock(service);
  for (const opType of [9, 300]) {
    const resource = { resource_type: 2, name: 't' };
    assert.deepEqual(
      await service.incrementalAlterConfigs({ admin_id: id, configs: [{ resource, ops: [{ name: 'k', value: 'v', op_type: opType }] }] }),
      { error: { code: LOCAL_ILLEGAL_ARGUMENT, message: `js server: ${opType} is not an AlterConfigOp.OpType` } },
    );
  }
});

test('a SCRAM mechanism code outside Java byte range is UNKNOWN, answered per user', async () => {
  const service = newService();
  const id = await mock(service);
  assert.deepEqual(
    await service.alterUserScramCredentials({ admin_id: id, alterations: [{ user: 'u', is_deletion: true, mechanism: 300 }] }),
    { entries: [{ key: { name: 'u' }, error: NOT_IMPLEMENTED }] },
  );
});

test('a delegation token crosses with the binding own hmacAsBase64String()', async () => {
  const service = newService();
  const id = await mock(service);
  const original = DelegationToken.prototype.hmacAsBase64String;
  DelegationToken.prototype.hmacAsBase64String = () => 'from-the-binding';
  try {
    const response = /** @type {{ token?: { hmac_as_base64: string }, error?: ProtoError }} */ (
      await service.createDelegationToken({ admin_id: id, renewers: [{ principal_type: 'User', name: 'r' }] })
    );
    assert.equal(response.error, undefined, JSON.stringify(response.error));
    assert.ok(response.token);
    assert.equal(response.token.hmac_as_base64, 'from-the-binding');
  } finally {
    DelegationToken.prototype.hmacAsBase64String = original;
  }
});

// ---- the request and value translation, ported from Python's
// test_grpc_admin_translate and .NET's AdminServiceTests ------------------------------

/**
 * Replaces `prototype[name]` for the length of `body`, as the hmac test above
 * replaces a method, so a test can see what the service passes the client or
 * hand it an answer the mock cannot give.
 *
 * @param {object} prototype
 * @param {string} name
 * @param {Function} replacement
 * @param {() => Promise<void>} body
 */
async function replacing(prototype, name, replacement, body) {
  const target = /** @type {Record<string, unknown>} */ (prototype);
  const original = target[name];
  target[name] = replacement;
  try {
    await body();
  } finally {
    target[name] = original;
  }
}

test('a NewTopic whose counts are absent (-1) is Java empty Optional, kept apart from counts given', async () => {
  const { MockAdminClient } = require('@confluentinc/kafka-javascript/admin');
  /** @type {Array<[string, number, number, string]>} */
  const seen = [];
  const original = MockAdminClient.prototype.createTopics;
  await replacing(
    MockAdminClient.prototype,
    'createTopics',
    /** @this {import('@confluentinc/kafka-javascript/admin').MockAdminClient} @param {{ newTopics: Iterable<import('@confluentinc/kafka-javascript/admin').NewTopic> }} args */
    function (args) {
      for (const topic of args.newTopics) seen.push([topic.name(), topic.numPartitions(), topic.replicationFactor(), String(topic)]);
      return original.call(this, /** @type {never} */ (args));
    },
    async () => {
      const service = newService();
      const id = await mock(service);
      const created = /** @type {Keyed} */ (
        await service.createTopics({
          admin_id: id,
          topics: [
            { name: 'absent', num_partitions: -1, replication_factor: -1 },
            { name: 'given', num_partitions: 3, replication_factor: 1 },
          ],
        })
      );
      assert.ok(created.entries);
      assert.deepEqual(
        created.entries.map((e) => [e.key, e.error]),
        [
          [{ name: 'absent' }, undefined],
          [{ name: 'given' }, undefined],
        ],
      );
      const given = /** @type {{ metadata: { num_partitions: number, replication_factor: number } }} */ (created.entries[1].value);
      assert.equal(given.metadata.num_partitions, 3);
      assert.equal(given.metadata.replication_factor, 1);
    },
  );
  // Java's NewTopic#numPartitions and #replicationFactor read an empty
  // Optional as -1 (CreateTopicsRequest.NO_NUM_PARTITIONS, NO_REPLICATION_FACTOR),
  // as they read an explicit -1; only toString and equals tell the two apart,
  // an empty Optional printing "default".
  assert.deepEqual(seen, [
    ['absent', -1, -1, '(name=absent, numPartitions=default, replicationFactor=default, replicasAssignments=null, configs=null)'],
    ['given', 3, 1, '(name=given, numPartitions=3, replicationFactor=1, replicasAssignments=null, configs=null)'],
  ]);
});

test("describeDelegationToken: owners unset is Java's null, every owner; owners present but empty is []", async () => {
  const { MockAdminClient } = require('@confluentinc/kafka-javascript/admin');
  /** @type {Array<unknown>} */
  const seen = [];
  const original = MockAdminClient.prototype.describeDelegationToken;
  await replacing(
    MockAdminClient.prototype,
    'describeDelegationToken',
    /** @this {import('@confluentinc/kafka-javascript/admin').MockAdminClient} @param {import('@confluentinc/kafka-javascript/admin').DescribeDelegationTokenOptions} options */
    function (options) {
      const owners = options.owners();
      seen.push(owners === null ? null : owners.map(String));
      return original.call(this, options);
    },
    async () => {
      const service = newService();
      const id = await mock(service);
      const created = /** @type {{ token?: unknown, error?: ProtoError }} */ (
        await service.createDelegationToken({ admin_id: id, renewers: [{ principal_type: 'User', name: 'r' }] })
      );
      assert.equal(created.error, undefined, JSON.stringify(created.error));
      // Java's mock lists every token for empty owners, and throws
      // NullPointerException for unset (null) ones (options.owners().isEmpty());
      // the core's mock, which this one is, lists every token for null too.
      // The server passes Java's null for unset owners.
      const unset = /** @type {{ tokens: unknown[] }} */ (await service.describeDelegationToken({ admin_id: id }));
      assert.equal(unset.tokens.length, 1);
      const empty = /** @type {{ tokens: unknown[] }} */ (
        await service.describeDelegationToken({ admin_id: id, owners: { principals: [] } })
      );
      assert.equal(empty.tokens.length, 1);
      const byOwner = /** @type {{ tokens: unknown[] }} */ (
        await service.describeDelegationToken({ admin_id: id, owners: { principals: [{ principal_type: 'User', name: 'nobody' }] } })
      );
      assert.equal(byOwner.tokens.length, 0);
    },
  );
  assert.deepEqual(seen, [null, [], ['User:nobody']]);
});

test('Node.isFenced and LogDirDescription.isCordoned cross the wire, both arms', () => {
  const { nodeToProto } = require('../../grpc-server/grpc_translate');
  const { logDirDescriptionToProto } = require('../../grpc-server/grpc_admin_translate');
  const { LogDirDescription } = require('@confluentinc/kafka-javascript/admin');
  const { Node, TopicPartitionMap } = require('@confluentinc/kafka-javascript/common');
  // A real cluster never reports a fenced broker or a cordoned dir to the
  // pooled multilanguage scenarios, so the true arms are pinned here.
  const fenced = /** @type {Record<string, unknown>} */ (
    nodeToProto(new Node({ id: 1, host: 'h', port: 9092, rack: null, isFenced: true }))
  );
  assert.equal(fenced.is_fenced, true);
  assert.equal(fenced.id, 1);
  assert.equal('rack' in fenced, false);
  assert.equal(
    /** @type {Record<string, unknown>} */ (nodeToProto(new Node({ id: 2, host: 'h', port: 9092, rack: 'r' }))).is_fenced,
    false,
  );
  const cordoned = logDirDescriptionToProto(
    new LogDirDescription({ error: null, replicaInfos: new TopicPartitionMap(), totalBytes: 10, usableBytes: 5, isCordoned: true }),
  );
  assert.deepEqual(cordoned, { is_cordoned: true, total_bytes: '10', usable_bytes: '5', replica_infos: [] });
  const plain = logDirDescriptionToProto(new LogDirDescription({ error: null, replicaInfos: new TopicPartitionMap() }));
  assert.deepEqual(plain, { is_cordoned: false, replica_infos: [] });
});

test('alterConsumerGroupOffsets, deleteConsumerGroupOffsets and removeMembersFromConsumerGroup: one failing key is that key error, not the whole call', async () => {
  const { MockAdminClient } = require('@confluentinc/kafka-javascript/admin');
  const { KafkaFuture, TopicPartition } = require('@confluentinc/kafka-javascript/common');
  const { GroupSubscribedToTopicError } = require('@confluentinc/kafka-javascript/common/errors');
  const bad = new TopicPartition({ topic: 't', partition: 1 });
  /** A failed future, as the client's own futures fail. */
  const failed = () => {
    const done = KafkaFuture.completedFuture(null);
    return done.thenApply(() => {
      throw new GroupSubscribedToTopicError('x');
    });
  };
  const result = {
    partitionResult: (/** @type {TopicPartition} */ partition) => (partition.equals(bad) ? failed() : KafkaFuture.completedFuture(null)),
    memberResult: (/** @type {import('@confluentinc/kafka-javascript/admin').MemberToRemove} */ member) =>
      member.groupInstanceId() === 'b' ? failed() : KafkaFuture.completedFuture(null),
    all: () => KafkaFuture.completedFuture(null),
  };
  const code = errorFfiId(new GroupSubscribedToTopicError('x'));
  const expected = {
    entries: [
      { key: { partition: { topic: 't', partition: 0 } } },
      { key: { partition: { topic: 't', partition: 1 } }, error: { code, message: 'x' } },
    ],
  };
  await replacing(
    MockAdminClient.prototype,
    'alterConsumerGroupOffsets',
    () => result,
    async () => {
      await replacing(
        MockAdminClient.prototype,
        'deleteConsumerGroupOffsets',
        () => result,
        async () => {
          const service = newService();
          const id = await mock(service);
          const partitions = [
            { topic: 't', partition: 0 },
            { topic: 't', partition: 1 },
          ];
          assert.deepEqual(
            await service.alterConsumerGroupOffsets({
              admin_id: id,
              group_id: 'g',
              offsets: partitions.map((partition) => ({ partition, offset: { offset: '1' } })),
            }),
            expected,
          );
          assert.deepEqual(await service.deleteConsumerGroupOffsets({ admin_id: id, group_id: 'g', partitions }), expected);
          assert.deepEqual(await service.deleteConsumerGroupOffsets({ admin_id: id, group_id: 'g', partitions: [] }), { entries: [] });
          await replacing(
            MockAdminClient.prototype,
            'removeMembersFromConsumerGroup',
            () => result,
            async () => {
              assert.deepEqual(
                await service.removeMembersFromConsumerGroup({
                  admin_id: id,
                  group_id: 'g',
                  members: { members: [{ group_instance_id: 'a' }, { group_instance_id: 'b' }] },
                }),
                { entries: [{ key: { name: 'a' } }, { key: { name: 'b' }, error: { code, message: 'x' } }] },
              );
            },
          );
        },
      );
    },
  );
});

test('listGroups with a non-Kafka listing error keeps the valid listings and sends the error as -4', async () => {
  const { GroupListing, MockAdminClient } = require('@confluentinc/kafka-javascript/admin');
  const { IllegalStateError } = require('@confluentinc/kafka-javascript');
  const { KafkaFuture } = require('@confluentinc/kafka-javascript/common');
  const listing = new GroupListing({ groupId: 'g1', type: null, protocol: 'consumer', groupState: null });
  const result = {
    valid: () => KafkaFuture.completedFuture([listing]),
    errors: () => KafkaFuture.completedFuture([new IllegalStateError('broker listing failed'), new Error('listing invariant broken')]),
  };
  await replacing(
    MockAdminClient.prototype,
    'listGroups',
    () => result,
    async () => {
      const service = newService();
      const id = await mock(service);
      const response = /** @type {{ valid: Array<{ group_id: string }>, listing_errors: ProtoError[], error?: ProtoError }} */ (
        await service.listGroups({ admin_id: id })
      );
      assert.equal(response.error, undefined);
      assert.deepEqual(
        response.valid.map((l) => l.group_id),
        ['g1'],
      );
      assert.deepEqual(response.listing_errors, [
        { code: -4, message: 'broker listing failed' },
        { code: -4, message: 'js server: Error: listing invariant broken' },
      ]);
    },
  );
});
