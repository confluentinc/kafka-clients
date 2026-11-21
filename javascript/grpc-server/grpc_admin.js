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

// The AdminService of rust/multilanguage-test-server/proto/admin_service.proto
// over the Node.js admin client (dist/admin), the JavaScript twin of
// python/grpc_server_async.py's AdminService. Each handler calls the client as
// a user does: it builds the call's arguments and options through the public
// classes, calls the method, which returns its Result at once, and awaits
// each future the Result holds. A per-key future's failure is the
// ExecutionError's cause, in that key's entry; a whole-call failure (a
// future over the whole call, or a call the client refused) is the
// response's top-level `error`, its entries empty.
//
// Everything runs on the one JS thread, so the admin map needs no lock; ids
// are allocated without an intervening await.

const {
  AbortTransactionOptions,
  AbortTransactionSpec,
  Admin,
  AlterClientQuotasOptions,
  AlterConfigOp,
  AlterConsumerGroupOffsetsOptions,
  AlterConfigsOptions,
  AlterPartitionReassignmentsOptions,
  AlterReplicaLogDirsOptions,
  AlterUserScramCredentialsOptions,
  ConfigEntry,
  CreateAclsOptions,
  CreateDelegationTokenOptions,
  CreatePartitionsOptions,
  CreateTopicsOptions,
  DeleteAclsOptions,
  DeleteConsumerGroupOffsetsOptions,
  DeleteConsumerGroupsOptions,
  DeleteRecordsOptions,
  DeleteTopicsOptions,
  DescribeAclsOptions,
  DescribeClassicGroupsOptions,
  DescribeClientQuotasOptions,
  DescribeClusterOptions,
  DescribeConfigsOptions,
  DescribeConsumerGroupsOptions,
  DescribeDelegationTokenOptions,
  DescribeFeaturesOptions,
  DescribeLogDirsOptions,
  DescribeProducersOptions,
  DescribeReplicaLogDirsOptions,
  DescribeTopicsOptions,
  DescribeTransactionsOptions,
  DescribeUserScramCredentialsOptions,
  ElectLeadersOptions,
  ExpireDelegationTokenOptions,
  FeatureUpdate,
  FenceProducersOptions,
  ListConfigResourcesOptions,
  ListConsumerGroupOffsetsOptions,
  ListConsumerGroupOffsetsSpec,
  ListGroupsOptions,
  ListOffsetsOptions,
  ListPartitionReassignmentsOptions,
  ListTopicsOptions,
  ListTransactionsOptions,
  MemberToRemove,
  MockAdminClient,
  NewPartitionReassignment,
  NewPartitions,
  NewTopic,
  OffsetSpec,
  RecordsToDelete,
  RemoveMembersFromConsumerGroupOptions,
  RenewDelegationTokenOptions,
  ScramCredentialInfo,
  ScramMechanism,
  TerminateTransactionOptions,
  TransactionState,
  UnregisterControllerOptions,
  UpdateFeaturesOptions,
  UserScramCredentialDeletion,
  UserScramCredentialUpsertion,
} = require('../dist/admin');
const {
  ElectionType,
  GroupState,
  GroupType,
  IsolationLevel,
  TopicCollection,
  TopicPartitionMap,
  TopicPartitionReplica,
  TopicPartitionReplicaMap,
  Uuid,
} = require('../dist/common');
const {
  AccessControlEntry,
  AccessControlEntryFilter,
  AclBinding,
  AclBindingFilter,
  AclOperation,
  AclPermissionType,
} = require('../dist/common/acl');
const { ConfigResource, ConfigResourceMap } = require('../dist/common/config');
const { ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent } = require('../dist/common/quota');
const { PatternType, ResourcePattern, ResourcePatternFilter, ResourceType } = require('../dist/common/resource');
const { KafkaPrincipal } = require('../dist/common/security/auth');
const { OffsetAndMetadata } = require('../dist/consumer');
const { int64 } = require('./grpc_translate');
const t = require('./grpc_admin_translate');

/** The proto's OffsetSpec.Kind -> the OffsetSpec factory of that Java variant. */
const OFFSET_SPEC_FACTORIES = new Map([
  [1, () => OffsetSpec.earliest()],
  [2, () => OffsetSpec.latest()],
  [3, () => OffsetSpec.maxTimestamp()],
  [4, () => OffsetSpec.earliestLocal()],
  [5, () => OffsetSpec.latestTiered()],
  [6, () => OffsetSpec.earliestPendingUpload()],
]);
const FOR_TIMESTAMP = 7;

/** admin_service.proto's ClientQuotaMatchKind. */
const MATCH_KIND_EXACT = 1;
const MATCH_KIND_DEFAULT = 2;
const MATCH_KIND_ANY = 3;

function log(message) {
  process.stderr.write(`js server: ${message}\n`);
}

class AdminService {
  #admins = new Map();
  #nextId = 1;

  /** The handlers, by RPC name, for grpc-js's addService. */
  handlers() {
    const unary = (fn) => (call, callback) => {
      fn.call(this, call.request).then(
        (response) => callback(null, response),
        (e) => {
          log(`unexpected failure: ${e && e.stack ? e.stack : e}`);
          callback({ code: 13 /* grpc.status.INTERNAL */, details: `js server: ${e}` });
        },
      );
    };
    /** @type {Record<string, Function>} */
    const handlers = {};
    for (const [rpc, method] of Object.entries(AdminService.RPCS)) {
      // A misspelled method would otherwise surface only when the RPC is called.
      if (typeof this[method] !== 'function') throw new TypeError(`AdminService has no method ${method} for ${rpc}`);
      handlers[rpc] = unary(this[method]);
    }
    return handlers;
  }

  /**
   * Run `fn` with the admin client `request.admin_id` names. An unknown id is
   * the response's error, and so is anything `fn` throws or rejects with, a
   * whole-call failure (python's handlers' `except Exception`).
   */
  async #withAdmin(request, fn) {
    const id = idOf(request.admin_id);
    const admin = this.#admins.get(id);
    if (admin === undefined) return { error: t.unknownAdmin(id) };
    try {
      return await fn(admin);
    } catch (e) {
      return { error: t.adminErrorToProto(e) };
    }
  }

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  async createAdmin(request) {
    const config = request.config || {};
    let admin;
    try {
      // The normative mock-selection rule (CreateAdminRequest): a mock over
      // num_brokers brokers, the first one the controller.
      if (t.adminSelectsMock(config)) {
        const brokers = t.mockBrokers(request.num_brokers === undefined ? 1 : request.num_brokers);
        admin = new MockAdminClient({ brokers, controller: brokers[0] });
      } else {
        admin = Admin.create({ configs: config });
      }
    } catch (e) {
      return { admin_id: 0, error: t.adminConstructorError(e) };
    }
    const id = this.#nextId++;
    this.#admins.set(String(id), admin);
    return { admin_id: id };
  }

  async close(request) {
    const id = idOf(request.admin_id);
    const admin = this.#admins.get(id);
    // Close is idempotent: an unknown id is a silent success.
    if (admin === undefined) return {};
    this.#admins.delete(id);
    try {
      if (request.timeout_ms === undefined) {
        await admin.close();
      } else {
        // int64 milliseconds; past 2^53 (the harness's i64::MAX) no limit.
        const ms = /** @type {bigint} */ (int64(request.timeout_ms));
        await admin.close({ timeout: ms > BigInt(Number.MAX_SAFE_INTEGER) ? Infinity : Number(ms) });
      }
    } catch (e) {
      return { error: t.adminErrorToProto(e) };
    }
    return {};
  }

  /** Close every admin client still open, each bounded by `timeoutMs`. */
  async closeAll(timeoutMs) {
    const admins = [...this.#admins.values()];
    this.#admins.clear();
    await Promise.allSettled(admins.map((a) => a.close({ timeout: timeoutMs })));
  }

  // ── Topics & partitions ───────────────────────────────────────────────────

  async createTopics(request) {
    return this.#withAdmin(request, async (admin) => {
      const options = new CreateTopicsOptions(
        t.optionFields(request, { validateOnly: request.validate_only === true, retryOnQuotaViolation: t.retryOnQuota(request) }),
      );
      const result = admin.createTopics({ newTopics: t.list(request.topics).map(newTopic), options });
      // The topic was created; its accessors fail when the broker did not
      // return its metadata: a value-level error (envelope exception 3).
      const entries = await t.collect(result.values(), t.nameKey, async (_, name) => {
        try {
          const topicId = await result.topicId(name).get();
          const numPartitions = await result.numPartitions(name).get();
          const replicationFactor = await result.replicationFactor(name).get();
          const config = await result.config(name).get();
          return {
            metadata: {
              topic_id: topicId.toString(),
              num_partitions: numPartitions,
              replication_factor: replicationFactor,
              configs: config === null ? [] : config.entries().map(t.configEntryToProto),
            },
          };
        } catch (e) {
          return { error: t.adminErrorToProto(e) };
        }
      });
      return { entries };
    });
  }

  async deleteTopics(request) {
    return this.#withAdmin(request, async (admin) => {
      const options = new DeleteTopicsOptions(t.optionFields(request, { retryOnQuotaViolation: t.retryOnQuota(request) }));
      if (request.topics === 'topic_ids') {
        const topics = TopicCollection.ofTopicIds(t.list(request.topic_ids.values).map((s) => Uuid.fromString(s)));
        const result = admin.deleteTopics({ topics, options });
        return t.keyedVoid(/** @type {Iterable<any>} */ (result.topicIdValues()), (id) => t.topicIdKey(id.toString()));
      }
      const names = request.names === undefined ? [] : t.list(request.names.values);
      const result = admin.deleteTopics({ topics: TopicCollection.ofTopicNames(names), options });
      return t.keyedVoid(/** @type {Iterable<any>} */ (result.topicNameValues()), t.nameKey);
    });
  }

  async listTopics(request) {
    return this.#withAdmin(request, async (admin) => {
      const options = new ListTopicsOptions(t.optionFields(request, { listInternal: request.list_internal === true }));
      // One future for the whole map: no listing fails on its own.
      const listings = await admin.listTopics(options).namesToListings().get();
      return { listings: [...listings.values()].map(t.topicListingToProto) };
    });
  }

  async describeTopics(request) {
    return this.#withAdmin(request, async (admin) => {
      const options = new DescribeTopicsOptions(
        t.optionFields(request, {
          includeAuthorizedOperations: request.include_authorized_operations === true,
          partitionSizeLimitPerResponse: request.partition_size_limit_per_response,
        }),
      );
      if (request.topics === 'topic_ids') {
        const topics = TopicCollection.ofTopicIds(t.list(request.topic_ids.values).map((s) => Uuid.fromString(s)));
        const result = admin.describeTopics({ topics, options });
        const entries = await t.collect(
          /** @type {Iterable<any>} */ (result.topicIdValues()),
          (id) => t.topicIdKey(id.toString()),
          t.topicDescriptionToProto,
        );
        return { entries };
      }
      const names = request.names === undefined ? [] : t.list(request.names.values);
      const result = admin.describeTopics({ topics: TopicCollection.ofTopicNames(names), options });
      const entries = await t.collect(/** @type {Iterable<any>} */ (result.topicNameValues()), t.nameKey, t.topicDescriptionToProto);
      return { entries };
    });
  }

  async createPartitions(request) {
    return this.#withAdmin(request, async (admin) => {
      const options = new CreatePartitionsOptions(
        t.optionFields(request, { validateOnly: request.validate_only === true, retryOnQuotaViolation: t.retryOnQuota(request) }),
      );
      // An absent new_assignments is increaseTo(int); a present one, even
      // empty, increaseTo(int, List<List<Integer>>): two broker requests.
      const newPartitions = new Map();
      for (const p of t.list(request.partitions)) {
        const totalCount = p.total_count === undefined ? 0 : p.total_count;
        newPartitions.set(
          p.topic === undefined ? '' : p.topic,
          p.new_assignments === undefined
            ? NewPartitions.increaseTo(totalCount)
            : NewPartitions.increaseTo({
                totalCount,
                newAssignments: t.list(p.new_assignments.assignments).map((a) => t.list(a.broker_ids)),
              }),
        );
      }
      return t.keyedVoid(admin.createPartitions({ newPartitions, options }).values(), t.nameKey);
    });
  }

  async deleteRecords(request) {
    return this.#withAdmin(request, async (admin) => {
      const records = new TopicPartitionMap();
      for (const r of t.list(request.records)) {
        records.set(t.tp(r.partition), RecordsToDelete.beforeOffset(int64(r.before_offset) ?? 0n));
      }
      const result = admin.deleteRecords({ recordsToDelete: records, options: new DeleteRecordsOptions(t.optionFields(request)) });
      const entries = await t.collect(result.lowWatermarks(), t.partitionKey, (deleted) => ({
        low_watermark: deleted.lowWatermark().toString(),
      }));
      return { entries };
    });
  }

  // ── Cluster, configs & log dirs ───────────────────────────────────────────

  async describeCluster(request) {
    return this.#withAdmin(request, async (admin) => {
      const options = new DescribeClusterOptions(
        t.optionFields(request, {
          includeAuthorizedOperations: request.include_authorized_operations === true,
          includeFencedBrokers: request.include_fenced_brokers === true,
        }),
      );
      // Four futures over one cluster: any failure is a whole-call failure.
      const result = admin.describeCluster(options);
      const clusterId = await result.clusterId().get();
      const nodes = await result.nodes().get();
      let controller = await result.controller().get();
      // No controller: Java's null, or the no-node sentinel's negative id.
      if (controller !== null && controller.id < 0) controller = null;
      const operations = await result.authorizedOperations().get();
      /** @type {Record<string, unknown>} */
      const description = { cluster_id: clusterId === null ? '' : clusterId, nodes: t.nodes(nodes) };
      if (controller !== null) description.controller = t.nodes([controller])[0];
      const ops = t.aclOperations(operations);
      if (ops !== undefined) description.authorized_operations = ops;
      return { description };
    });
  }

  async describeConfigs(request) {
    return this.#withAdmin(request, async (admin) => {
      const options = new DescribeConfigsOptions(
        t.optionFields(request, {
          includeSynonyms: request.include_synonyms === true,
          includeDocumentation: request.include_documentation === true,
        }),
      );
      const result = admin.describeConfigs({ resources: t.list(request.resources).map(configResource), options });
      const entries = await t.collect(result.values(), t.configResourceKey, (config) => ({
        entries: config.entries().map(t.fullConfigEntryToProto),
      }));
      return { entries };
    });
  }

  async incrementalAlterConfigs(request) {
    return this.#withAdmin(request, async (admin) => {
      const options = new AlterConfigsOptions(t.optionFields(request, { validateOnly: request.validate_only === true }));
      const configs = new ConfigResourceMap();
      for (const c of t.list(request.configs)) {
        // An absent value is Java's null, which a DELETE carries.
        configs.set(
          configResource(c.resource),
          t.list(c.ops).map(
            (op) =>
              new AlterConfigOp({
                configEntry: new ConfigEntry({
                  name: op.name === undefined ? '' : op.name,
                  value: op.value === undefined ? null : op.value,
                }),
                operationType: opType(op.op_type === undefined ? 0 : op.op_type),
              }),
          ),
        );
      }
      return t.keyedVoid(admin.incrementalAlterConfigs({ configs, options }).values(), t.configResourceKey);
    });
  }

  async listConfigResources(request) {
    return this.#withAdmin(request, async (admin) => {
      // An empty list is Java's empty Set: every supported type. An unknown
      // code is Type.UNKNOWN, as Java's forId makes it.
      const configResourceTypes = t.list(request.resource_types).map(resourceType);
      const result = admin.listConfigResources({ configResourceTypes, options: new ListConfigResourcesOptions(t.optionFields(request)) });
      const resources = await result.all().get();
      return { resources: resources.map(t.configResourceToProto) };
    });
  }

  async describeLogDirs(request) {
    return this.#withAdmin(request, async (admin) => {
      const result = admin.describeLogDirs({
        brokers: t.list(request.brokers),
        options: new DescribeLogDirsOptions(t.optionFields(request)),
      });
      const entries = await t.collect(result.descriptions(), t.brokerIdKey, (byPath) => {
        /** @type {Record<string, unknown>} */
        const logDirs = {};
        for (const [path, description] of byPath) logDirs[path] = t.logDirDescriptionToProto(description);
        return { log_dirs: logDirs };
      });
      return { entries };
    });
  }

  async alterReplicaLogDirs(request) {
    return this.#withAdmin(request, async (admin) => {
      const replicaAssignment = new TopicPartitionReplicaMap();
      for (const a of t.list(request.assignments)) replicaAssignment.set(replica(a.replica), a.log_dir === undefined ? '' : a.log_dir);
      const result = admin.alterReplicaLogDirs({ replicaAssignment, options: new AlterReplicaLogDirsOptions(t.optionFields(request)) });
      return t.keyedVoid(result.values(), t.replicaKey);
    });
  }

  async describeReplicaLogDirs(request) {
    return this.#withAdmin(request, async (admin) => {
      const result = admin.describeReplicaLogDirs({
        replicas: t.list(request.replicas).map(replica),
        options: new DescribeReplicaLogDirsOptions(t.optionFields(request)),
      });
      return { entries: await t.collect(result.values(), t.replicaKey, t.replicaLogDirInfoToProto) };
    });
  }

  // ── Elections, reassignments & offsets ────────────────────────────────────

  async electLeaders(request) {
    return this.#withAdmin(request, async (admin) => {
      const electionType = enumOf(() => ElectionType.valueOf({ value: request.election_type ?? 0 }), 'ElectionType', request.election_type);
      // Absent partitions is Java's null Set: every partition.
      const partitions = request.partitions === undefined ? null : t.list(request.partitions.partitions).map(t.tp);
      const result = admin.electLeaders({ electionType, partitions, options: new ElectLeadersOptions(t.optionFields(request)) });
      // One future for the whole map; each value is Java's Optional<Throwable>.
      return t.voidEntries(await result.partitions().get(), t.partitionKey);
    });
  }

  async alterPartitionReassignments(request) {
    return this.#withAdmin(request, async (admin) => {
      const options = new AlterPartitionReassignmentsOptions(
        t.optionFields(request, {
          // Java's default is true.
          allowReplicationFactorChange:
            request.allow_replication_factor_change === undefined ? true : request.allow_replication_factor_change,
        }),
      );
      // An absent reassignment is Java's empty Optional: cancel it.
      const reassignments = new TopicPartitionMap();
      for (const r of t.list(request.reassignments)) {
        reassignments.set(
          t.tp(r.partition),
          r.reassignment === undefined ? null : new NewPartitionReassignment(t.list(r.reassignment.target_replicas)),
        );
      }
      return t.keyedVoid(admin.alterPartitionReassignments({ reassignments, options }).values(), t.partitionKey);
    });
  }

  async listPartitionReassignments(request) {
    return this.#withAdmin(request, async (admin) => {
      const options = new ListPartitionReassignmentsOptions(t.optionFields(request));
      // Absent partitions is Java's listPartitionReassignments(options): every reassignment.
      const result =
        request.partitions === undefined
          ? admin.listPartitionReassignments({ options })
          : admin.listPartitionReassignments({ partitions: t.list(request.partitions.partitions).map(t.tp), options });
      const reassignments = await result.reassignments().get();
      return {
        reassignments: [...reassignments].map(([partition, reassignment]) => ({
          partition: t.tpToProto(partition),
          reassignment: {
            replicas: reassignment.replicas(),
            adding_replicas: reassignment.addingReplicas(),
            removing_replicas: reassignment.removingReplicas(),
          },
        })),
      };
    });
  }

  async listOffsets(request) {
    return this.#withAdmin(request, async (admin) => {
      const specs = new TopicPartitionMap();
      for (const p of t.list(request.specs)) specs.set(t.tp(p.partition), offsetSpec(p));
      const isolationLevel = enumOf(() => IsolationLevel.forId(request.isolation_level ?? 0), 'IsolationLevel', request.isolation_level);
      const options = new ListOffsetsOptions(isolationLevel);
      if (request.timeout_ms !== undefined) options.timeoutMs(request.timeout_ms);
      const result = admin.listOffsets({ topicPartitionOffsets: specs, options });
      const entries = await t.collect(
        [...specs.keys()].map((partition) => [partition, result.partitionResult(partition)]),
        t.partitionKey,
        t.listOffsetsInfoToProto,
      );
      return { entries };
    });
  }

  // ── Groups ────────────────────────────────────────────────────────────────

  async listGroups(request) {
    return this.#withAdmin(request, async (admin) => {
      // An empty filter is Java's default, every group: set only what the request names.
      const states = t.list(request.group_states);
      const protocolTypes = t.list(request.protocol_types);
      const types = t.list(request.types);
      const options = new ListGroupsOptions(
        t.optionFields(request, {
          groupStates: states.length > 0 ? states.map((name) => GroupState.parse(name)) : undefined,
          protocolTypes: protocolTypes.length > 0 ? protocolTypes : undefined,
          types: types.length > 0 ? types.map((name) => GroupType.parse(name)) : undefined,
        }),
      );
      // One future over the whole listing, split into valid() and an unkeyed errors().
      const result = admin.listGroups(options);
      const valid = await result.valid().get();
      const errors = await result.errors().get();
      return {
        valid: valid.map(t.groupListingToProto),
        // A null entry in errors() is skipped, as the C++ and Python servers skip it.
        listing_errors: errors.filter((e) => e !== null && e !== undefined).map(t.adminErrorToProto),
      };
    });
  }

  async describeConsumerGroups(request) {
    return this.#withAdmin(request, async (admin) => {
      const options = new DescribeConsumerGroupsOptions(
        t.optionFields(request, { includeAuthorizedOperations: request.include_authorized_operations === true }),
      );
      const result = admin.describeConsumerGroups({ groupIds: t.list(request.group_ids), options });
      const entries = await t.collect(result.describedGroups(), t.nameKey, (description, groupId) => {
        if (description === null)
          throw new t.EntryError(`describeConsumerGroups reported neither a description nor an error for '${groupId}'`);
        return t.consumerGroupDescriptionToProto(description);
      });
      return { entries };
    });
  }

  async describeClassicGroups(request) {
    return this.#withAdmin(request, async (admin) => {
      const options = new DescribeClassicGroupsOptions(
        t.optionFields(request, { includeAuthorizedOperations: request.include_authorized_operations === true }),
      );
      const result = admin.describeClassicGroups({ groupIds: t.list(request.group_ids), options });
      const entries = await t.collect(result.describedGroups(), t.nameKey, (description, groupId) => {
        if (description === null)
          throw new t.EntryError(`describeClassicGroups reported neither a description nor an error for '${groupId}'`);
        return t.classicGroupDescriptionToProto(description);
      });
      return { entries };
    });
  }

  async listConsumerGroupOffsets(request) {
    return this.#withAdmin(request, async (admin) => {
      // An absent topic_partitions is Java's unset collection (every partition
      // the group committed); present-but-empty selects nothing. A repeated
      // group id is refused, as the C entry point refuses it: a Map would let
      // the second replace the first.
      const groupSpecs = new Map();
      for (const p of t.list(request.group_specs)) {
        const groupId = p.group_id ?? '';
        if (groupSpecs.has(groupId))
          throw new t.AdminRequestError(`group id \`${groupId}\` appears more than once in listConsumerGroupOffsets`);
        groupSpecs.set(
          groupId,
          p.topic_partitions === undefined
            ? new ListConsumerGroupOffsetsSpec()
            : new ListConsumerGroupOffsetsSpec({ topicPartitions: t.list(p.topic_partitions.partitions).map(t.tp) }),
        );
      }
      const options = new ListConsumerGroupOffsetsOptions(t.optionFields(request, { requireStable: request.require_stable === true }));
      const result = admin.listConsumerGroupOffsets({ groupSpecs, options });
      const entries = await t.collect(
        [...groupSpecs.keys()].map((groupId) => [groupId, result.partitionsToOffsetAndMetadata(groupId)]),
        t.nameKey,
        (offsets, groupId) => {
          if (offsets === null) throw new t.EntryError(`listConsumerGroupOffsets reported neither offsets nor an error for '${groupId}'`);
          return t.groupOffsetsToProto(offsets);
        },
      );
      return { entries };
    });
  }

  async alterConsumerGroupOffsets(request) {
    return this.#withAdmin(request, async (admin) => {
      // An absent leader_epoch is Java's empty Optional, not epoch 0.
      const offsets = new TopicPartitionMap();
      for (const p of t.list(request.offsets)) {
        const o = p.offset || {};
        offsets.set(
          t.tp(p.partition),
          new OffsetAndMetadata({
            offset: int64(o.offset) ?? 0n,
            leaderEpoch: o.leader_epoch === undefined ? null : o.leader_epoch,
            metadata: o.metadata ?? '',
          }),
        );
      }
      const result = admin.alterConsumerGroupOffsets({
        groupId: request.group_id ?? '',
        offsets,
        options: new AlterConsumerGroupOffsetsOptions(t.optionFields(request)),
      });
      return perKey(result, [...offsets.keys()], (partition) => result.partitionResult(partition), t.partitionKey);
    });
  }

  async deleteConsumerGroupOffsets(request) {
    return this.#withAdmin(request, async (admin) => {
      const partitions = t.list(request.partitions).map(t.tp);
      const result = admin.deleteConsumerGroupOffsets({
        groupId: request.group_id ?? '',
        partitions,
        options: new DeleteConsumerGroupOffsetsOptions(t.optionFields(request)),
      });
      // One entry per partition, a repeated one once.
      const unique = new TopicPartitionMap(partitions.map((partition) => [partition, true]));
      return perKey(result, [...unique.keys()], (partition) => result.partitionResult(partition), t.partitionKey);
    });
  }

  async deleteConsumerGroups(request) {
    return this.#withAdmin(request, async (admin) => {
      const result = admin.deleteConsumerGroups({
        groupIds: t.list(request.group_ids),
        options: new DeleteConsumerGroupsOptions(t.optionFields(request)),
      });
      return t.keyedVoid(result.deletedGroups(), t.nameKey);
    });
  }

  async removeMembersFromConsumerGroup(request) {
    return this.#withAdmin(request, async (admin) => {
      // Absent members is Java's no-argument options (removeAll); a present
      // but empty list is the collection constructor, which Java refuses.
      const members =
        request.members === undefined ? null : t.list(request.members.members).map((m) => new MemberToRemove(m.group_instance_id ?? ''));
      const options = members === null ? new RemoveMembersFromConsumerGroupOptions() : new RemoveMembersFromConsumerGroupOptions(members);
      if (request.reason !== undefined) options.reason(request.reason);
      if (request.timeout_ms !== undefined) options.timeoutMs(request.timeout_ms);
      const result = admin.removeMembersFromConsumerGroup({ groupId: request.group_id ?? '', options });
      // removeAll has no per-member future: an empty result, whole-call error only.
      // Keyed by group.instance.id, a repeated one once.
      const byId = new Map();
      for (const member of members ?? []) if (!byId.has(member.groupInstanceId())) byId.set(member.groupInstanceId(), member);
      return perKey(
        result,
        [...byId.values()],
        (member) => result.memberResult(member),
        (member) => t.nameKey(member.groupInstanceId()),
      );
    });
  }

  // ── ACLs, quotas, SCRAM, delegation tokens & features ─────────────────────

  async createAcls(request) {
    return this.#withAdmin(request, async (admin) => {
      const result = admin.createAcls({
        acls: t.list(request.acls).map(aclBinding),
        options: new CreateAclsOptions(t.optionFields(request)),
      });
      return t.keyedVoid(result.values(), t.aclBindingKey);
    });
  }

  async describeAcls(request) {
    return this.#withAdmin(request, async (admin) => {
      // One future for the whole call: an empty list is a successful "nothing matched".
      const result = admin.describeAcls({
        filter: aclBindingFilter(request.filter),
        options: new DescribeAclsOptions(t.optionFields(request)),
      });
      const bindings = await result.values().get();
      return { acls: bindings.map(t.aclBindingToProto) };
    });
  }

  async deleteAcls(request) {
    return this.#withAdmin(request, async (admin) => {
      const result = admin.deleteAcls({
        filters: t.list(request.filters).map(aclBindingFilter),
        options: new DeleteAclsOptions(t.optionFields(request)),
      });
      // The filter's future can succeed while a matched ACL failed to delete: both levels cross.
      return { entries: await t.collect(result.values(), t.aclBindingFilterKey, t.filterResultsToProto) };
    });
  }

  async describeClientQuotas(request) {
    return this.#withAdmin(request, async (admin) => {
      const components = t.list(request.components).map(quotaFilterComponent);
      const filter = request.strict === true ? ClientQuotaFilter.containsOnly(components) : ClientQuotaFilter.contains(components);
      const result = admin.describeClientQuotas({ filter, options: new DescribeClientQuotasOptions(t.optionFields(request)) });
      const entities = await result.entities().get();
      // A removed quota is absent from the inner map, not reported as zero.
      return {
        entities: [...entities].map(([entity, values]) => ({
          entity: t.quotaEntityToProto(entity),
          values: [...values].map(([key, value]) => ({ key, value })),
        })),
      };
    });
  }

  async alterClientQuotas(request) {
    return this.#withAdmin(request, async (admin) => {
      // An op with no value is Java's null Double, which removes the quota.
      const entries = t.list(request.entries).map(
        (p) =>
          new ClientQuotaAlteration({
            entity: quotaEntity(p.entity),
            ops: t
              .list(p.ops)
              .map((op) => new ClientQuotaAlteration.Op({ key: op.key ?? '', value: op.value === undefined ? null : op.value })),
          }),
      );
      const options = new AlterClientQuotasOptions(t.optionFields(request, { validateOnly: request.validate_only === true }));
      return t.keyedVoid(admin.alterClientQuotas({ entries, options }).values(), t.quotaEntityKey);
    });
  }

  async describeUserScramCredentials(request) {
    return this.#withAdmin(request, async (admin) => {
      // An empty users list is Java's no-argument overload: every user.
      const users = t.list(request.users);
      const result = admin.describeUserScramCredentials({
        users: users.length > 0 ? users : null,
        options: new DescribeUserScramCredentialsOptions(t.optionFields(request)),
      });
      // Java's three views composed as every backend composes them: all()
      // first; when it fails, users() plus description(user). all() treats a
      // user with no credential (RESOURCE_NOT_FOUND) as a success.
      const toValue = (description) => ({
        name: description.name,
        credential_infos: description.credentialInfos.map((info) => ({
          mechanism: info.mechanism().type(),
          iterations: info.iterations(),
        })),
      });
      let all;
      try {
        all = await result.all().get();
      } catch (allError) {
        // users() failing is the response itself failing: the whole call's error.
        const names = await result.users().get();
        if (names.length === 0) throw allError;
        return {
          entries: await t.collect(
            names.map((name) => [name, result.description(name)]),
            t.nameKey,
            toValue,
          ),
        };
      }
      return { entries: [...all].map(([name, description]) => ({ key: t.nameKey(name), value: toValue(description) })) };
    });
  }

  async alterUserScramCredentials(request) {
    return this.#withAdmin(request, async (admin) => {
      const result = admin.alterUserScramCredentials({
        alterations: t.list(request.alterations).map(scramAlteration),
        options: new AlterUserScramCredentialsOptions(t.optionFields(request)),
      });
      return t.keyedVoid(result.values(), t.nameKey);
    });
  }

  async createDelegationToken(request) {
    return this.#withAdmin(request, async (admin) => {
      // An absent owner leaves Java's field null: the requester owns the token.
      const options = new CreateDelegationTokenOptions(
        t.optionFields(request, {
          renewers: t.list(request.renewers).map(principal),
          owner: request.owner === undefined ? undefined : principal(request.owner),
          maxLifetimeMs: int64(request.max_lifetime_ms) ?? 0n,
        }),
      );
      const token = await admin.createDelegationToken(options).delegationToken().get();
      return { token: t.delegationTokenToProto(token) };
    });
  }

  async renewDelegationToken(request) {
    return this.#withAdmin(request, async (admin) => {
      const options = new RenewDelegationTokenOptions(
        t.optionFields(request, { renewTimePeriodMs: int64(request.renew_time_period_ms) ?? 0n }),
      );
      const expiry = await admin
        .renewDelegationToken({ hmac: request.hmac ?? Buffer.alloc(0), options })
        .expiryTimestamp()
        .get();
      return { expiry_timestamp_ms: expiry.toString() };
    });
  }

  async expireDelegationToken(request) {
    return this.#withAdmin(request, async (admin) => {
      const options = new ExpireDelegationTokenOptions(
        t.optionFields(request, { expiryTimePeriodMs: int64(request.expiry_time_period_ms) ?? 0n }),
      );
      const expiry = await admin
        .expireDelegationToken({ hmac: request.hmac ?? Buffer.alloc(0), options })
        .expiryTimestamp()
        .get();
      return { expiry_timestamp_ms: expiry.toString() };
    });
  }

  async describeDelegationToken(request) {
    return this.#withAdmin(request, async (admin) => {
      // Absent owners is Java's unset filter (every token); present-but-empty an empty filter.
      const options = new DescribeDelegationTokenOptions(
        t.optionFields(request, { owners: request.owners === undefined ? undefined : t.list(request.owners.principals).map(principal) }),
      );
      const tokens = await admin.describeDelegationToken(options).delegationTokens().get();
      return { tokens: tokens.map(t.delegationTokenToProto) };
    });
  }

  async describeFeatures(request) {
    return this.#withAdmin(request, async (admin) => {
      // An absent node_id is Java's empty OptionalInt; node 0 is a legal broker.
      const options = new DescribeFeaturesOptions(t.optionFields(request, { nodeId: request.node_id }));
      const metadata = await admin.describeFeatures(options).featureMetadata().get();
      return { metadata: t.featureMetadataToProto(metadata) };
    });
  }

  async updateFeatures(request) {
    return this.#withAdmin(request, async (admin) => {
      // An empty map is passed on: the client refuses it (Java's
      // IllegalArgumentException), which is the response's error.
      const featureUpdates = new Map();
      for (const [feature, update] of Object.entries(request.feature_updates || {})) {
        featureUpdates.set(
          feature,
          new FeatureUpdate({
            maxVersionLevel: update.max_version_level ?? 0,
            upgradeType: orUnknown(() => FeatureUpdate.UpgradeType.fromCode(update.upgrade_type ?? 0), FeatureUpdate.UpgradeType.UNKNOWN),
          }),
        );
      }
      const options = new UpdateFeaturesOptions(t.optionFields(request, { validateOnly: request.validate_only === true }));
      return t.keyedVoid(admin.updateFeatures({ featureUpdates, options }).values(), t.nameKey);
    });
  }

  // ── Producers & transactions ──────────────────────────────────────────────

  async describeProducers(request) {
    return this.#withAdmin(request, async (admin) => {
      // An absent broker_id is Java's empty OptionalInt; broker 0 is legal.
      const partitions = t.list(request.partitions).map(t.tp);
      const options = new DescribeProducersOptions(t.optionFields(request, { brokerId: request.broker_id }));
      const result = admin.describeProducers({ partitions, options });
      const unique = new TopicPartitionMap(partitions.map((partition) => [partition, true]));
      const entries = await t.collect(
        [...unique.keys()].map((partition) => [partition, result.partitionResult(partition)]),
        t.partitionKey,
        (state, partition) => {
          if (state === null) {
            throw new t.EntryError(`describeProducers reported neither state nor error for ${partition.topic}-${partition.partition}`);
          }
          return { active_producers: t.list(state.activeProducers()).map(t.producerStateToProto) };
        },
      );
      return { entries };
    });
  }

  async describeTransactions(request) {
    return this.#withAdmin(request, async (admin) => {
      const transactionalIds = [...new Set(t.list(request.transactional_ids))];
      const result = admin.describeTransactions({
        transactionalIds: t.list(request.transactional_ids),
        options: new DescribeTransactionsOptions(t.optionFields(request)),
      });
      const entries = await t.collect(
        transactionalIds.map((id) => [id, result.description(id)]),
        t.nameKey,
        (description, id) => {
          if (description === null) throw new t.EntryError(`describeTransactions reported neither description nor error for '${id}'`);
          return t.transactionDescriptionToProto(description);
        },
      );
      return { entries };
    });
  }

  async abortTransaction(request) {
    return this.#withAdmin(request, async (admin) => {
      // Java's AbortTransactionSpec holds a TopicPartition: an absent one is a malformed request.
      if (request.topic_partition === undefined) throw new t.AdminRequestError('abort_transaction requires a topic_partition');
      const spec = new AbortTransactionSpec({
        topicPartition: t.tp(request.topic_partition),
        producerId: int64(request.producer_id) ?? 0n,
        producerEpoch: request.producer_epoch ?? 0,
        coordinatorEpoch: request.coordinator_epoch ?? 0,
      });
      await admin
        .abortTransaction({ spec, options: new AbortTransactionOptions(t.optionFields(request)) })
        .all()
        .get();
      return {};
    });
  }

  async forceTerminateTransaction(request) {
    return this.#withAdmin(request, async (admin) => {
      const result = admin.forceTerminateTransaction({
        transactionalId: request.transactional_id ?? '',
        options: new TerminateTransactionOptions(t.optionFields(request)),
      });
      await result.result().get();
      return {};
    });
  }

  async listTransactions(request) {
    return this.#withAdmin(request, async (admin) => {
      // Empty states and producer ids are Java's default, every one; a
      // negative duration no duration filter.
      const options = new ListTransactionsOptions(
        t.optionFields(request, {
          filteredStates: t.list(request.states).map((name) => TransactionState.parse(name)),
          filteredProducerIds: t.list(request.producer_ids).map((id) => BigInt(id)),
          filteredDuration: int64(request.duration_ms) ?? 0n,
          filteredTransactionalIdPattern: request.transactional_id_pattern,
        }),
      );
      // byBrokerId(), the one view that keeps a per-broker error; a failure
      // of the broker discovery is the whole call's.
      const byBroker = await admin.listTransactions(options).byBrokerId().get();
      return { entries: await t.collect(byBroker, t.brokerIdKey, (listings) => ({ listings: listings.map(t.transactionListingToProto) })) };
    });
  }

  async fenceProducers(request) {
    return this.#withAdmin(request, async (admin) => {
      const transactionalIds = [...new Set(t.list(request.transactional_ids))];
      const result = admin.fenceProducers({
        transactionalIds: t.list(request.transactional_ids),
        options: new FenceProducersOptions(t.optionFields(request)),
      });
      const fenced = result.fencedProducers();
      const entries = [];
      for (const id of transactionalIds) {
        /** @type {Record<string, unknown>} */
        const entry = { key: t.nameKey(id) };
        try {
          await fenced.get(id).get();
          const producerId = await result.producerId(id).get();
          const epoch = await result.epochId(id).get();
          entry.value = { producer_id: producerId.toString(), epoch };
        } catch (e) {
          entry.error = t.adminErrorToProto(e);
        }
        entries.push(entry);
      }
      return { entries };
    });
  }

  async unregisterController(request) {
    return this.#withAdmin(request, async (admin) => {
      const result = admin.unregisterController({
        controllerId: request.controller_id ?? 0,
        options: new UnregisterControllerOptions(t.optionFields(request)),
      });
      await result.all().get();
      return {};
    });
  }
}

/** Each RPC of the service and the method that serves it. */
AdminService.RPCS = Object.freeze({
  CreateAdmin: 'createAdmin',
  Close: 'close',
  CreateTopics: 'createTopics',
  DeleteTopics: 'deleteTopics',
  ListTopics: 'listTopics',
  DescribeTopics: 'describeTopics',
  CreatePartitions: 'createPartitions',
  DeleteRecords: 'deleteRecords',
  DescribeCluster: 'describeCluster',
  DescribeConfigs: 'describeConfigs',
  IncrementalAlterConfigs: 'incrementalAlterConfigs',
  ListConfigResources: 'listConfigResources',
  DescribeLogDirs: 'describeLogDirs',
  AlterReplicaLogDirs: 'alterReplicaLogDirs',
  DescribeReplicaLogDirs: 'describeReplicaLogDirs',
  ElectLeaders: 'electLeaders',
  AlterPartitionReassignments: 'alterPartitionReassignments',
  ListPartitionReassignments: 'listPartitionReassignments',
  ListOffsets: 'listOffsets',
  ListGroups: 'listGroups',
  DescribeConsumerGroups: 'describeConsumerGroups',
  DescribeClassicGroups: 'describeClassicGroups',
  ListConsumerGroupOffsets: 'listConsumerGroupOffsets',
  AlterConsumerGroupOffsets: 'alterConsumerGroupOffsets',
  DeleteConsumerGroupOffsets: 'deleteConsumerGroupOffsets',
  DeleteConsumerGroups: 'deleteConsumerGroups',
  RemoveMembersFromConsumerGroup: 'removeMembersFromConsumerGroup',
  CreateAcls: 'createAcls',
  DescribeAcls: 'describeAcls',
  DeleteAcls: 'deleteAcls',
  DescribeClientQuotas: 'describeClientQuotas',
  AlterClientQuotas: 'alterClientQuotas',
  DescribeUserScramCredentials: 'describeUserScramCredentials',
  AlterUserScramCredentials: 'alterUserScramCredentials',
  CreateDelegationToken: 'createDelegationToken',
  RenewDelegationToken: 'renewDelegationToken',
  ExpireDelegationToken: 'expireDelegationToken',
  DescribeDelegationToken: 'describeDelegationToken',
  DescribeFeatures: 'describeFeatures',
  UpdateFeatures: 'updateFeatures',
  DescribeProducers: 'describeProducers',
  DescribeTransactions: 'describeTransactions',
  AbortTransaction: 'abortTransaction',
  ForceTerminateTransaction: 'forceTerminateTransaction',
  ListTransactions: 'listTransactions',
  FenceProducers: 'fenceProducers',
  UnregisterController: 'unregisterController',
});

/** A uint64 admin id (a decimal string under `longs: String`) as a map key. */
function idOf(adminId) {
  return adminId === undefined ? '0' : String(adminId);
}

/** A proto NewTopic as a NewTopic (python's `_admin_new_topics`). */
function newTopic(p) {
  const name = p.name === undefined ? '' : p.name;
  const assignments = t.list(p.replicas_assignments);
  let topic;
  if (assignments.length > 0) {
    topic = new NewTopic({ name, replicasAssignments: new Map(assignments.map((a) => [a.partition ?? 0, t.list(a.broker_ids)])) });
  } else {
    // -1 is the wire's "absent", Java's Optional.empty().
    const numPartitions = p.num_partitions === undefined ? 0 : p.num_partitions;
    const replicationFactor = p.replication_factor === undefined ? 0 : p.replication_factor;
    topic = new NewTopic({
      name,
      numPartitions: numPartitions < 0 ? null : numPartitions,
      replicationFactor: replicationFactor < 0 ? null : replicationFactor,
    });
  }
  if (p.configs !== undefined && Object.keys(p.configs).length > 0) topic.configs(p.configs);
  return topic;
}

/** A proto ConfigResource as a ConfigResource; an unknown type code is Type.UNKNOWN. */
function configResource(p) {
  const r = p || {};
  return new ConfigResource({ type: resourceType(r.resource_type ?? 0), name: r.name === undefined ? '' : r.name });
}

/**
 * A ConfigResource.Type by its wire id: an unknown one is Type.UNKNOWN, as
 * Java's forId makes it, one outside Java's byte included.
 */
function resourceType(id) {
  try {
    return ConfigResource.Type.forId(id);
  } catch {
    return ConfigResource.Type.UNKNOWN;
  }
}

/** An AlterConfigOp.OpType by its wire id; an unknown one is a malformed request. */
function opType(id) {
  // forId throws a RangeError outside Java's byte: a malformed request too.
  let type;
  try {
    type = AlterConfigOp.OpType.forId(id);
  } catch {
    type = null;
  }
  if (type === null) throw new t.AdminRequestError(`${id} is not an AlterConfigOp.OpType`);
  return type;
}

/** The constant `lookup` returns, or a malformed request when it throws (no such constant). */
function enumOf(lookup, name, value) {
  try {
    return lookup();
  } catch {
    throw new t.AdminRequestError(`${value} is not a ${name}`);
  }
}

/** A proto TopicPartitionReplica as a TopicPartitionReplica. */
function replica(p) {
  const r = p || {};
  return new TopicPartitionReplica({ topic: r.topic ?? '', partition: r.partition ?? 0, brokerId: r.broker_id ?? 0 });
}

/**
 * A proto OffsetSpecEntry's spec as an OffsetSpec, through the named factory
 * of its variant (python's `_admin_offset_specs`). A dropped kind and a
 * FOR_TIMESTAMP with no timestamp are malformed requests, not a defaulted
 * variant.
 */
function offsetSpec(p) {
  const spec = p.spec || {};
  const partition = p.partition || {};
  const label = `(${JSON.stringify(partition.topic ?? '')}, ${partition.partition ?? 0})`;
  const kind = spec.kind ?? 0;
  if (kind === FOR_TIMESTAMP) {
    if (spec.timestamp === undefined) throw new t.AdminRequestError(`OffsetSpec FOR_TIMESTAMP for ${label} carries no timestamp`);
    return OffsetSpec.forTimestamp(int64(spec.timestamp));
  }
  const factory = OFFSET_SPEC_FACTORIES.get(kind);
  if (factory === undefined) throw new t.AdminRequestError(`OffsetSpec for ${label} has kind ${kind}, not a Java OffsetSpec variant`);
  return factory();
}

/**
 * One VoidResultEntry per key, each from its own future (`partitionResult` /
 * `memberResult`), as python's `_per_key` resolves them. With no key there is
 * only `all()`, whose failure is the whole call's error and whose success an
 * empty result.
 */
async function perKey(result, keys, futureOf, keyOf) {
  if (keys.length === 0) {
    await result.all().get();
    return { entries: [] };
  }
  return t.keyedVoid(
    keys.map((key) => [key, futureOf(key)]),
    keyOf,
  );
}

/** The constant `lookup` returns, or `unknown` when it throws: an unknown wire code is the enum's UNKNOWN, as Java's `fromCode` makes it. */
function orUnknown(lookup, unknown) {
  try {
    return lookup();
  } catch {
    return unknown;
  }
}

/** A proto AclBinding as an AclBinding; an unknown code is the enum's UNKNOWN. */
function aclBinding(p) {
  return new AclBinding({
    pattern: new ResourcePattern({
      resourceType: orUnknown(() => ResourceType.fromCode(p.resource_type ?? 0), ResourceType.UNKNOWN),
      name: p.resource_name ?? '',
      patternType: orUnknown(() => PatternType.fromCode(p.pattern_type ?? 0), PatternType.UNKNOWN),
    }),
    entry: new AccessControlEntry({
      principal: p.principal ?? '',
      host: p.host ?? '',
      operation: orUnknown(() => AclOperation.fromCode(p.operation ?? 0), AclOperation.UNKNOWN),
      permissionType: orUnknown(() => AclPermissionType.fromCode(p.permission_type ?? 0), AclPermissionType.UNKNOWN),
    }),
  });
}

/**
 * A proto AclBindingFilter as an AclBindingFilter: an absent name, principal
 * or host is Java's null (match any), never the empty string.
 */
function aclBindingFilter(proto) {
  const p = proto || {};
  return new AclBindingFilter({
    patternFilter: new ResourcePatternFilter({
      resourceType: orUnknown(() => ResourceType.fromCode(p.resource_type ?? 0), ResourceType.UNKNOWN),
      name: p.resource_name === undefined ? null : p.resource_name,
      patternType: orUnknown(() => PatternType.fromCode(p.pattern_type ?? 0), PatternType.UNKNOWN),
    }),
    entryFilter: new AccessControlEntryFilter({
      principal: p.principal === undefined ? null : p.principal,
      host: p.host === undefined ? null : p.host,
      operation: orUnknown(() => AclOperation.fromCode(p.operation ?? 0), AclOperation.UNKNOWN),
      permissionType: orUnknown(() => AclPermissionType.fromCode(p.permission_type ?? 0), AclPermissionType.UNKNOWN),
    }),
  });
}

/** A proto ClientQuotaEntity as a ClientQuotaEntity: an absent name is the default entity (null). */
function quotaEntity(proto) {
  const p = proto || {};
  return new ClientQuotaEntity(
    new Map(t.list(p.entries).map((e) => [e.entity_type ?? '', e.entity_name === undefined ? null : e.entity_name])),
  );
}

/**
 * A proto ClientQuotaFilterComponent through the named factory of its match
 * kind; an unspecified kind, or EXACT with no name, is a malformed request,
 * never a defaulted filter.
 */
function quotaFilterComponent(c) {
  const entityType = c.entity_type ?? '';
  const kind = c.match_kind ?? 0;
  if (kind === MATCH_KIND_EXACT) {
    if (c.match_name === undefined) throw new t.AdminRequestError('ClientQuotaFilterComponent with MATCH_KIND_EXACT carries no match_name');
    return ClientQuotaFilterComponent.ofEntity({ entityType, entityName: c.match_name });
  }
  if (kind === MATCH_KIND_DEFAULT) return ClientQuotaFilterComponent.ofDefaultEntity(entityType);
  if (kind === MATCH_KIND_ANY) return ClientQuotaFilterComponent.ofEntityType(entityType);
  throw new t.AdminRequestError(`ClientQuotaFilterComponent has no match_kind (got ${kind})`);
}

/**
 * A proto UserScramCredentialAlteration as a deletion or an upsertion:
 * `is_deletion` decides. An absent salt is Java's salt-generating
 * constructor; a present one, even empty, the one taking the salt.
 */
function scramAlteration(p) {
  const user = p.user ?? '';
  const mechanism = orUnknown(() => ScramMechanism.fromType(p.mechanism ?? 0), ScramMechanism.UNKNOWN);
  if (p.is_deletion === true) return new UserScramCredentialDeletion({ user, mechanism });
  const credentialInfo = new ScramCredentialInfo({ mechanism, iterations: p.iterations ?? 0 });
  const password = p.password ?? Buffer.alloc(0);
  return p.salt === undefined
    ? new UserScramCredentialUpsertion({ user, credentialInfo, password })
    : new UserScramCredentialUpsertion({ user, credentialInfo, password, salt: p.salt });
}

/** A proto KafkaPrincipal as a KafkaPrincipal. */
function principal(p) {
  return new KafkaPrincipal({
    principalType: p.principal_type ?? '',
    name: p.name ?? '',
    tokenAuthenticated: p.token_authenticated === true,
  });
}

module.exports = { AdminService };
