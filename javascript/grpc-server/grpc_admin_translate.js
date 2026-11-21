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

// Proto <-> JavaScript translation helpers for grpc_admin.js, the admin half of
// python/grpc_translate.py (its `_admin_*` helpers), over the client's public
// API. The messages are the plain objects grpc_server.js loads the protos into
// (keepCase, longs: String, defaults: false, oneofs: true): an unset field,
// a repeated field left empty or a map left empty included, is absent.

const { IllegalArgumentError } = require('../dist');
const { KafkaError, Node, TopicPartition } = require('../dist/common');
const { errorFfiId } = require('../dist/internal/errors');
const { LOCAL_ILLEGAL_STATE, futureError, kafkaErrorToProto, nodeToProto, serverError } = require('./grpc_translate');

/**
 * The FFI code of Java's IllegalArgumentException, which this server stamps on
 * a request it cannot translate, as the Python and C++ servers stamp
 * LOCAL_ILLEGAL_ARGUMENT (python/grpc_translate.py's AdminRequestError).
 */
const LOCAL_ILLEGAL_ARGUMENT = /** @type {number} */ (errorFfiId(new IllegalArgumentError('')));

/**
 * A request this server cannot translate into a call of the client: a caller
 * error, Java's IllegalArgumentException, which crosses as
 * LOCAL_ILLEGAL_ARGUMENT with this server's prefix (python's
 * AdminRequestError).
 */
class AdminRequestError extends Error {
  constructor(message) {
    super(message);
    this.name = 'AdminRequestError';
  }
}

/**
 * A per-entry state the client should not have produced (a future that
 * succeeded with no value): a value converter throws it, and `collect` puts
 * this server's LOCAL_ILLEGAL_STATE error in that entry alone, as python's
 * `_admin_synthetic_error` and the C++ server's `make_synthetic_error` do.
 */
class EntryError extends Error {
  constructor(message) {
    super(message);
    this.name = 'EntryError';
  }
}

/**
 * Translate an error a handler met into a proto KafkaError: a failed future's
 * ExecutionError as its cause, a malformed request as LOCAL_ILLEGAL_ARGUMENT,
 * anything else as grpc_translate.js's kafkaErrorToProto translates it.
 *
 * @param {unknown} err
 * @returns {Record<string, unknown>} the proto KafkaError
 */
function adminErrorToProto(err) {
  const error = futureError(err);
  if (error instanceof AdminRequestError) return { code: LOCAL_ILLEGAL_ARGUMENT, message: `js server: ${error.message}` };
  return kafkaErrorToProto(error);
}

/**
 * A *constructor* failure for CreateAdminResponse.error
 * (python's `_admin_constructor_error`): a KafkaError (a configuration the
 * core refused) as it is, anything else as LOCAL_ILLEGAL_ARGUMENT, Java's
 * IllegalArgumentException, the class the Python and C++ servers stamp.
 *
 * @param {unknown} err
 * @returns {Record<string, unknown>} the proto KafkaError
 */
function adminConstructorError(err) {
  if (err instanceof KafkaError) return kafkaErrorToProto(err);
  const text = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
  return { code: LOCAL_ILLEGAL_ARGUMENT, message: `js server: ${text}` };
}

/** The error for a request naming an admin client this server does not hold. */
function unknownAdmin(adminId) {
  return { code: LOCAL_ILLEGAL_STATE, message: `unknown admin_id ${adminId}` };
}

/**
 * The normative mock-selection rule of admin_service.proto's
 * CreateAdminRequest: an empty config, or one whose every value is empty.
 */
function adminSelectsMock(config) {
  return Object.values(config).every((value) => !value);
}

/**
 * `CreateAdminRequest.num_brokers` -> a mock's brokers: ids 0..n-1 on
 * localhost:9092+i, as python's `_admin_mock_brokers` builds them.
 *
 * @param {number} numBrokers
 * @returns {Node[]} the brokers
 * @throws {IllegalArgumentError} fewer than one broker
 */
function mockBrokers(numBrokers) {
  if (numBrokers < 1) throw new IllegalArgumentError('num_brokers must be at least 1');
  return Array.from({ length: numBrokers }, (_, i) => new Node({ id: i, host: 'localhost', port: 9092 + i }));
}

/**
 * The fields of an options class taken from a request (python's `_opts`): a
 * field whose value is undefined is left out, so the option keeps Java's
 * starting value; `timeout_ms` is the `timeoutMs` every options class has.
 *
 * @param {{ timeout_ms?: number }} request
 * @param {Record<string, unknown>} [fields]
 * @returns {Record<string, unknown>} the constructor's keyed object
 */
function optionFields(request, fields = {}) {
  /** @type {Record<string, unknown>} */
  const out = {};
  if (request.timeout_ms !== undefined) out.timeoutMs = request.timeout_ms;
  for (const [key, value] of Object.entries(fields)) if (value !== undefined) out[key] = value;
  return out;
}

/** `optional bool retry_on_quota_violation` -> Java's default, true, when absent. */
function retryOnQuota(request) {
  return request.retry_on_quota_violation === undefined ? true : request.retry_on_quota_violation;
}

/** A proto repeated field, absent when empty. */
function list(field) {
  return field === undefined || field === null ? [] : field;
}

/** A proto TopicPartition as a TopicPartition. */
function tp(proto) {
  const p = proto || {};
  return new TopicPartition({ topic: p.topic === undefined ? '' : p.topic, partition: p.partition === undefined ? 0 : p.partition });
}

/** A TopicPartition as the proto's. */
function tpToProto(partition) {
  return { topic: partition.topic === null ? '' : partition.topic, partition: partition.partition };
}

// ── Result keys ─────────────────────────────────────────────────────────────

function nameKey(name) {
  return { name };
}

function topicIdKey(topicId) {
  return { topic_id: topicId };
}

function partitionKey(partition) {
  return { partition: tpToProto(partition) };
}

function brokerIdKey(brokerId) {
  return { broker_id: brokerId };
}

/** A ConfigResource as the proto's: `resource_type` is Java's `Type.id()`. */
function configResourceToProto(resource) {
  return { resource_type: resource.type().id(), name: resource.name() };
}

function configResourceKey(resource) {
  return { config_resource: configResourceToProto(resource) };
}

function replicaToProto(replica) {
  return { topic: replica.topic(), partition: replica.partition(), broker_id: replica.brokerId() };
}

function replicaKey(replica) {
  return { replica: replicaToProto(replica) };
}

// ── Futures ─────────────────────────────────────────────────────────────────

/**
 * One envelope entry per key of `futures` (an iterable of `[key, future]`, a
 * `Map` or one of the client's maps), each future awaited as a user awaits
 * it: its value through `toValue` in the `value` arm, its failure (the
 * ExecutionError's cause) in the `error` arm. With `toValue` null the
 * future's value is void, and a success is an entry with no error
 * (VoidResultEntry).
 *
 * @param {Iterable<[unknown, { get(): Promise<unknown> }]>} futures
 * @param {(key: any) => Record<string, unknown>} keyOf
 * @param {((value: any, key: any) => unknown | Promise<unknown>) | null} toValue
 * @returns {Promise<Array<Record<string, unknown>>>} the entries
 */
async function collect(futures, keyOf, toValue) {
  const entries = [];
  for (const [key, future] of futures) {
    /** @type {Record<string, unknown>} */
    const entry = { key: keyOf(key) };
    let value;
    try {
      value = await future.get();
    } catch (e) {
      entry.error = adminErrorToProto(e);
      entries.push(entry);
      continue;
    }
    if (toValue !== null) {
      try {
        entry.value = await toValue(value, key);
      } catch (e) {
        if (!(e instanceof EntryError)) throw e;
        entry.error = serverError(e.message);
      }
    }
    entries.push(entry);
  }
  return entries;
}

/** The entries of a VoidKeyedResponse over `futures`. */
async function keyedVoid(futures, keyOf) {
  return { entries: await collect(futures, keyOf, null) };
}

/**
 * A VoidKeyedResponse over a resolved map whose values are an error or null
 * (Java's `Optional<Throwable>`, electLeaders' `partitions()`).
 */
function voidEntries(outcomes, keyOf) {
  const entries = [];
  for (const [key, outcome] of outcomes) {
    /** @type {Record<string, unknown>} */
    const entry = { key: keyOf(key) };
    if (outcome !== null && outcome !== undefined) entry.error = adminErrorToProto(outcome);
    entries.push(entry);
  }
  return { entries };
}

// ── Values ──────────────────────────────────────────────────────────────────

/** A list of Node as proto Nodes. */
function nodes(array) {
  return list(array).map(nodeToProto);
}

/** A set of AclOperation as its wire codes, sorted, or undefined for null (not reported). */
function aclOperations(operations) {
  if (operations === null || operations === undefined) return undefined;
  return { operations: [...operations].map((op) => op.code()).sort((a, b) => a - b) };
}

/** The five fields of a ConfigEntry createTopics reports (python's `_admin_config_entry_to_proto`). */
function configEntryToProto(entry) {
  /** @type {Record<string, unknown>} */
  const out = { name: entry.name(), is_default: entry.isDefault(), is_sensitive: entry.isSensitive(), is_read_only: entry.isReadOnly() };
  if (entry.value() !== null) out.value = entry.value();
  return out;
}

/**
 * All nine fields of a ConfigEntry (python's
 * `_admin_full_config_entry_to_proto`): `source` and `config_type` are the
 * Java constants' names, the synonyms in Java's precedence order.
 */
function fullConfigEntryToProto(entry) {
  const out = configEntryToProto(entry);
  if (entry.source() !== null) out.source = entry.source().name();
  if (entry.type() !== null) out.config_type = entry.type().name();
  if (entry.documentation() !== null) out.documentation = entry.documentation();
  out.synonyms = list(entry.synonyms()).map((synonym) => {
    /** @type {Record<string, unknown>} */
    const s = { name: synonym.name(), source: synonym.source().name() };
    if (synonym.value() !== null) s.value = synonym.value();
    return s;
  });
  return out;
}

/** A TopicPartitionInfo as the proto's; `elr` and `lastKnownElr` are nullable. */
function partitionInfoToProto(info) {
  /** @type {Record<string, unknown>} */
  const out = { partition: info.partition(), replicas: nodes(info.replicas()), isr: nodes(info.isr()) };
  const leader = nodeToProto(info.leader());
  if (leader !== undefined) out.leader = leader;
  if (info.elr() !== null) out.elr = { nodes: nodes(info.elr()) };
  if (info.lastKnownElr() !== null) out.last_known_elr = { nodes: nodes(info.lastKnownElr()) };
  return out;
}

/** A TopicDescription (a data object) as the proto's. */
function topicDescriptionToProto(description) {
  /** @type {Record<string, unknown>} */
  const out = {
    name: description.name === null ? '' : description.name,
    topic_id: description.topicId === null ? '' : description.topicId.toString(),
    is_internal: description.isInternal,
    partitions: description.partitions.map(partitionInfoToProto),
  };
  // Absent: the broker did not report the operations, not that none are authorized.
  const operations = aclOperations(description.authorizedOperations);
  if (operations !== undefined) out.authorized_operations = operations;
  return out;
}

/** A TopicListing as the proto's AdminTopicListing. */
function topicListingToProto(listing) {
  return {
    name: listing.name() === null ? '' : listing.name(),
    topic_id: listing.topicId() === null ? '' : listing.topicId().toString(),
    is_internal: listing.isInternal(),
  };
}

/** A LogDirDescription (a data object) as the proto's: its own error, not the broker's. */
function logDirDescriptionToProto(description) {
  /** @type {Record<string, unknown>} */
  const out = { is_cordoned: description.isCordoned };
  if (description.error !== null) out.error = adminErrorToProto(description.error);
  if (description.totalBytes !== null) out.total_bytes = description.totalBytes.toString();
  if (description.usableBytes !== null) out.usable_bytes = description.usableBytes.toString();
  out.replica_infos = [...description.replicaInfos].map(([partition, info]) => ({
    partition: tpToProto(partition),
    size: info.size().toString(),
    offset_lag: info.offsetLag().toString(),
    is_future: info.isFuture(),
  }));
  return out;
}

/** A ReplicaLogDirInfo as the proto's; both dirs nullable. */
function replicaLogDirInfoToProto(info) {
  /** @type {Record<string, unknown>} */
  const out = {
    current_replica_offset_lag: info.getCurrentReplicaOffsetLag().toString(),
    future_replica_offset_lag: info.getFutureReplicaOffsetLag().toString(),
  };
  if (info.getCurrentReplicaLogDir() !== null) out.current_replica_log_dir = info.getCurrentReplicaLogDir();
  if (info.getFutureReplicaLogDir() !== null) out.future_replica_log_dir = info.getFutureReplicaLogDir();
  return out;
}

/** A ListOffsetsResultInfo as the proto's; an absent leader epoch is not epoch 0. */
function listOffsetsInfoToProto(info) {
  /** @type {Record<string, unknown>} */
  const out = { offset: info.offset().toString(), timestamp: info.timestamp().toString() };
  if (info.leaderEpoch() !== null) out.leader_epoch = info.leaderEpoch();
  return out;
}

// ── Groups ──────────────────────────────────────────────────────────────────

/** TopicPartitions in (topic, partition) order, so the encoding is the same on every run. */
function sortedTps(partitions) {
  return [...list(partitions)].sort((a, b) =>
    a.topic === b.topic ? a.partition - b.partition : String(a.topic) < String(b.topic) ? -1 : 1,
  );
}

/** An enum constant as the proto's string (Java's `toString()`), or '' for null. */
function enumName(value) {
  return value === null || value === undefined ? '' : String(value);
}

/**
 * A GroupListing as the proto's: `group_type` and `group_state` are Java
 * Optionals, absent when empty, as Java's `toString()` names
 * (python's `_admin_group_listing_to_proto`).
 */
function groupListingToProto(listing) {
  /** @type {Record<string, unknown>} */
  const out = {
    group_id: listing.groupId() ?? '',
    protocol: listing.protocol() ?? '',
    is_simple_consumer_group: listing.isSimpleConsumerGroup(),
  };
  if (listing.type() !== null) out.group_type = String(listing.type());
  if (listing.groupState() !== null) out.group_state = String(listing.groupState());
  return out;
}

/** A MemberAssignment as the proto's, its partitions sorted. */
function memberAssignmentToProto(assignment) {
  return { topic_partitions: sortedTps(assignment.topicPartitions()).map(tpToProto) };
}

/**
 * A MemberDescription (a data object) as the proto's: the Optionals stay
 * absent when empty, and a null assignment is left absent rather than made
 * empty (python's `_admin_member_description_to_proto`).
 */
function memberDescriptionToProto(member) {
  /** @type {Record<string, unknown>} */
  const out = { consumer_id: member.consumerId, client_id: member.clientId, host: member.host };
  if (member.assignment !== null) out.assignment = memberAssignmentToProto(member.assignment);
  if (member.groupInstanceId !== null) out.group_instance_id = member.groupInstanceId;
  if (member.rackId !== null) out.rack_id = member.rackId;
  if (member.targetAssignment !== null) out.target_assignment = memberAssignmentToProto(member.targetAssignment);
  if (member.memberEpoch !== null) out.member_epoch = member.memberEpoch;
  if (member.upgraded !== null) out.upgraded = member.upgraded;
  return out;
}

/** A ConsumerGroupDescription (a data object) as the proto's. */
function consumerGroupDescriptionToProto(description) {
  /** @type {Record<string, unknown>} */
  const out = {
    group_id: description.groupId,
    is_simple_consumer_group: description.isSimpleConsumerGroup,
    members: description.members.map(memberDescriptionToProto),
    partition_assignor: description.partitionAssignor,
    group_type: enumName(description.type),
    group_state: enumName(description.groupState),
  };
  const coordinator = nodeToProto(description.coordinator);
  if (coordinator !== undefined) out.coordinator = coordinator;
  // Absent: the broker did not report the operations, not that none are authorized.
  const operations = aclOperations(description.authorizedOperations);
  if (operations !== undefined) out.authorized_operations = operations;
  if (description.groupEpoch !== null) out.group_epoch = description.groupEpoch;
  if (description.targetAssignmentEpoch !== null) out.target_assignment_epoch = description.targetAssignmentEpoch;
  return out;
}

/** A ClassicGroupDescription (a data object) as the proto's. */
function classicGroupDescriptionToProto(description) {
  /** @type {Record<string, unknown>} */
  const out = {
    group_id: description.groupId,
    protocol: description.protocol ?? '',
    protocol_data: description.protocolData,
    is_simple_consumer_group: description.isSimpleConsumerGroup,
    members: description.members.map(memberDescriptionToProto),
    state: enumName(description.state),
  };
  const coordinator = nodeToProto(description.coordinator);
  if (coordinator !== undefined) out.coordinator = coordinator;
  const operations = aclOperations(description.authorizedOperations);
  if (operations !== undefined) out.authorized_operations = operations;
  return out;
}

/** An OffsetAndMetadata (a data object) as the proto's; an absent leader epoch is not epoch 0. */
function offsetAndMetadataToProto(offset) {
  /** @type {Record<string, unknown>} */
  const out = { offset: offset.offset.toString(), metadata: offset.metadata };
  if (offset.leaderEpoch !== null) out.leader_epoch = offset.leaderEpoch;
  return out;
}

/**
 * A group's committed offsets as the proto's GroupOffsets: a null value
 * (Java's null map value, a partition with no commit) stays absent.
 */
function groupOffsetsToProto(offsets) {
  return {
    offsets: [...offsets].map(([partition, offset]) => {
      /** @type {Record<string, unknown>} */
      const pair = { partition: tpToProto(partition) };
      if (offset !== null) pair.offset = offsetAndMetadataToProto(offset);
      return pair;
    }),
  };
}

// ── ACLs, quotas, SCRAM, delegation tokens & features ──────────────────────

/** An AclBinding as the proto's: every field required, the enums as Java's codes. */
function aclBindingToProto(binding) {
  const pattern = binding.pattern();
  const entry = binding.entry();
  return {
    resource_type: pattern.resourceType().code(),
    resource_name: pattern.name(),
    pattern_type: pattern.patternType().code(),
    principal: entry.principal(),
    host: entry.host(),
    operation: entry.operation().code(),
    permission_type: entry.permissionType().code(),
  };
}

/** An AclBindingFilter as the proto's, each of its three nullable strings absent when null. */
function aclBindingFilterToProto(filter) {
  const pattern = filter.patternFilter();
  const entry = filter.entryFilter();
  /** @type {Record<string, unknown>} */
  const out = {
    resource_type: pattern.resourceType().code(),
    pattern_type: pattern.patternType().code(),
    operation: entry.operation().code(),
    permission_type: entry.permissionType().code(),
  };
  if (pattern.name() !== null) out.resource_name = pattern.name();
  if (entry.principal() !== null) out.principal = entry.principal();
  if (entry.host() !== null) out.host = entry.host();
  return out;
}

function aclBindingKey(binding) {
  return { acl_binding: aclBindingToProto(binding) };
}

function aclBindingFilterKey(filter) {
  return { acl_binding_filter: aclBindingFilterToProto(filter) };
}

/** A deleteAcls filter's FilterResults: each matched ACL with its own error. */
function filterResultsToProto(results) {
  return {
    values: results.values().map((result) => {
      /** @type {Record<string, unknown>} */
      const out = {};
      if (result.binding() !== null) out.binding = aclBindingToProto(result.binding());
      if (result.exception() !== null) out.exception = adminErrorToProto(result.exception());
      return out;
    }),
  };
}

/** A ClientQuotaEntity as the proto's: a null entity name (the default entity) stays absent. */
function quotaEntityToProto(entity) {
  return {
    entries: [...entity.entries()].map(([entityType, entityName]) => {
      /** @type {Record<string, unknown>} */
      const pair = { entity_type: entityType };
      if (entityName !== null) pair.entity_name = entityName;
      return pair;
    }),
  };
}

function quotaEntityKey(entity) {
  return { client_quota_entity: quotaEntityToProto(entity) };
}

/** A KafkaPrincipal as the proto's, `token_authenticated` included. */
function principalToProto(principal) {
  return { principal_type: principal.getPrincipalType(), name: principal.getName(), token_authenticated: principal.tokenAuthenticated() };
}

/**
 * A DelegationToken as the proto's; `hmac_as_base64` is the binding's own
 * `hmacAsBase64String()`, which the harness re-derives and checks.
 */
function delegationTokenToProto(token) {
  const info = token.tokenInfo();
  if (info === null) throw new Error('DelegationToken carries no token_info');
  const hmac = token.hmac();
  return {
    token_information: {
      token_id: info.tokenId(),
      owner: principalToProto(info.owner()),
      token_requester: principalToProto(info.tokenRequester()),
      renewers: info.renewers().map(principalToProto),
      issue_timestamp: info.issueTimestamp().toString(),
      max_timestamp: info.maxTimestamp().toString(),
      expiry_timestamp: info.expiryTimestamp().toString(),
    },
    hmac,
    hmac_as_base64: token.hmacAsBase64String(),
  };
}

/** A FeatureMetadata as the proto's; an absent epoch (Java's empty Optional) is not 0. */
function featureMetadataToProto(metadata) {
  /** @type {Record<string, unknown>} */
  const finalized = {};
  for (const [feature, range] of metadata.finalizedFeatures()) {
    finalized[feature] = { min_version_level: range.minVersionLevel(), max_version_level: range.maxVersionLevel() };
  }
  /** @type {Record<string, unknown>} */
  const supported = {};
  for (const [feature, range] of metadata.supportedFeatures()) {
    supported[feature] = { min_version: range.minVersion(), max_version: range.maxVersion() };
  }
  /** @type {Record<string, unknown>} */
  const out = { finalized_features: finalized, supported_features: supported };
  const epoch = metadata.finalizedFeaturesEpoch();
  if (epoch !== null) out.finalized_features_epoch = epoch.toString();
  return out;
}

// ── Producers & transactions ────────────────────────────────────────────────

/** A ProducerState as the proto's; its two Optionals stay absent when empty. */
function producerStateToProto(state) {
  /** @type {Record<string, unknown>} */
  const out = {
    producer_id: state.producerId().toString(),
    producer_epoch: state.producerEpoch(),
    last_sequence: state.lastSequence(),
    last_timestamp: state.lastTimestamp().toString(),
  };
  if (state.coordinatorEpoch() !== null) out.coordinator_epoch = state.coordinatorEpoch();
  if (state.currentTransactionStartOffset() !== null)
    out.current_transaction_start_offset = state.currentTransactionStartOffset().toString();
  return out;
}

/** A TransactionDescription (a data object) as the proto's, its partitions sorted. */
function transactionDescriptionToProto(description) {
  /** @type {Record<string, unknown>} */
  const out = {
    coordinator_id: description.coordinatorId,
    state: enumName(description.state),
    producer_id: description.producerId.toString(),
    producer_epoch: description.producerEpoch,
    transaction_timeout_ms: description.transactionTimeoutMs.toString(),
    topic_partitions: sortedTps(description.topicPartitions).map(tpToProto),
  };
  if (description.transactionStartTimeMs !== null) out.transaction_start_time_ms = description.transactionStartTimeMs.toString();
  return out;
}

/** A TransactionListing as the proto's. */
function transactionListingToProto(listing) {
  return {
    transactional_id: listing.transactionalId() ?? '',
    producer_id: listing.producerId().toString(),
    state: enumName(listing.state()),
  };
}

module.exports = {
  AdminRequestError,
  EntryError,
  aclBindingFilterKey,
  aclBindingFilterToProto,
  aclBindingKey,
  aclBindingToProto,
  classicGroupDescriptionToProto,
  consumerGroupDescriptionToProto,
  delegationTokenToProto,
  featureMetadataToProto,
  filterResultsToProto,
  groupListingToProto,
  groupOffsetsToProto,
  principalToProto,
  producerStateToProto,
  quotaEntityKey,
  quotaEntityToProto,
  sortedTps,
  transactionDescriptionToProto,
  transactionListingToProto,
  LOCAL_ILLEGAL_ARGUMENT,
  adminConstructorError,
  adminErrorToProto,
  adminSelectsMock,
  aclOperations,
  brokerIdKey,
  collect,
  configEntryToProto,
  configResourceKey,
  configResourceToProto,
  fullConfigEntryToProto,
  keyedVoid,
  list,
  listOffsetsInfoToProto,
  logDirDescriptionToProto,
  mockBrokers,
  nameKey,
  nodes,
  optionFields,
  partitionKey,
  replicaKey,
  replicaLogDirInfoToProto,
  replicaToProto,
  retryOnQuota,
  topicDescriptionToProto,
  topicIdKey,
  topicListingToProto,
  tp,
  tpToProto,
  unknownAdmin,
  voidEntries,
};
