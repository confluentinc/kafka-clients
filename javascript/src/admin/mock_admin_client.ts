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

// Java's MockAdminClient, over the C FFI's mock (javascript/CLAUDE.md, JavaScript
// Binding Conventions, Implementation over the FFI), as the .NET client
// wraps it: the same native handle as a KafkaAdminClient's, over the mock's
// `__as_Admin` view, so the RPCs and `close` take the admin client's path.
// Java ships it in its test fixtures; it stays public, as in the Rust core,
// C, Python and .NET (.claude/rules/admin-client.md §9). Its nested Builder
// is a class of the namespace merged with it, holding a native builder whose
// setters run at once, as Java's do.
//
// Left out, for want of an FFI entry point (the admin package's
// documentation lists them): brokers(), broker(int), injectTimeoutException,
// handleDescribeTopicsUsingIds, setMockMetrics, addedMetrics,
// disableTelemetry, setClientInstanceId, advanceTimeOnClientInstanceId,
// addStreamsGroupDescription, and the five-parameter form of addTopic,
// addTopic(internal, name, partitions, configs, usesTopicId).

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way (the addon is loaded there alone, javascript/CLAUDE.md "Files and layout")
import native = require('../internal/native');
import { isPlainObject, JavaForms, kind } from '../internal/args';
import * as calls from '../internal/admin_calls';
import type { AdminHandle } from '../internal/admin_result';
import { checkInt, checkLong, checkShort } from '../internal/java_number';
import { applyOptionKeys, checkBoolean, Setter } from '../internal/options';
import { Node } from '../common/node';
import { TopicPartitionInfo } from '../common/topic_partition_info';
import type { Duration } from '../duration';
import type { Admin } from './admin';
import type { CreatePartitionsOptions } from './create_partitions_options';
import type { CreatePartitionsResult } from './create_partitions_result';
import type { CreateTopicsOptions } from './create_topics_options';
import type { CreateTopicsResult } from './create_topics_result';
import type { DeleteRecordsOptions } from './delete_records_options';
import type { DeleteRecordsResult } from './delete_records_result';
import type { DeleteTopicsOptions } from './delete_topics_options';
import type { DeleteTopicsResult } from './delete_topics_result';
import type { DescribeTopicsOptions } from './describe_topics_options';
import type { DescribeTopicsResult } from './describe_topics_result';
import type { NewPartitions } from './new_partitions';
import type { NewTopic } from './new_topic';
import type { RecordsToDelete } from './records_to_delete';
import type { TopicCollection } from '../common/topic_collection';
import { TopicPartitionMap } from '../common/topic_partition_map';
import type { DescribeClusterOptions } from './describe_cluster_options';
import type { DescribeClusterResult } from './describe_cluster_result';
import type { CloseArgs } from './kafka_admin_client';
import type { ListTopicsOptions } from './list_topics_options';
import type { ListTopicsResult } from './list_topics_result';
import type { AlterConfigOp } from './alter_config_op';
import type { AlterConfigsOptions } from './alter_configs_options';
import type { AlterConfigsResult } from './alter_configs_result';
import type { AlterReplicaLogDirsOptions } from './alter_replica_log_dirs_options';
import type { AlterReplicaLogDirsResult } from './alter_replica_log_dirs_result';
import type { DescribeConfigsOptions } from './describe_configs_options';
import type { DescribeConfigsResult } from './describe_configs_result';
import type { DescribeLogDirsOptions } from './describe_log_dirs_options';
import type { DescribeLogDirsResult } from './describe_log_dirs_result';
import type { DescribeReplicaLogDirsOptions } from './describe_replica_log_dirs_options';
import type { DescribeReplicaLogDirsResult } from './describe_replica_log_dirs_result';
import type { ListConfigResourcesOptions } from './list_config_resources_options';
import type { ListConfigResourcesResult } from './list_config_resources_result';
import type { ConfigResource } from '../common/config/config_resource';
import type { ConfigResourceMap } from '../common/config/config_resource_map';
import type { TopicPartitionReplica } from '../common/topic_partition_replica';
import type { TopicPartitionReplicaMap } from '../common/topic_partition_replica_map';
import * as configCalls from '../internal/admin_config_calls';
import type { AlterPartitionReassignmentsOptions } from './alter_partition_reassignments_options';
import type { AlterPartitionReassignmentsResult } from './alter_partition_reassignments_result';
import type { ElectLeadersOptions } from './elect_leaders_options';
import type { ElectLeadersResult } from './elect_leaders_result';
import type { ListOffsetsOptions } from './list_offsets_options';
import type { ListOffsetsResult } from './list_offsets_result';
import type { ListPartitionReassignmentsOptions } from './list_partition_reassignments_options';
import type { ListPartitionReassignmentsResult } from './list_partition_reassignments_result';
import type { NewPartitionReassignment } from './new_partition_reassignment';
import type { OffsetSpec } from './offset_spec';
import type { ElectionType } from '../common/election_type';
import type { TopicPartition } from '../common/topic_partition';
import * as offsetCalls from '../internal/admin_offset_calls';
import type { AlterConsumerGroupOffsetsOptions } from './alter_consumer_group_offsets_options';
import type { AlterConsumerGroupOffsetsResult } from './alter_consumer_group_offsets_result';
import type { DeleteConsumerGroupOffsetsOptions } from './delete_consumer_group_offsets_options';
import type { DeleteConsumerGroupOffsetsResult } from './delete_consumer_group_offsets_result';
import type { DeleteConsumerGroupsOptions } from './delete_consumer_groups_options';
import type { DeleteConsumerGroupsResult } from './delete_consumer_groups_result';
import type { DescribeClassicGroupsOptions } from './describe_classic_groups_options';
import type { DescribeClassicGroupsResult } from './describe_classic_groups_result';
import type { DescribeConsumerGroupsOptions } from './describe_consumer_groups_options';
import type { DescribeConsumerGroupsResult } from './describe_consumer_groups_result';
import type { ListConsumerGroupOffsetsOptions } from './list_consumer_group_offsets_options';
import type { ListConsumerGroupOffsetsResult } from './list_consumer_group_offsets_result';
import type { ListConsumerGroupOffsetsSpec } from './list_consumer_group_offsets_spec';
import type { ListGroupsOptions } from './list_groups_options';
import type { ListGroupsResult } from './list_groups_result';
import type { RemoveMembersFromConsumerGroupOptions } from './remove_members_from_consumer_group_options';
import type { RemoveMembersFromConsumerGroupResult } from './remove_members_from_consumer_group_result';
import type { OffsetAndMetadata } from '../consumer/offset_and_metadata';
import * as groupCalls from '../internal/admin_group_calls';
import type { CreateAclsOptions } from './create_acls_options';
import type { CreateAclsResult } from './create_acls_result';
import type { DeleteAclsOptions } from './delete_acls_options';
import type { DeleteAclsResult } from './delete_acls_result';
import type { DescribeAclsOptions } from './describe_acls_options';
import type { DescribeAclsResult } from './describe_acls_result';
import type { AclBinding } from '../common/acl/acl_binding';
import type { AclBindingFilter } from '../common/acl/acl_binding_filter';
import * as securityCalls from '../internal/admin_security_calls';
import type { AlterClientQuotasOptions } from './alter_client_quotas_options';
import type { AlterClientQuotasResult } from './alter_client_quotas_result';
import type { AlterUserScramCredentialsOptions } from './alter_user_scram_credentials_options';
import type { AlterUserScramCredentialsResult } from './alter_user_scram_credentials_result';
import type { CreateDelegationTokenOptions } from './create_delegation_token_options';
import type { CreateDelegationTokenResult } from './create_delegation_token_result';
import type { DescribeClientQuotasOptions } from './describe_client_quotas_options';
import type { DescribeClientQuotasResult } from './describe_client_quotas_result';
import type { DescribeDelegationTokenOptions } from './describe_delegation_token_options';
import type { DescribeDelegationTokenResult } from './describe_delegation_token_result';
import type { DescribeUserScramCredentialsOptions } from './describe_user_scram_credentials_options';
import type { DescribeUserScramCredentialsResult } from './describe_user_scram_credentials_result';
import type { ExpireDelegationTokenOptions } from './expire_delegation_token_options';
import type { ExpireDelegationTokenResult } from './expire_delegation_token_result';
import type { RenewDelegationTokenOptions } from './renew_delegation_token_options';
import type { RenewDelegationTokenResult } from './renew_delegation_token_result';
import type { UserScramCredentialAlteration } from './user_scram_credential_alteration';
import type { ClientQuotaAlteration } from '../common/quota/client_quota_alteration';
import type { ClientQuotaFilter } from '../common/quota/client_quota_filter';
import type { AbortTransactionOptions } from './abort_transaction_options';
import type { AbortTransactionResult } from './abort_transaction_result';
import type { AbortTransactionSpec } from './abort_transaction_spec';
import type { DescribeProducersOptions } from './describe_producers_options';
import type { DescribeProducersResult } from './describe_producers_result';
import type { DescribeTransactionsOptions } from './describe_transactions_options';
import type { DescribeTransactionsResult } from './describe_transactions_result';
import type { FenceProducersOptions } from './fence_producers_options';
import type { FenceProducersResult } from './fence_producers_result';
import type { ListTransactionsOptions } from './list_transactions_options';
import type { ListTransactionsResult } from './list_transactions_result';
import type { TerminateTransactionOptions } from './terminate_transaction_options';
import type { TerminateTransactionResult } from './terminate_transaction_result';
import * as transactionCalls from '../internal/admin_transaction_calls';
import type { DescribeFeaturesOptions } from './describe_features_options';
import type { DescribeFeaturesResult } from './describe_features_result';
import type { FeatureUpdate } from './feature_update';
import type { UnregisterControllerOptions } from './unregister_controller_options';
import type { UnregisterControllerResult } from './unregister_controller_result';
import type { UpdateFeaturesOptions } from './update_features_options';
import type { UpdateFeaturesResult } from './update_features_result';
import * as featureCalls from '../internal/admin_feature_calls';

const CONSTRUCTOR_FORMS = new JavaForms('MockAdminClient', [[], ['brokers', 'controller']]);
const CREATE_FORMS = new JavaForms('create', [[]]);
const CONTROLLER_FORMS = new JavaForms('controller', [['controller']], {
  bareTypes: { controller: (value) => value === null || value instanceof Node },
});
const ADD_TOPIC_FORMS = new JavaForms('addTopic', [['internal', 'name', 'partitions', 'configs']]);
const MARK_TOPIC_FOR_DELETION_FORMS = new JavaForms('markTopicForDeletion', [['name']], {
  bareTypes: { name: (value) => value === null || typeof value === 'string' },
});
const SET_FETCHES_REMAINING_UNTIL_VISIBLE_FORMS = new JavaForms('setFetchesRemainingUntilVisible', [
  ['topicName', 'fetchesRemainingUntilVisible'],
]);
// Any bare value: a Map reaches the TypeError with its hint.
const UPDATE_BEGINNING_OFFSETS_FORMS = new JavaForms('updateBeginningOffsets', [['newOffsets']]);
const UPDATE_END_OFFSETS_FORMS = new JavaForms('updateEndOffsets', [['newOffsets']]);
const UPDATE_CONSUMER_GROUP_OFFSETS_FORMS = new JavaForms('updateConsumerGroupOffsets', [['newOffsets']]);
const TIMEOUT_NEXT_REQUEST_FORMS = new JavaForms('timeoutNextRequest', [['numberOfRequest']], {
  bareTypes: { numberOfRequest: (value) => typeof value === 'number' },
});

/**
 * The nodes of `value` (Java's `List<Node>`, any iterable), as their fields
 * for the addon, or null for `null`.
 *
 * @param value - the nodes given
 * @param name - the parameter's name
 * @returns the nodes' fields, or null
 */
function nodesFields(value: unknown, name: string): ReturnType<typeof calls.nodeFields>[] | null {
  if (value === null) return null;
  if (typeof value === 'string' || typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function') {
    throw new TypeError(`${name} must be an iterable of Node, not ${kind(value)}`);
  }
  const out = [];
  for (const node of value as Iterable<unknown>) {
    if (node === null) throw new TypeError(`${name} must not contain null`);
    if (!(node instanceof Node)) throw new TypeError(`${name} must contain only Node, not ${kind(node)}`);
    out.push(calls.nodeFields(node));
  }
  return out;
}

/**
 * A node, as its fields for the addon, or null.
 *
 * @param value - the node given
 * @param name - the parameter's name
 * @returns its fields, or null
 */
function nodeFieldsOrNull(value: unknown, name: string): ReturnType<typeof calls.nodeFields> | null {
  if (value === null) return null;
  if (!(value instanceof Node)) throw new TypeError(`${name} must be a Node, not ${kind(value)}`);
  return calls.nodeFields(value);
}

// The handle internalCreate hands the constructor (a mock the Builder built).
let adopting: AdminHandle | null = null;

/**
 * MockAdminClient mock client.
 *
 * @remarks It wraps the C FFI's mock, not a translation of Java's: its
 * futures complete through the same native path as a `KafkaAdminClient`'s.
 * Java ships it in its test fixtures; it is public here, as in the Rust core.
 * It emits no preview warning.
 *
 * *(deviation)* Java's mock completes a call's futures before it returns;
 * here, right after a call, a future's `isDone()`, `isCompletedExceptionally()`
 * and `getNow()` don't yet show the outcome, and a `thenApply` or
 * `whenComplete` registered then runs on a later turn, not inside the call.
 * They show it, and it runs, on a later turn of the event loop, once the
 * core's task answers; `await get()` is unaffected. Why: the core's
 * `kafka_common_KafkaFuture_get_cb` answers even a completed future from a
 * task it spawns, and the binding calls no blocking C function. Goes when: the
 * core queues a completed future's answer before `get_cb` returns; the binding
 * then answers its queued callbacks once at the end of each call, as
 * `MockProducer` does.
 *
 * *(deviation)* The calls Java's mock does not implement return their Result,
 * whose futures fail with `UnsupportedOperationError`, where Java's mock
 * throws from the call; the admin package's documentation lists them.
 *
 * *(deviation)* `createDelegationToken` with no renewer fails its future with
 * `IllegalArgumentError`, where Java's mock throws `IndexOutOfBoundsError` from
 * the call, as it makes the first renewer the owner. Why: the core's mock
 * answers so, since the C FFI's error enum has no id for Java's
 * `IndexOutOfBoundsException`. Goes when: the FFI has that id and the core's
 * mock throws it from the call.
 *
 * The admin package's documentation lists the mock's other differences from
 * Java's mock, each with its Why and Goes when.
 */
export class MockAdminClient implements Admin {
  /** The cluster id of a mock a Builder builds unless it sets one. */
  static readonly DEFAULT_CLUSTER_ID: string = 'I4ZmrWqfT2e-upky_4fdPA';

  /** The log dirs of each broker a Builder adds. */
  static readonly DEFAULT_LOG_DIRS: ReadonlyArray<string> = Object.freeze(['/tmp/kafka-logs']);

  readonly #handle: AdminHandle;

  /**
   * Java's `MockAdminClient()`: one broker, `Node.noNode()`, which is also
   * the controller.
   */
  constructor();
  /**
   * Java's `MockAdminClient(List<Node> brokers, Node controller)`.
   *
   * @param options - the brokers and the controller
   * @throws `IllegalArgumentError` when the controller is not one of the
   *   brokers ("The controller node must be in the list of brokers"), or for
   *   a form Java has not
   * @throws `TypeError` for a value of the wrong type or a null one
   */
  constructor(options: {
    /** The brokers, any iterable of Node. */
    brokers: Iterable<Node>;
    /** The controller, which must be one of the brokers. */
    controller: Node;
  });
  constructor(options?: unknown) {
    if (adopting !== null) {
      this.#handle = adopting;
      adopting = null;
      return;
    }
    if (CONSTRUCTOR_FORMS.match(options, arguments.length) === 0) {
      this.#handle = native.mockAdminCreate();
      return;
    }
    this.#handle = native.mockAdminCreate(
      nodesFields(CONSTRUCTOR_FORMS.value(options, 'brokers'), 'brokers'),
      nodeFieldsOrNull(CONSTRUCTOR_FORMS.value(options, 'controller'), 'controller'),
    );
  }

  /**
   * A mock over the handle a Builder built. Called by `MockAdminClient.Builder`.
   *
   * @param handle - the mock's native handle
   * @returns the mock
   * @internal
   */
  static internalCreate(handle: AdminHandle): MockAdminClient {
    adopting = handle;
    try {
      return new MockAdminClient();
    } finally {
      adopting = null;
    }
  }

  /**
   * Java's static `create()`: a new Builder.
   *
   * @returns the builder
   */
  static create(): MockAdminClient.Builder;
  static create(args?: unknown): MockAdminClient.Builder {
    CREATE_FORMS.match(args, arguments.length);
    return new MockAdminClient.Builder();
  }

  /**
   * Makes `controller` the controller `describeCluster` reports.
   *
   * @param controller - the node, one of the brokers, on its own or as `{ controller }`
   * @throws `IllegalArgumentError` when it is not one of the brokers ("The
   *   controller node must be in the list of brokers"), the controller then
   *   unchanged
   * @throws `TypeError` when it is null or not a Node
   */
  controller(controller: Node): void;
  /**
   * The same as `controller(controller)`, the parameter given by name.
   *
   * @param options - the node, one of the brokers
   * @throws `IllegalArgumentError` when it is not one of the brokers ("The
   *   controller node must be in the list of brokers"), the controller then
   *   unchanged
   * @throws `TypeError` when it is null or not a Node
   */
  controller(options: {
    /** The node, one of the brokers */
    controller: Node;
  }): void;
  controller(controller: unknown): void {
    CONTROLLER_FORMS.match(controller, arguments.length);
    native.mockAdminController(this.#handle, nodeFieldsOrNull(CONTROLLER_FORMS.value(controller, 'controller'), 'controller'));
  }

  /**
   * Adds a topic to the mock's cluster, as if it had been created, with the
   * given partitions and configuration.
   *
   * @param options - whether the topic is internal, its name, its partitions
   *   and its configuration (null for none)
   * @throws `IllegalArgumentError` when the topic was already added ("Topic
   *   <name> was already added."), when a partition's leader is not one of the
   *   brokers ("Leader broker unknown"), or when its replicas or in-sync
   *   replicas name another broker ("Unknown brokers in replica list",
   *   "Unknown brokers in isr list"), or for a form Java has not
   * @throws `TypeError` for a value of the wrong type
   */
  addTopic(options: {
    /** Whether the topic is internal. */
    internal: boolean;
    /** The topic's name. */
    name: string;
    /** The topic's partitions. */
    partitions: Iterable<TopicPartitionInfo>;
    /** The topic's configuration, or null. */
    configs: ReadonlyMap<string, string | null> | Readonly<Record<string, string | null>> | null;
  }): void {
    ADD_TOPIC_FORMS.match(options, arguments.length);
    const internal = ADD_TOPIC_FORMS.value(options, 'internal');
    if (typeof internal !== 'boolean') throw new TypeError(`internal must be a boolean, not ${kind(internal)}`);
    const name = ADD_TOPIC_FORMS.value(options, 'name');
    if (name !== null && typeof name !== 'string') throw new TypeError(`name must be a string, not ${kind(name)}`);
    const partitions = ADD_TOPIC_FORMS.value(options, 'partitions');
    if (partitions === null) throw new TypeError('partitions must not be null');
    if (typeof partitions !== 'object' || typeof (partitions as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function') {
      throw new TypeError(`partitions must be an iterable of TopicPartitionInfo, not ${kind(partitions)}`);
    }
    const infos = [];
    for (const p of partitions as Iterable<unknown>) {
      if (p === null) throw new TypeError('partitions must not contain null');
      if (!(p instanceof TopicPartitionInfo)) throw new TypeError(`partitions must contain only TopicPartitionInfo, not ${kind(p)}`);
      infos.push(calls.topicPartitionInfoFields(p));
    }
    const configs = ADD_TOPIC_FORMS.value(options, 'configs');
    native.mockAdminAddTopic(this.#handle, internal, name, infos, configs === null ? null : calls.stringMapPairs(configs, 'configs'));
  }

  /**
   * Marks a topic for deletion: the mock's describeTopics leaves it out from
   * then on.
   *
   * @param name - the topic's name, on its own or as `{ name }`
   * @throws `IllegalArgumentError` when the topic does not exist ("Topic
   *   <name> did not exist."), or for a form Java has not
   * @throws `TypeError` when the name is not a string
   */
  markTopicForDeletion(name: string): void;
  /**
   * The same as `markTopicForDeletion(name)`, the name given by name.
   *
   * @param options - the topic's name
   * @throws `IllegalArgumentError` when the topic does not exist ("Topic
   *   <name> did not exist."), or for a form Java has not
   * @throws `TypeError` when the name is not a string
   */
  markTopicForDeletion(options: {
    /** The topic's name. */
    name: string;
  }): void;
  markTopicForDeletion(name: unknown): void {
    MARK_TOPIC_FOR_DELETION_FORMS.match(name, arguments.length);
    const topic = MARK_TOPIC_FOR_DELETION_FORMS.value(name, 'name');
    if (topic !== null && typeof topic !== 'string') throw new TypeError(`name must be a string, not ${kind(topic)}`);
    native.mockAdminMarkTopicForDeletion(this.#handle, topic);
  }

  /**
   * The next RPCs fail their futures with a `TimeoutError`.
   *
   * @param numberOfRequest - how many RPCs time out, on its own or as `{ numberOfRequest }`
   * @throws `TypeError` or `RangeError` when it is not a Java `int`
   */
  timeoutNextRequest(numberOfRequest: number): void;
  /**
   * The same as `timeoutNextRequest(numberOfRequest)`, the parameter given by name.
   *
   * @param options - how many RPCs time out
   * @throws `TypeError` or `RangeError` when it is not a Java `int`
   */
  timeoutNextRequest(options: {
    /** How many RPCs time out */
    numberOfRequest: number;
  }): void;
  timeoutNextRequest(numberOfRequest: unknown): void {
    TIMEOUT_NEXT_REQUEST_FORMS.match(numberOfRequest, arguments.length);
    native.mockAdminTimeoutNextRequest(
      this.#handle,
      checkInt(TIMEOUT_NEXT_REQUEST_FORMS.value(numberOfRequest, 'numberOfRequest'), 'numberOfRequest'),
    );
  }

  /** @remarks The mock's close does nothing, as Java's `MockAdminClient.close(Duration)`. */
  close(): Promise<void>;
  close(options: CloseArgs): Promise<void>;
  close(timeout: Duration): Promise<void>;
  close(options?: unknown): Promise<void> {
    return calls.close(this.#handle, options, arguments.length);
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  listTopics(): ListTopicsResult;
  listTopics(args: {
    /** The options to use when listing the topics. */
    options: ListTopicsOptions;
  }): ListTopicsResult;
  listTopics(options: ListTopicsOptions): ListTopicsResult;
  listTopics(options?: unknown): ListTopicsResult {
    return calls.listTopics(this.#handle, options, arguments.length);
  }

  describeCluster(): DescribeClusterResult;
  describeCluster(args: {
    /** The options to use when getting information about the cluster. */
    options: DescribeClusterOptions;
  }): DescribeClusterResult;
  describeCluster(options: DescribeClusterOptions): DescribeClusterResult;
  describeCluster(options?: unknown): DescribeClusterResult {
    return calls.describeCluster(this.#handle, options, arguments.length);
  }

  createTopics(newTopics: Iterable<NewTopic>): CreateTopicsResult;
  createTopics(args: {
    /** The new topics to create. */
    newTopics: Iterable<NewTopic>;
    options?: never;
  }): CreateTopicsResult;
  createTopics(args: {
    /** The new topics to create. */
    newTopics: Iterable<NewTopic>;
    /** The options to use when creating the new topics. */
    options: CreateTopicsOptions;
  }): CreateTopicsResult;
  createTopics(args: unknown): CreateTopicsResult {
    return calls.createTopics(this.#handle, args, arguments.length);
  }

  deleteTopics(topics: Iterable<string> | TopicCollection): DeleteTopicsResult;
  deleteTopics(args: {
    /** The topic names, or the topics, to delete. */
    topics: Iterable<string> | TopicCollection;
    options?: never;
  }): DeleteTopicsResult;
  deleteTopics(args: {
    /** The topic names, or the topics, to delete. */
    topics: Iterable<string> | TopicCollection;
    /** The options to use when deleting the topics. */
    options: DeleteTopicsOptions;
  }): DeleteTopicsResult;
  deleteTopics(args: unknown): DeleteTopicsResult {
    return calls.deleteTopics(this.#handle, args, arguments.length);
  }

  describeTopics(topicNames: Iterable<string>): DescribeTopicsResult;
  describeTopics(args: {
    /** The names of the topics to describe. */
    topicNames: Iterable<string>;
    topics?: never;
    options?: never;
  }): DescribeTopicsResult;
  describeTopics(args: {
    /** The names of the topics to describe. */
    topicNames: Iterable<string>;
    /** The options to use when describing the topics. */
    options: DescribeTopicsOptions;
    topics?: never;
  }): DescribeTopicsResult;
  describeTopics(topics: TopicCollection): DescribeTopicsResult;
  describeTopics(args: {
    /** The topics to describe. */
    topics: TopicCollection;
    topicNames?: never;
    options?: never;
  }): DescribeTopicsResult;
  describeTopics(args: {
    /** The topics to describe. */
    topics: TopicCollection;
    /** The options to use when describing the topics. */
    options: DescribeTopicsOptions;
    topicNames?: never;
  }): DescribeTopicsResult;
  describeTopics(args: unknown): DescribeTopicsResult {
    return calls.describeTopics(this.#handle, args, arguments.length);
  }

  createPartitions(newPartitions: ReadonlyMap<string, NewPartitions>): CreatePartitionsResult;
  createPartitions(args: {
    /** The topics which should have new partitions created, and corresponding parameters for the created partitions. */
    newPartitions: ReadonlyMap<string, NewPartitions> | Readonly<Record<string, NewPartitions>>;
    options?: never;
  }): CreatePartitionsResult;
  createPartitions(args: {
    /** The topics which should have new partitions created, and corresponding parameters for the created partitions. */
    newPartitions: ReadonlyMap<string, NewPartitions> | Readonly<Record<string, NewPartitions>>;
    /** The options to use when creating the new partitions. */
    options: CreatePartitionsOptions;
  }): CreatePartitionsResult;
  createPartitions(args: unknown): CreatePartitionsResult {
    return calls.createPartitions(this.#handle, args, arguments.length);
  }

  deleteRecords(recordsToDelete: TopicPartitionMap<RecordsToDelete>): DeleteRecordsResult;
  deleteRecords(args: {
    /** The topic partitions and related offsets from which records deletion starts. */
    recordsToDelete: TopicPartitionMap<RecordsToDelete>;
    options?: never;
  }): DeleteRecordsResult;
  deleteRecords(args: {
    /** The topic partitions and related offsets from which records deletion starts. */
    recordsToDelete: TopicPartitionMap<RecordsToDelete>;
    /** The options to use when deleting records. */
    options: DeleteRecordsOptions;
  }): DeleteRecordsResult;
  deleteRecords(args: unknown): DeleteRecordsResult {
    return calls.deleteRecords(this.#handle, args, arguments.length);
  }

  describeConfigs(resources: Iterable<ConfigResource>): DescribeConfigsResult;
  describeConfigs(args: {
    /** The resources: see the relevant `ConfigResource.Type`. */
    resources: Iterable<ConfigResource>;
    options?: never;
  }): DescribeConfigsResult;
  describeConfigs(args: {
    /** The resources: see the relevant `ConfigResource.Type`. */
    resources: Iterable<ConfigResource>;
    /** The options to use. */
    options: DescribeConfigsOptions;
  }): DescribeConfigsResult;
  describeConfigs(args: unknown): DescribeConfigsResult {
    return configCalls.describeConfigs(this.#handle, args, arguments.length);
  }

  incrementalAlterConfigs(configs: ConfigResourceMap<Iterable<AlterConfigOp>>): AlterConfigsResult;
  incrementalAlterConfigs(args: {
    /** The resources with their configs. */
    configs: ConfigResourceMap<Iterable<AlterConfigOp>>;
    options?: never;
  }): AlterConfigsResult;
  incrementalAlterConfigs(args: {
    /** The resources with their configs. */
    configs: ConfigResourceMap<Iterable<AlterConfigOp>>;
    /** The options to use. */
    options: AlterConfigsOptions;
  }): AlterConfigsResult;
  incrementalAlterConfigs(args: unknown): AlterConfigsResult {
    return configCalls.incrementalAlterConfigs(this.#handle, args, arguments.length);
  }

  alterReplicaLogDirs(replicaAssignment: TopicPartitionReplicaMap<string>): AlterReplicaLogDirsResult;
  alterReplicaLogDirs(args: {
    /** The replicas with their log directory absolute path. */
    replicaAssignment: TopicPartitionReplicaMap<string>;
    options?: never;
  }): AlterReplicaLogDirsResult;
  alterReplicaLogDirs(args: {
    /** The replicas with their log directory absolute path. */
    replicaAssignment: TopicPartitionReplicaMap<string>;
    /** The options to use. */
    options: AlterReplicaLogDirsOptions;
  }): AlterReplicaLogDirsResult;
  alterReplicaLogDirs(args: unknown): AlterReplicaLogDirsResult {
    return configCalls.alterReplicaLogDirs(this.#handle, args, arguments.length);
  }

  describeLogDirs(brokers: Iterable<number>): DescribeLogDirsResult;
  describeLogDirs(args: {
    /** A list of brokers. */
    brokers: Iterable<number>;
    options?: never;
  }): DescribeLogDirsResult;
  describeLogDirs(args: {
    /** A list of brokers. */
    brokers: Iterable<number>;
    /** The options to use. */
    options: DescribeLogDirsOptions;
  }): DescribeLogDirsResult;
  describeLogDirs(args: unknown): DescribeLogDirsResult {
    return configCalls.describeLogDirs(this.#handle, args, arguments.length);
  }

  describeReplicaLogDirs(replicas: Iterable<TopicPartitionReplica>): DescribeReplicaLogDirsResult;
  describeReplicaLogDirs(args: {
    /** The replicas to query. */
    replicas: Iterable<TopicPartitionReplica>;
    options?: never;
  }): DescribeReplicaLogDirsResult;
  describeReplicaLogDirs(args: {
    /** The replicas to query. */
    replicas: Iterable<TopicPartitionReplica>;
    /** The options to use. */
    options: DescribeReplicaLogDirsOptions;
  }): DescribeReplicaLogDirsResult;
  describeReplicaLogDirs(args: unknown): DescribeReplicaLogDirsResult {
    return configCalls.describeReplicaLogDirs(this.#handle, args, arguments.length);
  }

  listConfigResources(): ListConfigResourcesResult;
  listConfigResources(args: {
    /** The set of configuration resource types to list. */
    configResourceTypes: Iterable<ConfigResource.Type>;
    /** The options to use when listing the configuration resources. */
    options: ListConfigResourcesOptions;
  }): ListConfigResourcesResult;
  listConfigResources(args?: unknown): ListConfigResourcesResult {
    return configCalls.listConfigResources(this.#handle, args, arguments.length);
  }

  electLeaders(args: {
    /** The type of election to conduct. */
    electionType: ElectionType;
    /** The topics and partitions for which to conduct elections, or null for all of them. */
    partitions: Iterable<TopicPartition> | null;
    options?: never;
  }): ElectLeadersResult;
  electLeaders(args: {
    /** The type of election to conduct. */
    electionType: ElectionType;
    /** The topics and partitions for which to conduct elections, or null for all of them. */
    partitions: Iterable<TopicPartition> | null;
    /** The options to use. */
    options: ElectLeadersOptions;
  }): ElectLeadersResult;
  electLeaders(args: unknown): ElectLeadersResult {
    return offsetCalls.electLeaders(this.#handle, args, arguments.length);
  }

  alterPartitionReassignments(reassignments: TopicPartitionMap<NewPartitionReassignment | null>): AlterPartitionReassignmentsResult;
  alterPartitionReassignments(args: {
    /** The reassignments to add, modify, or remove (null), see `NewPartitionReassignment`. */
    reassignments: TopicPartitionMap<NewPartitionReassignment | null>;
    options?: never;
  }): AlterPartitionReassignmentsResult;
  alterPartitionReassignments(args: {
    /** The reassignments to add, modify, or remove (null), see `NewPartitionReassignment`. */
    reassignments: TopicPartitionMap<NewPartitionReassignment | null>;
    /** The options to use. */
    options: AlterPartitionReassignmentsOptions;
  }): AlterPartitionReassignmentsResult;
  alterPartitionReassignments(args: unknown): AlterPartitionReassignmentsResult {
    return offsetCalls.alterPartitionReassignments(this.#handle, args, arguments.length);
  }

  listPartitionReassignments(): ListPartitionReassignmentsResult;
  listPartitionReassignments(partitions: Iterable<TopicPartition>): ListPartitionReassignmentsResult;
  listPartitionReassignments(args: {
    /** The topic partitions to list reassignments for. */
    partitions: Iterable<TopicPartition>;
    options?: never;
  }): ListPartitionReassignmentsResult;
  listPartitionReassignments(args: {
    /** The topic partitions to list reassignments for, or null for all of them. */
    partitions: Iterable<TopicPartition> | null;
    /** The options to use. */
    options: ListPartitionReassignmentsOptions;
  }): ListPartitionReassignmentsResult;
  listPartitionReassignments(args: {
    /** The options to use. */
    options: ListPartitionReassignmentsOptions;
    partitions?: never;
  }): ListPartitionReassignmentsResult;
  listPartitionReassignments(options: ListPartitionReassignmentsOptions): ListPartitionReassignmentsResult;
  listPartitionReassignments(args?: unknown): ListPartitionReassignmentsResult {
    return offsetCalls.listPartitionReassignments(this.#handle, args, arguments.length);
  }

  listOffsets(topicPartitionOffsets: TopicPartitionMap<OffsetSpec>): ListOffsetsResult;
  listOffsets(args: {
    /** The mapping from partition to the OffsetSpec to look up. */
    topicPartitionOffsets: TopicPartitionMap<OffsetSpec>;
    options?: never;
  }): ListOffsetsResult;
  listOffsets(args: {
    /** The mapping from partition to the OffsetSpec to look up. */
    topicPartitionOffsets: TopicPartitionMap<OffsetSpec>;
    /** The options to use. */
    options: ListOffsetsOptions;
  }): ListOffsetsResult;
  listOffsets(args: unknown): ListOffsetsResult {
    return offsetCalls.listOffsets(this.#handle, args, arguments.length);
  }

  listGroups(): ListGroupsResult;
  listGroups(args: {
    /** The options to use. */
    options: ListGroupsOptions;
  }): ListGroupsResult;
  listGroups(options: ListGroupsOptions): ListGroupsResult;
  listGroups(options?: unknown): ListGroupsResult {
    return groupCalls.listGroups(this.#handle, options, arguments.length);
  }

  describeConsumerGroups(groupIds: Iterable<string>): DescribeConsumerGroupsResult;
  describeConsumerGroups(args: {
    /** The IDs of the groups to describe. */
    groupIds: Iterable<string>;
    options?: never;
  }): DescribeConsumerGroupsResult;
  describeConsumerGroups(args: {
    /** The IDs of the groups to describe. */
    groupIds: Iterable<string>;
    /** The options to use. */
    options: DescribeConsumerGroupsOptions;
  }): DescribeConsumerGroupsResult;
  describeConsumerGroups(args: unknown): DescribeConsumerGroupsResult {
    return groupCalls.describeConsumerGroups(this.#handle, args, arguments.length);
  }

  describeClassicGroups(groupIds: Iterable<string>): DescribeClassicGroupsResult;
  describeClassicGroups(args: {
    /** The IDs of the groups to describe. */
    groupIds: Iterable<string>;
    options?: never;
  }): DescribeClassicGroupsResult;
  describeClassicGroups(args: {
    /** The IDs of the groups to describe. */
    groupIds: Iterable<string>;
    /** The options to use. */
    options: DescribeClassicGroupsOptions;
  }): DescribeClassicGroupsResult;
  describeClassicGroups(args: unknown): DescribeClassicGroupsResult {
    return groupCalls.describeClassicGroups(this.#handle, args, arguments.length);
  }

  listConsumerGroupOffsets(groupId: string): ListConsumerGroupOffsetsResult;
  listConsumerGroupOffsets(args: {
    /** The group to list the offsets of. */
    groupId: string;
    groupSpecs?: never;
    options?: never;
  }): ListConsumerGroupOffsetsResult;
  listConsumerGroupOffsets(args: {
    /** The group to list the offsets of. */
    groupId: string;
    /** The options to use when listing the consumer group offsets. */
    options: ListConsumerGroupOffsetsOptions;
    groupSpecs?: never;
  }): ListConsumerGroupOffsetsResult;
  listConsumerGroupOffsets(groupSpecs: ReadonlyMap<string, ListConsumerGroupOffsetsSpec>): ListConsumerGroupOffsetsResult;
  listConsumerGroupOffsets(args: {
    /** Map of consumer group ids to a spec that specifies the topic partitions of the group to list offsets for. */
    groupSpecs: ReadonlyMap<string, ListConsumerGroupOffsetsSpec> | Readonly<Record<string, ListConsumerGroupOffsetsSpec>>;
    groupId?: never;
    options?: never;
  }): ListConsumerGroupOffsetsResult;
  listConsumerGroupOffsets(args: {
    /** Map of consumer group ids to a spec that specifies the topic partitions of the group to list offsets for. */
    groupSpecs: ReadonlyMap<string, ListConsumerGroupOffsetsSpec> | Readonly<Record<string, ListConsumerGroupOffsetsSpec>>;
    /** The options to use when listing the consumer group offsets. */
    options: ListConsumerGroupOffsetsOptions;
    groupId?: never;
  }): ListConsumerGroupOffsetsResult;
  listConsumerGroupOffsets(args?: unknown): ListConsumerGroupOffsetsResult {
    return groupCalls.listConsumerGroupOffsets(this.#handle, args, arguments.length);
  }

  alterConsumerGroupOffsets(args: {
    /** The group for which to alter offsets. */
    groupId: string;
    /** A map of offsets by partition with associated metadata. Partitions not specified in the map are ignored. */
    offsets: TopicPartitionMap<OffsetAndMetadata>;
    options?: never;
  }): AlterConsumerGroupOffsetsResult;
  alterConsumerGroupOffsets(args: {
    /** The group for which to alter offsets. */
    groupId: string;
    /** A map of offsets by partition with associated metadata. Partitions not specified in the map are ignored. */
    offsets: TopicPartitionMap<OffsetAndMetadata>;
    /** The options to use. */
    options: AlterConsumerGroupOffsetsOptions;
  }): AlterConsumerGroupOffsetsResult;
  alterConsumerGroupOffsets(args: unknown): AlterConsumerGroupOffsetsResult {
    return groupCalls.alterConsumerGroupOffsets(this.#handle, args, arguments.length);
  }

  deleteConsumerGroupOffsets(args: {
    /** The group to delete the offsets of. */
    groupId: string;
    /** The partitions whose committed offsets to delete. */
    partitions: Iterable<TopicPartition>;
    options?: never;
  }): DeleteConsumerGroupOffsetsResult;
  deleteConsumerGroupOffsets(args: {
    /** The group to delete the offsets of. */
    groupId: string;
    /** The partitions whose committed offsets to delete. */
    partitions: Iterable<TopicPartition>;
    /** The options to use. */
    options: DeleteConsumerGroupOffsetsOptions;
  }): DeleteConsumerGroupOffsetsResult;
  deleteConsumerGroupOffsets(args: unknown): DeleteConsumerGroupOffsetsResult {
    return groupCalls.deleteConsumerGroupOffsets(this.#handle, args, arguments.length);
  }

  deleteConsumerGroups(groupIds: Iterable<string>): DeleteConsumerGroupsResult;
  deleteConsumerGroups(args: {
    /** The IDs of the groups to delete. */
    groupIds: Iterable<string>;
    options?: never;
  }): DeleteConsumerGroupsResult;
  deleteConsumerGroups(args: {
    /** The IDs of the groups to delete. */
    groupIds: Iterable<string>;
    /** The options to use. */
    options: DeleteConsumerGroupsOptions;
  }): DeleteConsumerGroupsResult;
  deleteConsumerGroups(args: unknown): DeleteConsumerGroupsResult {
    return groupCalls.deleteConsumerGroups(this.#handle, args, arguments.length);
  }

  removeMembersFromConsumerGroup(args: {
    /** The ID of the group to remove member from. */
    groupId: string;
    /** The options to carry removing members' information. */
    options: RemoveMembersFromConsumerGroupOptions;
  }): RemoveMembersFromConsumerGroupResult;
  removeMembersFromConsumerGroup(args?: unknown): RemoveMembersFromConsumerGroupResult {
    return groupCalls.removeMembersFromConsumerGroup(this.#handle, args, arguments.length);
  }

  describeAcls(filter: AclBindingFilter): DescribeAclsResult;
  describeAcls(args: {
    /** The filter to use. */
    filter: AclBindingFilter;
    options?: never;
  }): DescribeAclsResult;
  describeAcls(args: {
    /** The filter to use. */
    filter: AclBindingFilter;
    /** The options to use. */
    options: DescribeAclsOptions;
  }): DescribeAclsResult;
  describeAcls(args: unknown): DescribeAclsResult {
    return securityCalls.describeAcls(this.#handle, args, arguments.length);
  }

  createAcls(acls: Iterable<AclBinding>): CreateAclsResult;
  createAcls(args: {
    /** The ACLs to create */
    acls: Iterable<AclBinding>;
    options?: never;
  }): CreateAclsResult;
  createAcls(args: {
    /** The ACLs to create */
    acls: Iterable<AclBinding>;
    /** The options to use. */
    options: CreateAclsOptions;
  }): CreateAclsResult;
  createAcls(args: unknown): CreateAclsResult {
    return securityCalls.createAcls(this.#handle, args, arguments.length);
  }

  deleteAcls(filters: Iterable<AclBindingFilter>): DeleteAclsResult;
  deleteAcls(args: {
    /** The filters to use. */
    filters: Iterable<AclBindingFilter>;
    options?: never;
  }): DeleteAclsResult;
  deleteAcls(args: {
    /** The filters to use. */
    filters: Iterable<AclBindingFilter>;
    /** The options to use. */
    options: DeleteAclsOptions;
  }): DeleteAclsResult;
  deleteAcls(args: unknown): DeleteAclsResult {
    return securityCalls.deleteAcls(this.#handle, args, arguments.length);
  }

  createDelegationToken(): CreateDelegationTokenResult;
  createDelegationToken(args: {
    /** The options to use. */
    options: CreateDelegationTokenOptions;
  }): CreateDelegationTokenResult;
  createDelegationToken(options: CreateDelegationTokenOptions): CreateDelegationTokenResult;
  createDelegationToken(options?: unknown): CreateDelegationTokenResult {
    return securityCalls.createDelegationToken(this.#handle, options, arguments.length);
  }

  renewDelegationToken(hmac: Uint8Array): RenewDelegationTokenResult;
  renewDelegationToken(args: {
    /** HMAC of the Delegation token */
    hmac: Uint8Array;
    options?: never;
  }): RenewDelegationTokenResult;
  renewDelegationToken(args: {
    /** HMAC of the Delegation token */
    hmac: Uint8Array;
    /** The options to use. */
    options: RenewDelegationTokenOptions;
  }): RenewDelegationTokenResult;
  renewDelegationToken(args: unknown): RenewDelegationTokenResult {
    return securityCalls.renewDelegationToken(this.#handle, args, arguments.length);
  }

  expireDelegationToken(hmac: Uint8Array): ExpireDelegationTokenResult;
  expireDelegationToken(args: {
    /** HMAC of the Delegation token */
    hmac: Uint8Array;
    options?: never;
  }): ExpireDelegationTokenResult;
  expireDelegationToken(args: {
    /** HMAC of the Delegation token */
    hmac: Uint8Array;
    /** The options to use. */
    options: ExpireDelegationTokenOptions;
  }): ExpireDelegationTokenResult;
  expireDelegationToken(args: unknown): ExpireDelegationTokenResult {
    return securityCalls.expireDelegationToken(this.#handle, args, arguments.length);
  }

  describeDelegationToken(): DescribeDelegationTokenResult;
  describeDelegationToken(args: {
    /** The options to use. */
    options: DescribeDelegationTokenOptions;
  }): DescribeDelegationTokenResult;
  describeDelegationToken(options: DescribeDelegationTokenOptions): DescribeDelegationTokenResult;
  describeDelegationToken(options?: unknown): DescribeDelegationTokenResult {
    return securityCalls.describeDelegationToken(this.#handle, options, arguments.length);
  }

  describeClientQuotas(filter: ClientQuotaFilter): DescribeClientQuotasResult;
  describeClientQuotas(args: {
    /** the filter to apply to match entities */
    filter: ClientQuotaFilter;
    options?: never;
  }): DescribeClientQuotasResult;
  describeClientQuotas(args: {
    /** the filter to apply to match entities */
    filter: ClientQuotaFilter;
    /** The options to use. */
    options: DescribeClientQuotasOptions;
  }): DescribeClientQuotasResult;
  describeClientQuotas(args: unknown): DescribeClientQuotasResult {
    return securityCalls.describeClientQuotas(this.#handle, args, arguments.length);
  }

  alterClientQuotas(entries: Iterable<ClientQuotaAlteration>): AlterClientQuotasResult;
  alterClientQuotas(args: {
    /** the alterations to perform */
    entries: Iterable<ClientQuotaAlteration>;
    options?: never;
  }): AlterClientQuotasResult;
  alterClientQuotas(args: {
    /** the alterations to perform */
    entries: Iterable<ClientQuotaAlteration>;
    /** The options to use. */
    options: AlterClientQuotasOptions;
  }): AlterClientQuotasResult;
  alterClientQuotas(args: unknown): AlterClientQuotasResult {
    return securityCalls.alterClientQuotas(this.#handle, args, arguments.length);
  }

  describeUserScramCredentials(): DescribeUserScramCredentialsResult;
  describeUserScramCredentials(users: Iterable<string> | null): DescribeUserScramCredentialsResult;
  describeUserScramCredentials(args: {
    /** The users for which credentials are to be described; all users' credentials are described if null or empty. */
    users: Iterable<string> | null;
    options?: never;
  }): DescribeUserScramCredentialsResult;
  describeUserScramCredentials(args: {
    /** The users for which credentials are to be described; all users' credentials are described if null or empty. */
    users: Iterable<string> | null;
    /** The options to use when describing the credentials. */
    options: DescribeUserScramCredentialsOptions;
  }): DescribeUserScramCredentialsResult;
  describeUserScramCredentials(args?: unknown): DescribeUserScramCredentialsResult {
    return securityCalls.describeUserScramCredentials(this.#handle, args, arguments.length);
  }

  alterUserScramCredentials(alterations: Iterable<UserScramCredentialAlteration>): AlterUserScramCredentialsResult;
  alterUserScramCredentials(args: {
    /** the alterations to be applied */
    alterations: Iterable<UserScramCredentialAlteration>;
    options?: never;
  }): AlterUserScramCredentialsResult;
  alterUserScramCredentials(args: {
    /** the alterations to be applied */
    alterations: Iterable<UserScramCredentialAlteration>;
    /** The options to use. */
    options: AlterUserScramCredentialsOptions;
  }): AlterUserScramCredentialsResult;
  alterUserScramCredentials(args: unknown): AlterUserScramCredentialsResult {
    return securityCalls.alterUserScramCredentials(this.#handle, args, arguments.length);
  }

  describeProducers(partitions: Iterable<TopicPartition>): DescribeProducersResult;
  describeProducers(args: {
    /** The set of partitions to query */
    partitions: Iterable<TopicPartition>;
    options?: never;
  }): DescribeProducersResult;
  describeProducers(args: {
    /** The set of partitions to query */
    partitions: Iterable<TopicPartition>;
    /** The options to use. */
    options: DescribeProducersOptions;
  }): DescribeProducersResult;
  describeProducers(args: unknown): DescribeProducersResult {
    return transactionCalls.describeProducers(this.#handle, args, arguments.length);
  }

  describeTransactions(transactionalIds: Iterable<string>): DescribeTransactionsResult;
  describeTransactions(args: {
    /** The set of transactional IDs to query */
    transactionalIds: Iterable<string>;
    options?: never;
  }): DescribeTransactionsResult;
  describeTransactions(args: {
    /** The set of transactional IDs to query */
    transactionalIds: Iterable<string>;
    /** The options to use. */
    options: DescribeTransactionsOptions;
  }): DescribeTransactionsResult;
  describeTransactions(args: unknown): DescribeTransactionsResult {
    return transactionCalls.describeTransactions(this.#handle, args, arguments.length);
  }

  abortTransaction(spec: AbortTransactionSpec): AbortTransactionResult;
  abortTransaction(args: {
    /** The transaction specification including topic partition and producer details */
    spec: AbortTransactionSpec;
    options?: never;
  }): AbortTransactionResult;
  abortTransaction(args: {
    /** The transaction specification including topic partition and producer details */
    spec: AbortTransactionSpec;
    /** The options to use. */
    options: AbortTransactionOptions;
  }): AbortTransactionResult;
  abortTransaction(args: unknown): AbortTransactionResult {
    return transactionCalls.abortTransaction(this.#handle, args, arguments.length);
  }

  listTransactions(): ListTransactionsResult;
  listTransactions(args: {
    /** The options to use. */
    options: ListTransactionsOptions;
  }): ListTransactionsResult;
  listTransactions(options: ListTransactionsOptions): ListTransactionsResult;
  listTransactions(options?: unknown): ListTransactionsResult {
    return transactionCalls.listTransactions(this.#handle, options, arguments.length);
  }

  fenceProducers(transactionalIds: Iterable<string>): FenceProducersResult;
  fenceProducers(args: {
    /** The IDs of the producers to fence. */
    transactionalIds: Iterable<string>;
    options?: never;
  }): FenceProducersResult;
  fenceProducers(args: {
    /** The IDs of the producers to fence. */
    transactionalIds: Iterable<string>;
    /** The options to use. */
    options: FenceProducersOptions;
  }): FenceProducersResult;
  fenceProducers(args: unknown): FenceProducersResult {
    return transactionCalls.fenceProducers(this.#handle, args, arguments.length);
  }

  forceTerminateTransaction(transactionalId: string): TerminateTransactionResult;
  forceTerminateTransaction(args: {
    /** The ID of the transaction to terminate. */
    transactionalId: string;
    options?: never;
  }): TerminateTransactionResult;
  forceTerminateTransaction(args: {
    /** The ID of the transaction to terminate. */
    transactionalId: string;
    /** The options to use. */
    options: TerminateTransactionOptions;
  }): TerminateTransactionResult;
  forceTerminateTransaction(args: unknown): TerminateTransactionResult {
    return transactionCalls.forceTerminateTransaction(this.#handle, args, arguments.length);
  }

  describeFeatures(): DescribeFeaturesResult;
  describeFeatures(args: {
    /** The options to use. */
    options: DescribeFeaturesOptions;
  }): DescribeFeaturesResult;
  describeFeatures(options: DescribeFeaturesOptions): DescribeFeaturesResult;
  describeFeatures(options?: unknown): DescribeFeaturesResult {
    return featureCalls.describeFeatures(this.#handle, options, arguments.length);
  }

  updateFeatures(featureUpdates: ReadonlyMap<string, FeatureUpdate>): UpdateFeaturesResult;
  updateFeatures(args: {
    /** the map of finalized feature name to `FeatureUpdate` */
    featureUpdates: ReadonlyMap<string, FeatureUpdate> | Readonly<Record<string, FeatureUpdate>>;
    options?: never;
  }): UpdateFeaturesResult;
  updateFeatures(args: {
    /** the map of finalized feature name to `FeatureUpdate` */
    featureUpdates: ReadonlyMap<string, FeatureUpdate> | Readonly<Record<string, FeatureUpdate>>;
    /** The options to use. */
    options: UpdateFeaturesOptions;
  }): UpdateFeaturesResult;
  updateFeatures(args: unknown): UpdateFeaturesResult {
    return featureCalls.updateFeatures(this.#handle, args, arguments.length);
  }

  unregisterController(controllerId: number): UnregisterControllerResult;
  unregisterController(args: {
    /** the controller id to unregister. */
    controllerId: number;
    options?: never;
  }): UnregisterControllerResult;
  unregisterController(args: {
    /** the controller id to unregister. */
    controllerId: number;
    /** The options to use. */
    options: UnregisterControllerOptions;
  }): UnregisterControllerResult;
  unregisterController(args: unknown): UnregisterControllerResult {
    return featureCalls.unregisterController(this.#handle, args, arguments.length);
  }

  /**
   * Makes a topic visible to `describeTopics` only after the given number of
   * describe calls.
   *
   * @param options - the topic's name and the number of describe calls
   * @throws `IllegalArgumentError` when there is no such topic ("No such
   *   topic as <topicName>")
   * @throws `TypeError` or `RangeError` for a value of the wrong type
   */
  setFetchesRemainingUntilVisible(options: {
    /** The topic's name. */
    topicName: string;
    /** The number of describe calls before the topic is visible. */
    fetchesRemainingUntilVisible: number;
  }): void {
    SET_FETCHES_REMAINING_UNTIL_VISIBLE_FORMS.match(options, arguments.length);
    const topicName = SET_FETCHES_REMAINING_UNTIL_VISIBLE_FORMS.value(options, 'topicName');
    if (topicName !== null && typeof topicName !== 'string') throw new TypeError(`topicName must be a string, not ${kind(topicName)}`);
    const count = checkInt(
      SET_FETCHES_REMAINING_UNTIL_VISIBLE_FORMS.value(options, 'fetchesRemainingUntilVisible'),
      'fetchesRemainingUntilVisible',
    );
    native.mockAdminSetFetchesRemainingUntilVisible(this.#handle, topicName, count);
  }

  /**
   * Seeds the beginning offsets `listOffsets` answers for `OffsetSpec.earliest()`.
   *
   * @param newOffsets - the beginning offset of each partition
   * @throws `TypeError` for a null map, key or offset, or a value of the
   *   wrong type
   * @throws `RangeError` for a number beyond 2^53 - 1 or a bigint beyond
   *   Java's `long`
   *
   * @remarks *(deviation)* A null key, a key with a null topic or a null
   * offset is a `TypeError` at the call, where Java's mock stores it. Why: the
   * C API takes each key as a handle and each offset as a `long`. Goes when:
   * the FFI takes them.
   */
  updateBeginningOffsets(newOffsets: TopicPartitionMap<bigint | number>): void;
  /**
   * The same as `updateBeginningOffsets(newOffsets)`, the offsets given by name.
   *
   * @param options - the beginning offset of each partition
   * @throws `TypeError` for a null map, key or offset, or a value of the
   *   wrong type
   * @throws `RangeError` for a number beyond 2^53 - 1 or a bigint beyond
   *   Java's `long`
   *
   * @remarks *(deviation)* A null key, a key with a null topic or a null
   * offset is a `TypeError` at the call, where Java's mock stores it. Why: the
   * C API takes each key as a handle and each offset as a `long`. Goes when:
   * the FFI takes them.
   */
  updateBeginningOffsets(options: {
    /** The beginning offset of each partition. */
    newOffsets: TopicPartitionMap<bigint | number>;
  }): void;
  updateBeginningOffsets(options: unknown): void {
    UPDATE_BEGINNING_OFFSETS_FORMS.match(options, arguments.length);
    native.mockAdminUpdateOffsets(this.#handle, false, offsetRows(UPDATE_BEGINNING_OFFSETS_FORMS.value(options, 'newOffsets')));
  }

  /**
   * Seeds the end offsets `listOffsets` answers for the other specs.
   *
   * @param newOffsets - the end offset of each partition
   * @throws `TypeError` for a null map, key or offset, or a value of the
   *   wrong type
   * @throws `RangeError` for a number beyond 2^53 - 1 or a bigint beyond
   *   Java's `long`
   *
   * @remarks *(deviation)* A null key, a key with a null topic or a null
   * offset is a `TypeError` at the call, where Java's mock stores it. Why: the
   * C API takes each key as a handle and each offset as a `long`. Goes when:
   * the FFI takes them.
   */
  updateEndOffsets(newOffsets: TopicPartitionMap<bigint | number>): void;
  /**
   * The same as `updateEndOffsets(newOffsets)`, the offsets given by name.
   *
   * @param options - the end offset of each partition
   * @throws `TypeError` for a null map, key or offset, or a value of the
   *   wrong type
   * @throws `RangeError` for a number beyond 2^53 - 1 or a bigint beyond
   *   Java's `long`
   *
   * @remarks *(deviation)* A null key, a key with a null topic or a null
   * offset is a `TypeError` at the call, where Java's mock stores it. Why: the
   * C API takes each key as a handle and each offset as a `long`. Goes when:
   * the FFI takes them.
   */
  updateEndOffsets(options: {
    /** The end offset of each partition. */
    newOffsets: TopicPartitionMap<bigint | number>;
  }): void;
  updateEndOffsets(options: unknown): void {
    UPDATE_END_OFFSETS_FORMS.match(options, arguments.length);
    native.mockAdminUpdateOffsets(this.#handle, true, offsetRows(UPDATE_END_OFFSETS_FORMS.value(options, 'newOffsets')));
  }

  /**
   * Seeds the committed offsets `listConsumerGroupOffsets` answers, for any
   * group (Java's mock assumes each test works on a single group).
   *
   * @param newOffsets - the committed offset of each partition
   * @throws `TypeError` for a null map, key or offset, or a value of the
   *   wrong type
   * @throws `RangeError` for a number beyond 2^53 - 1 or a bigint beyond
   *   Java's `long`
   *
   * @remarks *(deviation)* A null key, a key with a null topic or a null
   * offset is a `TypeError` at the call, where Java's mock stores it. Why: the
   * C API takes each key as a handle and each offset as a `long`. Goes when:
   * the FFI takes them.
   */
  updateConsumerGroupOffsets(newOffsets: TopicPartitionMap<bigint | number>): void;
  /**
   * The same as `updateConsumerGroupOffsets(newOffsets)`, the offsets given by name.
   *
   * @param options - the committed offset of each partition
   * @throws `TypeError` for a null map, key or offset, or a value of the
   *   wrong type
   * @throws `RangeError` for a number beyond 2^53 - 1 or a bigint beyond
   *   Java's `long`
   *
   * @remarks *(deviation)* A null key, a key with a null topic or a null
   * offset is a `TypeError` at the call, where Java's mock stores it. Why: the
   * C API takes each key as a handle and each offset as a `long`. Goes when:
   * the FFI takes them.
   */
  updateConsumerGroupOffsets(options: {
    /** The committed offset of each partition. */
    newOffsets: TopicPartitionMap<bigint | number>;
  }): void;
  updateConsumerGroupOffsets(options: unknown): void {
    UPDATE_CONSUMER_GROUP_OFFSETS_FORMS.match(options, arguments.length);
    native.mockAdminUpdateConsumerGroupOffsets(this.#handle, offsetRows(UPDATE_CONSUMER_GROUP_OFFSETS_FORMS.value(options, 'newOffsets')));
  }
}

/**
 * A `Map<TopicPartition, Long>` given to the mock (a `TopicPartitionMap`), as
 * `[[topic, partition], offset]` rows for the addon.
 *
 * @param value - the map given
 * @returns the rows
 * @throws `TypeError` for null, another value, a null key, topic or offset
 */
function offsetRows(value: unknown): Array<[[string, number], bigint]> {
  return offsetCalls.topicPartitionMapEntries(value, 'newOffsets').map(([tp, offset]) => {
    if (tp === null) throw new TypeError('newOffsets must not contain a null key');
    if (tp.topic === null) throw new TypeError('newOffsets must not contain a TopicPartition with a null topic');
    if (offset === null) throw new TypeError('newOffsets must not contain null');
    return [[tp.topic, tp.partition], checkLong(offset, 'an offset')];
  });
}

const isInt = (value: unknown): boolean => typeof value === 'number';
const isMapLike = (value: unknown): boolean => value === null || value instanceof Map;
const NUM_BROKERS = new Setter('numBrokers', null, isInt);
const BROKERS = new Setter('brokers', null, (value) => value === null || (typeof value === 'object' && Symbol.iterator in value));
const CONTROLLER = new Setter('controller', null, isInt, 'index');
const CLUSTER_ID = new Setter('clusterId', null, (value) => value === null || typeof value === 'string');
const BROKER_LOG_DIRS = new Setter('brokerLogDirs', null, (value) => value === null || isIterable(value));
const DEFAULT_REPLICATION_FACTOR = new Setter('defaultReplicationFactor', null, isInt);
const USING_RAFT_CONTROLLER = new Setter('usingRaftController', null, (value) => typeof value === 'boolean');
const DEFAULT_PARTITIONS = new Setter('defaultPartitions', null, isInt, 'numPartitions');
const FEATURE_LEVELS = new Setter('featureLevels', null, isMapLike);
const MIN_SUPPORTED_FEATURE_LEVELS = new Setter('minSupportedFeatureLevels', null, isMapLike);
const MAX_SUPPORTED_FEATURE_LEVELS = new Setter('maxSupportedFeatureLevels', null, isMapLike);
const DEFAULT_GROUP_CONFIGS = new Setter('defaultGroupConfigs', null, isMapLike);
const BUILD_FORMS = new JavaForms('build', [[]]);

/**
 * A map keyed by strings (a `Map` or a plain object) as `[key, value]` pairs,
 * each value checked by `check`, or null.
 *
 * @param value - the map given
 * @param name - the parameter's name
 * @param check - the value's check
 * @returns the pairs, or null
 */
function stringMapPairs<V>(value: unknown, name: string, check: (v: unknown) => V): Array<[string, V]> | null {
  if (value === null) return null;
  let entries: Array<[unknown, unknown]>;
  if (value instanceof Map) entries = [...(value as Map<unknown, unknown>)];
  else if (isPlainObject(value)) entries = Object.entries(value);
  else throw new TypeError(`${name} must be a Map or a plain object, not ${kind(value)}`);
  return entries.map(([k, v]) => {
    if (typeof k !== 'string') throw new TypeError(`${name} must be keyed by strings`);
    return [k, check(v)];
  });
}

/**
 * A feature level, a Java `short`.
 *
 * @param value - the value given
 * @returns the level
 */
const featureLevel = (value: unknown): number => checkShort(value, 'a feature level');

/**
 * A config value of `defaultGroupConfigs`.
 *
 * @param value - the value given
 * @returns the value
 */
function checkString(value: unknown): string {
  if (typeof value !== 'string') throw new TypeError(`a group config value must be a string, not ${kind(value)}`);
  return value;
}

/**
 * Java's `brokerLogDirs`: an array of arrays of strings, or null.
 *
 * @param value - the value given
 * @returns the arrays
 */
function logDirs(value: unknown): string[][] | null {
  if (value === null) return null;
  // Java's List<List<String>>: any iterable of iterables of strings, never a
  // string (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family).
  const what = 'brokerLogDirs must be an iterable of iterables of strings';
  if (!isIterable(value)) throw new TypeError(`${what}, not ${kind(value)}`);
  return [...(value as Iterable<unknown>)].map((dirs) => {
    if (!isIterable(dirs)) throw new TypeError(`${what}, not one holding ${kind(dirs)}`);
    const list = [...(dirs as Iterable<unknown>)];
    for (const d of list) if (typeof d !== 'string') throw new TypeError(`${what}, not one holding ${kind(d)}`);
    return list as string[];
  });
}

/**
 * Whether `value` is an iterable a Java collection is taken as: an object
 * with an iterator, never a string.
 *
 * @param value - the value given
 * @returns whether it is such an iterable
 */
function isIterable(value: unknown): boolean {
  return value !== null && typeof value === 'object' && typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] === 'function';
}

// eslint-disable-next-line @typescript-eslint/no-namespace -- Java's nested class MockAdminClient.Builder, reached through the class name as in Java
export namespace MockAdminClient {
  /**
   * Java's `MockAdminClient.Builder`: a builder of mocks.
   *
   * @remarks It holds the C FFI's builder, whose setters run at once, as
   * Java's do. A null argument is a `TypeError` from the setter. For
   * `brokers(null)` Java's setter throws too. Java's `clusterId(null)` and
   * `brokerLogDirs(null)` store the null, and `build()` builds a mock with it;
   * its `featureLevels`, `minSupportedFeatureLevels`,
   * `maxSupportedFeatureLevels` and `defaultGroupConfigs` store the null, and
   * `build()` throws `NullPointerException` *(deviation: the setter throws.
   * Why: the FFI checks its mandatory parameters. Goes when: the FFI's builder
   * takes the nulls)*.
   */
  export class Builder {
    readonly #native = native.mockAdminBuilderNew();

    /**
     * Java's `new Builder()`, which calls `numBrokers(1)`, then the setter of
     * each key given, in Java's field order.
     *
     * @param options - the setters' values, each key optional
     * @throws `IllegalArgumentError` for a key the builder has not, or a
     *   setter's own `IllegalArgumentError`
     * @throws `TypeError` for a value of the wrong type, or a setter's own
     *   `TypeError`
     * @throws `RangeError` for a number outside the Java type of its setter
     *
     * @remarks *(deviation)* Java builds it through its setters only. Why:
     * one call builds a configured object; the setters stay.
     */
    constructor(options?: {
      /**
       * The cluster id.
       *
       * @defaultValue `'I4ZmrWqfT2e-upky_4fdPA'`
       */
      clusterId?: string;
      /** The brokers, any iterable of Node (`numBrokers(1)`'s one broker by default). */
      brokers?: Iterable<Node>;
      /** The index of the controller among the brokers (the first broker by default). */
      controller?: number;
      /** The log dirs of each broker (the default log dir of each by default). */
      brokerLogDirs?: ReadonlyArray<ReadonlyArray<string>>;
      /** The default partition count, a Java `short` (1 by default). */
      defaultPartitions?: number;
      /**
       * Whether the mock uses a KRaft controller.
       *
       * @defaultValue `false`
       */
      usingRaftController?: boolean;
      /** The default replication factor (`min(brokers, 3)` by default). */
      defaultReplicationFactor?: number;
      /** The finalized feature levels, by feature name (none by default). */
      featureLevels?: ReadonlyMap<string, number> | Readonly<Record<string, number>>;
      /** The minimum supported feature levels (none by default). */
      minSupportedFeatureLevels?: ReadonlyMap<string, number> | Readonly<Record<string, number>>;
      /** The maximum supported feature levels (none by default). */
      maxSupportedFeatureLevels?: ReadonlyMap<string, number> | Readonly<Record<string, number>>;
      /** The group configs overlaid onto every group's (none by default). */
      defaultGroupConfigs?: ReadonlyMap<string, string> | Readonly<Record<string, string>>;
    }) {
      applyOptionKeys('Builder', options, arguments.length, [
        // Each setter is given its parameter by name: a plain-object map
        // given alone would be read as the options object.
        ['clusterId', (v) => this.clusterId({ clusterId: v as string })],
        ['brokers', (v) => this.brokers({ brokers: v as Iterable<Node> })],
        ['controller', (v) => this.controller({ index: v as number })],
        ['brokerLogDirs', (v) => this.brokerLogDirs({ brokerLogDirs: v as string[][] })],
        ['defaultPartitions', (v) => this.defaultPartitions({ numPartitions: v as number })],
        ['usingRaftController', (v) => this.usingRaftController({ usingRaftController: v as boolean })],
        ['defaultReplicationFactor', (v) => this.defaultReplicationFactor({ defaultReplicationFactor: v as number })],
        ['featureLevels', (v) => this.featureLevels({ featureLevels: v as Map<string, number> })],
        ['minSupportedFeatureLevels', (v) => this.minSupportedFeatureLevels({ minSupportedFeatureLevels: v as Map<string, number> })],
        ['maxSupportedFeatureLevels', (v) => this.maxSupportedFeatureLevels({ maxSupportedFeatureLevels: v as Map<string, number> })],
        ['defaultGroupConfigs', (v) => this.defaultGroupConfigs({ defaultGroupConfigs: v as Map<string, string> })],
      ]);
    }

    /**
     * Sets the cluster id.
     *
     * @param clusterId - the cluster id, on its own or as `{ clusterId }`
     * @returns this builder
     * @throws `IllegalArgumentError` for a form Java has not
     * @throws `TypeError` when the cluster id is null
     */
    clusterId(clusterId: string): this;
    /**
     * The same as `clusterId(clusterId)`, the parameter given by name.
     *
     * @param options - the cluster id
     * @returns this builder
     * @throws `IllegalArgumentError` for a form Java has not
     * @throws `TypeError` when the cluster id is null
     */
    clusterId(options: {
      /** The cluster id. */
      clusterId: string;
    }): this;
    clusterId(clusterId: unknown): this {
      native.mockAdminBuilderSet(this.#native, 'clusterId', CLUSTER_ID.value(clusterId, arguments.length));
      return this;
    }

    /**
     * Sets the brokers: `numBrokers(brokers.size())`, then the list itself.
     *
     * @param brokers - the brokers, any iterable of Node, on its own or as `{ brokers }`
     * @returns this builder
     * @throws `IllegalArgumentError` when the log dirs set are fewer than the brokers
     * @throws `TypeError` when an element is not a Node
     */
    brokers(brokers: Iterable<Node>): this;
    /**
     * The same as `brokers(brokers)`, the parameter given by name.
     *
     * @param options - the brokers, any iterable of Node
     * @returns this builder
     * @throws `IllegalArgumentError` when the log dirs set are fewer than the brokers
     * @throws `TypeError` when an element is not a Node
     */
    brokers(options: {
      /** The brokers, any iterable of Node. */
      brokers: Iterable<Node>;
    }): this;
    brokers(brokers: unknown): this {
      native.mockAdminBuilderSet(this.#native, 'brokers', nodesFields(BROKERS.value(brokers, arguments.length), 'brokers'));
      return this;
    }

    /**
     * Sets the broker count: shrinking keeps the first brokers, growing adds
     * `Node(id, "localhost", 1000 + id)` with the default log dirs.
     *
     * @param numBrokers - the broker count, on its own or as `{ numBrokers }`
     * @returns this builder
     * @throws `IllegalArgumentError` for a negative count, or when shrinking
     *   past a shorter log-dir list
     * @throws `TypeError` when the count is not an integer
     * @throws `RangeError` when it is outside Java's `int`
     */
    numBrokers(numBrokers: number): this;
    /**
     * The same as `numBrokers(numBrokers)`, the parameter given by name.
     *
     * @param options - the broker count
     * @returns this builder
     * @throws `IllegalArgumentError` for a negative count, or when shrinking
     *   past a shorter log-dir list
     * @throws `TypeError` when the count is not an integer
     * @throws `RangeError` when it is outside Java's `int`
     */
    numBrokers(options: {
      /** The broker count. */
      numBrokers: number;
    }): this;
    numBrokers(numBrokers: unknown): this {
      native.mockAdminBuilderSet(this.#native, 'numBrokers', checkInt(NUM_BROKERS.value(numBrokers, arguments.length), 'numBrokers'));
      return this;
    }

    /**
     * Makes the broker at `index` the controller.
     *
     * @param index - the broker's index, on its own or as `{ index }`
     * @returns this builder
     * @throws `IllegalArgumentError` when the index is out of the broker list
     * @throws `TypeError` when the index is not an integer
     * @throws `RangeError` when it is outside Java's `int`
     */
    controller(index: number): this;
    /**
     * The same as `controller(index)`, the parameter given by name.
     *
     * @param options - the broker's index
     * @returns this builder
     * @throws `IllegalArgumentError` when the index is out of the broker list
     * @throws `TypeError` when the index is not an integer
     * @throws `RangeError` when it is outside Java's `int`
     */
    controller(options: {
      /** The broker's index. */
      index: number;
    }): this;
    controller(index: unknown): this {
      native.mockAdminBuilderSet(this.#native, 'controller', checkInt(CONTROLLER.value(index, arguments.length), 'index'));
      return this;
    }

    /**
     * Sets the log dirs of each broker; their count is not checked against
     * the broker count.
     *
     * @param brokerLogDirs - the log dirs of each broker, any iterable of iterables, on its own or as `{ brokerLogDirs }`
     * @returns this builder
     * @throws `IllegalArgumentError` for a form Java has not
     * @throws `TypeError` when the list is null or holds a value that is not a string
     */
    brokerLogDirs(brokerLogDirs: Iterable<Iterable<string>>): this;
    /**
     * The same as `brokerLogDirs(brokerLogDirs)`, the parameter given by name.
     *
     * @param options - the log dirs of each broker
     * @returns this builder
     * @throws `IllegalArgumentError` for a form Java has not
     * @throws `TypeError` when the list is null or holds a value that is not a string
     */
    brokerLogDirs(options: {
      /** The log dirs of each broker, any iterable of iterables. */
      brokerLogDirs: Iterable<Iterable<string>>;
    }): this;
    brokerLogDirs(brokerLogDirs: unknown): this {
      native.mockAdminBuilderSet(this.#native, 'brokerLogDirs', logDirs(BROKER_LOG_DIRS.value(brokerLogDirs, arguments.length)));
      return this;
    }

    /**
     * Sets the default replication factor.
     *
     * @param defaultReplicationFactor - the factor, on its own or as `{ defaultReplicationFactor }`
     * @returns this builder
     * @throws `IllegalArgumentError` for a form Java has not
     * @throws `TypeError` when the factor is not an integer
     * @throws `RangeError` when it is outside Java's `int`
     */
    defaultReplicationFactor(defaultReplicationFactor: number): this;
    /**
     * The same as `defaultReplicationFactor(defaultReplicationFactor)`, the parameter given by name.
     *
     * @param options - the factor
     * @returns this builder
     * @throws `IllegalArgumentError` for a form Java has not
     * @throws `TypeError` when the factor is not an integer
     * @throws `RangeError` when it is outside Java's `int`
     */
    defaultReplicationFactor(options: {
      /** The factor. */
      defaultReplicationFactor: number;
    }): this;
    defaultReplicationFactor(defaultReplicationFactor: unknown): this {
      const value = DEFAULT_REPLICATION_FACTOR.value(defaultReplicationFactor, arguments.length);
      native.mockAdminBuilderSet(this.#native, 'defaultReplicationFactor', checkInt(value, 'defaultReplicationFactor'));
      return this;
    }

    /**
     * Sets whether the mock uses a KRaft controller.
     *
     * @param usingRaftController - whether it does, on its own or as `{ usingRaftController }`
     * @returns this builder
     * @throws `IllegalArgumentError` for a form Java has not
     * @throws `TypeError` when the value is not a boolean
     */
    usingRaftController(usingRaftController: boolean): this;
    /**
     * The same as `usingRaftController(usingRaftController)`, the parameter given by name.
     *
     * @param options - whether it does
     * @returns this builder
     * @throws `IllegalArgumentError` for a form Java has not
     * @throws `TypeError` when the value is not a boolean
     */
    usingRaftController(options: {
      /** Whether it does. */
      usingRaftController: boolean;
    }): this;
    usingRaftController(usingRaftController: unknown): this {
      const value = checkBoolean(USING_RAFT_CONTROLLER.value(usingRaftController, arguments.length), 'usingRaftController');
      native.mockAdminBuilderSet(this.#native, 'usingRaftController', value);
      return this;
    }

    /**
     * Sets the default partition count.
     *
     * @param numPartitions - the count, a Java `short`, on its own or as `{ numPartitions }`
     * @returns this builder
     * @throws `IllegalArgumentError` for a form Java has not
     * @throws `TypeError` when the count is not an integer
     * @throws `RangeError` when it is outside Java's `short`
     */
    defaultPartitions(numPartitions: number): this;
    /**
     * The same as `defaultPartitions(numPartitions)`, the parameter given by name.
     *
     * @param options - the count, a Java `short`
     * @returns this builder
     * @throws `IllegalArgumentError` for a form Java has not
     * @throws `TypeError` when the count is not an integer
     * @throws `RangeError` when it is outside Java's `short`
     */
    defaultPartitions(options: {
      /** The count, a Java `short`. */
      numPartitions: number;
    }): this;
    defaultPartitions(numPartitions: unknown): this {
      native.mockAdminBuilderSet(
        this.#native,
        'defaultPartitions',
        checkShort(DEFAULT_PARTITIONS.value(numPartitions, arguments.length), 'numPartitions'),
      );
      return this;
    }

    /**
     * Sets the finalized feature levels.
     *
     * @param featureLevels - the levels by feature name, a `Map` on its own, or a `Map` or a plain object as `{ featureLevels }`
     * @returns this builder
     * @throws `IllegalArgumentError` for a form Java has not
     * @throws `TypeError` when the map is null, or a level is not an integer
     * @throws `RangeError` when a level is outside Java's `short`
     */
    featureLevels(featureLevels: ReadonlyMap<string, number>): this;
    /**
     * The same as `featureLevels(featureLevels)`, the parameter given by name.
     *
     * @param options - the levels by feature name, a `Map` on its own, or a `Map` or a plain object as `{ featureLevels }`
     * @returns this builder
     * @throws `IllegalArgumentError` for a form Java has not
     * @throws `TypeError` when the map is null, or a level is not an integer
     * @throws `RangeError` when a level is outside Java's `short`
     */
    featureLevels(options: {
      /** The levels by feature name, a `Map` on its own, or a `Map` or a plain object as `{ featureLevels }`. */
      featureLevels: ReadonlyMap<string, number> | Readonly<Record<string, number>>;
    }): this;
    featureLevels(featureLevels: unknown): this {
      const value = FEATURE_LEVELS.value(featureLevels, arguments.length);
      native.mockAdminBuilderSet(this.#native, 'featureLevels', stringMapPairs(value, 'featureLevels', featureLevel));
      return this;
    }

    /**
     * Sets the minimum supported feature levels.
     *
     * @param minSupportedFeatureLevels - the levels by feature name, a `Map` on its own, or a `Map` or a plain object by name
     * @returns this builder
     * @throws `IllegalArgumentError` for a form Java has not
     * @throws `TypeError` when the map is null, or a level is not an integer
     * @throws `RangeError` when a level is outside Java's `short`
     */
    minSupportedFeatureLevels(minSupportedFeatureLevels: ReadonlyMap<string, number>): this;
    /**
     * The same as `minSupportedFeatureLevels(minSupportedFeatureLevels)`, the parameter given by name.
     *
     * @param options - the levels by feature name, a `Map` on its own, or a `Map` or a plain object by name
     * @returns this builder
     * @throws `IllegalArgumentError` for a form Java has not
     * @throws `TypeError` when the map is null, or a level is not an integer
     * @throws `RangeError` when a level is outside Java's `short`
     */
    minSupportedFeatureLevels(options: {
      /** The levels by feature name, a `Map` on its own, or a `Map` or a plain object by name. */
      minSupportedFeatureLevels: ReadonlyMap<string, number> | Readonly<Record<string, number>>;
    }): this;
    minSupportedFeatureLevels(minSupportedFeatureLevels: unknown): this {
      const value = MIN_SUPPORTED_FEATURE_LEVELS.value(minSupportedFeatureLevels, arguments.length);
      native.mockAdminBuilderSet(
        this.#native,
        'minSupportedFeatureLevels',
        stringMapPairs(value, 'minSupportedFeatureLevels', featureLevel),
      );
      return this;
    }

    /**
     * Sets the maximum supported feature levels.
     *
     * @param maxSupportedFeatureLevels - the levels by feature name, a `Map` on its own, or a `Map` or a plain object by name
     * @returns this builder
     * @throws `IllegalArgumentError` for a form Java has not
     * @throws `TypeError` when the map is null, or a level is not an integer
     * @throws `RangeError` when a level is outside Java's `short`
     */
    maxSupportedFeatureLevels(maxSupportedFeatureLevels: ReadonlyMap<string, number>): this;
    /**
     * The same as `maxSupportedFeatureLevels(maxSupportedFeatureLevels)`, the parameter given by name.
     *
     * @param options - the levels by feature name, a `Map` on its own, or a `Map` or a plain object by name
     * @returns this builder
     * @throws `IllegalArgumentError` for a form Java has not
     * @throws `TypeError` when the map is null, or a level is not an integer
     * @throws `RangeError` when a level is outside Java's `short`
     */
    maxSupportedFeatureLevels(options: {
      /** The levels by feature name, a `Map` on its own, or a `Map` or a plain object by name. */
      maxSupportedFeatureLevels: ReadonlyMap<string, number> | Readonly<Record<string, number>>;
    }): this;
    maxSupportedFeatureLevels(maxSupportedFeatureLevels: unknown): this {
      const value = MAX_SUPPORTED_FEATURE_LEVELS.value(maxSupportedFeatureLevels, arguments.length);
      native.mockAdminBuilderSet(
        this.#native,
        'maxSupportedFeatureLevels',
        stringMapPairs(value, 'maxSupportedFeatureLevels', featureLevel),
      );
      return this;
    }

    /**
     * Sets the group configs overlaid onto every group's.
     *
     * @param defaultGroupConfigs - the configs, a `Map` on its own, or a `Map` or a plain object by name
     * @returns this builder
     * @throws `IllegalArgumentError` for a form Java has not
     * @throws `TypeError` when the map is null, or a value is not a string
     */
    defaultGroupConfigs(defaultGroupConfigs: ReadonlyMap<string, string>): this;
    /**
     * The same as `defaultGroupConfigs(defaultGroupConfigs)`, the parameter given by name.
     *
     * @param options - the configs, a `Map` on its own, or a `Map` or a plain object by name
     * @returns this builder
     * @throws `IllegalArgumentError` for a form Java has not
     * @throws `TypeError` when the map is null, or a value is not a string
     */
    defaultGroupConfigs(options: {
      /** The configs, a `Map` on its own, or a `Map` or a plain object by name. */
      defaultGroupConfigs: ReadonlyMap<string, string> | Readonly<Record<string, string>>;
    }): this;
    defaultGroupConfigs(defaultGroupConfigs: unknown): this {
      const value = DEFAULT_GROUP_CONFIGS.value(defaultGroupConfigs, arguments.length);
      native.mockAdminBuilderSet(this.#native, 'defaultGroupConfigs', stringMapPairs(value, 'defaultGroupConfigs', checkString));
      return this;
    }

    /**
     * A mock from the builder's current settings; the builder may build
     * again.
     *
     * @returns the mock
     * @throws `IllegalArgumentError` when there are no brokers, or the
     *   controller is no longer one of them
     */
    build(): MockAdminClient;
    build(args?: unknown): MockAdminClient {
      BUILD_FORMS.match(args, arguments.length);
      return MockAdminClient.internalCreate(native.mockAdminBuilderBuild(this.#native));
    }
  }
}
