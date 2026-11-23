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

// Java's KafkaAdminClient over the C FFI's admin client
// (javascript/CLAUDE.md). Its Java constructor is private: `Admin.create`
// builds it through `internalCreate` (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Class family). Every RPC returns its Result at once, as
// Java's does; `close()` returns a Promise.

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way (the addon is loaded there alone, javascript/CLAUDE.md "Files and layout")
import native = require('../internal/native');
import * as calls from '../internal/admin_calls';
import type { AdminHandle } from '../internal/admin_result';
import { adminConfigEntries } from '../internal/config';
import { debug } from '../internal/debug';
import { warnPreview } from '../internal/lifecycle';
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
import type { TopicPartitionMap } from '../common/topic_partition_map';
import type { DescribeClusterOptions } from './describe_cluster_options';
import type { DescribeClusterResult } from './describe_cluster_result';
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

/**
 * The parameters of the admin client's timed `close`, Java's
 * `close(Duration)`.
 *
 * @remarks *(js-only)* The object of named parameters of Java's
 * `close(Duration)`; Java's `close()` takes none.
 */
export type CloseArgs = {
  /**
   * The time to use for the wait time: a number of milliseconds (`Infinity`:
   * no limit), or a `Temporal.Duration`, rounded down to whole milliseconds.
   */
  timeout: Duration;
};

/**
 * The default implementation of `Admin`. An instance of this class is created
 * by invoking `Admin.create`. Users should not refer to this class directly.
 *
 * This class is thread-safe.
 *
 * @remarks The admin client is thread safe in Java; here every call is made
 * from the JS thread, and a future's outcome, with its `thenApply` and
 * `whenComplete` functions, arrives there too, from the event loop. An admin
 * client keeps Node running from its creation until `close()` closes it:
 * close every one (`close()`, or `await using`). Clients in worker threads
 * are not supported in the preview.
 */
export class KafkaAdminClient implements Admin {
  readonly #handle: AdminHandle;

  private constructor(handle: AdminHandle) {
    this.#handle = handle;
  }

  /**
   * Java's `KafkaAdminClient.createInternal`: the client over the
   * configuration. Called by `Admin.create`.
   *
   * @param configs - the configuration
   * @returns the client
   * @internal
   */
  static internalCreate(configs: unknown): KafkaAdminClient {
    const client = new KafkaAdminClient(native.adminCreate(adminConfigEntries(configs)));
    warnPreview();
    debug('KafkaAdminClient created');
    return client;
  }

  /**
   * @remarks Without a timeout it waits with no time limit, Java's
   * `Long.MAX_VALUE` milliseconds. A negative timeout is read as zero
   * *(deviation: Java's `KafkaAdminClient.close` throws
   * `IllegalArgumentException("The timeout cannot be negative.")`. Why: the
   * binding repeats no precondition. Goes when: the FFI returns Java's
   * error)*. A call after `close()` goes to the core: an RPC returns its
   * Result, whose futures fail with the core's error.
   */
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

  /**
   * @remarks It also throws the core's `IllegalArgumentError` for an empty
   * map or a blank feature name, as Java's `KafkaAdminClient` does.
   */
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
}
