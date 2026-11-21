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

// Java's Admin interface (root CLAUDE.md, JavaScript Binding Conventions,
// Class family, Q10): a TypeScript interface with the RPCs the C FFI has, and
// a runtime object of the same name holding its statics, `Admin.create`.
// What it leaves out is listed in the admin package's documentation
// (index.ts).

import { JavaForms } from '../internal/args';
import type { Duration } from '../duration';
import type { AdminClientConfigs } from './admin_client_configs';
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
import type { CloseArgs } from './kafka_admin_client';
import { KafkaAdminClient } from './kafka_admin_client';
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
import type { CreateAclsOptions } from './create_acls_options';
import type { CreateAclsResult } from './create_acls_result';
import type { DeleteAclsOptions } from './delete_acls_options';
import type { DeleteAclsResult } from './delete_acls_result';
import type { DescribeAclsOptions } from './describe_acls_options';
import type { DescribeAclsResult } from './describe_acls_result';
import type { AclBinding } from '../common/acl/acl_binding';
import type { AclBindingFilter } from '../common/acl/acl_binding_filter';
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
import type { DescribeFeaturesOptions } from './describe_features_options';
import type { DescribeFeaturesResult } from './describe_features_result';
import type { FeatureUpdate } from './feature_update';
import type { UnregisterControllerOptions } from './unregister_controller_options';
import type { UnregisterControllerResult } from './unregister_controller_result';
import type { UpdateFeaturesOptions } from './update_features_options';
import type { UpdateFeaturesResult } from './update_features_result';

const CREATE_FORMS = new JavaForms('create', [['configs']], {
  bareIsPlainObject: true,
  bareTypes: { configs: (value) => typeof value === 'object' },
});

/**
 * The administrative client for Kafka, which supports managing and inspecting
 * topics, brokers, configurations and ACLs.
 *
 * Instances returned from the `create` methods of this interface are
 * guaranteed to be thread safe. However, the `KafkaFuture`s returned from
 * request methods are executed by a single thread so it is important that any
 * code which executes on that thread when they complete (using
 * `KafkaFuture.thenApply`, for example) doesn't block for too long.
 *
 * The operations exposed by Admin follow a consistent pattern: each RPC takes
 * its options last and returns its `*Result` at once, holding a future per
 * key.
 *
 * @remarks A TypeScript interface; the runtime object `Admin` holds its
 * static `create`.
 */
export interface Admin {
  /**
   * Close the Admin and release all associated resources.
   *
   * See `close({ timeout })`.
   *
   * @returns resolving once the client closed
   */
  close(): Promise<void>;
  /**
   * Close the Admin client and release all associated resources.
   *
   * The close operation has a grace period during which current operations
   * will be allowed to complete, specified by the given duration. New
   * operations will not be accepted during the grace period. Once the grace
   * period is over, all operations that have not yet been completed will be
   * aborted with a `TimeoutError`.
   *
   * @param options - the time to use for the wait time
   * @returns resolving once the client closed
   */
  close(options: CloseArgs): Promise<void>;
  /**
   * The same as `close({ timeout })`: the timeout on its own.
   *
   * @param timeout - The time to use for the wait time.
   * @returns resolving once the client closed
   */
  close(timeout: Duration): Promise<void>;

  /**
   * Closes the client as `close()` does, so that `await using` closes it.
   *
   * @returns resolving once the client closed
   */
  [Symbol.asyncDispose](): Promise<void>;

  /**
   * List the topics available in the cluster with the default options.
   *
   * @returns The ListTopicsResult.
   */
  listTopics(): ListTopicsResult;
  /**
   * List the topics available in the cluster.
   *
   * @param args - The options to use when listing the topics.
   * @returns The ListTopicsResult.
   */
  listTopics(args: {
    /** The options to use when listing the topics. */
    options: ListTopicsOptions;
  }): ListTopicsResult;
  /**
   * The same as `listTopics({ options })`: the options on their own.
   *
   * @param options - The options to use when listing the topics.
   * @returns The ListTopicsResult.
   */
  listTopics(options: ListTopicsOptions): ListTopicsResult;

  /**
   * Get information about the nodes in the cluster, using the default
   * options.
   *
   * @returns The DescribeClusterResult.
   */
  describeCluster(): DescribeClusterResult;
  /**
   * Get information about the nodes in the cluster.
   *
   * To obtain broker cluster information, you must configure
   * `AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG`. To obtain controller
   * cluster information, you must configure
   * `AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG`.
   *
   * @param args - The options to use when getting information about the cluster.
   * @returns The DescribeClusterResult.
   */
  describeCluster(args: {
    /** The options to use when getting information about the cluster. */
    options: DescribeClusterOptions;
  }): DescribeClusterResult;
  /**
   * The same as `describeCluster({ options })`: the options on their own.
   *
   * @param options - The options to use when getting information about the cluster.
   * @returns The DescribeClusterResult.
   */
  describeCluster(options: DescribeClusterOptions): DescribeClusterResult;

  /**
   * Create a batch of new topics.
   *
   * This operation is not transactional so it may succeed for some topics
   * while fail for others. It may take several seconds after the result
   * returns successfully for all the brokers to become aware that the topics
   * have been created; during this time, `listTopics()` and `describeTopics()`
   * may not return information about the new topics. This operation is
   * supported by brokers with version 0.10.1.0 or higher. The validateOnly
   * option is supported from version 0.10.2.0.
   *
   * @param newTopics - The new topics to create.
   * @returns The CreateTopicsResult.
   */
  createTopics(newTopics: Iterable<NewTopic>): CreateTopicsResult;
  /**
   * The same as `createTopics(newTopics)`, the newTopics given by name.
   *
   * @param args - the newTopics
   * @returns The CreateTopicsResult.
   */
  createTopics(args: {
    /** The new topics to create. */
    newTopics: Iterable<NewTopic>;
    options?: never;
  }): CreateTopicsResult;
  /**
   * Create a batch of new topics.
   *
   * @param args - the newTopics, and the options to use
   * @returns The CreateTopicsResult.
   */
  createTopics(args: {
    /** The new topics to create. */
    newTopics: Iterable<NewTopic>;
    /** The options to use when creating the new topics. */
    options: CreateTopicsOptions;
  }): CreateTopicsResult;

  /**
   * Delete a batch of topics.
   *
   * This operation is not transactional so it may succeed for some topics
   * while fail for others. It may take several seconds after the result
   * returns successfully for all the brokers to become aware that the topics
   * are gone; during this time, `listTopics()` and `describeTopics()` may
   * continue to return information about the deleted topics. If
   * delete.topic.enable is false on the brokers, deleteTopics will mark the
   * topics for deletion, but not actually delete them; the futures will return
   * successfully in this case. When using topic IDs, this operation is
   * supported by brokers with inter-broker protocol 2.8 or higher; when using
   * topic names, by brokers with version 0.10.1.0 or higher.
   *
   * @param topics - The topic names, or the topics, to delete.
   * @returns The DeleteTopicsResult.
   */
  deleteTopics(topics: Iterable<string> | TopicCollection): DeleteTopicsResult;
  /**
   * The same as `deleteTopics(topics)`, the topics given by name.
   *
   * @param args - the topics
   * @returns The DeleteTopicsResult.
   */
  deleteTopics(args: {
    /** The topic names, or the topics, to delete. */
    topics: Iterable<string> | TopicCollection;
    options?: never;
  }): DeleteTopicsResult;
  /**
   * Delete a batch of topics.
   *
   * @param args - the topics, and the options to use
   * @returns The DeleteTopicsResult.
   */
  deleteTopics(args: {
    /** The topic names, or the topics, to delete. */
    topics: Iterable<string> | TopicCollection;
    /** The options to use when deleting the topics. */
    options: DeleteTopicsOptions;
  }): DeleteTopicsResult;

  /**
   * Describe some topics in the cluster.
   *
   * When using topic IDs, this operation is supported by brokers with version
   * 3.1.0 or higher.
   *
   * @param topicNames - The names of the topics to describe.
   * @returns The DescribeTopicsResult.
   */
  describeTopics(topicNames: Iterable<string>): DescribeTopicsResult;
  /**
   * The same as `describeTopics(topicNames)`, the topicNames given by name.
   *
   * @param args - the topicNames
   * @returns The DescribeTopicsResult.
   */
  describeTopics(args: {
    /** The names of the topics to describe. */
    topicNames: Iterable<string>;
    topics?: never;
    options?: never;
  }): DescribeTopicsResult;
  /**
   * Describe some topics in the cluster.
   *
   * @param args - the topicNames, and the options to use
   * @returns The DescribeTopicsResult.
   */
  describeTopics(args: {
    /** The names of the topics to describe. */
    topicNames: Iterable<string>;
    /** The options to use when describing the topics. */
    options: DescribeTopicsOptions;
    topics?: never;
  }): DescribeTopicsResult;
  /**
   * Describe some topics in the cluster.
   *
   * When using topic IDs, this operation is supported by brokers with version
   * 3.1.0 or higher.
   *
   * @param topics - The topics to describe.
   * @returns The DescribeTopicsResult.
   */
  describeTopics(topics: TopicCollection): DescribeTopicsResult;
  /**
   * The same as `describeTopics(topics)`, the topics given by name.
   *
   * @param args - the topics
   * @returns The DescribeTopicsResult.
   */
  describeTopics(args: {
    /** The topics to describe. */
    topics: TopicCollection;
    topicNames?: never;
    options?: never;
  }): DescribeTopicsResult;
  /**
   * Describe some topics in the cluster.
   *
   * @param args - the topics, and the options to use
   * @returns The DescribeTopicsResult.
   */
  describeTopics(args: {
    /** The topics to describe. */
    topics: TopicCollection;
    /** The options to use when describing the topics. */
    options: DescribeTopicsOptions;
    topicNames?: never;
  }): DescribeTopicsResult;

  /**
   * Increase the number of partitions of the topics given as the keys of
   * `newPartitions` according to the corresponding values. **If partitions are
   * increased for a topic that has a key, the partition logic or ordering of
   * the messages will be affected.**
   *
   * This operation is not transactional so it may succeed for some topics
   * while fail for others. It may take several seconds after this method
   * returns success for all the brokers to become aware that the partitions
   * have been created. This operation is supported by brokers with version
   * 1.0.0 or higher.
   *
   * @param newPartitions - The topics which should have new partitions created, and corresponding parameters for the created partitions.
   * @returns The CreatePartitionsResult.
   */
  createPartitions(newPartitions: ReadonlyMap<string, NewPartitions>): CreatePartitionsResult;
  /**
   * The same as `createPartitions(newPartitions)`, the newPartitions given by name.
   *
   * @param args - the newPartitions
   * @returns The CreatePartitionsResult.
   */
  createPartitions(args: {
    /** The topics which should have new partitions created, and corresponding parameters for the created partitions. */
    newPartitions: ReadonlyMap<string, NewPartitions> | Readonly<Record<string, NewPartitions>>;
    options?: never;
  }): CreatePartitionsResult;
  /**
   * Increase the number of partitions of the topics given as the keys of
   * `newPartitions` according to the corresponding values. **If partitions are
   * increased for a topic that has a key, the partition logic or ordering of
   * the messages will be affected.**
   *
   * @param args - the newPartitions, and the options to use
   * @returns The CreatePartitionsResult.
   */
  createPartitions(args: {
    /** The topics which should have new partitions created, and corresponding parameters for the created partitions. */
    newPartitions: ReadonlyMap<string, NewPartitions> | Readonly<Record<string, NewPartitions>>;
    /** The options to use when creating the new partitions. */
    options: CreatePartitionsOptions;
  }): CreatePartitionsResult;

  /**
   * Delete records whose offset is smaller than the given offset of the
   * corresponding partition.
   *
   * This operation is supported by brokers with version 0.11.0.0 or higher.
   *
   * @param recordsToDelete - The topic partitions and related offsets from which records deletion starts.
   * @returns The DeleteRecordsResult.
   */
  deleteRecords(recordsToDelete: TopicPartitionMap<RecordsToDelete>): DeleteRecordsResult;
  /**
   * The same as `deleteRecords(recordsToDelete)`, the recordsToDelete given by name.
   *
   * @param args - the recordsToDelete
   * @returns The DeleteRecordsResult.
   */
  deleteRecords(args: {
    /** The topic partitions and related offsets from which records deletion starts. */
    recordsToDelete: TopicPartitionMap<RecordsToDelete>;
    options?: never;
  }): DeleteRecordsResult;
  /**
   * Delete records whose offset is smaller than the given offset of the
   * corresponding partition.
   *
   * @param args - the recordsToDelete, and the options to use
   * @returns The DeleteRecordsResult.
   */
  deleteRecords(args: {
    /** The topic partitions and related offsets from which records deletion starts. */
    recordsToDelete: TopicPartitionMap<RecordsToDelete>;
    /** The options to use when deleting records. */
    options: DeleteRecordsOptions;
  }): DeleteRecordsResult;

  /**
   * Get the configuration for the specified resources with the default options.
   *
   * This operation is supported by brokers with version 0.11.0.0 or higher.
   *
   * @param resources - The resources: see the relevant `ConfigResource.Type`.
   * @returns The DescribeConfigsResult.
   */
  describeConfigs(resources: Iterable<ConfigResource>): DescribeConfigsResult;
  /**
   * The same as `describeConfigs(resources)`, the resources given by name.
   *
   * @param args - the resources
   * @returns The DescribeConfigsResult.
   */
  describeConfigs(args: {
    /** The resources: see the relevant `ConfigResource.Type`. */
    resources: Iterable<ConfigResource>;
    options?: never;
  }): DescribeConfigsResult;
  /**
   * Get the configuration for the specified resources.
   *
   * The returned configuration includes default values and the isDefault() method can be used to distinguish
   * them from user supplied values.
   *
   * The value of config entries where isSensitive() is true is always `null` so that sensitive information is
   * not disclosed.
   *
   * Config entries where isReadOnly() is true cannot be updated.
   *
   * The different behavior of nonexistent resource: `ConfigResource.Type.BROKER` will fail with a
   * `TimeoutError`; `ConfigResource.Type.TOPIC` will fail with an `UnknownTopicOrPartitionError`;
   * `ConfigResource.Type.GROUP` just returns default configs even if the target group is nonexistent;
   * `ConfigResource.Type.BROKER_LOGGER` will fail with a `TimeoutError`; `ConfigResource.Type.CLIENT_METRICS`
   * will return empty configs.
   *
   * Note that you cannot describe broker configs or broker logger using
   * `AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG`, and you cannot describe controller configs or
   * controller logger using `AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG`.
   *
   * This operation is supported by brokers with version 0.11.0.0 or higher.
   *
   * @param args - the resources, and the options to use
   * @returns The DescribeConfigsResult.
   */
  describeConfigs(args: {
    /** The resources: see the relevant `ConfigResource.Type`. */
    resources: Iterable<ConfigResource>;
    /** The options to use. */
    options: DescribeConfigsOptions;
  }): DescribeConfigsResult;

  /**
   * Incrementally updates the configuration for the specified resources with default options.
   *
   * This operation is supported by brokers with version 2.3.0 or higher.
   *
   * @param configs - The resources with their configs.
   * @returns The AlterConfigsResult.
   */
  incrementalAlterConfigs(configs: ConfigResourceMap<Iterable<AlterConfigOp>>): AlterConfigsResult;
  /**
   * The same as `incrementalAlterConfigs(configs)`, the configs given by name.
   *
   * @param args - the configs
   * @returns The AlterConfigsResult.
   */
  incrementalAlterConfigs(args: {
    /** The resources with their configs. */
    configs: ConfigResourceMap<Iterable<AlterConfigOp>>;
    options?: never;
  }): AlterConfigsResult;
  /**
   * Incrementally update the configuration for the specified resources.
   *
   * Updates are not transactional so they may succeed for some resources while fail for others. The configs
   * for a particular resource are updated atomically.
   *
   * The following errors can be anticipated when calling `get()` on the futures obtained from the returned
   * `AlterConfigsResult`: `ClusterAuthorizationError` if the authenticated user didn't have alter access to
   * the cluster; `TopicAuthorizationError` if the authenticated user didn't have alter access to the Topic;
   * `UnknownTopicOrPartitionError` if the Topic doesn't exist; `InvalidRequestError` if the request details
   * are invalid. e.g., a configuration key was specified more than once for a resource.
   *
   * This operation is supported by brokers with version 2.3.0 or higher.
   *
   * @param args - the configs, and the options to use
   * @returns The AlterConfigsResult.
   */
  incrementalAlterConfigs(args: {
    /** The resources with their configs. */
    configs: ConfigResourceMap<Iterable<AlterConfigOp>>;
    /** The options to use. */
    options: AlterConfigsOptions;
  }): AlterConfigsResult;

  /**
   * Change the log directory for the specified replicas. If the replica does not exist on the broker, the
   * result shows REPLICA_NOT_AVAILABLE for the given replica and the replica will be created in the given log
   * directory on the broker when it is created later. If the replica already exists on the broker, the
   * replica will be moved to the given log directory if it is not already there. For detailed result, inspect
   * the returned `AlterReplicaLogDirsResult` instance.
   *
   * This operation is not transactional so it may succeed for some replicas while fail for others.
   *
   * This operation is supported by brokers with version 1.1.0 or higher.
   *
   * @param replicaAssignment - The replicas with their log directory absolute path.
   * @returns The AlterReplicaLogDirsResult.
   */
  alterReplicaLogDirs(replicaAssignment: TopicPartitionReplicaMap<string>): AlterReplicaLogDirsResult;
  /**
   * The same as `alterReplicaLogDirs(replicaAssignment)`, the replicaAssignment given by name.
   *
   * @param args - the replicaAssignment
   * @returns The AlterReplicaLogDirsResult.
   */
  alterReplicaLogDirs(args: {
    /** The replicas with their log directory absolute path. */
    replicaAssignment: TopicPartitionReplicaMap<string>;
    options?: never;
  }): AlterReplicaLogDirsResult;
  /**
   * Change the log directory for the specified replicas. If the replica does not exist on the broker, the
   * result shows REPLICA_NOT_AVAILABLE for the given replica and the replica will be created in the given log
   * directory on the broker when it is created later. If the replica already exists on the broker, the
   * replica will be moved to the given log directory if it is not already there. For detailed result, inspect
   * the returned `AlterReplicaLogDirsResult` instance.
   *
   * This operation is not transactional so it may succeed for some replicas while fail for others.
   *
   * This operation is supported by brokers with version 1.1.0 or higher.
   *
   * @param args - the replicaAssignment, and the options to use
   * @returns The AlterReplicaLogDirsResult.
   */
  alterReplicaLogDirs(args: {
    /** The replicas with their log directory absolute path. */
    replicaAssignment: TopicPartitionReplicaMap<string>;
    /** The options to use. */
    options: AlterReplicaLogDirsOptions;
  }): AlterReplicaLogDirsResult;

  /**
   * Query the information of all log directories on the given set of brokers
   *
   * This operation is supported by brokers with version 1.0.0 or higher.
   *
   * @param brokers - A list of brokers.
   * @returns The DescribeLogDirsResult.
   */
  describeLogDirs(brokers: Iterable<number>): DescribeLogDirsResult;
  /**
   * The same as `describeLogDirs(brokers)`, the brokers given by name.
   *
   * @param args - the brokers
   * @returns The DescribeLogDirsResult.
   */
  describeLogDirs(args: {
    /** A list of brokers. */
    brokers: Iterable<number>;
    options?: never;
  }): DescribeLogDirsResult;
  /**
   * Query the information of all log directories on the given set of brokers
   *
   * This operation is supported by brokers with version 1.0.0 or higher.
   *
   * @param args - the brokers, and the options to use
   * @returns The DescribeLogDirsResult.
   */
  describeLogDirs(args: {
    /** A list of brokers. */
    brokers: Iterable<number>;
    /** The options to use. */
    options: DescribeLogDirsOptions;
  }): DescribeLogDirsResult;

  /**
   * Query the replica log directory information for the specified replicas.
   *
   * This operation is supported by brokers with version 1.0.0 or higher.
   *
   * @param replicas - The replicas to query.
   * @returns The DescribeReplicaLogDirsResult.
   */
  describeReplicaLogDirs(replicas: Iterable<TopicPartitionReplica>): DescribeReplicaLogDirsResult;
  /**
   * The same as `describeReplicaLogDirs(replicas)`, the replicas given by name.
   *
   * @param args - the replicas
   * @returns The DescribeReplicaLogDirsResult.
   */
  describeReplicaLogDirs(args: {
    /** The replicas to query. */
    replicas: Iterable<TopicPartitionReplica>;
    options?: never;
  }): DescribeReplicaLogDirsResult;
  /**
   * Query the replica log directory information for the specified replicas.
   *
   * This operation is supported by brokers with version 1.0.0 or higher.
   *
   * @param args - the replicas, and the options to use
   * @returns The DescribeReplicaLogDirsResult.
   */
  describeReplicaLogDirs(args: {
    /** The replicas to query. */
    replicas: Iterable<TopicPartitionReplica>;
    /** The options to use. */
    options: DescribeReplicaLogDirsOptions;
  }): DescribeReplicaLogDirsResult;

  /**
   * List all configuration resources available in the cluster with the
   * default options.
   *
   * @returns The ListConfigurationResourcesResult.
   */
  listConfigResources(): ListConfigResourcesResult;
  /**
   * List the configuration resources available in the cluster which matches
   * config resource type. If no config resource types are specified, all
   * configuration resources will be listed.
   *
   * @param args - the configResourceTypes, and the options to use
   * @returns The ListConfigurationResourcesResult.
   */
  listConfigResources(args: {
    /** The set of configuration resource types to list. */
    configResourceTypes: Iterable<ConfigResource.Type>;
    /** The options to use when listing the configuration resources. */
    options: ListConfigResourcesOptions;
  }): ListConfigResourcesResult;

  /**
   * Elect a replica as leader for topic partitions.
   *
   * This is a convenience method for `electLeaders({ electionType, partitions, options })` with default
   * options.
   *
   * @param args - the electionType, partitions
   * @returns The ElectLeadersResult.
   */
  electLeaders(args: {
    /** The type of election to conduct. */
    electionType: ElectionType;
    /** The topics and partitions for which to conduct elections, or null for all of them. */
    partitions: Iterable<TopicPartition> | null;
    options?: never;
  }): ElectLeadersResult;
  /**
   * Elect a replica as leader for the given `partitions`, or for all partitions if the argument to
   * `partitions` is null.
   *
   * This operation is not transactional so it may succeed for some partitions while fail for others.
   *
   * It may take several seconds after this method returns success for all the brokers in the cluster to
   * become aware that the partitions have new leaders. During this time, `describeTopics()` may not return
   * information about the partitions' new leaders.
   *
   * This operation is supported by brokers with version 2.2.0 or later if preferred election is use;
   * otherwise the brokers most be 2.4.0 or higher.
   *
   * The following errors can be anticipated when calling `get()` on the future obtained from the returned
   * `ElectLeadersResult`: `ClusterAuthorizationError` if the authenticated user didn't have alter access to
   * the cluster; `UnknownTopicOrPartitionError` if the topic or partition did not exist within the cluster;
   * `InvalidTopicError` if the topic was already queued for deletion; `NotControllerError` if the request was
   * sent to a broker that was not the controller for the cluster; `TimeoutError` if the request timed out
   * before the election was complete; `LeaderNotAvailableError` if the preferred leader was not alive or not
   * in the ISR.
   *
   * @param args - the electionType, partitions, and the options to use
   * @returns The ElectLeadersResult.
   */
  electLeaders(args: {
    /** The type of election to conduct. */
    electionType: ElectionType;
    /** The topics and partitions for which to conduct elections, or null for all of them. */
    partitions: Iterable<TopicPartition> | null;
    /** The options to use. */
    options: ElectLeadersOptions;
  }): ElectLeadersResult;

  /**
   * Change the reassignments for one or more partitions. Providing null will revert the reassignment for the
   * associated partition.
   *
   * This is a convenience method for `alterPartitionReassignments({ reassignments, options })` with default
   * options. See the overload for more details.
   *
   * @param reassignments - The reassignments to add, modify, or remove (null), see `NewPartitionReassignment`.
   * @returns The AlterPartitionReassignmentsResult.
   */
  alterPartitionReassignments(reassignments: TopicPartitionMap<NewPartitionReassignment | null>): AlterPartitionReassignmentsResult;
  /**
   * The same as `alterPartitionReassignments(reassignments)`, the reassignments given by name.
   *
   * @param args - the reassignments
   * @returns The AlterPartitionReassignmentsResult.
   */
  alterPartitionReassignments(args: {
    /** The reassignments to add, modify, or remove (null), see `NewPartitionReassignment`. */
    reassignments: TopicPartitionMap<NewPartitionReassignment | null>;
    options?: never;
  }): AlterPartitionReassignmentsResult;
  /**
   * Change the reassignments for one or more partitions. Providing null will revert the reassignment for the
   * associated partition.
   *
   * The following errors can be anticipated when calling `get()` on the futures obtained from the returned
   * `AlterPartitionReassignmentsResult`: `ClusterAuthorizationError` If the authenticated user didn't have
   * alter access to the cluster; `UnknownTopicOrPartitionError` If the topic or partition does not exist
   * within the cluster; `TimeoutError` if the request timed out before the controller could record the new
   * assignments; `InvalidReplicaAssignmentError` If the specified assignment was not valid;
   * `InvalidReplicationFactorError` If the replication factor was changed in an invalid way, only thrown when
   * `AlterPartitionReassignmentsOptions.allowReplicationFactorChange()` is set to false and the request is
   * attempting to alter reassignments (not cancel); `UnsupportedVersionError` If
   * `AlterPartitionReassignmentsOptions.allowReplicationFactorChange()` was changed outside the default and
   * the server does not support the option (e.g due to an old Kafka version); `NoReassignmentInProgressError`
   * If there was an attempt to cancel a reassignment for a partition which was not being reassigned.
   *
   * @param args - the reassignments, and the options to use
   * @returns The AlterPartitionReassignmentsResult.
   */
  alterPartitionReassignments(args: {
    /** The reassignments to add, modify, or remove (null), see `NewPartitionReassignment`. */
    reassignments: TopicPartitionMap<NewPartitionReassignment | null>;
    /** The options to use. */
    options: AlterPartitionReassignmentsOptions;
  }): AlterPartitionReassignmentsResult;

  /**
   * List all of the current partition reassignments.
   *
   * This is a convenience method for `listPartitionReassignments({ options })`
   * with default options. See the overload for more details.
   *
   * @returns The ListPartitionReassignmentsResult.
   */
  listPartitionReassignments(): ListPartitionReassignmentsResult;
  /**
   * List the current reassignments for the given partitions.
   *
   * This is a convenience method for `listPartitionReassignments({
   * partitions, options })` with default options. See the overload for more
   * details.
   *
   * @param partitions - The topic partitions to list reassignments for.
   * @returns The ListPartitionReassignmentsResult.
   */
  listPartitionReassignments(partitions: Iterable<TopicPartition>): ListPartitionReassignmentsResult;
  /**
   * The same as `listPartitionReassignments(partitions)`, the partitions given by name.
   *
   * @param args - the partitions
   * @returns The ListPartitionReassignmentsResult.
   */
  listPartitionReassignments(args: {
    /** The topic partitions to list reassignments for. */
    partitions: Iterable<TopicPartition>;
    options?: never;
  }): ListPartitionReassignmentsResult;
  /**
   * List the current reassignments for the given partitions, or for all
   * partitions when `partitions` is null (Java's empty `Optional`).
   *
   * The following errors can be anticipated when calling `get()` on the futures obtained from the
   * returned `ListPartitionReassignmentsResult`: `ClusterAuthorizationError` If the authenticated user
   * doesn't have alter access to the cluster; `UnknownTopicOrPartitionError` If a given topic or
   * partition does not exist; `TimeoutError` If the request timed out before the controller could list
   * the current reassignments.
   *
   * @param args - the partitions, and the options to use
   * @returns The ListPartitionReassignmentsResult.
   */
  listPartitionReassignments(args: {
    /** The topic partitions to list reassignments for, or null for all of them. */
    partitions: Iterable<TopicPartition> | null;
    /** The options to use. */
    options: ListPartitionReassignmentsOptions;
  }): ListPartitionReassignmentsResult;
  /**
   * List all of the current partition reassignments.
   *
   * The following errors can be anticipated when calling `get()` on the futures obtained from the
   * returned `ListPartitionReassignmentsResult`: `ClusterAuthorizationError` If the authenticated user
   * doesn't have alter access to the cluster; `UnknownTopicOrPartitionError` If a given topic or
   * partition does not exist; `TimeoutError` If the request timed out before the controller could list
   * the current reassignments.
   *
   * @param args - the options to use
   * @returns The ListPartitionReassignmentsResult.
   */
  listPartitionReassignments(args: {
    /** The options to use. */
    options: ListPartitionReassignmentsOptions;
    partitions?: never;
  }): ListPartitionReassignmentsResult;
  /**
   * The same as `listPartitionReassignments({ options })`: the options on their own.
   *
   * @param options - the options to use
   * @returns The ListPartitionReassignmentsResult.
   */
  listPartitionReassignments(options: ListPartitionReassignmentsOptions): ListPartitionReassignmentsResult;

  /**
   * List offset for the specified partitions and OffsetSpec. This operation enables to find the beginning
   * offset, end offset as well as the offset matching a timestamp in partitions.
   *
   * This is a convenience method for `listOffsets({ topicPartitionOffsets, options })`.
   *
   * @param topicPartitionOffsets - The mapping from partition to the OffsetSpec to look up.
   * @returns The ListOffsetsResult.
   */
  listOffsets(topicPartitionOffsets: TopicPartitionMap<OffsetSpec>): ListOffsetsResult;
  /**
   * The same as `listOffsets(topicPartitionOffsets)`, the topicPartitionOffsets given by name.
   *
   * @param args - the topicPartitionOffsets
   * @returns The ListOffsetsResult.
   */
  listOffsets(args: {
    /** The mapping from partition to the OffsetSpec to look up. */
    topicPartitionOffsets: TopicPartitionMap<OffsetSpec>;
    options?: never;
  }): ListOffsetsResult;
  /**
   * List offset for the specified partitions. This operation enables to find the beginning offset, end offset
   * as well as the offset matching a timestamp in partitions.
   *
   * @param args - the topicPartitionOffsets, and the options to use
   * @returns The ListOffsetsResult.
   */
  listOffsets(args: {
    /** The mapping from partition to the OffsetSpec to look up. */
    topicPartitionOffsets: TopicPartitionMap<OffsetSpec>;
    /** The options to use. */
    options: ListOffsetsOptions;
  }): ListOffsetsResult;

  /**
   * List the groups available in the cluster with the default options.
   *
   * This is a convenience method for `listGroups({ options })` with default options. See the overload for
   * more details.
   *
   * @returns The ListGroupsResult.
   */
  listGroups(): ListGroupsResult;
  /**
   * List the groups available in the cluster.
   *
   * @param args - the options to use
   * @returns The ListGroupsResult.
   */
  listGroups(args: {
    /** The options to use. */
    options: ListGroupsOptions;
  }): ListGroupsResult;
  /**
   * The same as `listGroups({ options })`: the options on their own.
   *
   * @param options - the options to use
   * @returns The ListGroupsResult.
   */
  listGroups(options: ListGroupsOptions): ListGroupsResult;

  /**
   * Describe some consumer groups in the cluster, with the default options.
   *
   * This is a convenience method for `describeConsumerGroups({ groupIds, options })` with default options.
   * See the overload for more details.
   *
   * @param groupIds - The IDs of the groups to describe.
   * @returns The DescribeConsumerGroupsResult.
   */
  describeConsumerGroups(groupIds: Iterable<string>): DescribeConsumerGroupsResult;
  /**
   * The same as `describeConsumerGroups(groupIds)`, the groupIds given by name.
   *
   * @param args - the groupIds
   * @returns The DescribeConsumerGroupsResult.
   */
  describeConsumerGroups(args: {
    /** The IDs of the groups to describe. */
    groupIds: Iterable<string>;
    options?: never;
  }): DescribeConsumerGroupsResult;
  /**
   * Describe some consumer groups in the cluster.
   *
   * @param args - the groupIds, and the options to use
   * @returns The DescribeConsumerGroupsResult.
   */
  describeConsumerGroups(args: {
    /** The IDs of the groups to describe. */
    groupIds: Iterable<string>;
    /** The options to use. */
    options: DescribeConsumerGroupsOptions;
  }): DescribeConsumerGroupsResult;

  /**
   * Describe some classic groups in the cluster, with the default options.
   *
   * This is a convenience method for `describeClassicGroups({ groupIds, options })` with default options. See
   * the overload for more details.
   *
   * @param groupIds - The IDs of the groups to describe.
   * @returns The DescribeClassicGroupsResult.
   */
  describeClassicGroups(groupIds: Iterable<string>): DescribeClassicGroupsResult;
  /**
   * The same as `describeClassicGroups(groupIds)`, the groupIds given by name.
   *
   * @param args - the groupIds
   * @returns The DescribeClassicGroupsResult.
   */
  describeClassicGroups(args: {
    /** The IDs of the groups to describe. */
    groupIds: Iterable<string>;
    options?: never;
  }): DescribeClassicGroupsResult;
  /**
   * Describe some classic groups in the cluster.
   *
   * @param args - the groupIds, and the options to use
   * @returns The DescribeClassicGroupsResult.
   */
  describeClassicGroups(args: {
    /** The IDs of the groups to describe. */
    groupIds: Iterable<string>;
    /** The options to use. */
    options: DescribeClassicGroupsOptions;
  }): DescribeClassicGroupsResult;

  /**
   * List the consumer group offsets available in the cluster with the
   * default options.
   *
   * This is a convenience method for `listConsumerGroupOffsets({ groupSpecs,
   * options })` to list offsets of all partitions of one group with default
   * options.
   *
   * @param groupId - The group to list the offsets of.
   * @returns The ListConsumerGroupOffsetsResult.
   */
  listConsumerGroupOffsets(groupId: string): ListConsumerGroupOffsetsResult;
  /**
   * The same as `listConsumerGroupOffsets(groupId)`, the groupId given by name.
   *
   * @param args - the groupId
   * @returns The ListConsumerGroupOffsetsResult.
   */
  listConsumerGroupOffsets(args: {
    /** The group to list the offsets of. */
    groupId: string;
    groupSpecs?: never;
    options?: never;
  }): ListConsumerGroupOffsetsResult;
  /**
   * List the consumer group offsets available in the cluster.
   *
   * @param args - the groupId, and the options to use
   * @returns The ListConsumerGroupOffsetsResult.
   */
  listConsumerGroupOffsets(args: {
    /** The group to list the offsets of. */
    groupId: string;
    /** The options to use when listing the consumer group offsets. */
    options: ListConsumerGroupOffsetsOptions;
    groupSpecs?: never;
  }): ListConsumerGroupOffsetsResult;
  /**
   * List the consumer group offsets available in the cluster for the
   * specified groups with the default options.
   *
   * This is a convenience method for `listConsumerGroupOffsets({ groupSpecs,
   * options })` with default options.
   *
   * @param groupSpecs - Map of consumer group ids to a spec that specifies the topic partitions of the group to list offsets for.
   * @returns The ListConsumerGroupOffsetsResult.
   */
  listConsumerGroupOffsets(groupSpecs: ReadonlyMap<string, ListConsumerGroupOffsetsSpec>): ListConsumerGroupOffsetsResult;
  /**
   * The same as `listConsumerGroupOffsets(groupSpecs)`, the groupSpecs given by name.
   *
   * @param args - the groupSpecs, a `Map` or a plain object
   * @returns The ListConsumerGroupOffsetsResult.
   */
  listConsumerGroupOffsets(args: {
    /** Map of consumer group ids to a spec that specifies the topic partitions of the group to list offsets for. */
    groupSpecs: ReadonlyMap<string, ListConsumerGroupOffsetsSpec> | Readonly<Record<string, ListConsumerGroupOffsetsSpec>>;
    groupId?: never;
    options?: never;
  }): ListConsumerGroupOffsetsResult;
  /**
   * List the consumer group offsets available in the cluster for the
   * specified consumer groups.
   *
   * @param args - the groupSpecs, and the options to use
   * @returns The ListConsumerGroupOffsetsResult.
   */
  listConsumerGroupOffsets(args: {
    /** Map of consumer group ids to a spec that specifies the topic partitions of the group to list offsets for. */
    groupSpecs: ReadonlyMap<string, ListConsumerGroupOffsetsSpec> | Readonly<Record<string, ListConsumerGroupOffsetsSpec>>;
    /** The options to use when listing the consumer group offsets. */
    options: ListConsumerGroupOffsetsOptions;
    groupId?: never;
  }): ListConsumerGroupOffsetsResult;

  /**
   * Alters offsets for the specified group. In order to succeed, the group must be empty.
   *
   * This is a convenience method for `alterConsumerGroupOffsets({ groupId, offsets, options })` with default
   * options. See the overload for more details.
   *
   * @param args - the groupId, offsets
   * @returns The AlterConsumerGroupOffsetsResult.
   */
  alterConsumerGroupOffsets(args: {
    /** The group for which to alter offsets. */
    groupId: string;
    /** A map of offsets by partition with associated metadata. Partitions not specified in the map are ignored. */
    offsets: TopicPartitionMap<OffsetAndMetadata>;
    options?: never;
  }): AlterConsumerGroupOffsetsResult;
  /**
   * Alters offsets for the specified group. In order to succeed, the group must be empty.
   *
   * This operation is not transactional so it may succeed for some partitions while fail for others.
   *
   * @param args - the groupId, offsets, and the options to use
   * @returns The AlterConsumerGroupOffsetsResult.
   */
  alterConsumerGroupOffsets(args: {
    /** The group for which to alter offsets. */
    groupId: string;
    /** A map of offsets by partition with associated metadata. Partitions not specified in the map are ignored. */
    offsets: TopicPartitionMap<OffsetAndMetadata>;
    /** The options to use. */
    options: AlterConsumerGroupOffsetsOptions;
  }): AlterConsumerGroupOffsetsResult;

  /**
   * Delete committed offsets for a set of partitions in a consumer group with the default options. This will
   * succeed at the partition level only if the group is not actively subscribed to the corresponding topic.
   *
   * @param args - the groupId, partitions
   * @returns The DeleteConsumerGroupOffsetsResult.
   */
  deleteConsumerGroupOffsets(args: {
    /** The group to delete the offsets of. */
    groupId: string;
    /** The partitions whose committed offsets to delete. */
    partitions: Iterable<TopicPartition>;
    options?: never;
  }): DeleteConsumerGroupOffsetsResult;
  /**
   * Delete committed offsets for a set of partitions in a consumer group. This will succeed at the partition
   * level only if the group is not actively subscribed to the corresponding topic.
   *
   * @param args - the groupId, partitions, and the options to use
   * @returns The DeleteConsumerGroupOffsetsResult.
   */
  deleteConsumerGroupOffsets(args: {
    /** The group to delete the offsets of. */
    groupId: string;
    /** The partitions whose committed offsets to delete. */
    partitions: Iterable<TopicPartition>;
    /** The options to use. */
    options: DeleteConsumerGroupOffsetsOptions;
  }): DeleteConsumerGroupOffsetsResult;

  /**
   * Delete consumer groups from the cluster with the default options.
   *
   * @param groupIds - The IDs of the groups to delete.
   * @returns The DeleteConsumerGroupsResult.
   */
  deleteConsumerGroups(groupIds: Iterable<string>): DeleteConsumerGroupsResult;
  /**
   * The same as `deleteConsumerGroups(groupIds)`, the groupIds given by name.
   *
   * @param args - the groupIds
   * @returns The DeleteConsumerGroupsResult.
   */
  deleteConsumerGroups(args: {
    /** The IDs of the groups to delete. */
    groupIds: Iterable<string>;
    options?: never;
  }): DeleteConsumerGroupsResult;
  /**
   * Delete consumer groups from the cluster.
   *
   * @param args - the groupIds, and the options to use
   * @returns The DeleteConsumerGroupsResult.
   */
  deleteConsumerGroups(args: {
    /** The IDs of the groups to delete. */
    groupIds: Iterable<string>;
    /** The options to use. */
    options: DeleteConsumerGroupsOptions;
  }): DeleteConsumerGroupsResult;

  /**
   * Remove members from the consumer group by given member identities.
   *
   * For possible error codes, refer to `LeaveGroupResponse`.
   *
   * @param args - The ID of the group to remove member from, and the options to carry removing members' information.
   * @returns The MembershipChangeResult.
   */
  removeMembersFromConsumerGroup(args: {
    /** The ID of the group to remove member from. */
    groupId: string;
    /** The options to carry removing members' information. */
    options: RemoveMembersFromConsumerGroupOptions;
  }): RemoveMembersFromConsumerGroupResult;

  /**
   * This is a convenience method for `describeAcls({ filter, options })` with default options. See the
   * overload for more details.
   *
   * This operation is supported by brokers with version 0.11.0.0 or higher.
   *
   * @param filter - The filter to use.
   * @returns The DescribeAclsResult.
   */
  describeAcls(filter: AclBindingFilter): DescribeAclsResult;
  /**
   * The same as `describeAcls(filter)`, the filter given by name.
   *
   * @param args - the filter
   * @returns The DescribeAclsResult.
   */
  describeAcls(args: {
    /** The filter to use. */
    filter: AclBindingFilter;
    options?: never;
  }): DescribeAclsResult;
  /**
   * Lists access control lists (ACLs) according to the supplied filter.
   *
   * Note: it may take some time for changes made by `createAcls` or `deleteAcls` to be reflected in the
   * output of `describeAcls`.
   *
   * This operation is supported by brokers with version 0.11.0.0 or higher.
   *
   * @param args - the filter, and the options to use
   * @returns The DescribeAclsResult.
   */
  describeAcls(args: {
    /** The filter to use. */
    filter: AclBindingFilter;
    /** The options to use. */
    options: DescribeAclsOptions;
  }): DescribeAclsResult;

  /**
   * This is a convenience method for `createAcls({ acls, options })` with default options. See the overload
   * for more details.
   *
   * This operation is supported by brokers with version 0.11.0.0 or higher.
   *
   * @param acls - The ACLs to create
   * @returns The CreateAclsResult.
   */
  createAcls(acls: Iterable<AclBinding>): CreateAclsResult;
  /**
   * The same as `createAcls(acls)`, the acls given by name.
   *
   * @param args - the acls
   * @returns The CreateAclsResult.
   */
  createAcls(args: {
    /** The ACLs to create */
    acls: Iterable<AclBinding>;
    options?: never;
  }): CreateAclsResult;
  /**
   * Creates access control lists (ACLs) which are bound to specific resources.
   *
   * This operation is not transactional so it may succeed for some ACLs while fail for others.
   *
   * If you attempt to add an ACL that duplicates an existing ACL, no error will be raised, but no changes
   * will be made.
   *
   * This operation is supported by brokers with version 0.11.0.0 or higher.
   *
   * @param args - the acls, and the options to use
   * @returns The CreateAclsResult.
   */
  createAcls(args: {
    /** The ACLs to create */
    acls: Iterable<AclBinding>;
    /** The options to use. */
    options: CreateAclsOptions;
  }): CreateAclsResult;

  /**
   * This is a convenience method for `deleteAcls({ filters, options })` with default options. See the
   * overload for more details.
   *
   * This operation is supported by brokers with version 0.11.0.0 or higher.
   *
   * @param filters - The filters to use.
   * @returns The DeleteAclsResult.
   */
  deleteAcls(filters: Iterable<AclBindingFilter>): DeleteAclsResult;
  /**
   * The same as `deleteAcls(filters)`, the filters given by name.
   *
   * @param args - the filters
   * @returns The DeleteAclsResult.
   */
  deleteAcls(args: {
    /** The filters to use. */
    filters: Iterable<AclBindingFilter>;
    options?: never;
  }): DeleteAclsResult;
  /**
   * Deletes access control lists (ACLs) according to the supplied filters.
   *
   * This operation is not transactional so it may succeed for some ACLs while fail for others.
   *
   * This operation is supported by brokers with version 0.11.0.0 or higher.
   *
   * @param args - the filters, and the options to use
   * @returns The DeleteAclsResult.
   */
  deleteAcls(args: {
    /** The filters to use. */
    filters: Iterable<AclBindingFilter>;
    /** The options to use. */
    options: DeleteAclsOptions;
  }): DeleteAclsResult;

  /**
   * Create a Delegation Token.
   *
   * This is a convenience method for `createDelegationToken({ options })` with default options. See the
   * overload for more details.
   *
   * @returns The CreateDelegationTokenResult.
   */
  createDelegationToken(): CreateDelegationTokenResult;
  /**
   * Create a Delegation Token.
   *
   * This operation is supported by brokers with version 1.1.0 or higher.
   *
   * @param args - the options to use
   * @returns The CreateDelegationTokenResult.
   */
  createDelegationToken(args: {
    /** The options to use. */
    options: CreateDelegationTokenOptions;
  }): CreateDelegationTokenResult;
  /**
   * The same as `createDelegationToken({ options })`: the options on their own.
   *
   * @param options - the options to use
   * @returns The CreateDelegationTokenResult.
   */
  createDelegationToken(options: CreateDelegationTokenOptions): CreateDelegationTokenResult;

  /**
   * Renew a Delegation Token.
   *
   * This is a convenience method for `renewDelegationToken({ hmac, options })` with default options. See the
   * overload for more details.
   *
   * @param hmac - HMAC of the Delegation token
   * @returns The RenewDelegationTokenResult.
   */
  renewDelegationToken(hmac: Uint8Array): RenewDelegationTokenResult;
  /**
   * The same as `renewDelegationToken(hmac)`, the hmac given by name.
   *
   * @param args - the hmac
   * @returns The RenewDelegationTokenResult.
   */
  renewDelegationToken(args: {
    /** HMAC of the Delegation token */
    hmac: Uint8Array;
    options?: never;
  }): RenewDelegationTokenResult;
  /**
   * Renew a Delegation Token.
   *
   * This operation is supported by brokers with version 1.1.0 or higher.
   *
   * @param args - the hmac, and the options to use
   * @returns The RenewDelegationTokenResult.
   */
  renewDelegationToken(args: {
    /** HMAC of the Delegation token */
    hmac: Uint8Array;
    /** The options to use. */
    options: RenewDelegationTokenOptions;
  }): RenewDelegationTokenResult;

  /**
   * Expire a Delegation Token.
   *
   * This is a convenience method for `expireDelegationToken({ hmac, options })` with default options. This
   * will expire the token immediately. See the overload for more details.
   *
   * @param hmac - HMAC of the Delegation token
   * @returns The ExpireDelegationTokenResult.
   */
  expireDelegationToken(hmac: Uint8Array): ExpireDelegationTokenResult;
  /**
   * The same as `expireDelegationToken(hmac)`, the hmac given by name.
   *
   * @param args - the hmac
   * @returns The ExpireDelegationTokenResult.
   */
  expireDelegationToken(args: {
    /** HMAC of the Delegation token */
    hmac: Uint8Array;
    options?: never;
  }): ExpireDelegationTokenResult;
  /**
   * Expire a Delegation Token.
   *
   * This operation is supported by brokers with version 1.1.0 or higher.
   *
   * @param args - the hmac, and the options to use
   * @returns The ExpireDelegationTokenResult.
   */
  expireDelegationToken(args: {
    /** HMAC of the Delegation token */
    hmac: Uint8Array;
    /** The options to use. */
    options: ExpireDelegationTokenOptions;
  }): ExpireDelegationTokenResult;

  /**
   * Describe the Delegation Tokens.
   *
   * This is a convenience method for `describeDelegationToken({ options })` with default options. This will
   * return all the user owned tokens and tokens where user have Describe permission. See the overload for
   * more details.
   *
   * @returns The DescribeDelegationTokenResult.
   */
  describeDelegationToken(): DescribeDelegationTokenResult;
  /**
   * Describe the Delegation Tokens.
   *
   * This operation is supported by brokers with version 1.1.0 or higher.
   *
   * @param args - the options to use
   * @returns The DescribeDelegationTokenResult.
   */
  describeDelegationToken(args: {
    /** The options to use. */
    options: DescribeDelegationTokenOptions;
  }): DescribeDelegationTokenResult;
  /**
   * The same as `describeDelegationToken({ options })`: the options on their own.
   *
   * @param options - the options to use
   * @returns The DescribeDelegationTokenResult.
   */
  describeDelegationToken(options: DescribeDelegationTokenOptions): DescribeDelegationTokenResult;

  /**
   * Describes all entities matching the provided filter that have at least one client quota configuration
   * value defined.
   *
   * This is a convenience method for `describeClientQuotas({ filter, options })` with default options. See
   * the overload for more details.
   *
   * This operation is supported by brokers with version 2.6.0 or higher.
   *
   * @param filter - the filter to apply to match entities
   * @returns The DescribeClientQuotasResult.
   */
  describeClientQuotas(filter: ClientQuotaFilter): DescribeClientQuotasResult;
  /**
   * The same as `describeClientQuotas(filter)`, the filter given by name.
   *
   * @param args - the filter
   * @returns The DescribeClientQuotasResult.
   */
  describeClientQuotas(args: {
    /** the filter to apply to match entities */
    filter: ClientQuotaFilter;
    options?: never;
  }): DescribeClientQuotasResult;
  /**
   * Describes all entities matching the provided filter that have at least one client quota configuration
   * value defined.
   *
   * This operation is supported by brokers with version 2.6.0 or higher.
   *
   * @param args - the filter, and the options to use
   * @returns The DescribeClientQuotasResult.
   */
  describeClientQuotas(args: {
    /** the filter to apply to match entities */
    filter: ClientQuotaFilter;
    /** The options to use. */
    options: DescribeClientQuotasOptions;
  }): DescribeClientQuotasResult;

  /**
   * Alters client quota configurations with the specified alterations.
   *
   * This is a convenience method for `alterClientQuotas({ entries, options })` with default options. See the
   * overload for more details.
   *
   * This operation is supported by brokers with version 2.6.0 or higher.
   *
   * @param entries - the alterations to perform
   * @returns The AlterClientQuotasResult.
   */
  alterClientQuotas(entries: Iterable<ClientQuotaAlteration>): AlterClientQuotasResult;
  /**
   * The same as `alterClientQuotas(entries)`, the entries given by name.
   *
   * @param args - the entries
   * @returns The AlterClientQuotasResult.
   */
  alterClientQuotas(args: {
    /** the alterations to perform */
    entries: Iterable<ClientQuotaAlteration>;
    options?: never;
  }): AlterClientQuotasResult;
  /**
   * Alters client quota configurations with the specified alterations.
   *
   * Alterations for a single entity are atomic, but across entities is not guaranteed. The resulting per-
   * entity error code should be evaluated to resolve the success or failure of all updates.
   *
   * This operation is supported by brokers with version 2.6.0 or higher.
   *
   * @param args - the entries, and the options to use
   * @returns The AlterClientQuotasResult.
   */
  alterClientQuotas(args: {
    /** the alterations to perform */
    entries: Iterable<ClientQuotaAlteration>;
    /** The options to use. */
    options: AlterClientQuotasOptions;
  }): AlterClientQuotasResult;

  /**
   * Describe all SASL/SCRAM credentials.
   *
   * This is a convenience method for `describeUserScramCredentials({ users, options })`.
   *
   * @returns The DescribeUserScramCredentialsResult.
   */
  describeUserScramCredentials(): DescribeUserScramCredentialsResult;
  /**
   * Describe SASL/SCRAM credentials for the given users.
   *
   * This is a convenience method for `describeUserScramCredentials({ users, options })`.
   *
   * @param users - the users for which credentials are to be described; all users' credentials are
   *   described if null or empty.
   * @returns The DescribeUserScramCredentialsResult.
   */
  describeUserScramCredentials(users: Iterable<string> | null): DescribeUserScramCredentialsResult;
  /**
   * The same as `describeUserScramCredentials(users)`, the users given by name.
   *
   * @param args - the users
   * @returns The DescribeUserScramCredentialsResult.
   */
  describeUserScramCredentials(args: {
    /** The users for which credentials are to be described; all users' credentials are described if null or empty. */
    users: Iterable<string> | null;
    options?: never;
  }): DescribeUserScramCredentialsResult;
  /**
   * Describe SASL/SCRAM credentials.
   *
   * This operation is supported by brokers with version 2.7.0 or higher.
   *
   * @param args - the users, and the options to use
   * @returns The DescribeUserScramCredentialsResult.
   */
  describeUserScramCredentials(args: {
    /** The users for which credentials are to be described; all users' credentials are described if null or empty. */
    users: Iterable<string> | null;
    /** The options to use when describing the credentials. */
    options: DescribeUserScramCredentialsOptions;
  }): DescribeUserScramCredentialsResult;

  /**
   * Alter SASL/SCRAM credentials for the given users.
   *
   * This is a convenience method for `alterUserScramCredentials({ alterations, options })`.
   *
   * @param alterations - the alterations to be applied
   * @returns The AlterUserScramCredentialsResult.
   */
  alterUserScramCredentials(alterations: Iterable<UserScramCredentialAlteration>): AlterUserScramCredentialsResult;
  /**
   * The same as `alterUserScramCredentials(alterations)`, the alterations given by name.
   *
   * @param args - the alterations
   * @returns The AlterUserScramCredentialsResult.
   */
  alterUserScramCredentials(args: {
    /** the alterations to be applied */
    alterations: Iterable<UserScramCredentialAlteration>;
    options?: never;
  }): AlterUserScramCredentialsResult;
  /**
   * Alter SASL/SCRAM credentials.
   *
   * This operation is supported by brokers with version 2.7.0 or higher.
   *
   * @param args - the alterations, and the options to use
   * @returns The AlterUserScramCredentialsResult.
   */
  alterUserScramCredentials(args: {
    /** the alterations to be applied */
    alterations: Iterable<UserScramCredentialAlteration>;
    /** The options to use. */
    options: AlterUserScramCredentialsOptions;
  }): AlterUserScramCredentialsResult;

  /**
   * Describe producer state on a set of topic partitions. See `describeProducers({ partitions, options })`
   * for more details.
   *
   * @param partitions - The set of partitions to query
   * @returns The DescribeProducersResult.
   */
  describeProducers(partitions: Iterable<TopicPartition>): DescribeProducersResult;
  /**
   * The same as `describeProducers(partitions)`, the partitions given by name.
   *
   * @param args - the partitions
   * @returns The DescribeProducersResult.
   */
  describeProducers(args: {
    /** The set of partitions to query */
    partitions: Iterable<TopicPartition>;
    options?: never;
  }): DescribeProducersResult;
  /**
   * Describe active producer state on a set of topic partitions. Unless a specific broker is requested
   * through `DescribeProducersOptions.brokerId(brokerId)`, this will query the partition leader to find the
   * producer state.
   *
   * @param args - the partitions, and the options to use
   * @returns The DescribeProducersResult.
   */
  describeProducers(args: {
    /** The set of partitions to query */
    partitions: Iterable<TopicPartition>;
    /** The options to use. */
    options: DescribeProducersOptions;
  }): DescribeProducersResult;

  /**
   * Describe the state of a set of transactional IDs. See `describeTransactions({ transactionalIds, options
   * })` for more details.
   *
   * @param transactionalIds - The set of transactional IDs to query
   * @returns The DescribeTransactionsResult.
   */
  describeTransactions(transactionalIds: Iterable<string>): DescribeTransactionsResult;
  /**
   * The same as `describeTransactions(transactionalIds)`, the transactionalIds given by name.
   *
   * @param args - the transactionalIds
   * @returns The DescribeTransactionsResult.
   */
  describeTransactions(args: {
    /** The set of transactional IDs to query */
    transactionalIds: Iterable<string>;
    options?: never;
  }): DescribeTransactionsResult;
  /**
   * Describe the state of a set of transactional IDs from the respective transaction coordinators, which are
   * dynamically discovered.
   *
   * @param args - the transactionalIds, and the options to use
   * @returns The DescribeTransactionsResult.
   */
  describeTransactions(args: {
    /** The set of transactional IDs to query */
    transactionalIds: Iterable<string>;
    /** The options to use. */
    options: DescribeTransactionsOptions;
  }): DescribeTransactionsResult;

  /**
   * Forcefully abort a transaction which is open on a topic partition. See `abortTransaction({ spec, options
   * })` for more details.
   *
   * @param spec - The transaction specification including topic partition and producer details
   * @returns The AbortTransactionResult.
   */
  abortTransaction(spec: AbortTransactionSpec): AbortTransactionResult;
  /**
   * The same as `abortTransaction(spec)`, the spec given by name.
   *
   * @param args - the spec
   * @returns The AbortTransactionResult.
   */
  abortTransaction(args: {
    /** The transaction specification including topic partition and producer details */
    spec: AbortTransactionSpec;
    options?: never;
  }): AbortTransactionResult;
  /**
   * Forcefully abort a transaction which is open on a topic partition. This will send a `WriteTxnMarkers`
   * request to the partition leader in order to abort the transaction. This requires administrative
   * privileges.
   *
   * @param args - the spec, and the options to use
   * @returns The AbortTransactionResult.
   */
  abortTransaction(args: {
    /** The transaction specification including topic partition and producer details */
    spec: AbortTransactionSpec;
    /** The options to use. */
    options: AbortTransactionOptions;
  }): AbortTransactionResult;

  /**
   * List active transactions in the cluster. See `listTransactions({ options })` for more details.
   *
   * @returns The ListTransactionsResult.
   */
  listTransactions(): ListTransactionsResult;
  /**
   * List active transactions in the cluster. This will query all potential transaction coordinators in the
   * cluster and collect the state of all transactions. Users should typically attempt to reduce the size of
   * the result set using `ListTransactionsOptions.filterProducerIds(producerIdFilters)` or
   * `ListTransactionsOptions.filterStates(states)` or `ListTransactionsOptions.filterOnDuration(durationMs)`.
   *
   * @param args - the options to use
   * @returns The ListTransactionsResult.
   */
  listTransactions(args: {
    /** The options to use. */
    options: ListTransactionsOptions;
  }): ListTransactionsResult;
  /**
   * The same as `listTransactions({ options })`: the options on their own.
   *
   * @param options - the options to use
   * @returns The ListTransactionsResult.
   */
  listTransactions(options: ListTransactionsOptions): ListTransactionsResult;

  /**
   * Fence out all active producers that use any of the provided transactional IDs, with the default options.
   *
   * This is a convenience method for `fenceProducers({ transactionalIds, options })` with default options.
   * See the overload for more details.
   *
   * @param transactionalIds - The IDs of the producers to fence.
   * @returns The FenceProducersResult.
   */
  fenceProducers(transactionalIds: Iterable<string>): FenceProducersResult;
  /**
   * The same as `fenceProducers(transactionalIds)`, the transactionalIds given by name.
   *
   * @param args - the transactionalIds
   * @returns The FenceProducersResult.
   */
  fenceProducers(args: {
    /** The IDs of the producers to fence. */
    transactionalIds: Iterable<string>;
    options?: never;
  }): FenceProducersResult;
  /**
   * Fence out all active producers that use any of the provided transactional IDs.
   *
   * @param args - the transactionalIds, and the options to use
   * @returns The FenceProducersResult.
   */
  fenceProducers(args: {
    /** The IDs of the producers to fence. */
    transactionalIds: Iterable<string>;
    /** The options to use. */
    options: FenceProducersOptions;
  }): FenceProducersResult;

  /**
   * Force terminate a transaction for the given transactional ID with the default options.
   *
   * This is a convenience method for `forceTerminateTransaction({ transactionalId, options })` with default
   * options.
   *
   * @param transactionalId - The ID of the transaction to terminate.
   * @returns The TerminateTransactionResult.
   */
  forceTerminateTransaction(transactionalId: string): TerminateTransactionResult;
  /**
   * The same as `forceTerminateTransaction(transactionalId)`, the transactionalId given by name.
   *
   * @param args - the transactionalId
   * @returns The TerminateTransactionResult.
   */
  forceTerminateTransaction(args: {
    /** The ID of the transaction to terminate. */
    transactionalId: string;
    options?: never;
  }): TerminateTransactionResult;
  /**
   * Force terminate a transaction for the given transactional ID. This operation aborts any ongoing
   * transaction associated with the transactional ID. It's similar to fenceProducers but only targets a
   * single transactional ID to handle long-running transactions when 2PC is enabled.
   *
   * @param args - the transactionalId, and the options to use
   * @returns The TerminateTransactionResult.
   */
  forceTerminateTransaction(args: {
    /** The ID of the transaction to terminate. */
    transactionalId: string;
    /** The options to use. */
    options: TerminateTransactionOptions;
  }): TerminateTransactionResult;

  /**
   * Describes finalized as well as supported features.
   *
   * This is a convenience method for `describeFeatures({ options })` with default options. See the overload
   * for more details.
   *
   * @returns The DescribeFeaturesResult.
   */
  describeFeatures(): DescribeFeaturesResult;
  /**
   * Describes finalized as well as supported features. The request is issued to any random broker.
   *
   * The following errors can be anticipated when calling `get()` on the future from the returned
   * `DescribeFeaturesResult`: `TimeoutError` (from `common/errors`), if the request timed out before the
   * describe operation could finish.
   *
   * @param args - the options to use
   * @returns The DescribeFeaturesResult.
   */
  describeFeatures(args: {
    /** The options to use. */
    options: DescribeFeaturesOptions;
  }): DescribeFeaturesResult;
  /**
   * The same as `describeFeatures({ options })`: the options on their own.
   *
   * @param options - the options to use
   * @returns The DescribeFeaturesResult.
   */
  describeFeatures(options: DescribeFeaturesOptions): DescribeFeaturesResult;

  /**
   * Applies specified updates to finalized features.
   *
   * This is a convenience method for `updateFeatures({ featureUpdates, options })` with default options. See
   * the overload for more details.
   *
   * @param featureUpdates - the map of finalized feature name to `FeatureUpdate`
   * @returns The UpdateFeaturesResult.
   */
  updateFeatures(featureUpdates: ReadonlyMap<string, FeatureUpdate>): UpdateFeaturesResult;
  /**
   * The same as `updateFeatures(featureUpdates)`, the featureUpdates given by name.
   *
   * @param args - the featureUpdates
   * @returns The UpdateFeaturesResult.
   */
  updateFeatures(args: {
    /** the map of finalized feature name to `FeatureUpdate` */
    featureUpdates: ReadonlyMap<string, FeatureUpdate> | Readonly<Record<string, FeatureUpdate>>;
    options?: never;
  }): UpdateFeaturesResult;
  /**
   * Applies specified updates to finalized features. This operation is not transactional so some updates may
   * succeed while the rest may fail.
   *
   * The API takes in a map of finalized feature names to `FeatureUpdate` that needs to be applied. Each entry
   * in the map specifies the finalized feature to be added or updated or deleted, along with the new max
   * feature version level value. This request is issued only to the controller since the API is only served
   * by the controller. The return value contains an error code for each supplied `FeatureUpdate`, and the
   * code indicates if the update succeeded or failed in the controller.
   *
   * Downgrading a feature version level is not a common operation and should only be performed when
   * necessary. It is permitted only if the `FeatureUpdate` specifies the `upgradeType` as either
   * `FeatureUpdate.UpgradeType.SAFE_DOWNGRADE` (downgrades that do not lead to metadata loss) or
   * `FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE` (downgrades that might result in metadata loss). Note that
   * even with these settings, certain downgrades may still be rejected by the controller if they are
   * considered unsafe or impossible. To delete a feature, set the `maxVersionLevel` to zero and specify the
   * `upgradeType` as either of those. The `FeatureUpdate.UpgradeType.UPGRADE` type cannot be used when the
   * `maxVersionLevel` is zero: attempting to do so will result in an `IllegalArgumentError`.
   *
   * The following errors can be anticipated when calling `get()` on the futures obtained from the returned
   * `UpdateFeaturesResult`: `ClusterAuthorizationError`, if the authenticated user didn't have alter access
   * to the cluster; `InvalidRequestError`, if the request details are invalid, e.g., a non-existing finalized
   * feature is attempted to be deleted or downgraded; `TimeoutError`, if the request timed out before the
   * updates could finish (it cannot be guaranteed whether the updates succeeded or not);
   * `FeatureUpdateFailedError`, an unexpected error encountered when the update was applied on the controller
   * (there is no guarantee on whether the update succeeded or failed; the best way to find out is to issue a
   * `describeFeatures({ options })` request).
   *
   * This operation is supported by brokers with version 2.7.0 or higher.
   *
   * @param args - the featureUpdates, and the options to use
   * @returns The UpdateFeaturesResult.
   */
  updateFeatures(args: {
    /** the map of finalized feature name to `FeatureUpdate` */
    featureUpdates: ReadonlyMap<string, FeatureUpdate> | Readonly<Record<string, FeatureUpdate>>;
    /** The options to use. */
    options: UpdateFeaturesOptions;
  }): UpdateFeaturesResult;

  /**
   * Unregister a controller.
   *
   * This is a convenience method for `unregisterController({ controllerId, options })`.
   *
   * @param controllerId - the controller id to unregister.
   * @returns The UnregisterControllerResult.
   */
  unregisterController(controllerId: number): UnregisterControllerResult;
  /**
   * The same as `unregisterController(controllerId)`, the controllerId given by name.
   *
   * @param args - the controllerId
   * @returns The UnregisterControllerResult.
   */
  unregisterController(args: {
    /** the controller id to unregister. */
    controllerId: number;
    options?: never;
  }): UnregisterControllerResult;
  /**
   * Unregister a controller.
   *
   * The following errors can be anticipated when calling `get()` on the future from the returned
   * `UnregisterControllerResult`: `TimeoutError`, if the request timed out before the unregister operation
   * could finish; `UnsupportedVersionError`, if the software is too old to support the unregistration API;
   * `ControllerIdNotRegisteredError`, if the requested controller id is not currently registered;
   * `NotControllerError`, if the request does not arrive at the active controller; `InvalidRequestError`, if
   * the request tries to unregister the current active controller id.
   *
   * @param args - the controllerId, and the options to use
   * @returns The UnregisterControllerResult.
   */
  unregisterController(args: {
    /** the controller id to unregister. */
    controllerId: number;
    /** The options to use. */
    options: UnregisterControllerOptions;
  }): UnregisterControllerResult;
}

/**
 * The runtime object of the `Admin` interface: its static `create`.
 */
export const Admin = {
  /**
   * Create a new Admin with the given configuration.
   *
   * @param options - The configuration, as `{ configs }` or on its own
   * @returns The new KafkaAdminClient.
   * @throws `KafkaError` when the client cannot be created ("Failed to
   *   create new KafkaAdminClient")
   * @throws `ConfigError` for a key that is not a string
   * @throws `TypeError` when the configuration is not a plain object
   */
  create(
    options:
      | {
          /** The configuration. */
          configs: AdminClientConfigs;
        }
      | AdminClientConfigs,
  ): Admin {
    CREATE_FORMS.match(options, arguments.length);
    return KafkaAdminClient.internalCreate(CREATE_FORMS.value(options, 'configs'));
  },
};
