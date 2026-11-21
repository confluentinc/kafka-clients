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
// builds it through `internalCreate` (root CLAUDE.md, JavaScript Binding
// Conventions, Class family, Q16). Every RPC returns its Result at once, as
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
 * by invoking `Admin.create`.
 *
 * Instances come from `Admin.create`.
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
   * Close the Admin and release all associated resources.
   *
   * @returns resolving once the client closed
   */
  close(): Promise<void>;
  /**
   * Close the Admin client and release all associated resources, with a
   * grace period during which current operations will be allowed to
   * complete. Once it is over, all operations that have not yet been
   * completed will be aborted with a `TimeoutError`.
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
   * Closes the client.
   *
   * @param options - nothing, or the timeout
   * @returns resolving once the client closed
   *
   * @remarks A negative timeout is read as zero *(deviation: Java's
   * `KafkaAdminClient.close` throws `IllegalArgumentException("The timeout
   * cannot be negative.")`. Why: the binding repeats no precondition. Goes
   * when: the FFI returns Java's error)*. A call after `close()` goes to the
   * core: an RPC returns its Result, whose futures fail with the core's
   * error.
   */
  close(options?: unknown): Promise<void> {
    return calls.close(this.#handle, options, arguments.length);
  }

  /**
   * Closes the client as `close()` does, so that `await using` closes it.
   *
   * @returns resolving once the client closed
   *
   * @remarks *(js-only)* JavaScript's form of Java's `AutoCloseable`.
   */
  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

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
   * Lists the topics.
   *
   * @param options - nothing, or the options
   * @returns The ListTopicsResult.
   * @throws `IllegalArgumentError` for a form Java has not
   * @throws `TypeError` when the options are not a `ListTopicsOptions`, or
   *   are null
   */
  listTopics(options?: unknown): ListTopicsResult {
    return calls.listTopics(this.#handle, options, arguments.length);
  }

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
   * Describes the cluster.
   *
   * @param options - nothing, or the options
   * @returns The DescribeClusterResult.
   * @throws `IllegalArgumentError` for a form Java has not
   * @throws `TypeError` when the options are not a `DescribeClusterOptions`,
   *   or are null
   */
  describeCluster(options?: unknown): DescribeClusterResult {
    return calls.describeCluster(this.#handle, options, arguments.length);
  }

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
   * Checks the call's form and runs the RPC.
   *
   * @param args - the call's argument
   * @returns the result
   * @throws `IllegalArgumentError` for a form Java has not
   * @throws `TypeError` for an argument of the wrong type, or a null one
   */
  createTopics(args: unknown): CreateTopicsResult {
    return calls.createTopics(this.#handle, args, arguments.length);
  }

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
   * Checks the call's form and runs the RPC.
   *
   * @param args - the call's argument
   * @returns the result
   * @throws `IllegalArgumentError` for a form Java has not
   * @throws `TypeError` for an argument of the wrong type, or a null one
   */
  deleteTopics(args: unknown): DeleteTopicsResult {
    return calls.deleteTopics(this.#handle, args, arguments.length);
  }

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
   * Checks the call's form and runs the RPC.
   *
   * @param args - the call's argument
   * @returns the result
   * @throws `IllegalArgumentError` for a form Java has not
   * @throws `TypeError` for an argument of the wrong type, or a null one
   */
  describeTopics(args: unknown): DescribeTopicsResult {
    return calls.describeTopics(this.#handle, args, arguments.length);
  }

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
  createPartitions(newPartitions: ReadonlyMap<string, NewPartitions> | Readonly<Record<string, NewPartitions>>): CreatePartitionsResult;
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
   * Checks the call's form and runs the RPC.
   *
   * @param args - the call's argument
   * @returns the result
   * @throws `IllegalArgumentError` for a form Java has not
   * @throws `TypeError` for an argument of the wrong type, or a null one
   */
  createPartitions(args: unknown): CreatePartitionsResult {
    return calls.createPartitions(this.#handle, args, arguments.length);
  }

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
   * Checks the call's form and runs the RPC.
   *
   * @param args - the call's argument
   * @returns the result
   * @throws `IllegalArgumentError` for a form Java has not
   * @throws `TypeError` for an argument of the wrong type, or a null one
   */
  deleteRecords(args: unknown): DeleteRecordsResult {
    return calls.deleteRecords(this.#handle, args, arguments.length);
  }

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
   * Checks the call's form and runs the RPC.
   *
   * @param args - the call's argument
   * @returns the result
   * @throws `IllegalArgumentError` for a form Java has not
   * @throws `TypeError` for an argument of the wrong type, or a null one
   */
  describeConfigs(args: unknown): DescribeConfigsResult {
    return configCalls.describeConfigs(this.#handle, args, arguments.length);
  }

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
   * Checks the call's form and runs the RPC.
   *
   * @param args - the call's argument
   * @returns the result
   * @throws `IllegalArgumentError` for a form Java has not
   * @throws `TypeError` for an argument of the wrong type, or a null one
   */
  incrementalAlterConfigs(args: unknown): AlterConfigsResult {
    return configCalls.incrementalAlterConfigs(this.#handle, args, arguments.length);
  }

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
   * Checks the call's form and runs the RPC.
   *
   * @param args - the call's argument
   * @returns the result
   * @throws `IllegalArgumentError` for a form Java has not
   * @throws `TypeError` for an argument of the wrong type, or a null one
   */
  alterReplicaLogDirs(args: unknown): AlterReplicaLogDirsResult {
    return configCalls.alterReplicaLogDirs(this.#handle, args, arguments.length);
  }

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
   * Checks the call's form and runs the RPC.
   *
   * @param args - the call's argument
   * @returns the result
   * @throws `IllegalArgumentError` for a form Java has not
   * @throws `TypeError` for an argument of the wrong type, or a null one
   */
  describeLogDirs(args: unknown): DescribeLogDirsResult {
    return configCalls.describeLogDirs(this.#handle, args, arguments.length);
  }

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
   * Checks the call's form and runs the RPC.
   *
   * @param args - the call's argument
   * @returns the result
   * @throws `IllegalArgumentError` for a form Java has not
   * @throws `TypeError` for an argument of the wrong type, or a null one
   */
  describeReplicaLogDirs(args: unknown): DescribeReplicaLogDirsResult {
    return configCalls.describeReplicaLogDirs(this.#handle, args, arguments.length);
  }

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
   * Checks the call's form and runs the RPC.
   *
   * @param args - the call's argument
   * @returns the result
   * @throws `IllegalArgumentError` for a form Java has not
   * @throws `TypeError` for an argument of the wrong type, or a null one
   */
  listConfigResources(args?: unknown): ListConfigResourcesResult {
    return configCalls.listConfigResources(this.#handle, args, arguments.length);
  }
}
