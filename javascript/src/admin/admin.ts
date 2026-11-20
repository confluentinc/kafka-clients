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
import type { DescribeClusterOptions } from './describe_cluster_options';
import type { DescribeClusterResult } from './describe_cluster_result';
import type { CloseArgs } from './kafka_admin_client';
import { KafkaAdminClient } from './kafka_admin_client';
import type { ListTopicsOptions } from './list_topics_options';
import type { ListTopicsResult } from './list_topics_result';

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
