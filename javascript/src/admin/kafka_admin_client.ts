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
import type { DescribeClusterOptions } from './describe_cluster_options';
import type { DescribeClusterResult } from './describe_cluster_result';
import type { ListTopicsOptions } from './list_topics_options';
import type { ListTopicsResult } from './list_topics_result';

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
}
