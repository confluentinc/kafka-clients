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
// Binding Conventions, Implementation over the FFI, Q62), as the .NET client
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
// disableTelemetry, setClientInstanceId, advanceTimeOnClientInstanceId and
// addStreamsGroupDescription.

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way (the addon is loaded there alone, javascript/CLAUDE.md "Files and layout")
import native = require('../internal/native');
import { JavaForms, kind } from '../internal/args';
import * as calls from '../internal/admin_calls';
import type { AdminHandle } from '../internal/admin_result';
import { checkInt } from '../internal/java_number';
import { applyOptionKeys, checkBoolean, Setter } from '../internal/options';
import { Node } from '../common/node';
import type { Duration } from '../duration';
import type { Admin } from './admin';
import type { DescribeClusterOptions } from './describe_cluster_options';
import type { DescribeClusterResult } from './describe_cluster_result';
import type { CloseArgs } from './kafka_admin_client';
import type { ListTopicsOptions } from './list_topics_options';
import type { ListTopicsResult } from './list_topics_result';

const CONSTRUCTOR_FORMS = new JavaForms('MockAdminClient', [[], ['brokers', 'controller']]);
const CREATE_FORMS = new JavaForms('create', [[]]);
const CONTROLLER_FORMS = new JavaForms('controller', [['controller']], {
  bareTypes: { controller: (value) => value === null || value instanceof Node },
});
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
   */
  constructor(options: {
    /** The brokers, any iterable of Node. */
    brokers: Iterable<Node>;
    /** The controller, which must be one of the brokers. */
    controller: Node;
  });
  /**
   * Builds the mock.
   *
   * @param options - nothing, or the brokers and the controller
   * @throws `IllegalArgumentError` when the controller is not one of the
   *   brokers ("The controller node must be in the list of brokers"), or for
   *   a form Java has not
   * @throws `TypeError` for a value of the wrong type or a null one
   */
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
  static create(): MockAdminClient.Builder {
    CREATE_FORMS.match(undefined, arguments.length);
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
   */
  controller(options: {
    /** The node, one of the brokers */
    controller: Node;
  }): void;
  /**
   * Makes `controller` the controller `describeCluster` reports.
   *
   * @param controller - the node, one of the brokers, on its own or as `{ controller }`
   * @throws `IllegalArgumentError` when it is not one of the brokers ("The
   *   controller node must be in the list of brokers"), the controller then
   *   unchanged
   * @throws `TypeError` when it is null or not a Node
   */
  controller(controller: unknown): void {
    CONTROLLER_FORMS.match(controller, arguments.length);
    native.mockAdminController(this.#handle, nodeFieldsOrNull(CONTROLLER_FORMS.value(controller, 'controller'), 'controller'));
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
   */
  timeoutNextRequest(options: {
    /** How many RPCs time out */
    numberOfRequest: number;
  }): void;
  /**
   * The next RPCs fail their futures with a `TimeoutError`.
   *
   * @param numberOfRequest - how many RPCs time out, on its own or as `{ numberOfRequest }`
   * @throws `TypeError` or `RangeError` when it is not a Java `int`
   */
  timeoutNextRequest(numberOfRequest: unknown): void {
    TIMEOUT_NEXT_REQUEST_FORMS.match(numberOfRequest, arguments.length);
    native.mockAdminTimeoutNextRequest(
      this.#handle,
      checkInt(TIMEOUT_NEXT_REQUEST_FORMS.value(numberOfRequest, 'numberOfRequest'), 'numberOfRequest'),
    );
  }

  /**
   * Closes the mock, as Java's `close(Duration)`, which does nothing.
   *
   * @returns resolving once the mock closed
   */
  close(): Promise<void>;
  /**
   * Closes the mock, as Java's `close(Duration)`, which does nothing.
   *
   * @param options - the time to use for the wait time
   * @returns resolving once the mock closed
   */
  close(options: CloseArgs): Promise<void>;
  /**
   * The same as `close({ timeout })`: the timeout on its own.
   *
   * @param timeout - The time to use for the wait time.
   * @returns resolving once the mock closed
   */
  close(timeout: Duration): Promise<void>;
  /**
   * Closes the mock.
   *
   * @param options - nothing, or the timeout
   * @returns resolving once the mock closed
   */
  close(options?: unknown): Promise<void> {
    return calls.close(this.#handle, options, arguments.length);
  }

  /**
   * Closes the mock as `close()` does, so that `await using` closes it.
   *
   * @returns resolving once the mock closed
   *
   * @remarks *(js-only)* JavaScript's form of Java's `AutoCloseable`.
   */
  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  /**
   * List the topics of the mock with the default options.
   *
   * @returns The ListTopicsResult.
   */
  listTopics(): ListTopicsResult;
  /**
   * List the topics of the mock.
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
   * Lists the mock's topics.
   *
   * @param options - nothing, or the options
   * @returns The ListTopicsResult.
   * @throws `IllegalArgumentError` for a form Java has not
   * @throws `TypeError` when the options are not a `ListTopicsOptions`, or are null
   */
  listTopics(options?: unknown): ListTopicsResult {
    return calls.listTopics(this.#handle, options, arguments.length);
  }

  /**
   * Describes the mock's cluster with the default options.
   *
   * @returns The DescribeClusterResult.
   */
  describeCluster(): DescribeClusterResult;
  /**
   * Describes the mock's cluster.
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
   * Describes the mock's cluster.
   *
   * @param options - nothing, or the options
   * @returns The DescribeClusterResult.
   * @throws `IllegalArgumentError` for a form Java has not
   * @throws `TypeError` when the options are not a `DescribeClusterOptions`, or are null
   */
  describeCluster(options?: unknown): DescribeClusterResult {
    return calls.describeCluster(this.#handle, options, arguments.length);
  }
}

const isInt = (value: unknown): boolean => typeof value === 'number';
const isMapLike = (value: unknown): boolean => value === null || value instanceof Map;
const NUM_BROKERS = new Setter('numBrokers', null, isInt);
const BROKERS = new Setter('brokers', null, (value) => value === null || (typeof value === 'object' && Symbol.iterator in value));
const CONTROLLER = new Setter('controller', null, isInt, 'index');
const CLUSTER_ID = new Setter('clusterId', null, (value) => value === null || typeof value === 'string');
const BROKER_LOG_DIRS = new Setter('brokerLogDirs', null, (value) => value === null || Array.isArray(value));
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
  else if (typeof value === 'object' && !Array.isArray(value)) entries = Object.entries(value);
  else throw new TypeError(`${name} must be a Map or a plain object, not ${kind(value)}`);
  return entries.map(([k, v]) => {
    if (typeof k !== 'string') throw new TypeError(`${name} must be keyed by strings`);
    return [k, check(v)];
  });
}

/**
 * A Java `short` feature level.
 *
 * @param value - the value given
 * @returns the level
 */
function checkShort(value: unknown): number {
  if (typeof value !== 'number' || !Number.isInteger(value)) throw new TypeError('a feature level must be an integer');
  if (value < -32768 || value > 32767) throw new RangeError('a feature level must be from -32768 to 32767');
  return value;
}

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
  if (!Array.isArray(value)) throw new TypeError(`brokerLogDirs must be an array of arrays of strings, not ${kind(value)}`);
  return value.map((dirs: unknown) => {
    if (!Array.isArray(dirs) || dirs.some((d) => typeof d !== 'string')) {
      throw new TypeError('brokerLogDirs must be an array of arrays of strings');
    }
    return [...(dirs as string[])];
  });
}

// eslint-disable-next-line @typescript-eslint/no-namespace -- Java's nested class MockAdminClient.Builder, reached through the class name as in Java
export namespace MockAdminClient {
  /**
   * Java's `MockAdminClient.Builder`: a builder of mocks.
   *
   * @remarks It holds the C FFI's builder, whose setters run at once, as
   * Java's do. A null list or map goes to the FFI, which reports it as a
   * `TypeError` *(deviation: Java's setters store the null, and `build()`
   * throws later. Why: the FFI checks its mandatory parameters)*.
   */
  export class Builder {
    readonly #native = native.mockAdminBuilderNew();

    /**
     * Java's `new Builder()`, which calls `numBrokers(1)`, then the setter of
     * each key given, in Java's field order.
     *
     * @param options - the setters' values, each key optional
     * @throws `IllegalArgumentError` for a key the builder has not, or a
     *   setter's own error
     * @throws `TypeError` for a value of the wrong type
     *
     * @remarks *(deviation)* Java builds it through its setters only. Why:
     * one call builds a configured object (Q19); the setters stay.
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
     */
    clusterId(clusterId: string): this;
    /**
     * The same as `clusterId(clusterId)`, the parameter given by name.
     *
     * @param options - the cluster id
     * @returns this builder
     */
    clusterId(options: {
      /** The cluster id. */
      clusterId: string;
    }): this;
    /**
     * Sets the cluster id.
     *
     * @param clusterId - on its own or as `{ clusterId }`: the cluster id
     * @returns this builder
     */
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
     */
    brokers(options: {
      /** The brokers, any iterable of Node. */
      brokers: Iterable<Node>;
    }): this;
    /**
     * Sets the brokers: `numBrokers(brokers.size())`, then the list itself.
     *
     * @param brokers - on its own or as `{ brokers }`: the brokers, any iterable of Node
     * @returns this builder
     * @throws `IllegalArgumentError` when the log dirs set are fewer than the brokers
     * @throws `TypeError` when an element is not a Node
     */
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
     */
    numBrokers(numBrokers: number): this;
    /**
     * The same as `numBrokers(numBrokers)`, the parameter given by name.
     *
     * @param options - the broker count
     * @returns this builder
     */
    numBrokers(options: {
      /** The broker count. */
      numBrokers: number;
    }): this;
    /**
     * Sets the broker count: shrinking keeps the first brokers, growing adds
     * `Node(id, "localhost", 1000 + id)` with the default log dirs.
     *
     * @param numBrokers - on its own or as `{ numBrokers }`: the broker count
     * @returns this builder
     * @throws `IllegalArgumentError` for a negative count, or when shrinking
     *   past a shorter log-dir list
     */
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
     */
    controller(index: number): this;
    /**
     * The same as `controller(index)`, the parameter given by name.
     *
     * @param options - the broker's index
     * @returns this builder
     */
    controller(options: {
      /** The broker's index. */
      index: number;
    }): this;
    /**
     * Makes the broker at `index` the controller.
     *
     * @param index - on its own or as `{ index }`: the broker's index
     * @returns this builder
     * @throws `IllegalArgumentError` when the index is out of the broker list
     */
    controller(index: unknown): this {
      native.mockAdminBuilderSet(this.#native, 'controller', checkInt(CONTROLLER.value(index, arguments.length), 'index'));
      return this;
    }

    /**
     * Sets the log dirs of each broker; their count is not checked against
     * the broker count.
     *
     * @param brokerLogDirs - an array of arrays of log dirs, on its own or as `{ brokerLogDirs }`
     * @returns this builder
     */
    brokerLogDirs(brokerLogDirs: ReadonlyArray<ReadonlyArray<string>>): this;
    /**
     * The same as `brokerLogDirs(brokerLogDirs)`, the parameter given by name.
     *
     * @param options - an array of arrays of log dirs
     * @returns this builder
     */
    brokerLogDirs(options: {
      /** An array of arrays of log dirs. */
      brokerLogDirs: ReadonlyArray<ReadonlyArray<string>>;
    }): this;
    /**
     * Sets the log dirs of each broker; their count is not checked against
     * the broker count.
     *
     * @param brokerLogDirs - on its own or as `{ brokerLogDirs }`: an array of arrays of log dirs
     * @returns this builder
     */
    brokerLogDirs(brokerLogDirs: unknown): this {
      native.mockAdminBuilderSet(this.#native, 'brokerLogDirs', logDirs(BROKER_LOG_DIRS.value(brokerLogDirs, arguments.length)));
      return this;
    }

    /**
     * Sets the default replication factor.
     *
     * @param defaultReplicationFactor - the factor, on its own or as `{ defaultReplicationFactor }`
     * @returns this builder
     */
    defaultReplicationFactor(defaultReplicationFactor: number): this;
    /**
     * The same as `defaultReplicationFactor(defaultReplicationFactor)`, the parameter given by name.
     *
     * @param options - the factor
     * @returns this builder
     */
    defaultReplicationFactor(options: {
      /** The factor. */
      defaultReplicationFactor: number;
    }): this;
    /**
     * Sets the default replication factor.
     *
     * @param defaultReplicationFactor - on its own or as `{ defaultReplicationFactor }`: the factor
     * @returns this builder
     */
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
     */
    usingRaftController(usingRaftController: boolean): this;
    /**
     * The same as `usingRaftController(usingRaftController)`, the parameter given by name.
     *
     * @param options - whether it does
     * @returns this builder
     */
    usingRaftController(options: {
      /** Whether it does. */
      usingRaftController: boolean;
    }): this;
    /**
     * Sets whether the mock uses a KRaft controller.
     *
     * @param usingRaftController - on its own or as `{ usingRaftController }`: whether it does
     * @returns this builder
     */
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
     */
    defaultPartitions(numPartitions: number): this;
    /**
     * The same as `defaultPartitions(numPartitions)`, the parameter given by name.
     *
     * @param options - the count, a Java `short`
     * @returns this builder
     */
    defaultPartitions(options: {
      /** The count, a Java `short`. */
      numPartitions: number;
    }): this;
    /**
     * Sets the default partition count.
     *
     * @param numPartitions - on its own or as `{ numPartitions }`: the count, a Java `short`
     * @returns this builder
     */
    defaultPartitions(numPartitions: unknown): this {
      native.mockAdminBuilderSet(this.#native, 'defaultPartitions', checkShort(DEFAULT_PARTITIONS.value(numPartitions, arguments.length)));
      return this;
    }

    /**
     * Sets the finalized feature levels.
     *
     * @param featureLevels - the levels by feature name, a `Map` on its own, or a `Map` or a plain object as `{ featureLevels }`
     * @returns this builder
     */
    featureLevels(featureLevels: ReadonlyMap<string, number>): this;
    /**
     * The same as `featureLevels(featureLevels)`, the parameter given by name.
     *
     * @param options - the levels by feature name, a `Map` on its own, or a `Map` or a plain object as `{ featureLevels }`
     * @returns this builder
     */
    featureLevels(options: {
      /** The levels by feature name, a `Map` on its own, or a `Map` or a plain object as `{ featureLevels }`. */
      featureLevels: ReadonlyMap<string, number> | Readonly<Record<string, number>>;
    }): this;
    /**
     * Sets the finalized feature levels.
     *
     * @param featureLevels - on its own or as `{ featureLevels }`: the levels by feature name, a `Map` on its own, or a `Map` or a plain object as `{ featureLevels }`
     * @returns this builder
     */
    featureLevels(featureLevels: unknown): this {
      const value = FEATURE_LEVELS.value(featureLevels, arguments.length);
      native.mockAdminBuilderSet(this.#native, 'featureLevels', stringMapPairs(value, 'featureLevels', checkShort));
      return this;
    }

    /**
     * Sets the minimum supported feature levels.
     *
     * @param minSupportedFeatureLevels - the levels by feature name, a `Map` on its own, or a `Map` or a plain object by name
     * @returns this builder
     */
    minSupportedFeatureLevels(minSupportedFeatureLevels: ReadonlyMap<string, number>): this;
    /**
     * The same as `minSupportedFeatureLevels(minSupportedFeatureLevels)`, the parameter given by name.
     *
     * @param options - the levels by feature name, a `Map` on its own, or a `Map` or a plain object by name
     * @returns this builder
     */
    minSupportedFeatureLevels(options: {
      /** The levels by feature name, a `Map` on its own, or a `Map` or a plain object by name. */
      minSupportedFeatureLevels: ReadonlyMap<string, number> | Readonly<Record<string, number>>;
    }): this;
    /**
     * Sets the minimum supported feature levels.
     *
     * @param minSupportedFeatureLevels - on its own or as `{ minSupportedFeatureLevels }`: the levels by feature name, a `Map` on its own, or a `Map` or a plain object by name
     * @returns this builder
     */
    minSupportedFeatureLevels(minSupportedFeatureLevels: unknown): this {
      const value = MIN_SUPPORTED_FEATURE_LEVELS.value(minSupportedFeatureLevels, arguments.length);
      native.mockAdminBuilderSet(this.#native, 'minSupportedFeatureLevels', stringMapPairs(value, 'minSupportedFeatureLevels', checkShort));
      return this;
    }

    /**
     * Sets the maximum supported feature levels.
     *
     * @param maxSupportedFeatureLevels - the levels by feature name, a `Map` on its own, or a `Map` or a plain object by name
     * @returns this builder
     */
    maxSupportedFeatureLevels(maxSupportedFeatureLevels: ReadonlyMap<string, number>): this;
    /**
     * The same as `maxSupportedFeatureLevels(maxSupportedFeatureLevels)`, the parameter given by name.
     *
     * @param options - the levels by feature name, a `Map` on its own, or a `Map` or a plain object by name
     * @returns this builder
     */
    maxSupportedFeatureLevels(options: {
      /** The levels by feature name, a `Map` on its own, or a `Map` or a plain object by name. */
      maxSupportedFeatureLevels: ReadonlyMap<string, number> | Readonly<Record<string, number>>;
    }): this;
    /**
     * Sets the maximum supported feature levels.
     *
     * @param maxSupportedFeatureLevels - on its own or as `{ maxSupportedFeatureLevels }`: the levels by feature name, a `Map` on its own, or a `Map` or a plain object by name
     * @returns this builder
     */
    maxSupportedFeatureLevels(maxSupportedFeatureLevels: unknown): this {
      const value = MAX_SUPPORTED_FEATURE_LEVELS.value(maxSupportedFeatureLevels, arguments.length);
      native.mockAdminBuilderSet(this.#native, 'maxSupportedFeatureLevels', stringMapPairs(value, 'maxSupportedFeatureLevels', checkShort));
      return this;
    }

    /**
     * Sets the group configs overlaid onto every group's.
     *
     * @param defaultGroupConfigs - the configs, a `Map` on its own, or a `Map` or a plain object by name
     * @returns this builder
     */
    defaultGroupConfigs(defaultGroupConfigs: ReadonlyMap<string, string>): this;
    /**
     * The same as `defaultGroupConfigs(defaultGroupConfigs)`, the parameter given by name.
     *
     * @param options - the configs, a `Map` on its own, or a `Map` or a plain object by name
     * @returns this builder
     */
    defaultGroupConfigs(options: {
      /** The configs, a `Map` on its own, or a `Map` or a plain object by name. */
      defaultGroupConfigs: ReadonlyMap<string, string> | Readonly<Record<string, string>>;
    }): this;
    /**
     * Sets the group configs overlaid onto every group's.
     *
     * @param defaultGroupConfigs - on its own or as `{ defaultGroupConfigs }`: the configs, a `Map` on its own, or a `Map` or a plain object by name
     * @returns this builder
     */
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
    build(): MockAdminClient {
      BUILD_FORMS.match(undefined, arguments.length);
      return MockAdminClient.internalCreate(native.mockAdminBuilderBuild(this.#native));
    }
  }
}
