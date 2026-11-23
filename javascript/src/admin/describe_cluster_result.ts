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

// Java's org.apache.kafka.clients.admin.DescribeClusterResult. Its Java
// constructor is package-private: a private constructor here, the client
// building it through `internalCreate` (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Class family).

import { JavaForms } from '../internal/args';
import type { AclOperation } from '../common/acl/acl_operation';
import type { KafkaFuture } from '../common/kafka_future';
import type { Node } from '../common/node';

const NODES_FORMS = new JavaForms('nodes', [[]], { reader: true });
const CONTROLLER_FORMS = new JavaForms('controller', [[]], { reader: true });
const CLUSTER_ID_FORMS = new JavaForms('clusterId', [[]], { reader: true });
const AUTHORIZED_OPERATIONS_FORMS = new JavaForms('authorizedOperations', [[]], { reader: true });

/**
 * The result of the `KafkaAdminClient.describeCluster()` call.
 */
export class DescribeClusterResult {
  readonly #nodes: KafkaFuture<Node[]>;
  readonly #controller: KafkaFuture<Node | null>;
  readonly #clusterId: KafkaFuture<string>;
  readonly #authorizedOperations: KafkaFuture<Set<AclOperation> | null>;

  /**
   * Instances come from the admin client's `describeCluster()`.
   *
   * @param nodes - the nodes' future
   * @param controller - the controller's future
   * @param clusterId - the cluster id's future
   * @param authorizedOperations - the authorized operations' future
   */
  private constructor(
    nodes: KafkaFuture<Node[]>,
    controller: KafkaFuture<Node | null>,
    clusterId: KafkaFuture<string>,
    authorizedOperations: KafkaFuture<Set<AclOperation> | null>,
  ) {
    this.#nodes = nodes;
    this.#controller = controller;
    this.#clusterId = clusterId;
    this.#authorizedOperations = authorizedOperations;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param nodes - the nodes' future
   * @param controller - the controller's future
   * @param clusterId - the cluster id's future
   * @param authorizedOperations - the authorized operations' future
   * @returns the result
   * @internal
   */
  static internalCreate(
    nodes: KafkaFuture<Node[]>,
    controller: KafkaFuture<Node | null>,
    clusterId: KafkaFuture<string>,
    authorizedOperations: KafkaFuture<Set<AclOperation> | null>,
  ): DescribeClusterResult {
    return new DescribeClusterResult(nodes, controller, clusterId, authorizedOperations);
  }

  /**
   * Returns a future which yields a collection of nodes.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  nodes(): KafkaFuture<Node[]> {
    if (arguments.length !== 0) NODES_FORMS.match(undefined, arguments.length);
    return this.#nodes;
  }

  /**
   * Returns a future which yields the current controller node.
   *
   * When using `AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG`, the controller
   * refer to a random broker. When using
   * `AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG`, it refers to the
   * current voter leader.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  controller(): KafkaFuture<Node | null> {
    if (arguments.length !== 0) CONTROLLER_FORMS.match(undefined, arguments.length);
    return this.#controller;
  }

  /**
   * Returns a future which yields the current cluster id.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  clusterId(): KafkaFuture<string> {
    if (arguments.length !== 0) CLUSTER_ID_FORMS.match(undefined, arguments.length);
    return this.#clusterId;
  }

  /**
   * Returns a future which yields authorized operations. The future value
   * will be non-null if the broker supplied this information, and null
   * otherwise.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  authorizedOperations(): KafkaFuture<Set<AclOperation> | null> {
    if (arguments.length !== 0) AUTHORIZED_OPERATIONS_FORMS.match(undefined, arguments.length);
    return this.#authorizedOperations;
  }
}
