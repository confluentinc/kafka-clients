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

// Java's org.apache.kafka.clients.admin.DescribeFeaturesOptions, an option
// class (root CLAUDE.md, JavaScript Binding Conventions, Class family, Q19).
// Java's getter / setter pair `nodeId` is one method (Q17); its `OptionalInt`
// reads as `number | null` (Types).

import { checkInt } from '../internal/java_number';
import { GetterSetter, applyOptionKeys } from '../internal/options';
import { AbstractOptions } from './abstract_options';

const NODE_ID = new GetterSetter('nodeId', (value) => typeof value === 'number');

/**
 * Options for `Admin.describeFeatures({ options })`.
 */
export class DescribeFeaturesOptions extends AbstractOptions {
  #nodeId: number | null = null;

  /**
   * The options, each key calling its setter.
   *
   * @param options - the setters' values, each key optional
   * @throws `IllegalArgumentError` for a key the class has not
   * @throws `TypeError` for a value of the wrong type
   *
   * @remarks *(deviation)* Java builds the options through their setters
   * only. Why: one call builds a configured object (Q19); the setters stay.
   */
  constructor(options?: {
    /**
     * The timeout in milliseconds, or null for the default api timeout.
     *
     * @defaultValue `null`
     */
    timeoutMs?: number | null;
    /**
     * The node id to which the request should be sent.
     *
     * @defaultValue none (Java's `OptionalInt.empty()`)
     */
    nodeId?: number;
  }) {
    super();
    applyOptionKeys('DescribeFeaturesOptions', options, arguments.length, [
      ['timeoutMs', (v) => this.timeoutMs(v as number | null)],
      ['nodeId', (v) => this.nodeId(v as number)],
    ]);
  }

  /**
   * The node id to which the request should be sent. If the node id is
   * empty, the request will be sent to the arbitrary controller/broker.
   *
   * @returns the node id, or null when none was set
   */
  nodeId(): number | null;
  /**
   * Set the node id to which the request should be sent.
   *
   * @param nodeId - the node id, on its own
   * @returns This DescribeFeaturesOptions object.
   * @throws `TypeError` when the value is not an integer (null included:
   *   Java's `int` cannot hold null)
   * @throws `RangeError` when it is beyond Java's `int`
   */
  nodeId(nodeId: number): this;
  /**
   * The same as `nodeId(nodeId)`.
   *
   * @param options - the node id
   * @returns This DescribeFeaturesOptions object.
   * @throws `TypeError` when the value is not an integer (null included:
   *   Java's `int` cannot hold null)
   * @throws `RangeError` when it is beyond Java's `int`
   */
  nodeId(options: {
    /** The node id. */
    nodeId: number;
  }): this;
  /**
   * Reads or sets the node id.
   *
   * @param options - the value to set, or nothing to read it
   * @returns the value, or this object
   * @throws `TypeError` when the value is not an integer (null included:
   *   Java's `int` cannot hold null)
   * @throws `RangeError` when it is beyond Java's `int`
   */
  nodeId(options?: unknown): number | null | this {
    if (!NODE_ID.sets(options, arguments.length)) return this.#nodeId;
    this.#nodeId = checkInt(NODE_ID.value(options), 'nodeId');
    this.internalRecord('nodeId', this.#nodeId);
    return this;
  }
}
