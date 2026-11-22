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

// Java's org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions,
// an option class (root CLAUDE.md, JavaScript Binding Conventions, Class
// family, Q19). Java's getter / setter pair is one method (Q17), its setter's
// parameter keeping Java's name, `allow`.

import { GetterSetter, applyOptionKeys, checkBoolean } from '../internal/options';
import { AbstractOptions } from './abstract_options';

const ALLOW_REPLICATION_FACTOR_CHANGE = new GetterSetter('allowReplicationFactorChange', (value) => typeof value === 'boolean', 'allow');

/**
 * Options for `Admin.alterPartitionReassignments(reassignments, options)`.
 */
export class AlterPartitionReassignmentsOptions extends AbstractOptions {
  #allowReplicationFactorChange = true;

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
     * Whether the call may alter the replication factor of a partition.
     *
     * @defaultValue `true`
     */
    allowReplicationFactorChange?: boolean;
  }) {
    super();
    applyOptionKeys('AlterPartitionReassignmentsOptions', options, arguments.length, [
      ['timeoutMs', (v) => this.timeoutMs(v as number | null)],
      ['allowReplicationFactorChange', (v) => this.allowReplicationFactorChange(v as boolean)],
    ]);
  }

  /**
   * A boolean indicating if the alter partition reassignments should be
   * allowed to alter the replication factor of a partition. In cases where it
   * is not allowed, any replication factor change will result in an error
   * thrown by the API.
   *
   * @returns whether the replication factor may change
   */
  allowReplicationFactorChange(): boolean;
  /**
   * Set the option indicating if the alter partition reassignments call
   * should be allowed to alter the replication factor of a partition. In
   * cases where it is not allowed, any replication factor change will result
   * in an error thrown by the API.
   *
   * @param allow - whether the replication factor may change, on its own
   * @returns This AlterPartitionReassignmentsOptions object.
   * @throws `TypeError` when the value is not a boolean
   */
  allowReplicationFactorChange(allow: boolean): this;
  /**
   * The same as `allowReplicationFactorChange(allow)`.
   *
   * @param options - whether the replication factor may change
   * @returns This AlterPartitionReassignmentsOptions object.
   * @throws `TypeError` when the value is not a boolean
   */
  allowReplicationFactorChange(options: {
    /** Whether the replication factor may change. */
    allow: boolean;
  }): this;
  /**
   * Reads or sets the value.
   *
   * @param options - the value to set, or nothing to read it
   * @returns the value, or this object
   * @throws `TypeError` when the value is not a boolean
   */
  allowReplicationFactorChange(options?: unknown): boolean | this {
    if (!ALLOW_REPLICATION_FACTOR_CHANGE.sets(options, arguments.length)) return this.#allowReplicationFactorChange;
    this.#allowReplicationFactorChange = checkBoolean(ALLOW_REPLICATION_FACTOR_CHANGE.value(options), 'allow');
    this.internalRecord('allowReplicationFactorChange', this.#allowReplicationFactorChange);
    return this;
  }
}
