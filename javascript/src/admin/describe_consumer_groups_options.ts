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

// Java's org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions, an option class
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family, Q19). Java's getter /
// setter pairs are one method each (Q17).

import { GetterSetter, applyOptionKeys, checkBoolean } from '../internal/options';
import { AbstractOptions } from './abstract_options';

const INCLUDE_AUTHORIZED_OPERATIONS = new GetterSetter('includeAuthorizedOperations', (value) => typeof value === 'boolean');

/**
 * Options for `Admin.describeConsumerGroups(groupIds)`.
 */
export class DescribeConsumerGroupsOptions extends AbstractOptions {
  #includeAuthorizedOperations = false;

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
     * Whether authorized operations are included.
     *
     * @defaultValue `false`
     */
    includeAuthorizedOperations?: boolean;
  }) {
    super();
    applyOptionKeys('DescribeConsumerGroupsOptions', options, arguments.length, [
      ['timeoutMs', (v) => this.timeoutMs(v as number | null)],
      ['includeAuthorizedOperations', (v) => this.includeAuthorizedOperations(v as boolean)],
    ]);
  }

  /**
   * Whether authorized operations are included.
   *
   * @returns whether authorized operations are included
   */
  includeAuthorizedOperations(): boolean;
  /**
   * Sets whether authorized operations are included.
   *
   * @param includeAuthorizedOperations - whether authorized operations are included, on its own
   * @returns This DescribeConsumerGroupsOptions object.
   * @throws `TypeError` when the value is not a boolean
   */
  includeAuthorizedOperations(includeAuthorizedOperations: boolean): this;
  /**
   * The same as `includeAuthorizedOperations(includeAuthorizedOperations)`.
   *
   * @param options - whether authorized operations are included
   * @returns This DescribeConsumerGroupsOptions object.
   * @throws `TypeError` when the value is not a boolean
   */
  includeAuthorizedOperations(options: {
    /** Whether authorized operations are included. */
    includeAuthorizedOperations: boolean;
  }): this;
  /**
   * Reads or sets the value.
   *
   * @param options - the value to set, or nothing to read it
   * @returns the value, or this object
   * @throws `TypeError` when the value is not a boolean
   */
  includeAuthorizedOperations(options?: unknown): boolean | this {
    if (!INCLUDE_AUTHORIZED_OPERATIONS.sets(options, arguments.length)) return this.#includeAuthorizedOperations;
    this.#includeAuthorizedOperations = checkBoolean(INCLUDE_AUTHORIZED_OPERATIONS.value(options), 'includeAuthorizedOperations');
    this.internalRecord('includeAuthorizedOperations', this.#includeAuthorizedOperations);
    return this;
  }
}
