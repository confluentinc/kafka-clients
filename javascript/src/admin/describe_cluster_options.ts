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

// Java's org.apache.kafka.clients.admin.DescribeClusterOptions, an option
// class (root CLAUDE.md, JavaScript Binding Conventions, Class family, Q19):
// one keyed constructor object, one key per setter field, and Java's getter /
// setter pairs as one method each (Q17).

import { applyOptionKeys, checkBoolean, GetterSetter } from '../internal/options';
import { AbstractOptions } from './abstract_options';

const isBoolean = (value: unknown): boolean => typeof value === 'boolean';
const INCLUDE_AUTHORIZED_OPERATIONS = new GetterSetter('includeAuthorizedOperations', isBoolean);
const INCLUDE_FENCED_BROKERS = new GetterSetter('includeFencedBrokers', isBoolean);

/**
 * Options for `Admin.describeCluster()`.
 */
export class DescribeClusterOptions extends AbstractOptions {
  #includeAuthorizedOperations = false;
  #includeFencedBrokers = false;

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
     * Whether authorized operations should be included in the response.
     *
     * @defaultValue `false`
     */
    includeAuthorizedOperations?: boolean;
    /**
     * Whether fenced brokers should be included in the response.
     *
     * @defaultValue `false`
     */
    includeFencedBrokers?: boolean;
  }) {
    super();
    applyOptionKeys('DescribeClusterOptions', options, arguments.length, [
      ['timeoutMs', (v) => this.timeoutMs(v as number | null)],
      ['includeAuthorizedOperations', (v) => this.includeAuthorizedOperations(v as boolean)],
      ['includeFencedBrokers', (v) => this.includeFencedBrokers(v as boolean)],
    ]);
  }

  /**
   * Specify if authorized operations should be included in the response.
   * Note that some older brokers cannot supply this information even if it is
   * requested.
   *
   * @returns whether they are included
   */
  includeAuthorizedOperations(): boolean;
  /**
   * Sets whether authorized operations should be included in the response.
   *
   * @param includeAuthorizedOperations - whether to include them, on its own
   * @returns this object
   */
  includeAuthorizedOperations(includeAuthorizedOperations: boolean): this;
  /**
   * The same as `includeAuthorizedOperations(includeAuthorizedOperations)`.
   *
   * @param options - whether to include them
   * @returns this object
   */
  includeAuthorizedOperations(options: {
    /** Whether authorized operations should be included. */
    includeAuthorizedOperations: boolean;
  }): this;
  /**
   * Reads or sets whether authorized operations are included.
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

  /**
   * Specify if fenced brokers should be included in the response. Note that
   * some older brokers cannot supply this information even if it is
   * requested.
   *
   * @returns whether they are included
   */
  includeFencedBrokers(): boolean;
  /**
   * Sets whether fenced brokers should be included in the response.
   *
   * @param includeFencedBrokers - whether to include them, on its own
   * @returns this object
   */
  includeFencedBrokers(includeFencedBrokers: boolean): this;
  /**
   * The same as `includeFencedBrokers(includeFencedBrokers)`.
   *
   * @param options - whether to include them
   * @returns this object
   */
  includeFencedBrokers(options: {
    /** Whether fenced brokers should be included. */
    includeFencedBrokers: boolean;
  }): this;
  /**
   * Reads or sets whether fenced brokers are included.
   *
   * @param options - the value to set, or nothing to read it
   * @returns the value, or this object
   * @throws `TypeError` when the value is not a boolean
   */
  includeFencedBrokers(options?: unknown): boolean | this {
    if (!INCLUDE_FENCED_BROKERS.sets(options, arguments.length)) return this.#includeFencedBrokers;
    this.#includeFencedBrokers = checkBoolean(INCLUDE_FENCED_BROKERS.value(options), 'includeFencedBrokers');
    this.internalRecord('includeFencedBrokers', this.#includeFencedBrokers);
    return this;
  }
}
