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

// Java's org.apache.kafka.clients.admin.DescribeTopicsOptions, an option class (root
// CLAUDE.md, JavaScript Binding Conventions, Class family, Q19). Java's getter /
// setter pairs are one method each (Q17).

import { checkInt } from '../internal/java_number';
import { GetterSetter, applyOptionKeys, checkBoolean } from '../internal/options';
import { AbstractOptions } from './abstract_options';

const INCLUDE_AUTHORIZED_OPERATIONS = new GetterSetter('includeAuthorizedOperations', (value) => typeof value === 'boolean');
const PARTITION_SIZE_LIMIT_PER_RESPONSE = new GetterSetter('partitionSizeLimitPerResponse', (value) => typeof value === 'number');

/**
 * Options for `Admin.describeTopics(topics)`.
 */
export class DescribeTopicsOptions extends AbstractOptions {
  #includeAuthorizedOperations = false;
  #partitionSizeLimitPerResponse = 2000;

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
    /**
     * The maximum number of partitions per response.
     *
     * @defaultValue `2000`
     */
    partitionSizeLimitPerResponse?: number;
  }) {
    super();
    applyOptionKeys('DescribeTopicsOptions', options, arguments.length, [
      ['timeoutMs', (v) => this.timeoutMs(v as number | null)],
      ['includeAuthorizedOperations', (v) => this.includeAuthorizedOperations(v as boolean)],
      ['partitionSizeLimitPerResponse', (v) => this.partitionSizeLimitPerResponse(v as number)],
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
   * @returns This DescribeTopicsOptions object.
   */
  includeAuthorizedOperations(includeAuthorizedOperations: boolean): this;
  /**
   * The same as `includeAuthorizedOperations(includeAuthorizedOperations)`.
   *
   * @param options - whether authorized operations are included
   * @returns This DescribeTopicsOptions object.
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

  /**
   * The maximum number of partitions to be returned in a single response.
   *
   * @returns the maximum number of partitions per response
   */
  partitionSizeLimitPerResponse(): number;
  /**
   * Sets the maximum number of partitions to be returned in a single response. This option is only effective when using topic names (not topic IDs), and will not be effective if it is larger than the server-side configuration `max.request.partition.size.limit`.
   *
   * @param partitionSizeLimitPerResponse - the maximum number of partitions per response, on its own
   * @returns This DescribeTopicsOptions object.
   */
  partitionSizeLimitPerResponse(partitionSizeLimitPerResponse: number): this;
  /**
   * The same as `partitionSizeLimitPerResponse(partitionSizeLimitPerResponse)`.
   *
   * @param options - the maximum number of partitions per response
   * @returns This DescribeTopicsOptions object.
   */
  partitionSizeLimitPerResponse(options: {
    /** The maximum number of partitions per response. */
    partitionSizeLimitPerResponse: number;
  }): this;
  /**
   * Reads or sets the value.
   *
   * @param options - the value to set, or nothing to read it
   * @returns the value, or this object
   * @throws `TypeError` when the value is not a 32-bit integer
   */
  partitionSizeLimitPerResponse(options?: unknown): number | this {
    if (!PARTITION_SIZE_LIMIT_PER_RESPONSE.sets(options, arguments.length)) return this.#partitionSizeLimitPerResponse;
    this.#partitionSizeLimitPerResponse = checkInt(PARTITION_SIZE_LIMIT_PER_RESPONSE.value(options), 'partitionSizeLimitPerResponse');
    this.internalRecord('partitionSizeLimitPerResponse', this.#partitionSizeLimitPerResponse);
    return this;
  }
}
