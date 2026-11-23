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

// Java's org.apache.kafka.clients.admin.DescribeConfigsOptions, an option class (root
// CLAUDE.md, JavaScript Binding Conventions, Class family, Q19). Java's getter /
// setter pairs are one method each (Q17).

import { GetterSetter, applyOptionKeys, checkBoolean } from '../internal/options';
import { AbstractOptions } from './abstract_options';

const INCLUDE_SYNONYMS = new GetterSetter('includeSynonyms', (value) => typeof value === 'boolean');
const INCLUDE_DOCUMENTATION = new GetterSetter('includeDocumentation', (value) => typeof value === 'boolean');

/**
 * Options for `Admin.describeConfigs(resources)`.
 */
export class DescribeConfigsOptions extends AbstractOptions {
  #includeSynonyms = false;
  #includeDocumentation = false;

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
     * Whether synonym configs are returned.
     *
     * @defaultValue `false`
     */
    includeSynonyms?: boolean;
    /**
     * Whether config documentation is returned.
     *
     * @defaultValue `false`
     */
    includeDocumentation?: boolean;
  }) {
    super();
    applyOptionKeys('DescribeConfigsOptions', options, arguments.length, [
      ['timeoutMs', (v) => this.timeoutMs(v as number | null)],
      ['includeSynonyms', (v) => this.includeSynonyms(v as boolean)],
      ['includeDocumentation', (v) => this.includeDocumentation(v as boolean)],
    ]);
  }

  /**
   * Return true if synonym configs should be returned in the response.
   *
   * @returns whether synonym configs are returned
   */
  includeSynonyms(): boolean;
  /**
   * Set to true if synonym configs should be returned in the response.
   *
   * @param includeSynonyms - whether synonym configs are returned, on its own
   * @returns This DescribeConfigsOptions object.
   * @throws `TypeError` when the value is not a boolean
   */
  includeSynonyms(includeSynonyms: boolean): this;
  /**
   * The same as `includeSynonyms(includeSynonyms)`.
   *
   * @param options - whether synonym configs are returned
   * @returns This DescribeConfigsOptions object.
   * @throws `TypeError` when the value is not a boolean
   */
  includeSynonyms(options: {
    /** Whether synonym configs are returned. */
    includeSynonyms: boolean;
  }): this;
  /**
   * Reads or sets the value.
   *
   * @param options - the value to set, or nothing to read it
   * @returns the value, or this object
   * @throws `TypeError` when the value is not a boolean
   */
  includeSynonyms(options?: unknown): boolean | this {
    if (!INCLUDE_SYNONYMS.sets(options, arguments.length)) return this.#includeSynonyms;
    this.#includeSynonyms = checkBoolean(INCLUDE_SYNONYMS.value(options), 'includeSynonyms');
    this.internalRecord('includeSynonyms', this.#includeSynonyms);
    return this;
  }

  /**
   * Return true if config documentation should be returned in the response.
   *
   * @returns whether config documentation is returned
   */
  includeDocumentation(): boolean;
  /**
   * Set to true if config documentation should be returned in the response.
   *
   * @param includeDocumentation - whether config documentation is returned, on its own
   * @returns This DescribeConfigsOptions object.
   * @throws `TypeError` when the value is not a boolean
   */
  includeDocumentation(includeDocumentation: boolean): this;
  /**
   * The same as `includeDocumentation(includeDocumentation)`.
   *
   * @param options - whether config documentation is returned
   * @returns This DescribeConfigsOptions object.
   * @throws `TypeError` when the value is not a boolean
   */
  includeDocumentation(options: {
    /** Whether config documentation is returned. */
    includeDocumentation: boolean;
  }): this;
  /**
   * Reads or sets the value.
   *
   * @param options - the value to set, or nothing to read it
   * @returns the value, or this object
   * @throws `TypeError` when the value is not a boolean
   */
  includeDocumentation(options?: unknown): boolean | this {
    if (!INCLUDE_DOCUMENTATION.sets(options, arguments.length)) return this.#includeDocumentation;
    this.#includeDocumentation = checkBoolean(INCLUDE_DOCUMENTATION.value(options), 'includeDocumentation');
    this.internalRecord('includeDocumentation', this.#includeDocumentation);
    return this;
  }
}
