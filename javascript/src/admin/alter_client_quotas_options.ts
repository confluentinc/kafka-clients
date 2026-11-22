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

// Java's org.apache.kafka.clients.admin.AlterClientQuotasOptions, an option class (root
// CLAUDE.md, JavaScript Binding Conventions, Class family, Q19). Java's getter /
// setter pairs are one method each (Q17).

import { GetterSetter, applyOptionKeys, checkBoolean } from '../internal/options';
import { AbstractOptions } from './abstract_options';

const VALIDATE_ONLY = new GetterSetter('validateOnly', (value) => typeof value === 'boolean');

/**
 * Options for `Admin.alterClientQuotas(entries)`.
 */
export class AlterClientQuotasOptions extends AbstractOptions {
  #validateOnly = false;

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
     * Whether the request should be validated without altering the configs.
     *
     * @defaultValue `false`
     */
    validateOnly?: boolean;
  }) {
    super();
    applyOptionKeys('AlterClientQuotasOptions', options, arguments.length, [
      ['timeoutMs', (v) => this.timeoutMs(v as number | null)],
      ['validateOnly', (v) => this.validateOnly(v as boolean)],
    ]);
  }

  /**
   * Returns whether the request should be validated without altering the
   * configs.
   *
   * @returns whether it should
   */
  validateOnly(): boolean;
  /**
   * Sets whether the request should be validated without altering the
   * configs.
   *
   * @param validateOnly - whether it should, on its own
   * @returns This AlterClientQuotasOptions object.
   * @throws `TypeError` when the value is not a boolean
   */
  validateOnly(validateOnly: boolean): this;
  /**
   * The same as `validateOnly(validateOnly)`.
   *
   * @param options - whether it should
   * @returns This AlterClientQuotasOptions object.
   * @throws `TypeError` when the value is not a boolean
   */
  validateOnly(options: {
    /** Whether the request should be validated without altering the configs. */
    validateOnly: boolean;
  }): this;
  /**
   * Reads or sets the value.
   *
   * @param options - the value to set, or nothing to read it
   * @returns the value, or this object
   * @throws `TypeError` when the value is not a boolean
   */
  validateOnly(options?: unknown): boolean | this {
    if (!VALIDATE_ONLY.sets(options, arguments.length)) return this.#validateOnly;
    this.#validateOnly = checkBoolean(VALIDATE_ONLY.value(options), 'validateOnly');
    this.internalRecord('validateOnly', this.#validateOnly);
    return this;
  }
}
