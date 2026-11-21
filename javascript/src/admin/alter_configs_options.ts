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

// Java's org.apache.kafka.clients.admin.AlterConfigsOptions, an option class
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family, Q19). Java's setter
// validateOnly keeps its reader's own name, shouldValidateOnly (Q17).

import { JavaForms } from '../internal/args';
import { Setter, applyOptionKeys, checkBoolean } from '../internal/options';
import { AbstractOptions } from './abstract_options';

const VALIDATE_ONLY = new Setter('validateOnly', 'shouldValidateOnly', (value) => typeof value === 'boolean');
const VALIDATE_ONLY_READER_FORMS = new JavaForms('shouldValidateOnly', [[]], { reader: true });

/**
 * Options for `Admin.incrementalAlterConfigs(configs)`.
 */
export class AlterConfigsOptions extends AbstractOptions {
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
     * Whether the request is only validated.
     *
     * @defaultValue `false`
     */
    validateOnly?: boolean;
  }) {
    super();
    applyOptionKeys('AlterConfigsOptions', options, arguments.length, [
      ['timeoutMs', (v) => this.timeoutMs(v as number | null)],
      ['validateOnly', (v) => this.validateOnly(v as boolean)],
    ]);
  }

  /**
   * Set to true if the request should be validated without altering the configs.
   *
   * @param validateOnly - whether the request is only validated, on its own
   * @returns This AlterConfigsOptions object.
   */
  validateOnly(validateOnly: boolean): this;
  /**
   * The same as `validateOnly(validateOnly)`.
   *
   * @param options - whether the request is only validated
   * @returns This AlterConfigsOptions object.
   */
  validateOnly(options: {
    /** Whether the request is only validated. */
    validateOnly: boolean;
  }): this;
  /**
   * Sets the value.
   *
   * @param options - the value
   * @returns this object
   * @throws `TypeError` when the value is missing or not a boolean
   */
  validateOnly(options: unknown): this {
    this.#validateOnly = checkBoolean(VALIDATE_ONLY.value(options, arguments.length), 'validateOnly');
    this.internalRecord('validateOnly', this.#validateOnly);
    return this;
  }

  /**
   * Return true if the request should be validated without altering the configs.
   *
   * @returns whether the request is only validated
   * @throws `TypeError` when it is given an argument: it only reads
   */
  shouldValidateOnly(): boolean {
    if (arguments.length !== 0) VALIDATE_ONLY_READER_FORMS.match(undefined, arguments.length);
    return this.#validateOnly;
  }
}
