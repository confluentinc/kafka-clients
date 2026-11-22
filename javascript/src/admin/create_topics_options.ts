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

// Java's org.apache.kafka.clients.admin.CreateTopicsOptions, an option class
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family, Q19). Java's setters
// validateOnly and retryOnQuotaViolation keep their readers' own names
// (Q17).

import { JavaForms } from '../internal/args';
import { Setter, applyOptionKeys, checkBoolean } from '../internal/options';
import { AbstractOptions } from './abstract_options';

const VALIDATE_ONLY = new Setter('validateOnly', 'shouldValidateOnly', (value) => typeof value === 'boolean');
const VALIDATE_ONLY_READER_FORMS = new JavaForms('shouldValidateOnly', [[]], { reader: true });
const RETRY_ON_QUOTA_VIOLATION = new Setter('retryOnQuotaViolation', 'shouldRetryOnQuotaViolation', (value) => typeof value === 'boolean');
const RETRY_ON_QUOTA_VIOLATION_READER_FORMS = new JavaForms('shouldRetryOnQuotaViolation', [[]], { reader: true });

/**
 * Options for `Admin.createTopics(newTopics)`.
 */
export class CreateTopicsOptions extends AbstractOptions {
  #validateOnly = false;
  #retryOnQuotaViolation = true;

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
    /**
     * Whether a quota violation is retried.
     *
     * @defaultValue `true`
     */
    retryOnQuotaViolation?: boolean;
  }) {
    super();
    applyOptionKeys('CreateTopicsOptions', options, arguments.length, [
      ['timeoutMs', (v) => this.timeoutMs(v as number | null)],
      ['validateOnly', (v) => this.validateOnly(v as boolean)],
      ['retryOnQuotaViolation', (v) => this.retryOnQuotaViolation(v as boolean)],
    ]);
  }

  /**
   * Set to true if the request should be validated without creating the topic.
   *
   * @param validateOnly - whether the request is only validated, on its own
   * @returns This CreateTopicsOptions object.
   * @throws `TypeError` when the value is missing or not a boolean
   */
  validateOnly(validateOnly: boolean): this;
  /**
   * The same as `validateOnly(validateOnly)`.
   *
   * @param options - whether the request is only validated
   * @returns This CreateTopicsOptions object.
   * @throws `TypeError` when the value is missing or not a boolean
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
   * Return true if the request should be validated without creating the topic.
   *
   * @returns whether the request is only validated
   * @throws `TypeError` when it is given an argument: it only reads
   */
  shouldValidateOnly(): boolean {
    if (arguments.length !== 0) VALIDATE_ONLY_READER_FORMS.match(undefined, arguments.length);
    return this.#validateOnly;
  }

  /**
   * Set to true if quota violation should be automatically retried.
   *
   * @param retryOnQuotaViolation - whether a quota violation is retried, on its own
   * @returns This CreateTopicsOptions object.
   * @throws `TypeError` when the value is missing or not a boolean
   */
  retryOnQuotaViolation(retryOnQuotaViolation: boolean): this;
  /**
   * The same as `retryOnQuotaViolation(retryOnQuotaViolation)`.
   *
   * @param options - whether a quota violation is retried
   * @returns This CreateTopicsOptions object.
   * @throws `TypeError` when the value is missing or not a boolean
   */
  retryOnQuotaViolation(options: {
    /** Whether a quota violation is retried. */
    retryOnQuotaViolation: boolean;
  }): this;
  /**
   * Sets the value.
   *
   * @param options - the value
   * @returns this object
   * @throws `TypeError` when the value is missing or not a boolean
   */
  retryOnQuotaViolation(options: unknown): this {
    this.#retryOnQuotaViolation = checkBoolean(RETRY_ON_QUOTA_VIOLATION.value(options, arguments.length), 'retryOnQuotaViolation');
    this.internalRecord('retryOnQuotaViolation', this.#retryOnQuotaViolation);
    return this;
  }

  /**
   * Returns true if quota violation should be automatically retried.
   *
   * @returns whether a quota violation is retried
   * @throws `TypeError` when it is given an argument: it only reads
   */
  shouldRetryOnQuotaViolation(): boolean {
    if (arguments.length !== 0) RETRY_ON_QUOTA_VIOLATION_READER_FORMS.match(undefined, arguments.length);
    return this.#retryOnQuotaViolation;
  }
}
