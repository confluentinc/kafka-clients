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

// Java's org.apache.kafka.clients.admin.DeleteTopicsOptions, an option class (root
// CLAUDE.md, JavaScript Binding Conventions, Class family, Q19). Java's setter retryOnQuotaViolation keeps
// its reader's own name (Q17).

import { JavaForms } from '../internal/args';
import { Setter, applyOptionKeys, checkBoolean } from '../internal/options';
import { AbstractOptions } from './abstract_options';

const RETRY_ON_QUOTA_VIOLATION = new Setter('retryOnQuotaViolation', 'shouldRetryOnQuotaViolation', (value) => typeof value === 'boolean');
const RETRY_ON_QUOTA_VIOLATION_READER_FORMS = new JavaForms('shouldRetryOnQuotaViolation', [[]], { reader: true });

/**
 * Options for `Admin.deleteTopics(topics)`.
 */
export class DeleteTopicsOptions extends AbstractOptions {
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
     * Whether a quota violation is retried.
     *
     * @defaultValue `true`
     */
    retryOnQuotaViolation?: boolean;
  }) {
    super();
    applyOptionKeys('DeleteTopicsOptions', options, arguments.length, [
      ['timeoutMs', (v) => this.timeoutMs(v as number | null)],
      ['retryOnQuotaViolation', (v) => this.retryOnQuotaViolation(v as boolean)],
    ]);
  }

  /**
   * Set to true if quota violation should be automatically retried.
   *
   * @param retryOnQuotaViolation - whether a quota violation is retried, on its own
   * @returns This DeleteTopicsOptions object.
   * @throws `TypeError` when the value is missing or not a boolean
   */
  retryOnQuotaViolation(retryOnQuotaViolation: boolean): this;
  /**
   * The same as `retryOnQuotaViolation(retryOnQuotaViolation)`.
   *
   * @param options - whether a quota violation is retried
   * @returns This DeleteTopicsOptions object.
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
