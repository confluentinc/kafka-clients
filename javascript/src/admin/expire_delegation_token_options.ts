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

// Java's org.apache.kafka.clients.admin.ExpireDelegationTokenOptions, an option class (root CLAUDE.md,
// JavaScript Binding Conventions, Class family, Q19). Java's getter / setter
// pair is one method (Q17); its `long` is a `bigint` (Types).

import { GetterSetter, applyOptionKeys } from '../internal/options';
import { checkLong } from '../internal/java_number';
import { AbstractOptions } from './abstract_options';

const EXPIRY_TIME_PERIOD_MS = new GetterSetter('expiryTimePeriodMs', (value) => typeof value === 'bigint' || typeof value === 'number');

/**
 * Options for `Admin.expireDelegationToken(hmac)`.
 */
export class ExpireDelegationTokenOptions extends AbstractOptions {
  #expiryTimePeriodMs = -1n;

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
     * The time period until we should expire this token.
     *
     * @defaultValue `-1n`
     */
    expiryTimePeriodMs?: bigint | number;
  }) {
    super();
    applyOptionKeys('ExpireDelegationTokenOptions', options, arguments.length, [
      ['timeoutMs', (v) => this.timeoutMs(v as number | null)],
      ['expiryTimePeriodMs', (v) => this.expiryTimePeriodMs(v as bigint)],
    ]);
  }

  /**
   * The time period until we should expire this token.
   *
   * @returns the value, in milliseconds
   */
  expiryTimePeriodMs(): bigint;
  /**
   * Sets the time period until we should expire this token. `expiryTimePeriodMs` >= 0: the token will update the expiration timestamp to min(now + expiryTimePeriodMs, maxTimestamp); `expiryTimePeriodMs` < 0: token will be expired immediately.
   *
   * @param expiryTimePeriodMs - the value in milliseconds, a `long`, on its own
   * @returns This ExpireDelegationTokenOptions object.
   * @throws `TypeError` when the value is not a whole number
   * @throws `RangeError` when it is outside Java's `long`
   */
  expiryTimePeriodMs(expiryTimePeriodMs: bigint | number): this;
  /**
   * The same as `expiryTimePeriodMs(expiryTimePeriodMs)`, the parameter given by name.
   *
   * @param options - the value in milliseconds
   * @returns This ExpireDelegationTokenOptions object.
   * @throws `TypeError` when the value is not a whole number
   * @throws `RangeError` when it is outside Java's `long`
   */
  expiryTimePeriodMs(options: {
    /** The value in milliseconds. */
    expiryTimePeriodMs: bigint | number;
  }): this;
  /**
   * Reads or sets the value.
   *
   * @param options - the value to set, or nothing to read it
   * @returns the value, or this object
   * @throws `TypeError` when the value is not a whole number
   * @throws `RangeError` when it is outside Java's `long`
   */
  expiryTimePeriodMs(options?: unknown): bigint | this {
    if (!EXPIRY_TIME_PERIOD_MS.sets(options, arguments.length)) return this.#expiryTimePeriodMs;
    this.#expiryTimePeriodMs = checkLong(EXPIRY_TIME_PERIOD_MS.value(options), 'expiryTimePeriodMs');
    this.internalRecord('expiryTimePeriodMs', this.#expiryTimePeriodMs);
    return this;
  }
}
