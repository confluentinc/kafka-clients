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

// Java's org.apache.kafka.clients.admin.RenewDelegationTokenOptions, an option class (javascript/CLAUDE.md,
// JavaScript Binding Conventions, Class family). Java's getter / setter
// pair is one method; its `long` is a `bigint` (Types).

import { GetterSetter, applyOptionKeys } from '../internal/options';
import { checkLong } from '../internal/java_number';
import { AbstractOptions } from './abstract_options';

const RENEW_TIME_PERIOD_MS = new GetterSetter('renewTimePeriodMs', (value) => typeof value === 'bigint' || typeof value === 'number');

/**
 * Options for `Admin.renewDelegationToken(hmac)`.
 */
export class RenewDelegationTokenOptions extends AbstractOptions {
  #renewTimePeriodMs = -1n;

  /**
   * The options, each key calling its setter.
   *
   * @param options - the setters' values, each key optional
   * @throws `IllegalArgumentError` for a key the class has not
   * @throws `TypeError` for a value of the wrong type
   *
   * @remarks *(deviation)* Java builds the options through their setters
   * only. Why: one call builds a configured object; the setters stay.
   */
  constructor(options?: {
    /**
     * The timeout in milliseconds, or null for the default api timeout.
     *
     * @defaultValue `null`
     */
    timeoutMs?: number | null;
    /**
     * The renew time period in milliseconds, `-1` for the server's default.
     *
     * @defaultValue `-1n`
     */
    renewTimePeriodMs?: bigint | number;
  }) {
    super();
    applyOptionKeys('RenewDelegationTokenOptions', options, arguments.length, [
      ['timeoutMs', (v) => this.timeoutMs(v as number | null)],
      ['renewTimePeriodMs', (v) => this.renewTimePeriodMs(v as bigint)],
    ]);
  }

  /**
   * The renew time period in milliseconds, `-1` for the server's default.
   *
   * @returns the value, in milliseconds
   */
  renewTimePeriodMs(): bigint;
  /**
   * Sets the renew time period in milliseconds, `-1` for the server's default.
   *
   * @param renewTimePeriodMs - the value in milliseconds, a `long`, on its own
   * @returns This RenewDelegationTokenOptions object.
   * @throws `TypeError` when the value is not a whole number
   * @throws `RangeError` when it is outside Java's `long`
   */
  renewTimePeriodMs(renewTimePeriodMs: bigint | number): this;
  /**
   * The same as `renewTimePeriodMs(renewTimePeriodMs)`, the parameter given by name.
   *
   * @param options - the value in milliseconds
   * @returns This RenewDelegationTokenOptions object.
   * @throws `TypeError` when the value is not a whole number
   * @throws `RangeError` when it is outside Java's `long`
   */
  renewTimePeriodMs(options: {
    /** The value in milliseconds. */
    renewTimePeriodMs: bigint | number;
  }): this;
  renewTimePeriodMs(options?: unknown): bigint | this {
    if (!RENEW_TIME_PERIOD_MS.sets(options, arguments.length)) return this.#renewTimePeriodMs;
    this.#renewTimePeriodMs = checkLong(RENEW_TIME_PERIOD_MS.value(options), 'renewTimePeriodMs');
    this.internalRecord('renewTimePeriodMs', this.#renewTimePeriodMs);
    return this;
  }
}
