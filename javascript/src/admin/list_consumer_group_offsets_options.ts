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

// Java's org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions, an option class (root
// CLAUDE.md, JavaScript Binding Conventions, Class family, Q19). Java's getter /
// setter pairs are one method each (Q17).

import { GetterSetter, applyOptionKeys, checkBoolean } from '../internal/options';
import { AbstractOptions } from './abstract_options';

const REQUIRE_STABLE = new GetterSetter('requireStable', (value) => typeof value === 'boolean');

/**
 * Options for `Admin.listConsumerGroupOffsets({ groupSpecs })` and `Admin.listConsumerGroupOffsets(groupId)`.
 */
export class ListConsumerGroupOffsetsOptions extends AbstractOptions {
  #requireStable = false;

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
     * Whether the broker returns stable offsets only.
     *
     * @defaultValue `false`
     */
    requireStable?: boolean;
  }) {
    super();
    applyOptionKeys('ListConsumerGroupOffsetsOptions', options, arguments.length, [
      ['timeoutMs', (v) => this.timeoutMs(v as number | null)],
      ['requireStable', (v) => this.requireStable(v as boolean)],
    ]);
  }

  /**
   * Whether the broker returns stable offsets only.
   *
   * @returns whether the broker returns stable offsets only
   */
  requireStable(): boolean;
  /**
   * Sets whether the broker returns stable offsets only.
   *
   * @param requireStable - whether the broker returns stable offsets only, on its own
   * @returns This ListConsumerGroupOffsetsOptions object.
   */
  requireStable(requireStable: boolean): this;
  /**
   * The same as `requireStable(requireStable)`.
   *
   * @param options - whether the broker returns stable offsets only
   * @returns This ListConsumerGroupOffsetsOptions object.
   */
  requireStable(options: {
    /** Whether the broker returns stable offsets only. */
    requireStable: boolean;
  }): this;
  /**
   * Reads or sets the value.
   *
   * @param options - the value to set, or nothing to read it
   * @returns the value, or this object
   * @throws `TypeError` when the value is not a boolean
   */
  requireStable(options?: unknown): boolean | this {
    if (!REQUIRE_STABLE.sets(options, arguments.length)) return this.#requireStable;
    this.#requireStable = checkBoolean(REQUIRE_STABLE.value(options), 'requireStable');
    this.internalRecord('requireStable', this.#requireStable);
    return this;
  }
}
