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

// Java's org.apache.kafka.clients.admin.DescribeDelegationTokenOptions, an
// option class (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family,
// Q19). Java's getter / setter pair is one method (Q17). Java stores the
// owners as given, null (every token the caller may describe) included; this
// class holds a copy and hands out a new array, or null.

import { kind } from '../internal/args';
import { GetterSetter, applyOptionKeys } from '../internal/options';
import { KafkaPrincipal } from '../common/security/auth/kafka_principal';
import { AbstractOptions } from './abstract_options';

const OWNERS = new GetterSetter(
  'owners',
  (value) =>
    value === null ||
    typeof value === 'string' ||
    (typeof value === 'object' && Symbol.iterator in value && !(value instanceof AbstractOptions)),
);

/**
 * Options for `Admin.describeDelegationToken()`.
 */
export class DescribeDelegationTokenOptions extends AbstractOptions {
  #owners: Array<KafkaPrincipal | null> | null = null;

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
     * The owners that we want to describe delegation tokens for, or null for
     * all the tokens the user owns or may describe.
     *
     * @defaultValue `null`
     */
    owners?: Iterable<KafkaPrincipal> | null;
  }) {
    super();
    applyOptionKeys('DescribeDelegationTokenOptions', options, arguments.length, [
      ['timeoutMs', (v) => this.timeoutMs(v as number | null)],
      ['owners', (v) => this.owners(v as Iterable<KafkaPrincipal> | null)],
    ]);
  }

  /**
   * The owners that we want to describe delegation tokens for.
   *
   * @returns a new array of the owners, or null
   */
  owners(): KafkaPrincipal[] | null;
  /**
   * If owners is null, all the user owned tokens and tokens where user have
   * Describe permission will be returned.
   *
   * @param owners - The owners that we want to describe delegation tokens
   *   for, on its own
   * @returns this instance
   */
  owners(owners: Iterable<KafkaPrincipal> | null): this;
  /**
   * The same as `owners(owners)`, the parameter given by name.
   *
   * @param options - the owners
   * @returns this instance
   */
  owners(options: {
    /** The owners that we want to describe delegation tokens for. */
    owners: Iterable<KafkaPrincipal> | null;
  }): this;
  /**
   * Reads or sets the owners.
   *
   * @param options - the value to set, or nothing to read it
   * @returns the owners, or this object
   * @throws `TypeError` when the value is not an iterable of KafkaPrincipal
   */
  owners(options?: unknown): KafkaPrincipal[] | null | this {
    if (!OWNERS.sets(options, arguments.length)) return (this.#owners === null ? null : [...this.#owners]) as KafkaPrincipal[] | null;
    const value = OWNERS.value(options);
    if (value === null) {
      this.#owners = null;
    } else {
      if (typeof value === 'string' || typeof value !== 'object' || !(Symbol.iterator in value)) {
        throw new TypeError(`owners must be an iterable of KafkaPrincipal, not ${kind(value)}`);
      }
      const out: Array<KafkaPrincipal | null> = [];
      for (const o of value as Iterable<unknown>) {
        if (o !== null && !(o instanceof KafkaPrincipal)) throw new TypeError(`owners must contain only KafkaPrincipal, not ${kind(o)}`);
        out.push(o);
      }
      this.#owners = out;
    }
    this.internalRecord(
      'owners',
      this.#owners === null ? null : this.#owners.map((o) => (o === null ? null : [o.getPrincipalType(), o.getName()])),
    );
    return this;
  }
}
