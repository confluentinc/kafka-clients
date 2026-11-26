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

// Java's org.apache.kafka.clients.admin.CreateDelegationTokenOptions, an
// option class (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family):
// a keyed constructor, one key per setter field, in Java's field order.
// Java's getter / setter pairs are one method each; the deprecated
// `maxlifeTimeMs` pair is not translated (root §3). Java stores the renewers
// list as given, a null included, which the admin client's call refuses as
// Java's does; this class holds a copy and hands out a new array. `owner()`
// is Java's `Optional<KafkaPrincipal>`, null when empty (Types).

import { kind } from '../internal/args';
import { checkLong } from '../internal/java_number';
import { GetterSetter, applyOptionKeys } from '../internal/options';
import { KafkaPrincipal } from '../common/security/auth/kafka_principal';
import { AbstractOptions } from './abstract_options';

const isIterableOrNull = (value: unknown): boolean =>
  value === null ||
  typeof value === 'string' ||
  (typeof value === 'object' && Symbol.iterator in value && !(value instanceof AbstractOptions));
const MAX_LIFETIME_MS = new GetterSetter('maxLifetimeMs', (value) => typeof value === 'bigint' || typeof value === 'number');
const RENEWERS = new GetterSetter('renewers', isIterableOrNull);
const OWNER = new GetterSetter('owner', (value) => value === null || value instanceof KafkaPrincipal);

/**
 * The renewers given, copied, or null; a null element kept, as Java's list
 * keeps it.
 *
 * @param value - the value given
 * @returns the renewers, or null
 */
function renewersOf(value: unknown): Array<KafkaPrincipal | null> | null {
  if (value === null) return null;
  if (typeof value === 'string' || typeof value !== 'object' || !(Symbol.iterator in value)) {
    throw new TypeError(`renewers must be an iterable of KafkaPrincipal, not ${kind(value)}`);
  }
  const out: Array<KafkaPrincipal | null> = [];
  for (const r of value as Iterable<unknown>) {
    if (r !== null && !(r instanceof KafkaPrincipal)) throw new TypeError(`renewers must contain only KafkaPrincipal, not ${kind(r)}`);
    out.push(r);
  }
  return out;
}

/**
 * Options for `Admin.createDelegationToken()`.
 */
export class CreateDelegationTokenOptions extends AbstractOptions {
  #maxLifetimeMs = -1n;
  #renewers: Array<KafkaPrincipal | null> | null = [];
  #owner: KafkaPrincipal | null = null;

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
     * The token's maximum lifetime in milliseconds, `-1` for the server's
     * default.
     *
     * @defaultValue `-1n`
     */
    maxLifetimeMs?: bigint | number;
    /**
     * The principals allowed to renew the token.
     *
     * @defaultValue the empty list
     */
    renewers?: Iterable<KafkaPrincipal>;
    /**
     * The token's owner, or null for the request's principal.
     *
     * @defaultValue `null`
     */
    owner?: KafkaPrincipal | null;
  }) {
    super();
    applyOptionKeys('CreateDelegationTokenOptions', options, arguments.length, [
      ['timeoutMs', (v) => this.timeoutMs(v as number | null)],
      ['maxLifetimeMs', (v) => this.maxLifetimeMs(v as bigint)],
      ['renewers', (v) => this.renewers(v as Iterable<KafkaPrincipal>)],
      ['owner', (v) => this.owner(v as KafkaPrincipal | null)],
    ]);
  }

  /**
   * The principals allowed to renew the token.
   *
   * @returns a new array of the renewers
   */
  renewers(): KafkaPrincipal[];
  /**
   * Sets the principals allowed to renew the token.
   *
   * @param renewers - the renewers, on its own
   * @returns This CreateDelegationTokenOptions object.
   * @throws `TypeError` when the value is not an iterable of KafkaPrincipal
   */
  renewers(renewers: Iterable<KafkaPrincipal>): this;
  /**
   * The same as `renewers(renewers)`, the parameter given by name.
   *
   * @param options - the renewers
   * @returns This CreateDelegationTokenOptions object.
   * @throws `TypeError` when the value is not an iterable of KafkaPrincipal
   */
  renewers(options: {
    /** The principals allowed to renew the token. */
    renewers: Iterable<KafkaPrincipal>;
  }): this;
  renewers(options?: unknown): KafkaPrincipal[] | this {
    if (!RENEWERS.sets(options, arguments.length)) {
      return (this.#renewers === null ? null : [...this.#renewers]) as KafkaPrincipal[];
    }
    this.#renewers = renewersOf(RENEWERS.value(options));
    this.internalRecord(
      'renewers',
      this.#renewers === null ? null : this.#renewers.map((r) => (r === null ? null : [r.getPrincipalType(), r.getName()])),
    );
    return this;
  }

  /**
   * The token's owner.
   *
   * @returns the owner, or null when it is not set (Java's empty `Optional`)
   */
  owner(): KafkaPrincipal | null;
  /**
   * Sets the token's owner.
   *
   * @param owner - the owner, or null for the request's principal, on its own
   * @returns This CreateDelegationTokenOptions object.
   * @throws `TypeError` when the value is neither a KafkaPrincipal nor null
   */
  owner(owner: KafkaPrincipal | null): this;
  /**
   * The same as `owner(owner)`, the parameter given by name.
   *
   * @param options - the owner
   * @returns This CreateDelegationTokenOptions object.
   * @throws `TypeError` when the value is neither a KafkaPrincipal nor null
   */
  owner(options: {
    /** The token's owner, or null for the request's principal. */
    owner: KafkaPrincipal | null;
  }): this;
  owner(options?: unknown): KafkaPrincipal | null | this {
    if (!OWNER.sets(options, arguments.length)) return this.#owner;
    const owner = OWNER.value(options);
    if (owner !== null && !(owner instanceof KafkaPrincipal))
      throw new TypeError(`owner must be a KafkaPrincipal or null, not ${kind(owner)}`);
    this.#owner = owner;
    this.internalRecord('owner', owner === null ? null : [owner.getPrincipalType(), owner.getName()]);
    return this;
  }

  /**
   * The token's maximum lifetime in milliseconds.
   *
   * @returns the lifetime, `-1` for the server's default
   */
  maxLifetimeMs(): bigint;
  /**
   * Sets the token's maximum lifetime in milliseconds.
   *
   * @param maxLifetimeMs - the lifetime, a `long`, on its own
   * @returns This CreateDelegationTokenOptions object.
   * @throws `TypeError` when the value is not a whole number
   * @throws `RangeError` when it is outside Java's `long`
   */
  maxLifetimeMs(maxLifetimeMs: bigint | number): this;
  /**
   * The same as `maxLifetimeMs(maxLifetimeMs)`, the parameter given by name.
   *
   * @param options - the lifetime
   * @returns This CreateDelegationTokenOptions object.
   * @throws `TypeError` when the value is not a whole number
   * @throws `RangeError` when it is outside Java's `long`
   */
  maxLifetimeMs(options: {
    /** The token's maximum lifetime in milliseconds. */
    maxLifetimeMs: bigint | number;
  }): this;
  maxLifetimeMs(options?: unknown): bigint | this {
    if (!MAX_LIFETIME_MS.sets(options, arguments.length)) return this.#maxLifetimeMs;
    this.#maxLifetimeMs = checkLong(MAX_LIFETIME_MS.value(options), 'maxLifetimeMs');
    this.internalRecord('maxLifetimeMs', this.#maxLifetimeMs);
    return this;
  }
}
