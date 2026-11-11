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

// Java's org.apache.kafka.common.config.SslClientAuth, as
// python/confluent_kafka/common/config/ssl_client_auth.py translates it: an
// enum whose constants are frozen instances, with java.lang.Enum's `name()`
// and `ordinal()`.

import { JavaForms } from '../../internal/args';
import { IllegalArgumentError } from '../../illegal_argument_error';

const JAVA_CLASS = 'org.apache.kafka.common.config.SslClientAuth';

// Java's forms (src/internal/args.ts): the bare value or `{key}` / `{name}`,
// and no argument for the methods of no parameter.
const isName = (value: unknown): boolean => value === null || typeof value === 'string';
const FOR_CONFIG_FORMS = new JavaForms('forConfig', [['key']], { bareTypes: { key: isName } });
const VALUE_OF_FORMS = new JavaForms('valueOf', [['name']], { bareTypes: { name: isName } });
const VALUES_FORMS = new JavaForms('values', [[]], { reader: true });
const NAME_FORMS = new JavaForms('name', [[]], { reader: true });
const ORDINAL_FORMS = new JavaForms('ordinal', [[]], { reader: true });

/**
 * Describes whether the server should require or request client
 * authentication.
 *
 * The constants are `REQUIRED`, `REQUESTED` and `NONE`; `VALUES` is Java's
 * `List.of(values())`, a frozen array in Java's order.
 *
 * Java: `org.apache.kafka.common.config.SslClientAuth`.
 */
export class SslClientAuth {
  #name: string;

  #ordinal: number;

  /**
   * Made by this class only (the enum's constants).
   */
  private constructor(ordinal: number, name: string) {
    this.#name = name;
    this.#ordinal = ordinal;
    Object.freeze(this);
  }

  static readonly REQUIRED: SslClientAuth = new SslClientAuth(0, 'REQUIRED');

  static readonly REQUESTED: SslClientAuth = new SslClientAuth(1, 'REQUESTED');

  static readonly NONE: SslClientAuth = new SslClientAuth(2, 'NONE');

  /**
   * The constants in Java's order, Java's `VALUES`.
   */
  static readonly VALUES: ReadonlyArray<SslClientAuth> = Object.freeze([SslClientAuth.REQUIRED, SslClientAuth.REQUESTED, SslClientAuth.NONE]);

  /**
   * The constants in declaration order, Java's `values()`.
   *
   * @returns a new array of the constants
   * @throws `TypeError` when it is given an argument: it only reads
   */
  static values(): SslClientAuth[] {
    // An argument Java's values() does not take is refused, never dropped.
    VALUES_FORMS.match(undefined, arguments.length);
    return [SslClientAuth.REQUIRED, SslClientAuth.REQUESTED, SslClientAuth.NONE];
  }

  /**
   * The constant named exactly `name`, Java's `valueOf(String)`. The name is
   * given on its own, `valueOf('NONE')`, or named, `valueOf({ name: 'NONE' })`.
   *
   * @param name - the name of the constant to return
   * @returns the constant with the specified name
   * @throws `IllegalArgumentError` when no constant has that name, or the
   *   given parameters are not `{name}`
   * @throws `TypeError` when the name is null or not a string
   */
  static valueOf(name: string): SslClientAuth;
  /**
   * The constant named exactly `name`, the same as `valueOf(name)`.
   *
   * @param options - the constant's name
   * @returns the constant
   * @throws `IllegalArgumentError` when no constant has that name
   */
  static valueOf(options: {
    /** The name of the constant to return. */
    name: string;
  }): SslClientAuth;
  static valueOf(options: unknown): SslClientAuth {
    VALUE_OF_FORMS.match(options, arguments.length);
    const name = VALUE_OF_FORMS.value(options, 'name');
    // java.lang.Enum.valueOf: requireNonNull's "Name is null".
    if (name === null) throw new TypeError('Name is null');
    if (typeof name !== 'string') throw new TypeError(`name must be a string, not ${typeof name}`);
    const found = SslClientAuth.VALUES.find((auth) => auth.#name === name);
    if (found === undefined) throw new IllegalArgumentError(`No enum constant ${JAVA_CLASS}.${name}`);
    return found;
  }

  /**
   * The constant whose name is `key` in any case; `NONE` for `null`, and
   * `null` when no constant has that name, as Java's `forConfig` returns
   * `null`. The key is given on its own, `forConfig('required')`, or named,
   * `forConfig({ key: 'required' })`.
   *
   * @throws `IllegalArgumentError` when the given parameters are not `{key}`
   */
  static forConfig(key: string | null): SslClientAuth | null;
  /**
   * The constant whose name is `key` in any case, the same as
   * `forConfig(key)`.
   *
   * @param options - the constant's name
   * @returns the constant, or `null` when no constant has that name
   */
  static forConfig(options: {
    /** The constant's name in any case, or `null` for `NONE`. */
    key: string | null;
  }): SslClientAuth | null;
  static forConfig(options: unknown): SslClientAuth | null {
    FOR_CONFIG_FORMS.match(options, arguments.length);
    const key = FOR_CONFIG_FORMS.value(options, 'key');
    if (key === null) return SslClientAuth.NONE;
    if (typeof key !== 'string') throw new TypeError(`key must be a string, not ${typeof key}`);
    const upperCaseKey = key.toUpperCase();
    return SslClientAuth.VALUES.find((auth) => auth.#name === upperCaseKey) ?? null;
  }

  /**
   * The name of this constant, exactly as declared, Java's `Enum.name()`.
   *
   * @returns the name
   * @throws `TypeError` when it is given an argument: it only reads
   */
  name(): string {
    NAME_FORMS.match(undefined, arguments.length);
    return this.#name;
  }

  /**
   * The position of this constant in its enum declaration, the first one
   * being 0, Java's `Enum.ordinal()`.
   *
   * @returns the ordinal
   * @throws `TypeError` when it is given an argument: it only reads
   */
  ordinal(): number {
    ORDINAL_FORMS.match(undefined, arguments.length);
    return this.#ordinal;
  }

  /**
   * The name of this constant, as `name()` returns it.
   *
   * @returns the name
   *
   * @remarks *(deviation)* Java's `SslClientAuth.toString()` returns the name
   * in lower case. Why: an enum's `toString()` and `toJSON()` return its name
   * (root CLAUDE.md, JavaScript Binding Conventions, Class family). Use
   * `name().toLowerCase()` for Java's text.
   */
  toString(): string {
    return this.#name;
  }

  /**
   * The name, which `JSON.stringify` writes for the constant.
   *
   * @param key - the key `JSON.stringify` passes, which is ignored
   * @returns the name
   *
   * @remarks *(js-only)* JavaScript's JSON protocol.
   */
  toJSON(key?: string): string {
    void key;
    return this.#name;
  }
}
