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

// Java's org.apache.kafka.common.security.auth.SecurityProtocol, as
// python/confluent_kafka/common/security/auth/security_protocol.py translates
// it: an enum whose constants are frozen instances carrying Java's public
// field `id`, with java.lang.Enum's `name()` and `ordinal()`.

import { JavaForms } from '../../../internal/args';
import { IllegalArgumentError } from '../../../illegal_argument_error';

const JAVA_CLASS = 'org.apache.kafka.common.security.auth.SecurityProtocol';

// Java's forms (src/internal/args.ts): the bare value or `{id}` / `{name}`,
// and no argument for the methods of no parameter.
// Java's id is a short, never null: `undefined` is a wrong type, not Java's null.
const isName = (value: unknown): boolean => value === null || typeof value === 'string';
const FOR_ID_FORMS = new JavaForms('forId', [['id']], {
  undefinedMessages: { id: 'id must be a number, not undefined' },
  bareTypes: { id: (value) => typeof value === 'number' },
});
const FOR_NAME_FORMS = new JavaForms('forName', [['name']], { bareTypes: { name: isName } });
const VALUE_OF_FORMS = new JavaForms('valueOf', [['name']], { bareTypes: { name: isName } });
const VALUES_FORMS = new JavaForms('values', [[]], { reader: true });
const NAMES_FORMS = new JavaForms('names', [[]], { reader: true });
const NAME_FORMS = new JavaForms('name', [[]], { reader: true });
const ORDINAL_FORMS = new JavaForms('ordinal', [[]], { reader: true });

/**
 * The security protocol of a connection: `PLAINTEXT`, `SSL`,
 * `SASL_PLAINTEXT` or `SASL_SSL` (the value of `security.protocol`).
 *
 * Java: `org.apache.kafka.common.security.auth.SecurityProtocol`.
 */
export class SecurityProtocol {
  /**
   * The permanent and immutable id of a security protocol, which can't change
   * and will be used in ZK, Java's `id`.
   */
  readonly id: number;

  #name: string;

  #ordinal: number;

  /**
   * Made by this class only (the enum's constants).
   */
  private constructor(ordinal: number, id: number, name: string) {
    this.id = id;
    this.#name = name;
    this.#ordinal = ordinal;
    Object.freeze(this);
  }

  /**
   * Un-authenticated, non-encrypted channel.
   */
  static readonly PLAINTEXT: SecurityProtocol = new SecurityProtocol(0, 0, 'PLAINTEXT');

  /**
   * SSL channel.
   */
  static readonly SSL: SecurityProtocol = new SecurityProtocol(1, 1, 'SSL');

  /**
   * SASL authenticated, non-encrypted channel.
   */
  static readonly SASL_PLAINTEXT: SecurityProtocol = new SecurityProtocol(2, 2, 'SASL_PLAINTEXT');

  /**
   * SASL authenticated, SSL channel.
   */
  static readonly SASL_SSL: SecurityProtocol = new SecurityProtocol(3, 3, 'SASL_SSL');

  /**
   * The constants in declaration order, Java's `values()`.
   *
   * @returns a new array of the constants
   * @throws `TypeError` when it is given an argument: it only reads
   */
  static values(): SecurityProtocol[] {
    // An argument Java's values() does not take is refused, never dropped.
    VALUES_FORMS.match(undefined, arguments.length);
    return [SecurityProtocol.PLAINTEXT, SecurityProtocol.SSL, SecurityProtocol.SASL_PLAINTEXT, SecurityProtocol.SASL_SSL];
  }

  /**
   * The constant named exactly `name`, Java's `valueOf(String)`. The name is
   * given on its own, `valueOf('SSL')`, or named, `valueOf({ name: 'SSL' })`.
   *
   * @param name - the name of the constant to return
   * @returns the constant with the specified name
   * @throws `IllegalArgumentError` when no constant has that name, or the
   *   given parameters are not `{name}`
   * @throws `TypeError` when the name is null or not a string
   */
  static valueOf(name: string): SecurityProtocol;
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
  }): SecurityProtocol;
  static valueOf(options: unknown): SecurityProtocol {
    VALUE_OF_FORMS.match(options, arguments.length);
    const name = VALUE_OF_FORMS.value(options, 'name');
    // java.lang.Enum.valueOf: requireNonNull's "Name is null".
    if (name === null) throw new TypeError('Name is null');
    if (typeof name !== 'string') throw new TypeError(`name must be a string, not ${typeof name}`);
    const found = SecurityProtocol.values().find((p) => p.#name === name);
    if (found === undefined) throw new IllegalArgumentError(`No enum constant ${JAVA_CLASS}.${name}`);
    return found;
  }

  /**
   * The names of the protocols, in declaration order.
   *
   * @returns the names
   * @throws `TypeError` when it is given an argument: it only reads
   */
  static names(): string[] {
    // An argument Java's names() does not take is refused, never dropped.
    NAMES_FORMS.match(undefined, arguments.length);
    return SecurityProtocol.values().map((p) => p.#name);
  }

  /**
   * The protocol whose `id` is `id`, or `null`, as Java's map lookup returns
   * `null`. The id is given on its own, `forId(1)`, or named, `forId({ id: 1 })`.
   */
  static forId(id: number): SecurityProtocol | null;
  /**
   * The protocol whose `id` is `id`, the same as `forId(id)`.
   *
   * @param options - the protocol's id
   * @returns the protocol, or `null`
   */
  static forId(options: {
    /** The protocol's id. */
    id: number;
  }): SecurityProtocol | null;
  static forId(id: unknown): SecurityProtocol | null {
    FOR_ID_FORMS.match(id, arguments.length);
    const value = FOR_ID_FORMS.value(id, 'id');
    // Java's id is a short: no null, no other type.
    if (typeof value !== 'number') throw new TypeError(`id must be a number, not ${value === null ? 'null' : typeof value}`);
    return SecurityProtocol.values().find((p) => p.id === value) ?? null;
  }

  /**
   * The protocol named `name` in any case, as Java's
   * `valueOf(name.toUpperCase(Locale.ROOT))`. The name is given on its own,
   * `forName('ssl')`, or named, `forName({ name: 'ssl' })`.
   *
   * @throws `IllegalArgumentError` when no protocol has that name, or the
   *   given parameters are not `{name}`
   */
  static forName(name: string): SecurityProtocol;
  /**
   * The protocol named `name` in any case, the same as `forName(name)`.
   *
   * @param options - the protocol's name
   * @returns the protocol
   * @throws `IllegalArgumentError` when no protocol has that name
   */
  static forName(options: {
    /** The protocol's name, in any case. */
    name: string;
  }): SecurityProtocol;
  static forName(options: unknown): SecurityProtocol {
    FOR_NAME_FORMS.match(options, arguments.length);
    const name = FOR_NAME_FORMS.value(options, 'name');
    // Java's forName dereferences the name (name.toUpperCase): a NullPointerException.
    if (name === null) throw new TypeError('name must not be null');
    if (typeof name !== 'string') throw new TypeError(`name must be a string, not ${typeof name}`);
    return SecurityProtocol.valueOf(name.toUpperCase());
  }

  /**
   * The name of this constant, exactly as declared, Java's `Enum.name()`
   * (the same as Java's public field `name`).
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
   * Java's `Enum.toString()`: the name.
   *
   * @returns the name
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
