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

// Java's org.apache.kafka.common.security.auth.KafkaPrincipal, a value class
// (root CLAUDE.md, JavaScript Binding Conventions, Class family): Java's two
// public constructors as one keyed form each, its getters (`getName`,
// `getPrincipalType`, which keep Java's names), the `tokenAuthenticated`
// getter and setter pair as one method (Q17), and Java's equals, hashCode and
// toString. Java's `implements Principal` is not translated (the JDK
// interface table: Principal is dropped, the class keeps its own methods).

import { JavaForms, kind } from '../../../internal/args';
import { stringHashCode, sameClass } from '../../../internal/java_object';
import { GetterSetter, checkBoolean } from '../../../internal/options';

const FORMS = new JavaForms('KafkaPrincipal', [
  ['principalType', 'name'],
  ['principalType', 'name', 'tokenAuthenticated'],
]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const GET_NAME_FORMS = READER('getName');
const GET_PRINCIPAL_TYPE_FORMS = READER('getPrincipalType');
const HASH_CODE_FORMS = READER('hashCode');
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const TOKEN_AUTHENTICATED = new GetterSetter('tokenAuthenticated', (value) => typeof value === 'boolean');

/**
 * Principals in Kafka are defined by a type and a name. The principal type
 * will always be `"User"` for the simple authorizer that is enabled by
 * default, but custom authorizers can leverage different principal types
 * (such as to enable group or role-based ACLs).
 */
export class KafkaPrincipal {
  /** The principal type of a user. */
  static readonly USER_TYPE: string = 'User';

  readonly #principalType: string;
  readonly #name: string;
  #tokenAuthenticated: boolean;

  /**
   * A principal of the given type and name, not token-authenticated.
   *
   * @param options - the principal type and the name
   * @throws `TypeError` when the type or the name is null or not a string
   */
  constructor(options: {
    /** The principal type. */
    principalType: string;
    /** The principal's name. */
    name: string;
    tokenAuthenticated?: never;
  });
  /**
   * A principal of the given type and name.
   *
   * @param options - the principal type, the name, and whether the principal
   *   was authenticated with a delegation token
   * @throws `TypeError` when the type or the name is null or not a string
   */
  constructor(options: {
    /** The principal type. */
    principalType: string;
    /** The principal's name. */
    name: string;
    /** Whether the principal was authenticated with a delegation token. */
    tokenAuthenticated: boolean;
  });
  constructor(options: unknown) {
    const form = FORMS.match(options, arguments.length);
    const principalType = FORMS.value(options, 'principalType');
    const name = FORMS.value(options, 'name');
    if (principalType === null) throw new TypeError('Principal type cannot be null');
    if (typeof principalType !== 'string') throw new TypeError(`principalType must be a string, not ${kind(principalType)}`);
    if (name === null) throw new TypeError('Principal name cannot be null');
    if (typeof name !== 'string') throw new TypeError(`name must be a string, not ${kind(name)}`);
    this.#principalType = principalType;
    this.#name = name;
    this.#tokenAuthenticated = form === 0 ? false : checkBoolean(FORMS.value(options, 'tokenAuthenticated'), 'tokenAuthenticated');
  }

  /** The anonymous user, `User:ANONYMOUS`. */
  static readonly ANONYMOUS: KafkaPrincipal = new KafkaPrincipal({ principalType: KafkaPrincipal.USER_TYPE, name: 'ANONYMOUS' });

  /**
   * The principal's name.
   *
   * @returns the name
   * @throws `TypeError` when it is given an argument: it only reads
   */
  getName(): string {
    if (arguments.length !== 0) GET_NAME_FORMS.match(undefined, arguments.length);
    return this.#name;
  }

  /**
   * The principal's type.
   *
   * @returns the type
   * @throws `TypeError` when it is given an argument: it only reads
   */
  getPrincipalType(): string {
    if (arguments.length !== 0) GET_PRINCIPAL_TYPE_FORMS.match(undefined, arguments.length);
    return this.#principalType;
  }

  /**
   * Whether the principal was authenticated with a delegation token.
   *
   * @returns whether it was
   */
  tokenAuthenticated(): boolean;
  /**
   * Sets whether the principal was authenticated with a delegation token.
   *
   * @param tokenAuthenticated - whether it was, on its own
   * @throws `TypeError` when the value is not a boolean
   */
  tokenAuthenticated(tokenAuthenticated: boolean): void;
  /**
   * The same as `tokenAuthenticated(tokenAuthenticated)`, the parameter given
   * by name.
   *
   * @param options - whether it was
   * @throws `TypeError` when the value is not a boolean
   */
  tokenAuthenticated(options: {
    /** Whether the principal was authenticated with a delegation token. */
    tokenAuthenticated: boolean;
  }): void;
  /**
   * Reads or sets whether the principal was token-authenticated.
   *
   * @param options - the value to set, or nothing to read it
   * @returns the value when it reads
   * @throws `TypeError` when the value is not a boolean
   */
  tokenAuthenticated(options?: unknown): boolean | void {
    if (!TOKEN_AUTHENTICATED.sets(options, arguments.length)) return this.#tokenAuthenticated;
    this.#tokenAuthenticated = checkBoolean(TOKEN_AUTHENTICATED.value(options), 'tokenAuthenticated');
  }

  /**
   * Whether `o` is a KafkaPrincipal of exactly this class with the same type
   * and name (`tokenAuthenticated` is not compared, as in Java).
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as KafkaPrincipal;
    return this.#principalType === that.#principalType && this.#name === that.#name;
  }

  /**
   * Java's `hashCode()`: `31 * principalType.hashCode() + name.hashCode()`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return (Math.imul(31, stringHashCode(this.#principalType)) + stringHashCode(this.#name)) | 0;
  }

  /**
   * Java's `toString()`: `<principalType>:<name>`.
   *
   * @returns the text
   */
  toString(): string {
    return `${this.#principalType}:${this.#name}`;
  }

  /**
   * The principal as `JSON.stringify` writes it: `{ principalType, name,
   * tokenAuthenticated }`.
   *
   * @returns the principal's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { principalType: string; name: string; tokenAuthenticated: boolean } {
    return { principalType: this.#principalType, name: this.#name, tokenAuthenticated: this.#tokenAuthenticated };
  }
}
