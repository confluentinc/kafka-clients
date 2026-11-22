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

// Java's org.apache.kafka.clients.admin.ScramMechanism, an enum (root
// CLAUDE.md, JavaScript Binding Conventions, Class family): frozen constants
// carrying Java's `type()` and `mechanismName()`, with java.lang.Enum's
// `name()`, `ordinal()`, `values()` and `valueOf()`; `toString()` and
// `toJSON()` return the name.

import { JavaForms, kind } from '../internal/args';
import { IllegalArgumentError } from '../illegal_argument_error';

const JAVA_CLASS = 'org.apache.kafka.clients.admin.ScramMechanism';

const isString = (value: unknown): boolean => value === null || typeof value === 'string';
const FROM_TYPE_FORMS = new JavaForms('fromType', [['type']], { bareTypes: { type: (value) => typeof value === 'number' } });
const FROM_MECHANISM_NAME_FORMS = new JavaForms('fromMechanismName', [['mechanismName']], { bareTypes: { mechanismName: isString } });
const VALUE_OF_FORMS = new JavaForms('valueOf', [['name']], { bareTypes: { name: isString } });
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const VALUES_FORMS = READER('values');
const MECHANISM_NAME_FORMS = READER('mechanismName');
const TYPE_FORMS = READER('type');
const NAME_FORMS = READER('name');
const ORDINAL_FORMS = READER('ordinal');

/**
 * Representation of a SASL/SCRAM Mechanism.
 *
 * See KIP-554: Add Broker-side SCRAM Config API.
 */
export class ScramMechanism {
  readonly #type: number;
  readonly #name: string;
  readonly #ordinal: number;
  readonly #mechanismName: string;

  /**
   * Made by this class only (the enum's constants).
   *
   * @param ordinal - the constant's position in Java's declaration
   * @param name - the constant's name
   * @param type - Java's type indicator
   */
  private constructor(ordinal: number, name: string, type: number) {
    this.#type = type;
    this.#name = name;
    this.#ordinal = ordinal;
    this.#mechanismName = name.replace(/_/g, '-');
    Object.freeze(this);
  }

  /** A mechanism this client does not know. */
  static readonly UNKNOWN: ScramMechanism = new ScramMechanism(0, 'UNKNOWN', 0);

  /** SCRAM-SHA-256. */
  static readonly SCRAM_SHA_256: ScramMechanism = new ScramMechanism(1, 'SCRAM_SHA_256', 1);

  /** SCRAM-SHA-512. */
  static readonly SCRAM_SHA_512: ScramMechanism = new ScramMechanism(2, 'SCRAM_SHA_512', 2);

  /**
   * The constants in declaration order, Java's `values()`.
   *
   * @returns a new array of the constants
   * @throws `TypeError` when it is given an argument: it only reads
   */
  static values(): ScramMechanism[] {
    VALUES_FORMS.match(undefined, arguments.length);
    return [ScramMechanism.UNKNOWN, ScramMechanism.SCRAM_SHA_256, ScramMechanism.SCRAM_SHA_512];
  }

  /**
   * The constant named exactly `name`, Java's `valueOf(String)`.
   *
   * @param name - the name, on its own or as `{ name }`
   * @returns the constant with the specified name
   * @throws `IllegalArgumentError` when no constant has that name
   * @throws `TypeError` when the name is null or not a string
   */
  static valueOf(name: string): ScramMechanism;
  /**
   * The same as `valueOf(name)`, the parameter given by name.
   *
   * @param options - the constant's name
   * @returns the constant with the specified name
   */
  static valueOf(options: {
    /** The constant's name */
    name: string;
  }): ScramMechanism;
  static valueOf(name: unknown): ScramMechanism {
    VALUE_OF_FORMS.match(name, arguments.length);
    const value = VALUE_OF_FORMS.value(name, 'name');
    if (value === null) throw new TypeError('Name is null');
    if (typeof value !== 'string') throw new TypeError(`name must be a string, not ${typeof value}`);
    const found = ScramMechanism.values().find((o) => o.#name === value);
    if (found === undefined) throw new IllegalArgumentError(`No enum constant ${JAVA_CLASS}.${value}`);
    return found;
  }

  /**
   * The instance corresponding to the given type indicator.
   *
   * @param type - the type indicator, a Java `byte`, on its own or as `{ type }`
   * @returns the instance corresponding to the given type indicator, otherwise `UNKNOWN`
   * @throws `TypeError` when the type is not an integer
   * @throws `RangeError` when it is outside Java's `byte`
   */
  static fromType(type: number): ScramMechanism;
  /**
   * The same as `fromType(type)`, the parameter given by name.
   *
   * @param options - the type indicator, a Java `byte`
   * @returns the instance corresponding to the given type indicator, otherwise `UNKNOWN`
   */
  static fromType(options: {
    /** The type indicator, a Java `byte`. */
    type: number;
  }): ScramMechanism;
  static fromType(type: unknown): ScramMechanism {
    FROM_TYPE_FORMS.match(type, arguments.length);
    const value = FROM_TYPE_FORMS.value(type, 'type');
    if (typeof value !== 'number' || !Number.isInteger(value)) throw new TypeError('type must be an integer');
    if (value < -128 || value > 127) throw new RangeError('type must be from -128 to 127');
    return ScramMechanism.values().find((o) => o.#type === value) ?? ScramMechanism.UNKNOWN;
  }

  /**
   * The SASL SCRAM mechanism of the given name.
   *
   * @param mechanismName - the SASL SCRAM mechanism name, on its own or as
   *   `{ mechanismName }`
   * @returns the corresponding SASL SCRAM mechanism enum, otherwise `UNKNOWN`
   * @throws `TypeError` when the name is not a string or null
   */
  static fromMechanismName(mechanismName: string | null): ScramMechanism;
  /**
   * The same as `fromMechanismName(mechanismName)`, the parameter given by
   * name.
   *
   * @param options - the SASL SCRAM mechanism name
   * @returns the corresponding SASL SCRAM mechanism enum, otherwise `UNKNOWN`
   */
  static fromMechanismName(options: {
    /** The SASL SCRAM mechanism name. */
    mechanismName: string | null;
  }): ScramMechanism;
  static fromMechanismName(mechanismName: unknown): ScramMechanism {
    FROM_MECHANISM_NAME_FORMS.match(mechanismName, arguments.length);
    const value = FROM_MECHANISM_NAME_FORMS.value(mechanismName, 'mechanismName');
    if (value !== null && typeof value !== 'string') throw new TypeError(`mechanismName must be a string, not ${kind(value)}`);
    return ScramMechanism.values().find((o) => o.#mechanismName === value) ?? ScramMechanism.UNKNOWN;
  }

  /**
   * The corresponding SASL SCRAM mechanism name.
   *
   * @returns the mechanism name, as `SCRAM-SHA-256`
   * @throws `TypeError` when it is given an argument: it only reads
   */
  mechanismName(): string {
    MECHANISM_NAME_FORMS.match(undefined, arguments.length);
    return this.#mechanismName;
  }

  /**
   * The type indicator for this SASL SCRAM mechanism.
   *
   * @returns the type indicator
   * @throws `TypeError` when it is given an argument: it only reads
   */
  type(): number {
    TYPE_FORMS.match(undefined, arguments.length);
    return this.#type;
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
   * The position of this constant in its enum declaration, Java's `Enum.ordinal()`.
   *
   * @returns the ordinal
   * @throws `TypeError` when it is given an argument: it only reads
   */
  ordinal(): number {
    ORDINAL_FORMS.match(undefined, arguments.length);
    return this.#ordinal;
  }

  /**
   * The constant's name, as Java's `Enum.toString()`.
   *
   * @returns the name
   */
  toString(): string {
    return this.#name;
  }

  /**
   * The constant's name, which `JSON.stringify` writes for it.
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
