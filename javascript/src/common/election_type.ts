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

// Java's org.apache.kafka.common.ElectionType, an enum (javascript/CLAUDE.md,
// JavaScript Binding Conventions, Class family): frozen constants carrying
// Java's public field `value`, as SecurityProtocol carries `id`, with
// java.lang.Enum's `name()`, `ordinal()`, `values()` and `valueOf()`. Java's
// two static `valueOf`s, Enum's `valueOf(String)` and its own
// `valueOf(byte)`, are one method, the form picked by the parameter's name,
// or by the bare value's type.

import { JavaForms } from '../internal/args';
import { IllegalArgumentError } from '../illegal_argument_error';

const JAVA_CLASS = 'org.apache.kafka.common.ElectionType';

const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const VALUES_FORMS = READER('values');
const NAME_FORMS = READER('name');
const ORDINAL_FORMS = READER('ordinal');
const VALUE_OF_FORMS = new JavaForms('valueOf', [['name'], ['value']], {
  bareTypes: { name: (value) => value === null || typeof value === 'string', value: (value) => typeof value === 'number' },
});

/**
 * Options for `Admin.electLeaders(electionType, partitions, options)`.
 */
export class ElectionType {
  /** The byte of the election type on the wire, Java's `value`. */
  readonly value: number;

  readonly #ordinal: number;
  readonly #name: string;

  /**
   * Made by this class only (the enum's constants).
   *
   * @param ordinal - the constant's position in Java's declaration
   * @param name - the constant's name
   * @param value - Java's byte `value`
   */
  private constructor(ordinal: number, name: string, value: number) {
    this.value = value;
    this.#ordinal = ordinal;
    this.#name = name;
    Object.freeze(this);
  }

  /** The preferred replica becomes the leader. */
  static readonly PREFERRED: ElectionType = new ElectionType(0, 'PREFERRED', 0);

  /** A replica outside the in-sync replicas may become the leader. */
  static readonly UNCLEAN: ElectionType = new ElectionType(1, 'UNCLEAN', 1);

  /**
   * The constants in declaration order, Java's `values()`.
   *
   * @returns a new array of the constants
   * @throws `TypeError` when it is given an argument: it only reads
   */
  static values(): ElectionType[] {
    VALUES_FORMS.match(undefined, arguments.length);
    return [ElectionType.PREFERRED, ElectionType.UNCLEAN];
  }

  /**
   * The constant named exactly `name`, Java's `valueOf(String)`.
   *
   * @param name - the name of the constant to return
   * @returns the constant with the specified name
   */
  static valueOf(name: string): ElectionType;
  /**
   * The same as `valueOf(name)`, the name given by name.
   *
   * @param options - the name of the constant to return
   * @returns the constant with the specified name
   */
  static valueOf(options: {
    /** The name of the constant to return. */
    name: string;
    value?: never;
  }): ElectionType;
  /**
   * The election type whose `value` is `value`, Java's `valueOf(byte)`.
   *
   * @param value - the byte
   * @returns the election type
   */
  static valueOf(value: number): ElectionType;
  /**
   * The same as `valueOf(value)`, the value given by name.
   *
   * @param options - the byte
   * @returns the election type
   */
  static valueOf(options: {
    /** The byte. */
    value: number;
    name?: never;
  }): ElectionType;
  /**
   * Looks the constant up by its name or its `value`.
   *
   * @param options - the name or the byte, on its own or by name
   * @returns the constant
   * @throws `IllegalArgumentError` when no constant has that name, or "Value
   *   <value> must be one of [PREFERRED, UNCLEAN]" for a byte no constant has
   * @throws `TypeError` when the name is null or the value of the wrong type
   * @throws `RangeError` when the byte is outside Java's `byte`
   */
  static valueOf(options: unknown): ElectionType {
    const form = VALUE_OF_FORMS.match(options, arguments.length);
    if (form === 1) {
      const value = VALUE_OF_FORMS.value(options, 'value');
      if (typeof value !== 'number' || !Number.isInteger(value)) throw new TypeError('value must be an integer');
      if (value < -128 || value > 127) throw new RangeError('value must be from -128 to 127');
      const byValue = ElectionType.values().find((t) => t.value === value);
      if (byValue === undefined) throw new IllegalArgumentError(`Value ${value} must be one of [PREFERRED, UNCLEAN]`);
      return byValue;
    }
    const name = VALUE_OF_FORMS.value(options, 'name');
    if (name === null) throw new TypeError('Name is null');
    if (typeof name !== 'string') throw new TypeError(`name must be a string, not ${typeof name}`);
    const found = ElectionType.values().find((t) => t.#name === name);
    if (found === undefined) throw new IllegalArgumentError(`No enum constant ${JAVA_CLASS}.${name}`);
    return found;
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
