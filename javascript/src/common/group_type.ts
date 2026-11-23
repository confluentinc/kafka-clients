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

// Java's org.apache.kafka.common.GroupType, an enum (root CLAUDE.md, JavaScript Binding
// Conventions, Class family): frozen constants with java.lang.Enum's
// `name()`, `ordinal()`, `values()` and `valueOf()`. Java overrides
// `toString()` to return each constant's display name (`Consumer`),
// while `toJSON()` returns the constant's name, as for every enum here.

import { JavaForms } from '../internal/args';
import { IllegalArgumentError } from '../illegal_argument_error';

const JAVA_CLASS = 'org.apache.kafka.common.GroupType';

const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const VALUES_FORMS = READER('values');
const NAME_FORMS = READER('name');
const ORDINAL_FORMS = READER('ordinal');
const isName = (value: unknown): boolean => value === null || typeof value === 'string';
const VALUE_OF_FORMS = new JavaForms('valueOf', [['name']], { bareTypes: { name: isName } });
const PARSE_FORMS = new JavaForms('parse', [['name']], { bareTypes: { name: isName } });

/**
 * The type of a group.
 */
export class GroupType {
  readonly #ordinal: number;
  readonly #name: string;
  readonly #displayName: string;

  /**
   * Made by this class only (the enum's constants).
   *
   * @param ordinal - the constant's position in Java's declaration
   * @param name - the constant's name
   * @param displayName - Java's `name` field, which `toString()` returns
   */
  private constructor(ordinal: number, name: string, displayName: string) {
    this.#ordinal = ordinal;
    this.#name = name;
    this.#displayName = displayName;
    Object.freeze(this);
  }

  /** The `Unknown` value. */
  static readonly UNKNOWN: GroupType = new GroupType(0, 'UNKNOWN', 'Unknown');

  /** The `Consumer` value. */
  static readonly CONSUMER: GroupType = new GroupType(1, 'CONSUMER', 'Consumer');

  /** The `Classic` value. */
  static readonly CLASSIC: GroupType = new GroupType(2, 'CLASSIC', 'Classic');

  /** The `Share` value. */
  static readonly SHARE: GroupType = new GroupType(3, 'SHARE', 'Share');

  /** The `Streams` value. */
  static readonly STREAMS: GroupType = new GroupType(4, 'STREAMS', 'Streams');

  /**
   * The constants in declaration order, Java's `values()`.
   *
   * @returns a new array of the constants
   * @throws `TypeError` when it is given an argument: it only reads
   */
  static values(): GroupType[] {
    VALUES_FORMS.match(undefined, arguments.length);
    return [GroupType.UNKNOWN, GroupType.CONSUMER, GroupType.CLASSIC, GroupType.SHARE, GroupType.STREAMS];
  }

  /**
   * The constant named exactly `name`, Java's `valueOf(String)`.
   *
   * @param name - the name of the constant to return
   * @returns the constant with the specified name
   * @throws `IllegalArgumentError` when no constant has that name
   * @throws `TypeError` when the name is null or not a string
   */
  static valueOf(name: string): GroupType;
  /**
   * The same as `valueOf(name)`, the name given by name.
   *
   * @param options - the name of the constant to return
   * @returns the constant with the specified name
   * @throws `IllegalArgumentError` when no constant has that name
   * @throws `TypeError` when the name is null or not a string
   */
  static valueOf(options: {
    /** The name of the constant to return. */
    name: string;
  }): GroupType;
  /**
   * Looks the constant up by its name.
   *
   * @param name - the name, on its own or as `{ name }`
   * @returns the constant
   * @throws `IllegalArgumentError` when no constant has that name
   * @throws `TypeError` when the name is null or not a string
   */
  static valueOf(name: unknown): GroupType {
    VALUE_OF_FORMS.match(name, arguments.length);
    const value = VALUE_OF_FORMS.value(name, 'name');
    if (value === null) throw new TypeError('Name is null');
    if (typeof value !== 'string') throw new TypeError(`name must be a string, not ${typeof value}`);
    const found = GroupType.values().find((c) => c.#name === value);
    if (found === undefined) throw new IllegalArgumentError(`No enum constant ${JAVA_CLASS}.${value}`);
    return found;
  }

  /**
   * Parse a string into a consumer group type, in a case-insensitive manner.
   *
   * @param name - the name, on its own or as `{ name }`
   * @returns the constant, or `UNKNOWN` when no constant has that name
   * @throws `TypeError` when the name is not a string or null
   */
  static parse(name: string | null): GroupType;
  /**
   * The same as `parse(name)`, the name given by name.
   *
   * @param options - the name
   * @returns the constant, or `UNKNOWN` when no constant has that name
   */
  static parse(options: {
    /** The name to parse. */
    name: string | null;
  }): GroupType;
  /**
   * Parses the name.
   *
   * @param name - the name, on its own or as `{ name }`
   * @returns the constant
   */
  static parse(name: unknown): GroupType {
    PARSE_FORMS.match(name, arguments.length);
    const value = PARSE_FORMS.value(name, 'name');
    if (value === null) return GroupType.UNKNOWN;
    if (typeof value !== 'string') throw new TypeError(`name must be a string, not ${typeof value}`);
    const key = value.toLowerCase();
    return GroupType.values().find((c) => c.#displayName.toLowerCase() === key) ?? GroupType.UNKNOWN;
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
   * Java's `toString()` override: the display name (`Share`).
   *
   * @returns the display name
   */
  toString(): string {
    return this.#displayName;
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
