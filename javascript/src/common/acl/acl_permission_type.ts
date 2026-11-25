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

// Java's org.apache.kafka.common.acl.AclPermissionType, an enum (javascript/CLAUDE.md,
// JavaScript Binding Conventions, Class family): frozen constants carrying
// Java's `code()`, with java.lang.Enum's `name()`, `ordinal()`, `values()`
// and `valueOf()`; `toString()` and `toJSON()` return the name.

import { JavaForms } from '../../internal/args';
import { IllegalArgumentError } from '../../illegal_argument_error';

const JAVA_CLASS = 'org.apache.kafka.common.acl.AclPermissionType';

const isString = (value: unknown): boolean => value === null || typeof value === 'string';
const FROM_STRING_FORMS = new JavaForms('fromString', [['str']], { bareTypes: { str: isString } });
const FROM_CODE_FORMS = new JavaForms('fromCode', [['code']], { bareTypes: { code: (value) => typeof value === 'number' } });
const VALUE_OF_FORMS = new JavaForms('valueOf', [['name']], { bareTypes: { name: isString } });
const VALUES_FORMS = new JavaForms('values', [[]], { reader: true });
const CODE_FORMS = new JavaForms('code', [[]], { reader: true });
const IS_UNKNOWN_FORMS = new JavaForms('isUnknown', [[]], { reader: true });
const NAME_FORMS = new JavaForms('name', [[]], { reader: true });
const ORDINAL_FORMS = new JavaForms('ordinal', [[]], { reader: true });

/**
 * Represents whether an ACL grants or denies permissions.
 */
export class AclPermissionType {
  readonly #code: number;
  readonly #name: string;
  readonly #ordinal: number;

  /**
   * Made by this class only (the enum's constants).
   *
   * @param ordinal - the constant's position in Java's declaration
   * @param name - the constant's name
   * @param code - Java's byte code
   */
  private constructor(ordinal: number, name: string, code: number) {
    this.#code = code;
    this.#name = name;
    this.#ordinal = ordinal;
    Object.freeze(this);
  }

  /** Represents any AclPermissionType which this client cannot understand, perhaps because this client is too old. */
  static readonly UNKNOWN: AclPermissionType = new AclPermissionType(0, 'UNKNOWN', 0);

  /** In a filter, matches any AclPermissionType. */
  static readonly ANY: AclPermissionType = new AclPermissionType(1, 'ANY', 1);

  /** Disallows access. */
  static readonly DENY: AclPermissionType = new AclPermissionType(2, 'DENY', 2);

  /** Grants access. */
  static readonly ALLOW: AclPermissionType = new AclPermissionType(3, 'ALLOW', 3);

  /**
   * The constants in declaration order, Java's `values()`.
   *
   * @returns a new array of the constants
   * @throws `TypeError` when it is given an argument: it only reads
   */
  static values(): AclPermissionType[] {
    VALUES_FORMS.match(undefined, arguments.length);
    return [AclPermissionType.UNKNOWN, AclPermissionType.ANY, AclPermissionType.DENY, AclPermissionType.ALLOW];
  }

  /**
   * The constant named exactly `name`, Java's `valueOf(String)`.
   *
   * @param name - the name, on its own or as `{ name }`
   * @returns the constant with the specified name
   * @throws `IllegalArgumentError` when no constant has that name
   * @throws `TypeError` when the name is null or not a string
   */
  static valueOf(name: string): AclPermissionType;
  /**
   * The same as `valueOf(name)`, the parameter given by name.
   *
   * @param options - the constant's name
   * @returns the constant with the specified name
   * @throws `IllegalArgumentError` when no constant has that name
   * @throws `TypeError` when the name is null or not a string
   */
  static valueOf(options: {
    /** The constant's name */
    name: string;
  }): AclPermissionType;
  static valueOf(name: unknown): AclPermissionType {
    VALUE_OF_FORMS.match(name, arguments.length);
    const value = VALUE_OF_FORMS.value(name, 'name');
    if (value === null) throw new TypeError('Name is null');
    if (typeof value !== 'string') throw new TypeError(`name must be a string, not ${typeof value}`);
    const found = AclPermissionType.values().find((o) => o.#name === value);
    if (found === undefined) throw new IllegalArgumentError(`No enum constant ${JAVA_CLASS}.${value}`);
    return found;
  }

  /**
   * Parse the given string as an ACL permission.
   *
   * @param str - The string to parse, on its own or as `{ str }`.
   * @returns The AclPermissionType, or UNKNOWN if the string could not be matched.
   * @throws `TypeError` when the string is null (Java dereferences it)
   */
  static fromString(str: string): AclPermissionType;
  /**
   * The same as `fromString(str)`, the parameter given by name.
   *
   * @param options - The string to parse.
   * @returns The AclPermissionType, or UNKNOWN if the string could not be matched.
   * @throws `TypeError` when the string is null (Java dereferences it)
   */
  static fromString(options: {
    /** The string to parse. */
    str: string;
  }): AclPermissionType;
  static fromString(str: unknown): AclPermissionType {
    FROM_STRING_FORMS.match(str, arguments.length);
    const value = FROM_STRING_FORMS.value(str, 'str');
    if (value === null) throw new TypeError('str must not be null');
    if (typeof value !== 'string') throw new TypeError(`str must be a string, not ${typeof value}`);
    const upper = value.toUpperCase();
    return AclPermissionType.values().find((o) => o.#name === upper) ?? AclPermissionType.UNKNOWN;
  }

  /**
   * Return the AclPermissionType with the provided code or `AclPermissionType.UNKNOWN` if one
   * cannot be found.
   *
   * @param code - the code, a Java `byte`, on its own or as `{ code }`
   * @returns the constant
   * @throws `TypeError` when the code is not an integer
   * @throws `RangeError` when it is outside Java's `byte`
   */
  static fromCode(code: number): AclPermissionType;
  /**
   * The same as `fromCode(code)`, the parameter given by name.
   *
   * @param options - the code, a Java `byte`
   * @returns the constant
   * @throws `TypeError` when the code is not an integer
   * @throws `RangeError` when it is outside Java's `byte`
   */
  static fromCode(options: {
    /** The code, a Java `byte` */
    code: number;
  }): AclPermissionType;
  static fromCode(code: unknown): AclPermissionType {
    FROM_CODE_FORMS.match(code, arguments.length);
    const value = FROM_CODE_FORMS.value(code, 'code');
    if (typeof value !== 'number' || !Number.isInteger(value)) throw new TypeError('code must be an integer');
    if (value < -128 || value > 127) throw new RangeError('code must be from -128 to 127');
    return AclPermissionType.values().find((o) => o.#code === value) ?? AclPermissionType.UNKNOWN;
  }

  /**
   * Return the code of this permission type.
   *
   * @returns the code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  code(): number {
    CODE_FORMS.match(undefined, arguments.length);
    return this.#code;
  }

  /**
   * Return true if this permission type is UNKNOWN.
   *
   * @returns whether this is `UNKNOWN`
   * @throws `TypeError` when it is given an argument: it only reads
   */
  isUnknown(): boolean {
    IS_UNKNOWN_FORMS.match(undefined, arguments.length);
    return this === AclPermissionType.UNKNOWN;
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
