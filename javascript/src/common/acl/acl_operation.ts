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

// Java's org.apache.kafka.common.acl.AclOperation, an enum (javascript/CLAUDE.md,
// JavaScript Binding Conventions, Class family): frozen constants carrying
// Java's `code()`, with java.lang.Enum's `name()`, `ordinal()`, `values()`
// and `valueOf()`; `toString()` and `toJSON()` return the name.

import { JavaForms } from '../../internal/args';
import { IllegalArgumentError } from '../../illegal_argument_error';

const JAVA_CLASS = 'org.apache.kafka.common.acl.AclOperation';

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
 * Represents an operation which an ACL grants or denies permission to perform.
 *
 * Some operations imply other operations:
 *
 * - `ALLOW ALL` implies `ALLOW` everything
 * - `DENY ALL` implies `DENY` everything
 * - `ALLOW READ` implies `ALLOW DESCRIBE`
 * - `ALLOW WRITE` implies `ALLOW DESCRIBE`
 * - `ALLOW DELETE` implies `ALLOW DESCRIBE`
 * - `ALLOW ALTER` implies `ALLOW DESCRIBE`
 * - `ALLOW ALTER_CONFIGS` implies `ALLOW DESCRIBE_CONFIGS`
 *
 * The API for this class is still evolving and we may break compatibility in
 * minor releases, if necessary.
 */
export class AclOperation {
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

  /** Represents any AclOperation which this client cannot understand, perhaps because this client is too old. */
  static readonly UNKNOWN: AclOperation = new AclOperation(0, 'UNKNOWN', 0);

  /** In a filter, matches any AclOperation. */
  static readonly ANY: AclOperation = new AclOperation(1, 'ANY', 1);

  /** ALL operation. */
  static readonly ALL: AclOperation = new AclOperation(2, 'ALL', 2);

  /** READ operation. */
  static readonly READ: AclOperation = new AclOperation(3, 'READ', 3);

  /** WRITE operation. */
  static readonly WRITE: AclOperation = new AclOperation(4, 'WRITE', 4);

  /** CREATE operation. */
  static readonly CREATE: AclOperation = new AclOperation(5, 'CREATE', 5);

  /** DELETE operation. */
  static readonly DELETE: AclOperation = new AclOperation(6, 'DELETE', 6);

  /** ALTER operation. */
  static readonly ALTER: AclOperation = new AclOperation(7, 'ALTER', 7);

  /** DESCRIBE operation. */
  static readonly DESCRIBE: AclOperation = new AclOperation(8, 'DESCRIBE', 8);

  /** CLUSTER_ACTION operation. */
  static readonly CLUSTER_ACTION: AclOperation = new AclOperation(9, 'CLUSTER_ACTION', 9);

  /** DESCRIBE_CONFIGS operation. */
  static readonly DESCRIBE_CONFIGS: AclOperation = new AclOperation(10, 'DESCRIBE_CONFIGS', 10);

  /** ALTER_CONFIGS operation. */
  static readonly ALTER_CONFIGS: AclOperation = new AclOperation(11, 'ALTER_CONFIGS', 11);

  /** IDEMPOTENT_WRITE operation. */
  static readonly IDEMPOTENT_WRITE: AclOperation = new AclOperation(12, 'IDEMPOTENT_WRITE', 12);

  /** CREATE_TOKENS operation. */
  static readonly CREATE_TOKENS: AclOperation = new AclOperation(13, 'CREATE_TOKENS', 13);

  /** DESCRIBE_TOKENS operation. */
  static readonly DESCRIBE_TOKENS: AclOperation = new AclOperation(14, 'DESCRIBE_TOKENS', 14);

  /** TWO_PHASE_COMMIT operation. */
  static readonly TWO_PHASE_COMMIT: AclOperation = new AclOperation(15, 'TWO_PHASE_COMMIT', 15);

  /**
   * The constants in declaration order, Java's `values()`.
   *
   * @returns a new array of the constants
   * @throws `TypeError` when it is given an argument: it only reads
   */
  static values(): AclOperation[] {
    VALUES_FORMS.match(undefined, arguments.length);
    return [
      AclOperation.UNKNOWN,
      AclOperation.ANY,
      AclOperation.ALL,
      AclOperation.READ,
      AclOperation.WRITE,
      AclOperation.CREATE,
      AclOperation.DELETE,
      AclOperation.ALTER,
      AclOperation.DESCRIBE,
      AclOperation.CLUSTER_ACTION,
      AclOperation.DESCRIBE_CONFIGS,
      AclOperation.ALTER_CONFIGS,
      AclOperation.IDEMPOTENT_WRITE,
      AclOperation.CREATE_TOKENS,
      AclOperation.DESCRIBE_TOKENS,
      AclOperation.TWO_PHASE_COMMIT,
    ];
  }

  /**
   * The constant named exactly `name`, Java's `valueOf(String)`.
   *
   * @param name - the name, on its own or as `{ name }`
   * @returns the constant with the specified name
   * @throws `IllegalArgumentError` when no constant has that name
   * @throws `TypeError` when the name is null or not a string
   */
  static valueOf(name: string): AclOperation;
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
  }): AclOperation;
  /**
   * The constant named exactly `name`, Java's `valueOf(String)`.
   *
   * @param name - the name, on its own or as `{ name }`
   * @returns the constant with the specified name
   * @throws `IllegalArgumentError` when no constant has that name
   * @throws `TypeError` when the name is null or not a string
   */
  static valueOf(name: unknown): AclOperation {
    VALUE_OF_FORMS.match(name, arguments.length);
    const value = VALUE_OF_FORMS.value(name, 'name');
    if (value === null) throw new TypeError('Name is null');
    if (typeof value !== 'string') throw new TypeError(`name must be a string, not ${typeof value}`);
    const found = AclOperation.values().find((o) => o.#name === value);
    if (found === undefined) throw new IllegalArgumentError(`No enum constant ${JAVA_CLASS}.${value}`);
    return found;
  }

  /**
   * Parse the given string as an ACL operation.
   *
   * @param str - The string to parse, on its own or as `{ str }`.
   * @returns The AclOperation, or UNKNOWN if the string could not be matched.
   * @throws `TypeError` when the string is null (Java dereferences it)
   */
  static fromString(str: string): AclOperation;
  /**
   * The same as `fromString(str)`, the parameter given by name.
   *
   * @param options - The string to parse.
   * @returns The AclOperation, or UNKNOWN if the string could not be matched.
   * @throws `TypeError` when the string is null (Java dereferences it)
   */
  static fromString(options: {
    /** The string to parse. */
    str: string;
  }): AclOperation;
  /**
   * Parse the given string as an ACL operation.
   *
   * @param str - The string to parse, on its own or as `{ str }`.
   * @returns The AclOperation, or UNKNOWN if the string could not be matched.
   * @throws `TypeError` when the string is null (Java dereferences it)
   */
  static fromString(str: unknown): AclOperation {
    FROM_STRING_FORMS.match(str, arguments.length);
    const value = FROM_STRING_FORMS.value(str, 'str');
    if (value === null) throw new TypeError('str must not be null');
    if (typeof value !== 'string') throw new TypeError(`str must be a string, not ${typeof value}`);
    const upper = value.toUpperCase();
    return AclOperation.values().find((o) => o.#name === upper) ?? AclOperation.UNKNOWN;
  }

  /**
   * Return the AclOperation with the provided code or `AclOperation.UNKNOWN`
   * if one cannot be found.
   *
   * @param code - the code, a Java `byte`, on its own or as `{ code }`
   * @returns the operation
   * @throws `TypeError` when the code is not an integer
   * @throws `RangeError` when it is outside Java's `byte`
   */
  static fromCode(code: number): AclOperation;
  /**
   * The same as `fromCode(code)`, the parameter given by name.
   *
   * @param options - the code, a Java `byte`
   * @returns the operation
   * @throws `TypeError` when the code is not an integer
   * @throws `RangeError` when it is outside Java's `byte`
   */
  static fromCode(options: {
    /** The code, a Java `byte` */
    code: number;
  }): AclOperation;
  /**
   * Return the AclOperation with the provided code or `AclOperation.UNKNOWN`
   * if one cannot be found.
   *
   * @param code - the code, a Java `byte`, on its own or as `{ code }`
   * @returns the operation
   * @throws `TypeError` when the code is not an integer
   * @throws `RangeError` when it is outside Java's `byte`
   */
  static fromCode(code: unknown): AclOperation {
    FROM_CODE_FORMS.match(code, arguments.length);
    const value = FROM_CODE_FORMS.value(code, 'code');
    if (typeof value !== 'number' || !Number.isInteger(value)) throw new TypeError('code must be an integer');
    if (value < -128 || value > 127) throw new RangeError('code must be from -128 to 127');
    return AclOperation.values().find((o) => o.#code === value) ?? AclOperation.UNKNOWN;
  }

  /**
   * Return the code of this operation.
   *
   * @returns the code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  code(): number {
    CODE_FORMS.match(undefined, arguments.length);
    return this.#code;
  }

  /**
   * Return true if this operation is UNKNOWN.
   *
   * @returns whether this is `UNKNOWN`
   * @throws `TypeError` when it is given an argument: it only reads
   */
  isUnknown(): boolean {
    IS_UNKNOWN_FORMS.match(undefined, arguments.length);
    return this === AclOperation.UNKNOWN;
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
