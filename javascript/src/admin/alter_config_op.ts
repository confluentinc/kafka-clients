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

// Java's org.apache.kafka.clients.admin.AlterConfigOp, a value class that
// keeps Java's methods, with its nested OpType enum, a class of the namespace
// merged with it (root CLAUDE.md, JavaScript Binding Conventions, Names,
// Q11). equals, hashCode and toString are Java's.

import { JavaForms, kind } from '../internal/args';
import { objectHashCode, objectsEquals, sameClass } from '../internal/java_object';
import { javaString } from '../internal/java_string';
import { IllegalArgumentError } from '../illegal_argument_error';
import { ConfigEntry } from './config_entry';

const FORMS = new JavaForms('AlterConfigOp', [['configEntry', 'operationType']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const CONFIG_ENTRY_FORMS = READER('configEntry');
const OP_TYPE_FORMS = READER('opType');
const HASH_CODE_FORMS = READER('hashCode');
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const OP_ID_FORMS = READER('id');
const OP_FOR_ID_FORMS = new JavaForms('forId', [['id']], { bareTypes: { id: (value) => typeof value === 'number' } });
const OP_VALUES_FORMS = READER('values');
const OP_VALUE_OF_FORMS = new JavaForms('valueOf', [['name']], {
  bareTypes: { name: (value) => value === null || typeof value === 'string' },
});
const OP_NAME_FORMS = READER('name');
const OP_ORDINAL_FORMS = READER('ordinal');

/**
 * A class representing an alter configuration entry containing name, value
 * and operation type.
 *
 * **Note for Broker Logger Configuration:** When altering broker logger
 * levels (using `ConfigResource.Type.BROKER_LOGGER`), it is strongly
 * recommended to use log level constants from `LogLevelConfig` instead of
 * string literals. This ensures compatibility with Kafka's log level
 * validation and avoids potential configuration errors.
 *
 * Example:
 *
 * ```
 * Recommended approach:
 * new AlterConfigOp({ configEntry: new ConfigEntry({ name: loggerName, value: LogLevelConfig.DEBUG_LOG_LEVEL }), operationType: AlterConfigOp.OpType.SET })
 *
 * Avoid this:
 * new AlterConfigOp({ configEntry: new ConfigEntry({ name: loggerName, value: 'DEBUG' }), operationType: AlterConfigOp.OpType.SET })
 * ```
 */
export class AlterConfigOp {
  readonly #configEntry: ConfigEntry | null;
  readonly #opType: AlterConfigOp.OpType | null;

  /**
   * An operation on one configuration entry.
   *
   * @param options - the entry, and how to alter it
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{configEntry, operationType}`
   * @throws `TypeError` for a value of the wrong type
   */
  constructor(options: {
    /** The configuration entry to alter. */
    configEntry: ConfigEntry | null;
    /** How to alter it. */
    operationType: AlterConfigOp.OpType | null;
  }) {
    FORMS.match(options, arguments.length);
    const entry = FORMS.value(options, 'configEntry');
    const opType = FORMS.value(options, 'operationType');
    if (entry !== null && !(entry instanceof ConfigEntry))
      throw new TypeError(`configEntry must be a ConfigEntry or null, not ${kind(entry)}`);
    if (opType !== null && !(opType instanceof AlterConfigOp.OpType)) {
      throw new TypeError(`operationType must be an AlterConfigOp.OpType or null, not ${kind(opType)}`);
    }
    this.#configEntry = entry;
    this.#opType = opType;
  }

  /**
   * The configuration entry to alter.
   *
   * @returns the entry
   * @throws `TypeError` when it is given an argument: it only reads
   */
  configEntry(): ConfigEntry | null {
    if (arguments.length !== 0) CONFIG_ENTRY_FORMS.match(undefined, arguments.length);
    return this.#configEntry;
  }

  /**
   * How to alter the entry.
   *
   * @returns the operation type
   * @throws `TypeError` when it is given an argument: it only reads
   */
  opType(): AlterConfigOp.OpType | null {
    if (arguments.length !== 0) OP_TYPE_FORMS.match(undefined, arguments.length);
    return this.#opType;
  }

  /**
   * Whether `o` is an AlterConfigOp of the same type and an equal entry.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as AlterConfigOp;
    return this.#opType === that.#opType && objectsEquals(this.#configEntry, that.#configEntry);
  }

  /**
   * Java's `Objects.hash(opType, configEntry)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    let h = 1;
    for (const part of [objectHashCode(this.#opType), objectHashCode(this.#configEntry)]) h = (Math.imul(31, h) + part) | 0;
    return h;
  }

  /**
   * Java's `toString()`: `AlterConfigOp{opType=<type>, configEntry=<entry>}`.
   *
   * @returns the text
   */
  toString(): string {
    return `AlterConfigOp{opType=${javaString(this.#opType)}, configEntry=${javaString(this.#configEntry)}}`;
  }

  /**
   * The operation as `JSON.stringify` writes it: `{ opType, configEntry }`,
   * the type by name.
   *
   * @returns the operation's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { opType: string | null; configEntry: ConfigEntry | null } {
    return { opType: this.#opType === null ? null : this.#opType.name(), configEntry: this.#configEntry };
  }
}

// eslint-disable-next-line @typescript-eslint/no-namespace -- Java's nested enum AlterConfigOp.OpType, reached through the class name as in Java
export namespace AlterConfigOp {
  /**
   * The ways a configuration entry is altered.
   */
  export class OpType {
    readonly #ordinal: number;
    readonly #name: string;
    readonly #id: number;

    /**
     * Made by this class only (the enum's constants).
     *
     * @param ordinal - the constant's position in Java's declaration
     * @param name - the constant's name
     * @param id - Java's byte id
     */
    private constructor(ordinal: number, name: string, id: number) {
      this.#ordinal = ordinal;
      this.#name = name;
      this.#id = id;
      Object.freeze(this);
    }

    /** Set the value of the configuration entry. */
    static readonly SET: OpType = new OpType(0, 'SET', 0);

    /** Revert the configuration entry to the default value (possibly null). */
    static readonly DELETE: OpType = new OpType(1, 'DELETE', 1);

    /**
     * (For list-type configuration entries only.) Add the specified values to
     * the current value of the configuration entry. If the configuration
     * value has not been set, adds to the default value.
     */
    static readonly APPEND: OpType = new OpType(2, 'APPEND', 2);

    /**
     * (For list-type configuration entries only.) Removes the specified
     * values from the current value of the configuration entry. It is legal
     * to remove values that are not currently in the configuration entry.
     * Removing all entries from the current configuration value leaves an
     * empty list and does NOT revert to the default value of the entry.
     */
    static readonly SUBTRACT: OpType = new OpType(3, 'SUBTRACT', 3);

    /**
     * The constants in declaration order, Java's `values()`.
     *
     * @returns a new array of the constants
     * @throws `TypeError` when it is given an argument: it only reads
     */
    static values(): OpType[] {
      OP_VALUES_FORMS.match(undefined, arguments.length);
      return [OpType.SET, OpType.DELETE, OpType.APPEND, OpType.SUBTRACT];
    }

    /**
     * The constant named exactly `name`, Java's `valueOf(String)`.
     *
     * @param name - the name of the constant to return
     * @returns the constant with the specified name
     * @throws `IllegalArgumentError` when no constant has that name
     * @throws `TypeError` when the name is null or not a string
     */
    static valueOf(name: string): OpType;
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
    }): OpType;
    /**
     * Looks the constant up.
     *
     * @param name - the name, on its own or as `{ name }`
     * @returns the constant with the specified name
     * @throws `IllegalArgumentError` when no constant has that name
     * @throws `TypeError` when the name is null or not a string
     */
    static valueOf(name: unknown): OpType {
      OP_VALUE_OF_FORMS.match(name, arguments.length);
      const value = OP_VALUE_OF_FORMS.value(name, 'name');
      if (value === null) throw new TypeError('Name is null');
      if (typeof value !== 'string') throw new TypeError(`name must be a string, not ${typeof value}`);
      const found = OpType.values().find((t) => t.#name === value);
      if (found === undefined)
        throw new IllegalArgumentError(`No enum constant org.apache.kafka.clients.admin.AlterConfigOp.OpType.${value}`);
      return found;
    }

    /**
     * The operation type with the given wire byte, or null for one Java does
     * not know.
     *
     * @param id - the byte, on its own or as `{ id }`
     * @returns the operation type, or null
     * @throws `TypeError` when the id is not an integer
     * @throws `RangeError` when it is outside Java's `byte`
     */
    static forId(id: number): OpType | null;
    /**
     * The same as `forId(id)`, the id given by name.
     *
     * @param options - the byte
     * @returns the operation type, or null
     * @throws `TypeError` when the id is not an integer
     * @throws `RangeError` when it is outside Java's `byte`
     */
    static forId(options: {
      /** The wire byte. */
      id: number;
    }): OpType | null;
    /**
     * Looks the operation type up.
     *
     * @param id - the byte, on its own or as `{ id }`
     * @returns the operation type, or null for an unknown byte
     * @throws `TypeError` when the id is not an integer
     * @throws `RangeError` when it is outside Java's `byte`
     */
    static forId(id: unknown): OpType | null {
      OP_FOR_ID_FORMS.match(id, arguments.length);
      const value = OP_FOR_ID_FORMS.value(id, 'id');
      if (typeof value !== 'number' || !Number.isInteger(value)) throw new TypeError('id must be an integer');
      if (value < -128 || value > 127) throw new RangeError('id must be from -128 to 127');
      return OpType.values().find((t) => t.#id === value) ?? null;
    }

    /**
     * The operation type's wire byte.
     *
     * @returns the id
     * @throws `TypeError` when it is given an argument: it only reads
     */
    id(): number {
      OP_ID_FORMS.match(undefined, arguments.length);
      return this.#id;
    }

    /**
     * The name of this constant, exactly as declared, Java's `Enum.name()`.
     *
     * @returns the name
     * @throws `TypeError` when it is given an argument: it only reads
     */
    name(): string {
      OP_NAME_FORMS.match(undefined, arguments.length);
      return this.#name;
    }

    /**
     * The position of this constant in its enum declaration, Java's `Enum.ordinal()`.
     *
     * @returns the ordinal
     * @throws `TypeError` when it is given an argument: it only reads
     */
    ordinal(): number {
      OP_ORDINAL_FORMS.match(undefined, arguments.length);
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
}
