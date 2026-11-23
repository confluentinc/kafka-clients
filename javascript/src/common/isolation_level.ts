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

// Java's org.apache.kafka.common.IsolationLevel, an enum (javascript/CLAUDE.md,
// JavaScript Binding Conventions, Class family): frozen constants carrying
// Java's `id()`, with java.lang.Enum's `name()`, `ordinal()`, `values()` and
// `valueOf()`. `toString()` is Java's override, the name in lower case;
// `toJSON()` is the name.

import { JavaForms } from '../internal/args';
import { IllegalArgumentError } from '../illegal_argument_error';

const JAVA_CLASS = 'org.apache.kafka.common.IsolationLevel';

const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const VALUES_FORMS = READER('values');
const ID_FORMS = READER('id');
const NAME_FORMS = READER('name');
const ORDINAL_FORMS = READER('ordinal');
const FOR_ID_FORMS = new JavaForms('forId', [['id']], { bareTypes: { id: (value) => typeof value === 'number' } });
const VALUE_OF_FORMS = new JavaForms('valueOf', [['name']], {
  bareTypes: { name: (value) => value === null || typeof value === 'string' },
});

/**
 * The isolation level of a read: whether it returns the records of aborted
 * and open transactions.
 */
export class IsolationLevel {
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

  /** Every record, those of aborted and open transactions included. */
  static readonly READ_UNCOMMITTED: IsolationLevel = new IsolationLevel(0, 'READ_UNCOMMITTED', 0);

  /** Only the records of committed transactions, and non-transactional records. */
  static readonly READ_COMMITTED: IsolationLevel = new IsolationLevel(1, 'READ_COMMITTED', 1);

  /**
   * The constants in declaration order, Java's `values()`.
   *
   * @returns a new array of the constants
   * @throws `TypeError` when it is given an argument: it only reads
   */
  static values(): IsolationLevel[] {
    VALUES_FORMS.match(undefined, arguments.length);
    return [IsolationLevel.READ_UNCOMMITTED, IsolationLevel.READ_COMMITTED];
  }

  /**
   * The constant named exactly `name`, Java's `valueOf(String)`.
   *
   * @param name - the name of the constant to return
   * @returns the constant with the specified name
   * @throws `IllegalArgumentError` when no constant has that name
   * @throws `TypeError` when the name is null or not a string
   */
  static valueOf(name: string): IsolationLevel;
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
  }): IsolationLevel;
  static valueOf(name: unknown): IsolationLevel {
    VALUE_OF_FORMS.match(name, arguments.length);
    const value = VALUE_OF_FORMS.value(name, 'name');
    if (value === null) throw new TypeError('Name is null');
    if (typeof value !== 'string') throw new TypeError(`name must be a string, not ${typeof value}`);
    const found = IsolationLevel.values().find((l) => l.#name === value);
    if (found === undefined) throw new IllegalArgumentError(`No enum constant ${JAVA_CLASS}.${value}`);
    return found;
  }

  /**
   * The isolation level of the given id.
   *
   * @param id - the byte, on its own or as `{ id }`
   * @returns the isolation level
   * @throws `IllegalArgumentError` "Unknown isolation level <id>" for an id
   *   no level has
   * @throws `TypeError` when the id is not an integer
   * @throws `RangeError` when it is outside Java's `byte`
   */
  static forId(id: number): IsolationLevel;
  /**
   * The same as `forId(id)`, the id given by name.
   *
   * @param options - the byte
   * @returns the isolation level
   * @throws `IllegalArgumentError` "Unknown isolation level <id>" for an id
   *   no level has
   * @throws `TypeError` when the id is not an integer
   * @throws `RangeError` when it is outside Java's `byte`
   */
  static forId(options: {
    /** The id. */
    id: number;
  }): IsolationLevel;
  static forId(id: unknown): IsolationLevel {
    FOR_ID_FORMS.match(id, arguments.length);
    const value = FOR_ID_FORMS.value(id, 'id');
    if (typeof value !== 'number' || !Number.isInteger(value)) throw new TypeError('id must be an integer');
    if (value < -128 || value > 127) throw new RangeError('id must be from -128 to 127');
    const found = IsolationLevel.values().find((l) => l.#id === value);
    if (found === undefined) throw new IllegalArgumentError(`Unknown isolation level ${value}`);
    return found;
  }

  /**
   * The isolation level's byte id.
   *
   * @returns the id
   * @throws `TypeError` when it is given an argument: it only reads
   */
  id(): number {
    ID_FORMS.match(undefined, arguments.length);
    return this.#id;
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
   * Java's `toString()` override: the name in lower case
   * (`read_committed`).
   *
   * @returns the text
   */
  toString(): string {
    return this.#name.toLowerCase();
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
