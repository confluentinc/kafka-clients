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

// Java's org.apache.kafka.clients.admin.TransactionState, an enum (root
// CLAUDE.md, JavaScript Binding Conventions, Class family): frozen constants
// with java.lang.Enum's `name()`, `ordinal()`, `values()` and `valueOf()`.
// Java overrides `toString()` to return each constant's display name
// (`Ongoing`), while `toJSON()` returns the constant's name, as for every
// enum here. It crosses the addon as its ordinal: the C enum's order is
// Java's declaration order.

import { JavaForms } from '../internal/args';
import { IllegalArgumentError } from '../illegal_argument_error';

const JAVA_CLASS = 'org.apache.kafka.clients.admin.TransactionState';

const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const VALUES_FORMS = READER('values');
const NAME_FORMS = READER('name');
const ORDINAL_FORMS = READER('ordinal');
const isName = (value: unknown): boolean => value === null || typeof value === 'string';
const VALUE_OF_FORMS = new JavaForms('valueOf', [['name']], { bareTypes: { name: isName } });
const PARSE_FORMS = new JavaForms('parse', [['name']], { bareTypes: { name: isName } });

/**
 * The state of a transaction.
 */
export class TransactionState {
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

  /** The `Ongoing` value. */
  static readonly ONGOING: TransactionState = new TransactionState(0, 'ONGOING', 'Ongoing');

  /** The `PrepareAbort` value. */
  static readonly PREPARE_ABORT: TransactionState = new TransactionState(1, 'PREPARE_ABORT', 'PrepareAbort');

  /** The `PrepareCommit` value. */
  static readonly PREPARE_COMMIT: TransactionState = new TransactionState(2, 'PREPARE_COMMIT', 'PrepareCommit');

  /** The `CompleteAbort` value. */
  static readonly COMPLETE_ABORT: TransactionState = new TransactionState(3, 'COMPLETE_ABORT', 'CompleteAbort');

  /** The `CompleteCommit` value. */
  static readonly COMPLETE_COMMIT: TransactionState = new TransactionState(4, 'COMPLETE_COMMIT', 'CompleteCommit');

  /** The `Empty` value. */
  static readonly EMPTY: TransactionState = new TransactionState(5, 'EMPTY', 'Empty');

  /** The `PrepareEpochFence` value. */
  static readonly PREPARE_EPOCH_FENCE: TransactionState = new TransactionState(6, 'PREPARE_EPOCH_FENCE', 'PrepareEpochFence');

  /** The `Unknown` value. */
  static readonly UNKNOWN: TransactionState = new TransactionState(7, 'UNKNOWN', 'Unknown');

  /**
   * The constants in declaration order, Java's `values()`.
   *
   * @returns a new array of the constants
   * @throws `TypeError` when it is given an argument: it only reads
   */
  static values(): TransactionState[] {
    VALUES_FORMS.match(undefined, arguments.length);
    return [
      TransactionState.ONGOING,
      TransactionState.PREPARE_ABORT,
      TransactionState.PREPARE_COMMIT,
      TransactionState.COMPLETE_ABORT,
      TransactionState.COMPLETE_COMMIT,
      TransactionState.EMPTY,
      TransactionState.PREPARE_EPOCH_FENCE,
      TransactionState.UNKNOWN,
    ];
  }

  /**
   * The constant named exactly `name`, Java's `valueOf(String)`.
   *
   * @param name - the name of the constant to return
   * @returns the constant with the specified name
   */
  static valueOf(name: string): TransactionState;
  /**
   * The same as `valueOf(name)`, the name given by name.
   *
   * @param options - the name of the constant to return
   * @returns the constant with the specified name
   */
  static valueOf(options: {
    /** The name of the constant to return. */
    name: string;
  }): TransactionState;
  /**
   * Looks the constant up by its name.
   *
   * @param name - the name, on its own or as `{ name }`
   * @returns the constant
   * @throws `IllegalArgumentError` when no constant has that name
   * @throws `TypeError` when the name is null or not a string
   */
  static valueOf(name: unknown): TransactionState {
    VALUE_OF_FORMS.match(name, arguments.length);
    const value = VALUE_OF_FORMS.value(name, 'name');
    if (value === null) throw new TypeError('Name is null');
    if (typeof value !== 'string') throw new TypeError(`name must be a string, not ${typeof value}`);
    const found = TransactionState.values().find((c) => c.#name === value);
    if (found === undefined) throw new IllegalArgumentError(`No enum constant ${JAVA_CLASS}.${value}`);
    return found;
  }

  /**
   * The constant whose display name is exactly `name` (`Ongoing`), or
   * `UNKNOWN` for any other name, null included.
   *
   * @param name - the display name
   * @returns the constant
   */
  static parse(name: string | null): TransactionState;
  /**
   * The same as `parse(name)`, the name given by name.
   *
   * @param options - the display name
   * @returns the constant
   */
  static parse(options: {
    /** The display name, or null. */
    name: string | null;
  }): TransactionState;
  /**
   * Parses the display name, as Java's `NAME_TO_ENUM.getOrDefault(name, UNKNOWN)`.
   *
   * @param name - the name, on its own or as `{ name }`
   * @returns the constant
   * @throws `TypeError` when the name is neither a string nor null
   */
  static parse(name: unknown): TransactionState {
    PARSE_FORMS.match(name, arguments.length);
    const value = PARSE_FORMS.value(name, 'name');
    if (value !== null && typeof value !== 'string') throw new TypeError(`name must be a string or null, not ${typeof value}`);
    return TransactionState.values().find((c) => c.#displayName === value) ?? TransactionState.UNKNOWN;
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
   * Java's `toString()` override: the display name (`Ongoing`).
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
