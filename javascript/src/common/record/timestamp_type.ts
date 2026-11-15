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

// Java's org.apache.kafka.common.record.TimestampType, as
// python/confluent_kafka/common/record/timestamp_type.py translates it: an
// enum whose constants are frozen instances carrying Java's public field
// `id`, with java.lang.Enum's `name()` and `ordinal()`. Java's public field
// `name` ("CreateTime") is reached through Java's own toString(), which
// returns it, as Python's Enum rule reaches it: JavaScript cannot hold a
// field and the Enum method `name()` under one name.

import { JavaForms } from '../../internal/args';
import { IllegalArgumentError } from '../../illegal_argument_error';
import { NoSuchElementError } from '../../no_such_element_error';

const JAVA_CLASS = 'org.apache.kafka.common.record.TimestampType';

// Java's forms (src/internal/args.ts): the bare value or `{name}`, and no
// argument for the methods of no parameter.
const isName = (value: unknown): boolean => value === null || typeof value === 'string';
const FOR_NAME_FORMS = new JavaForms('forName', [['name']], { bareTypes: { name: isName } });
const VALUE_OF_FORMS = new JavaForms('valueOf', [['name']], { bareTypes: { name: isName } });
const VALUES_FORMS = new JavaForms('values', [[]], { reader: true });
const NAME_FORMS = new JavaForms('name', [[]], { reader: true });
const ORDINAL_FORMS = new JavaForms('ordinal', [[]], { reader: true });

/**
 * The timestamp type of the records.
 *
 * Java: `org.apache.kafka.common.record.TimestampType`.
 */
export class TimestampType {
  /** Java's public field `id`: -1, 0 or 1. */
  readonly id: number;

  // Java's public field `name` ("CreateTime"), which toString() returns.
  readonly #text: string;

  readonly #name: string;

  readonly #ordinal: number;

  /**
   * Made by this class only (the enum's constants).
   *
   * @param ordinal the constant's position in Java's declaration, its `ordinal()`
   * @param name the constant's name, its `name()`
   * @param id Java's `id`
   * @param text Java's `name`, which `toString()` returns
   */
  private constructor(ordinal: number, name: string, id: number, text: string) {
    this.id = id;
    this.#text = text;
    this.#name = name;
    this.#ordinal = ordinal;
    Object.freeze(this);
  }

  /** No timestamp type: Java's `NO_TIMESTAMP_TYPE(-1, "NoTimestampType")`. */
  static readonly NO_TIMESTAMP_TYPE: TimestampType = new TimestampType(0, 'NO_TIMESTAMP_TYPE', -1, 'NoTimestampType');

  /** The record's creation time: Java's `CREATE_TIME(0, "CreateTime")`. */
  static readonly CREATE_TIME: TimestampType = new TimestampType(1, 'CREATE_TIME', 0, 'CreateTime');

  /** The broker's append time: Java's `LOG_APPEND_TIME(1, "LogAppendTime")`. */
  static readonly LOG_APPEND_TIME: TimestampType = new TimestampType(2, 'LOG_APPEND_TIME', 1, 'LogAppendTime');

  /**
   * The constants in declaration order, Java's `values()`.
   *
   * @returns a new array of the constants
   * @throws `TypeError` when it is given an argument: it only reads
   */
  static values(): TimestampType[] {
    VALUES_FORMS.match(undefined, arguments.length);
    return [TimestampType.NO_TIMESTAMP_TYPE, TimestampType.CREATE_TIME, TimestampType.LOG_APPEND_TIME];
  }

  /**
   * The constant named exactly `name`, Java's `valueOf(String)`. The name is
   * given on its own, `valueOf('CREATE_TIME')`, or named.
   *
   * @param name - the name of the constant to return
   * @returns the constant with the specified name
   * @throws `IllegalArgumentError` when no constant has that name, or the
   *   given parameters are not `{name}`
   * @throws `TypeError` when the name is null or not a string
   */
  static valueOf(name: string): TimestampType;
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
  }): TimestampType;
  static valueOf(options: unknown): TimestampType {
    VALUE_OF_FORMS.match(options, arguments.length);
    const name = VALUE_OF_FORMS.value(options, 'name');
    // java.lang.Enum.valueOf: requireNonNull's "Name is null".
    if (name === null) throw new TypeError('Name is null');
    if (typeof name !== 'string') throw new TypeError(`name must be a string, not ${typeof name}`);
    const found = TimestampType.values().find((t) => t.#name === name);
    if (found === undefined) throw new IllegalArgumentError(`No enum constant ${JAVA_CLASS}.${name}`);
    return found;
  }

  /**
   * The constant whose Java `name` field is `name` (`'CreateTime'`), Java's
   * `forName`. The name is given on its own, `forName('CreateTime')`, or
   * named.
   *
   * @param name - the type's name, as `toString()` writes it
   * @returns the constant
   * @throws `NoSuchElementError` when no constant has that name ("Invalid
   *   timestamp type <name>")
   * @throws `IllegalArgumentError` when the given parameters are not `{name}`
   * @throws `TypeError` when the name is not a string
   */
  static forName(name: string | null): TimestampType;
  /**
   * The constant whose Java `name` field is `name`, the same as
   * `forName(name)`.
   *
   * @param options - the type's name
   * @returns the constant
   * @throws `NoSuchElementError` when no constant has that name
   */
  static forName(options: {
    /** The type's name, as `toString()` writes it. */
    name: string | null;
  }): TimestampType;
  static forName(options: unknown): TimestampType {
    FOR_NAME_FORMS.match(options, arguments.length);
    const name = FOR_NAME_FORMS.value(options, 'name');
    if (name !== null && typeof name !== 'string') throw new TypeError(`name must be a string, not ${typeof name}`);
    // Java's t.name.equals(name): a null name matches none.
    const found = TimestampType.values().find((t) => t.#text === name);
    if (found === undefined) throw new NoSuchElementError(`Invalid timestamp type ${String(name)}`);
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
   * Java's override: the public field `name`, as `'CreateTime'`.
   *
   * @returns the type's name
   *
   * @remarks `toJSON()` writes the constant's name, as for every enum.
   */
  toString(): string {
    return this.#text;
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
