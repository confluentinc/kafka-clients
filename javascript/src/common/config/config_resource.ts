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

// Java's org.apache.kafka.common.config.ConfigResource, a value class that
// keeps Java's methods, with its nested Type enum, a class of the namespace
// merged with it (javascript/CLAUDE.md, JavaScript Binding Conventions, Names).
// equals, hashCode and toString are Java's.

import { JavaForms } from '../../internal/args';
import { objectHashCode, sameClass, stringHashCode } from '../../internal/java_object';
import { IllegalArgumentError } from '../../illegal_argument_error';

const FORMS = new JavaForms('ConfigResource', [['type', 'name']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const TYPE_FORMS = READER('type');
const NAME_FORMS = READER('name');
const IS_DEFAULT_FORMS = READER('isDefault');
const HASH_CODE_FORMS = READER('hashCode');
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const TYPE_ID_FORMS = READER('id');
const TYPE_FOR_ID_FORMS = new JavaForms('forId', [['id']], { bareTypes: { id: (value) => typeof value === 'number' } });
const TYPE_VALUES_FORMS = READER('values');
const TYPE_VALUE_OF_FORMS = new JavaForms('valueOf', [['name']], {
  bareTypes: { name: (value) => value === null || typeof value === 'string' },
});
const TYPE_NAME_FORMS = READER('name');
const TYPE_ORDINAL_FORMS = READER('ordinal');

/**
 * A class representing resources that have configs.
 */
export class ConfigResource {
  readonly #type: ConfigResource.Type;
  readonly #name: string;

  /**
   * Create an instance of this class with the provided parameters.
   *
   * @param options - a non-null resource type, and a non-null resource name
   * @throws `TypeError` "type should not be null" or "name should not be
   *   null", as Java's `Objects.requireNonNull`, or for a value of the wrong
   *   type
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{type, name}`
   */
  constructor(options: {
    /** A non-null resource type. */
    type: ConfigResource.Type;
    /** A non-null resource name. */
    name: string;
  }) {
    FORMS.match(options, arguments.length);
    const type = FORMS.value(options, 'type');
    const name = FORMS.value(options, 'name');
    if (type === null) throw new TypeError('type should not be null');
    if (name === null) throw new TypeError('name should not be null');
    if (!(type instanceof ConfigResource.Type)) throw new TypeError('type must be a ConfigResource.Type');
    if (typeof name !== 'string') throw new TypeError('name must be a string');
    this.#type = type;
    this.#name = name;
  }

  /**
   * Return the resource type.
   *
   * @returns the resource type
   * @throws `TypeError` when it is given an argument: it only reads
   */
  type(): ConfigResource.Type {
    if (arguments.length !== 0) TYPE_FORMS.match(undefined, arguments.length);
    return this.#type;
  }

  /**
   * Return the resource name.
   *
   * @returns the resource name
   * @throws `TypeError` when it is given an argument: it only reads
   */
  name(): string {
    if (arguments.length !== 0) NAME_FORMS.match(undefined, arguments.length);
    return this.#name;
  }

  /**
   * Returns true if this is the default resource of a resource type.
   * Resource name is empty for the default resource.
   *
   * @returns whether the name is empty
   * @throws `TypeError` when it is given an argument: it only reads
   */
  isDefault(): boolean {
    if (arguments.length !== 0) IS_DEFAULT_FORMS.match(undefined, arguments.length);
    return this.#name.length === 0;
  }

  /**
   * Whether `o` is a ConfigResource of the same type and name.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as ConfigResource;
    return this.#type === that.#type && this.#name === that.#name;
  }

  /**
   * Java's `hashCode()`: the type's, then the name's.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return (Math.imul(31, objectHashCode(this.#type)) + stringHashCode(this.#name)) | 0;
  }

  /**
   * Java's `toString()`: `ConfigResource(type=<type>, name='<name>')`.
   *
   * @returns the text
   */
  toString(): string {
    return `ConfigResource(type=${this.#type.name()}, name='${this.#name}')`;
  }

  /**
   * The resource as `JSON.stringify` writes it: `{ type, name }`, the type
   * by name.
   *
   * @returns the resource's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { type: string; name: string } {
    return { type: this.#type.name(), name: this.#name };
  }

  /**
   * The key `ConfigResourceMap` stores the resource under: the fields
   * `equals` compares. Called by the map.
   *
   * @returns the key
   * @internal
   */
  internalKey(): string {
    return JSON.stringify([this.#type.name(), this.#name]);
  }
}

// eslint-disable-next-line @typescript-eslint/no-namespace -- Java's nested enum ConfigResource.Type, reached through the class name as in Java
export namespace ConfigResource {
  /**
   * Type of resource.
   */
  export class Type {
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

    /** A group. */
    static readonly GROUP: Type = new Type(0, 'GROUP', 32);

    /** Client metrics. */
    static readonly CLIENT_METRICS: Type = new Type(1, 'CLIENT_METRICS', 16);

    /** A broker logger. */
    static readonly BROKER_LOGGER: Type = new Type(2, 'BROKER_LOGGER', 8);

    /** A broker. */
    static readonly BROKER: Type = new Type(3, 'BROKER', 4);

    /** A topic. */
    static readonly TOPIC: Type = new Type(4, 'TOPIC', 2);

    /** Unknown. */
    static readonly UNKNOWN: Type = new Type(5, 'UNKNOWN', 0);

    /**
     * The constants in declaration order, Java's `values()`.
     *
     * @returns a new array of the constants
     * @throws `TypeError` when it is given an argument: it only reads
     */
    static values(): Type[] {
      TYPE_VALUES_FORMS.match(undefined, arguments.length);
      return [Type.GROUP, Type.CLIENT_METRICS, Type.BROKER_LOGGER, Type.BROKER, Type.TOPIC, Type.UNKNOWN];
    }

    /**
     * The constant named exactly `name`, Java's `valueOf(String)`.
     *
     * @param name - the name of the constant to return
     * @returns the constant with the specified name
     * @throws `IllegalArgumentError` when no constant has that name
     * @throws `TypeError` when the name is null or not a string
     */
    static valueOf(name: string): Type;
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
    }): Type;
    static valueOf(name: unknown): Type {
      TYPE_VALUE_OF_FORMS.match(name, arguments.length);
      const value = TYPE_VALUE_OF_FORMS.value(name, 'name');
      if (value === null) throw new TypeError('Name is null');
      if (typeof value !== 'string') throw new TypeError(`name must be a string, not ${typeof value}`);
      const found = Type.values().find((t) => t.#name === value);
      if (found === undefined)
        throw new IllegalArgumentError(`No enum constant org.apache.kafka.common.config.ConfigResource.Type.${value}`);
      return found;
    }

    /**
     * The type with the given wire byte, `UNKNOWN` for one this client does
     * not know.
     *
     * @param id - the byte, on its own or as `{ id }`
     * @returns the type
     * @throws `TypeError` when the id is not an integer
     * @throws `RangeError` when it is outside Java's `byte`
     */
    static forId(id: number): Type;
    /**
     * The same as `forId(id)`, the id given by name.
     *
     * @param options - the byte
     * @returns the type
     * @throws `TypeError` when the id is not an integer
     * @throws `RangeError` when it is outside Java's `byte`
     */
    static forId(options: {
      /** The wire byte. */
      id: number;
    }): Type;
    static forId(id: unknown): Type {
      TYPE_FOR_ID_FORMS.match(id, arguments.length);
      const value = TYPE_FOR_ID_FORMS.value(id, 'id');
      if (typeof value !== 'number' || !Number.isInteger(value)) throw new TypeError('id must be an integer');
      if (value < -128 || value > 127) throw new RangeError('id must be from -128 to 127');
      return Type.values().find((t) => t.#id === value) ?? Type.UNKNOWN;
    }

    /**
     * The type's wire byte.
     *
     * @returns the id
     * @throws `TypeError` when it is given an argument: it only reads
     */
    id(): number {
      TYPE_ID_FORMS.match(undefined, arguments.length);
      return this.#id;
    }

    /**
     * The name of this constant, exactly as declared, Java's `Enum.name()`.
     *
     * @returns the name
     * @throws `TypeError` when it is given an argument: it only reads
     */
    name(): string {
      TYPE_NAME_FORMS.match(undefined, arguments.length);
      return this.#name;
    }

    /**
     * The position of this constant in its enum declaration, Java's `Enum.ordinal()`.
     *
     * @returns the ordinal
     * @throws `TypeError` when it is given an argument: it only reads
     */
    ordinal(): number {
      TYPE_ORDINAL_FORMS.match(undefined, arguments.length);
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
