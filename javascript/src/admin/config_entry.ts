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

// Java's org.apache.kafka.clients.admin.ConfigEntry, a value class that keeps
// Java's methods, with its nested ConfigType and ConfigSource enums and its
// ConfigSynonym class, classes of the namespace merged with it (root
// CLAUDE.md, JavaScript Binding Conventions, Names, Q11). ConfigSynonym's
// Java constructor is package-private: the client builds one through
// `internalCreate`.

import { JavaForms, kind } from '../internal/args';
import { listEquals, listHashCode, objectHashCode, objectsEquals, sameClass, stringHashCode } from '../internal/java_object';
import { javaString } from '../internal/java_string';
import { IllegalArgumentError } from '../illegal_argument_error';

const FORMS = new JavaForms('ConfigEntry', [
  ['name', 'value'],
  ['name', 'value', 'source', 'isSensitive', 'isReadOnly', 'synonyms', 'type', 'documentation'],
]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const NAME_FORMS = READER('name');
const VALUE_FORMS = READER('value');
const SOURCE_FORMS = READER('source');
const IS_DEFAULT_FORMS = READER('isDefault');
const IS_SENSITIVE_FORMS = READER('isSensitive');
const IS_READ_ONLY_FORMS = READER('isReadOnly');
const SYNONYMS_FORMS = READER('synonyms');
const TYPE_FORMS = READER('type');
const DOCUMENTATION_FORMS = READER('documentation');
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = READER('hashCode');
const TYPE_VALUE_OF_FORMS = new JavaForms('valueOf', [['name']], {
  bareTypes: { name: (value) => value === null || typeof value === 'string' },
});
const TYPE_VALUES_FORMS = new JavaForms('values', [[]], { reader: true });
const TYPE_NAME_FORMS = new JavaForms('name', [[]], { reader: true });
const TYPE_ORDINAL_FORMS = new JavaForms('ordinal', [[]], { reader: true });
const SOURCE_VALUE_OF_FORMS = new JavaForms('valueOf', [['name']], {
  bareTypes: { name: (value) => value === null || typeof value === 'string' },
});
const SOURCE_VALUES_FORMS = new JavaForms('values', [[]], { reader: true });
const SOURCE_NAME_FORMS = new JavaForms('name', [[]], { reader: true });
const SOURCE_ORDINAL_FORMS = new JavaForms('ordinal', [[]], { reader: true });

/**
 * A configuration entry for a topic or broker.
 */
export class ConfigEntry {
  readonly #name: string;
  readonly #value: string | null;
  readonly #source: ConfigEntry.ConfigSource | null;
  readonly #isSensitive: boolean;
  readonly #isReadOnly: boolean;
  readonly #synonyms: readonly ConfigEntry.ConfigSynonym[] | null;
  readonly #type: ConfigEntry.ConfigType | null;
  readonly #documentation: string | null;

  /**
   * Create a configuration entry with the provided values.
   *
   * @param options - the non-null config name, and the config value or null
   */
  constructor(options: {
    /** The non-null config name. */
    name: string;
    /** The config value, or null. */
    value: string | null;
    source?: never;
    isSensitive?: never;
    isReadOnly?: never;
    synonyms?: never;
    type?: never;
    documentation?: never;
  });
  /**
   * Create a configuration with the provided values.
   *
   * @param options - the non-null config name, the config value or null,
   *   the source of this config entry, whether the config value is sensitive
   *   (the broker never returns the value if it is sensitive), whether the
   *   config is read-only and cannot be updated, synonym configs in order of
   *   precedence, the config's type and its documentation
   */
  constructor(options: {
    /** The non-null config name. */
    name: string;
    /** The config value, or null. */
    value: string | null;
    /** The source of this config entry. */
    source: ConfigEntry.ConfigSource | null;
    /** Whether the config value is sensitive: the broker never returns the value if it is. */
    isSensitive: boolean;
    /** Whether the config is read-only and cannot be updated. */
    isReadOnly: boolean;
    /** Synonym configs in order of precedence. */
    synonyms: Iterable<ConfigEntry.ConfigSynonym> | null;
    /** The config data type. */
    type: ConfigEntry.ConfigType | null;
    /** The config documentation, or null. */
    documentation: string | null;
  });
  /**
   * Builds the entry.
   *
   * @param options - the fields of one of Java's two constructors
   * @throws `IllegalArgumentError` when the given parameters are no Java form
   * @throws `TypeError` when the name is null ("name should not be null"),
   *   or a value has the wrong type
   */
  constructor(options: unknown) {
    const form = FORMS.match(options, arguments.length);
    const name = FORMS.value(options, 'name');
    // Java's Objects.requireNonNull(name, "name should not be null").
    if (name === null) throw new TypeError('name should not be null');
    if (typeof name !== 'string') throw new TypeError(`name must be a string, not ${kind(name)}`);
    const value = FORMS.value(options, 'value');
    if (value !== null && typeof value !== 'string') throw new TypeError(`value must be a string or null, not ${kind(value)}`);
    this.#name = name;
    this.#value = value;
    if (form === 0) {
      this.#source = ConfigEntry.ConfigSource.UNKNOWN;
      this.#isSensitive = false;
      this.#isReadOnly = false;
      this.#synonyms = Object.freeze([]);
      this.#type = ConfigEntry.ConfigType.UNKNOWN;
      this.#documentation = null;
      return;
    }
    const source = FORMS.value(options, 'source');
    if (source !== null && !(source instanceof ConfigEntry.ConfigSource)) throw new TypeError('source must be a ConfigSource or null');
    const isSensitive = FORMS.value(options, 'isSensitive');
    const isReadOnly = FORMS.value(options, 'isReadOnly');
    if (typeof isSensitive !== 'boolean') throw new TypeError('isSensitive must be a boolean');
    if (typeof isReadOnly !== 'boolean') throw new TypeError('isReadOnly must be a boolean');
    const synonyms = FORMS.value(options, 'synonyms');
    // Java's List<ConfigSynonym>: any iterable, copied.
    if (
      synonyms !== null &&
      (typeof synonyms !== 'object' || typeof (synonyms as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function')
    ) {
      throw new TypeError(`synonyms must be an iterable of ConfigSynonym or null, not ${kind(synonyms)}`);
    }
    const type = FORMS.value(options, 'type');
    if (type !== null && !(type instanceof ConfigEntry.ConfigType)) throw new TypeError('type must be a ConfigType or null');
    const documentation = FORMS.value(options, 'documentation');
    if (documentation !== null && typeof documentation !== 'string') throw new TypeError('documentation must be a string or null');
    this.#source = source;
    this.#isSensitive = isSensitive;
    this.#isReadOnly = isReadOnly;
    const list = synonyms === null ? null : [...(synonyms as Iterable<unknown>)];
    if (list !== null) {
      for (const synonym of list) {
        if (!(synonym instanceof ConfigEntry.ConfigSynonym))
          throw new TypeError(`synonyms must contain only ConfigSynonym, not ${kind(synonym)}`);
      }
    }
    this.#synonyms = list === null ? null : Object.freeze(list as ConfigEntry.ConfigSynonym[]);
    this.#type = type;
    this.#documentation = documentation;
  }

  /**
   * Return the config name.
   *
   * @returns the name
   * @throws `TypeError` when it is given an argument: it only reads
   */
  name(): string {
    if (arguments.length !== 0) NAME_FORMS.match(undefined, arguments.length);
    return this.#name;
  }

  /**
   * Return the value or null. Null is returned if the config is unset or if
   * isSensitive is true.
   *
   * @returns the value, or null
   * @throws `TypeError` when it is given an argument: it only reads
   */
  value(): string | null {
    if (arguments.length !== 0) VALUE_FORMS.match(undefined, arguments.length);
    return this.#value;
  }

  /**
   * Return the source of this configuration entry.
   *
   * @returns the source
   * @throws `TypeError` when it is given an argument: it only reads
   */
  source(): ConfigEntry.ConfigSource | null {
    if (arguments.length !== 0) SOURCE_FORMS.match(undefined, arguments.length);
    return this.#source;
  }

  /**
   * Return whether the config value is the default or if it's been
   * explicitly set.
   *
   * @returns whether the source is `DEFAULT_CONFIG`
   * @throws `TypeError` when it is given an argument: it only reads
   */
  isDefault(): boolean {
    if (arguments.length !== 0) IS_DEFAULT_FORMS.match(undefined, arguments.length);
    return this.#source === ConfigEntry.ConfigSource.DEFAULT_CONFIG;
  }

  /**
   * Return whether the config value is sensitive. The value is always set to
   * null by the broker if the config value is sensitive.
   *
   * @returns whether the value is sensitive
   * @throws `TypeError` when it is given an argument: it only reads
   */
  isSensitive(): boolean {
    if (arguments.length !== 0) IS_SENSITIVE_FORMS.match(undefined, arguments.length);
    return this.#isSensitive;
  }

  /**
   * Return whether the config is read-only and cannot be updated.
   *
   * @returns whether the config is read-only
   * @throws `TypeError` when it is given an argument: it only reads
   */
  isReadOnly(): boolean {
    if (arguments.length !== 0) IS_READ_ONLY_FORMS.match(undefined, arguments.length);
    return this.#isReadOnly;
  }

  /**
   * Returns all config values that may be used as the value of this config
   * along with their source, in the order of precedence. The list starts
   * with the value returned in this ConfigEntry. The list is empty if
   * synonyms were not requested using `DescribeConfigsOptions.includeSynonyms`.
   *
   * @returns the synonyms, a new array, or null
   * @throws `TypeError` when it is given an argument: it only reads
   */
  synonyms(): ConfigEntry.ConfigSynonym[] | null {
    if (arguments.length !== 0) SYNONYMS_FORMS.match(undefined, arguments.length);
    return this.#synonyms === null ? null : [...this.#synonyms];
  }

  /**
   * Return the config data type.
   *
   * @returns the type
   * @throws `TypeError` when it is given an argument: it only reads
   */
  type(): ConfigEntry.ConfigType | null {
    if (arguments.length !== 0) TYPE_FORMS.match(undefined, arguments.length);
    return this.#type;
  }

  /**
   * Return the config documentation.
   *
   * @returns the documentation, or null
   * @throws `TypeError` when it is given an argument: it only reads
   */
  documentation(): string | null {
    if (arguments.length !== 0) DOCUMENTATION_FORMS.match(undefined, arguments.length);
    return this.#documentation;
  }

  /**
   * Whether `o` is a ConfigEntry with equal fields.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as ConfigEntry;
    return (
      this.#name === that.#name &&
      this.#value === that.#value &&
      this.#isSensitive === that.#isSensitive &&
      this.#isReadOnly === that.#isReadOnly &&
      this.#source === that.#source &&
      listEquals(this.#synonyms, that.#synonyms) &&
      this.#type === that.#type &&
      this.#documentation === that.#documentation
    );
  }

  /**
   * Java's `hashCode()` over the entry's fields.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    const prime = 31;
    let result = 1;
    result = (Math.imul(prime, result) + stringHashCode(this.#name)) | 0;
    result = (Math.imul(prime, result) + objectHashCode(this.#value)) | 0;
    result = (Math.imul(prime, result) + (this.#isSensitive ? 1 : 0)) | 0;
    result = (Math.imul(prime, result) + (this.#isReadOnly ? 1 : 0)) | 0;
    result = (Math.imul(prime, result) + objectHashCode(this.#source)) | 0;
    result = (Math.imul(prime, result) + listHashCode(this.#synonyms)) | 0;
    result = (Math.imul(prime, result) + objectHashCode(this.#type)) | 0;
    result = (Math.imul(prime, result) + objectHashCode(this.#documentation)) | 0;
    return result;
  }

  /**
   * The entry as JSON: its fields, the enums by name. A sensitive value is
   * `"Redacted"`, as in `toString()`, and so are its synonyms' values.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): Record<string, unknown> {
    return {
      name: this.#name,
      value: this.#isSensitive ? 'Redacted' : this.#value,
      source: this.#source,
      isSensitive: this.#isSensitive,
      isReadOnly: this.#isReadOnly,
      synonyms:
        this.#synonyms === null || !this.#isSensitive
          ? this.#synonyms
          : this.#synonyms.map((synonym) => ({ ...synonym.toJSON(), value: 'Redacted' })),
      type: this.#type,
      documentation: this.#documentation,
    };
  }

  /**
   * Java's `toString()`, which redacts a sensitive value. WARNING, user
   * should be responsible to set the correct "isSensitive" field for each
   * config entry.
   *
   * @returns the text
   */
  toString(): string {
    return (
      `ConfigEntry(name=${this.#name}, value=${this.#isSensitive ? 'Redacted' : String(this.#value)}` +
      `, source=${String(this.#source)}, isSensitive=${String(this.#isSensitive)}, isReadOnly=${String(this.#isReadOnly)}` +
      `, synonyms=${javaString(this.#synonyms)}, type=${String(this.#type)}, documentation=${String(this.#documentation)})`
    );
  }
}

// eslint-disable-next-line @typescript-eslint/no-namespace -- Java's nested types ConfigEntry.ConfigType, ConfigSource and ConfigSynonym, reached through the class name as in Java
export namespace ConfigEntry {
  /**
   * Data type of configuration entry.
   */
  export class ConfigType {
    readonly #name: string;
    readonly #ordinal: number;

    /**
     * Made by this class only (the enum's constants).
     *
     * @param ordinal - the constant's position in Java's declaration
     * @param name - the constant's name
     */
    private constructor(ordinal: number, name: string) {
      this.#name = name;
      this.#ordinal = ordinal;
      Object.freeze(this);
    }

    /** UNKNOWN configuration data type. */
    static readonly UNKNOWN: ConfigType = new ConfigType(0, 'UNKNOWN');

    /** BOOLEAN configuration data type. */
    static readonly BOOLEAN: ConfigType = new ConfigType(1, 'BOOLEAN');

    /** STRING configuration data type. */
    static readonly STRING: ConfigType = new ConfigType(2, 'STRING');

    /** INT configuration data type. */
    static readonly INT: ConfigType = new ConfigType(3, 'INT');

    /** SHORT configuration data type. */
    static readonly SHORT: ConfigType = new ConfigType(4, 'SHORT');

    /** LONG configuration data type. */
    static readonly LONG: ConfigType = new ConfigType(5, 'LONG');

    /** DOUBLE configuration data type. */
    static readonly DOUBLE: ConfigType = new ConfigType(6, 'DOUBLE');

    /** LIST configuration data type. */
    static readonly LIST: ConfigType = new ConfigType(7, 'LIST');

    /** CLASS configuration data type. */
    static readonly CLASS: ConfigType = new ConfigType(8, 'CLASS');

    /** PASSWORD configuration data type. */
    static readonly PASSWORD: ConfigType = new ConfigType(9, 'PASSWORD');

    /**
     * The constants in declaration order, Java's `values()`.
     *
     * @returns a new array of the constants
     * @throws `TypeError` when it is given an argument: it only reads
     */
    static values(): ConfigType[] {
      TYPE_VALUES_FORMS.match(undefined, arguments.length);
      return [
        ConfigType.UNKNOWN,
        ConfigType.BOOLEAN,
        ConfigType.STRING,
        ConfigType.INT,
        ConfigType.SHORT,
        ConfigType.LONG,
        ConfigType.DOUBLE,
        ConfigType.LIST,
        ConfigType.CLASS,
        ConfigType.PASSWORD,
      ];
    }

    /**
     * The constant named exactly `name`, Java's `valueOf(String)`.
     *
     * @param name - the name of the constant to return
     * @returns the constant with the specified name
     */
    static valueOf(name: string): ConfigType;
    /**
     * The same as `valueOf(name)`, the name given by name.
     *
     * @param options - the name of the constant to return
     * @returns the constant with the specified name
     */
    static valueOf(options: {
      /** The name of the constant to return. */
      name: string;
    }): ConfigType;
    /**
     * Looks the constant up.
     *
     * @param name - the name, on its own or as `{ name }`
     * @returns the constant with the specified name
     * @throws `IllegalArgumentError` when no constant has that name
     * @throws `TypeError` when the name is null or not a string
     */
    static valueOf(name: unknown): ConfigType {
      TYPE_VALUE_OF_FORMS.match(name, arguments.length);
      const value = TYPE_VALUE_OF_FORMS.value(name, 'name');
      if (value === null) throw new TypeError('Name is null');
      if (typeof value !== 'string') throw new TypeError(`name must be a string, not ${typeof value}`);
      const found = ConfigType.values().find((c) => c.#name === value);
      if (found === undefined)
        throw new IllegalArgumentError(`No enum constant org.apache.kafka.clients.admin.ConfigEntry.ConfigType.${value}`);
      return found;
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

  /**
   * Source of configuration entries.
   */
  export class ConfigSource {
    readonly #name: string;
    readonly #ordinal: number;

    /**
     * Made by this class only (the enum's constants).
     *
     * @param ordinal - the constant's position in Java's declaration
     * @param name - the constant's name
     */
    private constructor(ordinal: number, name: string) {
      this.#name = name;
      this.#ordinal = ordinal;
      Object.freeze(this);
    }

    /** Dynamic topic config that is configured for a specific topic. */
    static readonly DYNAMIC_TOPIC_CONFIG: ConfigSource = new ConfigSource(0, 'DYNAMIC_TOPIC_CONFIG');

    /** Dynamic broker logger config that is configured for a specific broker. */
    static readonly DYNAMIC_BROKER_LOGGER_CONFIG: ConfigSource = new ConfigSource(1, 'DYNAMIC_BROKER_LOGGER_CONFIG');

    /** Dynamic broker config that is configured for a specific broker. */
    static readonly DYNAMIC_BROKER_CONFIG: ConfigSource = new ConfigSource(2, 'DYNAMIC_BROKER_CONFIG');

    /** Dynamic broker config that is configured as default for all brokers in the cluster. */
    static readonly DYNAMIC_DEFAULT_BROKER_CONFIG: ConfigSource = new ConfigSource(3, 'DYNAMIC_DEFAULT_BROKER_CONFIG');

    /** Dynamic client metrics subscription config that is configured for all clients. */
    static readonly DYNAMIC_CLIENT_METRICS_CONFIG: ConfigSource = new ConfigSource(4, 'DYNAMIC_CLIENT_METRICS_CONFIG');

    /** Dynamic group config that is configured for a specific group. */
    static readonly DYNAMIC_GROUP_CONFIG: ConfigSource = new ConfigSource(5, 'DYNAMIC_GROUP_CONFIG');

    /** Static broker config provided as broker properties at start up (e.g. server.properties file). */
    static readonly STATIC_BROKER_CONFIG: ConfigSource = new ConfigSource(6, 'STATIC_BROKER_CONFIG');

    /** Built-in default configuration for configs that have a default value. */
    static readonly DEFAULT_CONFIG: ConfigSource = new ConfigSource(7, 'DEFAULT_CONFIG');

    /** Source unknown e.g. in the ConfigEntry used for alter requests where source is not set. */
    static readonly UNKNOWN: ConfigSource = new ConfigSource(8, 'UNKNOWN');

    /**
     * The constants in declaration order, Java's `values()`.
     *
     * @returns a new array of the constants
     * @throws `TypeError` when it is given an argument: it only reads
     */
    static values(): ConfigSource[] {
      SOURCE_VALUES_FORMS.match(undefined, arguments.length);
      return [
        ConfigSource.DYNAMIC_TOPIC_CONFIG,
        ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG,
        ConfigSource.DYNAMIC_BROKER_CONFIG,
        ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG,
        ConfigSource.DYNAMIC_CLIENT_METRICS_CONFIG,
        ConfigSource.DYNAMIC_GROUP_CONFIG,
        ConfigSource.STATIC_BROKER_CONFIG,
        ConfigSource.DEFAULT_CONFIG,
        ConfigSource.UNKNOWN,
      ];
    }

    /**
     * The constant named exactly `name`, Java's `valueOf(String)`.
     *
     * @param name - the name of the constant to return
     * @returns the constant with the specified name
     */
    static valueOf(name: string): ConfigSource;
    /**
     * The same as `valueOf(name)`, the name given by name.
     *
     * @param options - the name of the constant to return
     * @returns the constant with the specified name
     */
    static valueOf(options: {
      /** The name of the constant to return. */
      name: string;
    }): ConfigSource;
    /**
     * Looks the constant up.
     *
     * @param name - the name, on its own or as `{ name }`
     * @returns the constant with the specified name
     * @throws `IllegalArgumentError` when no constant has that name
     * @throws `TypeError` when the name is null or not a string
     */
    static valueOf(name: unknown): ConfigSource {
      SOURCE_VALUE_OF_FORMS.match(name, arguments.length);
      const value = SOURCE_VALUE_OF_FORMS.value(name, 'name');
      if (value === null) throw new TypeError('Name is null');
      if (typeof value !== 'string') throw new TypeError(`name must be a string, not ${typeof value}`);
      const found = ConfigSource.values().find((c) => c.#name === value);
      if (found === undefined)
        throw new IllegalArgumentError(`No enum constant org.apache.kafka.clients.admin.ConfigEntry.ConfigSource.${value}`);
      return found;
    }

    /**
     * The name of this constant, exactly as declared, Java's `Enum.name()`.
     *
     * @returns the name
     * @throws `TypeError` when it is given an argument: it only reads
     */
    name(): string {
      SOURCE_NAME_FORMS.match(undefined, arguments.length);
      return this.#name;
    }

    /**
     * The position of this constant in its enum declaration, Java's `Enum.ordinal()`.
     *
     * @returns the ordinal
     * @throws `TypeError` when it is given an argument: it only reads
     */
    ordinal(): number {
      SOURCE_ORDINAL_FORMS.match(undefined, arguments.length);
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

  /**
   * Class representing a configuration synonym of a `ConfigEntry`.
   *
   * Instances come from the admin client's `ConfigEntry`s.
   */
  export class ConfigSynonym {
    readonly #name: string | null;
    readonly #value: string | null;
    readonly #source: ConfigSource | null;

    private constructor(name: string | null, value: string | null, source: ConfigSource | null) {
      this.#name = name;
      this.#value = value;
      this.#source = source;
    }

    /**
     * Java's package-private constructor. Called by the admin client.
     *
     * @param name - Configuration name (this may be different from the name of the associated `ConfigEntry`)
     * @param value - Configuration value
     * @param source - `ConfigSource` of this configuration
     * @returns the synonym
     * @internal
     */
    static internalCreate(name: string | null, value: string | null, source: ConfigSource | null): ConfigSynonym {
      return new ConfigSynonym(name, value, source);
    }

    /**
     * Returns the name of this configuration.
     *
     * @returns the name
     * @throws `TypeError` when it is given an argument: it only reads
     */
    name(): string | null {
      if (arguments.length !== 0) NAME_FORMS.match(undefined, arguments.length);
      return this.#name;
    }

    /**
     * Returns the value of this configuration, which may be null if the
     * configuration is sensitive.
     *
     * @returns the value, or null
     * @throws `TypeError` when it is given an argument: it only reads
     */
    value(): string | null {
      if (arguments.length !== 0) VALUE_FORMS.match(undefined, arguments.length);
      return this.#value;
    }

    /**
     * Returns the source of this configuration.
     *
     * @returns the source
     * @throws `TypeError` when it is given an argument: it only reads
     */
    source(): ConfigSource | null {
      if (arguments.length !== 0) SOURCE_FORMS.match(undefined, arguments.length);
      return this.#source;
    }

    /**
     * Whether `o` is a ConfigSynonym with equal fields.
     *
     * @param o - the value to compare with, on its own or as `{ o }`
     * @returns whether they are equal
     */
    equals(o: unknown): boolean {
      EQUALS_FORMS.match(o, arguments.length);
      const other = EQUALS_FORMS.value(o, 'o');
      if (other === this) return true;
      if (!sameClass(this, other)) return false;
      const that = other as ConfigSynonym;
      return objectsEquals(this.#name, that.#name) && objectsEquals(this.#value, that.#value) && this.#source === that.#source;
    }

    /**
     * Java's `Objects.hash(name, value, source)`.
     *
     * @returns the hash code
     * @throws `TypeError` when it is given an argument: it only reads
     */
    hashCode(): number {
      if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
      let h = 1;
      for (const part of [objectHashCode(this.#name), objectHashCode(this.#value), objectHashCode(this.#source)]) {
        h = (Math.imul(31, h) + part) | 0;
      }
      return h;
    }

    /**
     * The synonym as JSON: its fields, the source by name.
     *
     * @returns the fields
     *
     * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
     * once released.
     */
    toJSON(): Record<string, unknown> {
      return { name: this.#name, value: this.#value, source: this.#source };
    }

    /**
     * Java's `toString()`: `ConfigSynonym(name=<n>, value=<v>, source=<s>)`.
     *
     * @returns the text
     */
    toString(): string {
      return `ConfigSynonym(name=${String(this.#name)}, value=${String(this.#value)}, source=${String(this.#source)})`;
    }
  }
}
