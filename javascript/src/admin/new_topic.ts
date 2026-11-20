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

// Java's org.apache.kafka.clients.admin.NewTopic, a value class that keeps
// Java's methods. Java's constructors (name, int, short) and (name,
// Optional<Integer>, Optional<Short>) take one form here, `null` standing for
// an empty Optional (javascript/CLAUDE.md, JavaScript Binding Conventions, Types:
// `Optional<T>` is `T | null`); (name, replicasAssignments) is the other.
// configs() is a getter and setter of one name (Q17), the setter of a config
// map, which also takes the bare map.

import { JavaForms, kind } from '../internal/args';
import { checkInt } from '../internal/java_number';
import { mapEquals, mapHashCode, objectHashCode, sameClass } from '../internal/java_object';
import { javaString } from '../internal/java_string';
import { readOnlyMap } from '../internal/read_only_map';

const FORMS = new JavaForms('NewTopic', [
  ['name', 'numPartitions', 'replicationFactor'],
  ['name', 'replicasAssignments'],
]);
const CONFIGS_FORMS = new JavaForms('configs', [[], ['configs']], {
  bareIsPlainObject: true,
  bareTypes: { configs: (value) => value === null || typeof value === 'object' },
});
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const NAME_FORMS = READER('name');
const NUM_PARTITIONS_FORMS = READER('numPartitions');
const REPLICATION_FACTOR_FORMS = READER('replicationFactor');
const REPLICAS_ASSIGNMENTS_FORMS = READER('replicasAssignments');
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = READER('hashCode');

// CreateTopicsRequest.NO_NUM_PARTITIONS and NO_REPLICATION_FACTOR.
const NO_NUM_PARTITIONS = -1;
const NO_REPLICATION_FACTOR = -1;

/**
 * A Java `short` or null (an empty Optional).
 *
 * @param value - the value given
 * @param name - the parameter's name
 * @returns the number, or null
 */
function optionalShort(value: unknown, name: string): number | null {
  if (value === null) return null;
  const n = checkInt(value, name);
  if (n < -32768 || n > 32767) throw new RangeError(`${name} must be from -32768 to 32767`);
  return n;
}

/**
 * A map keyed by strings (a `Map` or a plain object) of string values, as a
 * `Map`: a `Map` kept as given, a plain object read into a new one.
 *
 * @param value - the map given
 * @returns the map
 */
function stringMap(value: object): Map<string, string> {
  const entries: Array<[unknown, unknown]> = value instanceof Map ? [...(value as Map<unknown, unknown>)] : Object.entries(value);
  for (const [k, v] of entries) {
    if (typeof k !== 'string') throw new TypeError('configs must be keyed by strings');
    if (v !== null && typeof v !== 'string') throw new TypeError(`configs must map to strings, not ${kind(v)}`);
  }
  return value instanceof Map ? (value as Map<string, string>) : new Map(entries as Array<[string, string]>);
}

/**
 * A new topic to be created via `Admin.createTopics`.
 */
export class NewTopic {
  readonly #name: string | null;
  readonly #numPartitions: number | null;
  readonly #replicationFactor: number | null;
  readonly #replicasAssignments: ReadonlyMap<number, readonly number[]> | null;
  #configs: Map<string, string> | null = null;

  /**
   * A new topic with the specified replication factor and number of
   * partitions.
   *
   * @param options - the topic name, the number of partitions and the replication factor
   */
  constructor(options: {
    /** The topic name. */
    name: string;
    /** The number of partitions. */
    numPartitions: number;
    /** The replication factor, a Java `short`. */
    replicationFactor: number;
    replicasAssignments?: never;
  });
  /**
   * A new topic that optionally defaults `numPartitions` and
   * `replicationFactor` to the broker configurations for `num.partitions`
   * and `default.replication.factor` respectively.
   *
   * @param options - the topic name, the number of partitions and the replication factor
   */
  constructor(options: {
    /** The topic name. */
    name: string;
    /** The number of partitions, or null for the broker's `num.partitions`. */
    numPartitions: number | null;
    /** The replication factor, a Java `short`, or null for the broker's `default.replication.factor`. */
    replicationFactor: number | null;
    replicasAssignments?: never;
  });
  /**
   * A new topic with the specified replica assignment configuration.
   *
   * @param options - the topic name, and a map from partition id to replica
   *   ids (i.e. broker ids). Although not enforced, it is generally a good
   *   idea for all partitions to have the same number of replicas. The first
   *   replica will be treated as the preferred leader.
   */
  constructor(options: {
    /** The topic name. */
    name: string;
    /** A map from partition id to replica ids (i.e. broker ids). */
    replicasAssignments: ReadonlyMap<number, Iterable<number>>;
    numPartitions?: never;
    replicationFactor?: never;
  });
  /**
   * Builds the new topic.
   *
   * @param options - the fields of one of Java's forms
   * @throws `IllegalArgumentError` when the given parameters are no Java form
   * @throws `TypeError` for a value of the wrong type, or a null assignment map
   * @throws `RangeError` for a count outside Java's type
   */
  constructor(options: unknown) {
    const form = FORMS.match(options, arguments.length);
    const name = FORMS.value(options, 'name');
    if (name !== null && typeof name !== 'string') throw new TypeError(`name must be a string, not ${kind(name)}`);
    this.#name = name;
    if (form === 0) {
      const numPartitions = FORMS.value(options, 'numPartitions');
      this.#numPartitions = numPartitions === null ? null : checkInt(numPartitions, 'numPartitions');
      this.#replicationFactor = optionalShort(FORMS.value(options, 'replicationFactor'), 'replicationFactor');
      this.#replicasAssignments = null;
      return;
    }
    const assignments = FORMS.value(options, 'replicasAssignments');
    // Java's Collections.unmodifiableMap dereferences the map.
    if (assignments === null) throw new TypeError('replicasAssignments must not be null');
    if (!(assignments instanceof Map)) throw new TypeError(`replicasAssignments must be a Map, not ${kind(assignments)}`);
    const entries: Array<[number, readonly number[]]> = [];
    for (const [partition, brokers] of assignments as Map<unknown, unknown>) {
      if (typeof brokers !== 'object' || brokers === null || !(Symbol.iterator in brokers)) {
        throw new TypeError('replicasAssignments must map a partition to an iterable of broker ids');
      }
      entries.push([
        checkInt(partition, 'a partition id'),
        Object.freeze([...(brokers as Iterable<unknown>)].map((b) => checkInt(b, 'a broker id'))),
      ]);
    }
    this.#numPartitions = null;
    this.#replicationFactor = null;
    this.#replicasAssignments = readOnlyMap(entries);
  }

  /**
   * The name of the topic to be created.
   *
   * @returns the topic name
   * @throws `TypeError` when it is given an argument: it only reads
   */
  name(): string {
    if (arguments.length !== 0) NAME_FORMS.match(undefined, arguments.length);
    return this.#name as string;
  }

  /**
   * The number of partitions for the new topic or -1 if a replica assignment
   * has been specified.
   *
   * @returns the number of partitions, or -1
   * @throws `TypeError` when it is given an argument: it only reads
   */
  numPartitions(): number {
    if (arguments.length !== 0) NUM_PARTITIONS_FORMS.match(undefined, arguments.length);
    return this.#numPartitions ?? NO_NUM_PARTITIONS;
  }

  /**
   * The replication factor for the new topic or -1 if a replica assignment
   * has been specified.
   *
   * @returns the replication factor, or -1
   * @throws `TypeError` when it is given an argument: it only reads
   */
  replicationFactor(): number {
    if (arguments.length !== 0) REPLICATION_FACTOR_FORMS.match(undefined, arguments.length);
    return this.#replicationFactor ?? NO_REPLICATION_FACTOR;
  }

  /**
   * A map from partition id to replica ids (i.e. broker ids) or null if the
   * number of partitions and replication factor have been specified instead.
   *
   * @returns the read-only map, or null
   * @throws `TypeError` when it is given an argument: it only reads
   */
  replicasAssignments(): ReadonlyMap<number, readonly number[]> | null {
    if (arguments.length !== 0) REPLICAS_ASSIGNMENTS_FORMS.match(undefined, arguments.length);
    return this.#replicasAssignments;
  }

  /**
   * The configuration for the new topic or null if no configs ever specified.
   *
   * @returns the configuration map, or null
   */
  configs(): Map<string, string> | null;
  /**
   * Set the configuration to use on the new topic.
   *
   * @param configs - The configuration map, a `Map` or a plain object of its
   *   keys, on its own
   * @returns This NewTopic object.
   */
  configs(configs: ReadonlyMap<string, string> | Readonly<Record<string, string>>): this;
  /**
   * Set the configuration to use on the new topic, the map given by name.
   *
   * @param options - The configuration map.
   * @returns This NewTopic object.
   */
  configs(options: {
    /** The configuration map, a `Map` or a plain object, or null. */
    configs: ReadonlyMap<string, string> | Readonly<Record<string, string>> | null;
  }): this;
  /**
   * Reads or sets the configuration.
   *
   * @param options - nothing to read, or the configuration map
   * @returns the configuration, or this object
   * @throws `TypeError` when the map is not keyed by strings of string values
   */
  configs(options?: unknown): Map<string, string> | null | this {
    if (arguments.length === 0) return this.#configs;
    if (CONFIGS_FORMS.match(options, arguments.length) === 0) {
      throw new TypeError('configs() takes the value to set: leave the argument out to read it');
    }
    const configs = CONFIGS_FORMS.value(options, 'configs');
    this.#configs = configs === null ? null : stringMap(configs as object);
    return this;
  }

  /**
   * The topic's fields as the addon takes them: -1 for an empty Optional,
   * the assignments and the configs as pairs, or null. Called by the admin
   * clients.
   *
   * @returns the fields
   * @internal
   */
  internalToNative(): {
    name: string | null;
    numPartitions: number;
    replicationFactor: number;
    assignments: Array<[number, number[]]> | null;
    configs: Array<[string, string | null]> | null;
  } {
    return {
      name: this.#name,
      numPartitions: this.#numPartitions ?? NO_NUM_PARTITIONS,
      replicationFactor: this.#replicationFactor ?? NO_REPLICATION_FACTOR,
      assignments: this.#replicasAssignments === null ? null : [...this.#replicasAssignments].map(([p, b]) => [p, [...b]]),
      configs: this.#configs === null ? null : [...this.#configs],
    };
  }

  /**
   * The new topic as JSON: its fields, Java's sentinels for the defaults.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): Record<string, unknown> {
    const assignments = this.#replicasAssignments;
    return {
      name: this.#name,
      numPartitions: this.numPartitions(),
      replicationFactor: this.replicationFactor(),
      replicasAssignments: assignments === null ? null : Object.fromEntries([...assignments].map(([p, b]) => [String(p), [...b]])),
      configs: this.#configs === null ? null : Object.fromEntries(this.#configs),
    };
  }

  /**
   * Java's `toString()`: `(name=<n>, numPartitions=<n|default>,
   * replicationFactor=<n|default>, replicasAssignments=<map>, configs=<map>)`.
   *
   * @returns the text
   */
  toString(): string {
    return (
      `(name=${String(this.#name)}, numPartitions=${this.#numPartitions ?? 'default'}` +
      `, replicationFactor=${this.#replicationFactor ?? 'default'}` +
      `, replicasAssignments=${javaString(this.#replicasAssignments)}, configs=${javaString(this.#configs)})`
    );
  }

  /**
   * Whether `o` is a NewTopic with equal fields.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as NewTopic;
    return (
      this.#name === that.#name &&
      this.#numPartitions === that.#numPartitions &&
      this.#replicationFactor === that.#replicationFactor &&
      mapEquals(this.#replicasAssignments, that.#replicasAssignments) &&
      mapEquals(this.#configs, that.#configs)
    );
  }

  /**
   * Java's `Objects.hash(name, numPartitions, replicationFactor,
   * replicasAssignments, configs)`, an empty Optional hashing to 0.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    let h = 1;
    for (const part of [
      objectHashCode(this.#name),
      objectHashCode(this.#numPartitions),
      objectHashCode(this.#replicationFactor),
      mapHashCode(this.#replicasAssignments),
      mapHashCode(this.#configs),
    ])
      h = (Math.imul(31, h) + part) | 0;
    return h;
  }
}
