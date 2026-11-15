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

// The payloads of the generated error classes (private): Java's constructor
// parameters other than the message and the cause, and the fields its payload
// getters read (root CLAUDE.md, JavaScript Binding Conventions, Errors, Q39;
// Types, Q23 and Q25). Each Java type a payload has is a `Kind`: how the
// constructor takes a value of it (its run-time check, the copy Java's
// constructor makes or a collection read once), how a field of it is read
// from the plain value the native layer reads through the FFI's accessors
// (native/error.c), and the copy a getter returns ("A returned collection is
// a new object that the caller may change").
//
// | Java                     | taken                         | returned                  |
// |--------------------------|-------------------------------|---------------------------|
// | String                   | string or null                | string or null            |
// | int, double              | number                        | number                    |
// | long                     | bigint or a whole number      | bigint                    |
// | Object                   | any value                     | (no getter)               |
// | Set<String>              | an iterable of strings        | a new Set                 |
// | Collection<TopicPartition> | an iterable of TopicPartition, as given | (no getter) |
// | Set<TopicPartition>      | an iterable of TopicPartition | a new TopicPartitionSet   |
// | TopicPartition           | a TopicPartition              | (no getter)               |
// | Map<TopicPartition, Long>| a TopicPartitionMap           | a new TopicPartitionMap<bigint> |
// | KafkaMetric              | a KafkaMetric                 | the same KafkaMetric      |
// | ByteBuffer               | a Uint8Array, or null         | a Buffer over its memory  |
// | Headers                  | a Headers (kept), or an iterable of Header | the same Headers |
// | an enum                  | a constant, or null           | the constant              |
// | Map<TopicPartition, OffsetAndMetadata> | a TopicPartitionMap | a new TopicPartitionMap |

import { kind as kindOf } from './args';
import { checkInt, checkLong } from './java_number';
import { Header } from '../common/header/header';
import { Headers } from '../common/header/headers';
import { KafkaMetric } from '../common/metrics/kafka_metric';
import { OffsetAndMetadata } from '../consumer/offset_and_metadata';
import { TopicPartition } from '../common/topic_partition';
import { TopicPartitionMap } from '../common/topic_partition_map';
import { TopicPartitionSet } from '../common/topic_partition_set';

/** How a payload of one Java type is taken, read from the core and returned. */
export interface Kind {
  /** A constructor argument named `name`, checked and copied as Java's constructor keeps it. */
  take(value: unknown, name: string): unknown;
  /** A field's value from the plain value native/error.c reads through the FFI. */
  core(raw: unknown): unknown;
  /** What a getter returns for the stored value: a copy of a collection. */
  copy(stored: unknown): unknown;
}

const same = (value: unknown): unknown => value;

/** Java's `String`: a string or null. */
export const string: Kind = {
  take(value, name) {
    if (value !== null && typeof value !== 'string') throw new TypeError(`${name} must be a string or null`);
    return value;
  },
  core: same,
  copy: same,
};

/** Java's `int`: a whole number in Java's range. */
export const int: Kind = { take: (value, name) => checkInt(value, name), core: same, copy: same };

/** Java's `long`: a bigint, or a whole number up to 2^53 (root CLAUDE.md, Types, Q23). */
export const long: Kind = { take: (value, name) => checkLong(value, name), core: same, copy: same };

/** Java's `double`: a number. */
export const double: Kind = {
  take(value, name) {
    if (typeof value !== 'number') throw new TypeError(`${name} must be a number`);
    return value;
  },
  core: same,
  copy: same,
};

/** Java's `Object`: any value, as it is. */
export const object: Kind = { take: same, core: same, copy: same };

/**
 * An iterable argument read once, as Java's copy reads its collection: null
 * stays null, a string is refused (Java cannot take one for a
 * `Collection<String>`, and JavaScript would iterate its characters).
 *
 * @param value the argument given
 * @param name the argument's name, for the error
 * @param what what the elements are, for the error
 * @returns its elements, or null
 */
function iterableOf(value: unknown, name: string, what: string): unknown[] | null {
  if (value === null) return null;
  if (typeof value === 'string') throw new TypeError(`${name} must be an iterable of ${what}, not string`);
  if (typeof value !== 'object' || typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function') {
    throw new TypeError(`${name} must be an iterable of ${what}`);
  }
  return [...(value as Iterable<unknown>)];
}

/**
 * The elements of a collection argument, each one `test` accepts or null; an
 * `undefined` element throws, as Java has no such value (root CLAUDE.md,
 * JavaScript Binding Conventions, Signatures, Q18).
 *
 * @param items the elements
 * @param name the argument's name, for the error
 * @param what what the elements are, for the error
 * @param test whether an element has the right type
 */
function checkElements(items: readonly unknown[], name: string, what: string, test: (x: unknown) => boolean): void {
  for (const x of items) {
    if (x === undefined) throw new TypeError(`${name} must not hold undefined: pass null for Java's null`);
    if (x !== null && !test(x)) throw new TypeError(`${name} must hold only ${what} or null, not ${kindOf(x)}`);
  }
}

/** Java's `Set<String>` (or `Collection<String>`): an iterable of strings, kept as a new `Set`. */
export const stringSet: Kind = {
  take(value, name) {
    const items = iterableOf(value, name, 'strings');
    if (items === null) return null;
    checkElements(items, name, 'strings', (x) => typeof x === 'string');
    return new Set(items);
  },
  core: (raw) => new Set(raw as Iterable<unknown>),
  copy: (stored) => (stored === null ? null : new Set(stored as Set<unknown>)),
};

const isTopicPartition = (x: unknown): boolean => x instanceof TopicPartition;

/**
 * A `TopicPartitionSet` of `items`, each a TopicPartition or null.
 *
 * @param items the elements
 * @param name the argument's name, for the error
 * @returns the set
 */
function topicPartitionSetOf(items: readonly unknown[], name: string): TopicPartitionSet {
  checkElements(items, name, 'TopicPartition', isTopicPartition);
  return new TopicPartitionSet(items as Array<TopicPartition | null>);
}

/**
 * The `TopicPartition` the native layer reads as `[topic, partition]`.
 *
 * @param raw what the native layer gave
 * @returns the partition
 */
function coreTopicPartition(raw: unknown): TopicPartition {
  const [topic, partition] = raw as [string | null, number];
  return new TopicPartition({ topic, partition });
}

/** Java's `Collection<TopicPartition>` or `Set<TopicPartition>`: an iterable of TopicPartition, kept as a `TopicPartitionSet`. */
export const topicPartitions: Kind = {
  take(value, name) {
    const items = iterableOf(value, name, 'TopicPartition');
    return items === null ? null : topicPartitionSetOf(items, name);
  },
  core: (raw) => new TopicPartitionSet((raw as unknown[]).map(coreTopicPartition)),
  copy: (stored) => (stored === null ? null : new TopicPartitionSet(stored as TopicPartitionSet)),
};

/**
 * Java's `Collection<TopicPartition>` taken: an iterable of TopicPartition
 * (or null elements), read once into an array, as given, so a message Java
 * builds from it prints it whole, duplicates included.
 */
export const topicPartitionList: Kind = {
  take(value, name) {
    const items = iterableOf(value, name, 'TopicPartition');
    if (items === null) return null;
    checkElements(items, name, 'TopicPartition', isTopicPartition);
    return items;
  },
  core: same,
  copy: same,
};

/**
 * Java's `Set.copyOf(x)` of a collection of TopicPartition: a
 * `TopicPartitionSet`. Java dereferences `x` and refuses a null element:
 * `TypeError` "<name> must not be null" / "<name> must not contain null".
 *
 * @param x the collection given
 * @param name the argument's name, for the error
 * @returns the set
 */
export function topicPartitionSetCopyOf(x: unknown, name: string): TopicPartitionSet {
  if (x === null) throw new TypeError(`${name} must not be null`);
  const items = x as unknown[];
  if (items.includes(null)) throw new TypeError(`${name} must not contain null`);
  return topicPartitionSetOf(items, name);
}

/** Java's `TopicPartition`: a TopicPartition or null. */
export const topicPartition: Kind = {
  take(value, name) {
    if (value !== null && !(value instanceof TopicPartition)) throw new TypeError(`${name} must be a TopicPartition`);
    return value;
  },
  core: (raw) => (raw === null ? null : coreTopicPartition(raw)),
  copy: same,
};

/**
 * Java's `Map<TopicPartition, Long>`: a `TopicPartitionMap` only, whose
 * values are each a bigint or a whole number (or null), kept as a new
 * `TopicPartitionMap<bigint>`; a `Map` or a plain object is refused with a
 * hint (root CLAUDE.md, JavaScript Binding Conventions, Types, Q25).
 */
export const topicPartitionLongMap: Kind = {
  take(value, name) {
    if (value === null) return null;
    if (!(value instanceof TopicPartitionMap)) {
      throw new TypeError(
        `${name} must be a TopicPartitionMap, not ${value instanceof Map ? 'a Map' : typeof value}: a Map keyed by TopicPartition compares ` +
          'its keys by identity, so build one with new TopicPartitionMap(entries)',
      );
    }
    const copy = new TopicPartitionMap<bigint | null>();
    for (const [tp, v] of value as TopicPartitionMap<unknown>) copy.set(tp, v === null ? null : checkLong(v, `a value of ${name}`));
    return copy;
  },
  core(raw) {
    if (raw === null) return null;
    const map = new TopicPartitionMap<bigint>();
    for (const [topic, partition, offset] of raw as Array<[string | null, number, bigint]>) map.set(new TopicPartition({ topic, partition }), offset);
    return map;
  },
  copy: (stored) => (stored === null ? null : new TopicPartitionMap(stored as TopicPartitionMap<unknown>)),
};

/** Java's `KafkaMetric`: a KafkaMetric or null. */
export const kafkaMetric: Kind = {
  take(value, name) {
    if (value !== null && !(value instanceof KafkaMetric)) throw new TypeError(`${name} must be a KafkaMetric`);
    return value;
  },
  core: same,
  copy: same,
};

/**
 * Java's `Collections.singleton(x)` of a TopicPartition, as a
 * `TopicPartitionSet`: a null `x` is its one member, as Java's set holds it.
 *
 * @param x the partition, or null
 * @returns the set
 */
export function singleton(x: unknown): TopicPartitionSet {
  return new TopicPartitionSet([x as TopicPartition | null]);
}

/**
 * A value Java dereferences (`Set.copyOf(x)`, `Collections.unmodifiableMap(x)`,
 * `x.keySet()`): `TypeError` "<name> must not be null" for null, as Java's
 * NullPointerException (root CLAUDE.md, JavaScript Binding Conventions,
 * Errors, Q36).
 *
 * @param x the value given
 * @param name the argument's name, for the error
 * @returns the value
 */
export function nonNull<T>(x: T | null, name: string): T {
  if (x === null) throw new TypeError(`${name} must not be null`);
  return x;
}

/**
 * The check of a payload getter, a method that only reads: an argument is a
 * `TypeError` (root CLAUDE.md, JavaScript Binding Conventions, Class family,
 * Q17).
 *
 * @param method the method's name, for the error
 * @param count the number of arguments given
 */
export function readOnly(method: string, count: number): void {
  if (count > 0) throw new TypeError(`${method}() only reads: it takes no argument`);
}

/** Java's `ByteBuffer`: a `Uint8Array` or null, kept as a `Buffer` over the same memory, never copied. */
export const bytes: Kind = {
  take(value, name) {
    if (value === null) return null;
    if (!(value instanceof Uint8Array)) throw new TypeError(`${name} must be a Uint8Array or null`);
    return Buffer.isBuffer(value) ? value : Buffer.from(value.buffer, value.byteOffset, value.byteLength);
  },
  core: same,
  copy: same,
};

/**
 * Java's `Headers`: a `Headers`, kept as it is, as Java keeps the object it
 * is given; any other iterable of `Header` as a new `Headers`; null stays
 * null.
 */
export const headers: Kind = {
  take(value, name) {
    if (value === null || value instanceof Headers) return value;
    if (typeof value !== 'object' || typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function') {
      throw new TypeError(`${name} must be a Headers or an iterable of Header`);
    }
    return new Headers({ headers: value as Iterable<Header> });
  },
  core: (raw) =>
    raw === null
      ? null
      : new Headers({ headers: (raw as Array<[string, Buffer | null]>).map(([key, value]) => new Header({ key, value })) }),
  copy: same,
};

/** An enum class: its constants, in declaration order (`values()`). */
interface EnumClass {
  readonly name: string;
  values(): unknown[];
}

/**
 * An enum class's kind: a constant of it or null; the core gives its ordinal.
 *
 * @param cls the enum class
 * @returns its kind
 */
export function enumOf(cls: EnumClass): Kind {
  return {
    take(value, name) {
      if (value !== null && !cls.values().includes(value)) throw new TypeError(`${name} must be a ${cls.name}`);
      return value;
    },
    core: (raw) => (raw === null ? null : cls.values()[raw as number]),
    copy: same,
  };
}

/**
 * Java's `Map<TopicPartition, OffsetAndMetadata>`: a `TopicPartitionMap`
 * only, whose values are each an `OffsetAndMetadata` (or null), kept as a new
 * map.
 */
export const topicPartitionOffsetMap: Kind = {
  take(value, name) {
    if (value === null) return null;
    if (!(value instanceof TopicPartitionMap)) {
      throw new TypeError(
        `${name} must be a TopicPartitionMap, not ${value instanceof Map ? 'a Map' : typeof value}: a Map keyed by TopicPartition compares ` +
          'its keys by identity, so build one with new TopicPartitionMap(entries)',
      );
    }
    const copy = new TopicPartitionMap<OffsetAndMetadata | null>();
    for (const [tp, v] of value as TopicPartitionMap<unknown>) {
      if (v !== null && !(v instanceof OffsetAndMetadata)) throw new TypeError(`a value of ${name} must be an OffsetAndMetadata`);
      copy.set(tp, v);
    }
    return copy;
  },
  core(raw) {
    if (raw === null) return null;
    const map = new TopicPartitionMap<OffsetAndMetadata>();
    for (const [topic, partition, offset, leaderEpoch, metadata] of raw as Array<[string | null, number, bigint, number, string]>) {
      map.set(new TopicPartition({ topic, partition }), new OffsetAndMetadata({ offset, leaderEpoch, metadata }));
    }
    return map;
  },
  copy: (stored) => (stored === null ? null : new TopicPartitionMap(stored as TopicPartitionMap<unknown>)),
};
