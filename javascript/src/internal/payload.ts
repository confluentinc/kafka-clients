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
// | Collection<TopicPartition>, Set<TopicPartition> | an iterable of TopicPartition | a new TopicPartitionSet |
// | TopicPartition           | a TopicPartition              | (no getter)               |
// | Map<TopicPartition, Long>| a TopicPartitionMap           | a new TopicPartitionMap<bigint> |
// | KafkaMetric              | a KafkaMetric                 | the same KafkaMetric      |

import { checkInt, checkLong } from './java_number';
import { KafkaMetric } from '../common/metrics/kafka_metric';
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
 */
function iterableOf(value: unknown, name: string, what: string): unknown[] | null {
  if (value === null) return null;
  if (typeof value === 'string') throw new TypeError(`${name} must be an iterable of ${what}, not string`);
  if (typeof value !== 'object' || typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function') {
    throw new TypeError(`${name} must be an iterable of ${what}`);
  }
  return [...(value as Iterable<unknown>)];
}

/** Java's `Set<String>` (or `Collection<String>`): an iterable of strings, kept as a new `Set`. */
export const stringSet: Kind = {
  take(value, name) {
    const items = iterableOf(value, name, 'strings');
    return items === null ? null : new Set(items);
  },
  core: (raw) => new Set(raw as Iterable<unknown>),
  copy: (stored) => (stored === null ? null : new Set(stored as Set<unknown>)),
};

/** A `TopicPartitionSet` of `items`, each a TopicPartition (a null one is `"<name> must not contain null"`). */
function topicPartitionSetOf(items: readonly unknown[], name: string): TopicPartitionSet {
  for (const tp of items) {
    if (tp === null || tp === undefined) throw new TypeError(`${name} must not contain null`);
    if (!(tp instanceof TopicPartition)) throw new TypeError(`${name} must be an iterable of TopicPartition`);
  }
  return new TopicPartitionSet(items as TopicPartition[]);
}

/** The `TopicPartition` the native layer reads as `[topic, partition]`. */
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
 * `TopicPartitionSet`.
 *
 * @throws `TypeError` when `x` is null: a `TopicPartitionSet` holds
 *   TopicPartitions only
 */
export function singleton(x: unknown, name: string): TopicPartitionSet {
  if (x === null) throw new TypeError(`${name} must not be null`);
  return topicPartitionSetOf([x], name);
}

/**
 * A value Java dereferences (`Set.copyOf(x)`, `Collections.unmodifiableMap(x)`,
 * `x.keySet()`): `TypeError` "<name> must not be null" for null, as Java's
 * NullPointerException (root CLAUDE.md, JavaScript Binding Conventions,
 * Errors, Q36).
 */
export function nonNull<T>(x: T | null, name: string): T {
  if (x === null) throw new TypeError(`${name} must not be null`);
  return x;
}

/**
 * The check of a payload getter, a method that only reads: an argument is a
 * `TypeError` (root CLAUDE.md, JavaScript Binding Conventions, Class family,
 * Q17).
 */
export function readOnly(method: string, count: number): void {
  if (count > 0) throw new TypeError(`${method}() only reads: it takes no argument`);
}
