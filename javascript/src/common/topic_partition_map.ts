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

// TopicPartitionMap (js-only): Java's Map<TopicPartition, V>, which JavaScript
// cannot key by value (root CLAUDE.md, JavaScript Binding Conventions, Class
// family, Q25). It wraps a Map keyed by the string `<topic>:<partition>`
// (TopicPartition.internalKey), such as 'orders:3', and keeps each original
// TopicPartition, so keys() and entries() return TopicPartition objects. The
// partition is an integer, so the text after the last ':' is always the
// partition and two topic partitions never share a key; a null topic's key is
// the partition alone, which has no ':'. A TopicPartition is frozen, so its
// key never changes once stored.

import { JavaForms } from '../internal/args';
import { javaString } from '../internal/java_string';
import { TopicPartition } from './topic_partition';

const FORMS = new JavaForms('TopicPartitionMap', [[], ['entries']], {
  bareTypes: { entries: (value) => typeof value === 'object' && value !== null },
});
const isTopicPartition = (value: unknown): boolean => value instanceof TopicPartition;
const GET_FORMS = new JavaForms('get', [['key']], { bareTypes: { key: isTopicPartition } });
const HAS_FORMS = new JavaForms('has', [['key']], { bareTypes: { key: isTopicPartition } });
const DELETE_FORMS = new JavaForms('delete', [['key']], { bareTypes: { key: isTopicPartition } });
const SIZE_FORMS = new JavaForms('size', [[]], { reader: true });

/** The key of `tp`, which must be a TopicPartition. */
function keyOf(tp: unknown): string {
  if (!(tp instanceof TopicPartition)) throw new TypeError('key must be a TopicPartition');
  return tp.internalKey();
}

/**
 * A map keyed by `TopicPartition`, whose keys compare as
 * `TopicPartition.equals` does: by topic and partition.
 *
 * @typeParam V - the type of the values
 *
 * @remarks *(js-only)* JavaScript has no value equality for map keys. It
 * does not extend `Map`, so `instanceof Map` is false. A map the client
 * returns is a new one, which the caller may change.
 */
export class TopicPartitionMap<V> implements Iterable<[TopicPartition, V]> {
  readonly #entries = new Map<string, [TopicPartition, V]>();

  /**
   * Creates an empty map.
   *
   * @throws `IllegalArgumentError` when it is given more than one argument
   */
  constructor();
  /**
   * Creates a map holding the given entries (a later entry with an equal key
   * replaces an earlier one).
   *
   * @param entries - the `[TopicPartition, value]` entries, such as another
   *   `TopicPartitionMap`
   * @throws `IllegalArgumentError` when it is given more than one argument
   * @throws `TypeError` when `entries` is `undefined` or not an iterable, or
   *   an entry is not a `[TopicPartition, value]` pair
   */
  constructor(entries: Iterable<readonly [TopicPartition, V]>);
  constructor(entries?: unknown) {
    const form = FORMS.match(entries, arguments.length);
    if (form === 0) return;
    const given = FORMS.value(entries, 'entries');
    if (typeof (given as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function') {
      throw new TypeError('entries must be an iterable of [TopicPartition, value] pairs');
    }
    for (const entry of given as Iterable<unknown>) {
      if (!Array.isArray(entry) || !(entry[0] instanceof TopicPartition)) {
        throw new TypeError('an entry must be a [TopicPartition, value] pair');
      }
      const tp: TopicPartition = entry[0];
      this.#entries.set(tp.internalKey(), [tp, entry[1] as V]);
    }
  }

  /**
   * The number of entries, Java's `Map.size()`.
   *
   * @returns the number of entries
   * @throws `TypeError` when it is given an argument
   */
  size(): number {
    SIZE_FORMS.match(undefined, arguments.length);
    return this.#entries.size;
  }

  /**
   * The value of the entry whose key equals `key`.
   *
   * @param key - the topic partition
   * @returns the value, or null when there is none, as Java's `Map.get`
   *   returns
   * @throws `IllegalArgumentError` when the given parameters are not `{key}`
   * @throws `TypeError` when the key is not a TopicPartition
   */
  get(key: TopicPartition): V | null {
    GET_FORMS.match(key, arguments.length);
    const entry = this.#entries.get(keyOf(GET_FORMS.value(key, 'key')));
    return entry === undefined ? null : entry[1];
  }

  /**
   * Whether an entry's key equals `key`.
   *
   * @param key - the topic partition
   * @returns true when there is such an entry
   * @throws `IllegalArgumentError` when the given parameters are not `{key}`
   * @throws `TypeError` when the key is not a TopicPartition
   */
  has(key: TopicPartition): boolean {
    HAS_FORMS.match(key, arguments.length);
    return this.#entries.has(keyOf(HAS_FORMS.value(key, 'key')));
  }

  /**
   * Stores `value` under `key`, replacing the value of an entry whose key
   * equals it (the entry keeps its place and its original key, as Java's
   * `HashMap.put` keeps the key it holds).
   *
   * @param key - the topic partition
   * @param value - the value
   * @returns this map
   * @throws `TypeError` when the key is not a TopicPartition
   */
  set(key: TopicPartition, value: V): this {
    const k = keyOf(key);
    const kept = this.#entries.get(k);
    this.#entries.set(k, [kept === undefined ? key : kept[0], value]);
    return this;
  }

  /**
   * Removes the entry whose key equals `key`.
   *
   * @param key - the topic partition
   * @returns true when there was such an entry
   * @throws `IllegalArgumentError` when the given parameters are not `{key}`
   * @throws `TypeError` when the key is not a TopicPartition
   */
  delete(key: TopicPartition): boolean {
    DELETE_FORMS.match(key, arguments.length);
    return this.#entries.delete(keyOf(DELETE_FORMS.value(key, 'key')));
  }

  /** Removes every entry. */
  clear(): void {
    this.#entries.clear();
  }

  /**
   * The keys, the original `TopicPartition` objects.
   *
   * @returns an iterator over the keys
   */
  keys(): IterableIterator<TopicPartition> {
    return [...this.#entries.values()].map(([tp]) => tp)[Symbol.iterator]();
  }

  /**
   * The values.
   *
   * @returns an iterator over the values
   */
  values(): IterableIterator<V> {
    return [...this.#entries.values()].map(([, value]) => value)[Symbol.iterator]();
  }

  /**
   * The `[TopicPartition, value]` entries.
   *
   * @returns an iterator over the entries
   */
  entries(): IterableIterator<[TopicPartition, V]> {
    return [...this.#entries.values()].map(([tp, value]): [TopicPartition, V] => [tp, value])[Symbol.iterator]();
  }

  /**
   * Calls `callback` for each entry, as `Map.forEach` does.
   *
   * @param callback - called with the value, the key and the map
   */
  forEach(callback: (value: V, key: TopicPartition, map: TopicPartitionMap<V>) => void): void {
    for (const [tp, value] of [...this.#entries.values()]) callback(value, tp, this);
  }

  /**
   * The map as Java's `AbstractMap.toString()` writes it:
   * `{orders-0=5, orders-1=7}`.
   *
   * @returns the map's text
   */
  toString(): string {
    const parts: string[] = [];
    for (const [tp, value] of this.#entries.values()) parts.push(`${tp.toString()}=${javaString(value)}`);
    return `{${parts.join(', ')}}`;
  }

  /**
   * The map as `JSON.stringify` writes it: an object of its entries, each
   * keyed by the map's string key (`'orders:3'`), in the order of the
   * entries, a `bigint` value written as a decimal string. It ignores the key
   * `JSON.stringify` passes.
   *
   * @returns the entries, by the map's string key
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): Record<string, unknown> {
    const out: Record<string, unknown> = {};
    for (const [key, [, value]] of this.#entries) out[key] = typeof value === 'bigint' ? value.toString() : value;
    return out;
  }

  /**
   * The entries, as `entries()` gives them.
   *
   * @returns an iterator over the entries
   */
  [Symbol.iterator](): IterableIterator<[TopicPartition, V]> {
    return this.entries();
  }
}
