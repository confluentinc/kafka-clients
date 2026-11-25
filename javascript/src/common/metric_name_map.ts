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

// MetricNameMap (js-only): Java's Map<MetricName, V>, which JavaScript cannot
// key by value. It wraps a Map keyed by the string of a MetricName's group,
// name and tags sorted by key (JSON.stringify, MetricName.internalKey), the
// fields MetricName.equals compares, and keeps each original MetricName, so
// keys() and entries() return MetricName objects. A key is computed when its
// entry is stored. Read-only, as the map
// Java's metrics() returns is unmodifiable. A client's metrics() map is live,
// as Java's: it reads a source the library gives it (`internalLive`) at each
// call, every iteration taking the source's entries as they are then.

import { JavaForms } from '../internal/args';
import { MetricName } from './metric_name';

const FORMS = new JavaForms('MetricNameMap', [[], ['entries']], {
  bareTypes: { entries: (value) => typeof value === 'object' && value !== null },
});
const isMetricName = (value: unknown): boolean => value instanceof MetricName;
const GET_FORMS = new JavaForms('get', [['key']], { bareTypes: { key: isMetricName } });
const HAS_FORMS = new JavaForms('has', [['key']], { bareTypes: { key: isMetricName } });
const SIZE_FORMS = new JavaForms('size', [[]], { reader: true });

/**
 * The key of `name`, which must be a MetricName.
 *
 * @param name - the name given
 * @returns its key
 */
function keyOf(name: unknown): string {
  return checkName(name).internalKey();
}

/**
 * `name`, which must be a MetricName.
 *
 * @param name - the name given
 * @returns the name
 */
function checkName(name: unknown): MetricName {
  if (!(name instanceof MetricName)) throw new TypeError('key must be a MetricName');
  return name;
}

/** What a live map reads at each call (`MetricNameMap.internalLive`). */
interface LiveSource<V> {
  size(): number;
  has(key: MetricName): boolean;
  get(key: MetricName): V | null;
  // The entries as they are now, each key the source's own MetricName.
  entries(): Array<[MetricName, V]>;
}

/**
 * A read-only map keyed by `MetricName`, whose keys compare as
 * `MetricName.equals` does: by group, name and tags.
 *
 * @typeParam V - the type of the values
 *
 * @remarks *(js-only)* JavaScript has no value equality for map keys. It
 * does not extend `Map`, so `instanceof Map` is false.
 *
 * A key is computed when its entry is stored: a later change to the name's
 * tags makes that entry unreachable by an equal new name, as a mutated key
 * does in Java's `HashMap`.
 */
export class MetricNameMap<V> implements Iterable<[MetricName, V]> {
  readonly #entries = new Map<string, [MetricName, V]>();

  // A live map's source, read at each call instead of the entries; null for
  // a map built from entries.
  #source: LiveSource<V> | null = null;

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
   * @param options - the entries
   * @throws `IllegalArgumentError` when it is given more than one argument
   * @throws `TypeError` when `entries` is `undefined` or not an iterable, or
   *   an entry is not a `[MetricName, value]` pair
   */
  constructor(options: {
    /** The `[MetricName, value]` entries. */
    entries: Iterable<readonly [MetricName, V]>;
  });
  /**
   * Creates a map holding the given entries: the entries on their own, the
   * same as `new MetricNameMap({entries})`.
   *
   * @param entries - the `[MetricName, value]` entries
   */
  constructor(entries: Iterable<readonly [MetricName, V]>);
  constructor(entries?: unknown) {
    const form = FORMS.match(entries, arguments.length);
    if (form === 0) return;
    const given = FORMS.value(entries, 'entries');
    if (typeof (given as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function') {
      throw new TypeError('entries must be an iterable of [MetricName, value] pairs');
    }
    for (const entry of given as Iterable<unknown>) {
      if (!Array.isArray(entry) || !(entry[0] instanceof MetricName)) throw new TypeError('an entry must be a [MetricName, value] pair');
      const name: MetricName = entry[0];
      this.#entries.set(name.internalKey(), [name, entry[1] as V]);
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
    if (this.#source !== null) return this.#source.size();
    return this.#entries.size;
  }

  /**
   * The value of the entry whose key equals `key`.
   *
   * @param options - the key
   * @returns the value, or null when there is none, as Java's `Map.get`
   *   returns
   * @throws `IllegalArgumentError` when the given parameters are not `{key}`
   * @throws `TypeError` when the key is not a MetricName
   */
  get(options: {
    /** The metric name. */
    key: MetricName;
  }): V | null;
  /**
   * The value of the entry whose key equals `key`: the key on its own, the same as
   * `get({key})`.
   *
   * @param key - the metric name
   * @returns the value, or null when there is none, as Java's `Map.get`
   *   returns
   */
  get(key: MetricName): V | null;
  get(key: unknown): V | null {
    GET_FORMS.match(key, arguments.length);
    if (this.#source !== null) return this.#source.get(checkName(GET_FORMS.value(key, 'key')));
    const entry = this.#entries.get(keyOf(GET_FORMS.value(key, 'key')));
    return entry === undefined ? null : entry[1];
  }

  /**
   * Whether an entry's key equals `key`.
   *
   * @param options - the key
   * @returns true when there is such an entry
   * @throws `IllegalArgumentError` when the given parameters are not `{key}`
   * @throws `TypeError` when the key is not a MetricName
   */
  has(options: {
    /** The metric name. */
    key: MetricName;
  }): boolean;
  /**
   * Whether an entry's key equals `key`: the key on its own, the same as
   * `has({key})`.
   *
   * @param key - the metric name
   * @returns true when there is such an entry
   */
  has(key: MetricName): boolean;
  has(key: unknown): boolean {
    HAS_FORMS.match(key, arguments.length);
    if (this.#source !== null) return this.#source.has(checkName(HAS_FORMS.value(key, 'key')));
    return this.#entries.has(keyOf(HAS_FORMS.value(key, 'key')));
  }

  /**
   * The keys, the original `MetricName` objects.
   *
   * @returns an iterator over the keys
   */
  keys(): IterableIterator<MetricName> {
    return this.#current()
      .map(([name]) => name)
      [Symbol.iterator]();
  }

  /**
   * The values.
   *
   * @returns an iterator over the values
   */
  values(): IterableIterator<V> {
    return this.#current()
      .map(([, value]) => value)
      [Symbol.iterator]();
  }

  /**
   * The `[MetricName, value]` entries.
   *
   * @returns an iterator over the entries
   */
  entries(): IterableIterator<[MetricName, V]> {
    return this.#current()
      .map(([name, value]): [MetricName, V] => [name, value])
      [Symbol.iterator]();
  }

  /**
   * Calls `callback` for each entry, as `Map.forEach` does.
   *
   * @param callback - called with the value, the key and the map
   */
  forEach(callback: (value: V, key: MetricName, map: MetricNameMap<V>) => void): void {
    for (const [name, value] of this.#current()) callback(value, name, this);
  }

  /**
   * The map as `JSON.stringify` writes it: an object of its entries, each
   * keyed by the map's string key (the JSON text of the group, the name and
   * the tags sorted by key), in the order of the entries. It ignores the key
   * `JSON.stringify` passes.
   *
   * @returns the entries, by the map's string key
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released, as `TopicPartitionMap`'s is.
   */
  toJSON(): Record<string, V> {
    const out: Record<string, V> = {};
    if (this.#source !== null) {
      for (const [name, value] of this.#source.entries()) out[name.internalKey()] = value;
      return out;
    }
    for (const [key, [, value]] of this.#entries) out[key] = value;
    return out;
  }

  /**
   * The entries, as `entries()` gives them.
   *
   * @returns an iterator over the entries
   */
  [Symbol.iterator](): IterableIterator<[MetricName, V]> {
    return this.entries();
  }

  // The entries now: the source's, read at this call, or the stored ones.
  #current(): Array<[MetricName, V]> {
    if (this.#source !== null) return this.#source.entries();
    return [...this.#entries.values()];
  }

  /**
   * A live map: each call reads `source` as it is then, as Java's
   * `metrics()` is an unmodifiable live view. Called by the clients.
   *
   * @param source - what the map reads: its size, its keys, its values, its
   *   entries
   * @returns the map
   *
   * @internal
   */
  static internalLive<V>(source: LiveSource<V>): MetricNameMap<V> {
    const map = new MetricNameMap<V>();
    map.#source = source;
    return map;
  }
}
