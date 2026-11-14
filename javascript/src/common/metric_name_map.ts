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
// keys() and entries() return MetricName objects. Read-only, as the map
// Java's metrics() returns is unmodifiable.

import { JavaForms } from '../internal/args';
import { MetricName } from './metric_name';

const FORMS = new JavaForms('MetricNameMap', [[], ['entries']], {
  bareTypes: { entries: (value) => typeof value === 'object' && value !== null },
});
const isMetricName = (value: unknown): boolean => value instanceof MetricName;
const GET_FORMS = new JavaForms('get', [['key']], { bareTypes: { key: isMetricName } });
const HAS_FORMS = new JavaForms('has', [['key']], { bareTypes: { key: isMetricName } });
const SIZE_FORMS = new JavaForms('size', [[]], { reader: true });

/** The key of `name`, which must be a MetricName. */
function keyOf(name: unknown): string {
  if (!(name instanceof MetricName)) throw new TypeError('key must be a MetricName');
  return name.internalKey();
}

/**
 * A read-only map keyed by `MetricName`, whose keys compare as
 * `MetricName.equals` does: by group, name and tags.
 *
 * @typeParam V - the type of the values
 *
 * @remarks *(js-only)* JavaScript has no value equality for map keys. It
 * does not extend `Map`, so `instanceof Map` is false.
 */
export class MetricNameMap<V> implements Iterable<[MetricName, V]> {
  readonly #entries = new Map<string, [MetricName, V]>();

  /**
   * Creates a map, empty or holding the given entries (a later entry with an
   * equal key replaces an earlier one).
   *
   * @param entries - the `[MetricName, value]` entries
   * @throws `IllegalArgumentError` when it is given more than one argument
   * @throws `TypeError` when an entry is not a `[MetricName, value]` pair
   */
  constructor(entries?: Iterable<readonly [MetricName, V]>);
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
    return this.#entries.size;
  }

  /**
   * The value of the entry whose key equals `key`.
   *
   * @param key - the metric name
   * @returns the value, or undefined when there is none
   * @throws `IllegalArgumentError` when the given parameters are not `{key}`
   * @throws `TypeError` when the key is not a MetricName
   */
  get(key: MetricName): V | undefined {
    GET_FORMS.match(key, arguments.length);
    const entry = this.#entries.get(keyOf(GET_FORMS.value(key, 'key')));
    return entry === undefined ? undefined : entry[1];
  }

  /**
   * Whether an entry's key equals `key`.
   *
   * @param key - the metric name
   * @returns true when there is such an entry
   * @throws `IllegalArgumentError` when the given parameters are not `{key}`
   * @throws `TypeError` when the key is not a MetricName
   */
  has(key: MetricName): boolean {
    HAS_FORMS.match(key, arguments.length);
    return this.#entries.has(keyOf(HAS_FORMS.value(key, 'key')));
  }

  /**
   * The keys, the original `MetricName` objects.
   *
   * @returns an iterator over the keys
   */
  keys(): IterableIterator<MetricName> {
    return [...this.#entries.values()].map(([name]) => name)[Symbol.iterator]();
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
   * The `[MetricName, value]` entries.
   *
   * @returns an iterator over the entries
   */
  entries(): IterableIterator<[MetricName, V]> {
    return [...this.#entries.values()].map(([name, value]): [MetricName, V] => [name, value])[Symbol.iterator]();
  }

  /**
   * Calls `callback` for each entry, as `Map.forEach` does.
   *
   * @param callback - called with the value, the key and the map
   */
  forEach(callback: (value: V, key: MetricName, map: MetricNameMap<V>) => void): void {
    for (const [name, value] of this.#entries.values()) callback(value, name, this);
  }

  /**
   * The entries, as `entries()` gives them.
   *
   * @returns an iterator over the entries
   */
  [Symbol.iterator](): IterableIterator<[MetricName, V]> {
    return this.entries();
  }
}
