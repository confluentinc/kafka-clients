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

// ConfigResourceMap (js-only): Java's Map<ConfigResource, V>, which JavaScript cannot key by
// value (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family, Q25; the
// admin client's value-keyed maps, design decision D2). It is built as
// TopicPartitionMap is: it wraps a Map keyed by the JSON text of the fields
// ConfigResource.equals compares (the type and the name; ConfigResource.internalKey), and keeps each original
// ConfigResource, so keys() and entries() return ConfigResource objects. A null key, which
// Java's HashMap holds, is the key 'null', which no ConfigResource gives (its key is
// a JSON array). A value is never undefined: null is Java's null. A ConfigResource
// is immutable, so its key never changes once stored.

import { JavaForms } from '../../internal/args';
import { javaString } from '../../internal/java_string';
import { ConfigResource } from './config_resource';

const FORMS = new JavaForms('ConfigResourceMap', [[], ['entries']], {
  bareTypes: { entries: (value) => typeof value === 'object' && value !== null },
});
const isKey = (value: unknown): boolean => value === null || value instanceof ConfigResource;
const GET_FORMS = new JavaForms('get', [['key']], { bareTypes: { key: isKey } });
const HAS_FORMS = new JavaForms('has', [['key']], { bareTypes: { key: isKey } });
const DELETE_FORMS = new JavaForms('delete', [['key']], { bareTypes: { key: isKey } });
const SIZE_FORMS = new JavaForms('size', [[]], { reader: true });

/**
 * `value`, which is never `undefined`: null is Java's null.
 *
 * @param value - the value given
 * @param what - the argument's name, for the error
 * @returns the value
 */
function valueOf<V>(value: unknown, what: string): V {
  if (value === undefined) throw new TypeError(`${what} must not be undefined: pass null for Java's null`);
  return value as V;
}

/** The string key of a null key: no ConfigResource's key (ConfigResource.internalKey, a JSON array). */
const NULL_KEY = 'null';

/**
 * The key of `tp`, which must be a ConfigResource or null.
 *
 * @param tp - the ConfigResource given
 * @returns its key
 */
function keyOf(tp: unknown): string {
  if (tp === null) return NULL_KEY;
  if (!(tp instanceof ConfigResource)) throw new TypeError('key must be a ConfigResource or null');
  return tp.internalKey();
}

/**
 * A map keyed by `ConfigResource`, whose keys compare as
 * `ConfigResource.equals` does: by their type and name.
 *
 * @typeParam V - the type of the values
 *
 * @remarks *(js-only)* JavaScript has no value equality for map keys. It
 * does not extend `Map`, so `instanceof Map` is false. A map the client
 * returns is a new one, which the caller may change. A `null` key is held,
 * as Java's `HashMap` holds one; the keys are typed `ConfigResource`, Java's
 * type.
 */
export class ConfigResourceMap<V> implements Iterable<[ConfigResource, V]> {
  readonly #entries = new Map<string, [ConfigResource | null, V]>();

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
   *   an entry is not a `[ConfigResource, value]` pair, or its value is
   *   `undefined`
   */
  constructor(options: {
    /** The `[ConfigResource, value]` entries, such as another `ConfigResourceMap`. */
    entries: Iterable<readonly [ConfigResource | null, V]>;
  });
  /**
   * Creates a map holding the given entries: the entries on their own, the
   * same as `new ConfigResourceMap({entries})`.
   *
   * @param entries - the `[ConfigResource, value]` entries, such as another `ConfigResourceMap`
   */
  constructor(entries: Iterable<readonly [ConfigResource | null, V]>);
  constructor(entries?: unknown) {
    const form = FORMS.match(entries, arguments.length);
    if (form === 0) return;
    const given = FORMS.value(entries, 'entries');
    if (typeof (given as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function') {
      throw new TypeError('entries must be an iterable of [ConfigResource, value] pairs');
    }
    for (const entry of given as Iterable<unknown>) {
      if (!Array.isArray(entry) || !isKey(entry[0])) {
        throw new TypeError('an entry must be a [ConfigResource, value] pair');
      }
      const tp = entry[0] as ConfigResource | null;
      this.#entries.set(keyOf(tp), [tp, valueOf(entry[1], "an entry's value")]);
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
   * @param options - the key
   * @returns the value, or null when there is none, as Java's `Map.get`
   *   returns
   * @throws `IllegalArgumentError` when the given parameters are not `{key}`
   * @throws `TypeError` when the key is neither a ConfigResource nor null
   */
  get(options: {
    /** The ConfigResource, or null. */
    key: ConfigResource | null;
  }): V | null;
  /**
   * The value of the entry whose key equals `key`: the key on its own, the same as
   * `get({key})`.
   *
   * @param key - the ConfigResource, or null
   * @returns the value, or null when there is none, as Java's `Map.get`
   *   returns
   */
  get(key: ConfigResource | null): V | null;
  get(key: unknown): V | null {
    GET_FORMS.match(key, arguments.length);
    const entry = this.#entries.get(keyOf(GET_FORMS.value(key, 'key')));
    return entry === undefined ? null : entry[1];
  }

  /**
   * Whether an entry's key equals `key`.
   *
   * @param options - the key
   * @returns true when there is such an entry
   * @throws `IllegalArgumentError` when the given parameters are not `{key}`
   * @throws `TypeError` when the key is neither a ConfigResource nor null
   */
  has(options: {
    /** The ConfigResource, or null. */
    key: ConfigResource | null;
  }): boolean;
  /**
   * Whether an entry's key equals `key`: the key on its own, the same as
   * `has({key})`.
   *
   * @param key - the ConfigResource, or null
   * @returns true when there is such an entry
   */
  has(key: ConfigResource | null): boolean;
  has(key: unknown): boolean {
    HAS_FORMS.match(key, arguments.length);
    return this.#entries.has(keyOf(HAS_FORMS.value(key, 'key')));
  }

  /**
   * Stores `value` under `key`, replacing the value of an entry whose key
   * equals it (the entry keeps its place and its original key, as Java's
   * `HashMap.put` keeps the key it holds).
   *
   * @param key - the ConfigResource, or null
   * @param value - the value
   * @returns this map
   * @throws `TypeError` when the key is neither a ConfigResource nor null,
   *   or the value is `undefined` or left out
   */
  set(key: ConfigResource | null, value: V): this {
    const k = keyOf(key);
    const v = valueOf<V>(value, 'value');
    const kept = this.#entries.get(k);
    this.#entries.set(k, [kept === undefined ? key : kept[0], v]);
    return this;
  }

  /**
   * Removes the entry whose key equals `key`.
   *
   * @param options - the key
   * @returns true when there was such an entry
   * @throws `IllegalArgumentError` when the given parameters are not `{key}`
   * @throws `TypeError` when the key is neither a ConfigResource nor null
   */
  delete(options: {
    /** The ConfigResource, or null. */
    key: ConfigResource | null;
  }): boolean;
  /**
   * Removes the entry whose key equals `key`: the key on its own, the same as
   * `delete({key})`.
   *
   * @param key - the ConfigResource, or null
   * @returns true when there was such an entry
   */
  delete(key: ConfigResource | null): boolean;
  delete(key: unknown): boolean {
    DELETE_FORMS.match(key, arguments.length);
    return this.#entries.delete(keyOf(DELETE_FORMS.value(key, 'key')));
  }

  /** Removes every entry. */
  clear(): void {
    this.#entries.clear();
  }

  /**
   * The keys, the original `ConfigResource` objects.
   *
   * @returns an iterator over the keys
   */
  keys(): IterableIterator<ConfigResource> {
    return [...this.#entries.values()].map(([tp]) => tp as ConfigResource)[Symbol.iterator]();
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
   * The `[ConfigResource, value]` entries.
   *
   * @returns an iterator over the entries
   */
  entries(): IterableIterator<[ConfigResource, V]> {
    return [...this.#entries.values()].map(([tp, value]): [ConfigResource, V] => [tp as ConfigResource, value])[Symbol.iterator]();
  }

  /**
   * Calls `callback` for each entry, as `Map.forEach` does.
   *
   * @param callback - called with the value, the key and the map
   */
  forEach(callback: (value: V, key: ConfigResource, map: ConfigResourceMap<V>) => void): void {
    for (const [tp, value] of [...this.#entries.values()]) callback(value, tp as ConfigResource, this);
  }

  /**
   * The map as Java's `AbstractMap.toString()` writes it:
   * `{orders-0=5, orders-1=7}`.
   *
   * @returns the map's text
   */
  toString(): string {
    const parts: string[] = [];
    for (const [tp, value] of this.#entries.values()) parts.push(`${javaString(tp)}=${javaString(value)}`);
    return `{${parts.join(', ')}}`;
  }

  /**
   * The map as `JSON.stringify` writes it: an object of its entries, each
   * keyed by the map's string key (the JSON array of the type's name and the name, `'null'` for a null key), in the order of the
   * entries (no key is integer-like), a `bigint` value written as a decimal string. It ignores the key
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
  [Symbol.iterator](): IterableIterator<[ConfigResource, V]> {
    return this.entries();
  }
}
