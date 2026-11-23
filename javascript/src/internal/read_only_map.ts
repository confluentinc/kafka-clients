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

// A read-only Map (private), as Java's `Collections.unmodifiableMap` is:
// reading works as on any Map; `set`, `delete` and `clear` throw
// `UnsupportedOperationError`, as Java's unmodifiable map throws
// `UnsupportedOperationException`.

import { create } from './throwable';
import { UnsupportedOperationError } from '../unsupported_operation_error';

/** A `Map` whose `set`, `delete` and `clear` throw. */
class ReadOnlyMap<K, V> extends Map<K, V> {
  /**
   * The map of `entries`, in their order.
   *
   * @param entries - the entries
   */
  constructor(entries: Iterable<readonly [K, V]>) {
    super();
    for (const [k, v] of entries) Map.prototype.set.call(this, k, v);
  }

  /**
   * Throws `UnsupportedOperationError`: the map is read-only.
   *
   * @param key - the key not set
   * @param value - the value not set
   */
  override set(key: K, value: V): this {
    void key;
    void value;
    throw create(UnsupportedOperationError, '');
  }

  /**
   * Throws `UnsupportedOperationError`: the map is read-only.
   *
   * @param key - the key not deleted
   */
  override delete(key: K): boolean {
    void key;
    throw create(UnsupportedOperationError, '');
  }

  /** Throws `UnsupportedOperationError`: the map is read-only. */
  override clear(): void {
    throw create(UnsupportedOperationError, '');
  }
}

/**
 * A read-only map of `entries`, in their order.
 *
 * @param entries - the entries
 * @returns the map
 */
export function readOnlyMap<K, V>(entries: Iterable<readonly [K, V]>): ReadonlyMap<K, V> {
  return new ReadOnlyMap(entries);
}
