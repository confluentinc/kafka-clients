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

// A constant Java `Set` (private): a read-only set (root CLAUDE.md,
// JavaScript Binding Conventions, Names), as Java's `Set.of` or
// `Collections.unmodifiableSet` is. Reading works as on any `Set` (`has`,
// `size`, iteration in Java's order); `add`, `delete` and `clear` throw
// `UnsupportedOperationError`, as Java's unmodifiable set throws
// `UnsupportedOperationException`. The generated config classes build their
// `Set` constants with `readOnlySet`.

import { create } from './throwable';
import { UnsupportedOperationError } from '../unsupported_operation_error';

/** A `Set` whose `add`, `delete` and `clear` throw. */
class ReadOnlySet<T> extends Set<T> {
  /**
   * The set of `items`, in their order.
   *
   * @param items - the members
   */
  constructor(items: Iterable<T>) {
    super();
    // Set's own add: this class's throws.
    for (const item of items) Set.prototype.add.call(this, item);
  }

  /** Throws `UnsupportedOperationError`: the set is read-only. */
  override add(value: T): this {
    void value;
    throw create(UnsupportedOperationError, '');
  }

  /** Throws `UnsupportedOperationError`: the set is read-only. */
  override delete(value: T): boolean {
    void value;
    throw create(UnsupportedOperationError, '');
  }

  /** Throws `UnsupportedOperationError`: the set is read-only. */
  override clear(): void {
    throw create(UnsupportedOperationError, '');
  }
}

/**
 * A read-only set of `items`, in their order.
 *
 * @param items - the members
 * @returns the set, typed `ReadonlySet`
 */
export function readOnlySet<T>(items: Iterable<T>): ReadonlySet<T> {
  return Object.freeze(new ReadOnlySet(items));
}
