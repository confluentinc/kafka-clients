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

// TopicPartitionSet (js-only): the Set<TopicPartition> the client returns,
// the set form of TopicPartitionMap (root CLAUDE.md, JavaScript Binding
// Conventions, Class family and Types, Q25). It wraps a Map keyed by
// TopicPartition.internalKey and keeps each original TopicPartition, so its
// members compare by value, as Java's HashSet compares them with equals. A
// null member, which Java's HashSet holds, is the key 'null', which no
// TopicPartition gives.

import { JavaForms } from '../internal/args';
import { TopicPartition } from './topic_partition';

const FORMS = new JavaForms('TopicPartitionSet', [[], ['values']], {
  bareTypes: { values: (value) => typeof value === 'object' && value !== null },
});
const isMember = (value: unknown): boolean => value === null || value instanceof TopicPartition;
const HAS_FORMS = new JavaForms('has', [['value']], { bareTypes: { value: isMember } });
const ADD_FORMS = new JavaForms('add', [['value']], { bareTypes: { value: isMember } });
const DELETE_FORMS = new JavaForms('delete', [['value']], { bareTypes: { value: isMember } });
const SIZE_FORMS = new JavaForms('size', [[]], { reader: true });

/** The string key of a null member: no TopicPartition's key (TopicPartition.internalKey). */
const NULL_KEY = 'null';

/** The key of `tp`, which must be a TopicPartition or null. */
function keyOf(tp: unknown): string {
  if (tp === null) return NULL_KEY;
  if (!(tp instanceof TopicPartition)) throw new TypeError('value must be a TopicPartition or null');
  return tp.internalKey();
}

/**
 * A set of `TopicPartition`s, whose members compare as
 * `TopicPartition.equals` does: by topic and partition.
 *
 * @remarks *(js-only)* JavaScript has no value equality for set members. It
 * does not extend `Set`, so `instanceof Set` is false. A set the client
 * returns is a new one, which the caller may change. A `null` member is
 * held, as Java's `HashSet` holds one; the members are typed
 * `TopicPartition`, Java's type.
 */
export class TopicPartitionSet implements Iterable<TopicPartition> {
  readonly #members = new Map<string, TopicPartition | null>();

  /**
   * Creates an empty set.
   *
   * @throws `IllegalArgumentError` when it is given more than one argument
   */
  constructor();
  /**
   * Creates a set holding the given topic partitions (an equal one later is
   * the same member).
   *
   * @param values - the topic partitions, such as an array or another
   *   `TopicPartitionSet`
   * @throws `IllegalArgumentError` when it is given more than one argument
   * @throws `TypeError` when `values` is `undefined` or not an iterable, or a
   *   member is not a TopicPartition
   */
  constructor(values: Iterable<TopicPartition | null>);
  constructor(values?: unknown) {
    const form = FORMS.match(values, arguments.length);
    if (form === 0) return;
    const given = FORMS.value(values, 'values');
    if (typeof (given as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function') {
      throw new TypeError('values must be an iterable of TopicPartition');
    }
    for (const tp of given as Iterable<unknown>) {
      const key = keyOf(tp);
      if (!this.#members.has(key)) this.#members.set(key, tp as TopicPartition | null);
    }
  }

  /**
   * The number of members, Java's `Set.size()`.
   *
   * @returns the number of members
   * @throws `TypeError` when it is given an argument
   */
  size(): number {
    SIZE_FORMS.match(undefined, arguments.length);
    return this.#members.size;
  }

  /**
   * Whether a member equals `value`.
   *
   * @param value - the topic partition, or null
   * @returns true when there is such a member
   * @throws `IllegalArgumentError` when the given parameters are not `{value}`
   * @throws `TypeError` when the value is neither a TopicPartition nor null
   */
  has(value: TopicPartition | null): boolean {
    HAS_FORMS.match(value, arguments.length);
    return this.#members.has(keyOf(HAS_FORMS.value(value, 'value')));
  }

  /**
   * Adds `value`, unless a member equals it (that member is kept, as Java's
   * `HashSet.add` keeps it).
   *
   * @param value - the topic partition, or null
   * @returns this set
   * @throws `IllegalArgumentError` when the given parameters are not `{value}`
   * @throws `TypeError` when the value is neither a TopicPartition nor null
   */
  add(value: TopicPartition | null): this {
    ADD_FORMS.match(value, arguments.length);
    const tp = ADD_FORMS.value(value, 'value');
    const key = keyOf(tp);
    if (!this.#members.has(key)) this.#members.set(key, tp as TopicPartition | null);
    return this;
  }

  /**
   * Removes the member that equals `value`.
   *
   * @param value - the topic partition, or null
   * @returns true when there was such a member
   * @throws `IllegalArgumentError` when the given parameters are not `{value}`
   * @throws `TypeError` when the value is neither a TopicPartition nor null
   */
  delete(value: TopicPartition | null): boolean {
    DELETE_FORMS.match(value, arguments.length);
    return this.#members.delete(keyOf(DELETE_FORMS.value(value, 'value')));
  }

  /** Removes every member. */
  clear(): void {
    this.#members.clear();
  }

  /**
   * The members, the original `TopicPartition` objects.
   *
   * @returns an iterator over the members
   */
  values(): IterableIterator<TopicPartition> {
    return ([...this.#members.values()] as TopicPartition[])[Symbol.iterator]();
  }

  /**
   * Calls `callback` for each member, as `Set.forEach` does.
   *
   * @param callback - called with the member, the member again and the set
   */
  forEach(callback: (value: TopicPartition, key: TopicPartition, set: TopicPartitionSet) => void): void {
    for (const tp of [...this.#members.values()] as TopicPartition[]) callback(tp, tp, this);
  }

  /**
   * The set as Java's `AbstractCollection.toString()` writes it:
   * `[orders-0, orders-1]`.
   *
   * @returns the set's text
   */
  toString(): string {
    return `[${[...this.#members.values()].map((tp) => (tp === null ? 'null' : tp.toString())).join(', ')}]`;
  }

  /**
   * The set as `JSON.stringify` writes it: an array of its members, each
   * `{ topic, partition }`. It ignores the key `JSON.stringify` passes.
   *
   * @returns the members
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): Array<TopicPartition | null> {
    return [...this.#members.values()];
  }

  /**
   * The members, as `values()` gives them.
   *
   * @returns an iterator over the members
   */
  [Symbol.iterator](): IterableIterator<TopicPartition> {
    return this.values();
  }
}
