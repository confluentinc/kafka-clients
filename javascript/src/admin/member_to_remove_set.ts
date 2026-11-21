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
// MemberToRemoveSet (js-only): Java's Set<MemberToRemove>, the set form of the
// value-keyed maps (root CLAUDE.md, JavaScript Binding Conventions, Class family
// and Types, Q25; the admin client's value-keyed maps, decided by the owner on
// 2026-11-20). It wraps a Map keyed by MemberToRemove.internalKey, the group
// instance id Java's equals compares, and keeps each original MemberToRemove,
// so its members compare by value, as Java's HashSet compares them. A null
// member, which Java's HashSet holds, is the key 'null', which no member gives
// (its key is a JSON array).

import { JavaForms } from '../internal/args';
import { MemberToRemove } from './member_to_remove';

const FORMS = new JavaForms('MemberToRemoveSet', [[], ['values']], {
  bareTypes: { values: (value) => typeof value === 'object' && value !== null },
});
const isMember = (value: unknown): boolean => value === null || value instanceof MemberToRemove;
const HAS_FORMS = new JavaForms('has', [['value']], { bareTypes: { value: isMember } });
const ADD_FORMS = new JavaForms('add', [['value']], { bareTypes: { value: isMember } });
const DELETE_FORMS = new JavaForms('delete', [['value']], { bareTypes: { value: isMember } });
const SIZE_FORMS = new JavaForms('size', [[]], { reader: true });

/** The string key of a null member: no MemberToRemove's key (MemberToRemove.internalKey). */
const NULL_KEY = 'null';

/**
 * The key of `tp`, which must be a MemberToRemove or null.
 *
 * @param tp - the partition given
 * @returns its key
 */
function keyOf(tp: unknown): string {
  if (tp === null) return NULL_KEY;
  if (!(tp instanceof MemberToRemove)) throw new TypeError('value must be a MemberToRemove or null');
  return tp.internalKey();
}

/**
 * A set of `MemberToRemove`s, whose members compare as
 * `MemberToRemove.equals` does: by group instance id.
 *
 * @remarks *(js-only)* JavaScript has no value equality for set members. It
 * does not extend `Set`, so `instanceof Set` is false. A set the client
 * returns is a new one, which the caller may change. A `null` member is
 * held, as Java's `HashSet` holds one; the members are typed
 * `MemberToRemove`, Java's type.
 */
export class MemberToRemoveSet implements Iterable<MemberToRemove> {
  readonly #members = new Map<string, MemberToRemove | null>();

  /**
   * Creates an empty set.
   *
   * @throws `IllegalArgumentError` when it is given more than one argument
   */
  constructor();
  /**
   * Creates a set holding the given members (an equal one later is
   * the same member).
   *
   * @param options - the members
   * @throws `IllegalArgumentError` when it is given more than one argument
   * @throws `TypeError` when `values` is `undefined` or not an iterable, or a
   *   member is not a MemberToRemove
   */
  constructor(options: {
    /** The members, such as an array or another `MemberToRemoveSet`. */
    values: Iterable<MemberToRemove | null>;
  });
  /**
   * Creates a set holding the given members: the values on their
   * own, the same as `new MemberToRemoveSet({values})`.
   *
   * @param values - the members, such as an array or another `MemberToRemoveSet`
   */
  constructor(values: Iterable<MemberToRemove | null>);
  constructor(values?: unknown) {
    const form = FORMS.match(values, arguments.length);
    if (form === 0) return;
    const given = FORMS.value(values, 'values');
    if (typeof (given as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function') {
      throw new TypeError('values must be an iterable of MemberToRemove');
    }
    for (const tp of given as Iterable<unknown>) {
      const key = keyOf(tp);
      if (!this.#members.has(key)) this.#members.set(key, tp as MemberToRemove | null);
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
   * @param options - the value
   * @returns true when there is such a member
   * @throws `IllegalArgumentError` when the given parameters are not `{value}`
   * @throws `TypeError` when the value is neither a MemberToRemove nor null
   */
  has(options: {
    /** The member, or null. */
    value: MemberToRemove | null;
  }): boolean;
  /**
   * Whether a member equals `value`: the value on its own, the same as
   * `has({value})`.
   *
   * @param value - the member, or null
   * @returns true when there is such a member
   */
  has(value: MemberToRemove | null): boolean;
  has(value: unknown): boolean {
    HAS_FORMS.match(value, arguments.length);
    return this.#members.has(keyOf(HAS_FORMS.value(value, 'value')));
  }

  /**
   * Adds `value`, unless a member equals it (that member is kept, as Java's
   * `HashSet.add` keeps it).
   *
   * @param options - the value
   * @returns this set
   * @throws `IllegalArgumentError` when the given parameters are not `{value}`
   * @throws `TypeError` when the value is neither a MemberToRemove nor null
   */
  add(options: {
    /** The member, or null. */
    value: MemberToRemove | null;
  }): this;
  /**
   * Adds `value`, unless a member equals it (that member is kept, as Java's `HashSet.add` keeps it): the value on its
   * own, the same as `add({value})`.
   *
   * @param value - the member, or null
   * @returns this set
   */
  add(value: MemberToRemove | null): this;
  add(value: unknown): this {
    ADD_FORMS.match(value, arguments.length);
    const tp = ADD_FORMS.value(value, 'value');
    const key = keyOf(tp);
    if (!this.#members.has(key)) this.#members.set(key, tp as MemberToRemove | null);
    return this;
  }

  /**
   * Removes the member that equals `value`.
   *
   * @param options - the value
   * @returns true when there was such a member
   * @throws `IllegalArgumentError` when the given parameters are not `{value}`
   * @throws `TypeError` when the value is neither a MemberToRemove nor null
   */
  delete(options: {
    /** The member, or null. */
    value: MemberToRemove | null;
  }): boolean;
  /**
   * Removes the member that equals `value`: the value on its own, the same as
   * `delete({value})`.
   *
   * @param value - the member, or null
   * @returns true when there was such a member
   */
  delete(value: MemberToRemove | null): boolean;
  delete(value: unknown): boolean {
    DELETE_FORMS.match(value, arguments.length);
    return this.#members.delete(keyOf(DELETE_FORMS.value(value, 'value')));
  }

  /** Removes every member. */
  clear(): void {
    this.#members.clear();
  }

  /**
   * The members, the original `MemberToRemove` objects.
   *
   * @returns an iterator over the members
   */
  values(): IterableIterator<MemberToRemove> {
    return ([...this.#members.values()] as MemberToRemove[])[Symbol.iterator]();
  }

  /**
   * Calls `callback` for each member, as `Set.forEach` does.
   *
   * @param callback - called with the member, the member again and the set
   */
  forEach(callback: (value: MemberToRemove, key: MemberToRemove, set: MemberToRemoveSet) => void): void {
    for (const tp of [...this.#members.values()] as MemberToRemove[]) callback(tp, tp, this);
  }

  /**
   * The set as Java's `AbstractCollection.toString()` writes it, each member
   * as Java's `Object.toString()`, which MemberToRemove does not override:
   * `[org.apache.kafka.clients.admin.MemberToRemove@a1b2c3, null]`.
   *
   * @returns the set's text
   */
  toString(): string {
    return `[${[...this.#members.values()].map((m) => (m === null ? 'null' : m.internalObjectString())).join(', ')}]`;
  }

  /**
   * The set as `JSON.stringify` writes it: an array of its members, each
   * `{ groupInstanceId }`. It ignores the key `JSON.stringify` passes.
   *
   * @returns the members
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): Array<MemberToRemove | null> {
    return [...this.#members.values()];
  }

  /**
   * The members, as `values()` gives them.
   *
   * @returns an iterator over the members
   */
  [Symbol.iterator](): IterableIterator<MemberToRemove> {
    return this.values();
  }
}
