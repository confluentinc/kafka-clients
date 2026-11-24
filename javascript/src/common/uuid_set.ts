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

// UuidSet (js-only): Java's Set<Uuid>, the set form of UuidMap
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family), built
// as TopicPartitionSet is. It wraps a Map keyed by Uuid.internalKey,
// the base64 text of the two halves Uuid.equals compares, and keeps each
// original Uuid, so its members compare by value, as Java's sets compare them
// with equals. A null member, which Java's HashSet holds, is the key 'null',
// which no Uuid gives (its text has 22 characters). `Uuid.RESERVED`, Java's
// `Set.of(ZERO_UUID, ONE_UUID)`, is a read-only one: its writes throw, as
// Java's unmodifiable set's do.

import { JavaForms } from '../internal/args';
import { create } from '../internal/throwable';
import { UnsupportedOperationError } from '../unsupported_operation_error';
import { Uuid } from './uuid';

const FORMS = new JavaForms('UuidSet', [[], ['values']], {
  bareTypes: { values: (value) => typeof value === 'object' && value !== null },
});
const isMember = (value: unknown): boolean => value === null || value instanceof Uuid;
const HAS_FORMS = new JavaForms('has', [['value']], { bareTypes: { value: isMember } });
const ADD_FORMS = new JavaForms('add', [['value']], { bareTypes: { value: isMember } });
const DELETE_FORMS = new JavaForms('delete', [['value']], { bareTypes: { value: isMember } });
const SIZE_FORMS = new JavaForms('size', [[]], { reader: true });

/** The string key of a null member: no Uuid's key (Uuid.internalKey). */
const NULL_KEY = 'null';

/**
 * The key of `id`, which must be a Uuid or null.
 *
 * @param id - the Uuid given
 * @returns its key
 */
function keyOf(id: unknown): string {
  if (id === null) return NULL_KEY;
  if (!(id instanceof Uuid)) throw new TypeError('value must be a Uuid or null');
  return id.internalKey();
}

/**
 * A set of `Uuid`s, whose members compare as `Uuid.equals` does: by their
 * two halves.
 *
 * @remarks *(js-only)* JavaScript has no value equality for set members. It
 * does not extend `Set`, so `instanceof Set` is false. A `null` member is
 * held, as Java's `HashSet` holds one; the members are typed `Uuid`, Java's
 * type. `Uuid.RESERVED` is a read-only one, as Java's constant set is: its
 * `add`, `delete` and `clear` throw `UnsupportedOperationError`.
 */
export class UuidSet implements Iterable<Uuid> {
  readonly #members = new Map<string, Uuid | null>();

  // A read-only set (Uuid.RESERVED), as Java's unmodifiable set: the writes
  // throw.
  #readOnly = false;

  /**
   * Creates an empty set.
   *
   * @throws `IllegalArgumentError` when it is given more than one argument
   */
  constructor();
  /**
   * Creates a set holding the given Uuids (an equal one later is the same
   * member).
   *
   * @param options - the Uuids
   * @throws `IllegalArgumentError` when it is given more than one argument
   * @throws `TypeError` when `values` is `undefined` or not an iterable, or a
   *   member is not a Uuid
   */
  constructor(options: {
    /** The Uuids, such as an array or another `UuidSet`. */
    values: Iterable<Uuid | null>;
  });
  /**
   * Creates a set holding the given Uuids: the values on their own, the same
   * as `new UuidSet({values})`.
   *
   * @param values - the Uuids, such as an array or another `UuidSet`
   */
  constructor(values: Iterable<Uuid | null>);
  constructor(values?: unknown) {
    const form = FORMS.match(values, arguments.length);
    if (form === 0) return;
    const given = FORMS.value(values, 'values');
    if (typeof (given as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function') {
      throw new TypeError('values must be an iterable of Uuid');
    }
    for (const id of given as Iterable<unknown>) {
      const key = keyOf(id);
      if (!this.#members.has(key)) this.#members.set(key, id as Uuid | null);
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
   * @throws `TypeError` when the value is neither a Uuid nor null
   */
  has(options: {
    /** The Uuid, or null. */
    value: Uuid | null;
  }): boolean;
  /**
   * Whether a member equals `value`: the value on its own, the same as
   * `has({value})`.
   *
   * @param value - the Uuid, or null
   * @returns true when there is such a member
   */
  has(value: Uuid | null): boolean;
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
   * @throws `TypeError` when the value is neither a Uuid nor null
   * @throws `UnsupportedOperationError` when the set is read-only
   */
  add(options: {
    /** The Uuid, or null. */
    value: Uuid | null;
  }): this;
  /**
   * Adds `value`, unless a member equals it (that member is kept, as Java's
   * `HashSet.add` keeps it): the value on its own, the same as
   * `add({value})`.
   *
   * @param value - the Uuid, or null
   * @returns this set
   */
  add(value: Uuid | null): this;
  add(value: unknown): this {
    ADD_FORMS.match(value, arguments.length);
    this.#checkWritable();
    const id = ADD_FORMS.value(value, 'value');
    const key = keyOf(id);
    if (!this.#members.has(key)) this.#members.set(key, id as Uuid | null);
    return this;
  }

  /**
   * Removes the member that equals `value`.
   *
   * @param options - the value
   * @returns true when there was such a member
   * @throws `IllegalArgumentError` when the given parameters are not `{value}`
   * @throws `TypeError` when the value is neither a Uuid nor null
   * @throws `UnsupportedOperationError` when the set is read-only
   */
  delete(options: {
    /** The Uuid, or null. */
    value: Uuid | null;
  }): boolean;
  /**
   * Removes the member that equals `value`: the value on its own, the same as
   * `delete({value})`.
   *
   * @param value - the Uuid, or null
   * @returns true when there was such a member
   */
  delete(value: Uuid | null): boolean;
  delete(value: unknown): boolean {
    DELETE_FORMS.match(value, arguments.length);
    this.#checkWritable();
    return this.#members.delete(keyOf(DELETE_FORMS.value(value, 'value')));
  }

  /**
   * Removes every member.
   *
   * @throws `UnsupportedOperationError` when the set is read-only, as Java's
   *   unmodifiable set throws
   */
  clear(): void {
    this.#checkWritable();
    this.#members.clear();
  }

  /**
   * Makes the set read-only, as Java's constant set is. Called by `Uuid`
   * for `RESERVED`.
   *
   * @returns this set
   * @internal
   */
  internalSetReadOnly(): this {
    this.#readOnly = true;
    return this;
  }

  /** Throws `UnsupportedOperationError` when the set is read-only. */
  #checkWritable(): void {
    if (this.#readOnly) throw create(UnsupportedOperationError, '');
  }

  /**
   * The members, the original `Uuid` objects.
   *
   * @returns an iterator over the members
   */
  values(): IterableIterator<Uuid> {
    return ([...this.#members.values()] as Uuid[])[Symbol.iterator]();
  }

  /**
   * Calls `callback` for each member, as `Set.forEach` does.
   *
   * @param callback - called with the member, the member again and the set
   */
  forEach(callback: (value: Uuid, key: Uuid, set: UuidSet) => void): void {
    for (const id of [...this.#members.values()] as Uuid[]) callback(id, id, this);
  }

  /**
   * The set as Java's `AbstractCollection.toString()` writes it:
   * `[AAAAAAAAAAAAAAAAAAAAAA, AAAAAAAAAAAAAAAAAAAAAQ]`.
   *
   * @returns the set's text
   */
  toString(): string {
    return `[${[...this.#members.values()].map((id) => (id === null ? 'null' : id.toString())).join(', ')}]`;
  }

  /**
   * The set as `JSON.stringify` writes it: an array of its members, each its
   * base64 text. It ignores the key `JSON.stringify` passes.
   *
   * @returns the members
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): Array<Uuid | null> {
    return [...this.#members.values()];
  }

  /**
   * The members, as `values()` gives them.
   *
   * @returns an iterator over the members
   */
  [Symbol.iterator](): IterableIterator<Uuid> {
    return this.values();
  }
}
