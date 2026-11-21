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

// Java's org.apache.kafka.clients.admin.PartitionReassignment, a value class
// that keeps Java's methods: a partition's reassignment, as listed.

import { JavaForms, kind } from '../internal/args';
import { checkInt } from '../internal/java_number';
import { javaString } from '../internal/java_string';

const FORMS = new JavaForms('PartitionReassignment', [['replicas', 'addingReplicas', 'removingReplicas']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const REPLICAS_FORMS = READER('replicas');
const ADDING_REPLICAS_FORMS = READER('addingReplicas');
const REMOVING_REPLICAS_FORMS = READER('removingReplicas');

/**
 * A list of broker ids, Java's `Collections.unmodifiableList`, which
 * dereferences the list but not its elements.
 *
 * @param value - the list given
 * @param name - the parameter's name
 * @returns the broker ids, frozen
 */
function brokerIds(value: unknown, name: string): readonly number[] {
  if (value === null) throw new TypeError(`${name} must not be null`);
  if (
    typeof value === 'string' ||
    typeof value !== 'object' ||
    typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function'
  ) {
    throw new TypeError(`${name} must be an iterable of numbers, not ${kind(value)}`);
  }
  // A null element is kept, as Java's list keeps it.
  return Object.freeze(
    [...(value as Iterable<unknown>)].map((id) => (id === null ? (null as unknown as number) : checkInt(id, `an element of ${name}`))),
  );
}

/**
 * A partition reassignment, which has been listed via
 * `Admin.listPartitionReassignments()`.
 */
export class PartitionReassignment {
  readonly #replicas: readonly number[];
  readonly #addingReplicas: readonly number[];
  readonly #removingReplicas: readonly number[];

  /**
   * A partition's reassignment.
   *
   * @param options - the replicas, the ones being added and the ones being
   *   removed, each any iterable, copied
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{replicas, addingReplicas, removingReplicas}`
   * @throws `TypeError` for a null list or a value of the wrong type
   */
  constructor(options: {
    /** The brokers which this partition currently resides on. */
    replicas: Iterable<number>;
    /** The brokers that we are adding this partition to as part of a reassignment. */
    addingReplicas: Iterable<number>;
    /** The brokers that we are removing this partition from as part of a reassignment. */
    removingReplicas: Iterable<number>;
  }) {
    FORMS.match(options, arguments.length);
    this.#replicas = brokerIds(FORMS.value(options, 'replicas'), 'replicas');
    this.#addingReplicas = brokerIds(FORMS.value(options, 'addingReplicas'), 'addingReplicas');
    this.#removingReplicas = brokerIds(FORMS.value(options, 'removingReplicas'), 'removingReplicas');
  }

  /**
   * The brokers which this partition currently resides on.
   *
   * @returns the broker ids, a new array
   * @throws `TypeError` when it is given an argument: it only reads
   */
  replicas(): number[] {
    if (arguments.length !== 0) REPLICAS_FORMS.match(undefined, arguments.length);
    return [...this.#replicas];
  }

  /**
   * The brokers that we are adding this partition to as part of a
   * reassignment. A subset of replicas.
   *
   * @returns the broker ids, a new array
   * @throws `TypeError` when it is given an argument: it only reads
   */
  addingReplicas(): number[] {
    if (arguments.length !== 0) ADDING_REPLICAS_FORMS.match(undefined, arguments.length);
    return [...this.#addingReplicas];
  }

  /**
   * The brokers that we are removing this partition from as part of a
   * reassignment. A subset of replicas.
   *
   * @returns the broker ids, a new array
   * @throws `TypeError` when it is given an argument: it only reads
   */
  removingReplicas(): number[] {
    if (arguments.length !== 0) REMOVING_REPLICAS_FORMS.match(undefined, arguments.length);
    return [...this.#removingReplicas];
  }

  /**
   * Java's `toString()`:
   * `PartitionReassignment(replicas=[..], addingReplicas=[..], removingReplicas=[..])`.
   *
   * @returns the text
   */
  toString(): string {
    return (
      `PartitionReassignment(replicas=${javaString(this.#replicas)}, addingReplicas=${javaString(this.#addingReplicas)}, ` +
      `removingReplicas=${javaString(this.#removingReplicas)})`
    );
  }

  /**
   * The reassignment as `JSON.stringify` writes it: `{ replicas,
   * addingReplicas, removingReplicas }`.
   *
   * @returns the reassignment's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { replicas: number[]; addingReplicas: number[]; removingReplicas: number[] } {
    return { replicas: [...this.#replicas], addingReplicas: [...this.#addingReplicas], removingReplicas: [...this.#removingReplicas] };
  }
}
