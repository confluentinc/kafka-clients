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

// Java's org.apache.kafka.clients.admin.NewPartitions: its constructor is
// private, so its static factories build it (one method for Java's two
// increaseTo forms).

import { JavaForms, kind } from '../internal/args';
import { checkInt } from '../internal/java_number';
import { javaString } from '../internal/java_string';

const isIterable = (value: unknown): boolean =>
  value !== null && typeof value === 'object' && typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] === 'function';
const INCREASE_TO_FORMS = new JavaForms('increaseTo', [['totalCount'], ['totalCount', 'newAssignments']], {
  bareTypes: { totalCount: (value) => typeof value === 'number' },
});
const TOTAL_COUNT_FORMS = new JavaForms('totalCount', [[]], { reader: true });
const ASSIGNMENTS_FORMS = new JavaForms('assignments', [[]], { reader: true });

/**
 * Describes new partitions for a particular topic in a call to
 * `Admin.createPartitions`.
 *
 * Instances come from `NewPartitions.increaseTo`.
 */
export class NewPartitions {
  readonly #totalCount: number;
  readonly #newAssignments: readonly (readonly number[])[] | null;

  private constructor(totalCount: number, newAssignments: readonly (readonly number[])[] | null) {
    this.#totalCount = totalCount;
    this.#newAssignments = newAssignments;
  }

  /**
   * Increase the partition count for a topic to the given `totalCount`. The
   * assignment of new replicas to brokers will be decided by the broker.
   *
   * @param totalCount - The total number of partitions after the operation succeeds.
   * @returns the new partitions
   * @throws `IllegalArgumentError` when the given parameters are no Java form
   * @throws `TypeError` or `RangeError` when the count is not a Java `int`
   */
  static increaseTo(totalCount: number): NewPartitions;
  /**
   * The same as `increaseTo(totalCount)`, the count given by name.
   *
   * @param options - the total number of partitions after the operation succeeds
   * @returns the new partitions
   * @throws `IllegalArgumentError` when the given parameters are no Java form
   * @throws `TypeError` or `RangeError` when the count is not a Java `int`
   */
  static increaseTo(options: {
    /** The total number of partitions after the operation succeeds. */
    totalCount: number;
    newAssignments?: never;
  }): NewPartitions;
  /**
   * Increase the partition count for a topic to the given `totalCount`
   * assigning the new partitions according to the given `newAssignments`.
   *
   * The length of the given `newAssignments` should equal
   * `totalCount - oldCount`, since the assignment of existing partitions are
   * not changed. Each inner list of `newAssignments` should have a length
   * equal to the topic's replication factor. The first broker id in each
   * inner list is the "preferred replica".
   *
   * For example, suppose a topic currently has a replication factor of 2,
   * and has 3 partitions. The number of partitions can be increased to 6
   * using `NewPartitions.increaseTo({ totalCount: 6, newAssignments: [[1, 2], [2, 3], [3, 1]] })`.
   * In this example partition 3's preferred leader will be broker 1,
   * partition 4's preferred leader will be broker 2 and partition 5's
   * preferred leader will be broker 3.
   *
   * @param options - the total number of partitions after the operation
   *   succeeds, and the replica assignments for the new partitions
   * @returns the new partitions
   * @throws `IllegalArgumentError` when the given parameters are no Java form
   * @throws `TypeError` or `RangeError` when a count or a broker id is not a Java `int`
   */
  static increaseTo(options: {
    /** The total number of partitions after the operation succeeds. */
    totalCount: number;
    /** The replica assignments for the new partitions, or null for the controller's. */
    newAssignments: Iterable<Iterable<number>> | null;
  }): NewPartitions;
  /**
   * Builds the new partitions.
   *
   * @param options - the count, and the assignments
   * @returns the new partitions
   * @throws `IllegalArgumentError` when the given parameters are no Java form
   * @throws `TypeError` or `RangeError` when a count or a broker id is not a Java `int`
   */
  static increaseTo(options: unknown): NewPartitions {
    INCREASE_TO_FORMS.match(options, arguments.length);
    const totalCount = checkInt(INCREASE_TO_FORMS.value(options, 'totalCount'), 'totalCount');
    const given = INCREASE_TO_FORMS.value(options, 'newAssignments');
    if (given === undefined || given === null) return new NewPartitions(totalCount, null);
    // Java's List<List<Integer>>: any iterable of iterables, copied.
    const what = 'newAssignments must be an iterable of iterables of broker ids';
    if (!isIterable(given)) throw new TypeError(`${what}, not ${kind(given)}`);
    const assignments = [...(given as Iterable<unknown>)].map((brokers) => {
      if (!isIterable(brokers)) throw new TypeError(`${what}, not one holding ${kind(brokers)}`);
      return Object.freeze([...(brokers as Iterable<unknown>)].map((b) => checkInt(b, 'a broker id')));
    });
    return new NewPartitions(totalCount, Object.freeze(assignments));
  }

  /**
   * The total number of partitions after the operation succeeds.
   *
   * @returns the total count
   * @throws `TypeError` when it is given an argument: it only reads
   */
  totalCount(): number {
    if (arguments.length !== 0) TOTAL_COUNT_FORMS.match(undefined, arguments.length);
    return this.#totalCount;
  }

  /**
   * The replica assignments for the new partitions, or null if the
   * assignment will be done by the controller.
   *
   * @returns the assignments, new arrays, or null
   * @throws `TypeError` when it is given an argument: it only reads
   */
  assignments(): number[][] | null {
    if (arguments.length !== 0) ASSIGNMENTS_FORMS.match(undefined, arguments.length);
    return this.#newAssignments === null ? null : this.#newAssignments.map((b) => [...b]);
  }

  /**
   * The fields as the addon takes them. Called by the admin clients.
   *
   * @returns the count and the assignments
   * @internal
   */
  internalToNative(): { totalCount: number; assignments: number[][] | null } {
    return { totalCount: this.#totalCount, assignments: this.assignments() };
  }

  /**
   * The new partitions as JSON.
   *
   * @returns the total count and the assignments
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): Record<string, unknown> {
    return { totalCount: this.#totalCount, newAssignments: this.#newAssignments === null ? null : this.#newAssignments.map((b) => [...b]) };
  }

  /**
   * Java's `toString()`: `(totalCount=<n>, newAssignments=<lists>)`.
   *
   * @returns the text
   */
  toString(): string {
    return `(totalCount=${this.#totalCount}, newAssignments=${javaString(this.#newAssignments)})`;
  }
}
