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

// Java's org.apache.kafka.clients.admin.NewPartitionReassignment, a value
// class that keeps Java's methods: the target replicas of a reassignment.

import { JavaForms, kind } from '../internal/args';
import { checkInt } from '../internal/java_number';
import { IllegalArgumentError } from '../illegal_argument_error';

const isIterable = (value: unknown): boolean =>
  value !== null && typeof value === 'object' && typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] === 'function';
const FORMS = new JavaForms('NewPartitionReassignment', [['targetReplicas']], {
  bareTypes: { targetReplicas: (value) => value === null || isIterable(value) },
});
const TARGET_REPLICAS_FORMS = new JavaForms('targetReplicas', [[]], { reader: true });

/**
 * A new partition reassignment, which can be applied via
 * `Admin.alterPartitionReassignments(reassignments, options)`.
 */
export class NewPartitionReassignment {
  readonly #targetReplicas: readonly number[];

  /**
   * A reassignment to the given replicas.
   *
   * @param targetReplicas - the broker ids of the target replicas, any
   *   iterable, copied
   * @throws `IllegalArgumentError` "Cannot create a new partition
   *   reassignment without any replicas" when none are supplied
   * @throws `TypeError` for a null element or a value of the wrong type
   */
  constructor(targetReplicas: Iterable<number>);
  /**
   * The same as `new NewPartitionReassignment(targetReplicas)`, the replicas
   * given by name.
   *
   * @param options - the broker ids of the target replicas
   */
  constructor(options: {
    /** The broker ids of the target replicas, any iterable, copied. */
    targetReplicas: Iterable<number>;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const given = FORMS.value(options, 'targetReplicas');
    if (given === null) throw new IllegalArgumentError('Cannot create a new partition reassignment without any replicas');
    if (typeof given === 'string' || !isIterable(given)) {
      throw new TypeError(`targetReplicas must be an iterable of numbers, not ${kind(given)}`);
    }
    const replicas = [...(given as Iterable<unknown>)];
    if (replicas.length === 0) throw new IllegalArgumentError('Cannot create a new partition reassignment without any replicas');
    // Java's List.copyOf refuses a null element.
    this.#targetReplicas = Object.freeze(
      replicas.map((r) => {
        if (r === null) throw new TypeError('targetReplicas must not contain null');
        return checkInt(r, 'a target replica');
      }),
    );
  }

  /**
   * The broker ids of the target replicas.
   *
   * @returns the broker ids, a new array
   * @throws `TypeError` when it is given an argument: it only reads
   */
  targetReplicas(): number[] {
    if (arguments.length !== 0) TARGET_REPLICAS_FORMS.match(undefined, arguments.length);
    return [...this.#targetReplicas];
  }

  /**
   * The reassignment as `JSON.stringify` writes it: `{ targetReplicas }`.
   *
   * @returns the reassignment's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { targetReplicas: number[] } {
    return { targetReplicas: [...this.#targetReplicas] };
  }
}
