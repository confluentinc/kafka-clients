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

// Java's TopicPartition, a data class (root CLAUDE.md, JavaScript Binding
// Conventions, Class family): `tp.topic` and `tp.partition` are readonly
// fields, and the object is frozen when it is created, so a key cannot change
// after it goes into a map. `equals` and `hashCode` are Java's bodies (Q29).

import { JavaForms } from '../internal/args';
import { objectHashCode, objectsEquals, sameClass } from '../internal/java_object';
import { checkInt } from '../internal/java_number';

const FORMS = new JavaForms('TopicPartition', [['topic', 'partition']]);
const EQUALS_FORMS = new JavaForms('equals', [['obj']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/**
 * A topic name and partition number
 */
export class TopicPartition {
  /** The topic name, or null. */
  readonly topic: string | null;

  /** The partition number. */
  readonly partition: number;

  /**
   * Makes `TopicPartition` nominal: TypeScript checks it by name, not by
   * shape, so an object literal with the same fields is no `TopicPartition`.
   */
  readonly #brand = true;

  /**
   * Creates a topic partition.
   *
   * @param options - the topic and the partition
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{topic, partition}`
   * @throws `TypeError` when the topic is neither a string nor null, the
   *   partition is not an integer, or either is `undefined`
   * @throws `RangeError` when the partition is beyond Java's `int`
   *
   * @remarks The object is frozen. A null topic is accepted, as Java accepts
   * it.
   */
  constructor(options: {
    /** The topic name. */
    topic: string | null;
    /** The partition number. */
    partition: number;
  }) {
    FORMS.match(options, arguments.length);
    const topic = FORMS.value(options, 'topic');
    const partition = FORMS.value(options, 'partition');
    if (topic !== null && typeof topic !== 'string') throw new TypeError('topic must be a string or null');
    this.topic = topic;
    this.partition = checkInt(partition, 'partition');
    Object.freeze(this);
  }

  /**
   * Java's hash code of the topic partition: `31 + partition`, then `31 *
   * result` plus the topic's hash code (0 for a null topic).
   *
   * @returns the hash code, a 32-bit integer
   * @throws `TypeError` when it is given an argument
   */
  hashCode(): number {
    HASH_CODE_FORMS.match(undefined, arguments.length);
    const prime = 31;
    const result = (prime + this.partition) | 0;
    return (Math.imul(prime, result) + objectHashCode(this.topic)) | 0;
  }

  /**
   * Whether `obj` is a `TopicPartition` of the same topic and partition,
   * Java's `TopicPartition.equals`.
   *
   * @param options - the object to compare with
   * @returns true when `obj` is an equal topic partition
   * @throws `IllegalArgumentError` when the given parameters are not `{obj}`
   */
  equals(options: {
    /** The object to compare with. */
    obj: unknown;
  }): boolean;
  /**
   * The same as `equals({obj})`, with the object on its own; a plain object
   * is read as the options object.
   *
   * @param obj - the object to compare with
   * @returns true when `obj` is an equal topic partition
   */
  equals(obj: unknown): boolean;
  equals(options: unknown): boolean {
    EQUALS_FORMS.match(options, arguments.length);
    const obj = EQUALS_FORMS.value(options, 'obj');
    if (this === obj) return true;
    if (obj === null || obj === undefined) return false;
    if (!sameClass(this, obj)) return false;
    const other = obj as TopicPartition;
    return this.partition === other.partition && objectsEquals(this.topic, other.topic);
  }

  /**
   * The topic partition as Java writes it: `<topic>-<partition>`.
   *
   * @returns the topic partition's text
   */
  toString(): string {
    return `${String(this.topic)}-${this.partition}`;
  }

  /**
   * The topic partition as `JSON.stringify` writes it: `{ topic, partition }`.
   * It ignores the key `JSON.stringify` passes.
   *
   * @returns the topic and the partition
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { topic: string | null; partition: number } {
    return { topic: this.topic, partition: this.partition };
  }
}
