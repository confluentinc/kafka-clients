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
// after it goes into a map. Java's equals() and hashCode() are not translated
// (no JavaScript operator or collection calls them).

import { JavaForms } from '../internal/args';

const FORMS = new JavaForms('TopicPartition', [['topic', 'partition']]);

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
    if (typeof partition !== 'number' || !Number.isInteger(partition)) throw new TypeError('partition must be an integer');
    this.topic = topic;
    this.partition = partition;
    Object.freeze(this);
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
