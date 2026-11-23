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
// Java's org.apache.kafka.clients.admin.MemberAssignment. Java's
// `Set<TopicPartition>` is held by value (Java's `Set.copyOf`) and returned as
// a new TopicPartitionSet (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Types); `equals`, `hashCode` and `toString` are Java's.

import { JavaForms, kind } from '../internal/args';
import { sameClass } from '../internal/java_object';
import { TopicPartition } from '../common/topic_partition';
import { TopicPartitionSet } from '../common/topic_partition_set';

const FORMS = new JavaForms('MemberAssignment', [['topicPartitions']], {
  bareTypes: { topicPartitions: (value) => value === null || (typeof value === 'object' && Symbol.iterator in value) },
});
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });
const TOPIC_PARTITIONS_FORMS = new JavaForms('topicPartitions', [[]], { reader: true });

/**
 * A description of the assignments of a specific group member.
 */
export class MemberAssignment {
  // By TopicPartition.internalKey, in the order given.
  readonly #topicPartitions: ReadonlyMap<string, TopicPartition>;

  /**
   * Creates an instance with the specified parameters.
   *
   * @param topicPartitions - List of topic partitions, on its own or as `{ topicPartitions }` (null: none)
   * @throws `TypeError` when it is not an iterable of TopicPartition, or holds null
   */
  constructor(topicPartitions: Iterable<TopicPartition> | null);
  /**
   * The same as `new MemberAssignment(topicPartitions)`, the partitions given by name.
   *
   * @param options - the topic partitions
   */
  constructor(options: {
    /** List of topic partitions (null: none). */
    topicPartitions: Iterable<TopicPartition> | null;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const given = FORMS.value(options, 'topicPartitions');
    const members = new Map<string, TopicPartition>();
    if (given !== null) {
      if (typeof given === 'string' || typeof given !== 'object' || !(Symbol.iterator in given)) {
        throw new TypeError(`topicPartitions must be an iterable of TopicPartition, not ${kind(given)}`);
      }
      for (const tp of given as Iterable<unknown>) {
        // Java's Set.copyOf refuses a null element.
        if (tp === null) throw new TypeError('topicPartitions must not contain null');
        if (!(tp instanceof TopicPartition)) throw new TypeError(`topicPartitions must contain only TopicPartition, not ${kind(tp)}`);
        if (!members.has(tp.internalKey())) members.set(tp.internalKey(), tp);
      }
    }
    this.#topicPartitions = members;
    Object.freeze(this);
  }

  /**
   * The topic partitions assigned to a group member.
   *
   * @returns a new set of the topic partitions
   * @throws `TypeError` when it is given an argument: it only reads
   */
  topicPartitions(): TopicPartitionSet {
    if (arguments.length !== 0) TOPIC_PARTITIONS_FORMS.match(undefined, arguments.length);
    return new TopicPartitionSet([...this.#topicPartitions.values()]);
  }

  /**
   * Whether `o` is a MemberAssignment of the same topic partitions.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = (other as MemberAssignment).#topicPartitions;
    if (that.size !== this.#topicPartitions.size) return false;
    for (const key of this.#topicPartitions.keys()) if (!that.has(key)) return false;
    return true;
  }

  /**
   * Java's `Set.hashCode()` of the topic partitions.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    let h = 0;
    for (const tp of this.#topicPartitions.values()) h = (h + tp.hashCode()) | 0;
    return h;
  }

  /**
   * Java's `toString()`: `(topicPartitions=t-0,t-1)`.
   *
   * @returns the text
   */
  toString(): string {
    return `(topicPartitions=${[...this.#topicPartitions.values()].map(String).join(',')})`;
  }

  /**
   * The assignment as JSON: `{ topicPartitions }`, an array of `{ topic, partition }`.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { topicPartitions: TopicPartition[] } {
    return { topicPartitions: [...this.#topicPartitions.values()] };
  }
}
