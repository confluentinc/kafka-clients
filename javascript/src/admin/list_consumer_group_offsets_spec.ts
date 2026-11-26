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
// Java's org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec, a class
// Java builds with its setter (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Class family): a keyed constructor, and Java's getter /
// setter pair `topicPartitions` as one method. Java's
// `Collection<TopicPartition>` is taken as any iterable, copied, and returned
// as a new array (Types); null lists every partition of the group.

import { JavaForms, kind } from '../internal/args';
import { listEquals, listHashCode } from '../internal/java_object';
import { javaString } from '../internal/java_string';
import { GetterSetter, applyOptionKeys } from '../internal/options';
import { TopicPartition } from '../common/topic_partition';

const TOPIC_PARTITIONS = new GetterSetter(
  'topicPartitions',
  (value) =>
    value === null ||
    typeof value === 'string' ||
    (typeof value === 'object' && Symbol.iterator in value && !(value instanceof TopicPartition)),
);
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/**
 * Specification of consumer group offsets to list using
 * `Admin.listConsumerGroupOffsets({ groupSpecs })`.
 */
export class ListConsumerGroupOffsetsSpec {
  #topicPartitions: TopicPartition[] | null = null;

  /**
   * The spec, each key calling its setter.
   *
   * @param options - the setters' values, each key optional
   * @throws `IllegalArgumentError` for a key the class has not
   * @throws `TypeError` for a value of the wrong type
   *
   * @remarks *(deviation)* Java builds the spec through its setter only.
   * Why: one call builds a configured object; the setter stays.
   */
  constructor(options?: {
    /**
     * The topic partitions whose offsets are to be listed, or null for all
     * of the group's.
     *
     * @defaultValue `null`
     */
    topicPartitions?: Iterable<TopicPartition> | null;
  }) {
    applyOptionKeys('ListConsumerGroupOffsetsSpec', options, arguments.length, [
      ['topicPartitions', (v) => this.topicPartitions(v as Iterable<TopicPartition> | null)],
    ]);
  }

  /**
   * Returns the topic partitions whose offsets are to be listed for a
   * consumer group. `null` indicates that offsets of all partitions of the
   * group are to be listed.
   *
   * @returns a new array of the topic partitions, or null
   */
  topicPartitions(): TopicPartition[] | null;
  /**
   * Set the topic partitions whose offsets are to be listed for a consumer
   * group. `null` includes all topic partitions.
   *
   * @param topicPartitions - List of topic partitions to include, on its own
   * @returns This ListConsumerGroupOffsetSpec
   * @throws `TypeError` when the value is not an iterable of TopicPartition or null
   */
  topicPartitions(topicPartitions: Iterable<TopicPartition> | null): this;
  /**
   * The same as `topicPartitions(topicPartitions)`.
   *
   * @param options - List of topic partitions to include
   * @returns This ListConsumerGroupOffsetSpec
   * @throws `TypeError` when the value is not an iterable of TopicPartition or null
   */
  topicPartitions(options: {
    /** List of topic partitions to include, or null for all. */
    topicPartitions: Iterable<TopicPartition> | null;
  }): this;
  topicPartitions(options?: unknown): TopicPartition[] | null | this {
    if (!TOPIC_PARTITIONS.sets(options, arguments.length)) return this.#topicPartitions === null ? null : [...this.#topicPartitions];
    const value = TOPIC_PARTITIONS.value(options);
    if (value === null) {
      this.#topicPartitions = null;
      return this;
    }
    if (typeof value === 'string' || typeof value !== 'object' || !(Symbol.iterator in value)) {
      throw new TypeError(`topicPartitions must be an iterable of TopicPartition or null, not ${kind(value)}`);
    }
    const list = [...(value as Iterable<unknown>)];
    for (const tp of list) {
      if (tp !== null && !(tp instanceof TopicPartition)) {
        throw new TypeError(`topicPartitions must contain only TopicPartition, not ${kind(tp)}`);
      }
    }
    this.#topicPartitions = list as TopicPartition[];
    return this;
  }

  /**
   * Whether `o` is a ListConsumerGroupOffsetsSpec of equal topic partitions.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!(other instanceof ListConsumerGroupOffsetsSpec)) return false;
    return listEquals(this.#topicPartitions, other.#topicPartitions);
  }

  /**
   * Java's `Objects.hash(topicPartitions)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return (31 + listHashCode(this.#topicPartitions)) | 0;
  }

  /**
   * Java's `toString()`: `ListConsumerGroupOffsetsSpec(topicPartitions=[t-0])`.
   *
   * @returns the text
   */
  toString(): string {
    return `ListConsumerGroupOffsetsSpec(topicPartitions=${javaString(this.#topicPartitions)})`;
  }

  /**
   * The spec as JSON: `{ topicPartitions }`, null for all partitions.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { topicPartitions: TopicPartition[] | null } {
    return { topicPartitions: this.#topicPartitions === null ? null : [...this.#topicPartitions] };
  }
}
