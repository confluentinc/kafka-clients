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

// Java's org.apache.kafka.common.TopicPartitionReplica, a value class that
// keeps Java's methods (it is not one of the data classes): the topic name,
// partition number and broker id of a replica. equals, hashCode and toString
// are Java's.

import { JavaForms } from '../internal/args';
import { checkInt } from '../internal/java_number';
import { sameClass, stringHashCode } from '../internal/java_object';

const FORMS = new JavaForms('TopicPartitionReplica', [['topic', 'partition', 'brokerId']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const TOPIC_FORMS = READER('topic');
const PARTITION_FORMS = READER('partition');
const BROKER_ID_FORMS = READER('brokerId');
const HASH_CODE_FORMS = READER('hashCode');
const EQUALS_FORMS = new JavaForms('equals', [['obj']]);

/**
 * The topic name, partition number and the brokerId of the replica
 */
export class TopicPartitionReplica {
  readonly #topic: string;
  readonly #partition: number;
  readonly #brokerId: number;

  /**
   * The replica of `partition` of `topic` on broker `brokerId`.
   *
   * @param options - the topic name, the partition number and the broker id
   * @throws `TypeError` "topic must not be null", as Java's
   *   `Objects.requireNonNull`, or for a value of the wrong type
   * @throws `RangeError` for a partition or broker id outside Java's `int`
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{topic, partition, brokerId}`
   */
  constructor(options: {
    /** The topic name. */
    topic: string;
    /** The partition number. */
    partition: number;
    /** The id of the broker holding the replica. */
    brokerId: number;
  }) {
    FORMS.match(options, arguments.length);
    const topic = FORMS.value(options, 'topic');
    if (topic === null) throw new TypeError('topic must not be null');
    if (typeof topic !== 'string') throw new TypeError('topic must be a string');
    this.#topic = topic;
    this.#partition = checkInt(FORMS.value(options, 'partition'), 'partition');
    this.#brokerId = checkInt(FORMS.value(options, 'brokerId'), 'brokerId');
  }

  /**
   * The topic name.
   *
   * @returns the topic
   * @throws `TypeError` when it is given an argument: it only reads
   */
  topic(): string {
    if (arguments.length !== 0) TOPIC_FORMS.match(undefined, arguments.length);
    return this.#topic;
  }

  /**
   * The partition number.
   *
   * @returns the partition
   * @throws `TypeError` when it is given an argument: it only reads
   */
  partition(): number {
    if (arguments.length !== 0) PARTITION_FORMS.match(undefined, arguments.length);
    return this.#partition;
  }

  /**
   * The id of the broker holding the replica.
   *
   * @returns the broker id
   * @throws `TypeError` when it is given an argument: it only reads
   */
  brokerId(): number {
    if (arguments.length !== 0) BROKER_ID_FORMS.match(undefined, arguments.length);
    return this.#brokerId;
  }

  /**
   * Java's `hashCode()` over the topic, the partition and the broker id.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    let result = 1;
    result = (Math.imul(31, result) + stringHashCode(this.#topic)) | 0;
    result = (Math.imul(31, result) + this.#partition) | 0;
    result = (Math.imul(31, result) + this.#brokerId) | 0;
    return result;
  }

  /**
   * Whether `obj` is a TopicPartitionReplica of the same topic, partition
   * and broker.
   *
   * @param obj - the value to compare with, on its own or as `{ obj }`
   * @returns whether they are equal
   */
  equals(obj: unknown): boolean {
    EQUALS_FORMS.match(obj, arguments.length);
    const other = EQUALS_FORMS.value(obj, 'obj');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as TopicPartitionReplica;
    return this.#partition === that.#partition && this.#brokerId === that.#brokerId && this.#topic === that.#topic;
  }

  /**
   * Java's `toString()`: `<topic>-<partition>-<brokerId>`.
   *
   * @returns the text
   */
  toString(): string {
    return `${this.#topic}-${this.#partition}-${this.#brokerId}`;
  }

  /**
   * The replica as `JSON.stringify` writes it: `{ topic, partition, brokerId }`.
   *
   * @returns the replica's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { topic: string; partition: number; brokerId: number } {
    return { topic: this.#topic, partition: this.#partition, brokerId: this.#brokerId };
  }

  /**
   * The key `TopicPartitionReplicaMap` stores the replica under: the fields
   * `equals` compares. Called by the map.
   *
   * @returns the key
   * @internal
   */
  internalKey(): string {
    return JSON.stringify([this.#topic, this.#partition, this.#brokerId]);
  }
}
