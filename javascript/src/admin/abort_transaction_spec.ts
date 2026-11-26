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

// Java's org.apache.kafka.clients.admin.AbortTransactionSpec: `equals`,
// `hashCode` and `toString` are Java's; a `long` is a `bigint`, a `short` a
// `number`. A null partition is stored, as Java's constructor stores it.

import { JavaForms, kind } from '../internal/args';
import { checkInt, checkLong } from '../internal/java_number';
import { objectHashCode, objectsEquals, sameClass } from '../internal/java_object';
import { TopicPartition } from '../common/topic_partition';

const FORMS = new JavaForms('AbortTransactionSpec', [['topicPartition', 'producerId', 'producerEpoch', 'coordinatorEpoch']]);
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const HASH_CODE_FORMS = READER('hashCode');
const TOPIC_PARTITION_FORMS = READER('topicPartition');
const PRODUCER_ID_FORMS = READER('producerId');
const PRODUCER_EPOCH_FORMS = READER('producerEpoch');
const COORDINATOR_EPOCH_FORMS = READER('coordinatorEpoch');

/**
 * The specification of a hanging transaction to abort, for
 * `Admin.abortTransaction(spec)`.
 */
export class AbortTransactionSpec {
  readonly #topicPartition: TopicPartition | null;
  readonly #producerId: bigint;
  readonly #producerEpoch: number;
  readonly #coordinatorEpoch: number;

  /**
   * A specification.
   *
   * @param options - the partition the transaction hangs on, the producer's
   *   id and epoch, and the coordinator's epoch
   * @throws `IllegalArgumentError` when the given parameters are not Java's
   * @throws `TypeError` for a value of the wrong type
   * @throws `RangeError` for a value beyond its Java type
   */
  constructor(options: {
    /** The topic partition. */
    topicPartition: TopicPartition | null;
    /** The producer id. */
    producerId: bigint | number;
    /** The producer epoch, a Java `short`. */
    producerEpoch: number;
    /** The coordinator epoch. */
    coordinatorEpoch: number;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const tp = FORMS.value(options, 'topicPartition');
    if (tp !== null && !(tp instanceof TopicPartition))
      throw new TypeError(`topicPartition must be a TopicPartition or null, not ${kind(tp)}`);
    const producerId = checkLong(FORMS.value(options, 'producerId'), 'producerId');
    const producerEpoch = checkInt(FORMS.value(options, 'producerEpoch'), 'producerEpoch');
    if (producerEpoch < -32768 || producerEpoch > 32767) throw new RangeError('producerEpoch must be from -32768 to 32767');
    this.#topicPartition = tp;
    this.#producerId = producerId;
    this.#producerEpoch = producerEpoch;
    this.#coordinatorEpoch = checkInt(FORMS.value(options, 'coordinatorEpoch'), 'coordinatorEpoch');
    Object.freeze(this);
  }

  /**
   * The partition the transaction hangs on.
   *
   * @returns the topic partition
   * @throws `TypeError` when it is given an argument: it only reads
   */
  topicPartition(): TopicPartition | null {
    if (arguments.length !== 0) TOPIC_PARTITION_FORMS.match(undefined, arguments.length);
    return this.#topicPartition;
  }

  /**
   * The producer id.
   *
   * @returns the producer id
   * @throws `TypeError` when it is given an argument: it only reads
   */
  producerId(): bigint {
    if (arguments.length !== 0) PRODUCER_ID_FORMS.match(undefined, arguments.length);
    return this.#producerId;
  }

  /**
   * The producer epoch.
   *
   * @returns the producer epoch
   * @throws `TypeError` when it is given an argument: it only reads
   */
  producerEpoch(): number {
    if (arguments.length !== 0) PRODUCER_EPOCH_FORMS.match(undefined, arguments.length);
    return this.#producerEpoch;
  }

  /**
   * The epoch of the transaction coordinator.
   *
   * @returns the coordinator epoch
   * @throws `TypeError` when it is given an argument: it only reads
   */
  coordinatorEpoch(): number {
    if (arguments.length !== 0) COORDINATOR_EPOCH_FORMS.match(undefined, arguments.length);
    return this.#coordinatorEpoch;
  }

  /**
   * Whether `o` is an AbortTransactionSpec with the same fields.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as AbortTransactionSpec;
    return (
      this.#producerId === that.#producerId &&
      this.#producerEpoch === that.#producerEpoch &&
      this.#coordinatorEpoch === that.#coordinatorEpoch &&
      objectsEquals(this.#topicPartition, that.#topicPartition)
    );
  }

  /**
   * Java's `Objects.hash(topicPartition, producerId, producerEpoch, coordinatorEpoch)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    let h = 1;
    for (const part of [this.#topicPartition, this.#producerId, this.#producerEpoch, this.#coordinatorEpoch])
      h = (Math.imul(31, h) + objectHashCode(part)) | 0;
    return h;
  }

  /**
   * Java's `toString()`: `AbortTransactionSpec(topicPartition=t-0,
   * producerId=1, producerEpoch=0, coordinatorEpoch=5)`.
   *
   * @returns the text
   */
  toString(): string {
    return (
      `AbortTransactionSpec(topicPartition=${String(this.#topicPartition)}, producerId=${this.#producerId}` +
      `, producerEpoch=${this.#producerEpoch}, coordinatorEpoch=${this.#coordinatorEpoch})`
    );
  }

  /**
   * The specification as JSON: its fields, the producer id as a decimal string.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): Record<string, unknown> {
    return {
      topicPartition: this.#topicPartition,
      producerId: this.#producerId.toString(),
      producerEpoch: this.#producerEpoch,
      coordinatorEpoch: this.#coordinatorEpoch,
    };
  }
}
