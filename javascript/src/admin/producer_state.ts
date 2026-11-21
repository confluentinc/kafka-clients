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

// Java's org.apache.kafka.clients.admin.ProducerState. Its `OptionalInt` and
// `OptionalLong` are `number | null` and `bigint | null` (root CLAUDE.md,
// JavaScript Binding Conventions, Types); a `long` is a `bigint`; `equals`,
// `hashCode` and `toString` are Java's.

import { JavaForms } from '../internal/args';
import { checkInt, checkLong } from '../internal/java_number';
import { objectHashCode, sameClass } from '../internal/java_object';

const FORMS = new JavaForms('ProducerState', [
  ['producerId', 'producerEpoch', 'lastSequence', 'lastTimestamp', 'coordinatorEpoch', 'currentTransactionStartOffset'],
]);
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const HASH_CODE_FORMS = READER('hashCode');
const PRODUCER_ID_FORMS = READER('producerId');
const PRODUCER_EPOCH_FORMS = READER('producerEpoch');
const LAST_SEQUENCE_FORMS = READER('lastSequence');
const LAST_TIMESTAMP_FORMS = READER('lastTimestamp');
const CURRENT_TRANSACTION_START_OFFSET_FORMS = READER('currentTransactionStartOffset');
const COORDINATOR_EPOCH_FORMS = READER('coordinatorEpoch');

/**
 * The state of an active producer of a partition.
 */
export class ProducerState {
  readonly #producerId: bigint;
  readonly #producerEpoch: number;
  readonly #lastSequence: number;
  readonly #lastTimestamp: bigint;
  readonly #coordinatorEpoch: number | null;
  readonly #currentTransactionStartOffset: bigint | null;

  /**
   * A producer's state.
   *
   * @param options - the producer's id and epoch, its last sequence and
   *   timestamp, the coordinator's epoch (null: empty) and the start offset of
   *   its ongoing transaction (null: empty)
   * @throws `IllegalArgumentError` when the given parameters are not Java's
   * @throws `TypeError` for a value of the wrong type
   * @throws `RangeError` for a value beyond its Java type
   */
  constructor(options: {
    /** The producer id. */
    producerId: bigint | number;
    /** The producer epoch. */
    producerEpoch: number;
    /** The last sequence. */
    lastSequence: number;
    /** The last timestamp. */
    lastTimestamp: bigint | number;
    /** The coordinator epoch, or null for Java's `OptionalInt.empty()`. */
    coordinatorEpoch: number | null;
    /** The current transaction start offset, or null for Java's `OptionalLong.empty()`. */
    currentTransactionStartOffset: bigint | null;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const value = (name: string): unknown => FORMS.value(options, name);
    this.#producerId = checkLong(value('producerId'), 'producerId');
    this.#producerEpoch = checkInt(value('producerEpoch'), 'producerEpoch');
    this.#lastSequence = checkInt(value('lastSequence'), 'lastSequence');
    this.#lastTimestamp = checkLong(value('lastTimestamp'), 'lastTimestamp');
    const coordinatorEpoch = value('coordinatorEpoch');
    this.#coordinatorEpoch = coordinatorEpoch === null ? null : checkInt(coordinatorEpoch, 'coordinatorEpoch');
    const start = value('currentTransactionStartOffset');
    this.#currentTransactionStartOffset = start === null ? null : checkLong(start, 'currentTransactionStartOffset');
    Object.freeze(this);
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
   * The last sequence.
   *
   * @returns the last sequence
   * @throws `TypeError` when it is given an argument: it only reads
   */
  lastSequence(): number {
    if (arguments.length !== 0) LAST_SEQUENCE_FORMS.match(undefined, arguments.length);
    return this.#lastSequence;
  }

  /**
   * The last timestamp.
   *
   * @returns the last timestamp
   * @throws `TypeError` when it is given an argument: it only reads
   */
  lastTimestamp(): bigint {
    if (arguments.length !== 0) LAST_TIMESTAMP_FORMS.match(undefined, arguments.length);
    return this.#lastTimestamp;
  }

  /**
   * The start offset of the producer's ongoing transaction.
   *
   * @returns the offset, or null when it is empty
   * @throws `TypeError` when it is given an argument: it only reads
   */
  currentTransactionStartOffset(): bigint | null {
    if (arguments.length !== 0) CURRENT_TRANSACTION_START_OFFSET_FORMS.match(undefined, arguments.length);
    return this.#currentTransactionStartOffset;
  }

  /**
   * The epoch of the transaction coordinator.
   *
   * @returns the epoch, or null when it is empty
   * @throws `TypeError` when it is given an argument: it only reads
   */
  coordinatorEpoch(): number | null {
    if (arguments.length !== 0) COORDINATOR_EPOCH_FORMS.match(undefined, arguments.length);
    return this.#coordinatorEpoch;
  }

  /**
   * Whether `o` is a ProducerState with the same fields.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as ProducerState;
    return (
      this.#producerId === that.#producerId &&
      this.#producerEpoch === that.#producerEpoch &&
      this.#lastSequence === that.#lastSequence &&
      this.#lastTimestamp === that.#lastTimestamp &&
      this.#coordinatorEpoch === that.#coordinatorEpoch &&
      this.#currentTransactionStartOffset === that.#currentTransactionStartOffset
    );
  }

  /**
   * Java's `Objects.hash(producerId, producerEpoch, lastSequence,
   * lastTimestamp, coordinatorEpoch, currentTransactionStartOffset)`, an
   * empty optional hashing as 0, as Java's.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    let h = 1;
    for (const part of [
      this.#producerId,
      this.#producerEpoch,
      this.#lastSequence,
      this.#lastTimestamp,
      this.#coordinatorEpoch,
      this.#currentTransactionStartOffset,
    ])
      h = (Math.imul(31, h) + objectHashCode(part)) | 0;
    return h;
  }

  /**
   * Java's `toString()`: `ProducerState(producerId=1, producerEpoch=0,
   * lastSequence=5, lastTimestamp=10, coordinatorEpoch=OptionalInt.empty,
   * currentTransactionStartOffset=OptionalLong[100])`.
   *
   * @returns the text
   */
  toString(): string {
    const epoch = this.#coordinatorEpoch === null ? 'OptionalInt.empty' : `OptionalInt[${this.#coordinatorEpoch}]`;
    const start =
      this.#currentTransactionStartOffset === null ? 'OptionalLong.empty' : `OptionalLong[${this.#currentTransactionStartOffset}]`;
    return (
      `ProducerState(producerId=${this.#producerId}, producerEpoch=${this.#producerEpoch}, lastSequence=${this.#lastSequence}` +
      `, lastTimestamp=${this.#lastTimestamp}, coordinatorEpoch=${epoch}, currentTransactionStartOffset=${start})`
    );
  }

  /**
   * The state as JSON: its fields, each `long` as a decimal string, an empty
   * optional as null.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): Record<string, unknown> {
    return {
      producerId: this.#producerId.toString(),
      producerEpoch: this.#producerEpoch,
      lastSequence: this.#lastSequence,
      lastTimestamp: this.#lastTimestamp.toString(),
      coordinatorEpoch: this.#coordinatorEpoch,
      currentTransactionStartOffset: this.#currentTransactionStartOffset === null ? null : this.#currentTransactionStartOffset.toString(),
    };
  }
}
