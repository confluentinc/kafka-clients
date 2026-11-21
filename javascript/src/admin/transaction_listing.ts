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

// Java's org.apache.kafka.clients.admin.TransactionListing: `equals`,
// `hashCode` and `toString` are Java's; a `long` is a `bigint`.

import { JavaForms, kind } from '../internal/args';
import { checkLong } from '../internal/java_number';
import { objectHashCode, objectsEquals, sameClass } from '../internal/java_object';
import { TransactionState } from './transaction_state';

const FORMS = new JavaForms('TransactionListing', [['transactionalId', 'producerId', 'transactionState']]);
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const HASH_CODE_FORMS = READER('hashCode');
const TRANSACTIONAL_ID_FORMS = READER('transactionalId');
const PRODUCER_ID_FORMS = READER('producerId');
const STATE_FORMS = READER('state');

/**
 * A transaction as `Admin.listTransactions()` lists it.
 */
export class TransactionListing {
  readonly #transactionalId: string | null;
  readonly #producerId: bigint;
  readonly #transactionState: TransactionState | null;

  /**
   * A listing.
   *
   * @param options - the transactional id, the producer id and the state
   * @throws `IllegalArgumentError` when the given parameters are not Java's
   * @throws `TypeError` for a value of the wrong type
   * @throws `RangeError` for a producer id beyond Java's `long`
   */
  constructor(options: {
    /** The transactional id. */
    transactionalId: string | null;
    /** The producer id. */
    producerId: bigint | number;
    /** The state of the transaction. */
    transactionState: TransactionState | null;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const id = FORMS.value(options, 'transactionalId');
    if (id !== null && typeof id !== 'string') throw new TypeError(`transactionalId must be a string or null, not ${kind(id)}`);
    const producerId = checkLong(FORMS.value(options, 'producerId'), 'producerId');
    const state = FORMS.value(options, 'transactionState');
    if (state !== null && !(state instanceof TransactionState)) {
      throw new TypeError(`transactionState must be a TransactionState or null, not ${kind(state)}`);
    }
    this.#transactionalId = id;
    this.#producerId = producerId;
    this.#transactionState = state;
    Object.freeze(this);
  }

  /**
   * The transactional id.
   *
   * @returns the transactional id
   * @throws `TypeError` when it is given an argument: it only reads
   */
  transactionalId(): string | null {
    if (arguments.length !== 0) TRANSACTIONAL_ID_FORMS.match(undefined, arguments.length);
    return this.#transactionalId;
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
   * The state of the transaction.
   *
   * @returns the state
   * @throws `TypeError` when it is given an argument: it only reads
   */
  state(): TransactionState | null {
    if (arguments.length !== 0) STATE_FORMS.match(undefined, arguments.length);
    return this.#transactionState;
  }

  /**
   * Whether `o` is a TransactionListing with the same fields.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as TransactionListing;
    return (
      this.#producerId === that.#producerId &&
      objectsEquals(this.#transactionalId, that.#transactionalId) &&
      this.#transactionState === that.#transactionState
    );
  }

  /**
   * Java's `Objects.hash(transactionalId, producerId, transactionState)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    let h = 1;
    for (const part of [this.#transactionalId, this.#producerId, this.#transactionState]) h = (Math.imul(31, h) + objectHashCode(part)) | 0;
    return h;
  }

  /**
   * Java's `toString()`: `TransactionListing(transactionalId='t',
   * producerId=1, transactionState=Ongoing)`.
   *
   * @returns the text
   */
  toString(): string {
    return (
      `TransactionListing(transactionalId='${String(this.#transactionalId)}', producerId=${this.#producerId}` +
      `, transactionState=${String(this.#transactionState)})`
    );
  }

  /**
   * The listing as JSON: `{ transactionalId, producerId, transactionState }`,
   * the producer id as a decimal string and the state as its constant's name.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): Record<string, unknown> {
    return { transactionalId: this.#transactionalId, producerId: this.#producerId.toString(), transactionState: this.#transactionState };
  }
}
