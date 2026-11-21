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

// Java's org.apache.kafka.clients.admin.TransactionDescription, a data class
// (root CLAUDE.md, JavaScript Binding Conventions, Class family; the admin
// descriptions, decided by the owner on 2026-11-20): Java's getters are
// readonly fields, set once in the constructor, and the object is frozen; its
// set of partitions is a read-only TopicPartitionSet (null stored, as Java's
// constructor stores it). Its `OptionalLong` is `bigint | null` (Types);
// `equals`, `hashCode` and `toString` are Java's.

import { JavaForms, kind } from '../internal/args';
import { checkInt, checkLong } from '../internal/java_number';
import { objectHashCode, sameClass } from '../internal/java_object';
import { TopicPartition } from '../common/topic_partition';
import { TopicPartitionSet } from '../common/topic_partition_set';
import { TransactionState } from './transaction_state';

const FORMS = new JavaForms('TransactionDescription', [
  ['coordinatorId', 'state', 'producerId', 'producerEpoch', 'transactionTimeoutMs', 'transactionStartTimeMs', 'topicPartitions'],
]);
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/**
 * A description of a transaction, as `Admin.describeTransactions` gives it.
 */
export class TransactionDescription {
  /** The id of the transaction's coordinator. */
  readonly coordinatorId: number;

  /** The state of the transaction. */
  readonly state: TransactionState | null;

  /** The producer id. */
  readonly producerId: bigint;

  /** The producer epoch. */
  readonly producerEpoch: number;

  /** The transaction timeout, in milliseconds. */
  readonly transactionTimeoutMs: bigint;

  /** The start time of the transaction, in milliseconds, or null when it is empty. */
  readonly transactionStartTimeMs: bigint | null;

  /** The partitions in the transaction, read-only, or null. */
  readonly topicPartitions: TopicPartitionSet | null;

  /**
   * A description.
   *
   * @param options - the coordinator's id, the state, the producer's id and
   *   epoch, the transaction's timeout and start time (null: empty), and its
   *   partitions
   * @throws `IllegalArgumentError` when the given parameters are not Java's
   * @throws `TypeError` for a value of the wrong type
   * @throws `RangeError` for a value beyond its Java type
   */
  constructor(options: {
    /** The coordinator id. */
    coordinatorId: number;
    /** The state of the transaction. */
    state: TransactionState | null;
    /** The producer id. */
    producerId: bigint | number;
    /** The producer epoch. */
    producerEpoch: number;
    /** The transaction timeout, in milliseconds. */
    transactionTimeoutMs: bigint | number;
    /** The start time, in milliseconds, or null for Java's `OptionalLong.empty()`. */
    transactionStartTimeMs: bigint | null;
    /** The partitions in the transaction, or null. */
    topicPartitions: Iterable<TopicPartition> | null;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const value = (name: string): unknown => FORMS.value(options, name);
    const coordinatorId = checkInt(value('coordinatorId'), 'coordinatorId');
    const state = value('state');
    if (state !== null && !(state instanceof TransactionState))
      throw new TypeError(`state must be a TransactionState or null, not ${kind(state)}`);
    const producerId = checkLong(value('producerId'), 'producerId');
    const producerEpoch = checkInt(value('producerEpoch'), 'producerEpoch');
    const timeout = checkLong(value('transactionTimeoutMs'), 'transactionTimeoutMs');
    const start = value('transactionStartTimeMs');
    const given = value('topicPartitions');
    let partitions: TopicPartitionSet | null = null;
    if (given !== null) {
      if (typeof given === 'string' || typeof given !== 'object' || !(Symbol.iterator in given)) {
        throw new TypeError(`topicPartitions must be an iterable of TopicPartition, not ${kind(given)}`);
      }
      partitions = new TopicPartitionSet();
      for (const tp of given as Iterable<unknown>) {
        // Java's set holds what it is given, null included.
        if (tp !== null && !(tp instanceof TopicPartition))
          throw new TypeError(`topicPartitions must contain only TopicPartition, not ${kind(tp)}`);
        partitions.add(tp);
      }
      partitions.internalSetReadOnly();
    }
    this.coordinatorId = coordinatorId;
    this.state = state;
    this.producerId = producerId;
    this.producerEpoch = producerEpoch;
    this.transactionTimeoutMs = timeout;
    this.transactionStartTimeMs = start === null ? null : checkLong(start, 'transactionStartTimeMs');
    this.topicPartitions = partitions;
    Object.freeze(this);
  }

  /**
   * Whether `o` is a TransactionDescription with the same fields.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as TransactionDescription;
    return (
      this.coordinatorId === that.coordinatorId &&
      this.producerId === that.producerId &&
      this.producerEpoch === that.producerEpoch &&
      this.transactionTimeoutMs === that.transactionTimeoutMs &&
      this.state === that.state &&
      this.transactionStartTimeMs === that.transactionStartTimeMs &&
      sameMembers(this.topicPartitions, that.topicPartitions)
    );
  }

  /**
   * Java's `Objects.hash(coordinatorId, state, producerId, producerEpoch,
   * transactionTimeoutMs, transactionStartTimeMs, topicPartitions)`, the set
   * hashing as Java's `Set.hashCode()`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    let set = 0;
    if (this.topicPartitions !== null) for (const tp of this.topicPartitions) set = (set + objectHashCode(tp)) | 0;
    let h = 1;
    for (const part of [
      this.coordinatorId,
      this.state,
      this.producerId,
      this.producerEpoch,
      this.transactionTimeoutMs,
      this.transactionStartTimeMs,
    ])
      h = (Math.imul(31, h) + objectHashCode(part)) | 0;
    return (Math.imul(31, h) + (this.topicPartitions === null ? 0 : set)) | 0;
  }

  /**
   * Java's `toString()`: `TransactionDescription(coordinatorId=1,
   * state=Ongoing, producerId=2, producerEpoch=0, transactionTimeoutMs=60000,
   * transactionStartTimeMs=OptionalLong[5], topicPartitions=[t-0])`.
   *
   * @returns the text
   */
  toString(): string {
    const start = this.transactionStartTimeMs === null ? 'OptionalLong.empty' : `OptionalLong[${this.transactionStartTimeMs}]`;
    const partitions = this.topicPartitions === null ? 'null' : `[${[...this.topicPartitions].map(String).join(', ')}]`;
    return (
      `TransactionDescription(coordinatorId=${this.coordinatorId}, state=${String(this.state)}, producerId=${this.producerId}` +
      `, producerEpoch=${this.producerEpoch}, transactionTimeoutMs=${this.transactionTimeoutMs}` +
      `, transactionStartTimeMs=${start}, topicPartitions=${partitions})`
    );
  }

  /**
   * The description as JSON: its fields, each `long` as a decimal string
   * (null when empty), the state as its constant's name and the partitions
   * as an array of `{ topic, partition }`.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): Record<string, unknown> {
    return {
      coordinatorId: this.coordinatorId,
      state: this.state,
      producerId: this.producerId.toString(),
      producerEpoch: this.producerEpoch,
      transactionTimeoutMs: this.transactionTimeoutMs.toString(),
      transactionStartTimeMs: this.transactionStartTimeMs === null ? null : this.transactionStartTimeMs.toString(),
      topicPartitions: this.topicPartitions === null ? null : [...this.topicPartitions],
    };
  }
}

/**
 * Java's `Objects.equals` of two sets of partitions.
 *
 * @param a - a set, or null
 * @param b - a set, or null
 * @returns whether they hold equal members
 */
function sameMembers(a: TopicPartitionSet | null, b: TopicPartitionSet | null): boolean {
  if (a === null || b === null) return a === b;
  if (a.size() !== b.size()) return false;
  for (const tp of a) if (!b.has(tp)) return false;
  return true;
}
