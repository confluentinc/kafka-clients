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

// Java's org.apache.kafka.common.TopicPartitionInfo, a value class that keeps
// Java's methods. Java's constructors wrap the lists in unmodifiable views;
// here each list is copied and frozen, and each getter returns a new array
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Types).

import { JavaForms, kind } from '../internal/args';
import { checkInt } from '../internal/java_number';
import { listEquals, listHashCode, objectHashCode, objectsEquals, sameClass } from '../internal/java_object';
import { Node } from './node';

const FORMS = new JavaForms('TopicPartitionInfo', [
  ['partition', 'leader', 'replicas', 'isr', 'elr', 'lastKnownElr'],
  ['partition', 'leader', 'replicas', 'isr'],
]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const PARTITION_FORMS = READER('partition');
const LEADER_FORMS = READER('leader');
const REPLICAS_FORMS = READER('replicas');
const ISR_FORMS = READER('isr');
const ELR_FORMS = READER('elr');
const LAST_KNOWN_ELR_FORMS = READER('lastKnownElr');
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = READER('hashCode');

/**
 * The nodes of `value` (Java's `List<Node>`, any iterable), copied and frozen.
 *
 * @param value - the nodes given
 * @param name - the parameter's name
 * @returns the nodes
 */
function nodes(value: unknown, name: string): readonly Node[] {
  // Java's Collections.unmodifiableList dereferences the list.
  if (value === null) throw new TypeError(`${name} must not be null`);
  if (typeof value !== 'object' || typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function') {
    throw new TypeError(`${name} must be an iterable of Node, not ${kind(value)}`);
  }
  const out: Node[] = [];
  for (const n of value as Iterable<unknown>) {
    if (n !== null && !(n instanceof Node)) throw new TypeError(`${name} must contain only Node, not ${kind(n)}`);
    out.push(n as Node);
  }
  return Object.freeze(out);
}

/**
 * A class containing leadership, replicas and ISR information for a topic
 * partition.
 */
export class TopicPartitionInfo {
  readonly #partition: number;
  readonly #leader: Node | null;
  readonly #replicas: readonly Node[];
  readonly #isr: readonly Node[];
  readonly #elr: readonly Node[] | null;
  readonly #lastKnownElr: readonly Node[] | null;

  /**
   * Create an instance of this class with the provided parameters.
   *
   * @param options - the partition id, the leader of the partition or null
   *   if there is none, the replicas of the partition in the same order as the
   *   replica assignment (the preferred replica is the head of the list), the
   *   in-sync replicas, the eligible leader replicas and the last known
   *   eligible leader replicas
   * @throws `IllegalArgumentError` when the given parameters are no Java form
   * @throws `TypeError` for a value of the wrong type, or a null list
   */
  constructor(options: {
    /** The partition id. */
    partition: number;
    /** The leader of the partition, or null if there is none. */
    leader: Node | null;
    /** The replicas, in the order of the replica assignment. */
    replicas: Iterable<Node>;
    /** The in-sync replicas. */
    isr: Iterable<Node>;
    /** The eligible leader replicas. */
    elr: Iterable<Node>;
    /** The last known eligible leader replicas. */
    lastKnownElr: Iterable<Node>;
  });
  /**
   * Create an instance of this class with the provided parameters, with no
   * eligible leader replicas (`elr()` and `lastKnownElr()` return null).
   *
   * @param options - the partition id, the leader or null, the replicas and
   *   the in-sync replicas
   * @throws `IllegalArgumentError` when the given parameters are no Java form
   * @throws `TypeError` for a value of the wrong type, or a null list
   */
  constructor(options: {
    /** The partition id. */
    partition: number;
    /** The leader of the partition, or null if there is none. */
    leader: Node | null;
    /** The replicas, in the order of the replica assignment. */
    replicas: Iterable<Node>;
    /** The in-sync replicas. */
    isr: Iterable<Node>;
    elr?: never;
    lastKnownElr?: never;
  });
  /**
   * Builds the partition's information.
   *
   * @param options - the fields of one of Java's two constructors
   * @throws `IllegalArgumentError` when the given parameters are no Java form
   * @throws `TypeError` for a value of the wrong type, or a null list
   */
  constructor(options: unknown) {
    const form = FORMS.match(options, arguments.length);
    this.#partition = checkInt(FORMS.value(options, 'partition'), 'partition');
    const leader = FORMS.value(options, 'leader');
    if (leader !== null && !(leader instanceof Node)) throw new TypeError(`leader must be a Node or null, not ${kind(leader)}`);
    this.#leader = leader;
    this.#replicas = nodes(FORMS.value(options, 'replicas'), 'replicas');
    this.#isr = nodes(FORMS.value(options, 'isr'), 'isr');
    this.#elr = form === 0 ? nodes(FORMS.value(options, 'elr'), 'elr') : null;
    this.#lastKnownElr = form === 0 ? nodes(FORMS.value(options, 'lastKnownElr'), 'lastKnownElr') : null;
  }

  /**
   * Return the partition id.
   *
   * @returns the partition id
   * @throws `TypeError` when it is given an argument: it only reads
   */
  partition(): number {
    if (arguments.length !== 0) PARTITION_FORMS.match(undefined, arguments.length);
    return this.#partition;
  }

  /**
   * Return the leader of the partition or null if there is none.
   *
   * @returns the leader, or null
   * @throws `TypeError` when it is given an argument: it only reads
   */
  leader(): Node | null {
    if (arguments.length !== 0) LEADER_FORMS.match(undefined, arguments.length);
    return this.#leader;
  }

  /**
   * Return the replicas of the partition in the same order as the replica
   * assignment. The preferred replica is the head of the list.
   *
   * Brokers with version lower than 0.11.0.0 return the replicas in
   * unspecified order due to a bug.
   *
   * @returns the replicas, a new array
   * @throws `TypeError` when it is given an argument: it only reads
   */
  replicas(): Node[] {
    if (arguments.length !== 0) REPLICAS_FORMS.match(undefined, arguments.length);
    return [...this.#replicas];
  }

  /**
   * Return the in-sync replicas of the partition. Note that the ordering of
   * the result is unspecified.
   *
   * @returns the in-sync replicas, a new array
   * @throws `TypeError` when it is given an argument: it only reads
   */
  isr(): Node[] {
    if (arguments.length !== 0) ISR_FORMS.match(undefined, arguments.length);
    return [...this.#isr];
  }

  /**
   * Return the eligible leader replicas of the partition. Note that the
   * ordering of the result is unspecified.
   *
   * @returns the eligible leader replicas, a new array, or null
   * @throws `TypeError` when it is given an argument: it only reads
   */
  elr(): Node[] | null {
    if (arguments.length !== 0) ELR_FORMS.match(undefined, arguments.length);
    return this.#elr === null ? null : [...this.#elr];
  }

  /**
   * Return the last known eligible leader replicas of the partition. Note
   * that the ordering of the result is unspecified.
   *
   * @returns the last known eligible leader replicas, a new array, or null
   * @throws `TypeError` when it is given an argument: it only reads
   */
  lastKnownElr(): Node[] | null {
    if (arguments.length !== 0) LAST_KNOWN_ELR_FORMS.match(undefined, arguments.length);
    return this.#lastKnownElr === null ? null : [...this.#lastKnownElr];
  }

  /**
   * The partition's information as JSON: its fields.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): Record<string, unknown> {
    return {
      partition: this.#partition,
      leader: this.#leader,
      replicas: this.#replicas,
      isr: this.#isr,
      elr: this.#elr,
      lastKnownElr: this.#lastKnownElr,
    };
  }

  /**
   * Java's `toString()`: `(partition=<p>, leader=<n>, replicas=<...>, isr=<...>,
   * elr=<...>, lastKnownElr=<...>)`, `N/A` for an eligible leader list that is null.
   *
   * @returns the text
   */
  toString(): string {
    const list = (l: readonly Node[]): string => l.map(String).join(', ');
    const elr = this.#elr !== null ? list(this.#elr) : 'N/A';
    const lastKnownElr = this.#lastKnownElr !== null ? list(this.#lastKnownElr) : 'N/A';
    return (
      `(partition=${this.#partition}, leader=${String(this.#leader)}, replicas=${list(this.#replicas)}, isr=${list(this.#isr)}` +
      `, elr=${elr}, lastKnownElr=${lastKnownElr})`
    );
  }

  /**
   * Whether `o` is a TopicPartitionInfo with equal fields.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as TopicPartitionInfo;
    return (
      this.#partition === that.#partition &&
      objectsEquals(this.#leader, that.#leader) &&
      listEquals(this.#replicas, that.#replicas) &&
      listEquals(this.#isr, that.#isr) &&
      listEquals(this.#elr, that.#elr) &&
      listEquals(this.#lastKnownElr, that.#lastKnownElr)
    );
  }

  /**
   * Java's `hashCode()` over the partition, the leader and the four lists.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    let result = this.#partition;
    result = (Math.imul(31, result) + objectHashCode(this.#leader)) | 0;
    result = (Math.imul(31, result) + listHashCode(this.#replicas)) | 0;
    result = (Math.imul(31, result) + listHashCode(this.#isr)) | 0;
    result = (Math.imul(31, result) + listHashCode(this.#elr)) | 0;
    result = (Math.imul(31, result) + listHashCode(this.#lastKnownElr)) | 0;
    return result;
  }
}
