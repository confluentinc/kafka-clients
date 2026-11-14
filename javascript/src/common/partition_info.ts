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

// Java's PartitionInfo. It is not a data class (root CLAUDE.md, JavaScript
// Binding Conventions, Class family: the data-class list does not name it),
// so it keeps Java's methods, `topic()`, `partition()`, `leader()`,
// `replicas()`, `inSyncReplicas()` and `offlineReplicas()`, over private
// fields. A Java `Node[]` taken is any iterable of `Node`, and one returned is
// a new array each call (plan §5, item 1; the root's Types table). `equals`
// and `hashCode` are Java's bodies (Q29).

import { JavaForms, kind } from '../internal/args';
import { objectHashCode, objectsEquals, sameClass } from '../internal/java_object';
import { checkInt } from '../internal/java_number';
import { Node } from './node';

// Java's public constructors, in Java declaration order (src/internal/args.ts).
const FORMS = new JavaForms('PartitionInfo', [
  ['topic', 'partition', 'leader', 'replicas', 'inSyncReplicas'],
  ['topic', 'partition', 'leader', 'replicas', 'inSyncReplicas', 'offlineReplicas'],
]);
const TOPIC_FORMS = new JavaForms('topic', [[]], { reader: true });
const PARTITION_FORMS = new JavaForms('partition', [[]], { reader: true });
const LEADER_FORMS = new JavaForms('leader', [[]], { reader: true });
const REPLICAS_FORMS = new JavaForms('replicas', [[]], { reader: true });
const IN_SYNC_REPLICAS_FORMS = new JavaForms('inSyncReplicas', [[]], { reader: true });
const OFFLINE_REPLICAS_FORMS = new JavaForms('offlineReplicas', [[]], { reader: true });
const EQUALS_FORMS = new JavaForms('equals', [['obj']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/** A Java `Node[]` argument: null, or an iterable of `Node` (or null, as a Java array holds), copied. */
function checkNodes(value: unknown, what: string): readonly (Node | null)[] | null {
  if (value === null) return null;
  if (typeof value !== 'object' || typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function') {
    throw new TypeError(`${what} must be an iterable of Node, or null`);
  }
  const out: (Node | null)[] = [];
  for (const node of value as Iterable<unknown>) {
    if (node !== null && !(node instanceof Node)) throw new TypeError(`${what} must hold only Node, not ${kind(node)}`);
    out.push(node);
  }
  return Object.freeze(out);
}

// Java's Arrays.hashCode(Object[]): 0 for null, else 31 * h + each element's.
function arrayHashCode(nodes: readonly (Node | null)[] | null): number {
  if (nodes === null) return 0;
  let h = 1;
  for (const node of nodes) h = (Math.imul(31, h) + objectHashCode(node)) | 0;
  return h;
}

// Java's Objects.deepEquals of two Node[]: the same array, both null, or
// equal elements in order.
function arrayEquals(a: readonly (Node | null)[] | null, b: readonly (Node | null)[] | null): boolean {
  if (a === b) return true;
  if (a === null || b === null || a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (!objectsEquals(a[i], b[i])) return false;
  return true;
}

// Java's formatNodeIds: the node ids, comma-separated, in brackets.
function formatNodeIds(nodes: readonly (Node | null)[] | null): string {
  if (nodes === null) return '[]';
  // A null element is Java's NullPointerException there.
  return `[${nodes.map((node) => (node as Node).idString).join(',')}]`;
}

/**
 * This is used to describe per-partition state in the MetadataResponse.
 */
export class PartitionInfo {
  readonly #topic: string;
  readonly #partition: number;
  readonly #leader: Node | null;
  readonly #replicas: readonly (Node | null)[] | null;
  readonly #inSyncReplicas: readonly (Node | null)[] | null;
  readonly #offlineReplicas: readonly (Node | null)[] | null;

  /**
   * Creates a partition's information, Java's `PartitionInfo(topic,
   * partition, leader, replicas, inSyncReplicas)`: no offline replicas.
   *
   * @param options - the topic, the partition, its leader, and its replicas
   *   and in-sync replicas
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{topic, partition, leader, replicas, inSyncReplicas}` or
   *   `{topic, partition, leader, replicas, inSyncReplicas, offlineReplicas}`
   * @throws `TypeError` when an argument has the wrong type or is `undefined`
   * @throws `RangeError` when the partition is beyond Java's `int`
   *
   * @remarks A Java `Node[]` is any iterable of `Node` here, copied when the
   * object is built.
   */
  constructor(options: {
    /** The topic name. */
    topic: string;
    /** The partition id. */
    partition: number;
    /** The leader, or null. */
    leader: Node | null;
    /** The complete set of replicas, or null. */
    replicas: Iterable<Node> | null;
    /** The in-sync replicas, or null. */
    inSyncReplicas: Iterable<Node> | null;
    offlineReplicas?: never;
  });
  /**
   * Creates a partition's information, Java's `PartitionInfo(topic,
   * partition, leader, replicas, inSyncReplicas, offlineReplicas)`.
   *
   * @param options - the topic, the partition, its leader, and its replicas,
   *   in-sync replicas and offline replicas
   */
  constructor(options: {
    /** The topic name. */
    topic: string;
    /** The partition id. */
    partition: number;
    /** The leader, or null. */
    leader: Node | null;
    /** The complete set of replicas, or null. */
    replicas: Iterable<Node> | null;
    /** The in-sync replicas, or null. */
    inSyncReplicas: Iterable<Node> | null;
    /** The offline replicas, or null. */
    offlineReplicas: Iterable<Node> | null;
  });
  constructor(options: unknown) {
    const form = FORMS.match(options, arguments.length);
    const topic = FORMS.value(options, 'topic');
    if (typeof topic !== 'string') throw new TypeError('topic must be a string');
    const partition = checkInt(FORMS.value(options, 'partition'), 'partition');
    const leader = FORMS.value(options, 'leader');
    if (leader !== null && !(leader instanceof Node)) throw new TypeError('leader must be a Node or null');
    this.#topic = topic;
    this.#partition = partition;
    this.#leader = leader;
    this.#replicas = checkNodes(FORMS.value(options, 'replicas'), 'replicas');
    this.#inSyncReplicas = checkNodes(FORMS.value(options, 'inSyncReplicas'), 'inSyncReplicas');
    // Java's five-parameter constructor passes new Node[0].
    this.#offlineReplicas = form === 1 ? checkNodes(FORMS.value(options, 'offlineReplicas'), 'offlineReplicas') : Object.freeze([]);
  }

  /**
   * The topic name
   *
   * @returns the topic name
   * @throws `TypeError` when it is given an argument
   */
  topic(): string {
    TOPIC_FORMS.match(undefined, arguments.length);
    return this.#topic;
  }

  /**
   * The partition id
   *
   * @returns the partition id
   * @throws `TypeError` when it is given an argument
   */
  partition(): number {
    PARTITION_FORMS.match(undefined, arguments.length);
    return this.#partition;
  }

  /**
   * The node id of the node currently acting as a leader for this partition
   * or null if there is no leader
   *
   * @returns the leader, or null
   * @throws `TypeError` when it is given an argument
   */
  leader(): Node | null {
    LEADER_FORMS.match(undefined, arguments.length);
    return this.#leader;
  }

  /**
   * The complete set of replicas for this partition regardless of whether
   * they are alive or up-to-date. The preferred replica is the first.
   *
   * @returns the replicas, a new array each call, or null
   * @throws `TypeError` when it is given an argument
   */
  replicas(): Node[] | null {
    REPLICAS_FORMS.match(undefined, arguments.length);
    return this.#replicas === null ? null : ([...this.#replicas] as Node[]);
  }

  /**
   * The subset of the replicas that are in sync, that is caught-up to the
   * leader and ready to take over as leader if the leader should fail
   *
   * @returns the in-sync replicas, a new array each call, or null
   * @throws `TypeError` when it is given an argument
   */
  inSyncReplicas(): Node[] | null {
    IN_SYNC_REPLICAS_FORMS.match(undefined, arguments.length);
    return this.#inSyncReplicas === null ? null : ([...this.#inSyncReplicas] as Node[]);
  }

  /**
   * The subset of the replicas that are offline
   *
   * @returns the offline replicas, a new array each call, or null
   * @throws `TypeError` when it is given an argument
   */
  offlineReplicas(): Node[] | null {
    OFFLINE_REPLICAS_FORMS.match(undefined, arguments.length);
    return this.#offlineReplicas === null ? null : ([...this.#offlineReplicas] as Node[]);
  }

  /**
   * Java's hash code of the partition's information: `Objects.hash` of the
   * topic, the partition, the leader and the `Arrays.hashCode` of each array.
   *
   * @returns the hash code, a 32-bit integer
   * @throws `TypeError` when it is given an argument
   */
  hashCode(): number {
    HASH_CODE_FORMS.match(undefined, arguments.length);
    let h = 1;
    for (const part of [
      objectHashCode(this.#topic),
      this.#partition,
      objectHashCode(this.#leader),
      arrayHashCode(this.#replicas),
      arrayHashCode(this.#inSyncReplicas),
      arrayHashCode(this.#offlineReplicas),
    ]) {
      h = (Math.imul(31, h) + part) | 0;
    }
    return h;
  }

  /**
   * Whether `obj` is a `PartitionInfo` with equal fields, Java's
   * `PartitionInfo.equals`: the arrays compared element by element.
   *
   * @param options - the object to compare with
   * @returns true when `obj` is equal partition information
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
   * @returns true when `obj` is equal partition information
   */
  equals(obj: unknown): boolean;
  equals(options: unknown): boolean {
    EQUALS_FORMS.match(options, arguments.length);
    const obj = EQUALS_FORMS.value(options, 'obj');
    if (this === obj) return true;
    if (obj === null || obj === undefined || !sameClass(this, obj)) return false;
    const other = obj as PartitionInfo;
    return (
      objectsEquals(this.#topic, other.#topic) &&
      this.#partition === other.#partition &&
      objectsEquals(this.#leader, other.#leader) &&
      arrayEquals(this.#replicas, other.#replicas) &&
      arrayEquals(this.#inSyncReplicas, other.#inSyncReplicas) &&
      arrayEquals(this.#offlineReplicas, other.#offlineReplicas)
    );
  }

  /**
   * The partition as Java writes it: `Partition(topic = <topic>, partition =
   * <partition>, leader = <the leader's id, or none>, replicas = [<ids>], isr
   * = [<ids>], offlineReplicas = [<ids>])`.
   *
   * @returns the partition's text
   */
  toString(): string {
    return (
      `Partition(topic = ${this.#topic}, partition = ${this.#partition}, ` +
      `leader = ${this.#leader === null ? 'none' : this.#leader.idString}, ` +
      `replicas = ${formatNodeIds(this.#replicas)}, isr = ${formatNodeIds(this.#inSyncReplicas)}, ` +
      `offlineReplicas = ${formatNodeIds(this.#offlineReplicas)})`
    );
  }
}
