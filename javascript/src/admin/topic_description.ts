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

// Java's org.apache.kafka.clients.admin.TopicDescription, a data class
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family; the admin
// descriptions, decided by the owner on 2026-11-20): Java's getters are
// readonly fields, set once in the constructor, and the object is frozen; the
// collections it holds are read-only (a frozen array of partitions, a
// read-only set of operations). Java's three constructors are one, taking
// the union of their parameters; equals, hashCode and toString are Java's.

import { JavaForms, kind } from '../internal/args';
import { listEquals, listHashCode, objectHashCode, objectsEquals, sameClass, setEquals, setHashCode } from '../internal/java_object';
import { javaString } from '../internal/java_string';
import { readOnlySet } from '../internal/read_only_set';
import { AclOperation } from '../common/acl/acl_operation';
import { TopicPartitionInfo } from '../common/topic_partition_info';
import { Uuid } from '../common/uuid';

const FORMS = new JavaForms('TopicDescription', [
  ['name', 'internal', 'partitions'],
  ['name', 'internal', 'partitions', 'authorizedOperations'],
  ['name', 'internal', 'partitions', 'authorizedOperations', 'topicId'],
]);
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/**
 * A detailed description of a single topic in the cluster.
 */
export class TopicDescription {
  /** The name of the topic. */
  readonly name: string | null;

  /**
   * Whether the topic is internal to Kafka. An example of an internal topic
   * is the offsets and group management topic: __consumer_offsets.
   */
  readonly isInternal: boolean;

  /** The id of the topic. */
  readonly topicId: Uuid | null;

  /**
   * A list of partitions where the index represents the partition id and the
   * element contains leadership and replica information for that partition.
   */
  readonly partitions: readonly TopicPartitionInfo[];

  /** Authorized operations for this topic, or null if this is not known. */
  readonly authorizedOperations: ReadonlySet<AclOperation> | null;

  /**
   * Create an instance with the specified parameters.
   *
   * @param options - the topic name, whether the topic is internal to Kafka,
   *   and a list of partitions where the index represents the partition id
   *   and the element contains leadership and replica information for that
   *   partition
   */
  constructor(options: {
    /** The topic name. */
    name: string | null;
    /** Whether the topic is internal to Kafka. */
    internal: boolean;
    /** The partitions, the index the partition id. */
    partitions: Iterable<TopicPartitionInfo>;
    authorizedOperations?: never;
    topicId?: never;
  });
  /**
   * Create an instance with the specified parameters.
   *
   * @param options - the topic name, whether the topic is internal to Kafka,
   *   a list of partitions, and the authorized operations for this topic
   */
  constructor(options: {
    /** The topic name. */
    name: string | null;
    /** Whether the topic is internal to Kafka. */
    internal: boolean;
    /** The partitions, the index the partition id. */
    partitions: Iterable<TopicPartitionInfo>;
    /** The authorized operations for this topic, or null if not known. */
    authorizedOperations: Iterable<AclOperation> | null;
    topicId?: never;
  });
  /**
   * Create an instance with the specified parameters.
   *
   * @param options - the topic name, whether the topic is internal to Kafka,
   *   a list of partitions, the authorized operations for this topic and the
   *   topic id
   */
  constructor(options: {
    /** The topic name. */
    name: string | null;
    /** Whether the topic is internal to Kafka. */
    internal: boolean;
    /** The partitions, the index the partition id. */
    partitions: Iterable<TopicPartitionInfo>;
    /** The authorized operations for this topic, or null if not known. */
    authorizedOperations: Iterable<AclOperation> | null;
    /** The topic id. */
    topicId: Uuid | null;
  });
  /**
   * Builds the description.
   *
   * @param options - the fields of one of Java's three constructors
   * @throws `IllegalArgumentError` when the given parameters are no Java form
   * @throws `TypeError` for a value of the wrong type, or null partitions
   */
  constructor(options: unknown) {
    const form = FORMS.match(options, arguments.length);
    const name = FORMS.value(options, 'name');
    if (name !== null && typeof name !== 'string') throw new TypeError(`name must be a string or null, not ${kind(name)}`);
    const internal = FORMS.value(options, 'internal');
    if (typeof internal !== 'boolean') throw new TypeError(`internal must be a boolean, not ${kind(internal)}`);
    const partitions = FORMS.value(options, 'partitions');
    // Java's toString and equals read the list.
    if (partitions === null || typeof partitions !== 'object' || !(Symbol.iterator in partitions)) {
      throw new TypeError(`partitions must be an iterable of TopicPartitionInfo, not ${kind(partitions)}`);
    }
    const list = [...(partitions as Iterable<unknown>)];
    for (const p of list) {
      if (p === null) throw new TypeError('partitions must not contain null');
      if (!(p instanceof TopicPartitionInfo)) throw new TypeError(`partitions must contain only TopicPartitionInfo, not ${kind(p)}`);
    }
    const operations = form === 0 ? [] : FORMS.value(options, 'authorizedOperations');
    let operationSet: ReadonlySet<AclOperation> | null = null;
    if (operations !== null) {
      if (typeof operations !== 'object' || !(Symbol.iterator in operations)) {
        throw new TypeError(`authorizedOperations must be an iterable of AclOperation or null, not ${kind(operations)}`);
      }
      const members = [...(operations as Iterable<unknown>)];
      for (const op of members) {
        if (!(op instanceof AclOperation)) throw new TypeError(`authorizedOperations must contain only AclOperation, not ${kind(op)}`);
      }
      operationSet = readOnlySet(members as AclOperation[]);
    }
    const topicId = form === 2 ? FORMS.value(options, 'topicId') : Uuid.ZERO_UUID;
    if (topicId !== null && !(topicId instanceof Uuid)) throw new TypeError(`topicId must be a Uuid or null, not ${kind(topicId)}`);
    this.name = name;
    this.isInternal = internal;
    this.topicId = topicId;
    this.partitions = Object.freeze(list as TopicPartitionInfo[]);
    this.authorizedOperations = operationSet;
    Object.freeze(this);
  }

  /**
   * Whether `o` is a TopicDescription with an equal name, internal flag,
   * partitions and authorized operations (Java's equals leaves the topic id
   * out).
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as TopicDescription;
    return (
      this.isInternal === that.isInternal &&
      objectsEquals(this.name, that.name) &&
      listEquals(this.partitions, that.partitions) &&
      setEquals(this.authorizedOperations, that.authorizedOperations)
    );
  }

  /**
   * Java's `Objects.hash(name, internal, partitions, authorizedOperations)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    let h = 1;
    for (const part of [
      objectHashCode(this.name),
      objectHashCode(this.isInternal),
      listHashCode(this.partitions),
      setHashCode(this.authorizedOperations),
    ])
      h = (Math.imul(31, h) + part) | 0;
    return h;
  }

  /**
   * The description as JSON: its fields, the authorized operations an array
   * (null when not known).
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): Record<string, unknown> {
    return {
      name: this.name,
      isInternal: this.isInternal,
      topicId: this.topicId,
      partitions: this.partitions,
      authorizedOperations: this.authorizedOperations === null ? null : [...this.authorizedOperations],
    };
  }

  /**
   * Java's `toString()`: `(name=<n>, internal=<b>, partitions=<p,...>,
   * authorizedOperations=<set>)`.
   *
   * @returns the text
   */
  toString(): string {
    return (
      `(name=${String(this.name)}, internal=${String(this.isInternal)}, partitions=${this.partitions.map(String).join(',')}` +
      `, authorizedOperations=${javaString(this.authorizedOperations)})`
    );
  }
}
