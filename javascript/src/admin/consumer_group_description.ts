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
// Java's org.apache.kafka.clients.admin.ConsumerGroupDescription, a data class
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family; the admin
// descriptions, decided by the owner on 2026-11-20): Java's getters are
// readonly fields, set once in the constructor, and the object is frozen; its
// members are a frozen array and its operations a read-only set. Java's
// `Optional<Integer>`s are `number | null` (Types). Only Java's constructor
// that is not deprecated is translated, and not the deprecated `state()`
// (root §3); equals, hashCode and toString are Java's.

import { JavaForms, kind } from '../internal/args';
import { checkInt } from '../internal/java_number';
import { listEquals, listHashCode, objectHashCode, objectsEquals, sameClass, setEquals, setHashCode } from '../internal/java_object';
import { javaString } from '../internal/java_string';
import { readOnlySet } from '../internal/read_only_set';
import { AclOperation } from '../common/acl/acl_operation';
import { Node } from '../common/node';
import { MemberDescription } from './member_description';
import { GroupState } from '../common/group_state';
import { GroupType } from '../common/group_type';

const FORMS = new JavaForms('ConsumerGroupDescription', [
  [
    'groupId',
    'isSimpleConsumerGroup',
    'members',
    'partitionAssignor',
    'type',
    'groupState',
    'coordinator',
    'authorizedOperations',
    'groupEpoch',
    'targetAssignmentEpoch',
  ],
]);
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/**
 * A string or null.
 *
 * @param value - the value given
 * @param name - the parameter's name
 * @returns the string, or null
 */
function stringOrNull(value: unknown, name: string): string | null {
  if (value !== null && typeof value !== 'string') throw new TypeError(`${name} must be a string or null, not ${kind(value)}`);
  return value;
}

/**
 * The members, Java's `List.copyOf` (null: none).
 *
 * @param value - the members given
 * @returns a frozen array of them
 */
function membersOf(value: unknown): readonly MemberDescription[] {
  if (value === null) return Object.freeze([]);
  if (typeof value !== 'object' || !(Symbol.iterator in value)) {
    throw new TypeError(`members must be an iterable of MemberDescription or null, not ${kind(value)}`);
  }
  const list = [...(value as Iterable<unknown>)];
  for (const m of list) {
    // Java's List.copyOf refuses a null element.
    if (m === null) throw new TypeError('members must not contain null');
    if (!(m instanceof MemberDescription)) throw new TypeError(`members must contain only MemberDescription, not ${kind(m)}`);
  }
  return Object.freeze(list as MemberDescription[]);
}

/**
 * The authorized operations, a read-only set, or null.
 *
 * @param value - the operations given
 * @returns the set, or null
 */
function operationsOf(value: unknown): ReadonlySet<AclOperation> | null {
  if (value === null) return null;
  if (typeof value !== 'object' || !(Symbol.iterator in value)) {
    throw new TypeError(`authorizedOperations must be an iterable of AclOperation or null, not ${kind(value)}`);
  }
  const members = [...(value as Iterable<unknown>)];
  for (const op of members) {
    if (!(op instanceof AclOperation)) throw new TypeError(`authorizedOperations must contain only AclOperation, not ${kind(op)}`);
  }
  return readOnlySet(members as AclOperation[]);
}

/**
 * A node, or null.
 *
 * @param value - the node given
 * @returns the node, or null
 */
function nodeOrNull(value: unknown): Node | null {
  if (value !== null && !(value instanceof Node)) throw new TypeError(`coordinator must be a Node or null, not ${kind(value)}`);
  return value;
}

/**
 * Java's `Objects.hash` of the parts.
 *
 * @param parts - the hash codes
 * @returns the hash code
 */
function hashOf(parts: number[]): number {
  let h = 1;
  for (const part of parts) h = (Math.imul(31, h) + part) | 0;
  return h;
}

/**
 * A detailed description of a single consumer group in the cluster.
 */
export class ConsumerGroupDescription {
  /** The id of the consumer group. */
  readonly groupId: string;

  /** If consumer group is simple or not. */
  readonly isSimpleConsumerGroup: boolean;

  /** A list of the members of the consumer group. */
  readonly members: readonly MemberDescription[];

  /** The consumer group partition assignor. */
  readonly partitionAssignor: string;

  /**
   * The group type (or the protocol) of this consumer group. It defaults to
   * Classic if not provided by the server.
   */
  readonly type: GroupType | null;

  /** The group state, or UNKNOWN if the state is too new for us to parse. */
  readonly groupState: GroupState | null;

  /** The consumer group coordinator, or null if the coordinator is not known. */
  readonly coordinator: Node | null;

  /** authorizedOperations for this group, or null if that information is not known. */
  readonly authorizedOperations: ReadonlySet<AclOperation> | null;

  /**
   * The epoch of the consumer group. It is set to an integer if it is a
   * `GroupType.CONSUMER` group, and to null if it is a `GroupType.CLASSIC`
   * group.
   */
  readonly groupEpoch: number | null;

  /**
   * The epoch of the target assignment. It is set to an integer if it is a
   * `GroupType.CONSUMER` group, and to null if it is a `GroupType.CLASSIC`
   * group.
   */
  readonly targetAssignmentEpoch: number | null;

  /**
   * Creates an instance with the specified parameters.
   *
   * @param options - the group's fields (a null group id or partition
   *   assignor is the empty string, null members none; null for an epoch
   *   Java's empty `Optional`)
   * @throws `IllegalArgumentError` when the given parameters are not Java's
   * @throws `TypeError` for a value of the wrong type
   */
  constructor(options: {
    /** The group id. */
    groupId: string | null;
    /** Whether the group is a simple consumer group. */
    isSimpleConsumerGroup: boolean;
    /** The members, or null for none. */
    members: Iterable<MemberDescription> | null;
    /** The partition assignor. */
    partitionAssignor: string | null;
    /** The group type. */
    type: GroupType | null;
    /** The group state. */
    groupState: GroupState | null;
    /** The coordinator, or null. */
    coordinator: Node | null;
    /** The authorized operations, or null when not known. */
    authorizedOperations: Iterable<AclOperation> | null;
    /** The group epoch, or null. */
    groupEpoch: number | null;
    /** The target assignment epoch, or null. */
    targetAssignmentEpoch: number | null;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const value = (name: string): unknown => FORMS.value(options, name);
    const isSimple = value('isSimpleConsumerGroup');
    if (typeof isSimple !== 'boolean') throw new TypeError(`isSimpleConsumerGroup must be a boolean, not ${kind(isSimple)}`);
    const type = value('type');
    if (type !== null && !(type instanceof GroupType)) throw new TypeError(`type must be a GroupType or null, not ${kind(type)}`);
    const state = value('groupState');
    if (state !== null && !(state instanceof GroupState))
      throw new TypeError(`groupState must be a GroupState or null, not ${kind(state)}`);
    const groupEpoch = value('groupEpoch');
    const targetEpoch = value('targetAssignmentEpoch');
    this.groupId = stringOrNull(value('groupId'), 'groupId') ?? '';
    this.isSimpleConsumerGroup = isSimple;
    this.members = membersOf(value('members'));
    this.partitionAssignor = stringOrNull(value('partitionAssignor'), 'partitionAssignor') ?? '';
    this.type = type;
    this.groupState = state;
    this.coordinator = nodeOrNull(value('coordinator'));
    this.authorizedOperations = operationsOf(value('authorizedOperations'));
    this.groupEpoch = groupEpoch === null ? null : checkInt(groupEpoch, 'groupEpoch');
    this.targetAssignmentEpoch = targetEpoch === null ? null : checkInt(targetEpoch, 'targetAssignmentEpoch');
    Object.freeze(this);
  }

  /**
   * Whether `o` is a ConsumerGroupDescription with the same fields.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as ConsumerGroupDescription;
    return (
      this.isSimpleConsumerGroup === that.isSimpleConsumerGroup &&
      this.groupId === that.groupId &&
      listEquals(this.members, that.members) &&
      this.partitionAssignor === that.partitionAssignor &&
      this.type === that.type &&
      this.groupState === that.groupState &&
      objectsEquals(this.coordinator, that.coordinator) &&
      setEquals(this.authorizedOperations, that.authorizedOperations) &&
      this.groupEpoch === that.groupEpoch &&
      this.targetAssignmentEpoch === that.targetAssignmentEpoch
    );
  }

  /**
   * Java's `Objects.hash` of the fields, in Java's order.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return hashOf([
      objectHashCode(this.groupId),
      objectHashCode(this.isSimpleConsumerGroup),
      listHashCode(this.members),
      objectHashCode(this.partitionAssignor),
      objectHashCode(this.type),
      objectHashCode(this.groupState),
      objectHashCode(this.coordinator),
      setHashCode(this.authorizedOperations),
      objectHashCode(this.groupEpoch),
      objectHashCode(this.targetAssignmentEpoch),
    ]);
  }

  /**
   * Java's `toString()`: `(groupId=g, isSimpleConsumerGroup=false, members=..., partitionAssignor=...,
   * type=Consumer, groupState=Stable, coordinator=..., authorizedOperations=..., groupEpoch=..., targetAssignmentEpoch=...)`.
   *
   * @returns the text
   */
  toString(): string {
    return (
      `(groupId=${this.groupId}, isSimpleConsumerGroup=${String(this.isSimpleConsumerGroup)}` +
      `, members=${this.members.map(String).join(',')}, partitionAssignor=${this.partitionAssignor}` +
      `, type=${javaString(this.type)}, groupState=${javaString(this.groupState)}, coordinator=${javaString(this.coordinator)}` +
      `, authorizedOperations=${javaString(this.authorizedOperations)}, groupEpoch=${String(this.groupEpoch)}` +
      `, targetAssignmentEpoch=${String(this.targetAssignmentEpoch)})`
    );
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
      groupId: this.groupId,
      isSimpleConsumerGroup: this.isSimpleConsumerGroup,
      members: this.members,
      partitionAssignor: this.partitionAssignor,
      type: this.type,
      groupState: this.groupState,
      coordinator: this.coordinator,
      authorizedOperations: this.authorizedOperations === null ? null : [...this.authorizedOperations],
      groupEpoch: this.groupEpoch,
      targetAssignmentEpoch: this.targetAssignmentEpoch,
    };
  }
}
