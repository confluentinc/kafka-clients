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
// Java's org.apache.kafka.clients.admin.ClassicGroupDescription, a data class
// (root CLAUDE.md, JavaScript Binding Conventions, Class family; the admin
// descriptions, decided by the owner on 2026-11-20): Java's getters are
// readonly fields, set once in the constructor, and the object is frozen; its
// members are a frozen array and its operations a read-only set. Java's two
// constructors are one, taking the union of their parameters; equals,
// hashCode and toString are Java's.

import { JavaForms, kind } from '../internal/args';
import { listEquals, listHashCode, objectHashCode, objectsEquals, sameClass, setEquals, setHashCode } from '../internal/java_object';
import { javaString } from '../internal/java_string';
import { readOnlySet } from '../internal/read_only_set';
import { AclOperation } from '../common/acl/acl_operation';
import { Node } from '../common/node';
import { MemberDescription } from './member_description';
import { ClassicGroupState } from '../common/classic_group_state';

const FORMS = new JavaForms('ClassicGroupDescription', [
  ['groupId', 'protocol', 'protocolData', 'members', 'state', 'coordinator'],
  ['groupId', 'protocol', 'protocolData', 'members', 'state', 'coordinator', 'authorizedOperations'],
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
 * A detailed description of a single classic group in the cluster.
 */
export class ClassicGroupDescription {
  /** The id of the classic group. */
  readonly groupId: string;

  /** The group protocol type. */
  readonly protocol: string | null;

  /**
   * The group protocol data. The meaning depends on the group protocol type.
   * For a classic consumer group, this is the partition assignor name. For a
   * classic connect group, this indicates which Connect protocols are enabled.
   */
  readonly protocolData: string;

  /**
   * If the group is a simple consumer group or not.
   *
   * @remarks *(deviation)* Java computes it when called, and a null protocol
   * throws `NullPointerException` there; a field is computed once, so a null
   * protocol reads as false. Why: the description is a data class (decided by
   * the owner on 2026-11-20).
   */
  readonly isSimpleConsumerGroup: boolean;

  /** A list of the members of the classic group. */
  readonly members: readonly MemberDescription[];

  /** The classic group state, or UNKNOWN if the state is too new for us to parse. */
  readonly state: ClassicGroupState | null;

  /** The classic group coordinator, or null if the coordinator is not known. */
  readonly coordinator: Node | null;

  /** authorizedOperations for this group, or null if that information is not known. */
  readonly authorizedOperations: ReadonlySet<AclOperation> | null;

  /**
   * Creates an instance with the specified parameters, its authorized
   * operations the empty set, as Java's shorter constructor passes.
   *
   * @param options - the group's fields (a null group id or protocol data is
   *   the empty string, null members none)
   */
  constructor(options: {
    /** The group id. */
    groupId: string | null;
    /** The protocol type. */
    protocol: string | null;
    /** The protocol data. */
    protocolData: string | null;
    /** The members, or null for none. */
    members: Iterable<MemberDescription> | null;
    /** The group state. */
    state: ClassicGroupState | null;
    /** The coordinator, or null. */
    coordinator: Node | null;
    authorizedOperations?: never;
  });
  /**
   * Creates an instance with the specified parameters.
   *
   * @param options - the group's fields (a null group id or protocol data is
   *   the empty string, null members none)
   */
  constructor(options: {
    /** The group id. */
    groupId: string | null;
    /** The protocol type. */
    protocol: string | null;
    /** The protocol data. */
    protocolData: string | null;
    /** The members, or null for none. */
    members: Iterable<MemberDescription> | null;
    /** The group state. */
    state: ClassicGroupState | null;
    /** The coordinator, or null. */
    coordinator: Node | null;
    /** The authorized operations, or null when not known. */
    authorizedOperations: Iterable<AclOperation> | null;
  });
  /**
   * Java's two constructors.
   *
   * @param options - the group's fields
   * @throws `IllegalArgumentError` when the given parameters are not Java's
   * @throws `TypeError` for a value of the wrong type
   */
  constructor(options: unknown) {
    const form = FORMS.match(options, arguments.length);
    const value = (name: string): unknown => FORMS.value(options, name);
    const state = value('state');
    if (state !== null && !(state instanceof ClassicGroupState)) {
      throw new TypeError(`state must be a ClassicGroupState or null, not ${kind(state)}`);
    }
    const protocol = stringOrNull(value('protocol'), 'protocol');
    this.groupId = stringOrNull(value('groupId'), 'groupId') ?? '';
    this.protocol = protocol;
    this.protocolData = stringOrNull(value('protocolData'), 'protocolData') ?? '';
    this.isSimpleConsumerGroup = protocol === '';
    this.members = membersOf(value('members'));
    this.state = state;
    this.coordinator = nodeOrNull(value('coordinator'));
    this.authorizedOperations = form === 0 ? readOnlySet<AclOperation>([]) : operationsOf(value('authorizedOperations'));
    Object.freeze(this);
  }

  /**
   * Whether `o` is a ClassicGroupDescription with the same fields.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as ClassicGroupDescription;
    return (
      this.groupId === that.groupId &&
      this.protocol === that.protocol &&
      this.protocolData === that.protocolData &&
      listEquals(this.members, that.members) &&
      this.state === that.state &&
      objectsEquals(this.coordinator, that.coordinator) &&
      setEquals(this.authorizedOperations, that.authorizedOperations)
    );
  }

  /**
   * Java's `Objects.hash(groupId, protocol, protocolData, members, state,
   * coordinator, authorizedOperations)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return hashOf([
      objectHashCode(this.groupId),
      objectHashCode(this.protocol),
      objectHashCode(this.protocolData),
      listHashCode(this.members),
      objectHashCode(this.state),
      objectHashCode(this.coordinator),
      setHashCode(this.authorizedOperations),
    ]);
  }

  /**
   * Java's `toString()`: `(groupId=g, protocol='consumer', protocolData=range,
   * members=..., state=Stable, coordinator=..., authorizedOperations=[])`.
   *
   * @returns the text
   */
  toString(): string {
    return (
      `(groupId=${this.groupId}, protocol='${String(this.protocol)}', protocolData=${this.protocolData}` +
      `, members=${this.members.map(String).join(',')}, state=${javaString(this.state)}` +
      `, coordinator=${javaString(this.coordinator)}, authorizedOperations=${javaString(this.authorizedOperations)})`
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
      protocol: this.protocol,
      protocolData: this.protocolData,
      isSimpleConsumerGroup: this.isSimpleConsumerGroup,
      members: this.members,
      state: this.state,
      coordinator: this.coordinator,
      authorizedOperations: this.authorizedOperations === null ? null : [...this.authorizedOperations],
    };
  }
}
