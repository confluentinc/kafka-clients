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
// Java's org.apache.kafka.clients.admin.MemberDescription, a data class (root
// CLAUDE.md, JavaScript Binding Conventions, Class family; the admin
// descriptions, decided by the owner on 2026-11-20): Java's getters are
// readonly fields, set once in the constructor, and the object is frozen.
// Java's `Optional`s are `T | null` (Types). Only Java's constructor that is
// not deprecated is translated (root §3); equals, hashCode and toString are
// Java's.

import { JavaForms, kind } from '../internal/args';
import { checkInt } from '../internal/java_number';
import { objectHashCode, objectsEquals, sameClass } from '../internal/java_object';
import { MemberAssignment } from './member_assignment';

const FORMS = new JavaForms('MemberDescription', [
  ['memberId', 'groupInstanceId', 'rackId', 'clientId', 'host', 'assignment', 'targetAssignment', 'memberEpoch', 'upgraded'],
]);
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/**
 * A string or null, Java's `String` (or `Optional<String>`).
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
 * A detailed description of a single group member in the cluster.
 */
export class MemberDescription {
  /** The consumer id of the group member. */
  readonly consumerId: string;

  /** The instance id of the group member, or null. */
  readonly groupInstanceId: string | null;

  /**
   * The rack id of the group member, or null.
   *
   * It is only available for consumer groups using the new consumer group
   * protocol (`group.protocol=consumer`).
   */
  readonly rackId: string | null;

  /** The client id of the group member. */
  readonly clientId: string;

  /** The host where the group member is running. */
  readonly host: string;

  /** The assignment of the group member. Provided for both classic group and consumer group. */
  readonly assignment: MemberAssignment;

  /** The target assignment of the member, or null. Provided only for consumer group. */
  readonly targetAssignment: MemberAssignment | null;

  /**
   * The epoch of the group member. It is set to an integer if the member is
   * in a `GroupType.CONSUMER` group, and to null if it is in a
   * `GroupType.CLASSIC` group.
   */
  readonly memberEpoch: number | null;

  /**
   * The flag indicating whether a member within a `GroupType.CONSUMER` group
   * uses the `GroupType.CONSUMER` protocol. It is set to true if it does, to
   * false if it does not, and to null if it is unknown or if the group is a
   * `GroupType.CLASSIC` group.
   */
  readonly upgraded: boolean | null;

  /**
   * Creates an instance with the specified parameters.
   *
   * @param options - the member's fields (a null member id, client id or host
   *   is the empty string, a null assignment an empty one; null for each
   *   optional field Java's empty `Optional`)
   * @throws `IllegalArgumentError` when the given parameters are not Java's
   * @throws `TypeError` for a value of the wrong type
   */
  constructor(options: {
    /** The member id. */
    memberId: string | null;
    /** The group instance id, or null. */
    groupInstanceId: string | null;
    /** The rack id, or null. */
    rackId: string | null;
    /** The client id. */
    clientId: string | null;
    /** The host. */
    host: string | null;
    /** The assignment. */
    assignment: MemberAssignment | null;
    /** The target assignment, or null. */
    targetAssignment: MemberAssignment | null;
    /** The member epoch, or null. */
    memberEpoch: number | null;
    /** Whether the member uses the consumer protocol, or null. */
    upgraded: boolean | null;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const value = (name: string): unknown => FORMS.value(options, name);
    const assignment = value('assignment');
    if (assignment !== null && !(assignment instanceof MemberAssignment)) {
      throw new TypeError(`assignment must be a MemberAssignment or null, not ${kind(assignment)}`);
    }
    const target = value('targetAssignment');
    if (target !== null && !(target instanceof MemberAssignment)) {
      throw new TypeError(`targetAssignment must be a MemberAssignment or null, not ${kind(target)}`);
    }
    const epoch = value('memberEpoch');
    const upgraded = value('upgraded');
    if (upgraded !== null && typeof upgraded !== 'boolean')
      throw new TypeError(`upgraded must be a boolean or null, not ${kind(upgraded)}`);
    this.consumerId = stringOrNull(value('memberId'), 'memberId') ?? '';
    this.groupInstanceId = stringOrNull(value('groupInstanceId'), 'groupInstanceId');
    this.rackId = stringOrNull(value('rackId'), 'rackId');
    this.clientId = stringOrNull(value('clientId'), 'clientId') ?? '';
    this.host = stringOrNull(value('host'), 'host') ?? '';
    this.assignment = assignment ?? new MemberAssignment(null);
    this.targetAssignment = target;
    this.memberEpoch = epoch === null ? null : checkInt(epoch, 'memberEpoch');
    this.upgraded = upgraded;
    Object.freeze(this);
  }

  /**
   * Whether `o` is a MemberDescription with the same fields.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as MemberDescription;
    return (
      this.consumerId === that.consumerId &&
      this.groupInstanceId === that.groupInstanceId &&
      this.rackId === that.rackId &&
      this.clientId === that.clientId &&
      this.host === that.host &&
      this.assignment.equals(that.assignment) &&
      objectsEquals(this.targetAssignment, that.targetAssignment) &&
      this.memberEpoch === that.memberEpoch &&
      this.upgraded === that.upgraded
    );
  }

  /**
   * Java's `Objects.hash(memberId, groupInstanceId, rackId, clientId, host,
   * assignment, targetAssignment, memberEpoch, upgraded)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    let h = 1;
    for (const part of [
      this.consumerId,
      this.groupInstanceId,
      this.rackId,
      this.clientId,
      this.host,
      this.assignment,
      this.targetAssignment,
      this.memberEpoch,
      this.upgraded,
    ])
      h = (Math.imul(31, h) + objectHashCode(part)) | 0;
    return h;
  }

  /**
   * Java's `toString()`: `(memberId=m, groupInstanceId=null, rackId=null,
   * clientId=c, host=h, assignment=(topicPartitions=), targetAssignment=Optional.empty,
   * memberEpoch=null, upgraded=null)`.
   *
   * @returns the text
   */
  toString(): string {
    const target = this.targetAssignment === null ? 'Optional.empty' : `Optional[${this.targetAssignment.toString()}]`;
    return (
      `(memberId=${this.consumerId}, groupInstanceId=${String(this.groupInstanceId)}, rackId=${String(this.rackId)}` +
      `, clientId=${this.clientId}, host=${this.host}, assignment=${this.assignment.toString()}, targetAssignment=${target}` +
      `, memberEpoch=${String(this.memberEpoch)}, upgraded=${String(this.upgraded)})`
    );
  }

  /**
   * The description as JSON: its fields.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): Record<string, unknown> {
    return {
      consumerId: this.consumerId,
      groupInstanceId: this.groupInstanceId,
      rackId: this.rackId,
      clientId: this.clientId,
      host: this.host,
      assignment: this.assignment,
      targetAssignment: this.targetAssignment,
      memberEpoch: this.memberEpoch,
      upgraded: this.upgraded,
    };
  }
}
