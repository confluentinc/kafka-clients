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
// Java's org.apache.kafka.clients.admin.MemberToRemove: the group instance id
// of a member removeMembersFromConsumerGroup removes. `equals` and `hashCode`
// are Java's; Java has no `toString()` of its own. Its package-private
// `toMemberIdentity()` is the library's `internalKey()`, the key a
// MemberToRemoveSet holds it by.

import { JavaForms, kind } from '../internal/args';
import { objectHashCode } from '../internal/java_object';

const FORMS = new JavaForms('MemberToRemove', [['groupInstanceId']], {
  bareTypes: { groupInstanceId: (value) => value === null || typeof value === 'string' },
});
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });
const GROUP_INSTANCE_ID_FORMS = new JavaForms('groupInstanceId', [[]], { reader: true });

/**
 * A struct containing information about the member to be removed.
 */
export class MemberToRemove {
  readonly #groupInstanceId: string | null;

  /**
   * The member of the given group instance id.
   *
   * @param groupInstanceId - the group instance id, on its own or as `{ groupInstanceId }`
   * @throws `TypeError` when the id is neither a string nor null
   */
  constructor(groupInstanceId: string | null);
  /**
   * The same as `new MemberToRemove(groupInstanceId)`, the id given by name.
   *
   * @param options - the group instance id
   */
  constructor(options: {
    /** The group instance id. */
    groupInstanceId: string | null;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const id = FORMS.value(options, 'groupInstanceId');
    if (id !== null && typeof id !== 'string') throw new TypeError(`groupInstanceId must be a string or null, not ${kind(id)}`);
    this.#groupInstanceId = id;
    Object.freeze(this);
  }

  /**
   * The group instance id of the member.
   *
   * @returns the id
   * @throws `TypeError` when it is given an argument: it only reads
   */
  groupInstanceId(): string | null {
    if (arguments.length !== 0) GROUP_INSTANCE_ID_FORMS.match(undefined, arguments.length);
    return this.#groupInstanceId;
  }

  /**
   * Whether `o` is a MemberToRemove of the same group instance id.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   * @throws `TypeError` when this member's id is null (Java dereferences it)
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (!(other instanceof MemberToRemove)) return false;
    if (this.#groupInstanceId === null) throw new TypeError('groupInstanceId must not be null');
    return this.#groupInstanceId === other.#groupInstanceId;
  }

  /**
   * Java's `Objects.hash(groupInstanceId)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return (31 + objectHashCode(this.#groupInstanceId)) | 0;
  }

  /**
   * The member as JSON: `{ groupInstanceId }`.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { groupInstanceId: string | null } {
    return { groupInstanceId: this.#groupInstanceId };
  }

  /**
   * The key a MemberToRemoveSet holds this member by: the JSON array of its
   * group instance id, the field Java's `equals` compares.
   *
   * @returns the key
   * @internal
   */
  internalKey(): string {
    return JSON.stringify([this.#groupInstanceId]);
  }

  /**
   * Java's `Object.toString()` of this member, which Java's MemberToRemove
   * does not override: its class name, `@` and its hash code in hex, as the
   * collections and messages that print a member write it.
   *
   * @returns the text
   * @internal
   */
  internalObjectString(): string {
    return `org.apache.kafka.clients.admin.MemberToRemove@${(this.hashCode() >>> 0).toString(16)}`;
  }
}
