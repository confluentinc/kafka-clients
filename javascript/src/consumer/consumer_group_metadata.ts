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

// Java's ConsumerGroupMetadata, a class with methods (javascript/CLAUDE.md,
// JavaScript Binding Conventions, Class family). Java deprecates its public
// constructors, so the constructor is private and the library builds it
// through `internalCreate`, from the fields the core gives. `groupInstanceId`
// is Java's `Optional<String>`, `string | null` (Types).

import { JavaForms } from '../internal/args';
import { objectHashCode, sameClass } from '../internal/java_object';

const GROUP_ID_FORMS = new JavaForms('groupId', [[]], { reader: true });
const GENERATION_ID_FORMS = new JavaForms('generationId', [[]], { reader: true });
const MEMBER_ID_FORMS = new JavaForms('memberId', [[]], { reader: true });
const GROUP_INSTANCE_ID_FORMS = new JavaForms('groupInstanceId', [[]], { reader: true });
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/**
 * A metadata struct containing the consumer group information.
 * Note: Any change to this class is considered public and requires a KIP.
 */
export class ConsumerGroupMetadata {
  readonly #groupId: string;

  readonly #generationId: number;

  readonly #memberId: string;

  readonly #groupInstanceId: string | null;

  /**
   * Made by the library only: instances come from the consumer's
   * `groupMetadata()`, as Java deprecates its public constructors.
   *
   * @param groupId - the group ID
   * @param generationId - the generation ID
   * @param memberId - the member ID
   * @param groupInstanceId - the group instance ID, or null
   */
  private constructor(groupId: string, generationId: number, memberId: string, groupInstanceId: string | null) {
    this.#groupId = groupId;
    this.#generationId = generationId;
    this.#memberId = memberId;
    this.#groupInstanceId = groupInstanceId;
  }

  /**
   * The library's own factory, for the group metadata the core gives.
   *
   * @param groupId - the group ID
   * @param generationId - the generation ID
   * @param memberId - the member ID
   * @param groupInstanceId - the group instance ID, or null for a dynamic member
   * @returns the metadata
   *
   * @internal
   */
  static internalCreate(groupId: string, generationId: number, memberId: string, groupInstanceId: string | null): ConsumerGroupMetadata {
    return new ConsumerGroupMetadata(groupId, generationId, memberId, groupInstanceId);
  }

  /**
   * Returns the consumer group ID.
   *
   * @returns The group ID
   * @throws `TypeError` when it is given an argument
   */
  groupId(): string {
    GROUP_ID_FORMS.match(undefined, arguments.length);
    return this.#groupId;
  }

  /**
   * Returns the generation ID of the consumer group.
   *
   * @returns The generation ID
   * @throws `TypeError` when it is given an argument
   */
  generationId(): number {
    GENERATION_ID_FORMS.match(undefined, arguments.length);
    return this.#generationId;
  }

  /**
   * Returns the member ID of this consumer.
   *
   * @returns The member ID
   * @throws `TypeError` when it is given an argument
   */
  memberId(): string {
    MEMBER_ID_FORMS.match(undefined, arguments.length);
    return this.#memberId;
  }

  /**
   * Returns the group instance ID if this is a static member.
   *
   * @returns The group instance ID, or null if this is a dynamic member
   * @throws `TypeError` when it is given an argument
   */
  groupInstanceId(): string | null {
    GROUP_INSTANCE_ID_FORMS.match(undefined, arguments.length);
    return this.#groupInstanceId;
  }

  /**
   * The metadata as Java writes it: `GroupMetadata(groupId = g, generationId =
   * 2, memberId = m, groupInstanceId = i)`, an empty instance ID for a dynamic
   * member.
   *
   * @returns the text
   */
  toString(): string {
    return (
      `GroupMetadata(groupId = ${this.#groupId}, generationId = ${this.#generationId}, ` +
      `memberId = ${this.#memberId}, groupInstanceId = ${this.#groupInstanceId ?? ''})`
    );
  }

  /**
   * Whether `o` is a `ConsumerGroupMetadata` of the same fields, Java's
   * `equals`.
   *
   * @param options - the object to compare with
   * @returns true when `o` is equal
   * @throws `IllegalArgumentError` when the given parameters are not `{o}`
   */
  equals(options: {
    /** The object to compare with. */
    o: unknown;
  }): boolean;
  /**
   * The same as `equals({o})`, with the object on its own; a plain object is
   * read as the options object.
   *
   * @param o - the object to compare with
   * @returns true when `o` is equal
   */
  equals(o: unknown): boolean;
  equals(options: unknown): boolean {
    EQUALS_FORMS.match(options, arguments.length);
    const o = EQUALS_FORMS.value(options, 'o');
    if (this === o) return true;
    if (o === null || o === undefined || !sameClass(this, o)) return false;
    const that = o as ConsumerGroupMetadata;
    return (
      this.#generationId === that.#generationId &&
      this.#groupId === that.#groupId &&
      this.#memberId === that.#memberId &&
      this.#groupInstanceId === that.#groupInstanceId
    );
  }

  /**
   * Java's `Objects.hash(groupId, generationId, memberId, groupInstanceId)`.
   *
   * @returns the hash code, a 32-bit integer
   * @throws `TypeError` when it is given an argument
   */
  hashCode(): number {
    HASH_CODE_FORMS.match(undefined, arguments.length);
    let h = 1;
    h = (Math.imul(31, h) + objectHashCode(this.#groupId)) | 0;
    h = (Math.imul(31, h) + this.#generationId) | 0;
    h = (Math.imul(31, h) + objectHashCode(this.#memberId)) | 0;
    // Optional.hashCode: the value's, 0 when empty.
    return (Math.imul(31, h) + objectHashCode(this.#groupInstanceId)) | 0;
  }

  /**
   * The metadata as `JSON.stringify` writes it: `{ groupId, generationId,
   * memberId, groupInstanceId }`. It ignores the key `JSON.stringify` passes.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { groupId: string; generationId: number; memberId: string; groupInstanceId: string | null } {
    return { groupId: this.#groupId, generationId: this.#generationId, memberId: this.#memberId, groupInstanceId: this.#groupInstanceId };
  }
}
