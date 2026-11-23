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
// Java's org.apache.kafka.clients.admin.GroupListing. Its `Optional<GroupType>`
// and `Optional<GroupState>` are `GroupType | null` and `GroupState | null`
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Types); `equals`,
// `hashCode` and `toString` are Java's.

import { JavaForms, kind } from '../internal/args';
import { objectHashCode, objectsEquals } from '../internal/java_object';
import { GroupState } from '../common/group_state';
import { GroupType } from '../common/group_type';

const FORMS = new JavaForms('GroupListing', [['groupId', 'type', 'protocol', 'groupState']]);
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const HASH_CODE_FORMS = READER('hashCode');
const GROUP_ID_FORMS = READER('groupId');
const TYPE_FORMS = READER('type');
const PROTOCOL_FORMS = READER('protocol');
const GROUP_STATE_FORMS = READER('groupState');
const IS_SIMPLE_CONSUMER_GROUP_FORMS = READER('isSimpleConsumerGroup');

/**
 * A listing of a group in the cluster.
 */
export class GroupListing {
  readonly #groupId: string | null;
  readonly #type: GroupType | null;
  readonly #protocol: string | null;
  readonly #groupState: GroupState | null;

  /**
   * Create an instance with the specified parameters.
   *
   * @param options - the group id, the group type (null: not available), the
   *   protocol and the group state (null: not available)
   * @throws `IllegalArgumentError` when the given parameters are not Java's
   * @throws `TypeError` for a value of the wrong type
   */
  constructor(options: {
    /** Group Id. */
    groupId: string | null;
    /** Group type, or null when it is not available. */
    type: GroupType | null;
    /** Protocol. */
    protocol: string | null;
    /** Group state, or null when it is not available. */
    groupState: GroupState | null;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const groupId = FORMS.value(options, 'groupId');
    if (groupId !== null && typeof groupId !== 'string') throw new TypeError(`groupId must be a string or null, not ${kind(groupId)}`);
    const type = FORMS.value(options, 'type');
    if (type !== null && !(type instanceof GroupType)) throw new TypeError(`type must be a GroupType or null, not ${kind(type)}`);
    const protocol = FORMS.value(options, 'protocol');
    if (protocol !== null && typeof protocol !== 'string') throw new TypeError(`protocol must be a string or null, not ${kind(protocol)}`);
    const groupState = FORMS.value(options, 'groupState');
    if (groupState !== null && !(groupState instanceof GroupState)) {
      throw new TypeError(`groupState must be a GroupState or null, not ${kind(groupState)}`);
    }
    this.#groupId = groupId;
    this.#type = type;
    this.#protocol = protocol;
    this.#groupState = groupState;
    Object.freeze(this);
  }

  /**
   * The group Id.
   *
   * @returns Group Id
   * @throws `TypeError` when it is given an argument: it only reads
   */
  groupId(): string | null {
    if (arguments.length !== 0) GROUP_ID_FORMS.match(undefined, arguments.length);
    return this.#groupId;
  }

  /**
   * The type of the group.
   *
   * If the broker returns a group type which is not recognised, as might
   * happen when talking to a broker with a later version, the type will be
   * `GroupType.UNKNOWN`. If the broker is earlier than version 2.6.0, the
   * group type will not be available, and the type will be null.
   *
   * @returns the type, if available, else null
   * @throws `TypeError` when it is given an argument: it only reads
   */
  type(): GroupType | null {
    if (arguments.length !== 0) TYPE_FORMS.match(undefined, arguments.length);
    return this.#type;
  }

  /**
   * The protocol of the group.
   *
   * @returns The protocol
   * @throws `TypeError` when it is given an argument: it only reads
   */
  protocol(): string | null {
    if (arguments.length !== 0) PROTOCOL_FORMS.match(undefined, arguments.length);
    return this.#protocol;
  }

  /**
   * The group state.
   *
   * If the broker returns a group state which is not recognised, as might
   * happen when talking to a broker with a later version, the state will be
   * `GroupState.UNKNOWN`.
   *
   * @returns the state, if available, else null
   * @throws `TypeError` when it is given an argument: it only reads
   */
  groupState(): GroupState | null {
    if (arguments.length !== 0) GROUP_STATE_FORMS.match(undefined, arguments.length);
    return this.#groupState;
  }

  /**
   * If the group is a simple consumer group or not.
   *
   * @returns whether the group is a simple consumer group
   * @throws `TypeError` when it is given an argument: it only reads, or when
   *   the group is classic and its protocol null (Java dereferences it)
   */
  isSimpleConsumerGroup(): boolean {
    if (arguments.length !== 0) IS_SIMPLE_CONSUMER_GROUP_FORMS.match(undefined, arguments.length);
    if (this.#type !== GroupType.CLASSIC) return false;
    if (this.#protocol === null) throw new TypeError('protocol must not be null');
    return this.#protocol === '';
  }

  /**
   * Java's `toString()`: `(groupId='g', type=Classic, protocol='consumer',
   * groupState=Stable)`, `none` for a type or state that is not available.
   *
   * @returns the text
   */
  toString(): string {
    return (
      `(groupId='${String(this.#groupId)}', type=${this.#type === null ? 'none' : this.#type.toString()}, ` +
      `protocol='${String(this.#protocol)}', groupState=${this.#groupState === null ? 'none' : this.#groupState.toString()})`
    );
  }

  /**
   * Java's `Objects.hash(groupId, type, protocol, groupState)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    let h = 1;
    for (const part of [this.#groupId, this.#type, this.#protocol, this.#groupState]) h = (Math.imul(31, h) + objectHashCode(part)) | 0;
    return h;
  }

  /**
   * Whether `o` is a GroupListing with the same group id, type, protocol and state.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!(other instanceof GroupListing)) return false;
    return (
      objectsEquals(this.#groupId, other.#groupId) &&
      this.#type === other.#type &&
      objectsEquals(this.#protocol, other.#protocol) &&
      this.#groupState === other.#groupState
    );
  }

  /**
   * The listing as JSON: `{ groupId, type, protocol, groupState }`, the type
   * and state their constants' names, or null.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { groupId: string | null; type: GroupType | null; protocol: string | null; groupState: GroupState | null } {
    return { groupId: this.#groupId, type: this.#type, protocol: this.#protocol, groupState: this.#groupState };
  }
}
