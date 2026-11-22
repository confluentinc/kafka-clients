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
// Java's org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions.
// Java builds it through its two constructors, `()` (remove every member) and
// `(members)`, which stay its only forms, as Python's; its setters are
// `timeoutMs`, inherited, and `reason(String)`, which returns nothing, as
// Java's `void`. Java's `Set<MemberToRemove>` is a MemberToRemoveSet
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Types, Q25; decided by the owner
// on 2026-11-20). The members are not a setter: the client hands them to the
// addon apart from the setters (`internalMembers`), which builds the native
// options with Java's `(members)` constructor.

import { JavaForms, kind } from '../internal/args';
import { illegalArgument } from '../internal/errors';
import { GetterSetter } from '../internal/options';
import { AbstractOptions } from './abstract_options';
import { MemberToRemove } from './member_to_remove';
import { MemberToRemoveSet } from './member_to_remove_set';

const FORMS = new JavaForms('RemoveMembersFromConsumerGroupOptions', [[], ['members']], {
  bareTypes: { members: (value) => value === null || typeof value === 'string' || (typeof value === 'object' && Symbol.iterator in value) },
});
const REASON = new GetterSetter('reason', (value) => value === null || typeof value === 'string');
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const MEMBERS_FORMS = READER('members');
const REMOVE_ALL_FORMS = READER('removeAll');

/**
 * Options for `Admin.removeMembersFromConsumerGroup(groupId, options)`. It
 * carries the members to be removed from the consumer group.
 */
export class RemoveMembersFromConsumerGroupOptions extends AbstractOptions {
  readonly #members: Array<MemberToRemove | null>;
  #reason: string | null = null;

  /**
   * Options removing every member of the group (`removeAll()` mode).
   */
  constructor();
  /**
   * Options removing the given members.
   *
   * @param members - the members to remove, on its own
   * @throws `IllegalArgumentError` "Invalid empty members has been provided"
   *   for no member
   * @throws `TypeError` when it is null, a string, or not an iterable of MemberToRemove
   */
  constructor(members: Iterable<MemberToRemove>);
  /**
   * The same as `new RemoveMembersFromConsumerGroupOptions(members)`.
   *
   * @param options - the members to remove
   */
  constructor(options: {
    /** The members to remove. */
    members: Iterable<MemberToRemove>;
  });
  constructor(options?: unknown) {
    super();
    const form = FORMS.match(options, arguments.length);
    const members: Array<MemberToRemove | null> = [];
    if (form === 1) {
      const given = FORMS.value(options, 'members');
      // Java calls members.isEmpty().
      if (given === null) throw new TypeError('members must not be null');
      if (typeof given === 'string' || typeof given !== 'object' || !(Symbol.iterator in given)) {
        throw new TypeError(`members must be an iterable of MemberToRemove, not ${kind(given)}`);
      }
      const set = new MemberToRemoveSet();
      for (const m of given as Iterable<unknown>) {
        if (m !== null && !(m instanceof MemberToRemove)) throw new TypeError(`members must contain only MemberToRemove, not ${kind(m)}`);
        if (!set.has(m)) {
          set.add(m);
          members.push(m);
        }
      }
      if (members.length === 0) throw illegalArgument('Invalid empty members has been provided');
    }
    this.#members = members;
  }

  /**
   * The optional reason.
   *
   * @returns the reason, or null
   */
  reason(): string | null;
  /**
   * Sets an optional reason.
   *
   * @param reason - the reason, or null, on its own
   * @throws `TypeError` when the value is neither a string nor null
   */
  reason(reason: string | null): void;
  /**
   * The same as `reason(reason)`.
   *
   * @param options - the reason, or null
   * @throws `TypeError` when the value is neither a string nor null
   */
  reason(options: {
    /** The reason, or null. */
    reason: string | null;
  }): void;
  /**
   * Reads or sets the reason.
   *
   * @param options - the value to set, or nothing to read it
   * @returns the reason, or nothing once set
   * @throws `TypeError` when the value is neither a string nor null
   */
  reason(options?: unknown): string | null | void {
    if (!REASON.sets(options, arguments.length)) return this.#reason;
    const value = REASON.value(options);
    if (value !== null && typeof value !== 'string') throw new TypeError(`reason must be a string or null, not ${kind(value)}`);
    this.#reason = value;
    this.internalRecord('reason', value);
  }

  /**
   * The members to remove.
   *
   * @returns a new set of the members, empty in `removeAll()` mode
   * @throws `TypeError` when it is given an argument: it only reads
   */
  members(): MemberToRemoveSet {
    if (arguments.length !== 0) MEMBERS_FORMS.match(undefined, arguments.length);
    return new MemberToRemoveSet(this.#members);
  }

  /**
   * Whether every member of the group is removed: no member was given.
   *
   * @returns true in `removeAll()` mode
   * @throws `TypeError` when it is given an argument: it only reads
   */
  removeAll(): boolean {
    if (arguments.length !== 0) REMOVE_ALL_FORMS.match(undefined, arguments.length);
    return this.#members.length === 0;
  }

  /**
   * The members, for the addon, or null in `removeAll()` mode. Called by
   * the admin client.
   *
   * @returns the members, or null
   * @internal
   */
  internalMembers(): Array<MemberToRemove | null> | null {
    return this.#members.length === 0 ? null : [...this.#members];
  }
}
