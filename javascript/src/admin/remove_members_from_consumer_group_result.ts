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
// Java's org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupResult.
// Its Java constructor is package-private, over a future of the internal
// `Map<MemberIdentity, Errors>` and the requested members: a private
// constructor here, built by `internalCreate`, over a future of each member's
// error or null (Java's `Errors.NONE`) keyed by its group instance id (a
// MemberToRemove's identity: Java's `toMemberIdentity()` carries only it), its
// logic Java's; or, for the client, by `internalOfClient`, over the core's own
// `memberResult(member)` and `all()` futures, which carry Java's outcomes and
// messages, as Python's.

import { JavaForms, kind } from '../internal/args';
import { illegalArgument } from '../internal/errors';
import { create } from '../internal/throwable';
import { KafkaFuture } from '../common/kafka_future';
import { KafkaError } from '../common/kafka_error';
import { MemberToRemove } from './member_to_remove';
import type { MemberToRemoveSet } from './member_to_remove_set';

const MEMBER_RESULT_FORMS = new JavaForms('memberResult', [['member']], {
  bareTypes: { member: (value) => value === null || value instanceof MemberToRemove },
});
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });
// Java's null, the value of a KafkaFuture<Void>.
const VOID = null as unknown as void;

/**
 * Java's generated `MemberIdentity.toString()` of a member to remove: an
 * unknown member id, its group instance id and no reason.
 *
 * @param groupInstanceId - the group instance id
 * @returns the text
 */
function memberIdentityString(groupInstanceId: string | null): string {
  return `MemberIdentity(memberId='', groupInstanceId=${groupInstanceId === null ? 'null' : `'${groupInstanceId}'`}, reason=null)`;
}

/** The core's own futures. */
interface ClientFutures {
  memberResult(member: MemberToRemove): KafkaFuture<void>;
  all(): KafkaFuture<void>;
}

/**
 * The result of the `Admin.removeMembersFromConsumerGroup(groupId, options)`
 * call.
 */
export class RemoveMembersFromConsumerGroupResult {
  readonly #future: KafkaFuture<Map<string | null, Error | null>> | null;
  readonly #memberInfos: MemberToRemoveSet;
  readonly #client: ClientFutures | null;

  /**
   * Instances come from the admin client's `removeMembersFromConsumerGroup()`.
   *
   * @param future - the future of each member's error, or null
   * @param memberInfos - the members of the request (none: every member)
   * @param client - the core's futures, or null for Java's composition
   */
  private constructor(
    future: KafkaFuture<Map<string | null, Error | null>> | null,
    memberInfos: MemberToRemoveSet,
    client: ClientFutures | null,
  ) {
    this.#future = future;
    this.#memberInfos = memberInfos;
    this.#client = client;
  }

  /**
   * Java's package-private constructor, the members keyed by their group
   * instance id, the errors null for Java's `Errors.NONE`.
   *
   * @param future - the future of each member's error, or null
   * @param memberInfos - the members of the request (none: every member)
   * @returns the result
   * @internal
   */
  static internalCreate(
    future: KafkaFuture<Map<string | null, Error | null>>,
    memberInfos: MemberToRemoveSet,
  ): RemoveMembersFromConsumerGroupResult {
    return new RemoveMembersFromConsumerGroupResult(future, memberInfos, null);
  }

  /**
   * The result over the core's own futures, each read when asked for.
   * Called by the admin client.
   *
   * @param memberResult - the core's `memberResult(member)`
   * @param all - the core's `all()`
   * @param memberInfos - the members of the request
   * @returns the result
   * @internal
   */
  static internalOfClient(
    memberResult: (member: MemberToRemove) => KafkaFuture<void>,
    all: () => KafkaFuture<void>,
    memberInfos: MemberToRemoveSet,
  ): RemoveMembersFromConsumerGroupResult {
    return new RemoveMembersFromConsumerGroupResult(null, memberInfos, { memberResult, all });
  }

  /**
   * Returns a future which indicates whether the request was 100% success,
   * i.e. no either top level or member level error. If not, the first member
   * error shall be returned.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<void> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    if (this.#client !== null) return this.#client.all();
    const result = KafkaFuture.internalCreate<void>(false);
    (this.#future as KafkaFuture<Map<string | null, Error | null>>).whenComplete((memberErrors, throwable) => {
      if (throwable !== null && throwable !== undefined) {
        result.internalComplete(VOID, throwable);
        return;
      }
      const errors = memberErrors as Map<string | null, Error | null>;
      if (this.#memberInfos.size() === 0) {
        for (const [member, error] of errors) {
          if (error !== null) {
            const message = `Encounter exception when trying to remove: ${memberIdentityString(member)}`;
            result.internalComplete(VOID, create(KafkaError, message, error));
            return;
          }
        }
      } else {
        for (const member of this.#memberInfos) {
          if (maybeCompleteWithError(errors, member.groupInstanceId(), result)) return;
        }
      }
      result.internalComplete(VOID, null);
    });
    return result;
  }

  /**
   * Returns the selected member future.
   *
   * @param member - the member, on its own or as `{ member }`
   * @returns the future
   * @throws `IllegalArgumentError` in `removeAll` mode, or when the member was
   *   not included in the original request
   */
  memberResult(member: MemberToRemove): KafkaFuture<void>;
  /**
   * The same as `memberResult(member)`.
   *
   * @param options - the member
   * @returns the future
   */
  memberResult(options: {
    /** The member. */
    member: MemberToRemove;
  }): KafkaFuture<void>;
  memberResult(options: unknown): KafkaFuture<void> {
    MEMBER_RESULT_FORMS.match(options, arguments.length);
    const member = MEMBER_RESULT_FORMS.value(options, 'member');
    if (member !== null && !(member instanceof MemberToRemove)) throw new TypeError(`member must be a MemberToRemove, not ${kind(member)}`);
    // Java's two checks, also on a client's Result for a null member or one
    // whose group instance id is null: the C API cannot take either, and the
    // request holds neither (the call refuses such a member).
    if (member === null || this.#client === null || member.groupInstanceId() === null) {
      if (this.#memberInfos.size() === 0) throw illegalArgument("The method: memberResult is not applicable in 'removeAll' mode");
      if (!this.#memberInfos.has(member)) {
        const text = member === null ? 'null' : member.internalObjectString();
        throw illegalArgument(`Member ${text} was not included in the original request`);
      }
    }
    if (this.#client !== null) return this.#client.memberResult(member as MemberToRemove);
    const id = (member as MemberToRemove).groupInstanceId();
    const result = KafkaFuture.internalCreate<void>(false);
    (this.#future as KafkaFuture<Map<string | null, Error | null>>).whenComplete((memberErrors, throwable) => {
      if (throwable !== null && throwable !== undefined) result.internalComplete(VOID, throwable);
      else if (!maybeCompleteWithError(memberErrors as Map<string | null, Error | null>, id, result)) result.internalComplete(VOID, null);
    });
    return result;
  }
}

/**
 * Java's `maybeCompleteExceptionally`, through `KafkaAdminClient.getSubLevelError`.
 *
 * @param errors - each member's error, or null, by group instance id
 * @param member - the member's group instance id
 * @param result - the future to fail
 * @returns whether it failed the future
 */
function maybeCompleteWithError(errors: Map<string | null, Error | null>, member: string | null, result: KafkaFuture<void>): boolean {
  let error: Error | null;
  if (!errors.has(member)) {
    error = illegalArgument(`Member "${memberIdentityString(member)}" was not included in the removal response`);
  } else {
    error = errors.get(member) ?? null;
  }
  if (error === null) return false;
  result.internalComplete(VOID, error);
  return true;
}
