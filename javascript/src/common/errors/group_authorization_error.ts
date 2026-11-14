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

// GENERATED, DO NOT EDIT. Produced by `cargo xtask generate-error-codes` from
// the Java source of `org.apache.kafka.common.errors.GroupAuthorizationException`;
// `cargo xtask check-generated` checks it is up to date.

import { JavaForms } from '../../internal/args';
import { javaString } from '../../internal/java_string';
import * as P from '../../internal/payload';
import { payloadOf, setForms } from '../../internal/throwable';
import { AuthorizationError } from './authorization_error';

const FOR_GROUP_ID_FORMS = new JavaForms('forGroupId', [['groupId']], {
  bareTypes: { groupId: (value) => value === null || typeof value === 'string' },
});

/**
 * Java: `org.apache.kafka.common.errors.GroupAuthorizationException`.
 */
export class GroupAuthorizationError extends AuthorizationError {
  /**
   * Makes `GroupAuthorizationError` nominal: TypeScript checks it by name, not
   * by shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * Java's field `String groupId`.
   */
  readonly #groupId: string | null;

  /**
   * The FFI id of the class, `kafka_common_Error_GROUP_AUTHORIZATION_FAILED`,
   * for the binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = 30;

  /**
   * Java's `(String message, String groupId)` constructor.
   *
   * @param options - the message and the `groupId`
   */
  constructor(options: { /** The message; `null` reads as `''`. */ message: string | null; /** Java's `String groupId`. */ groupId: string | null; name?: never });
  /**
   * Java's `(String message)` constructor.
   *
   * @param options - the message
   */
  constructor(options: { /** The message; `null` reads as `''`. */ message: string | null; name?: never });
  /**
   * Java's `(String message)` constructor. Given the message on its own, the
   * same as `{ message }`.
   *
   * @param message - the message; `null` reads as `''`
   */
  constructor(message: string | null);
  constructor(...args: unknown[]) {
    super(...(args as [{ message: string | null; name?: never }]));
    const fields = payloadOf(this);
    this.#groupId = fields.groupId as string | null;
  }

  /**
   * Java's static `forGroupId(String groupId)`.
   *
   * @param groupId - Java's `groupId` @returns the error @throws
   * `IllegalArgumentError` when the given parameters are not `{groupId}`
   */
  static forGroupId(groupId: string | null): GroupAuthorizationError;
  /**
   * The same as `forGroupId(groupId)`, with the groupId named.
   *
   * @param options - the groupId @returns the error
   */
  static forGroupId(options: {
    /** Java's `String groupId`. */
    groupId: string | null;
  }): GroupAuthorizationError;
  static forGroupId(options: unknown): GroupAuthorizationError {
    FOR_GROUP_ID_FORMS.match(options, arguments.length);
    const groupId = FOR_GROUP_ID_FORMS.value(options, 'groupId');
    return new GroupAuthorizationError({ message: 'Not authorized to access group: ' + javaString(groupId), groupId: groupId as string | null });
  }

  /**
   * Java's `groupId()`.
   *
   * @returns Java's `groupId` @throws `TypeError` when it is given an argument:
   * it only reads
   */
  groupId(): string | null {
    P.readOnly('groupId', arguments.length);
    return this.#groupId;
  }
}

setForms(GroupAuthorizationError, [
  [['message', 'groupId'], 'message', (v) => ({ groupId: v.groupId })],
  [['message'], 'message', () => ({ groupId: null })],
], {
  order: ['message', 'groupId'],
  params: { groupId: P.string },
  fields: { groupId: [P.string, null] },
});
