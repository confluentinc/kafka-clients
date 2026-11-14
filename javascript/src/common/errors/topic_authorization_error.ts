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
// the Java source of `org.apache.kafka.common.errors.TopicAuthorizationException`;
// `cargo xtask check-generated` checks it is up to date.

import { javaString } from '../../internal/java_string';
import * as P from '../../internal/payload';
import { payloadOf, setForms } from '../../internal/throwable';
import { AuthorizationError } from './authorization_error';

/**
 * Java: `org.apache.kafka.common.errors.TopicAuthorizationException`.
 */
export class TopicAuthorizationError extends AuthorizationError {
  /**
   * Makes `TopicAuthorizationError` nominal: TypeScript checks it by name, not
   * by shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * Java's field `Set<String> unauthorizedTopics`.
   */
  readonly #unauthorizedTopics: Set<string> | null;

  /**
   * The FFI id of the class, `kafka_common_Error_TOPIC_AUTHORIZATION_FAILED`,
   * for the binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = 29;

  /**
   * Java's `(String message, Set<String> unauthorizedTopics)` constructor.
   *
   * @param options - the message and the `unauthorizedTopics`
   */
  constructor(options: { /** The message; `null` reads as `''`. */ message: string | null; /** Java's `Set<String> unauthorizedTopics`. */ unauthorizedTopics: Iterable<string>; name?: never });
  /**
   * Java's `(Set<String> unauthorizedTopics)` constructor. The message is the
   * one Java's constructor builds from the payload.
   *
   * @param options - the `unauthorizedTopics`
   */
  constructor(options: { /** Java's `Set<String> unauthorizedTopics`. */ unauthorizedTopics: Iterable<string>; name?: never });
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
    this.#unauthorizedTopics = fields.unauthorizedTopics as Set<string> | null;
  }

  /**
   * Java's `unauthorizedTopics()`.
   *
   * @returns Java's `unauthorizedTopics`, as a new Set the caller may change
   * @throws `TypeError` when it is given an argument: it only reads
   */
  unauthorizedTopics(): Set<string> {
    P.readOnly('unauthorizedTopics', arguments.length);
    return P.stringSet.copy(this.#unauthorizedTopics) as Set<string>;
  }
}

setForms(TopicAuthorizationError, [
  [['message', 'unauthorizedTopics'], 'message', (v) => ({ unauthorizedTopics: v.unauthorizedTopics })],
  [['unauthorizedTopics'], (v) => 'Not authorized to access topics: ' + javaString(v.unauthorizedTopics), (v) => ({ unauthorizedTopics: v.unauthorizedTopics })],
  [['message'], 'message', () => ({ unauthorizedTopics: new Set<string>() })],
], {
  order: ['message', 'unauthorizedTopics'],
  params: { unauthorizedTopics: P.stringSet },
  fields: { unauthorizedTopics: [P.stringSet, null] },
});
