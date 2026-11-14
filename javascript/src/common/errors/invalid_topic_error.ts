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
// the Java source of `org.apache.kafka.common.errors.InvalidTopicException`;
// `cargo xtask check-generated` checks it is up to date.

import { javaString } from '../../internal/java_string';
import * as P from '../../internal/payload';
import { payloadOf, setForms } from '../../internal/throwable';
import { InvalidConfigurationError } from './invalid_configuration_error';

/**
 * The client has attempted to perform an operation on an invalid topic. For
 * example the topic name is too long, contains invalid characters etc. This
 * exception is not retriable because the operation won't suddenly become valid.
 *
 * Java: `org.apache.kafka.common.errors.InvalidTopicException`.
 */
export class InvalidTopicError extends InvalidConfigurationError {
  /**
   * Makes `InvalidTopicError` nominal: TypeScript checks it by name, not by
   * shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * Java's field `Set<String> invalidTopics`.
   */
  readonly #invalidTopics: Set<string> | null;

  /**
   * The FFI id of the class, `kafka_common_Error_INVALID_TOPIC_ERROR`, for the
   * binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = 17;

  /**
   * Java's no-argument constructor. The message is empty, Java's null.
   */
  constructor();
  /**
   * Java's `(String message, Throwable cause)` constructor.
   *
   * @param options - the message and the cause
   */
  constructor(options: { /** The message; `null` reads as `''`. */ message: string | null; /** The cause (`Error`'s `cause`). */ cause: unknown; name?: never });
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
  /**
   * Java's `(Throwable cause)` constructor. The message is the cause's
   * `String()`, as Java's is `cause.toString()` (`''` for a null cause).
   *
   * @param options - the cause
   */
  constructor(options: { /** The cause (`Error`'s `cause`). */ cause: unknown; name?: never });
  /**
   * Java's `(Throwable cause)` constructor. The message is the cause's
   * `String()`, as Java's is `cause.toString()` (`''` for a null cause). Given
   * the cause on its own, the same as `{ cause }`.
   *
   * @param cause - the cause (`Error`'s `cause`)
   */
  constructor(cause: Error);
  /**
   * Java's `(Set<String> invalidTopics)` constructor. The message is the one
   * Java's constructor builds from the payload.
   *
   * @param options - the `invalidTopics`
   */
  constructor(options: { /** Java's `Set<String> invalidTopics`. */ invalidTopics: Iterable<string>; name?: never });
  /**
   * Java's `(String message, Set<String> invalidTopics)` constructor.
   *
   * @param options - the message and the `invalidTopics`
   */
  constructor(options: { /** The message; `null` reads as `''`. */ message: string | null; /** Java's `Set<String> invalidTopics`. */ invalidTopics: Iterable<string>; name?: never });
  constructor(...args: unknown[]) {
    super(...(args as [{ message: string | null; cause: unknown; name?: never }]));
    const fields = payloadOf(this);
    this.#invalidTopics = fields.invalidTopics as Set<string> | null;
  }

  /**
   * Java's `invalidTopics()`.
   *
   * @returns Java's `invalidTopics`, as a new Set the caller may change @throws
   * `TypeError` when it is given an argument: it only reads
   */
  invalidTopics(): Set<string> {
    P.readOnly('invalidTopics', arguments.length);
    return P.stringSet.copy(this.#invalidTopics) as Set<string>;
  }
}

setForms(InvalidTopicError, [
  [[], null, () => ({ invalidTopics: new Set<string>() })],
  [['message', 'cause'], 'message', () => ({ invalidTopics: new Set<string>() })],
  [['message'], 'message', () => ({ invalidTopics: new Set<string>() })],
  [['cause'], 'cause.toString()', () => ({ invalidTopics: new Set<string>() })],
  [['invalidTopics'], (v) => 'Invalid topics: ' + javaString(v.invalidTopics), (v) => ({ invalidTopics: v.invalidTopics })],
  [['message', 'invalidTopics'], 'message', (v) => ({ invalidTopics: v.invalidTopics })],
], {
  order: ['message', 'cause', 'invalidTopics'],
  params: { invalidTopics: P.stringSet },
  fields: { invalidTopics: [P.stringSet, null] },
});
