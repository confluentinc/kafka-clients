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
// the Java source of `org.apache.kafka.clients.consumer.RetriableCommitFailedException`;
// `cargo xtask check-generated` checks it is up to date.

import { setForms } from '../internal/throwable';
import { RetriableError } from '../common/errors/retriable_error';

/**
 * Exception thrown when an offset commit fails with a retriable error. This
 * exception is generated on the client side upon receiving retriable error
 * codes from the Group Coordinator in a commit response.
 *
 * Unlike `CommitFailedException`, this exception indicates that the commit can
 * be retried. The consumer should attempt to commit the offsets again.
 *
 * Java: `org.apache.kafka.clients.consumer.RetriableCommitFailedException`.
 */
export class RetriableCommitFailedError extends RetriableError {
  /**
   * Makes `RetriableCommitFailedError` nominal: TypeScript checks it by name,
   * not by shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * The FFI id of the class,
   * `kafka_common_Error_CONSUMER_RETRIABLE_COMMIT_FAILED`, for the binding and
   * the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = -23;

  /**
   * Java's `(Throwable cause)` constructor. The message is Java's own: "Offset
   * commit failed with a retriable exception. You should retry committing the
   * latest consumed offsets.".
   *
   * @param options - the cause
   */
  constructor(options: { /** The cause (`Error`'s `cause`). */ cause: unknown; name?: never });
  /**
   * Java's `(Throwable cause)` constructor. The message is Java's own: "Offset
   * commit failed with a retriable exception. You should retry committing the
   * latest consumed offsets.". Given the cause on its own, the same as `{ cause
   * }`.
   *
   * @param cause - the cause (`Error`'s `cause`)
   */
  constructor(cause: Error);
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
   * Java's `(String message, Throwable cause)` constructor.
   *
   * @param options - the message and the cause
   */
  constructor(options: { /** The message; `null` reads as `''`. */ message: string | null; /** The cause (`Error`'s `cause`). */ cause: unknown; name?: never });
  constructor(...args: unknown[]) {
    super(...(args as [{ message: string | null; cause: unknown; name?: never }]));
  }
}

setForms(RetriableCommitFailedError, [
  [['cause'], { text: 'Offset commit failed with a retriable exception. You should retry committing the latest consumed offsets.' }],
  [['message'], 'message'],
  [['message', 'cause'], 'message'],
]);
