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
// the Java source of `org.apache.kafka.common.errors.ApplicationRecoverableException`;
// `cargo xtask check-generated` checks it is up to date.

import { setForms } from '../../internal/throwable';
import { ApiError } from './api_error';

/**
 * Indicates that the error is fatal to the producer, and the application needs
 * to restart the producer after handling the error. Depending on the
 * application, different recovery strategies (e.g., re-balancing task,
 * restoring from checkpoints) may be employed.
 *
 * Java: `org.apache.kafka.common.errors.ApplicationRecoverableException`.
 *
 * A catch-only base: constructing it throws `TypeError`.
 */
export abstract class ApplicationRecoverableError extends ApiError {
  /**
   * Makes `ApplicationRecoverableError` nominal: TypeScript checks it by name,
   * not by shape. It holds no state.
   */
  readonly #brand = true;

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
   * Java's no-argument constructor. The message is empty, Java's null.
   */
  constructor();
  constructor(...args: unknown[]) {
    if (new.target === ApplicationRecoverableError) {
      throw new TypeError('ApplicationRecoverableError is an abstract catch-only base; it is never thrown directly');
    }
    super(...(args as [{ message: string | null; cause: unknown; name?: never }]));
  }
}

setForms(ApplicationRecoverableError, [
  [['message', 'cause'], 'message'],
  [['message'], 'message'],
  [['cause'], 'cause.toString()'],
  [[], null],
]);
