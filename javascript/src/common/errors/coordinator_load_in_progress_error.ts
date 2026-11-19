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
// the Java source of `org.apache.kafka.common.errors.CoordinatorLoadInProgressException`;
// `cargo xtask check-generated` checks it is up to date.

import { setForms } from '../../internal/throwable';
import { RetriableError } from './retriable_error';

/**
 * In the context of the group coordinator, the broker returns this error code
 * for any coordinator request if it is still loading the group metadata (e.g.
 * after a leader change for that group metadata topic partition).
 *
 * In the context of the transactional coordinator, this error will be returned
 * if there is a pending transactional request with the same transactional id,
 * or if the transaction cache is currently being populated from the transaction
 * log.
 *
 * Java: `org.apache.kafka.common.errors.CoordinatorLoadInProgressException`.
 */
export class CoordinatorLoadInProgressError extends RetriableError {
  /**
   * Makes `CoordinatorLoadInProgressError` nominal: TypeScript checks it by
   * name, not by shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * The FFI id of the class, `kafka_common_Error_COORDINATOR_LOAD_IN_PROGRESS`,
   * for the binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = 14;

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

setForms(CoordinatorLoadInProgressError, [
  [['message'], 'message'],
  [['message', 'cause'], 'message'],
]);
