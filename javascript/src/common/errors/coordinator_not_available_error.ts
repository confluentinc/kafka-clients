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
// the Java source of `org.apache.kafka.common.errors.CoordinatorNotAvailableException`;
// `cargo xtask check-generated` checks it is up to date.

import { setForms } from '../../internal/throwable';
import { RefreshRetriableError } from './refresh_retriable_error';

/**
 * In the context of the group coordinator, the broker returns this error code
 * for metadata or offset commit requests if the group metadata topic has not
 * been created yet.
 *
 * In the context of the transactional coordinator, this error will be returned
 * if the underlying transactional log is under replicated or if an append to
 * the log times out.
 *
 * Java: `org.apache.kafka.common.errors.CoordinatorNotAvailableException`.
 */
export class CoordinatorNotAvailableError extends RefreshRetriableError {
  /**
   * Makes `CoordinatorNotAvailableError` nominal: TypeScript checks it by name,
   * not by shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * The FFI id of the class, `kafka_common_Error_COORDINATOR_NOT_AVAILABLE`,
   * for the binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = 15;

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

setForms(CoordinatorNotAvailableError, [
  [['message'], 'message'],
  [['message', 'cause'], 'message'],
]);
