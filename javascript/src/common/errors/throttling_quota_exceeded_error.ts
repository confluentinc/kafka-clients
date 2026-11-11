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
// the Java source of `org.apache.kafka.common.errors.ThrottlingQuotaExceededException`;
// `cargo xtask check-generated` checks it is up to date.
//
// Not generated: Java's constructors that take a payload, which come with the
// payload getters: `ThrottlingQuotaExceededException(int, String)`.

import { setForms } from '../../internal/throwable';
import { RetriableError } from './retriable_error';

/**
 * Exception thrown if an operation on a resource exceeds the throttling quota.
 *
 * Java: `org.apache.kafka.common.errors.ThrottlingQuotaExceededException`.
 */
export class ThrottlingQuotaExceededError extends RetriableError {
  /**
   * Makes `ThrottlingQuotaExceededError` nominal: TypeScript checks it by name,
   * not by shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * The FFI id of the class, `kafka_common_Error_THROTTLING_QUOTA_EXCEEDED`,
   * for the binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = 89;

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
    super(...(args as [{ message: string | null; cause: unknown; name?: never }]));
  }
}

setForms(ThrottlingQuotaExceededError, [
  [['message'], 'message'],
]);
