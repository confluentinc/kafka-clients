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
// the Java source of `org.apache.kafka.common.errors.UnknownLeaderEpochException`;
// `cargo xtask check-generated` checks it is up to date.

import { setForms } from '../../internal/throwable';
import { RetriableError } from './retriable_error';

/**
 * The request contained a leader epoch which is larger than that on the broker
 * that received the request. This can happen if the client observes a metadata
 * update before it has been propagated to all brokers. Clients need not refresh
 * metadata before retrying.
 *
 * Java: `org.apache.kafka.common.errors.UnknownLeaderEpochException`.
 */
export class UnknownLeaderEpochError extends RetriableError {
  /**
   * Makes `UnknownLeaderEpochError` nominal: TypeScript checks it by name, not
   * by shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * The FFI id of the class, `kafka_common_Error_UNKNOWN_LEADER_EPOCH`, for the
   * binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = 75;

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

setForms(UnknownLeaderEpochError, [
  [['message'], 'message'],
  [['message', 'cause'], 'message'],
]);
