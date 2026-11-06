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
// the Java source of `java.lang.IllegalArgumentException`;
// `cargo xtask check-generated` checks it is up to date.

import { init, messageOf, optionsOf, setFfiId } from './_throwable';

/**
 * Java's built-in `java.lang.IllegalArgumentException`.
 */
export class IllegalArgumentError extends Error {
  /**
   * The `kafka_common_Error_e` value of the error: for an error the client
   * reports, the core's; for one constructed here, its class's (`-1`,
   * `UNKNOWN_SERVER_ERROR`, for a class without one).
   */
  readonly code!: number;

  /**
   * The `kafka_common_Error_e` enumerator name of `code`
   * (`SASL_AUTHENTICATION_FAILED`, `LOCAL_ILLEGAL_STATE`, ...); `null` for a
   * class the core has no value for.
   */
  readonly errorName!: string | null;

  /**
   * Java's `instanceof RetriableException`.
   */
  readonly retriable!: boolean;

  /**
   * Java's `instanceof ApiException`.
   */
  readonly apiError!: boolean;

  /**
   * The error's cause, Java's `getCause()`: set by `super()` (`Error`'s
   * `options.cause`), declared here for the targets whose `lib` predates
   * ES2022's `Error.cause`. A `declare` field, which emits nothing: a class
   * field would overwrite it.
   */
  declare readonly cause: unknown;

  /**
   * Java's constructors as JavaScript's `Error`'s: `message` is Java's
   * message (`null` reads as `''`), `options.cause` its cause. Given a cause
   * and no message, the message is the cause's `String()`, as Java's
   * `(Throwable cause)` constructor takes `cause.toString()`.
   */
  constructor(message?: string | null, options?: { cause?: unknown }) {
    super(messageOf(message, options), optionsOf(options));
    init(this, new.target);
  }
}

setFfiId(IllegalArgumentError, -3); // kafka_common_Error_LOCAL_ILLEGAL_ARGUMENT
