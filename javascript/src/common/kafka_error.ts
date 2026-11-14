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
// the Java source of `org.apache.kafka.common.KafkaException`;
// `cargo xtask check-generated` checks it is up to date.

import { errorArgs, init, setForms } from '../internal/throwable';

/**
 * The base class of all other Kafka exceptions
 *
 * Java: `org.apache.kafka.common.KafkaException`.
 */
export class KafkaError extends Error {
  /**
   * The error's cause, Java's `getCause()`: set by `super()` (`Error`'s
   * `options.cause`), declared here for the targets whose `lib` predates
   * ES2022's `Error.cause`. A `declare` field, which emits nothing: a class
   * field would overwrite it.
   */
  declare readonly cause: unknown;

  /**
   * Makes `KafkaError` nominal: TypeScript checks it by name, not by shape. It
   * holds no state.
   */
  readonly #brand = true;

  /**
   * The FFI id of the class, `kafka_common_Error_UNKNOWN_SERVER_ERROR`, for the
   * binding and the test harness. A bare KafkaException is reported as
   * UNKNOWN_SERVER_ERROR; UnknownServerError owns that id in the id -> class
   * table.
   *
   * @internal
   */
  static readonly _ffiId: number = -1;

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
    super(...errorArgs(new.target, args));
    init(this, new.target);
  }
}

setForms(KafkaError, [
  [['message', 'cause'], 'message'],
  [['message'], 'message'],
  [['cause'], 'cause.toString()'],
  [[], null],
]);
