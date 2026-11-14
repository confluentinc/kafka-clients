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
// the Java source of `org.apache.kafka.common.errors.AuthenticationException`;
// `cargo xtask check-generated` checks it is up to date.

import { setForms } from '../../internal/throwable';
import { InvalidConfigurationError } from './invalid_configuration_error';

/**
 * This exception indicates that SASL authentication has failed. On
 * authentication failure, clients abort the operation requested and raise one
 * of the subclasses of this exception:
 *
 * `SaslAuthenticationException` if SASL handshake fails with invalid
 * credentials or any other failure specific to the SASL mechanism used for
 * authentication `UnsupportedSaslMechanismException` if the SASL mechanism
 * requested by the client is not supported on the broker.
 * `IllegalSaslStateException` if an unexpected request is received on during
 * SASL handshake. This could be due to misconfigured security protocol.
 * `SslAuthenticationException` if SSL handshake failed due to any
 * `SSLException`.
 *
 * Java: `org.apache.kafka.common.errors.AuthenticationException`.
 */
export class AuthenticationError extends InvalidConfigurationError {
  /**
   * Makes `AuthenticationError` nominal: TypeScript checks it by name, not by
   * shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * The FFI id of the class, `kafka_common_Error_AUTHENTICATION`, for the
   * binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = -7;

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
   * Java's `(String message, Throwable cause)` constructor.
   *
   * @param options - the message and the cause
   */
  constructor(options: { /** The message; `null` reads as `''`. */ message: string | null; /** The cause (`Error`'s `cause`). */ cause: unknown; name?: never });
  constructor(...args: unknown[]) {
    super(...(args as [{ message: string | null; cause: unknown; name?: never }]));
  }
}

setForms(AuthenticationError, [
  [['message'], 'message'],
  [['cause'], 'cause.toString()'],
  [['message', 'cause'], 'message'],
]);
