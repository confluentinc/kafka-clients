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
// the Java source of `org.apache.kafka.common.errors.SslAuthenticationException`;
// `cargo xtask check-generated` checks it is up to date.

import { setForms } from '../../internal/throwable';
import { AuthenticationError } from './authentication_error';

/**
 * This exception indicates that SSL handshake has failed. See `getCause()` for
 * the `SSLException` that caused this failure.
 *
 * SSL handshake failures in clients may indicate client authentication failure
 * due to untrusted certificates if server is configured to request client
 * certificates. Handshake failures could also indicate misconfigured security
 * including protocol/cipher suite mismatch, server certificate authentication
 * failure or server host name verification failure.
 *
 * Java: `org.apache.kafka.common.errors.SslAuthenticationException`.
 */
export class SslAuthenticationError extends AuthenticationError {
  /**
   * Makes `SslAuthenticationError` nominal: TypeScript checks it by name, not
   * by shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * The FFI id of the class, `kafka_common_Error_SSL_AUTHENTICATION`, for the
   * binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = -16;

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
    super(...(args as [{ message: string | null; name?: never }]));
  }
}

setForms(SslAuthenticationError, [
  [['message'], 'message'],
  [['message', 'cause'], 'message'],
]);
