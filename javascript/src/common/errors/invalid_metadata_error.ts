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
// the Java source of `org.apache.kafka.common.errors.InvalidMetadataException`;
// `cargo xtask check-generated` checks it is up to date.

import { isCreate, setForms } from '../../internal/throwable';
import { RefreshRetriableError } from './refresh_retriable_error';

/**
 * An exception that may indicate the client's metadata is out of date
 *
 * Java: `org.apache.kafka.common.errors.InvalidMetadataException`.
 *
 * A catch-only base.
 */
export abstract class InvalidMetadataError extends RefreshRetriableError {
  /**
   * Makes `InvalidMetadataError` nominal: TypeScript checks it by name, not by
   * shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * Java's no-argument constructor. The message is empty, Java's null.
   *
   * @throws `TypeError` when `new.target` is `InvalidMetadataError` itself, a
   *   catch-only base
   */
  constructor();
  /**
   * Java's `(String message)` constructor.
   *
   * @param options - the message
   * @throws `TypeError` when `new.target` is `InvalidMetadataError` itself, a
   *   catch-only base
   */
  constructor(options: { /** The message; `null` reads as `''`. */ message: string | null; name?: never });
  /**
   * Java's `(String message)` constructor, given the message alone.
   *
   * @param message - the message; `null` reads as `''`
   * @throws `TypeError` when `new.target` is `InvalidMetadataError` itself, a
   *   catch-only base
   */
  constructor(message: string | null);
  /**
   * Java's `(String message, Throwable cause)` constructor.
   *
   * @param options - the message and the cause
   * @throws `TypeError` when `new.target` is `InvalidMetadataError` itself, a
   *   catch-only base
   */
  constructor(options: { /** The message; `null` reads as `''`. */ message: string | null; /** The cause (`Error`'s `cause`). */ cause: unknown; name?: never });
  /**
   * Java's `(Throwable cause)` constructor. The message is the cause's
   * `String()`, as Java's is `cause.toString()` (`''` for a null cause).
   *
   * @param options - the cause
   * @throws `TypeError` when `new.target` is `InvalidMetadataError` itself, a
   *   catch-only base
   */
  constructor(options: { /** The cause (`Error`'s `cause`). */ cause: unknown; name?: never });
  /**
   * Java's `(Throwable cause)` constructor, given the cause alone. The message
   * is the cause's `String()`, as Java's is `cause.toString()` (`''` for a null
   * cause).
   *
   * @param cause - the cause (`Error`'s `cause`)
   * @throws `TypeError` when `new.target` is `InvalidMetadataError` itself, a
   *   catch-only base
   */
  constructor(cause: Error);
  constructor(...args: unknown[]) {
    if (new.target === InvalidMetadataError && !isCreate(args)) {
      throw new TypeError('InvalidMetadataError is an abstract catch-only base; it is never thrown directly');
    }
    super(...(args as [{ message: string | null; cause: unknown; name?: never }]));
  }
}

setForms(InvalidMetadataError, [
  [[], null],
  [['message'], 'message'],
  [['message', 'cause'], 'message'],
  [['cause'], 'cause.toString()'],
]);
