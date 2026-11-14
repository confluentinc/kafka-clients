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
// the Java source of `org.apache.kafka.common.errors.DuplicateResourceException`;
// `cargo xtask check-generated` checks it is up to date.

import * as P from '../../internal/payload';
import { payloadOf, setForms } from '../../internal/throwable';
import { ApiError } from './api_error';

/**
 * Exception thrown due to a request that illegally refers to the same resource
 * twice (for example, trying to both create and delete the same SCRAM
 * credential for a particular user in a single request).
 *
 * Java: `org.apache.kafka.common.errors.DuplicateResourceException`.
 */
export class DuplicateResourceError extends ApiError {
  /**
   * Makes `DuplicateResourceError` nominal: TypeScript checks it by name, not
   * by shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * Java's field `String resource`.
   */
  readonly #resource: string | null;

  /**
   * The FFI id of the class, `kafka_common_Error_DUPLICATE_RESOURCE`, for the
   * binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = 92;

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
  /**
   * Java's `(String resource, String message)` constructor.
   *
   * @param options - the `resource` and the message
   */
  constructor(options: { /** Java's `String resource`. */ resource: string | null; /** The message; `null` reads as `''`. */ message: string | null; name?: never });
  /**
   * Java's `(String resource, String message, Throwable cause)` constructor.
   *
   * @param options - the `resource` and the message and the cause
   */
  constructor(options: { /** Java's `String resource`. */ resource: string | null; /** The message; `null` reads as `''`. */ message: string | null; /** The cause (`Error`'s `cause`). */ cause: unknown; name?: never });
  constructor(...args: unknown[]) {
    super(...(args as [{ message: string | null; cause: unknown; name?: never }]));
    const fields = payloadOf(this);
    this.#resource = fields.resource as string | null;
  }

  /**
   * Java's `resource()`.
   *
   * @returns Java's `resource` @throws `TypeError` when it is given an
   * argument: it only reads
   */
  resource(): string | null {
    P.readOnly('resource', arguments.length);
    return this.#resource;
  }
}

setForms(DuplicateResourceError, [
  [['message'], 'message', () => ({ resource: null })],
  [['message', 'cause'], 'message', () => ({ resource: null })],
  [['resource', 'message'], 'message', (v) => ({ resource: v.resource })],
  [['resource', 'message', 'cause'], 'message', (v) => ({ resource: v.resource })],
], {
  order: ['resource', 'message', 'cause'],
  params: { resource: P.string },
  fields: { resource: [P.string, null] },
});
