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
// the Java source of `org.apache.kafka.common.requests.CorrelationIdMismatchException`;
// `cargo xtask check-generated` checks it is up to date.
//
// An error the core reports has no payload for `requestCorrelationId` (it is
// -1, a value Java's counter never assigns, as the Python client reads it) and
// `responseCorrelationId` (it is -1, a value Java's counter never assigns, as
// the Python client reads it): the FFI's view of the class has no accessor for
// it.

import { IllegalStateError } from '../../illegal_state_error';
import * as P from '../../internal/payload';
import { payloadOf, setForms } from '../../internal/throwable';

/**
 * Raised if the correlationId in a response header does not match the expected
 * value from the request header.
 *
 * Java: `org.apache.kafka.common.requests.CorrelationIdMismatchException`.
 */
export class CorrelationIdMismatchError extends IllegalStateError {
  /**
   * Makes `CorrelationIdMismatchError` nominal: TypeScript checks it by name,
   * not by shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * Java's field `int requestCorrelationId`.
   */
  readonly #requestCorrelationId: number;

  /**
   * Java's field `int responseCorrelationId`.
   */
  readonly #responseCorrelationId: number;

  /**
   * The FFI id of the class, `kafka_common_Error_CORRELATION_ID_MISMATCH`, for
   * the binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = -24;

  /**
   * Java's `(String message, int requestCorrelationId, int
   * responseCorrelationId)` constructor.
   *
   * @param options - the message and the `requestCorrelationId` and the
   * `responseCorrelationId`
   */
  constructor(options: { /** The message; `null` reads as `''`. */ message: string | null; /** Java's `int requestCorrelationId`. */ requestCorrelationId: number; /** Java's `int responseCorrelationId`. */ responseCorrelationId: number; name?: never });
  constructor(...args: unknown[]) {
    super(...(args as []));
    const fields = payloadOf(this);
    this.#requestCorrelationId = fields.requestCorrelationId as number;
    this.#responseCorrelationId = fields.responseCorrelationId as number;
  }

  /**
   * Java's `requestCorrelationId()`.
   *
   * @returns Java's `requestCorrelationId` @throws `TypeError` when it is given
   * an argument: it only reads
   */
  requestCorrelationId(): number {
    P.readOnly('requestCorrelationId', arguments.length);
    return this.#requestCorrelationId;
  }

  /**
   * Java's `responseCorrelationId()`.
   *
   * @returns Java's `responseCorrelationId` @throws `TypeError` when it is
   * given an argument: it only reads
   */
  responseCorrelationId(): number {
    P.readOnly('responseCorrelationId', arguments.length);
    return this.#responseCorrelationId;
  }
}

setForms(CorrelationIdMismatchError, [
  [['message', 'requestCorrelationId', 'responseCorrelationId'], 'message', (v) => ({ requestCorrelationId: v.requestCorrelationId, responseCorrelationId: v.responseCorrelationId })],
], {
  order: ['message', 'requestCorrelationId', 'responseCorrelationId'],
  params: { requestCorrelationId: P.int, responseCorrelationId: P.int },
  fields: { requestCorrelationId: [P.int, -1], responseCorrelationId: [P.int, -1] },
});
