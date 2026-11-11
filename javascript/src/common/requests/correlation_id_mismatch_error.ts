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
// Not generated: Java's constructors that take a payload, which come with the
// payload getters: `CorrelationIdMismatchException(String, int, int)`.

import { setForms } from '../../internal/throwable';
import { IllegalStateError } from '../../illegal_state_error';

/**
 * Raised if the correlationId in a response header does not match the expected
 * value from the request header.
 *
 * Java: `org.apache.kafka.common.requests.CorrelationIdMismatchException`.
 *
 * Not constructible yet: Java's constructors take a payload the client does not
 * translate yet; the client builds this error when the core reports it.
 */
export class CorrelationIdMismatchError extends IllegalStateError {
  /**
   * Makes `CorrelationIdMismatchError` nominal: TypeScript checks it by name,
   * not by shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * The FFI id of the class, `kafka_common_Error_CORRELATION_ID_MISMATCH`, for
   * the binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = -24;

  /**
   * Not constructible yet: Java's constructors take a payload the client does
   * not translate yet.
   *
   * @param args - refused: constructing the error throws `TypeError`
   */
  protected constructor(...args: unknown[]) {
    super(...(args as []));
  }
}

setForms(CorrelationIdMismatchError, []);
