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
// the Java source of `org.apache.kafka.clients.producer.BufferExhaustedException`;
// `cargo xtask check-generated` checks it is up to date.

import { setForms } from '../internal/throwable';
import { TimeoutError } from '../common/errors/timeout_error';

/**
 * This exception is thrown if the producer cannot allocate memory for a record
 * within max.block.ms due to the buffer being too full.
 *
 * In earlier versions a TimeoutException was thrown instead of this. To keep
 * existing catch-clauses working this class extends TimeoutException.
 *
 * Java: `org.apache.kafka.clients.producer.BufferExhaustedException`.
 */
export class BufferExhaustedError extends TimeoutError {
  /**
   * Makes `BufferExhaustedError` nominal: TypeScript checks it by name, not by
   * shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * The FFI id of the class, `kafka_common_Error_PRODUCER_BUFFER_EXHAUSTED`,
   * for the binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = -28;

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
    super(...(args as []));
  }
}

setForms(BufferExhaustedError, [
  [['message'], 'message'],
]);
