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
// the Java source of `org.apache.kafka.common.metrics.QuotaViolationException`;
// `cargo xtask check-generated` checks it is up to date.
//
// Not generated: Java's constructors that take a payload, which come with the
// payload getters: `QuotaViolationException(KafkaMetric, double, double)`.

import { setForms } from '../../internal/throwable';
import { KafkaError } from '../kafka_error';

/**
 * Thrown when a sensor records a value that causes a metric to go outside the
 * bounds configured as its quota
 *
 * Java: `org.apache.kafka.common.metrics.QuotaViolationException`.
 *
 * Not constructible yet: Java's constructors take a payload the client does not
 * translate yet; the client builds this error when the core reports it.
 */
export class QuotaViolationError extends KafkaError {
  /**
   * Makes `QuotaViolationError` nominal: TypeScript checks it by name, not by
   * shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * The FFI id of the class, `kafka_common_Error_QUOTA_VIOLATION`, for the
   * binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = -26;

  /**
   * Not constructible yet: Java's constructors take a payload the client does
   * not translate yet.
   *
   * @param args - refused: constructing the error throws `TypeError`
   */
  protected constructor(...args: unknown[]) {
    super(...(args as [{ message: string | null; cause: unknown; name?: never }]));
  }
}

setForms(QuotaViolationError, []);
