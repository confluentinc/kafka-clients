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
// the Java source of `org.apache.kafka.common.errors.WakeupException`;
// `cargo xtask check-generated` checks it is up to date.

import { setForms } from '../../internal/throwable';
import { KafkaError } from '../kafka_error';

/**
 * Exception used to indicate preemption of a blocking operation by an external
 * thread. For example, `org.apache.kafka.clients.consumer.KafkaConsumer.wakeup`
 * can be used to break out of an active
 * `org.apache.kafka.clients.consumer.KafkaConsumer.poll(java.time.Duration)`,
 * which would raise an instance of this exception.
 *
 * Java: `org.apache.kafka.common.errors.WakeupException`.
 */
export class WakeupError extends KafkaError {
  /**
   * Makes `WakeupError` nominal: TypeScript checks it by name, not by shape. It
   * holds no state.
   */
  readonly #brand = true;

  /**
   * The FFI id of the class, `kafka_common_Error_WAKEUP`, for the binding and
   * the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = -18;

  /**
   * Java's no-argument constructor. The message is empty, Java's null.
   */
  constructor();
  constructor(...args: unknown[]) {
    super(...(args as [{ message: string | null; cause: unknown; name?: never }]));
  }
}

setForms(WakeupError, [
  [[], null],
]);
