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
// An error the core reports has no payload for `metric` (it is `null`, Java's
// default): the FFI's view of the class has no accessor for it.

import { javaDoubleString, javaString } from '../../internal/java_string';
import * as P from '../../internal/payload';
import { payloadOf, setForms } from '../../internal/throwable';
import { KafkaError } from '../kafka_error';
import type { KafkaMetric } from './kafka_metric';

/**
 * Thrown when a sensor records a value that causes a metric to go outside the
 * bounds configured as its quota
 *
 * Java: `org.apache.kafka.common.metrics.QuotaViolationException`.
 */
export class QuotaViolationError extends KafkaError {
  /**
   * Makes `QuotaViolationError` nominal: TypeScript checks it by name, not by
   * shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * Java's field `KafkaMetric metric`.
   */
  readonly #metric: KafkaMetric | null;

  /**
   * Java's field `double value`.
   */
  readonly #value: number;

  /**
   * Java's field `double bound`.
   */
  readonly #bound: number;

  /**
   * The FFI id of the class, `kafka_common_Error_QUOTA_VIOLATION`, for the
   * binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = -26;

  /**
   * Java's `(KafkaMetric metric, double value, double bound)` constructor. The
   * message is empty, Java's null.
   *
   * @param options - the `metric` and the `value` and the `bound`
   */
  constructor(options: { /** Java's `KafkaMetric metric`. */ metric: KafkaMetric; /** Java's `double value`. */ value: number; /** Java's `double bound`. */ bound: number; name?: never });
  constructor(...args: unknown[]) {
    super(...(args as [{ message: string | null; cause: unknown; name?: never }]));
    const fields = payloadOf(this);
    this.#metric = fields.metric as KafkaMetric | null;
    this.#value = fields.value as number;
    this.#bound = fields.bound as number;
  }

  /**
   * Java's `metric()`.
   *
   * @returns Java's `metric` @throws `TypeError` when it is given an argument:
   * it only reads
   */
  metric(): KafkaMetric {
    P.readOnly('metric', arguments.length);
    return this.#metric as KafkaMetric;
  }

  /**
   * Java's `value()`.
   *
   * @returns Java's `value` @throws `TypeError` when it is given an argument:
   * it only reads
   */
  value(): number {
    P.readOnly('value', arguments.length);
    return this.#value;
  }

  /**
   * Java's `bound()`.
   *
   * @returns Java's `bound` @throws `TypeError` when it is given an argument:
   * it only reads
   */
  bound(): number {
    P.readOnly('bound', arguments.length);
    return this.#bound;
  }

  /**
   * Java's `toString()` override, Java's class name written as the error's
   * `name`; `Error`'s text for an error the core reports without the field it
   * reads.
   *
   * @returns the error's text
   */
  override toString(): string {
    if (this.#metric === null) return super.toString();
    return this.name + ': \'' + javaString(this.#metric.metricName()) + '\' violated quota. Actual: ' + javaDoubleString(this.#value) + ', Threshold: ' + javaDoubleString(this.#bound);
  }
}

setForms(QuotaViolationError, [
  [['metric', 'value', 'bound'], null, (v) => ({ metric: v.metric, value: v.value, bound: v.bound })],
], {
  order: ['metric', 'value', 'bound'],
  params: { metric: P.kafkaMetric, value: P.double, bound: P.double },
  fields: { metric: [P.kafkaMetric, null], value: [P.double, 0], bound: [P.double, 0] },
});
