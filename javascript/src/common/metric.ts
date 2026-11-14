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

// Java's Metric interface, which the client hands out (the values of
// KafkaProducer.metrics()): KafkaMetric implements it.

import type { MetricName } from './metric_name';

/**
 * A metric tracked for monitoring purposes.
 */
export interface Metric {
  /**
   * A name for this metric
   *
   * @returns the metric's name
   */
  metricName(): MetricName;

  /**
   * The value of the metric, which may be measurable or a non-measurable
   * gauge
   *
   * @returns the value: a number, a bigint for a Java long, or a string
   */
  metricValue(): unknown;
}
