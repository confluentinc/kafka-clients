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

// The public classes of Java's org.apache.kafka.common.metrics this client
// has: KafkaMetric, and the placeholder types of the untranslated
// MetricValueProvider, MetricConfig and Measurable it takes or returns.
// QuotaViolationError is generated from the Java sources.
//
// Left out (Q30): the setter KafkaMetric.config(MetricConfig), whose only
// extra parameter over the getter is `config`, and which Java says is for the
// server only.

export { KafkaMetric } from './kafka_metric';
export type { Measurable } from './measurable';
export type { MetricConfig } from './metric_config';
export type { MetricValueProvider } from './metric_value_provider';
// BEGIN GENERATED EXPORTS (cargo xtask generate-error-codes; do not edit)
export { QuotaViolationError } from './quota_violation_error';
// END GENERATED EXPORTS

// tsc's CommonJS output sets `exports.__esModule`: declared, a default import
// under `esModuleInterop` is a compile error rather than `undefined` (import by
// name or `import * as` instead; Node's own ESM default import still works).
export declare const __esModule: true;
