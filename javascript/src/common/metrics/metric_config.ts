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

// Java's org.apache.kafka.common.metrics.MetricConfig is not translated (root
// CLAUDE.md, JavaScript Binding Conventions, Scope): a placeholder type for
// KafkaMetric's constructor and config(), which cannot be dropped.

/**
 * Configuration values for metrics
 *
 * @remarks *(deviation)* A placeholder: Java's `MetricConfig` is not
 * translated, so a value of this type is only passed on, to the value
 * provider. Why: a placeholder typed `unknown` checks nothing, but
 * `KafkaMetric`'s constructor and `config()`, which take and return it,
 * cannot be dropped. Goes when: `MetricConfig` is translated.
 */
export type MetricConfig = unknown;
