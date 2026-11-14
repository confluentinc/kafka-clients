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

// Java's org.apache.kafka.common.metrics.Measurable is not translated (root
// CLAUDE.md, JavaScript Binding Conventions, Scope): a placeholder type for
// KafkaMetric.measurable(), which returns it and cannot be dropped.

/**
 * A measurable quantity that can be registered as a metric
 *
 * @remarks *(deviation)* A placeholder: Java's `Measurable` is not
 * translated, so its value is the metric's own value provider, read as a
 * `measure(config, now)` method. Why: a placeholder typed `unknown` checks
 * nothing, but `KafkaMetric.measurable()`, which returns it, cannot be
 * dropped. Goes when: `Measurable` is translated.
 */
export type Measurable = unknown;
