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

// Java's org.apache.kafka.clients.producer.Partitioner is not translated (root
// CLAUDE.md, JavaScript Binding Conventions, Scope, Q30): a placeholder type
// for MockProducer's constructor, which takes it and cannot be dropped.

/**
 * Partitioner Interface
 *
 * @remarks *(deviation)* A placeholder: Java's `Partitioner` is not
 * translated, and the client selects a built-in partitioner by
 * `partitioner.type` instead. Why: a placeholder typed `unknown` checks
 * nothing, but `MockProducer`'s constructor, which takes it, cannot be
 * dropped. Goes when: `Partitioner` is translated.
 */
export type Partitioner = unknown;
