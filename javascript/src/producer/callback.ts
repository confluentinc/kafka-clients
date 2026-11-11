// Copyright 2025 Confluent Inc.
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

// Java's Callback interface, a single method: a JS function taking one object
// of named parameters, `({ metadata, exception }) => unknown`, whose result the
// producer wraps with Promise.resolve and awaits. This module holds only its type.

import type { RecordMetadata } from './record_metadata';

/**
 * A callback interface that the user can implement to allow code to execute
 * when the request is complete. This callback will generally execute in the
 * background I/O thread so it should be fast.
 *
 * This method will be called when the record sent to the server has been
 * acknowledged. When `exception` is not null in the callback, metadata will
 * contain the special -1 value for all fields. If topicPartition cannot be
 * chosen, a -1 value will be assigned.
 *
 * @param options - the record's metadata and error
 * @returns anything: the result is wrapped with `Promise.resolve`, and the
 *   record completes once that settled
 *
 * @remarks Java's `onCompletion(metadata, exception)` is one function taking
 * one object, named as Java names the parameters. It runs on the JS thread,
 * inside the producer's callback drain. *(deviation)* A callback that returns
 * a Promise (or any thenable) is awaited, as JavaScript cannot block: the record's future completes, and a `flush()` or
 * `close()` covering the record returns, only once it settled, as they wait
 * for a Java callback to return. A rejection is handled as a throw: the
 * producer logs it, as Java logs a throwing `onCompletion`, and it reaches no
 * caller. A callback's Promise must not wait for a `flush()` or `close()` of
 * its own producer, which wait for it; Java refuses `flush()` inside a
 * callback for the same reason.
 */
export type Callback = (options: {
  /**
   * The metadata for the record that was sent (i.e. the partition and
   * offset). An empty metadata with -1 value for all fields will be returned
   * if an error occurred.
   */
  metadata: RecordMetadata;
  /**
   * The error thrown during processing of this record, a `KafkaError` or one
   * of the Java built-in errors at the package root (`IllegalStateError`,
   * ...). Null if no error occurred.
   */
  exception: Error | null;
}) => unknown;
