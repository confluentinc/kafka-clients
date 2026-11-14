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
// the Java source of `org.apache.kafka.clients.consumer.LogTruncationException`;
// `cargo xtask check-generated` checks it is up to date.
//
// Not generated: Java's constructors that take a payload, which come with the
// payload getters: `LogTruncationException(Map<TopicPartition, Long>,
// Map<TopicPartition, OffsetAndMetadata>)`, `LogTruncationException(String,
// Map<TopicPartition, Long>, Map<TopicPartition, OffsetAndMetadata>)`.

import { setForms } from '../internal/throwable';
import { OffsetOutOfRangeError } from './offset_out_of_range_error';

/**
 * In the event of an unclean leader election, the log will be truncated,
 * previously committed data will be lost, and new data will be written over
 * these offsets. When this happens, the consumer will detect the truncation and
 * raise this exception (if no automatic reset policy has been defined) with the
 * first offset known to diverge from what the consumer previously read.
 *
 * Java: `org.apache.kafka.clients.consumer.LogTruncationException`.
 *
 * Not constructible yet: Java's constructors take a payload the client does not
 * translate yet; the client builds this error when the core reports it.
 */
export class LogTruncationError extends OffsetOutOfRangeError {
  /**
   * Makes `LogTruncationError` nominal: TypeScript checks it by name, not by
   * shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * The FFI id of the class, `kafka_common_Error_CONSUMER_LOG_TRUNCATION`, for
   * the binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = -20;

  /**
   * Not constructible yet: Java's constructors take a payload the client does
   * not translate yet.
   *
   * @param args - refused: constructing the error throws `TypeError`
   */
  protected constructor(...args: unknown[]) {
    super(...args);
  }
}

setForms(LogTruncationError, []);
