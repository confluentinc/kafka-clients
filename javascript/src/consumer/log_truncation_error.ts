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

import type { TopicPartitionMap } from '../common/topic_partition_map';
import { javaString } from '../internal/java_string';
import * as P from '../internal/payload';
import { payloadOf, setForms } from '../internal/throwable';
import type { OffsetAndMetadata } from './offset_and_metadata';
import { OffsetOutOfRangeError } from './offset_out_of_range_error';

/**
 * In the event of an unclean leader election, the log will be truncated,
 * previously committed data will be lost, and new data will be written over
 * these offsets. When this happens, the consumer will detect the truncation and
 * raise this exception (if no automatic reset policy has been defined) with the
 * first offset known to diverge from what the consumer previously read.
 *
 * Java: `org.apache.kafka.clients.consumer.LogTruncationException`.
 */
export class LogTruncationError extends OffsetOutOfRangeError {
  /**
   * Makes `LogTruncationError` nominal: TypeScript checks it by name, not by
   * shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * Java's field `Map<TopicPartition, OffsetAndMetadata> divergentOffsets`.
   */
  readonly #divergentOffsets: TopicPartitionMap<OffsetAndMetadata> | null;

  /**
   * The FFI id of the class, `kafka_common_Error_CONSUMER_LOG_TRUNCATION`, for
   * the binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = -20;

  /**
   * Java's `(Map<TopicPartition, Long> fetchOffsets, Map<TopicPartition,
   * OffsetAndMetadata> divergentOffsets)` constructor. The message is the one
   * Java's constructor builds from the payload.
   *
   * @param options - the `fetchOffsets` and the `divergentOffsets`
   */
  constructor(options: { /** Java's `Map<TopicPartition, Long> fetchOffsets`. */ fetchOffsets: TopicPartitionMap<bigint | number>; /** Java's `Map<TopicPartition, OffsetAndMetadata> divergentOffsets`. */ divergentOffsets: TopicPartitionMap<OffsetAndMetadata>; name?: never });
  /**
   * Java's `(String message, Map<TopicPartition, Long> fetchOffsets,
   * Map<TopicPartition, OffsetAndMetadata> divergentOffsets)` constructor.
   *
   * @param options - the message and the `fetchOffsets` and the
   *   `divergentOffsets`
   */
  constructor(options: { /** The message; `null` reads as `''`. */ message: string | null; /** Java's `Map<TopicPartition, Long> fetchOffsets`. */ fetchOffsets: TopicPartitionMap<bigint | number>; /** Java's `Map<TopicPartition, OffsetAndMetadata> divergentOffsets`. */ divergentOffsets: TopicPartitionMap<OffsetAndMetadata>; name?: never });
  constructor(...args: unknown[]) {
    super(...(args as [{ offsetOutOfRangePartitions: TopicPartitionMap<bigint | number>; name?: never }]));
    const fields = payloadOf(this);
    this.#divergentOffsets = fields.divergentOffsets as TopicPartitionMap<OffsetAndMetadata> | null;
  }

  /**
   * Java's `divergentOffsets()`.
   *
   * @returns Java's `divergentOffsets`, as a new TopicPartitionMap the caller
   *   may change
   * @throws `TypeError` when it is given an argument: it only reads
   */
  divergentOffsets(): TopicPartitionMap<OffsetAndMetadata> {
    P.readOnly('divergentOffsets', arguments.length);
    return P.topicPartitionOffsetMap.copy(this.#divergentOffsets) as TopicPartitionMap<OffsetAndMetadata>;
  }
}

setForms(LogTruncationError, [
  [['fetchOffsets', 'divergentOffsets'], (v) => 'Truncated partitions detected with divergent offsets ' + javaString(v.divergentOffsets), (v) => ({ offsetOutOfRangePartitions: v.fetchOffsets, divergentOffsets: P.nonNull(v.divergentOffsets, 'divergentOffsets') })],
  [['message', 'fetchOffsets', 'divergentOffsets'], 'message', (v) => ({ offsetOutOfRangePartitions: v.fetchOffsets, divergentOffsets: P.nonNull(v.divergentOffsets, 'divergentOffsets') })],
], {
  order: ['message', 'fetchOffsets', 'divergentOffsets'],
  params: { fetchOffsets: P.topicPartitionLongMap, divergentOffsets: P.topicPartitionOffsetMap },
  fields: { offsetOutOfRangePartitions: [P.topicPartitionLongMap, null], divergentOffsets: [P.topicPartitionOffsetMap, null] },
});
