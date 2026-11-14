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
// the Java source of `org.apache.kafka.clients.consumer.OffsetOutOfRangeException`;
// `cargo xtask check-generated` checks it is up to date.

import type { TopicPartitionMap } from '../common/topic_partition_map';
import { TopicPartitionSet } from '../common/topic_partition_set';
import { javaString } from '../internal/java_string';
import * as P from '../internal/payload';
import { payloadOf, setForms } from '../internal/throwable';
import { InvalidOffsetError } from './invalid_offset_error';

/**
 * No reset policy has been defined, and the offsets for these partitions are
 * either larger or smaller than the range of offsets the server has for the
 * given partition.
 *
 * Java: `org.apache.kafka.clients.consumer.OffsetOutOfRangeException`.
 */
export class OffsetOutOfRangeError extends InvalidOffsetError {
  /**
   * Makes `OffsetOutOfRangeError` nominal: TypeScript checks it by name, not by
   * shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * Java's field `Map<TopicPartition, Long> offsetOutOfRangePartitions`.
   */
  readonly #offsetOutOfRangePartitions: TopicPartitionMap<bigint> | null;

  /**
   * The FFI id of the class, `kafka_common_Error_CONSUMER_OFFSET_OUT_OF_RANGE`,
   * for the binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = -22;

  /**
   * Java's `(Map<TopicPartition, Long> offsetOutOfRangePartitions)`
   * constructor. The message is the one Java's constructor builds from the
   * payload.
   *
   * @param options - the `offsetOutOfRangePartitions`
   */
  constructor(options: { /** Java's `Map<TopicPartition, Long> offsetOutOfRangePartitions`. */ offsetOutOfRangePartitions: TopicPartitionMap<bigint | number>; name?: never });
  /**
   * Java's `(String message, Map<TopicPartition, Long>
   * offsetOutOfRangePartitions)` constructor.
   *
   * @param options - the message and the `offsetOutOfRangePartitions`
   */
  constructor(options: { /** The message; `null` reads as `''`. */ message: string | null; /** Java's `Map<TopicPartition, Long> offsetOutOfRangePartitions`. */ offsetOutOfRangePartitions: TopicPartitionMap<bigint | number>; name?: never });
  constructor(...args: unknown[]) {
    super(...(args as [{ message: string | null; name?: never }]));
    const fields = payloadOf(this);
    this.#offsetOutOfRangePartitions = fields.offsetOutOfRangePartitions as TopicPartitionMap<bigint> | null;
  }

  /**
   * Java's `offsetOutOfRangePartitions()`.
   *
   * @returns Java's `offsetOutOfRangePartitions`, as a new TopicPartitionMap
   * the caller may change @throws `TypeError` when it is given an argument: it
   * only reads
   */
  offsetOutOfRangePartitions(): TopicPartitionMap<bigint> {
    P.readOnly('offsetOutOfRangePartitions', arguments.length);
    return P.topicPartitionLongMap.copy(this.#offsetOutOfRangePartitions) as TopicPartitionMap<bigint>;
  }

  /**
   * Java's `partitions()`: the partitions of `offsetOutOfRangePartitions`.
   *
   * @returns a new TopicPartitionSet of the keys of
   * `offsetOutOfRangePartitions` @throws `TypeError` when it is given an
   * argument (it only reads), or `offsetOutOfRangePartitions` is null
   */
  override partitions(): TopicPartitionSet {
    P.readOnly('partitions', arguments.length);
    return new TopicPartitionSet(P.nonNull(this.#offsetOutOfRangePartitions, 'offsetOutOfRangePartitions').keys());
  }
}

setForms(OffsetOutOfRangeError, [
  [['offsetOutOfRangePartitions'], (v) => 'Offsets out of range with no configured reset policy for partitions: ' + javaString(v.offsetOutOfRangePartitions), (v) => ({ offsetOutOfRangePartitions: v.offsetOutOfRangePartitions })],
  [['message', 'offsetOutOfRangePartitions'], 'message', (v) => ({ offsetOutOfRangePartitions: v.offsetOutOfRangePartitions })],
], {
  order: ['message', 'offsetOutOfRangePartitions'],
  params: { offsetOutOfRangePartitions: P.topicPartitionLongMap },
  fields: { offsetOutOfRangePartitions: [P.topicPartitionLongMap, null] },
});
