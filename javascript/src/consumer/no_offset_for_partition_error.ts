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
// the Java source of `org.apache.kafka.clients.consumer.NoOffsetForPartitionException`;
// `cargo xtask check-generated` checks it is up to date.

import type { TopicPartition } from '../common/topic_partition';
import type { TopicPartitionSet } from '../common/topic_partition_set';
import { javaString } from '../internal/java_string';
import * as P from '../internal/payload';
import { payloadOf, setForms } from '../internal/throwable';
import { InvalidOffsetError } from './invalid_offset_error';

/**
 * Indicates that there is no stored offset for a partition and no defined
 * offset reset policy.
 *
 * Java: `org.apache.kafka.clients.consumer.NoOffsetForPartitionException`.
 */
export class NoOffsetForPartitionError extends InvalidOffsetError {
  /**
   * Makes `NoOffsetForPartitionError` nominal: TypeScript checks it by name,
   * not by shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * Java's field `Set<TopicPartition> partitions`.
   */
  readonly #partitions: TopicPartitionSet | null;

  /**
   * The FFI id of the class,
   * `kafka_common_Error_CONSUMER_NO_OFFSET_FOR_PARTITION`, for the binding and
   * the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = -21;

  /**
   * Java's `(TopicPartition partition)` constructor. The message is the one
   * Java's constructor builds from the payload.
   *
   * @param options - the `partition`
   */
  constructor(options: { /** Java's `TopicPartition partition`. */ partition: TopicPartition; name?: never });
  /**
   * Java's `(Collection<TopicPartition> partitions)` constructor. The message
   * is the one Java's constructor builds from the payload.
   *
   * @param options - the `partitions`
   */
  constructor(options: { /** Java's `Collection<TopicPartition> partitions`. */ partitions: Iterable<TopicPartition>; name?: never });
  constructor(...args: unknown[]) {
    super(...(args as [{ message: string | null; name?: never }]));
    const fields = payloadOf(this);
    this.#partitions = fields.partitions as TopicPartitionSet | null;
  }

  /**
   * Java's `partitions()`.
   *
   * @returns Java's `partitions`, as a new TopicPartitionSet the caller may
   * change @throws `TypeError` when it is given an argument: it only reads
   */
  override partitions(): TopicPartitionSet {
    P.readOnly('partitions', arguments.length);
    return P.topicPartitions.copy(this.#partitions) as TopicPartitionSet;
  }
}

setForms(NoOffsetForPartitionError, [
  [['partition'], (v) => 'Undefined offset with no reset policy for partition: ' + javaString(v.partition), (v) => ({ partitions: P.singleton(v.partition, 'partition') })],
  [['partitions'], (v) => 'Undefined offset with no reset policy for partitions: ' + javaString(v.partitions), (v) => ({ partitions: P.nonNull(v.partitions, 'partitions') })],
], {
  order: ['partition', 'partitions'],
  params: { partition: P.topicPartition, partitions: P.topicPartitions },
  fields: { partitions: [P.topicPartitions, null] },
});
