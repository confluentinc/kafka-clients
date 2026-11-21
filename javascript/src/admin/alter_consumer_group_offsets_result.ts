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
// Java's org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult. Its
// Java constructor is package-private, over a future of the internal
// `Map<TopicPartition, Errors>`, which the C FFI does not hand out: a private
// constructor here over the core's own `partitionResult(partition)` and
// `all()` futures, which carry Java's outcomes and messages, the client
// building it through `internalOfClient` (root CLAUDE.md, JavaScript Binding
// Conventions, Class family, Q16), as Python's.

import { JavaForms, kind } from '../internal/args';
import type { KafkaFuture } from '../common/kafka_future';
import { TopicPartition } from '../common/topic_partition';

const PARTITION_RESULT_FORMS = new JavaForms('partitionResult', [['partition']], {
  bareTypes: { partition: (value) => value === null || value instanceof TopicPartition },
});
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });

/**
 * The result of the `Admin.alterConsumerGroupOffsets(groupId, offsets)` call.
 *
 * Instances come from the admin client's `alterConsumerGroupOffsets()`.
 */
export class AlterConsumerGroupOffsetsResult {
  readonly #partitionResult: (partition: TopicPartition) => KafkaFuture<void>;
  readonly #all: () => KafkaFuture<void>;

  private constructor(partitionResult: (partition: TopicPartition) => KafkaFuture<void>, all: () => KafkaFuture<void>) {
    this.#partitionResult = partitionResult;
    this.#all = all;
  }

  /**
   * The result over the core's own futures, each read when asked for.
   * Called by the admin client.
   *
   * @param partitionResult - the core's `partitionResult(partition)`
   * @param all - the core's `all()`
   * @returns the result
   * @internal
   */
  static internalOfClient(
    partitionResult: (partition: TopicPartition) => KafkaFuture<void>,
    all: () => KafkaFuture<void>,
  ): AlterConsumerGroupOffsetsResult {
    return new AlterConsumerGroupOffsetsResult(partitionResult, all);
  }

  /**
   * Return a future which can be used to check the result for a given
   * partition.
   *
   * @param partition - the partition, on its own or as `{ partition }`
   * @returns the future, failed with `IllegalArgumentError` when the
   *   partition was not attempted
   * @throws `TypeError` when the partition is null or not a TopicPartition
   *
   * @remarks *(deviation)* A null partition, or one with a null topic, is a
   * `TypeError` at the call, where Java's future fails with the request's
   * error or with "Alter offset for partition "null" was not attempted". Why:
   * the C API takes the partition as a handle, whose topic cannot be `NULL`.
   * Goes when: the FFI takes them.
   */
  partitionResult(partition: TopicPartition): KafkaFuture<void>;
  /**
   * The same as `partitionResult(partition)`, the partition given by name.
   *
   * @param options - the partition
   * @returns the future
   */
  partitionResult(options: {
    /** The partition. */
    partition: TopicPartition;
  }): KafkaFuture<void>;
  /**
   * The future of one partition.
   *
   * @param options - the partition
   * @returns the future
   */
  partitionResult(options: unknown): KafkaFuture<void> {
    PARTITION_RESULT_FORMS.match(options, arguments.length);
    const partition = PARTITION_RESULT_FORMS.value(options, 'partition');
    // The C API takes the partition as a handle (see the deviation above).
    if (partition === null) throw new TypeError('partition must not be null');
    if (!(partition instanceof TopicPartition)) throw new TypeError(`partition must be a TopicPartition, not ${kind(partition)}`);
    if (partition.topic === null) throw new TypeError('partition must not be a TopicPartition with a null topic');
    return this.#partitionResult(partition);
  }

  /**
   * Return a future which succeeds if all the alter offsets succeed.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<void> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    return this.#all();
  }
}
