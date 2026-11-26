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
// Java's org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult. Its
// Java constructor is package-private, over a future of the internal
// `Map<TopicPartition, Errors>` and the requested partitions: a private
// constructor here over a future of each partition's error or null (Java's
// `Errors.NONE`), built through `internalCreate`, its logic Java's. The client
// makes that future from the core's `partitionResult(partition)` of each
// requested partition, as Python's.

import { JavaForms, kind } from '../internal/args';
import { illegalArgument } from '../internal/errors';
import { KafkaFuture } from '../common/kafka_future';
import { TopicPartition } from '../common/topic_partition';
import type { TopicPartitionMap } from '../common/topic_partition_map';
import type { TopicPartitionSet } from '../common/topic_partition_set';

const PARTITION_RESULT_FORMS = new JavaForms('partitionResult', [['partition']], {
  bareTypes: { partition: (value) => value === null || value instanceof TopicPartition },
});
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });
// Java's null, the value of a KafkaFuture<Void>.
const VOID = null as unknown as void;

/**
 * The result of the `Admin.deleteConsumerGroupOffsets(groupId, partitions)`
 * call.
 */
export class DeleteConsumerGroupOffsetsResult {
  readonly #future: KafkaFuture<TopicPartitionMap<Error | null>>;
  readonly #partitions: TopicPartitionSet;

  /**
   * Instances come from the admin client's `deleteConsumerGroupOffsets()`.
   *
   * @param future - the future of each partition's error, or null
   * @param partitions - the partitions of the request
   */
  private constructor(future: KafkaFuture<TopicPartitionMap<Error | null>>, partitions: TopicPartitionSet) {
    this.#future = future;
    this.#partitions = partitions;
  }

  /**
   * Java's package-private constructor, the errors null for Java's
   * `Errors.NONE`. Called by the admin client.
   *
   * @param future - the future of each partition's error, or null
   * @param partitions - the partitions of the request
   * @returns the result
   * @internal
   */
  static internalCreate(
    future: KafkaFuture<TopicPartitionMap<Error | null>>,
    partitions: TopicPartitionSet,
  ): DeleteConsumerGroupOffsetsResult {
    return new DeleteConsumerGroupOffsetsResult(future, partitions);
  }

  /**
   * Return a future which can be used to check the result for a given
   * partition.
   *
   * @param partition - the partition, on its own or as `{ partition }`
   * @returns the future
   * @throws `IllegalArgumentError` when the partition was not included in the
   *   original request
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
  partitionResult(options: unknown): KafkaFuture<void> {
    PARTITION_RESULT_FORMS.match(options, arguments.length);
    const partition = PARTITION_RESULT_FORMS.value(options, 'partition');
    if (partition !== null && !(partition instanceof TopicPartition)) {
      throw new TypeError(`partition must be a TopicPartition, not ${kind(partition)}`);
    }
    if (!this.#partitions.has(partition)) {
      throw illegalArgument(`Partition ${String(partition)} was not included in the original request`);
    }
    const result = KafkaFuture.internalCreate<void>(false);
    this.#future.whenComplete((topicPartitions, throwable) => {
      if (throwable !== null && throwable !== undefined) result.internalComplete(VOID, throwable);
      else if (!maybeCompleteWithError(topicPartitions as TopicPartitionMap<Error | null>, partition, result))
        result.internalComplete(VOID, null);
    });
    return result;
  }

  /**
   * Return a future which succeeds only if all the deletions succeed. If not,
   * the first partition error shall be returned.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<void> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    const result = KafkaFuture.internalCreate<void>(false);
    this.#future.whenComplete((topicPartitions, throwable) => {
      if (throwable !== null && throwable !== undefined) {
        result.internalComplete(VOID, throwable);
        return;
      }
      for (const partition of this.#partitions) {
        if (maybeCompleteWithError(topicPartitions as TopicPartitionMap<Error | null>, partition, result)) return;
      }
      result.internalComplete(VOID, null);
    });
    return result;
  }
}

/**
 * Java's `maybeCompleteExceptionally`, through `KafkaAdminClient.getSubLevelError`.
 *
 * @param errors - each partition's error, or null
 * @param partition - the partition
 * @param result - the future to fail
 * @returns whether it failed the future
 */
function maybeCompleteWithError(
  errors: TopicPartitionMap<Error | null>,
  partition: TopicPartition | null,
  result: KafkaFuture<void>,
): boolean {
  let error: Error | null;
  if (!errors.has(partition)) {
    error = illegalArgument(`Offset deletion result for partition "${String(partition)}" was not included in the response`);
  } else {
    error = errors.get(partition);
  }
  if (error === null) return false;
  result.internalComplete(VOID, error);
  return true;
}
