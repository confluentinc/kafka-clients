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
// Java's org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult. Its
// Java constructor is package-private, over futures keyed by the internal
// `CoordinatorKey`, which Java keys by its group id: a private constructor
// here over the futures by group id, the client building it through
// `internalCreate` (javascript/CLAUDE.md, JavaScript Binding Conventions, Class
// family, Q16), each group's future the core's
// `partitionsToOffsetAndMetadata(groupId)`, as Python's. Java's
// `Map<TopicPartition, OffsetAndMetadata>` is a TopicPartitionMap whose value
// is null for a partition the group has no committed offset of (Q25).

import { JavaForms } from '../internal/args';
import { illegalArgument, illegalState } from '../internal/errors';
import { KafkaFuture } from '../common/kafka_future';
import type { TopicPartitionMap } from '../common/topic_partition_map';
import type { OffsetAndMetadata } from '../consumer/offset_and_metadata';

const PARTITIONS_FORMS = new JavaForms('partitionsToOffsetAndMetadata', [[], ['groupId']], {
  bareTypes: { groupId: (value) => value === null || typeof value === 'string' },
});
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });

/**
 * The result of the `Admin.listConsumerGroupOffsets({ groupSpecs })` and
 * `Admin.listConsumerGroupOffsets(groupId)` calls.
 *
 * Instances come from the admin client's `listConsumerGroupOffsets()`.
 */
export class ListConsumerGroupOffsetsResult {
  readonly #futures: Map<string, KafkaFuture<TopicPartitionMap<OffsetAndMetadata | null>>>;

  private constructor(futures: Map<string, KafkaFuture<TopicPartitionMap<OffsetAndMetadata | null>>>) {
    this.#futures = futures;
  }

  /**
   * Java's package-private constructor, its futures by group id. Called by
   * the admin client.
   *
   * @param futures - the futures, by group id
   * @returns the result
   * @internal
   */
  static internalCreate(futures: Map<string, KafkaFuture<TopicPartitionMap<OffsetAndMetadata | null>>>): ListConsumerGroupOffsetsResult {
    return new ListConsumerGroupOffsetsResult(futures);
  }

  /**
   * Return a future which yields a map of topic partitions to
   * OffsetAndMetadata objects. If the group does not have a committed offset
   * for this partition, the corresponding value in the returned map will be
   * null.
   *
   * @returns the future
   * @throws `IllegalStateError` when offsets of several groups were requested
   */
  partitionsToOffsetAndMetadata(): KafkaFuture<TopicPartitionMap<OffsetAndMetadata | null>>;
  /**
   * Return a future which yields a map of topic partitions to
   * OffsetAndMetadata objects for the specified group. If the group doesn't
   * have a committed offset for a specific partition, the corresponding value
   * in the returned map will be null.
   *
   * @param groupId - the group id, on its own
   * @returns the future
   * @throws `IllegalArgumentError` when that group's offsets were not requested
   */
  partitionsToOffsetAndMetadata(groupId: string): KafkaFuture<TopicPartitionMap<OffsetAndMetadata | null>>;
  /**
   * The same as `partitionsToOffsetAndMetadata(groupId)`, the group id given by name.
   *
   * @param options - the group id
   * @returns the future
   */
  partitionsToOffsetAndMetadata(options: {
    /** The group id. */
    groupId: string;
  }): KafkaFuture<TopicPartitionMap<OffsetAndMetadata | null>>;
  /**
   * The future of one group's offsets.
   *
   * @param options - nothing, or the group id
   * @returns the future
   */
  partitionsToOffsetAndMetadata(options?: unknown): KafkaFuture<TopicPartitionMap<OffsetAndMetadata | null>> {
    const form = PARTITIONS_FORMS.match(options, arguments.length);
    if (form === 0) {
      if (this.#futures.size !== 1) {
        throw illegalState(
          'Offsets from multiple consumer groups were requested. ' +
            'Use partitionsToOffsetAndMetadata(groupId) instead to get future for a specific group.',
        );
      }
      return [...this.#futures.values()][0];
    }
    const groupId = PARTITIONS_FORMS.value(options, 'groupId');
    const future = typeof groupId === 'string' ? this.#futures.get(groupId) : undefined;
    if (future === undefined) throw illegalArgument(`Offsets for consumer group '${String(groupId)}' were not requested.`);
    return future;
  }

  /**
   * Return a future which yields all `Map<String, Map<TopicPartition,
   * OffsetAndMetadata>>` objects, if requests for all the groups succeed.
   *
   * @returns the future of the offsets, by group id
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<Map<string, TopicPartitionMap<OffsetAndMetadata | null>>> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    const futures = this.#futures;
    return KafkaFuture.allOf([...futures.values()]).thenApply(() => {
      const offsets = new Map<string, TopicPartitionMap<OffsetAndMetadata | null>>();
      for (const [groupId, future] of futures) offsets.set(groupId, future.getNow(null) as TopicPartitionMap<OffsetAndMetadata | null>);
      return offsets;
    });
  }
}
