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

// Java's org.apache.kafka.clients.admin.DeleteRecordsResult, whose Java
// constructor is public. Java's map keyed by TopicPartition is a
// TopicPartitionMap (javascript/CLAUDE.md, JavaScript Binding Conventions, Class
// family).

import { JavaForms, kind } from '../internal/args';
import { KafkaFuture } from '../common/kafka_future';
import { TopicPartitionMap } from '../common/topic_partition_map';
import type { DeletedRecords } from './deleted_records';

const FORMS = new JavaForms('DeleteRecordsResult', [['futures']], {
  bareTypes: { futures: (value) => value === null || value instanceof TopicPartitionMap },
});
const LOW_WATERMARKS_FORMS = new JavaForms('lowWatermarks', [[]], { reader: true });
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });

/**
 * The result of the `Admin.deleteRecords(recordsToDelete)` call.
 */
export class DeleteRecordsResult {
  readonly #futures: TopicPartitionMap<KafkaFuture<DeletedRecords>> | null;

  /**
   * The result over the futures of each partition.
   *
   * @param futures - the futures, by partition, on its own
   * @throws `TypeError` when they are not a TopicPartitionMap
   */
  constructor(futures: TopicPartitionMap<KafkaFuture<DeletedRecords>>);
  /**
   * The same as `new DeleteRecordsResult(futures)`.
   *
   * @param options - the futures, by partition
   * @throws `TypeError` when they are not a TopicPartitionMap
   */
  constructor(options: {
    /** The futures, by partition. */
    futures: TopicPartitionMap<KafkaFuture<DeletedRecords>>;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const futures = FORMS.value(options, 'futures');
    if (futures !== null && !(futures instanceof TopicPartitionMap)) {
      throw new TypeError(`futures must be a TopicPartitionMap, not ${kind(futures)}`);
    }
    this.#futures = futures as TopicPartitionMap<KafkaFuture<DeletedRecords>> | null;
  }

  /**
   * Return a map from topic partition to futures which can be used to check
   * the status of individual deletions.
   *
   * @returns the futures, by partition
   * @throws `TypeError` when it is given an argument: it only reads
   */
  lowWatermarks(): TopicPartitionMap<KafkaFuture<DeletedRecords>> {
    if (arguments.length !== 0) LOW_WATERMARKS_FORMS.match(undefined, arguments.length);
    // Java only stores a null map, so the type is the non-null one.
    return this.#futures as TopicPartitionMap<KafkaFuture<DeletedRecords>>;
  }

  /**
   * Return a future which succeeds only if all the records deletions
   * succeed.
   *
   * @returns the future
   * @throws `TypeError` when the futures are null (Java dereferences them)
   */
  all(): KafkaFuture<void> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    if (this.#futures === null) throw new TypeError('futures must not be null');
    return KafkaFuture.allOf([...this.#futures.values()]);
  }
}
