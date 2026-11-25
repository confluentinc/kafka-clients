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

// Java's org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult. Its
// Java constructor is package-private: a private constructor here, the client
// building it through `internalCreate` (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Class family). Java's map keyed by TopicPartition is a
// TopicPartitionMap.

import { JavaForms } from '../internal/args';
import { KafkaFuture } from '../common/kafka_future';
import type { TopicPartitionMap } from '../common/topic_partition_map';

const VALUES_FORMS = new JavaForms('values', [[]], { reader: true });
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });

/**
 * The result of `Admin.alterPartitionReassignments(reassignments, options)`.
 */
export class AlterPartitionReassignmentsResult {
  readonly #futures: TopicPartitionMap<KafkaFuture<void>>;

  /**
   * Instances come from the admin client's `alterPartitionReassignments()`.
   *
   * @param futures - the futures, by partition
   */
  private constructor(futures: TopicPartitionMap<KafkaFuture<void>>) {
    this.#futures = futures;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param futures - the futures, by partition
   * @returns the result
   * @internal
   */
  static internalCreate(futures: TopicPartitionMap<KafkaFuture<void>>): AlterPartitionReassignmentsResult {
    return new AlterPartitionReassignmentsResult(futures);
  }

  /**
   * Return a map from partitions to futures which can be used to check the
   * status of the reassignment.
   *
   * Possible error codes:
   *
   * INVALID_REPLICA_ASSIGNMENT (39) - if the specified replica assignment was
   * not valid -- for example, if it included negative numbers, repeated
   * numbers, or specified a broker ID that the controller was not aware of.
   * NO_REASSIGNMENT_IN_PROGRESS (85) - if the request wants to cancel
   * reassignments but none exist UNKNOWN (-1)
   *
   * @returns the futures, by partition
   * @throws `TypeError` when it is given an argument: it only reads
   */
  values(): TopicPartitionMap<KafkaFuture<void>> {
    if (arguments.length !== 0) VALUES_FORMS.match(undefined, arguments.length);
    return this.#futures;
  }

  /**
   * Return a future which succeeds only if all the reassignments were
   * successfully initiated.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<void> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    return KafkaFuture.allOf([...this.#futures.values()]);
  }
}
