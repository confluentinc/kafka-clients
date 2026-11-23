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

// Java's org.apache.kafka.clients.admin.ListPartitionReassignmentsResult. Its
// Java constructor is package-private: a private constructor here, the client
// building it through `internalCreate` (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Class family, Q16). Java's map keyed by TopicPartition is a
// TopicPartitionMap (Q25).

import { JavaForms } from '../internal/args';
import type { KafkaFuture } from '../common/kafka_future';
import type { TopicPartitionMap } from '../common/topic_partition_map';
import type { PartitionReassignment } from './partition_reassignment';

const REASSIGNMENTS_FORMS = new JavaForms('reassignments', [[]], { reader: true });

/**
 * The result of `Admin.listPartitionReassignments(options)`.
 *
 * Instances come from the admin client's `listPartitionReassignments()`.
 */
export class ListPartitionReassignmentsResult {
  readonly #future: KafkaFuture<TopicPartitionMap<PartitionReassignment>>;

  private constructor(reassignments: KafkaFuture<TopicPartitionMap<PartitionReassignment>>) {
    this.#future = reassignments;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param reassignments - the future of the reassignments, by partition
   * @returns the result
   * @internal
   */
  static internalCreate(reassignments: KafkaFuture<TopicPartitionMap<PartitionReassignment>>): ListPartitionReassignmentsResult {
    return new ListPartitionReassignmentsResult(reassignments);
  }

  /**
   * Return a future which yields a map containing each partition's
   * reassignments.
   *
   * @returns the future of the reassignments, by partition
   * @throws `TypeError` when it is given an argument: it only reads
   */
  reassignments(): KafkaFuture<TopicPartitionMap<PartitionReassignment>> {
    if (arguments.length !== 0) REASSIGNMENTS_FORMS.match(undefined, arguments.length);
    return this.#future;
  }
}
