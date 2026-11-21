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

// Java's org.apache.kafka.clients.admin.ElectLeadersResult. Its Java
// constructor is package-private: a private constructor here, the client
// building it through `internalCreate` (root CLAUDE.md, JavaScript Binding
// Conventions, Class family, Q16). Java's `Map<TopicPartition,
// Optional<Throwable>>` is a TopicPartitionMap of the error or null (Q25,
// Types: `Optional<T>` is `T | null`).

import { JavaForms } from '../internal/args';
import { KafkaFuture } from '../common/kafka_future';
import type { TopicPartitionMap } from '../common/topic_partition_map';

const PARTITIONS_FORMS = new JavaForms('partitions', [[]], { reader: true });
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });
// Java's null, the value of a KafkaFuture<Void>.
const VOID = null as unknown as void;

/**
 * The result of `Admin.electLeaders(electionType, partitions, options)`.
 *
 * Instances come from the admin client's `electLeaders()`.
 */
export class ElectLeadersResult {
  readonly #electionFuture: KafkaFuture<TopicPartitionMap<unknown>>;

  private constructor(electionFuture: KafkaFuture<TopicPartitionMap<unknown>>) {
    this.#electionFuture = electionFuture;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param electionFuture - the future of the elections' outcomes
   * @returns the result
   * @internal
   */
  static internalCreate(electionFuture: KafkaFuture<TopicPartitionMap<unknown>>): ElectLeadersResult {
    return new ElectLeadersResult(electionFuture);
  }

  /**
   * Get a future for the topic partitions for which a leader election was
   * attempted. If the election succeeded then the value for a topic partition
   * will be null. Otherwise the election failed and the value will be the
   * error.
   *
   * @returns the future of the outcomes, by partition
   * @throws `TypeError` when it is given an argument: it only reads
   */
  partitions(): KafkaFuture<TopicPartitionMap<unknown>> {
    if (arguments.length !== 0) PARTITIONS_FORMS.match(undefined, arguments.length);
    return this.#electionFuture;
  }

  /**
   * Return a future which succeeds if all the topic elections succeed.
   *
   * @returns the future, failed with the first partition's error
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<void> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    const result = KafkaFuture.internalCreate<void>(false);
    this.#electionFuture.whenComplete((topicPartitions, throwable) => {
      if (throwable !== null && throwable !== undefined) {
        result.internalComplete(VOID, throwable);
        return;
      }
      for (const error of (topicPartitions as TopicPartitionMap<unknown>).values()) {
        if (error !== null) {
          result.internalComplete(VOID, error);
          return;
        }
      }
      result.internalComplete(VOID, null);
    });
    return result;
  }
}
