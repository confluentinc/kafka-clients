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

// Java's org.apache.kafka.clients.admin.AbortTransactionResult. Its Java
// constructor is package-private: a private constructor here, the client
// building it through `internalCreate` (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Class family, Q16). Java holds one future per partition, the
// spec's one: `all()` is Java's `allOf` over them.

import { JavaForms } from '../internal/args';
import { KafkaFuture } from '../common/kafka_future';
import type { TopicPartitionMap } from '../common/topic_partition_map';

const ALL_FORMS = new JavaForms('all', [[]], { reader: true });

/**
 * The result of `Admin.abortTransaction(spec, options)`.
 *
 * Instances come from the admin client's `abortTransaction()`.
 */
export class AbortTransactionResult {
  readonly #futures: TopicPartitionMap<KafkaFuture<void>>;

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
  static internalCreate(futures: TopicPartitionMap<KafkaFuture<void>>): AbortTransactionResult {
    return new AbortTransactionResult(futures);
  }

  /**
   * Get a future which completes when the transaction specified by
   * `AbortTransactionSpec` in the respective call to
   * `Admin.abortTransaction(spec, options)` returns successfully or fails due
   * to an error or timeout.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<void> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    return KafkaFuture.allOf([...this.#futures.values()]);
  }
}
