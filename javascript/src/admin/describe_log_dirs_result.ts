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

// Java's org.apache.kafka.clients.admin.DescribeLogDirsResult. Its Java
// constructor is package-private: a private constructor here, the client
// building it through `internalCreate` (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Class family). Java's Map<Integer, ...> keyed by broker
// id is a Map of numbers.

import { JavaForms } from '../internal/args';
import { KafkaFuture } from '../common/kafka_future';
import type { LogDirDescription } from './log_dir_description';

const DESCRIPTIONS_FORMS = new JavaForms('descriptions', [[]], { reader: true });
const ALL_DESCRIPTIONS_FORMS = new JavaForms('allDescriptions', [[]], { reader: true });

/**
 * The result of the `Admin.describeLogDirs(brokers)` call.
 */
export class DescribeLogDirsResult {
  readonly #futures: Map<number, KafkaFuture<Map<string, LogDirDescription>>>;

  /**
   * Instances come from the admin client's `describeLogDirs()`.
   *
   * @param futures - the futures, by broker id
   */
  private constructor(futures: Map<number, KafkaFuture<Map<string, LogDirDescription>>>) {
    this.#futures = futures;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param futures - the futures, by broker id
   * @returns the result
   * @internal
   */
  static internalCreate(futures: Map<number, KafkaFuture<Map<string, LogDirDescription>>>): DescribeLogDirsResult {
    return new DescribeLogDirsResult(futures);
  }

  /**
   * Return a map from brokerId to future which can be used to check the
   * information of partitions on each individual broker. The result of the
   * future is a map from broker log directory path to a description of that
   * log directory.
   *
   * @returns the futures, by broker id
   * @throws `TypeError` when it is given an argument: it only reads
   */
  descriptions(): Map<number, KafkaFuture<Map<string, LogDirDescription>>> {
    if (arguments.length !== 0) DESCRIPTIONS_FORMS.match(undefined, arguments.length);
    return this.#futures;
  }

  /**
   * Return a future which succeeds only if all the brokers have responded
   * without error. The result of the future is a map from brokerId to a map
   * from broker log directory path to a description of that log directory.
   *
   * @returns the future of the descriptions, by broker id
   * @throws `TypeError` when it is given an argument: it only reads
   */
  allDescriptions(): KafkaFuture<Map<number, Map<string, LogDirDescription>>> {
    if (arguments.length !== 0) ALL_DESCRIPTIONS_FORMS.match(undefined, arguments.length);
    const futures = this.#futures;
    return KafkaFuture.allOf([...futures.values()]).thenApply(() => {
      const descriptions = new Map<number, Map<string, LogDirDescription>>();
      for (const [broker, future] of futures) {
        descriptions.set(broker, future.getNow({ valueIfAbsent: null }) as Map<string, LogDirDescription>);
      }
      return descriptions;
    });
  }
}
