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

// Java's org.apache.kafka.clients.admin.CreatePartitionsResult. Its Java
// constructor is package-private: a private constructor here, the client
// building it through `internalCreate` (root CLAUDE.md, JavaScript Binding
// Conventions, Class family, Q16).

import { JavaForms } from '../internal/args';
import { KafkaFuture } from '../common/kafka_future';

const VALUES_FORMS = new JavaForms('values', [[]], { reader: true });
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });

/**
 * The result of the `Admin.createPartitions(newPartitions)` call.
 *
 * Instances come from the admin client's `createPartitions()`.
 */
export class CreatePartitionsResult {
  readonly #values: Map<string, KafkaFuture<void>>;

  private constructor(values: Map<string, KafkaFuture<void>>) {
    this.#values = values;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param values - the futures, by topic name
   * @returns the result
   * @internal
   */
  static internalCreate(values: Map<string, KafkaFuture<void>>): CreatePartitionsResult {
    return new CreatePartitionsResult(values);
  }

  /**
   * Return a map from topic names to futures, which can be used to check the
   * status of individual partition creations.
   *
   * @returns the futures, by topic name
   * @throws `TypeError` when it is given an argument: it only reads
   */
  values(): Map<string, KafkaFuture<void>> {
    if (arguments.length !== 0) VALUES_FORMS.match(undefined, arguments.length);
    return this.#values;
  }

  /**
   * Return a future which succeeds if all the partition creations succeed.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<void> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    return KafkaFuture.allOf([...this.#values.values()]);
  }
}
