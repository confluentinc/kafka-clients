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
// Java's org.apache.kafka.clients.admin.DeleteConsumerGroupsResult. Its Java
// constructor is package-private: a private constructor here, the client
// building it through `internalCreate` (root CLAUDE.md, JavaScript Binding
// Conventions, Class family, Q16). `deletedGroups()` returns Java's map as a
// `Map`; `all()` is Java's `allOf` over its futures.

import { JavaForms } from '../internal/args';
import { KafkaFuture } from '../common/kafka_future';

const DELETED_GROUPS_FORMS = new JavaForms('deletedGroups', [[]], { reader: true });
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });

/**
 * The result of the `Admin.deleteConsumerGroups(groupIds)` call.
 *
 * Instances come from the admin client's `deleteConsumerGroups()`.
 */
export class DeleteConsumerGroupsResult {
  readonly #futures: Map<string, KafkaFuture<void>>;

  private constructor(futures: Map<string, KafkaFuture<void>>) {
    this.#futures = futures;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param futures - the futures, by group id
   * @returns the result
   * @internal
   */
  static internalCreate(futures: Map<string, KafkaFuture<void>>): DeleteConsumerGroupsResult {
    return new DeleteConsumerGroupsResult(futures);
  }

  /**
   * Return a map from group id to futures which can be used to check the
   * status of individual deletions.
   *
   * @returns a new map of the futures, by group id
   * @throws `TypeError` when it is given an argument: it only reads
   */
  deletedGroups(): Map<string, KafkaFuture<void>> {
    if (arguments.length !== 0) DELETED_GROUPS_FORMS.match(undefined, arguments.length);
    return new Map(this.#futures);
  }

  /**
   * Return a future which succeeds only if all the consumer group deletions
   * succeed.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<void> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    return KafkaFuture.allOf([...this.#futures.values()]);
  }
}
