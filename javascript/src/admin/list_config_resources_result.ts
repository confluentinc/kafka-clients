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

// Java's org.apache.kafka.clients.admin.ListConfigResourcesResult. Its Java
// constructor is package-private: a private constructor here, the client
// building it through `internalCreate` (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Class family). `all()` is Java's: a new future completed
// from the one the call made, on each call.

import { JavaForms } from '../internal/args';
import type { KafkaFuture } from '../common/kafka_future';
import type { ConfigResource } from '../common/config/config_resource';

const ALL_FORMS = new JavaForms('all', [[]], { reader: true });

/**
 * The result of the `Admin.listConfigResources()` call.
 */
export class ListConfigResourcesResult {
  readonly #future: KafkaFuture<ConfigResource[]>;

  /**
   * Instances come from the admin client's `listConfigResources()`.
   *
   * @param future - the future of the resources
   */
  private constructor(future: KafkaFuture<ConfigResource[]>) {
    this.#future = future;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param future - the future of the resources
   * @returns the result
   * @internal
   */
  static internalCreate(future: KafkaFuture<ConfigResource[]>): ListConfigResourcesResult {
    return new ListConfigResourcesResult(future);
  }

  /**
   * Returns a future that yields either an error, or the full set of config
   * resources.
   *
   * In the event of a failure, the future yields nothing but the first error
   * which occurred.
   *
   * @returns the future of the resources
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<ConfigResource[]> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    return this.#future.thenApply((resources) => resources);
  }
}
