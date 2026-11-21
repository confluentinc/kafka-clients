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

// Java's org.apache.kafka.clients.admin.AlterConfigsResult. Its Java
// constructor is package-private: a private constructor here, the client
// building it through `internalCreate` (root CLAUDE.md, JavaScript Binding
// Conventions, Class family, Q16). Java's map keyed by ConfigResource is a
// ConfigResourceMap (Q25).

import { JavaForms } from '../internal/args';
import { KafkaFuture } from '../common/kafka_future';
import type { ConfigResourceMap } from '../common/config/config_resource_map';

const VALUES_FORMS = new JavaForms('values', [[]], { reader: true });
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });

/**
 * The result of the `Admin.incrementalAlterConfigs(configs, options)` call.
 *
 * Instances come from the admin client's `incrementalAlterConfigs()`.
 */
export class AlterConfigsResult {
  readonly #futures: ConfigResourceMap<KafkaFuture<void>>;

  private constructor(futures: ConfigResourceMap<KafkaFuture<void>>) {
    this.#futures = futures;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param futures - the futures, by resource
   * @returns the result
   * @internal
   */
  static internalCreate(futures: ConfigResourceMap<KafkaFuture<void>>): AlterConfigsResult {
    return new AlterConfigsResult(futures);
  }

  /**
   * Return a map from resources to futures which can be used to check the
   * status of the operation on each resource.
   *
   * @returns the futures, by resource
   * @throws `TypeError` when it is given an argument: it only reads
   */
  values(): ConfigResourceMap<KafkaFuture<void>> {
    if (arguments.length !== 0) VALUES_FORMS.match(undefined, arguments.length);
    return this.#futures;
  }

  /**
   * Return a future which succeeds only if all the alter configs operations
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
