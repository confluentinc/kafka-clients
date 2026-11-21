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

// Java's org.apache.kafka.clients.admin.UpdateFeaturesResult. Its Java
// constructor is package-private: a private constructor here, the client
// building it through `internalCreate` (root CLAUDE.md, JavaScript Binding
// Conventions, Class family, Q16). `all()` is Java's `allOf` over the
// futures.

import { JavaForms } from '../internal/args';
import { KafkaFuture } from '../common/kafka_future';

const VALUES_FORMS = new JavaForms('values', [[]], { reader: true });
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });

/**
 * The result of the `Admin.updateFeatures(featureUpdates)` call.
 *
 * Instances come from the admin client's `updateFeatures()`.
 */
export class UpdateFeaturesResult {
  readonly #futures: Map<string, KafkaFuture<void>>;

  private constructor(futures: Map<string, KafkaFuture<void>>) {
    this.#futures = futures;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param futures - a map from feature name to future, which can be used to
   *   check the status of individual feature updates
   * @returns the result
   * @internal
   */
  static internalCreate(futures: Map<string, KafkaFuture<void>>): UpdateFeaturesResult {
    return new UpdateFeaturesResult(futures);
  }

  /**
   * A map from feature name to future, which can be used to check the status
   * of individual feature updates.
   *
   * @returns the futures, by feature name
   * @throws `TypeError` when it is given an argument: it only reads
   */
  values(): Map<string, KafkaFuture<void>> {
    if (arguments.length !== 0) VALUES_FORMS.match(undefined, arguments.length);
    return this.#futures;
  }

  /**
   * Return a future which succeeds if all the feature updates succeed.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<void> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    return KafkaFuture.allOf([...this.#futures.values()]);
  }
}
