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

// Java's org.apache.kafka.clients.admin.UnregisterControllerResult. Its Java
// constructor is package-private: a private constructor here, the client
// building it through `internalCreate` (root CLAUDE.md, JavaScript Binding
// Conventions, Class family, Q16).

import { JavaForms } from '../internal/args';
import type { KafkaFuture } from '../common/kafka_future';

const ALL_FORMS = new JavaForms('all', [[]], { reader: true });

/**
 * The result of the `Admin.unregisterController(controllerId)` call.
 *
 * Instances come from the admin client's `unregisterController()`.
 */
export class UnregisterControllerResult {
  readonly #future: KafkaFuture<void>;

  private constructor(future: KafkaFuture<void>) {
    this.#future = future;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param future - the future of the unregistration
   * @returns the result
   * @internal
   */
  static internalCreate(future: KafkaFuture<void>): UnregisterControllerResult {
    return new UnregisterControllerResult(future);
  }

  /**
   * Return a future which succeeds if the operation is successful.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<void> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    return this.#future;
  }
}
