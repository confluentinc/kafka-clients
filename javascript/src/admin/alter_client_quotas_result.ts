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

// Java's org.apache.kafka.clients.admin.AlterClientQuotasResult, whose Java
// constructor is public and stores its map as given, a null included, which
// `all()` dereferences. Java's map keyed by ClientQuotaEntity is a
// ClientQuotaEntityMap (root CLAUDE.md, JavaScript Binding Conventions, Class
// family, Q25); `all()` is Java's `allOf` over its futures.

import { JavaForms, kind } from '../internal/args';
import { KafkaFuture } from '../common/kafka_future';
import { ClientQuotaEntityMap } from '../common/quota/client_quota_entity_map';

const FORMS = new JavaForms('AlterClientQuotasResult', [['futures']]);
const VALUES_FORMS = new JavaForms('values', [[]], { reader: true });
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });

/**
 * The result of the `Admin.alterClientQuotas(entries)` call.
 */
export class AlterClientQuotasResult {
  readonly #futures: ClientQuotaEntityMap<KafkaFuture<void>> | null;

  /**
   * Maps an entity to its alteration result.
   *
   * @param futures - maps entity to its alteration result, on its own
   * @throws `TypeError` when they are not a ClientQuotaEntityMap
   */
  constructor(futures: ClientQuotaEntityMap<KafkaFuture<void>>);
  /**
   * The same as `new AlterClientQuotasResult(futures)`.
   *
   * @param options - maps entity to its alteration result
   * @throws `TypeError` when they are not a ClientQuotaEntityMap
   */
  constructor(options: {
    /** Maps entity to its alteration result. */
    futures: ClientQuotaEntityMap<KafkaFuture<void>>;
  });
  /**
   * Builds the result.
   *
   * @param options - the futures
   * @throws `TypeError` when they are not a ClientQuotaEntityMap
   */
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const futures = FORMS.value(options, 'futures');
    if (futures !== null && !(futures instanceof ClientQuotaEntityMap)) {
      throw new TypeError(`futures must be a ClientQuotaEntityMap, not ${kind(futures)}: build one with new ClientQuotaEntityMap()`);
    }
    this.#futures = futures as ClientQuotaEntityMap<KafkaFuture<void>> | null;
  }

  /**
   * Returns a map from quota entity to a future which can be used to check
   * the status of the operation.
   *
   * @returns the futures, by entity
   * @throws `TypeError` when it is given an argument: it only reads
   */
  values(): ClientQuotaEntityMap<KafkaFuture<void>> {
    if (arguments.length !== 0) VALUES_FORMS.match(undefined, arguments.length);
    // Java only stores a null map, so the type is the non-null one.
    return this.#futures as ClientQuotaEntityMap<KafkaFuture<void>>;
  }

  /**
   * Returns a future which succeeds only if all quota alterations succeed.
   *
   * @returns the future
   * @throws `TypeError` when the futures are null (Java dereferences them)
   */
  all(): KafkaFuture<void> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    if (this.#futures === null) throw new TypeError('futures must not be null');
    return KafkaFuture.allOf([...this.#futures.values()]);
  }
}
