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

// Java's org.apache.kafka.clients.admin.DescribeClientQuotasResult, whose
// Java constructor is public and stores its future as given, a null
// included. Java's map keyed by ClientQuotaEntity is a ClientQuotaEntityMap
// (root CLAUDE.md, JavaScript Binding Conventions, Class family, Q25); each
// entity's quotas are a `Map` of quota name to value.

import { JavaForms, kind } from '../internal/args';
import { KafkaFuture } from '../common/kafka_future';
import type { ClientQuotaEntityMap } from '../common/quota/client_quota_entity_map';

const FORMS = new JavaForms('DescribeClientQuotasResult', [['entities']], {
  bareTypes: { entities: (value) => value === null || value instanceof KafkaFuture },
});
const ENTITIES_FORMS = new JavaForms('entities', [[]], { reader: true });

/**
 * The result of the `Admin.describeClientQuotas(filter)` call.
 */
export class DescribeClientQuotasResult {
  readonly #entities: KafkaFuture<ClientQuotaEntityMap<Map<string, number>>> | null;

  /**
   * Maps an entity to its configured quota value(s). Note if no value is
   * defined for a quota type for that entity's config, then it is not
   * included in the resulting value map.
   *
   * @param entities - future for the collection of entities that matched the
   *   filter, on its own
   * @throws `TypeError` when it is not a KafkaFuture
   */
  constructor(entities: KafkaFuture<ClientQuotaEntityMap<Map<string, number>>>);
  /**
   * The same as `new DescribeClientQuotasResult(entities)`.
   *
   * @param options - future for the collection of entities that matched the
   *   filter
   * @throws `TypeError` when it is not a KafkaFuture
   */
  constructor(options: {
    /** Future for the collection of entities that matched the filter. */
    entities: KafkaFuture<ClientQuotaEntityMap<Map<string, number>>>;
  });
  /**
   * Builds the result.
   *
   * @param options - the future
   * @throws `TypeError` when it is not a KafkaFuture
   */
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const entities = FORMS.value(options, 'entities');
    if (entities !== null && !(entities instanceof KafkaFuture))
      throw new TypeError(`entities must be a KafkaFuture, not ${kind(entities)}`);
    this.#entities = entities as KafkaFuture<ClientQuotaEntityMap<Map<string, number>>> | null;
  }

  /**
   * Returns a map from quota entity to a future which can be used to check
   * the status of the operation.
   *
   * @returns the future of the entities and their quotas
   * @throws `TypeError` when it is given an argument: it only reads
   */
  entities(): KafkaFuture<ClientQuotaEntityMap<Map<string, number>>> {
    if (arguments.length !== 0) ENTITIES_FORMS.match(undefined, arguments.length);
    // Java only stores a null future, so the type is the non-null one.
    return this.#entities as KafkaFuture<ClientQuotaEntityMap<Map<string, number>>>;
  }
}
