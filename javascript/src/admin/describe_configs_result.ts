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

// Java's org.apache.kafka.clients.admin.DescribeConfigsResult. Its Java
// constructor is protected: a protected constructor here, and an `@internal`
// static the client calls (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Class family). Java's map keyed by ConfigResource is a
// ConfigResourceMap.

import { JavaForms } from '../internal/args';
import { KafkaFuture } from '../common/kafka_future';
import { ConfigResourceMap } from '../common/config/config_resource_map';
import type { Config } from './config';

const VALUES_FORMS = new JavaForms('values', [[]], { reader: true });
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });

/**
 * The result of the `KafkaAdminClient.describeConfigs(resources)` call.
 *
 * Instances come from the admin client's `describeConfigs()`.
 */
export class DescribeConfigsResult {
  readonly #futures: ConfigResourceMap<KafkaFuture<Config>>;

  /**
   * Java's protected constructor.
   *
   * @param futures - the futures, by resource
   */
  protected constructor(futures: ConfigResourceMap<KafkaFuture<Config>>) {
    this.#futures = futures;
  }

  /**
   * Java's protected constructor, for the admin client.
   *
   * @param futures - the futures, by resource
   * @returns the result
   * @internal
   */
  static internalCreate(futures: ConfigResourceMap<KafkaFuture<Config>>): DescribeConfigsResult {
    return new DescribeConfigsResult(futures);
  }

  /**
   * Return a map from resources to futures which can be used to check the
   * status of the configuration for each resource.
   *
   * @returns the futures, by resource
   * @throws `TypeError` when it is given an argument: it only reads
   */
  values(): ConfigResourceMap<KafkaFuture<Config>> {
    if (arguments.length !== 0) VALUES_FORMS.match(undefined, arguments.length);
    return this.#futures;
  }

  /**
   * Return a future which succeeds only if all the config descriptions
   * succeed.
   *
   * @returns the future of the configurations, by resource
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<ConfigResourceMap<Config>> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    const futures = this.#futures;
    return KafkaFuture.allOf([...futures.values()]).thenApply(() => {
      const configs = new ConfigResourceMap<Config>();
      for (const [resource, future] of futures) configs.set(resource, future.getNow({ valueIfAbsent: null }) as Config);
      return configs;
    });
  }
}
