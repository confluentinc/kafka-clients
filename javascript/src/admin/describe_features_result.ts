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

// Java's org.apache.kafka.clients.admin.DescribeFeaturesResult. Its Java
// constructor is protected: a protected constructor here, and an `@internal`
// static the client calls (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Class family, Q16).

import { JavaForms } from '../internal/args';
import type { KafkaFuture } from '../common/kafka_future';
import type { FeatureMetadata } from './feature_metadata';

const FEATURE_METADATA_FORMS = new JavaForms('featureMetadata', [[]], { reader: true });

/**
 * The result of the `Admin.describeFeatures({ options })` call.
 *
 * Instances come from the admin client's `describeFeatures()`.
 */
export class DescribeFeaturesResult {
  readonly #future: KafkaFuture<FeatureMetadata>;

  /**
   * Java's protected constructor.
   *
   * @param future - the future of the feature metadata
   */
  protected constructor(future: KafkaFuture<FeatureMetadata>) {
    this.#future = future;
  }

  /**
   * Java's protected constructor, for the admin client.
   *
   * @param future - the future of the feature metadata
   * @returns the result
   * @internal
   */
  static internalCreate(future: KafkaFuture<FeatureMetadata>): DescribeFeaturesResult {
    return new DescribeFeaturesResult(future);
  }

  /**
   * The future of the cluster's finalized and supported features.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  featureMetadata(): KafkaFuture<FeatureMetadata> {
    if (arguments.length !== 0) FEATURE_METADATA_FORMS.match(undefined, arguments.length);
    return this.#future;
  }
}
