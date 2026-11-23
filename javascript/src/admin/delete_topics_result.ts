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

// Java's org.apache.kafka.clients.admin.DeleteTopicsResult. Its Java
// constructor is protected: a protected constructor here, and Java's
// package-private factories ofTopicIds and ofTopicNames are `@internal`
// statics the client calls (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Class family). Java's map keyed by Uuid is a UuidMap.

import { JavaForms } from '../internal/args';
import { IllegalArgumentError } from '../illegal_argument_error';
import { KafkaFuture } from '../common/kafka_future';
import type { UuidMap } from '../common/uuid_map';

const TOPIC_ID_VALUES_FORMS = new JavaForms('topicIdValues', [[]], { reader: true });
const TOPIC_NAME_VALUES_FORMS = new JavaForms('topicNameValues', [[]], { reader: true });
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });

/**
 * The result of the `Admin.deleteTopics(topics)` call.
 *
 * Instances come from the admin client's `deleteTopics()`.
 */
export class DeleteTopicsResult {
  readonly #topicIdFutures: UuidMap<KafkaFuture<void>> | null;
  readonly #nameFutures: Map<string, KafkaFuture<void>> | null;

  /**
   * Java's protected constructor: exactly one of the two maps.
   *
   * @param topicIdFutures - the futures by topic id, or null
   * @param nameFutures - the futures by topic name, or null
   * @throws `IllegalArgumentError` when both or neither are given
   */
  protected constructor(topicIdFutures: UuidMap<KafkaFuture<void>> | null, nameFutures: Map<string, KafkaFuture<void>> | null) {
    if (topicIdFutures !== null && nameFutures !== null)
      throw new IllegalArgumentError('topicIdFutures and nameFutures cannot both be specified.');
    if (topicIdFutures === null && nameFutures === null)
      throw new IllegalArgumentError('topicIdFutures and nameFutures cannot both be null.');
    this.#topicIdFutures = topicIdFutures;
    this.#nameFutures = nameFutures;
  }

  /**
   * Java's package-private `ofTopicIds`. Called by the admin client.
   *
   * @param topicIdFutures - the futures by topic id
   * @returns the result
   * @internal
   */
  static internalOfTopicIds(topicIdFutures: UuidMap<KafkaFuture<void>>): DeleteTopicsResult {
    return new DeleteTopicsResult(topicIdFutures, null);
  }

  /**
   * Java's package-private `ofTopicNames`. Called by the admin client.
   *
   * @param nameFutures - the futures by topic name
   * @returns the result
   * @internal
   */
  static internalOfTopicNames(nameFutures: Map<string, KafkaFuture<void>>): DeleteTopicsResult {
    return new DeleteTopicsResult(null, nameFutures);
  }

  /**
   * Use when `Admin.deleteTopics(topics, options)` used a TopicIdCollection.
   *
   * @returns a map from topic IDs to futures which can be used to check the
   *   status of individual deletions if the deleteTopics request used topic
   *   IDs. Otherwise return null.
   * @throws `TypeError` when it is given an argument: it only reads
   */
  topicIdValues(): UuidMap<KafkaFuture<void>> | null {
    if (arguments.length !== 0) TOPIC_ID_VALUES_FORMS.match(undefined, arguments.length);
    return this.#topicIdFutures;
  }

  /**
   * Use when `Admin.deleteTopics(topics, options)` used a
   * TopicNameCollection.
   *
   * @returns a map from topic names to futures which can be used to check the
   *   status of individual deletions if the deleteTopics request used topic
   *   names. Otherwise return null.
   * @throws `TypeError` when it is given an argument: it only reads
   */
  topicNameValues(): Map<string, KafkaFuture<void>> | null {
    if (arguments.length !== 0) TOPIC_NAME_VALUES_FORMS.match(undefined, arguments.length);
    return this.#nameFutures;
  }

  /**
   * Return a future which succeeds only if all the topic deletions succeed.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<void> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    const futures = this.#topicIdFutures === null ? this.#nameFutures!.values() : this.#topicIdFutures.values();
    return KafkaFuture.allOf([...futures]);
  }
}
