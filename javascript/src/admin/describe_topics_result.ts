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

// Java's org.apache.kafka.clients.admin.DescribeTopicsResult. Its Java
// constructor is protected: a protected constructor here, and Java's
// package-private factories ofTopicIds and ofTopicNames are `@internal`
// statics the client calls (root CLAUDE.md, JavaScript Binding Conventions,
// Class family, Q16). Java's map keyed by Uuid is a UuidMap (Q25).

import { JavaForms } from '../internal/args';
import { IllegalArgumentError } from '../illegal_argument_error';
import { KafkaFuture } from '../common/kafka_future';
import { UuidMap } from '../common/uuid_map';
import type { TopicDescription } from './topic_description';

const TOPIC_ID_VALUES_FORMS = new JavaForms('topicIdValues', [[]], { reader: true });
const TOPIC_NAME_VALUES_FORMS = new JavaForms('topicNameValues', [[]], { reader: true });
const ALL_TOPIC_NAMES_FORMS = new JavaForms('allTopicNames', [[]], { reader: true });
const ALL_TOPIC_IDS_FORMS = new JavaForms('allTopicIds', [[]], { reader: true });

/**
 * The result of the `KafkaAdminClient.describeTopics(topics)` call.
 *
 * Instances come from the admin client's `describeTopics()`.
 */
export class DescribeTopicsResult {
  readonly #topicIdFutures: UuidMap<KafkaFuture<TopicDescription>> | null;
  readonly #nameFutures: Map<string, KafkaFuture<TopicDescription>> | null;

  /**
   * Java's protected constructor: exactly one of the two maps.
   *
   * @param topicIdFutures - the futures by topic id, or null
   * @param nameFutures - the futures by topic name, or null
   * @throws `IllegalArgumentError` when both or neither are given
   */
  protected constructor(
    topicIdFutures: UuidMap<KafkaFuture<TopicDescription>> | null,
    nameFutures: Map<string, KafkaFuture<TopicDescription>> | null,
  ) {
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
  static internalOfTopicIds(topicIdFutures: UuidMap<KafkaFuture<TopicDescription>>): DescribeTopicsResult {
    return new DescribeTopicsResult(topicIdFutures, null);
  }

  /**
   * Java's package-private `ofTopicNames`. Called by the admin client.
   *
   * @param nameFutures - the futures by topic name
   * @returns the result
   * @internal
   */
  static internalOfTopicNames(nameFutures: Map<string, KafkaFuture<TopicDescription>>): DescribeTopicsResult {
    return new DescribeTopicsResult(null, nameFutures);
  }

  /**
   * Use when `Admin.describeTopics(topics, options)` used a
   * TopicIdCollection.
   *
   * @returns a map from topic IDs to futures which can be used to check the
   *   status of individual topics if the request used topic IDs, otherwise
   *   return null.
   * @throws `TypeError` when it is given an argument: it only reads
   */
  topicIdValues(): UuidMap<KafkaFuture<TopicDescription>> | null {
    if (arguments.length !== 0) TOPIC_ID_VALUES_FORMS.match(undefined, arguments.length);
    return this.#topicIdFutures;
  }

  /**
   * Use when `Admin.describeTopics(topics, options)` used a
   * TopicNameCollection.
   *
   * @returns a map from topic names to futures which can be used to check
   *   the status of individual topics if the request used topic names,
   *   otherwise return null.
   * @throws `TypeError` when it is given an argument: it only reads
   */
  topicNameValues(): Map<string, KafkaFuture<TopicDescription>> | null {
    if (arguments.length !== 0) TOPIC_NAME_VALUES_FORMS.match(undefined, arguments.length);
    return this.#nameFutures;
  }

  /**
   * A future map from topic names to descriptions.
   *
   * @returns A future map from topic names to descriptions which can be used
   *   to check the status of individual description if the describe topic
   *   request used topic names, otherwise return null, this request succeeds
   *   only if all the topic descriptions succeed
   * @throws `TypeError` when it is given an argument: it only reads
   */
  allTopicNames(): KafkaFuture<Map<string, TopicDescription>> | null {
    if (arguments.length !== 0) ALL_TOPIC_NAMES_FORMS.match(undefined, arguments.length);
    const futures = this.#nameFutures;
    if (futures === null) return null;
    return all(futures.values()).thenApply(() => {
      const descriptions = new Map<string, TopicDescription>();
      for (const [name, future] of futures) descriptions.set(name, doneValue(future));
      return descriptions;
    });
  }

  /**
   * A future map from topic ids to descriptions.
   *
   * @returns A future map from topic ids to descriptions which can be used
   *   to check the status of individual description if the describe topic
   *   request used topic ids, otherwise return null, this request succeeds
   *   only if all the topic descriptions succeed
   * @throws `TypeError` when it is given an argument: it only reads
   */
  allTopicIds(): KafkaFuture<UuidMap<TopicDescription>> | null {
    if (arguments.length !== 0) ALL_TOPIC_IDS_FORMS.match(undefined, arguments.length);
    const futures = this.#topicIdFutures;
    if (futures === null) return null;
    return all(futures.values()).thenApply(() => {
      const descriptions = new UuidMap<TopicDescription>();
      for (const [id, future] of futures) descriptions.set(id, doneValue(future));
      return descriptions;
    });
  }
}

/**
 * Java's private `all(futures)`: a future that succeeds once all succeed.
 *
 * @param futures - the futures
 * @returns the future
 */
function all(futures: Iterable<KafkaFuture<TopicDescription>>): KafkaFuture<void> {
  return KafkaFuture.allOf([...futures]);
}

/**
 * The value of a future `allOf` found successful (Java's `get()`, which
 * cannot block or fail there).
 *
 * @param future - the future
 * @returns its value
 */
function doneValue(future: KafkaFuture<TopicDescription>): TopicDescription {
  return future.getNow({ valueIfAbsent: null }) as TopicDescription;
}
