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

// Java's org.apache.kafka.clients.admin.ListTopicsResult. Its Java
// constructor is package-private: a private constructor here, the client
// building it through `internalCreate`. `listings()` and `names()` are Java's
// `thenApply` views of the one future: `Map.values()` is no `Set`, so
// `listings()` yields an array, and `Map.keySet()` a `Set`.

import { JavaForms } from '../internal/args';
import type { KafkaFuture } from '../common/kafka_future';
import type { TopicListing } from './topic_listing';

const NAMES_TO_LISTINGS_FORMS = new JavaForms('namesToListings', [[]], { reader: true });
const LISTINGS_FORMS = new JavaForms('listings', [[]], { reader: true });
const NAMES_FORMS = new JavaForms('names', [[]], { reader: true });

/**
 * The result of the `Admin.listTopics()` call.
 */
export class ListTopicsResult {
  readonly #future: KafkaFuture<Map<string, TopicListing>>;

  /**
   * Instances come from the admin client's `listTopics()`.
   *
   * @param future - the future of the topics' listings by name
   */
  private constructor(future: KafkaFuture<Map<string, TopicListing>>) {
    this.#future = future;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param future - the future of the topics' listings by name
   * @returns the result
   * @internal
   */
  static internalCreate(future: KafkaFuture<Map<string, TopicListing>>): ListTopicsResult {
    return new ListTopicsResult(future);
  }

  /**
   * Return a future which yields a map of topic names to TopicListing objects.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  namesToListings(): KafkaFuture<Map<string, TopicListing>> {
    if (arguments.length !== 0) NAMES_TO_LISTINGS_FORMS.match(undefined, arguments.length);
    return this.#future;
  }

  /**
   * Return a future which yields a collection of TopicListing objects.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  listings(): KafkaFuture<TopicListing[]> {
    if (arguments.length !== 0) LISTINGS_FORMS.match(undefined, arguments.length);
    return this.#future.thenApply((listings) => [...listings.values()]);
  }

  /**
   * Return a future which yields a collection of topic names.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  names(): KafkaFuture<Set<string>> {
    if (arguments.length !== 0) NAMES_FORMS.match(undefined, arguments.length);
    return this.#future.thenApply((listings) => new Set(listings.keys()));
  }
}
