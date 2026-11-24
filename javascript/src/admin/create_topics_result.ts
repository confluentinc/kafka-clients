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

// Java's org.apache.kafka.clients.admin.CreateTopicsResult. Its Java
// constructor is protected, taking one future of TopicMetadataAndConfig per
// topic, which `values()`, `config()`, `topicId()`, `numPartitions()` and
// `replicationFactor()` refine with thenApply: a protected constructor here,
// as Java's (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family).
// The C FFI hands out each refinement as a future of its own instead,
// so the client builds the Result through the `@internal` `internalCreate`,
// over the C result's per-topic futures and its refinements; `values()` and
// `all()` work alike on both.

import { JavaForms, kind } from '../internal/args';
import { checkInt } from '../internal/java_number';
import { ApiError } from '../common/errors/api_error';
import { KafkaFuture } from '../common/kafka_future';
import { Uuid } from '../common/uuid';
import { Config } from './config';

const TOPIC_FORMS = (name: string): JavaForms =>
  new JavaForms(name, [['topic']], { bareTypes: { topic: (value) => value === null || typeof value === 'string' } });
const CONFIG_FORMS = TOPIC_FORMS('config');
const TOPIC_ID_FORMS = TOPIC_FORMS('topicId');
const NUM_PARTITIONS_FORMS = TOPIC_FORMS('numPartitions');
const REPLICATION_FACTOR_FORMS = TOPIC_FORMS('replicationFactor');
const VALUES_FORMS = new JavaForms('values', [[]], { reader: true });
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });

/** The C result's per-topic futures (private). */
interface Refinements {
  config(topic: string): KafkaFuture<Config | null>;
  topicId(topic: string): KafkaFuture<Uuid>;
  numPartitions(topic: string): KafkaFuture<number>;
  replicationFactor(topic: string): KafkaFuture<number>;
}

/** Java's `CreateTopicsResult.UNKNOWN`: an unknown partition count or replication factor. */
const UNKNOWN = -1;

/**
 * The topic of a refinement call.
 *
 * @param forms - the method's forms
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the topic
 * @throws `TypeError` for a null topic: Java's map has no future for it,
 *   which Java's refinement dereferences
 */
function topicOf(forms: JavaForms, args: unknown, count: number): string {
  forms.match(args, count);
  const topic = forms.value(args, 'topic');
  if (topic === null) throw new TypeError('topic must not be null');
  if (typeof topic !== 'string') throw new TypeError(`topic must be a string, not ${kind(topic)}`);
  return topic;
}

/**
 * The result of `Admin.createTopics(newTopics)`.
 */
export class CreateTopicsResult {
  // The sources of values() and all(): Java's futures of
  // TopicMetadataAndConfig, or the C result's per-topic futures.
  readonly #futures: Map<string, KafkaFuture<unknown>>;
  // The C result's refinements; null for Java's futures, which are refined
  // with thenApply.
  #refinements: Refinements | null = null;

  /**
   * Java's protected constructor, for a subclass: one future of the topic's
   * metadata and configuration per topic.
   *
   * Instances come from the admin client's `createTopics()`.
   *
   * @param futures - the futures, by topic name
   * @throws `TypeError` when they are not a Map
   */
  protected constructor(futures: Map<string, KafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>>) {
    if (!(futures instanceof Map)) throw new TypeError(`futures must be a Map, not ${kind(futures)}`);
    this.#futures = futures;
  }

  /**
   * The Result over the C result's futures. Called by the admin client.
   *
   * @param futures - the C result's futures, by topic name
   * @param refinements - the per-topic futures of the C result
   * @returns the result
   * @internal
   */
  static internalCreate(
    futures: Map<string, KafkaFuture<void>>,
    refinements: {
      config(topic: string): KafkaFuture<Config | null>;
      topicId(topic: string): KafkaFuture<Uuid>;
      numPartitions(topic: string): KafkaFuture<number>;
      replicationFactor(topic: string): KafkaFuture<number>;
    },
  ): CreateTopicsResult {
    // The constructor stores the sources only; their type is the C result's.
    const result = new CreateTopicsResult(futures as unknown as Map<string, KafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>>);
    result.#refinements = refinements;
    return result;
  }

  /**
   * Return a map from topic names to futures, which can be used to check the
   * status of individual topic creations.
   *
   * @returns the futures, by topic name: new dependants of the topics'
   *   futures, in a new map, on every call
   * @throws `TypeError` when it is given an argument: it only reads
   */
  values(): Map<string, KafkaFuture<void>> {
    if (arguments.length !== 0) VALUES_FORMS.match(undefined, arguments.length);
    const values = new Map<string, KafkaFuture<void>>();
    for (const [topic, future] of this.#futures)
      values.set(
        topic,
        future.thenApply(() => null as unknown as void),
      );
    return values;
  }

  /**
   * Return a future which succeeds if all the topic creations succeed.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<void> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    return KafkaFuture.allOf([...this.#futures.values()]);
  }

  /**
   * The future of `topic`'s metadata and configuration, refined by `read`:
   * Java's `futures.get(topic).thenApply(read)`.
   *
   * @param topic - the topic
   * @param read - the TopicMetadataAndConfig accessor
   * @returns the dependant future
   * @throws `TypeError` when the topic was not in the request: Java
   *   dereferences the missing future
   */
  #refine<T>(topic: string, read: (m: CreateTopicsResult.TopicMetadataAndConfig) => T): KafkaFuture<T> {
    const future = this.#futures.get(topic);
    if (future === undefined) throw new TypeError(`futures has no future for topic ${topic}`);
    return future.thenApply((m) => read(m as CreateTopicsResult.TopicMetadataAndConfig));
  }

  /**
   * Returns a future that provides topic configs for the topic when the
   * request completes.
   *
   * If broker version doesn't support replication factor in the response,
   * the future fails with `UnsupportedVersionError`. If broker returned an
   * error for topic configs, it fails with the appropriate error. For example,
   * `TopicAuthorizationError` if user does not have permission to describe
   * topic configs. Note that the values for the type and documentation fields
   * will be null.
   *
   * @param topic - the topic, on its own or as `{ topic }`
   * @returns the future
   * @throws `TypeError` for a null topic
   *
   * @remarks *(deviation)* A topic not in the request fails the client's
   * future with `IllegalArgumentError`, where Java throws
   * `NullPointerException`. Why: the FFI answers so. Goes when: it answers as
   * Java.
   */
  config(topic: string): KafkaFuture<Config | null>;
  /**
   * The same as `config(topic)`, the topic given by name.
   *
   * @param options - the topic
   * @returns the future
   * @throws `TypeError` for a null topic
   *
   * @remarks *(deviation)* A topic not in the request fails the client's
   * future with `IllegalArgumentError`, where Java throws
   * `NullPointerException`. Why: the FFI answers so. Goes when: it answers as
   * Java.
   */
  config(options: {
    /** The topic. */
    topic: string;
  }): KafkaFuture<Config | null>;
  config(topic: unknown): KafkaFuture<Config | null> {
    const t = topicOf(CONFIG_FORMS, topic, arguments.length);
    return this.#refinements !== null ? this.#refinements.config(t) : this.#refine(t, (m) => m.config());
  }

  /**
   * Returns a future that provides topic ID for the topic when the request
   * completes.
   *
   * If broker version doesn't support replication factor in the response,
   * the future fails with `UnsupportedVersionError`. If broker returned an
   * error for topic configs, it fails with the appropriate error. For example,
   * `TopicAuthorizationError` if user does not have permission to describe
   * topic configs.
   *
   * @param topic - the topic, on its own or as `{ topic }`
   * @returns the future
   * @throws `TypeError` for a null topic
   *
   * @remarks *(deviation)* As for `config(topic)`: a topic not in the request
   * gets a future failed with `IllegalArgumentError`.
   */
  topicId(topic: string): KafkaFuture<Uuid>;
  /**
   * The same as `topicId(topic)`, the topic given by name.
   *
   * @param options - the topic
   * @returns the future
   * @throws `TypeError` for a null topic
   */
  topicId(options: {
    /** The topic. */
    topic: string;
  }): KafkaFuture<Uuid>;
  topicId(topic: unknown): KafkaFuture<Uuid> {
    const t = topicOf(TOPIC_ID_FORMS, topic, arguments.length);
    return this.#refinements !== null ? this.#refinements.topicId(t) : this.#refine(t, (m) => m.topicId() as Uuid);
  }

  /**
   * Returns a future that provides number of partitions in the topic when the
   * request completes.
   *
   * If broker version doesn't support replication factor in the response,
   * the future fails with `UnsupportedVersionError`. If broker returned an
   * error for topic configs, it fails with the appropriate error. For example,
   * `TopicAuthorizationError` if user does not have permission to describe
   * topic configs.
   *
   * @param topic - the topic, on its own or as `{ topic }`
   * @returns the future
   * @throws `TypeError` for a null topic
   *
   * @remarks *(deviation)* As for `config(topic)`: a topic not in the request
   * gets a future failed with `IllegalArgumentError`.
   */
  numPartitions(topic: string): KafkaFuture<number>;
  /**
   * The same as `numPartitions(topic)`, the topic given by name.
   *
   * @param options - the topic
   * @returns the future
   * @throws `TypeError` for a null topic
   */
  numPartitions(options: {
    /** The topic. */
    topic: string;
  }): KafkaFuture<number>;
  numPartitions(topic: unknown): KafkaFuture<number> {
    const t = topicOf(NUM_PARTITIONS_FORMS, topic, arguments.length);
    return this.#refinements !== null ? this.#refinements.numPartitions(t) : this.#refine(t, (m) => m.numPartitions());
  }

  /**
   * Returns a future that provides replication factor for the topic when the
   * request completes.
   *
   * If broker version doesn't support replication factor in the response,
   * the future fails with `UnsupportedVersionError`. If broker returned an
   * error for topic configs, it fails with the appropriate error. For example,
   * `TopicAuthorizationError` if user does not have permission to describe
   * topic configs.
   *
   * @param topic - the topic, on its own or as `{ topic }`
   * @returns the future
   * @throws `TypeError` for a null topic
   *
   * @remarks *(deviation)* As for `config(topic)`: a topic not in the request
   * gets a future failed with `IllegalArgumentError`.
   */
  replicationFactor(topic: string): KafkaFuture<number>;
  /**
   * The same as `replicationFactor(topic)`, the topic given by name.
   *
   * @param options - the topic
   * @returns the future
   * @throws `TypeError` for a null topic
   */
  replicationFactor(options: {
    /** The topic. */
    topic: string;
  }): KafkaFuture<number>;
  replicationFactor(topic: unknown): KafkaFuture<number> {
    const t = topicOf(REPLICATION_FACTOR_FORMS, topic, arguments.length);
    return this.#refinements !== null ? this.#refinements.replicationFactor(t) : this.#refine(t, (m) => m.replicationFactor());
  }
}

const METADATA_FORMS = new JavaForms('TopicMetadataAndConfig', [
  ['topicId', 'numPartitions', 'replicationFactor', 'config'],
  ['exception'],
]);

// eslint-disable-next-line @typescript-eslint/no-namespace -- Java's nested class CreateTopicsResult.TopicMetadataAndConfig, reached through the class name as in Java
export namespace CreateTopicsResult {
  /** The metadata and configuration of a created topic, or its failure. */
  export class TopicMetadataAndConfig {
    readonly #error: ApiError | null;
    readonly #topicId: Uuid | null;
    readonly #numPartitions: number;
    readonly #replicationFactor: number;
    readonly #config: Config | null;

    /**
     * The topic's metadata and configuration.
     *
     * @param options - the topic id, the number of partitions, the
     *   replication factor and the configuration
     * @throws `IllegalArgumentError` when the given parameters are no Java form
     * @throws `TypeError` for a value of the wrong type
     */
    constructor(options: {
      /** The topic id. */
      topicId: Uuid | null;
      /** The number of partitions. */
      numPartitions: number;
      /** The replication factor. */
      replicationFactor: number;
      /** The configuration. */
      config: Config | null;
    });
    /**
     * The topic's failure: its accessors throw it.
     *
     * @param options - the error
     * @throws `IllegalArgumentError` when the given parameters are no Java form
     * @throws `TypeError` for a value of the wrong type
     */
    constructor(options: {
      /** The error. */
      exception: ApiError | null;
    });
    constructor(options: unknown) {
      const form = METADATA_FORMS.match(options, arguments.length);
      if (form === 1) {
        const error = METADATA_FORMS.value(options, 'exception');
        if (error !== null && !(error instanceof ApiError))
          throw new TypeError(`exception must be an ApiError or null, not ${kind(error)}`);
        this.#error = error;
        this.#topicId = Uuid.ZERO_UUID;
        this.#numPartitions = UNKNOWN;
        this.#replicationFactor = UNKNOWN;
        this.#config = null;
        return;
      }
      const topicId = METADATA_FORMS.value(options, 'topicId');
      if (topicId !== null && !(topicId instanceof Uuid)) throw new TypeError(`topicId must be a Uuid or null, not ${kind(topicId)}`);
      const config = METADATA_FORMS.value(options, 'config');
      if (config !== null && !(config instanceof Config)) throw new TypeError(`config must be a Config or null, not ${kind(config)}`);
      this.#error = null;
      this.#topicId = topicId;
      this.#numPartitions = checkInt(METADATA_FORMS.value(options, 'numPartitions'), 'numPartitions');
      this.#replicationFactor = checkInt(METADATA_FORMS.value(options, 'replicationFactor'), 'replicationFactor');
      this.#config = config;
    }

    /**
     * The topic id.
     *
     * @returns the topic id
     * @throws `ApiError` the topic's failure
     */
    topicId(): Uuid | null {
      this.#ensureSuccess();
      return this.#topicId;
    }

    /**
     * The number of partitions.
     *
     * @returns the number of partitions
     * @throws `ApiError` the topic's failure
     */
    numPartitions(): number {
      this.#ensureSuccess();
      return this.#numPartitions;
    }

    /**
     * The replication factor.
     *
     * @returns the replication factor
     * @throws `ApiError` the topic's failure
     */
    replicationFactor(): number {
      this.#ensureSuccess();
      return this.#replicationFactor;
    }

    /**
     * The configuration.
     *
     * @returns the configuration
     * @throws `ApiError` the topic's failure
     */
    config(): Config | null {
      this.#ensureSuccess();
      return this.#config;
    }

    /**
     * The topic's metadata and configuration as JSON, or its failure as its text.
     *
     * @returns the fields
     *
     * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
     * once released.
     */
    toJSON(): Record<string, unknown> {
      return {
        topicId: this.#topicId,
        numPartitions: this.#numPartitions,
        replicationFactor: this.#replicationFactor,
        config: this.#config,
        exception: this.#error === null ? null : String(this.#error),
      };
    }

    #ensureSuccess(): void {
      if (this.#error !== null) throw this.#error;
    }
  }
}
