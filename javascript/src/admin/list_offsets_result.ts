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

// Java's org.apache.kafka.clients.admin.ListOffsetsResult, with its nested
// ListOffsetsResultInfo, a class of the namespace merged with it (root
// CLAUDE.md, JavaScript Binding Conventions, Names, Q11). Both Java
// constructors are public. Java's map keyed by TopicPartition is a
// TopicPartitionMap (Q25); the client builds it from the native result's
// `partitionResult` of each partition it asked for, as the Python client
// does.

import { JavaForms, kind } from '../internal/args';
import { checkInt, checkLong } from '../internal/java_number';
import { IllegalArgumentError } from '../illegal_argument_error';
import { KafkaFuture } from '../common/kafka_future';
import { TopicPartition } from '../common/topic_partition';
import { TopicPartitionMap } from '../common/topic_partition_map';

const FORMS = new JavaForms('ListOffsetsResult', [['futures']]);
const PARTITION_RESULT_FORMS = new JavaForms('partitionResult', [['partition']], {
  bareTypes: { partition: (value) => value === null || value instanceof TopicPartition },
});
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });
const INFO_FORMS = new JavaForms('ListOffsetsResultInfo', [['offset', 'timestamp', 'leaderEpoch']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const OFFSET_FORMS = READER('offset');
const TIMESTAMP_FORMS = READER('timestamp');
const LEADER_EPOCH_FORMS = READER('leaderEpoch');

/**
 * The result of the `Admin.listOffsets(topicPartitionOffsets)` call.
 */
export class ListOffsetsResult {
  readonly #futures: TopicPartitionMap<KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> | null;

  /**
   * The result over the futures of each partition.
   *
   * @param futures - the futures, by partition, kept as given (null too, as
   *   Java's constructor stores it)
   * @throws `TypeError` for a value that is not a `TopicPartitionMap`
   */
  constructor(futures: TopicPartitionMap<KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>>);
  /**
   * The same as `new ListOffsetsResult(futures)`, the futures given by name.
   *
   * @param options - the futures, by partition
   */
  constructor(options: {
    /** The futures, by partition, kept as given. */
    futures: TopicPartitionMap<KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>>;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const futures = FORMS.value(options, 'futures');
    // Java's constructor stores a null map; its methods dereference it.
    if (futures !== null && !(futures instanceof TopicPartitionMap)) {
      throw new TypeError(`futures must be a TopicPartitionMap, not ${kind(futures)}: build one with new TopicPartitionMap()`);
    }
    this.#futures = futures as TopicPartitionMap<KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> | null;
  }

  /**
   * Return a future which can be used to check the result for a given
   * partition.
   *
   * @param partition - the partition, on its own or as `{ partition }`
   * @returns the future
   * @throws `IllegalArgumentError` "List Offsets for partition "<partition>"
   *   was not attempted" for a partition the call did not ask for
   * @throws `TypeError` "futures must not be null" when the futures are null
   *   (Java dereferences them)
   */
  partitionResult(partition: TopicPartition | null): KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>;
  /**
   * The same as `partitionResult(partition)`, the partition given by name.
   *
   * @param options - the partition
   * @returns the future
   */
  partitionResult(options: {
    /** The partition. */
    partition: TopicPartition | null;
  }): KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>;
  partitionResult(options: unknown): KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo> {
    PARTITION_RESULT_FORMS.match(options, arguments.length);
    const partition = PARTITION_RESULT_FORMS.value(options, 'partition');
    if (partition !== null && !(partition instanceof TopicPartition)) {
      throw new TypeError(`partition must be a TopicPartition, not ${kind(partition)}`);
    }
    if (this.#futures === null) throw new TypeError('futures must not be null');
    const future = this.#futures.get(partition);
    if (future === null) throw new IllegalArgumentError(`List Offsets for partition "${String(partition)}" was not attempted`);
    return future;
  }

  /**
   * Return a future which succeeds only if offsets for all specified
   * partitions have been successfully retrieved.
   *
   * @returns the future of the offsets, by partition
   * @throws `TypeError` when it is given an argument: it only reads, or when
   *   the futures are null (Java dereferences them)
   */
  all(): KafkaFuture<TopicPartitionMap<ListOffsetsResult.ListOffsetsResultInfo>> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    const futures = this.#futures;
    if (futures === null) throw new TypeError('futures must not be null');
    return KafkaFuture.allOf([...futures.values()]).thenApply(() => {
      const offsets = new TopicPartitionMap<ListOffsetsResult.ListOffsetsResultInfo>();
      for (const [partition, future] of futures) {
        offsets.set(partition, future.getNow({ valueIfAbsent: null }) as ListOffsetsResult.ListOffsetsResultInfo);
      }
      return offsets;
    });
  }
}

// eslint-disable-next-line @typescript-eslint/no-namespace -- Java's nested class ListOffsetsResult.ListOffsetsResultInfo, reached through the class name as in Java
export namespace ListOffsetsResult {
  /**
   * The offset of a partition, its timestamp and its leader epoch.
   */
  export class ListOffsetsResultInfo {
    readonly #offset: bigint;
    readonly #timestamp: bigint;
    readonly #leaderEpoch: number | null;

    /**
     * A partition's listed offset.
     *
     * @param options - the offset, its timestamp, and the leader epoch or
     *   null (Java's `Optional.empty()`)
     * @throws `IllegalArgumentError` when the given parameters are not
     *   `{offset, timestamp, leaderEpoch}`
     * @throws `TypeError` for a value of the wrong type
     * @throws `RangeError` for a number beyond 2^53 - 1 or a bigint beyond
     *   Java's `long`, or an epoch outside Java's `int`
     */
    constructor(options: {
      /** The offset, a `bigint` or a whole number. */
      offset: bigint | number;
      /** The timestamp, a `bigint` or a whole number. */
      timestamp: bigint | number;
      /** The leader epoch, or null when it is not known (Java's `Optional.empty()`). */
      leaderEpoch: number | null;
    }) {
      INFO_FORMS.match(options, arguments.length);
      this.#offset = checkLong(INFO_FORMS.value(options, 'offset'), 'offset');
      this.#timestamp = checkLong(INFO_FORMS.value(options, 'timestamp'), 'timestamp');
      const epoch = INFO_FORMS.value(options, 'leaderEpoch');
      this.#leaderEpoch = epoch === null ? null : checkInt(epoch, 'leaderEpoch');
    }

    /**
     * The offset.
     *
     * @returns the offset
     * @throws `TypeError` when it is given an argument: it only reads
     */
    offset(): bigint {
      if (arguments.length !== 0) OFFSET_FORMS.match(undefined, arguments.length);
      return this.#offset;
    }

    /**
     * The timestamp.
     *
     * @returns the timestamp
     * @throws `TypeError` when it is given an argument: it only reads
     */
    timestamp(): bigint {
      if (arguments.length !== 0) TIMESTAMP_FORMS.match(undefined, arguments.length);
      return this.#timestamp;
    }

    /**
     * The leader epoch.
     *
     * @returns the epoch, or null when it is not known (Java's `Optional.empty()`)
     * @throws `TypeError` when it is given an argument: it only reads
     */
    leaderEpoch(): number | null {
      if (arguments.length !== 0) LEADER_EPOCH_FORMS.match(undefined, arguments.length);
      return this.#leaderEpoch;
    }

    /**
     * Java's `toString()`: `ListOffsetsResultInfo(offset=<n>, timestamp=<n>,
     * leaderEpoch=Optional[<n>])`, `Optional.empty` for no epoch.
     *
     * @returns the text
     */
    toString(): string {
      const epoch = this.#leaderEpoch === null ? 'Optional.empty' : `Optional[${this.#leaderEpoch}]`;
      return `ListOffsetsResultInfo(offset=${this.#offset}, timestamp=${this.#timestamp}, leaderEpoch=${epoch})`;
    }

    /**
     * The information as `JSON.stringify` writes it: `{ offset, timestamp,
     * leaderEpoch }`, each `long` as a decimal string.
     *
     * @returns the information's fields
     *
     * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
     * once released.
     */
    toJSON(): { offset: string; timestamp: string; leaderEpoch: number | null } {
      return { offset: this.#offset.toString(), timestamp: this.#timestamp.toString(), leaderEpoch: this.#leaderEpoch };
    }
  }
}
