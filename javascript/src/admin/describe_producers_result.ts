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

// Java's org.apache.kafka.clients.admin.DescribeProducersResult, with its
// nested PartitionProducerState, a class of the namespace merged with it
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Names, Q11). The Result's
// Java constructor is package-private: a private constructor here, the client
// building it through `internalCreate` (Class family, Q16). Java's map keyed
// by TopicPartition is a TopicPartitionMap (Q25); `all()` is Java's `allOf`
// then `thenApply` over its futures.

import { JavaForms, kind } from '../internal/args';
import { illegalArgument } from '../internal/errors';
import { KafkaError } from '../common/kafka_error';
import { KafkaFuture } from '../common/kafka_future';
import { TopicPartition } from '../common/topic_partition';
import { TopicPartitionMap } from '../common/topic_partition_map';
import { ProducerState } from './producer_state';

const PARTITION_RESULT_FORMS = new JavaForms('partitionResult', [['partition']], {
  bareTypes: { partition: (value) => value === null || value instanceof TopicPartition },
});
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });
const PARTITION_PRODUCER_STATE_FORMS = new JavaForms('PartitionProducerState', [['activeProducers']], {
  bareTypes: { activeProducers: (value) => value === null || (typeof value === 'object' && Symbol.iterator in value) },
});
const ACTIVE_PRODUCERS_FORMS = new JavaForms('activeProducers', [[]], { reader: true });

/**
 * The result of the `Admin.describeProducers(partitions)` call.
 *
 * Instances come from the admin client's `describeProducers()`.
 */
export class DescribeProducersResult {
  readonly #futures: TopicPartitionMap<KafkaFuture<DescribeProducersResult.PartitionProducerState>>;

  private constructor(futures: TopicPartitionMap<KafkaFuture<DescribeProducersResult.PartitionProducerState>>) {
    this.#futures = futures;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param futures - the futures, by partition
   * @returns the result
   * @internal
   */
  static internalCreate(futures: TopicPartitionMap<KafkaFuture<DescribeProducersResult.PartitionProducerState>>): DescribeProducersResult {
    return new DescribeProducersResult(futures);
  }

  /**
   * The future of a partition's producer state.
   *
   * @param partition - the partition, on its own or as `{ partition }`
   * @returns the future
   * @throws `IllegalArgumentError` when the partition was not included in the
   *   request
   * @throws `TypeError` when it is neither a TopicPartition nor null
   */
  partitionResult(partition: TopicPartition): KafkaFuture<DescribeProducersResult.PartitionProducerState>;
  /**
   * The same as `partitionResult(partition)`, the partition given by name.
   *
   * @param options - the partition
   * @returns the future
   */
  partitionResult(options: {
    /** The partition. */
    partition: TopicPartition;
  }): KafkaFuture<DescribeProducersResult.PartitionProducerState>;
  partitionResult(partition: unknown): KafkaFuture<DescribeProducersResult.PartitionProducerState> {
    PARTITION_RESULT_FORMS.match(partition, arguments.length);
    const tp = PARTITION_RESULT_FORMS.value(partition, 'partition');
    if (tp !== null && !(tp instanceof TopicPartition)) throw new TypeError(`partition must be a TopicPartition, not ${kind(tp)}`);
    const future = this.#futures.get(tp);
    if (future === null) throw illegalArgument(`Topic partition ${String(tp)} was not included in the request`);
    return future;
  }

  /**
   * A future of every partition's producer state, by partition: it fails if
   * any partition's does.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<TopicPartitionMap<DescribeProducersResult.PartitionProducerState>> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    const entries = [...this.#futures];
    return KafkaFuture.allOf(entries.map(([, f]) => f)).thenApply(() => {
      const results = new TopicPartitionMap<DescribeProducersResult.PartitionProducerState>();
      for (const [tp, future] of entries) {
        try {
          results.set(tp, future.getNow({ valueIfAbsent: null }) as DescribeProducersResult.PartitionProducerState);
        } catch (e) {
          // This should be unreachable, because allOf ensured that all the
          // futures completed successfully.
          throw new KafkaError({ cause: e });
        }
      }
      return results;
    });
  }
}

// eslint-disable-next-line @typescript-eslint/no-namespace -- Java's nested class DescribeProducersResult.PartitionProducerState, reached through the class name as in Java
export namespace DescribeProducersResult {
  /**
   * The producer state of a partition: its active producers.
   */
  export class PartitionProducerState {
    readonly #activeProducers: readonly (ProducerState | null)[] | null;

    /**
     * The state of a partition.
     *
     * @param activeProducers - the active producers, on its own or as
     *   `{ activeProducers }` (null is stored, as Java's constructor stores it)
     * @throws `TypeError` when it is not an iterable of ProducerState
     */
    constructor(activeProducers: Iterable<ProducerState> | null);
    /**
     * The same as `new PartitionProducerState(activeProducers)`, the producers given by name.
     *
     * @param options - the active producers
     */
    constructor(options: {
      /** The active producers. */
      activeProducers: Iterable<ProducerState> | null;
    });
    constructor(options: unknown) {
      PARTITION_PRODUCER_STATE_FORMS.match(options, arguments.length);
      const given = PARTITION_PRODUCER_STATE_FORMS.value(options, 'activeProducers');
      if (given === null) {
        this.#activeProducers = null;
      } else {
        if (typeof given === 'string' || typeof given !== 'object' || !(Symbol.iterator in given)) {
          throw new TypeError(`activeProducers must be an iterable of ProducerState, not ${kind(given)}`);
        }
        const list: Array<ProducerState | null> = [];
        for (const p of given as Iterable<unknown>) {
          // Java's list holds what it is given, null included.
          if (p !== null && !(p instanceof ProducerState))
            throw new TypeError(`activeProducers must contain only ProducerState, not ${kind(p)}`);
          list.push(p);
        }
        this.#activeProducers = list;
      }
      Object.freeze(this);
    }

    /**
     * The active producers.
     *
     * @returns a new array of the producers, or null
     * @throws `TypeError` when it is given an argument: it only reads
     */
    activeProducers(): ProducerState[] | null {
      if (arguments.length !== 0) ACTIVE_PRODUCERS_FORMS.match(undefined, arguments.length);
      return this.#activeProducers === null ? null : ([...this.#activeProducers] as ProducerState[]);
    }

    /**
     * Java's `toString()`: `PartitionProducerState(activeProducers=[...])`.
     *
     * @returns the text
     */
    toString(): string {
      const list = this.#activeProducers === null ? 'null' : `[${this.#activeProducers.map(String).join(', ')}]`;
      return `PartitionProducerState(activeProducers=${list})`;
    }

    /**
     * The state as JSON: `{ activeProducers }`.
     *
     * @returns the fields
     *
     * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
     * once released.
     */
    toJSON(): Record<string, unknown> {
      return { activeProducers: this.#activeProducers === null ? null : [...this.#activeProducers] };
    }
  }
}
