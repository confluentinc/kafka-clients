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

// Java's org.apache.kafka.clients.admin.FenceProducersResult. Its Java
// constructor is package-private: a private constructor here, the client
// building it through `internalCreate` (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Class family, Q16). Java keys its futures of the internal
// `ProducerIdAndEpoch` by the internal `CoordinatorKey.byTransactionalId`,
// which compares by the id alone: here a `Map` keyed by the id, of futures of
// `{ producerId, epoch }`. Each method composes over them as Java's does:
// `fencedProducers()`, `producerId()` and `epochId()` with `thenApply`,
// `all()` with `allOf`.

import { JavaForms, kind } from '../internal/args';
import { illegalArgument } from '../internal/errors';
import { KafkaFuture } from '../common/kafka_future';

const FENCED_PRODUCERS_FORMS = new JavaForms('fencedProducers', [[]], { reader: true });
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });
const isId = (value: unknown): boolean => value === null || typeof value === 'string';
const PRODUCER_ID_FORMS = new JavaForms('producerId', [['transactionalId']], { bareTypes: { transactionalId: isId } });
const EPOCH_ID_FORMS = new JavaForms('epochId', [['transactionalId']], { bareTypes: { transactionalId: isId } });

/** Java's null, the value of a `KafkaFuture<Void>`. */
const VOID = null as unknown as void;

/** Java's internal `ProducerIdAndEpoch`, as each future delivers it. */
type ProducerIdAndEpoch = { readonly producerId: bigint; readonly epoch: number };

/**
 * The result of the `Admin.fenceProducers(transactionalIds)` call.
 *
 * Instances come from the admin client's `fenceProducers()`.
 */
export class FenceProducersResult {
  readonly #futures: ReadonlyMap<string, KafkaFuture<ProducerIdAndEpoch>>;

  private constructor(futures: ReadonlyMap<string, KafkaFuture<ProducerIdAndEpoch>>) {
    this.#futures = futures;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param futures - the futures of each id's producer id and epoch
   * @returns the result
   * @internal
   */
  static internalCreate(
    futures: ReadonlyMap<string, KafkaFuture<{ readonly producerId: bigint; readonly epoch: number }>>,
  ): FenceProducersResult {
    return new FenceProducersResult(futures);
  }

  /**
   * Return a map from transactional ID to futures which can be used to check
   * the status of individual fencings.
   *
   * @returns a new map of new futures, by transactional ID
   * @throws `TypeError` when it is given an argument: it only reads
   */
  fencedProducers(): Map<string, KafkaFuture<void>> {
    if (arguments.length !== 0) FENCED_PRODUCERS_FORMS.match(undefined, arguments.length);
    const out = new Map<string, KafkaFuture<void>>();
    for (const [id, future] of this.#futures)
      out.set(
        id,
        future.thenApply(() => VOID),
      );
    return out;
  }

  /**
   * Returns a future that provides the producer ID generated while
   * initializing the given transaction when the request completes.
   *
   * @param transactionalId - the transactional ID
   * @returns the future
   * @throws `IllegalArgumentError` if the `transactionalId` was not included in the request
   */
  producerId(transactionalId: string): KafkaFuture<bigint>;
  /**
   * The same as `producerId(transactionalId)`, the id given by name.
   *
   * @param options - the transactional ID
   * @returns the future
   */
  producerId(options: {
    /** The transactional ID. */
    transactionalId: string;
  }): KafkaFuture<bigint>;
  producerId(transactionalId: unknown): KafkaFuture<bigint> {
    PRODUCER_ID_FORMS.match(transactionalId, arguments.length);
    return this.#find(PRODUCER_ID_FORMS.value(transactionalId, 'transactionalId')).thenApply((p) => p.producerId);
  }

  /**
   * Returns a future that provides the epoch ID generated while initializing
   * the given transaction when the request completes.
   *
   * @param transactionalId - the transactional ID
   * @returns the future
   * @throws `IllegalArgumentError` if the `transactionalId` was not included in the request
   */
  epochId(transactionalId: string): KafkaFuture<number>;
  /**
   * The same as `epochId(transactionalId)`, the id given by name.
   *
   * @param options - the transactional ID
   * @returns the future
   */
  epochId(options: {
    /** The transactional ID. */
    transactionalId: string;
  }): KafkaFuture<number>;
  epochId(transactionalId: unknown): KafkaFuture<number> {
    EPOCH_ID_FORMS.match(transactionalId, arguments.length);
    return this.#find(EPOCH_ID_FORMS.value(transactionalId, 'transactionalId')).thenApply((p) => p.epoch);
  }

  /**
   * Return a future which succeeds only if all the producer fencings succeed.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<void> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    return KafkaFuture.allOf([...this.#futures.values()]);
  }

  /**
   * The future of an id, Java's `findAndApply` lookup.
   *
   * @param id - the transactional ID
   * @returns its future
   */
  #find(id: unknown): KafkaFuture<ProducerIdAndEpoch> {
    if (id !== null && typeof id !== 'string') throw new TypeError(`transactionalId must be a string, not ${kind(id)}`);
    const future = id === null ? undefined : this.#futures.get(id);
    if (future === undefined) throw illegalArgument(`TransactionalId \`${String(id)}\` was not included in the request`);
    return future;
  }
}
