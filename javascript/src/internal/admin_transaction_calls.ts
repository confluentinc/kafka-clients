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

// The admin client's transaction and producer RPCs (private), as
// src/internal/admin_calls.ts has the others. Each checks the call's form and
// its arguments in Java's order, runs the RPC and builds its Result from the
// native result's futures, as Python's confluent_kafka/admin/_conversions.py
// composes them. A transaction state crosses the addon as its ordinal. Where
// Java stores a null and reads it only later, on its own thread, the call
// reports it with a `TypeError`, a deviation the admin package's
// documentation lists.

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way
import native = require('./native');
import { kind } from './args';
import { adminFuture, adminFutureOfSlot } from './admin_result';
import type { AdminHandle } from './admin_result';
import { instancesOf, optionSetters, optionsFirst, rpcForms, topicNames, voidOf } from './admin_calls';
import { partitionPairs, topicPartitionOf } from './admin_offset_calls';
import { KafkaFuture } from '../common/kafka_future';
import { TopicPartition } from '../common/topic_partition';
import { TopicPartitionMap } from '../common/topic_partition_map';
import { AbortTransactionOptions } from '../admin/abort_transaction_options';
import { AbortTransactionResult } from '../admin/abort_transaction_result';
import { AbortTransactionSpec } from '../admin/abort_transaction_spec';
import { DescribeProducersOptions } from '../admin/describe_producers_options';
import { DescribeProducersResult } from '../admin/describe_producers_result';
import { DescribeTransactionsOptions } from '../admin/describe_transactions_options';
import { DescribeTransactionsResult } from '../admin/describe_transactions_result';
import { FenceProducersOptions } from '../admin/fence_producers_options';
import { FenceProducersResult } from '../admin/fence_producers_result';
import { ListTransactionsOptions } from '../admin/list_transactions_options';
import { ListTransactionsResult } from '../admin/list_transactions_result';
import { ProducerState } from '../admin/producer_state';
import { TerminateTransactionOptions } from '../admin/terminate_transaction_options';
import { TerminateTransactionResult } from '../admin/terminate_transaction_result';
import { TransactionDescription } from '../admin/transaction_description';
import { TransactionListing } from '../admin/transaction_listing';
import { TransactionState } from '../admin/transaction_state';

const DESCRIBE_PRODUCERS_FORMS = rpcForms('describeProducers', ['partitions'], DescribeProducersOptions);
const DESCRIBE_TRANSACTIONS_FORMS = rpcForms('describeTransactions', ['transactionalIds'], DescribeTransactionsOptions);
const ABORT_TRANSACTION_FORMS = rpcForms('abortTransaction', ['spec'], AbortTransactionOptions);
const FORCE_TERMINATE_TRANSACTION_FORMS = rpcForms('forceTerminateTransaction', ['transactionalId'], TerminateTransactionOptions);
const LIST_TRANSACTIONS_FORMS = rpcForms('listTransactions', [], ListTransactionsOptions);
const FENCE_PRODUCERS_FORMS = rpcForms('fenceProducers', ['transactionalIds'], FenceProducersOptions);

/** A producer's state as the addon reads it. */
type ProducerStateFields = [bigint, number, number, bigint, number | null, bigint | null];

/** A transaction's description as the addon reads it. */
type TransactionDescriptionFields = [number, number, bigint, number, bigint, bigint | null, Array<[string, number]>];

/**
 * A partition's producer state the addon read: its active producers.
 *
 * @param raw - each producer's fields
 * @returns the state
 */
function partitionProducerStateOf(raw: unknown): DescribeProducersResult.PartitionProducerState {
  return new DescribeProducersResult.PartitionProducerState(
    (raw as ProducerStateFields[]).map(
      ([producerId, producerEpoch, lastSequence, lastTimestamp, coordinatorEpoch, currentTransactionStartOffset]) =>
        new ProducerState({ producerId, producerEpoch, lastSequence, lastTimestamp, coordinatorEpoch, currentTransactionStartOffset }),
    ),
  );
}

/**
 * A transaction's description the addon read.
 *
 * @param raw - its fields
 * @returns the description
 */
function transactionDescriptionOf(raw: unknown): TransactionDescription {
  const [coordinatorId, state, producerId, producerEpoch, transactionTimeoutMs, transactionStartTimeMs, partitions] =
    raw as TransactionDescriptionFields;
  return new TransactionDescription({
    coordinatorId,
    state: TransactionState.values()[state] ?? TransactionState.UNKNOWN,
    producerId,
    producerEpoch,
    transactionTimeoutMs,
    transactionStartTimeMs,
    topicPartitions: partitions.map(topicPartitionOf),
  });
}

/**
 * A broker's listings the addon read.
 *
 * @param raw - each listing's `[transactionalId, producerId, state]`
 * @returns the listings, in the broker's order
 */
function transactionListingsOf(raw: unknown): TransactionListing[] {
  return (raw as Array<[string, bigint, number]>).map(
    ([transactionalId, producerId, state]) =>
      new TransactionListing({
        transactionalId,
        producerId,
        transactionState: TransactionState.values()[state] ?? TransactionState.UNKNOWN,
      }),
  );
}

/**
 * The distinct ids of a Java `Collection<String>` of transactional ids,
 * checked in Java's order around the options.
 *
 * @param forms - the RPC's forms
 * @param args - the call's argument
 * @returns the ids, each once
 * @throws `TypeError` for null ids (Java dereferences them), null options
 *   (Java's next dereference), or a null id (the deviation the admin package
 *   lists)
 */
function transactionalIds(forms: ReturnType<typeof rpcForms>, args: unknown): string[] {
  // Java's first dereference is the collection's (newFuture's stream).
  const ids = topicNames(forms.value(args, 'transactionalIds'), 'transactionalIds');
  optionsFirst(forms, args);
  // CoordinatorKey stores a null id, which fails the future later.
  if (ids.includes(null)) throw new TypeError('transactionalIds must not contain null');
  return [...new Set(ids as string[])];
}

/**
 * `describeProducers(partitions)` and `describeProducers(partitions, options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function describeProducers(handle: AdminHandle, args: unknown, count: number): DescribeProducersResult {
  DESCRIBE_PRODUCERS_FORMS.match(args, count);
  // Java's first dereferences are the partitions' (new HashSet, then each
  // partition as a key of an unmodifiable map), then the options'
  // (brokerId()); it reads a partition's topic when it builds the request.
  const partitions = instancesOf(DESCRIBE_PRODUCERS_FORMS.value(args, 'partitions'), 'partitions', TopicPartition);
  optionsFirst(DESCRIBE_PRODUCERS_FORMS, args);
  const pairs = partitionPairs(partitions, 'partitions');
  const box = native.adminDescribeProducers(handle, pairs, optionSetters(DESCRIBE_PRODUCERS_FORMS, args, DescribeProducersOptions));
  const futures = new TopicPartitionMap<KafkaFuture<DescribeProducersResult.PartitionProducerState>>();
  for (const pair of pairs)
    futures.set(topicPartitionOf(pair), adminFuture(handle, box, 'partition_result', pair, partitionProducerStateOf));
  return DescribeProducersResult.internalCreate(futures);
}

/**
 * `describeTransactions(transactionalIds)` and
 * `describeTransactions(transactionalIds, options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function describeTransactions(handle: AdminHandle, args: unknown, count: number): DescribeTransactionsResult {
  DESCRIBE_TRANSACTIONS_FORMS.match(args, count);
  const ids = transactionalIds(DESCRIBE_TRANSACTIONS_FORMS, args);
  const box = native.adminDescribeTransactions(handle, ids, optionSetters(DESCRIBE_TRANSACTIONS_FORMS, args, DescribeTransactionsOptions));
  const futures = new Map<string, KafkaFuture<TransactionDescription>>();
  for (const id of ids) futures.set(id, adminFuture(handle, box, 'description', id, transactionDescriptionOf));
  return DescribeTransactionsResult.internalCreate(futures);
}

/**
 * `abortTransaction(spec)` and `abortTransaction(spec, options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function abortTransaction(handle: AdminHandle, args: unknown, count: number): AbortTransactionResult {
  ABORT_TRANSACTION_FORMS.match(args, count);
  // Java's first dereferences are the spec's (topicPartition()), its
  // partition's (a key of an unmodifiable map), then the options'; it reads
  // the partition's topic when it builds the request.
  const spec = ABORT_TRANSACTION_FORMS.value(args, 'spec');
  if (spec === null) throw new TypeError('spec must not be null');
  if (!(spec instanceof AbortTransactionSpec)) throw new TypeError(`spec must be an AbortTransactionSpec, not ${kind(spec)}`);
  const tp = spec.topicPartition();
  if (tp === null) throw new TypeError('spec must not be an AbortTransactionSpec with a null topicPartition');
  optionsFirst(ABORT_TRANSACTION_FORMS, args);
  if (tp.topic === null) throw new TypeError('spec must not be an AbortTransactionSpec with a TopicPartition with a null topic');
  const pair: [string, number] = [tp.topic, tp.partition];
  const box = native.adminAbortTransaction(
    handle,
    [pair, spec.producerId(), spec.producerEpoch(), spec.coordinatorEpoch()],
    optionSetters(ABORT_TRANSACTION_FORMS, args, AbortTransactionOptions),
  );
  // Java's one entry, for the spec's partition, whose future is the C
  // result's all(): it has no per-partition accessor, and all() completes as
  // that one entry does.
  const futures = new TopicPartitionMap<KafkaFuture<void>>();
  futures.set(tp, adminFuture(handle, box, 'all', null, voidOf));
  return AbortTransactionResult.internalCreate(futures);
}

/**
 * `forceTerminateTransaction(transactionalId)` and
 * `forceTerminateTransaction(transactionalId, options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function forceTerminateTransaction(handle: AdminHandle, args: unknown, count: number): TerminateTransactionResult {
  FORCE_TERMINATE_TRANSACTION_FORMS.match(args, count);
  // Java's first dereference is the options' (timeoutMs()); it stores a null
  // id, which fails the future later.
  optionsFirst(FORCE_TERMINATE_TRANSACTION_FORMS, args);
  const id = FORCE_TERMINATE_TRANSACTION_FORMS.value(args, 'transactionalId');
  if (id === null) throw new TypeError('transactionalId must not be null');
  if (typeof id !== 'string') throw new TypeError(`transactionalId must be a string, not ${kind(id)}`);
  const box = native.adminForceTerminateTransaction(
    handle,
    id,
    optionSetters(FORCE_TERMINATE_TRANSACTION_FORMS, args, TerminateTransactionOptions),
  );
  return TerminateTransactionResult.internalCreate(adminFuture(handle, box, 'result', null, voidOf));
}

/**
 * `listTransactions()` and `listTransactions(options)`.
 *
 * @param handle - the client's handle
 * @param options - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function listTransactions(handle: AdminHandle, options: unknown, count: number): ListTransactionsResult {
  LIST_TRANSACTIONS_FORMS.match(options, count);
  optionsFirst(LIST_TRANSACTIONS_FORMS, options);
  const given = LIST_TRANSACTIONS_FORMS.value(options, 'options');
  // Java's request builder reads the filters' members.
  if (given instanceof ListTransactionsOptions) {
    if ([...given.filteredStates()].includes(null as unknown as TransactionState)) {
      throw new TypeError('ListTransactionsOptions.filteredStates must not contain null');
    }
    if ([...given.filteredProducerIds()].includes(null as unknown as bigint)) {
      throw new TypeError('ListTransactionsOptions.filteredProducerIds must not contain null');
    }
  }
  const box = native.adminListTransactions(handle, optionSetters(LIST_TRANSACTIONS_FORMS, options, ListTransactionsOptions));
  return ListTransactionsResult.internalCreate(
    adminFuture(
      handle,
      box,
      'by_broker_id',
      null,
      (raw) =>
        new Map(
          (raw as Array<[number, number]>).map(([brokerId, slot]): [number, KafkaFuture<TransactionListing[]>] => [
            brokerId,
            adminFutureOfSlot(handle, slot, transactionListingsOf),
          ]),
        ),
    ),
  );
}

/**
 * `fenceProducers(transactionalIds)` and `fenceProducers(transactionalIds, options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function fenceProducers(handle: AdminHandle, args: unknown, count: number): FenceProducersResult {
  FENCE_PRODUCERS_FORMS.match(args, count);
  const ids = transactionalIds(FENCE_PRODUCERS_FORMS, args);
  const box = native.adminFenceProducers(handle, ids, optionSetters(FENCE_PRODUCERS_FORMS, args, FenceProducersOptions));
  const futures = new Map<string, KafkaFuture<{ readonly producerId: bigint; readonly epoch: number }>>();
  for (const id of ids) {
    // The C result hands out the pair only as its two refinements, which
    // complete together with that id's fencing; the pair fails with the first
    // one's error, as Python's composition does.
    const producerId = adminFuture(handle, box, 'producer_id', id, (raw) => raw as bigint);
    const epoch = adminFuture(handle, box, 'epoch_id', id, (raw) => raw as number);
    futures.set(
      id,
      KafkaFuture.allOf([producerId, epoch]).thenApply(() => ({
        producerId: producerId.getNow({ valueIfAbsent: null }) as bigint,
        epoch: epoch.getNow({ valueIfAbsent: null }) as number,
      })),
    );
  }
  return FenceProducersResult.internalCreate(futures);
}
