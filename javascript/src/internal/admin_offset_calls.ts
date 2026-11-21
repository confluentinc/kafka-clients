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

// The admin client's leader, reassignment and offset RPCs (private):
// electLeaders, alterPartitionReassignments, listPartitionReassignments and
// listOffsets, as src/internal/admin_calls.ts has the others. Each checks the
// call's form and its arguments, runs the RPC and builds its Result from the
// native result's futures, as Python's confluent_kafka/admin/_conversions.py
// composes them.

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way
import native = require('./native');
import { JavaForms, kind } from './args';
import { adminFuture, adminFutures } from './admin_result';
import type { AdminHandle } from './admin_result';
import { instancesOf, optionSetters, rpcForms, voidOf, optionsFirst } from './admin_calls';
import { checkLong } from './java_number';
import { ElectionType } from '../common/election_type';
import { IsolationLevel } from '../common/isolation_level';
import { TopicPartition } from '../common/topic_partition';
import { TopicPartitionMap } from '../common/topic_partition_map';
import type { KafkaFuture } from '../common/kafka_future';
import { AbstractOptions } from '../admin/abstract_options';
import { AlterPartitionReassignmentsOptions } from '../admin/alter_partition_reassignments_options';
import { AlterPartitionReassignmentsResult } from '../admin/alter_partition_reassignments_result';
import { ElectLeadersOptions } from '../admin/elect_leaders_options';
import { ElectLeadersResult } from '../admin/elect_leaders_result';
import { ListOffsetsOptions } from '../admin/list_offsets_options';
import { ListOffsetsResult } from '../admin/list_offsets_result';
import { ListPartitionReassignmentsOptions } from '../admin/list_partition_reassignments_options';
import { ListPartitionReassignmentsResult } from '../admin/list_partition_reassignments_result';
import { NewPartitionReassignment } from '../admin/new_partition_reassignment';
import { OffsetSpec } from '../admin/offset_spec';
import { PartitionReassignment } from '../admin/partition_reassignment';

const ELECT_LEADERS_FORMS = rpcForms('electLeaders', ['electionType', 'partitions'], ElectLeadersOptions);
const ALTER_PARTITION_REASSIGNMENTS_FORMS = rpcForms('alterPartitionReassignments', ['reassignments'], AlterPartitionReassignmentsOptions);
// Java's (), (partitions), (partitions, options), (options), and the abstract
// (Optional<Set> partitions, options), whose empty Optional is a null
// `partitions`.
const LIST_PARTITION_REASSIGNMENTS_FORMS = new JavaForms(
  'listPartitionReassignments',
  [[], ['partitions'], ['partitions', 'options'], ['options']],
  {
    bareTypes: {
      partitions: (value) => value !== null && typeof value === 'object' && !(value instanceof AbstractOptions),
      options: (value) => value === null || value instanceof ListPartitionReassignmentsOptions,
    },
  },
);
const LIST_OFFSETS_FORMS = rpcForms('listOffsets', ['topicPartitionOffsets'], ListOffsetsOptions);

/**
 * A partition the addon read as `[topic, partition]`.
 *
 * @param raw - the pair
 * @returns the partition
 */
export function topicPartitionOf(raw: unknown): TopicPartition {
  const [topic, partition] = raw as [string, number];
  return new TopicPartition({ topic, partition });
}

/**
 * The partitions of a Java `Set<TopicPartition>` (any iterable), for the
 * addon, each once.
 *
 * @param value - the partitions given
 * @param name - the RPC's parameter
 * @returns the `[topic, partition]` pairs
 * @throws `TypeError` for a string, a null element, or another value
 */
export function partitionPairs(value: unknown, name: string): Array<[string, number]> {
  const seen = new TopicPartitionMap<true>();
  const out: Array<[string, number]> = [];
  for (const tp of instancesOf(value, name, TopicPartition)) {
    if (seen.has(tp)) continue;
    seen.set(tp, true);
    // The C API takes a topic as a string: Java's request builder dereferences it.
    if (tp.topic === null) throw new TypeError(`${name} must not contain a TopicPartition with a null topic`);
    out.push([tp.topic, tp.partition]);
  }
  return out;
}

/**
 * A partition's key, for the addon.
 *
 * @param tp - the partition
 * @param name - the RPC's parameter
 * @returns the `[topic, partition]` pair
 * @throws `TypeError` for a null partition or topic
 */
function partitionKey(tp: TopicPartition | null, name: string): [string, number] {
  if (tp === null) throw new TypeError(`${name} must not contain a null key`);
  if (tp.topic === null) throw new TypeError(`${name} must not contain a TopicPartition with a null topic`);
  return [tp.topic, tp.partition];
}

/**
 * The entries of a `TopicPartitionMap` an RPC takes.
 *
 * @param value - the map given
 * @param name - the RPC's parameter
 * @returns its entries
 * @throws `TypeError` for null or another value
 */
export function topicPartitionMapEntries(value: unknown, name: string): Array<[TopicPartition | null, unknown]> {
  if (value === null) throw new TypeError(`${name} must not be null`);
  if (!(value instanceof TopicPartitionMap)) {
    throw new TypeError(`${name} must be a TopicPartitionMap, not ${kind(value)}: build one with new TopicPartitionMap()`);
  }
  return [...(value as TopicPartitionMap<unknown>)];
}

/**
 * A spec in the C enum's order (native/admin_values.c, offset_spec_of): the
 * subclass, as Java's `KafkaAdminClient.getOffsetFromSpec` reads it, any
 * other spec, null included (its `instanceof` chain's fall-through), as the
 * latest offset.
 *
 * @param spec - the spec
 * @returns the spec's number, and its timestamp
 */
export function offsetSpecFields(spec: OffsetSpec | null): [number, bigint] {
  if (spec instanceof OffsetSpec.TimestampSpec) return [6, spec.internalTimestamp()];
  if (spec instanceof OffsetSpec.EarliestSpec) return [0, 0n];
  if (spec instanceof OffsetSpec.MaxTimestampSpec) return [2, 0n];
  if (spec instanceof OffsetSpec.EarliestLocalSpec) return [3, 0n];
  if (spec instanceof OffsetSpec.LatestTieredSpec) return [4, 0n];
  if (spec instanceof OffsetSpec.EarliestPendingUploadSpec) return [5, 0n];
  return [1, 0n];
}

/**
 * `electLeaders(electionType, partitions)` and `electLeaders(electionType,
 * partitions, options)`, null partitions for every partition.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function electLeaders(handle: AdminHandle, args: unknown, count: number): ElectLeadersResult {
  ELECT_LEADERS_FORMS.match(args, count);
  // Java's body reads the options before the arguments it only stores.
  optionsFirst(ELECT_LEADERS_FORMS, args);
  const electionType = ELECT_LEADERS_FORMS.value(args, 'electionType');
  if (electionType === null) throw new TypeError('electionType must not be null');
  if (!(electionType instanceof ElectionType)) throw new TypeError(`electionType must be an ElectionType, not ${kind(electionType)}`);
  const given = ELECT_LEADERS_FORMS.value(args, 'partitions');
  const partitions = given === null ? null : partitionPairs(given, 'partitions');
  const box = native.adminElectLeaders(
    handle,
    electionType.ordinal(),
    partitions,
    optionSetters(ELECT_LEADERS_FORMS, args, ElectLeadersOptions),
  );
  return ElectLeadersResult.internalCreate(
    adminFuture(handle, box, 'partitions', null, (raw) => {
      const out = new TopicPartitionMap<unknown>();
      for (const [key, error] of raw as Array<[unknown, Error | null]>) out.set(topicPartitionOf(key), error);
      return out;
    }),
  );
}

/**
 * `alterPartitionReassignments(reassignments)` and its form with the options,
 * the map a TopicPartitionMap whose null value cancels.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function alterPartitionReassignments(handle: AdminHandle, args: unknown, count: number): AlterPartitionReassignmentsResult {
  ALTER_PARTITION_REASSIGNMENTS_FORMS.match(args, count);
  const entries: Array<[[string, number], number[] | null]> = [];
  for (const [tp, reassignment] of topicPartitionMapEntries(
    ALTER_PARTITION_REASSIGNMENTS_FORMS.value(args, 'reassignments'),
    'reassignments',
  )) {
    const key = partitionKey(tp, 'reassignments');
    if (reassignment === null) {
      entries.push([key, null]);
      continue;
    }
    if (!(reassignment instanceof NewPartitionReassignment)) {
      throw new TypeError(`reassignments must map to NewPartitionReassignment or null, not ${kind(reassignment)}`);
    }
    entries.push([key, reassignment.targetReplicas()]);
  }
  const box = native.adminAlterPartitionReassignments(
    handle,
    entries,
    optionSetters(ALTER_PARTITION_REASSIGNMENTS_FORMS, args, AlterPartitionReassignmentsOptions),
  );
  const futures = adminFutures(handle, box, 'values', topicPartitionOf, voidOf) ?? [];
  return AlterPartitionReassignmentsResult.internalCreate(new TopicPartitionMap<KafkaFuture<void>>(futures));
}

/**
 * `listPartitionReassignments()` and its forms with the partitions and the
 * options; null partitions given with the options (Java's empty `Optional`)
 * list every partition.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function listPartitionReassignments(handle: AdminHandle, args: unknown, count: number): ListPartitionReassignmentsResult {
  const form = LIST_PARTITION_REASSIGNMENTS_FORMS.match(args, count);
  let partitions: Array<[string, number]> | null = null;
  if (form === 1 || form === 2) {
    const given = LIST_PARTITION_REASSIGNMENTS_FORMS.value(args, 'partitions');
    // Java's (Set) form calls Optional.of(partitions); only the (partitions,
    // options) form, Java's (Optional<Set>, options), reads null as empty.
    if (given === null && form === 1) throw new TypeError('partitions must not be null');
    if (given !== null) partitions = partitionPairs(given, 'partitions');
  }
  const box = native.adminListPartitionReassignments(
    handle,
    partitions,
    optionSetters(LIST_PARTITION_REASSIGNMENTS_FORMS, args, ListPartitionReassignmentsOptions),
  );
  return ListPartitionReassignmentsResult.internalCreate(
    adminFuture(handle, box, 'reassignments', null, (raw) => {
      const out = new TopicPartitionMap<PartitionReassignment>();
      for (const [key, f] of raw as Array<[unknown, { replicas: number[]; addingReplicas: number[]; removingReplicas: number[] }]>) {
        out.set(topicPartitionOf(key), new PartitionReassignment(f));
      }
      return out;
    }),
  );
}

/** A partition's listed offset as the addon reads it. */
interface ListOffsetsFields {
  offset: bigint;
  timestamp: bigint;
  leaderEpoch: number | null;
}

/**
 * `listOffsets(topicPartitionOffsets)` and its form with the options, the map
 * a TopicPartitionMap of OffsetSpec.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function listOffsets(handle: AdminHandle, args: unknown, count: number): ListOffsetsResult {
  LIST_OFFSETS_FORMS.match(args, count);
  const partitions: TopicPartition[] = [];
  const rows: Array<[[string, number], number, bigint]> = [];
  const given = topicPartitionMapEntries(LIST_OFFSETS_FORMS.value(args, 'topicPartitionOffsets'), 'topicPartitionOffsets');
  // Java's newFuture copies the keys into an unmodifiable map, which refuses
  // a null key; it reads options.timeoutMs() next, and the topics and specs
  // only when it builds the requests.
  for (const [tp] of given) if (tp === null) throw new TypeError('topicPartitionOffsets must not contain a null key');
  optionsFirst(LIST_OFFSETS_FORMS, args);
  for (const [tp, spec] of given) {
    const key = partitionKey(tp, 'topicPartitionOffsets');
    // A null spec is Java's getOffsetFromSpec fall-through: the latest offset.
    if (spec !== null && !(spec instanceof OffsetSpec))
      throw new TypeError(`topicPartitionOffsets must map to OffsetSpec, not ${kind(spec)}`);
    const [specNumber, timestamp] = offsetSpecFields(spec);
    partitions.push(tp as TopicPartition);
    rows.push([key, specNumber, checkLong(timestamp, 'timestamp')]);
  }
  const options = LIST_OFFSETS_FORMS.value(args, 'options');
  let readCommitted = false;
  if (options instanceof ListOffsetsOptions) {
    const level = options.isolationLevel();
    // Java's request builder dereferences it; the FFI cannot carry it.
    if (level === null) throw new TypeError('ListOffsetsOptions.isolationLevel must not be null');
    readCommitted = level === IsolationLevel.READ_COMMITTED;
  }
  const box = native.adminListOffsets(handle, rows, readCommitted, optionSetters(LIST_OFFSETS_FORMS, args, ListOffsetsOptions));
  const futures = new TopicPartitionMap<KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>>();
  for (const [i, tp] of partitions.entries()) {
    futures.set(
      tp,
      adminFuture(
        handle,
        box,
        'partition_result',
        rows[i][0],
        (raw) => new ListOffsetsResult.ListOffsetsResultInfo(raw as ListOffsetsFields),
      ),
    );
  }
  return new ListOffsetsResult(futures);
}
