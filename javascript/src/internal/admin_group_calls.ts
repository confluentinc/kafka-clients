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
// The admin client's group RPCs (private): listGroups, describeConsumerGroups,
// describeClassicGroups, listConsumerGroupOffsets, alterConsumerGroupOffsets,
// deleteConsumerGroupOffsets, deleteConsumerGroups and
// removeMembersFromConsumerGroup, as src/internal/admin_calls.ts has the
// others. Each checks the call's form and its arguments, runs the RPC and
// builds its Result from the native result's futures, as Python's
// confluent_kafka/admin/_conversions.py composes them. A group id crosses as
// a C string, so a null one, which Java reads only later, is reported by the
// call, a deviation the admin package's documentation marks.

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way
import native = require('./native');
import { JavaForms, kind } from './args';
import { adminFuture, adminFutures } from './admin_result';
import type { AdminHandle } from './admin_result';
import { aclOperationsOf, nodeOf, optionSetters, rpcForms, stringKeyedEntries, voidOf, optionsFirst } from './admin_calls';
import { partitionPairs, topicPartitionMapEntries, topicPartitionOf } from './admin_offset_calls';
import { ClassicGroupState } from '../common/classic_group_state';
import { GroupState } from '../common/group_state';
import { GroupType } from '../common/group_type';
import { KafkaFuture } from '../common/kafka_future';
import type { TopicPartition } from '../common/topic_partition';
import { TopicPartitionMap } from '../common/topic_partition_map';
import { TopicPartitionSet } from '../common/topic_partition_set';
import { OffsetAndMetadata } from '../consumer/offset_and_metadata';
import { AlterConsumerGroupOffsetsOptions } from '../admin/alter_consumer_group_offsets_options';
import { AlterConsumerGroupOffsetsResult } from '../admin/alter_consumer_group_offsets_result';
import { ClassicGroupDescription } from '../admin/classic_group_description';
import { ConsumerGroupDescription } from '../admin/consumer_group_description';
import { DeleteConsumerGroupOffsetsOptions } from '../admin/delete_consumer_group_offsets_options';
import { DeleteConsumerGroupOffsetsResult } from '../admin/delete_consumer_group_offsets_result';
import { DeleteConsumerGroupsOptions } from '../admin/delete_consumer_groups_options';
import { DeleteConsumerGroupsResult } from '../admin/delete_consumer_groups_result';
import { DescribeClassicGroupsOptions } from '../admin/describe_classic_groups_options';
import { DescribeClassicGroupsResult } from '../admin/describe_classic_groups_result';
import { DescribeConsumerGroupsOptions } from '../admin/describe_consumer_groups_options';
import { DescribeConsumerGroupsResult } from '../admin/describe_consumer_groups_result';
import { GroupListing } from '../admin/group_listing';
import { ListConsumerGroupOffsetsOptions } from '../admin/list_consumer_group_offsets_options';
import { ListConsumerGroupOffsetsResult } from '../admin/list_consumer_group_offsets_result';
import { ListConsumerGroupOffsetsSpec } from '../admin/list_consumer_group_offsets_spec';
import { ListGroupsOptions } from '../admin/list_groups_options';
import { ListGroupsResult } from '../admin/list_groups_result';
import { MemberAssignment } from '../admin/member_assignment';
import { MemberDescription } from '../admin/member_description';
import type { MemberToRemove } from '../admin/member_to_remove';
import { MemberToRemoveSet } from '../admin/member_to_remove_set';
import { RemoveMembersFromConsumerGroupOptions } from '../admin/remove_members_from_consumer_group_options';
import { RemoveMembersFromConsumerGroupResult } from '../admin/remove_members_from_consumer_group_result';

const LIST_GROUPS_FORMS = rpcForms('listGroups', [], ListGroupsOptions);
const DESCRIBE_CONSUMER_GROUPS_FORMS = rpcForms('describeConsumerGroups', ['groupIds'], DescribeConsumerGroupsOptions);
const DESCRIBE_CLASSIC_GROUPS_FORMS = rpcForms('describeClassicGroups', ['groupIds'], DescribeClassicGroupsOptions);
const DELETE_CONSUMER_GROUPS_FORMS = rpcForms('deleteConsumerGroups', ['groupIds'], DeleteConsumerGroupsOptions);
// Java's (groupId), (groupId, options), (groupSpecs) and (groupSpecs,
// options): a bare string is a group id, a bare Map the specs; a plain object
// of specs goes by name, as a plain object given alone is the options object.
const LIST_CONSUMER_GROUP_OFFSETS_FORMS = new JavaForms(
  'listConsumerGroupOffsets',
  [['groupId'], ['groupId', 'options'], ['groupSpecs'], ['groupSpecs', 'options']],
  {
    order: ['groupId', 'groupSpecs', 'options'],
    bareTypes: {
      groupId: (value) => value === null || typeof value === 'string',
      groupSpecs: (value) => value instanceof Map,
    },
  },
);
const ALTER_CONSUMER_GROUP_OFFSETS_FORMS = rpcForms('alterConsumerGroupOffsets', ['groupId', 'offsets'], AlterConsumerGroupOffsetsOptions);
const DELETE_CONSUMER_GROUP_OFFSETS_FORMS = rpcForms(
  'deleteConsumerGroupOffsets',
  ['groupId', 'partitions'],
  DeleteConsumerGroupOffsetsOptions,
);
// Java has only the (groupId, options) form.
const REMOVE_MEMBERS_FROM_CONSUMER_GROUP_FORMS = new JavaForms('removeMembersFromConsumerGroup', [['groupId', 'options']]);

/** A GroupListing's fields as the addon reads them, the type and state their ordinals. */
interface GroupListingFields {
  groupId: string | null;
  type: number | null;
  protocol: string | null;
  groupState: number | null;
}

/**
 * A group listing the addon read.
 *
 * @param raw - its fields
 * @returns the listing
 */
function groupListingOf(raw: unknown): GroupListing {
  const f = raw as GroupListingFields;
  return new GroupListing({
    groupId: f.groupId,
    type: f.type === null ? null : GroupType.values()[f.type],
    protocol: f.protocol,
    groupState: f.groupState === null ? null : GroupState.values()[f.groupState],
  });
}

/**
 * A member's assignment the addon read as its `[topic, partition]` pairs.
 *
 * @param raw - the pairs, or null
 * @returns the assignment, or null
 */
function memberAssignmentOf(raw: unknown): MemberAssignment | null {
  return raw === null ? null : new MemberAssignment((raw as unknown[]).map(topicPartitionOf));
}

/** A MemberDescription's fields as the addon reads them. */
interface MemberDescriptionFields {
  memberId: string | null;
  groupInstanceId: string | null;
  rackId: string | null;
  clientId: string | null;
  host: string | null;
  assignment: unknown;
  targetAssignment: unknown;
  memberEpoch: number | null;
  upgraded: boolean | null;
}

/**
 * A member's description the addon read.
 *
 * @param raw - its fields
 * @returns the description
 */
function memberDescriptionOf(raw: unknown): MemberDescription {
  const f = raw as MemberDescriptionFields;
  return new MemberDescription({
    memberId: f.memberId,
    groupInstanceId: f.groupInstanceId,
    rackId: f.rackId,
    clientId: f.clientId,
    host: f.host,
    assignment: memberAssignmentOf(f.assignment),
    targetAssignment: memberAssignmentOf(f.targetAssignment),
    memberEpoch: f.memberEpoch,
    upgraded: f.upgraded,
  });
}

/**
 * A consumer group's description the addon read.
 *
 * @param raw - its fields
 * @returns the description
 */
function consumerGroupDescriptionOf(raw: unknown): ConsumerGroupDescription {
  const f = raw as {
    groupId: string | null;
    isSimpleConsumerGroup: boolean;
    members: unknown[];
    partitionAssignor: string | null;
    type: number | null;
    groupState: number | null;
    coordinator: unknown;
    authorizedOperations: unknown;
    groupEpoch: number | null;
    targetAssignmentEpoch: number | null;
  };
  return new ConsumerGroupDescription({
    groupId: f.groupId,
    isSimpleConsumerGroup: f.isSimpleConsumerGroup,
    members: f.members.map(memberDescriptionOf),
    partitionAssignor: f.partitionAssignor,
    type: f.type === null ? null : GroupType.values()[f.type],
    groupState: f.groupState === null ? null : GroupState.values()[f.groupState],
    coordinator: nodeOf(f.coordinator),
    authorizedOperations: aclOperationsOf(f.authorizedOperations),
    groupEpoch: f.groupEpoch,
    targetAssignmentEpoch: f.targetAssignmentEpoch,
  });
}

/**
 * A classic group's description the addon read.
 *
 * @param raw - its fields
 * @returns the description
 */
function classicGroupDescriptionOf(raw: unknown): ClassicGroupDescription {
  const f = raw as {
    groupId: string | null;
    protocol: string | null;
    protocolData: string | null;
    members: unknown[];
    state: number | null;
    coordinator: unknown;
    authorizedOperations: unknown;
  };
  return new ClassicGroupDescription({
    groupId: f.groupId,
    protocol: f.protocol,
    protocolData: f.protocolData,
    members: f.members.map(memberDescriptionOf),
    state: f.state === null ? null : ClassicGroupState.values()[f.state],
    coordinator: nodeOf(f.coordinator),
    authorizedOperations: aclOperationsOf(f.authorizedOperations),
  });
}

/**
 * A group's committed offsets the addon read, null for a partition with no
 * committed offset.
 *
 * @param raw - the `[[topic, partition], fields or null]` pairs
 * @returns the offsets, by partition
 */
function groupOffsetsOf(raw: unknown): TopicPartitionMap<OffsetAndMetadata | null> {
  const out = new TopicPartitionMap<OffsetAndMetadata | null>();
  for (const [key, f] of raw as Array<[unknown, { offset: bigint; metadata: string | null; leaderEpoch: number | null } | null]>) {
    out.set(
      topicPartitionOf(key),
      f === null ? null : new OffsetAndMetadata({ offset: f.offset, leaderEpoch: f.leaderEpoch, metadata: f.metadata ?? '' }),
    );
  }
  return out;
}

/**
 * The group ids of an RPC's `Collection<String>` parameter, copied.
 *
 * @param value - the collection given
 * @param name - the RPC's parameter
 * @returns the ids
 * @throws `TypeError` for null, a string, a null id, or another value than
 *   an iterable of strings
 */
function groupIdsOf(value: unknown, name: string): string[] {
  if (value === null) throw new TypeError(`${name} must not be null`);
  if (typeof value === 'string') throw new TypeError(`${name} must be an iterable of strings, not string`);
  if (typeof value !== 'object' || typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function') {
    throw new TypeError(`${name} must be an iterable of strings, not ${kind(value)}`);
  }
  const out: string[] = [];
  for (const v of value as Iterable<unknown>) {
    if (v === null) throw new TypeError(`${name} must not contain null`);
    if (typeof v !== 'string') throw new TypeError(`${name} must be an iterable of strings, not one holding ${kind(v)}`);
    out.push(v);
  }
  return out;
}

/**
 * A group id the C API takes as a string.
 *
 * @param value - the id given
 * @param name - the RPC's parameter
 * @returns the id
 * @throws `TypeError` for null or another value than a string
 */
function groupIdOf(value: unknown, name: string): string {
  if (value === null) throw new TypeError(`${name} must not be null`);
  if (typeof value !== 'string') throw new TypeError(`${name} must be a string, not ${kind(value)}`);
  return value;
}

/**
 * One future of each partition's outcome, as Python's `outcomes`: a
 * partition whose future succeeded is null, one that failed its error,
 * complete once every partition's future is. It is Java's `Map<TopicPartition,
 * Errors>` future, read from the core's per-partition futures, each of which
 * already completes as Java's result completes that partition.
 *
 * @param futures - the futures, by partition
 * @returns the future of the outcomes
 */
function outcomes(futures: TopicPartitionMap<KafkaFuture<void>>): KafkaFuture<TopicPartitionMap<Error | null>> {
  const result = KafkaFuture.internalCreate<TopicPartitionMap<Error | null>>(false);
  const errors = new TopicPartitionMap<Error | null>();
  let remaining = futures.size();
  for (const [tp, future] of futures) {
    future.whenComplete((_value, error) => {
      errors.set(tp, error === null || error === undefined ? null : (error as Error));
      remaining -= 1;
      if (remaining === 0) {
        const ordered = new TopicPartitionMap<Error | null>();
        for (const key of futures.keys()) ordered.set(key, errors.get(key));
        result.internalComplete(ordered, null);
      }
    });
  }
  return result;
}

/**
 * `listGroups()` and `listGroups(options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function listGroups(handle: AdminHandle, args: unknown, count: number): ListGroupsResult {
  LIST_GROUPS_FORMS.match(args, count);
  const box = native.adminListGroups(handle, optionSetters(LIST_GROUPS_FORMS, args, ListGroupsOptions));
  const listings = (raw: unknown): GroupListing[] => (raw as unknown[]).map(groupListingOf);
  return ListGroupsResult.internalOfClient(
    adminFuture(handle, box, 'all', null, listings),
    adminFuture(handle, box, 'valid', null, listings),
    adminFuture(handle, box, 'errors', null, (raw) => raw as unknown[]),
  );
}

/**
 * `describeConsumerGroups(groupIds)` and its form with the options.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function describeConsumerGroups(handle: AdminHandle, args: unknown, count: number): DescribeConsumerGroupsResult {
  DESCRIBE_CONSUMER_GROUPS_FORMS.match(args, count);
  const givenIds = DESCRIBE_CONSUMER_GROUPS_FORMS.value(args, 'groupIds');
  // Java's newFuture iterates the ids, storing a null one, then reads the
  // options: a null collection first, then the options, then the ids.
  if (givenIds === null) throw new TypeError('groupIds must not be null');
  optionsFirst(DESCRIBE_CONSUMER_GROUPS_FORMS, args);
  const groupIds = groupIdsOf(givenIds, 'groupIds');
  const box = native.adminDescribeConsumerGroups(
    handle,
    groupIds,
    optionSetters(DESCRIBE_CONSUMER_GROUPS_FORMS, args, DescribeConsumerGroupsOptions),
  );
  const futures = adminFutures(handle, box, 'described_groups', (raw) => raw as string, consumerGroupDescriptionOf) ?? [];
  return new DescribeConsumerGroupsResult(new Map(futures));
}

/**
 * `describeClassicGroups(groupIds)` and its form with the options.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function describeClassicGroups(handle: AdminHandle, args: unknown, count: number): DescribeClassicGroupsResult {
  DESCRIBE_CLASSIC_GROUPS_FORMS.match(args, count);
  const givenIds = DESCRIBE_CLASSIC_GROUPS_FORMS.value(args, 'groupIds');
  // Java's newFuture iterates the ids, storing a null one, then reads the
  // options: a null collection first, then the options, then the ids.
  if (givenIds === null) throw new TypeError('groupIds must not be null');
  optionsFirst(DESCRIBE_CLASSIC_GROUPS_FORMS, args);
  const groupIds = groupIdsOf(givenIds, 'groupIds');
  const box = native.adminDescribeClassicGroups(
    handle,
    groupIds,
    optionSetters(DESCRIBE_CLASSIC_GROUPS_FORMS, args, DescribeClassicGroupsOptions),
  );
  const futures = adminFutures(handle, box, 'described_groups', (raw) => raw as string, classicGroupDescriptionOf) ?? [];
  return new DescribeClassicGroupsResult(new Map(futures));
}

/**
 * `listConsumerGroupOffsets(groupId)`, `listConsumerGroupOffsets(groupSpecs)`
 * and their forms with the options, the specs a `Map` or a plain object of
 * ListConsumerGroupOffsetsSpec by group id.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function listConsumerGroupOffsets(handle: AdminHandle, args: unknown, count: number): ListConsumerGroupOffsetsResult {
  const form = LIST_CONSUMER_GROUP_OFFSETS_FORMS.match(args, count);
  // Java's (Map, options) body reads groupSpecs.keySet() first, then
  // options.requireStable(), and the specs only when it builds the request;
  // the groupId forms wrap the id in a map, so the options come first there.
  if (form >= 2 && LIST_CONSUMER_GROUP_OFFSETS_FORMS.value(args, 'groupSpecs') === null) {
    throw new TypeError('groupSpecs must not be null');
  }
  optionsFirst(LIST_CONSUMER_GROUP_OFFSETS_FORMS, args);
  let groupIds: string[];
  let groupId: string | null = null;
  let specs: Array<[string, Array<[string, number]> | null]> | null = null;
  if (form < 2) {
    groupId = groupIdOf(LIST_CONSUMER_GROUP_OFFSETS_FORMS.value(args, 'groupId'), 'groupId');
    groupIds = [groupId];
  } else {
    specs = stringKeyedEntries(LIST_CONSUMER_GROUP_OFFSETS_FORMS.value(args, 'groupSpecs'), 'groupSpecs').map(([id, spec]) => {
      if (spec === null) throw new TypeError('groupSpecs must not contain null');
      if (!(spec instanceof ListConsumerGroupOffsetsSpec)) {
        throw new TypeError(`groupSpecs must map to ListConsumerGroupOffsetsSpec, not ${kind(spec)}`);
      }
      const partitions = spec.topicPartitions();
      return [id, partitions === null ? null : partitionPairs(partitions, 'groupSpecs')];
    });
    groupIds = specs.map(([id]) => id);
  }
  const box = native.adminListConsumerGroupOffsets(
    handle,
    groupId,
    specs,
    optionSetters(LIST_CONSUMER_GROUP_OFFSETS_FORMS, args, ListConsumerGroupOffsetsOptions),
  );
  const futures = new Map<string, KafkaFuture<TopicPartitionMap<OffsetAndMetadata | null>>>();
  for (const id of groupIds) {
    futures.set(id, adminFuture(handle, box, 'partitions_to_offset_and_metadata_with_group_id', id, groupOffsetsOf));
  }
  return ListConsumerGroupOffsetsResult.internalCreate(futures);
}

/**
 * `alterConsumerGroupOffsets(groupId, offsets)` and its form with the
 * options, the offsets a TopicPartitionMap of OffsetAndMetadata.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function alterConsumerGroupOffsets(handle: AdminHandle, args: unknown, count: number): AlterConsumerGroupOffsetsResult {
  ALTER_CONSUMER_GROUP_OFFSETS_FORMS.match(args, count);
  // Java's body reads the options before the arguments it only stores.
  optionsFirst(ALTER_CONSUMER_GROUP_OFFSETS_FORMS, args);
  const groupId = groupIdOf(ALTER_CONSUMER_GROUP_OFFSETS_FORMS.value(args, 'groupId'), 'groupId');
  const rows: Array<[[string, number], bigint, number, string]> = [];
  for (const [tp, value] of topicPartitionMapEntries(ALTER_CONSUMER_GROUP_OFFSETS_FORMS.value(args, 'offsets'), 'offsets')) {
    if (tp === null) throw new TypeError('offsets must not contain a null key');
    if (tp.topic === null) throw new TypeError('offsets must not contain a TopicPartition with a null topic');
    if (value === null) throw new TypeError('offsets must not contain null');
    if (!(value instanceof OffsetAndMetadata)) throw new TypeError(`offsets must map to OffsetAndMetadata, not ${kind(value)}`);
    rows.push([[tp.topic, tp.partition], value.offset, value.leaderEpoch ?? -1, value.metadata]);
  }
  const box = native.adminAlterConsumerGroupOffsets(
    handle,
    groupId,
    rows,
    optionSetters(ALTER_CONSUMER_GROUP_OFFSETS_FORMS, args, AlterConsumerGroupOffsetsOptions),
  );
  return AlterConsumerGroupOffsetsResult.internalOfClient(
    (partition: TopicPartition) => adminFuture(handle, box, 'partition_result', [partition.topic, partition.partition], voidOf),
    () => adminFuture(handle, box, 'all', null, voidOf),
  );
}

/**
 * `deleteConsumerGroupOffsets(groupId, partitions)` and its form with the
 * options, the partitions Java's `Set<TopicPartition>`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function deleteConsumerGroupOffsets(handle: AdminHandle, args: unknown, count: number): DeleteConsumerGroupOffsetsResult {
  DELETE_CONSUMER_GROUP_OFFSETS_FORMS.match(args, count);
  // Java's body reads the options before the arguments it only stores.
  optionsFirst(DELETE_CONSUMER_GROUP_OFFSETS_FORMS, args);
  const groupId = groupIdOf(DELETE_CONSUMER_GROUP_OFFSETS_FORMS.value(args, 'groupId'), 'groupId');
  const partitions = partitionPairs(DELETE_CONSUMER_GROUP_OFFSETS_FORMS.value(args, 'partitions'), 'partitions');
  const box = native.adminDeleteConsumerGroupOffsets(
    handle,
    groupId,
    partitions,
    optionSetters(DELETE_CONSUMER_GROUP_OFFSETS_FORMS, args, DeleteConsumerGroupOffsetsOptions),
  );
  const requested = new TopicPartitionSet(partitions.map(topicPartitionOf));
  let future: KafkaFuture<TopicPartitionMap<Error | null>>;
  if (partitions.length === 0) {
    // No partition, no per-partition future: Java's future then fails as the
    // call fails, which is the core's all(), and is otherwise empty.
    future = adminFuture(handle, box, 'all', null, voidOf).thenApply(() => new TopicPartitionMap<Error | null>());
  } else {
    const futures = new TopicPartitionMap<KafkaFuture<void>>();
    for (const pair of partitions) futures.set(topicPartitionOf(pair), adminFuture(handle, box, 'partition_result', pair, voidOf));
    future = outcomes(futures);
  }
  return DeleteConsumerGroupOffsetsResult.internalCreate(future, requested);
}

/**
 * `deleteConsumerGroups(groupIds)` and its form with the options.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function deleteConsumerGroups(handle: AdminHandle, args: unknown, count: number): DeleteConsumerGroupsResult {
  DELETE_CONSUMER_GROUPS_FORMS.match(args, count);
  const givenIds = DELETE_CONSUMER_GROUPS_FORMS.value(args, 'groupIds');
  // Java's newFuture iterates the ids, storing a null one, then reads the
  // options: a null collection first, then the options, then the ids.
  if (givenIds === null) throw new TypeError('groupIds must not be null');
  optionsFirst(DELETE_CONSUMER_GROUPS_FORMS, args);
  const groupIds = groupIdsOf(givenIds, 'groupIds');
  const box = native.adminDeleteConsumerGroups(
    handle,
    groupIds,
    optionSetters(DELETE_CONSUMER_GROUPS_FORMS, args, DeleteConsumerGroupsOptions),
  );
  return DeleteConsumerGroupsResult.internalCreate(
    new Map(adminFutures(handle, box, 'deleted_groups', (raw) => raw as string, voidOf) ?? []),
  );
}

/**
 * `removeMembersFromConsumerGroup(groupId, options)`, Java's only form.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function removeMembersFromConsumerGroup(handle: AdminHandle, args: unknown, count: number): RemoveMembersFromConsumerGroupResult {
  REMOVE_MEMBERS_FROM_CONSUMER_GROUP_FORMS.match(args, count);
  // Java's body reads the options before the arguments it only stores.
  optionsFirst(REMOVE_MEMBERS_FROM_CONSUMER_GROUP_FORMS, args);
  const groupId = groupIdOf(REMOVE_MEMBERS_FROM_CONSUMER_GROUP_FORMS.value(args, 'groupId'), 'groupId');
  const options = REMOVE_MEMBERS_FROM_CONSUMER_GROUP_FORMS.value(args, 'options');
  let ids: string[] | null = null;
  let memberInfos = new MemberToRemoveSet();
  if (options instanceof RemoveMembersFromConsumerGroupOptions) {
    const members = options.internalMembers();
    if (members !== null) {
      ids = members.map((m) => {
        // Java's request builder reads each member's identity.
        if (m === null) throw new TypeError('RemoveMembersFromConsumerGroupOptions.members must not contain null');
        const id = m.groupInstanceId();
        if (id === null) {
          throw new TypeError(
            'RemoveMembersFromConsumerGroupOptions.members must not contain a MemberToRemove with a null groupInstanceId',
          );
        }
        return id;
      });
    }
    memberInfos = options.members();
  }
  const box = native.adminRemoveMembersFromConsumerGroup(
    handle,
    groupId,
    ids,
    optionSetters(REMOVE_MEMBERS_FROM_CONSUMER_GROUP_FORMS, args, RemoveMembersFromConsumerGroupOptions),
  );
  return RemoveMembersFromConsumerGroupResult.internalOfClient(
    (member: MemberToRemove) => {
      // The Result answers a member whose group instance id is null with
      // Java's checks before it asks the C API, which takes the id.
      return adminFuture(handle, box, 'member_result', member.groupInstanceId() as string, voidOf);
    },
    () => adminFuture(handle, box, 'all', null, voidOf),
    memberInfos,
  );
}
