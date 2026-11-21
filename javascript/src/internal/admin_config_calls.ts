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

// The admin client's config and log-dir RPCs (private): describeConfigs,
// incrementalAlterConfigs, listConfigResources, describeLogDirs,
// alterReplicaLogDirs and describeReplicaLogDirs, as src/internal/admin_calls.ts
// has the others. Each checks the call's form and its arguments, runs the RPC
// and builds its Result from the native result's futures, as Python's
// confluent_kafka/admin/_conversions.py composes them.

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way
import native = require('./native');
import { JavaForms, kind } from './args';
import { adminFuture, adminFutures } from './admin_result';
import type { AdminHandle } from './admin_result';
import { configOf, instancesOf, optionSetters, rpcForms, voidOf, optionsFirst } from './admin_calls';
import { checkInt } from './java_number';
import { ConfigResource } from '../common/config/config_resource';
import { ConfigResourceMap } from '../common/config/config_resource_map';
import { ApiError } from '../common/errors/api_error';
import { TopicPartition } from '../common/topic_partition';
import { TopicPartitionMap } from '../common/topic_partition_map';
import { TopicPartitionReplica } from '../common/topic_partition_replica';
import { TopicPartitionReplicaMap } from '../common/topic_partition_replica_map';
import type { KafkaFuture } from '../common/kafka_future';
import { AlterConfigOp } from '../admin/alter_config_op';
import { AlterConfigsOptions } from '../admin/alter_configs_options';
import { AlterConfigsResult } from '../admin/alter_configs_result';
import { AlterReplicaLogDirsOptions } from '../admin/alter_replica_log_dirs_options';
import { AlterReplicaLogDirsResult } from '../admin/alter_replica_log_dirs_result';
import type { Config } from '../admin/config';
import { DescribeConfigsOptions } from '../admin/describe_configs_options';
import { DescribeConfigsResult } from '../admin/describe_configs_result';
import { DescribeLogDirsOptions } from '../admin/describe_log_dirs_options';
import { DescribeLogDirsResult } from '../admin/describe_log_dirs_result';
import { DescribeReplicaLogDirsOptions } from '../admin/describe_replica_log_dirs_options';
import { DescribeReplicaLogDirsResult } from '../admin/describe_replica_log_dirs_result';
import { ListConfigResourcesOptions } from '../admin/list_config_resources_options';
import { ListConfigResourcesResult } from '../admin/list_config_resources_result';
import { LogDirDescription } from '../admin/log_dir_description';
import { ReplicaInfo } from '../admin/replica_info';

const DESCRIBE_CONFIGS_FORMS = rpcForms('describeConfigs', ['resources'], DescribeConfigsOptions);
const INCREMENTAL_ALTER_CONFIGS_FORMS = rpcForms('incrementalAlterConfigs', ['configs'], AlterConfigsOptions);
// Java's listConfigResources() and listConfigResources(configResourceTypes, options) only.
const LIST_CONFIG_RESOURCES_FORMS = new JavaForms('listConfigResources', [[], ['configResourceTypes', 'options']]);
const DESCRIBE_LOG_DIRS_FORMS = rpcForms('describeLogDirs', ['brokers'], DescribeLogDirsOptions);
const ALTER_REPLICA_LOG_DIRS_FORMS = rpcForms('alterReplicaLogDirs', ['replicaAssignment'], AlterReplicaLogDirsOptions);
const DESCRIBE_REPLICA_LOG_DIRS_FORMS = rpcForms('describeReplicaLogDirs', ['replicas'], DescribeReplicaLogDirsOptions);

/**
 * A resource the addon read as `[typeOrdinal, name]`.
 *
 * @param raw - the pair
 * @returns the resource
 */
export function configResourceOf(raw: unknown): ConfigResource {
  const [ordinal, name] = raw as [number, string];
  return new ConfigResource({ type: ConfigResource.Type.values()[ordinal], name });
}

/**
 * A resource, for the addon: `[typeId, name]`, Java's byte id.
 *
 * @param resource - the resource
 * @returns its fields
 */
function configResourceFields(resource: ConfigResource): [number, string] {
  return [resource.type().id(), resource.name()];
}

/**
 * A replica the addon read as `[topic, partition, brokerId]`.
 *
 * @param raw - the triple
 * @returns the replica
 */
export function topicPartitionReplicaOf(raw: unknown): TopicPartitionReplica {
  const [topic, partition, brokerId] = raw as [string, number, number];
  return new TopicPartitionReplica({ topic, partition, brokerId });
}

/**
 * A replica, for the addon: `[topic, partition, brokerId]`.
 *
 * @param replica - the replica
 * @returns its fields
 */
function topicPartitionReplicaFields(replica: TopicPartitionReplica): [string, number, number] {
  return [replica.topic(), replica.partition(), replica.brokerId()];
}

/** A log directory's description as the addon reads it. */
interface LogDirFields {
  error: unknown;
  replicaInfos: Array<[[string, number], { size: bigint; offsetLag: bigint; isFuture: boolean }]>;
  totalBytes: bigint;
  usableBytes: bigint;
  isCordoned: boolean;
}

/**
 * A broker's log directories the addon read, `[[path, fields]]`, as Java's
 * `Map<String, LogDirDescription>`.
 *
 * @param raw - the directories
 * @returns the descriptions, by path
 */
export function logDirDescriptionsOf(raw: unknown): Map<string, LogDirDescription> {
  const out = new Map<string, LogDirDescription>();
  for (const [path, f] of raw as Array<[string, LogDirFields]>) {
    const infos = new TopicPartitionMap<ReplicaInfo>();
    for (const [[topic, partition], info] of f.replicaInfos) {
      infos.set(new TopicPartition({ topic, partition }), new ReplicaInfo(info));
    }
    out.set(
      path,
      new LogDirDescription({
        error: f.error as ApiError | null,
        replicaInfos: infos,
        totalBytes: f.totalBytes,
        usableBytes: f.usableBytes,
        isCordoned: f.isCordoned,
      }),
    );
  }
  return out;
}

/** A replica's log directories as the addon reads them. */
interface ReplicaLogDirFields {
  currentReplicaLogDir: string | null;
  currentReplicaOffsetLag: bigint;
  futureReplicaLogDir: string | null;
  futureReplicaOffsetLag: bigint;
}

/**
 * A replica's log directories the addon read.
 *
 * @param raw - the fields
 * @returns the information
 */
function replicaLogDirInfoOf(raw: unknown): DescribeReplicaLogDirsResult.ReplicaLogDirInfo {
  const f = raw as ReplicaLogDirFields;
  return DescribeReplicaLogDirsResult.ReplicaLogDirInfo.internalCreate(
    f.currentReplicaLogDir,
    f.currentReplicaOffsetLag,
    f.futureReplicaLogDir,
    f.futureReplicaOffsetLag,
  );
}

/**
 * Whether `value` is an iterable that is not a string.
 *
 * @param value - the value given
 * @returns whether it is one
 */
function isIterable(value: unknown): value is Iterable<unknown> {
  return value !== null && typeof value === 'object' && typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] === 'function';
}

/**
 * `describeConfigs(resources)` and `describeConfigs(resources, options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function describeConfigs(handle: AdminHandle, args: unknown, count: number): DescribeConfigsResult {
  DESCRIBE_CONFIGS_FORMS.match(args, count);
  const resources = instancesOf(DESCRIBE_CONFIGS_FORMS.value(args, 'resources'), 'resources', ConfigResource).map(configResourceFields);
  const box = native.adminDescribeConfigs(handle, resources, optionSetters(DESCRIBE_CONFIGS_FORMS, args, DescribeConfigsOptions));
  const futures = adminFutures(handle, box, 'values', configResourceOf, (raw) => configOf(raw) as Config) ?? [];
  return DescribeConfigsResult.internalCreate(new ConfigResourceMap<KafkaFuture<Config>>(futures));
}

/**
 * `incrementalAlterConfigs(configs)` and `incrementalAlterConfigs(configs,
 * options)`, the map a ConfigResourceMap of iterables of AlterConfigOp.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function incrementalAlterConfigs(handle: AdminHandle, args: unknown, count: number): AlterConfigsResult {
  INCREMENTAL_ALTER_CONFIGS_FORMS.match(args, count);
  const configs = INCREMENTAL_ALTER_CONFIGS_FORMS.value(args, 'configs');
  if (configs === null) throw new TypeError('configs must not be null');
  if (!(configs instanceof ConfigResourceMap)) {
    throw new TypeError(`configs must be a ConfigResourceMap, not ${kind(configs)}: build one with new ConfigResourceMap()`);
  }
  const entries: Array<[[number, string], Array<[number, string, string | null]>]> = [];
  // Java's KafkaAdminClient dereferences each resource first (nodeFor), then
  // reads options.timeoutMs() for its calls, and the operations only when it
  // builds the request.
  const given = [...(configs as ConfigResourceMap<unknown>)];
  for (const [resource] of given) if (resource === null) throw new TypeError('configs must not contain a null key');
  if (given.length > 0) optionsFirst(INCREMENTAL_ALTER_CONFIGS_FORMS, args);
  for (const [resource, ops] of given) {
    if (resource === null) throw new TypeError('configs must not contain a null key');
    if (ops === null) throw new TypeError('configs must not contain null');
    if (typeof ops === 'string' || !isIterable(ops))
      throw new TypeError(`configs must map to iterables of AlterConfigOp, not ${kind(ops)}`);
    const rows: Array<[number, string, string | null]> = [];
    for (const op of ops) {
      if (op === null) throw new TypeError('configs must not contain null');
      if (!(op instanceof AlterConfigOp)) throw new TypeError(`configs must contain only AlterConfigOp, not ${kind(op)}`);
      const entry = op.configEntry();
      const opType = op.opType();
      if (entry === null) throw new TypeError('configs must not contain an AlterConfigOp with a null configEntry');
      if (opType === null) throw new TypeError('configs must not contain an AlterConfigOp with a null opType');
      rows.push([opType.id(), entry.name(), entry.value()]);
    }
    entries.push([configResourceFields(resource), rows]);
  }
  const box = native.adminIncrementalAlterConfigs(
    handle,
    entries,
    optionSetters(INCREMENTAL_ALTER_CONFIGS_FORMS, args, AlterConfigsOptions),
  );
  const futures = adminFutures(handle, box, 'values', configResourceOf, voidOf) ?? [];
  return AlterConfigsResult.internalCreate(new ConfigResourceMap<KafkaFuture<void>>(futures));
}

/**
 * `listConfigResources()` and `listConfigResources(configResourceTypes,
 * options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function listConfigResources(handle: AdminHandle, args: unknown, count: number): ListConfigResourcesResult {
  const form = LIST_CONFIG_RESOURCES_FORMS.match(args, count);
  // Java's body reads the options before the arguments it only stores.
  optionsFirst(LIST_CONFIG_RESOURCES_FORMS, args);
  let typeIds: number[] = [];
  if (form === 1) {
    const types = LIST_CONFIG_RESOURCES_FORMS.value(args, 'configResourceTypes');
    if (types === null) throw new TypeError('configResourceTypes must not be null');
    if (typeof types === 'string' || !isIterable(types)) {
      throw new TypeError(`configResourceTypes must be an iterable of ConfigResource.Type, not ${kind(types)}`);
    }
    const ids = new Set<number>();
    for (const t of types) {
      if (t === null) throw new TypeError('configResourceTypes must not contain null');
      if (!(t instanceof ConfigResource.Type)) {
        throw new TypeError(`configResourceTypes must contain only ConfigResource.Type, not ${kind(t)}`);
      }
      ids.add(t.id());
    }
    typeIds = [...ids];
  }
  const box = native.adminListConfigResources(
    handle,
    typeIds,
    optionSetters(LIST_CONFIG_RESOURCES_FORMS, args, ListConfigResourcesOptions),
  );
  return ListConfigResourcesResult.internalCreate(adminFuture(handle, box, 'all', null, (raw) => (raw as unknown[]).map(configResourceOf)));
}

/**
 * `describeLogDirs(brokers)` and `describeLogDirs(brokers, options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function describeLogDirs(handle: AdminHandle, args: unknown, count: number): DescribeLogDirsResult {
  DESCRIBE_LOG_DIRS_FORMS.match(args, count);
  const given = DESCRIBE_LOG_DIRS_FORMS.value(args, 'brokers');
  if (given === null) throw new TypeError('brokers must not be null');
  if (typeof given === 'string' || !isIterable(given)) throw new TypeError(`brokers must be an iterable of numbers, not ${kind(given)}`);
  const brokers: number[] = [];
  for (const b of given) {
    // Java's loop reads options.timeoutMs() for each broker's call before it
    // unboxes the broker id.
    if (brokers.length === 0) optionsFirst(DESCRIBE_LOG_DIRS_FORMS, args);
    // Java unboxes each broker id.
    if (b === null) throw new TypeError('brokers must not contain null');
    brokers.push(checkInt(b, 'a broker id'));
  }
  const box = native.adminDescribeLogDirs(handle, brokers, optionSetters(DESCRIBE_LOG_DIRS_FORMS, args, DescribeLogDirsOptions));
  const futures = adminFutures(handle, box, 'descriptions', (raw) => raw as number, logDirDescriptionsOf) ?? [];
  return DescribeLogDirsResult.internalCreate(new Map(futures));
}

/**
 * `alterReplicaLogDirs(replicaAssignment)` and
 * `alterReplicaLogDirs(replicaAssignment, options)`, the map a
 * TopicPartitionReplicaMap of log directories.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function alterReplicaLogDirs(handle: AdminHandle, args: unknown, count: number): AlterReplicaLogDirsResult {
  ALTER_REPLICA_LOG_DIRS_FORMS.match(args, count);
  const assignment = ALTER_REPLICA_LOG_DIRS_FORMS.value(args, 'replicaAssignment');
  if (assignment === null) throw new TypeError('replicaAssignment must not be null');
  if (!(assignment instanceof TopicPartitionReplicaMap)) {
    throw new TypeError(
      `replicaAssignment must be a TopicPartitionReplicaMap, not ${kind(assignment)}: build one with new TopicPartitionReplicaMap()`,
    );
  }
  const entries: Array<[[string, number, number], string | null]> = [];
  for (const [replica, dir] of assignment as TopicPartitionReplicaMap<unknown>) {
    // Java's KafkaAdminClient dereferences each replica.
    if (replica === null) throw new TypeError('replicaAssignment must not contain a null key');
    if (dir !== null && typeof dir !== 'string') throw new TypeError(`replicaAssignment must map to strings, not ${kind(dir)}`);
    entries.push([topicPartitionReplicaFields(replica), dir]);
  }
  const box = native.adminAlterReplicaLogDirs(
    handle,
    entries,
    optionSetters(ALTER_REPLICA_LOG_DIRS_FORMS, args, AlterReplicaLogDirsOptions),
  );
  const futures = adminFutures(handle, box, 'values', topicPartitionReplicaOf, voidOf) ?? [];
  return AlterReplicaLogDirsResult.internalCreate(new TopicPartitionReplicaMap<KafkaFuture<void>>(futures));
}

/**
 * `describeReplicaLogDirs(replicas)` and `describeReplicaLogDirs(replicas,
 * options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function describeReplicaLogDirs(handle: AdminHandle, args: unknown, count: number): DescribeReplicaLogDirsResult {
  DESCRIBE_REPLICA_LOG_DIRS_FORMS.match(args, count);
  const replicas = instancesOf(DESCRIBE_REPLICA_LOG_DIRS_FORMS.value(args, 'replicas'), 'replicas', TopicPartitionReplica).map(
    topicPartitionReplicaFields,
  );
  const box = native.adminDescribeReplicaLogDirs(
    handle,
    replicas,
    optionSetters(DESCRIBE_REPLICA_LOG_DIRS_FORMS, args, DescribeReplicaLogDirsOptions),
  );
  const futures = adminFutures(handle, box, 'values', topicPartitionReplicaOf, replicaLogDirInfoOf) ?? [];
  return DescribeReplicaLogDirsResult.internalCreate(
    new TopicPartitionReplicaMap<KafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>>(futures),
  );
}
