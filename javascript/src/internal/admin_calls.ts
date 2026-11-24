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

// The admin RPCs both admin clients make (private): KafkaAdminClient and
// MockAdminClient share one native handle type (native/admin.c), so each
// method of theirs calls the function of the same name here, which checks
// the call's form, runs the RPC and builds its Result from the native
// result's futures (src/internal/admin_result.ts). A form or argument error
// throws; the RPC returns its Result at once, as Java's does.

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way
import native = require('./native');
import { isPlainObject, JavaForms, kind } from './args';
import { durationToMillis, isDurationOrNull } from './duration';
import { adminFuture, adminFutures } from './admin_result';
import type { AdminHandle } from './admin_result';
import { AclOperation } from '../common/acl/acl_operation';
import { Node } from '../common/node';
import { TopicCollection } from '../common/topic_collection';
import { TopicPartition } from '../common/topic_partition';
import { TopicPartitionInfo } from '../common/topic_partition_info';
import { TopicPartitionMap } from '../common/topic_partition_map';
import { Uuid } from '../common/uuid';
import { UuidMap } from '../common/uuid_map';
import type { KafkaFuture } from '../common/kafka_future';
import { Config } from '../admin/config';
import { ConfigEntry } from '../admin/config_entry';
import { CreatePartitionsOptions } from '../admin/create_partitions_options';
import { CreatePartitionsResult } from '../admin/create_partitions_result';
import { CreateTopicsOptions } from '../admin/create_topics_options';
import { CreateTopicsResult } from '../admin/create_topics_result';
import { DeleteRecordsOptions } from '../admin/delete_records_options';
import { DeleteRecordsResult } from '../admin/delete_records_result';
import { DeleteTopicsOptions } from '../admin/delete_topics_options';
import { DeleteTopicsResult } from '../admin/delete_topics_result';
import { DeletedRecords } from '../admin/deleted_records';
import { DescribeTopicsOptions } from '../admin/describe_topics_options';
import { DescribeTopicsResult } from '../admin/describe_topics_result';
import { NewPartitions } from '../admin/new_partitions';
import { NewTopic } from '../admin/new_topic';
import { RecordsToDelete } from '../admin/records_to_delete';
import { TopicDescription } from '../admin/topic_description';
import type { AbstractOptions } from '../admin/abstract_options';
import { DescribeClusterOptions } from '../admin/describe_cluster_options';
import { DescribeClusterResult } from '../admin/describe_cluster_result';
import { ListTopicsOptions } from '../admin/list_topics_options';
import { ListTopicsResult } from '../admin/list_topics_result';
import { TopicListing } from '../admin/topic_listing';

/** The fields of a node, as the addon reads them. */
interface NodeFields {
  id: number;
  host: string | null;
  port: number;
  rack: string | null;
  isFenced: boolean;
}

/**
 * The forms of an RPC `name(..., options)`: Java's forms without and with
 * the options, the options also taken on its own when the RPC takes nothing
 * else.
 *
 * @param name - the method's name
 * @param params - the parameters before the options, in Java's order
 * @param options - the options class
 * @returns the forms
 */
export function rpcForms(name: string, params: readonly string[], options: abstract new (...args: never[]) => AbstractOptions): JavaForms {
  if (params.length === 0) {
    return new JavaForms(name, [[], ['options']], { bareTypes: { options: (value) => value === null || value instanceof options } });
  }
  return new JavaForms(name, [[...params], [...params, 'options']]);
}

/**
 * The setters an RPC's options ran, for the addon: Java's `new XxxOptions()`
 * when they are left out, Java's null for `null` (which the FFI reports as
 * `TypeError`, "options must not be null").
 *
 * @param forms - the RPC's forms
 * @param args - the call's argument
 * @param cls - the options class
 * @returns the setters, or null
 * @throws `TypeError` when the options are not an instance of the class
 */
export function optionSetters(
  forms: JavaForms,
  args: unknown,
  cls: (new () => AbstractOptions) & { readonly name: string },
): Array<[string, unknown]> | null {
  const options = forms.value(args, 'options');
  if (options === undefined) return new cls().internalSetters();
  if (options === null) return null;
  if (!(options instanceof cls))
    throw new TypeError(`options must be a ${cls.name}, not ${kind(options)}: build one with new ${cls.name}()`);
  return options.internalSetters();
}

/**
 * Java's first dereference, `options`, checked before the arguments Java only
 * stores (javascript/CLAUDE.md, JavaScript Binding Conventions, Errors: the
 * checks run in Java's order), for the RPCs whose Java body reads the options
 * first. Options left out are Java's `new XxxOptions()`.
 *
 * @param forms - the RPC's forms
 * @param args - the call's argument
 * @throws `TypeError` "options must not be null", the FFI's own text
 */
export function optionsFirst(forms: JavaForms, args: unknown): void {
  if (forms.value(args, 'options') === null) throw new TypeError('options must not be null');
}

/**
 * A node the addon read, as Java's `new Node(id, host, port, rack, isFenced)`.
 *
 * @param fields - the node's fields, or null
 * @returns the node, or null
 */
export function nodeOf(fields: unknown): Node | null {
  return fields === null ? null : new Node(fields as NodeFields);
}

/**
 * A node's fields, for the addon.
 *
 * @param node - the node
 * @returns its fields
 */
export function nodeFields(node: Node): NodeFields {
  return { id: node.id, host: node.host, port: node.port, rack: node.rack, isFenced: node.isFenced };
}

/**
 * A Uuid the addon read as its two halves.
 *
 * @param raw - `[mostSigBits, leastSigBits]`, or null
 * @returns the Uuid, or null
 */
export function uuidOf(raw: unknown): Uuid | null {
  if (raw === null) return null;
  const [mostSigBits, leastSigBits] = raw as [bigint, bigint];
  return new Uuid({ mostSigBits, leastSigBits });
}

/**
 * The operations the addon read as their codes, as Java's `Set<AclOperation>`.
 *
 * @param raw - the codes, or null (Java's null: not requested)
 * @returns the operations, or null
 */
export function aclOperationsOf(raw: unknown): Set<AclOperation> | null {
  return raw === null ? null : new Set((raw as number[]).map((code) => AclOperation.fromCode(code)));
}

/**
 * A Uuid's two halves, for the addon.
 *
 * @param id - the Uuid
 * @returns `[mostSigBits, leastSigBits]`
 */
export function uuidFields(id: Uuid): [bigint, bigint] {
  return [id.getMostSignificantBits(), id.getLeastSignificantBits()];
}

/**
 * The value of a `KafkaFuture<Void>`: Java's null.
 *
 * @returns null, as void
 */
export function voidOf(): void {
  return null as unknown as void;
}

/** A TopicPartitionInfo's fields as the addon reads them. */
interface TopicPartitionInfoFields {
  partition: number;
  leader: unknown;
  replicas: unknown[];
  isr: unknown[];
  elr: unknown[] | null;
  lastKnownElr: unknown[] | null;
}

/**
 * A partition's information the addon read, through Java's constructor of
 * the same fields.
 *
 * @param raw - its fields
 * @returns the information
 */
function topicPartitionInfoOf(raw: unknown): TopicPartitionInfo {
  const f = raw as TopicPartitionInfoFields;
  const nodes = (list: unknown[]): Node[] => list.map((n) => nodeOf(n) as Node);
  const base = { partition: f.partition, leader: nodeOf(f.leader), replicas: nodes(f.replicas), isr: nodes(f.isr) };
  if (f.elr === null || f.lastKnownElr === null) return new TopicPartitionInfo(base);
  return new TopicPartitionInfo({ ...base, elr: nodes(f.elr), lastKnownElr: nodes(f.lastKnownElr) });
}

/**
 * A partition's information, for the addon.
 *
 * @param info - the information
 * @returns its fields
 */
export function topicPartitionInfoFields(info: TopicPartitionInfo): TopicPartitionInfoFields {
  const leader = info.leader();
  return {
    partition: info.partition(),
    leader: leader === null ? null : nodeFields(leader),
    replicas: info.replicas().map(nodeFields),
    isr: info.isr().map(nodeFields),
    elr: info.elr()?.map(nodeFields) ?? null,
    lastKnownElr: info.lastKnownElr()?.map(nodeFields) ?? null,
  };
}

/**
 * A topic's description the addon read.
 *
 * @param raw - its fields
 * @returns the description
 */
function topicDescriptionOf(raw: unknown): TopicDescription {
  const f = raw as { name: string | null; internal: boolean; topicId: unknown; partitions: unknown[]; authorizedOperations: unknown };
  return new TopicDescription({
    name: f.name,
    internal: f.internal,
    partitions: f.partitions.map(topicPartitionInfoOf),
    authorizedOperations: aclOperationsOf(f.authorizedOperations),
    topicId: uuidOf(f.topicId),
  });
}

/** A ConfigEntry's fields as the addon reads them, the source and type their ordinals (-1 for Java's null). */
interface ConfigEntryFields {
  name: string;
  value: string | null;
  source: number;
  isSensitive: boolean;
  isReadOnly: boolean;
  synonyms: Array<{ name: string | null; value: string | null; source: number }>;
  type: number;
  documentation: string | null;
}

/**
 * A topic's configuration the addon read, as its entries' fields.
 *
 * @param raw - the entries' fields, or null
 * @returns the configuration, or null
 */
export function configOf(raw: unknown): Config | null {
  if (raw === null) return null;
  const sources = ConfigEntry.ConfigSource.values();
  const types = ConfigEntry.ConfigType.values();
  const source = (ordinal: number): ConfigEntry.ConfigSource | null => (ordinal < 0 ? null : sources[ordinal]);
  const entries = (raw as ConfigEntryFields[]).map(
    (e) =>
      new ConfigEntry({
        name: e.name,
        value: e.value,
        source: source(e.source),
        isSensitive: e.isSensitive,
        isReadOnly: e.isReadOnly,
        synonyms: e.synonyms.map((s) => ConfigEntry.ConfigSynonym.internalCreate(s.name, s.value, source(s.source))),
        type: e.type < 0 ? null : types[e.type],
        documentation: e.documentation,
      }),
  );
  return new Config(entries);
}

/**
 * The elements of an iterable Java takes as a `Collection` of `cls`.
 *
 * @param value - the collection given
 * @param name - the parameter's name
 * @param cls - the elements' class
 * @returns the elements
 * @throws `TypeError` when it is null, not an iterable, or holds another value
 */
export function instancesOf<T>(value: unknown, name: string, cls: abstract new (...args: never[]) => T): T[] {
  if (value === null) throw new TypeError(`${name} must not be null`);
  if (typeof value !== 'object' || typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function') {
    throw new TypeError(`${name} must be an iterable of ${cls.name}, not ${kind(value)}`);
  }
  const out: T[] = [];
  for (const v of value as Iterable<unknown>) {
    if (v === null) throw new TypeError(`${name} must not contain null`);
    if (!(v instanceof cls)) throw new TypeError(`${name} must contain only ${cls.name}, not ${kind(v)}`);
    out.push(v);
  }
  return out;
}

/**
 * The entries of a map keyed by strings: a `Map`, or a plain object.
 *
 * @param value - the map given
 * @param name - the parameter's name
 * @returns its entries
 * @throws `TypeError` when it is null or neither a `Map` nor a plain object
 */
export function stringKeyedEntries(value: unknown, name: string): Array<[string, unknown]> {
  if (value === null) throw new TypeError(`${name} must not be null`);
  if (value instanceof Map) {
    const out: Array<[string, unknown]> = [];
    for (const [k, v] of value as Map<unknown, unknown>) {
      if (typeof k !== 'string') throw new TypeError(`${name} must be keyed by strings, not ${kind(k)}`);
      out.push([k, v]);
    }
    return out;
  }
  if (!isPlainObject(value)) throw new TypeError(`${name} must be a Map or a plain object, not ${kind(value)}`);
  return Object.entries(value);
}

/**
 * A map of strings keyed by strings (a `Map` or a plain object) as its
 * pairs, for the addon.
 *
 * @param value - the map given
 * @param name - the parameter's name
 * @returns its pairs
 * @throws `TypeError` when it is null, not such a map, or holds another value
 */
export function stringMapPairs(value: unknown, name: string): Array<[string, string | null]> {
  return stringKeyedEntries(value, name).map(([k, v]) => {
    if (v !== null && typeof v !== 'string') throw new TypeError(`${name} must map to strings, not ${kind(v)}`);
    return [k, v];
  });
}

/**
 * The form a topic collection takes for the addon: 0 for names given as an
 * iterable of strings, 1 for a TopicNameCollection, 2 for a
 * TopicIdCollection, with its items.
 *
 * @param topics - the collection given
 * @returns the form and its items
 * @throws `TypeError` for null, a string, or another value
 */
function topicCollectionForm(topics: unknown): [number, unknown[]] {
  if (topics instanceof TopicCollection.TopicIdCollection) {
    return [
      2,
      topics.topicIds().map((id) => {
        // Java's KafkaAdminClient dereferences each id.
        if (id === null) throw new TypeError('topics must not contain null');
        return uuidFields(id);
      }),
    ];
  }
  if (topics instanceof TopicCollection.TopicNameCollection) return [1, topics.topicNames()];
  if (topics instanceof TopicCollection) throw new TypeError('topics must be a TopicIdCollection or a TopicNameCollection');
  return [0, topicNames(topics, 'topics')];
}

/**
 * The topic names of an RPC's `Collection<String>` parameter, copied: a null
 * name goes to the core, which reports it.
 *
 * @param value - the collection given
 * @param name - the RPC's parameter
 * @returns the names
 * @throws `TypeError` for null, a string, or another value than an iterable
 *   of strings
 */
export function topicNames(value: unknown, name: string): Array<string | null> {
  if (value === null) throw new TypeError(`${name} must not be null`);
  if (typeof value === 'string') throw new TypeError(`${name} must be an iterable of strings, not string`);
  if (typeof value !== 'object' || typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function') {
    throw new TypeError(`${name} must be an iterable of strings, not ${kind(value)}`);
  }
  const out: Array<string | null> = [];
  for (const v of value as Iterable<unknown>) {
    if (v !== null && typeof v !== 'string') throw new TypeError(`${name} must be an iterable of strings, not one holding ${kind(v)}`);
    out.push(v);
  }
  return out;
}

const DESCRIBE_CLUSTER_FORMS = rpcForms('describeCluster', [], DescribeClusterOptions);
const LIST_TOPICS_FORMS = rpcForms('listTopics', [], ListTopicsOptions);
const CREATE_TOPICS_FORMS = rpcForms('createTopics', ['newTopics'], CreateTopicsOptions);
const DELETE_TOPICS_FORMS = rpcForms('deleteTopics', ['topics'], DeleteTopicsOptions);
const DESCRIBE_TOPICS_FORMS = new JavaForms(
  'describeTopics',
  [['topicNames'], ['topicNames', 'options'], ['topics'], ['topics', 'options']],
  {
    bareTypes: { topicNames: (value) => !(value instanceof TopicCollection), topics: (value) => value instanceof TopicCollection },
  },
);
const CREATE_PARTITIONS_FORMS = rpcForms('createPartitions', ['newPartitions'], CreatePartitionsOptions);
const DELETE_RECORDS_FORMS = rpcForms('deleteRecords', ['recordsToDelete'], DeleteRecordsOptions);
const CLOSE_FORMS = new JavaForms('close', [[], ['timeout']], { bareTypes: { timeout: isDurationOrNull } });

/**
 * `describeCluster()` and `describeCluster(options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function describeCluster(handle: AdminHandle, args: unknown, count: number): DescribeClusterResult {
  DESCRIBE_CLUSTER_FORMS.match(args, count);
  const box = native.adminDescribeCluster(handle, optionSetters(DESCRIBE_CLUSTER_FORMS, args, DescribeClusterOptions));
  return DescribeClusterResult.internalCreate(
    adminFuture(handle, box, 'nodes', null, (raw) => (raw as unknown[]).map((n) => nodeOf(n) as Node)),
    adminFuture(handle, box, 'controller', null, nodeOf),
    adminFuture(handle, box, 'cluster_id', null, (raw) => raw as string),
    adminFuture(handle, box, 'authorized_operations', null, aclOperationsOf),
  );
}

/**
 * `listTopics()` and `listTopics(options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function listTopics(handle: AdminHandle, args: unknown, count: number): ListTopicsResult {
  LIST_TOPICS_FORMS.match(args, count);
  const box = native.adminListTopics(handle, optionSetters(LIST_TOPICS_FORMS, args, ListTopicsOptions));
  return ListTopicsResult.internalCreate(
    adminFuture(handle, box, 'names_to_listings', null, (raw) => {
      const listings = new Map<string, TopicListing>();
      for (const [name, fields] of raw as Array<[string, { name: string; topicId: unknown; internal: boolean }]>) {
        listings.set(name, new TopicListing({ name: fields.name, topicId: uuidOf(fields.topicId), internal: fields.internal }));
      }
      return listings;
    }),
  );
}

/**
 * `createTopics(newTopics)` and `createTopics(newTopics, options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function createTopics(handle: AdminHandle, args: unknown, count: number): CreateTopicsResult {
  CREATE_TOPICS_FORMS.match(args, count);
  const newTopics = instancesOf(CREATE_TOPICS_FORMS.value(args, 'newTopics'), 'newTopics', NewTopic).map((t) => t.internalToNative());
  const box = native.adminCreateTopics(handle, newTopics, optionSetters(CREATE_TOPICS_FORMS, args, CreateTopicsOptions));
  const futures = new Map(adminFutures(handle, box, 'values', (raw) => raw as string, voidOf) ?? []);
  return CreateTopicsResult.internalCreate(futures, {
    config: (topic) => adminFuture(handle, box, 'config', topic, configOf),
    topicId: (topic) => adminFuture(handle, box, 'topic_id', topic, (raw) => uuidOf(raw) as Uuid),
    numPartitions: (topic) => adminFuture(handle, box, 'num_partitions', topic, (raw) => raw as number),
    replicationFactor: (topic) => adminFuture(handle, box, 'replication_factor', topic, (raw) => raw as number),
  });
}

/**
 * `deleteTopics(topics)` and `deleteTopics(topics, options)`, the topics an
 * iterable of names or a TopicCollection.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function deleteTopics(handle: AdminHandle, args: unknown, count: number): DeleteTopicsResult {
  DELETE_TOPICS_FORMS.match(args, count);
  const [form, items] = topicCollectionForm(DELETE_TOPICS_FORMS.value(args, 'topics'));
  const box = native.adminDeleteTopics(handle, form, items, optionSetters(DELETE_TOPICS_FORMS, args, DeleteTopicsOptions));
  if (form === 2) {
    const futures = adminFutures(handle, box, 'topic_id_values', (raw) => uuidOf(raw) as Uuid, voidOf) ?? [];
    return DeleteTopicsResult.internalOfTopicIds(new UuidMap(futures));
  }
  return DeleteTopicsResult.internalOfTopicNames(
    new Map(adminFutures(handle, box, 'topic_name_values', (raw) => raw as string, voidOf) ?? []),
  );
}

/**
 * `describeTopics(topicNames)`, `describeTopics(topics)` and their forms
 * with the options.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function describeTopics(handle: AdminHandle, args: unknown, count: number): DescribeTopicsResult {
  const index = DESCRIBE_TOPICS_FORMS.match(args, count);
  const given = DESCRIBE_TOPICS_FORMS.value(args, index < 2 ? 'topicNames' : 'topics');
  let form: number;
  let items: unknown[];
  if (index < 2) {
    form = 0;
    items = topicNames(given, 'topicNames');
  } else {
    if (given === null) throw new TypeError('topics must not be null');
    [form, items] = topicCollectionForm(given);
  }
  const box = native.adminDescribeTopics(handle, form, items, optionSetters(DESCRIBE_TOPICS_FORMS, args, DescribeTopicsOptions));
  if (form === 2) {
    const futures = adminFutures(handle, box, 'topic_id_values', (raw) => uuidOf(raw) as Uuid, topicDescriptionOf) ?? [];
    return DescribeTopicsResult.internalOfTopicIds(new UuidMap(futures));
  }
  const futures = adminFutures(handle, box, 'topic_name_values', (raw) => raw as string, topicDescriptionOf) ?? [];
  return DescribeTopicsResult.internalOfTopicNames(new Map(futures));
}

/**
 * `createPartitions(newPartitions)` and `createPartitions(newPartitions,
 * options)`, the map a `Map` or a plain object keyed by topic name.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function createPartitions(handle: AdminHandle, args: unknown, count: number): CreatePartitionsResult {
  CREATE_PARTITIONS_FORMS.match(args, count);
  const entries = stringKeyedEntries(CREATE_PARTITIONS_FORMS.value(args, 'newPartitions'), 'newPartitions').map(([topic, value]) => {
    if (value === null) throw new TypeError('newPartitions must not contain null');
    if (!(value instanceof NewPartitions)) throw new TypeError(`newPartitions must hold only NewPartitions, not ${kind(value)}`);
    return [topic, value.internalToNative()] as [string, unknown];
  });
  const box = native.adminCreatePartitions(handle, entries, optionSetters(CREATE_PARTITIONS_FORMS, args, CreatePartitionsOptions));
  return CreatePartitionsResult.internalCreate(new Map(adminFutures(handle, box, 'values', (raw) => raw as string, voidOf) ?? []));
}

/**
 * `deleteRecords(recordsToDelete)` and `deleteRecords(recordsToDelete,
 * options)`, the map a TopicPartitionMap.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function deleteRecords(handle: AdminHandle, args: unknown, count: number): DeleteRecordsResult {
  DELETE_RECORDS_FORMS.match(args, count);
  const records = DELETE_RECORDS_FORMS.value(args, 'recordsToDelete');
  if (records === null) throw new TypeError('recordsToDelete must not be null');
  if (!(records instanceof TopicPartitionMap)) {
    throw new TypeError(`recordsToDelete must be a TopicPartitionMap, not ${kind(records)}: build one with new TopicPartitionMap()`);
  }
  const entries: Array<[[string | null, number], bigint]> = [];
  for (const [tp, value] of records as TopicPartitionMap<unknown>) {
    if (tp === null) throw new TypeError('recordsToDelete must not contain a null key');
    if (value === null) throw new TypeError('recordsToDelete must not contain null');
    if (!(value instanceof RecordsToDelete)) throw new TypeError(`recordsToDelete must hold only RecordsToDelete, not ${kind(value)}`);
    entries.push([[tp.topic, tp.partition], value.beforeOffset()]);
  }
  const box = native.adminDeleteRecords(handle, entries, optionSetters(DELETE_RECORDS_FORMS, args, DeleteRecordsOptions));
  const futures = adminFutures(
    handle,
    box,
    'low_watermarks',
    (raw) => {
      const [topic, partition] = raw as [string, number];
      return new TopicPartition({ topic, partition });
    },
    (raw) => new DeletedRecords(raw as bigint),
  );
  return new DeleteRecordsResult(new TopicPartitionMap<KafkaFuture<DeletedRecords>>(futures ?? []));
}

/**
 * `close()` and `close(timeout)`: the form picks the FFI's call; a negative
 * timeout goes to it as it is, which reads it as zero.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns a Promise settling once the client closed
 */
export function close(handle: AdminHandle, args: unknown, count: number): Promise<void> {
  let timeoutMs: number | bigint | null;
  try {
    const form = CLOSE_FORMS.match(args, count);
    timeoutMs = form === 0 ? null : durationToMillis(CLOSE_FORMS.value(args, 'timeout'), 'timeout');
  } catch (error) {
    // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what the checks threw: an Error of the client's or JavaScript's (typed unknown by catch)
    return Promise.reject(error);
  }
  return new Promise<void>((resolve, reject) => {
    native.adminClose(handle, timeoutMs, (error) => {
      // An answer the addon could not build rejects with the failure; the
      // client closed all the same.
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with the failure to build the answer, typed unknown at the native boundary
      if (error !== null) reject(error);
      else resolve();
    });
  });
}
