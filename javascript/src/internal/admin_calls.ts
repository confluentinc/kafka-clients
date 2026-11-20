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
import { JavaForms, kind } from './args';
import { durationToMillis, isDurationOrNull } from './duration';
import { adminFuture } from './admin_result';
import type { AdminHandle } from './admin_result';
import { AclOperation } from '../common/acl/acl_operation';
import { Node } from '../common/node';
import { Uuid } from '../common/uuid';
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

const DESCRIBE_CLUSTER_FORMS = rpcForms('describeCluster', [], DescribeClusterOptions);
const LIST_TOPICS_FORMS = rpcForms('listTopics', [], ListTopicsOptions);
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
  return new Promise<void>((resolve) => {
    native.adminClose(handle, timeoutMs, () => resolve());
  });
}
