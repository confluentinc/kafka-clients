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

// The cluster metadata the clients' partitionsFor and listTopics answer, from
// the fields the addon reads (native/producer.c, partitions_to_js), and the
// partitions a MockConsumer is given, as the addon takes them
// (native/values.c, partition_info_list_from_js).

import { Node } from '../common/node';
import { PartitionInfo } from '../common/partition_info';

/** A node's fields, as the addon reads them. */
interface NodeFields {
  id: number;
  host: string | null;
  port: number;
  rack: string | null;
  isFenced: boolean;
}

/** A partition's fields, as the addon reads them. */
interface PartitionFields {
  topic: string;
  partition: number;
  leader: NodeFields | null;
  replicas: NodeFields[] | null;
  inSyncReplicas: NodeFields[] | null;
  offlineReplicas: NodeFields[] | null;
}

/**
 * The native layer's node fields as a Node, Java's five-parameter constructor.
 *
 * @param fields - the node's fields, or null
 * @returns the node, or null
 */
function nodeOf(fields: NodeFields | null): Node | null {
  return fields === null ? null : new Node(fields);
}

/**
 * The native layer's partition fields as a PartitionInfo.
 *
 * @param fields - the partition's fields
 * @returns the partition's information
 */
export function partitionInfoOf(fields: PartitionFields): PartitionInfo {
  const nodes = (list: NodeFields[] | null): Node[] | null => (list === null ? null : list.map((n) => nodeOf(n) as Node));
  return new PartitionInfo({
    topic: fields.topic,
    partition: fields.partition,
    leader: nodeOf(fields.leader),
    replicas: nodes(fields.replicas),
    inSyncReplicas: nodes(fields.inSyncReplicas),
    offlineReplicas: nodes(fields.offlineReplicas),
  });
}

/** A node's fields, as the addon takes them: a null host crosses as NULL. */
export interface NodeArgs {
  id: number;
  host: string | null;
  port: number;
  rack: string | null;
  isFenced: boolean;
}

/** A partition's fields, as the addon takes them: a null topic, list or element crosses as NULL. */
export interface PartitionInfoArgs {
  topic: string | null;
  partition: number;
  leader: NodeArgs | null;
  replicas: Array<NodeArgs | null> | null;
  inSyncReplicas: Array<NodeArgs | null> | null;
  offlineReplicas: Array<NodeArgs | null> | null;
}

/**
 * A node's fields as the addon takes them, the reverse of `nodeOf`.
 *
 * @param node - the node, or null
 * @returns its fields, or null
 */
function nodeArgs(node: Node | null): NodeArgs | null {
  return node === null ? null : { id: node.id, host: node.host, port: node.port, rack: node.rack, isFenced: node.isFenced };
}

/**
 * A PartitionInfo's fields as the addon takes them, the reverse of
 * `partitionInfoOf`.
 *
 * @param info - the partition's information
 * @returns its fields
 */
export function partitionInfoArgs(info: PartitionInfo): PartitionInfoArgs {
  const nodes = (list: Array<Node | null> | null): Array<NodeArgs | null> | null => (list === null ? null : list.map(nodeArgs));
  return {
    topic: info.topic(),
    partition: info.partition(),
    leader: nodeArgs(info.leader()),
    replicas: nodes(info.replicas()),
    inSyncReplicas: nodes(info.inSyncReplicas()),
    offlineReplicas: nodes(info.offlineReplicas()),
  };
}
