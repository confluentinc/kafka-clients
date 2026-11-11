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

// Java's PartitionInfo. It is not a data class (root CLAUDE.md, JavaScript
// Binding Conventions, Class family: the data-class list does not name it),
// so it keeps Java's methods, `topic()`, `partition()` and `leader()`, over
// private fields. Java's equals() and hashCode() are not translated (no
// JavaScript operator or collection calls them).
//
// Not translated: Java's public constructors, which take Node[] arrays, and
// the Node[] getters replicas(), inSyncReplicas() and offlineReplicas(): how
// a Java array other than byte[] is typed is not decided yet (Q25, other
// collections). The client builds a PartitionInfo through its library-only
// factory.

import { JavaForms } from '../internal/args';
import type { Node } from './node';

const TOPIC_FORMS = new JavaForms('topic', [[]], { reader: true });
const PARTITION_FORMS = new JavaForms('partition', [[]], { reader: true });
const LEADER_FORMS = new JavaForms('leader', [[]], { reader: true });

/** The fields the client builds a partition's information from. */
interface PartitionInfoFields {
  topic: string;
  partition: number;
  leader: Node | null;
  replicas: readonly Node[] | null;
  inSyncReplicas: readonly Node[] | null;
  offlineReplicas: readonly Node[] | null;
}

// Java's formatNodeIds: the node ids, comma-separated, in brackets.
function formatNodeIds(nodes: readonly Node[] | null): string {
  return `[${nodes === null ? '' : nodes.map((node) => node.idString).join(',')}]`;
}

/**
 * This is used to describe per-partition state in the MetadataResponse.
 */
export class PartitionInfo {
  readonly #topic: string;
  readonly #partition: number;
  readonly #leader: Node | null;
  readonly #replicas: readonly Node[] | null;
  readonly #inSyncReplicas: readonly Node[] | null;
  readonly #offlineReplicas: readonly Node[] | null;

  private constructor(fields: PartitionInfoFields) {
    this.#topic = fields.topic;
    this.#partition = fields.partition;
    this.#leader = fields.leader;
    this.#replicas = fields.replicas;
    this.#inSyncReplicas = fields.inSyncReplicas;
    this.#offlineReplicas = fields.offlineReplicas;
  }

  /**
   * A partition's information, from the fields the client read. Called by
   * the client, and by the tests.
   *
   * @internal
   */
  static internalCreate(fields: PartitionInfoFields): PartitionInfo {
    return new PartitionInfo(fields);
  }

  /**
   * The topic name
   *
   * @returns the topic name
   * @throws `TypeError` when it is given an argument
   */
  topic(): string {
    TOPIC_FORMS.match(undefined, arguments.length);
    return this.#topic;
  }

  /**
   * The partition id
   *
   * @returns the partition id
   * @throws `TypeError` when it is given an argument
   */
  partition(): number {
    PARTITION_FORMS.match(undefined, arguments.length);
    return this.#partition;
  }

  /**
   * The node id of the node currently acting as a leader for this partition
   * or null if there is no leader
   *
   * @returns the leader, or null
   * @throws `TypeError` when it is given an argument
   */
  leader(): Node | null {
    LEADER_FORMS.match(undefined, arguments.length);
    return this.#leader;
  }

  /**
   * The partition as Java writes it: `Partition(topic = <topic>, partition =
   * <partition>, leader = <the leader's id, or none>, replicas = [<ids>], isr
   * = [<ids>], offlineReplicas = [<ids>])`.
   *
   * @returns the partition's text
   */
  toString(): string {
    return (
      `Partition(topic = ${this.#topic}, partition = ${this.#partition}, ` +
      `leader = ${this.#leader === null ? 'none' : this.#leader.idString}, ` +
      `replicas = ${formatNodeIds(this.#replicas)}, isr = ${formatNodeIds(this.#inSyncReplicas)}, ` +
      `offlineReplicas = ${formatNodeIds(this.#offlineReplicas)})`
    );
  }
}
