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

// Java's org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult, with its
// nested ReplicaLogDirInfo, a class of the namespace merged with it
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Names, Q11). Both Java
// constructors are package-private: private constructors here, the client
// building them through `internalCreate` (Class family, Q16). Java's map
// keyed by TopicPartitionReplica is a TopicPartitionReplicaMap (Q25).

import { JavaForms } from '../internal/args';
import { javaString } from '../internal/java_string';
import { KafkaFuture } from '../common/kafka_future';
import { TopicPartitionReplicaMap } from '../common/topic_partition_replica_map';

const VALUES_FORMS = new JavaForms('values', [[]], { reader: true });
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const CURRENT_DIR_FORMS = READER('getCurrentReplicaLogDir');
const CURRENT_LAG_FORMS = READER('getCurrentReplicaOffsetLag');
const FUTURE_DIR_FORMS = READER('getFutureReplicaLogDir');
const FUTURE_LAG_FORMS = READER('getFutureReplicaOffsetLag');

/**
 * The result of `Admin.describeReplicaLogDirs(replicas)`.
 *
 * Instances come from the admin client's `describeReplicaLogDirs()`.
 */
export class DescribeReplicaLogDirsResult {
  readonly #futures: TopicPartitionReplicaMap<KafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>>;

  private constructor(futures: TopicPartitionReplicaMap<KafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>>) {
    this.#futures = futures;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param futures - the futures, by replica
   * @returns the result
   * @internal
   */
  static internalCreate(
    futures: TopicPartitionReplicaMap<KafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>>,
  ): DescribeReplicaLogDirsResult {
    return new DescribeReplicaLogDirsResult(futures);
  }

  /**
   * Return a map from replica to future which can be used to check the log
   * directory information of individual replicas
   *
   * @returns the futures, by replica
   * @throws `TypeError` when it is given an argument: it only reads
   */
  values(): TopicPartitionReplicaMap<KafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> {
    if (arguments.length !== 0) VALUES_FORMS.match(undefined, arguments.length);
    return this.#futures;
  }

  /**
   * Return a future which succeeds if log directory information of all
   * replicas are available
   *
   * @returns the future of the information, by replica
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<TopicPartitionReplicaMap<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    const futures = this.#futures;
    return KafkaFuture.allOf([...futures.values()]).thenApply(() => {
      const infos = new TopicPartitionReplicaMap<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>();
      for (const [replica, future] of futures) {
        infos.set(replica, future.getNow({ valueIfAbsent: null }) as DescribeReplicaLogDirsResult.ReplicaLogDirInfo);
      }
      return infos;
    });
  }
}

// eslint-disable-next-line @typescript-eslint/no-namespace -- Java's nested class DescribeReplicaLogDirsResult.ReplicaLogDirInfo, reached through the class name as in Java
export namespace DescribeReplicaLogDirsResult {
  /**
   * The log directories of a replica on a broker.
   *
   * Instances come from the futures of `DescribeReplicaLogDirsResult.values()`.
   */
  export class ReplicaLogDirInfo {
    readonly #currentReplicaLogDir: string | null;
    readonly #currentReplicaOffsetLag: bigint;
    readonly #futureReplicaLogDir: string | null;
    readonly #futureReplicaOffsetLag: bigint;

    private constructor(
      currentReplicaLogDir: string | null,
      currentReplicaOffsetLag: bigint,
      futureReplicaLogDir: string | null,
      futureReplicaOffsetLag: bigint,
    ) {
      this.#currentReplicaLogDir = currentReplicaLogDir;
      this.#currentReplicaOffsetLag = currentReplicaOffsetLag;
      this.#futureReplicaLogDir = futureReplicaLogDir;
      this.#futureReplicaOffsetLag = futureReplicaOffsetLag;
    }

    /**
     * Java's package-private constructor. Called by the admin client.
     *
     * @param currentReplicaLogDir - the current log directory, or null
     * @param currentReplicaOffsetLag - the current replica's offset lag
     * @param futureReplicaLogDir - the future log directory, or null
     * @param futureReplicaOffsetLag - the future replica's offset lag, -1 for none
     * @returns the information
     * @internal
     */
    static internalCreate(
      currentReplicaLogDir: string | null,
      currentReplicaOffsetLag: bigint,
      futureReplicaLogDir: string | null,
      futureReplicaOffsetLag: bigint,
    ): ReplicaLogDirInfo {
      return new ReplicaLogDirInfo(currentReplicaLogDir, currentReplicaOffsetLag, futureReplicaLogDir, futureReplicaOffsetLag);
    }

    /**
     * The current log directory of the replica of this partition on the
     * given broker. Null if no replica is not found for this partition on the
     * given broker.
     *
     * @returns the directory, or null
     * @throws `TypeError` when it is given an argument: it only reads
     */
    getCurrentReplicaLogDir(): string | null {
      if (arguments.length !== 0) CURRENT_DIR_FORMS.match(undefined, arguments.length);
      return this.#currentReplicaLogDir;
    }

    /**
     * Defined as max(HW of partition - LEO of the replica, 0).
     *
     * @returns the lag
     * @throws `TypeError` when it is given an argument: it only reads
     */
    getCurrentReplicaOffsetLag(): bigint {
      if (arguments.length !== 0) CURRENT_LAG_FORMS.match(undefined, arguments.length);
      return this.#currentReplicaOffsetLag;
    }

    /**
     * The future log directory of the replica of this partition on the given
     * broker. Null if the replica of this partition is not being moved to
     * another log directory on the given broker.
     *
     * @returns the directory, or null
     * @throws `TypeError` when it is given an argument: it only reads
     */
    getFutureReplicaLogDir(): string | null {
      if (arguments.length !== 0) FUTURE_DIR_FORMS.match(undefined, arguments.length);
      return this.#futureReplicaLogDir;
    }

    /**
     * The LEO of the replica - LEO of the future log of this replica in the
     * destination log directory. -1 if either there is not replica for this
     * partition or the replica of this partition is not being moved to
     * another log directory on the given broker.
     *
     * @returns the lag
     * @throws `TypeError` when it is given an argument: it only reads
     */
    getFutureReplicaOffsetLag(): bigint {
      if (arguments.length !== 0) FUTURE_LAG_FORMS.match(undefined, arguments.length);
      return this.#futureReplicaOffsetLag;
    }

    /**
     * Java's `toString()`: `(currentReplicaLogDir=<d>, futureReplicaLogDir=<d>,
     * futureReplicaOffsetLag=<n>)` while the replica moves, else
     * `ReplicaLogDirInfo(currentReplicaLogDir=<d>)`.
     *
     * @returns the text
     */
    toString(): string {
      if (this.#futureReplicaLogDir !== null) {
        return (
          `(currentReplicaLogDir=${javaString(this.#currentReplicaLogDir)}, futureReplicaLogDir=${this.#futureReplicaLogDir}` +
          `, futureReplicaOffsetLag=${this.#futureReplicaOffsetLag})`
        );
      }
      return `ReplicaLogDirInfo(currentReplicaLogDir=${javaString(this.#currentReplicaLogDir)})`;
    }

    /**
     * The information as `JSON.stringify` writes it: its four fields, each
     * `long` as a decimal string.
     *
     * @returns the fields
     *
     * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
     * once released.
     */
    toJSON(): Record<string, string | null> {
      return {
        currentReplicaLogDir: this.#currentReplicaLogDir,
        currentReplicaOffsetLag: this.#currentReplicaOffsetLag.toString(),
        futureReplicaLogDir: this.#futureReplicaLogDir,
        futureReplicaOffsetLag: this.#futureReplicaOffsetLag.toString(),
      };
    }
  }
}
