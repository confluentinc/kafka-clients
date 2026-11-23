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

// Java's org.apache.kafka.clients.admin.AlterReplicaLogDirsResult. Its Java
// constructor is package-private: a private constructor here, the client
// building it through `internalCreate` (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Class family). Java's map keyed by TopicPartitionReplica
// is a TopicPartitionReplicaMap.

import { JavaForms } from '../internal/args';
import { KafkaFuture } from '../common/kafka_future';
import type { TopicPartitionReplicaMap } from '../common/topic_partition_replica_map';

const VALUES_FORMS = new JavaForms('values', [[]], { reader: true });
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });

/**
 * The result of `Admin.alterReplicaLogDirs(replicaAssignment, options)`.
 *
 * To retrieve the detailed result per specified `TopicPartitionReplica`, use
 * `values()`. To retrieve the overall result only, use `all()`.
 *
 * Instances come from the admin client's `alterReplicaLogDirs()`.
 */
export class AlterReplicaLogDirsResult {
  readonly #futures: TopicPartitionReplicaMap<KafkaFuture<void>>;

  private constructor(futures: TopicPartitionReplicaMap<KafkaFuture<void>>) {
    this.#futures = futures;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param futures - the futures, by replica
   * @returns the result
   * @internal
   */
  static internalCreate(futures: TopicPartitionReplicaMap<KafkaFuture<void>>): AlterReplicaLogDirsResult {
    return new AlterReplicaLogDirsResult(futures);
  }

  /**
   * Return a map from `TopicPartitionReplica` to `KafkaFuture` which holds
   * the status of individual replica movement.
   *
   * To check the result of individual replica movement, call
   * `KafkaFuture.get()` from the value contained in the returned map. If
   * there is no error, it will return silently; if not, it rejects with an
   * error like the following:
   *
   * - `CancellationError`: The task was canceled.
   * - `ExecutionError`: Execution failed with the following causes:
   *   - `ClusterAuthorizationError`: Authorization failed.
   *     (CLUSTER_AUTHORIZATION_FAILED, 31)
   *   - `InvalidTopicError`: The specified topic name is too long.
   *     (INVALID_TOPIC_EXCEPTION, 17)
   *   - `LogDirNotFoundError`: The specified log directory is not found in
   *     the broker. (LOG_DIR_NOT_FOUND, 57)
   *   - `ReplicaNotAvailableError`: The replica does not exist on the
   *     broker. (REPLICA_NOT_AVAILABLE, 9)
   *   - `KafkaStorageError`: Disk error occurred. (KAFKA_STORAGE_ERROR, 56)
   *   - `UnknownServerError`: Unknown. (UNKNOWN_SERVER_ERROR, -1)
   *
   * @returns the futures, by replica
   * @throws `TypeError` when it is given an argument: it only reads
   */
  values(): TopicPartitionReplicaMap<KafkaFuture<void>> {
    if (arguments.length !== 0) VALUES_FORMS.match(undefined, arguments.length);
    return this.#futures;
  }

  /**
   * Return a `KafkaFuture` which succeeds on `KafkaFuture.get()` if all the
   * replica movement have succeeded. if not, it rejects with an error
   * described in `values()` method.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<void> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    return KafkaFuture.allOf([...this.#futures.values()]);
  }
}
