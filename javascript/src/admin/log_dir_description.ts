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

// Java's org.apache.kafka.clients.admin.LogDirDescription, a data class (root
// CLAUDE.md, JavaScript Binding Conventions, Class family; the admin
// descriptions, decided by the owner on 2026-11-20): Java's getters are
// readonly fields, set once in the constructor, and the object is frozen; its
// map of replicas is a read-only TopicPartitionMap, Java's unmodifiable view.
// Java's three constructors are one, taking the union of their parameters;
// toString is Java's.

import { JavaForms, kind } from '../internal/args';
import { checkLong } from '../internal/java_number';
import { javaString } from '../internal/java_string';
import { ApiError } from '../common/errors/api_error';
import { TopicPartition } from '../common/topic_partition';
import { TopicPartitionMap } from '../common/topic_partition_map';
import { ReplicaInfo } from './replica_info';

const FORMS = new JavaForms('LogDirDescription', [
  ['error', 'replicaInfos'],
  ['error', 'replicaInfos', 'totalBytes', 'usableBytes'],
  ['error', 'replicaInfos', 'totalBytes', 'usableBytes', 'isCordoned'],
]);

/** Java's `DescribeLogDirsResponse.UNKNOWN_VOLUME_BYTES`: the broker gave no size. */
const UNKNOWN_VOLUME_BYTES = -1n;

/**
 * Java's `OptionalLong.toString()`.
 *
 * @param value - the value, or null for empty
 * @returns the text
 */
function optionalLongString(value: bigint | null): string {
  return value === null ? 'OptionalLong.empty' : `OptionalLong[${value}]`;
}

/**
 * A description of a log directory on a particular broker.
 */
export class LogDirDescription {
  /**
   * The error if the log directory is offline or an error occurred,
   * otherwise null.
   *
   * - `KafkaStorageError` - The log directory is offline.
   * - `UnknownServerError` - The server experienced an unexpected error when
   *   processing the request.
   */
  readonly error: ApiError | null;

  /**
   * A map from topic partition to replica information for that partition in
   * this log directory, read-only.
   */
  readonly replicaInfos: TopicPartitionMap<ReplicaInfo>;

  /**
   * The total size of the volume this log directory is on or null if the
   * broker did not return a value. For volumes larger than Long.MAX_VALUE,
   * Long.MAX_VALUE is returned. This value does not include the size of data
   * stored in remote storage.
   */
  readonly totalBytes: bigint | null;

  /**
   * The usable size on the volume this log directory is on or null if the
   * broker did not return a value. For usable sizes larger than
   * Long.MAX_VALUE, Long.MAX_VALUE is returned. This value does not include
   * the size of data stored in remote storage.
   */
  readonly usableBytes: bigint | null;

  /** Whether this log directory is cordoned or not. */
  readonly isCordoned: boolean;

  /**
   * A log directory's description, its sizes unknown.
   *
   * @param options - the error, or null, and the replicas by partition
   */
  constructor(options: {
    /** The error if the log directory is offline or an error occurred, otherwise null. */
    error: ApiError | null;
    /** The replicas of this log directory, by partition. */
    replicaInfos: TopicPartitionMap<ReplicaInfo>;
    totalBytes?: never;
    usableBytes?: never;
    isCordoned?: never;
  });
  /**
   * A log directory's description.
   *
   * @param options - the error, or null, the replicas by partition, and the
   *   volume's total and usable bytes (-1 for unknown)
   */
  constructor(options: {
    /** The error if the log directory is offline or an error occurred, otherwise null. */
    error: ApiError | null;
    /** The replicas of this log directory, by partition. */
    replicaInfos: TopicPartitionMap<ReplicaInfo>;
    /** The volume's total bytes, -1 for unknown. */
    totalBytes: bigint | number;
    /** The volume's usable bytes, -1 for unknown. */
    usableBytes: bigint | number;
    isCordoned?: never;
  });
  /**
   * A log directory's description.
   *
   * @param options - the error, or null, the replicas by partition, the
   *   volume's total and usable bytes (-1 for unknown), and whether the
   *   directory is cordoned
   */
  constructor(options: {
    /** The error if the log directory is offline or an error occurred, otherwise null. */
    error: ApiError | null;
    /** The replicas of this log directory, by partition. */
    replicaInfos: TopicPartitionMap<ReplicaInfo>;
    /** The volume's total bytes, -1 for unknown. */
    totalBytes: bigint | number;
    /** The volume's usable bytes, -1 for unknown. */
    usableBytes: bigint | number;
    /** Whether this log directory is cordoned. */
    isCordoned: boolean;
  });
  /**
   * Builds the description.
   *
   * @param options - the fields of one of Java's three constructors
   * @throws `IllegalArgumentError` when the given parameters are no Java form
   * @throws `TypeError` for a value of the wrong type, or null replicaInfos
   * @throws `RangeError` for a size beyond 2^53 - 1 as a number, or beyond
   *   Java's `long`
   *
   * @remarks The map is copied into a read-only one, so a null map is a
   * `TypeError` here *(deviation: Java stores it, and its getter's
   * `unmodifiableMap(null)` throws `NullPointerException`. Why: the field is
   * set once, frozen, in the constructor)*.
   */
  constructor(options: unknown) {
    const form = FORMS.match(options, arguments.length);
    const error = FORMS.value(options, 'error');
    if (error !== null && !(error instanceof ApiError)) throw new TypeError(`error must be an ApiError or null, not ${kind(error)}`);
    const infos = FORMS.value(options, 'replicaInfos');
    if (infos === null) throw new TypeError('replicaInfos must not be null');
    if (!(infos instanceof TopicPartitionMap)) {
      throw new TypeError(`replicaInfos must be a TopicPartitionMap, not ${kind(infos)}: build one with new TopicPartitionMap()`);
    }
    const copy = new TopicPartitionMap<ReplicaInfo>();
    for (const [tp, info] of infos as TopicPartitionMap<unknown>) {
      if (tp !== null && !(tp instanceof TopicPartition)) throw new TypeError('replicaInfos must be keyed by TopicPartition');
      if (info !== null && !(info instanceof ReplicaInfo))
        throw new TypeError(`replicaInfos must hold only ReplicaInfo, not ${kind(info)}`);
      copy.set(tp, info as ReplicaInfo);
    }
    const total = form === 0 ? UNKNOWN_VOLUME_BYTES : checkLong(FORMS.value(options, 'totalBytes'), 'totalBytes');
    const usable = form === 0 ? UNKNOWN_VOLUME_BYTES : checkLong(FORMS.value(options, 'usableBytes'), 'usableBytes');
    const cordoned = form === 2 ? FORMS.value(options, 'isCordoned') : false;
    if (typeof cordoned !== 'boolean') throw new TypeError(`isCordoned must be a boolean, not ${kind(cordoned)}`);
    this.error = error;
    this.replicaInfos = copy.internalSetReadOnly();
    this.totalBytes = total === UNKNOWN_VOLUME_BYTES ? null : total;
    this.usableBytes = usable === UNKNOWN_VOLUME_BYTES ? null : usable;
    this.isCordoned = cordoned;
    Object.freeze(this);
  }

  /**
   * Java's `toString()`: `LogDirDescription(replicaInfos=<map>, error=<e>,
   * totalBytes=<optional>, usableBytes=<optional>, isCordoned=<b>)`.
   *
   * @returns the text
   */
  toString(): string {
    return (
      `LogDirDescription(replicaInfos=${this.replicaInfos.toString()}, error=${javaString(this.error)}` +
      `, totalBytes=${optionalLongString(this.totalBytes)}, usableBytes=${optionalLongString(this.usableBytes)}` +
      `, isCordoned=${String(this.isCordoned)})`
    );
  }

  /**
   * The description as JSON: its fields, the error as its `String()` text
   * (null when none), each size as a decimal string (null when unknown), the
   * replicas by the map's string key.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): Record<string, unknown> {
    return {
      error: this.error === null ? null : String(this.error),
      replicaInfos: this.replicaInfos.toJSON(),
      totalBytes: this.totalBytes === null ? null : this.totalBytes.toString(),
      usableBytes: this.usableBytes === null ? null : this.usableBytes.toString(),
      isCordoned: this.isCordoned,
    };
  }
}
