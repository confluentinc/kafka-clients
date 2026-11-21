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

// Java's org.apache.kafka.clients.admin.OffsetSpec, a class hierarchy: the
// base class, with Java's public no-argument constructor and its static
// factories, and its nested subclasses, classes of the namespace merged with
// it (root CLAUDE.md, JavaScript Binding Conventions, Names, Q11).
// TimestampSpec's Java constructor and `timestamp()` are package-private: a
// private constructor built through `internalCreate`, and the `@internal`
// `internalTimestamp()` the admin client reads. The specs hold no public
// state, so they have no `toJSON()`.

import { JavaForms } from '../internal/args';
import { checkLong } from '../internal/java_number';

const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const LATEST_FORMS = READER('latest');
const EARLIEST_FORMS = READER('earliest');
const MAX_TIMESTAMP_FORMS = READER('maxTimestamp');
const EARLIEST_LOCAL_FORMS = READER('earliestLocal');
const LATEST_TIERED_FORMS = READER('latestTiered');
const EARLIEST_PENDING_UPLOAD_FORMS = READER('earliestPendingUpload');
const CONSTRUCTOR_FORMS = new JavaForms('OffsetSpec', [[]]);
const FOR_TIMESTAMP_FORMS = new JavaForms('forTimestamp', [['timestamp']], {
  bareTypes: { timestamp: (value) => typeof value === 'number' || typeof value === 'bigint' },
});

/**
 * This class allows to specify the desired offsets when using
 * `KafkaAdminClient.listOffsets(topicPartitionOffsets, options)`.
 */
export class OffsetSpec {
  /**
   * Java's public no-argument constructor: a spec of no offset in particular,
   * which `listOffsets` reads as the latest offset, as Java's does.
   *
   * @throws `IllegalArgumentError` when it is given an argument
   */
  constructor();
  constructor(args?: unknown) {
    CONSTRUCTOR_FORMS.match(args, arguments.length);
  }

  /**
   * Used to retrieve the latest offset of a partition.
   *
   * @returns a new `LatestSpec`
   * @throws `TypeError` when it is given an argument
   */
  static latest(): OffsetSpec {
    LATEST_FORMS.match(undefined, arguments.length);
    return new OffsetSpec.LatestSpec();
  }

  /**
   * Used to retrieve the earliest offset of a partition.
   *
   * @returns a new `EarliestSpec`
   * @throws `TypeError` when it is given an argument
   */
  static earliest(): OffsetSpec {
    EARLIEST_FORMS.match(undefined, arguments.length);
    return new OffsetSpec.EarliestSpec();
  }

  /**
   * Used to retrieve the earliest offset whose timestamp is greater than or
   * equal to the given timestamp in the corresponding partition.
   *
   * @param timestamp - in milliseconds, a `bigint` or a whole number
   * @returns a new `TimestampSpec`
   */
  static forTimestamp(timestamp: bigint | number): OffsetSpec;
  /**
   * The same as `forTimestamp(timestamp)`, the timestamp given by name.
   *
   * @param options - the timestamp, in milliseconds
   * @returns a new `TimestampSpec`
   */
  static forTimestamp(options: {
    /** In milliseconds, a `bigint` or a whole number. */
    timestamp: bigint | number;
  }): OffsetSpec;
  /**
   * Makes the spec.
   *
   * @param timestamp - the timestamp, on its own or as `{ timestamp }`
   * @returns a new `TimestampSpec`
   * @throws `TypeError` when the timestamp is not a `bigint` or a whole number
   * @throws `RangeError` for a number beyond 2^53 - 1 or a bigint beyond
   *   Java's `long`
   */
  static forTimestamp(timestamp: unknown): OffsetSpec {
    FOR_TIMESTAMP_FORMS.match(timestamp, arguments.length);
    return OffsetSpec.TimestampSpec.internalCreate(checkLong(FOR_TIMESTAMP_FORMS.value(timestamp, 'timestamp'), 'timestamp'));
  }

  /**
   * Used to retrieve the offset with the largest timestamp of a partition as
   * message timestamps can be specified client side this may not match the
   * log end offset returned by LatestSpec.
   *
   * @returns a new `MaxTimestampSpec`
   * @throws `TypeError` when it is given an argument
   */
  static maxTimestamp(): OffsetSpec {
    MAX_TIMESTAMP_FORMS.match(undefined, arguments.length);
    return new OffsetSpec.MaxTimestampSpec();
  }

  /**
   * Used to retrieve the local log start offset. Local log start offset is
   * the offset of a log above which reads are guaranteed to be served from
   * the disk of the leader broker.
   *
   * Note: When tiered Storage is not enabled, it behaves the same as
   * retrieving the earliest timestamp offset.
   *
   * @returns a new `EarliestLocalSpec`
   * @throws `TypeError` when it is given an argument
   */
  static earliestLocal(): OffsetSpec {
    EARLIEST_LOCAL_FORMS.match(undefined, arguments.length);
    return new OffsetSpec.EarliestLocalSpec();
  }

  /**
   * Used to retrieve the highest offset of data stored in remote storage.
   *
   * Note: When tiered storage is not enabled, we will return unknown offset.
   *
   * @returns a new `LatestTieredSpec`
   * @throws `TypeError` when it is given an argument
   */
  static latestTiered(): OffsetSpec {
    LATEST_TIERED_FORMS.match(undefined, arguments.length);
    return new OffsetSpec.LatestTieredSpec();
  }

  /**
   * Used to retrieve the earliest offset of records that are pending upload
   * to remote storage.
   *
   * Note: When tiered storage is not enabled, we will return unknown offset.
   *
   * @returns a new `EarliestPendingUploadSpec`
   * @throws `TypeError` when it is given an argument
   */
  static earliestPendingUpload(): OffsetSpec {
    EARLIEST_PENDING_UPLOAD_FORMS.match(undefined, arguments.length);
    return new OffsetSpec.EarliestPendingUploadSpec();
  }
}

// eslint-disable-next-line @typescript-eslint/no-namespace -- Java's nested classes OffsetSpec.*Spec, reached through the class name as in Java
export namespace OffsetSpec {
  /** The spec of the earliest offset. */
  export class EarliestSpec extends OffsetSpec {}

  /** The spec of the latest offset. */
  export class LatestSpec extends OffsetSpec {}

  /** The spec of the offset with the largest timestamp. */
  export class MaxTimestampSpec extends OffsetSpec {}

  /** The spec of the local log start offset. */
  export class EarliestLocalSpec extends OffsetSpec {}

  /** The spec of the highest offset in remote storage. */
  export class LatestTieredSpec extends OffsetSpec {}

  /** The spec of the earliest offset pending upload to remote storage. */
  export class EarliestPendingUploadSpec extends OffsetSpec {}

  /**
   * The spec of the earliest offset whose timestamp is at least the given
   * one. Instances come from `OffsetSpec.forTimestamp(timestamp)`.
   */
  export class TimestampSpec extends OffsetSpec {
    readonly #timestamp: bigint;

    private constructor(timestamp: bigint) {
      super();
      this.#timestamp = timestamp;
    }

    /**
     * Java's package-private constructor. Called by `OffsetSpec.forTimestamp`.
     *
     * @param timestamp - the timestamp, in milliseconds
     * @returns the spec
     * @internal
     */
    static internalCreate(timestamp: bigint): TimestampSpec {
      return new TimestampSpec(timestamp);
    }

    /**
     * Java's package-private `timestamp()`. Called by the admin client.
     *
     * @returns the timestamp, in milliseconds
     * @internal
     */
    internalTimestamp(): bigint {
      return this.#timestamp;
    }
  }
}
