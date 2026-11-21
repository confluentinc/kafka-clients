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

// Java's org.apache.kafka.clients.admin.ReplicaInfo, a value class that keeps
// Java's methods: a description of a replica on a particular broker.

import { JavaForms, kind } from '../internal/args';
import { checkLong } from '../internal/java_number';

const FORMS = new JavaForms('ReplicaInfo', [['size', 'offsetLag', 'isFuture']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const SIZE_FORMS = READER('size');
const OFFSET_LAG_FORMS = READER('offsetLag');
const IS_FUTURE_FORMS = READER('isFuture');

/**
 * A description of a replica on a particular broker.
 */
export class ReplicaInfo {
  readonly #size: bigint;
  readonly #offsetLag: bigint;
  readonly #isFuture: boolean;

  /**
   * A replica's description.
   *
   * @param options - the size of its log segments, its offset lag, and
   *   whether it is a future replica
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{size, offsetLag, isFuture}`
   * @throws `TypeError` for a value of the wrong type
   * @throws `RangeError` for a number beyond 2^53 - 1 or a bigint beyond
   *   Java's `long`
   */
  constructor(options: {
    /** The total size of the log segments in this replica in bytes. */
    size: bigint | number;
    /** The lag of the log's LEO. */
    offsetLag: bigint | number;
    /** Whether this replica is a future replica. */
    isFuture: boolean;
  }) {
    FORMS.match(options, arguments.length);
    this.#size = checkLong(FORMS.value(options, 'size'), 'size');
    this.#offsetLag = checkLong(FORMS.value(options, 'offsetLag'), 'offsetLag');
    const isFuture = FORMS.value(options, 'isFuture');
    if (typeof isFuture !== 'boolean') throw new TypeError(`isFuture must be a boolean, not ${kind(isFuture)}`);
    this.#isFuture = isFuture;
  }

  /**
   * The total size of the log segments in this replica in bytes.
   * This value does not include the size of data stored in remote storage.
   *
   * @returns the size
   * @throws `TypeError` when it is given an argument: it only reads
   */
  size(): bigint {
    if (arguments.length !== 0) SIZE_FORMS.match(undefined, arguments.length);
    return this.#size;
  }

  /**
   * The lag of the log's LEO with respect to the partition's high watermark
   * (if it is the current log for the partition) or the current replica's
   * LEO (if it is the future log for the partition).
   *
   * @returns the offset lag
   * @throws `TypeError` when it is given an argument: it only reads
   */
  offsetLag(): bigint {
    if (arguments.length !== 0) OFFSET_LAG_FORMS.match(undefined, arguments.length);
    return this.#offsetLag;
  }

  /**
   * Whether this replica has been created by a AlterReplicaLogDirsRequest but
   * not yet replaced the current replica on the broker.
   *
   * @returns true if this log is created by AlterReplicaLogDirsRequest and
   *   will replace the current log of the replica at some time in the future.
   * @throws `TypeError` when it is given an argument: it only reads
   */
  isFuture(): boolean {
    if (arguments.length !== 0) IS_FUTURE_FORMS.match(undefined, arguments.length);
    return this.#isFuture;
  }

  /**
   * Java's `toString()`: `ReplicaInfo(size=<n>, offsetLag=<n>, isFuture=<b>)`.
   *
   * @returns the text
   */
  toString(): string {
    return `ReplicaInfo(size=${this.#size}, offsetLag=${this.#offsetLag}, isFuture=${String(this.#isFuture)})`;
  }

  /**
   * The replica as `JSON.stringify` writes it: `{ size, offsetLag, isFuture }`,
   * each `long` as a decimal string.
   *
   * @returns the replica's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { size: string; offsetLag: string; isFuture: boolean } {
    return { size: this.#size.toString(), offsetLag: this.#offsetLag.toString(), isFuture: this.#isFuture };
  }
}
