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

// Java's org.apache.kafka.clients.admin.DeletedRecords.

import { JavaForms } from '../internal/args';
import { checkLong } from '../internal/java_number';

const FORMS = new JavaForms('DeletedRecords', [['lowWatermark']], {
  bareTypes: { lowWatermark: (value) => typeof value === 'bigint' || typeof value === 'number' },
});
const LOW_WATERMARK_FORMS = new JavaForms('lowWatermark', [[]], { reader: true });

/**
 * Represents information about deleted records.
 */
export class DeletedRecords {
  readonly #lowWatermark: bigint;

  /**
   * Create an instance of this class with the provided parameters.
   *
   * @param lowWatermark - "low watermark" for the topic partition on which
   *   the deletion was executed, on its own
   * @throws `TypeError` or `RangeError` when it is not a Java `long`
   */
  constructor(lowWatermark: bigint | number);
  /**
   * Create an instance of this class with the provided parameters.
   *
   * @param options - "low watermark" for the topic partition on which the
   *   deletion was executed
   * @throws `TypeError` or `RangeError` when it is not a Java `long`
   */
  constructor(options: {
    /** "low watermark" for the topic partition on which the deletion was executed. */
    lowWatermark: bigint | number;
  });
  /**
   * Builds the deleted records.
   *
   * @param options - the low watermark
   * @throws `TypeError` or `RangeError` when it is not a Java `long`
   */
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    this.#lowWatermark = checkLong(FORMS.value(options, 'lowWatermark'), 'lowWatermark');
  }

  /**
   * Return the "low watermark" for the topic partition on which the deletion
   * was executed.
   *
   * @returns the low watermark
   * @throws `TypeError` when it is given an argument: it only reads
   */
  lowWatermark(): bigint {
    if (arguments.length !== 0) LOW_WATERMARK_FORMS.match(undefined, arguments.length);
    return this.#lowWatermark;
  }

  /**
   * The deleted records as JSON, the low watermark a decimal string.
   *
   * @returns the low watermark
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): Record<string, unknown> {
    return { lowWatermark: this.#lowWatermark.toString() };
  }
}
