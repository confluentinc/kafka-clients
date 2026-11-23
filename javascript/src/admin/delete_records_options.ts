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

// Java's org.apache.kafka.clients.admin.DeleteRecordsOptions, an option class
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family) with no
// field of its own.

import { applyOptionKeys } from '../internal/options';
import { AbstractOptions } from './abstract_options';

/**
 * Options for `Admin.deleteRecords(recordsToDelete, options)`.
 */
export class DeleteRecordsOptions extends AbstractOptions {
  /**
   * The options, each key calling its setter.
   *
   * @param options - the setters' values, each key optional
   * @throws `IllegalArgumentError` for a key the class has not
   * @throws `TypeError` for a value of the wrong type
   *
   * @remarks *(deviation)* Java builds the options through their setters
   * only. Why: one call builds a configured object; the setters stay.
   */
  constructor(options?: {
    /**
     * The timeout in milliseconds, or null for the default api timeout.
     *
     * @defaultValue `null`
     */
    timeoutMs?: number | null;
  }) {
    super();
    applyOptionKeys('DeleteRecordsOptions', options, arguments.length, [['timeoutMs', (v) => this.timeoutMs(v as number | null)]]);
  }
}
