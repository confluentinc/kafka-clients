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

// Java's org.apache.kafka.clients.admin.ListOffsetsOptions. Its isolation
// level is a final field Java's two constructors set, read by
// `isolationLevel()`. Java builds it with a constructor of its own, so it keeps
// Java's constructors only, `()` and `(isolationLevel)`, as Python's does
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family).

import { JavaForms, kind } from '../internal/args';
import { IsolationLevel } from '../common/isolation_level';
import { AbstractOptions } from './abstract_options';

const CONSTRUCTOR_FORMS = new JavaForms('ListOffsetsOptions', [[], ['isolationLevel']], {
  bareTypes: { isolationLevel: (value) => value === null || value instanceof IsolationLevel },
});
const ISOLATION_LEVEL_FORMS = new JavaForms('isolationLevel', [[]], { reader: true });

/**
 * Options for `Admin.listOffsets(topicPartitionOffsets, options)`.
 */
export class ListOffsetsOptions extends AbstractOptions {
  #isolationLevel: IsolationLevel | null = IsolationLevel.READ_UNCOMMITTED;

  /**
   * Java's `ListOffsetsOptions()`: `READ_UNCOMMITTED`.
   *
   * @remarks The class takes no keyed constructor of its setters, as Java's
   * class has a constructor of its own. Set the timeout with
   * `timeoutMs()`.
   */
  constructor();
  /**
   * Java's `ListOffsetsOptions(isolationLevel)`.
   *
   * @param isolationLevel - the isolation level of the offsets to list, on
   *   its own; null is stored, as Java stores it
   * @throws `IllegalArgumentError` for a form Java has not
   * @throws `TypeError` when the isolation level is not an IsolationLevel
   *
   * @remarks The class takes no keyed constructor of its setters, as Java's
   * class has a constructor of its own. Set the timeout with
   * `timeoutMs()`.
   */
  constructor(isolationLevel: IsolationLevel | null);
  /**
   * The same as `new ListOffsetsOptions(isolationLevel)`, the isolation level
   * given by name.
   *
   * @param options - the isolation level of the offsets to list
   * @throws `IllegalArgumentError` for a form Java has not
   * @throws `TypeError` when the isolation level is not an IsolationLevel
   *
   * @remarks The class takes no keyed constructor of its setters, as Java's
   * class has a constructor of its own. Set the timeout with
   * `timeoutMs()`.
   */
  constructor(options: {
    /** The isolation level of the offsets to list; null is stored, as Java stores it. */
    isolationLevel: IsolationLevel | null;
  });
  constructor(options?: unknown) {
    super();
    if (CONSTRUCTOR_FORMS.match(options, arguments.length) === 0) return;
    const v = CONSTRUCTOR_FORMS.value(options, 'isolationLevel');
    if (v !== null && !(v instanceof IsolationLevel)) throw new TypeError(`isolationLevel must be an IsolationLevel, not ${kind(v)}`);
    this.#isolationLevel = v;
  }

  /**
   * The isolation level of the offsets to list.
   *
   * @returns the isolation level
   * @throws `TypeError` when it is given an argument: it only reads
   *
   * @remarks *(deviation)* `listOffsets` with a null isolation level throws
   * `TypeError` "ListOffsetsOptions.isolationLevel must not be null" at the
   * call, where Java fails only when it builds a request, and Java's
   * `MockAdminClient` never reads it. Why: the C API builds these options
   * from an isolation level singleton, which cannot be `NULL`. Goes when: the
   * FFI takes a null isolation level.
   */
  isolationLevel(): IsolationLevel | null {
    if (arguments.length !== 0) ISOLATION_LEVEL_FORMS.match(undefined, arguments.length);
    return this.#isolationLevel;
  }
}
