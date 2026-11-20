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

// GENERATED, DO NOT EDIT. Produced by `cargo xtask generate-error-codes` from
// Java's `org.apache.kafka.common.config.LogLevelConfig`;
// `cargo xtask check-generated` checks it is up to date.

import { readOnlySet } from '../../internal/read_only_set';

/**
 * The constants of Java's `org.apache.kafka.common.config.LogLevelConfig`:
 * every `public static final` field, evaluated from the Java source, is a
 * static property of the same name; a deprecated one is left out. A `List`
 * constant is a frozen array and a `Set` constant a read-only set whose `add`,
 * `delete` and `clear` throw `UnsupportedOperationError`, both in Java's order.
 *
 * Java: `org.apache.kafka.common.config.LogLevelConfig`.
 */
export class LogLevelConfig {
  /**
   * Java's `LogLevelConfig.FATAL_LOG_LEVEL`.
   */
  static readonly FATAL_LOG_LEVEL: string = 'FATAL';
  /**
   * Java's `LogLevelConfig.ERROR_LOG_LEVEL`.
   */
  static readonly ERROR_LOG_LEVEL: string = 'ERROR';
  /**
   * Java's `LogLevelConfig.WARN_LOG_LEVEL`.
   */
  static readonly WARN_LOG_LEVEL: string = 'WARN';
  /**
   * Java's `LogLevelConfig.INFO_LOG_LEVEL`.
   */
  static readonly INFO_LOG_LEVEL: string = 'INFO';
  /**
   * Java's `LogLevelConfig.DEBUG_LOG_LEVEL`.
   */
  static readonly DEBUG_LOG_LEVEL: string = 'DEBUG';
  /**
   * Java's `LogLevelConfig.TRACE_LOG_LEVEL`.
   */
  static readonly TRACE_LOG_LEVEL: string = 'TRACE';
  /**
   * Java's `LogLevelConfig.VALID_LOG_LEVELS`.
   */
  static readonly VALID_LOG_LEVELS: ReadonlySet<string> = readOnlySet(['FATAL', 'ERROR', 'WARN', 'INFO', 'DEBUG', 'TRACE']);

  /**
   * Java's default constructor. The class holds only its constants, which are static.
   */
  constructor() {}
}
