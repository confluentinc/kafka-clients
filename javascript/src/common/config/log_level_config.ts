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
 * This class holds definitions for log level configurations related to Kafka's
 * application logging. See KIP-412 for additional information
 */
export class LogLevelConfig {
  /**
   * The `FATAL` level designates a very severe error that will lead the Kafka
   * broker to abort.
   */
  static readonly FATAL_LOG_LEVEL: string = 'FATAL';
  /**
   * The `ERROR` level designates error events that might still allow the broker
   * to continue running.
   */
  static readonly ERROR_LOG_LEVEL: string = 'ERROR';
  /**
   * The `WARN` level designates potentially harmful situations.
   */
  static readonly WARN_LOG_LEVEL: string = 'WARN';
  /**
   * The `INFO` level designates informational messages that highlight normal
   * Kafka events at a coarse-grained level
   */
  static readonly INFO_LOG_LEVEL: string = 'INFO';
  /**
   * The `DEBUG` level designates fine-grained informational events that are
   * most useful to debug Kafka
   */
  static readonly DEBUG_LOG_LEVEL: string = 'DEBUG';
  /**
   * The `TRACE` level designates finer-grained informational events than the
   * `DEBUG` level.
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
