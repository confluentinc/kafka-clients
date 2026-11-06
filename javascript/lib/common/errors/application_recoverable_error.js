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
// the Java source of `org.apache.kafka.common.errors.ApplicationRecoverableException`;
// `cargo xtask check-generated` checks it is up to date.

'use strict';

const { ApiError } = require('./api_error');

/**
 * Indicates that the error is fatal to the producer, and the application needs
 * to restart the producer after handling the error. Depending on the
 * application, different recovery strategies (e.g., re-balancing task,
 * restoring from checkpoints) may be employed.
 *
 * Java: `org.apache.kafka.common.errors.ApplicationRecoverableException`.
 *
 * A catch-only base: constructing it throws `TypeError`.
 */
class ApplicationRecoverableError extends ApiError {
  /**
   * A catch-only base: constructing it directly throws `TypeError`.
   *
   * @param {string | null} [message]
   * @param {{cause?: unknown}} [options]
   */
  constructor(message, options) {
    if (new.target === ApplicationRecoverableError) {
      throw new TypeError('ApplicationRecoverableError is an abstract catch-only base; it is never thrown directly');
    }
    super(message, options);
  }
}

module.exports = { ApplicationRecoverableError };
