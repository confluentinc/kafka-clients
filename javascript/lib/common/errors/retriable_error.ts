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
// the Java source of `org.apache.kafka.common.errors.RetriableException`;
// `cargo xtask check-generated` checks it is up to date.

import { ApiError } from './api_error';

/**
 * A retriable exception is a transient exception that if retried may succeed.
 *
 * Java: `org.apache.kafka.common.errors.RetriableException`.
 *
 * A catch-only base: constructing it throws `TypeError`.
 */
export class RetriableError extends ApiError {
  /**
   * A catch-only base: constructing it directly throws `TypeError`.
   *
   * @param message - the message a subclass passes on, Java's message
   * @param options - `cause`: the cause a subclass passes on, Java's cause
   * @throws `TypeError` when constructed directly
   */
  constructor(message?: string | null, options?: { cause?: unknown }) {
    if (new.target === RetriableError) {
      throw new TypeError('RetriableError is an abstract catch-only base; it is never thrown directly');
    }
    super(message, options);
  }
}
