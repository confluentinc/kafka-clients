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
// the Java source of `org.apache.kafka.common.requests.CorrelationIdMismatchException`;
// `cargo xtask check-generated` checks it is up to date.

'use strict';

const _throwable = require('../../_throwable');
const { IllegalStateError } = require('../../illegal_state_error');

/**
 * Raised if the correlationId in a response header does not match the expected
 * value from the request header.
 *
 * Java: `org.apache.kafka.common.requests.CorrelationIdMismatchException`.
 */
class CorrelationIdMismatchError extends IllegalStateError {}

_throwable.setFfiId(CorrelationIdMismatchError, -24); // kafka_common_Error_CORRELATION_ID_MISMATCH

module.exports = { CorrelationIdMismatchError };
