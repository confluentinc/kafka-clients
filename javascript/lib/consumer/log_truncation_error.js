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
// the Java source of `org.apache.kafka.clients.consumer.LogTruncationException`;
// `cargo xtask check-generated` checks it is up to date.

'use strict';

const _throwable = require('../_throwable');
const { OffsetOutOfRangeError } = require('./offset_out_of_range_error');

/**
 * In the event of an unclean leader election, the log will be truncated,
 * previously committed data will be lost, and new data will be written over
 * these offsets. When this happens, the consumer will detect the truncation and
 * raise this exception (if no automatic reset policy has been defined) with the
 * first offset known to diverge from what the consumer previously read.
 *
 * Java: `org.apache.kafka.clients.consumer.LogTruncationException`.
 */
class LogTruncationError extends OffsetOutOfRangeError {}

_throwable.setFfiId(LogTruncationError, -20); // kafka_common_Error_CONSUMER_LOG_TRUNCATION

module.exports = { LogTruncationError };
