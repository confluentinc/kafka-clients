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
// the Java source of `org.apache.kafka.common.errors.InvalidTxnTimeoutException`;
// `cargo xtask check-generated` checks it is up to date.

import { setFfiId } from '../../_throwable';
import { ApiError } from './api_error';

/**
 * The transaction coordinator returns this error code if the timeout received
 * via the InitProducerIdRequest is larger than the `transaction.max.timeout.ms`
 * config value.
 *
 * Java: `org.apache.kafka.common.errors.InvalidTxnTimeoutException`.
 */
export class InvalidTxnTimeoutError extends ApiError {}

setFfiId(InvalidTxnTimeoutError, 50); // kafka_common_Error_INVALID_TRANSACTION_TIMEOUT
