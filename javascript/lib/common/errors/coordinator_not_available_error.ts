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
// the Java source of `org.apache.kafka.common.errors.CoordinatorNotAvailableException`;
// `cargo xtask check-generated` checks it is up to date.

import { setFfiId } from '../../_throwable';
import { RefreshRetriableError } from './refresh_retriable_error';

/**
 * In the context of the group coordinator, the broker returns this error code
 * for metadata or offset commit requests if the group metadata topic has not
 * been created yet.
 *
 * In the context of the transactional coordinator, this error will be returned
 * if the underlying transactional log is under replicated or if an append to
 * the log times out.
 *
 * Java: `org.apache.kafka.common.errors.CoordinatorNotAvailableException`.
 */
export class CoordinatorNotAvailableError extends RefreshRetriableError {}

setFfiId(CoordinatorNotAvailableError, 15); // kafka_common_Error_COORDINATOR_NOT_AVAILABLE
