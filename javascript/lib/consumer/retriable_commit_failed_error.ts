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
// the Java source of `org.apache.kafka.clients.consumer.RetriableCommitFailedException`;
// `cargo xtask check-generated` checks it is up to date.

import { setFfiId } from '../_throwable';
import { RetriableError } from '../common/errors/retriable_error';

/**
 * Exception thrown when an offset commit fails with a retriable error. This
 * exception is generated on the client side upon receiving retriable error
 * codes from the Group Coordinator in a commit response.
 *
 * Unlike `CommitFailedException`, this exception indicates that the commit can
 * be retried. The consumer should attempt to commit the offsets again.
 *
 * Java: `org.apache.kafka.clients.consumer.RetriableCommitFailedException`.
 */
export class RetriableCommitFailedError extends RetriableError {}

setFfiId(RetriableCommitFailedError, -23); // kafka_common_Error_CONSUMER_RETRIABLE_COMMIT_FAILED
