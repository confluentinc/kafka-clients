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
// the Java source of `org.apache.kafka.common.errors.KafkaStorageException`;
// `cargo xtask check-generated` checks it is up to date.

'use strict';

const _throwable = require('../../_throwable');
const { InvalidMetadataError } = require('./invalid_metadata_error');

/**
 * Miscellaneous disk-related IOException occurred when handling a request.
 * Client should request metadata update and retry if the response shows
 * KafkaStorageException
 *
 * Here are the guidelines on how to handle KafkaStorageException and
 * IOException:
 *
 * 1) If the server has not finished loading logs, IOException does not need to
 * be converted to KafkaStorageException 2) After the server has finished
 * loading logs, IOException should be caught and trigger
 * LogDirFailureChannel.maybeAddOfflineLogDir() Then the IOException should
 * either be swallowed and logged, or be converted and re-thrown as
 * KafkaStorageException 3) It is preferred for IOException to be caught in Log
 * rather than in ReplicaManager or LogSegment.
 *
 * Java: `org.apache.kafka.common.errors.KafkaStorageException`.
 */
class KafkaStorageError extends InvalidMetadataError {}

_throwable.setFfiId(KafkaStorageError, 56); // kafka_common_Error_KAFKA_STORAGE_ERROR

module.exports = { KafkaStorageError };
