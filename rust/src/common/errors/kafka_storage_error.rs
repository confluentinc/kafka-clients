// Copyright 2025 Confluent Inc.
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

//! Translated from `org.apache.kafka.common.errors.KafkaStorageException`.

use crate::common::error::kafka_error_type;
use crate::common::protocol::Errors;

kafka_error_type! {
    /// Miscellaneous disk-related `IOException` occurred when handling a request. Client should
    /// request metadata update and retry if the response shows `KafkaStorageError`
    ///
    /// Here are the guidelines on how to handle `KafkaStorageError` and `IOException`:
    ///
    /// 1. If the server has not finished loading logs, `IOException` does not need to be
    ///    converted to `KafkaStorageError`
    /// 2. After the server has finished loading logs, `IOException` should be caught and trigger
    ///    `LogDirFailureChannel.maybeAddOfflineLogDir()`. Then the `IOException` should either be
    ///    swallowed and logged, or be converted and re-thrown as `KafkaStorageError`
    /// 3. It is preferred for `IOException` to be caught in Log rather than in ReplicaManager or
    ///    LogSegment.
    ///
    /// # Translation notes
    ///
    /// Error code `Errors::KafkaStorageError`.
    #[doc(alias = "org.apache.kafka.common.errors.KafkaStorageException")]
    KafkaStorageError,
    code: Errors::KafkaStorageError,
    extends: [
        is_kafka_error,
        is_api_error,
        is_retriable_error,
        is_refresh_retriable_error,
        is_invalid_metadata_error,
        is_kafka_storage_error,
    ],
}
