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

//! Translated from `org.apache.kafka.common.errors.CoordinatorNotAvailableException`.

use crate::common::error::kafka_error_type;
use crate::common::protocol::Errors;

kafka_error_type! {
    /// In the context of the group coordinator, the broker returns this error code for metadata or
    /// offset commit requests if the group metadata topic has not been created yet.
    ///
    /// In the context of the transactional coordinator, this error will be returned if the
    /// underlying transactional log is under replicated or if an append to the log times out.
    ///
    /// # Translation notes
    ///
    /// Error code `Errors::CoordinatorNotAvailable`. Java `extends` chain:
    /// `CoordinatorNotAvailableException` -> `RefreshRetriableException` -> `RetriableException` ->
    /// `ApiException` -> `KafkaException`.
    #[doc(alias = "org.apache.kafka.common.errors.CoordinatorNotAvailableException")]
    CoordinatorNotAvailableError,
    code: Errors::CoordinatorNotAvailable,
    extends: [
        is_kafka_error,
        is_api_error,
        is_retriable_error,
        is_refresh_retriable_error,
        is_coordinator_not_available_error,
    ],
}
