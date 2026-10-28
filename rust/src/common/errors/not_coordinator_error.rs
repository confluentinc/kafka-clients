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

//! Translated from `org.apache.kafka.common.errors.NotCoordinatorException`.

use crate::common::error::kafka_error_type;
use crate::common::protocol::Errors;

kafka_error_type! {
    /// In the context of the group coordinator, the broker returns this error code if it receives
    /// an offset fetch or commit request for a group it's not the coordinator of.
    ///
    /// In the context of the transactional coordinator, it returns this error when it receives a
    /// transactional request with a transactionalId the coordinator doesn't own.
    ///
    /// # Translation notes
    ///
    /// Error code `Errors::NotCoordinator`. Java `extends` chain: `NotCoordinatorException` ->
    /// `RefreshRetriableException` -> `RetriableException` -> `ApiException` -> `KafkaException`.
    #[doc(alias = "org.apache.kafka.common.errors.NotCoordinatorException")]
    NotCoordinatorError,
    code: Errors::NotCoordinator,
    extends: [
        is_kafka_error,
        is_api_error,
        is_retriable_error,
        is_refresh_retriable_error,
        is_not_coordinator_error,
    ],
}
