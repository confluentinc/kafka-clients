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

//! Translated from `org.apache.kafka.common.errors.TransactionCoordinatorFencedException`.

use crate::common::Errors;
use crate::common::error::kafka_error_class;

kafka_error_class! {
    /// Indicates that the transaction coordinator sending a WriteTxnMarker is no
    /// longer the current coordinator for a given producer.
    ///
    /// Corresponds to Java's `TransactionCoordinatorFencedException`, error code [`Errors::TransactionCoordinatorFenced`].
    ///
    /// Java `extends` chain:
    ///    `TransactionCoordinatorFencedException` -> `ApiException` ->
    ///   `KafkaException`
    TransactionCoordinatorFencedError,
    code: Errors::TransactionCoordinatorFenced,
    extends: [
        is_kafka_error,
        is_api_error,
    ],
}
