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

//! Translated from `org.apache.kafka.common.errors.PositionOutOfRangeException`.

use crate::common::Errors;
use crate::common::error::kafka_error_class;

kafka_error_class! {
    /// Requested position is not greater than or equal to zero, and less than the
    /// size of the snapshot.
    ///
    /// Corresponds to Java's `PositionOutOfRangeException`, error code [`Errors::PositionOutOfRange`].
    ///
    /// Java `extends` chain:
    ///    `PositionOutOfRangeException` -> `ApiException` -> `KafkaException`
    PositionOutOfRangeError,
    code: Errors::PositionOutOfRange,
    extends: [
        is_kafka_error,
        is_api_error,
    ],
}
