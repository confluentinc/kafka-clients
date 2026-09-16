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

//! Translated from `org.apache.kafka.common.errors.UnknownProducerIdException`.

use crate::common::kafka_error::kafka_error_class;
use crate::common::protocol::Errors;

kafka_error_class! {
    /// This exception is raised by the broker if it could not locate the producer
    /// metadata associated with the producerId in question. This could happen if,
    /// for instance, the producer's records were deleted because their retention
    /// time had elapsed. Once the last records of the producerId are removed, the
    /// producer's metadata is removed from the broker, and future appends by the
    /// producer will return this exception.
    ///
    /// Corresponds to Java's `UnknownProducerIdException`, error code [`Errors::UnknownProducerId`].
    ///
    /// Java `extends` chain:
    ///    `UnknownProducerIdException` -> `OutOfOrderSequenceException` ->
    ///   `ApiException` -> `KafkaException`
    UnknownProducerIdError,
    code: Errors::UnknownProducerId,
    extends: [
        is_kafka_error,
        is_api_error,
        is_out_of_order_sequence_error,
    ],
}
