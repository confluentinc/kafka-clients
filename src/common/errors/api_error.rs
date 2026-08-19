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

//! Translated from `org.apache.kafka.common.errors.ApiException`.

use crate::common::kafka_error::kafka_error_class;

kafka_error_class! {
    /// Any exception the broker can report through the protocol.
    ///
    /// The concrete base of the API family: Java throws it directly when no more
    /// specific subclass applies. Being an intermediate class it is also the
    /// subject of [`is_api_error`](crate::common::Error::is_api_error).
    ///
    /// Corresponds to Java's `ApiException`. It has no entry in `Errors`, so it
    /// carries no protocol code.
    ///
    /// Java `extends` chain:
    ///    `ApiException` -> `KafkaException`
    ApiError,
    extends: [
        is_kafka_error,
        is_api_error,
    ],
}
