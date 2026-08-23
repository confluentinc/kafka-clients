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

//! Translated from `org.apache.kafka.common.errors.InvalidRequestException`.

use crate::common::kafka_error::kafka_error_class;
use crate::common::protocol::Errors;

kafka_error_class! {
    /// This most likely occurs because of a request being malformed by the client
    /// library or the message was sent to an incompatible broker. See the broker
    /// logs for more details.
    ///
    /// Corresponds to Java's `InvalidRequestException`, error code [`Errors::InvalidRequest`].
    ///
    /// Java `extends` chain:
    ///    `InvalidRequestException` -> `ApiException` -> `KafkaException`
    InvalidRequestError,
    code: Errors::InvalidRequest,
    extends: [
        is_kafka_error,
        is_api_error,
    ],
}
