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

//! Translated from `org.apache.kafka.common.errors.SerializationException`.

use crate::common::kafka_error::kafka_error_class;

kafka_error_class! {
    /// Any exception during serialization in the producer, or deserialization
    /// in the consumer.
    ///
    /// Corresponds to Java's `SerializationException`. It has no entry in
    /// `Errors`, so it carries no protocol code.
    ///
    /// Java `extends` chain:
    ///    `SerializationException` -> `KafkaException`
    ///
    /// Note it bypasses `ApiException`, which is why
    /// [`is_kafka_error`](crate::common::Error::is_kafka_error) and
    /// [`is_api_error`](crate::common::Error::is_api_error) disagree on it. It is
    /// itself an intermediate class; its subclass is
    /// `RecordDeserializationException`.
    SerializationError,
    extends: [
        is_kafka_error,
        is_serialization_error,
    ],
}
