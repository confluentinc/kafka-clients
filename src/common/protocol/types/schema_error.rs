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

//! Translated from `org.apache.kafka.common.protocol.types.SchemaException`.

use crate::common::kafka_error::kafka_error_class;

kafka_error_class! {
    /// Raised if the protocol schema validation fails while parsing a request or
    /// response.
    ///
    /// Corresponds to Java's `SchemaException`. It has no entry in `Errors`, so
    /// it carries no protocol code — Java's `Errors.forException` walk finds
    /// none on `KafkaException` either and falls through to
    /// `UNKNOWN_SERVER_ERROR`.
    ///
    /// Java `extends` chain:
    ///    `SchemaException` -> `KafkaException`
    ///
    /// Like `SerializationException` it bypasses `ApiException`, so
    /// [`is_kafka_error`](crate::common::Error::is_kafka_error) is `true` while
    /// [`is_api_error`](crate::common::Error::is_api_error) is `false`.
    ///
    /// `NetworkClient.parseResponse` (`NetworkClient.java:824-840`) is the only
    /// site in the client that raises it: a buffer underflow while parsing a
    /// response, and a correlation-id mismatch on a response that is unrelated
    /// to a SASL request.
    SchemaError,
    extends: [
        is_kafka_error,
    ],
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Error;
    use crate::common::protocol::Errors;

    /// `SchemaException extends KafkaException` and stops there — it bypasses
    /// `ApiException`, so a `catch (ApiException)` does not see it while a
    /// `catch (KafkaException)` does.
    #[test]
    fn is_a_kafka_error_but_not_an_api_error() {
        let error = Error::schema("Array size -1 cannot be negative");
        assert!(error.is_kafka_error());
        assert!(!error.is_api_error());
        assert!(!error.is_retriable_error());
        assert!(!error.is_serialization_error());
        assert!(!crate::common::requests::request_utils::is_fatal_error(&error));
        assert_eq!(error.message(), "Array size -1 cannot be negative");
        assert_eq!(error.to_string(), "SchemaError: Array size -1 cannot be negative");
        // No entry in `Errors.java`, and `KafkaException` has none either, so
        // Java's `Errors.forException` walk falls through to UNKNOWN_SERVER_ERROR.
        assert_eq!(error.error(), Errors::UnknownServerError);
    }

    /// Java's `SchemaException(String, Throwable)` — the constructor
    /// `NetworkClient.parseResponse` uses for the buffer-underflow clause.
    #[test]
    fn carries_its_cause() {
        let error = SchemaError::new_source("outer", Error::local_illegal_state("inner"));
        assert_eq!(error.message(), "outer");
        let cause = error.source().expect("the cause must be carried");
        assert_eq!(cause.message(), "inner");
    }
}
