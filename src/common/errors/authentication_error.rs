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

//! Translated from `org.apache.kafka.common.errors.AuthenticationException`.

use crate::common::kafka_error::kafka_error_class;

kafka_error_class! {
    /// Authentication failed.
    ///
    /// Not to be confused with [`common::network::AuthenticationError`]
    /// (crate::common::network::AuthenticationError), which has no Java
    /// counterpart and exists only to carry "this was a genuine authentication
    /// failure" across an `io::Error` boundary during the handshake.
    ///
    /// Corresponds to Java's `AuthenticationException`. It has no entry in `Errors`, so it
    /// carries no protocol code.
    ///
    /// Java `extends` chain:
    ///    `AuthenticationException` -> `InvalidConfigurationException` ->
    ///   `ApiException` -> `KafkaException`
    AuthenticationError,
    extends: [
        is_kafka_error,
        is_api_error,
        is_invalid_configuration_error,
        is_authentication_error,
    ],
}
