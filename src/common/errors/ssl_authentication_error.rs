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

//! Translated from `org.apache.kafka.common.errors.SslAuthenticationException`.

use crate::common::kafka_error::kafka_error_class;

kafka_error_class! {
    /// The TLS handshake failed.
    ///
    /// Raised client-side, so it never arrives from a broker and carries no code.
    ///
    /// Corresponds to Java's `SslAuthenticationException`. It has no entry in `Errors`, so it
    /// carries no protocol code.
    ///
    /// Java `extends` chain:
    ///    `SslAuthenticationException` -> `AuthenticationException` ->
    ///   `InvalidConfigurationException` -> `ApiException` -> `KafkaException`
    SslAuthenticationError,
    extends: [
        is_kafka_error,
        is_api_error,
        is_invalid_configuration_error,
        is_authentication_error,
    ],
}
