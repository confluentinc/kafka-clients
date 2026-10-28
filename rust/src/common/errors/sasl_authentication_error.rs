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

//! Translated from `org.apache.kafka.common.errors.SaslAuthenticationException`.

use crate::common::error::kafka_error_type;
use crate::common::protocol::Errors;

kafka_error_type! {
    /// This error indicates that SASL authentication has failed. The error message in the error
    /// indicates the actual cause of failure.
    ///
    /// SASL authentication failures typically indicate invalid credentials, but could also include
    /// other failures specific to the SASL mechanism used for authentication.
    ///
    /// **Note:**If `SaslServer::evaluateResponse(byte[])` throws this error during authentication,
    /// the message from the error will be sent to clients in the SaslAuthenticate response. Custom
    /// `SaslServer` implementations may throw this error in order to provide custom error messages
    /// to clients, but should take care not to include any security-critical information in the
    /// message that should not be leaked to unauthenticated clients.
    ///
    /// # Translation notes
    ///
    /// Error code `Errors::SaslAuthenticationFailed`. Java `extends` chain:
    /// `SaslAuthenticationException` -> `AuthenticationException` ->
    /// `InvalidConfigurationException` -> `ApiException` -> `KafkaException`.
    #[doc(alias = "org.apache.kafka.common.errors.SaslAuthenticationException")]
    SaslAuthenticationError,
    code: Errors::SaslAuthenticationFailed,
    extends: [
        is_kafka_error,
        is_api_error,
        is_invalid_configuration_error,
        is_authentication_error,
        is_sasl_authentication_error,
    ],
}
