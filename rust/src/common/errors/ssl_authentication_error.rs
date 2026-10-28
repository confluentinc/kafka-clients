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

use crate::common::error::kafka_error_type;
use crate::common::protocol::Errors;

kafka_error_type! {
    /// This error indicates that SSL handshake has failed. See `source()` for the `SSLException`
    /// that caused this failure.
    ///
    /// SSL handshake failures in clients may indicate client authentication failure due to
    /// untrusted certificates if server is configured to request client certificates. Handshake
    /// failures could also indicate misconfigured security including protocol/cipher suite
    /// mismatch, server certificate authentication failure or server host name verification failure.
    ///
    /// # Translation notes
    ///
    /// Error code `Errors::InvalidConfig`, inherited from `InvalidConfigurationException`; that
    /// code's own class stays `InvalidConfigurationError`.
    #[doc(alias = "org.apache.kafka.common.errors.SslAuthenticationException")]
    SslAuthenticationError,
    code: Errors::InvalidConfig,
    extends: [
        is_kafka_error,
        is_api_error,
        is_invalid_configuration_error,
        is_authentication_error,
        is_ssl_authentication_error,
    ],
}
