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

#![allow(dead_code)]
//! Factory for creating the appropriate `ChannelBuilder` based on `SecurityProtocol`.
//!
//! Translated from `org.apache.kafka.common.network.ChannelBuilders.clientChannelBuilder()`
//! and the private `create()` method (Java).
//!
//! In Java, `ChannelBuilders` switches on `SecurityProtocol` to instantiate the
//! correct builder. In Rust, the channel builders already accept typed config
//! structs in their constructors, so there is no separate `configure()` step.
//!
//! Excluded Java code:
//! - `JaasContext` loading -- simplified to direct `SaslConfig` struct
//! - `channelBuilderConfigs()` -- Java's config extraction from `AbstractConfig` is not applicable
//! - `serverChannelBuilder()` -- server-side, out of scope
//! - `createPrincipalBuilder()` -- server-side, out of scope
//! - `requireNonNullMode()` -- replaced by Rust exhaustive match

use std::io;

use crate::common::config::{SaslConfig, SslConfig};
use crate::common::network::ChannelBuilder;
use crate::common::network::ListenerName;
use crate::common::network::PlaintextChannelBuilder;
use crate::common::network::SaslChannelBuilder;
use crate::common::network::SslChannelBuilder;
use crate::common::security::SecurityProtocol;
use crate::common::security::SslFactory;
use crate::common::utils::LogContext;

/// Translates the Java static-utility class `org.apache.kafka.common.network.ChannelBuilders`,
/// which has no instance state, so it becomes a unit struct hosting its
/// statics as associated items.
pub struct ChannelBuilders;

impl ChannelBuilders {
    /// Creates a client-side `ChannelBuilder` for the given security protocol.
    ///
    /// Translated from `ChannelBuilders.clientChannelBuilder()` (Java).
    ///
    /// # Arguments
    ///
    /// * `security_protocol` - The security protocol to use
    /// * `ssl_config` - Required for `SSL` and `SASL_SSL` protocols
    /// * `sasl_config` - Required for `SASL_PLAINTEXT` and `SASL_SSL` protocols
    /// * `listener_name` - Optional listener name (server-side only, `None` for clients)
    /// * `client_id` - The Kafka client ID
    /// * `log_context` - Contextual log prefix
    ///
    /// # Errors
    ///
    /// Returns an error if required configs are missing for the given protocol.
    pub fn client_channel_builder(
        security_protocol: SecurityProtocol,
        ssl_config: Option<&SslConfig>,
        sasl_config: Option<&SaslConfig>,
        listener_name: Option<ListenerName>,
        client_id: &str,
        log_context: LogContext,
    ) -> io::Result<Box<dyn ChannelBuilder>> {
        match security_protocol {
            SecurityProtocol::Plaintext => Ok(Box::new(PlaintextChannelBuilder::new(listener_name))),
            SecurityProtocol::Ssl => {
                let ssl_config = ssl_config
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "SSL protocol requires ssl_config"))?;
                let ssl_factory = SslFactory::new(ssl_config)?;
                Ok(Box::new(SslChannelBuilder::new(ssl_factory, listener_name)))
            },
            SecurityProtocol::SaslPlaintext => {
                let sasl_config = sasl_config.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "SASL_PLAINTEXT protocol requires sasl_config")
                })?;
                Ok(Box::new(SaslChannelBuilder::new(
                    SecurityProtocol::SaslPlaintext,
                    sasl_config.clone(),
                    None,
                    listener_name,
                    client_id,
                    log_context,
                )?))
            },
            SecurityProtocol::SaslSsl => {
                let ssl_config = ssl_config.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "SASL_SSL protocol requires ssl_config")
                })?;
                let sasl_config = sasl_config.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "SASL_SSL protocol requires sasl_config")
                })?;
                let ssl_factory = SslFactory::new(ssl_config)?;
                Ok(Box::new(SaslChannelBuilder::new(
                    SecurityProtocol::SaslSsl,
                    sasl_config.clone(),
                    Some(ssl_factory),
                    listener_name,
                    client_id,
                    log_context,
                )?))
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plaintext_builder() {
        let result = ChannelBuilders::client_channel_builder(
            SecurityProtocol::Plaintext,
            None,
            None,
            None,
            "test",
            LogContext::empty(),
        );
        assert!(result.is_ok(), "Plaintext should not require any configs");
    }

    #[test]
    fn test_ssl_builder() {
        let ssl_config = SslConfig::default();
        let result = ChannelBuilders::client_channel_builder(
            SecurityProtocol::Ssl,
            Some(&ssl_config),
            None,
            None,
            "test",
            LogContext::empty(),
        );
        assert!(result.is_ok(), "SSL with valid config should succeed");
    }

    #[test]
    fn test_ssl_missing_config() {
        let result = ChannelBuilders::client_channel_builder(
            SecurityProtocol::Ssl,
            None,
            None,
            None,
            "test",
            LogContext::empty(),
        );
        let err = result.err().expect("Should return an error");
        assert!(
            err.to_string().contains("ssl_config"),
            "Error should mention ssl_config: {}",
            err
        );
    }

    #[test]
    fn test_sasl_plaintext_builder() {
        let sasl_config = SaslConfig {
            mechanism: "PLAIN".to_string(),
            username: Some("alice".to_string()),
            password: Some("secret".to_string()),
            ..SaslConfig::default()
        };
        let result = ChannelBuilders::client_channel_builder(
            SecurityProtocol::SaslPlaintext,
            None,
            Some(&sasl_config),
            None,
            "test",
            LogContext::empty(),
        );
        assert!(result.is_ok(), "SASL_PLAINTEXT with valid config should succeed");
    }

    #[test]
    fn test_sasl_ssl_builder() {
        let ssl_config = SslConfig::default();
        let sasl_config = SaslConfig {
            mechanism: "PLAIN".to_string(),
            username: Some("alice".to_string()),
            password: Some("secret".to_string()),
            ..SaslConfig::default()
        };
        let result = ChannelBuilders::client_channel_builder(
            SecurityProtocol::SaslSsl,
            Some(&ssl_config),
            Some(&sasl_config),
            None,
            "test",
            LogContext::empty(),
        );
        assert!(result.is_ok(), "SASL_SSL with both configs should succeed");
    }

    #[test]
    fn test_sasl_ssl_missing_ssl_config() {
        let sasl_config = SaslConfig {
            mechanism: "PLAIN".to_string(),
            username: Some("alice".to_string()),
            password: Some("secret".to_string()),
            ..SaslConfig::default()
        };
        let result = ChannelBuilders::client_channel_builder(
            SecurityProtocol::SaslSsl,
            None,
            Some(&sasl_config),
            None,
            "test",
            LogContext::empty(),
        );
        let err = result.err().expect("Should return an error");
        assert!(
            err.to_string().contains("ssl_config"),
            "Error should mention ssl_config: {}",
            err
        );
    }

    #[test]
    fn test_sasl_plaintext_missing_sasl_config() {
        let result = ChannelBuilders::client_channel_builder(
            SecurityProtocol::SaslPlaintext,
            None,
            None,
            None,
            "test",
            LogContext::empty(),
        );
        let err = result.err().expect("Should return an error");
        assert!(
            err.to_string().contains("sasl_config"),
            "Error should mention sasl_config: {}",
            err
        );
    }
}
