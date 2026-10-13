// Copyright 2026 Confluent Inc.
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

//! The bootstrap DNS resolution settings `NetworkClient` needs (KIP-909).
//!
//! Translated from `org.apache.kafka.clients.BootstrapConfiguration`.

use crate::common::Error;
use crate::{ClientDnsLookup, ClientUtils};

/// The bootstrap servers and DNS resolution budget `NetworkClient` uses to
/// resolve `bootstrap.servers` asynchronously, or [`Self::DISABLED`] when the
/// client resolved them synchronously at construction
/// (`bootstrap.resolve.timeout.ms=0`, the default).
///
/// Translated from `org.apache.kafka.clients.BootstrapConfiguration` (KIP-909).
/// Java's `public final` fields become crate-private fields: the class is not
/// `@InterfaceAudience.Public`, so the type is crate-private (CLAUDE.md §2).
#[derive(Clone, Debug, PartialEq, Eq)]
#[doc(alias = "org.apache.kafka.clients.BootstrapConfiguration")]
pub(crate) struct BootstrapConfiguration {
    pub(crate) bootstrap_servers: Vec<String>,
    /// `None` only for [`Self::DISABLED`], whose `clientDnsLookup` is `null` in Java.
    pub(crate) client_dns_lookup: Option<ClientDnsLookup>,
    pub(crate) bootstrap_resolve_timeout_ms: i64,
    pub(crate) retry_backoff_ms: i64,
}

impl BootstrapConfiguration {
    /// Asynchronous bootstrap resolution is off: the client primed its metadata
    /// synchronously at construction.
    ///
    /// Java compares against this instance by identity
    /// (`bootstrapConfiguration != BootstrapConfiguration.DISABLED`); Rust
    /// compares by value through [`Self::is_disabled`], which is equivalent
    /// because [`Self::enabled`] always carries a `client_dns_lookup`.
    #[doc(alias = "org.apache.kafka.clients.BootstrapConfiguration#DISABLED")]
    pub(crate) const DISABLED: BootstrapConfiguration = BootstrapConfiguration {
        bootstrap_servers: Vec::new(),
        client_dns_lookup: None,
        bootstrap_resolve_timeout_ms: 0,
        retry_backoff_ms: 0,
    };

    /// Java's `private BootstrapConfiguration(...)` constructor.
    #[doc(alias = "org.apache.kafka.clients.BootstrapConfiguration#BootstrapConfiguration")]
    fn new(
        bootstrap_servers: Vec<String>,
        client_dns_lookup: Option<ClientDnsLookup>,
        bootstrap_resolve_timeout_ms: i64,
        retry_backoff_ms: i64,
    ) -> Self {
        Self {
            bootstrap_servers,
            client_dns_lookup,
            bootstrap_resolve_timeout_ms,
            retry_backoff_ms,
        }
    }

    /// An enabled configuration: `NetworkClient` resolves `bootstrap_servers`
    /// asynchronously, retrying every `retry_backoff_ms`, for at most
    /// `bootstrap_resolve_timeout_ms`.
    ///
    /// Every URL is validated up front, without resolving it, so a malformed
    /// `bootstrap.servers` still fails construction.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] with Java's text: `"Invalid url in bootstrap.servers: <url>"`
    /// when the URL does not match `HOST_PORT_PATTERN`, `"Invalid port in
    /// bootstrap.servers: <url>"` when its port does not fit an `int`
    /// (KIP-909 follow-up 0720ba1141).
    #[doc(alias = "org.apache.kafka.clients.BootstrapConfiguration#enabled")]
    pub(crate) fn enabled(
        bootstrap_servers: &[String],
        client_dns_lookup: ClientDnsLookup,
        bootstrap_resolve_timeout_ms: i64,
        retry_backoff_ms: i64,
    ) -> Result<Self, Error> {
        for url in bootstrap_servers {
            ClientUtils::validate_url(url)?;
        }
        Ok(Self::new(
            bootstrap_servers.to_vec(),
            Some(client_dns_lookup),
            bootstrap_resolve_timeout_ms,
            retry_backoff_ms,
        ))
    }

    /// `this == BootstrapConfiguration.DISABLED`.
    pub(crate) fn is_disabled(&self) -> bool {
        *self == Self::DISABLED
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn urls(urls: &[&str]) -> Vec<String> {
        urls.iter().map(|u| u.to_string()).collect()
    }

    #[test]
    fn test_enabled_keeps_its_settings() {
        let config = BootstrapConfiguration::enabled(
            &urls(&["127.0.0.1:8000", "unresolvable.invalid:9092"]),
            ClientDnsLookup::UseAllDnsIps,
            5000,
            100,
        )
        .unwrap();
        assert!(!config.is_disabled());
        assert_eq!(config.bootstrap_servers, urls(&["127.0.0.1:8000", "unresolvable.invalid:9092"]));
        assert_eq!(config.client_dns_lookup, Some(ClientDnsLookup::UseAllDnsIps));
        assert_eq!(config.bootstrap_resolve_timeout_ms, 5000);
        assert_eq!(config.retry_backoff_ms, 100);
        assert!(BootstrapConfiguration::DISABLED.is_disabled());
    }

    /// `enabled` validates the URLs without resolving them: an unresolvable
    /// host is accepted, a malformed URL is "Invalid url" and a port that does
    /// not fit an `int` is "Invalid port" (0720ba1141).
    #[test]
    fn test_enabled_validates_urls() {
        let error = |url: &str| match BootstrapConfiguration::enabled(
            &urls(&[url]),
            ClientDnsLookup::UseAllDnsIps,
            1000,
            100,
        ) {
            Err(Error::Config(e)) => e.message().to_string(),
            other => panic!("expected a ConfigError for {url:?}, got {other:?}"),
        };
        assert_eq!(error("localhost"), "Invalid url in bootstrap.servers: localhost");
        assert_eq!(error("local host:9092"), "Invalid url in bootstrap.servers: local host:9092");
        assert_eq!(
            error("localhost:99999999999"),
            "Invalid port in bootstrap.servers: localhost:99999999999"
        );
        // Out of the 0-65535 range but an `int`: `Utils.getPort` accepts it.
        assert!(
            BootstrapConfiguration::enabled(&urls(&["localhost:70000"]), ClientDnsLookup::UseAllDnsIps, 1000, 100)
                .is_ok()
        );
    }
}
