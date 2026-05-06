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

//! Translation of `org.apache.kafka.common.network.ChannelMetadataRegistry`
//! and the test-fixture `DefaultChannelMetadataRegistry`.

use super::{CipherInformation, ClientInformation};

/// Trait counterpart of the Java `ChannelMetadataRegistry` interface.
/// Mirrors the four register/get methods plus `close`. Java's
/// `Closeable.close()` is modelled as an explicit method on the trait;
/// implementors that hold no resources can use the default no-op.
pub trait ChannelMetadataRegistry {
    /// Register information about the SSL cipher we are using.
    /// Re-registering overwrites the previous value. Mirrors
    /// `registerCipherInformation`.
    fn register_cipher_information(&mut self, cipher_information: CipherInformation);

    /// Return the currently registered cipher information.
    fn cipher_information(&self) -> Option<&CipherInformation>;

    /// Register information about the client we are talking to.
    /// Mirrors `registerClientInformation`. Re-registering overwrites
    /// the previous value (Java accepts repeated `ApiVersionsRequest`s).
    fn register_client_information(&mut self, client_information: ClientInformation);

    /// Return the currently registered client information.
    fn client_information(&self) -> Option<&ClientInformation>;

    /// Unregister everything that has been registered and close the
    /// registry. Mirrors `close()`.
    fn close(&mut self);
}

/// Concrete in-memory `ChannelMetadataRegistry`. Mirrors the
/// `DefaultChannelMetadataRegistry` test fixture used by the Java client.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DefaultChannelMetadataRegistry {
    cipher_information: Option<CipherInformation>,
    client_information: Option<ClientInformation>,
}

impl DefaultChannelMetadataRegistry {
    /// Construct an empty registry.
    pub fn new() -> Self {
        Self::default()
    }
}

impl ChannelMetadataRegistry for DefaultChannelMetadataRegistry {
    fn register_cipher_information(&mut self, cipher_information: CipherInformation) {
        self.cipher_information = Some(cipher_information);
    }

    fn cipher_information(&self) -> Option<&CipherInformation> {
        self.cipher_information.as_ref()
    }

    fn register_client_information(&mut self, client_information: ClientInformation) {
        self.client_information = Some(client_information);
    }

    fn client_information(&self) -> Option<&ClientInformation> {
        self.client_information.as_ref()
    }

    fn close(&mut self) {
        self.cipher_information = None;
        self.client_information = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_registry_returns_none() {
        let reg = DefaultChannelMetadataRegistry::new();
        assert!(reg.cipher_information().is_none());
        assert!(reg.client_information().is_none());
    }

    #[test]
    fn register_and_overwrite_cipher() {
        let mut reg = DefaultChannelMetadataRegistry::new();
        reg.register_cipher_information(CipherInformation::new("c1", "TLSv1.3"));
        assert_eq!(reg.cipher_information().unwrap().cipher(), "c1");
        // Re-register overwrites.
        reg.register_cipher_information(CipherInformation::new("c2", "TLSv1.3"));
        assert_eq!(reg.cipher_information().unwrap().cipher(), "c2");
    }

    #[test]
    fn register_and_overwrite_client() {
        let mut reg = DefaultChannelMetadataRegistry::new();
        reg.register_client_information(ClientInformation::new("kafka", "4.0"));
        assert_eq!(reg.client_information().unwrap().software_name(), "kafka");
        // Repeated `ApiVersionsRequest` overwrites.
        reg.register_client_information(ClientInformation::new("kafka", "4.1"));
        assert_eq!(reg.client_information().unwrap().software_version(), "4.1");
    }

    #[test]
    fn close_clears_state() {
        let mut reg = DefaultChannelMetadataRegistry::new();
        reg.register_cipher_information(CipherInformation::new("c", "p"));
        reg.register_client_information(ClientInformation::new("n", "v"));
        reg.close();
        assert!(reg.cipher_information().is_none());
        assert!(reg.client_information().is_none());
    }
}
