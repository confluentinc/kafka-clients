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

//! Channel metadata registry for collecting cipher and client information.
//!
//! Translated from `org.apache.kafka.common.network.ChannelMetadataRegistry`.
//!
//! Metadata about a channel is provided in various places in the network stack.
//! This registry is used as a common place to collect them.

use super::cipher_information::CipherInformation;
use super::client_information::ClientInformation;

/// A registry for collecting channel metadata such as cipher and client information.
///
/// Translated from the Java `ChannelMetadataRegistry` interface.
pub trait ChannelMetadataRegistry: Send {
    /// Register information about the SSL cipher we are using.
    /// Re-registering the information will overwrite the previous one.
    fn register_cipher_information(&mut self, cipher_information: CipherInformation);

    /// Get the currently registered cipher information.
    fn cipher_information(&self) -> Option<&CipherInformation>;

    /// Register information about the client we are using.
    /// Depending on the clients, the ApiVersionsRequest could be received
    /// multiple times or not at all. Re-registering the information will
    /// overwrite the previous one.
    fn register_client_information(&mut self, client_information: ClientInformation);

    /// Get the currently registered client information.
    fn client_information(&self) -> Option<&ClientInformation>;

    /// Unregister everything that has been registered and close the registry.
    fn close(&mut self);
}

/// Default implementation of [`ChannelMetadataRegistry`] that simply stores values.
///
/// Translated from `org.apache.kafka.common.network.DefaultChannelMetadataRegistry`
/// (test support class in Java).
///
/// Metrics integration is deferred; this implementation stores values without
/// recording metric events.
pub struct DefaultChannelMetadataRegistry {
    cipher_information: Option<CipherInformation>,
    client_information: Option<ClientInformation>,
}

impl DefaultChannelMetadataRegistry {
    /// Creates a new empty `DefaultChannelMetadataRegistry`.
    pub fn new() -> Self {
        Self { cipher_information: None, client_information: None }
    }
}

impl Default for DefaultChannelMetadataRegistry {
    fn default() -> Self {
        Self::new()
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
