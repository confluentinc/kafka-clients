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

//! Client software name and version information.
//!
//! Translated from `org.apache.kafka.common.network.ClientInformation`.

use std::fmt;

/// Client software name and version information.
///
/// Empty names and versions are replaced with [`ClientInformation::CLIENT_INFORMATION_UNKNOWN_NAME_OR_VERSION`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientInformation {
    software_name: String,
    software_version: String,
}

impl ClientInformation {
    /// The value used when the client software name or version is unknown.
    pub const CLIENT_INFORMATION_UNKNOWN_NAME_OR_VERSION: &str = "unknown";

    /// Creates a new `ClientInformation` with the given software name and version.
    ///
    /// Empty strings are replaced with [`Self::CLIENT_INFORMATION_UNKNOWN_NAME_OR_VERSION`].
    pub fn new(software_name: &str, software_version: &str) -> Self {
        Self {
            software_name: if software_name.is_empty() {
                ClientInformation::CLIENT_INFORMATION_UNKNOWN_NAME_OR_VERSION.to_string()
            } else {
                software_name.to_string()
            },
            software_version: if software_version.is_empty() {
                ClientInformation::CLIENT_INFORMATION_UNKNOWN_NAME_OR_VERSION.to_string()
            } else {
                software_version.to_string()
            },
        }
    }

    /// Returns an empty `ClientInformation` with unknown name and version.
    pub fn empty() -> Self {
        Self::new(
            ClientInformation::CLIENT_INFORMATION_UNKNOWN_NAME_OR_VERSION,
            ClientInformation::CLIENT_INFORMATION_UNKNOWN_NAME_OR_VERSION,
        )
    }

    /// Returns the software name.
    pub fn software_name(&self) -> &str {
        &self.software_name
    }

    /// Returns the software version.
    pub fn software_version(&self) -> &str {
        &self.software_version
    }
}

impl fmt::Display for ClientInformation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ClientInformation(softwareName={}, softwareVersion={})",
            self.software_name, self.software_version
        )
    }
}
