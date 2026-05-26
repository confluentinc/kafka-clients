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

//! Translation of `org.apache.kafka.common.network.ClientInformation`.

/// Mirrors `ClientInformation.UNKNOWN_NAME_OR_VERSION`.
pub const UNKNOWN_NAME_OR_VERSION: &str = "unknown";

/// Software name and version reported by an `ApiVersionsRequest`.
/// Mirrors the Java `ClientInformation` value type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientInformation {
    software_name: String,
    software_version: String,
}

impl ClientInformation {
    /// Construct from explicit name/version. Empty inputs are normalised
    /// to the literal `"unknown"`, matching Java.
    pub fn new(software_name: impl Into<String>, software_version: impl Into<String>) -> Self {
        let software_name = software_name.into();
        let software_version = software_version.into();
        ClientInformation {
            software_name: if software_name.is_empty() {
                UNKNOWN_NAME_OR_VERSION.to_owned()
            } else {
                software_name
            },
            software_version: if software_version.is_empty() {
                UNKNOWN_NAME_OR_VERSION.to_owned()
            } else {
                software_version
            },
        }
    }

    /// Mirrors `ClientInformation.EMPTY`.
    pub fn empty() -> Self {
        ClientInformation {
            software_name: UNKNOWN_NAME_OR_VERSION.to_owned(),
            software_version: UNKNOWN_NAME_OR_VERSION.to_owned(),
        }
    }

    /// Mirrors `ClientInformation.softwareName()`.
    pub fn software_name(&self) -> &str {
        &self.software_name
    }

    /// Mirrors `ClientInformation.softwareVersion()`.
    pub fn software_version(&self) -> &str {
        &self.software_version
    }
}

impl std::fmt::Display for ClientInformation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ClientInformation(softwareName={}, softwareVersion={})",
            self.software_name, self.software_version
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn populated_inputs_are_preserved() {
        let info = ClientInformation::new("apache-kafka-java", "4.2.0");
        assert_eq!(info.software_name(), "apache-kafka-java");
        assert_eq!(info.software_version(), "4.2.0");
    }

    #[test]
    fn empty_inputs_become_unknown() {
        let info = ClientInformation::new("", "");
        assert_eq!(info.software_name(), "unknown");
        assert_eq!(info.software_version(), "unknown");
    }

    #[test]
    fn empty_constant() {
        let empty = ClientInformation::empty();
        assert_eq!(empty.software_name(), "unknown");
        assert_eq!(empty.software_version(), "unknown");
    }

    #[test]
    fn equality_and_hash() {
        use std::collections::HashSet;
        let a = ClientInformation::new("n", "v");
        let b = ClientInformation::new("n", "v");
        let c = ClientInformation::new("n", "x");
        assert_eq!(a, b);
        assert_ne!(a, c);
        let mut set = HashSet::new();
        set.insert(a);
        assert!(set.contains(&b));
    }

    #[test]
    fn display_format() {
        let info = ClientInformation::new("kafka", "4.0.0");
        assert_eq!(info.to_string(), "ClientInformation(softwareName=kafka, softwareVersion=4.0.0)");
    }
}
