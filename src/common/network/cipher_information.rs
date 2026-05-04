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

//! Translation of `org.apache.kafka.common.network.CipherInformation`.

const UNKNOWN: &str = "unknown";

/// Information about the SSL cipher and protocol negotiated for a
/// channel. Mirrors the Java `CipherInformation` value type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CipherInformation {
    cipher: String,
    protocol: String,
}

impl CipherInformation {
    /// Construct a new `CipherInformation`. Empty / null inputs are
    /// normalised to the literal `"unknown"`, matching Java.
    pub fn new(cipher: impl Into<String>, protocol: impl Into<String>) -> Self {
        let cipher = cipher.into();
        let protocol = protocol.into();
        CipherInformation {
            cipher: if cipher.is_empty() { UNKNOWN.to_owned() } else { cipher },
            protocol: if protocol.is_empty() { UNKNOWN.to_owned() } else { protocol },
        }
    }

    /// Mirrors `CipherInformation.cipher()`.
    pub fn cipher(&self) -> &str {
        &self.cipher
    }

    /// Mirrors `CipherInformation.protocol()`.
    pub fn protocol(&self) -> &str {
        &self.protocol
    }
}

impl std::fmt::Display for CipherInformation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CipherInformation(cipher={}, protocol={})", self.cipher, self.protocol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn populated_inputs_are_preserved() {
        let info = CipherInformation::new("TLS_AES_256_GCM_SHA384", "TLSv1.3");
        assert_eq!(info.cipher(), "TLS_AES_256_GCM_SHA384");
        assert_eq!(info.protocol(), "TLSv1.3");
    }

    #[test]
    fn empty_inputs_become_unknown() {
        let info = CipherInformation::new("", "");
        assert_eq!(info.cipher(), "unknown");
        assert_eq!(info.protocol(), "unknown");
    }

    #[test]
    fn equality_and_hash() {
        use std::collections::HashSet;
        let a = CipherInformation::new("c", "p");
        let b = CipherInformation::new("c", "p");
        let c = CipherInformation::new("d", "p");
        assert_eq!(a, b);
        assert_ne!(a, c);
        let mut set = HashSet::new();
        set.insert(a);
        assert!(set.contains(&b));
    }

    #[test]
    fn display_format() {
        let info = CipherInformation::new("c", "p");
        assert_eq!(info.to_string(), "CipherInformation(cipher=c, protocol=p)");
    }
}
