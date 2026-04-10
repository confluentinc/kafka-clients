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

//! SSL cipher and protocol information.
//!
//! Translated from `org.apache.kafka.common.network.CipherInformation`.

use std::fmt;

/// SSL cipher and protocol information for a connection.
///
/// Empty or missing values are replaced with "unknown".
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CipherInformation {
    cipher: String,
    protocol: String,
}

impl CipherInformation {
    /// Creates a new `CipherInformation` with the given cipher and protocol.
    ///
    /// Empty strings are replaced with "unknown".
    pub fn new(cipher: &str, protocol: &str) -> Self {
        Self {
            cipher: if cipher.is_empty() {
                "unknown".to_string()
            } else {
                cipher.to_string()
            },
            protocol: if protocol.is_empty() {
                "unknown".to_string()
            } else {
                protocol.to_string()
            },
        }
    }

    /// Returns the cipher name.
    pub fn cipher(&self) -> &str {
        &self.cipher
    }

    /// Returns the protocol name.
    pub fn protocol(&self) -> &str {
        &self.protocol
    }
}

impl fmt::Display for CipherInformation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CipherInformation(cipher={}, protocol={})", self.cipher, self.protocol)
    }
}
