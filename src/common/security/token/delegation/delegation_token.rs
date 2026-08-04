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

//! Delegation token.
//!
//! Corresponds to
//! `org.apache.kafka.common.security.token.delegation.DelegationToken`.

use std::fmt;

use crate::common::security::token::delegation::TokenInformation;

/// Standard Base64 alphabet (RFC 4648), matching `java.util.Base64.getEncoder()`.
const BASE64_ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Encodes bytes using the standard Base64 alphabet with padding, mirroring
/// `java.util.Base64.getEncoder().encodeToString`.
fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(BASE64_ALPHABET[((triple >> 18) & 0x3F) as usize] as char);
        out.push(BASE64_ALPHABET[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(BASE64_ALPHABET[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(BASE64_ALPHABET[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

/// A class representing a delegation token.
///
/// Corresponds to
/// `org.apache.kafka.common.security.token.delegation.DelegationToken`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DelegationToken {
    token_information: TokenInformation,
    hmac: Vec<u8>,
}

impl DelegationToken {
    /// Creates a delegation token from its information and HMAC bytes.
    pub fn new(token_information: TokenInformation, hmac: Vec<u8>) -> Self {
        Self { token_information, hmac }
    }

    /// Returns the token information.
    ///
    /// Mirrors `DelegationToken.tokenInfo`.
    pub fn token_info(&self) -> &TokenInformation {
        &self.token_information
    }

    /// Returns a mutable reference to the token information.
    ///
    /// Enables the mock's `renewDelegationToken`, which mutates the stored
    /// token's expiry timestamp (`token.tokenInfo().setExpiryTimestamp(...)`).
    pub fn token_info_mut(&mut self) -> &mut TokenInformation {
        &mut self.token_information
    }

    /// Returns the HMAC bytes.
    ///
    /// Mirrors `DelegationToken.hmac`.
    pub fn hmac(&self) -> &[u8] {
        &self.hmac
    }

    /// Returns the HMAC bytes as a standard Base64 string.
    ///
    /// Mirrors `DelegationToken.hmacAsBase64String`.
    pub fn hmac_as_base64_string(&self) -> String {
        base64_encode(&self.hmac)
    }
}

impl fmt::Display for DelegationToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Mirrors Java's `toString`, which redacts the HMAC.
        write!(
            f,
            "DelegationToken{{tokenInformation={}, hmac=[*******]}}",
            self.token_information
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::security::auth::KafkaPrincipal;

    fn token_info() -> TokenInformation {
        TokenInformation::new(
            "token-id",
            KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "alice"),
            vec![KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "bob")],
            1,
            100,
            50,
        )
    }

    #[test]
    fn accessors_return_components() {
        let token = DelegationToken::new(token_info(), b"hmac-bytes".to_vec());
        assert_eq!(token.token_info().token_id(), "token-id");
        assert_eq!(token.hmac(), b"hmac-bytes");
    }

    #[test]
    fn base64_matches_java_encoder() {
        // Known vectors from java.util.Base64.getEncoder().encodeToString.
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
        // Bytes exercising the '+' and '/' alphabet entries.
        assert_eq!(base64_encode(&[0xFB, 0xFF, 0xFE]), "+//+");
    }

    #[test]
    fn hmac_as_base64_string_uses_standard_encoder() {
        let token = DelegationToken::new(token_info(), b"foobar".to_vec());
        assert_eq!(token.hmac_as_base64_string(), "Zm9vYmFy");
    }

    #[test]
    fn display_redacts_hmac() {
        let token = DelegationToken::new(token_info(), b"secret".to_vec());
        let rendered = token.to_string();
        assert!(rendered.contains("hmac=[*******]"), "{rendered}");
        assert!(!rendered.contains("secret"), "{rendered}");
    }

    #[test]
    fn equals_considers_info_and_hmac() {
        let a = DelegationToken::new(token_info(), b"x".to_vec());
        let b = DelegationToken::new(token_info(), b"x".to_vec());
        let c = DelegationToken::new(token_info(), b"y".to_vec());
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
