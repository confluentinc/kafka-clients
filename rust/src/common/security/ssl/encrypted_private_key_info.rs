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

//! A PKCS#8 `EncryptedPrivateKeyInfo` (RFC 5958 §3), decrypted with a password.
//!
//! Kafka decrypts the `ENCRYPTED PRIVATE KEY` of `ssl.keystore.key` with the JDK's
//! `javax.crypto.EncryptedPrivateKeyInfo` (`DefaultSslEngineFactory.PemStore.privateKey`):
//!
//! ```java
//! EncryptedPrivateKeyInfo keyInfo = new EncryptedPrivateKeyInfo(keyBytes);
//! String algorithm = keyInfo.getAlgName();
//! SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(algorithm);
//! SecretKey pbeKey = keyFactory.generateSecret(new PBEKeySpec(keyPassword));
//! Cipher cipher = Cipher.getInstance(algorithm);
//! cipher.init(Cipher.DECRYPT_MODE, pbeKey, keyInfo.getAlgParameters());
//! keySpec = keyInfo.getKeySpec(cipher);
//! ```
//!
//! Neither rustls nor `rustls-pki-types` has such a type, so it is written here from the
//! RFCs (RFC 8018 §6.2 PBES2 and §5.2 PBKDF2, RFC 5958), on `aws-lc-rs`, the crypto backend
//! of the client, with no new dependency. It reads what the JDK reads of PBES2: PBKDF2
//! with HMAC-SHA1 (the default), -SHA224, -SHA256, -SHA384 or -SHA512, and AES-128-CBC or
//! AES-256-CBC (the JDK has no AES-192-CBC parameters either). Not read, as the JDK's other
//! `PBEWith...` algorithms (PBES1, 3DES) are not: scrypt, and the legacy PBES1 ciphers.

use std::num::NonZeroU32;

use aws_lc_rs::cipher::{AES_128, AES_256, DecryptionContext, PaddedBlockDecryptingKey, UnboundCipherKey};
use aws_lc_rs::hmac;
use aws_lc_rs::iv::FixedLength;
use aws_lc_rs::pbkdf2;

/// `1.2.840.113549.1.5.13`
const OID_PBES2: &[u8] = &[0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x05, 0x0D];
/// `1.2.840.113549.1.5.12`
const OID_PBKDF2: &[u8] = &[0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x05, 0x0C];
/// The prefix of `hmacWithSHA1` (`...2.7`), `hmacWithSHA224` (`...2.8`), -256 (`...2.9`), -384 (`...2.10`), -512 (`...2.11`).
const OID_HMAC_PREFIX: &[u8] = &[0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x02];
/// `2.16.840.1.101.3.4.1.2`, AES-128-CBC
const OID_AES128_CBC: &[u8] = &[0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x01, 0x02];
/// `2.16.840.1.101.3.4.1.42`, AES-256-CBC
const OID_AES256_CBC: &[u8] = &[0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x01, 0x2A];

const TAG_INTEGER: u8 = 0x02;
const TAG_OCTET_STRING: u8 = 0x04;
const TAG_OID: u8 = 0x06;
const TAG_SEQUENCE: u8 = 0x30;

/// The message of every parse failure: the entry is not what a PKCS#8 file holds.
const NOT_ENCRYPTED_PRIVATE_KEY_INFO: &str = "the entry is not a PKCS#8 EncryptedPrivateKeyInfo";
/// What a user whose key is encrypted in a way that is not read can do.
const RE_ENCRYPT_HINT: &str = "re-encrypt the key with `openssl pkcs8 -topk8 -v2 aes-256-cbc`";
/// The message when the entry is a plain `PrivateKeyInfo`, which a key password cannot be for.
const NOT_ENCRYPTED: &str = "the private key is not encrypted, but ssl.key.password is set";

/// The pseudorandom function of PBKDF2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Prf {
    HmacSha1,
    HmacSha224,
    HmacSha256,
    HmacSha384,
    HmacSha512,
}

/// The AES-CBC cipher of the encryption scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cipher {
    Aes128Cbc,
    Aes256Cbc,
}

impl Cipher {
    fn key_len(self) -> usize {
        match self {
            Cipher::Aes128Cbc => 16,
            Cipher::Aes256Cbc => 32,
        }
    }
}

/// A PKCS#8 `EncryptedPrivateKeyInfo` whose `encryptionAlgorithm` is PBES2 with PBKDF2 and AES-CBC.
#[doc(alias = "javax.crypto.EncryptedPrivateKeyInfo")]
#[derive(Debug)]
pub(crate) struct EncryptedPrivateKeyInfo {
    salt: Vec<u8>,
    iterations: NonZeroU32,
    prf: Prf,
    cipher: Cipher,
    iv: [u8; 16],
    encrypted_data: Vec<u8>,
}

impl EncryptedPrivateKeyInfo {
    /// Parses the DER of an `EncryptedPrivateKeyInfo`.
    ///
    /// # Errors
    ///
    /// The text of the failure: the bytes are not an `EncryptedPrivateKeyInfo`, or it is encrypted with
    /// an algorithm that is not read (see the module documentation).
    #[doc(alias = "javax.crypto.EncryptedPrivateKeyInfo#EncryptedPrivateKeyInfo")]
    pub(crate) fn new(der: &[u8]) -> Result<Self, String> {
        // A `PrivateKeyInfo` starts with its version, an INTEGER, where the algorithm of an
        // `EncryptedPrivateKeyInfo` is a SEQUENCE: a key that is not encrypted gets its own message.
        let malformed = || {
            if Der::sequence(der).is_some_and(|info| info.peek_tag() == Some(TAG_INTEGER)) {
                NOT_ENCRYPTED.to_string()
            } else {
                NOT_ENCRYPTED_PRIVATE_KEY_INFO.to_string()
            }
        };

        // EncryptedPrivateKeyInfo ::= SEQUENCE { encryptionAlgorithm AlgorithmIdentifier, encryptedData OCTET STRING }
        let mut info = Der::sequence(der).ok_or_else(malformed)?;
        let mut algorithm = info.sequence_next().ok_or_else(malformed)?;
        let encrypted_data = info.next(TAG_OCTET_STRING).ok_or_else(malformed)?;
        if !info.is_empty() {
            return Err(malformed());
        }

        // AlgorithmIdentifier ::= SEQUENCE { algorithm OBJECT IDENTIFIER, parameters ANY }
        let algorithm_oid = algorithm.next(TAG_OID).ok_or_else(malformed)?;
        if algorithm_oid != OID_PBES2 {
            return Err(format!(
                "unsupported encryption algorithm {}: only PBES2 is supported ({RE_ENCRYPT_HINT})",
                describe_oid(algorithm_oid)
            ));
        }

        // PBES2-params ::= SEQUENCE { keyDerivationFunc AlgorithmIdentifier, encryptionScheme AlgorithmIdentifier }
        let mut params = algorithm.sequence_next().ok_or_else(malformed)?;
        let mut kdf = params.sequence_next().ok_or_else(malformed)?;
        let mut scheme = params.sequence_next().ok_or_else(malformed)?;

        let kdf_oid = kdf.next(TAG_OID).ok_or_else(malformed)?;
        if kdf_oid != OID_PBKDF2 {
            return Err(format!(
                "unsupported PBES2 key derivation function {}: only PBKDF2 is supported",
                describe_oid(kdf_oid)
            ));
        }
        // PBKDF2-params ::= SEQUENCE { salt OCTET STRING (the `otherSource` choice is not read),
        //   iterationCount INTEGER, keyLength INTEGER OPTIONAL, prf AlgorithmIdentifier DEFAULT hmacWithSHA1 }
        let mut kdf_params = kdf.sequence_next().ok_or_else(malformed)?;
        let salt = kdf_params.next(TAG_OCTET_STRING).ok_or_else(malformed)?;
        let iterations =
            u32::try_from(Der::unsigned(kdf_params.next(TAG_INTEGER).ok_or_else(malformed)?).ok_or_else(malformed)?)
                .ok()
                .and_then(NonZeroU32::new)
                .ok_or_else(malformed)?;
        let mut key_length = None;
        if kdf_params.peek_tag() == Some(TAG_INTEGER) {
            key_length =
                Some(Der::unsigned(kdf_params.next(TAG_INTEGER).ok_or_else(malformed)?).ok_or_else(malformed)?);
        }
        let prf = if kdf_params.is_empty() {
            Prf::HmacSha1
        } else {
            let mut prf = kdf_params.sequence_next().ok_or_else(malformed)?;
            let oid = prf.next(TAG_OID).ok_or_else(malformed)?;
            match oid.strip_prefix(OID_HMAC_PREFIX) {
                Some([7]) => Prf::HmacSha1,
                Some([8]) => Prf::HmacSha224,
                Some([9]) => Prf::HmacSha256,
                Some([10]) => Prf::HmacSha384,
                Some([11]) => Prf::HmacSha512,
                _ => {
                    return Err(format!("unsupported PBKDF2 pseudorandom function {}", describe_oid(oid)));
                },
            }
        };

        let scheme_oid = scheme.next(TAG_OID).ok_or_else(malformed)?;
        let cipher = match scheme_oid {
            OID_AES128_CBC => Cipher::Aes128Cbc,
            OID_AES256_CBC => Cipher::Aes256Cbc,
            _ => {
                return Err(format!(
                    "unsupported PBES2 encryption scheme {}: AES-128-CBC and AES-256-CBC are supported",
                    describe_oid(scheme_oid)
                ));
            },
        };
        // an optional `keyLength` must agree with the cipher (RFC 8018 A.2)
        if key_length.is_some_and(|len| len != cipher.key_len() as u64) {
            return Err(malformed());
        }
        let iv: [u8; 16] = scheme
            .next(TAG_OCTET_STRING)
            .and_then(|iv| iv.try_into().ok())
            .ok_or_else(malformed)?;

        Ok(Self {
            salt: salt.to_vec(),
            iterations,
            prf,
            cipher,
            iv,
            encrypted_data: encrypted_data.to_vec(),
        })
    }

    /// Whether `der` has the shape of an `EncryptedPrivateKeyInfo` (an algorithm identifier and the
    /// encrypted data), whatever the algorithm, which this type may not read.
    pub(crate) fn looks_encrypted(der: &[u8]) -> bool {
        Der::sequence(der).is_some_and(|mut info| {
            info.sequence_next().is_some() && info.next(TAG_OCTET_STRING).is_some() && info.is_empty()
        })
    }

    /// The decrypted `PrivateKeyInfo`: the DER of the PKCS#8 private key.
    ///
    /// `password` is encoded as UTF-8, as the JDK's `PBKDF2WithHmac...` key factories encode a `PBEKeySpec`
    /// password.
    ///
    /// # Errors
    ///
    /// When the password is wrong, or the encrypted data is corrupt: the padding is not valid, or
    /// what was decrypted is not a DER sequence.
    #[doc(alias = "javax.crypto.EncryptedPrivateKeyInfo#getKeySpec")]
    pub(crate) fn decrypt(&self, password: &str) -> Result<Vec<u8>, String> {
        let wrong_password = || "ssl.key.password is wrong, or the key is corrupt".to_string();

        let mut key = vec![0u8; self.cipher.key_len()];
        derive_key(self.prf, password.as_bytes(), &self.salt, self.iterations, &mut key);
        let algorithm = match self.cipher {
            Cipher::Aes128Cbc => &AES_128,
            Cipher::Aes256Cbc => &AES_256,
        };
        let decrypting_key = UnboundCipherKey::new(algorithm, &key)
            .and_then(PaddedBlockDecryptingKey::cbc_pkcs7)
            .map_err(|_| "the cipher key could not be created".to_string());
        wipe(&mut key);

        let mut data = self.encrypted_data.clone();
        let decrypted_len = decrypting_key?
            .decrypt(&mut data, DecryptionContext::Iv128(FixedLength::from(self.iv)))
            .map(|plain| plain.len())
            .map_err(|_| wrong_password());
        let decrypted_len = match decrypted_len {
            Ok(len) => len,
            Err(e) => {
                wipe(&mut data);
                return Err(e);
            },
        };
        data.truncate(decrypted_len);

        // A wrong password still yields valid PKCS#7 padding once in 256 times: what comes out of
        // a right one is a PrivateKeyInfo, a DER SEQUENCE that fills the whole plaintext.
        if Der::sequence(&data).is_none() {
            wipe(&mut data);
            return Err(wrong_password());
        }
        Ok(data)
    }
}

/// PBKDF2 (RFC 8018 §5.2) with the given pseudorandom function, filling `out`.
fn derive_key(prf: Prf, password: &[u8], salt: &[u8], iterations: NonZeroU32, out: &mut [u8]) {
    let algorithm = match prf {
        Prf::HmacSha1 => pbkdf2::PBKDF2_HMAC_SHA1,
        Prf::HmacSha256 => pbkdf2::PBKDF2_HMAC_SHA256,
        Prf::HmacSha384 => pbkdf2::PBKDF2_HMAC_SHA384,
        Prf::HmacSha512 => pbkdf2::PBKDF2_HMAC_SHA512,
        Prf::HmacSha224 => return pbkdf2_hmac_sha224(password, salt, iterations, out),
    };
    pbkdf2::derive(algorithm, iterations, salt, password, out);
}

/// PBKDF2-HMAC-SHA224, which `aws_lc_rs::pbkdf2` does not offer but the JDK reads
/// (`PBKDF2WithHmacSHA224`): `T_i = U_1 xor ... xor U_c`, `U_1 = HMAC(P, S || INT(i))`, `U_j = HMAC(P, U_{j-1})`.
fn pbkdf2_hmac_sha224(password: &[u8], salt: &[u8], iterations: NonZeroU32, out: &mut [u8]) {
    const HASH_LEN: usize = 28;
    let key = hmac::Key::new(hmac::HMAC_SHA224, password);
    for (index, block) in out.chunks_mut(HASH_LEN).enumerate() {
        let mut context = hmac::Context::with_key(&key);
        context.update(salt);
        context.update(&(index as u32 + 1).to_be_bytes());
        let mut u = context.sign();
        let mut t = [0u8; HASH_LEN];
        t.copy_from_slice(u.as_ref());
        for _ in 1..iterations.get() {
            u = hmac::sign(&key, u.as_ref());
            for (t, u) in t.iter_mut().zip(u.as_ref()) {
                *t ^= u;
            }
        }
        block.copy_from_slice(&t[..block.len()]);
        wipe(&mut t);
    }
}

/// Best-effort erasure of key material: volatile writes, which the compiler may not drop.
fn wipe(bytes: &mut [u8]) {
    for byte in bytes {
        // SAFETY: `byte` is a valid, aligned, exclusive reference to a `u8`.
        unsafe { std::ptr::write_volatile(byte, 0) };
    }
}

/// The algorithm an OBJECT IDENTIFIER names, for the ones that a key may be encrypted with and that this
/// type does not read, by the name its specification gives it: PBES1 (RFC 8018 appendix A.3) and the
/// PKCS#12 ciphers (RFC 7292 appendix C), other PBES2 schemes, scrypt, and the PBKDF2 hashes that the
/// JDK reads and this type does not.
fn oid_name(dotted: &str) -> Option<&'static str> {
    Some(match dotted {
        // PBES1, PKCS#5
        "1.2.840.113549.1.5.1" => "pbeWithMD2AndDES-CBC",
        "1.2.840.113549.1.5.3" => "pbeWithMD5AndDES-CBC",
        "1.2.840.113549.1.5.4" => "pbeWithMD2AndRC2-CBC",
        "1.2.840.113549.1.5.6" => "pbeWithMD5AndRC2-CBC",
        "1.2.840.113549.1.5.10" => "pbeWithSHA1AndDES-CBC",
        "1.2.840.113549.1.5.11" => "pbeWithSHA1AndRC2-CBC",
        // PKCS#12 password-based encryption
        "1.2.840.113549.1.12.1.1" => "pbeWithSHAAnd128BitRC4",
        "1.2.840.113549.1.12.1.2" => "pbeWithSHAAnd40BitRC4",
        "1.2.840.113549.1.12.1.3" => "pbeWithSHAAnd3-KeyTripleDES-CBC",
        "1.2.840.113549.1.12.1.4" => "pbeWithSHAAnd2-KeyTripleDES-CBC",
        "1.2.840.113549.1.12.1.5" => "pbeWithSHAAnd128BitRC2-CBC",
        "1.2.840.113549.1.12.1.6" => "pbeWithSHAAnd40BitRC2-CBC",
        // PBES2 encryption schemes
        "1.2.840.113549.3.7" => "des-EDE3-CBC",
        "1.3.14.3.2.7" => "des-CBC",
        "1.2.840.113549.3.2" => "rc2-CBC",
        "2.16.840.1.101.3.4.1.22" => "aes192-CBC",
        "2.16.840.1.101.3.4.1.6" => "aes128-GCM",
        "2.16.840.1.101.3.4.1.46" => "aes256-GCM",
        "1.2.392.200011.61.1.1.1.2" => "camellia128-CBC",
        "1.2.392.200011.61.1.1.1.3" => "camellia192-CBC",
        "1.2.392.200011.61.1.1.1.4" => "camellia256-CBC",
        // PBES2 key derivation function
        "1.3.6.1.4.1.11591.4.11" => "scrypt",
        // PBKDF2 pseudorandom functions
        "1.2.840.113549.2.12" => "hmacWithSHA512-224",
        "1.2.840.113549.2.13" => "hmacWithSHA512-256",
        _ => return None,
    })
}

/// `name (1.2.840...)` when the identifier has a [name](oid_name), else `1.2.840...`.
fn describe_oid(oid: &[u8]) -> String {
    let dotted = oid_to_string(oid);
    match oid_name(&dotted) {
        Some(name) => format!("{name} ({dotted})"),
        None => dotted,
    }
}

/// `1.2.840...` for the content octets of an OBJECT IDENTIFIER, for the messages.
fn oid_to_string(oid: &[u8]) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut value: u64 = 0;
    for &byte in oid {
        value = (value << 7) | u64::from(byte & 0x7F);
        if byte & 0x80 == 0 {
            if parts.is_empty() {
                // the first subidentifier carries two arcs: `40 * first + second`
                let first = (value / 40).min(2);
                parts.push(first.to_string());
                parts.push((value - first * 40).to_string());
            } else {
                parts.push(value.to_string());
            }
            value = 0;
        }
    }
    parts.join(".")
}

/// A reader of a DER value's content, one TLV at a time.
struct Der<'a> {
    data: &'a [u8],
}

impl<'a> Der<'a> {
    /// The content of `der` if it is exactly one SEQUENCE, with nothing before or after it.
    fn sequence(der: &'a [u8]) -> Option<Der<'a>> {
        let mut outer = Der { data: der };
        let content = outer.next(TAG_SEQUENCE)?;
        outer.is_empty().then_some(Der { data: content })
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn peek_tag(&self) -> Option<u8> {
        self.data.first().copied()
    }

    /// The content of the next TLV if its tag is `tag`; definite lengths only (DER).
    fn next(&mut self, tag: u8) -> Option<&'a [u8]> {
        let (&found, rest) = self.data.split_first()?;
        if found != tag {
            return None;
        }
        let (&first, rest) = rest.split_first()?;
        let (len, rest) = if first < 0x80 {
            (usize::from(first), rest)
        } else {
            let count = usize::from(first & 0x7F);
            // `0x80` is the indefinite form; more than 4 length bytes is beyond any key
            if count == 0 || count > 4 || rest.len() < count {
                return None;
            }
            let len = rest[..count].iter().fold(0usize, |acc, &b| (acc << 8) | usize::from(b));
            (len, &rest[count..])
        };
        if rest.len() < len {
            return None;
        }
        let (content, rest) = rest.split_at(len);
        self.data = rest;
        Some(content)
    }

    /// The next TLV, which must be a SEQUENCE, as a reader of its content.
    fn sequence_next(&mut self) -> Option<Der<'a>> {
        self.next(TAG_SEQUENCE).map(|content| Der { data: content })
    }

    /// The value of a non-negative INTEGER that fits in 8 bytes.
    fn unsigned(content: &[u8]) -> Option<u64> {
        let digits = match content {
            [] => return None,
            [first, ..] if first & 0x80 != 0 => return None,
            [0, rest @ ..] if !rest.is_empty() => rest,
            _ => content,
        };
        (digits.len() <= 8).then(|| digits.iter().fold(0u64, |acc, &b| (acc << 8) | u64::from(b)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::security::ssl::pem_parser::PemParser;
    use crate::common::security::ssl::private_key_test_vectors as vectors;

    fn der_of(pem: &str) -> Vec<u8> {
        let mut entries = PemParser::new("PRIVATE KEY").pem_entries(pem).unwrap();
        assert_eq!(1, entries.len());
        entries.remove(0)
    }

    fn decrypt(pem: &str, password: &str) -> Result<Vec<u8>, String> {
        EncryptedPrivateKeyInfo::new(&der_of(pem))?.decrypt(password)
    }

    /// Decrypting each variant gives exactly the PKCS#8 DER of the unencrypted key.
    #[test]
    fn test_decrypts_to_the_unencrypted_pkcs8() {
        let ec = der_of(vectors::EC_PLAIN);
        for pem in [
            vectors::EC_ENC_AES256_SHA256,
            vectors::EC_ENC_AES128_SHA1,
            vectors::EC_ENC_AES128_SHA256,
            vectors::EC_ENC_AES256_SHA224,
            vectors::EC_ENC_AES256_SHA384,
            vectors::EC_ENC_AES256_SHA512,
        ] {
            assert_eq!(ec, decrypt(pem, vectors::PASSWORD).unwrap());
        }
        assert_eq!(
            der_of(vectors::RSA_PLAIN),
            decrypt(vectors::RSA_ENC_AES256_SHA256, vectors::PASSWORD).unwrap()
        );
    }

    #[test]
    fn test_parameters_are_read_from_the_der() {
        let info = EncryptedPrivateKeyInfo::new(&der_of(vectors::EC_ENC_AES128_SHA1)).unwrap();
        assert_eq!(
            (Prf::HmacSha1, Cipher::Aes128Cbc, 2048),
            (info.prf, info.cipher, info.iterations.get())
        );
        assert_eq!(16, info.salt.len());
        let info = EncryptedPrivateKeyInfo::new(&der_of(vectors::EC_ENC_AES256_SHA224)).unwrap();
        assert_eq!((Prf::HmacSha224, Cipher::Aes256Cbc), (info.prf, info.cipher));
        let info = EncryptedPrivateKeyInfo::new(&der_of(vectors::EC_ENC_AES256_SHA512)).unwrap();
        assert_eq!(Prf::HmacSha512, info.prf);
    }

    /// Java: `InvalidKeySpecException: Cannot retrieve the PKCS8EncodedKeySpec`.
    #[test]
    fn test_wrong_password_is_an_error_for_every_variant() {
        for pem in [
            vectors::EC_ENC_AES256_SHA256,
            vectors::EC_ENC_AES128_SHA1,
            vectors::EC_ENC_AES256_SHA224,
        ] {
            for password in ["wrong", "", "S3cr3t-pass_une", "s3cr3t-pass_üñí"] {
                assert_eq!(
                    "ssl.key.password is wrong, or the key is corrupt",
                    decrypt(pem, password).unwrap_err(),
                    "{password:?}"
                );
            }
        }
    }

    /// PBKDF2-HMAC-SHA224, written here, against a known answer that OpenSSL
    /// (`openssl kdf -keylen 32 -kdfopt digest:SHA224 -kdfopt pass:password -kdfopt salt:salt -kdfopt iter:2 PBKDF2`) and
    /// Java (`SecretKeyFactory.getInstance("PBKDF2WithHmacSHA224")`, 2 iterations, 256 bits) agree on.
    #[test]
    fn test_pbkdf2_sha224_known_answer() {
        let mut out = [0u8; 32];
        pbkdf2_hmac_sha224(b"password", b"salt", NonZeroU32::new(2).unwrap(), &mut out);
        assert_eq!(
            "93200ffa96c5776d38fa10abdf8f5bfc0054b9718513df472d2331d2d1e66a3f",
            out.iter().map(|b| format!("{b:02x}")).collect::<String>()
        );
    }

    /// What the user is told to do about a key encrypted in a way that is not read.
    const RE_ENCRYPT: &str = "re-encrypt the key with `openssl pkcs8 -topk8 -v2 aes-256-cbc`";

    /// An unsupported algorithm is named, as well as numbered, and the way out is given.
    #[test]
    fn test_unsupported_algorithms_are_named() {
        for (pem, expected) in [
            (
                vectors::EC_ENC_AES192_SHA512,
                "unsupported PBES2 encryption scheme aes192-CBC (2.16.840.1.101.3.4.1.22): \
                 AES-128-CBC and AES-256-CBC are supported",
            ),
            (
                vectors::EC_ENC_CAMELLIA256,
                "unsupported PBES2 encryption scheme camellia256-CBC (1.2.392.200011.61.1.1.1.4): \
                 AES-128-CBC and AES-256-CBC are supported",
            ),
            (
                vectors::EC_ENC_SCRYPT,
                "unsupported PBES2 key derivation function scrypt (1.3.6.1.4.1.11591.4.11): only PBKDF2 is supported",
            ),
            (
                vectors::EC_ENC_AES256_SHA512_224,
                "unsupported PBKDF2 pseudorandom function hmacWithSHA512-224 (1.2.840.113549.2.12)",
            ),
            (
                vectors::EC_ENC_AES256_SHA512_256,
                "unsupported PBKDF2 pseudorandom function hmacWithSHA512-256 (1.2.840.113549.2.13)",
            ),
        ] {
            assert_eq!(expected, EncryptedPrivateKeyInfo::new(&der_of(pem)).unwrap_err());
        }
        // a PBES1 `PBEWithMD5AndDES` (1.2.840.113549.1.5.3) AlgorithmIdentifier
        let pbes1: Vec<u8> = vec![
            0x30, 0x1D, 0x30, 0x13, 0x06, 0x09, 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x05, 0x03, 0x30, 0x06, 0x04,
            0x01, 0x00, 0x02, 0x01, 0x01, 0x04, 0x06, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF,
        ];
        assert_eq!(
            format!(
                "unsupported encryption algorithm pbeWithMD5AndDES-CBC (1.2.840.113549.1.5.3): only PBES2 is supported ({RE_ENCRYPT})"
            ),
            EncryptedPrivateKeyInfo::new(&pbes1).unwrap_err()
        );
        // the PKCS#12 `pbeWithSHAAnd3-KeyTripleDES-CBC` (1.2.840.113549.1.12.1.3), `PBEWithSHA1AndDESede` in the JDK
        let pkcs12_3des: Vec<u8> = vec![
            0x30, 0x1E, 0x30, 0x14, 0x06, 0x0A, 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x0C, 0x01, 0x03, 0x30, 0x06,
            0x04, 0x01, 0x00, 0x02, 0x01, 0x01, 0x04, 0x06, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF,
        ];
        assert_eq!(
            format!(
                "unsupported encryption algorithm pbeWithSHAAnd3-KeyTripleDES-CBC (1.2.840.113549.1.12.1.3): only PBES2 is supported ({RE_ENCRYPT})"
            ),
            EncryptedPrivateKeyInfo::new(&pkcs12_3des).unwrap_err()
        );
        // an algorithm with no name here is still numbered
        let unknown: Vec<u8> = vec![
            0x30, 0x0E, 0x30, 0x04, 0x06, 0x02, 0x2A, 0x03, 0x04, 0x06, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF,
        ];
        assert_eq!(
            format!("unsupported encryption algorithm 1.2.3: only PBES2 is supported ({RE_ENCRYPT})"),
            EncryptedPrivateKeyInfo::new(&unknown).unwrap_err()
        );
    }

    /// Anything that is not an `EncryptedPrivateKeyInfo` is rejected, not trusted: truncations of a valid
    /// DER at every length, trailing bytes, indefinite and oversized lengths.
    #[test]
    fn test_malformed_der_is_an_error_not_a_panic() {
        let der = der_of(vectors::EC_ENC_AES256_SHA256);
        for len in 0..der.len() {
            assert!(EncryptedPrivateKeyInfo::new(&der[..len]).is_err(), "truncated to {len} bytes");
        }
        let mut trailing = der.clone();
        trailing.push(0);
        assert!(EncryptedPrivateKeyInfo::new(&trailing).is_err());
        for bad in [
            &[][..],
            &[0x30][..],
            &[0x30, 0x80, 0x00, 0x00][..],
            &[0x30, 0x85, 1, 2, 3, 4, 5][..],
            &[0x04, 0x00][..],
        ] {
            assert_eq!(
                NOT_ENCRYPTED_PRIVATE_KEY_INFO,
                EncryptedPrivateKeyInfo::new(bad).unwrap_err(),
                "{bad:?}"
            );
        }
    }

    /// A SEQUENCE that starts with an INTEGER, the version of a `PrivateKeyInfo`, is not an
    /// `EncryptedPrivateKeyInfo` either, and says so.
    #[test]
    fn test_a_sequence_starting_with_a_version_is_not_encrypted() {
        assert_eq!(
            "the private key is not encrypted, but ssl.key.password is set",
            EncryptedPrivateKeyInfo::new(&[0x30, 0x03, 0x02, 0x01, 0x01]).unwrap_err()
        );
    }

    /// Ciphertext whose length is not a whole number of blocks, or that is empty, is "wrong password or corrupt".
    #[test]
    fn test_bad_ciphertext_lengths_are_an_error() {
        let mut info = EncryptedPrivateKeyInfo::new(&der_of(vectors::EC_ENC_AES256_SHA256)).unwrap();
        info.encrypted_data.truncate(info.encrypted_data.len() - 1);
        assert_eq!(
            "ssl.key.password is wrong, or the key is corrupt",
            info.decrypt(vectors::PASSWORD).unwrap_err()
        );
        info.encrypted_data.clear();
        assert_eq!(
            "ssl.key.password is wrong, or the key is corrupt",
            info.decrypt(vectors::PASSWORD).unwrap_err()
        );
    }

    /// An unencrypted PKCS#8 key is told apart from damaged data: a key password cannot be for it.
    #[test]
    fn test_a_plain_pkcs8_key_is_reported_as_not_encrypted() {
        assert_eq!(
            "the private key is not encrypted, but ssl.key.password is set",
            EncryptedPrivateKeyInfo::new(&der_of(vectors::EC_PLAIN)).unwrap_err()
        );
        assert_eq!(
            "the private key is not encrypted, but ssl.key.password is set",
            EncryptedPrivateKeyInfo::new(&der_of(vectors::RSA_PLAIN)).unwrap_err()
        );
    }

    #[test]
    fn test_looks_encrypted() {
        assert!(EncryptedPrivateKeyInfo::looks_encrypted(&der_of(vectors::EC_ENC_AES256_SHA256)));
        // encrypted with an algorithm that is not read still has the shape
        assert!(EncryptedPrivateKeyInfo::looks_encrypted(&der_of(vectors::EC_ENC_AES192_SHA512)));
        assert!(!EncryptedPrivateKeyInfo::looks_encrypted(&der_of(vectors::EC_PLAIN)));
        assert!(!EncryptedPrivateKeyInfo::looks_encrypted(&[0x30, 0x00]));
        assert!(!EncryptedPrivateKeyInfo::looks_encrypted(&[]));
    }

    #[test]
    fn test_oid_to_string() {
        assert_eq!("1.2.840.113549.1.5.13", oid_to_string(OID_PBES2));
        assert_eq!("2.16.840.1.101.3.4.1.42", oid_to_string(OID_AES256_CBC));
        assert_eq!("1.2.840.113549.2.7", oid_to_string(&[OID_HMAC_PREFIX, &[7]].concat()));
    }
}
