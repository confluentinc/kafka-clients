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

//! SSL/TLS factory for building rustls client configurations.
//!
//! Translated from `org.apache.kafka.common.security.ssl.SslFactory` and
//! `org.apache.kafka.common.security.ssl.DefaultSslEngineFactory`.
//!
//! In Java, `SslFactory` delegates to `SslEngineFactory` which creates `SSLEngine`
//! instances from a `SSLContext`. In Rust, this is simplified to build an
//! `Arc<rustls::ClientConfig>` from [`SslConfigs`], using `rustls` for TLS.
//!
//! Key differences:
//! - Only PEM format is supported. JKS and PKCS12 return an error.
//! - `aws-lc-rs` is used as the crypto backend (FIPS compliant).
//! - Hostname verification is controlled by `endpoint_identification_algorithm`:
//!   when empty, a custom `ServerCertVerifier` validates the cert chain but skips
//!   hostname matching.
//! - `ssl.protocol` is the highest TLS version used, as Java's
//!   `SSLContext.getInstance(protocol)` is; `ssl.enabled.protocols` and
//!   `ssl.cipher.suites` pick the versions and suites as Java's
//!   `setEnabledProtocols` / `setEnabledCipherSuites` do. Their errors are
//!   reported when the factory is built, where Java reports some of them at the
//!   first connection or handshake (see [`SslFactory::new`]).

use crate::common::config::SslConfigs;

use std::io;
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};

/// SSL/TLS factory that builds `Arc<rustls::ClientConfig>` from [`SslConfigs`].
///
/// Translated from `org.apache.kafka.common.security.ssl.SslFactory`.
///
/// Holds the compiled TLS client configuration and a flag indicating whether
/// hostname verification is enabled. The configuration is built once and shared
/// across all connections via `Arc`.
#[doc(alias = "org.apache.kafka.common.security.ssl.SslFactory")]
pub struct SslFactory {
    /// Compiled TLS client configuration.
    client_config: Arc<ClientConfig>,
    /// Whether hostname verification is enabled.
    hostname_verification: bool,
}

impl std::fmt::Debug for SslFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SslFactory")
            .field("hostname_verification", &self.hostname_verification)
            .finish_non_exhaustive()
    }
}

impl SslFactory {
    /// Creates a new `SslFactory` from the given [`SslConfigs`].
    ///
    /// Loads trust anchors (CA certificates), optionally loads client certificate
    /// and private key for mTLS, and configures TLS protocol versions.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The truststore or keystore format is not PEM (JKS/PKCS12 are unsupported)
    /// - PEM files cannot be read or parsed
    /// - No valid certificates are found in the truststore
    /// - The private key cannot be loaded, is encrypted (see
    ///   [`is_encrypted_pem_key`]), or is missing when a client cert is provided
    /// - `ssl.protocol` names no client `SSLContext` of the JDK (`TLSv1.3`,
    ///   `TLSv1.2`, `TLSv1.1`, `TLSv1`, `SSLv3`, `TLS`, `SSL`), or an entry of
    ///   `ssl.enabled.protocols` is not `TLSv1.3` or `TLSv1.2`
    /// - an entry of `ssl.cipher.suites` is not a cipher suite of the TLS
    ///   provider (`Unsupported CipherSuite: <name>`)
    /// - no cipher suite is enabled, or no enabled protocol can be used with
    ///   an enabled suite (see the private `resolve_tls_parameters` for how
    ///   `ssl.protocol` selects the default lists)
    ///
    /// Java reports an unsupported cipher suite when it creates the first
    /// `SSLEngine`, and the last two at the handshake; here all are reported
    /// when the factory is built.
    #[doc(alias = "org.apache.kafka.common.security.ssl.SslFactory#SslFactory")]
    pub fn new(ssl_config: &SslConfigs) -> io::Result<Self> {
        // Install the aws-lc-rs crypto provider if not already installed.
        // This is idempotent — subsequent calls are no-ops.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // Validate store formats
        validate_store_format(&ssl_config.truststore_type, "truststore")?;
        validate_store_format(&ssl_config.keystore_type, "keystore")?;

        // Build root cert store
        let root_store = build_root_cert_store(ssl_config)?;

        // Determine hostname verification
        let hostname_verification = !ssl_config.endpoint_identification_algorithm.is_empty();

        // Determine the TLS versions and cipher suites from `ssl.protocol`,
        // `ssl.enabled.protocols` and `ssl.cipher.suites`, as JSSE does.
        let installed = rustls::crypto::CryptoProvider::get_default()
            .cloned()
            .unwrap_or_else(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()));
        let (versions, suites) = resolve_tls_parameters(
            &ssl_config.protocol,
            &ssl_config.enabled_protocols,
            &ssl_config.cipher_suites,
            &installed.cipher_suites,
        )?;

        // The installed provider when its suites are the enabled ones;
        // otherwise a per-config provider with the enabled suites, in the
        // configured order: the aws-lc-rs provider for `ssl.cipher.suites`,
        // whose suites they name, else the installed one restricted to the
        // `ssl.protocol` context's default suites.
        let builder = if suites == installed.cipher_suites {
            ClientConfig::builder_with_provider(installed).with_protocol_versions(&versions)
        } else {
            let base = if ssl_config.cipher_suites.is_empty() {
                (*installed).clone()
            } else {
                rustls::crypto::aws_lc_rs::default_provider()
            };
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::CryptoProvider {
                cipher_suites: suites,
                ..base
            }))
            .with_protocol_versions(&versions)
        }
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

        // Server certificate verification
        let builder = if hostname_verification {
            builder.with_root_certificates(root_store)
        } else {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoHostnameVerifier::new(root_store)))
        };

        // Client authentication
        let config = match load_client_identity(ssl_config)? {
            Some((certs, key)) => builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("Failed to set client auth: {e}")))?,
            None => builder.with_no_client_auth(),
        };

        Ok(Self { client_config: Arc::new(config), hostname_verification })
    }

    /// Converts a peer hostname string to a `ServerName` for TLS SNI.
    ///
    /// Tries DNS name first, then falls back to IP address.
    ///
    /// # Errors
    ///
    /// Returns an error if the hostname cannot be parsed as a valid DNS name
    /// or IP address.
    pub fn create_server_name(peer_host: &str) -> io::Result<ServerName<'static>> {
        ServerName::try_from(peer_host.to_string())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("Invalid server name '{peer_host}': {e}")))
    }

    /// Returns whether hostname verification is enabled.
    pub fn hostname_verification(&self) -> bool {
        self.hostname_verification
    }

    /// Returns a reference to the compiled client config.
    pub fn client_config(&self) -> &Arc<ClientConfig> {
        &self.client_config
    }
}

/// The error for an encrypted PEM private key, a Rust-only message: Java
/// decrypts an encrypted PKCS#8 key (an `EncryptedPrivateKeyInfo`) with
/// `ssl.key.password` (`org.apache.kafka.common.security.ssl.DefaultSslEngineFactory.PemStore#privateKey`), which this
/// client does not implement, so the key password is not defined
/// (`SslConfigs` module docs) and an encrypted key is reported as such rather
/// than as a missing or unparseable key.
const ENCRYPTED_PEM_KEY_MESSAGE: &str =
    "Encrypted PEM private keys are not supported by this client; provide an unencrypted PKCS#8 key";

/// Whether `pem` holds an encrypted private key: a PKCS#8
/// `ENCRYPTED PRIVATE KEY` section (what Java decrypts at
/// `org.apache.kafka.common.security.ssl.DefaultSslEngineFactory.PemStore#privateKey`), or a legacy OpenSSL block whose
/// `Proc-Type: 4,ENCRYPTED` header marks it encrypted (with its `DEK-Info`,
/// e.g. in an `RSA PRIVATE KEY` section). A text check only: nothing is
/// decrypted.
fn is_encrypted_pem_key(pem: &[u8]) -> bool {
    const ENCRYPTED_PKCS8_BEGIN: &[u8] = b"-----BEGIN ENCRYPTED PRIVATE KEY-----";
    pem.split(|&b| b == b'\n').map(<[u8]>::trim_ascii).any(|line| {
        line.starts_with(ENCRYPTED_PKCS8_BEGIN)
            || line
                .strip_prefix(b"Proc-Type:")
                .is_some_and(|value| value.trim_ascii().ends_with(b"ENCRYPTED"))
    })
}

/// Fails with [`ENCRYPTED_PEM_KEY_MESSAGE`] when `pem` holds an encrypted
/// private key ([`is_encrypted_pem_key`]). It runs before the key is parsed,
/// since the PEM parser skips an `ENCRYPTED PRIVATE KEY` section (leaving
/// `No private key found`) and fails to decode a legacy block's headers.
fn reject_encrypted_pem_key(pem: &[u8]) -> io::Result<()> {
    if is_encrypted_pem_key(pem) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, ENCRYPTED_PEM_KEY_MESSAGE));
    }
    Ok(())
}

/// Validates that a store format is PEM. Returns an error for JKS or PKCS12.
fn validate_store_format(format: &str, store_name: &str) -> io::Result<()> {
    match format.to_uppercase().as_str() {
        "PEM" => Ok(()),
        "JKS" => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "JKS format is not supported for {store_name} in Rust. \
                 Convert to PEM format using: keytool -exportcert -alias <alias> -keystore <keystore.jks> -rfc"
            ),
        )),
        "PKCS12" | "P12" => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "PKCS12 format is not supported for {store_name} in Rust. \
                 Convert to PEM format using: openssl pkcs12 -in <keystore.p12> -out <keystore.pem> -nodes"
            ),
        )),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Unsupported {store_name} format: '{other}'. Only PEM is supported."),
        )),
    }
}

/// Builds a root certificate store from the SSL config.
///
/// Loads certificates from:
/// 1. `truststore_location` — PEM file path
/// 2. `truststore_certificates` — inline PEM string
/// 3. System roots via `webpki-roots` (fallback when neither 1 nor 2 is set)
fn build_root_cert_store(ssl_config: &SslConfigs) -> io::Result<RootCertStore> {
    let mut root_store = RootCertStore::empty();

    if let Some(ref path) = ssl_config.truststore_location {
        let file = std::fs::File::open(path)
            .map_err(|e| io::Error::new(e.kind(), format!("Failed to open truststore file '{path}': {e}")))?;
        let certs = CertificateDer::pem_reader_iter(file)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to parse certificates from truststore file '{path}': {e}"),
                )
            })?;
        if certs.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("No certificates found in truststore file '{path}'"),
            ));
        }
        for cert in certs {
            root_store.add(cert).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to add certificate from truststore file: {e}"),
                )
            })?;
        }
    } else if let Some(ref pem_data) = ssl_config.truststore_certificates {
        let certs = CertificateDer::pem_slice_iter(pem_data.value().as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to parse inline truststore certificates: {e}"),
                )
            })?;
        if certs.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "No certificates found in inline truststore certificates",
            ));
        }
        for cert in certs {
            root_store.add(cert).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to add inline truststore certificate: {e}"),
                )
            })?;
        }
    } else {
        // Fallback to system roots
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    Ok(root_store)
}

/// Loads client certificate and private key for mTLS, if configured.
///
/// Returns `None` if no client identity is configured.
fn load_client_identity(
    ssl_config: &SslConfigs,
) -> io::Result<Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>> {
    // Check for inline PEM cert chain + key
    let certs = if let Some(ref chain_pem) = ssl_config.keystore_certificate_chain {
        let certs = CertificateDer::pem_slice_iter(chain_pem.value().as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to parse inline keystore certificate chain: {e}"),
                )
            })?;
        if certs.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "No certificates found in inline keystore certificate chain",
            ));
        }
        Some(certs)
    } else if let Some(ref path) = ssl_config.keystore_location {
        let pem = std::fs::read(path)
            .map_err(|e| io::Error::new(e.kind(), format!("Failed to open keystore file '{path}': {e}")))?;
        // The key is in the same file, and a legacy encrypted key's
        // `Proc-Type` / `DEK-Info` headers would fail the certificate parse
        // with a base64 error, so the encrypted-key check runs first.
        reject_encrypted_pem_key(&pem)?;
        let certs = CertificateDer::pem_slice_iter(&pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to parse certificates from keystore file '{path}': {e}"),
                )
            })?;
        if certs.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("No certificates found in keystore file '{path}'"),
            ));
        }
        Some(certs)
    } else {
        None
    };

    let key = if let Some(ref key_pem) = ssl_config.keystore_key {
        // `.next().transpose()` yields `Result<Option<PrivateKeyDer>, pem::Error>` —
        // the same shape the old `rustls_pemfile::private_key` returned — so a PEM
        // with no key section stays `Ok(None)` and produces the "No private key
        // found" message below rather than a parse error.
        reject_encrypted_pem_key(key_pem.value().as_bytes())?;
        let key = PrivateKeyDer::pem_slice_iter(key_pem.value().as_bytes())
            .next()
            .transpose()
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to parse inline keystore private key: {e}"),
                )
            })?
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "No private key found in inline keystore key"))?;
        Some(key)
    } else if let Some(ref path) = ssl_config.keystore_location {
        // When keystore_location is used, key is expected in the same file
        let pem = std::fs::read(path)
            .map_err(|e| io::Error::new(e.kind(), format!("Failed to open keystore file for key '{path}': {e}")))?;
        reject_encrypted_pem_key(&pem)?;
        let key = PrivateKeyDer::pem_slice_iter(&pem)
            .next()
            .transpose()
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to parse private key from keystore file '{path}': {e}"),
                )
            })?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("No private key found in keystore file '{path}'"),
                )
            })?;
        Some(key)
    } else {
        None
    };

    match (certs, key) {
        (Some(c), Some(k)) => Ok(Some((c, k))),
        (None, None) => Ok(None),
        (Some(_), None) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Client certificate chain provided but no private key configured",
        )),
        (None, Some(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Private key provided but no client certificate chain configured",
        )),
    }
}

/// The protocols the `SSLContext` of `ssl.protocol` enables by default.
///
/// `protocol` is the name Kafka passes to `SSLContext.getInstance(protocol)`
/// (`org.apache.kafka.common.security.ssl.DefaultSslEngineFactory#createSSLContext`), matched ignoring case as JCA
/// algorithm names are. The names are the client `SSLContext`s of the JDK's
/// SunJSSE provider (`sun.security.ssl.SunJSSE`), aliases included, and each
/// context's default client protocols (`SSLContextImpl.java`):
///
/// | `ssl.protocol` | JSSE context | default protocols | here |
/// |---|---|---|---|
/// | `TLS`, `SSL` | `TLSContext` | TLSv1.3, TLSv1.2 | TLS 1.3, TLS 1.2 |
/// | `TLSv1.3` | `TLS13Context` | TLSv1.3 and lower | TLS 1.3, TLS 1.2 |
/// | `TLSv1.2` | `TLS12Context` | TLSv1.2 and lower | TLS 1.2 |
/// | `TLSv1.1` | `TLS11Context` | TLSv1.1 and lower | none |
/// | `TLSv1`, `SSLv3` | `TLS10Context` | TLSv1, SSLv3 | none |
///
/// rustls implements no version below TLS 1.2, and the JDK disables them by
/// default (`jdk.tls.disabledAlgorithms`), so the last two rows enable no
/// protocol in either client. Any other name is an error, as
/// `SSLContext.getInstance` throws `NoSuchAlgorithmException` for it: the DTLS
/// contexts, which Java's `SSLSocket` and Kafka's TCP transport cannot use, and
/// `Default`, whose context Java refuses to `init` ("Default SSLContext is
/// initialized automatically"), included.
fn context_default_versions(protocol: &str) -> io::Result<&'static [&'static rustls::SupportedProtocolVersion]> {
    const TLS13_CONTEXT: &[&rustls::SupportedProtocolVersion] = &[&rustls::version::TLS13, &rustls::version::TLS12];
    const TLS12_CONTEXT: &[&rustls::SupportedProtocolVersion] = &[&rustls::version::TLS12];
    let is = |name: &str| protocol.eq_ignore_ascii_case(name);
    if is("TLS") || is("SSL") || is("TLSv1.3") {
        Ok(TLS13_CONTEXT)
    } else if is("TLSv1.2") {
        Ok(TLS12_CONTEXT)
    } else if is("TLSv1.1") || is("TLSv1") || is("SSLv3") {
        Ok(&[])
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Unsupported TLS protocol: '{protocol}'. \
                 Supported protocols: TLSv1.3, TLSv1.2, TLSv1.1, TLSv1, SSLv3, TLS, SSL"
            ),
        ))
    }
}

/// Resolves `ssl.protocol`, `ssl.enabled.protocols` and `ssl.cipher.suites`
/// to the TLS versions and cipher suites a connection may use, as JSSE does
/// for the `SSLEngine` Kafka's `DefaultSslEngineFactory` creates.
///
/// `ssl.protocol` is not a ceiling of its own. It selects the `SSLContext`
/// (see [`context_default_versions`]), which supplies only the *default* lists:
/// its default protocols, and its default cipher suites, the suites usable
/// with one of those protocols (`sun.security.ssl.SSLContextImpl`). Kafka replaces
/// the suites when `ssl.cipher.suites` is not empty and the protocols when
/// `ssl.enabled.protocols` is not empty (`org.apache.kafka.common.security.ssl.DefaultSslEngineFactory#configure`,
/// `org.apache.kafka.common.security.ssl.DefaultSslEngineFactory#createSslEngine`); an empty list leaves the context's default. A protocol is then
/// used only when an enabled suite can be used with it
/// (`sun.security.ssl.HandshakeContext#getActiveProtocols`). So `TLSv1.2` keeps TLS
/// 1.3 out only while its default suites are in use: with the default
/// `ssl.enabled.protocols` (`TLSv1.2,TLSv1.3`) and a TLS 1.3 suite in
/// `ssl.cipher.suites`, Java negotiates TLS 1.3, and so does this client. The
/// documented ceiling of `org.apache.kafka.common.config.SslConfigs#SSL_PROTOCOL_DOC` is the default-suites case.
///
/// `default_suites` are the TLS provider's suites, rustls's counterpart of the
/// JSSE provider's default-enabled ones. An empty `ssl.enabled.protocols`
/// gives the context's default protocols (rustls's [`rustls::DEFAULT_VERSIONS`]
/// under `TLS` and `TLSv1.3`).
///
/// Java reports an empty result at the handshake (`No appropriate protocol
/// (protocol is disabled or cipher suites are inappropriate)`); here the
/// factory fails to build:
/// - with no enabled cipher suite, which happens under `TLSv1.1`, `TLSv1` and
///   `SSLv3` with an empty `ssl.cipher.suites`: their default suites are the
///   ones usable with TLS 1.1 or lower, and every rustls suite needs TLS 1.2
///   or higher;
/// - with no enabled protocol that an enabled suite can be used with.
///
/// Each error names the keys involved.
fn resolve_tls_parameters(
    protocol: &str,
    enabled_protocols: &[String],
    cipher_suites: &[String],
    default_suites: &[rustls::SupportedCipherSuite],
) -> io::Result<(
    Vec<&'static rustls::SupportedProtocolVersion>,
    Vec<rustls::SupportedCipherSuite>,
)> {
    // Java's order: the context (`SSLContext.getInstance`), then the suites
    // (`setEnabledCipherSuites`), then the protocols (`setEnabledProtocols`).
    let context_versions = context_default_versions(protocol)?;
    let suites: Vec<rustls::SupportedCipherSuite> = if cipher_suites.is_empty() {
        default_suites
            .iter()
            .filter(|suite| context_versions.contains(&suite.version()))
            .copied()
            .collect()
    } else {
        resolve_cipher_suites(cipher_suites)?
    };
    let versions = if enabled_protocols.is_empty() {
        context_versions.to_vec()
    } else {
        resolve_enabled_protocols(enabled_protocols)?
    };
    if suites.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "No cipher suite is enabled: {} is empty and none of the TLS provider's cipher suites can be \
                 used with the protocols of {} = {protocol} (this client implements TLS 1.2 and TLS 1.3 only)",
                SslConfigs::SSL_CIPHER_SUITES_CONFIG,
                SslConfigs::SSL_PROTOCOL_CONFIG,
            ),
        ));
    }
    let active: Vec<_> = versions
        .into_iter()
        .filter(|version| suites.iter().any(|suite| suite.version() == *version))
        .collect();
    if active.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "No TLS protocol of {} = [{}] can be used with the enabled cipher suites ({} = {protocol}, {} = [{}])",
                SslConfigs::SSL_ENABLED_PROTOCOLS_CONFIG,
                enabled_protocols.join(", "),
                SslConfigs::SSL_PROTOCOL_CONFIG,
                SslConfigs::SSL_CIPHER_SUITES_CONFIG,
                cipher_suites.join(", "),
            ),
        ));
    }
    Ok((active, suites))
}

/// Resolves the entries of a non-empty `ssl.enabled.protocols` to rustls
/// `SupportedProtocolVersion` references.
fn resolve_enabled_protocols(
    enabled_protocols: &[String],
) -> io::Result<Vec<&'static rustls::SupportedProtocolVersion>> {
    enabled_protocols
        .iter()
        .map(|proto| match proto.as_str() {
            "TLSv1.3" => Ok(&rustls::version::TLS13),
            "TLSv1.2" => Ok(&rustls::version::TLS12),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Unsupported TLS protocol version: '{other}'. Supported versions: TLSv1.2, TLSv1.3"),
            )),
        })
        .collect()
}

/// Resolves `ssl.cipher.suites` to the TLS provider's cipher suites, in the
/// configured order (Java's `SSLEngine.setEnabledCipherSuites` order is the
/// preference order, `org.apache.kafka.common.security.ssl.DefaultSslEngineFactory#createSslEngine`).
///
/// A name is the Java / IANA one. Every suite of the aws-lc-rs provider
/// ([`rustls::crypto::aws_lc_rs::ALL_CIPHER_SUITES`]) is accepted, as Java's
/// `ssl.cipher.suites` accepts every suite the JSSE provider supports. rustls
/// names the TLS 1.3 suites `TLS13_*` where Java and IANA say `TLS_*`
/// (`TLS_AES_128_GCM_SHA256`), so those names are matched with that prefix.
/// An unknown name fails with `Unsupported CipherSuite: <name>`, the message of
/// Java's `IllegalArgumentException` from `setEnabledCipherSuites`.
fn resolve_cipher_suites(names: &[String]) -> io::Result<Vec<rustls::SupportedCipherSuite>> {
    names
        .iter()
        .map(|name| {
            rustls::crypto::aws_lc_rs::ALL_CIPHER_SUITES
                .iter()
                .find(|suite| {
                    suite
                        .suite()
                        .as_str()
                        .is_some_and(|rustls_name| is_java_cipher_suite_name(rustls_name, name))
                })
                .copied()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("Unsupported CipherSuite: {name}")))
        })
        .collect()
}

/// Whether `java_name` is the Java / IANA name of the rustls suite named
/// `rustls_name`: rustls's `TLS13_` prefix is IANA's `TLS_`.
fn is_java_cipher_suite_name(rustls_name: &str, java_name: &str) -> bool {
    match rustls_name.strip_prefix("TLS13_") {
        Some(rest) => java_name.strip_prefix("TLS_") == Some(rest),
        None => rustls_name == java_name,
    }
}

/// A custom `ServerCertVerifier` that validates the certificate chain but
/// skips hostname verification.
///
/// This is used when `endpoint_identification_algorithm` is empty, matching
/// Java's behavior where setting `ssl.endpoint.identification.algorithm=`
/// disables hostname verification but still validates the certificate chain
/// against the truststore.
///
/// # Safety
///
/// Disabling hostname verification weakens TLS security. An attacker with a
/// valid certificate from a trusted CA (for any hostname) could impersonate
/// the server. This should only be used in development/testing environments
/// or when hostname verification is handled at a different layer.
#[derive(Debug)]
struct NoHostnameVerifier {
    /// The underlying verifier that does standard WebPKI validation.
    inner: Arc<rustls::client::WebPkiServerVerifier>,
}

impl NoHostnameVerifier {
    fn new(roots: RootCertStore) -> Self {
        let inner = rustls::client::WebPkiServerVerifier::builder(Arc::new(roots))
            .build()
            .expect("Failed to build WebPkiServerVerifier");
        Self { inner }
    }
}

impl ServerCertVerifier for NoHostnameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        // Delegate to the inner verifier but with a dummy server name.
        // The inner verifier validates the certificate chain against the root
        // store, but we bypass hostname matching by ignoring any hostname
        // verification errors.
        //
        // We call the inner verifier with the original server name — it will
        // validate the chain (signature, expiry, trust anchor) and then check
        // the hostname. If hostname matching fails, we still accept the cert.
        match self
            .inner
            .verify_server_cert(end_entity, intermediates, _server_name, ocsp_response, now)
        {
            Ok(verified) => Ok(verified),
            // Catch both the legacy `NotValidForName` and the newer
            // `NotValidForNameContext` variant (rustls 0.23.29+).
            // The `WebPkiServerVerifier` produces `NotValidForNameContext`
            // in practice, but we match both for forward/backward compat.
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName)) => {
                Ok(ServerCertVerified::assertion())
            },
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForNameContext { .. })) => {
                Ok(ServerCertVerified::assertion())
            },
            Err(other) => Err(other),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::config::SslConfigs;
    use crate::common::config::types::Password;

    /// Self-signed CA certificate for testing (generated with openssl).
    const TEST_CA_CERT: &str = "\
-----BEGIN CERTIFICATE-----
MIIDCTCCAfGgAwIBAgIUeaxoShuhQMgL+e9XSkpkOYB62B8wDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJVGVzdCBDQSAwMB4XDTI2MDQxMDE1NTEzM1oXDTM2MDQw
NzE1NTEzM1owFDESMBAGA1UEAwwJVGVzdCBDQSAwMIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAnWTUXwFF9rP+ZThZhC2ccJb9cNGBDXo2rBkduFXwT85Z
BslfsTRpEgK9y221F+mJmTb8xqVimT/xol5yQNd0oc27qaY+Wix/jcfwyIlayMky
lIBlfIAmRX2feDS/oZWk6ihtePELvf5u8ERwIf8kh6XlvQ/xH7wY7kKRPUR0LYso
3oPTEcUozf2tL0JiLRLWQJ1/Q2fDdPh/v+egGA1Y1lklO3ajyReyQ+qajUXjEctv
OrMttzeQQ6J8Hm/BSsXFwNCnfxVIpBNvRFJId9she4ydBGQ2gzWkTAsgn+adrZY0
c72Ykis/xxpLCtiKk2Xsf6gk8MStHlL9azOlm4ot8QIDAQABo1MwUTAdBgNVHQ4E
FgQUlBja26GL4PKtj3rCJ5Iwq3+u5a0wHwYDVR0jBBgwFoAUlBja26GL4PKtj3rC
J5Iwq3+u5a0wDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAGnrf
AX1IHz1/KtRzO/8SMXA9efFKODSmo1H+ZTNnP0+RfG1LA+2v1GPCcoS9i05WMfpa
wm98abPq7KBNHvxhFvdui5KCBoN/PEviJUMEk0Fu5nuht0KiqvnKKvPh7PD6v5Oz
Oxt42VeXKNFVtvYIKWW8D0iwrQBnT6CmeqJQbG8S7WeAvkOe9vMBQ6LgY3YI6Jcs
nnaaqxKnerj7Fil1xZE5JgOrkilifVAARp2aXLCiscOfMVFnHxJKznid+veR3LK1
MDk4OUeK2IKuiT7k/GVyM8ljLZvEBRQfNkihCYIk760XmJpcTZ0U/iZ9MOR/BcpW
mjUgssBOOouMAPthmw==
-----END CERTIFICATE-----";

    /// Client certificate for mTLS testing, signed by TEST_CA_CERT.
    const TEST_CLIENT_CERT: &str = "\
-----BEGIN CERTIFICATE-----
MIIC+jCCAeKgAwIBAgIUH4OJqMpyw6s1/MSNeTBVyyZ3tAwwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJVGVzdCBDQSAwMB4XDTI2MDQxMDE1NTE0N1oXDTM2MDQw
NzE1NTE0N1owFjEUMBIGA1UEAwwLVGVzdCBDbGllbnQwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQD2sWsqzWg9rTlTq7CBNoUkoFV2ez3hFe9FgUtqGXZA
aSXkE5z/B+IJMAHYMKApsLo6il/oYB3zvXG5LJB3P7Yj9YfnIJAqZ0IY5mc1r9N8
bnqe6wxvqcxEJUF27eOrLcg5cSDFDRX1bhLlfA6RkWs5MDZChVksdA7q6X0enMxV
S+3D0pcWiDXDllT8XNlyDA1wp/ShD06+uoziNRkWIX238Js6ZY05H/lptGjNwaXL
ngjBZj3IqpnHz7dowrTuMohfHuJdbmXuoFCQottGKj1MyY3hWSeozwPJMnOW5wsM
gx/U5WX1B7uAFwwkP2iEVnzedd+/tWuIsbcXxLAEvSeFAgMBAAGjQjBAMB0GA1Ud
DgQWBBQhhyLz8Swp9WWFlcwxgm1+JSE4gTAfBgNVHSMEGDAWgBSUGNrboYvg8q2P
esInkjCrf67lrTANBgkqhkiG9w0BAQsFAAOCAQEAV/+N+WnYkS5JPHkoNrxR8Byc
qbL4WXv4m3nkya6CPWmUNTzM27Opt8wxQGp1tXAgFTe08nbyYoE2jhjk2tJsrDwa
SRGSSMyqc4PrWeSsSUvNdnz46pdwdnC34Xgd0vndLqMtrasCkhowYFo0t1KZPeyA
2uyCyyW008dWD5qDBc1m0Fc4j7yZijQ1y3LoltK8H9TmRZO/RJc5qkpb+R//5+gV
UK0URE33zZSp2ESKIvJDx7+fbKQcSEBdLMbcKbJ4o7kOFt4o2HL39duRk4X/7umR
h3LWG64YQyNcfvKnF+STY1BBJnk99QDWmMPvDdrp8p7uoPzpw7WiAT/edAe0SA==
-----END CERTIFICATE-----";

    /// Test private key for mTLS testing (matches TEST_CLIENT_CERT).
    const TEST_CLIENT_KEY: &str = "\
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQD2sWsqzWg9rTlT
q7CBNoUkoFV2ez3hFe9FgUtqGXZAaSXkE5z/B+IJMAHYMKApsLo6il/oYB3zvXG5
LJB3P7Yj9YfnIJAqZ0IY5mc1r9N8bnqe6wxvqcxEJUF27eOrLcg5cSDFDRX1bhLl
fA6RkWs5MDZChVksdA7q6X0enMxVS+3D0pcWiDXDllT8XNlyDA1wp/ShD06+uozi
NRkWIX238Js6ZY05H/lptGjNwaXLngjBZj3IqpnHz7dowrTuMohfHuJdbmXuoFCQ
ottGKj1MyY3hWSeozwPJMnOW5wsMgx/U5WX1B7uAFwwkP2iEVnzedd+/tWuIsbcX
xLAEvSeFAgMBAAECggEAWjZbOFXRTuyv0Bcy/q2PVuuDFUbQRCWfUE2N5IjXI4rQ
Hm3FtcWONUnnAsYVa+mC0LGVjQbJLT8T/WF8mv8jflblkfHohnkoRK7NA2b+8bv8
/2x5KcRwPGNbY0BvR0QAunDSSP0WEKBmLKGHOlhxW4Jz9TOKfqUaZ3FnHfC9EFty
E11Agq/UVFl0rsN3kaAMmC9jIcHUB3ndDIl+5qq7nX5CWfvHSxw4/3d5BIGx3/ca
GIU/cVuC+JNZ4O0ClLUD2TI6AkTZ15RvlLtLE3Ob9JSuPUrYL8sPawb8rCGRCRIk
rVw2bRSBzGgQ3/N0Uzp5mVDgvOCFcyyCBIXBt9eG6QKBgQD9RY184qb0YGPk4Ed2
ipQi1YdheVYcBGZbKELLL3xc3woSKS07PRANiTWPMHO6HIbjcduaLYYbSb6PVFXu
9+75bu9rcNxm9iGWXvuzWhlWFq1Nx5q+qrxr72ao8Fn1dw2sB4AgD7drS8MSMmhl
1LxeNoS5kY1BQeTD/eiviZCNjwKBgQD5WblX0OCaiS8NLCsx7Vj1Tn8H1LcxN+03
UcAbGLmn3OAgxIB+hKqMynbwuugeeAanA12wwnD+X7+mSv/Yd0L5wQOHMqezQOSu
8blGOqfHfAFYSNFNFbV00EN5Q8NmRG95JgYOyQSyuTuAV8iQ1QBLAlOHJvyUdPe/
4ad3Wj1XqwKBgQDwNp2BSz7qHNnh5E4jQkBJ4ZfrfTeMjye9YawoJjufofNdUiyS
ONIW5IIl8uBwLkpJQl30FyVQkFrqeiSe6AyCCxONJZgFF4C3rBKyAsxw+EUatiww
lqLrBD6sEHph867F8L82qXFflJXJloGpw2F9QdwUXNZKhILC2PluM90kRQKBgGSb
wj/fhLB1x6lN+APGG42m5XR4bI4MXcdjUdrdCBPl9/zgrGPgDZyPGJybHYslrLF4
lzX5znOkmIR1YHOr8zconM7RLn8SIPNBjxr6EbZYn4ZKo0CyEKwYWBE2uUGrPTsp
j2opy559RLfNM5zUhLC/OIqgvWr9IvWmC9cJbxTPAoGBAKGodr8CFad7nsl9x0cH
Dlqddi21TOtLHuQuxJqlASFSCzYesT3OIQz7zxb8jS6ESF3YmwZLzTX+GD7TxWcZ
AW5Pv9kQ+0OdOQ+xjJfk9XN4iPkWupwYtLPM3LY1tleJVEf9ZNgXr9e6zkY+iMW+
B2V9lhUZNk+pRjtJw9unpXsM
-----END PRIVATE KEY-----";

    #[test]
    fn test_build_with_system_roots() {
        let config = SslConfigs::default();
        let factory = SslFactory::new(&config).unwrap();
        assert!(factory.hostname_verification());
    }

    #[test]
    fn test_build_with_inline_truststore() {
        let config = SslConfigs {
            truststore_certificates: Some(Password::new(TEST_CA_CERT)),
            ..SslConfigs::default()
        };
        let factory = SslFactory::new(&config).unwrap();
        assert!(factory.hostname_verification());
    }

    #[test]
    fn test_build_with_truststore_file() {
        // Write test cert to a temp file
        let dir = std::env::temp_dir().join("kafka_ssl_test");
        std::fs::create_dir_all(&dir).unwrap();
        let cert_path = dir.join("test_ca.pem");
        std::fs::write(&cert_path, TEST_CA_CERT).unwrap();

        let config = SslConfigs {
            truststore_location: Some(cert_path.to_str().unwrap().to_string()),
            ..SslConfigs::default()
        };
        let factory = SslFactory::new(&config).unwrap();
        assert!(factory.hostname_verification());

        // Cleanup
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_build_with_client_cert() {
        let config = SslConfigs {
            truststore_certificates: Some(Password::new(TEST_CA_CERT)),
            keystore_certificate_chain: Some(Password::new(TEST_CLIENT_CERT)),
            keystore_key: Some(Password::new(TEST_CLIENT_KEY)),
            ..SslConfigs::default()
        };
        let factory = SslFactory::new(&config);
        assert!(factory.is_ok(), "Should build with client cert: {:?}", factory.err());
    }

    #[test]
    fn test_unsupported_jks_format() {
        let config = SslConfigs { truststore_type: "JKS".to_string(), ..SslConfigs::default() };
        let result = SslFactory::new(&config);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("JKS format is not supported"),
            "Error should mention JKS: {}",
            err
        );
    }

    #[test]
    fn test_unsupported_pkcs12_format() {
        let config = SslConfigs { keystore_type: "PKCS12".to_string(), ..SslConfigs::default() };
        let result = SslFactory::new(&config);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("PKCS12 format is not supported"),
            "Error should mention PKCS12: {}",
            err
        );
    }

    #[test]
    fn test_hostname_verification_enabled() {
        let config = SslConfigs { endpoint_identification_algorithm: "https".to_string(), ..SslConfigs::default() };
        let factory = SslFactory::new(&config).unwrap();
        assert!(factory.hostname_verification());
    }

    #[test]
    fn test_hostname_verification_disabled() {
        let config = SslConfigs { endpoint_identification_algorithm: String::new(), ..SslConfigs::default() };
        let factory = SslFactory::new(&config).unwrap();
        assert!(!factory.hostname_verification());
    }

    #[test]
    fn test_server_name_dns() {
        let name = SslFactory::create_server_name("kafka.example.com").unwrap();
        assert!(matches!(name, ServerName::DnsName(_)));
    }

    #[test]
    fn test_server_name_ip() {
        let name = SslFactory::create_server_name("192.168.1.1").unwrap();
        assert!(matches!(name, ServerName::IpAddress(_)));
    }

    #[test]
    fn test_tls_version_configuration() {
        // Only TLSv1.3
        let config = SslConfigs { enabled_protocols: vec!["TLSv1.3".to_string()], ..SslConfigs::default() };
        let factory = SslFactory::new(&config);
        assert!(factory.is_ok());

        // Only TLSv1.2
        let config = SslConfigs { enabled_protocols: vec!["TLSv1.2".to_string()], ..SslConfigs::default() };
        let factory = SslFactory::new(&config);
        assert!(factory.is_ok());

        // Unsupported version
        let config = SslConfigs { enabled_protocols: vec!["TLSv1.1".to_string()], ..SslConfigs::default() };
        let result = SslFactory::new(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unsupported TLS protocol version"));
    }

    /// An empty `ssl.enabled.protocols` passes config validation (Java's
    /// `ValidList.anyNonDuplicateValues(true, false)` allows it) and means
    /// the `ssl.protocol` context's default protocols, as Java's
    /// `DefaultSslEngineFactory` does, rather than failing every TLS
    /// connection.
    #[test]
    fn test_empty_enabled_protocols_uses_provider_defaults() {
        assert_eq!(resolve("TLSv1.3", &[], &[]).unwrap(), rustls::DEFAULT_VERSIONS.to_vec());

        let config = SslConfigs::parse_for_test(&[("ssl.enabled.protocols", "")]).unwrap();
        assert!(config.enabled_protocols.is_empty());
        assert!(SslFactory::new(&config).is_ok());
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| (*v).to_string()).collect()
    }

    /// [`resolve_tls_parameters`] over the aws-lc-rs provider's suites, the
    /// ones [`SslFactory::new`] installs: the TLS versions a connection may use.
    fn resolve(
        protocol: &str,
        enabled: &[&str],
        suites: &[&str],
    ) -> io::Result<Vec<&'static rustls::SupportedProtocolVersion>> {
        let defaults = rustls::crypto::aws_lc_rs::default_provider().cipher_suites;
        resolve_tls_parameters(protocol, &strings(enabled), &strings(suites), &defaults).map(|(versions, _)| versions)
    }

    const BOTH: &[&str] = &["TLSv1.2", "TLSv1.3"];
    const T13: &str = "TLS_AES_128_GCM_SHA256";
    const T12: &str = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256";

    fn tls12() -> Vec<&'static rustls::SupportedProtocolVersion> {
        vec![&rustls::version::TLS12]
    }

    fn tls13() -> Vec<&'static rustls::SupportedProtocolVersion> {
        vec![&rustls::version::TLS13]
    }

    fn tls12_and_13() -> Vec<&'static rustls::SupportedProtocolVersion> {
        vec![&rustls::version::TLS12, &rustls::version::TLS13]
    }

    fn assert_no_protocol(result: io::Result<Vec<&'static rustls::SupportedProtocolVersion>>, message: &str) {
        let err = result.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(err.to_string(), message);
    }

    /// The cases of a JDK 25 (Corretto) probe that builds the client context
    /// with `SSLContext.getInstance(ssl.protocol)`, sets the cipher suites and
    /// then the protocols as `DefaultSslEngineFactory.createSslEngine` does
    /// (`org.apache.kafka.common.security.ssl.DefaultSslEngineFactory#createSslEngine`), and handshakes with a JSSE server offering TLS 1.2 and TLS
    /// 1.3 with an RSA certificate. Each comment gives the protocol Java
    /// negotiated; the versions asserted are the ones this client may use, of
    /// which rustls negotiates the highest. `T13` is `TLS_AES_128_GCM_SHA256`,
    /// `T12` `TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256`.
    #[test]
    fn test_ssl_protocol_follows_jsse_context_defaults() {
        // 01: Java negotiates TLSv1.2: the TLSv1.2 context's default suites
        // leave TLS 1.3 out.
        assert_eq!(resolve("TLSv1.2", BOTH, &[]).unwrap(), tls12());
        // 02: the same, the name matched ignoring case.
        assert_eq!(resolve("tlsv1.2", BOTH, &[]).unwrap(), tls12());
        // 03, 04: Java negotiates TLSv1.3.
        assert_eq!(resolve("TLSv1.3", BOTH, &[]).unwrap(), tls12_and_13());
        assert_eq!(resolve("TLS", BOTH, &[]).unwrap(), tls12_and_13());
        // 11, 12: Java negotiates TLSv1.3: a TLS 1.3 suite in
        // `ssl.cipher.suites` replaces the context's defaults, whatever its
        // position.
        assert_eq!(resolve("TLSv1.2", BOTH, &[T13, T12]).unwrap(), tls12_and_13());
        assert_eq!(resolve("TLSv1.2", BOTH, &[T12, T13]).unwrap(), tls12_and_13());
        // 13: Java negotiates TLSv1.3.
        assert_eq!(resolve("TLSv1.2", BOTH, &[T13]).unwrap(), tls13());
        // 15: Java negotiates TLSv1.3: `SSL` is an alias of `TLS`.
        assert_eq!(resolve("SSL", BOTH, &[]).unwrap(), tls12_and_13());
        // 27: the same in lower case.
        assert_eq!(resolve("ssl", BOTH, &[]).unwrap(), tls12_and_13());
        // 17: Java negotiates TLSv1.3: the TLSv1.1 context's defaults are
        // both replaced.
        assert_eq!(resolve("TLSv1.1", BOTH, &[T13, T12]).unwrap(), tls12_and_13());
        // 23, 24, 31: Java negotiates TLSv1.3; `SSLv3` is an alias of `TLSv1`.
        assert_eq!(resolve("SSLv3", BOTH, &[T13, T12]).unwrap(), tls12_and_13());
        assert_eq!(resolve("TLSv1", BOTH, &[T13, T12]).unwrap(), tls12_and_13());
        assert_eq!(resolve("sslv3", BOTH, &[T13, T12]).unwrap(), tls12_and_13());
        // 20: Java negotiates TLSv1.2.
        assert_eq!(resolve("TLSv1.2", BOTH, &[T12]).unwrap(), tls12());
        // 21: Java negotiates TLSv1.2: the TLSv1.2 context's default protocols.
        assert_eq!(resolve("TLSv1.2", &[], &[]).unwrap(), tls12());
        // 22, 30: Java negotiates TLSv1.3.
        assert_eq!(resolve("TLS", &[], &[]).unwrap(), rustls::DEFAULT_VERSIONS.to_vec());
        assert_eq!(resolve("TLSv1.3", &[], &[]).unwrap(), rustls::DEFAULT_VERSIONS.to_vec());
        // 26: Java negotiates TLSv1.3.
        assert_eq!(resolve("TLSv1.2", &["TLSv1.3"], &[T13]).unwrap(), tls13());
    }

    /// The probe's failing cases. Java fails each at the handshake with `No
    /// appropriate protocol (protocol is disabled or cipher suites are
    /// inappropriate)`; here the factory fails to build, naming the keys.
    #[test]
    fn test_ssl_protocol_without_usable_protocol_fails() {
        // 05: the TLSv1.2 context's default suites cannot be used with TLS 1.3.
        assert_no_protocol(
            resolve("TLSv1.2", &["TLSv1.3"], &[]),
            "No TLS protocol of ssl.enabled.protocols = [TLSv1.3] can be used with the enabled cipher suites \
             (ssl.protocol = TLSv1.2, ssl.cipher.suites = [])",
        );
        // 06, 29: no enabled suite can be used with the enabled protocol.
        assert_no_protocol(
            resolve("TLSv1.3", &["TLSv1.3"], &[T12]),
            "No TLS protocol of ssl.enabled.protocols = [TLSv1.3] can be used with the enabled cipher suites \
             (ssl.protocol = TLSv1.3, ssl.cipher.suites = [TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256])",
        );
        assert_no_protocol(
            resolve("TLSv1.3", &["TLSv1.2"], &[T13]),
            "No TLS protocol of ssl.enabled.protocols = [TLSv1.2] can be used with the enabled cipher suites \
             (ssl.protocol = TLSv1.3, ssl.cipher.suites = [TLS_AES_128_GCM_SHA256])",
        );
        // 14: the TLSv1.2 context's default protocols leave TLS 1.3 out.
        assert_no_protocol(
            resolve("TLSv1.2", &[], &[T13]),
            "No TLS protocol of ssl.enabled.protocols = [] can be used with the enabled cipher suites \
             (ssl.protocol = TLSv1.2, ssl.cipher.suites = [TLS_AES_128_GCM_SHA256])",
        );
        // 25: the TLSv1.1 context's default protocols are TLS 1.1 and lower.
        assert_no_protocol(
            resolve("TLSv1.1", &[], &[T13, T12]),
            "No TLS protocol of ssl.enabled.protocols = [] can be used with the enabled cipher suites \
             (ssl.protocol = TLSv1.1, ssl.cipher.suites = [TLS_AES_128_GCM_SHA256, \
             TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256])",
        );
        // 16, 18, 19: the default suites of these contexts are the ones usable
        // with TLS 1.1 or lower; rustls has none.
        for protocol in ["TLSv1.1", "TLSv1", "SSLv3"] {
            assert_no_protocol(
                resolve(protocol, BOTH, &[]),
                &format!(
                    "No cipher suite is enabled: ssl.cipher.suites is empty and none of the TLS provider's cipher \
                     suites can be used with the protocols of ssl.protocol = {protocol} (this client implements \
                     TLS 1.2 and TLS 1.3 only)"
                ),
            );
        }
    }

    /// The factory applies the resolution: `TLSv1.2` with a TLS 1.3 suite
    /// keeps TLS 1.3 (case 13), `TLSv1.2` with the default suites restricts
    /// the provider to its TLS 1.2 suites (case 01), and `SSL` builds (case
    /// 15).
    #[test]
    fn test_factory_applies_ssl_protocol_context_defaults() {
        let config = SslConfigs::parse_for_test(&[("ssl.protocol", "TLSv1.2"), ("ssl.cipher.suites", T13)]).unwrap();
        let factory = SslFactory::new(&config).unwrap();
        assert_eq!(
            factory
                .client_config()
                .crypto_provider()
                .cipher_suites
                .iter()
                .map(|s| s.suite())
                .collect::<Vec<_>>(),
            [rustls::CipherSuite::TLS13_AES_128_GCM_SHA256]
        );

        let config = SslConfigs::parse_for_test(&[("ssl.protocol", "TLSv1.2")]).unwrap();
        let factory = SslFactory::new(&config).unwrap();
        let suites = &factory.client_config().crypto_provider().cipher_suites;
        assert!(!suites.is_empty());
        assert!(suites.iter().all(|s| s.version() == &rustls::version::TLS12), "{suites:?}");

        let config = SslConfigs::parse_for_test(&[("ssl.protocol", "SSL")]).unwrap();
        assert!(SslFactory::new(&config).is_ok());

        let config = SslConfigs::parse_for_test(&[("ssl.protocol", "TLSv1.1")]).unwrap();
        assert_eq!(
            SslFactory::new(&config).unwrap_err().to_string(),
            "No cipher suite is enabled: ssl.cipher.suites is empty and none of the TLS provider's cipher suites \
             can be used with the protocols of ssl.protocol = TLSv1.1 (this client implements TLS 1.2 and TLS \
             1.3 only)"
        );
    }

    /// A name the JDK has no client `SSLContext` for fails, as
    /// `SSLContext.getInstance` throws `NoSuchAlgorithmException` (probe case
    /// 10, `TLSv9`). `Default` fails too: Java refuses to `init` it (case
    /// 09), and the DTLS contexts cannot serve a TCP connection (case 28).
    #[test]
    fn test_unsupported_protocol_fails() {
        for protocol in ["TLSv9", "Default", "DTLS", "DTLSv1.2", ""] {
            let err = resolve(protocol, &["TLSv1.2"], &[]).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(
                err.to_string(),
                format!(
                    "Unsupported TLS protocol: '{protocol}'. \
                     Supported protocols: TLSv1.3, TLSv1.2, TLSv1.1, TLSv1, SSLv3, TLS, SSL"
                )
            );
        }
        let config = SslConfigs::parse_for_test(&[("ssl.protocol", "TLSv9")]).unwrap();
        assert_eq!(
            SslFactory::new(&config).unwrap_err().to_string(),
            "Unsupported TLS protocol: 'TLSv9'. Supported protocols: TLSv1.3, TLSv1.2, TLSv1.1, TLSv1, SSLv3, TLS, SSL"
        );
    }

    /// The factory reports a protocol no enabled suite can be used with when
    /// it is built (probe case 05; Java fails at the handshake).
    #[test]
    fn test_protocol_ceiling_leaving_no_version_fails() {
        let config =
            SslConfigs::parse_for_test(&[("ssl.protocol", "TLSv1.2"), ("ssl.enabled.protocols", "TLSv1.3")]).unwrap();
        let err = SslFactory::new(&config).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            err.to_string(),
            "No TLS protocol of ssl.enabled.protocols = [TLSv1.3] can be used with the enabled cipher suites \
             (ssl.protocol = TLSv1.2, ssl.cipher.suites = [])"
        );
    }

    /// Every suite of the aws-lc-rs provider resolves by its Java / IANA name.
    #[test]
    fn test_every_provider_cipher_suite_resolves_by_java_name() {
        let names = [
            "TLS_AES_256_GCM_SHA384",
            "TLS_AES_128_GCM_SHA256",
            "TLS_CHACHA20_POLY1305_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
        ];
        assert_eq!(names.len(), rustls::crypto::aws_lc_rs::ALL_CIPHER_SUITES.len());
        for name in names {
            let suites = resolve_cipher_suites(&strings(&[name])).unwrap();
            assert_eq!(suites.len(), 1, "{name}");
            assert!(rustls::crypto::aws_lc_rs::ALL_CIPHER_SUITES.contains(&suites[0]), "{name}");
        }
        let all = resolve_cipher_suites(&strings(&names)).unwrap();
        let mut provider_suites = rustls::crypto::aws_lc_rs::ALL_CIPHER_SUITES.to_vec();
        provider_suites.sort_by_key(|s| u16::from(s.suite()));
        let mut resolved = all.clone();
        resolved.sort_by_key(|s| u16::from(s.suite()));
        assert_eq!(resolved, provider_suites);
    }

    #[test]
    fn test_cipher_suites_keep_configured_order() {
        let names = [
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_CHACHA20_POLY1305_SHA256",
            "TLS_AES_256_GCM_SHA384",
        ];
        let suites = resolve_cipher_suites(&strings(&names)).unwrap();
        let expected = [
            rustls::CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
            rustls::CipherSuite::TLS13_CHACHA20_POLY1305_SHA256,
            rustls::CipherSuite::TLS13_AES_256_GCM_SHA384,
        ];
        assert_eq!(suites.iter().map(|s| s.suite()).collect::<Vec<_>>(), expected);

        let config = SslConfigs { cipher_suites: strings(&names), ..SslConfigs::default() };
        let factory = SslFactory::new(&config).unwrap();
        assert_eq!(
            factory
                .client_config()
                .crypto_provider()
                .cipher_suites
                .iter()
                .map(|s| s.suite())
                .collect::<Vec<_>>(),
            expected
        );
    }

    /// An unknown name fails with the message of Java's
    /// `setEnabledCipherSuites`; rustls's own `TLS13_` spelling is not a
    /// Java name.
    #[test]
    fn test_unknown_cipher_suite_fails() {
        for name in [
            "TLS_RSA_WITH_AES_128_CBC_SHA",
            "TLS13_AES_128_GCM_SHA256",
            "tls_aes_128_gcm_sha256",
        ] {
            let config = SslConfigs {
                cipher_suites: strings(&["TLS_AES_128_GCM_SHA256", name]),
                ..SslConfigs::default()
            };
            let err = SslFactory::new(&config).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(err.to_string(), format!("Unsupported CipherSuite: {name}"));
        }
    }

    /// TLS 1.2-only suites with only TLS 1.3 enabled leave no usable
    /// protocol (probe case 06; Java fails at the handshake).
    #[test]
    fn test_cipher_suites_unusable_with_enabled_protocols_fail() {
        let config = SslConfigs::parse_for_test(&[
            ("ssl.enabled.protocols", "TLSv1.3"),
            ("ssl.cipher.suites", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"),
        ])
        .unwrap();
        let err = SslFactory::new(&config).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            err.to_string(),
            "No TLS protocol of ssl.enabled.protocols = [TLSv1.3] can be used with the enabled cipher suites \
             (ssl.protocol = TLSv1.3, ssl.cipher.suites = [TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256])"
        );
    }

    /// An empty `ssl.cipher.suites` keeps the installed provider.
    #[test]
    fn test_empty_cipher_suites_keep_provider_defaults() {
        let factory = SslFactory::new(&SslConfigs::default()).unwrap();
        let installed = rustls::crypto::CryptoProvider::get_default().unwrap();
        assert!(Arc::ptr_eq(factory.client_config().crypto_provider(), installed));
    }

    #[test]
    fn test_invalid_pem_truststore() {
        let config = SslConfigs {
            truststore_certificates: Some(Password::new("not a valid PEM")),
            ..SslConfigs::default()
        };
        let result = SslFactory::new(&config);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("No certificates found"),
            "Should report no certs found in invalid PEM"
        );
    }

    #[test]
    fn test_cert_without_key_error() {
        let config = SslConfigs {
            keystore_certificate_chain: Some(Password::new(TEST_CLIENT_CERT)),
            // No key provided
            ..SslConfigs::default()
        };
        let result = SslFactory::new(&config);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("no private key"),
            "Should report missing key"
        );
    }

    #[test]
    fn test_missing_truststore_file() {
        let config = SslConfigs {
            truststore_location: Some("/nonexistent/path/truststore.pem".to_string()),
            ..SslConfigs::default()
        };
        let result = SslFactory::new(&config);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("Failed to open truststore"),
            "Should report file not found"
        );
    }

    /// PKCS#1 (traditional) RSA private key in a `RSA PRIVATE KEY` section.
    /// Test-only throwaway key generated once with `openssl genrsa -traditional
    /// 2048`; never used outside these tests. Exercises the PKCS#1 branch of
    /// `PrivateKeyDer`'s `PemObject` impl.
    const TEST_CLIENT_KEY_PKCS1: &str = "\
-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAubuKKSCCpGdlIjRDTtqMh9PA/kqJqnlZP7XdM7gmOkS+ZJmv
VpHVN3J/+L4jlX1UeyCqjC9U/035FMO7WHdQuRwuCkUsjdSgzUnnJQhTAZva8Rro
cfweMnYf2l81RH2+L3D8PWhfX6Rr5GdaCenfXypzvW8wpP73WrNsFpakP01MMJ/6
nGdAkzl06FI0U3EMFTJIHygs8NV5XEBbK9F7E9dxKOQ82y0n2EaN6bKWiSaXYX48
ln5fJP3xXEEciUJW8xWcz0hfuq+fv89S1QloXLtuKL6MZxj0NH3T5DPX7MokTtQm
ip+3o6aHBzhMxUmdxtlc1iwF/E5otu1WrxoX7QIDAQABAoIBACscl0gPGgwHzP+Y
woPf8m1Hz30nN5fItZj2gasy5DqzwL7wtnShgqytaPuHxRI6xnyTG0tRZEkjo/y4
Jzk0btoc6qnrHxmn4JBuRlc//KZ7syEyhgCJTUgYx7GEVoS4rEfuUbp+St8UNxAM
FmgFL5pzK+ztjYzK+wcEbUM8AdGIXcVHbpX2jOETUQ2EH/wr6krPPlq+bA2RUnU2
FW+pDXoOfbKs/6t8Elc8BpywdaO0PCEAkTZX0Yw1pas5zmvtdrSl2uT2+r21sSZf
eOiX741i4DwwiG7xvoQqerllbDbxd3q4YhtvZMeWjdFBXG4lcSK8J677Tz95cefm
nuYrxTkCgYEA7+dIEGCfPBXDliuarkLmpYFZHyacg9TBAQtNLHhnRucWIBgTI8bo
rRbQbQkSOfO4W86PjrGqcOPsTvKw+k2cxKH7L/7EJaRYX2Dtu7jPcPsWBF+y7P1A
zo0UqJx8ppCrwHpNwuo68UcD/mNEgMpNQt5VZukvhcbcnjV8UmP8tfUCgYEAxjHJ
2LMvmqUUdtySASKagZeLtlspDY4jM/Qy3yxnYw2X6xpa8oZ3VHP33uOcvgBbaJlT
/e1+eRStFf9EFUM+GVjfbpBCQ5NMT6D6Ox7A2mJ3viMmOgA2eMNiVql5biFFPFn6
ROrhh5ZlwCpLuQouDJxJI0n6DXeMR/tZ5/hkJxkCgYBLwXYBLfG6kIjVp+lNc/TP
eJwC+TddbdaQlNgG3fKG8jwLYG7S0mHxHo4skDvALxUdcInmnnXa9i5N89ctA0so
0Jm4WzPnSxFY9Qu9RqvC2pbmRK4EpihL0UMfx2KZWn/R5Q0fKsX4INvC/efI35ks
L3c6axJFpTlPgEZE4iwP7QKBgQC2kx7x9P8hYnkblc0aJppWTxhxAEEtL4bWUVqb
o/gJk3gjJ8AoMh6zObFZLiwyC3opPTXWcXVUbOuVm/VH8Q2/o9MPsuyX+5UMtapN
0+KsSpEsIo0QWSnUzYhFdRFblmKFWg3mdskTckSBZXUkHHCXRVFKpNcomLwtgyor
OvJn4QKBgBnSC1Xv9gZTyryVZGlHrJ8cM4rRKk6G3bCFSMAVHT618THe7MNNBaBu
IKmDn39m0ACwVR/L3Mniml8IbRSJO4+DesGGNHOKpXw2XaKlEUsuhXN1ilpC/pfC
E3avyzZb2N0jTRBQXmC0k1x6FCcofIg6QP/+E/QfFFyuC3CbjmsE
-----END RSA PRIVATE KEY-----";

    /// SEC1 EC private key in an `EC PRIVATE KEY` section. Test-only throwaway
    /// key generated once with `openssl ecparam -name prime256v1 -genkey
    /// -noout`; never used outside these tests. Exercises the SEC1 branch of
    /// `PrivateKeyDer`'s `PemObject` impl.
    const TEST_CLIENT_KEY_SEC1: &str = "\
-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIAmY54vNSbYzPMGQBAio3CeIhAGQi0gyMfWUAZ9xm4wzoAoGCCqGSM49
AwEHoUQDQgAE90/rEfdSHQxK8WDAhYBjwSAJIfP1IZzU7Rt3A4Gtk6b0GCmzArRT
uviKbp188irqAMaMg47Y9WVEjN8mjQM5ag==
-----END EC PRIVATE KEY-----";

    #[test]
    fn test_load_client_identity_from_keystore_file() {
        // A single PEM file holding both the client cert and its key exercises
        // the `keystore_location` path, where certs are read skipping the key
        // section and the key is read skipping the cert section — the semantics
        // the removed rustls-pemfile crate provided, now provided directly by
        // `PemObject`'s iterators.
        let dir = std::env::temp_dir().join("kafka_ssl_test_keystore_file");
        std::fs::create_dir_all(&dir).unwrap();

        // Cert first, then key.
        let cert_then_key = dir.join("cert_then_key.pem");
        std::fs::write(&cert_then_key, format!("{TEST_CLIENT_CERT}\n{TEST_CLIENT_KEY}")).unwrap();
        let config = SslConfigs {
            keystore_location: Some(cert_then_key.to_str().unwrap().to_string()),
            ..SslConfigs::default()
        };
        let (certs, _key) = load_client_identity(&config)
            .unwrap()
            .expect("cert+key file should yield a client identity");
        assert_eq!(certs.len(), 1, "should read exactly one cert, skipping the key section");

        // Key first, then cert (reverse order) — reading must be order-independent.
        let key_then_cert = dir.join("key_then_cert.pem");
        std::fs::write(&key_then_cert, format!("{TEST_CLIENT_KEY}\n{TEST_CLIENT_CERT}")).unwrap();
        let config = SslConfigs {
            keystore_location: Some(key_then_cert.to_str().unwrap().to_string()),
            ..SslConfigs::default()
        };
        let (certs, _key) = load_client_identity(&config)
            .unwrap()
            .expect("key+cert file should yield a client identity");
        assert_eq!(certs.len(), 1, "cert reading must skip the key section regardless of order");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_malformed_pem_missing_end_marker() {
        // A BEGIN line with no matching END line is a parse error (not an empty
        // result), so it exercises the `map_err` branch rather than the
        // `is_empty()` branch.
        let malformed = "-----BEGIN CERTIFICATE-----\nMIIC+jCCAeKgAwIBAgIUH4OJqMpyw6s1/MSNeTBVyyZ3tAww\n";
        let config = SslConfigs { truststore_certificates: Some(Password::new(malformed)), ..SslConfigs::default() };
        let err = SslFactory::new(&config).unwrap_err().to_string();
        // Our prefix is byte-identical to the pre-migration message; only the
        // interpolated suffix is library-owned text, now sourced from
        // `rustls::pki_types::pem::Error` ("missing section end marker").
        assert!(
            err.starts_with("Failed to parse inline truststore certificates: "),
            "unexpected error prefix: {err}"
        );
        assert!(
            err.contains("missing section end marker"),
            "expected pki-types 'missing section end marker' text: {err}"
        );
    }

    #[test]
    fn test_keystore_key_with_no_key_section() {
        // Passing a cert PEM as the key must skip the cert section, find no key,
        // and produce our exact "No private key found" message — the `Ok(None)`
        // path that `.next().transpose()` preserves (not a parse error).
        let config = SslConfigs {
            keystore_certificate_chain: Some(Password::new(TEST_CLIENT_CERT)),
            keystore_key: Some(Password::new(TEST_CLIENT_CERT)),
            ..SslConfigs::default()
        };
        let err = load_client_identity(&config).unwrap_err();
        assert_eq!(err.to_string(), "No private key found in inline keystore key");
    }

    /// Encrypted PKCS#8 private key (`openssl pkcs8 -topk8 -v2 aes-256-cbc`
    /// over a throwaway P-256 key, test-only), the form Java decrypts with
    /// `ssl.key.password` (`org.apache.kafka.common.security.ssl.DefaultSslEngineFactory.PemStore#privateKey`).
    const TEST_ENCRYPTED_PKCS8_KEY: &str = "\
-----BEGIN ENCRYPTED PRIVATE KEY-----
MIH0MF8GCSqGSIb3DQEFDTBSMDEGCSqGSIb3DQEFDDAkBBChEvfxzqdTnTzntfu+
/JNkAgIIADAMBggqhkiG9w0CCQUAMB0GCWCGSAFlAwQBKgQQnLCb4V4KZtTCKIhZ
9yXZ7wSBkCLHwAv0KMXYwGPsegoyjbzjMqy9/wkGNO/R4N6pN1b1eqQiBx+V9aaC
zP4ok3TCR4yUhO6VeiPIx+Di6mWV74UaekldPQ6VWGQZC4HgG5/mMyFv+nPXF3/5
9/Onw4P8LvTiGdSqlJedaHo3rIqkoPlTfpyr7TqYZv1vQe8KOVvx+kEwi2uGVxUw
1nFxoNqAcg==
-----END ENCRYPTED PRIVATE KEY-----";

    /// Legacy OpenSSL encrypted key (`openssl genrsa -traditional -aes128`,
    /// throwaway 512-bit, test-only): an `RSA PRIVATE KEY` section with
    /// `Proc-Type: 4,ENCRYPTED` and `DEK-Info` headers.
    const TEST_ENCRYPTED_LEGACY_KEY: &str = "\
-----BEGIN RSA PRIVATE KEY-----
Proc-Type: 4,ENCRYPTED
DEK-Info: AES-128-CBC,CF9AF4AB2F444E8C246411640F3CF3AD

hpSwOZCb7kAJ8APc2ddKf/HujxTmR/RVcxz6zHiFVN4H1NyOX/EMTWUyCVhl4Ktd
XXS+DvBFndAsFUvCYy57jF+x2LdlRDntmdMuWzt17VuOl10Azwe7UrgCXAJaqRto
Kl6q8xZ6LkBVjPHCMzj9mVEFClpBSVihPA/IyiknTgH3WzRW0CY0iqccckpM8uZo
VH0vfjR8QYm5Bh2O64nFBY/7eyJxsaLOFOvUuexNLXQY5Dd3EvmXGG2QAb+CmMdP
RPzEy/xVAdxrIFRm1dvwbNVNSSWhRnjOnKgpSalAQbMtUavP9yhn6JL92udU3qwP
8gtGUJLTV5nrN/fLQju85ndXGIPJ6tUtlBz2mpFlodJBN0bvkxtuyzdr5LLk32t4
0LjkDh76Pb2/CFv8rmSdPW4DRinxozSNWbyzBv18/s4=
-----END RSA PRIVATE KEY-----";

    /// An encrypted inline `ssl.keystore.key`, in either form, fails with
    /// the encrypted-key message rather than `No private key found` (PKCS#8)
    /// or a base64 parse error (legacy).
    #[test]
    fn test_encrypted_inline_keystore_key_rejected() {
        for key in [TEST_ENCRYPTED_PKCS8_KEY, TEST_ENCRYPTED_LEGACY_KEY] {
            let config = SslConfigs {
                keystore_certificate_chain: Some(Password::new(TEST_CLIENT_CERT)),
                keystore_key: Some(Password::new(key)),
                ..SslConfigs::default()
            };
            let err = SslFactory::new(&config).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{key}");
            assert_eq!(
                err.to_string(),
                "Encrypted PEM private keys are not supported by this client; provide an unencrypted PKCS#8 key",
                "{key}"
            );
        }
    }

    /// An encrypted key in the `ssl.keystore.location` file, in either form,
    /// fails with the same message.
    #[test]
    fn test_encrypted_keystore_file_key_rejected() {
        let dir = std::env::temp_dir().join("kafka_ssl_test_encrypted_keystore_file");
        std::fs::create_dir_all(&dir).unwrap();
        for (name, key) in [
            ("pkcs8.pem", TEST_ENCRYPTED_PKCS8_KEY),
            ("legacy.pem", TEST_ENCRYPTED_LEGACY_KEY),
        ] {
            let path = dir.join(name);
            std::fs::write(&path, format!("{TEST_CLIENT_CERT}\n{key}")).unwrap();
            let config = SslConfigs {
                keystore_location: Some(path.to_str().unwrap().to_string()),
                ..SslConfigs::default()
            };
            let err = SslFactory::new(&config).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{name}");
            assert_eq!(
                err.to_string(),
                "Encrypted PEM private keys are not supported by this client; provide an unencrypted PKCS#8 key",
                "{name}"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The check looks at the PEM text only: an unencrypted key, a key file
    /// with no key and a certificate are not encrypted keys.
    #[test]
    fn test_is_encrypted_pem_key() {
        assert!(is_encrypted_pem_key(TEST_ENCRYPTED_PKCS8_KEY.as_bytes()));
        assert!(is_encrypted_pem_key(TEST_ENCRYPTED_LEGACY_KEY.replace('\n', "\r\n").as_bytes()));
        assert!(!is_encrypted_pem_key(TEST_CLIENT_KEY.as_bytes()));
        assert!(!is_encrypted_pem_key(TEST_CLIENT_CERT.as_bytes()));
        assert!(!is_encrypted_pem_key(b""));
    }

    #[test]
    fn test_all_private_key_encodings_accepted() {
        // PKCS#8, PKCS#1 and SEC1 keys must each load into the matching
        // `PrivateKeyDer` variant — the same three kinds the old
        // `rustls_pemfile::private_key` accepted. Parsing does not check the key
        // against the cert chain, so any cert works as the chain.
        let load = |key_pem: &str| -> PrivateKeyDer<'static> {
            let config = SslConfigs {
                keystore_certificate_chain: Some(Password::new(TEST_CLIENT_CERT)),
                keystore_key: Some(Password::new(key_pem)),
                ..SslConfigs::default()
            };
            let (_certs, key) = load_client_identity(&config).unwrap().unwrap();
            key
        };
        assert!(matches!(load(TEST_CLIENT_KEY), PrivateKeyDer::Pkcs8(_)), "PKCS#8 (PRIVATE KEY)");
        assert!(
            matches!(load(TEST_CLIENT_KEY_PKCS1), PrivateKeyDer::Pkcs1(_)),
            "PKCS#1 (RSA PRIVATE KEY)"
        );
        assert!(
            matches!(load(TEST_CLIENT_KEY_SEC1), PrivateKeyDer::Sec1(_)),
            "SEC1 (EC PRIVATE KEY)"
        );
    }

    /// CA certificate for hostname verification tests.
    /// This CA signs the server certificate below.
    const TEST_SERVER_CA_CERT: &str = "\
-----BEGIN CERTIFICATE-----
MIIDCTCCAfGgAwIBAgIUC5R8mdpVjJEz/FruPGeJ/ikTAnMwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJVGVzdCBDQSAwMB4XDTI2MDQxMDE2NDY1M1oXDTM2MDQw
NzE2NDY1M1owFDESMBAGA1UEAwwJVGVzdCBDQSAwMIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAxaS+sjus9Ok0PyuFry9YzjoVRYTCgB8b4MMPS/ah+Tj6
bfYHKRDq9bwzdhPfUCjz0bsowa6eqSesY6TE5Ab4bN+xvLQSF0n5R/D2Wm2pdDfO
uHt4CTwLYeiicnYuROlWu1SiKXe5UvsOzvAr5xmoWcRflV8V2lTMg/he9rHbJ53Q
Me1WcJCsLQEquXVbt4HMamiUc/L4ZBWrAmhN9OPbXbSIb4NpQTIZIB3HtNPyEnDF
NwV6RlNdObhZl7kO8Qwir/uCk4q5q+IABF6aq7r0Iwtw46OCtOjBYlCcz+lVa+BT
lNgxJ29xhrADL7oO8eDr4kzMjXpwlhPCmmYEN4tFgQIDAQABo1MwUTAdBgNVHQ4E
FgQUyh9E/2sbqQE5haeyMngCy3eKBBUwHwYDVR0jBBgwFoAUyh9E/2sbqQE5haey
MngCy3eKBBUwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAA+Z4
MQ4wzjGpDhMgzm3ljG6eHL8cBnf2I0JJNUcnpsGgD3WzPKf+xO8RxcE6kljk0nRf
xBewgiXQF4/FETcpFiVmlKHpLqU2lNT/MSTq5rN8BoEhEMkh1mIAZNszS8guIOTy
h+2dKizB9ZRPrfruSDMkEaRTEFesk8p4lqkwoT9b8eoqAo28y8p4+3fss9yw0eYw
KqOvwXT4zRHWRvQHyJILwYimHqe8F7JzZd5o5vbjnEq7rs68wu0LHZX+zJv2jUcZ
LdDIWI+gQteHuKRpN5lBOgmo2m2pn6e5P+4XrIEXiMtK5BJsMP7YdXqgEVFdcciK
lgTtrJ71uAem7GmYGQ==
-----END CERTIFICATE-----";

    /// Server certificate for hostname verification tests.
    /// SAN = DNS:server.example.com, signed by TEST_SERVER_CA_CERT.
    /// Using this cert against server name "kafka.example.com" triggers
    /// `CertificateError::NotValidForNameContext`.
    const TEST_SERVER_CERT: &str = "\
-----BEGIN CERTIFICATE-----
MIIDQTCCAimgAwIBAgIUJV3rP+B5V0MLRhSyxxpapdVaBwAwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJVGVzdCBDQSAwMB4XDTI2MDQxMDE2NDY1M1oXDTM2MDQw
NzE2NDY1M1owHTEbMBkGA1UEAwwSc2VydmVyLmV4YW1wbGUuY29tMIIBIjANBgkq
hkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAsJBKXkGSLj4fd1/OCaugp7x1E4rlDeGm
aQmHcJ9O0KOrXgfL9Z0BRjEyXSAbaqC2TosPzk60MZbvtN8kD0PEjY0ErPV/Mg7t
FVsDxX80QCdGw6ee5QylCCMJJYAJls3ZHdfN8pvRm++xipAeTLxSYsxNI56bmXsy
JSz236JO4jx68hKJ3Yvxo8Fu68QzdzZ9ZqqMWsqkmVWPLmLPXziohkBcYc3nzzDJ
Ahjw8wqtw7CvZyqMASumd5NVra5+wCpMsBCqzANPdX0JscUvM0vlhenfqM6Agbi9
nC1jr1mVH1SmoxhQgQ1ttCGj39/FxMjMmWtQe9rNaMhjAcDhSnVQtQIDAQABo4GB
MH8wHQYDVR0RBBYwFIISc2VydmVyLmV4YW1wbGUuY29tMAkGA1UdEwQCMAAwEwYD
VR0lBAwwCgYIKwYBBQUHAwEwHQYDVR0OBBYEFOXHjrPlSM3ZHxgwJJjBk7AGaG4W
MB8GA1UdIwQYMBaAFMofRP9rG6kBOYWnsjJ4Ast3igQVMA0GCSqGSIb3DQEBCwUA
A4IBAQBBbckvJ3Tl+MdMeIBQSLA1CMSSPh2Hal8TOiKaSOAUzOK7GKXxGKGfOi9M
pgnUJD+9RJsl9mV34H9hFBB45vjm7nBWnez0d4/RdOo3qBbg4YhNFDeKtdlTcuLr
yu/PETHwchY62l/T516oAjFO5jM6LQm7C7+WGMB/MOR3tRgI0YZCrnLvYXSyT89K
NUvILfVT3/ABSv5HCBZHI3STe2vpRma3vd24ACO3WAMbHmVFfyNEVcGL9JiNcM6j
Tk/mn3Nd0C3DIaEo6yDUzcUuD+qqrqyOZ3g1XzpDSet+L7UPEW6s8hgqeAr5XjMw
0reaLAXPKPyfTHTnl3fsXHIk/p9Q
-----END CERTIFICATE-----";

    /// Parses PEM-encoded certificates into `CertificateDer` values.
    fn parse_certs(pem: &str) -> Vec<CertificateDer<'static>> {
        CertificateDer::pem_slice_iter(pem.as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    #[test]
    fn test_no_hostname_verifier_accepts_mismatched_hostname() {
        // Ensure the crypto provider is installed.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // Build a NoHostnameVerifier that trusts TEST_SERVER_CA_CERT.
        let mut root_store = RootCertStore::empty();
        for cert in parse_certs(TEST_SERVER_CA_CERT) {
            root_store.add(cert).unwrap();
        }
        let verifier = NoHostnameVerifier::new(root_store);

        // Parse the server certificate (SAN = server.example.com).
        let certs = parse_certs(TEST_SERVER_CERT);
        let end_entity = &certs[0];

        // Use a mismatched server name — "kafka.example.com" vs cert's
        // "server.example.com". The inner WebPkiServerVerifier will
        // return NotValidForNameContext, which NoHostnameVerifier must
        // suppress.
        let mismatched_name = ServerName::try_from("kafka.example.com").unwrap();
        let now = rustls::pki_types::UnixTime::now();

        let result = verifier.verify_server_cert(end_entity, &[], &mismatched_name, &[], now);
        assert!(
            result.is_ok(),
            "NoHostnameVerifier should accept a cert with mismatched hostname, got: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_no_hostname_verifier_accepts_matching_hostname() {
        // Ensure the crypto provider is installed.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // Build a NoHostnameVerifier that trusts TEST_SERVER_CA_CERT.
        let mut root_store = RootCertStore::empty();
        for cert in parse_certs(TEST_SERVER_CA_CERT) {
            root_store.add(cert).unwrap();
        }
        let verifier = NoHostnameVerifier::new(root_store);

        // Parse the server certificate (SAN = server.example.com).
        let certs = parse_certs(TEST_SERVER_CERT);
        let end_entity = &certs[0];

        // Use the matching server name — this should also succeed.
        let matching_name = ServerName::try_from("server.example.com").unwrap();
        let now = rustls::pki_types::UnixTime::now();

        let result = verifier.verify_server_cert(end_entity, &[], &matching_name, &[], now);
        assert!(
            result.is_ok(),
            "NoHostnameVerifier should accept a cert with matching hostname, got: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_no_hostname_verifier_rejects_untrusted_cert() {
        // Ensure the crypto provider is installed.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // Build a NoHostnameVerifier with a DIFFERENT CA (the original TEST_CA_CERT).
        // The server cert is signed by TEST_SERVER_CA_CERT, so verification
        // must fail with UnknownIssuer — NoHostnameVerifier should NOT suppress
        // non-hostname-related errors.
        let mut root_store = RootCertStore::empty();
        for cert in parse_certs(TEST_CA_CERT) {
            root_store.add(cert).unwrap();
        }
        let verifier = NoHostnameVerifier::new(root_store);

        let certs = parse_certs(TEST_SERVER_CERT);
        let end_entity = &certs[0];

        let server_name = ServerName::try_from("server.example.com").unwrap();
        let now = rustls::pki_types::UnixTime::now();

        let result = verifier.verify_server_cert(end_entity, &[], &server_name, &[], now);
        assert!(result.is_err(), "NoHostnameVerifier should reject a cert from an untrusted CA");
    }
}
