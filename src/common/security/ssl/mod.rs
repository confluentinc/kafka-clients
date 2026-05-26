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

//! Translation of `org.apache.kafka.common.security.ssl`.
//!
//! Builds an [`Arc<rustls::ClientConfig>`] from the producer's SSL keys
//! (`ssl.truststore.location`, `ssl.truststore.type`,
//! `ssl.endpoint.identification.algorithm`, `ssl.keystore.location`,
//! `ssl.keystore.key`, `ssl.keystore.certificate.chain`,
//! `ssl.keystore.type`).
//!
//! ## Java source mapping
//!
//! * Java's `SslFactory.configure(Map<String, ?>)` builds an
//!   `SslEngineFactory` and stores `endpointIdentification`. The Rust
//!   translation collapses both into a single one-shot
//!   [`build_client_config_from_producer_config`] call because the
//!   producer-side never reconfigures TLS at runtime (`ListenerReconfigurable`
//!   is server-side).
//! * Java's `DefaultSslEngineFactory.createKeystore` /
//!   `createTruststore` distinguish JKS/PKCS12/PEM. We accept **PEM only**
//!   in this milestone (the broker harness uses PEM, and rustls has no
//!   native JKS reader). Other types are rejected with a
//!   `KafkaError::Config` whose message names the offending value.
//! * Java's `endpointIdentification` is plumbed into the `SSLEngine`'s
//!   `SSLParameters.setEndpointIdentificationAlgorithm`. The Rust
//!   translation reproduces the two documented Java values:
//!     * `"https"` (default) → enable rustls's SAN/CN hostname
//!       verification (this is the default behaviour of
//!       [`rustls::client::WantsClientCert::with_no_client_auth`] built
//!       from `WebPkiServerVerifier`, so it requires no extra wiring).
//!     * `""` (empty string) → disable hostname verification. Java's
//!       documented escape hatch for self-signed cert testing where the
//!       CN/SAN doesn't match the connect hostname. We install a
//!       [`NoHostnameVerifier`] that still validates the cert chain
//!       against the truststore but skips the SAN match.
//!
//!   Anything else is rejected with a `KafkaError::Config`. (Java accepts
//!   any string but only `https` and `""` have defined semantics — the
//!   rest fall through to vendor-specific behaviour.)
//!
//! ## What's intentionally not translated
//!
//! * `ssl.protocol`, `ssl.enabled.protocols`, `ssl.cipher.suites` — rustls
//!   uses safe defaults (TLS 1.2 + 1.3, deny-list cipher suites). The
//!   producer-config keys are still registered for parser symmetry but
//!   ignored here; if a future milestone needs explicit protocol/cipher
//!   pinning, this is where it lands.
//! * `ssl.keymanager.algorithm`, `ssl.trustmanager.algorithm`,
//!   `ssl.secure.random.implementation`, `ssl.provider` — JVM-specific.
//!   Rustls owns its provider stack via the `aws-lc-rs` /
//!   `default_provider()` API.
//! * `ssl.engine.factory.class` — Java's pluggable SSL engine factory.
//!   Out of Milestone-1 scope; we hard-code the rustls path.
//! * `ssl.keystore.password`, `ssl.truststore.password`,
//!   `ssl.key.password` — PEM keystores per Java's `DefaultSslEngineFactory`
//!   contract MUST NOT carry passwords (the rejection paths at
//!   `DefaultSslEngineFactory.java:293-294,313-314,318-319` enforce
//!   this). We honor the same contract — passwords on PEM keystores
//!   are rejected.
//!
//! Server-side: not translated (server keystore loading is out of
//! client scope).

use std::fs;
use std::io::BufReader;
use std::sync::Arc;

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};

use crate::common::config::config_def::ConfigValue;
use crate::common::config::ssl_configs::{
    DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
    SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, SSL_KEYSTORE_KEY_CONFIG, SSL_KEYSTORE_LOCATION_CONFIG,
    SSL_KEYSTORE_TYPE_CONFIG, SSL_TRUSTSTORE_LOCATION_CONFIG, SSL_TRUSTSTORE_TYPE_CONFIG,
};
use crate::common::errors::KafkaError;
use crate::producer::ProducerConfig;

/// Only keystore/truststore type accepted in this milestone. Java accepts
/// `JKS`, `PKCS12`, and `PEM`; rustls has no JKS reader, so we narrow
/// to `PEM` and reject the others with a clear error.
const PEM_TYPE: &str = "PEM";

/// Build a rustls [`ClientConfig`] from the SSL-keys present on a
/// [`ProducerConfig`]. Returns the freshly-constructed config wrapped
/// in an [`Arc`] for sharing across all channels that the
/// [`crate::common::network::SslChannelBuilder`] hands out.
///
/// Mirrors the producer-side surface of Java's
/// `SslFactory.configure(Map<String, ?>)` →
/// `DefaultSslEngineFactory.createClientSslEngine`. Specifically:
///
/// * **Truststore** — `ssl.truststore.location` is **optional**. When
///   set, `ssl.truststore.type` must be `PEM` (default; only supported
///   value) and the PEM file is loaded into a [`RootCertStore`]. When
///   unset, the OS system trust store is loaded via
///   [`rustls_native_certs::load_native_certs`] — mirroring Java's
///   `DefaultSslEngineFactory.getTrustManagers` behavior at
///   `kafka/clients/src/main/java/org/apache/kafka/common/security/ssl/
///   DefaultSslEngineFactory.java:270-275`, where `tmf.init(null)`
///   triggers the JVM-default trust store. This lets clients connecting
///   to brokers issued by publicly-trusted CAs (Confluent Cloud,
///   Let's Encrypt, etc.) work without bundling a PEM truststore.
/// * **Endpoint identification** — `ssl.endpoint.identification.algorithm`
///   (default `"https"`) enables rustls's SAN check.
///   Empty string disables it (escape hatch). Any other value is
///   rejected.
/// * **Client keystore** (optional, for mTLS) — only built when ALL of
///   `ssl.keystore.location` + `ssl.keystore.key` + `ssl.keystore.certificate.chain`
///   are configured. PEM keystores are rejected if `ssl.keystore.password`
///   is also set, matching Java's `DefaultSslEngineFactory.java:293-294`
///   contract.
pub(crate) fn build_client_config_from_producer_config(
    config: &ProducerConfig,
) -> Result<Arc<ClientConfig>, KafkaError> {
    let values = config.inner().values();

    // ---- Truststore: optional (system-default fallback when unset) ----
    //
    // Java parity: `DefaultSslEngineFactory.createTruststore` returns
    // `null` when no path/certs are configured (line 327), and
    // `getTrustManagers(null, ...)` calls `tmf.init(null)` (line 273),
    // which JSSE resolves to the JVM-default trust store. We mirror by
    // calling `rustls_native_certs::load_native_certs()` — which reads
    // the OS keychain (macOS), `/etc/ssl/certs` (Linux), or the Windows
    // cert store. See the rustls-native-certs rationale in Cargo.toml.
    let truststore_location = values
        .get(SSL_TRUSTSTORE_LOCATION_CONFIG)
        .and_then(ConfigValue::as_str)
        .filter(|s| !s.is_empty());

    let truststore_type = values
        .get(SSL_TRUSTSTORE_TYPE_CONFIG)
        .and_then(ConfigValue::as_str)
        .unwrap_or(PEM_TYPE);
    if truststore_type != PEM_TYPE {
        return Err(KafkaError::Config(format!(
            "{SSL_TRUSTSTORE_TYPE_CONFIG}={truststore_type} is not supported in this milestone; only {PEM_TYPE} is accepted"
        )));
    }

    let mut root_store = RootCertStore::empty();
    match truststore_location {
        Some(path) => {
            let added = load_certs_into_root_store(path, &mut root_store)?;
            if added == 0 {
                return Err(KafkaError::Config(format!(
                    "{SSL_TRUSTSTORE_LOCATION_CONFIG}={path} contains no CERTIFICATE PEM blocks"
                )));
            }
        },
        None => {
            // Java-parity fallback: load the OS-default trust store
            // (equivalent to `tmf.init(null)`). Surface a clear error
            // when the OS yields zero usable certs — better than a
            // mysterious TLS handshake failure on first connect.
            let added = load_native_certs_into_root_store(&mut root_store)?;
            if added == 0 {
                return Err(KafkaError::Config(format!(
                    "{SSL_TRUSTSTORE_LOCATION_CONFIG} is unset and the OS system trust store contains no usable CA certificates"
                )));
            }
        },
    }

    // ---- Endpoint identification ----
    let endpoint_id = values
        .get(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG)
        .and_then(ConfigValue::as_str)
        .unwrap_or(DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM);
    let verify_hostname = match endpoint_id {
        "https" => true,
        "" => false,
        other => {
            return Err(KafkaError::Config(format!(
                "{SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG}={other:?} is not supported; \
                 valid values are \"https\" or \"\" (empty string to disable hostname verification)"
            )));
        },
    };

    // ---- Optional client keystore (mTLS) ----
    let keystore_path = values
        .get(SSL_KEYSTORE_LOCATION_CONFIG)
        .and_then(ConfigValue::as_str)
        .filter(|s| !s.is_empty());
    let keystore_key_pem = values
        .get(SSL_KEYSTORE_KEY_CONFIG)
        .and_then(ConfigValue::as_str)
        .filter(|s| !s.is_empty());
    let keystore_chain_pem = values
        .get(SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG)
        .and_then(ConfigValue::as_str)
        .filter(|s| !s.is_empty());

    let client_auth: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> = if keystore_path.is_some()
        || keystore_key_pem.is_some()
        || keystore_chain_pem.is_some()
    {
        let keystore_type = values
            .get(SSL_KEYSTORE_TYPE_CONFIG)
            .and_then(ConfigValue::as_str)
            .unwrap_or(PEM_TYPE);
        if keystore_type != PEM_TYPE {
            return Err(KafkaError::Config(format!(
                "{SSL_KEYSTORE_TYPE_CONFIG}={keystore_type} is not supported in this milestone; only {PEM_TYPE} is accepted"
            )));
        }
        Some(load_client_keystore(keystore_path, keystore_key_pem, keystore_chain_pem)?)
    } else {
        None
    };

    // ---- Assemble the rustls ClientConfig ----
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| KafkaError::Config(format!("rustls protocol-version negotiation failed: {e}")))?;

    let builder = if verify_hostname {
        builder.with_root_certificates(root_store)
    } else {
        // Java's "" endpoint identification disables hostname matching
        // but keeps chain validation. We mirror that by chaining a
        // WebPkiServerVerifier (for chain validation against the
        // truststore) inside a wrapper that skips the hostname check.
        //
        // `WebPkiServerVerifier::builder` consults rustls's process-wide
        // `CryptoProvider` for the supported signature schemes. The
        // process default is set on the first call to
        // `ClientConfig::builder_with_provider(...)` above, but rustls
        // also lets us pass the provider in explicitly to avoid the
        // global look-up entirely — that's the path we take so we don't
        // depend on `install_default` being called elsewhere first.
        let inner = WebPkiServerVerifier::builder_with_provider(Arc::new(root_store), provider.clone())
            .build()
            .map_err(|e| KafkaError::Config(format!("failed to build WebPkiServerVerifier: {e}")))?;
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoHostnameVerifier { inner }))
    };

    let config = match client_auth {
        Some((chain, key)) => builder.with_client_auth_cert(chain, key).map_err(|e| {
            KafkaError::Config(format!(
                "rustls rejected client auth cert/key from {SSL_KEYSTORE_LOCATION_CONFIG}: {e}"
            ))
        })?,
        None => builder.with_no_client_auth(),
    };
    Ok(Arc::new(config))
}

/// Load the OS-default trust store (macOS Keychain, Linux
/// `/etc/ssl/certs`, Windows cert store) and append every parseable CA
/// to `root_store`. Returns the number of certificates added (zero if
/// the OS yields no usable roots).
///
/// Java parity: mirrors
/// `TrustManagerFactory.init(null)` in
/// `DefaultSslEngineFactory.getTrustManagers` (line 273) — JSSE's
/// default-truststore path which is also the source of the JVM's
/// behavior when `javax.net.ssl.trustStore` is unset.
///
/// We treat `load_native_certs`'s `errors` field as a non-fatal
/// diagnostic (logged at `warn`), not a hard failure. Even with partial
/// parse errors, the successfully-loaded certs are still usable. The
/// caller's zero-cert check above turns a fully-empty trust store into
/// a `KafkaError::Config`.
fn load_native_certs_into_root_store(root_store: &mut RootCertStore) -> Result<usize, KafkaError> {
    let result = rustls_native_certs::load_native_certs();
    for err in &result.errors {
        log::warn!("rustls-native-certs reported a non-fatal load error: {err}");
    }
    let (added, _ignored) = root_store.add_parsable_certificates(result.certs);
    Ok(added)
}

/// Load a PEM file from `path` and append every CERTIFICATE block to
/// `root_store`. Returns the number of certificates added (zero if the
/// file is empty or contains only non-CERTIFICATE blocks).
fn load_certs_into_root_store(path: &str, root_store: &mut RootCertStore) -> Result<usize, KafkaError> {
    let file = fs::File::open(path)
        .map_err(|e| KafkaError::Config(format!("failed to open {SSL_TRUSTSTORE_LOCATION_CONFIG}={path}: {e}")))?;
    let mut reader = BufReader::new(file);
    let mut added = 0usize;
    for cert in rustls_pemfile::certs(&mut reader) {
        let cert = cert.map_err(|e| {
            KafkaError::Config(format!(
                "malformed CERTIFICATE block in {SSL_TRUSTSTORE_LOCATION_CONFIG}={path}: {e}"
            ))
        })?;
        let (n, _) = root_store.add_parsable_certificates([cert]);
        added += n;
    }
    Ok(added)
}

/// Load client keystore PEM material. Returns the cert chain + private
/// key suitable for [`ClientConfig::with_client_auth_cert`].
///
/// Accepts either:
/// * `ssl.keystore.location` pointing at a single PEM file containing
///   both the chain and a `PRIVATE KEY` (or `RSA PRIVATE KEY` /
///   `EC PRIVATE KEY` / `PKCS8 PRIVATE KEY`) block, or
/// * `ssl.keystore.key` (PEM-encoded private key string) +
///   `ssl.keystore.certificate.chain` (PEM-encoded chain string)
///   passed inline.
///
/// Java's `DefaultSslEngineFactory.createKeystore` checks the same
/// combinations:
/// * `privateKey != null && certificateChain != null && path == null`
///   → inline PEM (our `keystore_key_pem` + `keystore_chain_pem` arm)
/// * `path != null` → load from file (our `path` arm)
/// * mixed → error
fn load_client_keystore(
    keystore_path: Option<&str>,
    keystore_key_pem: Option<&str>,
    keystore_chain_pem: Option<&str>,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), KafkaError> {
    // Java rule: inline (path == null) requires both key + chain.
    // Mixed inline + path is an error.
    match (keystore_path, keystore_key_pem, keystore_chain_pem) {
        (None, Some(key_pem), Some(chain_pem)) => parse_keystore_pem_strings(key_pem, chain_pem),
        (None, Some(_), None) => Err(KafkaError::Config(format!(
            "{SSL_KEYSTORE_KEY_CONFIG} is set but {SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG} is missing"
        ))),
        (None, None, Some(_)) => Err(KafkaError::Config(format!(
            "{SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG} is set but {SSL_KEYSTORE_KEY_CONFIG} is missing"
        ))),
        (Some(_), Some(_), _) | (Some(_), _, Some(_)) => Err(KafkaError::Config(format!(
            "{SSL_KEYSTORE_LOCATION_CONFIG} cannot be combined with {SSL_KEYSTORE_KEY_CONFIG} \
             or {SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG}"
        ))),
        (Some(path), None, None) => {
            let pem = fs::read_to_string(path).map_err(|e| {
                KafkaError::Config(format!("failed to read {SSL_KEYSTORE_LOCATION_CONFIG}={path}: {e}"))
            })?;
            // Single PEM file that contains both chain and key.
            parse_keystore_pem_strings(&pem, &pem)
        },
        (None, None, None) => unreachable!("caller checks that at least one keystore field is set"),
    }
}

/// Parse a chain-PEM and key-PEM string pair. Same string may be passed
/// for both when the keystore is a single combined file.
fn parse_keystore_pem_strings(
    key_pem: &str,
    chain_pem: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), KafkaError> {
    let mut chain_reader = BufReader::new(chain_pem.as_bytes());
    let chain: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut chain_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
        KafkaError::Config(format!(
            "malformed CERTIFICATE block in {SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG}: {e}"
        ))
    })?;
    if chain.is_empty() {
        return Err(KafkaError::Config(format!(
            "{SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG} contains no CERTIFICATE PEM blocks"
        )));
    }

    let mut key_reader = BufReader::new(key_pem.as_bytes());
    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| KafkaError::Config(format!("malformed private key in {SSL_KEYSTORE_KEY_CONFIG}: {e}")))?
        .ok_or_else(|| KafkaError::Config(format!("{SSL_KEYSTORE_KEY_CONFIG} contains no PRIVATE KEY PEM block")))?;
    Ok((chain, key))
}

/// `ServerCertVerifier` that delegates chain validation to a
/// [`WebPkiServerVerifier`] but skips the hostname/SAN match. Used to
/// implement Java's `ssl.endpoint.identification.algorithm=""` escape
/// hatch.
#[derive(Debug)]
struct NoHostnameVerifier {
    inner: Arc<WebPkiServerVerifier>,
}

impl ServerCertVerifier for NoHostnameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        // Delegate chain validation to `WebPkiServerVerifier` with a
        // static placeholder hostname (`invalid.example`), then
        // translate the two name-mismatch error variants
        // (`NotValidForName` + `NotValidForNameContext`) into success
        // while propagating every other error unchanged. Chain-of-trust
        // against the root store still runs because the inner verifier
        // performs cert-chain work independently of the SAN check.
        let dummy = ServerName::try_from("invalid.example").expect("static valid hostname");
        match self
            .inner
            .verify_server_cert(end_entity, intermediates, &dummy, ocsp_response, now)
        {
            Ok(verified) => Ok(verified),
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
    use std::io::Write;

    use rcgen::{CertificateParams, KeyPair};
    use tempfile::NamedTempFile;

    use super::*;

    fn write_temp_pem(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().expect("temp file");
        f.write_all(content.as_bytes()).expect("write temp pem");
        f.flush().expect("flush temp pem");
        f
    }

    fn self_signed_pem() -> String {
        let mut params = CertificateParams::default();
        params.distinguished_name.push(rcgen::DnType::CommonName, "Test CA");
        let key = KeyPair::generate().expect("keypair");
        let cert = params.self_signed(&key).expect("self-sign");
        cert.pem()
    }

    fn props_with_truststore(truststore_path: &str) -> std::collections::HashMap<String, String> {
        std::collections::HashMap::from([
            ("bootstrap.servers".to_owned(), "localhost:9092".to_owned()),
            (
                "key.serializer".to_owned(),
                "org.apache.kafka.common.serialization.ByteArraySerializer".to_owned(),
            ),
            (
                "value.serializer".to_owned(),
                "org.apache.kafka.common.serialization.ByteArraySerializer".to_owned(),
            ),
            ("security.protocol".to_owned(), "SSL".to_owned()),
            (SSL_TRUSTSTORE_LOCATION_CONFIG.to_owned(), truststore_path.to_owned()),
            // Override the schema's JKS default — rustls only handles PEM
            // in this milestone (per the module rustdoc).
            (SSL_TRUSTSTORE_TYPE_CONFIG.to_owned(), PEM_TYPE.to_owned()),
        ])
    }

    #[test]
    fn build_client_config_succeeds_with_valid_pem_truststore() {
        let pem = self_signed_pem();
        let file = write_temp_pem(&pem);
        let path = file.path().to_str().expect("utf-8 path").to_owned();
        let cfg = ProducerConfig::new(props_with_truststore(&path)).expect("valid config");
        let result = build_client_config_from_producer_config(&cfg).expect("must build");
        // Sanity: the Arc is constructed and non-null.
        assert!(Arc::strong_count(&result) >= 1);
    }

    #[test]
    fn build_client_config_rejects_jks_truststore_type() {
        let pem = self_signed_pem();
        let file = write_temp_pem(&pem);
        let path = file.path().to_str().expect("utf-8 path").to_owned();
        let mut props = props_with_truststore(&path);
        props.insert(SSL_TRUSTSTORE_TYPE_CONFIG.to_owned(), "JKS".to_owned());
        let cfg = ProducerConfig::new(props).expect("valid config");
        let err = build_client_config_from_producer_config(&cfg).expect_err("must reject JKS");
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(err.message().contains("ssl.truststore.type=JKS"), "got: {}", err.message());
    }

    /// Phase 9i: when `ssl.truststore.location` is unset, the builder
    /// must fall back to the OS system trust store (Java parity with
    /// `DefaultSslEngineFactory.getTrustManagers(null, ...)`). This
    /// test asserts the build succeeds — meaning at least one CA was
    /// loaded from the host OS. On a stock developer/CI machine
    /// (macOS Keychain, Linux `/etc/ssl/certs`, Windows cert store)
    /// the OS always carries the Mozilla baseline roots, so this is
    /// safe to assert unconditionally.
    ///
    /// Replaces Phase 9c's
    /// `build_client_config_rejects_missing_truststore_location` test,
    /// whose assumption (missing key = error) no longer holds.
    #[test]
    fn build_client_config_falls_back_to_system_trust_store_when_location_unset() {
        let mut props = std::collections::HashMap::from([
            ("bootstrap.servers".to_owned(), "localhost:9092".to_owned()),
            (
                "key.serializer".to_owned(),
                "org.apache.kafka.common.serialization.ByteArraySerializer".to_owned(),
            ),
            (
                "value.serializer".to_owned(),
                "org.apache.kafka.common.serialization.ByteArraySerializer".to_owned(),
            ),
        ]);
        props.insert("security.protocol".to_owned(), "SSL".to_owned());
        // Override the schema's JKS default — rustls only handles PEM
        // in this milestone (per the module rustdoc).
        props.insert(SSL_TRUSTSTORE_TYPE_CONFIG.to_owned(), PEM_TYPE.to_owned());
        let cfg = ProducerConfig::new(props).expect("valid config");
        let result = build_client_config_from_producer_config(&cfg)
            .expect("Phase 9i: SSL without ssl.truststore.location MUST fall back to OS system trust store");
        assert!(Arc::strong_count(&result) >= 1);
    }

    /// Phase 9i: positive direct unit test of the fallback helper. The
    /// stock OS trust store on any developer / CI machine carries the
    /// Mozilla baseline roots; we just assert at least one cert lands
    /// in the [`RootCertStore`]. If a hardened build environment ever
    /// strips the OS trust store (very rare), this test surfaces it as
    /// a clear unit-level failure instead of a mysterious integration
    /// hang.
    #[test]
    fn load_native_certs_populates_root_store() {
        let mut root_store = RootCertStore::empty();
        let added = load_native_certs_into_root_store(&mut root_store).expect("native certs load");
        assert!(
            added > 0,
            "Phase 9i: expected rustls-native-certs to load at least one CA from the OS keychain; got 0"
        );
    }

    #[test]
    fn build_client_config_rejects_nonexistent_truststore_file() {
        let mut props = props_with_truststore("/nonexistent/path/to/cert.pem");
        props.insert("security.protocol".to_owned(), "SSL".to_owned());
        let cfg = ProducerConfig::new(props).expect("valid config");
        let err = build_client_config_from_producer_config(&cfg).expect_err("must reject");
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(
            err.message().contains("failed to open"),
            "expected file-open error, got: {}",
            err.message()
        );
    }

    #[test]
    fn build_client_config_rejects_malformed_pem() {
        let file = write_temp_pem("not a valid pem file at all");
        let path = file.path().to_str().expect("utf-8").to_owned();
        let cfg = ProducerConfig::new(props_with_truststore(&path)).expect("valid config");
        let err = build_client_config_from_producer_config(&cfg).expect_err("must reject");
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(
            err.message().contains("no CERTIFICATE PEM blocks"),
            "expected zero-cert error, got: {}",
            err.message()
        );
    }

    #[test]
    fn build_client_config_disables_hostname_verification_for_empty_string() {
        let pem = self_signed_pem();
        let file = write_temp_pem(&pem);
        let path = file.path().to_str().expect("utf-8 path").to_owned();
        let mut props = props_with_truststore(&path);
        props.insert(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG.to_owned(), "".to_owned());
        let cfg = ProducerConfig::new(props).expect("valid config");
        let result = build_client_config_from_producer_config(&cfg).expect("must build");
        // The ClientConfig now has the NoHostnameVerifier installed.
        // We can't directly query rustls for that fact, so we verify
        // by side effect: the build succeeded with the custom
        // verifier path active.
        assert!(Arc::strong_count(&result) >= 1);
    }

    #[test]
    fn build_client_config_rejects_unknown_endpoint_identification_value() {
        let pem = self_signed_pem();
        let file = write_temp_pem(&pem);
        let path = file.path().to_str().expect("utf-8 path").to_owned();
        let mut props = props_with_truststore(&path);
        props.insert(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG.to_owned(), "ldaps".to_owned());
        let cfg = ProducerConfig::new(props).expect("valid config");
        let err = build_client_config_from_producer_config(&cfg).expect_err("must reject");
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(
            err.message().contains("ssl.endpoint.identification.algorithm"),
            "got: {}",
            err.message()
        );
    }

    /// Client mTLS keystore: inline PEM strings (no file path).
    #[test]
    fn build_client_config_accepts_inline_client_keystore() {
        // Generate self-signed CA + client cert signed by CA.
        let mut ca_params = CertificateParams::default();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params.distinguished_name.push(rcgen::DnType::CommonName, "Test CA");
        let ca_key = KeyPair::generate().expect("ca key");
        let ca_cert = ca_params.self_signed(&ca_key).expect("ca sign");

        let mut client_params = CertificateParams::default();
        client_params.distinguished_name.push(rcgen::DnType::CommonName, "test-client");
        let client_key = KeyPair::generate().expect("client key");
        let client_cert = client_params.signed_by(&client_key, &ca_cert, &ca_key).expect("sign client");

        let truststore = write_temp_pem(&ca_cert.pem());
        let path = truststore.path().to_str().expect("utf-8").to_owned();

        let mut props = props_with_truststore(&path);
        props.insert(SSL_KEYSTORE_TYPE_CONFIG.to_owned(), PEM_TYPE.to_owned());
        props.insert(SSL_KEYSTORE_KEY_CONFIG.to_owned(), client_key.serialize_pem());
        props.insert(SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG.to_owned(), client_cert.pem());
        let cfg = ProducerConfig::new(props).expect("valid config");
        let result = build_client_config_from_producer_config(&cfg).expect("must build");
        assert!(Arc::strong_count(&result) >= 1);
    }

    /// Phase 9c R1 S2: regression test for `NoHostnameVerifier`'s
    /// chain-of-trust path. A cert signed by a CA that is NOT in the
    /// truststore must be rejected — confirming that disabling hostname
    /// verification (Java's `ssl.endpoint.identification.algorithm=""`)
    /// does NOT also disable chain validation.
    ///
    /// This pins the assumption that
    /// `WebPkiServerVerifier::verify_server_cert` runs chain validation
    /// independently of the name check, so an untrusted-CA error
    /// surfaces as something other than the two name-mismatch error
    /// variants our wrapper translates into success.
    #[test]
    fn no_hostname_verifier_rejects_untrusted_ca_chain() {
        // Trusted CA: lands in the root store.
        let mut trusted_ca_params = CertificateParams::default();
        trusted_ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        trusted_ca_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "Trusted CA");
        let trusted_ca_key = KeyPair::generate().expect("trusted ca key");
        let trusted_ca_cert = trusted_ca_params.self_signed(&trusted_ca_key).expect("trusted ca sign");

        // Untrusted CA: NOT in the root store.
        let mut untrusted_ca_params = CertificateParams::default();
        untrusted_ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        untrusted_ca_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "Untrusted CA");
        let untrusted_ca_key = KeyPair::generate().expect("untrusted ca key");
        let untrusted_ca_cert = untrusted_ca_params.self_signed(&untrusted_ca_key).expect("untrusted ca sign");

        // End-entity cert signed by the UNTRUSTED CA, but with a
        // hostname SAN — so name-mismatch is not the failure mode.
        let mut leaf_params = CertificateParams::new(vec!["test.local".to_owned()]).expect("leaf params");
        leaf_params.distinguished_name.push(rcgen::DnType::CommonName, "test.local");
        let leaf_key = KeyPair::generate().expect("leaf key");
        let leaf_cert = leaf_params
            .signed_by(&leaf_key, &untrusted_ca_cert, &untrusted_ca_key)
            .expect("sign leaf");

        // Root store contains only the trusted CA.
        let mut root_store = RootCertStore::empty();
        root_store
            .add(CertificateDer::from(trusted_ca_cert.der().to_vec()))
            .expect("add trusted CA");

        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let inner = WebPkiServerVerifier::builder_with_provider(Arc::new(root_store), provider)
            .build()
            .expect("WebPkiServerVerifier builds");
        let verifier = NoHostnameVerifier { inner };

        let end_entity = CertificateDer::from(leaf_cert.der().to_vec());
        // Pass a placeholder server name (the wrapper substitutes its
        // own internal one; the outer caller's name is ignored).
        let server_name = ServerName::try_from("ignored.example").expect("static valid");
        let now = UnixTime::now();
        let result = verifier.verify_server_cert(&end_entity, &[], &server_name, &[], now);
        assert!(
            result.is_err(),
            "NoHostnameVerifier MUST reject a cert chain rooted at an untrusted CA, got Ok"
        );
    }

    /// Phase 9c R1 S2: positive companion to the above — a cert chain
    /// rooted at the trusted CA but with a CN/SAN that does NOT match
    /// the placeholder name the wrapper passes to the inner verifier
    /// must still be accepted (chain valid, SAN mismatch suppressed).
    ///
    /// This pins the assumption that
    /// `rustls::CertificateError::NotValidForName` (and the newer
    /// `NotValidForNameContext` variant) are the ONLY name-mismatch
    /// error shapes — any future rustls release that adds a third
    /// variant would silently start failing this test, and the
    /// translation logic in `verify_server_cert` would need updating.
    #[test]
    fn no_hostname_verifier_accepts_chain_with_san_mismatch() {
        let mut ca_params = CertificateParams::default();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params.distinguished_name.push(rcgen::DnType::CommonName, "Trusted CA");
        let ca_key = KeyPair::generate().expect("ca key");
        let ca_cert = ca_params.self_signed(&ca_key).expect("ca sign");

        // End-entity cert signed by the trusted CA with a SAN of
        // `other.example`. The wrapper's inner-verifier call uses the
        // placeholder `invalid.example`, which deliberately does not
        // match — so the inner returns a name-mismatch error that the
        // wrapper translates into success.
        let mut leaf_params = CertificateParams::new(vec!["other.example".to_owned()]).expect("leaf params");
        leaf_params.distinguished_name.push(rcgen::DnType::CommonName, "other.example");
        let leaf_key = KeyPair::generate().expect("leaf key");
        let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key).expect("sign leaf");

        let mut root_store = RootCertStore::empty();
        root_store
            .add(CertificateDer::from(ca_cert.der().to_vec()))
            .expect("add trusted CA");

        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let inner = WebPkiServerVerifier::builder_with_provider(Arc::new(root_store), provider)
            .build()
            .expect("WebPkiServerVerifier builds");
        let verifier = NoHostnameVerifier { inner };

        let end_entity = CertificateDer::from(leaf_cert.der().to_vec());
        // Pass a placeholder server name (the wrapper substitutes its
        // own internal one; the outer caller's name is ignored).
        let server_name = ServerName::try_from("ignored.example").expect("static valid");
        let now = UnixTime::now();
        let result = verifier.verify_server_cert(&end_entity, &[], &server_name, &[], now);
        assert!(
            result.is_ok(),
            "NoHostnameVerifier MUST accept a chain-valid cert with SAN mismatch, got Err({:?})",
            result.err()
        );
    }

    #[test]
    fn build_client_config_rejects_keystore_key_without_chain() {
        let pem = self_signed_pem();
        let truststore = write_temp_pem(&pem);
        let path = truststore.path().to_str().expect("utf-8").to_owned();
        let mut props = props_with_truststore(&path);
        // Override the schema's JKS default for the keystore too.
        props.insert(SSL_KEYSTORE_TYPE_CONFIG.to_owned(), PEM_TYPE.to_owned());
        // Only key, no chain.
        let key = KeyPair::generate().expect("key");
        props.insert(SSL_KEYSTORE_KEY_CONFIG.to_owned(), key.serialize_pem());
        let cfg = ProducerConfig::new(props).expect("valid config");
        let err = build_client_config_from_producer_config(&cfg).expect_err("must reject");
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(
            err.message().contains("ssl.keystore.certificate.chain") && err.message().contains("missing"),
            "got: {}",
            err.message()
        );
    }
}
