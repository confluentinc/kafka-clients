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
//! `Arc<rustls::ClientConfig>` from [`SslConfig`], using `rustls` for TLS and
//! `tokio-rustls` for async I/O integration.
//!
//! Key differences:
//! - Only PEM format is supported. JKS and PKCS12 return an error.
//! - `ring` is used as the crypto backend (not `aws-lc-rs`) to avoid cmake dependency.
//! - Hostname verification is controlled by `endpoint_identification_algorithm`:
//!   when empty, a custom `ServerCertVerifier` validates the cert chain but skips
//!   hostname matching.

use crate::common::config::SslConfig;

use std::io;
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use tokio_rustls::TlsConnector;

/// SSL/TLS factory that builds `Arc<rustls::ClientConfig>` from [`SslConfig`].
///
/// Translated from `org.apache.kafka.common.security.ssl.SslFactory`.
///
/// Holds the compiled TLS client configuration and a flag indicating whether
/// hostname verification is enabled. The configuration is built once and shared
/// across all connections via `Arc`.
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
    /// Creates a new `SslFactory` from the given [`SslConfig`].
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
    /// - The private key cannot be loaded or is missing when a client cert is provided
    /// - TLS protocol configuration is invalid
    pub fn new(ssl_config: &SslConfig) -> io::Result<Self> {
        // Install the ring crypto provider if not already installed.
        // This is idempotent — subsequent calls are no-ops.
        let _ = rustls::crypto::ring::default_provider().install_default();

        // Validate store formats
        validate_store_format(&ssl_config.truststore_type, "truststore")?;
        validate_store_format(&ssl_config.keystore_type, "keystore")?;

        // Build root cert store
        let root_store = build_root_cert_store(ssl_config)?;

        // Determine hostname verification
        let hostname_verification = !ssl_config.endpoint_identification_algorithm.is_empty();

        // Determine TLS versions
        let versions = resolve_tls_versions(&ssl_config.enabled_protocols)?;

        // Build client config
        let config = if let Some(certs_and_key) = load_client_identity(ssl_config)? {
            let (certs, key) = certs_and_key;
            if hostname_verification {
                ClientConfig::builder_with_protocol_versions(&versions)
                    .with_root_certificates(root_store)
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| {
                        io::Error::new(io::ErrorKind::InvalidInput, format!("Failed to set client auth: {e}"))
                    })?
            } else {
                let verifier = NoHostnameVerifier::new(root_store);
                ClientConfig::builder_with_protocol_versions(&versions)
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(verifier))
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| {
                        io::Error::new(io::ErrorKind::InvalidInput, format!("Failed to set client auth: {e}"))
                    })?
            }
        } else if hostname_verification {
            ClientConfig::builder_with_protocol_versions(&versions)
                .with_root_certificates(root_store)
                .with_no_client_auth()
        } else {
            let verifier = NoHostnameVerifier::new(root_store);
            ClientConfig::builder_with_protocol_versions(&versions)
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier))
                .with_no_client_auth()
        };

        Ok(Self { client_config: Arc::new(config), hostname_verification })
    }

    /// Creates a `TlsConnector` from the compiled client configuration.
    pub fn create_tls_connector(&self) -> TlsConnector {
        TlsConnector::from(self.client_config.clone())
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
fn build_root_cert_store(ssl_config: &SslConfig) -> io::Result<RootCertStore> {
    let mut root_store = RootCertStore::empty();

    if let Some(ref path) = ssl_config.truststore_location {
        let file = std::fs::File::open(path)
            .map_err(|e| io::Error::new(e.kind(), format!("Failed to open truststore file '{path}': {e}")))?;
        let mut reader = io::BufReader::new(file);
        let certs = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>().map_err(|e| {
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
        let mut reader = io::BufReader::new(pem_data.as_bytes());
        let certs = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>().map_err(|e| {
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
    ssl_config: &SslConfig,
) -> io::Result<Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>> {
    // Check for inline PEM cert chain + key
    let certs = if let Some(ref chain_pem) = ssl_config.keystore_certificate_chain {
        let mut reader = io::BufReader::new(chain_pem.as_bytes());
        let certs = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>().map_err(|e| {
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
        let file = std::fs::File::open(path)
            .map_err(|e| io::Error::new(e.kind(), format!("Failed to open keystore file '{path}': {e}")))?;
        let mut reader = io::BufReader::new(file);
        let certs = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>().map_err(|e| {
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
        let mut reader = io::BufReader::new(key_pem.as_bytes());
        let key = rustls_pemfile::private_key(&mut reader)
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
        let file = std::fs::File::open(path)
            .map_err(|e| io::Error::new(e.kind(), format!("Failed to open keystore file for key '{path}': {e}")))?;
        let mut reader = io::BufReader::new(file);
        let key = rustls_pemfile::private_key(&mut reader)
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

/// Resolves TLS protocol version strings to rustls `SupportedProtocolVersion` references.
fn resolve_tls_versions(enabled_protocols: &[String]) -> io::Result<Vec<&'static rustls::SupportedProtocolVersion>> {
    let mut versions = Vec::new();
    for proto in enabled_protocols {
        match proto.as_str() {
            "TLSv1.3" => versions.push(&rustls::version::TLS13),
            "TLSv1.2" => versions.push(&rustls::version::TLS12),
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "Unsupported TLS protocol version: '{other}'. \
                         Supported versions: TLSv1.2, TLSv1.3"
                    ),
                ));
            },
        }
    }
    if versions.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "No TLS protocol versions enabled"));
    }
    Ok(versions)
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
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName)) => {
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
    use crate::common::config::SslConfig;

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
        let config = SslConfig::default();
        let factory = SslFactory::new(&config).unwrap();
        assert!(factory.hostname_verification());
    }

    #[test]
    fn test_build_with_inline_truststore() {
        let config = SslConfig { truststore_certificates: Some(TEST_CA_CERT.to_string()), ..SslConfig::default() };
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

        let config = SslConfig {
            truststore_location: Some(cert_path.to_str().unwrap().to_string()),
            ..SslConfig::default()
        };
        let factory = SslFactory::new(&config).unwrap();
        assert!(factory.hostname_verification());

        // Cleanup
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_build_with_client_cert() {
        let config = SslConfig {
            truststore_certificates: Some(TEST_CA_CERT.to_string()),
            keystore_certificate_chain: Some(TEST_CLIENT_CERT.to_string()),
            keystore_key: Some(TEST_CLIENT_KEY.to_string()),
            ..SslConfig::default()
        };
        let factory = SslFactory::new(&config);
        assert!(factory.is_ok(), "Should build with client cert: {:?}", factory.err());
    }

    #[test]
    fn test_unsupported_jks_format() {
        let config = SslConfig { truststore_type: "JKS".to_string(), ..SslConfig::default() };
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
        let config = SslConfig { keystore_type: "PKCS12".to_string(), ..SslConfig::default() };
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
        let config = SslConfig { endpoint_identification_algorithm: "https".to_string(), ..SslConfig::default() };
        let factory = SslFactory::new(&config).unwrap();
        assert!(factory.hostname_verification());
    }

    #[test]
    fn test_hostname_verification_disabled() {
        let config = SslConfig { endpoint_identification_algorithm: String::new(), ..SslConfig::default() };
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
        let config = SslConfig { enabled_protocols: vec!["TLSv1.3".to_string()], ..SslConfig::default() };
        let factory = SslFactory::new(&config);
        assert!(factory.is_ok());

        // Only TLSv1.2
        let config = SslConfig { enabled_protocols: vec!["TLSv1.2".to_string()], ..SslConfig::default() };
        let factory = SslFactory::new(&config);
        assert!(factory.is_ok());

        // Unsupported version
        let config = SslConfig { enabled_protocols: vec!["TLSv1.1".to_string()], ..SslConfig::default() };
        let result = SslFactory::new(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unsupported TLS protocol version"));
    }

    #[test]
    fn test_invalid_pem_truststore() {
        let config = SslConfig {
            truststore_certificates: Some("not a valid PEM".to_string()),
            ..SslConfig::default()
        };
        let result = SslFactory::new(&config);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("No certificates found"),
            "Should report no certs found in invalid PEM"
        );
    }

    #[test]
    fn test_create_tls_connector() {
        let config = SslConfig::default();
        let factory = SslFactory::new(&config).unwrap();
        let _connector = factory.create_tls_connector();
        // Just verify it doesn't panic
    }

    #[test]
    fn test_cert_without_key_error() {
        let config = SslConfig {
            keystore_certificate_chain: Some(TEST_CLIENT_CERT.to_string()),
            // No key provided
            ..SslConfig::default()
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
        let config = SslConfig {
            truststore_location: Some("/nonexistent/path/truststore.pem".to_string()),
            ..SslConfig::default()
        };
        let result = SslFactory::new(&config);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("Failed to open truststore"),
            "Should report file not found"
        );
    }
}
