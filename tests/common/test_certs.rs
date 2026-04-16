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

//! Certificate generation utility for SSL integration tests.
//!
//! Uses `rcgen` to generate a self-signed CA and a broker certificate
//! signed by that CA, avoiding the need for checked-in certificate files
//! or external CLI tool dependencies.

use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, SanType};

/// Holds PEM-encoded certificates and key generated for testing.
pub struct TestCertificates {
    /// CA certificate in PEM format.
    pub ca_cert_pem: String,
    /// Broker certificate in PEM format (signed by the CA).
    pub broker_cert_pem: String,
    /// Broker private key in PEM format.
    pub broker_key_pem: String,
}

/// Generates a self-signed CA and a broker certificate for testing.
///
/// The broker certificate includes SANs for all provided `hostnames`,
/// plus `localhost` and `127.0.0.1` to cover all test connection scenarios.
pub fn generate_test_certificates(hostnames: &[&str]) -> TestCertificates {
    // Create CA key pair and self-signed certificate
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.distinguished_name.push(rcgen::DnType::CommonName, "Test CA");

    let ca_key_pair = KeyPair::generate().expect("Failed to generate CA key pair");
    let ca_cert = ca_params
        .self_signed(&ca_key_pair)
        .expect("Failed to create self-signed CA certificate");

    // Create broker key pair and certificate signed by the CA
    let mut broker_params = CertificateParams::default();
    broker_params.is_ca = IsCa::ExplicitNoCa;
    broker_params.distinguished_name.push(rcgen::DnType::CommonName, "Test Broker");

    // Build SANs: all provided hostnames + localhost + 127.0.0.1
    let mut sans = Vec::new();
    let mut seen_localhost = false;
    for &hostname in hostnames {
        if hostname == "localhost" {
            seen_localhost = true;
        }
        sans.push(SanType::DnsName(hostname.try_into().expect("Invalid hostname")));
    }
    if !seen_localhost {
        sans.push(SanType::DnsName("localhost".try_into().expect("Invalid localhost")));
    }
    sans.push(SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1))));
    broker_params.subject_alt_names = sans;

    let broker_key_pair = KeyPair::generate().expect("Failed to generate broker key pair");
    let broker_cert = broker_params
        .signed_by(&broker_key_pair, &ca_cert, &ca_key_pair)
        .expect("Failed to sign broker certificate with CA");

    TestCertificates {
        ca_cert_pem: ca_cert.pem(),
        broker_cert_pem: broker_cert.pem(),
        broker_key_pem: broker_key_pair.serialize_pem(),
    }
}
