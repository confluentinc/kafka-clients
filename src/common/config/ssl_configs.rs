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

//! Translation of `org.apache.kafka.common.config.SslConfigs` (constants only).
//!
//! These are the canonical SSL property names. The Rust client uses
//! `tokio_rustls` rather than the JVM's `SSLEngine`, but the property names
//! are part of the public API surface and must match the Java values.
//!
//! Defaults that hinge on JVM behaviour (`KeyManagerFactory.getDefaultAlgorithm()`,
//! `TrustManagerFactory.getDefaultAlgorithm()`) are intentionally not
//! translated here; ProducerConfig will resolve those at construction time
//! against `rustls`'s own defaults.

pub const SSL_PROTOCOL_CONFIG: &str = "ssl.protocol";
pub const DEFAULT_SSL_PROTOCOL: &str = "TLSv1.3";

pub const SSL_PROVIDER_CONFIG: &str = "ssl.provider";
pub const SSL_CIPHER_SUITES_CONFIG: &str = "ssl.cipher.suites";

pub const SSL_ENABLED_PROTOCOLS_CONFIG: &str = "ssl.enabled.protocols";
pub const DEFAULT_SSL_ENABLED_PROTOCOLS: &str = "TLSv1.2,TLSv1.3";

pub const SSL_KEYSTORE_TYPE_CONFIG: &str = "ssl.keystore.type";
pub const DEFAULT_SSL_KEYSTORE_TYPE: &str = "JKS";

pub const SSL_KEYSTORE_KEY_CONFIG: &str = "ssl.keystore.key";
pub const SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG: &str = "ssl.keystore.certificate.chain";
pub const SSL_TRUSTSTORE_CERTIFICATES_CONFIG: &str = "ssl.truststore.certificates";
pub const SSL_KEYSTORE_LOCATION_CONFIG: &str = "ssl.keystore.location";
pub const SSL_KEYSTORE_PASSWORD_CONFIG: &str = "ssl.keystore.password";
pub const SSL_KEY_PASSWORD_CONFIG: &str = "ssl.key.password";

pub const SSL_TRUSTSTORE_TYPE_CONFIG: &str = "ssl.truststore.type";
pub const DEFAULT_SSL_TRUSTSTORE_TYPE: &str = "JKS";
pub const SSL_TRUSTSTORE_LOCATION_CONFIG: &str = "ssl.truststore.location";
pub const SSL_TRUSTSTORE_PASSWORD_CONFIG: &str = "ssl.truststore.password";

pub const SSL_KEYMANAGER_ALGORITHM_CONFIG: &str = "ssl.keymanager.algorithm";
pub const SSL_TRUSTMANAGER_ALGORITHM_CONFIG: &str = "ssl.trustmanager.algorithm";

pub const SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG: &str = "ssl.secure.random.implementation";

pub const SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG: &str = "ssl.endpoint.identification.algorithm";
pub const DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM: &str = "https";

pub const SSL_CLIENT_AUTH_CONFIG: &str = "ssl.client.auth";

pub const SSL_ENGINE_FACTORY_CLASS_CONFIG: &str = "ssl.engine.factory.class";
