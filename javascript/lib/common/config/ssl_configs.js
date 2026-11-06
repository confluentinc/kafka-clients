// Copyright 2026 Confluent Inc.
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

// GENERATED, DO NOT EDIT. Produced by `cargo xtask generate-error-codes` from
// Java's `org.apache.kafka.common.config.SslConfigs`;
// `cargo xtask check-generated` checks it is up to date.

'use strict';

/**
 * The constants of Java's `org.apache.kafka.common.config.SslConfigs`: every
 * `public static final` field, evaluated from the Java source, is a static
 * property of the same name; a deprecated one is left out. A `List` or `Set`
 * constant is a frozen array in Java's order. Not generated:
 * `addClientSslSupport(ConfigDef)`, the `ConfigDef` machinery or methods that
 * take a type the client does not translate (`ConfigDef`, `AbstractConfig`).
 *
 * Java: `org.apache.kafka.common.config.SslConfigs`.
 */
class SslConfigs {
  /** @readonly @type {string} */
  static SSL_PROTOCOL_CONFIG = 'ssl.protocol';
  /** @readonly @type {string} */
  static SSL_PROTOCOL_DOC = 'The SSL protocol used to generate the SSLContext. The default is \'TLSv1.3\', which should be fine for most use cases. A typical alternative to the default is \'TLSv1.2\'. Allowed values for this config are dependent on the JVM. Clients using the defaults for this config and \'ssl.enabled.protocols\' will downgrade to \'TLSv1.2\' if the server does not support \'TLSv1.3\'. If this config is set to \'TLSv1.2\', however, clients will not use \'TLSv1.3\' even if it is one of the values in <code>ssl.enabled.protocols</code> and the server only supports \'TLSv1.3\'.';
  /** @readonly @type {string} */
  static DEFAULT_SSL_PROTOCOL = 'TLSv1.3';
  /** @readonly @type {string} */
  static SSL_PROVIDER_CONFIG = 'ssl.provider';
  /** @readonly @type {string} */
  static SSL_PROVIDER_DOC = 'The name of the security provider used for SSL connections. Default value is the default security provider of the JVM.';
  /** @readonly @type {string} */
  static SSL_CIPHER_SUITES_CONFIG = 'ssl.cipher.suites';
  /** @readonly @type {string} */
  static SSL_CIPHER_SUITES_DOC = 'A list of cipher suites. This is a named combination of authentication, encryption, MAC and key exchange algorithm used to negotiate the security settings for a network connection using TLS or SSL network protocol. By default all the available cipher suites are supported.';
  /** @readonly @type {string} */
  static SSL_ENABLED_PROTOCOLS_CONFIG = 'ssl.enabled.protocols';
  /** @readonly @type {string} */
  static SSL_ENABLED_PROTOCOLS_DOC = 'The list of protocols enabled for SSL connections. The default is \'TLSv1.2,TLSv1.3\'. This means that clients and servers will prefer TLSv1.3 if both support it and fallback to TLSv1.2 otherwise (assuming both support at least TLSv1.2). This default should be fine for most use cases. If this configuration is set to an empty list, Kafka will use the protocols enabled by default in the underlying SSLEngine, which may include additional protocols depending on the JVM version. Also see the config documentation for <code>ssl.protocol</code> to understand how it can impact the TLS version negotiation behavior.';
  /** @readonly @type {string} */
  static DEFAULT_SSL_ENABLED_PROTOCOLS = 'TLSv1.2,TLSv1.3';
  /** @readonly @type {string} */
  static SSL_KEYSTORE_TYPE_CONFIG = 'ssl.keystore.type';
  /** @readonly @type {string} */
  static SSL_KEYSTORE_TYPE_DOC = 'The file format of the key store file. This is optional for client. The values currently supported by the default <code>ssl.engine.factory.class</code> are [JKS, PKCS12, PEM].';
  /**
   * Translation note: Java's value. The Rust core's default `ssl.keystore.type`
   * is `PEM`, the only key store format it reads (no JKS or PKCS12).
   *
   * @readonly
   * @type {string}
   */
  static DEFAULT_SSL_KEYSTORE_TYPE = 'JKS';
  /** @readonly @type {string} */
  static SSL_KEYSTORE_KEY_CONFIG = 'ssl.keystore.key';
  /** @readonly @type {string} */
  static SSL_KEYSTORE_KEY_DOC = 'Private key in the format specified by \'ssl.keystore.type\'. Default SSL engine factory supports only PEM format with PKCS#8 keys. If the key is encrypted, key password must be specified using \'ssl.key.password\'';
  /** @readonly @type {string} */
  static SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG = 'ssl.keystore.certificate.chain';
  /** @readonly @type {string} */
  static SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC = 'Certificate chain in the format specified by \'ssl.keystore.type\'. Default SSL engine factory supports only PEM format with a list of X.509 certificates';
  /** @readonly @type {string} */
  static SSL_TRUSTSTORE_CERTIFICATES_CONFIG = 'ssl.truststore.certificates';
  /** @readonly @type {string} */
  static SSL_TRUSTSTORE_CERTIFICATES_DOC = 'Trusted certificates in the format specified by \'ssl.truststore.type\'. Default SSL engine factory supports only PEM format with X.509 certificates.';
  /** @readonly @type {string} */
  static SSL_KEYSTORE_LOCATION_CONFIG = 'ssl.keystore.location';
  /** @readonly @type {string} */
  static SSL_KEYSTORE_LOCATION_DOC = 'The location of the key store file. This is optional for client and can be used for two-way authentication for client.';
  /** @readonly @type {string} */
  static SSL_KEYSTORE_PASSWORD_CONFIG = 'ssl.keystore.password';
  /** @readonly @type {string} */
  static SSL_KEYSTORE_PASSWORD_DOC = 'The store password for the key store file. This is optional for client and only needed if \'ssl.keystore.location\' is configured. Key store password is not supported for PEM format.';
  /** @readonly @type {string} */
  static SSL_KEY_PASSWORD_CONFIG = 'ssl.key.password';
  /** @readonly @type {string} */
  static SSL_KEY_PASSWORD_DOC = 'The password of the private key in the key store file or the PEM key specified in \'ssl.keystore.key\'.';
  /** @readonly @type {string} */
  static SSL_TRUSTSTORE_TYPE_CONFIG = 'ssl.truststore.type';
  /** @readonly @type {string} */
  static SSL_TRUSTSTORE_TYPE_DOC = 'The file format of the trust store file. The values currently supported by the default <code>ssl.engine.factory.class</code> are [JKS, PKCS12, PEM].';
  /**
   * Translation note: Java's value. The Rust core's default
   * `ssl.truststore.type` is `PEM`, the only trust store format it reads (no
   * JKS or PKCS12).
   *
   * @readonly
   * @type {string}
   */
  static DEFAULT_SSL_TRUSTSTORE_TYPE = 'JKS';
  /** @readonly @type {string} */
  static SSL_TRUSTSTORE_LOCATION_CONFIG = 'ssl.truststore.location';
  /** @readonly @type {string} */
  static SSL_TRUSTSTORE_LOCATION_DOC = 'The location of the trust store file.';
  /** @readonly @type {string} */
  static SSL_TRUSTSTORE_PASSWORD_CONFIG = 'ssl.truststore.password';
  /** @readonly @type {string} */
  static SSL_TRUSTSTORE_PASSWORD_DOC = 'The password for the trust store file. If a password is not set, trust store file configured will still be used, but integrity checking is disabled. Trust store password is not supported for PEM format.';
  /** @readonly @type {string} */
  static SSL_KEYMANAGER_ALGORITHM_CONFIG = 'ssl.keymanager.algorithm';
  /** @readonly @type {string} */
  static SSL_KEYMANAGER_ALGORITHM_DOC = 'The algorithm used by key manager factory for SSL connections. Default value is the key manager factory algorithm configured for the Java Virtual Machine.';
  /** @readonly @type {string} */
  static DEFAULT_SSL_KEYMANGER_ALGORITHM = 'SunX509';
  /** @readonly @type {string} */
  static SSL_TRUSTMANAGER_ALGORITHM_CONFIG = 'ssl.trustmanager.algorithm';
  /** @readonly @type {string} */
  static SSL_TRUSTMANAGER_ALGORITHM_DOC = 'The algorithm used by trust manager factory for SSL connections. Default value is the trust manager factory algorithm configured for the Java Virtual Machine.';
  /** @readonly @type {string} */
  static DEFAULT_SSL_TRUSTMANAGER_ALGORITHM = 'PKIX';
  /** @readonly @type {string} */
  static SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG = 'ssl.endpoint.identification.algorithm';
  /** @readonly @type {string} */
  static SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC = 'The endpoint identification algorithm to validate server hostname using server certificate. ';
  /** @readonly @type {string} */
  static DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM = 'https';
  /** @readonly @type {string} */
  static SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG = 'ssl.secure.random.implementation';
  /** @readonly @type {string} */
  static SSL_SECURE_RANDOM_IMPLEMENTATION_DOC = 'The SecureRandom PRNG implementation to use for SSL cryptography operations. ';
  /** @readonly @type {string} */
  static SSL_ENGINE_FACTORY_CLASS_CONFIG = 'ssl.engine.factory.class';
  /** @readonly @type {string} */
  static SSL_ENGINE_FACTORY_CLASS_DOC = 'The class of type org.apache.kafka.common.security.auth.SslEngineFactory to provide SSLEngine objects. Default value is org.apache.kafka.common.security.ssl.DefaultSslEngineFactory. Alternatively, setting this to org.apache.kafka.common.security.ssl.CommonNameLoggingSslEngineFactory will log the common name of expired SSL certificates used by clients to authenticate at any of the brokers with log level INFO. Note that this will cause a tiny delay during establishment of new connections from mTLS clients to brokers due to the extra code for examining the certificate chain provided by the client. Note further that the implementation uses a custom truststore based on the standard Java truststore and thus might be considered a security risk due to not being as mature as the standard one.';
  /** @readonly @type {ReadonlyArray<string>} */
  static RECONFIGURABLE_CONFIGS = Object.freeze(['ssl.keystore.type', 'ssl.keystore.location', 'ssl.keystore.password', 'ssl.key.password', 'ssl.truststore.type', 'ssl.truststore.location', 'ssl.truststore.password', 'ssl.keystore.certificate.chain', 'ssl.keystore.key', 'ssl.truststore.certificates']);
  /** @readonly @type {ReadonlyArray<string>} */
  static NON_RECONFIGURABLE_CONFIGS = Object.freeze(['ssl.client.auth', 'ssl.protocol', 'ssl.provider', 'ssl.cipher.suites', 'ssl.enabled.protocols', 'ssl.keymanager.algorithm', 'ssl.trustmanager.algorithm', 'ssl.endpoint.identification.algorithm', 'ssl.secure.random.implementation', 'ssl.engine.factory.class']);
}

module.exports = { SslConfigs };
