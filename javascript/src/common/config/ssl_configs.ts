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

import { readOnlySet } from '../../internal/read_only_set';

/**
 * The constants of Java's `org.apache.kafka.common.config.SslConfigs`: every
 * `public static final` field, evaluated from the Java source, is a static
 * property of the same name; a deprecated one is left out. A `List` constant is
 * a frozen array and a `Set` constant a read-only set whose `add`, `delete` and
 * `clear` throw `UnsupportedOperationError`, both in Java's order. Not
 * generated: `addClientSslSupport(ConfigDef)`, the `ConfigDef` machinery or
 * methods that take a type the client does not translate (`ConfigDef`,
 * `AbstractConfig`).
 *
 * Java: `org.apache.kafka.common.config.SslConfigs`.
 */
export class SslConfigs {
  /**
   * The SSL protocol used to generate the SSLContext. The default is 'TLSv1.3',
   * which should be fine for most use cases. A typical alternative to the
   * default is 'TLSv1.2'. Allowed values for this config are dependent on the
   * JVM. Clients using the defaults for this config and 'ssl.enabled.protocols'
   * will downgrade to 'TLSv1.2' if the server does not support 'TLSv1.3'. If
   * this config is set to 'TLSv1.2', however, clients will not use 'TLSv1.3'
   * even if it is one of the values in `ssl.enabled.protocols` and the server
   * only supports 'TLSv1.3'.
   */
  static readonly SSL_PROTOCOL_CONFIG: string = 'ssl.protocol';
  /**
   * The documentation of `SslConfigs.SSL_PROTOCOL_CONFIG`.
   */
  static readonly SSL_PROTOCOL_DOC: string = 'The SSL protocol used to generate the SSLContext. The default is \'TLSv1.3\', which should be fine for most use cases. A typical alternative to the default is \'TLSv1.2\'. Allowed values for this config are dependent on the JVM. Clients using the defaults for this config and \'ssl.enabled.protocols\' will downgrade to \'TLSv1.2\' if the server does not support \'TLSv1.3\'. If this config is set to \'TLSv1.2\', however, clients will not use \'TLSv1.3\' even if it is one of the values in <code>ssl.enabled.protocols</code> and the server only supports \'TLSv1.3\'.';
  /**
   * Java's `SslConfigs.DEFAULT_SSL_PROTOCOL`.
   */
  static readonly DEFAULT_SSL_PROTOCOL: string = 'TLSv1.3';
  /**
   * The name of the security provider used for SSL connections. Default value
   * is the default security provider of the JVM.
   */
  static readonly SSL_PROVIDER_CONFIG: string = 'ssl.provider';
  /**
   * The documentation of `SslConfigs.SSL_PROVIDER_CONFIG`.
   */
  static readonly SSL_PROVIDER_DOC: string = 'The name of the security provider used for SSL connections. Default value is the default security provider of the JVM.';
  /**
   * A list of cipher suites. This is a named combination of authentication,
   * encryption, MAC and key exchange algorithm used to negotiate the security
   * settings for a network connection using TLS or SSL network protocol. By
   * default all the available cipher suites are supported.
   */
  static readonly SSL_CIPHER_SUITES_CONFIG: string = 'ssl.cipher.suites';
  /**
   * The documentation of `SslConfigs.SSL_CIPHER_SUITES_CONFIG`.
   */
  static readonly SSL_CIPHER_SUITES_DOC: string = 'A list of cipher suites. This is a named combination of authentication, encryption, MAC and key exchange algorithm used to negotiate the security settings for a network connection using TLS or SSL network protocol. By default all the available cipher suites are supported.';
  /**
   * The list of protocols enabled for SSL connections. The default is
   * 'TLSv1.2,TLSv1.3'. This means that clients and servers will prefer TLSv1.3
   * if both support it and fallback to TLSv1.2 otherwise (assuming both support
   * at least TLSv1.2). This default should be fine for most use cases. If this
   * configuration is set to an empty list, Kafka will use the protocols enabled
   * by default in the underlying SSLEngine, which may include additional
   * protocols depending on the JVM version. Also see the config documentation
   * for `ssl.protocol` to understand how it can impact the TLS version
   * negotiation behavior.
   */
  static readonly SSL_ENABLED_PROTOCOLS_CONFIG: string = 'ssl.enabled.protocols';
  /**
   * The documentation of `SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG`.
   */
  static readonly SSL_ENABLED_PROTOCOLS_DOC: string = 'The list of protocols enabled for SSL connections. The default is \'TLSv1.2,TLSv1.3\'. This means that clients and servers will prefer TLSv1.3 if both support it and fallback to TLSv1.2 otherwise (assuming both support at least TLSv1.2). This default should be fine for most use cases. If this configuration is set to an empty list, Kafka will use the protocols enabled by default in the underlying SSLEngine, which may include additional protocols depending on the JVM version. Also see the config documentation for <code>ssl.protocol</code> to understand how it can impact the TLS version negotiation behavior.';
  /**
   * Java's `SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS`.
   */
  static readonly DEFAULT_SSL_ENABLED_PROTOCOLS: string = 'TLSv1.2,TLSv1.3';
  /**
   * The file format of the key store file. This is optional for client. The
   * values currently supported by the default `ssl.engine.factory.class` are
   * [JKS, PKCS12, PEM].
   */
  static readonly SSL_KEYSTORE_TYPE_CONFIG: string = 'ssl.keystore.type';
  /**
   * The documentation of `SslConfigs.SSL_KEYSTORE_TYPE_CONFIG`.
   */
  static readonly SSL_KEYSTORE_TYPE_DOC: string = 'The file format of the key store file. This is optional for client. The values currently supported by the default <code>ssl.engine.factory.class</code> are [JKS, PKCS12, PEM].';
  /**
   * Java's `SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE`.
   *
   * Translation note: Java's value. The Rust core's default `ssl.keystore.type`
   * is `PEM`, the only key store format it reads (no JKS or PKCS12).
   */
  static readonly DEFAULT_SSL_KEYSTORE_TYPE: string = 'JKS';
  /**
   * Private key in the format specified by 'ssl.keystore.type'. Default SSL
   * engine factory supports only PEM format with PKCS#8 keys. If the key is
   * encrypted, key password must be specified using 'ssl.key.password'
   */
  static readonly SSL_KEYSTORE_KEY_CONFIG: string = 'ssl.keystore.key';
  /**
   * The documentation of `SslConfigs.SSL_KEYSTORE_KEY_CONFIG`.
   */
  static readonly SSL_KEYSTORE_KEY_DOC: string = 'Private key in the format specified by \'ssl.keystore.type\'. Default SSL engine factory supports only PEM format with PKCS#8 keys. If the key is encrypted, key password must be specified using \'ssl.key.password\'';
  /**
   * Certificate chain in the format specified by 'ssl.keystore.type'. Default
   * SSL engine factory supports only PEM format with a list of X.509
   * certificates
   */
  static readonly SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG: string = 'ssl.keystore.certificate.chain';
  /**
   * The documentation of `SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG`.
   */
  static readonly SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC: string = 'Certificate chain in the format specified by \'ssl.keystore.type\'. Default SSL engine factory supports only PEM format with a list of X.509 certificates';
  /**
   * Trusted certificates in the format specified by 'ssl.truststore.type'.
   * Default SSL engine factory supports only PEM format with X.509
   * certificates.
   */
  static readonly SSL_TRUSTSTORE_CERTIFICATES_CONFIG: string = 'ssl.truststore.certificates';
  /**
   * The documentation of `SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG`.
   */
  static readonly SSL_TRUSTSTORE_CERTIFICATES_DOC: string = 'Trusted certificates in the format specified by \'ssl.truststore.type\'. Default SSL engine factory supports only PEM format with X.509 certificates.';
  /**
   * The location of the key store file. This is optional for client and can be
   * used for two-way authentication for client.
   */
  static readonly SSL_KEYSTORE_LOCATION_CONFIG: string = 'ssl.keystore.location';
  /**
   * The documentation of `SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG`.
   */
  static readonly SSL_KEYSTORE_LOCATION_DOC: string = 'The location of the key store file. This is optional for client and can be used for two-way authentication for client.';
  /**
   * The store password for the key store file. This is optional for client and
   * only needed if 'ssl.keystore.location' is configured. Key store password is
   * not supported for PEM format.
   */
  static readonly SSL_KEYSTORE_PASSWORD_CONFIG: string = 'ssl.keystore.password';
  /**
   * The documentation of `SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG`.
   */
  static readonly SSL_KEYSTORE_PASSWORD_DOC: string = 'The store password for the key store file. This is optional for client and only needed if \'ssl.keystore.location\' is configured. Key store password is not supported for PEM format.';
  /**
   * The password of the private key in the key store file or the PEM key
   * specified in 'ssl.keystore.key'.
   */
  static readonly SSL_KEY_PASSWORD_CONFIG: string = 'ssl.key.password';
  /**
   * The documentation of `SslConfigs.SSL_KEY_PASSWORD_CONFIG`.
   */
  static readonly SSL_KEY_PASSWORD_DOC: string = 'The password of the private key in the key store file or the PEM key specified in \'ssl.keystore.key\'.';
  /**
   * The file format of the trust store file. The values currently supported by
   * the default `ssl.engine.factory.class` are [JKS, PKCS12, PEM].
   */
  static readonly SSL_TRUSTSTORE_TYPE_CONFIG: string = 'ssl.truststore.type';
  /**
   * The documentation of `SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG`.
   */
  static readonly SSL_TRUSTSTORE_TYPE_DOC: string = 'The file format of the trust store file. The values currently supported by the default <code>ssl.engine.factory.class</code> are [JKS, PKCS12, PEM].';
  /**
   * Java's `SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE`.
   *
   * Translation note: Java's value. The Rust core's default
   * `ssl.truststore.type` is `PEM`, the only trust store format it reads (no
   * JKS or PKCS12).
   */
  static readonly DEFAULT_SSL_TRUSTSTORE_TYPE: string = 'JKS';
  /**
   * The location of the trust store file.
   */
  static readonly SSL_TRUSTSTORE_LOCATION_CONFIG: string = 'ssl.truststore.location';
  /**
   * The documentation of `SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG`.
   */
  static readonly SSL_TRUSTSTORE_LOCATION_DOC: string = 'The location of the trust store file.';
  /**
   * The password for the trust store file. If a password is not set, trust
   * store file configured will still be used, but integrity checking is
   * disabled. Trust store password is not supported for PEM format.
   */
  static readonly SSL_TRUSTSTORE_PASSWORD_CONFIG: string = 'ssl.truststore.password';
  /**
   * The documentation of `SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG`.
   */
  static readonly SSL_TRUSTSTORE_PASSWORD_DOC: string = 'The password for the trust store file. If a password is not set, trust store file configured will still be used, but integrity checking is disabled. Trust store password is not supported for PEM format.';
  /**
   * The algorithm used by key manager factory for SSL connections. Default
   * value is the key manager factory algorithm configured for the Java Virtual
   * Machine.
   */
  static readonly SSL_KEYMANAGER_ALGORITHM_CONFIG: string = 'ssl.keymanager.algorithm';
  /**
   * The documentation of `SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG`.
   */
  static readonly SSL_KEYMANAGER_ALGORITHM_DOC: string = 'The algorithm used by key manager factory for SSL connections. Default value is the key manager factory algorithm configured for the Java Virtual Machine.';
  /**
   * Java's `SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM`.
   */
  static readonly DEFAULT_SSL_KEYMANGER_ALGORITHM: string = 'SunX509';
  /**
   * The algorithm used by trust manager factory for SSL connections. Default
   * value is the trust manager factory algorithm configured for the Java
   * Virtual Machine.
   */
  static readonly SSL_TRUSTMANAGER_ALGORITHM_CONFIG: string = 'ssl.trustmanager.algorithm';
  /**
   * The documentation of `SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG`.
   */
  static readonly SSL_TRUSTMANAGER_ALGORITHM_DOC: string = 'The algorithm used by trust manager factory for SSL connections. Default value is the trust manager factory algorithm configured for the Java Virtual Machine.';
  /**
   * Java's `SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM`.
   */
  static readonly DEFAULT_SSL_TRUSTMANAGER_ALGORITHM: string = 'PKIX';
  /**
   * The endpoint identification algorithm to validate server hostname using
   * server certificate.
   */
  static readonly SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG: string = 'ssl.endpoint.identification.algorithm';
  /**
   * The documentation of
   * `SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG`.
   */
  static readonly SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC: string = 'The endpoint identification algorithm to validate server hostname using server certificate. ';
  /**
   * Java's `SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM`.
   */
  static readonly DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM: string = 'https';
  /**
   * The SecureRandom PRNG implementation to use for SSL cryptography
   * operations.
   */
  static readonly SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG: string = 'ssl.secure.random.implementation';
  /**
   * The documentation of `SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG`.
   */
  static readonly SSL_SECURE_RANDOM_IMPLEMENTATION_DOC: string = 'The SecureRandom PRNG implementation to use for SSL cryptography operations. ';
  /**
   * The class of type org.apache.kafka.common.security.auth.SslEngineFactory to
   * provide SSLEngine objects. Default value is
   * org.apache.kafka.common.security.ssl.DefaultSslEngineFactory.
   * Alternatively, setting this to
   * org.apache.kafka.common.security.ssl.CommonNameLoggingSslEngineFactory will
   * log the common name of expired SSL certificates used by clients to
   * authenticate at any of the brokers with log level INFO. Note that this will
   * cause a tiny delay during establishment of new connections from mTLS
   * clients to brokers due to the extra code for examining the certificate
   * chain provided by the client. Note further that the implementation uses a
   * custom truststore based on the standard Java truststore and thus might be
   * considered a security risk due to not being as mature as the standard one.
   */
  static readonly SSL_ENGINE_FACTORY_CLASS_CONFIG: string = 'ssl.engine.factory.class';
  /**
   * The documentation of `SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG`.
   */
  static readonly SSL_ENGINE_FACTORY_CLASS_DOC: string = 'The class of type org.apache.kafka.common.security.auth.SslEngineFactory to provide SSLEngine objects. Default value is org.apache.kafka.common.security.ssl.DefaultSslEngineFactory. Alternatively, setting this to org.apache.kafka.common.security.ssl.CommonNameLoggingSslEngineFactory will log the common name of expired SSL certificates used by clients to authenticate at any of the brokers with log level INFO. Note that this will cause a tiny delay during establishment of new connections from mTLS clients to brokers due to the extra code for examining the certificate chain provided by the client. Note further that the implementation uses a custom truststore based on the standard Java truststore and thus might be considered a security risk due to not being as mature as the standard one.';
  /**
   * Java's `SslConfigs.RECONFIGURABLE_CONFIGS`.
   */
  static readonly RECONFIGURABLE_CONFIGS: ReadonlySet<string> = readOnlySet(['ssl.keystore.type', 'ssl.keystore.location', 'ssl.keystore.password', 'ssl.key.password', 'ssl.truststore.type', 'ssl.truststore.location', 'ssl.truststore.password', 'ssl.keystore.certificate.chain', 'ssl.keystore.key', 'ssl.truststore.certificates']);
  /**
   * Java's `SslConfigs.NON_RECONFIGURABLE_CONFIGS`.
   */
  static readonly NON_RECONFIGURABLE_CONFIGS: ReadonlySet<string> = readOnlySet(['ssl.client.auth', 'ssl.protocol', 'ssl.provider', 'ssl.cipher.suites', 'ssl.enabled.protocols', 'ssl.keymanager.algorithm', 'ssl.trustmanager.algorithm', 'ssl.endpoint.identification.algorithm', 'ssl.secure.random.implementation', 'ssl.engine.factory.class']);

  /**
   * Java's default constructor. The class holds only its constants, which are static.
   */
  constructor() {}
}
