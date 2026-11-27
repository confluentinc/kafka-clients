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

'use strict';

// The producer and the consumer over SSL and mTLS: runs only when the SSL broker's variables
// are set (`make -C javascript test-integration-ssl` starts the broker and
// writes the certificates). The broker's certificate is valid for localhost
// only, so a connection to 127.0.0.1 is a hostname mismatch.

const assert = require('node:assert/strict');

const { AuthenticationError, SslAuthenticationError } = require('@confluentinc/kafka-javascript/common/errors');
const { certPath, consumeOneRecord, pem, pollFailingAuthentication, produceOneRecord, sendFailingAuthentication } = require('./_security');

const SSL = process.env.KAFKA_SSL_BOOTSTRAP_SERVERS;
const MTLS = process.env.KAFKA_MTLS_BOOTSTRAP_SERVERS;
const PLAINTEXT = process.env.KAFKA_SSL_PLAINTEXT_BOOTSTRAP_SERVERS;
const SKIP =
  SSL && MTLS && PLAINTEXT && process.env.KAFKA_SECURITY_DIR
    ? false
    : 'KAFKA_SSL_BOOTSTRAP_SERVERS, KAFKA_MTLS_BOOTSTRAP_SERVERS, KAFKA_SSL_PLAINTEXT_BOOTSTRAP_SERVERS or KAFKA_SECURITY_DIR is not set: no SSL broker';
if (SKIP) console.log(`skipping the SSL integration tests: ${SKIP}`);

// Strings in every test that runs: the tests are skipped when one is unset.
const ssl = /** @type {string} */ (SSL);
const mtls = /** @type {string} */ (MTLS);
const plaintext = /** @type {string} */ (PLAINTEXT);

/**
 * The bootstrap address of `bootstrap` with its host replaced by 127.0.0.1,
 * an address the broker's certificate does not name.
 *
 * @param {string} bootstrap
 */
function byAddress(bootstrap) {
  return `127.0.0.1:${bootstrap.split(':').pop()}`;
}

(SKIP ? test.skip : test)(
  'produces over SSL with a truststore file: one record with headers, flush, close',
  async () => {
    await produceOneRecord(
      { 'bootstrap.servers': ssl, 'security.protocol': 'SSL', 'ssl.truststore.location': certPath('ca.pem') },
      plaintext,
      'ssl-file',
    );
  },
  120000,
);

(SKIP ? test.skip : test)(
  'produces over SSL with inline truststore certificates: one record with headers, flush, close',
  async () => {
    await produceOneRecord(
      { 'bootstrap.servers': ssl, 'security.protocol': 'SSL', 'ssl.truststore.type': 'PEM', 'ssl.truststore.certificates': pem('ca.pem') },
      plaintext,
      'ssl-inline',
    );
  },
  120000,
);

(SKIP ? test.skip : test)(
  'produces over mTLS with a PEM keystore file: one record with headers, flush, close',
  async () => {
    await produceOneRecord(
      {
        'bootstrap.servers': mtls,
        'security.protocol': 'SSL',
        'ssl.truststore.location': certPath('ca.pem'),
        'ssl.keystore.type': 'PEM',
        'ssl.keystore.location': certPath('client-keystore.pem'),
      },
      plaintext,
      'mtls-file',
    );
  },
  120000,
);

(SKIP ? test.skip : test)(
  'produces over mTLS with an inline certificate chain and key: one record with headers, flush, close',
  async () => {
    await produceOneRecord(
      {
        'bootstrap.servers': mtls,
        'security.protocol': 'SSL',
        'ssl.truststore.type': 'PEM',
        'ssl.truststore.certificates': pem('ca.pem'),
        'ssl.keystore.type': 'PEM',
        'ssl.keystore.certificate.chain': pem('client.pem'),
        'ssl.keystore.key': pem('client.key'),
      },
      plaintext,
      'mtls-inline',
    );
  },
  120000,
);

// Java's SslTransportLayer#handshakeFailure turns the handshake's
// SSLHandshakeException into SslAuthenticationException("SSL handshake
// failed"); NetworkClient#processDisconnection hands it to the metadata as a
// fatal error, ProducerMetadata#awaitUpdate throws it from the metadata wait,
// and KafkaProducer#doSend's ApiException catch runs the callback with it and
// returns a FutureFailure.
(SKIP ? test.skip : test)(
  "a certificate from a CA the truststore does not hold fails the first send with SslAuthenticationError('SSL handshake failed')",
  async () => {
    const failure = await sendFailingAuthentication(
      { 'bootstrap.servers': ssl, 'security.protocol': 'SSL', 'ssl.truststore.location': certPath('untrusted-ca.pem') },
      'ssl-untrusted',
    );
    assert.equal(failure.constructor, SslAuthenticationError);
    assert.ok(failure instanceof AuthenticationError);
    assert.equal(failure.name, 'SslAuthenticationError');
    assert.equal(failure.message, 'SSL handshake failed');
  },
  120000,
);

(SKIP ? test.skip : test)(
  "a host the broker's certificate does not name fails the first send with SslAuthenticationError under 'https'",
  async () => {
    const failure = await sendFailingAuthentication(
      {
        'bootstrap.servers': byAddress(ssl),
        'security.protocol': 'SSL',
        'ssl.truststore.location': certPath('ca.pem'),
        'ssl.endpoint.identification.algorithm': 'https',
      },
      'ssl-hostname',
    );
    assert.equal(failure.constructor, SslAuthenticationError);
    assert.equal(failure.message, 'SSL handshake failed');
  },
  120000,
);

// With ssl.endpoint.identification.algorithm empty, Java's
// DefaultSslEngineFactory#createSslEngine sets no endpoint identification, so
// the certificate's chain is checked and its names are not.
(SKIP ? test.skip : test)(
  "a host the broker's certificate does not name connects when ssl.endpoint.identification.algorithm is ''",
  async () => {
    await produceOneRecord(
      {
        'bootstrap.servers': byAddress(ssl),
        'security.protocol': 'SSL',
        'ssl.truststore.location': certPath('ca.pem'),
        'ssl.endpoint.identification.algorithm': '',
      },
      plaintext,
      'ssl-no-hostname',
    );
  },
  120000,
);

(SKIP ? test.skip : test)(
  'consumes over SSL with a truststore file: one record with its header, committed and read back',
  async () => {
    await consumeOneRecord(
      { 'bootstrap.servers': ssl, 'security.protocol': 'SSL', 'ssl.truststore.location': certPath('ca.pem') },
      plaintext,
      'consumer-ssl-file',
    );
  },
  120000,
);

(SKIP ? test.skip : test)(
  'consumes over SSL with inline truststore certificates: one record with its header, committed and read back',
  async () => {
    await consumeOneRecord(
      { 'bootstrap.servers': ssl, 'security.protocol': 'SSL', 'ssl.truststore.type': 'PEM', 'ssl.truststore.certificates': pem('ca.pem') },
      plaintext,
      'consumer-ssl-inline',
    );
  },
  120000,
);

(SKIP ? test.skip : test)(
  'consumes over mTLS with an inline certificate chain and key: one record with its header, committed and read back',
  async () => {
    await consumeOneRecord(
      {
        'bootstrap.servers': mtls,
        'security.protocol': 'SSL',
        'ssl.truststore.type': 'PEM',
        'ssl.truststore.certificates': pem('ca.pem'),
        'ssl.keystore.type': 'PEM',
        'ssl.keystore.certificate.chain': pem('client.pem'),
        'ssl.keystore.key': pem('client.key'),
      },
      plaintext,
      'consumer-mtls-inline',
    );
  },
  120000,
);

// Java's SslTransportLayer#handshakeFailure gives
// SslAuthenticationException("SSL handshake failed"), which the metadata
// keeps as a fatal error; NetworkClientDelegate#maybePropagateMetadataError
// takes it (the consumer's delegate keeps it rather than queue an
// ErrorEvent), ConsumerNetworkThread#maybeFailOnMetadataError fails the
// pending AsyncPollEvent with it, and AsyncKafkaConsumer#poll throws it.
(SKIP ? test.skip : test)(
  "a certificate from a CA the truststore does not hold fails the consumer's poll() with SslAuthenticationError('SSL handshake failed')",
  async () => {
    const failure = await pollFailingAuthentication(
      { 'bootstrap.servers': ssl, 'security.protocol': 'SSL', 'ssl.truststore.location': certPath('untrusted-ca.pem') },
      plaintext,
      'consumer-ssl-untrusted',
    );
    assert.ok(failure instanceof SslAuthenticationError, String(failure));
    assert.equal(failure.constructor, SslAuthenticationError);
    assert.ok(failure instanceof AuthenticationError);
    assert.equal(failure.message, 'SSL handshake failed');
  },
  120000,
);

(SKIP ? test.skip : test)(
  "a host the broker's certificate does not name fails the consumer's poll() with SslAuthenticationError under 'https'",
  async () => {
    const failure = await pollFailingAuthentication(
      {
        'bootstrap.servers': byAddress(ssl),
        'security.protocol': 'SSL',
        'ssl.truststore.location': certPath('ca.pem'),
        'ssl.endpoint.identification.algorithm': 'https',
      },
      plaintext,
      'consumer-ssl-hostname',
    );
    assert.ok(failure instanceof SslAuthenticationError, String(failure));
    assert.equal(failure.constructor, SslAuthenticationError);
    assert.equal(failure.message, 'SSL handshake failed');
  },
  120000,
);

(SKIP ? test.skip : test)(
  "a host the broker's certificate does not name: the consumer reads when ssl.endpoint.identification.algorithm is ''",
  async () => {
    await consumeOneRecord(
      {
        'bootstrap.servers': byAddress(ssl),
        'security.protocol': 'SSL',
        'ssl.truststore.location': certPath('ca.pem'),
        'ssl.endpoint.identification.algorithm': '',
      },
      plaintext,
      'consumer-ssl-no-hostname',
    );
  },
  120000,
);
