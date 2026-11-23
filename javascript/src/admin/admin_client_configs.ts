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
// Java's `AdminClientConfig` (with `SslConfigs` / `SaslConfigs`);
// `cargo xtask check-generated` checks it is up to date.

/**
 * An admin client's configuration: Java's `AdminClientConfig` keys, as a
 * plain object (a `Map` is refused). Each value is written as the string the
 * core parses (`true` as `"true"`, numbers and bigints in decimal, arrays
 * joined with `,`, anything else by `String()`); `null` and `undefined` leave
 * the key out. The core checks every key and value, with Java's messages.
 *
 * @remarks *(js-only)* The type of the configuration `Admin.create` takes,
 * Java's `Map<String, Object>` and `Properties` as one. It lists the keys
 * Java's `AdminClientConfig` defines, for autocompletion, and takes any other
 * string too, through `(string & {})`, since an unknown key goes to the
 * core.
 */
export type AdminClientConfigs = {
  [key in
    | 'bootstrap.controllers'
    | 'bootstrap.resolve.timeout.ms'
    | 'bootstrap.servers'
    | 'client.dns.lookup'
    | 'client.id'
    | 'config.providers'
    | 'connections.max.idle.ms'
    | 'default.api.timeout.ms'
    | 'enable.metrics.push'
    | 'metadata.cluster.check.enable'
    | 'metadata.max.age.ms'
    | 'metadata.recovery.rebootstrap.trigger.ms'
    | 'metadata.recovery.strategy'
    | 'metric.reporters'
    | 'metrics.num.samples'
    | 'metrics.recording.level'
    | 'metrics.sample.window.ms'
    | 'receive.buffer.bytes'
    | 'reconnect.backoff.max.ms'
    | 'reconnect.backoff.ms'
    | 'request.timeout.ms'
    | 'retries'
    | 'retry.backoff.max.ms'
    | 'retry.backoff.ms'
    | 'sasl.client.callback.handler.class'
    | 'sasl.jaas.config'
    | 'sasl.kerberos.kinit.cmd'
    | 'sasl.kerberos.min.time.before.relogin'
    | 'sasl.kerberos.service.name'
    | 'sasl.kerberos.ticket.renew.jitter'
    | 'sasl.kerberos.ticket.renew.window.factor'
    | 'sasl.login.callback.handler.class'
    | 'sasl.login.class'
    | 'sasl.login.connect.timeout.ms'
    | 'sasl.login.read.timeout.ms'
    | 'sasl.login.refresh.buffer.seconds'
    | 'sasl.login.refresh.min.period.seconds'
    | 'sasl.login.refresh.window.factor'
    | 'sasl.login.refresh.window.jitter'
    | 'sasl.login.retry.backoff.max.ms'
    | 'sasl.login.retry.backoff.ms'
    | 'sasl.mechanism'
    | 'sasl.oauthbearer.allow.unverified.audience'
    | 'sasl.oauthbearer.allow.unverified.issuer'
    | 'sasl.oauthbearer.assertion.algorithm'
    | 'sasl.oauthbearer.assertion.claim.aud'
    | 'sasl.oauthbearer.assertion.claim.exp.seconds'
    | 'sasl.oauthbearer.assertion.claim.iss'
    | 'sasl.oauthbearer.assertion.claim.jti.include'
    | 'sasl.oauthbearer.assertion.claim.nbf.seconds'
    | 'sasl.oauthbearer.assertion.claim.sub'
    | 'sasl.oauthbearer.assertion.file'
    | 'sasl.oauthbearer.assertion.private.key.file'
    | 'sasl.oauthbearer.assertion.private.key.passphrase'
    | 'sasl.oauthbearer.assertion.template.file'
    | 'sasl.oauthbearer.client.credentials.client.id'
    | 'sasl.oauthbearer.client.credentials.client.secret'
    | 'sasl.oauthbearer.clock.skew.seconds'
    | 'sasl.oauthbearer.expected.audience'
    | 'sasl.oauthbearer.expected.issuer'
    | 'sasl.oauthbearer.header.urlencode'
    | 'sasl.oauthbearer.jwks.endpoint.refresh.ms'
    | 'sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms'
    | 'sasl.oauthbearer.jwks.endpoint.retry.backoff.ms'
    | 'sasl.oauthbearer.jwks.endpoint.url'
    | 'sasl.oauthbearer.jwt.retriever.class'
    | 'sasl.oauthbearer.jwt.validator.class'
    | 'sasl.oauthbearer.scope'
    | 'sasl.oauthbearer.scope.claim.name'
    | 'sasl.oauthbearer.sub.claim.name'
    | 'sasl.oauthbearer.token.endpoint.url'
    | 'security.protocol'
    | 'security.providers'
    | 'send.buffer.bytes'
    | 'socket.connection.setup.timeout.max.ms'
    | 'socket.connection.setup.timeout.ms'
    | 'ssl.cipher.suites'
    | 'ssl.enabled.protocols'
    | 'ssl.endpoint.identification.algorithm'
    | 'ssl.engine.factory.class'
    | 'ssl.key.password'
    | 'ssl.keymanager.algorithm'
    | 'ssl.keystore.certificate.chain'
    | 'ssl.keystore.key'
    | 'ssl.keystore.location'
    | 'ssl.keystore.password'
    | 'ssl.keystore.type'
    | 'ssl.protocol'
    | 'ssl.provider'
    | 'ssl.secure.random.implementation'
    | 'ssl.trustmanager.algorithm'
    | 'ssl.truststore.certificates'
    | 'ssl.truststore.location'
    | 'ssl.truststore.password'
    | 'ssl.truststore.type'
    // Any other key, keeping the known keys' autocompletion: the core
    // lists the keys it does not use.
    | (string & {})]?: ConfigValue;
};

// A value the core parses.
type ConfigValue = string | number | boolean | bigint | ReadonlyArray<string | number | bigint> | null | undefined;
