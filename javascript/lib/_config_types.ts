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
// Java's `ProducerConfig` (with `SslConfigs` / `SaslConfigs`);
// `cargo xtask check-generated` checks it is up to date.

// The `ConfigDef` type of every producer config key (Java's `ProducerConfig`,
// with `withClientSslSupport()` / `withClientSaslSupport()`), and what its
// validator does with `null` (private; python/confluent_kafka/_config_types.py's
// PRODUCER / PRODUCER_NULL). `_config` checks a value's JavaScript type against
// its key's type and passes every other key to the core as given.
//
// PRODUCER_NULL maps each key whose `ConfigDef` validator rejects an explicit
// `null` (`ConfigDef.parseValue` runs the validator on it) to the
// `ConfigException` Java throws: ['named', message] for
// `ConfigException(name, null, message)`, ['plain', message] for
// `ConfigException(message)`. A key that is not there holds `null` in Java.

export const PRODUCER: ReadonlyMap<string, string> = new Map<string, string>([
  ['acks', 'STRING'],
  ['batch.size', 'INT'],
  ['bootstrap.resolve.timeout.ms', 'LONG'],
  ['bootstrap.servers', 'LIST'],
  ['buffer.memory', 'LONG'],
  ['buffer.memory.allocation.strategy', 'STRING'],
  ['client.dns.lookup', 'STRING'],
  ['client.id', 'STRING'],
  ['client.rack', 'STRING'],
  ['compression.gzip.level', 'INT'],
  ['compression.lz4.level', 'INT'],
  ['compression.type', 'STRING'],
  ['compression.zstd.level', 'INT'],
  ['config.providers', 'LIST'],
  ['connections.max.idle.ms', 'LONG'],
  ['delivery.timeout.ms', 'INT'],
  ['enable.idempotence', 'BOOLEAN'],
  ['enable.metrics.push', 'BOOLEAN'],
  ['interceptor.classes', 'LIST'],
  ['key.serializer', 'CLASS'],
  ['linger.ms', 'LONG'],
  ['max.block.ms', 'LONG'],
  ['max.in.flight.requests.per.connection', 'INT'],
  ['max.request.size', 'INT'],
  ['metadata.cluster.check.enable', 'BOOLEAN'],
  ['metadata.max.age.ms', 'LONG'],
  ['metadata.max.idle.ms', 'LONG'],
  ['metadata.recovery.rebootstrap.trigger.ms', 'LONG'],
  ['metadata.recovery.strategy', 'STRING'],
  ['metric.reporters', 'LIST'],
  ['metrics.num.samples', 'INT'],
  ['metrics.recording.level', 'STRING'],
  ['metrics.sample.window.ms', 'LONG'],
  ['partitioner.adaptive.partitioning.enable', 'BOOLEAN'],
  ['partitioner.availability.timeout.ms', 'LONG'],
  ['partitioner.class', 'CLASS'],
  ['partitioner.ignore.keys', 'BOOLEAN'],
  ['partitioner.rack.aware', 'BOOLEAN'],
  ['receive.buffer.bytes', 'INT'],
  ['reconnect.backoff.max.ms', 'LONG'],
  ['reconnect.backoff.ms', 'LONG'],
  ['request.timeout.ms', 'INT'],
  ['retries', 'INT'],
  ['retry.backoff.max.ms', 'LONG'],
  ['retry.backoff.ms', 'LONG'],
  ['sasl.client.callback.handler.class', 'CLASS'],
  ['sasl.jaas.config', 'PASSWORD'],
  ['sasl.kerberos.kinit.cmd', 'STRING'],
  ['sasl.kerberos.min.time.before.relogin', 'LONG'],
  ['sasl.kerberos.service.name', 'STRING'],
  ['sasl.kerberos.ticket.renew.jitter', 'DOUBLE'],
  ['sasl.kerberos.ticket.renew.window.factor', 'DOUBLE'],
  ['sasl.login.callback.handler.class', 'CLASS'],
  ['sasl.login.class', 'CLASS'],
  ['sasl.login.connect.timeout.ms', 'INT'],
  ['sasl.login.read.timeout.ms', 'INT'],
  ['sasl.login.refresh.buffer.seconds', 'SHORT'],
  ['sasl.login.refresh.min.period.seconds', 'SHORT'],
  ['sasl.login.refresh.window.factor', 'DOUBLE'],
  ['sasl.login.refresh.window.jitter', 'DOUBLE'],
  ['sasl.login.retry.backoff.max.ms', 'LONG'],
  ['sasl.login.retry.backoff.ms', 'LONG'],
  ['sasl.mechanism', 'STRING'],
  ['sasl.oauthbearer.allow.unverified.audience', 'BOOLEAN'],
  ['sasl.oauthbearer.allow.unverified.issuer', 'BOOLEAN'],
  ['sasl.oauthbearer.assertion.algorithm', 'STRING'],
  ['sasl.oauthbearer.assertion.claim.aud', 'STRING'],
  ['sasl.oauthbearer.assertion.claim.exp.seconds', 'INT'],
  ['sasl.oauthbearer.assertion.claim.iss', 'STRING'],
  ['sasl.oauthbearer.assertion.claim.jti.include', 'BOOLEAN'],
  ['sasl.oauthbearer.assertion.claim.nbf.seconds', 'INT'],
  ['sasl.oauthbearer.assertion.claim.sub', 'STRING'],
  ['sasl.oauthbearer.assertion.file', 'STRING'],
  ['sasl.oauthbearer.assertion.private.key.file', 'STRING'],
  ['sasl.oauthbearer.assertion.private.key.passphrase', 'PASSWORD'],
  ['sasl.oauthbearer.assertion.template.file', 'STRING'],
  ['sasl.oauthbearer.client.credentials.client.id', 'STRING'],
  ['sasl.oauthbearer.client.credentials.client.secret', 'PASSWORD'],
  ['sasl.oauthbearer.clock.skew.seconds', 'INT'],
  ['sasl.oauthbearer.expected.audience', 'LIST'],
  ['sasl.oauthbearer.expected.issuer', 'STRING'],
  ['sasl.oauthbearer.header.urlencode', 'BOOLEAN'],
  ['sasl.oauthbearer.jwks.endpoint.refresh.ms', 'LONG'],
  ['sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms', 'LONG'],
  ['sasl.oauthbearer.jwks.endpoint.retry.backoff.ms', 'LONG'],
  ['sasl.oauthbearer.jwks.endpoint.url', 'STRING'],
  ['sasl.oauthbearer.jwt.retriever.class', 'CLASS'],
  ['sasl.oauthbearer.jwt.validator.class', 'CLASS'],
  ['sasl.oauthbearer.scope', 'STRING'],
  ['sasl.oauthbearer.scope.claim.name', 'STRING'],
  ['sasl.oauthbearer.sub.claim.name', 'STRING'],
  ['sasl.oauthbearer.token.endpoint.url', 'STRING'],
  ['security.protocol', 'STRING'],
  ['security.providers', 'STRING'],
  ['send.buffer.bytes', 'INT'],
  ['socket.connection.setup.timeout.max.ms', 'LONG'],
  ['socket.connection.setup.timeout.ms', 'LONG'],
  ['ssl.cipher.suites', 'LIST'],
  ['ssl.enabled.protocols', 'LIST'],
  ['ssl.endpoint.identification.algorithm', 'STRING'],
  ['ssl.engine.factory.class', 'CLASS'],
  ['ssl.key.password', 'PASSWORD'],
  ['ssl.keymanager.algorithm', 'STRING'],
  ['ssl.keystore.certificate.chain', 'PASSWORD'],
  ['ssl.keystore.key', 'PASSWORD'],
  ['ssl.keystore.location', 'STRING'],
  ['ssl.keystore.password', 'PASSWORD'],
  ['ssl.keystore.type', 'STRING'],
  ['ssl.protocol', 'STRING'],
  ['ssl.provider', 'STRING'],
  ['ssl.secure.random.implementation', 'STRING'],
  ['ssl.trustmanager.algorithm', 'STRING'],
  ['ssl.truststore.certificates', 'PASSWORD'],
  ['ssl.truststore.location', 'STRING'],
  ['ssl.truststore.password', 'PASSWORD'],
  ['ssl.truststore.type', 'STRING'],
  ['transaction.timeout.ms', 'INT'],
  ['transaction.two.phase.commit.enable', 'BOOLEAN'],
  ['transactional.id', 'STRING'],
  ['value.serializer', 'CLASS'],
]);

/** The `ConfigException` a key's validator throws for `null`: its form and message. */
type NullRule = readonly ['named' | 'plain', string];

export const PRODUCER_NULL: ReadonlyMap<string, NullRule> = new Map<string, NullRule>([
  ['acks', ['named', 'String must be one of: all, -1, 0, 1']], // ValidString
  ['batch.size', ['named', 'Value must be non-null']], // Range
  ['bootstrap.resolve.timeout.ms', ['named', 'Value must be non-null']], // Range
  ['bootstrap.servers', ['plain', 'Configuration \'bootstrap.servers\' values must not be null.']], // ValidList
  ['buffer.memory', ['named', 'Value must be non-null']], // Range
  ['buffer.memory.allocation.strategy', ['named', 'String must be one of (case insensitive): FULL, INCREMENTAL']], // CaseInsensitiveValidString
  ['client.dns.lookup', ['named', 'String must be one of: use_all_dns_ips, resolve_canonical_bootstrap_servers_only']], // ValidString
  ['compression.gzip.level', ['named', 'Value must be non-null']], // Range
  ['compression.lz4.level', ['named', 'Value must be non-null']], // Range
  ['compression.type', ['named', 'String must be one of: none, gzip, snappy, lz4, zstd']], // ValidString
  ['compression.zstd.level', ['named', 'Value must be non-null']], // Range
  ['config.providers', ['plain', 'Configuration \'config.providers\' values must not be null.']], // ValidList
  ['delivery.timeout.ms', ['named', 'Value must be non-null']], // Range
  ['interceptor.classes', ['plain', 'Configuration \'interceptor.classes\' values must not be null.']], // ValidList
  ['linger.ms', ['named', 'Value must be non-null']], // Range
  ['max.block.ms', ['named', 'Value must be non-null']], // Range
  ['max.in.flight.requests.per.connection', ['named', 'Value must be non-null']], // Range
  ['max.request.size', ['named', 'Value must be non-null']], // Range
  ['metadata.max.age.ms', ['named', 'Value must be non-null']], // Range
  ['metadata.max.idle.ms', ['named', 'Value must be non-null']], // Range
  ['metadata.recovery.rebootstrap.trigger.ms', ['named', 'Value must be non-null']], // Range
  ['metadata.recovery.strategy', ['named', 'String must be one of (case insensitive): REBOOTSTRAP, NONE']], // CaseInsensitiveValidString
  ['metric.reporters', ['plain', 'Configuration \'metric.reporters\' values must not be null.']], // ValidList
  ['metrics.num.samples', ['named', 'Value must be non-null']], // Range
  ['metrics.recording.level', ['named', 'String must be one of: INFO, DEBUG, TRACE']], // ValidString
  ['metrics.sample.window.ms', ['named', 'Value must be non-null']], // Range
  ['partitioner.availability.timeout.ms', ['named', 'Value must be non-null']], // Range
  ['receive.buffer.bytes', ['named', 'Value must be non-null']], // Range
  ['reconnect.backoff.max.ms', ['named', 'Value must be non-null']], // Range
  ['reconnect.backoff.ms', ['named', 'Value must be non-null']], // Range
  ['request.timeout.ms', ['named', 'Value must be non-null']], // Range
  ['retries', ['named', 'Value must be non-null']], // Range
  ['retry.backoff.max.ms', ['named', 'Value must be non-null']], // Range
  ['retry.backoff.ms', ['named', 'Value must be non-null']], // Range
  ['sasl.login.refresh.buffer.seconds', ['named', 'Value must be non-null']], // Range
  ['sasl.login.refresh.min.period.seconds', ['named', 'Value must be non-null']], // Range
  ['sasl.login.refresh.window.factor', ['named', 'Value must be non-null']], // Range
  ['sasl.login.refresh.window.jitter', ['named', 'Value must be non-null']], // Range
  ['sasl.oauthbearer.assertion.algorithm', ['named', 'String must be one of (case insensitive): ES256, RS256']], // CaseInsensitiveValidString
  ['sasl.oauthbearer.assertion.claim.exp.seconds', ['named', 'Value must be non-null']], // Range
  ['sasl.oauthbearer.assertion.claim.nbf.seconds', ['named', 'Value must be non-null']], // Range
  ['sasl.oauthbearer.expected.audience', ['plain', 'Configuration \'sasl.oauthbearer.expected.audience\' values must not be null.']], // ValidList
  ['security.protocol', ['named', 'String must be one of (case insensitive): SASL_SSL, PLAINTEXT, SSL, SASL_PLAINTEXT']], // CaseInsensitiveValidString
  ['send.buffer.bytes', ['named', 'Value must be non-null']], // Range
  ['ssl.cipher.suites', ['plain', 'Configuration \'ssl.cipher.suites\' values must not be null.']], // ValidList
  ['ssl.enabled.protocols', ['plain', 'Configuration \'ssl.enabled.protocols\' values must not be null.']], // ValidList
]);
