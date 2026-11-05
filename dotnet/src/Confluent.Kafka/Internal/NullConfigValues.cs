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

using System;
using System.Collections.Generic;

using Confluent.Kafka.Common.Config;

namespace Confluent.Kafka.Internal;

/// <summary>
/// A client config's null values, answered as Java's <c>ConfigDef.parseValue</c> answers them: a key whose validator
/// rejects an explicit <c>null</c> throws that validator's <see cref="ConfigException"/>; any other null value is not
/// set, so the core's default applies.
/// </summary>
/// <remarks>
/// One table per client, from Java's <c>ProducerConfig</c>, <c>ConsumerConfig</c> and <c>AdminClientConfig</c>, with
/// their SSL and SASL keys. A unit test compares the tables with the Python binding's, which the build generates from
/// the same Java sources. A recorded deviation: for a key without a validator Java holds <c>null</c>, where the core,
/// which takes every value as a string, holds its default.
/// </remarks>
internal static class NullConfigValues
{
    internal static readonly IReadOnlyDictionary<string, NullRejection> Producer =
        new Dictionary<string, NullRejection>(StringComparer.Ordinal)
        {
            ["acks"] = new NullRejection(true, "String must be one of: all, -1, 0, 1"),
            ["batch.size"] = new NullRejection(true, "Value must be non-null"),
            ["bootstrap.resolve.timeout.ms"] = new NullRejection(true, "Value must be non-null"),
            ["bootstrap.servers"] = new NullRejection(false, "Configuration 'bootstrap.servers' values must not be null."),
            ["buffer.memory"] = new NullRejection(true, "Value must be non-null"),
            ["buffer.memory.allocation.strategy"] = new NullRejection(true, "String must be one of (case insensitive): FULL, INCREMENTAL"),
            ["client.dns.lookup"] = new NullRejection(true, "String must be one of: use_all_dns_ips, resolve_canonical_bootstrap_servers_only"),
            ["compression.gzip.level"] = new NullRejection(true, "Value must be non-null"),
            ["compression.lz4.level"] = new NullRejection(true, "Value must be non-null"),
            ["compression.type"] = new NullRejection(true, "String must be one of: none, gzip, snappy, lz4, zstd"),
            ["compression.zstd.level"] = new NullRejection(true, "Value must be non-null"),
            ["config.providers"] = new NullRejection(false, "Configuration 'config.providers' values must not be null."),
            ["delivery.timeout.ms"] = new NullRejection(true, "Value must be non-null"),
            ["interceptor.classes"] = new NullRejection(false, "Configuration 'interceptor.classes' values must not be null."),
            ["linger.ms"] = new NullRejection(true, "Value must be non-null"),
            ["max.block.ms"] = new NullRejection(true, "Value must be non-null"),
            ["max.in.flight.requests.per.connection"] = new NullRejection(true, "Value must be non-null"),
            ["max.request.size"] = new NullRejection(true, "Value must be non-null"),
            ["metadata.max.age.ms"] = new NullRejection(true, "Value must be non-null"),
            ["metadata.max.idle.ms"] = new NullRejection(true, "Value must be non-null"),
            ["metadata.recovery.rebootstrap.trigger.ms"] = new NullRejection(true, "Value must be non-null"),
            ["metadata.recovery.strategy"] = new NullRejection(true, "String must be one of (case insensitive): REBOOTSTRAP, NONE"),
            ["metric.reporters"] = new NullRejection(false, "Configuration 'metric.reporters' values must not be null."),
            ["metrics.num.samples"] = new NullRejection(true, "Value must be non-null"),
            ["metrics.recording.level"] = new NullRejection(true, "String must be one of: INFO, DEBUG, TRACE"),
            ["metrics.sample.window.ms"] = new NullRejection(true, "Value must be non-null"),
            ["partitioner.availability.timeout.ms"] = new NullRejection(true, "Value must be non-null"),
            ["receive.buffer.bytes"] = new NullRejection(true, "Value must be non-null"),
            ["reconnect.backoff.max.ms"] = new NullRejection(true, "Value must be non-null"),
            ["reconnect.backoff.ms"] = new NullRejection(true, "Value must be non-null"),
            ["request.timeout.ms"] = new NullRejection(true, "Value must be non-null"),
            ["retries"] = new NullRejection(true, "Value must be non-null"),
            ["retry.backoff.max.ms"] = new NullRejection(true, "Value must be non-null"),
            ["retry.backoff.ms"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.login.refresh.buffer.seconds"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.login.refresh.min.period.seconds"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.login.refresh.window.factor"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.login.refresh.window.jitter"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.oauthbearer.assertion.algorithm"] = new NullRejection(true, "String must be one of (case insensitive): ES256, RS256"),
            ["sasl.oauthbearer.assertion.claim.exp.seconds"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.oauthbearer.assertion.claim.nbf.seconds"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.oauthbearer.expected.audience"] = new NullRejection(false, "Configuration 'sasl.oauthbearer.expected.audience' values must not be null."),
            ["security.protocol"] = new NullRejection(true, "String must be one of (case insensitive): SASL_SSL, PLAINTEXT, SSL, SASL_PLAINTEXT"),
            ["send.buffer.bytes"] = new NullRejection(true, "Value must be non-null"),
            ["ssl.cipher.suites"] = new NullRejection(false, "Configuration 'ssl.cipher.suites' values must not be null."),
            ["ssl.enabled.protocols"] = new NullRejection(false, "Configuration 'ssl.enabled.protocols' values must not be null."),
        };

    internal static readonly IReadOnlyDictionary<string, NullRejection> Consumer =
        new Dictionary<string, NullRejection>(StringComparer.Ordinal)
        {
            ["auto.commit.interval.ms"] = new NullRejection(true, "Value must be non-null"),
            ["auto.offset.reset"] = new NullRejection(true, "Invalid value `null` for configuration auto.offset.reset. The value must be either 'earliest', 'latest', 'none' or of the format 'by_duration:<PnDTnHnMn.nS.>'."),
            ["bootstrap.resolve.timeout.ms"] = new NullRejection(true, "Value must be non-null"),
            ["bootstrap.servers"] = new NullRejection(false, "Configuration 'bootstrap.servers' values must not be null."),
            ["client.dns.lookup"] = new NullRejection(true, "String must be one of: use_all_dns_ips, resolve_canonical_bootstrap_servers_only"),
            ["config.providers"] = new NullRejection(false, "Configuration 'config.providers' values must not be null."),
            ["default.api.timeout.ms"] = new NullRejection(true, "Value must be non-null"),
            ["fetch.max.bytes"] = new NullRejection(true, "Value must be non-null"),
            ["fetch.max.wait.ms"] = new NullRejection(true, "Value must be non-null"),
            ["fetch.min.bytes"] = new NullRejection(true, "Value must be non-null"),
            ["group.protocol"] = new NullRejection(true, "String must be one of (case insensitive): CONSUMER, CLASSIC"),
            ["interceptor.classes"] = new NullRejection(false, "Configuration 'interceptor.classes' values must not be null."),
            ["isolation.level"] = new NullRejection(true, "String must be one of: read_committed, read_uncommitted"),
            ["max.partition.fetch.bytes"] = new NullRejection(true, "Value must be non-null"),
            ["max.poll.interval.ms"] = new NullRejection(true, "Value must be non-null"),
            ["max.poll.records"] = new NullRejection(true, "Value must be non-null"),
            ["metadata.max.age.ms"] = new NullRejection(true, "Value must be non-null"),
            ["metadata.recovery.rebootstrap.trigger.ms"] = new NullRejection(true, "Value must be non-null"),
            ["metadata.recovery.strategy"] = new NullRejection(true, "String must be one of (case insensitive): REBOOTSTRAP, NONE"),
            ["metric.reporters"] = new NullRejection(false, "Configuration 'metric.reporters' values must not be null."),
            ["metrics.num.samples"] = new NullRejection(true, "Value must be non-null"),
            ["metrics.recording.level"] = new NullRejection(true, "String must be one of: INFO, DEBUG, TRACE"),
            ["metrics.sample.window.ms"] = new NullRejection(true, "Value must be non-null"),
            ["partition.assignment.strategy"] = new NullRejection(false, "Configuration 'partition.assignment.strategy' values must not be null."),
            ["receive.buffer.bytes"] = new NullRejection(true, "Value must be non-null"),
            ["reconnect.backoff.max.ms"] = new NullRejection(true, "Value must be non-null"),
            ["reconnect.backoff.ms"] = new NullRejection(true, "Value must be non-null"),
            ["request.timeout.ms"] = new NullRejection(true, "Value must be non-null"),
            ["retry.backoff.max.ms"] = new NullRejection(true, "Value must be non-null"),
            ["retry.backoff.ms"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.login.refresh.buffer.seconds"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.login.refresh.min.period.seconds"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.login.refresh.window.factor"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.login.refresh.window.jitter"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.oauthbearer.assertion.algorithm"] = new NullRejection(true, "String must be one of (case insensitive): ES256, RS256"),
            ["sasl.oauthbearer.assertion.claim.exp.seconds"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.oauthbearer.assertion.claim.nbf.seconds"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.oauthbearer.expected.audience"] = new NullRejection(false, "Configuration 'sasl.oauthbearer.expected.audience' values must not be null."),
            ["security.protocol"] = new NullRejection(true, "String must be one of (case insensitive): SASL_SSL, PLAINTEXT, SSL, SASL_PLAINTEXT"),
            ["send.buffer.bytes"] = new NullRejection(true, "Value must be non-null"),
            ["share.acknowledgement.mode"] = new NullRejection(true, "Invalid value `null` for configuration share.acknowledgement.mode. The value must either be 'implicit' or 'explicit'."),
            ["share.acquire.mode"] = new NullRejection(true, "Invalid value `null` for configuration share.acquire.mode. The value must either be 'batch_optimized' or 'record_limit'."),
            ["ssl.cipher.suites"] = new NullRejection(false, "Configuration 'ssl.cipher.suites' values must not be null."),
            ["ssl.enabled.protocols"] = new NullRejection(false, "Configuration 'ssl.enabled.protocols' values must not be null."),
        };

    internal static readonly IReadOnlyDictionary<string, NullRejection> Admin =
        new Dictionary<string, NullRejection>(StringComparer.Ordinal)
        {
            ["bootstrap.controllers"] = new NullRejection(false, "Configuration 'bootstrap.controllers' values must not be null."),
            ["bootstrap.resolve.timeout.ms"] = new NullRejection(true, "Value must be non-null"),
            ["bootstrap.servers"] = new NullRejection(false, "Configuration 'bootstrap.servers' values must not be null."),
            ["client.dns.lookup"] = new NullRejection(true, "String must be one of: use_all_dns_ips, resolve_canonical_bootstrap_servers_only"),
            ["config.providers"] = new NullRejection(false, "Configuration 'config.providers' values must not be null."),
            ["default.api.timeout.ms"] = new NullRejection(true, "Value must be non-null"),
            ["metadata.max.age.ms"] = new NullRejection(true, "Value must be non-null"),
            ["metadata.recovery.rebootstrap.trigger.ms"] = new NullRejection(true, "Value must be non-null"),
            ["metadata.recovery.strategy"] = new NullRejection(true, "String must be one of (case insensitive): REBOOTSTRAP, NONE"),
            ["metric.reporters"] = new NullRejection(false, "Configuration 'metric.reporters' values must not be null."),
            ["metrics.num.samples"] = new NullRejection(true, "Value must be non-null"),
            ["metrics.recording.level"] = new NullRejection(true, "String must be one of: INFO, DEBUG, TRACE"),
            ["metrics.sample.window.ms"] = new NullRejection(true, "Value must be non-null"),
            ["receive.buffer.bytes"] = new NullRejection(true, "Value must be non-null"),
            ["reconnect.backoff.max.ms"] = new NullRejection(true, "Value must be non-null"),
            ["reconnect.backoff.ms"] = new NullRejection(true, "Value must be non-null"),
            ["request.timeout.ms"] = new NullRejection(true, "Value must be non-null"),
            ["retries"] = new NullRejection(true, "Value must be non-null"),
            ["retry.backoff.max.ms"] = new NullRejection(true, "Value must be non-null"),
            ["retry.backoff.ms"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.login.refresh.buffer.seconds"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.login.refresh.min.period.seconds"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.login.refresh.window.factor"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.login.refresh.window.jitter"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.oauthbearer.assertion.algorithm"] = new NullRejection(true, "String must be one of (case insensitive): ES256, RS256"),
            ["sasl.oauthbearer.assertion.claim.exp.seconds"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.oauthbearer.assertion.claim.nbf.seconds"] = new NullRejection(true, "Value must be non-null"),
            ["sasl.oauthbearer.expected.audience"] = new NullRejection(false, "Configuration 'sasl.oauthbearer.expected.audience' values must not be null."),
            ["security.protocol"] = new NullRejection(true, "String must be one of (case insensitive): SASL_SSL, PLAINTEXT, SSL, SASL_PLAINTEXT"),
            ["send.buffer.bytes"] = new NullRejection(true, "Value must be non-null"),
            ["ssl.cipher.suites"] = new NullRejection(false, "Configuration 'ssl.cipher.suites' values must not be null."),
            ["ssl.enabled.protocols"] = new NullRejection(false, "Configuration 'ssl.enabled.protocols' values must not be null."),
        };

    /// <summary>
    /// Throws for the first null value whose key rejects one, in the config's order; otherwise returns the config
    /// without its null values (the same instance when it has none).
    /// </summary>
    internal static IReadOnlyDictionary<string, string> Apply(
        IReadOnlyDictionary<string, string> config, IReadOnlyDictionary<string, NullRejection> rejections)
    {
        Dictionary<string, string>? set = null;
        foreach (KeyValuePair<string, string> entry in config)
        {
            if (entry.Value is not null)
            {
                continue;
            }

            if (rejections.TryGetValue(entry.Key, out NullRejection rejection))
            {
                throw rejection.Named
                    ? new ConfigException(entry.Key, null, rejection.Message)
                    : new ConfigException(rejection.Message);
            }

            if (set is null)
            {
                set = new Dictionary<string, string>(config.Count, StringComparer.Ordinal);
                foreach (KeyValuePair<string, string> kept in config)
                {
                    if (kept.Value is not null)
                    {
                        set.Add(kept.Key, kept.Value);
                    }
                }
            }
        }

        return set ?? config;
    }

    /// <summary>
    /// The <see cref="ConfigException"/> a validator throws for <c>null</c>: Java's
    /// <c>ConfigException(name, null, message)</c> when <see cref="Named"/>, else <c>ConfigException(message)</c>.
    /// </summary>
    internal readonly struct NullRejection
    {
        internal NullRejection(bool named, string message)
        {
            Named = named;
            Message = message;
        }

        internal bool Named { get; }

        internal string Message { get; }
    }
}
