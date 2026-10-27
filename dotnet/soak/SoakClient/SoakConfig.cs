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

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace Confluent.Kafka.Soak;

/// <summary>
/// Client-configuration routing, validation and file parsing: the Python soak's config helpers,
/// except <c>stringify_config</c> (every value here is already a string).
/// </summary>
internal static class SoakConfig
{
    // The core accepts an unknown key with one INFO line and uses the default, so a typo would
    // start the soak on a default; as Python's soak, it refuses an unlisted key at startup. Keep
    // the lists in sync with the core's ProducerConfig and ConsumerConfig.

    internal static readonly IReadOnlyCollection<string> ProducerConfigKeys = new HashSet<string>(StringComparer.Ordinal)
    {
        "acks",
        "batch.size",
        "bootstrap.resolve.timeout.ms",
        "bootstrap.servers",
        "buffer.memory",
        "buffer.memory.allocation.strategy",
        "client.dns.lookup",
        "client.id",
        "client.rack",
        "compression.gzip.level",
        "compression.lz4.level",
        "compression.type",
        "compression.zstd.level",
        "connections.max.idle.ms",
        "delivery.timeout.ms",
        "enable.idempotence",
        "linger.ms",
        "max.block.ms",
        "max.in.flight.requests.per.connection",
        "max.request.size",
        "metadata.cluster.check.enable",
        "metadata.max.age.ms",
        "metadata.max.idle.ms",
        "metadata.recovery.rebootstrap.trigger.ms",
        "metadata.recovery.strategy",
        "metrics.num.samples",
        "metrics.recording.level",
        "metrics.sample.window.ms",
        "partitioner.adaptive.partitioning.enable",
        "partitioner.availability.timeout.ms",
        "partitioner.ignore.keys",
        "partitioner.rack.aware",
        "partitioner.type",
        "receive.buffer.bytes",
        "reconnect.backoff.max.ms",
        "reconnect.backoff.ms",
        "request.timeout.ms",
        "retries",
        "retry.backoff.max.ms",
        "retry.backoff.ms",
        "sasl.jaas.config",
        "sasl.mechanism",
        "security.protocol",
        "send.buffer.bytes",
        "socket.connection.setup.timeout.max.ms",
        "socket.connection.setup.timeout.ms",
        "transaction.timeout.ms",
        "transaction.two.phase.commit.enable",
        "transactional.id",
    };

    internal static readonly IReadOnlyCollection<string> ConsumerConfigKeys = new HashSet<string>(StringComparer.Ordinal)
    {
        "allow.auto.create.topics",
        "auto.commit.interval.ms",
        "auto.offset.reset",
        "bootstrap.resolve.timeout.ms",
        "bootstrap.servers",
        "check.crcs",
        "client.dns.lookup",
        "client.id",
        "client.rack",
        "connections.max.idle.ms",
        "default.api.timeout.ms",
        "enable.auto.commit",
        "exclude.internal.topics",
        "fetch.max.bytes",
        "fetch.max.wait.ms",
        "fetch.min.bytes",
        "group.id",
        "group.instance.id",
        "group.protocol",
        "group.remote.assignor",
        "heartbeat.interval.ms",
        "internal.throw.on.fetch.stable.offset.unsupported",
        "isolation.level",
        "max.partition.fetch.bytes",
        "max.poll.interval.ms",
        "max.poll.records",
        "metadata.cluster.check.enable",
        "metadata.max.age.ms",
        "metadata.recovery.rebootstrap.trigger.ms",
        "metadata.recovery.strategy",
        "metrics.num.samples",
        "metrics.recording.level",
        "metrics.sample.window.ms",
        "receive.buffer.bytes",
        "reconnect.backoff.max.ms",
        "reconnect.backoff.ms",
        "request.timeout.ms",
        "retry.backoff.max.ms",
        "retry.backoff.ms",
        "sasl.jaas.config",
        "sasl.mechanism",
        "security.protocol",
        "send.buffer.bytes",
        "session.timeout.ms",
        "share.acknowledgement.mode",
        "share.acquire.mode",
        "socket.connection.setup.timeout.max.ms",
        "socket.connection.setup.timeout.ms",
    };

    internal static readonly IReadOnlyList<string> AcceptedConfigPrefixes = new[] { "ssl." };

    internal static Dictionary<string, string> FilterConfig(
        IReadOnlyDictionary<string, string> conf,
        IReadOnlyCollection<string> filterOut,
        string stripPrefix)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, string> entry in conf)
        {
            if (filterOut.Any(prefix => entry.Key.StartsWith(prefix, StringComparison.Ordinal)))
            {
                continue;
            }

            string key = entry.Key.StartsWith(stripPrefix, StringComparison.Ordinal)
                ? entry.Key.Substring(stripPrefix.Length)
                : entry.Key;
            result[key] = entry.Value;
        }

        return result;
    }

    // A shared file's key known only to the other client is dropped, not rejected, as Python's
    // soak; a key unknown to both still fails startup.
    internal static (Dictionary<string, string> Kept, IReadOnlyList<string> Routed) RouteSharedConfig(
        IReadOnlyDictionary<string, string> conf,
        IReadOnlyCollection<string> ownKeys,
        IReadOnlyCollection<string> otherKeys)
    {
        var routed = conf.Keys
            .Where(key => !ownKeys.Contains(key) && otherKeys.Contains(key))
            .OrderBy(key => key, StringComparer.Ordinal)
            .ToList();

        var kept = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, string> entry in conf)
        {
            if (!routed.Contains(entry.Key))
            {
                kept[entry.Key] = entry.Value;
            }
        }

        return (kept, routed);
    }

    internal static void ValidateConfig(
        IReadOnlyDictionary<string, string> conf,
        IReadOnlyCollection<string> accepted,
        string what)
    {
        var unknown = conf.Keys
            .Where(key => !accepted.Contains(key)
                && !AcceptedConfigPrefixes.Any(prefix => key.StartsWith(prefix, StringComparison.Ordinal)))
            .OrderBy(key => key, StringComparer.Ordinal)
            .ToList();

        if (unknown.Count == 0)
        {
            return;
        }

        throw new ArgumentException(string.Format(
            CultureInfo.InvariantCulture,
            "unknown {0} configuration key(s): {1}. The Rust client only logs a warning "
            + "for unrecognised keys, so this would silently run with the default value. "
            + "Fix the key, or prefix it for another client (producer./consumer./admin.). "
            + "Accepted {2} keys: {3}",
            what,
            string.Join(", ", unknown),
            what,
            string.Join(", ", accepted.OrderBy(key => key, StringComparer.Ordinal))));
    }

    internal static Dictionary<string, string> ParseConfigFile(TextReader reader)
    {
        var conf = new Dictionary<string, string>(StringComparer.Ordinal);
        string? line;
        while ((line = reader.ReadLine()) is not null)
        {
            line = line.Trim();
            if (line.Length == 0 || line[0] == '#')
            {
                continue;
            }

            // Everything after the FIRST '=' is the value, so a JAAS string containing
            // '=' needs no escaping.
            int separator = line.IndexOf('=');
            if (separator <= 0)
            {
                throw new ArgumentException(string.Format(
                    CultureInfo.InvariantCulture,
                    "Configuration lines must be `name=value..`, not {0}",
                    line));
            }

            conf[line.Substring(0, separator)] = line.Substring(separator + 1);
        }

        return conf;
    }

    // Finds a field as the core's SaslConfigs::parse_jaas_option does, so the check and the client agree on
    // whether it is present: a key counts only at an option start outside quotes, and a quoted value that a raw
    // line break cuts short is absent. It returns the raw quoted content, escapes not decoded, unlike the core;
    // the soak only checks that a field is present and never forwards it.
    internal static string? JaasField(string jaasConfig, string name)
    {
        string s = jaasConfig;
        int n = s.Length;

        int idx = 0;
        while (idx < n)
        {
            char c = s[idx];
            if (c == '"' || c == '\'')
            {
                idx = SkipQuoted(s, idx);
                continue;
            }

            bool atOptionStart = idx == 0 || char.IsWhiteSpace(s[idx - 1]);
            if (atOptionStart && StartsWithAt(s, idx, name))
            {
                int j = idx + name.Length;
                while (j < n && char.IsWhiteSpace(s[j]))
                {
                    j += 1;
                }

                if (j < n && s[j] == '=')
                {
                    j += 1;
                    while (j < n && char.IsWhiteSpace(s[j]))
                    {
                        j += 1;
                    }

                    if (j >= n)
                    {
                        return null;
                    }

                    char quote = s[j];
                    if (quote == '"' || quote == '\'')
                    {
                        int start = j + 1;
                        int k = start;
                        while (k < n)
                        {
                            if (s[k] == '\\')
                            {
                                k += 2;
                                continue;
                            }

                            if (s[k] == quote)
                            {
                                return s.Substring(start, k - start);
                            }

                            if (s[k] == '\r' || s[k] == '\n')
                            {
                                return null;
                            }

                            k += 1;
                        }

                        return null;
                    }

                    int bareEnd = j;
                    while (bareEnd < n && !char.IsWhiteSpace(s[bareEnd]) && s[bareEnd] != ';')
                    {
                        bareEnd += 1;
                    }

                    return s.Substring(j, bareEnd - j);
                }
            }

            idx += 1;
        }

        return null;
    }

    private static int SkipQuoted(string s, int i)
    {
        int n = s.Length;
        char quote = s[i];
        i += 1;
        while (i < n)
        {
            if (s[i] == '\\')
            {
                i += 2;
                continue;
            }

            if (s[i] == quote)
            {
                return i + 1;
            }

            if (s[i] == '\r' || s[i] == '\n')
            {
                return i;
            }

            i += 1;
        }

        return i;
    }

    private static bool StartsWithAt(string s, int index, string value)
    {
        if (index + value.Length > s.Length)
        {
            return false;
        }

        return string.CompareOrdinal(s, index, value, 0, value.Length) == 0;
    }

    internal static (string? Username, string? Password) JaasCredentials(string jaasConfig)
    {
        return (JaasField(jaasConfig, "username"), JaasField(jaasConfig, "password"));
    }

    // Fails fast, as Python: SASL without a username and password, or SASL settings under a
    // non-SASL protocol, where the core would connect unauthenticated.
    internal static void CheckAdminCredentials(IReadOnlyDictionary<string, string> conf)
    {
        string protocol = conf.TryGetValue("security.protocol", out string? protocolValue) ? protocolValue : string.Empty;
        bool protocolIsSasl = protocol.IndexOf("SASL", StringComparison.OrdinalIgnoreCase) >= 0;
        string mechanism = conf.TryGetValue("sasl.mechanism", out string? mechanismValue) ? mechanismValue : string.Empty;
        string? jaas = conf.TryGetValue("sasl.jaas.config", out string? jaasValue) ? jaasValue : null;

        (string? username, string? password) = !string.IsNullOrEmpty(jaas)
            ? JaasCredentials(jaas)
            : (null, null);
        bool saslCredsPresent = username is not null || password is not null;
        bool saslConfigured = mechanism.Length > 0 || saslCredsPresent;

        if (saslConfigured && !protocolIsSasl)
        {
            throw new SoakFatalStartupException(string.Format(
                CultureInfo.InvariantCulture,
                "sasl.mechanism='{0}' / sasl.jaas.config {1}, but security.protocol='{2}' is not a "
                + "SASL protocol, so the client would connect WITHOUT SASL (PLAINTEXT, the Rust "
                + "default) — silently unauthenticated. Set security.protocol to a SASL protocol "
                + "(e.g. SASL_SSL or SASL_PLAINTEXT). Restarting will not fix this.",
                mechanism,
                saslCredsPresent ? "has credentials" : "has no credentials",
                protocol));
        }

        bool saslExpected = protocolIsSasl || mechanism.Length > 0;
        if (!saslExpected)
        {
            return;
        }

        if (username is not null && password is not null)
        {
            return;
        }

        var missing = new List<string>();
        if (username is null)
        {
            missing.Add("username");
        }

        if (password is null)
        {
            missing.Add("password");
        }

        throw new SoakFatalStartupException(string.Format(
            CultureInfo.InvariantCulture,
            "security.protocol='{0}' / sasl.mechanism='{1}' require credentials, but "
            + "sasl.jaas.config {2}: could not extract {3}. Expected Java JAAS form: "
            + "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required "
            + "username=\"KEY\" password=\"SECRET\"; Restarting will not fix this.",
            protocol,
            mechanism,
            string.IsNullOrEmpty(jaas) ? "is not set" : "is set but unparseable",
            string.Join(" and ", missing)));
    }
}
